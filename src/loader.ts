/**
 * Pull-request driver. Owns wallet, signs extraData via synapse-core, then does
 * raw fetch() against ${CURIO_URL}/pdp/piece/pull so we can observe status codes
 * (especially 429 with Retry-After) and exact response bodies that the
 * synapse-sdk's pullPieces() wrapper hides inside PullError.
 *
 * For each pull we get back:
 *   - HTTP status code, Retry-After header
 *   - Parsed PullResponse on 200 (overall + per-piece status)
 *   - Per-attempt timing
 * The driver also exposes pollUntilDone() which re-POSTs the same idempotent
 * request until terminal.
 */

import { createPublicClient, createWalletClient, http, type Address, type Hex, type PublicClient, type WalletClient } from 'viem'
import { privateKeyToAccount } from 'viem/accounts'
import { calibration, mainnet, type Chain as FocChain } from '@filoz/synapse-core/chains'
import { parse as parsePieceCid } from '@filoz/synapse-core/piece'
import { signAddPieces, signCreateDataSetAndAddPieces } from '@filoz/synapse-core/typed-data'
import { randU256 } from '@filoz/synapse-core/utils'
import { getClientDataSets } from '@filoz/synapse-core/warm-storage'
import { getPDPProvider, getPDPProviders } from '@filoz/synapse-core/sp-registry'
import type { DerivedPiece } from './piece.js'

export type Network = 'calibration' | 'mainnet'

export interface LoaderConfig {
  network: Network
  privateKey: Hex
  /** Source server's public URL — Curio dials this. */
  sourcePublicUrl: string
  /** Provider ID in the on-chain SP registry. Resolves curioUrl + payee. */
  providerId?: bigint
  /** Explicit Curio URL override. If unset, discovered from providerId. */
  curioUrl?: string
  /** 0n = pick an existing dataset for this signer (auto), or create new. */
  dataSetId: bigint
  /** Required only when no existing dataset and providerId can't resolve it. */
  payee?: Address
  /** Defaults to chain.contracts.fwss.address. */
  recordKeeper?: Address
}

export type PullPieceInput = {
  pieceCid: string  // PieceCIDv2
  sourceUrl: string
}

export interface PullResponseBody {
  status: 'pending' | 'inProgress' | 'retrying' | 'complete' | 'failed'
  pieces: Array<{ pieceCid: string; status: PullResponseBody['status'] }>
}

export interface PullSubmitResult {
  /** HTTP status code from /pdp/piece/pull */
  httpStatus: number
  /** Parsed Retry-After header (seconds), if present */
  retryAfterSec?: number
  /** Parsed response body on 200; raw text on error */
  body: PullResponseBody | { error: string }
  /** ms from fetch start to response received */
  elapsedMs: number
  /** Wallet address that signed (also the per-client backpressure key) */
  signer: Address
}

export class PullLoader {
  private readonly cfg: LoaderConfig
  private readonly chain: FocChain
  private readonly client: WalletClient
  private readonly publicClient: PublicClient
  private readonly account: ReturnType<typeof privateKeyToAccount>

  private constructor(cfg: LoaderConfig) {
    this.cfg = cfg
    this.chain = cfg.network === 'calibration' ? calibration : mainnet
    this.account = privateKeyToAccount(cfg.privateKey)
    const transport = http()
    this.client = createWalletClient({ account: this.account, chain: this.chain, transport })
    this.publicClient = createPublicClient({ chain: this.chain, transport })
  }

  /**
   * Factory: builds a loader and runs auto-discovery for any unset fields
   * (dataSetId, payee). Prints what it found so the user can lock the values
   * into .env if they want stability across runs.
   */
  static async create(cfg: LoaderConfig): Promise<PullLoader> {
    const l = new PullLoader(cfg)
    await l.discover()
    return l
  }

  /**
   * Fill in curioUrl, payee, dataSetId from PROVIDER_ID + on-chain lookups
   * if any are missing.
   *
   * Priority:
   *   1. providerId → SP registry → curioUrl (= pdp.serviceURL) + payee
   *   2. Existing dataset for this signer → dataSetId (lets us use addPieces
   *      signing path; payee from dataset is also recorded for visibility)
   *   3. If we still need payee but have neither provider nor dataset → enumerate
   *      active PDP providers and try to host-match curioUrl.
   */
  private async discover(): Promise<void> {
    // Step 1: PROVIDER_ID → curioUrl + payee
    if (this.cfg.providerId != null) {
      const p = await getPDPProvider(this.publicClient as any, { providerId: this.cfg.providerId })
      if (!p) throw new Error(`provider id ${this.cfg.providerId} not found in SP registry`)
      if (!this.cfg.curioUrl) {
        this.cfg.curioUrl = p.pdp.serviceURL.replace(/\/+$/, '')
        console.log(`[discover] curioUrl=${this.cfg.curioUrl} (from providerId=${this.cfg.providerId})`)
      }
      if (!this.cfg.payee) {
        this.cfg.payee = (p as any).payee as Address
        console.log(`[discover] payee=${this.cfg.payee} (from providerId=${this.cfg.providerId})`)
      }
    }

    // Step 2: existing dataset for this signer
    if (this.cfg.dataSetId === 0n) {
      try {
        const sets = await getClientDataSets(this.publicClient as any, { address: this.account.address })
        const usable = sets.filter((s) => s.dataSetId > 0n)
        if (usable.length > 0) {
          // Prefer one matching providerId if we have it; else first.
          const pick = (this.cfg.providerId != null
            ? usable.find((s) => s.providerId === this.cfg.providerId)
            : undefined) ?? usable[0]
          this.cfg.dataSetId = pick.dataSetId
          if (!this.cfg.payee) this.cfg.payee = pick.payee
          console.log(`[discover] dataSetId=${pick.dataSetId} (existing, payee=${pick.payee} providerId=${pick.providerId})`)
          return  // dataset found → addPieces path, payee no longer required
        }
      } catch (e) {
        console.warn(`[discover] getClientDataSets failed: ${(e as Error).message}`)
      }
    } else {
      console.log(`[discover] dataSetId=${this.cfg.dataSetId} (from config)`)
    }

    // Step 3: still no curioUrl? Can't proceed.
    if (!this.cfg.curioUrl) {
      throw new Error('curioUrl unresolved — set PROVIDER_ID (recommended) or CURIO_URL in .env')
    }

    // Step 4: still no payee? Last-resort host match against the SP registry.
    if (!this.cfg.payee) {
      const curioHost = new URL(this.cfg.curioUrl).host.toLowerCase()
      const { providers } = await getPDPProviders(this.publicClient as any)
      const active = providers.filter((p: any) => p.isActive)
      const match = active.find((p: any) => {
        try { return new URL(p.pdp.serviceURL).host.toLowerCase() === curioHost }
        catch { return false }
      })
      if (match) {
        this.cfg.payee = match.payee as Address
        console.log(`[discover] payee=${match.payee} (host-matched providerId=${match.id} serviceURL=${match.pdp.serviceURL})`)
      } else {
        const list = active.map((p: any) => `  PROVIDER_ID=${p.id}  # ${p.pdp.serviceURL}`).join('\n')
        throw new Error(
          `Could not auto-discover payee for ${curioHost}.\n` +
          `Pick one of these and add to .env:\n${list || '  (no active providers on registry)'}`)
      }
    }
  }

  get signerAddress(): Address {
    return this.account.address
  }

  get effectiveDataSetId(): bigint {
    return this.cfg.dataSetId
  }

  get effectivePayee(): Address | undefined {
    return this.cfg.payee
  }

  get effectiveCurioUrl(): string {
    if (!this.cfg.curioUrl) throw new Error('curioUrl unresolved')
    return this.cfg.curioUrl
  }

  /**
   * Build a per-piece sourceUrl that the deterministic source-server can serve.
   * URL path is /piece/{cid} (required by Curio); query carries (i, size) so
   * source can regenerate bytes without state.
   */
  buildSourceUrl(piece: DerivedPiece): string {
    const u = new URL(`${this.cfg.sourcePublicUrl}/piece/${piece.pieceCid}`)
    u.searchParams.set('i', String(piece.index))
    u.searchParams.set('size', String(piece.size))
    return u.toString()
  }

  /**
   * Sign extraData appropriate for the (dataSetId, pieces) tuple. Each call
   * uses a fresh client-dataset-id nonce so two submissions hash to different
   * idempotency keys (otherwise the second one returns the first one's status).
   */
  async signExtraData(pieces: DerivedPiece[]): Promise<Hex> {
    const signablePieces = pieces.map((p) => ({ pieceCid: parsePieceCid(p.pieceCid) }))
    if (this.cfg.dataSetId === 0n) {
      if (!this.cfg.payee) throw new Error('payee is required when dataSetId=0')
      return signCreateDataSetAndAddPieces(this.client as any, {
        payee: this.cfg.payee,
        payer: this.account.address,
        pieces: signablePieces,
        clientDataSetId: randU256(),
      })
    }
    return signAddPieces(this.client as any, {
      clientDataSetId: randU256(),
      pieces: signablePieces,
    })
  }

  /** Submit a pull request and return the raw HTTP outcome. */
  async submit(pieces: DerivedPiece[], extraData?: Hex): Promise<PullSubmitResult> {
    const ed = extraData ?? (await this.signExtraData(pieces))
    const recordKeeper = this.cfg.recordKeeper ?? this.chain.contracts.fwss.address
    const body: Record<string, unknown> = {
      extraData: ed,
      recordKeeper,
      pieces: pieces.map((p) => ({
        pieceCid: p.pieceCid,
        sourceUrl: this.buildSourceUrl(p),
      })),
    }
    if (this.cfg.dataSetId > 0n) body.dataSetId = Number(this.cfg.dataSetId)

    if (!this.cfg.curioUrl) throw new Error('curioUrl not set — call PullLoader.create() (which runs discover) instead of new PullLoader()')
    const url = `${this.cfg.curioUrl}/pdp/piece/pull`
    const startedAt = Date.now()
    const res = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    })
    const elapsedMs = Date.now() - startedAt

    const retryAfterRaw = res.headers.get('Retry-After')
    const retryAfterSec = retryAfterRaw ? Number(retryAfterRaw) : undefined

    let parsed: PullResponseBody | { error: string }
    const text = await res.text()
    if (res.ok) {
      try {
        parsed = JSON.parse(text) as PullResponseBody
      } catch (e) {
        parsed = { error: `bad JSON: ${text.slice(0, 200)}` }
      }
    } else {
      parsed = { error: text.slice(0, 500) }
    }

    return {
      httpStatus: res.status,
      retryAfterSec: Number.isFinite(retryAfterSec) ? retryAfterSec : undefined,
      body: parsed,
      elapsedMs,
      signer: this.account.address,
    }
  }

  /**
   * Repeatedly re-submit the *same* idempotent pull (same extraData) until
   * terminal status (complete / failed) or timeout. Returns the final
   * PullSubmitResult plus the count of polls.
   */
  async pollUntilDone(
    pieces: DerivedPiece[],
    opts: { timeoutMs: number; intervalMs?: number; onStatus?: (r: PullSubmitResult) => void } = { timeoutMs: 10 * 60_000 },
  ): Promise<{ final: PullSubmitResult; polls: number }> {
    const interval = opts.intervalMs ?? 4000
    const deadline = Date.now() + opts.timeoutMs
    const extraData = await this.signExtraData(pieces)

    let polls = 0
    while (true) {
      polls++
      const r = await this.submit(pieces, extraData)
      opts.onStatus?.(r)
      if (r.httpStatus !== 200) {
        return { final: r, polls }
      }
      const body = r.body as PullResponseBody
      if (body.status === 'complete' || body.status === 'failed') {
        return { final: r, polls }
      }
      if (Date.now() + interval > deadline) {
        return { final: r, polls }
      }
      await sleep(interval)
    }
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms))
}

// ---------- Aggregated stats ----------

export interface SubmitStats {
  total: number
  byStatus: Map<number, number>
  retryAfterCount: number
  retryAfterMin?: number
  retryAfterMax?: number
  elapsedMsP50: number
  elapsedMsP95: number
  elapsedMsP99: number
}

export function summarize(results: PullSubmitResult[]): SubmitStats {
  const byStatus = new Map<number, number>()
  let retryAfterCount = 0
  let retryAfterMin: number | undefined
  let retryAfterMax: number | undefined
  const elapsed: number[] = []
  for (const r of results) {
    byStatus.set(r.httpStatus, (byStatus.get(r.httpStatus) ?? 0) + 1)
    if (r.retryAfterSec != null) {
      retryAfterCount++
      retryAfterMin = retryAfterMin == null ? r.retryAfterSec : Math.min(retryAfterMin, r.retryAfterSec)
      retryAfterMax = retryAfterMax == null ? r.retryAfterSec : Math.max(retryAfterMax, r.retryAfterSec)
    }
    elapsed.push(r.elapsedMs)
  }
  elapsed.sort((a, b) => a - b)
  const pct = (p: number) => elapsed.length === 0 ? 0 : elapsed[Math.min(elapsed.length - 1, Math.floor(elapsed.length * p))]
  return {
    total: results.length,
    byStatus,
    retryAfterCount,
    retryAfterMin,
    retryAfterMax,
    elapsedMsP50: pct(0.5),
    elapsedMsP95: pct(0.95),
    elapsedMsP99: pct(0.99),
  }
}

export function formatStats(s: SubmitStats): string {
  const codes = Array.from(s.byStatus.entries()).sort(([a], [b]) => a - b)
    .map(([k, v]) => `${k}=${v}`).join(' ')
  const ra = s.retryAfterCount === 0
    ? ''
    : ` retryAfter:${s.retryAfterCount} (${s.retryAfterMin}s..${s.retryAfterMax}s)`
  return `n=${s.total} ${codes}${ra} p50=${s.elapsedMsP50}ms p95=${s.elapsedMsP95}ms p99=${s.elapsedMsP99}ms`
}
