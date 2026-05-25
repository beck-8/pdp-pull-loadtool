#!/usr/bin/env node

import 'dotenv/config'
import { Command } from 'commander'
import type { Hex, Address } from 'viem'
import { calibration, mainnet } from '@filoz/synapse-core/chains'
import { SourceServer } from './source-server.js'
import { PullLoader, formatStats, summarize, type LoaderConfig, type Network } from './loader.js'
import { DEFAULT_SIZE_MIX, deriveBatch, parseSalt } from './piece.js'

const program = new Command()
  .name('pdp-pull-loadtool')
  .description('Load tool for Curio PDPv0 /pdp/piece/pull (issue #1241 / PR #1245)')
  .version('0.1.0')

// ---------- source ----------
program.command('source')
  .description('Run the deterministic source HTTP server')
  .option('--bind <host>', 'bind address', process.env.SOURCE_BIND ?? '0.0.0.0')
  .option('--port <port>', 'bind port', process.env.SOURCE_PORT ?? '8080')
  .option('-v, --verbose', 'verbose per-request logs', false)
  .action(async (opts) => {
    const salt = parseSalt(requireEnv('DERIVE_SALT'))
    const server = new SourceServer({
      bind: opts.bind,
      port: Number(opts.port),
      defaultSalt: salt,
      verbose: !!opts.verbose,
    })
    const shutdown = async () => {
      console.log('shutting down...')
      await server.stop()
      process.exit(0)
    }
    process.on('SIGINT', shutdown)
    process.on('SIGTERM', shutdown)
    await server.start()
  })

// ---------- smoke ----------
program.command('smoke')
  .description('Submit one pull request with N pieces and poll until terminal')
  .option('-n, --count <n>', 'number of pieces in the pull', '1')
  .option('-s, --size <bytes>', 'piece size (bytes); overrides default mix', '')
  .option('--start-index <i>', 'starting derivation index', String(Date.now() & 0xffffffff))
  .option('--poll-interval <ms>', 'poll interval', '4000')
  .option('--timeout <ms>', 'overall poll timeout', String(10 * 60_000))
  .action(async (opts) => {
    const loader = await buildLoader()
    const count = Number(opts.count)
    const startIndex = Number(opts.startIndex)

    let mix = DEFAULT_SIZE_MIX
    if (opts.size) {
      const sz = Number(opts.size)
      if (!Number.isFinite(sz) || sz < 127) throw new Error('--size must be >= 127')
      mix = [{ size: sz, weight: 1 }]
    }

    console.log(`signer: ${loader.signerAddress}`)
    console.log(`deriving ${count} pieces from index ${startIndex}...`)
    const salt = parseSalt(requireEnv('DERIVE_SALT'))
    const pieces = await deriveBatch(salt, startIndex, count, mix)
    for (const p of pieces) {
      console.log(`  piece[${p.index}] size=${p.size} cid=${p.pieceCid}`)
      console.log(`    sourceUrl=${loader.buildSourceUrl(p)}`)
    }

    const { final, polls } = await loader.pollUntilDone(pieces, {
      timeoutMs: Number(opts.timeout),
      intervalMs: Number(opts.pollInterval),
      onStatus: (r) => {
        const overall = (r.body as any).status ?? '?'
        console.log(`  poll http=${r.httpStatus} overall=${overall} elapsed=${r.elapsedMs}ms`)
      },
    })

    console.log('\n--- final ---')
    console.log(`polls: ${polls}`)
    console.log(`http: ${final.httpStatus}`)
    console.log(`body:`)
    console.log(JSON.stringify(final.body, null, 2))
    const ok = final.httpStatus === 200 && (final.body as any).status === 'complete'
    process.exit(ok ? 0 : 1)
  })

// ---------- backpressure ----------
program.command('backpressure')
  .description('Burst pulls to trigger global/per-client backpressure (429 + Retry-After)')
  .option('-b, --burst <n>', 'pulls to submit concurrently', '200')
  .option('-c, --concurrency <n>', 'max in-flight at once', '50')
  .option('--pieces-per-pull <n>', 'pieces in each pull request', '1')
  .option('-s, --size <bytes>', 'fixed piece size (default: small from mix)', '4096')
  .action(async (opts) => {
    const loader = await buildLoader()
    const burst = Number(opts.burst)
    const concurrency = Number(opts.concurrency)
    const piecesPerPull = Number(opts.piecesPerPull)
    const size = Number(opts.size)
    const salt = parseSalt(requireEnv('DERIVE_SALT'))
    const startIndex = Date.now() & 0xffffffff

    console.log(`burst=${burst} concurrency=${concurrency} piecesPerPull=${piecesPerPull} size=${size}`)
    console.log(`signer: ${loader.signerAddress}`)
    console.log(`expectation: per-client cap (10) hit fast → most requests 429 with Retry-After:60`)

    // Pre-derive all pieces (cheap for small sizes).
    const totalPieces = burst * piecesPerPull
    console.log(`deriving ${totalPieces} pieces...`)
    const pieces = await deriveBatch(salt, startIndex, totalPieces, [{ size, weight: 1 }])

    // Group into burst pull-requests.
    const pulls: typeof pieces[] = []
    for (let i = 0; i < burst; i++) {
      pulls.push(pieces.slice(i * piecesPerPull, (i + 1) * piecesPerPull))
    }

    const results = await mapPool(pulls, concurrency, async (group, idx) => {
      const r = await loader.submit(group)
      if (idx % 25 === 0 || r.httpStatus !== 200) {
        const overall = (r.body as any).status ?? '-'
        console.log(`  [${idx}] http=${r.httpStatus} overall=${overall} ra=${r.retryAfterSec ?? '-'} elapsed=${r.elapsedMs}ms`)
      }
      return r
    })

    console.log('\n--- summary ---')
    console.log(formatStats(summarize(results)))

    const had429 = results.some((r) => r.httpStatus === 429)
    const all429HaveRetry = results.filter((r) => r.httpStatus === 429).every((r) => r.retryAfterSec != null)
    console.log(`assertion: any 429 = ${had429}`)
    console.log(`assertion: every 429 has Retry-After = ${all429HaveRetry}`)
    process.exit(had429 && all429HaveRetry ? 0 : 1)
  })

// ---------- multi-url ----------
program.command('multi-url')
  .description('One pull with the same pieceCid repeated under N source URLs, some failing — PullPiece should try each until one works')
  .option('-n, --count <n>', 'distinct pieces in the pull', '3')
  .option('-s, --size <bytes>', 'piece size', '4096')
  .option('--bad-status <code>', 'fault to inject on the bad source URLs', '500')
  .option('--timeout <ms>', 'overall poll timeout', String(15 * 60_000))
  .action(async (opts) => {
    const loader = await buildLoader()
    const salt = parseSalt(requireEnv('DERIVE_SALT'))
    const count = Number(opts.count)
    const size = Number(opts.size)
    const badStatus = Number(opts.badStatus)
    const startIndex = Date.now() & 0xffffffff

    console.log(`signer: ${loader.signerAddress}`)
    console.log(`deriving ${count} pieces (each will appear 3x with [bad-404, bad-${badStatus}, good] sources)...`)
    const base = await deriveBatch(salt, startIndex, count, [{ size, weight: 1 }])

    // Build 9 pieces (3 unique CIDs × 3 source URLs each). Curio's handler
    // accepts duplicate pieceCids when sourceUrl differs — (fetch_id,
    // piece_cid, source_url) is the unique key. But FWSS's eth_call
    // validation checks pieceMetadata.length == pieces.length (otherwise
    // MetadataArrayCountMismatch, selector 0x9b7cf882), so the signed
    // extraData must also encode 9 piece-metadata slots — i.e. sign for the
    // expanded list, not just the distinct ones.
    const expandedPieces = base.flatMap((p) => [p, p, p])
    const ed = await loader.signExtraData(expandedPieces)
    const body: Record<string, unknown> = {
      extraData: ed,
      recordKeeper: (process.env.RECORD_KEEPER || defaultRecordKeeper()) as Address,
      // Curio iterates sources via index scan on PK (fetch_id, piece_cid,
      // source_url) — i.e. ASCII order of source_url, NOT body order. Prefix
      // an &attempt= tag so bad URLs lex-sort BEFORE the good one, otherwise
      // good gets picked first and fallback is never exercised.
      pieces: base.flatMap((p) => {
        const good = loader.buildSourceUrl(p)
        return [
          { pieceCid: p.pieceCid, sourceUrl: good + '&attempt=a-bad404&status=404' },
          { pieceCid: p.pieceCid, sourceUrl: good + `&attempt=b-bad${badStatus}&status=${badStatus}` },
          { pieceCid: p.pieceCid, sourceUrl: good + '&attempt=c-good' },
        ]
      }),
    }
    const dataSetId = BigInt(process.env.DATA_SET_ID ?? '0')
    if (dataSetId > 0n) body.dataSetId = Number(dataSetId)

    console.log(`submitting pull with ${(body.pieces as any[]).length} items (${count} unique pieceCids × 3 URLs)...`)
    const startedAt = Date.now()
    const res = await fetch(`${loader.effectiveCurioUrl}/pdp/piece/pull`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    })
    console.log(`initial POST: http=${res.status} elapsed=${Date.now() - startedAt}ms`)
    const text = await res.text()
    if (!res.ok) { console.error(text); process.exit(1) }
    console.log(text)

    // Now poll using same pieces (idempotent via the same extraData hash).
    // For simplicity reuse loader.pollUntilDone with the *base* pieces — but
    // that re-signs. We need to keep idempotency: re-POST the same body.
    const deadline = Date.now() + Number(opts.timeout)
    while (Date.now() < deadline) {
      await sleep(4000)
      const r = await fetch(`${loader.effectiveCurioUrl}/pdp/piece/pull`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      })
      const j = (await r.json()) as { status: string; pieces: Array<{ pieceCid: string; status: string }> }
      console.log(`poll: overall=${j.status} ${j.pieces.map((p) => p.status).join(',')}`)
      if (j.status === 'complete' || j.status === 'failed') {
        console.log('\nfinal:')
        console.log(JSON.stringify(j, null, 2))
        console.log('\nCheck source-server /admin/stats:')
        console.log('  - Each of the 3 pieces should show ok=1 (good URL succeeded).')
        console.log('  - fail should be ~2 per piece (the two bad URLs were tried and rejected first).')
        console.log('  - If fail=0, multi-URL fallback did NOT happen — investigate Curio source ordering.')
        process.exit(j.status === 'complete' ? 0 : 1)
      }
    }
    console.error('timed out')
    process.exit(2)
  })

// ---------- stale ----------
program.command('stale')
  .description('Trigger per-attempt timeout (10min) and 30min budget by serving very slow source')
  .option('-n, --count <n>', 'pieces in the pull', '1')
  .option('-s, --size <bytes>', 'piece size — bigger needs longer to stream slowly', '4096')
  .option('--bytes-per-sec <n>', 'source throttle (default 1B/s → guarantees attempt timeout)', '1')
  .option('--timeout <ms>', 'overall poll timeout (should exceed 30min budget)', String(35 * 60_000))
  .action(async (opts) => {
    const loader = await buildLoader()
    const salt = parseSalt(requireEnv('DERIVE_SALT'))
    const startIndex = Date.now() & 0xffffffff
    const pieces = await deriveBatch(salt, startIndex, Number(opts.count), [{ size: Number(opts.size), weight: 1 }])

    // Patch source URL to throttle.
    const origBuild = loader.buildSourceUrl.bind(loader)
    loader.buildSourceUrl = (p) => origBuild(p) + `&slow=${opts.bytesPerSec}`

    console.log(`signer: ${loader.signerAddress}`)
    console.log(`submitting ${pieces.length} pieces with ?slow=${opts.bytesPerSec} — expect attempt to time out after ~10min, then retry, finally fail at 30min budget`)
    const { final, polls } = await loader.pollUntilDone(pieces, {
      timeoutMs: Number(opts.timeout),
      intervalMs: 30_000,
      onStatus: (r) => {
        const overall = (r.body as any).status ?? '?'
        console.log(`  t=${(Date.now() / 1000).toFixed(0)} poll http=${r.httpStatus} overall=${overall}`)
      },
    })
    console.log(`\npolls=${polls} final http=${final.httpStatus}`)
    console.log(JSON.stringify(final.body, null, 2))
    const failed = (final.body as any).status === 'failed'
    console.log(`assertion: final status = "failed" → ${failed}`)
    process.exit(failed ? 0 : 1)
  })

// ---------- soak ----------
program.command('soak')
  .description('Sustained pull traffic at constant rate (mixed sizes). Operator observes Curio separately.')
  .option('--rate <perMin>', 'pulls per minute', '60')
  .option('--duration <sec>', 'soak duration in seconds', String(60 * 60))
  .option('--pieces-per-pull <n>', 'pieces per pull request', '1')
  .option('--concurrency <n>', 'max in-flight submits', '20')
  .action(async (opts) => {
    const loader = await buildLoader()
    const salt = parseSalt(requireEnv('DERIVE_SALT'))
    const ratePerMin = Number(opts.rate)
    const intervalMs = 60_000 / ratePerMin
    const durationMs = Number(opts.duration) * 1000
    const piecesPerPull = Number(opts.piecesPerPull)
    const concurrency = Number(opts.concurrency)

    console.log(`soak: rate=${ratePerMin}/min interval=${intervalMs.toFixed(0)}ms duration=${(durationMs / 1000)}s piecesPerPull=${piecesPerPull}`)
    console.log(`signer: ${loader.signerAddress}`)
    const startedAt = Date.now()
    const deadline = startedAt + durationMs
    let submittedTotal = 0
    let inFlight = 0
    const results: Awaited<ReturnType<typeof loader.submit>>[] = []
    let nextIndex = startedAt & 0xffffffff

    const reportTimer = setInterval(() => {
      const elapsed = (Date.now() - startedAt) / 1000
      const last100 = results.slice(-100)
      const byCode = new Map<number, number>()
      for (const r of last100) byCode.set(r.httpStatus, (byCode.get(r.httpStatus) ?? 0) + 1)
      const codes = Array.from(byCode.entries()).map(([k, v]) => `${k}=${v}`).join(' ')
      console.log(`  t=${elapsed.toFixed(0)}s submitted=${submittedTotal} inflight=${inFlight} last100: ${codes}`)
    }, 10_000)

    const submitOne = async () => {
      inFlight++
      submittedTotal++
      try {
        const pieces = await deriveBatch(salt, nextIndex, piecesPerPull, DEFAULT_SIZE_MIX)
        nextIndex += piecesPerPull
        const r = await loader.submit(pieces)
        results.push(r)
      } catch (e) {
        console.error(`submit error: ${(e as Error).message}`)
      } finally {
        inFlight--
      }
    }

    // Constant-rate scheduler, capped by concurrency.
    while (Date.now() < deadline) {
      if (inFlight < concurrency) {
        submitOne()  // fire-and-track
      }
      await sleep(intervalMs)
    }
    clearInterval(reportTimer)

    console.log(`\nwaiting for in-flight submits to settle (up to 60s)...`)
    const settleDeadline = Date.now() + 60_000
    while (inFlight > 0 && Date.now() < settleDeadline) await sleep(500)

    console.log('\n--- soak summary ---')
    console.log(formatStats(summarize(results)))
    process.exit(0)
  })

program.parseAsync(process.argv).catch((err) => {
  console.error(err instanceof Error ? err.stack ?? err.message : err)
  process.exit(1)
})

// ---------- helpers ----------

function requireEnv(name: string): string {
  const v = process.env[name]
  if (!v) {
    console.error(`missing env: ${name} (see .env.example)`)
    process.exit(2)
  }
  return v
}

async function buildLoader(): Promise<PullLoader> {
  const network = (process.env.NETWORK ?? 'calibration') as Network
  if (network !== 'calibration' && network !== 'mainnet') {
    throw new Error('NETWORK must be calibration or mainnet')
  }
  const pk = requireEnv('PRIVATE_KEY')
  const providerIdStr = process.env.PROVIDER_ID
  const curioUrlEnv = process.env.CURIO_URL?.replace(/\/+$/, '')
  if (!providerIdStr && !curioUrlEnv) {
    throw new Error('Set PROVIDER_ID (recommended — auto-discovers everything) or CURIO_URL in .env')
  }
  const cfg: LoaderConfig = {
    network,
    privateKey: (pk.startsWith('0x') ? pk : `0x${pk}`) as Hex,
    providerId: providerIdStr ? BigInt(providerIdStr) : undefined,
    curioUrl: curioUrlEnv,
    sourcePublicUrl: requireEnv('SOURCE_PUBLIC_URL').replace(/\/+$/, ''),
    dataSetId: BigInt(process.env.DATA_SET_ID ?? '0'),
    payee: (process.env.PAYEE_ADDRESS || undefined) as Address | undefined,
    recordKeeper: (process.env.RECORD_KEEPER || undefined) as Address | undefined,
  }
  return PullLoader.create(cfg)
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms))
}

function defaultRecordKeeper(): Address {
  const net = (process.env.NETWORK ?? 'calibration') as Network
  const chain = net === 'mainnet' ? mainnet : calibration
  return chain.contracts.fwss.address as Address
}

async function mapPool<T, R>(items: T[], concurrency: number, fn: (item: T, idx: number) => Promise<R>): Promise<R[]> {
  const out: R[] = new Array(items.length)
  let next = 0
  const workers: Promise<void>[] = []
  for (let w = 0; w < Math.min(concurrency, items.length); w++) {
    workers.push((async () => {
      while (true) {
        const i = next++
        if (i >= items.length) return
        out[i] = await fn(items[i], i)
      }
    })())
  }
  await Promise.all(workers)
  return out
}
