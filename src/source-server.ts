/**
 * HTTP source server. Curio GETs ${SOURCE_PUBLIC_URL}/piece/{pieceCidV2}?salt=&i=&size=
 * Streams deterministically-derived bytes back. Fault injection via query.
 *
 * Deploy behind Caddy / Cloudflare Tunnel / ngrok for HTTPS.
 *
 * Fault-injection query parameters (per-request):
 *   delay=<ms>           — sleep before first byte
 *   slow=<bytesPerSec>   — throttle output (use to trigger Curio's 10min/attempt timeout)
 *   status=<code>        — return this HTTP status instead of 200
 *   corrupt=<int>        — XOR every byte with this value (breaks Curio's CommP check)
 *   short=<bytes>        — stop after this many bytes (less than declared size)
 *
 * Hit counters per pieceCid are kept in memory and exposed at GET /admin/stats.
 */

import http from 'node:http'
import { parseSalt, derivePieceChunks } from './piece.js'

interface Options {
  bind: string
  port: number
  defaultSalt: Buffer
  verbose: boolean
}

interface PieceHit {
  ok: number
  fail: number
  bytesServed: number
}

export class SourceServer {
  private readonly opts: Options
  private readonly hits = new Map<string, PieceHit>()
  private server: http.Server | null = null
  private totalRequests = 0

  constructor(opts: Options) {
    this.opts = opts
  }

  start(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.server = http.createServer((req, res) => this.handle(req, res).catch((err) => {
        log(`unhandled handler error: ${err.stack ?? err.message}`)
        if (!res.headersSent) res.writeHead(500)
        res.end()
      }))
      // Allow many concurrent sockets without keep-alive starving.
      this.server.keepAliveTimeout = 75_000
      this.server.headersTimeout = 80_000
      this.server.listen(this.opts.port, this.opts.bind, () => {
        log(`source-server listening on ${this.opts.bind}:${this.opts.port}`)
        resolve()
      })
      this.server.on('error', reject)
    })
  }

  stop(): Promise<void> {
    return new Promise((resolve) => {
      if (!this.server) return resolve()
      this.server.close(() => resolve())
    })
  }

  private async handle(req: http.IncomingMessage, res: http.ServerResponse): Promise<void> {
    this.totalRequests++
    const url = new URL(req.url ?? '/', `http://${req.headers.host ?? 'x'}`)

    if (req.method === 'GET' && url.pathname === '/admin/stats') {
      return this.adminStats(res)
    }
    if (req.method === 'POST' && url.pathname === '/admin/reset') {
      this.hits.clear()
      this.totalRequests = 0
      res.writeHead(204).end()
      return
    }

    const m = url.pathname.match(/^\/piece\/([^/]+)$/)
    if (req.method !== 'GET' || !m) {
      res.writeHead(404).end('not found')
      return
    }
    const pieceCid = m[1]

    // Required derivation params (loader puts them in the URL).
    const indexStr = url.searchParams.get('i')
    const sizeStr = url.searchParams.get('size')
    if (!indexStr || !sizeStr) {
      this.recordFail(pieceCid)
      res.writeHead(400).end('missing i or size query param')
      return
    }
    const index = Number(indexStr)
    const size = Number(sizeStr)
    if (!Number.isFinite(index) || !Number.isFinite(size) || size < 0 || size > 16 * 1024 * 1024 * 1024) {
      this.recordFail(pieceCid)
      res.writeHead(400).end('invalid i or size')
      return
    }

    // Allow per-request salt override (mainly for tests), default to env salt.
    const saltStr = url.searchParams.get('salt')
    const salt = saltStr ? parseSalt(saltStr) : this.opts.defaultSalt

    // Fault-injection knobs.
    const delayMs = numParam(url, 'delay', 0)
    const bytesPerSec = numParam(url, 'slow', 0)
    const overrideStatus = numParam(url, 'status', 0)
    const corrupt = numParam(url, 'corrupt', 0) & 0xff
    const short = numParam(url, 'short', 0)

    if (this.opts.verbose) {
      log(`GET /piece/${pieceCid} i=${index} size=${size} delay=${delayMs} slow=${bytesPerSec} status=${overrideStatus} corrupt=${corrupt} short=${short}`)
    }

    if (overrideStatus >= 400) {
      this.recordFail(pieceCid)
      res.writeHead(overrideStatus).end(`forced status ${overrideStatus}`)
      return
    }

    if (delayMs > 0) {
      await sleep(delayMs)
      // Client may have given up by now.
      if (res.writableEnded || req.destroyed) {
        this.recordFail(pieceCid)
        return
      }
    }

    res.writeHead(200, {
      'Content-Type': 'application/octet-stream',
      'Content-Length': short > 0 && short < size ? String(short) : String(size),
    })

    let written = 0
    const cap = short > 0 ? Math.min(short, size) : size
    const startedAt = Date.now()

    try {
      for await (const chunk of derivePieceChunks({ salt, index, size })) {
        if (req.destroyed) break
        let out: Buffer = chunk
        if (corrupt) {
          out = Buffer.alloc(chunk.length)
          for (let i = 0; i < chunk.length; i++) out[i] = chunk[i] ^ corrupt
        }
        const remaining = cap - written
        if (remaining <= 0) break
        const slice = out.length > remaining ? out.subarray(0, remaining) : out
        written += slice.length

        // Throttle.
        if (bytesPerSec > 0) {
          const targetElapsedMs = (written / bytesPerSec) * 1000
          const actualElapsedMs = Date.now() - startedAt
          const sleepMs = targetElapsedMs - actualElapsedMs
          if (sleepMs > 5) await sleep(sleepMs)
        }

        const okToContinue = res.write(slice)
        if (!okToContinue) {
          await new Promise<void>((resolve) => res.once('drain', () => resolve()))
        }

        if (written >= cap) break
      }
      res.end()
      if (written === size && !corrupt && short === 0) {
        this.recordOk(pieceCid, written)
      } else {
        this.recordFail(pieceCid)
      }
    } catch (err) {
      this.recordFail(pieceCid)
      if (this.opts.verbose) log(`stream error: ${(err as Error).message}`)
      res.destroy()
    }
  }

  private adminStats(res: http.ServerResponse): void {
    const entries = Array.from(this.hits.entries())
      .map(([cid, h]) => ({ pieceCid: cid, ...h }))
      .sort((a, b) => (b.ok + b.fail) - (a.ok + a.fail))
    res.writeHead(200, { 'Content-Type': 'application/json' })
    res.end(JSON.stringify({
      totalRequests: this.totalRequests,
      uniquePieces: entries.length,
      pieces: entries,
    }, null, 2))
  }

  private recordOk(cid: string, bytes: number): void {
    const h = this.hits.get(cid) ?? { ok: 0, fail: 0, bytesServed: 0 }
    h.ok++
    h.bytesServed += bytes
    this.hits.set(cid, h)
  }

  private recordFail(cid: string): void {
    const h = this.hits.get(cid) ?? { ok: 0, fail: 0, bytesServed: 0 }
    h.fail++
    this.hits.set(cid, h)
  }
}

function numParam(url: URL, name: string, fallback: number): number {
  const v = url.searchParams.get(name)
  if (v == null) return fallback
  const n = Number(v)
  return Number.isFinite(n) ? n : fallback
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms))
}

function log(msg: string): void {
  console.log(`[${new Date().toISOString()}] [source] ${msg}`)
}
