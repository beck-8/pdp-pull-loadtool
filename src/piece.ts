/**
 * Deterministic piece derivation shared by loader and source-server.
 *
 * Bytes for (salt, index, size) are produced by AES-128-CTR keystream over
 * all-zero plaintext, with key = keccak256(salt || index_be32)[0:16] and a
 * zero IV. This gives ~1-2 GB/s on Node and lets both sides reproduce the
 * exact same byte stream without storing anything.
 *
 * PieceCIDv2 is computed by @filoz/synapse-core/piece (FRC-0069 hasher).
 */

import { createCipheriv, createHash } from 'node:crypto'
import { calculateFromIterable } from '@filoz/synapse-core/piece'

const CHUNK = 64 * 1024 // 64 KiB — balances syscall overhead vs memory

export interface PieceSpec {
  salt: Buffer       // 32 bytes
  index: number      // 0..2^32-1
  size: number       // raw byte size of the piece payload
}

/** Derive the AES key for a given (salt, index). */
function deriveKey(salt: Buffer, index: number): Buffer {
  const ib = Buffer.alloc(4)
  ib.writeUInt32BE(index, 0)
  // keccak256 isn't in node:crypto; we use sha256 which is just as good as a
  // KDF here (the only requirement is uniform key distribution).
  const h = createHash('sha256')
  h.update(salt)
  h.update(ib)
  return h.digest().subarray(0, 16)
}

/** Parse a 0x-prefixed hex salt into a 32-byte buffer. */
export function parseSalt(hex: string): Buffer {
  const h = hex.startsWith('0x') ? hex.slice(2) : hex
  if (h.length !== 64) throw new Error(`DERIVE_SALT must be 32 bytes (64 hex chars), got ${h.length}`)
  return Buffer.from(h, 'hex')
}

/**
 * Async generator that yields the piece bytes in 64 KiB chunks.
 * Total bytes yielded equals spec.size.
 */
export async function* derivePieceChunks(spec: PieceSpec): AsyncGenerator<Buffer> {
  const key = deriveKey(spec.salt, spec.index)
  const iv = Buffer.alloc(16)
  const cipher = createCipheriv('aes-128-ctr', key, iv)

  let remaining = spec.size
  const zero = Buffer.alloc(CHUNK)
  while (remaining > 0) {
    const n = Math.min(CHUNK, remaining)
    const plain = n === CHUNK ? zero : zero.subarray(0, n)
    const out = cipher.update(plain)
    remaining -= n
    yield out
  }
  // No final block in CTR mode, but call final() to release state.
  cipher.final()
}

/** Materialize the full piece into a Buffer. Use for small pieces only. */
export async function derivePieceBytes(spec: PieceSpec): Promise<Buffer> {
  const parts: Buffer[] = []
  for await (const c of derivePieceChunks(spec)) parts.push(c)
  return Buffer.concat(parts, spec.size)
}

/**
 * Compute the PieceCIDv2 for a spec, streaming bytes through the FRC-0069
 * hasher. Returns the canonical string form (bafkz...).
 */
export async function derivePieceCid(spec: PieceSpec): Promise<string> {
  const cid = await calculateFromIterable(derivePieceChunks(spec))
  return cid.toString()
}

export interface DerivedPiece extends PieceSpec {
  pieceCid: string
}

/**
 * Derive a batch of pieces with mixed sizes according to the given
 * distribution. Sizes are picked by weighted choice using a deterministic
 * PRNG seeded from (salt, "size-mix").
 */
export async function deriveBatch(
  salt: Buffer,
  startIndex: number,
  count: number,
  sizeMix: SizeMix,
): Promise<DerivedPiece[]> {
  const rng = mixPrng(salt)
  const out: DerivedPiece[] = []
  for (let k = 0; k < count; k++) {
    const size = pickSize(sizeMix, rng())
    const index = startIndex + k
    const pieceCid = await derivePieceCid({ salt, index, size })
    out.push({ salt, index, size, pieceCid })
  }
  return out
}

/**
 * Size distribution. Weights are relative; they don't need to sum to 1.
 * Default targets the Storacha-like mix called out in issue #1241:
 *   - 70% small (4 KiB)
 *   - 25% medium (1 MiB)
 *   - 5%  large (16 MiB)
 */
export type SizeMix = Array<{ size: number; weight: number }>

export const DEFAULT_SIZE_MIX: SizeMix = [
  { size: 4 * 1024, weight: 70 },
  { size: 1024 * 1024, weight: 25 },
  { size: 16 * 1024 * 1024, weight: 5 },
]

function pickSize(mix: SizeMix, r: number): number {
  const total = mix.reduce((a, b) => a + b.weight, 0)
  let target = r * total
  for (const entry of mix) {
    target -= entry.weight
    if (target <= 0) return entry.size
  }
  return mix[mix.length - 1].size
}

/** Tiny deterministic PRNG seeded by sha256(salt || "mix"). */
function mixPrng(salt: Buffer): () => number {
  const seed = createHash('sha256').update(salt).update('mix').digest()
  let i = 0
  return () => {
    if (i >= seed.length) {
      const next = createHash('sha256').update(seed).update(Buffer.from([i & 0xff])).digest()
      seed.set(next.subarray(0, seed.length))
      i = 0
    }
    // Use 4 bytes -> uniform [0, 1)
    const v = seed.readUInt32BE(i)
    i += 4
    return v / 0x1_0000_0000
  }
}
