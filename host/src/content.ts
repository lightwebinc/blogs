/**
 * Content checks, the twin of the Go internal/logrec/content.go: what an
 * archive's storage adapter or a reader runs on a batch's content, never
 * admission. Uses node:zlib, so it is for a host or a command, not a page.
 */
import { createHash } from 'node:crypto'
import { crc32, inflateRawSync } from 'node:zlib'
import { EncodingLines, EncodingLinesGzip, Refusal, type Batch, type Locator } from './logrec.js'

/** Fetched bytes against their locator: the length, then the digest. */
export function checkFetched(l: Locator, content: Uint8Array): void {
  if (content.length !== l.length) throw new Refusal('content', 'fetched length')
  const d = createHash('sha256').update(content).digest()
  if (!d.equals(l.digest)) throw new Refusal('content', 'fetched digest')
}

/**
 * The deflate stream's offset in one gzip member (RFC 1952): after the
 * fixed header and the optional extra field, name, comment and header CRC.
 */
function gzipBody(b: Uint8Array): number {
  if (b.length < 18 || b[0] !== 0x1f || b[1] !== 0x8b || b[2] !== 8) throw new Refusal('content', 'gzip: header')
  const flg = b[3]!
  if ((flg & 0xe0) !== 0) throw new Refusal('content', 'gzip: reserved flags')
  let i = 10
  if (flg & 0x04) {
    if (i + 2 > b.length) throw new Refusal('content', 'gzip: extra')
    i += 2 + (b[i]! | (b[i + 1]! << 8))
  }
  for (const bit of [0x08, 0x10]) {
    if (flg & bit) {
      while (i < b.length && b[i] !== 0) i++
      i++
    }
  }
  if (flg & 0x02) i += 2
  if (i > b.length) throw new Refusal('content', 'gzip: header')
  return i
}

/**
 * Decodes a batch's content (carried or fetched) under its encoding and
 * checks it against the record: exactly size bytes, ending in LF, holding
 * exactly count LFs. Decompression stops at size + 1 bytes, so a
 * compressed bomb costs at most 16 MiB plus one byte of output; exactly one
 * gzip member, with a matching trailer and nothing after it, is accepted.
 */
export function expand(b: Batch, content: Uint8Array): Uint8Array {
  let out: Uint8Array
  if (b.encoding === EncodingLines) {
    out = content
  } else if (b.encoding === EncodingLinesGzip) {
    const at = gzipBody(content)
    let r: { buffer: Buffer; engine: { bytesWritten: number } }
    try {
      r = inflateRawSync(content.subarray(at), { maxOutputLength: b.size + 1, info: true }) as unknown as typeof r
    } catch (e) {
      throw new Refusal('content', `gzip: ${(e as Error).message}`)
    }
    out = new Uint8Array(r.buffer.buffer, r.buffer.byteOffset, r.buffer.length)
    if (out.length !== b.size) throw new Refusal('content', 'expanded size')
    const end = at + r.engine.bytesWritten
    if (end + 8 !== content.length) throw new Refusal('content', 'bytes after the gzip member')
    const t = Buffer.from(content.subarray(end))
    if (t.readUInt32LE(0) !== crc32(out) || t.readUInt32LE(4) !== out.length % 2 ** 32) throw new Refusal('content', 'gzip trailer')
  } else {
    throw new Refusal('content', 'unknown encoding')
  }
  if (out.length !== b.size) throw new Refusal('content', 'size')
  let lfs = 0
  for (const c of out) if (c === 0x0a) lfs++
  if (out.length === 0 || out[out.length - 1] !== 0x0a || lfs !== b.count) throw new Refusal('content', 'count')
  return out
}
