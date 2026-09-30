/**
 * Script reading and canonical script rebuilding, the twin of the Go
 * internal/logrec/script.go. The reader is this package's own rather than
 * the SDK's, so that the Go and TypeScript codecs classify every byte string
 * the same way; every script a host checks is compared byte for byte with
 * the one rebuilt here.
 */
import { toBigInt, checkProducer, TagFunding } from './logrec.js'

const OP_0 = 0x00
const OP_PUSHDATA1 = 0x4c
const OP_PUSHDATA2 = 0x4d
const OP_PUSHDATA4 = 0x4e
const OP_1NEGATE = 0x4f
const OP_DROP = 0x75
const OP_2DROP = 0x6d
const OP_CHECKSIG = 0xac

/** One parsed script element. */
export interface Op {
  code: number
  data?: Uint8Array
}

/**
 * Splits s into opcodes. A push whose declared length runs past the end
 * refuses the whole script, so a truncated script claims nothing.
 */
export function parseScript(s: Uint8Array): Op[] | undefined {
  const out: Op[] = []
  let i = 0
  while (i < s.length) {
    const c = s[i++]!
    let n: number
    if (c === OP_0) {
      out.push({ code: c, data: new Uint8Array(0) })
      continue
    } else if (c >= 1 && c <= 75) {
      n = c
    } else if (c === OP_PUSHDATA1) {
      if (i + 1 > s.length) return undefined
      n = s[i]!
      i += 1
    } else if (c === OP_PUSHDATA2) {
      if (i + 2 > s.length) return undefined
      n = s[i]! | (s[i + 1]! << 8)
      i += 2
    } else if (c === OP_PUSHDATA4) {
      if (i + 4 > s.length) return undefined
      n = (s[i]! | (s[i + 1]! << 8) | (s[i + 2]! << 16)) + s[i + 3]! * 0x1000000
      i += 4
    } else {
      out.push({ code: c })
      continue
    }
    if (n > s.length - i) return undefined
    out.push({ code: c, data: s.subarray(i, i + n) })
    i += n
  }
  return out
}

/**
 * A lock-before PushDrop read leniently: a 33-byte push with a compressed
 * prefix, then OP_CHECKSIG, then the data pushes up to the first opcode
 * that is not one. Only classifies; canonicality is the rebuild's.
 */
export interface PushDropRead {
  key: Uint8Array
  /** Every push after OP_CHECKSIG, the signature included. */
  fields: Uint8Array[]
}

export function readPushDrop(s: Uint8Array): PushDropRead | undefined {
  const ops = parseScript(s)
  if (ops === undefined || ops.length < 3) return undefined
  const k = ops[0]!.data
  if (k === undefined || k.length !== 33 || (k[0] !== 0x02 && k[0] !== 0x03)) return undefined
  if (ops[1]!.code !== OP_CHECKSIG) return undefined
  const fields: Uint8Array[] = []
  for (const o of ops.slice(2)) {
    if (o.data === undefined) break
    fields.push(o.data)
  }
  if (fields.length === 0) return undefined
  return { key: k, fields }
}

/**
 * The minimal push of d, as the SDK's CreateMinimallyEncodedScriptChunk
 * writes it: OP_0 for nothing or a single zero byte, OP_1 to OP_16 and
 * OP_1NEGATE for the one-byte values they stand for, then a direct push, then
 * OP_PUSHDATA1, 2 and 4.
 */
export function minimalPushBytes(d: Uint8Array): Uint8Array {
  const n = d.length
  if (n === 0 || (n === 1 && d[0] === 0)) return Uint8Array.of(OP_0)
  if (n === 1 && d[0]! >= 1 && d[0]! <= 16) return Uint8Array.of(0x50 + d[0]!)
  if (n === 1 && d[0] === 0x81) return Uint8Array.of(OP_1NEGATE)
  let head: number[]
  if (n <= 75) head = [n]
  else if (n <= 255) head = [OP_PUSHDATA1, n]
  else if (n <= 65535) head = [OP_PUSHDATA2, n & 0xff, n >> 8]
  else head = [OP_PUSHDATA4, n & 0xff, (n >> 8) & 0xff, (n >> 16) & 0xff, (n >>> 24) & 0xff]
  const out = new Uint8Array(head.length + n)
  out.set(head)
  out.set(d, head.length)
  return out
}

function concat(parts: Uint8Array[]): Uint8Array {
  const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0))
  let at = 0
  for (const p of parts) {
    out.set(p, at)
    at += p.length
  }
  return out
}

/**
 * The one canonical lock-before PushDrop for a 33-byte compressed key,
 * fields and an optional field signature: the key, OP_CHECKSIG, each field
 * and then the signature as one minimal push, and the fewest OP_2DROP then
 * OP_DROP that clear them.
 */
export function pushDropScript(key: Uint8Array, fields: Uint8Array[], sig?: Uint8Array): Uint8Array {
  const all = sig === undefined ? fields : [...fields, sig]
  const parts: Uint8Array[] = [Uint8Array.of(33), key, Uint8Array.of(OP_CHECKSIG), ...all.map(minimalPushBytes)]
  for (let n = all.length; n > 0; n -= 2) parts.push(Uint8Array.of(n === 1 ? OP_DROP : OP_2DROP))
  return concat(parts)
}

/** A funding-tree output locked to key: <key> OP_CHECKSIG <62 6c 02> OP_DROP. */
export function fundingScript(key: Uint8Array): Uint8Array {
  return pushDropScript(key, [TagFunding])
}

/** A well-formed funding output under some canonical compressed key. */
export function isFundingShape(s: Uint8Array): boolean {
  if (s.length !== 1 + 33 + 1 + 1 + 3 + 1 || s[0] !== 33) return false
  const key = s.subarray(1, 34)
  try {
    checkProducer(key)
  } catch {
    return false
  }
  return bytesEq(s, fundingScript(key))
}

export function bytesEq(a: Uint8Array, b: Uint8Array): boolean {
  return a.length === b.length && a.every((x, i) => x === b[i])
}

const order = 0xfffffffffffffffffffffffffffffffebaaedce6af48a03bbfd25e8cd0364141n

function strictInt(b: Uint8Array): boolean {
  if (b.length === 0 || (b[0]! & 0x80) !== 0) return false
  return !(b.length > 1 && b[0] === 0 && (b[1]! & 0x80) === 0)
}

/**
 * A strict DER ECDSA signature (BIP 66) with R in [1, n-1] and S in
 * [1, n/2]: the one encoding a host accepts.
 */
export function strictSignature(der: Uint8Array): boolean {
  if (der.length < 8 || der.length > 72 || der[0] !== 0x30 || der[1] !== der.length - 2) return false
  const lenR = der[3]!
  if (der[2] !== 0x02 || lenR === 0 || 6 + lenR > der.length) return false
  const lenS = der[5 + lenR]!
  if (der[4 + lenR] !== 0x02 || lenS === 0 || 6 + lenR + lenS !== der.length) return false
  const r = der.subarray(4, 4 + lenR)
  const s = der.subarray(6 + lenR)
  if (!strictInt(r) || !strictInt(s)) return false
  const R = toBigInt(r)
  const S = toBigInt(s)
  return R > 0n && R < order && S > 0n && S <= order >> 1n
}
