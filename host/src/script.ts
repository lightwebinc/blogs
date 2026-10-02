/**
 * Script reading and canonical script rebuilding, the twin of the Go
 * internal/logrec/script.go. The reader is this package's own rather than
 * the SDK's, so that the Go and TypeScript codecs classify every byte string
 * the same way; every script a host checks is compared byte for byte with
 * the one rebuilt here.
 */
import { pushDropScript } from '@lightwebinc/bcommon'
import { checkProducer, TagFunding } from './logrec.js'

export { minimalPushBytes, pushDropScript, strictSignature } from '@lightwebinc/bcommon'

const OP_0 = 0x00
const OP_PUSHDATA1 = 0x4c
const OP_PUSHDATA2 = 0x4d
const OP_PUSHDATA4 = 0x4e
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
