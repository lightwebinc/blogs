import { test } from 'node:test'
import assert from 'node:assert/strict'
import { gzipSync } from 'node:zlib'
import { CborMap, encode } from '@lightwebinc/bcommon'
import {
  EncodingLines,
  EncodingLinesGzip,
  MaxKeys,
  MaxStreamName,
  ProfileDigest,
  ProfileFull,
  Refusal,
  SuffixLen,
  batchSupported,
  checkStream,
  decodeBatch,
  encodeBatch,
  isAnchor,
  isBatch,
  newStream,
  splitStream,
  topic,
  type Batch,
} from './logrec.js'
import { expand } from './content.js'
import { parseScript, pushDropScript, readPushDrop, strictSignature } from './script.js'
import { admit, fromHex, validatePolicy } from './admit.js'
import { Transaction } from '@bsv/sdk'

const producer = fromHex('0324653eac434488002cc06bbfb7f10fe18991e35f9fe4302dbea6d2353dc0ab1c')
const stream = 'example_app_qzxkvbmwtr'

function reason(f: () => unknown): string {
  try {
    f()
  } catch (e) {
    if (e instanceof Refusal) return e.reason
    throw e
  }
  return 'accepted'
}

function good(): Batch {
  const content = new TextEncoder().encode('one\ntwo\n')
  return {
    producer,
    stream,
    seq: 7,
    prev: new Uint8Array(32).fill(1),
    timestamp: 1767225600000,
    count: 2,
    encoding: EncodingLines,
    profile: ProfileFull,
    content,
    size: content.length,
    extra: [],
  }
}

test('stream identifiers: grammar, split, topic length', () => {
  for (const s of [stream, 'a_bcdefghijk', `${'a'.repeat(MaxStreamName)}_qzxkvbmwtr`]) {
    checkStream(s)
    assert.ok(topic(s).length <= 50)
  }
  for (const s of ['example_app', '_qzxkvbmwtr', 'a__b_qzxkvbmwtr', `${'a'.repeat(MaxStreamName + 1)}_qzxkvbmwtr`, 'app_qzxkvbmwté', 'App_qzxkvbmwtr', 'a_qzxkvbmwt1']) {
    assert.equal(reason(() => checkStream(s)), 'stream', s)
  }
  assert.deepEqual(splitStream('a_b_qzxkvbmwtr'), { name: 'a_b', suffix: 'qzxkvbmwtr' })
})

test('newStream draws a fresh uniform suffix and refuses a bad name', () => {
  const seen = new Set<string>()
  const counts = new Array<number>(26).fill(0)
  for (let i = 0; i < 400; i++) {
    const s = newStream('app')
    checkStream(s)
    seen.add(s)
    for (const c of splitStream(s).suffix) counts[c.charCodeAt(0) - 0x61]!++
  }
  assert.equal(seen.size, 400)
  // 4000 letters: each is about 154; a letter never drawn would be a broken draw.
  assert.ok(counts.every((n) => n > 80 && n < 240), counts.join(','))
  assert.equal(reason(() => newStream('Bad')), 'stream')
  assert.equal(splitStream(newStream('a')).suffix.length, SuffixLen)
})

test('a record at the key limit round-trips; one over is refused both ways', () => {
  const b = good()
  for (let k = 24n; b.extra.length + 10 + 1 < MaxKeys; k++) b.extra.push({ key: k, val: 0n })
  const enc = encodeBatch(b)
  assert.deepEqual(encodeBatch(decodeBatch(enc)), enc)
  b.extra.push({ key: 200n, val: 0n })
  assert.equal(reason(() => encodeBatch(b)), 'too-large')
  // Every map head width is classified the same.
  for (const head of [[0xb8, 0x40], [0xb9, 0x00, 0x40], [0xba, 0, 0, 0, 0x40], [0xbb, 0, 0, 0, 0, 0, 0, 0, 0x40]]) {
    const p = Uint8Array.from([...head, 0x00, 0x44, 0x62, 0x6c, 0x62, 0x01])
    assert.ok(isBatch(p) && !isAnchor(p))
  }
})

test('unknown profiles and encodings are links, not refusals', () => {
  const b = good()
  b.encoding = 9
  const d = decodeBatch(encodeBatch(b))
  assert.equal(batchSupported(d), false)
  b.encoding = EncodingLines
  b.profile = 3
  assert.equal(batchSupported(decodeBatch(encodeBatch(b))), false)
})

test('a locator with an unknown member is preserved and unsupported', () => {
  const b = good()
  delete b.content
  b.profile = ProfileDigest
  b.locator = { digest: new Uint8Array(32), length: b.size, urls: [], extra: [{ key: 5n, val: 'x' }] }
  const enc = encodeBatch(b)
  const d = decodeBatch(enc)
  assert.equal(batchSupported(d), false)
  assert.deepEqual(encodeBatch(d), enc)
})

test('the bound is checked before the decoder runs', () => {
  const huge = new Uint8Array(266241)
  huge[0] = 0xa1
  assert.equal(reason(() => decodeBatch(huge)), 'too-large')
})

test('gzip content: one member, a matching trailer, nothing after it, bounded output', () => {
  const lines = new TextEncoder().encode('a\nb\nc\n')
  const b = { ...good(), encoding: EncodingLinesGzip, count: 3, size: lines.length }
  const one = new Uint8Array(gzipSync(lines))
  assert.deepEqual(expand(b, one), lines)
  const two = new Uint8Array([...one, ...one])
  assert.equal(reason(() => expand({ ...b, size: lines.length }, two)), 'content')
  assert.equal(reason(() => expand(b, new Uint8Array([...one, 0]))), 'content')
  const badCrc = one.slice()
  badCrc[badCrc.length - 8]! ^= 1
  assert.equal(reason(() => expand(b, badCrc)), 'content')
  // A bomb: 16 MiB + 1 of zeros is refused without producing more than size + 1.
  const bomb = new Uint8Array(gzipSync(new Uint8Array(16 << 20).fill(0x0a)))
  assert.equal(reason(() => expand({ ...b, size: 1000, count: 1000 }, bomb)), 'content')
  assert.equal(reason(() => expand(b, new TextEncoder().encode('a\nb\nc'))), 'content')
})

test('script reading: truncation, canonical PushDrop, strict signatures', () => {
  for (const s of [[0x05, 1, 2], [0x4c], [0x4c, 3, 1], [0x4d, 0xff], [0x4e, 0xff, 0xff, 0xff, 0xff, 0]]) {
    assert.equal(parseScript(Uint8Array.from(s)), undefined)
  }
  const key = producer
  const s = pushDropScript(key, [Uint8Array.of(0x62, 0x6c, 0x01), new Uint8Array(70000)], new Uint8Array(71))
  const r = readPushDrop(s)
  assert.equal(r?.fields.length, 3)
  assert.equal(r?.fields[1]!.length, 70000)
  // A go-sdk signature (from the transaction vectors).
  const der = fromHex('3045022100d2df9017a11a355caf4102e3645edb48536ec685230bdfc925a17b90b0ef29db02203e989b29d728fbb2abecf85a091411780f6b3214f531d678f0a54f588145fd92')
  assert.ok(strictSignature(der))
})

test('a policy is open or names producers, exactly one', async () => {
  validatePolicy({ open: true })
  validatePolicy({ producers: [producer] })
  assert.throws(() => validatePolicy({ producers: [] }))
  await assert.rejects(admit(new Transaction(), new Map(), { stream, policy: { producers: [] } }))
  // A value no CBOR item has cannot pass for a map.
  assert.equal(reason(() => decodeBatch(encode(new CborMap([{ key: 'x', val: 1n }])))), 'key-type')
})
