/**
 * Every Go-generated vector under testdata/vectors, run by the TypeScript
 * codec: each record decodes to the fields the vector lists and re-encodes
 * to the identical bytes, each refusal is refused for the same reason, and
 * each transaction is admitted or refused exactly as the Go codec did.
 */
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { readFileSync, readdirSync } from 'node:fs'
import { Hash, type ChainTracker } from '@bsv/sdk'
import { encode, readerLockingKey } from '@lightwebinc/bcommon'
import {
  KeyAnchor,
  KeyBatch,
  KeyFund,
  LookupService,
  MagicAnchor,
  MagicBatch,
  MaxStreamName,
  Protocol,
  Refusal,
  SuffixLen,
  TagAnchor,
  TagFunding,
  decodeAnchor,
  decodeBatch,
  decodeLocator,
  encodeAnchor,
  encodeBatch,
  encodeLocator,
  isBatch,
  locatorUnsupported,
  splitStream,
  topic,
} from './logrec.js'
import { admit, fromHex, toHex, txFromBEEF, type Policy } from './admit.js'
import { checkFetched, expand } from './content.js'
import { intervalRoot, leafHash, pathIndex, prove, verifyBatch, type Step } from './merkle.js'
import { fundingScript } from './script.js'

const dir = new URL('../../testdata/vectors/', import.meta.url)
const read = (name: string): string => readFileSync(new URL(name, dir), 'utf8')
const json = <T>(name: string): T => JSON.parse(read(name)) as T
const hexFile = (name: string): string => read(name).trim()

function refusal(f: () => unknown): string {
  try {
    f()
  } catch (e) {
    if (e instanceof Refusal) return e.reason
    throw e
  }
  return 'accepted'
}

// Every file the Go generator writes is read by a test below.
const covered = new Set([
  'batch-v1.json',
  'batch-first.hex',
  'batch-lines.hex',
  'batch-gzip.hex',
  'batch-digest.hex',
  'locator-v1.json',
  'locator-hints.hex',
  'anchor-v1.json',
  'anchor-interval-1.hex',
  'anchor-interval-2.hex',
  'derivation-v1.json',
  'refusal-v1.json',
  'transaction-v1.json',
])

test('every vector file is run', () => {
  for (const f of readdirSync(dir)) assert.ok(covered.has(f), `${f} is not run by the TypeScript tests`)
})

interface BatchVector {
  name: string
  producer: string
  stream: string
  seq: number
  prev: string
  timestamp: number
  count: number
  encoding: number
  profile: number
  size: number
  content?: string
  expanded: string
  locator?: { digest: string; length: number; urls: string[]; unsupported: boolean; encoding: string }
  topic: string
  record: string
  recordLength: number
  extra?: Array<{ key: number; valueCbor: string }>
}

test('batch records decode to their fields and re-encode byte for byte', () => {
  const v = json<{ batches: BatchVector[] }>('batch-v1.json')
  assert.equal(v.batches.length, 4)
  for (const b of v.batches) {
    const rec = fromHex(b.record)
    assert.equal(rec.length, b.recordLength)
    assert.equal(hexFile(`${b.name}.hex`), b.record, b.name)
    assert.ok(isBatch(rec), b.name)
    const d = decodeBatch(rec)
    assert.equal(toHex(d.producer), b.producer)
    assert.equal(d.stream, b.stream)
    assert.equal(topic(d.stream), b.topic)
    assert.equal(d.seq, b.seq)
    assert.equal(toHex(d.prev), b.prev)
    assert.equal(d.timestamp, b.timestamp)
    assert.equal(d.count, b.count)
    assert.equal(d.encoding, b.encoding)
    assert.equal(d.profile, b.profile)
    assert.equal(d.size, b.size)
    assert.equal(d.content === undefined ? undefined : toHex(d.content), b.content)
    assert.deepEqual(
      d.extra.map((p) => ({ key: Number(p.key), valueCbor: toHex(encode(p.val)) })),
      b.extra ?? [],
    )
    if (b.locator !== undefined) {
      assert.ok(d.locator !== undefined)
      assert.equal(toHex(d.locator.digest), b.locator.digest)
      assert.equal(d.locator.length, b.locator.length)
      assert.deepEqual(d.locator.urls, b.locator.urls)
      assert.equal(locatorUnsupported(d.locator), b.locator.unsupported)
      assert.equal(toHex(encodeLocator(d.locator)), b.locator.encoding)
    }
    assert.equal(toHex(encodeBatch(d)), b.record, `${b.name} re-encodes`)
    // The content: carried, or fetched and checked against the locator.
    const content = d.content ?? fromHex(b.expanded)
    if (d.locator !== undefined) checkFetched(d.locator, content)
    assert.equal(toHex(expand(d, content)), b.expanded, `${b.name} expands`)
  }
})

test('locators decode alone and re-encode byte for byte', () => {
  const v = json<{ locators: Array<{ name: string; locator: { digest: string; length: number; urls: string[]; unsupported: boolean; encoding: string } }> }>(
    'locator-v1.json',
  )
  for (const { name, locator } of v.locators) {
    const l = decodeLocator(fromHex(locator.encoding))
    assert.equal(toHex(l.digest), locator.digest)
    assert.equal(l.length, locator.length)
    assert.deepEqual(l.urls, locator.urls)
    assert.equal(locatorUnsupported(l), locator.unsupported, name)
    assert.equal(toHex(encodeLocator(l)), locator.encoding, name)
  }
  assert.equal(hexFile('locator-hints.hex'), v.locators[0]!.locator.encoding)
})

interface AnchorVector {
  name: string
  producer: string
  stream: string
  interval: number
  prev: string
  first: number
  count: number
  timestamp: number
  root: string
  leaves: Array<{ seq: number; commitment: string; leaf: string; path: Array<{ hash: string; left: boolean }> }>
  record: string
  recordLength: number
  tokenFields: string[]
  tokenSignedHash: string
}

test('anchor records, leaves, roots and every inclusion path', () => {
  const v = json<{ anchors: AnchorVector[] }>('anchor-v1.json')
  for (const a of v.anchors) {
    const rec = fromHex(a.record)
    assert.equal(rec.length, a.recordLength)
    assert.equal(hexFile(`${a.name}.hex`), a.record)
    const d = decodeAnchor(rec)
    assert.equal(toHex(d.producer), a.producer)
    assert.equal(d.stream, a.stream)
    assert.equal(d.interval, a.interval)
    assert.equal(toHex(d.prev), a.prev)
    assert.equal(d.first, a.first)
    assert.equal(d.count, a.count)
    assert.equal(d.timestamp, a.timestamp)
    assert.equal(toHex(d.root), a.root)
    assert.equal(toHex(encodeAnchor(d)), a.record)
    const cs = a.leaves.map((l) => fromHex(l.commitment))
    assert.equal(toHex(intervalRoot(cs)), a.root)
    for (const [i, l] of a.leaves.entries()) {
      assert.equal(toHex(leafHash(cs[i]!)), l.leaf)
      const path: Step[] = l.path.map((s) => ({ hash: fromHex(s.hash), left: s.left }))
      assert.deepEqual(
        prove(cs, i).map((s) => ({ hash: toHex(s.hash), left: s.left })),
        l.path,
      )
      assert.equal(pathIndex(path, d.count), i)
      assert.ok(verifyBatch(d, cs[i]!, l.seq, path), `${a.name} seq ${l.seq}`)
      // A path is for its own position only.
      if (a.leaves.length > 1) assert.ok(!verifyBatch(d, cs[i]!, l.seq === d.first ? l.seq + 1 : l.seq - 1, path))
    }
    assert.deepEqual(a.tokenFields, [toHex(TagAnchor), a.record])
    assert.equal(toHex(Hash.sha256([...TagAnchor, ...rec])), a.tokenSignedHash)
  }
})

test('derivations, tags, magics and stream identifiers', () => {
  const v = json<{
    identityKey: string
    protocol: [number, string]
    keys: Array<{ keyId: string; invoice: string; lockingKey: string }>
    tagAnchor: string
    tagFunding: string
    magicBatch: string
    magicAnchor: string
    topic: string
    streams: Array<{ stream: string; name: string; suffix: string; topic: string }>
    maxStreamName: number
    suffixLength: number
    lookupService: string
  }>('derivation-v1.json')
  assert.deepEqual(v.protocol, Protocol)
  assert.deepEqual(
    v.keys.map((k) => k.keyId),
    [KeyBatch, KeyAnchor, KeyFund],
  )
  for (const k of v.keys) {
    assert.equal(k.invoice, `1-blogs-${k.keyId}`)
    assert.equal(readerLockingKey(Protocol, k.keyId, v.identityKey).toString(), k.lockingKey)
  }
  assert.equal(v.tagAnchor, toHex(TagAnchor))
  assert.equal(v.tagFunding, toHex(TagFunding))
  assert.equal(v.magicBatch, toHex(MagicBatch))
  assert.equal(v.magicAnchor, toHex(MagicAnchor))
  for (const s of v.streams) {
    assert.deepEqual(splitStream(s.stream), { name: s.name, suffix: s.suffix })
    assert.equal(topic(s.stream), s.topic)
  }
  assert.equal(v.topic, v.streams[0]!.topic)
  assert.equal(v.maxStreamName, MaxStreamName)
  assert.equal(v.suffixLength, SuffixLen)
  assert.equal(v.lookupService, LookupService)
})

test('every refusal vector is refused for its reason, and classified as the Go codec does', () => {
  const v = json<{ refusals: Array<{ name: string; kind: string; hex: string; reason: string; atAdmission: string }> }>('refusal-v1.json')
  assert.equal(v.refusals.length, 53)
  for (const r of v.refusals) {
    const b = fromHex(r.hex)
    const got = refusal(() => (r.kind === 'batch' ? decodeBatch(b) : decodeAnchor(b)))
    assert.equal(got, r.reason, r.name)
    if (r.kind === 'batch') assert.equal(isBatch(b) ? 'refuse' : 'skip', r.atAdmission, r.name)
  }
})

interface TxVector {
  name: string
  policy: 'open' | string[]
  txid: string
  rawTx: string
  beef: string
  verdict: 'admit' | 'refuse'
  reason?: string
  admits?: {
    kind: string
    outputIndex?: number
    commitment?: string
    record?: string
    seq?: number
    fundingTxid?: string
    fundingVout?: number
    interval?: number
    funding?: number[]
    tokenTxid?: string
  }
}

test('every transaction is admitted or refused as the Go codec does', async () => {
  const v = json<{
    stream: string
    identityKey: string
    batchLockingKey: string
    fundingScript: string
    headers: Array<{ height: number; merkleRoot: string }>
    transactions: TxVector[]
  }>('transaction-v1.json')
  assert.equal(toHex(fundingScript(fromHex(v.batchLockingKey))), v.fundingScript)
  const roots = new Map(v.headers.map((h) => [h.height, h.merkleRoot]))
  const headers: ChainTracker = {
    isValidRootForHeight: async (root, height) => roots.get(height) === root,
    currentHeight: async () => Math.max(...roots.keys()),
  }
  let admitted = 0
  let refused = 0
  for (const t of v.transactions) {
    const tx = txFromBEEF(fromHex(t.beef))
    assert.equal(tx.id('hex'), t.txid, t.name)
    assert.equal(tx.toHex(), t.rawTx, t.name)
    const policy: Policy = t.policy === 'open' ? { open: true } : { producers: t.policy.map(fromHex) }
    let got
    try {
      got = await admit(tx, { stream: v.stream, policy, headers })
    } catch (e) {
      if (!(e instanceof Refusal)) throw e
      assert.equal(t.verdict, 'refuse', `${t.name}: refused ${e.reason}: ${e.message}`)
      assert.equal(e.reason, t.reason, t.name)
      refused++
      continue
    }
    assert.equal(t.verdict, 'admit', `${t.name}: admitted, want ${t.reason}`)
    const a = t.admits!
    assert.equal(got.kind, a.kind, t.name)
    if (got.carrier !== undefined) {
      assert.equal(toHex(got.carrier.commitment), a.commitment)
      assert.equal(toHex(got.carrier.record), a.record)
      assert.equal(got.carrier.batch.seq, a.seq)
      assert.equal(toHex(got.carrier.funding.txid), a.fundingTxid)
      assert.equal(got.carrier.funding.vout, a.fundingVout)
    }
    if (got.token !== undefined) {
      assert.equal(got.token.outputIndex, a.outputIndex)
      assert.equal(toHex(got.token.record), a.record)
      assert.equal(got.token.anchor.interval, a.interval)
      assert.equal(toHex(got.token.txid), a.tokenTxid)
    }
    assert.deepEqual(got.funding, a.funding ?? [], t.name)
    admitted++
  }
  assert.equal(admitted + refused, v.transactions.length)
  assert.ok(admitted > 0 && refused > 0)
})
