/**
 * ls_log on its own: the question rules, and the statuses of spec sections
 * 8.2 to 8.4 over admitted outputs delivered in any order. The admissions
 * are the transaction vectors and, for the cases no vector covers (a fork,
 * a second chain, a failed join, a wrong root, the caps), transactions
 * built here. The lookup service trusts admission, so those need only the
 * shape it reads.
 */
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { LockingScript, MerklePath, Transaction, UnlockingScript } from '@bsv/sdk'
import { fromHex, toHex } from './admit.js'
import { LookupService, TagAnchor, encodeAnchor, encodeBatch, type Anchor } from './logrec.js'
import { LogLookupService, MaxCandidates, parseQuery } from './ls_log.js'
import { pushDropScript } from './script.js'
import { FakeHost, byName, txOf, txVectors } from './testutil.js'

const v = txVectors()
const producer = v.identityKey
const key = fromHex(producer)
const sig = Uint8Array.of(0x30, 0x06, 0x02, 0x01, 0x01, 0x02, 0x01, 0x01)
const tree = txOf(byName(v, 'funding-tree'))
const txid = (name: string): string => byName(v, name).txid

function service(host = new FakeHost()): LogLookupService {
  return new LogLookupService(new Set([v.topic]), host)
}

function admitTx(ls: LogLookupService, tx: Transaction, outputIndex = 0, topic = v.topic): void {
  ls.outputAdmittedByTopic({ mode: 'whole-tx', atomicBEEF: tx.toAtomicBEEF(), outputIndex, topic })
}

function admitVector(ls: LogLookupService, name: string): void {
  const t = byName(v, name)
  const tx = txOf(t)
  const outs = t.admits?.kind === 'batch' || t.admits?.kind === 'anchor' ? [t.admits.outputIndex!] : (t.admits?.funding ?? [])
  for (const i of outs) admitTx(ls, tx, i)
}

/** A carrier of a one-line batch at seq with prev, spending funding-tree output vout. */
function carrier(seq: number, prev: Uint8Array, line: string, vout = 5): Transaction {
  const content = new TextEncoder().encode(`${line}\n`)
  const record = encodeBatch({ producer: key, stream: v.stream, seq, prev, timestamp: 1, count: 1, encoding: 1, profile: 1, content, size: content.length, extra: [] })
  return new Transaction(
    1,
    [{ sourceTransaction: tree, sourceOutputIndex: vout, unlockingScript: UnlockingScript.fromBinary([]), sequence: 0 }],
    [{ lockingScript: LockingScript.fromBinary(Array.from(pushDropScript(key, [record], sig))), satoshis: 1 }],
    4102444800,
  )
}

/** A mined anchor token spending the given outpoints, at a block height and position. */
function token(a: Partial<Anchor> & Pick<Anchor, 'interval' | 'first' | 'count'>, spends: Array<[string, number]>, height: number, index = 0): Transaction {
  const record = encodeAnchor({ producer: key, stream: v.stream, prev: new Uint8Array(32), root: new Uint8Array(32), timestamp: 1, extra: [], ...a })
  const tx = new Transaction(
    1,
    spends.map(([t, i]) => ({ sourceTXID: t, sourceOutputIndex: i, unlockingScript: UnlockingScript.fromBinary([]), sequence: 0xffffffff })),
    [{ lockingScript: LockingScript.fromBinary(Array.from(pushDropScript(key, [TagAnchor, record], sig))), satoshis: 1 }],
    0,
  )
  tx.merklePath = new MerklePath(height, [[{ offset: index, hash: tx.id('hex'), txid: true }, { offset: index ^ 1, hash: '00'.repeat(32) }]])
  return tx
}

const hashOrder = (display: string): Uint8Array => fromHex(display).reverse()
const commitment = (tx: Transaction): Uint8Array => Uint8Array.from(tx.hash() as number[])

const ask = (ls: LogLookupService, query: unknown) => ls.lookup({ service: LookupService, query })
const txids = async (ls: LogLookupService, query: unknown) => (await ask(ls, query)).map((f) => f.txid)

test('a question is exactly one class, every member within its rule, or an error', async () => {
  const ls = service()
  const ok = { stream: v.stream, producer, seq: 1 }
  assert.deepEqual(parseQuery(ok), ok)
  assert.deepEqual(parseQuery({ stream: v.stream, producer, interval: 2 }), { stream: v.stream, producer, interval: 2 })
  assert.deepEqual(parseQuery({ ...ok, seq: 1.0 }), ok, 'a double with an integer value')
  const bad: unknown[] = [
    null,
    [],
    'seq=1',
    {},
    { stream: v.stream, producer },
    { stream: v.stream, seq: 1 },
    { producer, seq: 1 },
    { ...ok, interval: 1 },
    { ...ok, extra: true },
    { ...ok, stream: 'example_app' },
    { ...ok, stream: 'Example_app_qzxkvbmwtr' },
    { ...ok, stream: 7 },
    { ...ok, producer: producer.toUpperCase() },
    { ...ok, producer: producer.slice(2) },
    { ...ok, producer: `02${'ff'.repeat(32)}` },
    { ...ok, seq: 0 },
    { ...ok, seq: 1.5 },
    { ...ok, seq: 2 ** 53 },
    { ...ok, seq: '1' },
    { ...ok, seq: -1 },
  ]
  for (const q of bad) await assert.rejects(ask(ls, q), /ls_log/, JSON.stringify(q))
  await assert.rejects(ls.lookup({ service: 'ls_anytx', query: ok }), /ls_log/)
  // A member given twice takes its last value, as the JSON parser gives it.
  assert.deepEqual(parseQuery(JSON.parse(`{"stream":"${v.stream}","producer":"${producer}","seq":9,"seq":1}`)), ok)
  assert.deepEqual(await ask(ls, ok), [], 'a stream this host holds nothing of answers empty')
  // The tree class: exactly stream and tree.
  const t = 'ab'.repeat(32)
  assert.deepEqual(parseQuery({ stream: v.stream, tree: t }), { stream: v.stream, tree: t })
  const badTree: unknown[] = [
    { tree: t },
    { stream: v.stream, tree: t, producer },
    { stream: v.stream, tree: t, seq: 1 },
    { stream: v.stream, tree: t, vout: 0 },
    { stream: 'example_app', tree: t },
    { stream: v.stream, tree: t.toUpperCase() },
    { stream: v.stream, tree: t.slice(2) },
    { stream: v.stream, tree: 'zz'.repeat(32) },
    { stream: v.stream, tree: 1 },
  ]
  for (const q of badTree) await assert.rejects(ask(ls, q), /ls_log/, JSON.stringify(q))
  assert.deepEqual(await ask(ls, { stream: v.stream, tree: t }), [], 'a tree this host does not hold answers empty')
})

test('the tree class answers a held funding tree by txid with one output, spent or not, and nothing else', async () => {
  const ls = service()
  const id = txid('funding-tree')
  const q = { stream: v.stream, tree: id }
  assert.deepEqual(await ask(ls, q), [])
  admitVector(ls, 'funding-tree')
  assert.deepEqual(await ask(ls, q), [{ txid: id, outputIndex: 0 }], 'the lowest held funding output')
  assert.equal(ls.treeCount, 1)
  // Spends do not change the answer: every admitted output is retained.
  for (const n of ['carrier-seq-1', 'carrier-seq-2', 'carrier-seq-3', 'anchor-interval-1', 'anchor-interval-2']) admitVector(ls, n)
  assert.deepEqual(await ask(ls, q), [{ txid: id, outputIndex: 0 }])
  // A carrier and an anchor token are held coins, but not funding outputs: no tree answers them.
  assert.deepEqual(await ask(ls, { stream: v.stream, tree: txid('carrier-seq-1') }), [])
  assert.deepEqual(await ask(ls, { stream: v.stream, tree: txid('anchor-interval-1') }), [])
  // A sweep's tombstone is a funding output of a mined transaction, so its txid answers too.
  admitVector(ls, 'sweep')
  assert.deepEqual(await ask(ls, { stream: v.stream, tree: txid('sweep') }), [{ txid: txid('sweep'), outputIndex: 0 }])
  // Another stream's topic holds nothing of it.
  assert.deepEqual(await ask(ls, { stream: 'example_app_hdjvnwpxle', tree: id }), [])
  // An output the engine no longer holds leaves the answer; the last one empties it.
  ls.outputNoLongerRetainedInHistory(id, 0, v.topic)
  assert.deepEqual(await ask(ls, q), [{ txid: id, outputIndex: 1 }])
  for (const i of [1, 2, 3, 4]) ls.outputEvicted(id, i)
  assert.deepEqual(await ask(ls, q), [])
})

test('a gap is reported and closed by the late batch, in either delivery order', async () => {
  for (const order of [
    ['carrier-seq-1', 'carrier-seq-3', 'carrier-seq-2'],
    ['carrier-seq-3', 'carrier-seq-2', 'carrier-seq-1'],
  ]) {
    const ls = service()
    admitVector(ls, 'funding-tree')
    admitVector(ls, order[0]!)
    admitVector(ls, order[1]!)
    const mid = ls.report()[0]!
    assert.equal(mid.highest, 3)
    assert.ok(mid.holes.length === 1, JSON.stringify(mid.holes))
    admitVector(ls, order[2]!)
    const r = ls.report()[0]!
    assert.deepEqual(r.holes, [])
    for (const n of order) assert.equal(ls.batchStatusOf(producer, v.stream, txid(n)), 'linked', n)
  }
  const ls = service()
  admitVector(ls, 'carrier-seq-3')
  assert.equal(ls.batchStatusOf(producer, v.stream, txid('carrier-seq-3')), 'gap')
  assert.deepEqual(ls.report()[0]!.holes, [[1, 2]])
  // A producer may number a batch 2^53 - 1; the report walks held seqs only.
  admitTx(ls, carrier(Number.MAX_SAFE_INTEGER, new Uint8Array(32).fill(3), 'far'))
  assert.deepEqual(ls.report()[0]!.holes, [[1, 2], [4, Number.MAX_SAFE_INTEGER - 1]])
  assert.equal(ls.missingCount, Number.MAX_SAFE_INTEGER - 2)
})

test('a fork keeps both candidates, answers both by commitment, and the anchor root picks one', async () => {
  const host = new FakeHost()
  const ls = service(host)
  for (const n of ['carrier-seq-1', 'carrier-seq-2', 'carrier-seq-3', 'anchor-interval-1']) admitVector(ls, n)
  const c1 = hashOrder(txid('carrier-seq-1'))
  const other = carrier(2, c1, 'another second batch')
  admitTx(ls, other)
  const want = [txid('carrier-seq-2'), other.id('hex')].sort((a, b) => (toHex(hashOrder(a)) < toHex(hashOrder(b)) ? -1 : 1))
  assert.deepEqual(await txids(ls, { stream: v.stream, producer, seq: 2 }), want)
  assert.equal(ls.batchStatusOf(producer, v.stream, other.id('hex')), 'fork')
  assert.equal(ls.batchStatusOf(producer, v.stream, txid('carrier-seq-2')), 'fork')
  assert.equal(ls.batchStatusOf(producer, v.stream, txid('carrier-seq-3')), 'linked', 'its prev names a held candidate')
  const r = ls.report()[0]!
  assert.deepEqual(r.forks, [2])
  assert.deepEqual(r.intervals.map((i) => i.status), ['complete'], 'the root selects the real second batch')
  assert.equal(ls.forkSeqCount, 1)
  // A batch whose prev names no held batch at seq - 1 is a fork too.
  const stray = carrier(4, new Uint8Array(32).fill(7), 'stray')
  admitTx(ls, stray)
  assert.equal(ls.batchStatusOf(producer, v.stream, stray.id('hex')), 'fork')
})

test('an interval whose root no held choice reproduces is root-mismatch', async () => {
  const ls = service()
  for (const n of ['carrier-seq-1', 'carrier-seq-2', 'carrier-seq-3', 'anchor-interval-1', 'anchor-interval-2']) admitVector(ls, n)
  const c3 = commitment(txOf(byName(v, 'carrier-seq-3')))
  const b4 = carrier(4, c3, 'four')
  admitTx(ls, b4)
  const t3 = token({ interval: 3, prev: hashOrder(txid('anchor-interval-2')), first: 4, count: 1, root: new Uint8Array(32).fill(1) }, [[txid('anchor-interval-2'), 0]], 900)
  admitTx(ls, t3)
  const r = ls.report()[0]!
  assert.deepEqual(r.intervals.map((i) => [i.interval, i.status]), [[1, 'complete'], [2, 'complete'], [3, 'root-mismatch']])
  // Missing positions make it incomplete, not a mismatch.
  const t4 = token({ interval: 4, prev: hashOrder(t3.id('hex')), first: 5, count: 2 }, [[t3.id('hex'), 0]], 901)
  admitTx(ls, t4)
  assert.deepEqual(ls.report()[0]!.intervals[3], { interval: 4, first: 5, count: 2, txid: t4.id('hex'), status: 'incomplete', missing: [5, 6] })
})

test('a second chain is anchor-branch, a failed join anchor-join, an unheld predecessor pending', async () => {
  const ls = service()
  admitVector(ls, 'anchor-interval-1')
  const t1 = txid('anchor-interval-1')
  const later = token({ interval: 1, first: 1, count: 9 }, [[txid('funding-tree'), 5]], 5000)
  admitTx(ls, later)
  const badJoin = token({ interval: 2, prev: hashOrder(t1), first: 7, count: 1 }, [[t1, 0]], 5001)
  admitTx(ls, badJoin)
  const orphan = token({ interval: 6, prev: new Uint8Array(32).fill(9), first: 50, count: 1 }, [['ab'.repeat(32), 0]], 5002)
  admitTx(ls, orphan)
  const r = ls.report()[0]!
  assert.deepEqual(r.intervals.map((i) => [i.interval, i.txid]), [[1, t1]], 'the chain stops before the failed join')
  assert.deepEqual(
    r.tokens.map((t) => [t.interval, t.txid, t.status]),
    [
      [1, later.id('hex'), 'anchor-branch'],
      [2, badJoin.id('hex'), 'anchor-join'],
      [6, orphan.id('hex'), 'pending'],
    ],
  )
  assert.deepEqual(await txids(ls, { stream: v.stream, producer, interval: 1 }), [t1, later.id('hex')], 'canonical first')
  // A token mined earlier than the vector's interval 1 becomes canonical.
  const earlier = token({ interval: 1, first: 1, count: 2 }, [[txid('funding-tree'), 5]], 10)
  admitTx(ls, earlier)
  assert.equal((await txids(ls, { stream: v.stream, producer, interval: 1 }))[0], earlier.id('hex'))
})

test('at most 8 candidates per seq and 8 tokens per interval are kept and answered; the rest are counted', async () => {
  const host = new FakeHost()
  const ls = service(host)
  const made = Array.from({ length: MaxCandidates + 1 }, (_, i) => carrier(5, new Uint8Array(32).fill(1), `line ${i}`))
  for (const tx of made) admitTx(ls, tx)
  const keys = made.map((tx) => ({ id: tx.id('hex'), k: toHex(commitment(tx)) })).sort((a, b) => (a.k < b.k ? -1 : 1))
  assert.deepEqual(await txids(ls, { stream: v.stream, producer, seq: 5 }), keys.slice(0, MaxCandidates).map((x) => x.id))
  assert.equal(host.count('blogs_candidates_dropped_total'), 1)
  const toks = Array.from({ length: MaxCandidates + 1 }, (_, i) => token({ interval: 3, prev: new Uint8Array(32).fill(2), first: 9, count: 1 + i }, [['cd'.repeat(32), i]], 700 + i))
  for (const t of toks) admitTx(ls, t)
  assert.equal((await txids(ls, { stream: v.stream, producer, interval: 3 })).length, MaxCandidates)
  assert.equal(host.count('blogs_tokens_dropped_total'), 1)
})

test('only a mined spend by another transaction retracts; an unmined one does not', async () => {
  const ls = service()
  admitVector(ls, 'carrier-seq-1')
  // A second carrier on the same funding output (never mined) is not a retraction.
  const second = carrier(1, new Uint8Array(32), 'same funding output', 0)
  admitTx(ls, second)
  assert.equal(ls.batchStatusOf(producer, v.stream, txid('carrier-seq-1')), 'fork')
  assert.equal(ls.retractedCount, 0)
  admitVector(ls, 'sweep')
  assert.equal(ls.batchStatusOf(producer, v.stream, txid('carrier-seq-1')), 'retracted')
  assert.equal(ls.batchStatusOf(producer, v.stream, second.id('hex')), 'retracted')
  assert.deepEqual(await txids(ls, { stream: v.stream, producer, seq: 1 }), [])
  // A carrier that arrives after the sweep is retracted on arrival.
  const late = service()
  admitVector(late, 'sweep')
  admitVector(late, 'carrier-seq-2')
  assert.equal(late.batchStatusOf(producer, v.stream, txid('carrier-seq-2')), 'retracted')
})

test('admissions on topics the module does not declare are ignored; an evicted output leaves the index', async () => {
  const ls = service()
  admitTx(ls, txOf(byName(v, 'carrier-seq-1')), 0, 'tm_anytx')
  assert.equal(ls.batchCount, 0)
  admitVector(ls, 'carrier-seq-1')
  assert.equal(ls.batchCount, 1)
  ls.outputEvicted(txid('carrier-seq-1'), 0)
  assert.equal(ls.batchCount, 0)
  assert.deepEqual(await txids(ls, { stream: v.stream, producer, seq: 1 }), [])
})

test('the previous coins come from the index until storage is handed over', async () => {
  const ls = service()
  admitVector(ls, 'funding-tree')
  const coin = await ls.coin(v.topic, txid('funding-tree'), 3)
  assert.equal(toHex(coin!.lockingScript), toHex(Uint8Array.from(tree.outputs[3]!.lockingScript.toBinary())))
  assert.equal(await ls.coin(v.topic, txid('funding-tree'), 5), undefined, 'change is not admitted')
  assert.equal(await ls.coin('tm_anytx', txid('funding-tree'), 3), undefined)
})
