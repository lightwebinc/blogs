/**
 * Retention: proven carriers older than the policy leave the index and the
 * engine's storage, oldest intervals first, and never anything else; what
 * was dropped stays dropped across a restart; the configuration is
 * refused rather than guessed.
 */
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Engine } from '@bsv/overlay'
import { Transaction } from '@bsv/sdk'
import { LookupService, decodeAnchor } from './logrec.js'
import { LogLookupService } from './ls_log.js'
import { blogsModule } from './module.js'
import { MinKeepDays, StateFile, fileStore, memoryStore, parseRetention, type Retention } from './retention.js'
import { readPushDrop } from './script.js'
import { FakeHost, MemoryStorage, beefOf, byName, quietConsole, txOf, txVectors, vectorHeaders } from './testutil.js'

const v = txVectors()
const topic = v.topic
const producer = v.identityKey
const day = 24 * 60 * 60 * 1000
const txid = (name: string): string => byName(v, name).txid
const anchorTime = (name: string): number => decodeAnchor(readPushDrop(Uint8Array.from(txOf(byName(v, name)).outputs[0]!.lockingScript.toBinary()))!.fields[1]!).timestamp

function engine(ls: LogLookupService, mod: ReturnType<typeof blogsModule>, storage: MemoryStorage): Engine {
  return new Engine(mod.topics as never, { [LookupService]: ls } as never, storage as never, vectorHeaders(v), undefined, [], [], undefined, undefined, { [topic]: false }, false, undefined, undefined, undefined, quietConsole())
}

async function setup(retention: Retention, storage = new MemoryStorage()) {
  const host = new FakeHost()
  const mod = blogsModule(host, [{ stream: v.stream, policy: { open: true } }], undefined, retention)
  const ls = mod.lookups![LookupService] as LogLookupService
  await ls.restore(await storage.findUTXOsForTopic(topic, undefined, undefined, true), storage)
  await ls.retain()
  return { host, ls, storage, e: engine(ls, mod, storage) }
}

async function submit(e: Engine, ...names: string[]): Promise<void> {
  for (const n of names) await e.submit({ beef: beefOf(byName(v, n)), topics: [topic] })
}

const seqAnswer = (ls: LogLookupService, seq: number) => ls.lookup({ service: LookupService, query: { stream: v.stream, producer, seq } })
const treeAnswer = async (e: Engine, id: string) => {
  const a = await e.lookup({ service: LookupService, query: { stream: v.stream, tree: id } })
  if (a.type !== 'output-list') throw new Error('not an output list')
  return a.outputs.map((o) => [Transaction.fromBEEF(o.beef).id('hex'), o.outputIndex])
}

test('BLOGS_KEEP_DAYS and BLOGS_STATE_DIR: off by default, a floor, both or neither', () => {
  assert.equal(parseRetention({}), undefined)
  assert.equal(parseRetention({ BLOGS_KEEP_DAYS: ' ' }), undefined)
  assert.deepEqual(parseRetention({ BLOGS_KEEP_DAYS: '7', BLOGS_STATE_DIR: '/var/lib/blogs' }), { keepMs: 7 * day, stateDir: '/var/lib/blogs' })
  assert.deepEqual(parseRetention({ BLOGS_KEEP_DAYS: '1.5', BLOGS_STATE_DIR: '/s' }), { keepMs: 1.5 * day, stateDir: '/s' })
  assert.equal(MinKeepDays, 1)
  for (const bad of [
    { BLOGS_STATE_DIR: '/s' },
    { BLOGS_KEEP_DAYS: '0.5', BLOGS_STATE_DIR: '/s' },
    { BLOGS_KEEP_DAYS: '0', BLOGS_STATE_DIR: '/s' },
    { BLOGS_KEEP_DAYS: '-3', BLOGS_STATE_DIR: '/s' },
    { BLOGS_KEEP_DAYS: '7d', BLOGS_STATE_DIR: '/s' },
    { BLOGS_KEEP_DAYS: '7' },
    { BLOGS_KEEP_DAYS: '7', BLOGS_STATE_DIR: 'relative/dir' },
  ]) {
    assert.throws(() => parseRetention(bad), JSON.stringify(bad))
  }
})

test('the state file is written whole, read back, and a file that does not parse stops the host', () => {
  const dir = mkdtempSync(join(tmpdir(), 'blogs-retention-'))
  try {
    const st = fileStore(dir)
    assert.deepEqual(st.load(), { marks: [], trees: [] }, 'no file is the empty state')
    const state = { marks: [{ producer, stream: v.stream, interval: 2, seq: 3, c: 'ab'.repeat(32) }], trees: [[`${topic}|${'cd'.repeat(32)}`, 2]] as Array<[string, number]> }
    st.save(state)
    assert.deepEqual(fileStore(dir).load(), state)
    for (const bad of ['{', '{"v":2,"marks":[],"trees":[]}', '{"v":1,"marks":[{"producer":"p","stream":"s","interval":1,"seq":-1,"c":""}],"trees":[]}']) {
      writeFileSync(join(dir, StateFile), bad)
      assert.throws(() => fileStore(dir).load(), bad)
    }
  } finally {
    rmSync(dir, { recursive: true, force: true })
  }
})

test('proven intervals older than the policy are dropped, oldest first; anchors, trees and unproven carriers stay', async () => {
  const store = memoryStore()
  let now = anchorTime('anchor-interval-1') + day - 1
  const { host, ls, storage, e } = await setup({ keepMs: day, store, now: () => now })
  await submit(e, 'funding-tree', 'carrier-seq-1', 'carrier-seq-2', 'carrier-seq-3', 'anchor-interval-1')

  // Not old enough: nothing.
  assert.deepEqual(await ls.retain(), { intervals: 0, carriers: 0, deleted: 0 })
  assert.equal(ls.batchCount, 3)
  assert.equal(store.saved, 0)

  // Interval 1 (seqs 1 and 2) is old enough and complete; seq 3 has no
  // anchor yet, so it is unproven and stays.
  now = anchorTime('anchor-interval-2') + day
  assert.deepEqual(await ls.retain(), { intervals: 1, carriers: 2, deleted: 2 })
  assert.equal(ls.batchCount, 1)
  assert.deepEqual(await seqAnswer(ls, 1), [])
  assert.deepEqual(await seqAnswer(ls, 3), [{ txid: txid('carrier-seq-3'), outputIndex: 0 }])
  assert.equal(ls.batchStatusOf(producer, v.stream, txid('carrier-seq-3')), 'linked', 'its prev is the dropped seq 2')
  assert.equal(await storage.findOutput(txid('carrier-seq-1'), 0, topic), null, 'the carrier left storage')
  assert.ok((await storage.findOutput(txid('funding-tree'), 0, topic))?.spent, 'its funding output did not')
  assert.equal(host.count('blogs_retired_total'), 2)
  assert.deepEqual(store.state.marks, [{ producer, stream: v.stream, interval: 1, seq: 2, c: Buffer.from(txOf(byName(v, 'carrier-seq-2')).hash() as number[]).toString('hex') }])
  let r = ls.report(v.stream)[0]!
  assert.deepEqual(r.holes, [])
  assert.deepEqual(r.intervals, [{ interval: 1, first: 1, count: 2, txid: txid('anchor-interval-1'), status: 'complete', missing: [], retired: true }])
  assert.equal(ls.missingCount, 0)
  assert.equal(ls.retiredCount, 2)

  // Interval 2 arrives and is dropped at the next pass; the anchors and the
  // tree are still answered.
  await submit(e, 'anchor-interval-2')
  assert.deepEqual(await ls.retain(), { intervals: 1, carriers: 1, deleted: 1 })
  assert.equal(ls.batchCount, 0)
  r = ls.report(v.stream)[0]!
  assert.deepEqual(r.intervals.map((i) => [i.interval, i.status, i.retired]), [[1, 'complete', true], [2, 'complete', true]])
  assert.equal(r.highest, 3)
  for (const k of [1, 2]) {
    const a = await e.lookup({ service: LookupService, query: { stream: v.stream, producer, interval: k } })
    assert.ok(a.type === 'output-list' && a.outputs.length === 1, `interval ${k} still answered`)
  }
  assert.deepEqual(await treeAnswer(e, txid('funding-tree')), [[txid('funding-tree'), 0]])
  assert.deepEqual((await storage.findOutputsForTransaction(txid('funding-tree'))).map((o) => o.spent), [true, true, true, true, true])

  // A dropped carrier offered again is a duplicate to the engine: nothing changes.
  await submit(e, 'carrier-seq-1')
  assert.equal(ls.batchCount, 0)
  assert.equal(await storage.findOutput(txid('carrier-seq-1'), 0, topic), null)

  // A restart: restore from the engine's storage and the state, and the
  // index reads the same, the tree still answered.
  const live = ls.report()
  const again = await setup({ keepMs: day, store, now: () => now }, storage)
  assert.deepEqual(again.ls.report(), live)
  assert.equal(again.ls.batchCount, 0)
  assert.deepEqual(await treeAnswer(again.e, txid('funding-tree')), [[txid('funding-tree'), 0]])
  assert.equal(again.ls.treeCount, ls.treeCount)
})

test('without retention a host keeps everything and its answers are unchanged', async () => {
  const host = new FakeHost()
  const mod = blogsModule(host, [{ stream: v.stream, policy: { open: true } }])
  const ls = mod.lookups![LookupService] as LogLookupService
  const storage = new MemoryStorage()
  await ls.restore([], storage)
  const e = engine(ls, mod, storage)
  await submit(e, 'funding-tree', 'carrier-seq-1', 'carrier-seq-2', 'carrier-seq-3', 'anchor-interval-1', 'anchor-interval-2')
  assert.deepEqual(await ls.retain(), { intervals: 0, carriers: 0, deleted: 0 })
  assert.equal(ls.batchCount, 3)
  assert.equal(host.count('blogs_retired_total'), 0)
})

test('a store that cannot delete keeps dropped carriers in storage; restore skips them and a later pass deletes them', async () => {
  const store = memoryStore()
  const now = () => Number.MAX_SAFE_INTEGER
  const storage = new MemoryStorage()
  const noDelete = Object.create(storage) as MemoryStorage
  Object.defineProperty(noDelete, 'deleteOutput', { value: undefined })
  const first = await setup({ keepMs: day, store, now }, noDelete)
  await submit(first.e, 'funding-tree', 'carrier-seq-1', 'carrier-seq-2', 'carrier-seq-3', 'anchor-interval-1', 'anchor-interval-2')
  assert.deepEqual(await first.ls.retain(), { intervals: 2, carriers: 3, deleted: 0 })
  assert.ok(first.host.lines.some((l) => l.msg.includes('no deleteOutput')))
  assert.notEqual(await storage.findOutput(txid('carrier-seq-1'), 0, topic), null, 'still in storage')

  // The restart with a store that deletes: restore does not index them,
  // and the pass after it deletes them.
  const again = await setup({ keepMs: day, store, now }, storage)
  assert.equal(again.ls.batchCount, 0)
  assert.deepEqual(again.ls.report(), first.ls.report())
  for (const n of ['carrier-seq-1', 'carrier-seq-2', 'carrier-seq-3']) assert.equal(await storage.findOutput(txid(n), 0, topic), null, n)
})

test('the state is written before storage is touched; a write that fails deletes nothing', async () => {
  const store = memoryStore()
  let fail = true
  const failing = { ...store, save: (s: Parameters<typeof store.save>[0]) => (fail ? (() => { throw new Error('disk full') })() : store.save(s)) }
  const { host, ls, storage, e } = await setup({ keepMs: day, store: failing, now: () => Number.MAX_SAFE_INTEGER })
  await submit(e, 'funding-tree', 'carrier-seq-1', 'carrier-seq-2', 'carrier-seq-3', 'anchor-interval-1')
  assert.deepEqual(await ls.retain(), { intervals: 1, carriers: 2, deleted: 0 })
  assert.ok(host.lines.some((l) => l.msg.includes('could not write the retention state')))
  assert.notEqual(await storage.findOutput(txid('carrier-seq-1'), 0, topic), null)
  fail = false
  assert.deepEqual(await ls.retain(), { intervals: 0, carriers: 0, deleted: 2 })
  assert.equal(store.state.marks[0]?.seq, 2)
  assert.equal(await storage.findOutput(txid('carrier-seq-1'), 0, topic), null)
})

test('the module reads the policy from the environment and refuses a bad one', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'blogs-retention-'))
  const saved = { ...process.env }
  try {
    const { default: create } = await import('./module.js')
    process.env['BLOGS_STREAMS'] = `${v.stream}=open`
    process.env['OVERLAY_TOPICS'] = topic
    process.env['BLOGS_KEEP_DAYS'] = '0.5'
    process.env['BLOGS_STATE_DIR'] = dir
    assert.throws(() => create(new FakeHost()), /floor/)
    process.env['BLOGS_KEEP_DAYS'] = '3'
    const host = new FakeHost()
    create(host)
    assert.equal(host.lines.find((l) => l.msg === 'blogs module mounted')?.extra?.['retention'], '3 days')
    writeFileSync(join(dir, StateFile), 'not json')
    assert.throws(() => create(new FakeHost()))
    assert.equal(readFileSync(join(dir, StateFile), 'utf8'), 'not json')
  } finally {
    for (const k of ['BLOGS_STREAMS', 'OVERLAY_TOPICS', 'BLOGS_KEEP_DAYS', 'BLOGS_STATE_DIR']) {
      if (saved[k] === undefined) delete process.env[k]
      else process.env[k] = saved[k]
    }
    rmSync(dir, { recursive: true, force: true })
  }
})
