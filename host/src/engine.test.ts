/**
 * The module inside the real overlay engine (the upstream release the
 * reference host's engine is a one-patch fork of, at the same version),
 * over an in-memory store: what the engine records for a refusal, the
 * out-of-order anchor, a delivery in scrambled order, retraction, and a
 * restore from the engine's own storage.
 */
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { Engine } from '@bsv/overlay'
import { Transaction } from '@bsv/sdk'
import type { Module, TopicManager } from '@lightwebinc/bcommon'
import { LookupService } from './logrec.js'
import { LogLookupService } from './ls_log.js'
import { blogsModule } from './module.js'
import { Unrecorded } from './tm_log.js'
import { FakeHost, MemoryStorage, beefOf, byName, quietConsole, txVectors, vectorHeaders } from './testutil.js'

const v = txVectors()
const topic = v.topic
const producer = v.identityKey

function engine(mod: Module, storage: MemoryStorage): Engine {
  return new Engine(
    mod.topics as never,
    mod.lookups as never,
    storage as never,
    vectorHeaders(v),
    undefined,
    [],
    [],
    undefined,
    undefined,
    { [topic]: false },
    false,
    undefined,
    undefined,
    undefined,
    quietConsole(),
  )
}

async function submit(e: Engine, name: string): Promise<number[]> {
  const steak = await e.submit({ beef: beefOf(byName(v, name)), topics: [topic] })
  return steak[topic]?.outputsToAdmit ?? []
}

async function answer(e: Engine, query: Record<string, unknown>): Promise<string[]> {
  const a = await e.lookup({ service: LookupService, query })
  if (a.type !== 'output-list') throw new Error('not an output list')
  return a.outputs.map((o) => Transaction.fromBEEF(o.beef).id('hex'))
}

const txid = (name: string): string => byName(v, name).txid
const fundingOut4 = (s: MemoryStorage) => s.findOutput(txid('funding-tree'), 4, topic)

/** A module and its engine, with storage handed over as the host does. */
async function setup(opts: { naive?: boolean; restore?: boolean } = {}) {
  const host = new FakeHost()
  const mod = blogsModule(host, [{ stream: v.stream, policy: { open: true } }])
  const ls = mod.lookups![LookupService] as LogLookupService
  const storage = new MemoryStorage()
  if (opts.restore !== false) await ls.restore([], storage)
  if (opts.naive === true) {
    // A manager that returns every refusal as empty instructions, as a
    // manager that does not raise would.
    const tm = mod.topics![topic]!
    const naive: TopicManager = {
      identifyAdmissibleOutputs: async (beef, previousCoins) => {
        calls++
        try {
          return await tm.identifyAdmissibleOutputs(beef, previousCoins)
        } catch (e) {
          if (e instanceof Unrecorded) return { outputsToAdmit: [], coinsToRetain: [...previousCoins] }
          throw e
        }
      },
      getDocumentation: tm.getDocumentation.bind(tm),
      getMetaData: tm.getMetaData.bind(tm),
    }
    mod.topics![topic] = naive
  }
  let calls = 0
  return { host, mod, ls, storage, engine: engine(mod, storage), calls: () => calls }
}

test('the engine records a refusal returned as empty instructions, and never shows the transaction to a topic manager again', async () => {
  const { engine: e, storage, calls } = await setup({ naive: true })
  assert.deepEqual(await submit(e, 'funding-tree'), [0, 1, 2, 3, 4])
  // Interval 2 before interval 1: its predecessor is not held, its fee
  // input spends held funding output 4.
  assert.deepEqual(await submit(e, 'anchor-interval-2'), [])
  assert.ok(await storage.doesAppliedTransactionExist({ txid: txid('anchor-interval-2'), topic }), 'recorded as applied')
  assert.equal((await fundingOut4(storage))?.spent, true, 'and the held output it spends is marked spent')
  assert.deepEqual(await submit(e, 'anchor-interval-1'), [0])
  const before = calls()
  assert.deepEqual(await submit(e, 'anchor-interval-2'), [], 'offered again once its predecessor is held')
  assert.equal(calls(), before, 'a duplicate: the topic manager is not asked')
  assert.deepEqual(await answer(e, { stream: v.stream, producer, interval: 2 }), [], 'interval 2 is lost to this host')
})

test('an out-of-order anchor is not recorded, spends nothing, and is admitted when offered after its predecessor', async () => {
  const { engine: e, storage, host } = await setup()
  assert.deepEqual(await submit(e, 'funding-tree'), [0, 1, 2, 3, 4])
  assert.deepEqual(await submit(e, 'anchor-interval-2'), [])
  assert.equal(await storage.doesAppliedTransactionExist({ txid: txid('anchor-interval-2'), topic }), false, 'not recorded')
  assert.equal((await fundingOut4(storage))?.spent, false, 'the held funding output is untouched')
  assert.equal(host.count('blogs_refused_total', { reason: 'anchor-input', recorded: 'no' }), 1)
  assert.deepEqual(await submit(e, 'anchor-interval-1'), [0])
  assert.deepEqual(await submit(e, 'anchor-interval-2'), [0], 'decided afresh and admitted')
  assert.deepEqual(await answer(e, { stream: v.stream, producer, interval: 2 }), [txid('anchor-interval-2')])
  assert.equal((await fundingOut4(storage))?.spent, true)
})

test('without storage handed over, the previous coins come from the index and the same order works', async () => {
  const { engine: e } = await setup({ restore: false })
  await submit(e, 'funding-tree')
  assert.deepEqual(await submit(e, 'anchor-interval-2'), [])
  assert.deepEqual(await submit(e, 'anchor-interval-1'), [0])
  assert.deepEqual(await submit(e, 'anchor-interval-2'), [0])
})

test('scrambled delivery, retraction, and a restore from the engine storage in any order', async () => {
  const { engine: e, storage, ls } = await setup()
  const q = (k: string, n: number) => ({ stream: v.stream, producer, [k]: n })
  await submit(e, 'funding-tree')
  assert.deepEqual(await submit(e, 'carrier-seq-3'), [0])
  assert.deepEqual(await submit(e, 'carrier-seq-1'), [0])
  let r = ls.report(v.stream)[0]!
  assert.deepEqual(r.holes, [[2, 2]], 'seq 2 is a hole')
  assert.equal(ls.batchStatusOf(producer, v.stream, txid('carrier-seq-3')), 'gap')
  assert.deepEqual(await submit(e, 'anchor-interval-1'), [0])
  r = ls.report(v.stream)[0]!
  assert.deepEqual(r.intervals.map((i) => [i.interval, i.status, i.missing]), [[1, 'incomplete', [2]]])
  assert.deepEqual(await submit(e, 'carrier-seq-2'), [0])
  assert.deepEqual(await submit(e, 'anchor-interval-2'), [0])
  r = ls.report(v.stream)[0]!
  assert.deepEqual(r.holes, [])
  assert.equal(ls.batchStatusOf(producer, v.stream, txid('carrier-seq-3')), 'linked')
  assert.deepEqual(r.intervals.map((i) => [i.interval, i.status]), [[1, 'complete'], [2, 'complete']])
  for (const [seq, name] of [[1, 'carrier-seq-1'], [2, 'carrier-seq-2'], [3, 'carrier-seq-3']] as const) {
    assert.deepEqual(await answer(e, q('seq', seq)), [txid(name)])
  }
  assert.deepEqual(await answer(e, q('interval', 1)), [txid('anchor-interval-1')], 'a spent token is still answered')

  // The sweep retracts seq 1 and 2: nothing answers them, their receipts
  // still complete interval 1.
  assert.deepEqual(await submit(e, 'sweep'), [0])
  r = ls.report(v.stream)[0]!
  assert.deepEqual(r.retracted, [1, 2])
  assert.deepEqual(await answer(e, q('seq', 1)), [])
  assert.deepEqual(await answer(e, q('seq', 3)), [txid('carrier-seq-3')])
  assert.deepEqual(r.intervals.map((i) => i.status), ['complete', 'complete'])

  const live = ls.report()
  const rows = await storage.findUTXOsForTopic(topic, undefined, undefined, true)
  // A host that hands the rows without their BEEF, as an older reference
  // host does: each is read again from storage.
  const bare = (await storage.findUTXOsForTopic(topic)).map((o) => ({ ...o, beef: undefined }))
  for (const order of [rows, [...rows].reverse(), bare]) {
    const host = new FakeHost()
    const again = blogsModule(host, [{ stream: v.stream, policy: { open: true } }]).lookups![LookupService] as LogLookupService
    assert.ok((await again.restore(order, storage)) > 0)
    assert.deepEqual(again.report(), live, 'restore reaches the same statuses whatever order storage returns')
    for (const [k, n] of [['seq', 1], ['seq', 2], ['seq', 3], ['interval', 1], ['interval', 2]] as const) {
      assert.deepEqual(await again.lookup({ service: LookupService, query: q(k, n) }), await ls.lookup({ service: LookupService, query: q(k, n) }))
    }
  }
})
