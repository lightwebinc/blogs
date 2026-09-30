/**
 * The topic manager at the engine seam: every transaction vector through
 * identifyAdmissibleOutputs with its previous coins, every record refusal
 * vector inside a transaction, and the rule for which refusals the engine
 * may record. Together they refuse for every reason the spec names.
 */
import { test } from 'node:test'
import assert from 'node:assert/strict'
import { LockingScript, Transaction, UnlockingScript } from '@bsv/sdk'
import { fromHex, type Policy } from './admit.js'
import { TagAnchor, type Reason } from './logrec.js'
import { pushDropScript } from './script.js'
import { DeferredReasons, LogTopicManager, Reasons, Unrecorded } from './tm_log.js'
import { FakeHost, beefOf, byName, coinsOf, readVectors, txVectors, vectorHeaders } from './testutil.js'

const v = txVectors()
const headers = vectorHeaders(v)

function manager(policy: Policy, source: ConstructorParameters<typeof LogTopicManager>[2], host = new FakeHost()): LogTopicManager {
  return new LogTopicManager(v.topic, policy, source, host, headers)
}

/** Runs one submission; a raised refusal comes back as its reason. */
async function run(tm: LogTopicManager, beef: number[], previousCoins: number[]) {
  try {
    return { ok: await tm.identifyAdmissibleOutputs(beef, previousCoins) }
  } catch (e) {
    if (e instanceof Unrecorded) return { raised: e.reason }
    throw e
  }
}

const exercised = new Set<Reason>()

test('the 54 transaction vectors through the module: admitted, recorded refusal, or raised refusal', async () => {
  assert.equal(v.transactions.length, 54)
  let admitted = 0
  let recorded = 0
  let raised = 0
  for (const t of v.transactions) {
    const host = new FakeHost()
    const policy: Policy = t.policy === 'open' ? { open: true } : { producers: t.policy.map(fromHex) }
    const { previousCoins, source } = coinsOf(t)
    const got = await run(manager(policy, source, host), beefOf(t), previousCoins)
    if (t.verdict === 'admit') {
      assert.ok(got.ok !== undefined, `${t.name}: raised ${got.raised}`)
      const a = t.admits!
      const want = a.kind === 'batch' || a.kind === 'anchor' ? [a.outputIndex!] : (a.funding ?? [])
      assert.deepEqual(got.ok.outputsToAdmit, want, t.name)
      assert.deepEqual(got.ok.coinsToRetain, previousCoins, `${t.name}: every held output it spends is retained`)
      admitted++
      continue
    }
    const reason = t.reason as Reason
    exercised.add(reason)
    const mustRaise = previousCoins.length > 0 || DeferredReasons.includes(reason)
    if (mustRaise) {
      assert.equal(got.raised, reason, `${t.name}: want ${reason} raised`)
      raised++
    } else {
      assert.deepEqual(got.ok, { outputsToAdmit: [], coinsToRetain: [] }, `${t.name}: want ${reason} recorded`)
      recorded++
    }
    assert.equal(host.count('blogs_refused_total', { reason, recorded: mustRaise ? 'no' : 'yes' }), 1, t.name)
  }
  assert.equal(admitted + recorded + raised, 54)
  assert.ok(recorded > 0 && raised > 0)
})

test('an anchor whose predecessor is not held is raised, not recorded, even though it spends a held funding output', async () => {
  const t = byName(v, 'anchor-interval-2-token-not-held')
  const { previousCoins, source } = coinsOf(t)
  assert.equal(previousCoins.length, 1, 'the fee input spends a held funding output')
  assert.deepEqual(await run(manager({ open: true }, source), beefOf(t), previousCoins), { raised: 'anchor-input' })
})

test('an unmined anchor is raised, so that its mined publication is decided afresh', async () => {
  const t = byName(v, 'anchor-unmined')
  const { previousCoins, source } = coinsOf(t)
  assert.deepEqual(await run(manager({ open: true }, source), beefOf(t), previousCoins), { raised: 'unmined' })
})

test('an unmined funding tree is accepted with nothing admitted, and counted as none', async () => {
  const host = new FakeHost()
  const t = byName(v, 'funding-tree-unmined')
  const got = await run(manager({ open: true }, async () => undefined, host), beefOf(t), [])
  assert.deepEqual(got.ok, { outputsToAdmit: [], coinsToRetain: [] })
  assert.equal(host.count('blogs_admitted_total', { kind: 'none' }), 1)
})

test('a sweep spending held funding outputs admits its tombstone and retains what it spends', async () => {
  const host = new FakeHost()
  const t = byName(v, 'sweep')
  const { previousCoins, source } = coinsOf(t)
  const got = await run(manager({ open: true }, source, host), beefOf(t), previousCoins)
  assert.deepEqual(got.ok, { outputsToAdmit: [0], coinsToRetain: previousCoins })
  assert.equal(host.count('blogs_admitted_total', { kind: 'funding' }), 1)
})

/**
 * A transaction whose one output carries the byte string as a batch record
 * (a carrier) or as an anchor record (a token). Its input spends a parent
 * carried in its BEEF, of the same value, so carrier-shape passes and the
 * record's own rules are the first to decide.
 */
function carrying(kind: string, record: Uint8Array): number[] {
  const key = fromHex(v.identityKey)
  const sig = Uint8Array.of(0x30, 0x06, 0x02, 0x01, 0x01, 0x02, 0x01, 0x01)
  const fields = kind === 'batch' ? [record] : [TagAnchor, record]
  const parent = new Transaction(1, [], [{ lockingScript: LockingScript.fromBinary([0x51]), satoshis: 1 }], 0)
  const tx = new Transaction(
    1,
    [{ sourceTransaction: parent, sourceOutputIndex: 0, unlockingScript: UnlockingScript.fromBinary([]), sequence: 0 }],
    [{ lockingScript: LockingScript.fromBinary(Array.from(pushDropScript(key, fields, sig))), satoshis: 1 }],
    4102444800,
  )
  return tx.toBEEF()
}

test('the 53 record refusal vectors inside transactions: refused for the record reason, or skipped', async () => {
  const r = readVectors<{ refusals: Array<{ name: string; kind: string; hex: string; reason: Reason; atAdmission?: string }> }>('refusal-v1.json')
  assert.equal(r.refusals.length, 53)
  for (const x of r.refusals) {
    const host = new FakeHost()
    const got = await run(manager({ open: true }, async () => undefined, host), carrying(x.kind, fromHex(x.hex)), [])
    if (x.kind === 'batch' && x.atAdmission === 'skip') {
      assert.deepEqual(got.ok, { outputsToAdmit: [], coinsToRetain: [] }, x.name)
      assert.equal(host.count('blogs_admitted_total', { kind: 'none' }), 1, `${x.name}: claims nothing`)
      continue
    }
    assert.deepEqual(got.ok, { outputsToAdmit: [], coinsToRetain: [] }, `${x.name}: recorded`)
    assert.equal(host.count('blogs_refused_total', { reason: x.reason, recorded: 'yes' }), 1, `${x.name}: ${x.reason}`)
    exercised.add(x.reason)
  }
})

test('a token whose anchor record has the wrong magic is refused magic (a batch record with it only fails to claim)', async () => {
  const r = readVectors<{ refusals: Array<{ name: string; hex: string }> }>('refusal-v1.json')
  const b = readVectors<{ batches: Array<{ record: string }> }>('batch-v1.json')
  for (const hex of [r.refusals.find((x) => x.name === 'wrong-magic')!.hex, b.batches[0]!.record]) {
    const host = new FakeHost()
    const got = await run(manager({ open: true }, async () => undefined, host), carrying('anchor', fromHex(hex)), [])
    assert.deepEqual(got.ok, { outputsToAdmit: [], coinsToRetain: [] })
    assert.equal(host.count('blogs_refused_total', { reason: 'magic', recorded: 'yes' }), 1)
  }
  exercised.add('magic')
})

test('every refusal reason of the spec is exercised at module level', () => {
  assert.deepEqual([...exercised].sort(), [...Reasons].sort())
})

test('a stream the grammar refuses, or an empty producer set, cannot be mounted', () => {
  assert.throws(() => new LogTopicManager('tm_log_app', { open: true }, async () => undefined, new FakeHost()))
  assert.throws(() => new LogTopicManager('tm_other_app_qzxkvbmwtr', { open: true }, async () => undefined, new FakeHost()))
  assert.throws(() => new LogTopicManager(v.topic, { producers: [] }, async () => undefined, new FakeHost()))
})
