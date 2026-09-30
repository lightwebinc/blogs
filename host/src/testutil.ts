/**
 * Test helpers for the host module: a module host that records what the
 * module counts and logs, the transaction vectors, and an in-memory store
 * implementing the overlay engine's Storage interface, so that the real
 * engine runs in a test.
 */
import { readFileSync } from 'node:fs'
import { Transaction, type ChainTracker } from '@bsv/sdk'
import type { ModuleHost, Output } from '@lightwebinc/bcommon'
import { fromHex, outpointKey, type Coin } from './admit.js'

export class FakeHost implements ModuleHost {
  readonly counters = new Map<string, number>()
  readonly gauges = new Map<string, () => number>()
  readonly lines: Array<{ msg: string; extra?: Record<string, unknown> }> = []
  readonly log = (msg: string, extra?: Record<string, unknown>): void => {
    this.lines.push({ msg, extra })
  }
  readonly metrics = {
    inc: (name: string, labels?: Record<string, string>, by = 1): void => {
      const k = FakeHost.key(name, labels)
      this.counters.set(k, (this.counters.get(k) ?? 0) + by)
    },
    preset: (name: string, labels?: Record<string, string>): void => {
      const k = FakeHost.key(name, labels)
      if (!this.counters.has(k)) this.counters.set(k, 0)
    },
    gauge: (name: string, fn: () => number): void => {
      this.gauges.set(name, fn)
    },
  }
  static key(name: string, labels?: Record<string, string>): string {
    const l = Object.entries(labels ?? {})
      .sort(([a], [b]) => (a < b ? -1 : 1))
      .map(([k, v]) => `${k}="${v}"`)
      .join(',')
    return l === '' ? name : `${name}{${l}}`
  }
  count(name: string, labels?: Record<string, string>): number {
    return this.counters.get(FakeHost.key(name, labels)) ?? 0
  }
}

export interface TxVector {
  name: string
  policy: 'open' | string[]
  txid: string
  beef: string
  previousCoins: Array<{ txid: string; vout: number; lockingScript: string; satoshis: number }>
  verdict: 'admit' | 'refuse'
  reason?: string
  admits?: { kind: string; outputIndex?: number; funding?: number[]; interval?: number; seq?: number; commitment?: string }
}

export interface TxVectors {
  stream: string
  topic: string
  identityKey: string
  privateKey: string
  headers: Array<{ height: number; merkleRoot: string }>
  transactions: TxVector[]
}

const vectors = new URL('../../testdata/vectors/', import.meta.url)

export function readVectors<T>(name: string): T {
  return JSON.parse(readFileSync(new URL(name, vectors), 'utf8')) as T
}

export const txVectors = (): TxVectors => readVectors<TxVectors>('transaction-v1.json')

export function byName(v: TxVectors, name: string): TxVector {
  const t = v.transactions.find((x) => x.name === name)
  if (t === undefined) throw new Error(`no transaction vector ${name}`)
  return t
}

/** The vector headers as a chain tracker. */
export function vectorHeaders(v: TxVectors): ChainTracker {
  const roots = new Map(v.headers.map((h) => [h.height, h.merkleRoot]))
  return {
    isValidRootForHeight: async (root, height) => roots.get(height) === root,
    currentHeight: async () => Math.max(...roots.keys()),
  }
}

export const beefOf = (t: TxVector): number[] => Array.from(fromHex(t.beef))
export const txOf = (t: TxVector): Transaction => Transaction.fromBEEF(beefOf(t))

/** The vector's previous coins as the engine hands them: input indexes, and a source for their outputs. */
export function coinsOf(t: TxVector): { previousCoins: number[]; source: (topic: string, txid: string, vout: number) => Promise<Coin | undefined> } {
  const held = new Map(t.previousCoins.map((c) => [outpointKey(c.txid, c.vout), { lockingScript: fromHex(c.lockingScript), satoshis: c.satoshis }]))
  const tx = txOf(t)
  const previousCoins: number[] = []
  tx.inputs.forEach((input, i) => {
    const src = input.sourceTXID ?? input.sourceTransaction?.id('hex')
    if (src !== undefined && held.has(outpointKey(src, input.sourceOutputIndex))) previousCoins.push(i)
  })
  return { previousCoins, source: async (_topic, txid, vout) => held.get(outpointKey(txid, vout)) }
}

/**
 * The engine's Storage interface in memory, as the engine's own Knex store
 * behaves where the engine depends on it: findOutput matches spent and
 * unspent outputs unless `spent` is given, and returns the stored BEEF only
 * when asked; findUTXOsForTopic returns unspent outputs.
 */
export class MemoryStorage {
  readonly outputs = new Map<string, Output>()
  readonly applied = new Set<string>()
  readonly interactions = new Map<string, number>()

  private static k(txid: string, outputIndex: number, topic: string): string {
    return `${topic}|${txid}.${outputIndex}`
  }

  private static copy(o: Output, beef: boolean): Output {
    const c: Output = {
      ...o,
      outputScript: [...o.outputScript],
      outputsConsumed: o.outputsConsumed.map((x) => ({ ...x })),
      consumedBy: o.consumedBy.map((x) => ({ ...x })),
    }
    if (!beef) delete c.beef
    return c
  }

  async insertOutput(o: Output): Promise<void> {
    this.outputs.set(MemoryStorage.k(o.txid, o.outputIndex, o.topic), MemoryStorage.copy(o, true))
  }

  async findOutput(txid: string, outputIndex: number, topic?: string, spent?: boolean, includeBEEF = false): Promise<Output | null> {
    for (const o of this.outputs.values()) {
      if (o.txid !== txid || o.outputIndex !== outputIndex) continue
      if (topic !== undefined && o.topic !== topic) continue
      if (spent !== undefined && o.spent !== spent) continue
      return MemoryStorage.copy(o, includeBEEF)
    }
    return null
  }

  async findOutputsForTransaction(txid: string, includeBEEF = false): Promise<Output[]> {
    return [...this.outputs.values()].filter((o) => o.txid === txid).map((o) => MemoryStorage.copy(o, includeBEEF))
  }

  async findUTXOsForTopic(topic: string, _since?: number, _limit?: number, includeBEEF = false): Promise<Output[]> {
    return [...this.outputs.values()].filter((o) => o.topic === topic && !o.spent).map((o) => MemoryStorage.copy(o, includeBEEF))
  }

  async deleteOutput(txid: string, outputIndex: number, topic: string): Promise<void> {
    this.outputs.delete(MemoryStorage.k(txid, outputIndex, topic))
  }

  async markUTXOAsSpent(txid: string, outputIndex: number, topic: string): Promise<void> {
    const o = this.outputs.get(MemoryStorage.k(txid, outputIndex, topic))
    if (o !== undefined) o.spent = true
  }

  async updateConsumedBy(txid: string, outputIndex: number, topic: string, consumedBy: Array<{ txid: string; outputIndex: number }>): Promise<void> {
    const o = this.outputs.get(MemoryStorage.k(txid, outputIndex, topic))
    if (o !== undefined) o.consumedBy = consumedBy.map((x) => ({ ...x }))
  }

  async updateTransactionBEEF(txid: string, beef: number[]): Promise<void> {
    for (const o of this.outputs.values()) if (o.txid === txid) o.beef = [...beef]
  }

  async insertAppliedTransaction(tx: { txid: string; topic: string }): Promise<void> {
    this.applied.add(`${tx.topic}|${tx.txid}`)
  }

  async doesAppliedTransactionExist(tx: { txid: string; topic: string }): Promise<boolean> {
    return this.applied.has(`${tx.topic}|${tx.txid}`)
  }

  async updateLastInteraction(host: string, topic: string, since: number): Promise<void> {
    this.interactions.set(`${host}|${topic}`, since)
  }

  async getLastInteraction(host: string, topic: string): Promise<number> {
    return this.interactions.get(`${host}|${topic}`) ?? 0
  }
}

/** A console that keeps what the engine logs out of the test output. */
export function quietConsole(): typeof console & { errors: unknown[][] } {
  const errors: unknown[][] = []
  const c = Object.create(console) as typeof console & { errors: unknown[][] }
  c.errors = errors
  c.log = () => {}
  c.info = () => {}
  c.warn = () => {}
  c.debug = () => {}
  c.error = (...a: unknown[]) => {
    errors.push(a)
  }
  return c
}
