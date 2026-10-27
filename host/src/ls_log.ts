/**
 * `ls_log`: one lookup service for every stream a host carries (spec
 * sections 7.2 and 8.2 to 8.5).
 *
 * It indexes what the topic managers admitted, per (producer, stream):
 * batches by seq (at most 8 candidates per seq), anchor tokens by interval
 * (at most 8 per interval), and, per outpoint, the transactions that spent
 * it, read from each admitted transaction's own inputs. Every status (a
 * batch's `retracted`, `fork`, `gap` or `linked`; a token's place in the
 * canonical chain; an interval's completeness) is a function of that set,
 * computed when asked, so arrival order and restart order never change it.
 *
 * It answers exactly the three free question classes, `{stream, producer,
 * seq}`, `{stream, producer, interval}` and `{stream, tree}`, and refuses
 * every other query with an error. Statuses are reported by `report`, the
 * metrics, and the log, never inside an answer: the answer is the signed
 * contract's output list.
 *
 * The tree class answers a mined funding tree by its txid, from the funding
 * outputs the topic holds: every admitted funding output is retained once
 * spent (spec section 8.1), so a tree whose outputs are all spent, which a
 * pruning node no longer serves, is still answered here with its proof.
 *
 * With a retention policy (retention.ts), proven carriers older than the
 * policy are dropped from the index and from storage, a stream's oldest
 * intervals first; what was dropped is recorded in a state file so that a
 * restart neither counts those positions as holes nor indexes a dropped
 * carrier again.
 */
import { MerklePath, Transaction } from '@bsv/sdk'
import type {
  LookupFormula,
  LookupQuestion,
  LookupService,
  LookupServiceMetaData,
  ModuleHost,
  Output,
  OutputAdmittedByTopic,
  RestoreStorage,
} from '@lightwebinc/bcommon'
import { fromHex, outpointKey, toHex, type Coin } from './admit.js'
import {
  LookupService as LookupServiceName,
  MaxSafe,
  TagAnchor,
  TopicPrefix,
  checkProducer,
  checkStream,
  decodeAnchor,
  decodeBatch,
  isBatch,
  type Anchor,
} from './logrec.js'
import { intervalRoot } from './merkle.js'
import type { Mark, Retention } from './retention.js'
import { bytesEq, isFundingShape, readPushDrop } from './script.js'

/** Candidates kept and answered per seq, and anchor tokens per interval (spec section 11). */
export const MaxCandidates = 8

export type BatchStatus = 'retracted' | 'fork' | 'gap' | 'linked'
export type TokenStatus = 'canonical' | 'anchor-branch' | 'anchor-join' | 'pending'
export type IntervalStatus = 'complete' | 'incomplete' | 'root-mismatch'

/** One admitted batch carrier, or its receipt once retracted. */
interface BatchEntry {
  seq: number
  /** The previous batch's commitment, hash byte order, hex. */
  prev: string
  /** C, hash byte order. */
  c: Uint8Array
  /** C, hash byte order, hex: the candidate order. */
  cKey: string
  /** The carrier's txid, display hex. */
  txid: string
  outputIndex: number
  /** The funding outpoint the carrier spends (outpointKey). */
  funding: string
}

interface TokenEntry {
  anchor: Anchor
  /** Display hex. */
  txid: string
  /** Hash byte order, hex: the order among tokens of one interval. */
  txidKey: string
  outputIndex: number
  outpoint: string
  /** Block height and position in the block; Infinity when the proof is unknown. */
  height: number
  index: number
  /** The outpoints the token's transaction spends. */
  inputs: string[]
}

class Stream {
  readonly batches = new Map<number, BatchEntry[]>()
  readonly tokens = new Map<number, TokenEntry[]>()
  highest = 0
  /** Dropped by retention: intervals 1 to retiredInterval, seqs 1 to retiredSeq; retiredC is C at retiredSeq (hex). */
  retiredInterval = 0
  retiredSeq = 0
  retiredC = ''
  /** Cached canonical chain, index k - 1 for interval k; cleared on any token change. */
  chain: { tokens: TokenEntry[]; joinFailed: Set<string> } | undefined
  constructor(
    readonly producer: string,
    readonly stream: string,
  ) {}
}

export interface IntervalReport {
  interval: number
  first: number
  count: number
  txid: string
  status: IntervalStatus
  /** Seqs the interval covers that are held by no batch and no receipt. */
  missing: number[]
  /** Complete when retention dropped its carriers; nothing of it is held now but its token. */
  retired?: boolean
}

export interface StreamReport {
  producer: string
  stream: string
  /** The highest seq held. */
  highest: number
  /** Seqs from 1 to highest with no batch and no receipt, as inclusive ranges. */
  holes: Array<[number, number]>
  /** Seqs with the status fork. */
  forks: number[]
  /** Seqs whose every candidate is retracted. */
  retracted: number[]
  /** The canonical chain's intervals, in order. */
  intervals: IntervalReport[]
  /** Tokens outside the canonical chain, with why. */
  tokens: Array<{ interval: number; txid: string; status: Exclude<TokenStatus, 'canonical'> }>
}

/** What restore may ask storage for, with the engine's optional BEEF flag, and the delete retention uses. */
interface FullStorage extends RestoreStorage {
  findOutput: (txid: string, outputIndex: number, topic?: string, spent?: boolean, includeBEEF?: boolean) => Promise<Output | null>
  deleteOutput?: (txid: string, outputIndex: number, topic: string) => Promise<void>
}

/** What a retention pass did. */
export interface RetentionPass {
  /** Intervals whose carriers were dropped. */
  intervals: number
  /** Carriers dropped from the index. */
  carriers: number
  /** Carrier outputs deleted from storage, this pass's and any left from before. */
  deleted: number
}

const compareKey = (a: string, b: string): number => (a < b ? -1 : a > b ? 1 : 0)
const hashKey = (display: string): string => toHex(fromHex(display).reverse())

function position(tx: Transaction, txid: string): { height: number; index: number } {
  const p: MerklePath | undefined = tx.merklePath
  const leaf = p?.path[0]?.find((l) => l.hash === txid)
  if (p === undefined || leaf === undefined) return { height: Infinity, index: Infinity }
  return { height: p.blockHeight, index: leaf.offset }
}

function inputsOf(tx: Transaction): string[] {
  const out: string[] = []
  for (const input of tx.inputs) {
    const src = input.sourceTXID ?? input.sourceTransaction?.id('hex')
    if (src !== undefined) out.push(outpointKey(src, input.sourceOutputIndex))
  }
  return out
}

export class LogLookupService implements LookupService {
  /** The whole transaction: a carrier's funding outpoint and every spend are its inputs. */
  readonly admissionMode = 'whole-tx' as const
  /** Spends are read from admitted transactions, never from the engine's notices. */
  readonly spendNotificationMode = 'none' as const

  /** producer hex | stream -> index. */
  private readonly streams = new Map<string, Stream>()
  /** Outpoint -> what was indexed there, for eviction; an outpoint may be held on several topics. */
  private readonly byOutpoint = new Map<string, Array<{ topic: string; kind: 'batch' | 'token' | 'coin'; stream?: Stream }>>()
  /** topic | outpoint -> the admitted output, for the topic manager's previous coins. */
  private readonly held = new Map<string, Coin>()
  /** topic | txid -> the funding outputs the topic holds of that transaction, for the tree class. */
  private readonly trees = new Map<string, Set<number>>()
  /** Funding outpoint -> the carriers (display txid) that spend it. */
  private readonly carriersByFunding = new Map<string, Set<string>>()
  /** Outpoint -> txid -> whether that spender is mined. */
  private readonly spenders = new Map<string, Map<string, boolean>>()
  /** Carrier txids retracted by a mined spend of their funding outpoint. */
  private readonly retracted = new Set<string>()
  private readonly noted = new Set<string>()
  private forkSeqs = 0
  private storage: FullStorage | undefined
  /** producer | stream -> the retained prefix's mark, from the state file. */
  private readonly marks = new Map<string, Mark>()
  /** topic | tree txid -> the lowest funding output whose carrier retention dropped. */
  private readonly treeLow = new Map<string, number>()
  /** Carrier outputs to delete from storage. */
  private drops: Array<{ txid: string; vout: number; topic: string }> = []
  private restoring = false
  /** The marks or treeLow changed since the state file was last written. */
  private stateDirty = false
  private readonly warned = new Set<string>()

  constructor(
    private readonly topics: ReadonlySet<string>,
    private readonly host: ModuleHost,
    private readonly retention?: Retention,
  ) {
    if (retention !== undefined) {
      const st = retention.store.load()
      for (const m of st.marks) this.marks.set(`${m.producer}|${m.stream}`, m)
      for (const [k, v] of st.trees) this.treeLow.set(k, v)
    }
  }

  // ---- the topic manager's previous coins

  /**
   * The output the topic holds at an outpoint: from the engine's storage
   * once restore has handed it over (the host calls restore before it
   * accepts a submission), otherwise from what this service indexed.
   */
  readonly coin = async (topic: string, txid: string, vout: number): Promise<Coin | undefined> => {
    if (this.storage !== undefined) {
      const o = await this.storage.findOutput(txid, vout, topic)
      return o === null ? undefined : { lockingScript: Uint8Array.from(o.outputScript), satoshis: o.satoshis }
    }
    return this.held.get(`${topic}|${outpointKey(txid, vout)}`)
  }

  // ---- gauges

  get streamCount(): number {
    return this.streams.size
  }

  get batchCount(): number {
    let n = 0
    for (const s of this.streams.values()) for (const l of s.batches.values()) n += l.length
    return n
  }

  get tokenCount(): number {
    let n = 0
    for (const s of this.streams.values()) for (const l of s.tokens.values()) n += l.length
    return n
  }

  get retractedCount(): number {
    return this.retracted.size
  }

  /** Seqs below each stream's highest that nothing holds and retention did not drop. */
  get missingCount(): number {
    let n = 0
    for (const s of this.streams.values()) n += Math.max(0, s.highest - s.retiredSeq - s.batches.size)
    return n
  }

  /** Seqs whose carriers retention dropped, over every stream. */
  get retiredCount(): number {
    let n = 0
    for (const m of this.marks.values()) n += m.seq
    return n
  }

  /** Seqs holding more than one candidate. */
  get forkSeqCount(): number {
    return this.forkSeqs
  }

  // ---- admission

  outputAdmittedByTopic(payload: OutputAdmittedByTopic): void {
    if (payload.mode !== 'whole-tx' || !this.topics.has(payload.topic)) return
    let tx: Transaction
    try {
      tx = Transaction.fromAtomicBEEF(payload.atomicBEEF)
    } catch {
      this.host.log('ls_log ignored an admission whose BEEF did not parse', { topic: payload.topic })
      return
    }
    if (this.index(payload.topic, tx, payload.outputIndex)) this.noteSpends(tx)
  }

  outputEvicted(txid: string, outputIndex: number): void {
    this.drop(outpointKey(txid, outputIndex))
  }

  outputNoLongerRetainedInHistory(txid: string, outputIndex: number, topic: string): void {
    this.drop(outpointKey(txid, outputIndex), topic)
  }

  /**
   * Index one admitted output of tx on topic. False for a carrier at a
   * position retention has dropped: it is not indexed, and its output is
   * deleted from storage at the next pass.
   */
  private index(topic: string, tx: Transaction, outputIndex: number): boolean {
    const out = tx.outputs[outputIndex]
    if (out === undefined) return true
    const txid = tx.id('hex')
    const script = Uint8Array.from(out.lockingScript.toBinary())
    const op = outpointKey(txid, outputIndex)
    const stream = topic.slice(TopicPrefix.length)
    const pd = readPushDrop(script)
    if (pd !== undefined && isBatch(pd.fields[0]!)) {
      const b = decodeBatch(pd.fields[0]!)
      if (b.stream !== stream) return true
      const funding = inputsOf(tx)[0]
      if (funding === undefined) return true
      const s = this.streamOf(toHex(b.producer), b.stream)
      if (b.seq <= s.retiredSeq) {
        this.drops.push({ txid, vout: outputIndex, topic })
        return false
      }
      const c = Uint8Array.from(tx.hash() as number[])
      // A carrier output is never a valid anchor input; it is held with an
      // empty script so that rule 7 refuses an anchor that spends it.
      this.hold(topic, op, { lockingScript: new Uint8Array(0), satoshis: out.satoshis ?? 0 })
      this.addBatch(topic, s, { seq: b.seq, prev: toHex(b.prev), c, cKey: toHex(c), txid, outputIndex, funding })
      return true
    }
    this.hold(topic, op, { lockingScript: script, satoshis: out.satoshis ?? 0 })
    if (pd !== undefined && pd.fields.length === 3 && bytesEq(pd.fields[0]!, TagAnchor)) {
      const a = decodeAnchor(pd.fields[1]!)
      if (a.stream !== stream) return true
      const s = this.streamOf(toHex(a.producer), a.stream)
      const at = position(tx, txid)
      this.addToken(topic, s, { anchor: a, txid, txidKey: hashKey(txid), outputIndex, outpoint: op, ...at, inputs: inputsOf(tx) })
      return true
    }
    // A funding output: held so that a later spend of it can be read, and
    // so that the tree class can answer its transaction.
    if (!isFundingShape(script)) {
      this.host.log('ls_log indexed an output of no known kind', { topic, outpoint: op })
      return true
    }
    const k = `${topic}|${txid}`
    let vouts = this.trees.get(k)
    if (vouts === undefined) this.trees.set(k, (vouts = new Set()))
    vouts.add(outputIndex)
    return true
  }

  private hold(topic: string, op: string, coin: Coin): void {
    this.held.set(`${topic}|${op}`, coin)
    this.ref(op, { topic, kind: 'coin' })
  }

  private ref(op: string, r: { topic: string; kind: 'batch' | 'token' | 'coin'; stream?: Stream }): void {
    const l = this.byOutpoint.get(op)
    if (l === undefined) this.byOutpoint.set(op, [r])
    else if (!l.some((x) => x.topic === r.topic && x.kind === r.kind)) l.push(r)
  }

  private streamOf(producer: string, stream: string): Stream {
    const k = `${producer}|${stream}`
    let s = this.streams.get(k)
    if (s === undefined) {
      this.streams.set(k, (s = new Stream(producer, stream)))
      const m = this.marks.get(k)
      if (m !== undefined) {
        s.retiredInterval = m.interval
        s.retiredSeq = m.seq
        s.retiredC = m.c
        s.highest = m.seq
      }
    }
    return s
  }

  private addBatch(topic: string, s: Stream, e: BatchEntry): void {
    const list = s.batches.get(e.seq) ?? []
    if (list.some((x) => x.cKey === e.cKey)) return
    list.push(e)
    list.sort((a, b) => compareKey(a.cKey, b.cKey))
    if (list.length > MaxCandidates) {
      const cut = list.pop()!
      this.host.metrics.inc('blogs_candidates_dropped_total')
      this.host.log('ls_log dropped a fork candidate over the cap', { stream: s.stream, seq: e.seq, txid: cut.txid })
      if (cut === e) return
      this.forget(cut)
    }
    if (list.length === 2) this.forkSeqs++
    s.batches.set(e.seq, list)
    if (e.seq > s.highest) s.highest = e.seq
    this.ref(outpointKey(e.txid, e.outputIndex), { topic, kind: 'batch', stream: s })
    let set = this.carriersByFunding.get(e.funding)
    if (set === undefined) this.carriersByFunding.set(e.funding, (set = new Set()))
    set.add(e.txid)
    this.checkRetracted(e.funding)
  }

  private forget(e: BatchEntry): void {
    const set = this.carriersByFunding.get(e.funding)
    set?.delete(e.txid)
    if (set?.size === 0) this.carriersByFunding.delete(e.funding)
    this.retracted.delete(e.txid)
  }

  private addToken(topic: string, s: Stream, t: TokenEntry): void {
    const k = t.anchor.interval
    const list = s.tokens.get(k) ?? []
    if (list.some((x) => x.txid === t.txid)) return
    list.push(t)
    s.tokens.set(k, list)
    s.chain = undefined
    this.ref(t.outpoint, { topic, kind: 'token', stream: s })
    if (list.length > MaxCandidates) {
      // Canonical first, then by txid in hash byte order: the last goes.
      const canonical = this.chainOf(s).tokens[k - 1]
      list.sort((a, b) => (a === canonical ? -1 : b === canonical ? 1 : compareKey(a.txidKey, b.txidKey)))
      const cut = list.pop()!
      s.chain = undefined
      this.host.metrics.inc('blogs_tokens_dropped_total')
      this.host.log('ls_log dropped an anchor token over the cap', { stream: s.stream, interval: k, txid: cut.txid })
    }
  }

  /**
   * Record every input of an admitted transaction as a spend. Only a mined
   * spend retracts: a carrier never mines, and an unmined spend of a funding
   * output may never mine either (spec section 8.4).
   */
  private noteSpends(tx: Transaction): void {
    const txid = tx.id('hex')
    if (this.noted.has(txid)) return
    this.noted.add(txid)
    const mined = tx.merklePath !== undefined
    for (const op of inputsOf(tx)) {
      let m = this.spenders.get(op)
      if (m === undefined) this.spenders.set(op, (m = new Map()))
      m.set(txid, mined || m.get(txid) === true)
      if (mined) this.checkRetracted(op)
    }
  }

  private checkRetracted(funding: string): void {
    const carriers = this.carriersByFunding.get(funding)
    const spent = this.spenders.get(funding)
    if (carriers === undefined || spent === undefined) return
    for (const c of carriers) {
      if (this.retracted.has(c)) continue
      for (const [by, mined] of spent) {
        if (!mined || by === c) continue
        this.retracted.add(c)
        this.host.metrics.inc('blogs_retractions_total')
        this.host.log('ls_log retracted a batch', { carrier: c, funding, by })
        break
      }
    }
  }

  private drop(op: string, topic?: string): void {
    const refs = this.byOutpoint.get(op)
    if (refs === undefined) return
    const keep = refs.filter((r) => topic !== undefined && r.topic !== topic)
    for (const r of refs) {
      if (topic !== undefined && r.topic !== topic) continue
      this.held.delete(`${r.topic}|${op}`)
      if (r.kind === 'coin') this.dropTree(r.topic, op)
      if (r.kind === 'batch' && r.stream !== undefined) this.dropBatch(r.stream, op)
      if (r.kind === 'token' && r.stream !== undefined) this.dropToken(r.stream, op)
    }
    if (keep.length === 0) this.byOutpoint.delete(op)
    else this.byOutpoint.set(op, keep)
  }

  private dropTree(topic: string, op: string): void {
    const dot = op.lastIndexOf('.')
    const k = `${topic}|${op.slice(0, dot)}`
    const vouts = this.trees.get(k)
    if (vouts === undefined) return
    vouts.delete(Number(op.slice(dot + 1)))
    if (vouts.size === 0) this.trees.delete(k)
  }

  /** How many funding trees (and sweeps, by their tombstones) the tree class can answer. */
  get treeCount(): number {
    return this.trees.size
  }

  private dropBatch(s: Stream, op: string): void {
    for (const [seq, list] of s.batches) {
      const i = list.findIndex((e) => outpointKey(e.txid, e.outputIndex) === op)
      if (i < 0) continue
      this.forget(list[i]!)
      list.splice(i, 1)
      if (list.length === 1) this.forkSeqs--
      if (list.length === 0) {
        s.batches.delete(seq)
        if (seq === s.highest) {
          s.highest = s.retiredSeq
          for (const k of s.batches.keys()) if (k > s.highest) s.highest = k
        }
      }
      return
    }
  }

  private dropToken(s: Stream, op: string): void {
    for (const [k, list] of s.tokens) {
      const i = list.findIndex((t) => t.outpoint === op)
      if (i < 0) continue
      list.splice(i, 1)
      if (list.length === 0) s.tokens.delete(k)
      s.chain = undefined
      return
    }
  }

  // ---- statuses

  private isRetracted(e: BatchEntry): boolean {
    return this.retracted.has(e.txid)
  }

  /** A batch's status (spec section 8.2): the first of the table that applies. */
  private batchStatus(s: Stream, e: BatchEntry): BatchStatus {
    if (this.isRetracted(e)) return 'retracted'
    if ((s.batches.get(e.seq) ?? []).some((o) => o.cKey !== e.cKey)) return 'fork'
    if (e.seq === 1) return 'linked'
    if (e.seq - 1 === s.retiredSeq) return e.prev === s.retiredC ? 'linked' : 'fork'
    const before = s.batches.get(e.seq - 1)
    if (before === undefined || before.length === 0) return 'gap'
    return before.some((p) => p.cKey === e.prev) ? 'linked' : 'fork'
  }

  /**
   * The canonical chain (spec section 8.3): interval 1's token mined first,
   * then for each k the token that spends the canonical k - 1 token, if its
   * first continues the predecessor's. A token that fails that join is kept
   * and reported, and the chain stops before it.
   */
  private chainOf(s: Stream): { tokens: TokenEntry[]; joinFailed: Set<string> } {
    if (s.chain !== undefined) return s.chain
    const tokens: TokenEntry[] = []
    const joinFailed = new Set<string>()
    const firsts = [...(s.tokens.get(1) ?? [])].sort((a, b) => a.height - b.height || a.index - b.index || compareKey(a.txidKey, b.txidKey))
    let pred = firsts[0]
    while (pred !== undefined) {
      tokens.push(pred)
      const p = pred
      const next = (s.tokens.get(p.anchor.interval + 1) ?? []).find((t) => t.inputs.includes(p.outpoint))
      if (next === undefined) break
      if (next.anchor.first !== p.anchor.first + p.anchor.count) {
        joinFailed.add(next.txid)
        break
      }
      pred = next
    }
    s.chain = { tokens, joinFailed }
    return s.chain
  }

  private tokenStatus(s: Stream, t: TokenEntry): TokenStatus {
    const { tokens, joinFailed } = this.chainOf(s)
    if (tokens[t.anchor.interval - 1] === t) return 'canonical'
    if (joinFailed.has(t.txid)) return 'anchor-join'
    if (t.anchor.interval > 1) {
      const prev = toHex(t.anchor.prev)
      const held = (s.tokens.get(t.anchor.interval - 1) ?? []).some((p) => p.txidKey === prev)
      if (!held) return 'pending'
    }
    return 'anchor-branch'
  }

  /**
   * An interval's completeness (spec section 8.3): every covered seq held by
   * a batch or a receipt, and one choice of candidates whose root is the
   * token's. Each choice starts from a candidate at the last seq and follows
   * prev back; a position with one candidate that prev does not select is
   * tried anyway, so the root, not the link, decides.
   */
  private intervalReport(s: Stream, t: TokenEntry): IntervalReport {
    return this.intervalChoice(s, t).report
  }

  /** The interval's report, and when it is complete the candidates whose root is the token's, in seq order. */
  private intervalChoice(s: Stream, t: TokenEntry): { report: IntervalReport; chosen?: BatchEntry[] } {
    const a = t.anchor
    const last = a.first + a.count - 1
    const base = { interval: a.interval, first: a.first, count: a.count, txid: t.txid }
    if (a.interval <= s.retiredInterval) return { report: { ...base, status: 'complete', missing: [], retired: true } }
    const missing: number[] = []
    for (let seq = a.first; seq <= last; seq++) if ((s.batches.get(seq)?.length ?? 0) === 0) missing.push(seq)
    if (missing.length > 0) return { report: { ...base, status: 'incomplete', missing } }
    for (const end of s.batches.get(last)!) {
      const chosen: BatchEntry[] = [end]
      for (let seq = last - 1; seq >= a.first; seq--) {
        const cands = s.batches.get(seq)!
        const pick = cands.find((c) => c.cKey === chosen[0]!.prev) ?? (cands.length === 1 ? cands[0] : undefined)
        if (pick === undefined) break
        chosen.unshift(pick)
      }
      if (chosen.length === a.count && bytesEq(intervalRoot(chosen.map((e) => e.c)), a.root)) return { report: { ...base, status: 'complete', missing }, chosen }
    }
    return { report: { ...base, status: 'root-mismatch', missing } }
  }

  /** Statuses of every (producer, stream) held, or of one stream. */
  report(stream?: string): StreamReport[] {
    const out: StreamReport[] = []
    for (const s of this.streams.values()) {
      if (stream !== undefined && s.stream !== stream) continue
      const holes: Array<[number, number]> = []
      const forks: number[] = []
      const retracted: number[] = []
      // Over the held seqs only: a producer may number a batch 2^53 - 1.
      // Seqs retention dropped are not holes.
      let last = s.retiredSeq
      for (const seq of [...s.batches.keys()].sort((a, b) => a - b)) {
        if (seq > last + 1) holes.push([last + 1, seq - 1])
        last = seq
        const statuses = s.batches.get(seq)!.map((e) => this.batchStatus(s, e))
        if (statuses.includes('fork')) forks.push(seq)
        if (statuses.every((x) => x === 'retracted')) retracted.push(seq)
      }
      const { tokens } = this.chainOf(s)
      const others: StreamReport['tokens'] = []
      for (const list of s.tokens.values()) {
        for (const t of list) {
          const st = this.tokenStatus(s, t)
          if (st !== 'canonical') others.push({ interval: t.anchor.interval, txid: t.txid, status: st })
        }
      }
      out.push({
        producer: s.producer,
        stream: s.stream,
        highest: s.highest,
        holes,
        forks,
        retracted,
        intervals: tokens.map((t) => this.intervalReport(s, t)),
        tokens: others.sort((x, y) => x.interval - y.interval || compareKey(x.txid, y.txid)),
      })
    }
    return out
  }

  /** The status of the batch with commitment C (display txid) on a stream, if held. */
  batchStatusOf(producer: string, stream: string, txid: string): BatchStatus | undefined {
    const s = this.streams.get(`${producer}|${stream}`)
    if (s === undefined) return undefined
    for (const list of s.batches.values()) {
      const e = list.find((x) => x.txid === txid)
      if (e !== undefined) return this.batchStatus(s, e)
    }
    return undefined
  }

  // ---- questions

  async lookup(question: LookupQuestion): Promise<LookupFormula> {
    if (question.service !== LookupServiceName) throw new Error(`ls_log: this service is ${LookupServiceName}, not ${String(question.service)}`)
    const q = parseQuery(question.query)
    if ('tree' in q) {
      // One output answers the whole transaction: the engine hydrates it as
      // the tree's BEEF, with the proof it was admitted with.
      const topic = `${TopicPrefix}${q.stream}`
      const k = `${topic}|${q.tree}`
      const held = [...(this.trees.get(k) ?? [])]
      const low = this.treeLow.get(k)
      if (low !== undefined) held.push(low)
      if (held.length > 0) return [{ txid: q.tree, outputIndex: Math.min(...held) }]
      // A tree whose every carrier retention dropped is no longer in the
      // index; its outputs are still in storage, and output 0 is the lowest
      // there can be (a lower one dropped is in treeLow).
      if (this.retention !== undefined && this.storage !== undefined) {
        const o = await this.storage.findOutput(q.tree, 0, topic)
        if (o !== null && isFundingShape(Uint8Array.from(o.outputScript))) return [{ txid: q.tree, outputIndex: 0 }]
      }
      return []
    }
    const s = this.streams.get(`${q.producer}|${q.stream}`)
    if (s === undefined) return []
    if ('seq' in q) {
      return (s.batches.get(q.seq) ?? []).filter((e) => !this.isRetracted(e)).map((e) => ({ txid: e.txid, outputIndex: e.outputIndex }))
    }
    const canonical = this.chainOf(s).tokens[q.interval - 1]
    const list = [...(s.tokens.get(q.interval) ?? [])].sort((a, b) =>
      a === canonical ? -1 : b === canonical ? 1 : compareKey(a.txidKey, b.txidKey),
    )
    return list.map((t) => ({ txid: t.txid, outputIndex: t.outputIndex }))
  }

  // ---- restore

  /**
   * Rebuilds the index from storage (spec section 8.5). The host hands the
   * unspent outputs of this module's topics, with their BEEF (a row handed
   * without it is read again from storage with it); the index also needs
   * spent ones (each anchor token but the newest is spent by the next, and
   * a carrier's funding output by the carrier and by any sweep), so it
   * walks from every row to the outputs its transaction spent
   * (outputsConsumed), the outputs that spent it (consumedBy), and its
   * transaction's other outputs, asking storage for each. Every output
   * connected to an unspent one is found, whatever order storage returns
   * them in; statuses are functions of the set. Storage is kept for the
   * topic managers' previous coins. A storage failure propagates: a restore that
   * skipped a retraction would serve retracted batches.
   */
  async restore(outputs: Output[], storage: RestoreStorage): Promise<number> {
    this.restoring = true
    try {
      return await this.restoreIndex(outputs, storage)
    } finally {
      this.restoring = false
      if (this.retention !== undefined) {
        this.retain().catch((e: unknown) => this.host.log('ls_log retention pass failed', { err: String(e) }))
      }
    }
  }

  private async restoreIndex(outputs: Output[], storage: RestoreStorage): Promise<number> {
    this.streams.clear()
    this.byOutpoint.clear()
    this.held.clear()
    this.trees.clear()
    this.carriersByFunding.clear()
    this.spenders.clear()
    this.retracted.clear()
    this.noted.clear()
    this.forkSeqs = 0
    this.drops = []
    const full = storage as FullStorage
    this.storage = full
    const key = (topic: string, txid: string, vout: number): string => `${topic}|${outpointKey(txid, vout)}`
    const seen = new Set<string>()
    const queue: Output[] = []
    for (const o of outputs) {
      if (!this.topics.has(o.topic)) continue
      seen.add(key(o.topic, o.txid, o.outputIndex))
      queue.push(o)
    }
    const txs = new Map<string, Transaction | undefined>()
    // Outputs storage no longer has (a carrier retention deleted), and the
    // funding outputs every spender of which is such an output.
    const absent = new Set<string>()
    const fundings: Array<{ topic: string; op: string; spenders: string[] }> = []
    const visit = async (topic: string, txid: string, vout: number): Promise<void> => {
      const k = key(topic, txid, vout)
      if (seen.has(k)) return
      seen.add(k)
      const o = await full.findOutput(txid, vout, topic, undefined, true)
      if (o !== null) queue.push(o)
      else absent.add(k)
    }
    let indexed = 0
    while (queue.length > 0) {
      let o = queue.shift()!
      if (o.beef === undefined && !txs.has(o.txid)) {
        // A host that hands its unspent rows without their BEEF (an older
        // reference host asks storage for none): read the row again with it.
        o = (await full.findOutput(o.txid, o.outputIndex, o.topic, undefined, true)) ?? o
      }
      let tx = txs.get(o.txid)
      const first = !txs.has(o.txid)
      if (first) {
        try {
          tx = o.beef === undefined ? undefined : Transaction.fromBEEF(o.beef, o.txid)
        } catch {
          tx = undefined
        }
        txs.set(o.txid, tx)
      }
      if (tx === undefined) {
        this.host.log('ls_log could not restore an output without its transaction', { topic: o.topic, txid: o.txid, outputIndex: o.outputIndex })
      } else {
        try {
          if (this.index(o.topic, tx, o.outputIndex)) {
            this.noteSpends(tx)
            indexed++
            if (this.retention !== undefined && o.consumedBy.length > 0 && this.trees.get(`${o.topic}|${o.txid}`)?.has(o.outputIndex) === true) {
              fundings.push({ topic: o.topic, op: outpointKey(o.txid, o.outputIndex), spenders: o.consumedBy.map((c) => key(o.topic, c.txid, c.outputIndex)) })
            }
          }
        } catch (e) {
          // Admission checked every stored output; one that no longer
          // decodes is logged and skipped rather than stopping the start.
          this.host.log('ls_log could not restore an output', { topic: o.topic, txid: o.txid, outputIndex: o.outputIndex, err: String(e) })
        }
      }
      for (const c of o.outputsConsumed) await visit(o.topic, c.txid, c.outputIndex)
      for (const c of o.consumedBy) await visit(o.topic, c.txid, c.outputIndex)
      if (first && tx !== undefined) for (let j = 0; j < tx.outputs.length; j++) await visit(o.topic, o.txid, j)
    }
    // A funding output spent only by carriers retention deleted: the tree
    // class still answers its tree, and the index need not hold it.
    for (const f of fundings) {
      if (f.spenders.every((k) => absent.has(k)) && (this.carriersByFunding.get(f.op)?.size ?? 0) === 0) this.retireFunding(f.topic, f.op)
    }
    return indexed
  }

  // ---- retention (retention.ts)

  private running: Promise<RetentionPass> | undefined

  /**
   * One retention pass at the host's time: for each stream, its oldest
   * intervals not yet dropped, in order, while each is canonical, complete
   * and older than the policy, have every carrier dropped from the index;
   * the state file is written; then the dropped carriers' outputs are
   * deleted from storage. Never while a restore is in flight; a pass asked
   * for while one runs is that pass.
   */
  retain(): Promise<RetentionPass> {
    if (this.running !== undefined) return this.running
    this.running = this.retainPass().finally(() => {
      this.running = undefined
    })
    return this.running
  }

  private async retainPass(): Promise<RetentionPass> {
    const out: RetentionPass = { intervals: 0, carriers: 0, deleted: 0 }
    const r = this.retention
    if (r === undefined || this.restoring || this.storage === undefined) return out
    const now = (r.now ?? Date.now)()
    for (const s of this.streams.values()) {
      const chain = this.chainOf(s).tokens
      for (let k = s.retiredInterval + 1; k <= chain.length; k++) {
        const t = chain[k - 1]!
        if (now - t.anchor.timestamp < r.keepMs) break
        const { report, chosen } = this.intervalChoice(s, t)
        if (report.status !== 'complete' || chosen === undefined) break
        const topic = `${TopicPrefix}${s.stream}`
        for (let seq = t.anchor.first; seq <= t.anchor.first + t.anchor.count - 1; seq++) {
          const list = s.batches.get(seq) ?? []
          if (list.length > 1) this.forkSeqs--
          for (const e of list) {
            this.retireBatch(topic, e)
            out.carriers++
          }
          s.batches.delete(seq)
        }
        s.retiredInterval = k
        s.retiredSeq = t.anchor.first + t.anchor.count - 1
        s.retiredC = chosen[chosen.length - 1]!.cKey
        this.marks.set(`${s.producer}|${s.stream}`, { producer: s.producer, stream: s.stream, interval: k, seq: s.retiredSeq, c: s.retiredC })
        this.stateDirty = true
        out.intervals++
      }
    }
    if (out.carriers > 0) {
      this.host.metrics.inc('blogs_retired_total', undefined, out.carriers)
      this.host.log('ls_log retention dropped proven carriers', { intervals: out.intervals, carriers: out.carriers })
    }
    if (this.stateDirty) {
      // Written before anything is deleted: a crash between the two leaves
      // carriers in storage that restore skips and the next pass deletes,
      // never a deleted carrier counted as a hole. Until it is written,
      // nothing is deleted.
      try {
        r.store.save({ marks: [...this.marks.values()], trees: [...this.treeLow] })
        this.stateDirty = false
      } catch (e) {
        this.host.log('ls_log could not write the retention state; nothing is deleted from storage until it is', { err: String(e) })
        return out
      }
    }
    out.deleted = await this.deleteDrops()
    return out
  }

  /** Takes a carrier out of the index, with its funding output when no other carrier spends it, and queues its output for deletion. */
  private retireBatch(topic: string, e: BatchEntry): void {
    this.forget(e)
    const op = outpointKey(e.txid, e.outputIndex)
    this.held.delete(`${topic}|${op}`)
    this.unref(op, topic)
    this.noted.delete(e.txid)
    this.drops.push({ txid: e.txid, vout: e.outputIndex, topic })
    if ((this.carriersByFunding.get(e.funding)?.size ?? 0) === 0) this.retireFunding(topic, e.funding)
  }

  /**
   * Takes a funding output out of the index. Storage keeps it, spent, with
   * its transaction (spec section 8.1); the tree class answers its tree
   * from what is left of it in the index, from treeLow, or from storage.
   */
  private retireFunding(topic: string, op: string): void {
    this.held.delete(`${topic}|${op}`)
    this.unref(op, topic)
    this.spenders.delete(op)
    const dot = op.lastIndexOf('.')
    const k = `${topic}|${op.slice(0, dot)}`
    const vout = Number(op.slice(dot + 1))
    const vouts = this.trees.get(k)
    if (vouts === undefined || !vouts.has(vout)) return
    vouts.delete(vout)
    const low = Math.min(this.treeLow.get(k) ?? Infinity, vout)
    if (vouts.size > 0 || low !== 0) this.treeLow.set(k, low)
    else this.treeLow.delete(k)
    if (vouts.size === 0) this.trees.delete(k)
    this.stateDirty = true
  }

  private unref(op: string, topic: string): void {
    const refs = this.byOutpoint.get(op)
    if (refs === undefined) return
    const keep = refs.filter((r) => r.topic !== topic)
    if (keep.length === 0) this.byOutpoint.delete(op)
    else this.byOutpoint.set(op, keep)
  }

  /** Deletes the queued carrier outputs from storage; one that fails stays queued. */
  private async deleteDrops(): Promise<number> {
    const s = this.storage
    if (this.drops.length === 0 || s === undefined) return 0
    if (s.deleteOutput === undefined) {
      if (!this.warned.has('delete')) {
        this.warned.add('delete')
        this.host.log('ls_log cannot delete dropped carriers: the storage has no deleteOutput; they stay in storage and restore skips them')
      }
      this.drops = []
      return 0
    }
    const queue = this.drops
    this.drops = []
    let n = 0
    for (const d of queue) {
      try {
        await s.deleteOutput(d.txid, d.vout, d.topic)
        n++
      } catch (e) {
        this.drops.push(d)
        this.host.log('ls_log could not delete a dropped carrier; the next pass tries again', { txid: d.txid, err: String(e) })
      }
    }
    return n
  }

  async getDocumentation(): Promise<string> {
    return [
      '# ls_log',
      '',
      'Three free question classes, for every blogs stream this host carries:',
      '',
      '- `{"stream": s, "producer": p, "seq": n}`: the admitted, unretracted batch',
      '  carriers of p on s at seq n (at most 8, by commitment);',
      '- `{"stream": s, "producer": p, "interval": k}`: the admitted anchor tokens of',
      '  p on s for interval k (at most 8, canonical first);',
      '- `{"stream": s, "tree": t}`: the mined funding tree with txid t whose funding',
      '  outputs the topic of s holds, spent or not (one output, the lowest held; its',
      '  BEEF is the tree with its proof).',
      '',
      'Any other member, a missing member or a value out of range is refused with',
      'an error. An empty answer means only that this host holds nothing that answers.',
    ].join('\n')
  }

  async getMetaData(): Promise<LookupServiceMetaData> {
    return { name: LookupServiceName, shortDescription: 'Answers blogs batches by seq, anchor tokens by interval and funding trees by txid.' }
  }
}

export type Query =
  | { stream: string; producer: string; seq: number }
  | { stream: string; producer: string; interval: number }
  | { stream: string; tree: string }

/**
 * Validates a question as parsed (spec section 7.2): exactly the members of
 * one class, each within its rule. Anything else is an error, never an empty
 * answer and never ignored.
 */
export function parseQuery(query: unknown): Query {
  if (typeof query !== 'object' || query === null || Array.isArray(query)) throw new Error('ls_log: the query is not an object')
  const q = query as Record<string, unknown>
  const members = Object.keys(q).sort()
  const cls = members.includes('tree') ? 'tree' : members.includes('seq') ? 'seq' : 'interval'
  const want = (cls === 'tree' ? ['stream', 'tree'] : ['producer', cls, 'stream']).sort()
  if (members.length !== want.length || members.some((m, i) => m !== want[i])) {
    throw new Error(
      `ls_log: a query has exactly stream, producer and one of seq or interval, or exactly stream and tree; got ${members.join(', ') || 'nothing'}`,
    )
  }
  const stream = q['stream']
  if (typeof stream !== 'string') throw new Error('ls_log: stream is not a string')
  try {
    checkStream(stream)
  } catch {
    throw new Error('ls_log: stream is not a stream identifier <name>_<suffix>')
  }
  if (cls === 'tree') {
    const tree = q['tree']
    if (typeof tree !== 'string' || !/^[0-9a-f]{64}$/.test(tree)) throw new Error('ls_log: tree is not a txid of 64 lowercase hex characters')
    return { stream, tree }
  }
  const producer = q['producer']
  if (typeof producer !== 'string' || !/^[0-9a-f]{66}$/.test(producer)) throw new Error('ls_log: producer is not 66 lowercase hex characters')
  try {
    checkProducer(fromHex(producer))
  } catch {
    throw new Error('ls_log: producer is not a compressed secp256k1 key')
  }
  const n = q[cls]
  if (typeof n !== 'number' || !Number.isInteger(n) || n < 1 || n > MaxSafe) throw new Error(`ls_log: ${cls} is not an integer from 1 to 2^53 - 1`)
  return cls === 'seq' ? { stream, producer, seq: n } : { stream, producer, interval: n }
}
