/**
 * `tm_log_<name>_<suffix>`: the topic manager of one stream (spec section
 * 8.1), one instance per configured topic. The rules are admit.ts's; this
 * file is the engine seam: it reads the previous coins, runs the rules, and
 * turns the verdict into admittance instructions.
 *
 * How a refusal reaches the engine matters, because the overlay engine
 * records every transaction a topic manager returns instructions for as
 * applied, empty instructions included, and treats a later submission of
 * the same transaction as a duplicate that no topic manager sees again. It
 * also marks spent, or deletes, every held output the transaction spends.
 * A refusal is therefore returned (recorded) only when its verdict can never
 * change and the transaction spends nothing the topic holds. Otherwise it is
 * raised, and the engine records nothing: an anchor refused under rule 7 or
 * rule 8, whose verdict depends on what the topic holds or on the host's
 * headers, is decided afresh when it is offered again (spec section 12), and
 * a refused transaction never marks an admitted output spent.
 */
import { Transaction, type ChainTracker } from '@bsv/sdk'
import type { AdmittanceInstructions, ModuleHost, TopicManager } from '@lightwebinc/bcommon'
import { admit, outpointKey, validatePolicy, type Coin, type Policy } from './admit.js'
import { Refusal, TopicPrefix, checkStream, type Reason } from './logrec.js'

/** Every refusal label, for presetting the refusal counter. */
export const Reasons: readonly Reason[] = [
  'too-large',
  'cbor',
  'key-type',
  'magic',
  'missing',
  'type',
  'range',
  'producer',
  'stream',
  'prev',
  'profile-shape',
  'locator',
  'content',
  'carrier-shape',
  'anchor-shape',
  'producer-policy',
  'mineable',
  'lock',
  'signature',
  'funding',
  'unlock',
  'anchor-input',
  'unmined',
]

/** Refusals whose verdict can change with what the host holds or its headers. */
export const DeferredReasons: readonly Reason[] = ['anchor-input', 'unmined']

/**
 * What an accepted transaction was: a batch carrier, an anchor token, a
 * mined funding tree or sweep, a spend of held outputs, or none of these
 * (it claims nothing, spends nothing held and admits nothing).
 */
export const AdmitKinds = ['batch', 'anchor', 'funding', 'spend', 'none'] as const
export type AdmitKind = (typeof AdmitKinds)[number]

/**
 * The output the topic holds at an outpoint, or undefined. The lookup
 * service provides it: from the engine's storage once the host has handed
 * it over, otherwise from the outputs it has indexed.
 */
export type CoinSource = (topic: string, txid: string, vout: number) => Promise<Coin | undefined>

/**
 * Raised for a refusal the engine must not record. The engine counts it as
 * a failed topic for this submission and stores nothing.
 */
export class Unrecorded extends Error {
  constructor(
    readonly reason: Reason,
    detail: string,
  ) {
    super(`blogs: ${reason}, not recorded: ${detail}`)
    this.name = 'Unrecorded'
  }
}

/**
 * The engine has verified a submitted transaction's proof against the
 * host's headers before calling a topic manager (Transaction.verify on
 * submit; GASP verifies a graph before its unverified finalisation). What
 * remains for rule 8 and the funding tree rule is that the proof is present
 * and names the transaction, which MerklePath.verify checks before it asks
 * a tracker; this tracker answers yes to what the engine already checked.
 */
export const engineVerified: ChainTracker = {
  isValidRootForHeight: async () => true,
  currentHeight: async () => 0,
}

export class LogTopicManager implements TopicManager {
  readonly stream: string

  constructor(
    readonly topic: string,
    private readonly policy: Policy,
    private readonly coins: CoinSource,
    private readonly host: ModuleHost,
    private readonly headers: ChainTracker = engineVerified,
  ) {
    if (!topic.startsWith(TopicPrefix)) throw new Error(`blogs: ${topic} is not a tm_log_ topic`)
    this.stream = topic.slice(TopicPrefix.length)
    checkStream(this.stream)
    validatePolicy(policy)
  }

  async identifyAdmissibleOutputs(beef: number[], previousCoins: number[]): Promise<AdmittanceInstructions> {
    const tx = Transaction.fromBEEF(beef)
    const txid = tx.id('hex')
    const coins = new Map<string, Coin>()
    for (const i of previousCoins) {
      const input = tx.inputs[i]
      const src = input?.sourceTXID ?? input?.sourceTransaction?.id('hex')
      if (input === undefined || src === undefined) continue
      const coin = await this.coins(this.topic, src, input.sourceOutputIndex)
      if (coin !== undefined) coins.set(outpointKey(src, input.sourceOutputIndex), coin)
    }
    let kind: AdmitKind
    let outputsToAdmit: number[]
    try {
      const a = await admit(tx, coins, { stream: this.stream, policy: this.policy, headers: this.headers })
      if (a.kind === 'batch') {
        kind = 'batch'
        outputsToAdmit = [0]
      } else if (a.kind === 'anchor') {
        kind = 'anchor'
        outputsToAdmit = [a.token!.outputIndex]
      } else if (a.funding.length > 0) {
        kind = 'funding'
        outputsToAdmit = a.funding
      } else {
        kind = previousCoins.length > 0 ? 'spend' : 'none'
        outputsToAdmit = []
      }
    } catch (e) {
      if (!(e instanceof Refusal)) throw e
      const recorded = previousCoins.length === 0 && !DeferredReasons.includes(e.reason)
      this.host.metrics.inc('blogs_refused_total', { reason: e.reason, recorded: recorded ? 'yes' : 'no' })
      this.host.log('tm_log refused', { topic: this.topic, txid, reason: e.reason, detail: e.message, recorded })
      if (!recorded) {
        throw new Unrecorded(e.reason, previousCoins.length > 0 ? `${txid} spends held outputs` : `${txid} may be decided later`)
      }
      return { outputsToAdmit: [], coinsToRetain: [] }
    }
    this.host.metrics.inc('blogs_admitted_total', { kind })
    // Every held output an accepted transaction spends is retained, so that
    // no admitted output is deleted when something spends it (spec section
    // 8.1, retention and spends).
    return { outputsToAdmit, coinsToRetain: [...previousCoins] }
  }

  /** Nothing is needed: a carrier carries its ancestry, an anchor's spent outputs are held. */
  async identifyNeededInputs(): Promise<Array<{ txid: string; outputIndex: number }>> {
    return []
  }

  async getDocumentation(): Promise<string> {
    return [
      `# ${this.topic}`,
      '',
      `The blogs stream ${this.stream}: batch carriers, mined anchor tokens, and the`,
      'funding outputs of mined funding trees and sweeps, each admitted on its own',
      'validity (spec section 8.1). A transaction that spends held outputs and claims',
      'nothing is accepted for its inputs alone. Gaps and forks between batches are',
      'reported by ls_log, never refused.',
    ].join('\n')
  }

  async getMetaData(): Promise<{ name: string; shortDescription: string }> {
    return { name: this.topic, shortDescription: `Admits the blogs log stream ${this.stream}.` }
  }
}
