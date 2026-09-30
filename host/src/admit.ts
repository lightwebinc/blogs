/**
 * The topic manager's transaction rules of spec section 8.1, the twin of
 * the Go internal/logrec/admit.go: classification, then the batch carrier's
 * rules 1 to 9, the anchor token's rules 1 to 9, or the funding tree and
 * sweep rule. A transaction is refused for the first rule it breaks, with
 * the same reason label in both languages.
 */
import { PublicKey, Transaction, type ChainTracker } from '@bsv/sdk'
import { mineableRefusal, readerLockingKey, unlockingRefusal, verifyFieldSignature } from '@lightwebinc/bcommon'
import {
  KeyAnchor,
  KeyBatch,
  Protocol,
  Refusal,
  TagAnchor,
  checkStream,
  decodeAnchor,
  decodeBatch,
  isBatch,
  type Anchor,
  type Batch,
} from './logrec.js'
import { bytesEq, fundingScript, isFundingShape, pushDropScript, readPushDrop, strictSignature } from './script.js'

/**
 * A host's producer policy for one stream: open, or a set of compressed
 * keys. A closed policy with no producers is a configuration error.
 */
export type Policy = { open: true } | { producers: Uint8Array[] }

export function validatePolicy(p: Policy): void {
  if ('open' in p ? p.open !== true || 'producers' in p : p.producers.length === 0) {
    throw new Error('logrec: a policy is open or names producers, exactly one')
  }
}

function admits(p: Policy, producer: Uint8Array): boolean {
  return 'open' in p || p.producers.some((k) => bytesEq(k, producer))
}

export type Kind = 'none' | 'batch' | 'anchor'

export interface Carrier {
  batch: Batch
  record: Uint8Array
  /** C = txid(K), hash byte order. */
  commitment: Uint8Array
  /** The outpoint the carrier spends, txid in hash byte order. */
  funding: { txid: Uint8Array; vout: number }
}

export interface Token {
  anchor: Anchor
  record: Uint8Array
  outputIndex: number
  /** The token transaction's txid, hash byte order. */
  txid: Uint8Array
}

export interface Host {
  stream: string
  policy: Policy
  headers?: ChainTracker
}

/**
 * An output the topic holds, as the overlay engine hands it to the topic
 * manager for an input that spends it (BRC-22's previous coins).
 */
export interface Coin {
  lockingScript: Uint8Array
  satoshis: number
}

/**
 * The previous coins of one transaction, keyed by outpoint (outpointKey):
 * every output the topic holds that one of its inputs spends. An input
 * whose outpoint is absent spends something the topic does not hold.
 */
export type Coins = ReadonlyMap<string, Coin>

/** The key of an outpoint in Coins: the txid in display hex, a dot, the output index. */
export const outpointKey = (txid: string, vout: number): string => `${txid}.${vout}`

/** The coin input i spends, when the topic holds it. */
function spentCoin(tx: Transaction, i: number, coins: Coins): { coin: Coin; txid: string } | undefined {
  const input = tx.inputs[i]!
  const txid = input.sourceTXID ?? input.sourceTransaction?.id('hex')
  if (txid === undefined) return undefined
  const coin = coins.get(outpointKey(txid, input.sourceOutputIndex))
  return coin === undefined ? undefined : { coin, txid }
}

export interface Admission {
  kind: Kind
  carrier?: Carrier
  token?: Token
  /** Admitted funding outputs of a mined funding tree or sweep. */
  funding: number[]
}

export const fromHex = (s: string): Uint8Array => Uint8Array.from(s.match(/../g) ?? [], (b) => parseInt(b, 16))
export const toHex = (b: Uint8Array | number[]): string => Array.from(b, (x) => x.toString(16).padStart(2, '0')).join('')

function script(s: { toBinary(): number[] }): Uint8Array {
  return Uint8Array.from(s.toBinary())
}

/** The spent output, from the carrier's BEEF. */
function spent(tx: Transaction, i: number): { satoshis: number; script: Uint8Array; txid: Uint8Array } | undefined {
  const input = tx.inputs[i]
  const src = input?.sourceTransaction
  const out = src?.outputs[input!.sourceOutputIndex]
  if (src === undefined || out === undefined) return undefined
  return { satoshis: out.satoshis ?? 0, script: script(out.lockingScript), txid: Uint8Array.from(src.hash() as number[]) }
}

/**
 * Finds the one claiming output. More than one refuses the transaction:
 * carrier-shape when any claims a batch, else anchor-shape.
 */
export function classify(tx: Transaction): { kind: Kind; at: number } {
  let kind: Kind = 'none'
  let at = -1
  let batches = 0
  let anchors = 0
  tx.outputs.forEach((o, i) => {
    const p = readPushDrop(script(o.lockingScript))
    if (p === undefined) return
    if (isBatch(p.fields[0]!)) {
      batches++
      kind = 'batch'
      at = i
    } else if (bytesEq(p.fields[0]!, TagAnchor)) {
      anchors++
      if (kind !== 'batch') {
        kind = 'anchor'
        at = i
      }
    }
  })
  if (batches > 0 && batches + anchors > 1) throw new Refusal('carrier-shape', `${batches + anchors} claiming outputs`)
  if (anchors > 1) throw new Refusal('anchor-shape', `${anchors} claiming outputs`)
  return { kind, at }
}

function derived(keyID: string, producer: Uint8Array): Uint8Array {
  return Uint8Array.from(readerLockingKey(Protocol, keyID, toHex(producer)).encode(true) as number[])
}

function verifyField(key: Uint8Array, signed: Uint8Array, der: Uint8Array): boolean {
  if (!strictSignature(der)) return false
  return verifyFieldSignature(readerKey(key), Array.from(signed), Array.from(der))
}

// The PublicKey verifyFieldSignature takes, from its compressed bytes.
function readerKey(k: Uint8Array): PublicKey {
  return PublicKey.fromString(toHex(k))
}

function checkCarrier(tx: Transaction, h: Host): Carrier {
  // 1. carrier-shape.
  if (tx.inputs.length !== 1 || tx.outputs.length !== 1) throw new Refusal('carrier-shape', `${tx.inputs.length} inputs, ${tx.outputs.length} outputs`)
  const src = spent(tx, 0)
  const out = tx.outputs[0]!
  if (src === undefined) throw new Refusal('carrier-shape', 'the spent output is not in the BEEF')
  if (src.satoshis !== (out.satoshis ?? 0)) throw new Refusal('carrier-shape', 'value')
  const lock = script(out.lockingScript)
  const pd = readPushDrop(lock)!
  const record = pd.fields[0]!
  // 2. the record's own rules.
  const b = decodeBatch(record)
  // 3. stream; 4. producer-policy.
  if (b.stream !== h.stream) throw new Refusal('stream', "not this topic's stream")
  if (!admits(h.policy, b.producer)) throw new Refusal('producer-policy')
  // 5. mineable; 6. unlock: bcommon's checks, in carrier.Validate's order.
  if (mineableRefusal(tx) !== undefined) throw new Refusal('mineable')
  if (unlockingRefusal(tx) !== undefined) throw new Refusal('unlock')
  // 7. lock.
  const key = derived(KeyBatch, b.producer)
  if (pd.fields.length !== 2) throw new Refusal('lock', 'want the record and a signature')
  const sig = pd.fields[1]!
  if (!bytesEq(lock, pushDropScript(key, [record], sig))) throw new Refusal('lock')
  // 8. signature.
  if (!verifyField(key, record, sig)) throw new Refusal('signature')
  // 9. funding.
  if (!bytesEq(src.script, fundingScript(key))) throw new Refusal('funding')
  return {
    batch: b,
    record,
    commitment: Uint8Array.from(tx.hash() as number[]),
    funding: { txid: src.txid, vout: tx.inputs[0]!.sourceOutputIndex },
  }
}

/** A well-formed anchor token script, or undefined. Does not verify the signature. */
function readToken(s: Uint8Array): Anchor | undefined {
  const pd = readPushDrop(s)
  if (pd === undefined || !bytesEq(pd.fields[0]!, TagAnchor) || pd.fields.length !== 3) return undefined
  let a: Anchor
  try {
    a = decodeAnchor(pd.fields[1]!)
  } catch {
    return undefined
  }
  const key = derived(KeyAnchor, a.producer)
  return bytesEq(s, pushDropScript(key, [TagAnchor, pd.fields[1]!], pd.fields[2]!)) ? a : undefined
}

/**
 * Rule 7. Every input spends a coin the topic holds: for interval k > 1 the
 * anchor token of k - 1 in transaction prev, or a funding output of the
 * producer; for interval 1 a funding output of the producer. Interval
 * k > 1 spends its predecessor, and interval 1 a funding output.
 */
function anchorInput(tx: Transaction, a: Anchor, coins: Coins): boolean {
  const funding = fundingScript(derived(KeyBatch, a.producer))
  const prev = toHex(Uint8Array.from(a.prev).reverse())
  let found = false
  for (let i = 0; i < tx.inputs.length; i++) {
    const spent = spentCoin(tx, i, coins)
    if (spent === undefined) return false
    if (bytesEq(spent.coin.lockingScript, funding)) {
      found ||= a.interval === 1
      continue
    }
    if (a.interval === 1 || spent.txid !== prev) return false
    const p = readToken(spent.coin.lockingScript)
    if (p === undefined || !bytesEq(p.producer, a.producer) || p.stream !== a.stream || p.interval !== a.interval - 1) return false
    found = true
  }
  return found
}

/** Whether tx carries a proof that verifies against the headers. */
export async function mined(tx: Transaction, headers?: ChainTracker): Promise<boolean> {
  if (tx.merklePath === undefined || headers === undefined) return false
  try {
    return await tx.merklePath.verify(tx.id('hex'), headers)
  } catch {
    return false
  }
}

async function checkToken(tx: Transaction, at: number, coins: Coins, h: Host): Promise<Token> {
  const out = tx.outputs[at]!
  const lock = script(out.lockingScript)
  const pd = readPushDrop(lock)!
  // 1. anchor-shape.
  if (pd.fields.length !== 3) throw new Refusal('anchor-shape', 'want the tag, the record and a signature')
  if ((out.satoshis ?? 0) !== 1) throw new Refusal('anchor-shape', 'satoshis')
  const record = pd.fields[1]!
  const sig = pd.fields[2]!
  // 2. the record's own rules.
  const a = decodeAnchor(record)
  // 3. stream; 4. producer-policy.
  if (a.stream !== h.stream) throw new Refusal('stream', "not this topic's stream")
  if (!admits(h.policy, a.producer)) throw new Refusal('producer-policy')
  // 5. lock.
  const key = derived(KeyAnchor, a.producer)
  if (!bytesEq(lock, pushDropScript(key, [TagAnchor, record], sig))) throw new Refusal('lock')
  // 6. signature.
  const signed = new Uint8Array(TagAnchor.length + record.length)
  signed.set(TagAnchor)
  signed.set(record, TagAnchor.length)
  if (!verifyField(key, signed, sig)) throw new Refusal('signature')
  // 7. anchor-input.
  if (!anchorInput(tx, a, coins)) throw new Refusal('anchor-input')
  // 8. unmined.
  if (!(await mined(tx, h.headers))) throw new Refusal('unmined')
  // 9. unlock: each input as bcommon checks a carrier's one input.
  tx.inputs.forEach((input, i) => {
    if (unlockingRefusal(new Transaction(tx.version, [input], [], 0)) !== undefined) throw new Refusal('unlock', `input ${i}`)
  })
  return { anchor: a, record, outputIndex: at, txid: Uint8Array.from(tx.hash() as number[]) }
}

async function fundingOutputs(tx: Transaction, headers?: ChainTracker): Promise<number[]> {
  const outs: number[] = []
  tx.outputs.forEach((o, i) => {
    if (isFundingShape(script(o.lockingScript))) outs.push(i)
  })
  if (outs.length === 0 || !(await mined(tx, headers))) return []
  return outs
}

/**
 * Applies spec section 8.1 to tx, read from its BEEF (Transaction.fromBEEF),
 * given the previous coins the engine hands the topic manager for its
 * inputs. A carrier's funding output comes from the carrier's own BEEF,
 * which carries its unmined ancestry; an anchor token's spent outputs come
 * only from coins, because a mined transaction's BEEF carries no parents.
 * SPV of the BEEF is the engine's. Throws a Refusal for the first rule
 * broken.
 */
export async function admit(tx: Transaction, coins: Coins, h: Host): Promise<Admission> {
  validatePolicy(h.policy)
  try {
    checkStream(h.stream)
  } catch {
    throw new Error(`logrec: host stream ${JSON.stringify(h.stream)} breaks the grammar`)
  }
  const { kind, at } = classify(tx)
  if (kind === 'batch') return { kind, carrier: checkCarrier(tx, h), funding: [] }
  if (kind === 'anchor') return { kind, token: await checkToken(tx, at, coins, h), funding: [] }
  return { kind, funding: await fundingOutputs(tx, h.headers) }
}
