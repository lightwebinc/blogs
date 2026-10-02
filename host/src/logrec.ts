/**
 * The byte-level contract of a blogs log stream, the twin of the Go package
 * internal/logrec: the batch record a carrier holds, the digest-only
 * locator, the interval anchor record a mined token holds, the stream
 * identifier and its random suffix. docs/spec.md is the normative text; the
 * vectors under testdata/vectors hold both codecs to the same bytes.
 *
 * Every record is canonical CBOR through @lightwebinc/bcommon, so one value
 * has one encoding a reader accepts. Every decoder bounds its input before
 * the CBOR decoder runs, and the CBOR decoder bounds every declared length
 * against the bytes present before it allocates. Decoding follows the
 * refusal order of spec section 3.2 step for step, so both codecs refuse a
 * record for the same reason.
 */
import { CborError, CborMap, claimsRecord, decodeValue, encode, recordReader, type Pair, type Value } from '@lightwebinc/bcommon'

/** The BRC-43 protocol, security level 1, and the key ids under it. */
export const ProtocolName = 'blogs'
export const Protocol: [1, string] = [1, ProtocolName]
export const KeyBatch = 'batch'
export const KeyAnchor = 'anchor'
export const KeyFund = 'fund'

/** The overlay names (BRC-87). */
export const TopicPrefix = 'tm_log_'
export const LookupService = 'ls_log'

/** Tags and magics. The tag prefix "bl" belongs to blogs. */
export const TagAnchor = Uint8Array.of(0x62, 0x6c, 0x01)
export const TagFunding = Uint8Array.of(0x62, 0x6c, 0x02)
export const MagicBatch = Uint8Array.of(0x62, 0x6c, 0x62, 0x01)
export const MagicAnchor = Uint8Array.of(0x62, 0x6c, 0x61, 0x01)

/** Bounds. A reader accepts every record within them. */
export const MaxSafe = Number.MAX_SAFE_INTEGER
export const SuffixLen = 10
export const MaxStream = 50 - TopicPrefix.length
export const MaxStreamName = MaxStream - 1 - SuffixLen
export const MinStream = 1 + 1 + SuffixLen
export const MaxContent = 256 << 10
export const MaxExpanded = 16 << 20
export const MaxLocated = MaxExpanded
export const MaxBatchRecord = MaxContent + 4096
export const MaxAnchorRecord = 1024
export const MaxIntervalBatches = 1 << 16
export const MaxURLs = 4
export const MaxURL = 256
export const MaxKeys = 64
/** The bound DecodeLocator puts on a locator encoded alone. */
export const MaxLocatorAlone = 2048

/** Batch record keys. */
export const BK = { magic: 0, producer: 1, stream: 2, seq: 3, prev: 4, timestamp: 5, count: 6, encoding: 7, profile: 8, content: 9, locator: 10, size: 11 } as const
const bkLast = 11n
/** Anchor record keys. */
export const AK = { magic: 0, producer: 1, stream: 2, interval: 3, prev: 4, first: 5, count: 6, root: 7, timestamp: 8 } as const
const akLast = 8n

export const EncodingLines = 1
export const EncodingLinesGzip = 2
export const ProfileFull = 1
export const ProfileDigest = 2

/**
 * The fixed labels a host counts refusals by: the record reasons of spec
 * section 3.2 and the transaction reasons of section 8.1.
 */
export type Reason =
  | 'too-large'
  | 'cbor'
  | 'key-type'
  | 'magic'
  | 'missing'
  | 'type'
  | 'range'
  | 'producer'
  | 'stream'
  | 'prev'
  | 'profile-shape'
  | 'locator'
  | 'content'
  | 'carrier-shape'
  | 'anchor-shape'
  | 'producer-policy'
  | 'mineable'
  | 'lock'
  | 'signature'
  | 'funding'
  | 'unlock'
  | 'anchor-input'
  | 'unmined'

/** A refusal: the reason label, and a detail for people. */
export class Refusal extends Error {
  constructor(
    readonly reason: Reason,
    detail?: string,
  ) {
    super(detail === undefined ? `logrec: ${reason}` : `logrec: ${reason}: ${detail}`)
    this.name = 'Refusal'
  }
}

/** The secp256k1 field prime. */
const fieldP = 0xfffffffffffffffffffffffffffffffffffffffffffffffffffffffefffffc2fn

function modPow(b: bigint, e: bigint, m: bigint): bigint {
  let r = 1n
  b %= m
  while (e > 0n) {
    if (e & 1n) r = (r * b) % m
    b = (b * b) % m
    e >>= 1n
  }
  return r
}

export function toBigInt(b: Uint8Array): bigint {
  let x = 0n
  for (const byte of b) x = (x << 8n) | BigInt(byte)
  return x
}

/**
 * Refuses anything but a canonical compressed secp256k1 key: 33 bytes,
 * prefix 0x02 or 0x03, an x-coordinate below the field prime, and a point
 * on the curve. A reader that accepted x >= p would accept two encodings of
 * one key, so two byte strings would name one producer.
 */
export function checkProducer(k: Uint8Array): void {
  if (k.length !== 33 || (k[0] !== 0x02 && k[0] !== 0x03)) throw new Refusal('producer')
  const x = toBigInt(k.subarray(1))
  if (x >= fieldP) throw new Refusal('producer')
  const rhs = (modPow(x, 3n, fieldP) + 7n) % fieldP
  // Euler's criterion: rhs has a square root exactly when it is zero or
  // rhs^((p-1)/2) = 1.
  if (rhs !== 0n && modPow(rhs, (fieldP - 1n) / 2n, fieldP) !== 1n) throw new Refusal('producer')
}

function checkName(s: string, max: number): void {
  if (s.length === 0 || s.length > max) throw new Refusal('stream')
  for (let i = 0; i < s.length; i++) {
    const c = s.charCodeAt(i)
    if (c >= 0x61 && c <= 0x7a) continue
    if (c === 0x5f && i !== 0 && i !== s.length - 1 && s.charCodeAt(i - 1) !== 0x5f) continue
    throw new Refusal('stream')
  }
}

/** The grammar of a stream's readable part. */
export function checkStreamName(name: string): void {
  checkName(name, MaxStreamName)
}

/**
 * The stream identifier grammar: <name>_<suffix>, where name meets
 * checkStreamName and suffix is exactly SuffixLen lowercase ASCII letters.
 * Every character the grammar allows is one byte, so a string of other
 * characters fails whatever its length is counted in.
 */
export function checkStream(s: string): void {
  if (s.length < MinStream || s.length > MaxStream) throw new Refusal('stream')
  const cut = s.length - SuffixLen - 1
  if (s.charCodeAt(cut) !== 0x5f) throw new Refusal('stream')
  for (let i = cut + 1; i < s.length; i++) {
    const c = s.charCodeAt(i)
    if (c < 0x61 || c > 0x7a) throw new Refusal('stream')
  }
  checkStreamName(s.slice(0, cut))
}

/** A stream identifier's readable part and suffix. */
export function splitStream(s: string): { name: string; suffix: string } {
  checkStream(s)
  const cut = s.length - SuffixLen - 1
  return { name: s.slice(0, cut), suffix: s.slice(cut + 1) }
}

/**
 * Creates a stream identifier for name: name, an underscore, and SuffixLen
 * letters drawn uniformly at random from a to z. random(n) returns n random
 * bytes (crypto.getRandomValues when omitted). The draw is the Go codec's:
 * bytes are read 16 at a time and a byte of 234 or more (234 = 9 * 26) is
 * skipped, so every letter is equally likely and the same bytes give the
 * same suffix in both languages.
 */
export function newStream(name: string, random?: (n: number) => Uint8Array): string {
  checkStreamName(name)
  const draw = random ?? ((n: number) => crypto.getRandomValues(new Uint8Array(n)))
  let suffix = ''
  while (suffix.length < SuffixLen) {
    const buf = draw(16)
    if (buf.length !== 16) throw new Error('logrec: short random read')
    for (const c of buf) {
      if (c < 234 && suffix.length < SuffixLen) suffix += String.fromCharCode(0x61 + (c % 26))
    }
  }
  return `${name}_${suffix}`
}

/** The topic manager name of a stream identifier. */
export function topic(stream: string): string {
  checkStream(stream)
  return TopicPrefix + stream
}

const rec = recordReader((reason, detail) => new Refusal(reason as Reason, detail))
const decodeMap = rec.decodeMap
const intKeys = rec.split
const checkMagic = rec.checkMagic
const getBytes = rec.bytes
const needBytes = rec.bytesRange
const needUint = rec.uint
const needText = rec.text
const inRange = rec.inRange
const checkExtra = rec.checkExtraKeys

function isZero(b: Uint8Array): boolean {
  return b.every((x) => x === 0)
}

function validURL(u: string): boolean {
  if (u.length < 'https://x'.length || u.length > MaxURL || !u.startsWith('https://')) return false
  for (let i = 0; i < u.length; i++) {
    const c = u.charCodeAt(i)
    if (c <= 0x20 || c >= 0x7f) return false
  }
  return true
}

/**
 * A digest-only batch's pointer to its content. Every member is always
 * present. extra holds members this version does not define, whatever
 * their key type, preserved verbatim; a locator with any is unsupported.
 */
export interface Locator {
  digest: Uint8Array
  length: number
  urls: string[]
  extra: Pair[]
}

const locatorNames = new Set(['digest', 'length', 'urls'])

function validateLocator(l: Locator): void {
  if (!Number.isSafeInteger(l.length) || l.length < 1 || l.length > MaxLocated) throw new Refusal('locator', 'length')
  if (l.digest.length !== 32) throw new Refusal('locator', 'digest')
  if (l.urls.length > MaxURLs) throw new Refusal('locator', 'urls')
  for (const u of l.urls) if (!validURL(u)) throw new Refusal('locator', 'url')
  for (const p of l.extra) {
    if (typeof p.key === 'string' && locatorNames.has(p.key)) throw new Refusal('locator', 'member name')
  }
}

function locatorValue(l: Locator): CborMap {
  return new CborMap([
    { key: 'digest', val: l.digest },
    { key: 'length', val: BigInt(l.length) },
    { key: 'urls', val: [...l.urls] },
    ...l.extra,
  ])
}

function decodeLocatorValue(v: Value): Locator {
  if (!(v instanceof CborMap)) throw new Refusal('locator', 'not a map')
  let digest: Uint8Array | undefined
  let length: number | undefined
  let urls: string[] | undefined
  let seen = 0
  const extra: Pair[] = []
  for (const p of v.entries) {
    const k = typeof p.key === 'string' ? p.key : ''
    switch (k) {
      case 'digest':
        if (!(p.val instanceof Uint8Array) || p.val.length !== 32) throw new Refusal('locator', 'digest')
        digest = p.val
        seen++
        break
      case 'length':
        if (typeof p.val !== 'bigint' || p.val < 0n) throw new Refusal('locator', 'length')
        // Held to the bound here, not later, so no value past 2^53 becomes
        // a JS number; validate refuses it with the same reason.
        if (p.val < 1n || p.val > BigInt(MaxLocated)) throw new Refusal('locator', 'length')
        length = Number(p.val)
        seen++
        break
      case 'urls': {
        if (!Array.isArray(p.val) || p.val.length > MaxURLs) throw new Refusal('locator', 'urls')
        const us: string[] = []
        for (const e of p.val) {
          if (typeof e !== 'string') throw new Refusal('locator', 'url')
          us.push(e)
        }
        urls = us
        seen++
        break
      }
      default:
        extra.push(p)
    }
  }
  if (seen !== 3 || digest === undefined || length === undefined || urls === undefined) throw new Refusal('locator', 'a member is missing')
  const l: Locator = { digest, length, urls, extra }
  validateLocator(l)
  return l
}

export function locatorUnsupported(l: Locator): boolean {
  return l.extra.length > 0
}

/** A locator encoded alone, as it appears under batch key 10. */
export function encodeLocator(l: Locator): Uint8Array {
  validateLocator(l)
  return encode(locatorValue(l))
}

export function decodeLocator(b: Uint8Array): Locator {
  if (b.length > MaxLocatorAlone) throw new Refusal('too-large')
  let v: Value
  try {
    v = decodeValue(b)
  } catch (e) {
    if (e instanceof CborError) throw new Refusal('cbor', e.message)
    throw e
  }
  return decodeLocatorValue(v)
}

/** One batch record: the payload of one carrier. */
export interface Batch {
  producer: Uint8Array
  stream: string
  seq: number
  /** The previous batch's commitment, hash byte order; zero on seq 1. */
  prev: Uint8Array
  timestamp: number
  count: number
  encoding: number
  profile: number
  content?: Uint8Array
  locator?: Locator
  size: number
  /** Integer keys above 11, preserved verbatim and ignored. */
  extra: Pair[]
}

/**
 * Whether this version can read the batch's content. An unsupported batch
 * is still a valid link in its stream.
 */
export function batchSupported(b: Batch): boolean {
  if (b.encoding !== EncodingLines && b.encoding !== EncodingLinesGzip) return false
  if (b.profile === ProfileFull) return true
  if (b.profile === ProfileDigest) return b.locator !== undefined && !locatorUnsupported(b.locator)
  return false
}

function batchCrossRules(b: Batch): void {
  if ((b.seq === 1) !== isZero(b.prev)) throw new Refusal('prev')
  if (b.count > b.size) throw new Refusal('range', 'count above size')
  if (b.profile === ProfileFull) {
    if (b.content === undefined || b.locator !== undefined) throw new Refusal('profile-shape')
    if (b.encoding === EncodingLines && b.size !== b.content.length) throw new Refusal('content')
  } else if (b.profile === ProfileDigest) {
    if (b.content !== undefined || b.locator === undefined) throw new Refusal('profile-shape')
    if (b.encoding === EncodingLines && b.size !== b.locator.length) throw new Refusal('content')
  }
}

/** Every rule that needs only the record, in the refusal order. */
export function validateBatch(b: Batch): void {
  checkExtra(b.extra, bkLast)
  const known = 10 + (b.content !== undefined ? 1 : 0) + (b.locator !== undefined ? 1 : 0)
  if (b.extra.length + known > MaxKeys) throw new Refusal('too-large', `more than ${MaxKeys} keys`)
  checkProducer(b.producer)
  checkStream(b.stream)
  inRange(b.seq, 1, MaxSafe, 'seq')
  if (b.prev.length !== 32) throw new Refusal('range', 'prev')
  inRange(b.timestamp, 1, MaxSafe, 'timestamp')
  inRange(b.count, 1, MaxSafe, 'count')
  inRange(b.encoding, 1, MaxSafe, 'encoding')
  inRange(b.profile, 1, MaxSafe, 'profile')
  if (b.content !== undefined) inRange(b.content.length, 1, MaxContent, 'content')
  if (b.locator !== undefined) validateLocator(b.locator)
  inRange(b.size, 1, MaxExpanded, 'size')
  batchCrossRules(b)
}

/** Writes the batch record in canonical CBOR. */
export function encodeBatch(b: Batch): Uint8Array {
  validateBatch(b)
  const entries: Pair[] = [
    { key: 0n, val: MagicBatch },
    { key: 1n, val: b.producer },
    { key: 2n, val: b.stream },
    { key: 3n, val: BigInt(b.seq) },
    { key: 4n, val: b.prev },
    { key: 5n, val: BigInt(b.timestamp) },
    { key: 6n, val: BigInt(b.count) },
    { key: 7n, val: BigInt(b.encoding) },
    { key: 8n, val: BigInt(b.profile) },
    { key: 11n, val: BigInt(b.size) },
  ]
  if (b.content !== undefined) entries.push({ key: 9n, val: b.content })
  if (b.locator !== undefined) entries.push({ key: 10n, val: locatorValue(b.locator) })
  entries.push(...b.extra)
  const out = encode(new CborMap(entries))
  if (out.length > MaxBatchRecord) throw new Refusal('too-large')
  return out
}

const claims = claimsRecord

export function isBatch(payload: Uint8Array): boolean {
  return claims(payload, MagicBatch)
}

export function isAnchor(payload: Uint8Array): boolean {
  return claims(payload, MagicAnchor)
}

/**
 * Parses a batch record in the refusal order: the bound, then canonical
 * CBOR and the map's shape, then the magic, then each defined key in
 * ascending order (missing, type, the key's own rule), then the rules
 * across fields.
 */
export function decodeBatch(p: Uint8Array): Batch {
  const f = intKeys(decodeMap(p, MaxBatchRecord), bkLast)
  checkMagic(f, MagicBatch)
  const producer = getBytes(f, BK.producer)
  checkProducer(producer)
  const stream = needText(f, BK.stream)
  checkStream(stream)
  const seq = needUint(f, BK.seq, 1, MaxSafe)
  const prev = needBytes(f, BK.prev, 32, 32)
  const timestamp = needUint(f, BK.timestamp, 1, MaxSafe)
  const count = needUint(f, BK.count, 1, MaxSafe)
  const encoding = needUint(f, BK.encoding, 1, MaxSafe)
  const profile = needUint(f, BK.profile, 1, MaxSafe)
  let content: Uint8Array | undefined
  if (f.known.has(BK.content)) content = needBytes(f, BK.content, 1, MaxContent)
  let locator: Locator | undefined
  if (f.known.has(BK.locator)) {
    const v = f.known.get(BK.locator)!
    if (!(v instanceof CborMap)) throw new Refusal('type', `key ${BK.locator}`)
    locator = decodeLocatorValue(v)
  }
  const size = needUint(f, BK.size, 1, MaxExpanded)
  const b: Batch = { producer, stream, seq, prev, timestamp, count, encoding, profile, size, extra: f.extra }
  if (content !== undefined) b.content = content
  if (locator !== undefined) b.locator = locator
  batchCrossRules(b)
  return b
}

/**
 * One interval anchor record: the second field of a mined anchor token. It
 * commits the batches first to first + count - 1 of one producer's stream
 * under root.
 */
export interface Anchor {
  producer: Uint8Array
  stream: string
  interval: number
  /** The previous anchor token's txid, hash byte order; zero on interval 1. */
  prev: Uint8Array
  first: number
  count: number
  root: Uint8Array
  timestamp: number
  /** Integer keys above 8, preserved verbatim and ignored. */
  extra: Pair[]
}

export function anchorLast(a: Anchor): number {
  return a.first + a.count - 1
}

export function anchorCovers(a: Anchor, seq: number): boolean {
  return seq >= a.first && seq <= anchorLast(a)
}

function anchorCrossRules(a: Anchor): void {
  if ((a.interval === 1) !== isZero(a.prev)) throw new Refusal('prev')
  if (a.interval === 1 && a.first !== 1) throw new Refusal('range', 'first on interval 1')
  // first and count are both within 2^53 - 1 and 2^16, so their sum is
  // exact as a double.
  if (a.first + a.count - 1 > MaxSafe) throw new Refusal('range', 'last seq')
}

export function validateAnchor(a: Anchor): void {
  checkExtra(a.extra, akLast)
  if (a.extra.length + 9 > MaxKeys) throw new Refusal('too-large', `more than ${MaxKeys} keys`)
  checkProducer(a.producer)
  checkStream(a.stream)
  inRange(a.interval, 1, MaxSafe, 'interval')
  if (a.prev.length !== 32 || a.root.length !== 32) throw new Refusal('range', 'prev or root')
  inRange(a.first, 1, MaxSafe, 'first')
  inRange(a.count, 1, MaxIntervalBatches, 'count')
  inRange(a.timestamp, 1, MaxSafe, 'timestamp')
  anchorCrossRules(a)
}

export function encodeAnchor(a: Anchor): Uint8Array {
  validateAnchor(a)
  const out = encode(
    new CborMap([
      { key: 0n, val: MagicAnchor },
      { key: 1n, val: a.producer },
      { key: 2n, val: a.stream },
      { key: 3n, val: BigInt(a.interval) },
      { key: 4n, val: a.prev },
      { key: 5n, val: BigInt(a.first) },
      { key: 6n, val: BigInt(a.count) },
      { key: 7n, val: a.root },
      { key: 8n, val: BigInt(a.timestamp) },
      ...a.extra,
    ]),
  )
  if (out.length > MaxAnchorRecord) throw new Refusal('too-large')
  return out
}

export function decodeAnchor(p: Uint8Array): Anchor {
  const f = intKeys(decodeMap(p, MaxAnchorRecord), akLast)
  checkMagic(f, MagicAnchor)
  const producer = getBytes(f, AK.producer)
  checkProducer(producer)
  const stream = needText(f, AK.stream)
  checkStream(stream)
  const interval = needUint(f, AK.interval, 1, MaxSafe)
  const prev = needBytes(f, AK.prev, 32, 32)
  const first = needUint(f, AK.first, 1, MaxSafe)
  const count = needUint(f, AK.count, 1, MaxIntervalBatches)
  const root = needBytes(f, AK.root, 32, 32)
  const timestamp = needUint(f, AK.timestamp, 1, MaxSafe)
  const a: Anchor = { producer, stream, interval, prev, first, count, root, timestamp, extra: f.extra }
  anchorCrossRules(a)
  return a
}
