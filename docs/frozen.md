# Frozen at first publish

Every item below is hashed into a key, written into a record, or named by a
host. Once one record using it is on a public host, readers verify against
it for as long as anyone reads that record, so changing it afterwards is a
new identifier or a new record version, never an edit.

**Status: fully signed.** Every row below is signed. The registry rows in
section 1 belong in bcommon's registry (`docs/registry.md`), where they are
checked against every registered row; this repository does not edit that
registry.

## 1. Registry entries

| Kind | Value | Use | Status |
| --- | --- | --- | --- |
| Protocol | `[1, "blogs"]` | every blogs derivation (five characters, the BRC-43 minimum; `[1, "blog"]` and `[1, "log"]` are refused by the SDK) | signed |
| Key id | `batch` | carrier record output, its field signature, and funding-tree outputs | signed |
| Key id | `anchor` | anchor token lock and field signature | signed |
| Key id | `fund` | the embedded wallet's funding key | signed |
| Tag prefix | `bl` (`62 6c`) | owned by blogs; no registered or planned prefix is `bl` (registered: `bf`, `gw`, and `vx` for tests) | signed |
| Tag | `bl` `0x01` | anchor token, field 1 | signed |
| Tag | `bl` `0x02` | funding-tree output, its one field | signed |
| Record magic | `blb` `0x01` | batch record, version 1 (`"bl"`, `'b'`, version) | signed |
| Record magic | `bla` `0x01` | anchor record, version 1 (`"bl"`, `'a'`, version) | signed |
| Topics | `tm_log_<name>_<suffix>` | one topic manager name per stream: a readable `<name>` and a 10-letter random `<suffix>` fixed at the stream's creation (spec section 7.1); the `tm_log_` namespace belongs to blogs | signed |
| Lookup service | `ls_log` | one per host, all streams; a question names its stream by the `stream` member | signed |
| Baskets | `blogs fund`, `blogs batch funding`, `blogs anchor`, `blogs kill tombstone` | wallet baskets; never on a public host, registered so no two applications share one in a wallet | signed |

Type bytes `0x03` to `0xff` under `bl` and record letters other than `b` and
`a` are unassigned and stay blogs'.

## 2. Mechanism shared with the substrate

| Item | Value | Status |
| --- | --- | --- |
| Derivation setting | BRC-42, counterparty `anyone`, `forSelf = true` on the producer's side | signed |
| PushDrop layout | lock-before, field signature last, over `SHA-256` of the fields concatenated | signed |
| Carrier non-final device | producers write `nLockTime = 4102444800` and input `nSequence = 0`; hosts accept any `nLockTime >= 4102444800` and any `nSequence` below `0xFFFFFFFF` | signed |
| Carrier shape | exactly one input spending a producer funding output, unlocked by exactly one minimal push of a strict low-S DER signature and sighash `0x41` (SIGHASH_ALL with FORKID, bcommon `carrier.SigHashType`); exactly one output carrying the whole input value; refused in bcommon `carrier.Validate`'s order: `mineable`, then `unlock`, then `lock`, then `signature`, then `funding` (spec section 8.1) | signed |
| Script canonicality | every checked script equals, byte for byte, the one rebuilt from its fields and the derived key (compressed key, minimal pushes, fewest `OP_2DROP` then `OP_DROP`) | signed |
| Signatures | field and input signatures strict DER, low S; input signatures of carriers and anchor tokens carry sighash `0x41` only | signed |
| Funding output | `<derive(producer, "batch")> OP_CHECKSIG <62 6c 02> OP_DROP`, unsigned; admitted only from mined transactions | signed |
| Anchor token | exactly one anchor output per transaction, other outputs allowed; admitted only mined; lock-before PushDrop `[62 6c 01, A]` signed under `derive(producer, "anchor")`, 1 satoshi; interval `k > 1` spends interval `k - 1`'s token, interval 1 spends a producer funding output, and every input spends one of those or a producer funding output, read from the topic's previous coins (BRC-22), never from the BEEF; a spent output the topic does not hold refuses the token (`anchor-input`); every input unlocked by exactly one minimal push of a strict low-S DER signature and sighash `0x41` | signed |
| Canonical chain | interval 1: the first mined token (block height, then position); interval `k > 1`: the token that spends the canonical `k - 1` token | signed |
| Publication | funding trees, anchor tokens and sweeps are published to the object leg once, after they mine; carriers spend only mined trees | signed |
| Retraction | only a mined spend of a carrier's funding outpoint by another transaction retracts it | signed |
| Commitment | `C = txid(K)` in hash byte order | signed |
| CBOR | RFC 8949 section 4.2.1 core deterministic encoding, bcommon's subset, enforced on decode | signed |
| Admission | depends only on the object (the transaction and its BEEF, against the host's headers and producer policy), never on other objects held or arrival order, except that an anchor token's spent outputs are the topic's previous coins; a `seq`/`prev` link break between batches is a reported status (`gap`, `fork`), never a refusal | signed |

The carrier shape, script canonicality, signature and anchor token rows are
exercised by the transaction vectors (spec section 16).

## 3. The batch record (magic `blb` `0x01`)

| Item | Status |
| --- | --- |
| Key numbers and meanings: 0 `magic`, 1 `producer`, 2 `stream`, 3 `seq`, 4 `prev`, 5 `timestamp`, 6 `count`, 7 `encoding`, 8 `profile`, 9 `content`, 10 `locator`, 11 `size` | signed |
| Required keys: 0 to 8 and 11; key 9 exactly on profile 1, key 10 exactly on profile 2 | signed |
| Types: `producer` bytes(33) canonical compressed key; `stream` text; `prev` bytes(32); the rest unsigned integers or bytes as the spec tables | signed |
| `seq` starts at 1 and steps by 1; `prev` is the previous batch's commitment and all zero exactly on `seq = 1` | signed |
| `timestamp` is Unix milliseconds | signed |
| Integers at most 2^53 - 1 | signed |
| Unknown integer keys above 11 preserved and ignored; any non-integer key refuses the record; at most 64 keys | signed |
| The refusal order (spec section 3.2) and the reason labels the vectors carry; every refusal is decided by the record alone, and its `prev` rule compares `prev` only with the record's own `seq` | signed |
| The classifier: an output claims a batch when its first PushDrop field starts with a CBOR map head, key 0 and the batch magic; claims an anchor when its first field is `62 6c 01`; two claiming outputs refuse the transaction | signed |
| Profile values: `1` full-bytes, `2` digest-only; unknown values admitted as unsupported | signed |
| Encoding values: `1` `lines` (LF-terminated UTF-8 lines, `count` = LFs), `2` `lines+gzip` (one gzip member, nothing after it); unknown values admitted as unsupported | signed |
| `count <= size`; on `lines`, `size` equals the content length | signed |

## 4. The locator

| Item | Status |
| --- | --- |
| Member names `digest`, `length`, `urls`, all always present | signed |
| `digest` is unsalted SHA-256 of the content bytes as fetched (the UHRP hash) | signed |
| `urls`: 0 to 4 strings, each the lowercase bytes `https://` and 1 to 248 bytes in `0x21` to `0x7e` | signed |
| Any other member, of any key type, preserved; it makes the locator unsupported, never the batch | signed |

## 5. The anchor record (magic `bla` `0x01`) and the leaf rule

| Item | Status |
| --- | --- |
| Key numbers and meanings: 0 `magic`, 1 `producer`, 2 `stream`, 3 `interval`, 4 `prev`, 5 `first`, 6 `count`, 7 `root`, 8 `timestamp`; all required | signed |
| `interval` starts at 1 and steps by 1; `prev` is the previous anchor token's txid, all zero exactly on interval 1; interval 1 has `first = 1`; intervals partition the `seq` space contiguously | signed |
| Leaf datum is the batch commitment `C`; leaf `SHA-256(0x00 || C)`; node `SHA-256(0x01 || l || r)`; split at the largest power of two below `n`; no duplication; leaves in ascending `seq` | signed |
| The anchor record rides inside the mined token, not a separate carrier | signed |

## 6. Names and questions

| Item | Status |
| --- | --- |
| Stream identifier `<name>_<suffix>`, 12 to 43 bytes: `<name>` 1 to 32 bytes of `[a-z]` and single underscores, starting and ending with a letter; `<suffix>` exactly 10 letters `a` to `z`, drawn uniformly at random once at the stream's creation and fixed for its life; the topic `tm_log_<name>_<suffix>` is then at most 50 characters | signed |
| The full identifier is what records, lookup questions and topics carry; the readable part alone never identifies a stream | signed |
| Lookup classes, both free on every conforming host: `{stream, producer, seq}` and `{stream, producer, interval}` | signed |
| Query member names `stream`, `producer`, `seq`, `interval`; validated as parsed: `stream` the full identifier; producer 66 lowercase hex meeting the key rule; `seq` and `interval` numbers whose double value is an integer from 1 to 2^53 - 1 | signed |
| An unknown or missing query member is refused, never ignored; a priced class is never a superset of a free one | signed |

## 7. Bounds that are a floor

Readers accept every record within these, and a later version may raise
them but never lower them: content 262144 bytes, batch record 266240 bytes,
64 keys per record, decoded size 16777216 bytes, located length 16777216
bytes, anchor record 1024 bytes, 65536 batches per interval, 4 URLs of 256
bytes.
Signed as floors; the values themselves may rise later without a new
version. Content of 262144 bytes (256 KiB) is the minimum every reader
accepts.

## 8. Not frozen

- The funding tree's size and output value, and the anchor cadence.
- Host policy: producer sets, how long batches are kept, fetch allow lists,
  metric names. (Retaining every spent topic coin in storage is not policy:
  the spec requires it.)
- The cap of 8 fork candidates per `seq` and 8 anchors per interval, which
  a later version may raise.
- The lookup service's answer order.
- Anything the spec marks as a host's or a producer's choice.
