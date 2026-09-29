# blogs: the log stream contract

blogs carries log batches as overlay objects on the BEEF object plane. A
producer appends once; every archive subscribed to the stream's topic
receives the same signed, numbered batch; one mined anchor per interval
commits the interval's batches, so an archive proves its copy complete and an
auditor proves time. An archive is an ordinary overlay host running the
`tm_log_<name>_<suffix>` topic manager and the `ls_log` lookup service in
front of whatever storage it already has.

This document is normative. MUST, MUST NOT, SHOULD and MAY are used as in
RFC 2119. Every byte-level and name-level choice that cannot change once a
record is on a public host is listed in [frozen.md](frozen.md). The golden
vectors in `testdata/vectors` are part of the contract (section 16).

## 1. Objects

| Object | On chain | On the plane | What it is |
| --- | --- | --- | --- |
| **Batch record** `B` | never | inside a carrier | canonical CBOR: one batch of log records, or a locator for it (section 3) |
| **Carrier** `K` | never mined | a BEEF object | one unmined transaction whose one output holds `B` (section 6) |
| **Commitment** `C` | no | no | `C = txid(K)`, 32 bytes, hash byte order |
| **Funding tree** `F` | mined | a BEEF object | one transaction of tagged outputs; each carrier spends one |
| **Anchor record** `A` | yes, in the token | inside the token | canonical CBOR: an interval's range and root (section 5) |
| **Anchor token** `T` | mined | a BEEF object | a signed PushDrop `[tag, A]`; each interval's token spends the previous interval's |

Mine the state, not the data: batches are never mined; each interval costs
one mined anchor, and each funding tree one mined transaction.

```text
   funding tree F (mined) ──┬─ out 0 ─▶ K1 (seq 1, prev 0)
                            ├─ out 1 ─▶ K2 (seq 2, prev C1)
                            └─ out 2 ─▶ K3 (seq 3, prev C2)
                                          │
   interval 1: A{first 1, count 3, root = RFC6962(C1, C2, C3)}
   T1 (mined) ◀── spends ── T2 (mined, interval 2, prev txid(T1))
```

A stream is identified by its stream identifier `<name>_<suffix>`: a
readable name and a random suffix fixed when the stream is created
(section 7.1). Any number of producers may publish to one stream; each producer's batches form their own sequence, and the stream's
topic is the rendezvous. Everything below that is "per stream" is per
`(producer, stream)` pair unless it says otherwise.

## 2. Conventions

- **CBOR.** Every record is one CBOR map in the core deterministic encoding
  of RFC 8949 section 4.2.1, written and read by bcommon's `cbor` package,
  which enforces it on encode and on decode. The subset is unsigned and
  negative integers, byte strings, UTF-8 text strings, arrays, maps, `false`,
  `true` and `null`; tags, floats, indefinite lengths and nesting deeper
  than 16 are refused. One value therefore has exactly one encoding a reader
  accepts. CBOR is used, rather than a fixed byte layout, because every
  application on the same substrate writes its records this way and shares
  one codec in Go and TypeScript, and because a map keyed by integers lets a
  later version add a field without breaking a reader.
- **Record keys.** A record's top-level keys are unsigned integers, at most
  64 of them. A key this version defines is checked as its table says. A key
  above the last defined one is unknown: a reader MUST preserve it verbatim
  when it re-encodes and MUST otherwise ignore it.
- **Hashes.** SHA-256 unless stated. A commitment or txid in a record is in
  hash byte order (the order SHA-256d produces, the reverse of the usual
  display hex).
- **Integers** that a record or a question carries are at most
  2^53 - 1 (`MaxSafe`), so they are exact as JSON numbers and as JavaScript
  numbers.
- **Bounds before allocation.** A reader MUST check an encoded record's
  length against its bound before decoding it, and the decoder checks every
  declared length against the bytes present before it allocates for it.
- **Keys.** An identity key is a compressed secp256k1 point: 33 bytes,
  prefix `0x02` or `0x03`, an x-coordinate below the field prime, and a point
  on the curve. A reader MUST refuse any other byte string, including an
  x-coordinate at or above the prime that some parsers reduce and accept
  (the `producer-x-is-p-plus-1-alias` vector), because that would give one
  producer two names.
- **Signatures** a host checks (field signatures and the input signatures of
  section 8.1) are strict DER with a low S value. A producer's wallet writes
  them that way; a high-S or loosely encoded copy is a different byte string
  for the same signature and is refused.
- **Time.** Timestamps are Unix time in milliseconds (UTC) on the producer's
  clock. They are claims, never proofs: time is proven only by the block that
  mines an anchor (section 13).

## 3. The batch record

Magic `"blb"` `0x01`. The payload of a carrier's one output.

| Key | Field | Type and bounds | Rule |
| --- | --- | --- | --- |
| 0 | `magic` | bytes(4) | `62 6c 62 01` (`"bl"`, `'b'` for batch, version 1). Always the first key |
| 1 | `producer` | bytes; the key rule (section 2) | the producer's identity key |
| 2 | `stream` | text, 12 to 43 bytes; the stream identifier grammar (section 7.1) | the full `<name>_<suffix>`; MUST equal the topic's stream |
| 3 | `seq` | uint, 1 to 2^53 - 1 | the producer's sequence number on this stream; the first batch is 1, each next batch is `seq + 1` |
| 4 | `prev` | bytes(32) | the commitment `C` of the producer's batch `seq - 1` on this stream; all zero exactly when `seq = 1` |
| 5 | `timestamp` | uint, 1 to 2^53 - 1 | when the producer closed the batch, Unix milliseconds. A claim |
| 6 | `count` | uint, 1 to `size` | the number of log records in the decoded content |
| 7 | `encoding` | uint, 1 to 2^53 - 1 | how the content is encoded (section 3.1) |
| 8 | `profile` | uint, 1 to 2^53 - 1 | `1` full-bytes, `2` digest-only (section 9) |
| 9 | `content` | bytes, 1 to 262144 | the content as encoded. Present exactly when `profile = 1` |
| 10 | `locator` | map; section 4 | where the content is. Present exactly when `profile = 2` |
| 11 | `size` | uint, 1 to 16777216 | the decoded content's length in bytes |

Keys 0 to 8 and 11 are always present. Keys 12 to 63 and above are unknown
to this version (section 2). The encoded record is at most 266240 bytes
(`MaxBatchRecord`, the content bound plus 4 KiB).

A `profile` or `encoding` value this version does not define is not a
refusal: the record is a valid link in its stream (section 8), and its
content is reported unsupported. Keys 9 and 10, if present, still have the
types above, and no rule ties them to an unknown profile.

### 3.1 Encodings

| Value | Name | Content |
| --- | --- | --- |
| 1 | `lines` | UTF-8 text, one log record per line, every line terminated by LF (`0x0A`), the last included. `count` is the number of LFs |
| 2 | `lines+gzip` | `lines`, compressed as exactly one gzip member (RFC 1952) with nothing after it |

Checked by whoever decodes the content (an archive's storage adapter, a
reader), not at admission (section 8): the decoded bytes are exactly `size`
long, end in LF and hold exactly `count` LFs. A decoder MUST stop producing
output at `size + 1` bytes, so a compressed bomb costs at most
16 MiB plus one byte. A producer SHOULD write the gzip header with `MTIME`
zero and no `FNAME`, `FCOMMENT` or `FEXTRA`; a reader never displays gzip
header fields. Gzip output is not canonical (one text has many
compressions); that is harmless, because the signature and the commitment
bind the exact bytes carried.

Structured logs are lines: one JSON object per line is `lines` content.
Content is not required to be valid UTF-8 at admission; a renderer filters it
(section 10).

### 3.2 The refusal order

A record that breaks several rules is refused for the first, so that every
implementation reports the same reason. The steps, in order:

1. `too-large`: the encoded length is over the record's bound;
2. `cbor`: the bytes are not exactly one canonical item of the subset, or
   the item is not a map;
3. `too-large`: the map has more than 64 entries;
4. `key-type`: a key is not an unsigned integer;
5. `magic`: key 0 is absent, is not a byte string, or is not the magic;
6. each defined key in ascending order: `missing` if a required key is
   absent, then `type` if it has the wrong CBOR type, then the key's own
   rule: `producer` for key 1, `stream` for key 2, `locator` for key 10, and
   `range` for every other bound in the table;
7. across fields, in order: `prev` (zero exactly on `seq = 1`), `range`
   (`count <= size`), `profile-shape` (profile 1 has `content` and no
   `locator`; profile 2 the reverse), `content` (for profiles 1 and 2 with
   `encoding = 1`, `size` equals the content's length: the carried
   `content`, or the locator's `length`).

The anchor record (section 5.1) follows the same steps with its own keys;
its step 7 is `prev` (zero exactly on `interval = 1`), then `range` (`first
= 1` on interval 1, and `first + count - 1` at most 2^53 - 1).

## 4. The digest-only locator

The value of batch key 10. A map whose three known members are always
present (an optional member would give one locator two encodings):

| Member | Type and bounds | Rule |
| --- | --- | --- |
| `digest` | bytes(32) | SHA-256 of the content bytes exactly as fetched (encoded, before decompression). This is the hash a BRC-26 UHRP URL encodes |
| `length` | uint, 1 to 16777216 | the content's length in bytes as fetched |
| `urls` | array of 0 to 4 text strings | fetch hints. Each is the lowercase bytes `https://` followed by 1 to 248 bytes in `0x21` to `0x7e` (printable ASCII, no space) |

A known member that is absent, of the wrong type, or out of bounds refuses
the batch with `locator`, because the profile's shape is broken. Any other
member, whatever its key type, name or value and however many there are, is
preserved verbatim and makes the **locator** unsupported: the archive does
not fetch through it, and the batch is still admitted and linked.

The digest is SHA-256 of the content, unsalted, so that the same hash names
the content in UHRP and here. A digest of guessable content confirms a guess;
a producer that must hide low-entropy content puts a random line in it
(section 15).

How an archive fetches is section 9.2.

## 5. The interval anchor

### 5.1 The anchor record

Magic `"bla"` `0x01`. The second field of an anchor token.

| Key | Field | Type and bounds | Rule |
| --- | --- | --- | --- |
| 0 | `magic` | bytes(4) | `62 6c 61 01` (`"bl"`, `'a'` for anchor, version 1) |
| 1 | `producer` | bytes; the key rule | the producer's identity key |
| 2 | `stream` | text, 12 to 43 bytes; the stream identifier grammar | the full `<name>_<suffix>`; MUST equal the topic's stream |
| 3 | `interval` | uint, 1 to 2^53 - 1 | the interval number; the first is 1, each next is `interval + 1` |
| 4 | `prev` | bytes(32) | the txid, hash byte order, of the mined transaction holding the anchor token of `interval - 1`; all zero exactly when `interval = 1` |
| 5 | `first` | uint, 1 to 2^53 - 1 | the first `seq` the interval covers; exactly 1 when `interval = 1` |
| 6 | `count` | uint, 1 to 65536 | the number of batches covered: `first` to `first + count - 1`, which is at most 2^53 - 1 |
| 7 | `root` | bytes(32) | the interval root (section 5.2) |
| 8 | `timestamp` | uint, 1 to 2^53 - 1 | when the producer closed the interval, Unix milliseconds. A claim |

All of keys 0 to 8 are present; keys above 8 are unknown to this version.
The encoded record is at most 1024 bytes.

Intervals partition a producer's stream: interval `k + 1` has
`first = first_k + count_k`. An interval with no batches is not anchored;
the next anchor covers the next batches. So every `seq` is covered by exactly
one interval once the producer has anchored it.

### 5.2 The leaf rule and the root

The root is bcommon `commit`'s RFC 6962 Merkle Tree Hash over the covered
batches' commitments in ascending `seq` order:

- **leaf datum** `d_i = C` of the batch with `seq = first + i` (its carrier
  txid, hash byte order, 32 bytes);
- **leaf** `SHA-256(0x00 || d_i)`;
- **node** `SHA-256(0x01 || left || right)`, splitting `n` leaves at the
  largest power of two strictly below `n`; the last leaf is never duplicated.

The leaf datum is the commitment, not a hash of the record, because the
commitment is what the plane, the engine's storage and the lookup answer
already key a batch by, and because it binds the whole carrier: record,
funding input and signatures. An inclusion path for the batch at `seq` has
index `seq - first`; a verifier MUST check that the path's shape proves that
index in a tree of `count` leaves, since an RFC 6962 path verifies only the
leaf and the root and a path for another position would otherwise be taken
for this one. A path is a list of siblings from the leaf upward, each with its
side.

### 5.3 The anchor token

A mined transaction with exactly one output that carries a lock-before
PushDrop of two fields and the producer's field signature (its other outputs,
such as change, are the producer's business):

```text
<derive(producer, "anchor")> OP_CHECKSIG <62 6c 01> <A> <signature> OP_2DROP OP_DROP
```

- field 1 is the tag `"bl"` `0x01`; field 2 is the anchor record `A` as
  encoded; the signature is the wallet's DER signature over
  `SHA-256(field 1 || field 2)` under the locking key;
- the output holds 1 satoshi;
- for `interval > 1` the transaction MUST spend the anchor token output of
  `interval - 1`, whose transaction's txid is `prev`; for `interval = 1` it
  MUST spend one of the producer's funding outputs (section 6.3). Either way
  only the producer's key can make an anchor, and two anchors spending one
  predecessor are a double spend of which at most one is ever mined;
- every input's unlocking script is push-only: minimal pushes, each
  signature strict low-S DER with its sighash byte, so the transaction cannot
  be re-encoded under another txid by anyone but its signers;
- it is published once, after it mines, with its proof (section 6.3). The
  producer's next anchor names the mined txid in `prev` and spends the mined
  output.

The anchor record rides in the token itself, not in a separate carrier,
because an auditor then needs only the token's transaction, its block header
and the batches: the anchor is the state, and it is small.

## 6. Carriers, funding trees and derivations

Everything here is bcommon's, parameterised by the values in section 6.1.

### 6.1 Derivations and tags

| What | Value |
| --- | --- |
| BRC-43 protocol | `[1, "blogs"]` (security level 1) |
| Carrier record output, carrier field signature, funding-tree outputs | key id `batch`, invoice `1-blogs-batch` |
| Anchor token lock and field signature | key id `anchor`, invoice `1-blogs-anchor` |
| Embedded wallet funding key | key id `fund`, invoice `1-blogs-fund` |
| Anchor token tag | `62 6c 01` (`"bl"` `0x01`) |
| Funding-output tag | `62 6c 02` (`"bl"` `0x02`) |

Every derivation is BRC-42 with counterparty `anyone`: the producer's wallet
derives with `forSelf = true`, and a reader derives the same public key from
the producer's identity key alone. That is the one setting under which a
reader can recompute a locking key and the embedded field signature verifies
under it. The protocol name is five characters because BRC-43 key derivation
refuses a shorter one.

### 6.2 The carrier

Built by bcommon `carrier.Mint` and checked by `carrier.Validate` plus the
rules of section 8.1:

- **exactly one input**, spending one of the producer's funding-tree
  outputs, `nSequence = 0`, unlocked by exactly one push of the producer's
  signature (strict DER, low S, sighash byte `0x41`);
- **exactly one output**, a lock-before PushDrop of one field, the batch
  record, with the field signature, locked to `derive(producer, "batch")`,
  carrying the whole value of the output it spends (fee zero);
- **`nLockTime = 4102444800`** (the first second of the year 2100, UTC) with a non-final
  input, so it cannot be mined: BRC-60's non-final device, used to keep the
  batch off the chain while SPV still verifies it through its funding parent.

`C = txid(K)` is the batch's commitment. It is what the next batch's `prev`
names, what the anchor's leaves are made of, and what the plane and every
host key the batch by. The unlocking-script rule is what keeps `C` the
producer's: script verification does not require a canonical unlocking
script, so without it anyone could rewrite the input's script (prefix a push
and a drop, or negate S) and present the same record under a new txid.

### 6.3 Funding trees and publication

A funding tree is a mined transaction of equal outputs, each a PushDrop of
the one field `62 6c 02` with no signature, locked to
`derive(producer, "batch")`:
`<key> OP_CHECKSIG <62 6c 02> OP_DROP`. Each carrier spends one, so every
carrier's ancestry is one mined parent and a proof, and no batch's object
grows with the stream. Order lives in `seq` and `prev`, not in the spend
graph. bcommon `producer` runs the tree's lifecycle (spend, mint the next,
settle, keep the unproven BEEF, collect proofs and republish), and the tree's
size is the producer's choice: the carrier's BEEF carries the tree
transaction, so the tree's size is per-batch overhead.

Mined transactions (funding trees, anchor tokens, sweeps) go to the
settlement leg for mining and, once mined, as BEEF with their proof to the
stream's topic on the object leg, exactly once. A producer MUST NOT publish
one of them to the object leg before it mines: a host admits funding outputs
and anchor tokens only from mined transactions (section 8.1), and an overlay
engine treats a second publication of the same transaction as a duplicate
and does not show its proof to the topic manager or the lookup service, so
the mined state of a transaction first seen unmined would never reach the
index. A producer therefore mints its next funding tree before the current
one runs out (mint-ahead), so batches do not wait for a block at a tree's
rollover. Mint-ahead is added to bcommon `producer` as a change of its own,
and a blogs producer is built only on a bcommon version that has it. A
carrier spends only a mined tree's outputs; that also means no carrier can be
stranded by a different version of its parent being mined.

A carrier is published once, to the object leg only, naming
`tm_log_<name>_<suffix>` as its first topic.

### 6.4 Retraction

A producer retracts batches by sweeping a funding tree: one mined
transaction spending the tree's outputs, used and unused (bcommon
`carrier.Sweep`, whose output 0 is a funding-shaped tombstone). Once it is
mined, every carrier that spent one of those outputs is a double spend and
can never be valid again. A producer that only wants unused value back spends
only the unused outputs; that retracts nothing.

Retraction removes bytes, not positions: section 8.4.

## 7. Topics and lookup

### 7.1 Stream identifiers and topics

A stream's topic is `tm_log_<name>_<suffix>`, and its **stream identifier**
is `<name>_<suffix>`:

- `<name>` is the readable part: 1 to 32 bytes of lowercase ASCII letters
  (`a` to `z`) and single underscores, starting and ending with a letter;
- `<suffix>` is exactly 10 lowercase ASCII letters, each drawn uniformly at
  random from `a` to `z` (26^10 values, about 2^47) by the producer that
  creates the stream, once, when it creates it. It never changes for the
  stream's life, and a producer MUST NOT choose it other than by a uniform
  random draw.

The identifier is therefore 12 to 43 bytes. The suffix has no underscore,
so the identifier splits at its last underscore. The whole topic is a BRC-87
topic manager name: lowercase letters and underscores, no leading, trailing
or doubled underscore, and at most 50 characters. The prefix `tm_log_` takes
7 characters, the separating underscore 1 and the suffix 10, which leaves at
most **32 characters for `<name>`**. The `tm_log_` namespace belongs to
blogs.

The identifier, never the readable part, is what every record's `stream`
field, every lookup question and every topic carries. A reader MUST NOT
treat the readable part alone as identifying a stream, and a renderer that
shows the readable part SHOULD show the suffix beside it.

**Why a suffix.** The object plane is shared: anyone may publish to any
topic, and a topic is only a name. Readable names collide (every operator
would like `app`, `web` or `audit`), and two unrelated users on one topic
would receive each other's batches in their archives and their lookup
answers. Admission would still keep their batches apart by producer key, but
their subscriptions, storage and answers would not be. A suffix drawn at
random once per stream makes an accidental collision negligible (about
2^-47 for any one pair of streams) at the cost of 11 characters of the
topic. It is not a secret and not an access control: the suffix is public on
every host that carries the topic, and anyone who knows it can publish to
it; producer policy (section 8.1) is what restricts who is admitted.

### 7.2 The lookup service `ls_log`

One lookup service per host answers for every stream the host carries. It
answers BRC-24 questions of exactly two classes, both free on every
conforming host:

| Class | Question (`query` object) | Answer (`output-list`) |
| --- | --- | --- |
| batch | `{"stream": s, "producer": p, "seq": n}` | the admitted, unretracted batch carriers of `p` on `s` at `seq = n`: normally one, at most 8 (section 8.2) |
| interval | `{"stream": s, "producer": p, "interval": k}` | the admitted anchor tokens of `p` on `s` for interval `k`: normally one, at most 8 (section 8.3) |

The query is validated as parsed, so hosts in any language reach the same
verdict: a member given twice takes its last value, as JSON parsers do; `s`
is a string that meets the stream identifier grammar (section 7.1), the full
`<name>_<suffix>`, so a readable part alone is refused; `p` is a string of 66 lowercase
hex characters whose bytes meet the key rule (section 2); `n` and `k` are
numbers whose value, as an IEEE 754 double, is an integer from 1 to
2^53 - 1. A question MUST carry exactly the members of one class. A missing
member, a member no class defines, or a value that breaks these rules is
refused with an error, never answered empty and never ignored: ignoring an
extra member would let a priced question alias a free one. A class that is
ever priced (a BRC-105 402 on the lookup route) MUST be a different question,
never a superset of these two.

The batch class is how an archive fills a hole from a sibling. The interval
class is how an archive or an auditor checks completeness against the
anchor. There is no class for "the newest batch", ranges, inclusion paths or
search; those belong to a later archive contract. An empty answer means only
that this host holds nothing that answers: a reader MUST NOT read it as proof
that the batch or the interval does not exist.

The engine hydrates each answered output as BEEF: the batch record is inside
the carrier's BEEF, the anchor record inside the token's.

## 8. The host

### 8.1 Admission by `tm_log_<name>_<suffix>`

Admission depends only on the object: the transaction and its BEEF, checked
against the host's block headers and its configured producer policy, never
against what else the host holds or the order in which objects arrived. A
break in the `seq`/`prev` link between batches (no batch held at `seq - 1`,
a `prev` that names no held batch, two batches at one `seq`) is therefore
never a refusal: it is a reported status, `gap` or `fork` (section 8.2).
The `prev` rules of a record's own decoding (section 3.2) compare `prev`
with the same record's `seq` or `interval`, never with another object. Every
archive with the same configuration therefore admits the same set, whatever
the plane's delivery order, repair or lateness did. SPV of the BEEF (scripts, amounts,
the funding parent's proof or unproven ancestry) is the engine's and runs
before the topic manager. Checks then run in the order given; a transaction
is refused for the first rule it breaks, with that rule's reason.

Every script comparison below is byte for byte against the script rebuilt
from the decoded fields and the derived key, exactly as bcommon `pushdrop`
writes it: the 33-byte compressed key, `OP_CHECKSIG`, each field as one
minimal push, the signature as one minimal push when the output is signed,
and the fewest `OP_2DROP` then `OP_DROP` that clear the fields. A script that
decodes to the same fields in any other form is not the same script.

**Classify.** An output **claims a batch** when it is a PushDrop whose first
field is a CBOR map head followed by key 0 and the batch magic
(`00 44 62 6c 62 01`). An output **claims an anchor** when it is a PushDrop
whose first field is `62 6c 01`. A transaction with more than one claiming
output of either kind is refused (`carrier-shape` or `anchor-shape`).
Outputs that claim nothing are skipped, and a transaction whose outputs claim
nothing is handled as a funding tree or a spend (below).

**Batch carrier.** A transaction with an output that claims a batch:

1. `carrier-shape`: it does not have exactly one input and exactly one
   output, or the output's value differs from the input's source output
   value;
2. the record's own rules (section 3.2), with their reasons;
3. `stream`: the record's `stream` is not this topic's;
4. `producer-policy`: the host restricts this stream to a producer set and
   the producer is not in it (below);
5. `mineable`: `nLockTime < 4102444800`, or `nSequence = 0xFFFFFFFF`;
6. `lock`: the output script is not the well-formed one-field signed PushDrop
   locked to `derive(producer, "batch")`;
7. `signature`: the field signature is not strict low-S DER, or does not
   verify under that key;
8. `funding`: the input's source output (from the BEEF) is not a well-formed
   funding output `62 6c 02` locked to `derive(producer, "batch")`;
9. `unlock`: the input's unlocking script is not exactly one minimal push of
   a strict low-S DER signature followed by the sighash byte `0x41`.

Rules 5 to 7 are bcommon `carrier.Validate`'s order. Rules 8 and 9 make the
commitment the producer's alone: rule 8 means only the producer's key can
spend the input, and rule 9 means nobody else can re-encode that spend under
another txid. With both, a second batch at a producer's `seq` can only be a
batch the producer signed.

**Anchor token.** A transaction with an output that claims an anchor:

1. `anchor-shape`: the claiming output is not a PushDrop of exactly two
   fields plus a signature, or does not hold 1 satoshi (other outputs are
   allowed; two claiming outputs were refused at classification);
2. the anchor record's own rules (section 3.2), with their reasons;
3. `stream`: not this topic's;
4. `producer-policy`: as for a batch;
5. `lock`: the output script is not the well-formed two-field signed PushDrop
   locked to `derive(producer, "anchor")`;
6. `signature`: the field signature is not strict low-S DER, or does not
   verify;
7. `anchor-input`: for `interval > 1`, no input spends an output of txid
   `prev` whose source output (from the BEEF) is a well-formed anchor token of
   this producer for this stream and interval `interval - 1`; for
   `interval = 1`, no input spends a well-formed funding output locked to
   `derive(producer, "batch")`;
8. `unmined`: the transaction has no proof in the BEEF that verifies against
   the host's headers (a host whose headers lagged, and so refused a mined
   anchor, recovers it later from a sibling by the interval class);
9. `unlock`: an input's unlocking script is not push-only in the section 5.3
   form.

The rules speak of a "well-formed" anchor token or funding output: one whose
script matches the rebuilt script as above. Nothing in admission depends on
which anchors the host already holds.

**Funding tree or sweep.** A transaction whose outputs claim nothing, that
has a proof in the BEEF verifying against the host's headers, and that has at
least one well-formed unsigned funding output `62 6c 02`: its funding outputs
are admitted, so that a later spend of one is seen, and its inputs are
recorded (a sweep's tombstone is such an output). An unmined one admits
nothing. Admitting only mined funding outputs keeps a stranger from filling a
topic with free unmined outputs, since the engine does not refuse an unmined
double spend.

**Spend.** A transaction whose outputs claim nothing and that spends outputs
the topic holds is accepted for its inputs alone.

**Retention and spends.** For every transaction it accepts, the topic
manager lists every input that spends an output the topic holds as a coin to
retain, so no admitted output is deleted when something spends it: anchor
tokens stay answerable after the next interval spends them, and a funding
output stays in storage marked spent. The lookup service does not rely on the
engine's spend notices; it reads each admitted transaction's inputs itself
and persists, per outpoint, the set of transactions that spent it.

**Producer policy.** An open stream admits any producer. A host MAY restrict
a stream to a configured set of producer keys. Openness is configured
explicitly, never inferred: an empty producer set is a configuration error,
not an open stream.

A host counts refusals by reason. It MUST NOT evict unproven carriers:
carriers are unproven by design. As a second layer, the lookup service reads
a transaction's mined status from the BEEF the engine stores for it whenever
it joins, answers or restores, never from a notice it may not receive.

### 8.2 The link index

The lookup service keeps, per `(producer, stream)`, the admitted batches by
`seq`. Admission has already verified the producer's signature over `seq`
and `prev` and their own rules (section 3); the link between two batches is
checked here, over the whole admitted set, and its outcome is a status,
never a refusal. A retraction receipt (section 8.4) counts as a held batch.
When several statuses apply, the first in the table wins:

| Status of a batch | When |
| --- | --- |
| `retracted` | its funding output was spent by a mined sweep (section 8.4) |
| `fork` | another admitted batch has the same `seq` and a different `C`, or batches are held at `seq - 1` and none has `C = prev` |
| `gap` | no batch is held at `seq - 1`: the numbers between the highest contiguous `seq` held and this one are reported as a hole |
| `linked` | `seq = 1`, or a batch is held at `seq - 1` whose `C` equals this `prev` |

A status is recomputed whenever a batch at `seq - 1`, `seq` or `seq + 1`
arrives or is retracted, so a late batch closes its gap. Because the status
is a function of the set and not of the order the set arrived in, a restart
that rebuilds the index from storage in any order reaches the same statuses.

A gap is never refused: a late batch or a sibling's backfill fills it. A
fork is never refused either: with the carrier rules of section 8.1 only the
producer's key can make two batches for one `seq`, and every archive keeps
the candidates as evidence of it. The index keeps and answers at most 8
candidates per `seq`, the first 8 by commitment in hash byte order, and
counts the rest. The anchor decides which candidate the stream contains
(section 8.3). A producer that equivocates, or floods its stream, is a
producer an archive stops subscribing to or restricts by policy; storage
spent on its objects is the archive operator's, as with any subscription.

### 8.3 Anchors and completeness

The lookup service joins anchor tokens per `(producer, stream)`. Every
admitted token is mined, and admission (section 8.1 rule 7) already
guarantees that a token for interval `k > 1` spends a token of interval
`k - 1` whose txid is its `prev`, and that interval 1 is funded by the
producer. The join adds that `first` equals the predecessor's
`first + count`; a token that fails it is kept and reported, never used. A
token whose predecessor has not arrived waits for it.

The **canonical** chain: interval 1's canonical token is the one mined first
(lower block height, then lower position in the block); interval `k > 1`'s
canonical token is the one that spends the canonical token of `k - 1`, of
which there is at most one, because two spends of one output cannot both
mine. A producer that mines two interval-1 tokens starts two chains; the
second is reported as `anchor-branch` and never used. The service keeps at
most 8 tokens per interval, canonical first, then by txid in hash byte
order.

An interval is **complete** at an archive when the archive holds, for every
`seq` from `first` to `first + count - 1`, a batch or a retraction receipt,
and `commit.Root` over their commitments in `seq` order equals the canonical
token's `root`. Where a `seq` has fork candidates, the next batch's `prev`
selects one inside the interval, and for the interval's last `seq` the
archive tries each candidate (at most 8). If every position is held and no
choice reproduces the root, the status is `root-mismatch`: the producer
anchored commitments it did not publish, or published commitments it did not
anchor; the interval is never complete. The root is recomputed when a late
batch or a backfill arrives. An interval is **proven** when it is complete and
its canonical token's proof verifies against the archive's own block
headers.

### 8.4 Retraction

The service records the funding outpoint each carrier spends. When a mined
transaction other than that carrier spends it (a sweep, which the producer
publishes once, mined, and the topic admits for its tombstone and its inputs,
seen live or found in storage on restart), the carrier is retracted:

- the archive drops the batch's bytes and stops serving the carrier: the
  batch class answers nothing for it;
- it keeps a **receipt**: producer, stream, `seq` and `C`;
- the `seq` stays a numbered position. The anchor still commits to `C`, and
  the receipt's `C` is what completes the interval's root.

Only a mined spend retracts. An unmined spend of a funding output is not a
retraction, because a carrier cannot be a double spend of a transaction
that may never mine.

An archive that never held a batch before it was retracted recovers its
commitment from the next batch's `prev`, when it holds that batch. A
retracted batch that is the last of its interval, or followed only by
retracted batches to the end of it, leaves a commitment the archive cannot
recover from a sibling (siblings serve nothing for it); the archive reports
that interval as complete except for those retracted positions, and any
archive that held the receipts proves the interval.

### 8.5 Restore

The engine does not replay admissions. On start the service rebuilds its
index from the topic's stored outputs and the spender sets it persisted:
batches by `(producer, stream, seq)` and their statuses from the set
(order-independent, section 8.2); retractions from mined spends of carrier
funding outpoints; anchor tokens by interval, each interval's canonical
anchor chosen by section 8.3's rule, whatever order storage returns them in.

## 9. Profiles

### 9.1 Full-bytes (`profile = 1`)

The content rides in the record, so every archive on the topic receives the
data at the same moment. The recommended profile for archiving.

### 9.2 Digest-only (`profile = 2`)

The record carries the locator; the plane proves and orders, and the bytes
move out of band, for content that must stay in a jurisdiction or that a
producer will not put on the plane. An archive fetching the content:

- MUST NOT fetch from a URL merely because a record names it: `urls` are
  hints, used only when their host is on the archive's configured allow list;
  otherwise it resolves the digest through its configured resolver (for
  example BRC-26 UHRP by the digest);
- fetches over HTTPS only, follows no redirect off the origin, and reads at
  most `length` bytes, and never more than its own budget;
- checks the length, then the SHA-256 digest, and discards bytes that fail;
  unverified bytes are never stored or served;
- treats a failed fetch as content pending: the batch is admitted and linked
  regardless, and retrying is the archive's policy.

A stream MAY mix profiles batch by batch.

## 10. Rendering and terminal safety

Log content is text someone else wrote. Nothing in a record may drive a
terminal or a page: every renderer MUST filter content, stream identifiers, URLs
and any other record text before display, stripping control characters,
escape sequences, bidirectional and zero-width characters, and bounding the
output (bcommon `termsafe.Sanitize`; a web renderer escapes for HTML and
applies the same character rules). Content is carried and stored verbatim;
filtering happens at display, never at admission, so the stored bytes stay
the bytes the producer signed. The `batch-lines` vector carries an escape
sequence for exactly this reason. A producer SHOULD check its own lines with
`termsafe.Validate` before publishing, and MAY refuse to publish lines that
fail.

## 11. Size bounds

| Quantity | Bound | Why |
| --- | --- | --- |
| Batch content carried (`profile = 1`) | 262144 bytes (256 KiB) | the design point of about one second of a busy stream, compressed |
| Batch record, encoded | 266240 bytes | content plus 4 KiB for the other fields and unknown keys |
| Entries in a record map | 64 | bounds the work of one record |
| Decoded content (`size`) | 16777216 bytes (16 MiB) | bounds a decompression; 64:1 over the carried bound |
| Located content (`length`) | 16777216 bytes | the decoded bound applies whatever the profile |
| Anchor record, encoded | 1024 bytes | the fields need about 150 |
| Batches per interval | 65536 | bounds a root recomputation |
| Fork candidates kept and answered | 8 per `seq`, 8 anchor tokens per interval | bounds storage, answers and root searches |
| Carrier object on the plane | the plane operator's object bound (BRC-149) | a producer MUST check the carrier's BEEF (record, funding tree, proof or unproven ancestry) against it before publishing |

A reader accepts every record within these bounds. A later version may raise
a record bound, which is a coordinated change of producers, hosts and
readers; it never lowers one, so a record published under a bound stays
readable. Content larger than the decoded bound belongs to a later profile
that names a chunked store (BRC-167).

## 12. Repair and lateness

The plane repairs loss for every object it carries; nothing in blogs
disables, bypasses or asks for an exception from repair, for any profile,
stream, class or configuration.

- **Producer.** Publishes each carrier once. It does not retransmit a batch
  for loss; the network repairs. It republishes nothing but a
  carrier it built and may not have published before a crash (same bytes,
  same txid, so hosts deduplicate). It publishes each mined transaction
  exactly once, after it mines (section 6.3). A producer persists its head
  (`seq`, `C`) and the carrier bytes durably before publishing, and after a crash republishes the persisted
  carrier rather than building a new one for the same `seq`, which would be
  a fork. Its anchor head is the current anchor token, which its wallet keeps
  in a basket. A producer that has lost its head MUST NOT restart at
  `seq = 1` and MUST NOT guess the head from lookup answers (an empty answer
  proves nothing); it creates a new stream, with a new random suffix and so
  a new topic.
- **Archive.** Lateness is never a refusal. A batch or an anchor is admitted
  whenever it arrives, before or after its neighbours, before or after the
  interval's anchor, and the interval's status is recomputed. A hole that
  repair did not fill (the archive was down, or a loss outlived the repair
  window) is filled by the batch class from a sibling archive, without the
  producer. The anchor is what turns "probably everything" into a proof, and
  a missing position into a fetch.
- **Timestamps** play no part in admission or linking. An archive MAY report
  a producer timestamp that goes backwards along a stream; it proves nothing
  either way.

## 13. Verification

**One record, by an auditor holding block headers:**

1. the carrier `K` (from any archive, by the batch class): SPV through its
   funding parent against the auditor's own headers; the section 8.1 batch
   rules; `C = txid(K)`;
2. the anchor covering `seq`: intervals are numbered in `seq` order, so the
   auditor walks the interval class from an interval it holds (1, or one it
   verified earlier) forward, each anchor's `first + count` telling it whether
   `seq` is reached; an empty answer ends nothing, it only means this host
   does not hold that interval;
   the token is SPV-verified, mined, and proven against the auditor's
   headers; the section 8.1 anchor rules; same `producer` and `stream`;
   `first <= seq <= first + count - 1`;
3. the interval's commitments: the auditor asks the batch class for every
   `seq` the anchor covers (or takes them from an archive's export, a later
   contract), recomputes the root, requires it to equal `root`, and so
   obtains `C`'s inclusion at index `seq - first`; with a path in hand from
   elsewhere, it checks the path's shape for that index in a tree of `count`
   leaves;
4. for a digest-only batch, the fetched bytes against the locator.

The result proves that the producer's key signed the record and committed it
at that position of its stream no later than the block that mined the anchor.
It does not prove the record is true.

**An interval, by an archive:** complete and proven as section 8.3 says.

**The chain of anchors:** each interval's token spends the previous one's and
names its txid in `prev`, and `first` continues the previous `first + count`,
so a verifier walking the anchors sees every `seq` of the stream covered once.

## 14. Alignment with existing patterns

| Choice | Pattern followed | Where it differs, and why |
| --- | --- | --- |
| Records as canonical, signed, sequenced entries with `prev` hash links | BRC-109 section 13.4 (logs as monotone sequences with sequence numbers and a `prev_hash` chain; detached signatures over canonical bytes); BRC-60 (a hash chain of events instead of chains of dependent transactions) | canonical CBOR (RFC 8949 section 4.2.1) through bcommon rather than canonical JSON, so every application on the substrate shares one codec |
| Batches never mined, kept off chain by a non-final input | BRC-60's non-final transaction | used the other way round: to keep the record off the chain, not to update one transaction in place |
| Output layout: PushDrop fields plus a P2PK lock | BRC-48 Pay to Push Drop | lock-before (`<key> OP_CHECKSIG` first, BRC-48's example is lock-after), as bcommon `pushdrop` and the SDK write it; the field signature follows the fields; scripts compared byte for byte |
| First field of the anchor token identifies the token kind | BRC-88 SHIP and SLAP tokens (field 1 is the string `SHIP` or `SLAP`) | a registered two-byte prefix plus a type byte (`62 6c 01`) instead of an ASCII word, so the tag is short and its owner is recorded in bcommon's registry |
| Magic: prefix, record type, version | BRC-171 section 6 (a protocol prefix, a header version and a record type ahead of the fields; it credits BRC-224 for the convention, and BRC-224 versions its manifest with a `bmf` field) | inside the CBOR record as key 0, and ordered prefix, type letter, version as bcommon's registry rule fixes it (`"bl"`, `'b'`, `0x01`) |
| Derivation `[1, "blogs"]`, counterparty anyone | BRC-42 and BRC-43 | BRC-88 derives under counterparty `self`; anyone with `forSelf = true` is used here so a reader derives the lock from the identity key alone |
| Topic `tm_log_<name>_<suffix>`, lookup `ls_log` | BRC-87 naming; BRC-22 topic managers; BRC-24 lookup with `output-list` answers | the identifier grammar is BRC-87's master rules, narrowed to leave room for the prefix, with a random suffix so that unrelated users do not collide on one topic |
| Interval root: RFC 6962 over 32-byte commitments | RFC 6962; BRC-220 batch mode (a leaf over a 32-byte hash datum, split at the largest power of two, no duplication, the datum stated); BRC-122 ARIA (an epoch's root over its records ordered by `sequence`, committed on chain) | bcommon `commit`, the one routine every anchor in the application family uses; ARIA links an epoch's close to its own open, where anchors here link to the previous interval |
| Per-interval completeness by comparing roots | BRC-136 BASM (per-block completeness of a topic by root) | per producer interval rather than per block, and RFC 6962 rather than Bitcoin's duplicating SHA-256d tree, so no duplicate-leaf ambiguity (CVE-2012-2459) arises |
| Digest-only locator | BRC-26 UHRP (content named by its SHA-256) | the digest is UHRP's hash, so a UHRP resolver can serve it; URLs are hints, never instructions |
| Unknown record keys preserved and ignored | BRC-174 section 3.2 (ignore fields not recognised; preserve them when reconstructing) | the same rule for CBOR keys; an unknown locator member makes the locator unsupported rather than being ignored, because it may change how the content is found |
| Carrier ancestry and proofs | BRC-62 and BRC-95/96 BEEF; BRC-74 BUMP, guarded by bcommon `guard` | none |
| Sibling backfill by lookup | BRC-24 | BRC-76 GASP remains available between hosts; the batch class is the targeted form |
| Priced questions | BRC-105 | the seam is left open: both classes here are free, and a priced class must be a different question |
| Objects on the plane | BRC-148 and BRC-149 (the object frame; the operator bounds the size) | none |

No BRC defines a log batch, a log stream or an interval anchor for logs.
BRC-109 section 13 and BRC-122 are the nearest: both keep their log records
off chain and put at most roots or references on chain, as this does with
batches.

## 15. What this does not do

1. **Batching is the latency floor.** Records arrive when their batch does.
2. **Provenance is the producer's key plus chain time.** An anchor proves
   what was committed and no later than when; not that a record is true.
3. **No cross-producer order.** Order is per producer and stream; a merged
   view orders by producer timestamps, which are claims.
4. **Key compromise.** A stolen producer key can sign batches and forks;
   anchors already mined stay, and a new key is a new producer. Rotation is
   not part of this version.
5. **Digest-only hiding.** The locator's digest is unsalted, so guessable
   content can be confirmed from it; a producer that needs hiding adds a
   random line to the content.
6. **Retraction is not erasure.** Honest archives drop the bytes; a copy
   anyone kept stays a copy. The chain holds only commitments and roots.
7. **Metadata is public.** Producer keys, stream identifiers, sequence numbers,
   timestamps, sizes and counts are visible to every host on the topic.
8. **Retracted tails (a known limitation).** An archive that never held
   retracted batches at the end of an interval cannot recover their
   commitments from a sibling, and so cannot verify that interval by itself
   (section 8.4); an archive that held the receipts can.
9. **Auditing one record** costs a batch question per batch of its interval
   until an archive contract serves inclusion paths.

## 16. Vectors

`testdata/vectors` holds the golden vectors, generated by the codec in
`internal/logrec` with `GOWORK=off go run ./cmd/vectors`; `go test` regenerates
them and compares byte for byte, and refuses a file nothing generates.

| File | What it holds |
| --- | --- |
| `batch-v1.json`, `batch-*.hex` | four batch records: seq 1 with an unknown key; full-bytes `lines` whose content carries an escape sequence; full-bytes `lines+gzip`; digest-only |
| `locator-v1.json`, `locator-hints.hex` | a locator with a hint URL, and one with an unknown member (unsupported, preserved) |
| `anchor-v1.json`, `anchor-interval-*.hex` | two consecutive intervals (5 and 3 batches): commitments, leaves, root, every inclusion path, the record, the token's fields and the hash its signature covers |
| `derivation-v1.json` | the registry values, the keys the fixed test key derives for `batch`, `anchor` and `fund`, and two stream identifiers with one readable part and different suffixes, split, with their topics |
| `refusal-v1.json` | fifty-three byte strings the record decoder refuses, each with its reason (section 3.2) and what admission does with an output carrying it: refuse, or skip one that does not claim to be a record. Every one is decided by the record alone; a `seq`/`prev` link break is a status (section 8.2), never a refusal, so none is here |

The vectors' stream is `example_app_qzxkvbmwtr`; its suffix is a fixed
example so that the vectors are reproducible, where a real stream draws its
suffix at random. Commitments in the anchor vectors are fixed stand-ins,
`SHA-256("blogs/vector/commitment/<seq>")`; carrier and token transactions,
and the script and unlocking rules of section 8.1, get their vectors with the
transaction codec. The test key is 32 bytes of `0x42`, public, and never for real
use.
