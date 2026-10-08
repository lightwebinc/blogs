# The blogs command

`blogs` produces and reads blogs log streams ([spec.md](spec.md)). A
producer turns log lines into signed, numbered batches on a stream's topic
and anchors them in one mined token per interval. A reader asks overlay
hosts for a stream's batches and anchors and checks every answer itself,
against block headers it chooses, before it prints a line.

```text
  producer home                                   overlay hosts (tm_log_<stream>, ls_log)
  ─────────────                                   ───────────────────────────────────────
  init ─▶ fund ─▶ stream new ─▶ produce ──carriers, trees, anchors──▶ facade (mode plane)
                                  │                                 or each of hosts (mode unicast)
                                  │  └─trees, anchors──▶ settlement leg ─▶ chain
                                  └─ anchor (finish, resume)
  reader (no home)
  ────────────────
  read, verify-interval ◀── ls_log answers ── hosts;  proofs checked against header_url
  backfill: ls_log answers of the siblings ─▶ submit route of each host being filled
            (-sweeps: sweeps found by the mined spend ─▶ the same submit route)
  archive:  ls_log answers of one host ─▶ files: batches, anchors, interval proofs
```

## Build

Go 1.27.1 or later, or Docker.

```bash
make blogs              # bin/blogs
go build ./cmd/blogs    # or directly
make docker-build       # ghcr.io/lightwebinc/blogs and blogs-devchain, locally
```

In the image the home is `/home/nonroot/.blogs`; mount a named volume there
(`-v blogs-home:/home/nonroot/.blogs`) and pass settings as `BLOGS_<KEY>`
variables.

## Commands

| Command | Needs a home | Needs | What it does |
| --- | --- | --- | --- |
| `init` | creates it | nothing | identity key and empty coin pool; prints the identity and fund address |
| `fund -beef FILE` | yes | `header_url` | imports a payment to the fund address from its BEEF, as the wallet hands it over, with no lookup: how a producer on mainnet or testnet is funded |
| `fund -txid TXID` | yes | `chain`, `header_url` | imports a mined payment to the fund address, read from the chain view once its proof checks |
| `fund [-blocks N] [-rescan]` | yes | `rpc`, `asset` | coinbase: only on a regtest chain you run (development and tests). Mines coinbase to the fund address through `generatetoaddress` |
| `stream new <name>` | yes | nothing | draws the random suffix; prints the stream, its topic, and the host's `BLOGS_STREAMS` entry |
| `stream list` | yes | nothing | the home's streams and how far each is anchored |
| `produce <stream>` | yes | `facade` (plane) or `hosts` (unicast); `settle`, `chain` and `header_url`, which default on mainnet and testnet | stdin or a file to batches, published and anchored |
| `anchor <stream>` | yes | as `produce` | publishes a persisted carrier, anchors every published batch, waits |
| `doctor` | optional | nothing | the home's state, the chain view and the fee policy, and whether the node, headers, broadcaster and hosts answer; reads only |
| `read <stream> <producer>` | no | `hosts`, `header_url` | verifies and prints a stream's batches |
| `verify-interval <stream> <producer> <n>` | no | `header_url`, and `hosts` or `-archive DIR`; `chain` only for a fee tree no host serves | the auditor's check of interval n |
| `sweep <stream>` | yes | as `produce`; `hosts` for `-retract-seqs`, and to reach hosts off the plane | retracts batches by sweeping their funding outputs, or retires unused funding outputs to the pool |
| `backfill <stream> <producer>` | no | `-from` and `-to`, or two or more `hosts`; `chain` for `-sweeps`, and for a fee tree no sibling serves | fills a host's holes from its siblings, or brings every configured host into step; `-sweeps` also carries sweeps |
| `archive <stream> <producer>` | no | `-dir`, a host, `header_url`; `chain` to find retractions, and for a fee tree the host does not serve | keeps one host's copy of a stream, with each interval's proof and each anchor's fee tree, in local files |
| `version` | no | nothing | prints the version |

`<stream>` is always the full identifier `<name>_<suffix>`; the readable
name alone is refused. `<producer>` is the producer's identity key, 66
lowercase hex characters. `blogs <command> -h` prints a command's flags.

## Configuration

Settings come from, highest first: the global flags, `BLOGS_<KEY>` in the
environment, the config file (`~/.blogs/config` by default), and the
defaults. Every key, variable and flag, with examples for mainnet, testnet,
the plane and a reader, is in [configuration.md](configuration.md). Every
limit on them, with its reason, is in [limits.md](limits.md): a hard cap is
refused with exit 2 and a message naming the limit; a recommendation passed
prints `warning: ...` and goes on.

## The producer home

`init` creates the home at mode 0700. It holds:

| Path | What |
| --- | --- |
| `identity.json` | the identity key (mode 0600); the producer is this key |
| `wallet.json` | the coin pool (mode 0600): fees for funding trees come from here |
| `config` | the config file, when it is kept here |
| `lock` | held while `produce`, `anchor`, `sweep` or `fund` runs |
| `streams/<stream>/state.json` | one stream's state (mode 0600), written atomically before every publish |

A stream's state holds its head (`seq` and commitment), the carrier
persisted before it is published, the commitments not yet anchored, the
anchor chain's head and the anchor in flight, the funding trees, the trees
signed and not adopted yet with the coin each spends, and the sweeps built
from them. Losing it loses the head, and a producer that lost
its head must not restart the stream (specification section 12): `produce`
refuses a stream the home does not hold, and the answer is a new stream
from `stream new`.

One `produce`, `anchor` or `fund` runs per home at a time, because they
share the coin pool; a second exits 2. To run several producers at once,
give each its own home. The fund key is the embedded wallet's key under
`[1, "blogs"]`, key id `fund`, counterparty `self`: only this home can
derive it.

## Producing

```bash
blogs init                             # prints the fund address
blogs fund -beef payment.beef          # after paying the fund address from your own wallet
blogs fund -txid 6f1e...a9c2           # or, once the payment has a block, by its txid
blogs fund -blocks 101                 # coinbase: only on a regtest chain you run (development and tests)
blogs stream new app                   # prints app_<suffix> and tm_log_app_<suffix>
tail -F /var/log/app.log | blogs produce app_qzxkvbmwtr
blogs produce app_qzxkvbmwtr -file /var/log/app.log -follow
```

`stream new` prints the line a host operator adds to `BLOGS_STREAMS`
([host.md](host.md)) to admit only this producer on the stream, and the
topic to add to `OVERLAY_TOPICS`. A home holds at most 64 streams.

### Batches

Lines are read from stdin, or from `-file` (from where the last run on this
stream stopped reading it; with `-follow`, as it grows, through truncation
and through rotation to a new file at the same path). A batch is cut at
`-max-bytes` bytes of lines, at `-max-lines` lines, or `-max-wait` after its
first line, whichever comes first. A line longer than 65535 bytes is carried
as several lines, and a last line without an LF gets one.

| Flag | Default | |
| --- | --- | --- |
| `-profile` | `full` | `full`: the content rides in the record (profile 1). `digest`: the content is written to `-content-dir` under its SHA-256 in hex and the record carries a locator (profile 2) |
| `-encoding` | `lines` | `lines` or `gzip` (one gzip member, no time, no name) |
| `-content-dir DIR` | none | `digest` only: where content files are written; serve them yourself |
| `-url PREFIX` | none | `digest` only, up to 4: an `https://` prefix the digest's hex is appended to, a fetch hint in the locator |
| `-max-bytes N` | by mode: `full` `lines` 49152 on the plane, 262144 in unicast; `gzip` 262144 on the plane, 1048576 in unicast; `digest` 1048576 | bytes of lines per batch; at most 262144 for `full` `lines` (the record's bound), 16777216 otherwise |
| `-max-lines N` | 10000 | lines per batch, at most 65536 |
| `-max-wait DUR` | `1s` | the latency floor: a batch is published at most this long after its first line; `10ms` to `1h` |
| `-max-rate R` | 2 | batches a second, at most 50; a batch cut sooner waits, and the input waits with it |
| `-unsafe` | `warn` | lines holding control characters, escape sequences other than color, or zero-width and bidirectional characters (`termsafe.Validate`): `warn` publishes them and counts them at exit, `drop` does not publish them |

A batch whose encoding is over the record's bound (256 KiB carried, 16 MiB
located) is split at a line boundary into several batches. A carrier whose
BEEF is over `object_bound` is not published and the run stops. On the
plane, a carrier over 64 packets (72192 bytes) is published with a warning,
once per run: large objects are lost far more often under loss and
distance ([limits.md](limits.md)).

### Anchors

Once `-interval` (default `10m`, from `1m` to `24h`) has passed since the
last anchor and a published batch is unanchored, or `-anchor-batches`
(default 4096, at most 65536) batches are unanchored, `produce` builds the
next anchor over the unanchored batches (at most `-anchor-batches`), hands it to the settlement leg, and
publishes it to the facade once it has mined, with its proof. Nothing is
anchored while an anchor is in flight: the next one waits for it and covers
what arrived meanwhile. At the end of the input, `produce` anchors what is
left and waits up to `-anchor-wait` (default `10m`; `0` skips the final
anchor) for it to mine and be published. `anchor <stream>` does the same
without reading input: it publishes a persisted carrier, collects the anchor
in flight, anchors the rest, and waits up to `-wait`.

### Funding

Every carrier spends one output of a batch funding tree, and every anchor
pays its fee from one output of an anchor fee tree. Both are mined funding
trees on the stream's topic, minted from the coin pool, settled and waited
for, and published only once mined, because a host admits funding outputs
only from mined transactions. The next tree is minted in the background
once the current one has `-ahead` (or `-anchor-ahead`) outputs left, so a
tree running out does not stop the stream for a block.

| Flag | Default | |
| --- | --- | --- |
| `-tree-count` | 100 | outputs per batch tree, at most 1000. Each carrier's BEEF carries its tree, about 49 bytes an output, so this is per-batch overhead |
| `-tree-sats` | 1 | satoshis per batch output. A carrier carries its output's whole value and is never mined, so this value stays with the batch until a sweep |
| `-ahead` | 25 | mint the next batch tree at this many outputs left |
| `-anchor-tree-count` | 8 | outputs per anchor fee tree, at most 1000 |
| `-anchor-tree-sats` | 1000 | satoshis per anchor fee output; one pays an anchor's fee (at the default 100 satoshis per 1000 bytes, the 100 satoshi floor), and the rest returns to the pool as change |
| `-anchor-ahead` | 2 | mint the next anchor fee tree at this many outputs left |
| `-poll` | `5s` | how often proofs are asked for |

The pool is filled by `fund`. On mainnet or testnet, pay the fund address
`init` printed from your own wallet, then import the payment one of two
ways; neither needs a node:

- `fund -beef FILE` (`-` reads standard input) takes the payment as BEEF,
  binary or hex, as the wallet hands it over, and looks nothing up. A mined
  payment's proof must verify against `header_url`. A payment not mined yet
  is accepted when it is final and every transaction it spends carries a
  proof the headers hold, and its scripts verify against them; its outputs
  are held back (`doctor` counts them as held until mined) until a later
  `produce`, `anchor` or `sweep` collects its proof from the chain view, or
  until `fund -beef` is given the same payment mined. `fund_mined_only =
  true` or `-mined-only` refuses it instead (exit 1).
- `fund -txid TXID`, once the payment has a block, reads it and its proof
  from the chain view (`chain`), checks the proof against `header_url`.

Either adds every output paying the fund address to the pool. A payment
that pays nothing to the fund address, one not mined where only a mined one
is taken, and one whose proof does not hold are refused (exit 1). Without
`-txid` or `-beef`, `fund` mines coinbase to the fund address (coinbase:
only on a regtest chain you run, for development and tests; it needs `rpc`
and `asset`; usable 100 blocks later; `-blocks N` blocks, `-batch N` per
`generatetoaddress` call, and `-rescan` re-reads the last `-blocks` blocks
for coinbase the pool lacks instead of mining).

Fees follow `fee_rate`, by default the network's 100 satoshis per 1000
bytes with a floor of 100 satoshis per transaction (`fee_floor`), or with
`fee_source = arc` the rate the broadcaster publishes, read once a run and
held to `fee_min_rate` and `fee_max_rate` ([configuration.md](configuration.md#keys)).
At the defaults each batch costs about 6 satoshis (its share of its tree's
fee, and the satoshi it carries) and each anchor about 113.

A tree minted ahead that a run ends without using is recorded in the state,
and its change is back in the pool. The next `produce`, `anchor` or `sweep`
holds it again and switches to it when the current tree runs out, so its
outputs are used. `blogs sweep -retire` pays them back to the pool instead,
and a tree retired that way is not switched to.

### Restart and crash

Each carrier is written to the state before it is published. A run that
cannot publish it (the facade is down, or in mode unicast fewer hosts than
the quorum took it; each tried four times) exits 2 with the carrier
persisted, and the next `produce` or `anchor` publishes those same
bytes before anything else: the same txid, which a host that already holds
it answers as a duplicate. So a restart continues at the next `seq`, and no
two carriers are ever built for one. An anchor built and not yet published
is collected by the next run, and an anchor the network refused is built
again over the same batches from the next anchor fee output. Refused means
the chain view names another transaction as the spender of one of its inputs,
whether the settlement leg refused the anchor or accepted it first; any
other failure to settle is tried again on the next tick. When the input
spent elsewhere is the previous anchor token, no anchor can follow it: the
anchor stays in flight with a `WARNING` rather than being rebuilt, and
batches go on being published. An interrupt (SIGINT, SIGTERM) publishes the
batch being built and exits 0, as does an interrupt of `anchor` or of the
final wait; the anchor in flight is collected next time. A final wait or
`anchor -wait` that runs out exits 2.

### An interrupted funding tree

A funding tree's fee coin leaves the pool when the tree is signed, and the
tree becomes the stream's own (is adopted) only once it has mined; a tree
minted ahead is adopted later still, when the current tree runs out. So
that a run stopping in between loses neither, the state records every tree
with its kind (`batch` or `anchor`) and the coin it spends before the tree
is sent (`pendingTrees`), and the save that adopts the tree drops the
record. A tree that cannot be recorded is not sent, and its coin stays in
the pool.

A run that stopped in between (a crash, a kill, a machine switched off)
leaves the record, and `doctor` names it: `batch tree ... signed for coin
... and not adopted`. The next `produce`, `anchor` or `sweep` of that
stream asks the chain view what became of the tree before it spends
anything, and says what it found on stderr, after the line `... funding
tree TXID was signed by a run that stopped before it adopted the tree:
asking the chain view what became of it ...`:

| The chain view shows | The command does | It says |
| --- | --- | --- |
| the tree, its coin not spent by another transaction, and the current tree of its kind is used up or there is none | adopts the tree, takes its unspent change into the pool and publishes it | `funding tree ... is recovered`, `funding tree published: ...` |
| the tree, its coin not spent by another transaction, and the current tree of its kind still has outputs | takes its change, holds the tree and switches to it when the current tree runs out; the record stays until then | `funding tree ... is recovered and waits for the switch` |
| no such tree, and the coin unspent | puts the coin back in the pool and keeps the record for one more command; the second such answer drops it | `... never reached the chain: its fee coin ... is unspent and back in the pool`, then `its record is kept for one more command`; the second time `the chain view still does not know it, so its record is dropped` |
| no such tree, and the coin spent by another transaction | drops the record | `... never reached the chain: its fee coin ... is spent by ...` |
| the tree, with no proof, and the coin spent by another transaction | drops the record and adopts nothing: the tree lost a double spend and never mines, though the chain view still serves it | `... lost a double spend: the node still serves it, without a proof, and its fee coin ... is spent by ...` |
| nothing: it cannot answer, or the tree has not mined in time | keeps the record and goes on without the tree; the next command asks again | `... what became of it could not be settled now (...): its record is kept ...` |

Several trees can be held at once. They are used in the order they were
signed, each adopted at its own switch, and no tree is minted ahead while
one is held.

A tree that reaches the chain after its coin went back is found by the next
command, which adopts it and takes the coin, now spent, out of the pool
(`its fee coin ... is spent and is taken out of the pool`). Nothing is lost
unless the tree lands after two commands found nothing: then its outputs
are lost to the stream. The command that next takes the coin has its
transaction refused and exits 2, and the refusal takes the spent coin out
of the pool (`its fee coin ... is spent by ..., so it is out of the pool
and is not returned`). The command after that drops the refused
transaction's record.

**The wait at the start.** A tree is used only once it has mined, so a
tree the chain view knows and has not mined yet is waited for, as a new tree is.
The wait for all such trees together is at most the wait for one proof: 10
minutes for `produce` and `anchor`, `-wait` for `sweep`. The line above
says so before the wait begins. When it runs out the record is kept, the
command goes on (and mints a tree if it needs one), and the next command
asks again. An interrupt during the wait ends the command, exit 2, with
the record kept. So `produce -follow` under a supervisor starts reading at
most that long after a restart, and never waits without end.

Only commands that spend ask: `produce`, `anchor` and `sweep`. `read`,
`verify-interval`, `backfill`, `archive`, `doctor`, `stream` and `fund`
ask nothing about a tree and need no node for it. Each stream keeps its
own records, so a stream's tree is settled by the next command on that
stream.

**A tree adopted and not published.** A tree is adopted before it is
published to the hosts, so that a crash never leaves a tree on a host that
the state does not know. If the publish fails (the facade is down, or in
mode unicast fewer hosts than the quorum took it), the state records the
tree as not published (`unpublished`) and the publish is tried again with
the same retries as a carrier. While it still fails the command exits 2
(`funding tree TXID: ...; the tree is adopted and recorded as not
published, and the next produce, anchor or sweep publishes it`), and the
next command publishes the same bytes before anything else, as it does for
a persisted carrier. No second tree is minted. `doctor` names it: `tree
... adopted and not confirmed published`.

A producer pays for its trees from its own pool. A deployment that funded
trees through a BRC-100 wallet instead (bcommon's `Trees.Fund`) would not
be covered by any of this: such a wallet chooses the coins, signs and
broadcasts inside one call, so there is no moment before the broadcast to
record the tree in and no pool coin to put back, and a run that stopped
after the broadcast and before the tree was adopted would leave a tree
only that wallet knows. What else is left open is in
[limits.md](limits.md).

## Reading

```bash
blogs read app_qzxkvbmwtr 02c0ffee...                 # all of it, as lines
blogs read app_qzxkvbmwtr 02c0ffee... -from 120 -to 140 -meta
blogs read app_qzxkvbmwtr 02c0ffee... -json | jq -r '.lines[]'
blogs read app_qzxkvbmwtr 02c0ffee... -content-dir /srv/logs -allow-host logs.example.com
```

(`02c0ffee...` stands for the producer's 66-character key.)

For each `seq` from `-from`, `read` asks every host, and checks every
carrier answered: the carrier rules of specification section 8.1 with this
producer as the only producer, the asked `seq`, and SPV through its funding
parent against `header_url`. A host that answers something that does not
verify is named (`host URL: REFUSED seq N: <CODE> <reason>`), what it
answered is not used, and the exit is 3: what is printed comes from the
hosts whose answers verify, and one host at fault does not make the read
fail. A `seq` every host answered only with what does not verify is not
read and is not called absent (`seq N: NOT READ: ... none verifies against
the header source URL; it is not absent`): a header source that is behind
fails every answer the same way. A header source that cannot be asked
stops the read, exit 2, naming it. It
follows the `prev` links, and where a host answers several candidates for a
`seq` (a fork), the next batch's `prev` chooses. It decodes the content and
prints its lines, each filtered for the terminal (`termsafe`: controls,
escape sequences, zero-width and bidirectional characters removed; a line
longer than 512 columns is wrapped, not cut). Without `-to` it stops at the
first `seq` no host holds, which is the end of what the hosts hold or a
hole; an empty answer proves nothing. One read covers at most 65536 seqs:
a longer `-from`/`-to` range is refused, and without `-to` the read stops
there and names the `-from` to continue with.

| Flag | |
| --- | --- |
| `-from N`, `-to N` | the range; without `-to`, until no host holds the next `seq` |
| `-meta` | a line before each batch: `# seq N  timestamp MS  C line(s)  carrier TXID  VERIFIED  linked  content ok` |
| `-json` | one JSON object per batch: `seq`, `carrier`, `timestamp`, `count`, `profile`, `encoding`, `verdict`, `link`, `content`, `lines` (filtered as for a terminal) |
| `-ansi` | let color through; `-ascii` prints non-ASCII as `?` (the default when the locale is not UTF-8) |
| `-content-dir DIR` | repeatable: digest-only content named by SHA-256 hex |
| `-allow-host HOST` | repeatable: a host whose locator hints may be fetched |

A digest-only batch's content is taken from a `-content-dir`, or fetched
over HTTPS from a locator hint whose host is on `-allow-host`, following no
redirect off the origin and reading at most the locator's length; bytes
whose length or digest do not match are discarded. A hint on no list is
never fetched. Content found nowhere is reported `pending`; a profile,
encoding or locator this version does not read is `unsupported`. Neither is
a refusal.

`link` is `linked` (seq 1, or `prev` is the batch read before it),
`unchecked` (the batch before it was not read), or `BROKEN`.

## Auditing an interval

```bash
blogs verify-interval app_qzxkvbmwtr 02c0ffee... 3
```

The auditor's check of specification section 13, needing no producer home:

1. walk the interval class from 1 to n, keeping each interval's canonical
   token: for interval 1 the one mined first (block height, then position),
   for each later interval the one that spends the canonical token before
   it; each must be mined in `header_url`, and each must continue the
   previous interval's `first + count`;
2. ask the batch class for every `seq` interval n covers, from every host,
   and verify each carrier;
3. hold every token to the anchor rules of section 8.1, reading the outputs
   its inputs spend from the predecessor token, from the funding trees the
   interval's carriers carry, or from the funding tree itself. An anchor
   pays its fee from an anchor fee tree, which no carrier carries, so the
   auditor asks the hosts for it by its txid (the tree class,
   `{stream, tree}`). A host answers the tree with its proof however long
   ago its outputs were spent, so the audit does not depend on a node that
   prunes spent transactions. The tree must hash to the txid the anchor's
   input names, and its proof must hold against `header_url`; an answer
   that fails either is refused and named, and every host is asked. Only when no
   host serves the tree is the chain view (`chain`: WhatsOnChain by default,
   or a node) asked, its answer held to the txid. `-v` prints
   `interval K: funding tree T from <source>` for each tree fetched;
4. require the RFC 6962 root over the batches' commitments, choosing among
   fork candidates by `prev` and trying each at the last `seq`, to equal the
   token's root. A `seq` no host serves takes its commitment from the next
   batch's `prev`, as an archive proves an interval across a retraction
   (specification section 8.4), and is reported **unserved** (retracted, or
   not delivered). A `seq` the hosts answered only with what does not
   verify is treated the same way and reported **unverified**, never
   unserved. The `seq` before such a hole must then have a single
   candidate, since nothing names its commitment.

Every host, and the archive with `-archive`, is asked every question: the
interval class, the batch class and the tree class. An answer that does
not verify is named on stderr with what answered it (`host URL: REFUSED
<what>: <CODE> <reason>`, or `the archive DIR: REFUSED ...`) and is not
used. The interval is proven from what verified: the VERIFIED line is
printed, and the exit is 3, since one source at fault does not make the
audit fail. When nothing answered for an interval verifies, the audit is
INCOMPLETE and names the header source it checked against (`the hosts
answered N time(s), each answer named, and none verifies against the
header source URL`): a header source that does not answer, or is behind,
fails every answer the same way. A header source that cannot be asked at
all stops the audit, exit 2, naming it.

It prints each interval's canonical token, each batch, and one verdict line:

```text
interval 1  token 7c2f...  height 1204  seqs 1 to 5
interval 2  token 91ab...  height 1211  seqs 6 to 8
seq 6  carrier 3e0d...  timestamp 1700000000000  4 line(s)
seq 7  carrier c51a...  timestamp 1700000001000  4 line(s)
seq 8  carrier 0b77...  timestamp 1700000002000  4 line(s)
VERIFIED interval 2 of app_qzxkvbmwtr by 02c0ffee...: seqs 6 to 8, root 5d1e..., committed no later than block 1211
```

After seq 7 is retracted, the same audit prints seqs 6 and 8, then
`unserved seqs 7: no host serves them (retracted, or not delivered); each
commitment is the next batch's prev`, then the same VERIFIED line with the
same root. A host serves nothing for a retracted batch and nothing for one
it never received, and the auditor checks no chain for a sweep, so it
names the position and not the cause. An archive, which finds the mined
spend, is what says `retracted`; a backfill fills a batch that was only
undelivered.

`REFUSED interval n: <reason>` (exit 1) names the rule broken by what
verified, for example
`anchor-join` or `root-mismatch`. `INCOMPLETE interval n: <reason>` (exit 3)
means evidence was not found: no host answered an interval, a `seq`
whose commitment nothing names was answered by no host (the interval's
last `seq`, or the first of two in a row), or an input's funding tree was
served by nothing asked, or nothing answered for an interval verifies. The
reason then names the tree and what each source did, for example
`interval 1: input 0 spends T:0, a funding tree nothing asked serves: URL
holds no such tree; the chain view woc:main did not answer (...)`: a host that
predates the tree class `did not answer the tree class`, one that never
received the tree `holds no such tree`, and one whose answer failed a check
`answered something refused`. Every answer refused along the way is
listed on stderr, with or without `-v`.

`-archive DIR` adds an archive directory (`blogs archive -dir DIR`) as a
source asked before the hosts. Its stored tokens, carriers and funding
trees are checked exactly as a host's answers are, so a file that was
altered is refused, not believed. With `-archive` and no host configured,
the audit runs from the files and the header source alone:

```console
$ blogs -header-url woc:main verify-interval app_qzxkvbmwtr 02c0ffee... 12 -archive /srv/blogs-archive
```

## Backfill

```bash
blogs backfill app_qzxkvbmwtr 02c0ffee... \
  -from https://archive-a.example.com -to https://archive-b.example.com -interval 3
blogs backfill app_qzxkvbmwtr 02c0ffee... -seqs 1-120 -anchors 3   # every configured host, from the others
blogs backfill app_qzxkvbmwtr 02c0ffee... -seqs 1-120 -sweeps       # and carry the sweeps a host missed
```

Each position is asked of the host being filled first and, only when that
host holds nothing, fetched from the first sibling whose answer is checked
(the carrier rules, and its proof when `header_url` is set) and submitted
to the host's submit route. A sibling that answers what does not verify is
named (`seq N: the sibling URL answered something REFUSED: ...; it is not
used`) and the next sibling is asked: a repair names a bad answer and does
not fail on it. A position every sibling answered only with what does not
verify is `MISSING` and is not called absent. A header source that cannot
be asked stops the run, exit 2, naming it. `-to` names the host to fill, and
without it every configured host is filled in turn; `-from` names the
sibling, and without it the siblings are the other configured hosts. With
neither, the configured hosts are brought into step with each other, which
is how a unicast producer's hosts are repaired ([unicast.md](unicast.md)).

`-interval K` fills the anchor chain 1 to K, in order, and every `seq`
interval K covers; `-seqs A-B` a range of seqs (at most 65536); `-anchors
K` the anchor chain alone; `-seqs` and `-anchors` combine. A host admits an
anchor only once it holds the fee outputs the anchor spends, so when a host
does not admit a token, `backfill` asks the siblings for the funding trees
its inputs spend by txid (the tree class), reads one from the chain view
(`chain`) only when no sibling serves it, submits each with its proof
(`interval K: funding tree T submitted from <source>`), and offers the
token again.
One line per position (`held`, `filled from the sibling URL`, `MISSING`,
and a line for each `REFUSED` answer), then a count per host filled:
`backfill S to URL: H held, F filled, M missing, R refused, U unserved, W
sweep(s) carried`. A `seq` that neither the host nor any sibling serves is
missing, unless the `seq` after it is held or filled: that batch's `prev`
names its commitment, as for a retracted batch, so it is counted
`unserved` (retracted, or not delivered to any host asked) rather than
missing and does not fail the run (the interval is proven across it, as
`verify-interval` and `archive` do). A host that cannot be asked is
reported and the others are still filled.

No lookup class names the sweep of a batch, so a host that missed one keeps
serving the batches it retracted, and a backfill of batches cannot tell. `-sweeps`
carries them: for every `seq` the host holds, each carrier it serves is
checked for a mined spend of its funding output by another transaction
(named by the chain view and checked against `header_url`, as an archive
checks), and a sweep found is submitted to the host, which then
stops serving the batch. It costs one spend read per held carrier, so it is
asked for, not done by default; a backfill without it ends with
`note: backfill carried batches, anchors and their fee trees, not sweeps: ...`.

## Sweeping

```bash
blogs sweep app_qzxkvbmwtr -retract-seqs 7            # prints the plan, changes nothing
blogs sweep app_qzxkvbmwtr -retract-seqs 7-9 -yes     # retracts seqs 7 to 9
blogs sweep app_qzxkvbmwtr -retract-tree 5f0e... -yes # every batch one tree funded
blogs sweep app_qzxkvbmwtr -retire -yes               # unused outputs back to the pool
blogs sweep app_qzxkvbmwtr                            # finishes a sweep in flight, or lists them
```

A sweep is one mined transaction per funding tree (specification section
6.4), and needs the producer's home: only the producer can sign for its
funding outputs.

- **Retraction.** `-retract-seqs A-B` asks the hosts for the carriers at
  those seqs (every candidate, a fork included), checks that each is this
  producer's and spends a batch tree the stream recorded, and sweeps
  exactly the outputs they spent with bcommon `carrier.SweepAt`: output 0 is
  a funding-shaped tombstone carrying the swept value, the fee, at the
  configured fee policy, comes from the coin pool and its change goes back
  to it. `-retract-tree TXID` sweeps
  every output of one batch tree, used and unused. Once the sweep mines it
  is published, with its proof, to the stream's topic; each carrier that
  spent a swept output is then a double spend, and a host stops serving it
  and keeps its seq as a numbered position: the anchor still commits it, a
  reader finds it missing, and an archive and `verify-interval` prove the
  interval across the hole where the next batch's `prev` names the
  commitment. An archive that stored the batch keeps a receipt of it (see
  Archive). A retracted batch that is the last of its interval leaves
  nothing to name it, so an archive that did not hold it, and the auditor,
  cannot prove that interval (specification section 8.4). An anchor fee tree funds no carrier and cannot be
  retracted.
- **Retirement.** `-retire` spends the unused outputs of every tree no
  longer in use (batch trees, anchor fee trees, and trees minted ahead and
  never adopted) to the fund address, the fee coming out of their value,
  and pools the result. It retracts nothing. A tree minted ahead that is
  retired is not switched to afterwards. A tree whose unused outputs
  cannot pay the fee is left and named (`left: ...`), which is the usual
  case for a batch tree of one-satoshi outputs. A retirement of a tree the
  hosts hold is published once it mines; one of a tree minted ahead, which
  was never published, is not.

Without `-yes` a sweep prints its plan (`retract seqs 7: tree T outputs
3`, `retire anchor tree T: 5 unused output(s) of 1000 sat`) and changes
nothing. With it, each sweep is written to the stream's state before it is
settled and the swept outputs are marked used, so no later carrier or
anchor spends them; the run then settles it, waits up to `-wait` (default
`10m`) for it to mine, pools its change and publishes it.

Where a sweep goes. No lookup class names the sweep of a batch, so a host
receives one only when it is submitted to it. In mode plane a sweep goes to `facade`,
which the plane carries to every subscribed host, and directly to every
other host in `hosts`, so a host off the plane receives it too; with no
other host configured, the run says the sweep went to the facade only. In
mode unicast it goes to every host in `hosts`. Either way it is published
only when every host named took it, whatever `quorum` says; below that
the run stops with the sweep mined and persisted, and the next `sweep`
sends it again. A host that was named nowhere is given it later by
`backfill -sweeps`. A sweep in flight
when a run stops is finished by the next `sweep` run, with or without
flags, before anything new is built. `sweep` holds the home's lock, like
`produce`.

A refused sweep. A sweep is given up only when the chain view names another
transaction as the spender of one of its inputs: then its bytes can never
mine. The run prints `sweep T refused by the network: input N (OUTPOINT)
is spent by OTHER ...; it is marked failed and retracts nothing; ...` and
exits 1. The state records the sweep as failed with that reason, so it is
no longer in flight and the next `sweep` goes on. A retraction's fee coin
goes back to the pool when the chain view shows it unspent (`its fee coin
... is back in the pool`); a coin it shows spent, or cannot answer for, is
not given back, and the message says which. A retirement pays its fee from
the outputs it spends, so it has no coin to give back. A sweep whose inputs
the chain view shows unspent, or spent by the sweep itself, stays in flight
whatever a settlement leg said, since a leg answers a second submission of
a transaction it already holds the same way: a timeout, a busy leg or a
leg's refusal exits 2 and the next `sweep` sends it again. `doctor` lists a
sweep in flight (`sweep T (retract) in flight`) and a failed one (`sweep T
(retract) FAILED: ...`), and `sweep <stream>` with no flags lists every
sweep with its height or its failure. The outputs a failed sweep named stay
marked used: a retraction may be asked for again, and builds a new sweep
with a new fee coin; a retirement's outputs are not offered again.

## Archive

```bash
blogs archive app_qzxkvbmwtr 02c0ffee... -dir /srv/blogs-archive -host https://archive-a.example.com
blogs archive app_qzxkvbmwtr 02c0ffee... -dir /srv/blogs-archive -follow -poll 1m
```

`archive` is a storage adapter for one host: each pass asks the host for
every `seq` from where the last pass stopped until the host holds nothing,
checks each carrier as `read` does (the carrier rules, this producer, the
`seq`, SPV through its funding parent against `header_url`) and stores it;
then it walks the interval class from the last interval proven, keeps each
canonical anchor token (held to the anchor rules, its fee tree asked of the
host by txid and read from the chain view only when the host does not
serve it), fetches any `seq` the interval covers that is not yet stored,
writes the interval's proof, and keeps the fee tree with its proof under
`trees/` (`kept N funding tree(s) with their proofs in trees/`). A node
that prunes spent transactions stops serving a fee tree once its last
output is spent; the archive's copy, like the host's, does not depend on
it. An archive begun before trees were kept catches up on its next pass,
and says `a funding tree is not kept yet: ...` while nothing serves one. `-follow` repeats the pass every `-poll`.
`-content-dir` and `-allow-host` find digest-only content as for `read`.

Each pass first asks the host again about the seqs it has stored, at most
`-recheck` of them (default 1024, at most 65536; `0` asks about none). It
goes on after the last seq the previous pass asked about and wraps to the
first, so the seqs asked about longest ago come first and a stream of S
stored seqs is covered every S / `-recheck` passes; `-recheck-all` asks
about every one in this pass. The archive prints `rechecked N of M stored
seq(s)`. A stored carrier the host no longer serves is checked for a
retraction: a mined transaction other than the carrier that spends the
carrier's funding output. The spender is named by the chain view (WhatsOnChain's spends, or a
node's UTXO view), fetched with its proof, and checked against
`header_url`: that it spends the output and is mined. When it holds, the
archive writes a receipt and the sweep under `retracted/` and then drops
the batch's bytes (its BEEF, its record file and its content), and prints
`retracted seqs N: receipt and sweep kept in retracted/, bytes dropped`.
The receipt keeps the position: it completes the interval's root, and its
`prev` lets a proof continue past it, so an archive holding the receipt of
an interval's last `seq` still proves that interval. A carrier the host
stops serving with no such spend is kept and noted: an empty answer proves
nothing. The note says `its funding output ... is unspent` only when the
chain view shows it unspent (WhatsOnChain's "known, not spent", a node's
status `OK`); one that does not say (an output it does not know, another
status, a spent output naming no spender, a transaction it does not serve)
is noted `the chain view does not say whether its funding output ... is
spent`, and the next pass that asks about that seq asks again.

The layout, under `DIR/<stream>/<producer>/`:

| Path | What |
| --- | --- |
| `archive.json` | the cursor: the next `seq` to walk to, the last interval proven, the hosts followed, `rechecked`, the last stored `seq` asked about again, and `trees`, the last interval whose fee trees are kept |
| `batches/<seq>-<carrier>.beef` | each verified carrier's BEEF as the host answered it; it carries the funding tree the carrier spends and the tree's proof |
| `batches/<seq>-<carrier>.json` | what was read from it: `seq`, `prev`, `timestamp`, `count`, `encoding`, `profile`, `size`, the funding outpoint, the host, and `content` (`ok`, `pending`, `unsupported`, `invalid`) |
| `content/<seq>-<carrier>.log` | the decoded lines, as the producer wrote them (not filtered for a terminal), when the content was read |
| `anchors/<interval>-<token>.beef` | each canonical anchor token's BEEF, with its proof |
| `intervals/<interval>.json` | the interval's proof: `token`, `vout`, `height`, `position` in its block, `prevToken`, `first`, `count`, `root`, `status`, `holes`, `retracted`, and per `seq` its `commitment`, whether its bytes are `held` or it is held as a `retracted` receipt, and its RFC 6962 inclusion `path` (leaf to root, each step the sibling's hash in hex, `L:` before a left sibling) |
| `retracted/<seq>-<carrier>.json` | the receipt of a stored batch found retracted: what its batch file held (with `content` now `dropped`), its `commitment`, and the `sweep` txid and `sweepHeight` |
| `retracted/<seq>-<carrier>.sweep.beef` | that sweep's BEEF, with its proof: the evidence of the retraction |
| `trees/<txid>.beef` | each funding tree an anchor token spends (its fee tree), as BEEF with its proof, kept when the anchor is first proven. With these the directory is a complete source for `verify-interval -archive` |

`<seq>` and `<interval>` are 16 decimal digits, zero padded; `<carrier>`
and `<token>` are txids. Files are written atomically and never rewritten,
so an interrupted pass is continued by the next and two archives of the
same stream hold identical `intervals/` files. An interval's `status` is
`proven` when every `seq` is held as a batch or a receipt (receipts listed
in `retracted`), and `proven-with-holes` when some `seq` is held by
neither and its commitment was taken from the next batch's `prev` (a
retracted batch, or one the host never received); those seqs are listed
in `holes`. The root over the commitments must equal the token's, or the
interval is not written. An interval file records what the archive held
when it was proven: a later retraction adds a receipt and drops the bytes,
but does not rewrite the interval file, whose proof still holds.

It prints one line per interval proven, a summary, and last
`latest interval K root R token T`, so two archives are compared by that
line. An archive follows one host and asks no other beside it: an answer
of that host that does not verify is named on stderr, with or without
`-v` (`host URL: REFUSED <what>: ...; what it answered does not verify, so
it is not stored`), and the pass ends `the host URL answered N time(s) what
does not verify against the header source URL`, exit 1. Exit 0 when the
pass is done, 1 when the host answered something that does not verify or
the chain or a root does not hold, 3 when an interval the host answers
cannot be proven from what it holds (for example its last `seq` retracted
and never stored, with no receipt).

## Exit codes

| Code | Meaning |
| --- | --- |
| 0 | done; for a reader, every answer verified; for `backfill`, every position held, filled or unserved, whatever a sibling answered that does not verify (each is named) |
| 1 | refused: what verified contradicts what was asked (a fork nothing resolves, a broken link, a root mismatch, an anchor that does not join, invalid content); for `archive`, which follows one host and asks no other, an answer of that host that does not verify (named) |
| 2 | usage, configuration, local or transport error, including a host or header source that could not be asked (named): nothing was decided |
| 3 | incomplete: a host held nothing where evidence was needed, or a host answered something that does not verify (it is named and not used: what is printed or proven comes from what verified). It proves nothing either way |

A help request (`-h`, `help`) exits 0.

## Messages

Results go to stdout; progress and errors to stderr, errors as one line
`blogs: <message>`. These texts are stable:

| Text | When |
| --- | --- |
| `blogs: unknown command "X"` | exit 2 |
| `blogs: <key> must be configured (config key <key>)`, `blogs: no header source: set header_url ...`, `blogs: no chain view: set chain ...`, `blogs: no overlay host configured ...` | a missing setting, exit 2 (a regtest chain has no default header source, chain view or broadcaster) |
| `blogs: chain SPEC names WhatsOnChain's NET network but network is NET` | a chain view for another network, exit 2 |
| `blogs: this home holds no stream S (create one with ...)` | `produce` or `anchor` on a stream without state, exit 2 |
| `blogs: another blogs command is using the home H ...` | the home is locked, exit 2 |
| ``blogs: batch funding tree: fee input: ... (run `blogs fund`: ...)`` | the pool has no spendable coin, exit 2; pay the fund address and run `blogs fund -beef FILE` or `blogs fund -txid TXID` |
| `no coin is spendable and N funding tree(s) minted ahead hold one: waiting for the change` | a fee is asked for while a tree minted ahead holds the pool's last coin: the command waits for that tree to mine, no longer than the wait for one proof, and goes on |
| `blogs: fee input: ...: N funding tree(s) signed for a coin of this home are not settled ...` | the tree did not mine in time, exit 2; its record is kept and the next `produce`, `anchor` or `sweep` recovers it |
| `blogs: fee input: ...: N funding tree(s) minted ahead hold a coin and have not settled ...` | the wait for a tree minted ahead was interrupted, exit 2; the next command recovers the tree |
| `blogs: seq N: publish: ...; the carrier is persisted and a later run publishes it` | the facade did not take a carrier, or in mode unicast fewer hosts than the quorum did (`K of N host(s) took it and the quorum is Q (...)`), exit 2 |
| `<what>: host URL MISSED it after N tries: ...` | mode unicast: a host did not take an object (`seq N`, `anchor interval K`, `batch funding tree`, `anchor fee tree`, `sweep TXID`) |
| `host URL: took N object(s), missed M[: ...]` | mode unicast, at the end of `produce` and `anchor`, on stdout |
| `a host missed objects; once it is back, copy them from the others: blogs backfill ...` | mode unicast, on stdout, after the lines above |
| `seq N: publishing the carrier a previous run persisted` | a run resuming |
| `<kind> funding tree TXID was signed by a run that stopped before it adopted the tree: asking the chain view what became of it ...` | a run settling a tree an earlier run left; the lines after it say what the node showed (An interrupted funding tree) |
| `funding tree TXID: what became of it could not be settled now (...): its record is kept, ...` | the chain view could not answer, or the tree did not mine within the wait; the command goes on |
| `blogs: funding tree TXID: interrupted while asking what became of it ...` | an interrupt during the wait at the start, exit 2 |
| `funding tree TXID is adopted and not yet published: publishing it` | a run publishing a tree whose publish failed before |
| `blogs: ... funding tree TXID: ...; the tree is adopted and recorded as not published, and the next produce, anchor or sweep publishes it` | the facade did not take a new funding tree, or in mode unicast fewer hosts than the quorum did, exit 2 |
| `anchor interval K: seqs A to B, token TXID` | an anchor built and settled |
| `anchor interval K: mined at height H and published; seqs A to B anchored` | an anchor published |
| `seq N: the host admitted nothing: ...`, `seq N: host URL admitted nothing: ...` | a host answered a new carrier with nothing admitted: it held it already, or refused it; the answer is the same for both |
| `note: N line(s) held something a terminal must be protected from (termsafe); published` (or `dropped`) | `-unsafe` |
| `host URL: REFUSED <what>: <CODE> <reason>; what it answered does not verify, so it is not used` | `read`, `verify-interval`, `sweep -retract-seqs`: an answer that does not verify (`REFUSED-SEQ`, `REFUSED-KEY`, `REFUSED-SIG`, `REFUSED-BUMP`, `REFUSED-DECODE`, ...), where `<what>` is `seq N`, `interval K` or `funding tree T`; `the archive DIR: REFUSED ...` for a file of `-archive`; `... so it is not stored` in `archive`. Exit 3 (1 in `archive`) |
| `seq N: NOT READ: the hosts answered it K time(s), each answer named above, and none verifies against the header source URL; it is not absent` | `read`, exit 3 |
| `seq N: FORK: ...`, `seq N: BROKEN LINK: ...`, `seq N: MISSING: ...` | `read` |
| `... none verifies against the header source URL ...` | `verify-interval`, `archive`, `sweep -retract-seqs`: every answer to a question was refused; check the header source (`blogs doctor`) before blaming the hosts, exit 3 |
| `blogs: could not verify: the header source URL did not answer: ...` | a header source that could not be asked, exit 2 |
| `VERIFIED interval ...`, `REFUSED interval N: ...`, `INCOMPLETE interval N: ...` | `verify-interval`, on stdout |
| `unserved seqs N: no host serves them (retracted, or not delivered); each commitment is the next batch's prev` | `verify-interval`, on stdout, before VERIFIED |
| `unverified seqs N: the hosts answered them only with what does not verify (each answer named); each commitment is the next batch's prev` | `verify-interval`, on stdout, before VERIFIED; exit 3 |
| `sweep TXID: mined at height H and published; seqs ... retracted` | a retraction done |
| `retire <kind> tree T: left: ...` | a tree whose unused outputs cannot pay the fee |
| `latest interval K root R token T` | `archive`, on stdout |
| `retracted seqs N: receipt and sweep kept in retracted/, bytes dropped` | `archive`, on stdout: the mined sweep was found |
| `rechecked N of M stored seq(s)` | `archive`, on stdout |
| `backfill S to URL: H held, F filled, M missing, R refused, U unserved, W sweep(s) carried` | `backfill`, on stdout, one per host filled |
| `seq N: served by no host (retracted, or not delivered); seq N+1's prev names it: unserved` | `backfill`, on stdout |
| `seq N: the sibling URL answered something REFUSED: ...; it is not used` | `backfill`, on stdout: the next sibling is asked; it fails nothing |
| `note: backfill carried batches and anchors, not sweeps: ...` | `backfill` without `-sweeps` |
| `note: sweep TXID goes to the facade only: ...` | mode plane, a sweep with no host but the facade configured |
| `sweep TXID: published to the facade, but not to every host directly: ...` | mode plane, a host in `hosts` did not take a sweep, exit 2 |
| `warning: ...` | a setting past a recommended limit ([limits.md](limits.md)); the run goes on |
| `blogs: <setting> V is over (or under) the limit of L: <why> (docs/limits.md)` | a setting past a hard cap, exit 2 |
| `imported N of M output(s) paying ADDR, S sat, mined at height H; pool ...` | `fund -txid`, or `fund -beef` of a mined payment |
| `imported N of M output(s) paying ADDR, S sat, not mined yet: held until its proof arrives, ...` | `fund -beef` of a payment not mined yet; its outputs are spent once it mines |
| `blogs: bwallet: the payment pays nothing to this wallet's fund address: TXID (the fund address is ADDR)` | `fund` of a payment to someone else, exit 1 |
| `blogs: bwallet: the payment has not mined yet: TXID; ...` | `fund -txid` before the payment's block, or `fund -beef -mined-only` of an unmined payment, exit 1 |
| `blogs: coinbase needs a node: ...` | `fund` without `-txid` or `-beef` and no `rpc` and `asset`, exit 2 |

## Testing

```bash
make verify                                    # includes the command's tests
make e2e-client  REFERENCE_HOST=/path/to/host  # the command against two reference hosts
make e2e-unicast REFERENCE_HOST=/path/to/host  # the same in mode unicast
make docker-smoke                              # the images, offline and with the local chain
make quickstart-check                          # QUICKSTART.md, in Docker Compose
```

`make verify` runs the command's tests in process against a local chain and
a stand-in host (`internal/testchain`): every command path, restart and
crash resumption, a funding tree a stopped run left (never sent, on the
chain, landing late, minted ahead, adopted and not published), both
profiles and gzip, a followed file with rotation,
hostile answers, terminal filtering, forks and equivocation, backfill and
the audit's refusals, retraction and retirement, archives with and without
holes, an archive's receipts of retracted batches (including an interval's
last) and its recheck budget, the audit across an unserved seq, a payment
imported by `fund -txid` and by `fund -beef` (mined, and not mined yet and
released later), a producer with no node (WhatsOnChain's shape for the
chain view, arcade to broadcast), mode unicast with a host down under each quorum,
its repair, and a retraction that waits for every host, a sweep on the
plane reaching a host off it directly and by `backfill -sweeps`, and every
hard cap and warning of [limits.md](limits.md).

`make e2e-client` runs the command against two reference overlay hosts
loading the bundle, over the same local chain serving the node's API and
the header source: two produce runs and a digest-only one, read,
verify-interval, a backfill of the second host from the first, a host
restart, a publish that fails and is resumed, archives of both hosts
agreeing on every interval, and a retraction the host module applies at
the facade's host and at the host off the plane (sent to it directly),
which the archive that stored the batch then records and the audit proves
across.
`make e2e-unicast` runs a producer in mode unicast against two reference
hosts: every object on both, a host down under quorum `all` (the run stops)
and quorum `one` (it continues), the printed backfill repairing the host,
both archives agreeing, and a retraction reaching both, recorded by both
archives and proven across at both hosts. Both need Docker for
the hosts' MySQL and a reference host directory holding its compiled
`dist/index.js` and its `node_modules`.

`make quickstart-check` runs [QUICKSTART.md](../QUICKSTART.md) as written
in a throwaway Compose project, from the three images (`make docker-build`,
`make docker-build-host`).

`scripts/plane-demo.sh` runs the command against real hosts on the plane:
three archive hosts, two that receive the stream from the plane and one
that does not. Its endpoints come from a local file
(`scripts/plane-demo.env.example` lists the keys); `setup` creates the home,
funds it and creates the stream once, and prints the lines each host's
configuration needs; `run` publishes new lines and anchors them, reads them
back from the two plane hosts, audits the interval at both, backfills the
third host and audits it there, archives all three, and requires one root
from all six. Each run adds an interval, so it is run twice in a row.

### Moving bcommon

bcommon is pinned at an exact tag on both sides. To move it, change the Go
side with `go get github.com/lightwebinc/bcommon@<tag>`, pack the library's
`ts/` package at the same tag into
`host/vendor/lightwebinc-bcommon-<version>.tgz`, point `host/package.json`
at it, and reinstall (`cd host && npm install`). A test fails while the Go
and TypeScript sides name different versions, and `make deps-check` fails
while go-sdk resolves to anything but v1.7.1.
