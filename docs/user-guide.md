# blogs user guide

How to keep a log on Bitcoin, read it back, and prove it. New here? Start
with [QUICKSTART.md](../QUICKSTART.md). Every command, flag and message is
in [usage.md](usage.md), every default and cap in [limits.md](limits.md),
running without the multicast plane in [unicast.md](unicast.md), hosts in
[self-host.md](self-host.md) and [host.md](host.md), and the normative
reference in [spec.md](spec.md). Every key, txid, stream and hostname below
is illustrative.

## 1. What this is

A log you keep yourself can be edited by whoever holds the machine, and a
log someone else keeps can be edited by them. blogs gives a log a witness
nobody controls. A producer signs its lines in numbered batches and hands
them to overlay hosts, which keep copies. Once per interval the producer
mines one small anchor transaction that commits a Merkle root over that
interval's batches. From then on anyone holding block headers can prove
three things about a line: the producer's key signed it, it sat at that
position in the stream, and it was committed no later than the anchor's
block. Nobody, the producer included, can later insert, drop or reorder a
line inside an anchored interval without the proof failing.

What it does not prove: that a line is true, or when it was written before
the anchor. What it costs: about 6 satoshis per batch and 113 per anchor
at the default fee (section 11).

## 2. Install

- **Container image**: `ghcr.io/lightwebinc/blogs` (the command),
  `ghcr.io/lightwebinc/blogs-host` (a host), and
  `ghcr.io/lightwebinc/blogs-devchain` (a private regtest chain for the
  local development sandbox).
  Mount a named volume at `/home/nonroot/.blogs` and pass settings as
  `BLOGS_<KEY>` variables.
- **Go**: `go install github.com/lightwebinc/blogs/cmd/blogs@latest`
  (Go 1.27.2 or later).
- **Release binaries**: static `blogs` for Linux and macOS (amd64, arm64)
  on the [releases page](https://github.com/lightwebinc/blogs/releases),
  with `SHA256SUMS`.
- **From source**: `make blogs` builds `bin/blogs`. The only direct
  dependencies are go-sdk and bcommon, both pinned.

`blogs -version` prints the version. `blogs doctor` reads your settings and
home and asks every configured endpoint whether it answers; it changes
nothing, and is the first thing to run when something is wrong.

## 3. Concepts

| Word | What it is |
| --- | --- |
| **Producer** | An identity key in a producer home (`~/.blogs`). The key is the producer: readers name it, and hosts admit only its batches on its streams |
| **Stream** | `<name>_<suffix>`, for example `app_qzxkvbmwtr`: a readable name and ten random letters drawn once, so nobody else's stream shares its topic `tm_log_app_qzxkvbmwtr` |
| **Batch** | Up to a few thousand lines, signed, with a `seq` (1, 2, 3, ...) and `prev`, the commitment of the batch before it. The lines ride in the record (`full`) or live elsewhere under their SHA-256 (`digest`) |
| **Carrier** | The unmined transaction a batch rides in. It spends one output of a mined **funding tree**, so it verifies by SPV without paying for block space. Its txid is the batch's **commitment** |
| **Anchor** | One mined transaction per **interval** that commits the RFC 6962 root over the interval's batches and spends the previous interval's anchor, so the anchors form one chain covering every batch once |
| **Host** | An overlay host that keeps the stream's topic and answers three questions: the batch at a `seq`, the anchor of an interval, and a funding tree by its txid. Hosts are replicas, not authorities: a host can withhold, but it cannot forge |
| **Plane or unicast** | How a producer reaches the hosts: once to a facade on the multicast plane, which delivers to every subscribed host and repairs loss; or to each host itself (section 10) |
| **Retraction** | A producer takes batches back by mining a **sweep** that spends their funding outputs. Hosts drop the bytes and keep the position, which the anchor still commits |

## 4. Configuration

Settings come from global flags, then `BLOGS_<KEY>` variables, then the
config file (`$BLOGS_HOME/config`, else `~/.blogs/config`), then defaults.
The file is one `key = value` per line; an unknown key is an error, never
ignored. Every key, variable and flag is in [configuration.md](configuration.md).

`network` is `main` (the default) or `test`; `regtest` is for a chain you
run yourself. No node and no infrastructure of your own is needed: on
mainnet and testnet the chain services default to public ones, and what
they answer is checked.

| Role | Key | Default | What is checked |
| --- | --- | --- | --- |
| Header source | `header_url` | `woc:main` (`woc:test`), the public WhatsOnChain API | every header is hashed and must carry the proof of work it claims, so a lie costs a mined block |
| Chain view | `chain` | `woc:main` (`woc:test`) | a transaction must hash to its txid, and a proof must verify against the headers; "unspent" and "not mined" are WhatsOnChain's word |
| Broadcaster | `settle` | `arcade:main` (`arcade:test`), GorillaPool's public arcade | its verdict is held to the chain view's spends |
| Miner fee | `fee_rate` | `100/1000`, the network's rate, with a 100 satoshi floor per transaction | |

The overlay hosts are the one thing you name, because a default there would
send your batches, or your questions, to a server nobody chose. A producer
on mainnet, submitting to its hosts itself (section 10):

```text
network    = main
mode       = unicast
hosts      = https://logs-a.example.com,https://logs-b.example.com
```

On testnet, `network = test`, and the defaults follow it. On the multicast
plane, `mode = plane` (the default) and `facade =
https://logs-a.example.com` replace `mode = unicast`.

Any public service can be replaced by one you run, which is the stronger
choice where you have one: a node's asset API as the chain view (`chain =
asset:http://node.example.com:8090`, or `asset:URL,woc:main` to fall back to
WhatsOnChain for what the node lacks), a block-headers-service or an
arcade's header server as the header source (`header_url =
bhs:https://headers.example.com`, `arcade:https://arcade.example.com`), and
your own arcade or an ARC service as the broadcaster (`settle =
arcade:https://arcade.example.com`, `arc:https://arc.example.com`).
`fee_source = arc` pays the rate the broadcaster publishes instead of a
fixed one, within `fee_min_rate` and `fee_max_rate`.

A reader or auditor needs no node, no coin and no home, only the hosts:

```text
network    = main
hosts      = https://logs-a.example.com,https://logs-b.example.com
```

`header_url` is the header source every proof is checked against: a proof
is only as good as the headers it is checked against, and headers from the
host that answered would prove nothing.

## 5. Producing

```console
$ blogs init
created /home/me/.blogs
identity     02c0ffee...
fund address 1BLogs... (main)
```

Pay the fund address from your own wallet (about 50,000 satoshis starts a
stream; section 11), then import the payment. The simplest way is the
payment itself, as BEEF (a BRC-100 wallet hands a payment over in that
form), and
blogs checks it against the header source with no lookup and no wait for a
block. Its coin is spent once the payment mines; until then `doctor` counts
it as held. Or wait for one confirmation and import it by its txid: blogs
reads it with its proof from the chain view and checks the proof. Either
way every output paying the fund address goes into the coin pool:

```console
$ blogs fund -beef payment.beef
imported 1 of 1 output(s) paying 1BLogs..., 50000 sat, not mined yet: held until its proof arrives, which the next produce, anchor or sweep collects; pool 1 output(s), 50000 sat
$ blogs fund -txid 6f1e...a9c2
imported 1 of 1 output(s) paying 1BLogs..., 50000 sat, mined at height 970061; pool 1 output(s), 50000 sat
$ blogs stream new app
stream   app_qzxkvbmwtr
topic    tm_log_app_qzxkvbmwtr
producer 02c0ffee...
host     BLOGS_STREAMS=app_qzxkvbmwtr=02c0ffee...
$ tail -F /var/log/app.log | blogs produce app_qzxkvbmwtr
```

`blogs fund` with no `-txid` or `-beef` (and its `-blocks`, `-batch` and `-rescan`
flags) mines coinbase instead. That is coinbase: only on a regtest chain you
run (development and tests), such as the local development sandbox in
[QUICKSTART.md](../QUICKSTART.md); no public network answers it.

Give the `host` line to whoever runs your hosts: it admits only your key on
the stream. Then feed lines on stdin, or name a file with
`-file /var/log/app.log -follow`, which follows it through truncation and
rotation and resumes where the last run stopped.

A batch is cut at `-max-bytes`, at `-max-lines`, or `-max-wait` (1 s) after
its first line, whichever comes first, and at most `-max-rate` batches a
second are published (2 by default). Every `-interval` (10 minutes) the
batches published since the last anchor are anchored, or sooner once
`-anchor-batches` (4096) are waiting. At the end of the input the last
anchor is built and waited for.

Each carrier is written to the stream's state before it is published. A
crash or an unreachable facade stops the run with the carrier saved, and
the next `produce` or `anchor` publishes those same bytes first, so a
restart never forks the stream. An interrupt publishes the batch being
built and exits 0; the next run collects the anchor in flight.

The same holds for a funding tree. If a run stops while it is minting one
(a crash, a kill, the machine switched off), the next `produce`, `anchor`
or `sweep` asks the chain view what became of the tree before it spends
anything, and tells you: `funding tree ... is recovered` when the tree
reached the chain (nothing is lost), or `... never reached the chain: its
fee coin ... is unspent and back in the pool` when it did not. A tree that
is on the chain and not yet mined is waited for, 10 minutes at most; after
that the run goes on and the next one asks again. You have nothing to do;
`blogs doctor` shows a tree still waiting to be asked about. A tree the
hosts could not be given is published by the next run, first thing. The
details are in [usage.md](usage.md), An interrupted funding tree. This
covers trees paid from the home's own pool, which is how every producer
pays; a tree funded through a BRC-100 wallet (bcommon's `Trees.Fund`) is
not covered.

**Keep the home.** The stream's state holds its head. A producer that lost
it must not restart the stream, because it could not avoid reusing a
`seq`; create a new stream instead.

## 6. Reading

```console
$ blogs read app_qzxkvbmwtr 02c0ffee...
$ blogs read app_qzxkvbmwtr 02c0ffee... -from 120 -to 140 -meta
$ blogs read app_qzxkvbmwtr 02c0ffee... -json | jq -r '.lines[]'
```

For each `seq`, `read` asks every host, checks every carrier answered (the
producer's signature, the `seq`, its funding parent's proof against your
headers), follows the `prev` links, and prints the lines. A host that
answers something that does not verify is named and nothing it answered
is used (exit 3): what you see comes from the hosts whose answers verify.
A `seq` every host answered only with what does not verify is not read,
and is not called a hole. Every line is filtered before it reaches your terminal: control
characters, escape sequences and invisible characters are removed, since
log content is text someone else wrote.

Without `-to`, a read stops at the first `seq` no host holds: the end of the
stream, or a hole. An empty answer proves nothing either way, so a hole is
reported and never read as "that batch does not exist".

## 7. Auditing

```console
$ blogs verify-interval app_qzxkvbmwtr 02c0ffee... 3
interval 1  token 7c2f...  height 912410  seqs 1 to 5
interval 2  token 91ab...  height 912420  seqs 6 to 8
interval 3  token 0d4e...  height 912430  seqs 9 to 12
seq 9  carrier 3e0d...  timestamp 1700000000000  4 line(s)
...
VERIFIED interval 3 of app_qzxkvbmwtr by 02c0ffee...: seqs 9 to 12, root 5d1e..., committed no later than block 912430
```

The auditor walks the anchor chain from interval 1, keeps the one canonical
anchor of each interval, checks each is mined in its own headers, asks the
hosts for every batch of the interval, and requires the root over their
commitments to be the one the anchor committed. VERIFIED means every line
of the interval was signed by the producer, sits where it says, and was
committed no later than the block named.

Every host is asked every question. A host (or, with `-archive`, a file)
that answers something that does not verify is named, `host ...:
REFUSED ...`, and is not used: the interval is proven from what verified,
the VERIFIED line is printed, and the exit is 3. A `seq` answered only
with what does not verify is listed `unverified`, never `unserved`.

**Unserved positions.** A `seq` no host serves takes its commitment from
the next batch's `prev`, so the interval is still proven:

```text
unserved seqs 10: no host serves them (retracted, or not delivered); each commitment is the next batch's prev
VERIFIED interval 3 ...
```

A host serves nothing for a retracted batch and nothing for a batch it
never received, and a lookup answers the two alike, so the auditor names
the position, not the cause. An archive, which looks on chain for the sweep,
is what says `retracted` (section 8). A backfill fills the undelivered kind
(section 10).

An anchor pays its fee from a funding tree, which the audit also checks.
The hosts serve it by its txid, with its proof, for as long as they keep
the stream, so an auditor needs nothing but the hosts and a header source:
a node that has pruned the spent tree does not matter. The chain view
(`chain`) is asked only if no host serves a tree. When nothing
does, the audit is `INCOMPLETE` and names the tree and each source asked.

## 8. Archiving

```console
$ blogs archive app_qzxkvbmwtr 02c0ffee... -dir /srv/blogs-archive -host https://logs-a.example.com
$ blogs archive app_qzxkvbmwtr 02c0ffee... -dir /srv/blogs-archive -follow -poll 1m
```

An archive keeps one host's copy of a stream in local files: every verified
batch, every canonical anchor with the fee tree it spends (under `trees/`,
with its proof), and for each interval a proof file with
every `seq`'s commitment and inclusion path. With the trees kept, the
directory can be audited on its own, with no host:
`blogs verify-interval <stream> <producer> <n> -archive DIR` checks the
files against the header source exactly as it checks a host's answers. It ends with
`latest interval K root R token T`, so two archives are compared by that
line, and an interval file can be checked with no network at all.

Each pass also asks the host again about the batches it stored, at most
`-recheck` of them (1024) per pass, oldest checked first, so a large archive
covers them all over several passes; `-recheck-all` asks about every one.
A stored batch the host no longer serves is checked for a sweep: a mined
transaction spending its funding output. When one is found the archive
keeps a receipt and the sweep under `retracted/`, drops the batch's bytes,
and prints `retracted seqs N`. A batch the host stops serving with no such
spend is kept.

## 9. Sweeping

```console
$ blogs sweep app_qzxkvbmwtr -retract-seqs 7            # prints the plan, changes nothing
$ blogs sweep app_qzxkvbmwtr -retract-seqs 7-9 -yes     # retracts seqs 7 to 9
$ blogs sweep app_qzxkvbmwtr -retire -yes               # unused funding outputs back to the pool
```

A retraction mines one transaction per funding tree that spends the outputs
the named batches spent. Every such carrier is then a double spend: hosts
stop serving it, the position stays, and the anchor still commits it.
Retraction is not erasure: an honest host drops the bytes, but a copy
anyone kept stays a copy. A retirement returns unused funding outputs to
the coin pool and retracts nothing.

**Where a sweep goes.** No question serves a sweep, so a host receives one
only when it is sent to it. On the plane a sweep goes to the facade, which
the plane carries to every subscribed host, and directly to every other
host in `hosts`, so a host off the plane receives it too. In unicast mode it
goes to every host. Either way the sweep counts as published only when every
host named took it; otherwise the run stops with the sweep mined and saved,
and the next `sweep` sends it again. A host that was named nowhere is given
it later by `backfill -sweeps`, which finds each held batch's sweep on
chain.

## 10. Plane or unicast

| | Plane (`mode = plane`) | Unicast (`mode = unicast`) |
| --- | --- | --- |
| The producer sends each object | once, to `facade` | to every host in `hosts` |
| A host that missed something | is repaired by the plane | needs `blogs backfill` |
| The producer's upload | one copy | one copy per host |
| Batches | small: about 48 KiB of lines, under 64 packets | up to the record's 256 KiB |
| Needs | a plane carrying the stream's topic | only the hosts |

On the plane, objects travel as 1128-byte packets and every lost packet is
repaired, but the whole object must be reassembled within a fixed window,
so large objects are lost far more often under loss and distance; the
plane's defaults keep carriers small. Without the plane each host takes an
object in one HTTP request, so batches can be larger, but the producer's
work grows with every host.

In unicast mode `quorum` says how many hosts must take an object: `all` (the
default: every host complete, no repair needed), `majority`, `one`, or a
number. With less than `all`, a host that was down is repaired afterwards:

```console
$ blogs backfill app_qzxkvbmwtr 02c0ffee... -seqs 12-13 -anchors 4
backfill app_qzxkvbmwtr to https://logs-b.example.com: 0 held, 3 filled, 0 missing, 0 refused, 0 unserved, 0 sweep(s) carried
```

A backfill asks each host for each position and copies what it lacks from
the first sibling whose answer verifies. A sibling that answers something
that does not verify is named and the next is asked; it does not fail the
repair. It needs no producer home, so a host operator can repair their
own host. Add `-sweeps` when a host may have missed a sweep.

## 11. Limits

Every setting has a default sized for real use and a hard cap past which it
is refused, each with a reason in [limits.md](limits.md). The ones you are
most likely to meet:

| Setting | Default | Cap |
| --- | --- | --- |
| `-max-bytes`, full-bytes lines | 48 KiB on the plane, 256 KiB unicast | 256 KiB |
| `-max-lines` | 10000 | 65536 |
| `-max-rate` | 2 batches a second | 50 |
| `-interval` | 10 minutes | 1 minute to 24 hours |
| `-anchor-batches` | 4096 | 65536 |
| `hosts` | none | 16 (unicast warns above 5) |
| `archive -recheck` | 1024 a pass | 65536, or `-recheck-all` |
| one `read` | to the end | 65536 seqs |
| streams per home | none | 64 |

A setting past a cap exits 2 with a message naming the limit; one past a
recommendation prints `warning: ...` and goes on.

**Cost.** blogs pays the network's rate, 100 satoshis per 1000 bytes, with
a floor of 100 satoshis per transaction (`fee_rate`, `fee_floor`). A batch
costs about 6 satoshis and an anchor about 113. A stream anchored every ten
minutes around the clock spends about 16,000 satoshis a day on anchors; batches follow how much
you log. `blogs doctor` shows the pool; `blogs sweep -retire` returns unused
funding outputs to it.

## 12. Troubleshooting

Every command exits with one of four codes, and a script can tell a
forgery from an outage by them.

| Exit | Meaning | What to do |
| --- | --- | --- |
| 0 | Done; for a reader, every answer verified; for `backfill`, every position held or filled | Nothing |
| 1 | Refused: what verified contradicts what was asked (a fork nothing resolves, a broken link, a root mismatch); for `archive`, which follows one host, an answer of that host that does not verify | If every host agrees, the producer is at fault; for `archive`, look at the named host |
| 2 | Usage, configuration, local or transport error, including a host or header source that could not be asked: nothing was decided | Read the message; run `blogs doctor` |
| 3 | Incomplete: a host held nothing where evidence was needed, or a host answered something that does not verify (named, and not used), which proves nothing either way | Ask another host, wait for delivery, or backfill. For a named host, tell its operator |

Common messages:

| What you see | Exit | Fix |
| --- | --- | --- |
| `no header source: set header_url ...`, `no chain view: set chain ...` | 2 | Only on a regtest chain, which has no defaults: set `header_url` and `chain` (`asset:URL`) |
| `bwallet: the payment pays nothing to this wallet's fund address ...` | 1 | The payment is not to this home's fund address. `blogs init` prints the address; check the network |
| `bwallet: the payment has not mined yet ...` | 1 | Import it by txid once it has a block, or hand over its BEEF with `fund -beef` |
| `no overlay host configured ...` | 2 | Set `hosts` |
| `facade must be configured ...` | 2 | Set `facade`, or `mode = unicast` with `hosts` |
| `this home holds no stream S ...` | 2 | The stream's state is not in this home. Use the home that created it, or a new stream |
| `another blogs command is using the home ...` | 2 | One producer per home at a time; wait, or give the other stream its own home |
| ``batch funding tree: fee input: ... (run `blogs fund`: ...)`` | 2 | The coin pool is empty: pay the fund address and `blogs fund -beef FILE` or `blogs fund -txid TXID` |
| `fee input: ...: N funding tree(s) signed for a coin of this home are not settled ...` | 2 | The pool's last coin is in a funding tree that has not mined yet. Run the command again once a block has passed, or fund again so the pool holds a second coin |
| `seq N: publish: ...; the carrier is persisted and a later run publishes it` | 2 | The facade, or too few hosts, did not take it. Fix the endpoint and run `produce` or `anchor` again |
| `funding tree TXID: ...; the tree is adopted and recorded as not published, and the next produce, anchor or sweep publishes it` | 2 | The facade, or too few hosts, did not take a new funding tree. Fix the endpoint and run `produce` or `anchor` again; no second tree is minted |
| `... funding tree TXID was signed by a run that stopped before it adopted the tree: asking the chain view what became of it ...` | 0 | An earlier run stopped while minting a tree. The lines after it say what was found; nothing to do |
| `funding tree TXID: what became of it could not be settled now (...)` | 0 | The chain view could not answer, or the tree has not mined yet. The run goes on, and the next one asks again |
| `funding tree TXID: interrupted while asking what became of it ...` | 2 | The command was interrupted while it waited for the tree to mine. Run it again |
| `sweep TXID: published to the facade, but not to every host directly: ...` | 2 | A host in `hosts` is down. Run `blogs sweep <stream>` again once it is back |
| `sweep TXID refused by the network: input N (...) is spent by ...; it is marked failed ...` | 1 | Another transaction spent one of the sweep's inputs, so it can never mine. It is recorded as failed and no longer blocks; the message says whether its fee coin went back to the pool. Run the sweep again if it is still wanted |
| `sweep TXID: ...; it is persisted and the next sweep resumes it` | 2 | The sweep did not settle or mine in time, and its inputs are not spent elsewhere. Run `blogs sweep <stream>` again |
| `... is over the limit of ...` | 2 | A setting past a hard cap; [limits.md](limits.md) says why |
| `REFUSED interval N: root-mismatch ...` | 1 | The producer anchored batches it did not publish, or a host answered other batches. Try another host; if every host agrees, the producer's anchor is wrong |
| `seq N: BROKEN LINK: ...`, `seq N: FORK: ...` | 1 | A host answered a batch that does not continue the stream, or the producer signed two batches for one `seq` |
| `INCOMPLETE interval N: no host answered ...` | 3 | No host you asked holds that interval yet: wait for delivery, ask another host, or backfill |
| `INCOMPLETE interval N: ... a funding tree nothing asked serves: ...` | 3 | No source served the fee tree an anchor spends; the line says what each did. A host that `did not answer the tree class` runs an older module, and one that `holds no such tree` never received it: ask another host, add `-archive DIR`, or `backfill -anchors` the host from a sibling that has it |
| `INCOMPLETE interval N: ... cannot be recovered from the next batch's prev` | 3 | The interval's last batch is served by no host (retracted, or not delivered). An archive that stored it, or its receipt, still proves the interval |
| `seq N: MISSING: no host holds it` | 3 | A hole: not delivered, or retracted. `backfill` fills the first kind |
| `host ...: REFUSED ...` | 3 | A host answered something that does not verify (bytes changed on the way, or by the host). It is not used; what you see comes from the other hosts. Look at the named host. In `archive` this is exit 1 |
| `seq N: NOT READ: ... none verifies against the header source ...`, `... none verifies against the header source ...` | 3 | Every answer was refused. Check `header_url` (`blogs doctor`) before blaming the hosts: a header source that is behind fails every answer the same way |
| `could not verify: the header source URL did not answer: ...` | 2 | The header source is down or unreachable. Fix `header_url`, or wait for it |
| `unverified seqs N: ...` before `VERIFIED` | 3 | The interval is proven; those positions were answered only with what does not verify, each named |
| `unserved seqs N: ...` before `VERIFIED` | 0 | The interval is proven; those positions are empty at every host asked. An archive tells a retraction from a loss |

## 13. Where to go next

- [usage.md](usage.md): every command, flag, setting, exit code and message
- [unicast.md](unicast.md): quorums, retries and repair without the plane
- [limits.md](limits.md): every default and cap, and where the numbers come from
- [self-host.md](self-host.md) and [host.md](host.md): running a host
- [spec.md](spec.md) and [frozen.md](frozen.md): the records and rules a compatible producer, reader or host follows

`blogs help`, or `-h` after any command, prints its flags and exits 0.
