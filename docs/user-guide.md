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
the anchor. What it costs: about 50 satoshis per batch and 600 per anchor
at the default fee (section 11).

## 2. Install

- **Container image**: `ghcr.io/lightwebinc/blogs` (the command),
  `ghcr.io/lightwebinc/blogs-host` (a host), and
  `ghcr.io/lightwebinc/blogs-devchain` (a local chain for trying things).
  Mount a named volume at `/home/nonroot/.blogs` and pass settings as
  `BLOGS_<KEY>` variables.
- **From source**: `make blogs` builds `bin/blogs` (Go 1.27.1 or later).
  The only direct dependencies are go-sdk and bcommon, both pinned.

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
| **Host** | An overlay host that keeps the stream's topic and answers two questions: the batch at a `seq`, and the anchor of an interval. Hosts are replicas, not authorities: a host can withhold, but it cannot forge |
| **Plane or unicast** | How a producer reaches the hosts: once to a facade on the multicast plane, which delivers to every subscribed host and repairs loss; or to each host itself (section 10) |
| **Retraction** | A producer takes batches back by mining a **sweep** that spends their funding outputs. Hosts drop the bytes and keep the position, which the anchor still commits |

## 4. Configuration

Settings come from global flags, then `BLOGS_<KEY>` variables, then the
config file (`$BLOGS_HOME/config`, else `~/.blogs/config`), then defaults.
The file is one `key = value` per line; an unknown key is an error, never
ignored. The addresses have no default: a default would send your batches,
or your questions, to a server nobody chose.

A producer on the plane:

```text
network    = main
asset      = http://192.0.2.10:8090
settle     = arcade:https://arc.example.com/v1
facade     = https://logs-a.example.com
hosts      = https://logs-a.example.com,https://logs-b.example.com
header_url = woc:main
```

A reader or auditor needs no node, no coin and no home:

```text
network    = main
hosts      = https://logs-a.example.com,https://logs-b.example.com
header_url = woc:main
tx_source  = woc:main
```

`header_url` is the header source every proof is checked against. It has no
default: a proof is only as good as the headers it is checked against, and
headers from the host that answered would prove nothing. `woc:main` is the
public WhatsOnChain API, and every header it serves is checked for proof of
work.

## 5. Producing

```console
$ blogs init
created /home/me/.blogs
identity     02c0ffee...
fund address 1BLogs... (main)
$ blogs fund -txid 1111...1111        # after paying the fund address from any wallet
$ blogs stream new app
stream   app_qzxkvbmwtr
topic    tm_log_app_qzxkvbmwtr
producer 02c0ffee...
host     BLOGS_STREAMS=app_qzxkvbmwtr=02c0ffee...
$ tail -F /var/log/app.log | blogs produce app_qzxkvbmwtr
```

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

**Keep the home.** The stream's state holds its head. A producer that lost
it must not restart the stream, because it could not avoid reusing a
`seq`; create a new stream instead.

## 6. Reading

```console
$ blogs read app_qzxkvbmwtr 02c0ffee...
$ blogs read app_qzxkvbmwtr 02c0ffee... -from 120 -to 140 -meta
$ blogs read app_qzxkvbmwtr 02c0ffee... -json | jq -r '.lines[]'
```

For each `seq`, `read` asks the hosts in order until one answers, checks
the carrier (the producer's signature, the `seq`, its funding parent's
proof against your headers), follows the `prev` links, and prints the
lines. Every line is filtered before it reaches your terminal: control
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

An anchor pays its fee from a funding tree no question serves, so an
auditor also needs `asset` (a node) or `tx_source = woc:main`.

## 8. Archiving

```console
$ blogs archive app_qzxkvbmwtr 02c0ffee... -dir /srv/blogs-archive -host https://logs-a.example.com
$ blogs archive app_qzxkvbmwtr 02c0ffee... -dir /srv/blogs-archive -follow -poll 1m
```

An archive keeps one host's copy of a stream in local files: every verified
batch, every canonical anchor, and for each interval a proof file with
every `seq`'s commitment and inclusion path. It ends with
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
a sibling, checked. It needs no producer home, so a host operator can
repair their own host. Add `-sweeps` when a host may have missed a sweep.

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

**Cost.** At 1 satoshi per byte a batch costs about 50 satoshis and an
anchor about 600. A stream anchored every ten minutes around the clock
spends under 100,000 satoshis a day on anchors; batches follow how much
you log. `blogs doctor` shows the pool; `blogs sweep -retire` returns unused
funding outputs to it.

## 12. Troubleshooting

Every command exits with one of four codes, and a script can tell a
forgery from an outage by them.

| Exit | Meaning | What to do |
| --- | --- | --- |
| 0 | Done; for a reader, every answer verified | Nothing |
| 1 | Refused: an answer contradicts what was asked (a bad signature or proof, another `seq` or producer, a fork nothing resolves, a broken link, a root mismatch) | Do not trust the host that answered it; tell its operator. Run with `-v` to see every refused answer |
| 2 | Usage, configuration, local or transport error: nothing was decided | Read the message; run `blogs doctor` |
| 3 | Incomplete: a host held nothing where evidence was needed, which proves nothing either way | Ask another host, wait for delivery, or backfill |

Common messages:

| What you see | Exit | Fix |
| --- | --- | --- |
| `no header source: set header_url ...` | 2 | Set `header_url`, for example `woc:main` |
| `no overlay host configured ...` | 2 | Set `hosts` |
| `facade must be configured ...` | 2 | Set `facade`, or `mode = unicast` with `hosts` |
| `this home holds no stream S ...` | 2 | The stream's state is not in this home. Use the home that created it, or a new stream |
| `another blogs command is using the home ...` | 2 | One producer per home at a time; wait, or give the other stream its own home |
| `batch funding tree: fee input: ... (run blogs fund)` | 2 | The coin pool is empty: `blogs fund` |
| `seq N: publish: ...; the carrier is persisted and a later run publishes it` | 2 | The facade, or too few hosts, did not take it. Fix the endpoint and run `produce` or `anchor` again |
| `sweep TXID: published to the facade, but not to every host directly: ...` | 2 | A host in `hosts` is down. Run `blogs sweep <stream>` again once it is back |
| `... is over the limit of ...` | 2 | A setting past a hard cap; [limits.md](limits.md) says why |
| `REFUSED interval N: root-mismatch ...` | 1 | The producer anchored batches it did not publish, or a host answered other batches. Try another host; if every host agrees, the producer's anchor is wrong |
| `seq N: BROKEN LINK: ...`, `seq N: FORK: ...` | 1 | A host answered a batch that does not continue the stream, or the producer signed two batches for one `seq` |
| `INCOMPLETE interval N: no host answered ...` | 3 | No host you asked holds that interval yet: wait for delivery, ask another host, or backfill |
| `INCOMPLETE interval N: ... cannot be recovered from the next batch's prev` | 3 | The interval's last batch is served by no host (retracted, or not delivered). An archive that stored it, or its receipt, still proves the interval |
| `seq N: MISSING: no host holds it` | 3 | A hole: not delivered, or retracted. `backfill` fills the first kind |
| `unserved seqs N: ...` before `VERIFIED` | 0 | The interval is proven; those positions are empty at every host asked. An archive tells a retraction from a loss |

## 13. Where to go next

- [usage.md](usage.md): every command, flag, setting, exit code and message
- [unicast.md](unicast.md): quorums, retries and repair without the plane
- [limits.md](limits.md): every default and cap, and where the numbers come from
- [self-host.md](self-host.md) and [host.md](host.md): running a host
- [spec.md](spec.md) and [frozen.md](frozen.md): the records and rules a compatible producer, reader or host follows

`blogs help`, or `-h` after any command, prints its flags and exits 0.
