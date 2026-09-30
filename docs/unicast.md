# Unicast mode

blogs runs in one of two modes, chosen by the producer's `mode` setting.
The records, the hosts and the readers are the same in both; what differs
is how a producer's objects reach the hosts.

| | `plane` (the default) | `unicast` |
| --- | --- | --- |
| The producer submits each object | once, to `facade` | to every host in `hosts`, one by one |
| Every other host receives it | from the multicast plane, which repairs loss | only from the producer, or later from a sibling by `blogs backfill` |
| The producer's upload per object | one copy | one copy per host |
| A host that was down | receives what it missed from the plane's repair | misses it until a backfill copies it across |
| Needs | a plane that carries the stream's topic | nothing but the hosts |

Readers, auditors and archives are the same in both modes: they ask the
hosts in `hosts` directly with the lookup classes, and verify every answer
against their own header source.

```text
  plane:    producer ──submit──▶ facade ══plane══▶ host A, host B, host C
  unicast:  producer ──submit──▶ host A
                     ──submit──▶ host B
                     ──submit──▶ host C          (each retried on its own)
            reader   ──lookup──▶ any of them, verified
```

## The tradeoff

Without the plane, the producer does the plane's work itself. It sends every
carrier, funding tree and anchor to each host, so its upload, its time per
batch and its exposure to a slow or failing host all grow with the number of
hosts. The hosts do not pass objects to each other: a host that missed a
submission holds nothing for that position until it is backfilled from a
sibling (or synced with GASP, which carries batches but not every anchor;
see [host.md](host.md#limits)). With two or three hosts this is cheap and
simple; with many hosts, or many producers, the plane is what removes the
cost.

## Configuration

```text
# ~/.blogs/config
mode       = unicast
hosts      = https://logs-a.example.com,https://logs-b.example.com
quorum     = all
header_url = woc:main
settle     = arcade:https://arc.example.com/v1
asset      = http://192.0.2.10:8090
```

| Key | Flag | Default | Meaning |
| --- | --- | --- | --- |
| `mode` | `-mode` | `plane` | `plane` submits once to `facade`; `unicast` submits to each of `hosts` |
| `hosts` | `-hosts` | none | the hosts, comma separated, at most 16: every host a unicast producer submits to, and the hosts a reader asks, in order. Above 5 the producer warns: its upload per object is better done by the plane |
| `quorum` | `-quorum` | `all` | how many hosts must take an object for it to count as published: `all`, `majority` (more than half), `one`, or a number from 1 to the host count. Below a majority the producer warns |

In unicast mode `facade` must be unset (the producer refuses one, so a
configuration cannot mean both), and `hosts` must name at least one host.
A `quorum` above the host count is refused before anything is sent.

## What "published" means

Each object is sent to every host at once, and each host's answer is
recorded. A host **took** the object when it answered the submit with 200
and a STEAK for the topic. An answer that admits nothing still counts: the
host has decided (it holds the object already, or its topic manager refused
it), and the producer cannot tell those two apart from the answer
([spec.md](spec.md) section 6.3). The producer reports such a host, and a
reader's lookup shows whether it holds the object.

The object is **published** once at least `quorum` hosts took it. Then:

- **A carrier** is marked published in the stream's state and the next batch
  is built. Below the quorum the run stops with exit 2 and the carrier
  persisted, exactly as a plane producer stops when the facade is down; the
  next `produce` or `anchor` sends the same bytes to every host again, and a
  host that already holds them answers a duplicate.
- **An anchor** is recorded as the head of the anchor chain. Below the
  quorum it is sent again on the next tick.
- **A funding tree** is recorded before it is sent. Below the quorum the run
  stops. A host that misses a batch tree needs nothing: each carrier's BEEF
  carries the tree it spends. A host that misses an anchor fee tree is
  given it by `backfill` (from `asset` or `tx_source`) before the anchor.
- **A sweep** (a retraction or a retirement) needs every host, whatever the
  quorum, because no lookup class serves a sweep: a backfill of batches
  cannot carry one, and only `backfill -sweeps`, which finds the mined spend
  of each held batch's funding output, can give it to a host later. Below
  all hosts, `sweep` stops with the sweep mined and persisted, and the next
  `sweep` run sends it to every host. The same rule holds on the plane for
  the hosts in `hosts` other than the facade ([usage.md](usage.md#sweeping)).

`all` is the default because it keeps every host complete without a
backfill. `majority` or `one` keep producing while a host is down, at the
price of a backfill afterwards.

## Retries

Each host is retried on its own, with the waits 1s, 2s and 4s between four
tries, all hosts at once, so one slow host does not delay another's answer.
The tries and waits are fixed; each try is bounded by `timeout` (1s to 5m),
so the longest a host can hold up one object is four timeouts and 7s. One
object is in flight per stream at a time, so a producer has at most one
request open per host.
A host that fails every try **missed** the object. Its misses are listed as
they happen:

```text
seq 12: host https://logs-b.example.com MISSED it after 4 tries: publish: facade: ...
```

and at the end of every `produce` and `anchor` run, one line per host and,
when a host missed anything, the command that repairs it:

```text
host https://logs-a.example.com: took 14 object(s), missed 0
host https://logs-b.example.com: took 11 object(s), missed 3: seq 12, seq 13, anchor interval 4
a host missed objects; once it is back, copy them from the others: blogs backfill app_qzxkvbmwtr 02c0ffee... -seqs 12-13 -anchors 4
```

With `-v`, every host's answer to every object is listed too.

## Repairing a host

`blogs backfill` with no `-from` and no `-to` brings the configured hosts
into step: each host in turn is asked for every position, and a position it
lacks is fetched from the first other host that holds it, checked, and
submitted to it.

```bash
blogs backfill app_qzxkvbmwtr 02c0ffee... -seqs 12-13 -anchors 4
```

`-to URL` fills one host from all the others; `-from URL` copies from one
host only. Nothing needs the producer's home, so an archive operator can
repair their own host. When in doubt about what a host missed, repair the
whole stream: positions a host holds are reported `held` and cost one
question each. `-seqs` takes at most 65536 seqs per run. A host that was
down during a `sweep` also needs `-sweeps`, which carries the sweeps it
missed (one spend read per held batch).

```bash
blogs stream list                                   # seq S, anchored to interval K
blogs backfill <stream> <producer> -seqs 1-S -anchors K
```

A backfill cannot repair what no host holds. Every published object was
taken by at least one host, so a full repair completes as long as the hosts
that took each object still hold it.

## Limits

What differs from the plane, in brief ([limits.md](limits.md) has every
limit and its reason):

- batches can be larger: `-max-bytes` defaults to the record's 256 KiB
  carried bound for full-bytes lines (1 MiB of lines for gzip), because
  each host takes an object over HTTP and TCP repairs loss, where the plane
  cuts it into 1128-byte fragments that each need repair;
- the cost of a batch is multiplied by the host count: a producer at
  `-max-rate` R sends R times the number of hosts objects a second, which
  is why `hosts` is capped at 16 and a warning starts above 5.

## Trying it

[QUICKSTART.md](../QUICKSTART.md) runs a unicast producer against two local
hosts, stops one, and repairs it. `make e2e-unicast` does the same against
two reference hosts, with a retraction and archives of both copies.
