# The blogs command

`blogs` produces and reads blogs log streams ([spec.md](spec.md)). A
producer turns log lines into signed, numbered batches on a stream's topic
and anchors them in one mined token per interval. A reader asks overlay
hosts for a stream's batches and anchors and checks every answer itself,
against block headers it chooses, before it prints a line.

```text
  producer home                                   overlay hosts (tm_log_<stream>, ls_log)
  ─────────────                                   ───────────────────────────────────────
  init ─▶ fund ─▶ stream new ─▶ produce ──carriers, trees, anchors──▶ facade
                                  │  └─trees, anchors──▶ settlement leg ─▶ chain
                                  └─ anchor (finish, resume)
  reader (no home)
  ────────────────
  read, verify-interval ◀── ls_log answers ── hosts;  proofs checked against header_url
  backfill: ls_log answers of a sibling ─▶ submit route of the host being filled
  archive:  ls_log answers of one host ─▶ files: batches, anchors, interval proofs
```

## Build

Go 1.27 or later.

```bash
make blogs              # bin/blogs
go build ./cmd/blogs    # or directly
```

## Commands

| Command | Needs a home | Needs | What it does |
| --- | --- | --- | --- |
| `init` | creates it | nothing | identity key and empty coin pool; prints the identity and fund address |
| `fund [-blocks N]` | yes | `rpc`, `asset` | mines coinbase to the fund address through `generatetoaddress` |
| `stream new <name>` | yes | nothing | draws the random suffix; prints the stream, its topic, and the host's `BLOGS_STREAMS` entry |
| `stream list` | yes | nothing | the home's streams and how far each is anchored |
| `produce <stream>` | yes | `facade`, `settle`, `asset` | stdin or a file to batches, published and anchored |
| `anchor <stream>` | yes | `facade`, `settle`, `asset` | publishes a persisted carrier, anchors every published batch, waits |
| `doctor` | optional | nothing | the home's state and whether the node, headers, facade and hosts answer; reads only |
| `read <stream> <producer>` | no | `hosts`, `header_url` | verifies and prints a stream's batches |
| `verify-interval <stream> <producer> <n>` | no | `hosts`, `header_url`; `asset` for fee trees | the auditor's check of interval n |
| `sweep <stream>` | yes | `facade`, `settle`, `asset`; `hosts` for `-retract-seqs` | retracts batches by sweeping their funding outputs, or retires unused funding outputs to the pool |
| `backfill <stream> <producer>` | no | `-from`, `-to`; `asset` for fee trees | fills a host's holes from a sibling host |
| `archive <stream> <producer>` | no | `-dir`, a host, `header_url`, `asset` | keeps one host's copy of a stream, with each interval's proof, in local files |
| `version` | no | nothing | prints the version |

`<stream>` is always the full identifier `<name>_<suffix>`; the readable
name alone is refused. `<producer>` is the producer's identity key, 66
lowercase hex characters. `blogs <command> -h` prints a command's flags.

## Configuration

Settings come from, highest first: the global flags, `BLOGS_<KEY>` in the
environment (the key in upper case), the config file, and the defaults. The
config file is `-config PATH`, else `$BLOGS_HOME/config`, else
`$XDG_CONFIG_HOME/blogs/config`, else `~/.blogs/config`. Its grammar is one
`key = value` per line, `#` comments, blank lines ignored; a key it does not
define, a key given twice, or a line without `=` is an error, never ignored.

| Key | Default | Meaning |
| --- | --- | --- |
| `arcade_key` | none | bearer token for an arcade installation named in `settle` |
| `asset` | none | the node's asset API base URL: proofs, the chain tip, blocks, raw transactions |
| `facade` | none | the overlay host a producer publishes to (its `/submit` route) |
| `header_url` | none | the header source every proof is checked against: an overlay bridge's base URL (native `/v1/root/<height>` and `/v1/tip`), `woc:main`, `woc:test` or `chaintracks:URL` |
| `home` | `~/.blogs` | the producer home |
| `hosts` | none | the overlay hosts a reader asks, comma separated, in order |
| `network` | `main` | `main`, `test` or `regtest`: the fund address prefix and the header source's proof-of-work floor |
| `object_bound` | `1048576` | the plane's object bound in bytes; a carrier whose BEEF is larger is not published |
| `originator` | `blogs` | the BRC-100 originator presented to the wallet |
| `rpc` | none | the node's JSON-RPC URL (`fund`) |
| `rpc_pass`, `rpc_user` | `bitcoin` | the node's basic auth, also for `settle = rpc:` |
| `settle` | none | the settlement leg: `tcp:<host:port>` (bare EF to an ingress), `rpc:<url>` (a node's `sendrawtransaction`) or `arcade:<url>` |
| `timeout` | `15s` | bounds each request |

The addresses have no default on purpose: a default would send a producer's
objects, or a reader's questions, to a server nobody configured. A command
that needs one and finds none exits 2 naming the key.

Global flags, before the command: `-config PATH`, `-home DIR`,
`-hosts URLS`, `-header-url SOURCE`, `-network NET`, `-timeout DUR`, `-v`
(also accepted after the command), `-version`.

A producer's config file, with documentation addresses:

```text
# ~/.blogs/config
network    = test
asset      = http://192.0.2.10:8090
rpc        = http://192.0.2.10:9292
settle     = rpc:http://192.0.2.10:9292
facade     = https://archive-a.example.com
hosts      = https://archive-a.example.com,https://archive-b.example.com
header_url = https://headers.example.com
```

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
anchor chain's head and the anchor in flight, the funding trees, and the
sweeps built from them. Losing it loses the head, and a producer that lost
its head must not restart the stream (specification section 12): `produce`
refuses a stream the home does not hold, and the answer is a new stream
from `stream new`.

One `produce`, `anchor` or `fund` runs per home at a time, because they
share the coin pool; a second exits 2. To run several producers at once,
give each its own home. The fund key is the embedded wallet's key under
`[1, "blogs"]`, key id `fund`.

## Producing

```bash
blogs init
blogs fund -blocks 101                 # a chain you run; or pay the fund address
blogs stream new app                   # prints app_<suffix> and tm_log_app_<suffix>
tail -F /var/log/app.log | blogs produce app_qzxkvbmwtr
blogs produce app_qzxkvbmwtr -file /var/log/app.log -follow
```

`stream new` prints the line a host operator adds to `BLOGS_STREAMS`
([host.md](host.md)) to admit only this producer on the stream, and the
topic to add to `OVERLAY_TOPICS`.

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
| `-max-bytes N` | 262144 for `full` `lines`, else 1048576 | bytes of lines per batch |
| `-max-lines N` | 10000 | lines per batch |
| `-max-wait DUR` | `1s` | the latency floor: a batch is published at most this long after its first line |
| `-unsafe` | `warn` | lines holding control characters, escape sequences other than colour, or zero-width and bidirectional characters (`termsafe.Validate`): `warn` publishes them and counts them at exit, `drop` does not publish them |

A batch whose encoding is over the record's bound (256 KiB carried, 16 MiB
located) is split at a line boundary into several batches. A carrier whose
BEEF is over `object_bound` is not published and the run stops.

### Anchors

Once `-interval` (default `10m`) has passed since the last anchor and a
published batch is unanchored, `produce` builds the next anchor over every
unanchored batch (at most 65536), hands it to the settlement leg, and
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
| `-tree-count` | 100 | outputs per batch tree. Each carrier's BEEF carries its tree, so this is per-batch overhead |
| `-tree-sats` | 1 | satoshis per batch output. A carrier carries its output's whole value and is never mined, so this value stays with the batch until a sweep |
| `-ahead` | 25 | mint the next batch tree at this many outputs left |
| `-anchor-tree-count` | 8 | outputs per anchor fee tree |
| `-anchor-tree-sats` | 1000 | satoshis per anchor fee output; one pays an anchor's fee (at 1 sat/byte, about 450 to 550) |
| `-anchor-ahead` | 2 | mint the next anchor fee tree at this many outputs left |
| `-poll` | `5s` | how often proofs are asked for |

A tree minted ahead that a run ends without using is recorded in the state;
its change is back in the pool, its outputs are not spent by a carrier
again, and `blogs sweep -retire` pays them back to the pool.

### Restart and crash

Each carrier is written to the state before it is published. A run that
cannot publish it (the facade is down; tried four times) exits 2 with the
carrier persisted, and the next `produce` or `anchor` publishes those same
bytes before anything else: the same txid, which a host that already holds
it answers as a duplicate. So a restart continues at the next `seq`, and no
two carriers are ever built for one. An anchor built and not yet published
is collected by the next run, and an anchor the network refused is built
again over the same batches. An interrupt (SIGINT, SIGTERM) publishes the
batch being built and exits 0, as does an interrupt of `anchor` or of the
final wait; the anchor in flight is collected next time. A final wait or
`anchor -wait` that runs out exits 2.

## Reading

```bash
blogs read app_qzxkvbmwtr 02c0ffee...                 # all of it, as lines
blogs read app_qzxkvbmwtr 02c0ffee... -from 120 -to 140 -meta
blogs read app_qzxkvbmwtr 02c0ffee... -json | jq -r '.lines[]'
blogs read app_qzxkvbmwtr 02c0ffee... -content-dir /srv/logs -allow-host logs.example.com
```

(`02c0ffee...` stands for the producer's 66-character key.)

For each `seq` from `-from`, `read` asks the hosts in order until one
answers, and checks every carrier answered: the carrier rules of
specification section 8.1 with this producer as the only producer, the
asked `seq`, and SPV through its funding parent against `header_url`. It
follows the `prev` links, and where a host answers several candidates for a
`seq` (a fork), the next batch's `prev` chooses. It decodes the content and
prints its lines, each filtered for the terminal (`termsafe`: controls,
escape sequences, zero-width and bidirectional characters removed; a line
longer than 512 columns is wrapped, not cut). Without `-to` it stops at the
first `seq` no host holds, which is the end of what the hosts hold or a
hole; an empty answer proves nothing.

| Flag | |
| --- | --- |
| `-from N`, `-to N` | the range; without `-to`, until no host holds the next `seq` |
| `-meta` | a line before each batch: `# seq N  timestamp MS  C line(s)  carrier TXID  VERIFIED  linked  content ok` |
| `-json` | one JSON object per batch: `seq`, `carrier`, `timestamp`, `count`, `profile`, `encoding`, `verdict`, `link`, `content`, `lines` (filtered as for a terminal) |
| `-ansi` | let colour through; `-ascii` prints non-ASCII as `?` (the default when the locale is not UTF-8) |
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
   interval's carriers carry, or from `asset`. An anchor pays its fee from
   an anchor fee tree, which no carrier carries and no lookup class serves,
   so an auditor gives `asset` (any node's asset API);
4. require the RFC 6962 root over the batches' commitments, choosing among
   fork candidates by `prev` and trying each at the last `seq`, to equal the
   token's root.

It prints each interval's canonical token, each batch, and one verdict line:

```text
interval 1  token 7c2f...  height 1204  seqs 1 to 5
interval 2  token 91ab...  height 1211  seqs 6 to 8
seq 6  carrier 3e0d...  timestamp 1700000000000  4 line(s)
seq 7  carrier c51a...  timestamp 1700000001000  4 line(s)
seq 8  carrier 0b77...  timestamp 1700000002000  4 line(s)
VERIFIED interval 2 of app_qzxkvbmwtr by 02c0ffee...: seqs 6 to 8, root 5d1e..., committed no later than block 1211
```

`REFUSED interval n: <reason>` (exit 1) names the rule broken, for example
`anchor-join` or `root-mismatch`. `INCOMPLETE interval n: <reason>` (exit 3)
means evidence was not found: no host answered an interval or a `seq`, or
an input's funding tree was unobtainable. With `-v`, answers refused along
the way are listed on stderr.

## Backfill

```bash
blogs backfill app_qzxkvbmwtr 02c0ffee... \
  -from https://archive-a.example.com -to https://archive-b.example.com -interval 3
```

Each position is asked of `-to` first and fetched from `-from` only when
`-to` holds nothing; the answer is checked (the carrier rules, and its proof
when `header_url` is set) and submitted to `-to`'s submit route.
`-interval K` fills the anchor chain 1 to K, in order, and every `seq`
interval K covers; `-seqs A-B` a range of seqs; `-anchors K` the anchor
chain alone. A host admits an anchor only once it holds the fee outputs the
anchor spends, and no lookup class serves a funding tree, so when `-to` does
not admit a token, `backfill` reads the funding trees its inputs spend from
`asset`, with their proofs, submits them, and offers the token again. One
line per position (`held`, `filled from the sibling`, `MISSING`, `REFUSED`),
then a count.

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
  exactly the outputs they spent with bcommon `carrier.Sweep`: output 0 is
  a funding-shaped tombstone carrying the swept value, the fee comes from
  the coin pool and its change goes back to it. `-retract-tree TXID` sweeps
  every output of one batch tree, used and unused. Once the sweep mines it
  is published, with its proof, to the stream's topic; each carrier that
  spent a swept output is then a double spend, and a host stops serving it
  and keeps its seq as a numbered position: the anchor still commits it, a
  reader finds it missing, and an archive proves the interval with a hole
  where the next batch's `prev` names the commitment. A retracted batch
  that is the last of its interval leaves nothing to name it, so an
  archive that did not hold it cannot prove that interval (specification
  section 8.4). An anchor fee tree funds no carrier and cannot be
  retracted.
- **Retirement.** `-retire` spends the unused outputs of every tree no
  longer in use (batch trees, anchor fee trees, and trees minted ahead and
  never adopted) to the fund address, the fee coming out of their value,
  and pools the result. It retracts nothing. A tree whose unused outputs
  cannot pay the fee is left and named (`left: ...`), which is the usual
  case for a batch tree of one-satoshi outputs. A retirement of a tree the
  hosts hold is published once it mines; one of a tree minted ahead, which
  was never published, is not.

Without `-yes` a sweep prints its plan (`retract seqs 7: tree T outputs
3`, `retire anchor tree T: 5 unused output(s) of 1000 sat`) and changes
nothing. With it, each sweep is written to the stream's state before it is
settled and the swept outputs are marked used, so no later carrier or
anchor spends them; the run then settles it, waits up to `-wait` (default
`10m`) for it to mine, pools its change and publishes it. A sweep in flight
when a run stops is finished by the next `sweep` run, with or without
flags, before anything new is built. `sweep` holds the home's lock, like
`produce`.

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
canonical anchor token (held to the anchor rules, its fee tree read from
`asset`), fetches any `seq` the interval covers that is not yet stored, and
writes the interval's proof. `-follow` repeats the pass every `-poll`.
`-content-dir` and `-allow-host` find digest-only content as for `read`.

The layout, under `DIR/<stream>/<producer>/`:

| Path | What |
| --- | --- |
| `archive.json` | the cursor: the next `seq` to walk to, the last interval proven, the hosts followed |
| `batches/<seq>-<carrier>.beef` | each verified carrier's BEEF as the host answered it; it carries the funding tree the carrier spends and the tree's proof |
| `batches/<seq>-<carrier>.json` | what was read from it: `seq`, `prev`, `timestamp`, `count`, `encoding`, `profile`, `size`, the funding outpoint, the host, and `content` (`ok`, `pending`, `unsupported`, `invalid`) |
| `content/<seq>-<carrier>.log` | the decoded lines, as the producer wrote them (not filtered for a terminal), when the content was read |
| `anchors/<interval>-<token>.beef` | each canonical anchor token's BEEF, with its proof |
| `intervals/<interval>.json` | the interval's proof: `token`, `vout`, `height`, `position` in its block, `prevToken`, `first`, `count`, `root`, `status`, `holes`, and per `seq` its `commitment`, whether its bytes are `held`, and its RFC 6962 inclusion `path` (leaf to root, each step the sibling's hash in hex, `L:` before a left sibling) |

`<seq>` and `<interval>` are 16 decimal digits, zero padded; `<carrier>`
and `<token>` are txids. Files are written atomically and never rewritten,
so an interrupted pass is continued by the next and two archives of the
same stream hold identical `intervals/` files. An interval's `status` is
`proven` when every batch is held, and `proven-with-holes` when some `seq`
is held by no file and its commitment was taken from the next batch's
`prev` (a retracted batch, or one the host never received); those seqs are
listed in `holes`. The root over the commitments must equal the token's,
or the interval is not written. A batch the archive stored before it was
retracted stays stored.

It prints one line per interval proven, a summary, and last
`latest interval K root R token T`, so two archives are compared by that
line. Exit 0 when the pass is done, 1 when the host answered something
refused or the chain or a root does not hold, 3 when an interval the host
answers cannot be proven from what it holds (for example its last `seq`
retracted and never stored).

## Exit codes

| Code | Meaning |
| --- | --- |
| 0 | done; for a reader, every answer verified |
| 1 | refused: an answer contradicts what was asked (a bad signature or proof, another `seq` or producer, a fork nothing resolves, a broken link, a root mismatch, invalid content) |
| 2 | usage, configuration, local or transport error: nothing was decided |
| 3 | incomplete: a host held nothing where evidence was needed, which proves nothing either way |

A help request (`-h`, `help`) exits 0.

## Messages

Results go to stdout; progress and errors to stderr, errors as one line
`blogs: <message>`. These texts are stable:

| Text | When |
| --- | --- |
| `blogs: unknown command "X"` | exit 2 |
| `blogs: <key> must be configured (config key <key>)`, `blogs: no header source: set header_url ...`, `blogs: no overlay host configured ...` | a missing setting, exit 2 |
| `blogs: this home holds no stream S (create one with ...)` | `produce` or `anchor` on a stream without state, exit 2 |
| `blogs: another blogs command is using the home H ...` | the home is locked, exit 2 |
| `blogs: batch funding tree: fee input: ... (run `blogs fund`)` | the pool has no spendable coin, exit 2 |
| `blogs: seq N: publish: ...; the carrier is persisted and a later run publishes it` | the facade did not take a carrier, exit 2 |
| `seq N: publishing the carrier a previous run persisted` | a run resuming |
| `anchor interval K: seqs A to B, token TXID` | an anchor built and settled |
| `anchor interval K: mined at height H and published; seqs A to B anchored` | an anchor published |
| `seq N: the host admitted nothing: ...` | a host answered a new carrier with nothing admitted |
| `note: N line(s) held something a terminal must be protected from (termsafe); published` (or `dropped`) | `-unsafe` |
| `seq N: <CODE> <reason>` | `read`: a refused answer (`REFUSED-SEQ`, `REFUSED-KEY`, `REFUSED-SIG`, `REFUSED-BUMP`, `REFUSED-DECODE`, ...) |
| `seq N: FORK: ...`, `seq N: BROKEN LINK: ...`, `seq N: MISSING: ...` | `read` |
| `VERIFIED interval ...`, `REFUSED interval N: ...`, `INCOMPLETE interval N: ...` | `verify-interval`, on stdout |
| `sweep TXID: mined at height H and published; seqs ... retracted` | a retraction done |
| `retire <kind> tree T: left: ...` | a tree whose unused outputs cannot pay the fee |
| `latest interval K root R token T` | `archive`, on stdout |

## Testing

```bash
make verify                                        # includes the command's tests
make e2e-client REFERENCE_HOST=/path/to/host       # the command against local reference hosts
```

`make verify` runs the command's tests in process against a local chain and
a stand-in host (`internal/testchain`): every command path, restart and
crash resumption, both profiles and gzip, a followed file with rotation,
hostile answers, terminal filtering, forks and equivocation, backfill and
the audit's refusals, retraction and retirement, and archives with and
without holes. `make e2e-client` runs the command against two reference
overlay hosts loading the bundle, over the same local chain serving the
node's API and the header source: two produce runs and a digest-only one,
read, verify-interval, a backfill of the second host from the first, a host
restart, a publish that fails and is resumed, archives of both hosts
agreeing on every interval, and a retraction the host module applies. It needs
Docker for the hosts' MySQL and a reference host directory holding its
compiled `dist/index.js` and its `node_modules`.

`scripts/lab-demo.sh` runs the command against real hosts: three archive
hosts, two that receive the stream from the plane and one that does not.
Its endpoints come from a local file (`scripts/lab-demo.env.example` lists
the keys); `setup` creates the home, funds it and creates the stream once,
and prints the lines each host's configuration needs; `run` publishes new
lines and anchors them, reads them back from the two plane hosts, audits
the interval at both, backfills the third host and audits it there,
archives all three, and requires one root from all six. Each run adds an
interval, so it is run twice in a row.
