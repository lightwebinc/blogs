# The host module

`host/bundle/blogs-module.js` is the blogs module for a reference overlay
host: a `tm_log_<name>_<suffix>` topic manager for each configured stream
and one `ls_log` lookup service for all of them. The host carries no blogs
code; it imports this one file by absolute path and calls its default
export. The rules it applies are the specification's ([spec.md](spec.md)
sections 7 and 8).

## Building it

```sh
(cd host && npm ci)   # once
make module           # tsc into host/dist, then the bundle into host/bundle
```

The bundle is one ESM file built by esbuild from `host/src/module.ts`, with
`@lightwebinc/bcommon` inlined. It imports only `@bsv/sdk`, by bare
specifier, which every reference host already has in its own
`node_modules`, so the SDK that parses every BEEF is the host's copy at the
version the host pins. The bundle step refuses, and deletes the file, when
any input lies outside `host/src/` and bcommon's own package, when nothing
of bcommon was inlined, or when the file imports anything but `@bsv/sdk`
and Node built-ins. It carries no source map.

The first line names what it holds:

```text
// @lightwebinc/blogs-host <version>: tm_log_<stream> and ls_log, with @lightwebinc/bcommon <version> inlined; imports @bsv/sdk (built against <version>)
```

and the host logs the same text once, as the `build` field of the `blogs
module mounted` line.

## Loading it

Copy the file into the host's tree, below the directory that holds the
host's `node_modules`: Node resolves a bare import from the nearest
`node_modules` above the module file, so a copy elsewhere either fails to
import `@bsv/sdk` or loads a second copy. Nothing else from `host/` is
needed. For a host whose tree is `/srv/overlay-host`:

```sh
install -D -m 0644 host/bundle/blogs-module.js /srv/overlay-host/modules/blogs/blogs-module.js

OVERLAY_TOPICS=tm_anytx,tm_log_example_app_qzxkvbmwtr
OVERLAY_MODULES=/srv/overlay-host/modules/blogs/blogs-module.js
BLOGS_STREAMS=example_app_qzxkvbmwtr=open
```

`OVERLAY_MODULES` is a comma list of absolute paths, so the blogs module
loads beside other modules. `BLOGS_STREAMS` is a comma list, one entry per
stream: `<stream>=open`, or `<stream>=<key>+<key>...` with each producer key
as 66 lowercase hex characters. Openness is stated, never inferred; an empty
or malformed entry stops the host before its port opens. Every stream's
topic must be in `OVERLAY_TOPICS` (the host refuses otherwise), and every
`tm_log_` topic in `OVERLAY_TOPICS` must be configured here (the module
refuses otherwise), because a blogs topic left to the host's default manager
would admit anything. A host that receives its objects from the plane also
needs the stream's topic in its plane subscription; the module decides
admission, not delivery.

| Variable | Default | |
| --- | --- | --- |
| `BLOGS_STREAMS` | none (required) | the streams and their producer policy, as above |
| `BLOGS_KEEP_DAYS` | unset: keep everything | how long a proven batch carrier is kept, in days (a decimal is allowed); at least 1. See [Retention](#retention) |
| `BLOGS_STATE_DIR` | none | an absolute directory for the retention state; required with `BLOGS_KEEP_DAYS`, refused without it |

The tsc tree `host/dist/module.js` loads the same way from inside this
repository, where `host/node_modules` resolves its imports; the tests use
it, and deployments use the bundle.

## The image

`host/Dockerfile` builds `ghcr.io/lightwebinc/blogs-host`: a reference
overlay host image with the bundle at `/app/modules/blogs/blogs-module.js`
and `OVERLAY_MODULES` naming it. Its entrypoint requires `BLOGS_STREAMS`
(exit 64 without it), derives `OVERLAY_TOPICS` from it when unset
(`tm_log_<stream>` per entry), and uses a random `OVERLAY_ADMIN_TOKEN` for
the run when none is set. [self-host.md](self-host.md) runs it with MySQL
and a TLS front; `make docker-smoke-host` checks that the bundle imports
the host's own `@bsv/sdk`.

## What it exports

| Export | What it is |
| --- | --- |
| `default create(host)` | the factory the host calls: reads `BLOGS_STREAMS` and `OVERLAY_TOPICS`, returns `{ topics, lookups }` |
| `blogsModule(host, streams, overlayTopics?)` | the same, from a parsed configuration |
| `parseStreams(raw)` | parses `BLOGS_STREAMS` |
| `LogTopicManager` | one stream's topic manager |
| `LogLookupService` | `ls_log`, with `restore`, `retain`, `report(stream?)`, `batchStatusOf(producer, stream, txid)` and `treeCount` |
| `Unrecorded` | the error a topic manager raises for a refusal the engine must not record |
| `parseQuery(query)` | the question rules of spec section 7.2 |
| `parseRetention(env)` | reads `BLOGS_KEEP_DAYS` and `BLOGS_STATE_DIR` |

## Admission

Each topic manager runs the rules of spec section 8.1 on the submitted
transaction. An anchor's previous coins are read from the engine's storage,
which the host hands to `ls_log` when it calls `restore` before its port
opens (until then, from the outputs `ls_log` has indexed). A proof is taken
as verified when present: the engine has checked it against the host's
headers before the topic manager runs.

A refusal is either recorded or raised (spec section 8.1, refusals that
leave no record). It is raised when the transaction spends an output the
topic holds, or is an anchor refused `anchor-input` or `unmined`; the
engine then stores nothing and a later offer of the same transaction is
decided afresh. The reference host logs a raised refusal as `topic
admission FAILED` and counts it in `overlay_host_topic_failures_total`.
For an anchor that arrived before its predecessor this is expected and
clears when the anchor is offered again; one that keeps recurring is an
archive missing a predecessor or a funding tree, which it fetches from a
sibling by the interval class and the tree class.

## Questions and statuses

`ls_log` answers `{stream, producer, seq}` with the unretracted batch
carriers at that seq, and `{stream, producer, interval}` with the anchor
tokens of that interval, canonical first; at most 8 of either. It answers
`{stream, tree}` with the mined funding tree of that txid, as one output
(the lowest funding output the topic holds), whose BEEF is the tree with
its proof. Any other query is an error.

The tree class is what keeps an audit independent of a node's retention.
The topic manager retains every output a transaction spends, so a funding
output stays in storage, marked spent, with its transaction and proof, and
a tree is answered after its last output is spent, when a pruning node no
longer has it. Nothing new is stored for it: a host that admitted a tree
before it ran a module with this class serves that tree once the module is
replaced and the host restarted. Storage that has lost a tree's rows or
its transaction answers empty, and an auditor reports the audit incomplete,
naming the tree. Do not prune `tm_log_` topics' spent outputs.

Statuses are never in an answer. `report()` gives, per producer and stream,
the holes (`gap`), the seqs with a `fork`, the retracted seqs, each interval
of the canonical chain as `complete`, `incomplete` (with its missing seqs) or
`root-mismatch`, and every other token as `anchor-branch`, `anchor-join` or
`pending`. Every status is computed from the set held, so arrival order and
restart order never change it.

## Restore

The index is in memory. On start the host hands `restore` the unspent
outputs of the module's topics with their BEEF (a host that hands them
without it, as reference hosts before 0.2.2 do, costs one more storage read
per row: the service reads each such row again with its BEEF); the service walks from each
to the outputs its transaction spent, the outputs that spent it, and its
transaction's other outputs, asking storage for each, so spent anchor tokens
and spent funding outputs are found too. An output connected to no unspent
output is not restored (a funding tree whose every carrier retention
dropped is answered from storage instead). A storage failure stops the
start. Memory and start time scale with what the host keeps.

## Retention

Without `BLOGS_KEEP_DAYS` a host keeps every carrier it admitted, and its
memory and start time grow with the stream ([limits.md](limits.md)). With
it, a proven carrier is dropped once it is older than the policy. How long
batches are kept is host policy ([frozen.md](frozen.md) section 8); what
the specification requires a host to keep is kept regardless:

| What | Kept |
| --- | --- |
| an anchor token | for the host's life |
| a funding output, spent or not, with its transaction and proof, and a sweep | for the host's life (spec section 8.1; the tree class answers them) |
| a carrier no complete interval covers yet | until one does: unproven carriers are never dropped (spec section 8.1) |
| a carrier of a complete interval | until the interval's anchor timestamp is `BLOGS_KEEP_DAYS` old and every earlier interval of the stream is dropped |

An interval is dropped whole, oldest first, and only when it is canonical
and complete (every seq held as a batch or a retraction receipt, and the
held candidates reproduce its root): its candidates at every seq, fork
candidates and retracted carriers included, leave the index and the
engine's storage. The age is the anchor record's timestamp, the
producer's clock: a producer that dates its anchors early has its own
carriers dropped early, never another producer's. The floor of one day
leaves an archive that follows the host time to catch up.

Once an interval is dropped the host answers `{stream, producer, seq}` for
its seqs with nothing, as for any batch it does not hold; readers and
auditors ask an archive or another host. The interval class still answers
its token and the tree class every funding tree. `report()` shows a dropped
interval as `complete` with `retired: true`, its seqs are not holes, and
the next batch is `linked` when its `prev` is the dropped interval's last
commitment.

A pass runs once the index is restored and hourly after, never while a
restore is in flight. It writes the state file first, then deletes the
dropped carriers' outputs from storage. The engine keeps its record that a
dropped carrier's transaction was applied, so the same carrier offered
again (by a sibling's catch-up, or a backfill) is a duplicate and does not
return; a carrier at a dropped position that does reach the module is not
indexed and is deleted at the next pass. On a store without `deleteOutput`
the module says so once in the log, and what it dropped stays in storage,
unanswered and skipped by restore. A dropped position stays dropped:
turning retention off later does not bring its carriers back.

`BLOGS_STATE_DIR/blogs-retention.json` holds, per producer and stream, the
last dropped interval, its last seq and that seq's commitment, and, per
funding tree some of whose carriers were dropped, the lowest funding
output whose carrier was. It is written whole, flushed, and renamed over
the last; a write that fails deletes nothing until one succeeds. A file
that does not parse stops the host, because a dropped position read as
held again would be counted a hole. It names keys, streams, txids and
positions, never a line.

## The Go module

The same module exists in Go, `module/` in this repository, written to
bcommon's engine-neutral contract (`github.com/lightwebinc/bcommon/module`)
for a Go overlay host that carries the adapter: `tm_log_<stream>` for each
configured stream and `ls_log` for all of them, with the same three
question classes and the same answers, the same statuses, restore and
retention (the state file `blogs-retention.json` is the same file, so a
host can move between the two). It reads the same `BLOGS_STREAMS`,
`BLOGS_KEEP_DAYS` and `BLOGS_STATE_DIR` and refuses the same mistakes, and
counts the same metrics below. Its admission rules are `logrec.Admit`, its
questions `logrec.ParseQuery`, and its tests drive it with the transaction
and refusal vectors and the TypeScript module's own cases. blogs has no
priced class, so the Go module has no terms route and no port of its own:
`ls_log` answers through the host's `/lookup`.

On the Go host the topic manager reads the submission's bytes exactly as
they arrived (`module.WithSubmission`), as the TypeScript manager reads the
submitted BEEF, and decides recorded and raised refusals by the same rule.
`ls_log` answers with outputs the host hydrates from its storage. Retention
deletes a dropped carrier's output through `DeleteOutput` on the storage a
restore is handed; the host keeps its record that the carrier's
transaction was applied, as above. The host logs `blogs module mounted`
with the topics, `ls_log`, the build and the retention policy.

## Metrics

Every counter is preset at start.

| Series | Kind | Meaning |
| --- | --- | --- |
| `blogs_admitted_total{kind}` | counter | `batch`, `anchor`, `funding`, `spend`, or `none` (claims nothing, spends nothing held) |
| `blogs_refused_total{reason,recorded}` | counter | every refusal by its spec reason; `recorded="no"` for a raised one |
| `blogs_retractions_total` | counter | batches retracted by a mined spend of their funding output |
| `blogs_candidates_dropped_total`, `blogs_tokens_dropped_total` | counter | fork candidates and tokens over the cap of 8 |
| `blogs_streams`, `blogs_batches`, `blogs_anchor_tokens`, `blogs_retracted_batches` | gauge | what the index holds |
| `blogs_funding_trees` | gauge | mined transactions (funding trees, and sweeps by their tombstones) the tree class answers |
| `blogs_missing_positions` | gauge | seqs below each stream's highest that nothing holds |
| `blogs_fork_positions` | gauge | seqs holding more than one candidate |
| `blogs_retired_total` | counter | carriers retention dropped |
| `blogs_retired_positions` | gauge | seqs whose carriers retention dropped, over every stream |

## Limits

- GASP catch-up between hosts carries unspent outputs; an anchor token
  reaches a host that way only if the host already holds what it spends.
  The interval class is the catch-up path for anchors.
- No lookup class names the sweep of a batch. A host receives one from the plane, from
  a producer that names it in `hosts`, or from `blogs backfill -sweeps`;
  until then it keeps serving the batches the sweep retracted.
- The index is in memory and restore reads every unspent output of the
  module's topics, so a host's memory and start time grow with what it
  keeps ([limits.md](limits.md)); retention bounds what it keeps.
- An engine's unproven eviction, if an operator runs it, removes carriers,
  and the index follows it. The specification forbids evicting carriers.

## Tests

`make test` runs the module's tests: every transaction and refusal vector
through the topic manager, the lookup rules and statuses, the module loaded
by absolute path as the host loads it, the module inside the overlay engine
over an in-memory store, retention over that store and across a restart, the bundle mounted and admitting the golden
transactions, and the bundle step's refusals. `make e2e
REFERENCE_HOST=/path` stages the bundle beside the reference host's
`node_modules`, runs one publish and lookup on that host with a MySQL
container, restarts it, and asks again from the restored index; then
restarts it twice with `BLOGS_KEEP_DAYS` set and checks that the proven
carriers left the index and MySQL while the anchors and the funding tree
are still answered.
