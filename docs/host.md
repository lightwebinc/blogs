# The host module

`host/dist/module.js` is the blogs module for a reference overlay host: a
`tm_log_<name>_<suffix>` topic manager for each configured stream and one
`ls_log` lookup service for all of them. The host carries no blogs code; it
imports this file by absolute path and calls its default export. The rules
it applies are the specification's ([spec.md](spec.md) sections 7 and 8).

## Loading it

```sh
(cd host && npm ci && npm run build)

OVERLAY_TOPICS=tm_anytx,tm_log_example_app_qzxkvbmwtr
OVERLAY_MODULES=/abs/path/to/blogs/host/dist/module.js
BLOGS_STREAMS=example_app_qzxkvbmwtr=open
```

`BLOGS_STREAMS` is a comma list, one entry per stream: `<stream>=open`, or
`<stream>=<key>+<key>...` with each producer key as 66 lowercase hex
characters. Openness is stated, never inferred; an empty or malformed entry
stops the host before its port opens. Every stream's topic must be in
`OVERLAY_TOPICS` (the host refuses otherwise), and every `tm_log_` topic in
`OVERLAY_TOPICS` must be configured here (the module refuses otherwise),
because a blogs topic left to the host's default manager would admit
anything.

The module imports `@bsv/sdk` and `@lightwebinc/bcommon` by bare specifier,
and they resolve from `host/node_modules` beside `dist/`, so the host loads
it from this directory after `npm ci`. It parses every BEEF it is handed
with that SDK, at the version the host pins.

## What it exports

| Export | What it is |
| --- | --- |
| `default create(host)` | the factory the host calls: reads `BLOGS_STREAMS` and `OVERLAY_TOPICS`, returns `{ topics, lookups }` |
| `blogsModule(host, streams, overlayTopics?)` | the same, from a parsed configuration |
| `parseStreams(raw)` | parses `BLOGS_STREAMS` |
| `LogTopicManager` | one stream's topic manager |
| `LogLookupService` | `ls_log`, with `restore`, `report(stream?)` and `batchStatusOf(producer, stream, txid)` |
| `Unrecorded` | the error a topic manager raises for a refusal the engine must not record |
| `parseQuery(query)` | the question rules of spec section 7.2 |

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
sibling by the interval class.

## Questions and statuses

`ls_log` answers `{stream, producer, seq}` with the unretracted batch
carriers at that seq, and `{stream, producer, interval}` with the anchor
tokens of that interval, canonical first; at most 8 of either. Any other
query is an error.

Statuses are never in an answer. `report()` gives, per producer and stream,
the holes (`gap`), the seqs with a `fork`, the retracted seqs, each interval
of the canonical chain as `complete`, `incomplete` (with its missing seqs) or
`root-mismatch`, and every other token as `anchor-branch`, `anchor-join` or
`pending`. Every status is computed from the set held, so arrival order and
restart order never change it.

## Restore

The index is in memory. On start the host hands `restore` the unspent
outputs of the module's topics with their BEEF; the service walks from each
to the outputs its transaction spent, the outputs that spent it, and its
transaction's other outputs, asking storage for each, so spent anchor tokens
and spent funding outputs are found too. An output connected to no unspent
output is not restored. A storage failure stops the start. Memory and start
time scale with the topic.

## Metrics

Every counter is preset at start.

| Series | Kind | Meaning |
| --- | --- | --- |
| `blogs_admitted_total{kind}` | counter | `batch`, `anchor`, `funding`, `spend`, or `none` (claims nothing, spends nothing held) |
| `blogs_refused_total{reason,recorded}` | counter | every refusal by its spec reason; `recorded="no"` for a raised one |
| `blogs_retractions_total` | counter | batches retracted by a mined spend of their funding output |
| `blogs_candidates_dropped_total`, `blogs_tokens_dropped_total` | counter | fork candidates and tokens over the cap of 8 |
| `blogs_streams`, `blogs_batches`, `blogs_anchor_tokens`, `blogs_retracted_batches` | gauge | what the index holds |
| `blogs_missing_positions` | gauge | seqs below each stream's highest that nothing holds |
| `blogs_fork_positions` | gauge | seqs holding more than one candidate |

## Limits

- GASP catch-up between hosts carries unspent outputs; an anchor token
  reaches a host that way only if the host already holds what it spends.
  The interval class is the catch-up path for anchors.
- An engine's unproven eviction, if an operator runs it, removes carriers,
  and the index follows it. The specification forbids evicting carriers.

## Tests

`make test` runs the module's tests: every transaction and refusal vector
through the topic manager, the lookup rules and statuses, the module loaded
by absolute path as the host loads it, and the module inside the overlay
engine over an in-memory store. `make e2e REFERENCE_HOST=/path` runs one
publish and lookup on a local reference host with a MySQL container,
restarts it, and asks again from the restored index.
