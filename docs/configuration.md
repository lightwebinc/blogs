# Configuration

Every setting the `blogs` command reads, every flag it takes, and the
variables of the three container images. The commands themselves are in
[usage.md](usage.md); the defaults and caps, with the reason for each, are in
[limits.md](limits.md).

## Where settings come from

Highest first:

1. the global flags, before the command (`blogs -network test read ...`);
2. `BLOGS_<KEY>` in the environment, the key in upper case
   (`BLOGS_HEADER_URL=woc:main`);
3. the config file;
4. the built-in defaults.

The config file is `-config PATH`, else `$BLOGS_HOME/config`, else
`$XDG_CONFIG_HOME/blogs/config`, else `~/.blogs/config`. Its grammar is one
`key = value` per line, `#` comments, blank lines ignored. A key it does not
define, a key given twice, or a line without `=` is an error, never ignored:
a misspelled key that was silently skipped would leave a setting at its
default with no sign the file was read.

The addresses (hosts, facade, header source, broadcaster, node) have no
default on purpose: a default would send a producer's objects, or a reader's
questions, to a server nobody configured. A command that needs one and finds
none exits 2 naming the key.

## Keys

| Key | Variable | Default | Meaning |
| --- | --- | --- | --- |
| `arcade_key` | `BLOGS_ARCADE_KEY` | none | bearer token for the ARC service named in `settle = arcade:...` |
| `asset` | `BLOGS_ASSET` | none | a node's asset API base URL: proofs, the chain tip, blocks, raw transactions. A producer needs one; a reader uses it only for a funding tree no host serves |
| `facade` | `BLOGS_FACADE` | none | mode `plane`: the overlay host or bridge a producer publishes to (its `/submit` route), from which the plane delivers to every subscribed host. Refused in mode `unicast` |
| `header_url` | `BLOGS_HEADER_URL` | none | the header source every proof is checked against: `woc:main`, `woc:test` (public WhatsOnChain, proof of work checked), `chaintracks:URL`, or an overlay bridge's base URL (native `/v1/root/<height>` and `/v1/tip`) |
| `home` | `BLOGS_HOME` | `~/.blogs` | the producer home. `BLOGS_HOME` also moves the config file to `$BLOGS_HOME/config` |
| `hosts` | `BLOGS_HOSTS` | none | the overlay hosts, comma separated, at most 16: the hosts a reader asks, in order; in mode `unicast` every host a producer submits to; in mode `plane` the hosts other than `facade` every sweep is also sent to directly |
| `mode` | `BLOGS_MODE` | `plane` | `plane` or `unicast`: how a producer's objects reach the hosts ([unicast.md](unicast.md)) |
| `network` | `BLOGS_NETWORK` | `main` | `main`, `test` or `regtest`: the fund address prefix and the header source's proof-of-work floor. A `woc:` header source or `tx_source` must name the same network |
| `object_bound` | `BLOGS_OBJECT_BOUND` | `1048576` | the plane's object bound in bytes, at most 8388608; a carrier whose BEEF is larger is not published |
| `originator` | `BLOGS_ORIGINATOR` | `blogs` | the BRC-100 originator presented to the embedded wallet |
| `quorum` | `BLOGS_QUORUM` | `all` | mode `unicast`: how many hosts must take an object for it to count as published: `all`, `majority`, `one`, or a number up to the host count. A sweep always needs every host |
| `rpc` | `BLOGS_RPC` | none | a node's JSON-RPC URL, used only by `fund` without `-txid` (coinbase: only on a regtest chain you run, for development and tests) |
| `rpc_pass`, `rpc_user` | `BLOGS_RPC_PASS`, `BLOGS_RPC_USER` | `bitcoin` | the node's basic auth, for `rpc` and for `settle = rpc:` |
| `settle` | `BLOGS_SETTLE` | none | the broadcaster that settles funding trees, anchors and sweeps: `arcade:<url>` (an ARC-compatible service), `rpc:<url>` (a node's `sendrawtransaction`) or `tcp:<host:port>` (bare EF to an ingress) |
| `timeout` | `BLOGS_TIMEOUT` | `15s` | bounds each request to a host, node or header source; `1s` to `5m` |
| `tx_source` | `BLOGS_TX_SOURCE` | none | `woc:main` or `woc:test`: where a reader with no node reads a funding tree no host serves and the spender of a funding output, and where `fund -txid` reads a payment. Its answers are held to the txid and, with a proof, to the header source |

## Examples

A producer on mainnet, submitting to two hosts itself:

```text
# ~/.blogs/config
network    = main
header_url = woc:main
tx_source  = woc:main
settle     = arcade:https://arc.gorillapool.io/v1
asset      = http://node.example.com:8090
mode       = unicast
hosts      = https://logs-a.example.com,https://logs-b.example.com
quorum     = all
```

The same producer on testnet:

```text
network    = test
header_url = woc:test
tx_source  = woc:test
settle     = arcade:https://testnet.arc.gorillapool.io/v1
asset      = http://testnet-node.example.com:8090
mode       = unicast
hosts      = https://logs-a.example.com,https://logs-b.example.com
```

On the multicast plane, submitting once to a facade the plane delivers from:

```text
network    = main
header_url = woc:main
settle     = arcade:https://arc.gorillapool.io/v1
asset      = http://node.example.com:8090
facade     = https://logs-a.example.com
hosts      = https://logs-a.example.com,https://logs-b.example.com
```

A reader or auditor, with no node and no home:

```text
network    = main
hosts      = https://logs-a.example.com,https://logs-b.example.com
header_url = woc:main
tx_source  = woc:main
```

The same reader as one command, with nothing on disk:

```console
$ BLOGS_HEADER_URL=woc:main BLOGS_TX_SOURCE=woc:main BLOGS_HOSTS=https://logs-a.example.com \
    blogs verify-interval app_qzxkvbmwtr 02c0ffee... 1
```

## Global flags

Before the command. Each sets the key of the same name for this run.

| Flag | Key |
| --- | --- |
| `-config PATH` | the config file (no key) |
| `-home DIR` | `home` |
| `-hosts URLS` | `hosts` |
| `-header-url SOURCE` | `header_url` |
| `-mode MODE` | `mode` |
| `-network NET` | `network` |
| `-quorum Q` | `quorum` |
| `-timeout DUR` | `timeout` |
| `-tx-source SOURCE` | `tx_source` |
| `-v` | verbose output on stderr (also accepted after the command) |
| `-version` | print the version and exit |

## Command flags

Flags may follow the positional arguments. `blogs <command> -h` prints them.

**`fund`**

| Flag | Default | |
| --- | --- | --- |
| `-txid TXID` | none | import this mined payment to the fund address: how a producer on mainnet or testnet is funded |
| `-blocks N` | `101` | coinbase: only on a regtest chain you run (development and tests). Blocks to mine to the fund address |
| `-batch N` | `30` | coinbase: only on a regtest chain you run (development and tests). Blocks per `generatetoaddress` call |
| `-rescan` | off | coinbase: only on a regtest chain you run (development and tests). Re-read the last `-blocks` blocks for coinbase the pool lacks instead of mining |

**`produce <stream>`**: input and batching

| Flag | Default | |
| --- | --- | --- |
| `-file PATH` | stdin | read this file, from where the last run stopped |
| `-follow` | off | with `-file`: keep reading as it grows, through truncation and rotation |
| `-profile` | `full` | `full` (content in the record) or `digest` (content in `-content-dir`, a locator in the record) |
| `-encoding` | `lines` | `lines` or `gzip` |
| `-content-dir DIR` | none | `digest`: where content files are written, named by SHA-256 hex |
| `-url PREFIX` | none | `digest`, repeatable up to 4: an `https://` prefix the content's SHA-256 hex is appended to, as a fetch hint |
| `-max-bytes N` | by mode and profile | bytes of lines per batch ([limits.md](limits.md)) |
| `-max-lines N` | `10000` | lines per batch, at most 65536 |
| `-max-wait DUR` | `1s` | a batch is cut this long after its first line; `10ms` to `1h` |
| `-max-rate R` | `2` | batches a second, at most 50 |
| `-interval DUR` | `10m` | anchor once this long has passed since the last anchor; `1m` to `24h` |
| `-anchor-wait DUR` | `10m` | at the end of the input, wait this long for the last anchor to mine; `0` does not anchor at the end |
| `-unsafe` | `warn` | lines a terminal must be protected from: `warn` (publish and count) or `drop` |

**`produce`, `anchor` and `sweep`**: funding trees and anchors

| Flag | Default | |
| --- | --- | --- |
| `-poll DUR` | `5s` | how often proofs are asked for |
| `-tree-count N` | `100` | outputs per batch funding tree, at most 1000 |
| `-tree-sats N` | `1` | satoshis per batch funding output |
| `-ahead N` | `25` | mint the next batch tree at this many outputs left |
| `-anchor-tree-count N` | `8` | outputs per anchor fee tree, at most 1000 |
| `-anchor-tree-sats N` | `1000` | satoshis per anchor fee output |
| `-anchor-ahead N` | `2` | mint the next anchor fee tree at this many outputs left |
| `-anchor-batches N` | `4096` | anchor early once this many batches are unanchored, at most 65536 |

**`anchor <stream>`**: `-wait DUR` (default `10m`), how long to wait for the
anchors to mine.

**`sweep <stream>`**

| Flag | Default | |
| --- | --- | --- |
| `-retract-seqs A-B` | none | retract the batches at these seqs (or one seq) |
| `-retract-tree TXID` | none | retract every batch one batch tree funded |
| `-retire` | off | spend the unused outputs of every tree no longer in use back to the pool |
| `-yes` | off | build and settle the sweeps; without it, print the plan and change nothing |
| `-wait DUR` | `10m` | how long to wait for each sweep to mine |

**`read <stream> <producer>`**

| Flag | Default | |
| --- | --- | --- |
| `-from N`, `-to N` | `1`, `0` | the seq range; `-to 0` reads until no host holds the next seq |
| `-meta` | off | a line about each batch before its lines |
| `-json` | off | one JSON object per batch |
| `-ansi` | off | let color (SGR) through to the terminal |
| `-ascii` | by locale | print non-ASCII as `?` (the default when the locale is not UTF-8) |
| `-content-dir DIR` | none | repeatable: digest-only content named by SHA-256 hex |
| `-allow-host HOST` | none | repeatable: a host whose locator hints may be fetched over HTTPS |

**`verify-interval <stream> <producer> <n>`**: `-archive DIR`, an archive
directory (`blogs archive -dir`) asked before the hosts.

**`backfill <stream> <producer>`**

| Flag | Default | |
| --- | --- | --- |
| `-from URL` | every configured host but the one filled | the sibling to copy from |
| `-to URL` | every configured host in turn | the host to fill |
| `-seqs A-B` | none | a seq range, or one seq |
| `-interval K` | none | fill the anchor chain to interval K and the seqs it covers |
| `-anchors K` | none | fill the anchor chain 1 to K |
| `-sweeps` | off | carry sweeps found on chain (needs `asset` or `tx_source`, and `header_url`) |

**`archive <stream> <producer>`**

| Flag | Default | |
| --- | --- | --- |
| `-dir DIR` | none | the archive's directory (required) |
| `-host URL` | the first configured host | the host to follow |
| `-follow` | off | a pass every `-poll` until interrupted |
| `-poll DUR` | `30s` | with `-follow`, the time between passes |
| `-recheck N` | `1024` | stored seqs each pass asks the host about again, 0 to 65536 |
| `-recheck-all` | off | ask about every stored seq in this pass |
| `-content-dir DIR`, `-allow-host HOST` | none | as for `read` |

`init`, `stream new`, `stream list`, `doctor` and `version` take no flags
beyond `-v`.

## Container images

| Image | What it runs |
| --- | --- |
| `ghcr.io/lightwebinc/blogs` | the `blogs` command as its entrypoint, as uid 65532 (`nonroot`) on a distroless base. It listens on nothing |
| `ghcr.io/lightwebinc/blogs-host` | a reference overlay host with the blogs module built in ([self-host.md](self-host.md)) |
| `ghcr.io/lightwebinc/blogs-devchain` | the local development chain on port 8080: a private regtest chain for the sandbox, never for a network |

Each carries `LICENSE`, `NOTICE` and `LICENSE-THIRD-PARTY` under
`/usr/share/doc/blogs/`.

**The command image.** No setting is baked in. Pass them as `BLOGS_<KEY>`
variables, or mount a config file. The producer home is
`/home/nonroot/.blogs`, which the image carries empty, owned by nonroot and
mode 0700, so a new named volume mounted there starts with the right owner:

```console
$ docker run --rm -v blogs-home:/home/nonroot/.blogs ghcr.io/lightwebinc/blogs init
$ docker run --rm -v blogs-home:/home/nonroot/.blogs \
    -e BLOGS_HEADER_URL=woc:main -e BLOGS_TX_SOURCE=woc:main \
    ghcr.io/lightwebinc/blogs fund -txid 6f1e...a9c2
```

A bind mount keeps the host directory's owner instead: make it writable by
uid 65532, or run with `--user`.

**The host image.**

| Variable | Default | |
| --- | --- | --- |
| `BLOGS_STREAMS` | required | the streams the host keeps: a comma list of `<stream>=<key>[+<key>...]` or `<stream>=open`, as `blogs stream new` prints it. Empty: the image exits 64 |
| `OVERLAY_TOPICS` | derived | `tm_log_<stream>` for each entry of `BLOGS_STREAMS`. Set it only to carry other topics beside them, and then list every blogs topic too |
| `OVERLAY_ADMIN_TOKEN` | random per start | bearer token for the host's `/admin` routes |
| `OVERLAY_MODULES` | `/app/modules/blogs/blogs-module.js` | set by the image |

The other `OVERLAY_*` variables (database, header source, network, sync
peers) are the reference overlay host's own. `deploy/compose.yaml` sets them
from the variables in [self-host.md](self-host.md#2-start-it): `HOST`,
`HEADERS`, `NETWORK`, `ADMIN_TOKEN`, `SYNC_PEERS`, `DB_PASSWORD`,
`DB_BUFFER_POOL` and `BLOGS_HOST_IMAGE`.

**The devchain image.** Its entrypoint is `devchain -journal
/var/lib/devchain/journal`; mount a volume at `/var/lib/devchain` so the
chain survives a restart.

| Flag | Default | |
| --- | --- | --- |
| `-listen ADDR` | `:8080` | address to serve on |
| `-height N` | `700` | the tip's height before anything is mined |
| `-journal FILE` | none | append every accepted RPC call to this file and replay it at start |
| `-version` | off | print the version and exit |
