# Quickstart

Two ways in:

- **[On mainnet or testnet](#on-mainnet-or-testnet)**: no node and no
  infrastructure of your own. Read and audit a stream with nothing but the
  command, then produce your own, paid from coin you already own.
- **[Local development sandbox](#local-development-sandbox-no-coin-needed-a-private-regtest-chain)**:
  no coin needed, a private regtest chain. Two hosts, a stream, and a
  verified interval in about five minutes, on one machine.

## On mainnet or testnet

Install the command (`go install github.com/lightwebinc/blogs/cmd/blogs@latest`,
a [release binary](https://github.com/lightwebinc/blogs/releases), or the
`ghcr.io/lightwebinc/blogs` image). Every key, txid, stream and hostname
below is illustrative.

No node is needed. On mainnet and testnet the command uses public services
by default, and checks what they answer:

| Role | Default | What is checked |
| --- | --- | --- |
| Header source (`header_url`) | `woc:main` or `woc:test`, the public WhatsOnChain API | every header is hashed and must carry the proof of work it claims, so a lie costs a mined block |
| Chain view (`chain`) | `woc:main` or `woc:test` | a transaction must hash to its txid; a proof must verify against the headers. "Unspent" and "not mined" are WhatsOnChain's word |
| Broadcaster (`settle`) | `arcade:main` or `arcade:test`, GorillaPool's public arcade | its verdict is held to the chain view's spends |
| Fee rate (`fee_rate`) | 100 satoshis per 1000 bytes, the network's rate, with a 100 satoshi floor per transaction | |

A node of your own (`chain = asset:URL`), a header service you run
(`header_url = bhs:URL` or `arcade:URL`), or another broadcaster
(`settle = arc:URL`, `arcade:URL`, `rpc:URL`) replaces any of them, and is
the stronger choice where you have one ([docs/configuration.md](docs/configuration.md)).

### Read and audit: no node, no coin, no home

A reader names the hosts that keep a stream. Every proof is checked against
the header source.

```console
$ blogs doctor
...
headers     woc:main tip 970082
$ blogs -hosts https://logs.example.com read app_qzxkvbmwtr 02c0ffee…
$ blogs -hosts https://logs.example.com verify-interval app_qzxkvbmwtr 02c0ffee… 1
VERIFIED interval 1 of app_qzxkvbmwtr by 02c0ffee…: seqs 1 to 4, root 5d7f…3999, committed no later than block 912430
```

On testnet the same commands take `-network test`. Put the settings in
`~/.blogs/config` to stop repeating them ([docs/configuration.md](docs/configuration.md)).

### Produce: install, init, pay, fund, produce

1. **Create the home.** It holds the identity key that is the producer, and
   prints the fund address for the network:

   ```console
   $ blogs -network main init
   created /home/me/.blogs
   identity     02c0ffee…
   fund address 1BLogs… (main)
   ```

2. **Pay the fund address from your own wallet.** About 50,000 satoshis
   (0.0005 BSV) starts a stream (section [What it costs](#what-it-costs)).

3. **Import the payment.** Hand over the payment as BEEF, the form a
   BRC-100 wallet hands a payment over in (a file, or `-` for standard
   input): blogs checks it against
   the header source with no lookup at all, and need not wait for a block.
   Or wait for one confirmation and import it by its txid, which reads the
   transaction and its proof from the chain view. Either way every output
   paying the fund address goes into the coin pool:

   ```console
   $ blogs fund -beef payment.beef
   imported 1 of 1 output(s) paying 1BLogs…, 50000 sat, not mined yet: held until its proof arrives, which the next produce, anchor or sweep collects; pool 1 output(s), 50000 sat
   $ blogs fund -txid 6f1e…a9c2
   imported 1 of 1 output(s) paying 1BLogs…, 50000 sat, mined at height 970061; pool 1 output(s), 50000 sat
   ```

   A payment that is not mined yet is accepted when every transaction it
   spends is proven; its coin is spent once it mines. `fund_mined_only =
   true` (or `-mined-only`) takes mined payments only. A transaction that
   pays someone else is refused:

   ```console
   $ blogs fund -txid 5285…5be6
   blogs: bwallet: the payment pays nothing to this wallet's fund address: 5285…5be6 (the fund address is 1BLogs…)
   ```

4. **Create a stream**, and give its `host` line to whoever runs your
   hosts (your own: [docs/self-host.md](docs/self-host.md)):

   ```console
   $ blogs stream new app
   stream   app_qzxkvbmwtr
   topic    tm_log_app_qzxkvbmwtr
   producer 02c0ffee…
   host     BLOGS_STREAMS=app_qzxkvbmwtr=02c0ffee…
   ```

5. **Name the hosts** in `~/.blogs/config`, then produce:

   ```text
   network    = main
   mode       = unicast
   hosts      = https://logs-a.example.com,https://logs-b.example.com
   ```

   ```console
   $ blogs doctor
   $ tail -F /var/log/app.log | blogs produce app_qzxkvbmwtr
   ```

| Setting | What it is on the real network |
| --- | --- |
| Hosts (`hosts`) | your own ([docs/self-host.md](docs/self-host.md)), or anyone's that keeps your stream. In `mode = unicast` the producer submits to each; on the multicast plane (`mode = plane`, with `facade`) it submits once. The only setting with no default |
| Header source (`header_url`) | `woc:main` by default; `bhs:URL` for a block-headers-service you run, `arcade:URL` for an arcade's header server, `chaintracks:URL` |
| Chain view (`chain`) | `woc:main` by default; `asset:URL` for a node's asset API, or both (`asset:URL,woc:main`). `woc_key` and `woc_rate` carry a WhatsOnChain API key and its plan's rate, above the free 3 requests a second |
| Broadcaster (`settle`) | `arcade:main` by default; `arcade:URL` for an arcade you run, `arc:URL` for an ARC service (`arcade_key` carries a bearer token where it wants one), `rpc:URL` for a node |
| Fees (`fee_rate`, `fee_source`) | 100/1000 by default; `fee_source = arc` follows the broadcaster's published rate, bounded by `fee_min_rate` and `fee_max_rate` |
| Coin | real BSV you own: pay the fund address, then `blogs fund -beef FILE` or `blogs fund -txid TXID` |

**Testnet.** Everything above works on testnet with `network = test`: the
defaults become `woc:test` and `arcade:test`. `init` then prints a testnet
address (`m…` or `n…`); pay it from a testnet wallet or faucet.

### What it costs

blogs pays the network's rate, 100 satoshis per 1000 bytes, with a floor
of 100 satoshis per transaction. Each batch costs about 6 satoshis (its
share of the funding tree it spends, and the 1 satoshi it carries); a
carrier is never mined and pays no fee of its own. Each anchor costs about
113: the floor, and its share of the anchor fee tree. A stream anchored
every ten minutes around the clock spends about 16,000 satoshis
(0.00016 BSV) a day on anchors; the batch cost follows how much you log.
About 50,000 satoshis starts a stream: the first funding trees, and a
reserve so the next tree is minted before the current one runs out.
`blogs doctor` shows the pool, and `blogs sweep -retire` returns unused
funding outputs to it. Measured sizes are in [docs/limits.md](docs/limits.md).

## Local development sandbox (no coin needed, a private regtest chain)

On one machine: run two overlay hosts, produce a log stream to both in
unicast mode, read it back, and verify an interval against block headers.
A local regtest chain stands in for the network, so nothing here is money
and nothing here works on mainnet or testnet.

You need [Docker](https://docs.docker.com/get-docker/) with Compose, and
this repository (or just its `deploy/quickstart/compose.yaml`).

```mermaid
flowchart LR
  B["blogs (producer, reader)"] -->|"submit, each host"| A["host-a"]
  B -->|"submit, each host"| H["host-b"]
  B -->|lookup| A
  A --> D[(mysql)]
  H --> D
  B -->|"settle, proofs, headers"| C["chain (local)"]
  A -->|headers| C
  H -->|headers| C
  %% The last node is an invisible spacer in the rightmost rank: the rendered
  %% viewer places its zoom and pan controls over the bottom-right corner.
  A ~~~ pad
  pad["&nbsp;<br/>&nbsp;<br/>&nbsp;<br/>&nbsp;<br/>&nbsp;"]:::pad
  classDef pad fill:none,stroke:none,color:transparent
```

### 1. Start the local chain

```console
$ cd deploy/quickstart
$ docker compose up -d chain
$ alias blogs='docker compose run --rm -T blogs'
```

`chain` is a private regtest chain standing in for the network: it serves a node's API, mines every
transaction it is sent at once, and is the header source both the hosts
and `blogs` check proofs against. Nothing it mines is money. The `blogs`
service is already configured for it and for the two hosts, in unicast mode
(the settings are in `compose.yaml`).

The images are `ghcr.io/lightwebinc/blogs`, `blogs-host` and
`blogs-devchain`. To build them from a checkout instead of pulling them:
`make docker-build` and `make docker-build-host` ([docs/self-host.md](docs/self-host.md#5-building-the-image)).

### 2. Create a producer and a stream

```console
$ blogs init
created /home/nonroot/.blogs
identity     03965f…37ee
fund address mu6Fa…xH2 (regtest)
$ blogs fund        # coinbase: only on a regtest chain you run (development and tests)
pool after:  101 output(s), 505000000000 sat (+101); immature 100; node tip 801
$ blogs stream new app | tee stream.txt
stream   app_yuqhfqxzfo
topic    tm_log_app_yuqhfqxzfo
producer 03965f…37ee
host     BLOGS_STREAMS=app_yuqhfqxzfo=03965f…37ee
$ sed -n 's/^host *//p' stream.txt > .env
$ STREAM=$(sed -n 's/^stream *//p' stream.txt) PRODUCER=$(sed -n 's/^producer *//p' stream.txt)
```

`init` made an identity key, which is the producer, in the `home` volume.
`fund` with no `-txid` mined 101 blocks of coinbase to the fund address, so one
block's coin is mature. That path is coinbase: only on a regtest chain you run
(development and tests); on mainnet and testnet you pay the fund address and
import the payment with `fund -beef` or `fund -txid`.
The stream's name gets a random suffix, so nobody else's stream shares its
topic, and the `host` line is what a host needs to keep it: `.env` hands it
to both.

### 3. Start two hosts

```console
$ docker compose up -d
$ blogs doctor
...
mode        unicast: every object to each of 2 host(s), published once 2 take it (quorum all)
host        http://host-a:8080 answering
host        http://host-b:8080 answering
```

The first start takes a few seconds while MySQL initializes; run `doctor`
again until both hosts answer.

### 4. Produce, read, verify

```console
$ seq 1 40 | sed 's/^/hello from blogs, line /' | blogs produce $STREAM -max-lines 10
...
stream app_yuqhfqxzfo: published seq 1 to 4
anchored to seq 4: interval 1, token 0cead…52dc, mined at height 804
host http://host-a:8080: took 7 object(s), missed 0
host http://host-b:8080: took 7 object(s), missed 0
$ blogs read $STREAM $PRODUCER | tail -2
hello from blogs, line 39
hello from blogs, line 40
$ blogs verify-interval $STREAM $PRODUCER 1
interval 1  token 0cead…52dc  height 804  seqs 1 to 4
...
VERIFIED interval 1 of app_yuqhfqxzfo by 03965f…37ee: seqs 1 to 4, root 5d7f…3999, committed no later than block 804
```

`produce` cut the 40 lines into four signed batches, sent each (and the
funding trees they spend) to both hosts, then anchored all four in one mined
token. `read` checked every batch it printed: the producer's signature, its
place in the stream, and its funding parent's proof against the headers.
`verify-interval` is the auditor's check: the anchor is mined, and the root
over the four batches is the one the anchor commits. Exit status is `0`
verified, `1` refused, `2` a usage or transport error, `3` incomplete.

### 5. See the unicast tradeoff

With no multicast plane, hosts do not pass objects to each other. Stop one,
and let the producer publish with only one host taking each object:

```console
$ docker compose stop host-b
$ seq 41 50 | sed 's/^/hello from blogs, line /' | blogs -quorum one produce $STREAM
warning: a quorum of 1 of 2 hosts is below a majority: an object counts as published while most hosts lack it, until a backfill (docs/limits.md)
seq 5: host http://host-b:8080 MISSED it after 4 tries: ...
...
host http://host-a:8080: took 2 object(s), missed 0
host http://host-b:8080: took 0 object(s), missed 2: seq 5, anchor interval 2
a host missed objects; once it is back, copy them from the others: blogs backfill app_yuqhfqxzfo 03965f…37ee -seqs 5-5 -anchors 2
$ docker compose start host-b
$ blogs -hosts http://host-b:8080 verify-interval $STREAM $PRODUCER 2
interval 1  token 0cead…52dc  height 804  seqs 1 to 4
INCOMPLETE interval 2: no host answered; an empty answer proves nothing
$ blogs backfill $STREAM $PRODUCER -seqs 5-5 -anchors 2
...
backfill app_yuqhfqxzfo to http://host-b:8080: 1 held, 2 filled, 0 missing, 0 refused, 0 unserved, 0 sweep(s) carried
note: backfill carried batches, anchors and their fee trees, not sweeps: ...
$ blogs -hosts http://host-b:8080 verify-interval $STREAM $PRODUCER 2
VERIFIED interval 2 of app_yuqhfqxzfo ...
```

With the default quorum, `all`, that `produce` would have stopped at seq 5
with the batch saved, and the next run would publish it. The cost of no
plane is that the producer sends everything to every host itself, and a
host that missed something needs a backfill. [docs/unicast.md](docs/unicast.md)
has the whole design.

### 6. Clean up

```console
$ docker compose down        # stop; the volumes keep the chain, the hosts and the home
$ docker compose down -v     # and forget everything
```

## Next

- [docs/usage.md](docs/usage.md): every command, flag, setting, exit code and message
- [docs/user-guide.md](docs/user-guide.md): the guide: concepts, producing, reading, auditing, archiving, sweeping, troubleshooting
- [docs/unicast.md](docs/unicast.md): unicast mode, quorums, retries and repair
- [docs/limits.md](docs/limits.md): every default and cap, and why
- [docs/self-host.md](docs/self-host.md): a host other people reach
- [docs/spec.md](docs/spec.md): the record formats and the rules every host and reader applies
- Stuck? `blogs doctor` reports the home, the pool, the streams, and whether every configured endpoint answers.
