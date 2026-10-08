# blogs

[![CI](https://github.com/lightwebinc/blogs/actions/workflows/ci.yml/badge.svg)](https://github.com/lightwebinc/blogs/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/lightwebinc/blogs)](https://github.com/lightwebinc/blogs/releases)
[![Go version](https://img.shields.io/github/go-mod/go-version/lightwebinc/blogs)](go.mod)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

> [!WARNING]
> **Experimental software.** blogs is part of [bstack](https://github.com/lightwebinc/bstack),
> the applications and patterns built on [BSV Layered Multicast](https://github.com/lightwebinc/bsv-multicast).
> It is published to be built on and improved. Interfaces, formats and behavior may change
> rapidly between releases; pin an exact version.

Distributed logging on Bitcoin. A producer appends signed, numbered
batches of log lines to a stream; overlay hosts keep them; and one small
mined anchor per interval commits the interval's batches, so any host's
copy can be proven complete and any auditor can prove when a line was
committed, against block headers it checks itself.

```console
$ tail -F /var/log/app.log | blogs produce app_qzxkvbmwtr
$ blogs -hosts https://logs.example.com verify-interval app_qzxkvbmwtr 02c0ffee… 1
VERIFIED interval 1 of app_qzxkvbmwtr by 02c0ffee…: seqs 1 to 4, root 5d7f…3999, committed no later than block 912430
```

blogs runs on BSV mainnet (`-network main`, the default) and testnet
(`-network test`), and needs no node and no infrastructure of its own:
headers and chain lookups come from the public WhatsOnChain API, checked
against proof of work, and transactions are broadcast through GorillaPool's
public arcade. Readers and auditors need only the hosts that keep a
stream: no coin, no home. A producer pays for its own anchors and batches
from coin it already owns (about 113 satoshis an anchor and 6 a batch):
install, `init`, pay the fund address from your wallet, `fund`, produce
([QUICKSTART.md](QUICKSTART.md#on-mainnet-or-testnet)). A node of your own,
or a header service you run, can replace any public service
([docs/configuration.md](docs/configuration.md)).

**New here?** [QUICKSTART.md](QUICKSTART.md) starts on the real network,
then gives a local development sandbox (a private regtest chain, no coin
needed) with two hosts and a verified interval in about five minutes.

## How it works

- **Batches are never mined.** Each rides an unmined carrier transaction
  that spends one output of a mined funding tree, so it verifies by SPV
  without paying for block space, and the producer can retract it by
  spending that output.
- **One anchor per interval is mined.** It commits the RFC 6962 root over
  the interval's batches and spends the previous interval's anchor, so the
  anchors form one chain that covers every batch of the stream once.
- **Hosts are replicas, not authorities.** A reader and an auditor verify
  everything a host answers; a host can withhold, but it cannot forge. Any
  host's copy of an interval is either complete and proven, or visibly not.
- **Two ways to reach the hosts.** On the multicast plane a producer
  submits each object once and every subscribed host receives it. Without
  it, in [unicast mode](docs/unicast.md), the producer submits to each host
  itself and a host that missed something is repaired by backfill.

## Documentation

| If you want to | Read |
| --- | --- |
| Get started on mainnet or testnet, or try it locally with no coin | [QUICKSTART.md](QUICKSTART.md) |
| Learn what it is for and how to use it, end to end, with examples | [docs/user-guide.md](docs/user-guide.md) |
| See how the pieces fit: command, hosts, chain, code layout | [docs/architecture.md](docs/architecture.md) |
| Set a key, an environment variable or a flag | [docs/configuration.md](docs/configuration.md) |
| Look up a command, its flags, an exit code or a message | [docs/usage.md](docs/usage.md) |
| Run without the multicast plane, and repair a host | [docs/unicast.md](docs/unicast.md) |
| Know the defaults and caps, and why each is set where it is | [docs/limits.md](docs/limits.md) |
| Run it as a logging backend: syslog, OTLP and Loki push in, Grafana out, every line marked verified | [docs/backend.md](docs/backend.md) |
| Run a host other people reach | [docs/self-host.md](docs/self-host.md) |
| Load the module into an overlay host you already run | [docs/host.md](docs/host.md) |
| Implement a compatible producer, reader or host | [docs/spec.md](docs/spec.md) (the specification), [docs/frozen.md](docs/frozen.md) |

## Install

```bash
go install github.com/lightwebinc/blogs/cmd/blogs@latest      # Go 1.27.1 or later
```

| | |
| --- | --- |
| Release binaries | static `blogs` for Linux and macOS (amd64, arm64) on the [releases page](https://github.com/lightwebinc/blogs/releases), each tarball with its licenses, plus `SHA256SUMS` |
| Container | `docker run --rm -v blogs-home:/home/nonroot/.blogs ghcr.io/lightwebinc/blogs doctor` (the command), `ghcr.io/lightwebinc/blogs-host` (a host), `ghcr.io/lightwebinc/blogs-devchain` (the local development chain) |
| Source | `make blogs` into `bin/blogs` |
| A host of your own | `deploy/compose.yaml` ([docs/self-host.md](docs/self-host.md)) |

## Build and test

```bash
(cd host && npm ci)    # once: the host module's toolchain (Node 24)
make verify            # what CI runs: format, vet, dependency set, licenses, typecheck, vectors, tests under -race
make lint              # golangci-lint
make docker-build      # the command's and the local chain's images
make docker-smoke      # both images, offline and together
make e2e-client  REFERENCE_HOST=/path/to/host   # the command against two reference hosts (plane-style)
make e2e-unicast REFERENCE_HOST=/path/to/host   # the same in unicast mode
make e2e-backend REFERENCE_HOST=/path/to/host   # the logging backend against three reference hosts
make quickstart-check  # QUICKSTART.md, run as written, in Docker Compose
```

`NODE=/path/to/node` selects a Node 24 binary that is not on `PATH`. Two
direct Go dependencies, both pinned exactly and asserted by `make verify`:
[go-sdk](https://github.com/bsv-blockchain/go-sdk) at v1.7.1 and
[bcommon](https://github.com/lightwebinc/bcommon), whose TypeScript package
at the same version is vendored under `host/vendor`. Moving bcommon, the
vectors and the end-to-end targets are described in
[docs/usage.md](docs/usage.md#testing).

## Security

Report a vulnerability privately: [SECURITY.md](SECURITY.md).

## License

Apache-2.0 ([LICENSE](LICENSE)). One dependency is under the Open BSV
License Version 5, whose grant is revocable and which limits use to the BSV
blockchains; [NOTICE](NOTICE) explains, and
[LICENSE-THIRD-PARTY](LICENSE-THIRD-PARTY) carries every license text.
