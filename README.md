# blogs

Distributed logging on the BEEF object plane. A producer appends a signed,
numbered log batch once; every archive subscribed to the stream's topic
receives it at the same moment; one mined anchor per interval commits the
interval's batches, so any archive proves its copy complete and any auditor
proves time. Batches are never mined: they ride unmined carriers funded from
a funding tree, and only the anchors are mined.

**Status:** producer and reader. The record formats, the anchor, the topic
manager and lookup rules, and golden vectors are specified; the record codec
and the topic manager's transaction rules are built in Go (`internal/logrec`)
and in TypeScript (`host/`), held to the same vectors; the host module
(`tm_log_<name>_<suffix>` and `ls_log`) loads into a reference overlay host;
the `blogs` command produces streams from stdin or a followed file,
retracts batches by sweeping their funding outputs, and reads, audits,
backfills and archives them. The host module deploys as one file.

## Documentation

- [Specification](docs/spec.md): the batch record, the digest-only locator,
  the interval anchor and its RFC 6962 leaf rule, carriers and funding trees,
  topics and lookup, admission, profiles, bounds, repair and lateness
- [Frozen at first publish](docs/frozen.md): every name and byte that cannot
  change once a record is on a public host
- [Host module](docs/host.md): loading `tm_log` and `ls_log` into an overlay
  host, configuration, statuses, restore and metrics
- [The blogs command](docs/usage.md): `init`, `fund`, `stream`, `produce`,
  `anchor`, `sweep`, `read`, `verify-interval`, `backfill`, `archive`,
  `doctor`; configuration, exit codes and messages

```bash
blogs init && blogs fund && blogs stream new app
tail -F /var/log/app.log | blogs produce app_<suffix>
blogs read app_<suffix> <producer key>
blogs verify-interval app_<suffix> <producer key> 1
```

## Build and test

Go 1.27 or later and Node 24. Built on
[bcommon](https://github.com/lightwebinc/bcommon) at an exact tag, its
TypeScript package at the same version (vendored under `host/vendor`), and
github.com/bsv-blockchain/go-sdk at exactly v1.5.2.

```bash
(cd host && npm ci)  # once
make verify          # gofmt, vet, go-sdk pin, typecheck, vectors, tests
make test            # Go and TypeScript tests
make vectors         # regenerate testdata/vectors (Go) and testdata/ts (TypeScript)
make vectors-check   # compare both, write nothing
make blogs           # the command, into bin/blogs
make module          # the host module bundle, host/bundle/blogs-module.js
make e2e REFERENCE_HOST=/path/to/host         # the module on a local host (Docker)
make e2e-client REFERENCE_HOST=/path/to/host  # the command against local hosts (Docker)
```

`NODE=/path/to/node` selects a Node 24 binary that is not on `PATH`. To move
bcommon to another tag, pack its `ts/` package at that tag into
`host/vendor/lightwebinc-bcommon-<version>.tgz`, point `host/package.json` at
it and reinstall; a test fails while the Go and TypeScript sides name
different versions.

## Licence

Apache-2.0 ([LICENSE](LICENSE)).
