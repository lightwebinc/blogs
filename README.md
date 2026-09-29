# blogs

Distributed logging on the BEEF object plane. A producer appends a signed,
numbered log batch once; every archive subscribed to the stream's topic
receives it at the same moment; one mined anchor per interval commits the
interval's batches, so any archive proves its copy complete and any auditor
proves time. Batches are never mined: they ride unmined carriers funded from
a funding tree, and only the anchors are mined.

**Status:** contract. The record formats, the anchor, the topic manager and
lookup rules, and golden vectors are specified; the command and the host
module are not built yet.

## Documentation

- [Specification](docs/spec.md): the batch record, the digest-only locator,
  the interval anchor and its RFC 6962 leaf rule, carriers and funding trees,
  topics and lookup, admission, profiles, bounds, repair and lateness
- [Frozen at first publish](docs/frozen.md): every name and byte that cannot
  change once a record is on a public host

## Build and test

Go 1.27 or later. Built on [bcommon](https://github.com/lightwebinc/bcommon)
at an exact tag and github.com/bsv-blockchain/go-sdk at exactly v1.5.2.

```bash
GOWORK=off go vet ./...
GOWORK=off go test -race ./...
GOWORK=off go run ./cmd/vectors        # regenerate testdata/vectors
GOWORK=off go run ./cmd/vectors -check # compare, write nothing
```

## Licence

Apache-2.0 ([LICENSE](LICENSE)).
