# GOWORK=off everywhere on purpose: a workspace would resolve bcommon to a
# local checkout, so a go.mod skew would stay invisible here. What is tested
# is what go.mod pins.
#
# The TypeScript package under host/ needs Node 24 and its dependencies
# installed once (cd host && npm ci). The targets run tsc and node directly,
# so NODE may name a Node 24 binary that is not on PATH:
#
#	make test NODE=/path/to/node24

.PHONY: verify test go-test ts-test ts-check ts-build module node-check vet fmt-check deps-check vectors vectors-check e2e e2e-client blogs

NODE ?= node
TSC := $(NODE) node_modules/typescript/bin/tsc
SDK := github.com/bsv-blockchain/go-sdk
SDK_VERSION := v1.5.2

verify: fmt-check vet deps-check ts-check vectors-check test

# Go and TypeScript. The Go tests read the TypeScript-written vectors under
# testdata/ts, and the TypeScript tests read the Go-generated ones under
# testdata/vectors: each side decodes the other's bytes.
test: go-test ts-test

go-test:
	GOWORK=off go test -race -count=2 ./...

# The e2e build tag is vetted too, so the client end to end compiles.
vet:
	GOWORK=off go vet ./...
	GOWORK=off go vet -tags e2e ./cmd/blogs/

# The command, into bin/.
blogs:
	GOWORK=off go build -o bin/blogs ./cmd/blogs

fmt-check:
	@test -z "$$(gofmt -l .)" || { echo "gofmt:"; gofmt -l .; exit 1; }

# go-sdk at exactly the version bcommon pins, which the vectors were made
# with.
deps-check:
	@v=$$(GOWORK=off go list -m -f '{{.Version}}' $(SDK)); \
	if [ "$$v" != "$(SDK_VERSION)" ]; then echo "$(SDK) is $$v, want exactly $(SDK_VERSION)"; exit 1; fi

node-check:
	@$(NODE) -e 'process.exit(process.versions.node.split(".")[0] === "24" ? 0 : 1)' || \
		{ echo "host/ needs Node 24; set NODE=/path/to/node"; exit 1; }

ts-check: node-check
	cd host && $(TSC) --noEmit

# tsc into host/dist, then the deployable bundle into host/bundle.
ts-build: node-check
	cd host && $(NODE) -e "require('node:fs').rmSync('dist', { recursive: true, force: true })" && $(TSC) && $(NODE) scripts/bundle.js

# The host module a reference overlay host loads: one file,
# host/bundle/blogs-module.js, with bcommon inlined and nothing imported but
# @bsv/sdk (the host's own copy) and Node built-ins. It is copied into the
# host's tree and named by absolute path in OVERLAY_MODULES (docs/host.md).
# Its tests (make test) mount that file and run the golden vectors through it.
module: ts-build

ts-test: ts-build
	cd host && $(NODE) --test "dist/**/*.test.js"

# One publish-and-lookup on a local reference overlay host loading the
# bundle, staged beside the host's node_modules as a deployment places it,
# with restart and restore. Not part of verify: it needs Docker (for MySQL) and a reference
# host directory holding its compiled dist/index.js and its node_modules.
#
#	make e2e REFERENCE_HOST=/path/to/reference-host
e2e: ts-build
	@test -n "$(REFERENCE_HOST)" || { echo "set REFERENCE_HOST=/path/to/reference-host"; exit 1; }
	cd host && $(NODE) dist/e2e.js $(abspath $(REFERENCE_HOST))

# The blogs command end to end: init, fund, stream, produce (both profiles,
# gzip, a restart, a publish that fails and is resumed), read,
# verify-interval, backfill, archive and sweep, against two local reference
# hosts loading the bundle, over a local chain (internal/testchain) serving
# the node's API and the header source. Not part of verify: it needs Docker
# (for MySQL) and a reference host directory holding its compiled
# dist/index.js and its node_modules.
#
#	make e2e-client REFERENCE_HOST=/path/to/reference-host
e2e-client: ts-build
	@test -n "$(REFERENCE_HOST)" || { echo "set REFERENCE_HOST=/path/to/reference-host"; exit 1; }
	BLOGS_E2E_REFERENCE_HOST=$(abspath $(REFERENCE_HOST)) BLOGS_E2E_NODE=$(NODE) \
		GOWORK=off go test -tags e2e -count=1 -v -run TestClientE2E ./cmd/blogs/

# Write both vector sets: the Go codec's under testdata/vectors, the
# TypeScript codec's under testdata/ts. A change to either is a change to
# the contract's goldens.
vectors: ts-build
	GOWORK=off go run ./cmd/vectors
	cd host && $(NODE) dist/tsvectors.js

# Regenerate both in memory and compare byte for byte; write nothing.
vectors-check: ts-build
	GOWORK=off go run ./cmd/vectors -check
	cd host && $(NODE) dist/tsvectors.js -check
