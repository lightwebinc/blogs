# syntax=docker/dockerfile:1.27.1@sha256:4edf897a3ffa55b89f906fc8cc78afdb3f1834cc9c7083565e611a8a7d5fe99e
#
# Two images from one build, each a single static binary on a distroless
# nonroot base:
#
#   docker build -t ghcr.io/lightwebinc/blogs .                             # the command (default)
#   docker build --target devchain -t ghcr.io/lightwebinc/blogs-devchain .  # the local chain
#
# The builder is pinned by digest and is a later toolchain than go.mod's
# floor, so scanning sees the standard library that ships.
#
# No ENV defaults are baked in. The hosts, the header source, the node and
# the settlement leg are addresses of somebody's deployment and have no
# default anywhere: a default would send a producer's objects, or a
# reader's questions, to a server nobody configured. Pass them as
# BLOGS_<KEY> variables or in a config file (docs/usage.md).
#
# The command keeps a producer's state (identity, coin pool, streams) under
# $BLOGS_HOME, ~/.blogs by default, which is /home/nonroot/.blogs here. The
# image carries that directory, empty, owned by nonroot and mode 0700, so a
# new named volume mounted there starts with the same owner and mode:
#
#   docker run --rm -v blogs-home:/home/nonroot/.blogs ghcr.io/lightwebinc/blogs init
#
# A volume mounted on a path the image lacks is created root-owned, and
# every write to it is then refused. A bind mount keeps the host
# directory's owner instead: make it writable by uid 65532, or run with
# --user.

# The builder runs on the build machine's own platform and cross-compiles
# to the target's, so an arm64 image needs no emulation.
FROM --platform=$BUILDPLATFORM golang:1.27-alpine@sha256:cf6fca6641884b8433441b2b0652976f975e1d0fdd26d177eaaf8596087f3125 AS builder
RUN apk add --no-cache git ca-certificates
WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    GOWORK=off go mod download

COPY . .
ARG VERSION=dev
ARG TARGETOS=linux
ARG TARGETARCH=amd64
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    set -eux; \
    mkdir -p /out /state/.blogs /state/devchain; \
    for cmd in blogs devchain; do \
      GOWORK=off CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
        go build -trimpath -buildvcs=false \
          -ldflags "-s -w -X main.version=${VERSION}" \
          -o /out/$cmd ./cmd/$cmd; \
    done

# The local chain for trying blogs without a node or coin: a node's RPC and
# asset API and a header source on port 8080. It has no proof of work and
# no peers, and nothing it mines is money. Its journal, which replays the
# chain after a restart, is kept in /var/lib/devchain; mount a volume there.
FROM gcr.io/distroless/static:nonroot@sha256:1c2c046bc09ed40fad370b599a0b1ae7987f55b01e247cf27a7c27cd97e5bbc7 AS devchain
USER nonroot:nonroot
COPY --from=builder /out/devchain /usr/local/bin/devchain
COPY --from=builder --chown=65532:65532 --chmod=0700 /state/devchain /var/lib/devchain
COPY LICENSE NOTICE LICENSE-THIRD-PARTY /usr/share/doc/blogs/
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/devchain", "-journal", "/var/lib/devchain/journal"]

# The command. Last, so a plain build produces it.
FROM gcr.io/distroless/static:nonroot@sha256:1c2c046bc09ed40fad370b599a0b1ae7987f55b01e247cf27a7c27cd97e5bbc7 AS blogs
USER nonroot:nonroot
COPY --from=builder /out/blogs /usr/local/bin/blogs
COPY --from=builder --chown=65532:65532 --chmod=0700 /state/.blogs /home/nonroot/.blogs

# The licences travel with the image. The binary statically contains
# go-sdk and the other modules LICENSE-THIRD-PARTY lists, whose licences
# require their text in any copy, and static linking leaves those files
# behind.
COPY LICENSE NOTICE LICENSE-THIRD-PARTY /usr/share/doc/blogs/

# No EXPOSE: blogs listens on nothing. It is a client of overlay hosts, a
# header source and, for a producer, a node and a settlement leg.
ENTRYPOINT ["/usr/local/bin/blogs"]
