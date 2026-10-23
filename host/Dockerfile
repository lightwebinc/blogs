# syntax=docker/dockerfile:1.28.0@sha256:bb22d9815c728170f72750f4e5b0d672e06176142e1d602c7e66c050100b7e5b
#
# blogs-host: a reference overlay host image with the blogs module
# (tm_log_<stream> and ls_log) built in, so a host is one image and needs no
# file copied onto it. Build from the repository root:
#
#   docker build -f host/Dockerfile \
#     --build-arg REFERENCE_HOST_IMAGE=<reference overlay host image> \
#     -t ghcr.io/lightwebinc/blogs-host .
#
# REFERENCE_HOST_IMAGE names the overlay host image the module is added to:
# one whose application is /app/dist/index.js with its node_modules beside
# it, that loads the absolute paths in OVERLAY_MODULES and reads the
# OVERLAY_* settings docs/self-host.md lists. It is a build argument with no
# default, so what the image is built on is always stated, and a release
# pins it; the placeholder below only makes a build that forgot it fail
# with a name that says so.

ARG REFERENCE_HOST_IMAGE=set-the-reference-host-image-build-arg

# The bundle is JavaScript, the same bytes on every platform, so it is
# built once on the build machine's own platform; the final stage only
# copies files onto the host image, so an arm64 image needs no emulation.
FROM --platform=$BUILDPLATFORM node:24@sha256:6449e19d8184d84062525b6a7524def03146c2cb04a7f08b076680d0e8e4240a AS bundle
WORKDIR /src/host
COPY host/package.json host/package-lock.json ./
COPY host/vendor ./vendor
RUN npm ci --no-audit --no-fund
COPY host/ ./
RUN npm run check && npm run build && npm run bundle

FROM ${REFERENCE_HOST_IMAGE}
# Beside the host's own node_modules, so the bundle's one import (@bsv/sdk)
# resolves to the host's copy (docs/host.md).
COPY --from=bundle /src/host/bundle/blogs-module.js /app/modules/blogs/blogs-module.js
COPY --chmod=0755 host/entrypoint.sh /usr/local/bin/blogs-host
COPY LICENSE NOTICE LICENSE-THIRD-PARTY /usr/share/doc/blogs/
ENV OVERLAY_MODULES=/app/modules/blogs/blogs-module.js
ENTRYPOINT ["/usr/local/bin/blogs-host"]
