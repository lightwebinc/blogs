#!/bin/sh
# Starts the reference overlay host with the blogs module. Two things are
# added to the stock start:
#
#  - BLOGS_STREAMS is required, and when OVERLAY_TOPICS is unset it is
#    derived from it (tm_log_<stream> for each entry), so a host is
#    configured by one variable. Each entry is <stream>=open or
#    <stream>=<producer key>+<producer key>..., as `blogs stream new` prints
#    it; the module checks the grammar.
#  - an unset OVERLAY_ADMIN_TOKEN is replaced by a random one for this run,
#    because the host requires one. Nothing can use the admin surface until
#    the operator sets their own.
set -eu

if [ -z "${BLOGS_STREAMS:-}" ]; then
  echo "blogs-host: BLOGS_STREAMS is empty. Create a stream with 'blogs stream new <name>' and set BLOGS_STREAMS to the line it prints (several streams: comma separated)." >&2
  exit 64
fi

if [ -z "${OVERLAY_TOPICS:-}" ]; then
  topics=""
  for entry in $(printf '%s' "$BLOGS_STREAMS" | tr ',' ' '); do
    stream=${entry%%=*}
    topics="${topics:+$topics,}tm_log_$stream"
  done
  OVERLAY_TOPICS=$topics
  export OVERLAY_TOPICS
fi

if [ -z "${OVERLAY_ADMIN_TOKEN:-}" ]; then
  OVERLAY_ADMIN_TOKEN=$(od -An -N32 -tx1 /dev/urandom | tr -d ' \n')
  export OVERLAY_ADMIN_TOKEN
  echo "blogs-host: OVERLAY_ADMIN_TOKEN is unset; using a random one for this run (set it to use /admin)" >&2
fi

echo "blogs-host: topics $OVERLAY_TOPICS" >&2
exec node /app/dist/index.js "$@"
