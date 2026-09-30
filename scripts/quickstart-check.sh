#!/usr/bin/env bash
# QUICKSTART.md, run as written, in a throwaway copy of the quickstart
# compose project: the local chain, two hosts and MySQL; init, fund, a
# stream, a unicast produce, read, verify-interval; a host that misses
# objects while it is down, and the backfill that repairs it.
#
#   scripts/quickstart-check.sh
#
# Needs the three images the compose file names (make docker-build, make
# docker-build-host, or the published ones). COMPOSE names the compose
# command (default "docker compose"). Everything it creates, volumes
# included, is removed at the end, pass or fail.
set -euo pipefail

here=$(cd "$(dirname "$0")/.." && pwd)
compose=${COMPOSE:-docker compose}
work=$(mktemp -d)
project=blogs-quickstart-check-$$
cp "$here/deploy/quickstart/compose.yaml" "$work/"
cd "$work"

dc() { $compose -p "$project" "$@"; }
blogs() { dc run --rm -T blogs "$@"; }
cleanup() {
	dc --profile cli down -v --remove-orphans >/dev/null 2>&1 || true
	rm -rf "$work"
}
trap cleanup EXIT
step() { printf '\n== %s\n' "$*"; }
fail() {
	echo "quickstart-check: $*" >&2
	dc logs --no-color --tail 40 >&2 || true
	exit 1
}

step "1. the local chain"
dc up -d chain

step "2. a producer: init, fund, a stream"
blogs init
blogs fund
blogs stream new app | tee stream.txt
sed -n 's/^host *//p' stream.txt >.env
stream=$(sed -n 's/^stream *//p' stream.txt)
producer=$(sed -n 's/^producer *//p' stream.txt)
[ -n "$stream" ] && [ -n "$producer" ] && grep -q '^BLOGS_STREAMS=' .env || fail "stream new printed no stream"

step "3. two hosts that keep the stream"
dc up -d
for i in $(seq 1 90); do
	out=$(blogs doctor 2>/dev/null || true)
	[ "$(printf '%s\n' "$out" | grep -c '^host .* answering')" = 2 ] && break
	sleep 2
done
printf '%s\n' "$out"
[ "$(printf '%s\n' "$out" | grep -c '^host .* answering')" = 2 ] || fail "the hosts did not come up"

step "4. produce in unicast mode, read it back, verify the interval"
seq 1 40 | sed 's/^/hello from blogs, line /' | blogs produce "$stream" -max-lines 10 | tee produce.txt
grep -q 'published seq 1 to 4' produce.txt || fail "produce did not publish seq 1 to 4"
grep -q 'anchored to seq 4: interval 1' produce.txt || fail "produce did not anchor interval 1"
[ "$(grep -c '^host .*missed 0$' produce.txt)" = 2 ] || fail "a host missed something"
blogs read "$stream" "$producer" | tee read.txt | tail -3
[ "$(wc -l <read.txt)" = 40 ] && [ "$(tail -1 read.txt)" = "hello from blogs, line 40" ] || fail "read did not print the 40 lines"
blogs verify-interval "$stream" "$producer" 1 | tee verify.txt
grep -q '^VERIFIED interval 1 ' verify.txt || fail "interval 1 did not verify"

step "5. the unicast tradeoff: a host that is down misses objects"
dc stop host-b
seq 41 50 | sed 's/^/hello from blogs, line /' | blogs -quorum one produce "$stream" | tee produce2.txt
grep -q 'anchored to seq 5: interval 2' produce2.txt || fail "the second produce did not anchor interval 2"
repair=$(sed -n 's/.*copy them from the others: blogs //p' produce2.txt)
[ -n "$repair" ] || fail "no repair command was printed"
dc start host-b
for i in $(seq 1 60); do blogs -hosts http://host-b:8080 doctor 2>/dev/null | grep -q '^host .* answering' && break; sleep 2; done
rc=0
blogs -hosts http://host-b:8080 verify-interval "$stream" "$producer" 2 || rc=$?
[ "$rc" = 3 ] || fail "host-b should be INCOMPLETE for interval 2 (exit 3), got $rc"
# shellcheck disable=SC2086
blogs $repair | tee repair.txt
grep -q '0 missing, 0 refused' repair.txt || fail "the backfill did not repair host-b"
blogs -hosts http://host-b:8080 verify-interval "$stream" "$producer" 2 | tee verify2.txt
grep -q '^VERIFIED interval 2 ' verify2.txt || fail "interval 2 did not verify at host-b after the repair"

echo
echo "quickstart-check: ok"
