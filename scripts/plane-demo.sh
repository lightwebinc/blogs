#!/usr/bin/env bash
# The blogs demo against three archive hosts: two overlay hosts that receive
# the stream from the plane, and a third host process that does not.
#
#   scripts/plane-demo.sh [-c ENV_FILE] setup   # home, funds, stream; prints the host lines
#   scripts/plane-demo.sh [-c ENV_FILE] run     # the demo; run it twice, green both times
#
# Every endpoint comes from ENV_FILE (default scripts/plane-demo.env, which git
# ignores); scripts/plane-demo.env.example lists the keys with documentation
# addresses. Nothing here names a network.
#
# setup is idempotent: it creates the producer home once, funds it when it
# holds no spendable coin, and creates the stream once (a stream's topic and
# producer key must be in each host's configuration before run). It writes
# DEMO_DIR/stream.env and prints the BLOGS_STREAMS entry and topic.
#
# run publishes DEMO_LINES new lines on the stream and anchors them, then:
#   1. waits until archives A and B (on the plane) hold the new interval;
#   2. reads the new lines from A and from B and compares them to the input;
#   3. verify-interval against A and B;
#   4. finds the new interval missing at C (off the plane, deliberately empty),
#      backfills C from A, then reads and verifies it at C;
#   5. runs blogs archive against A, B and C into three directories;
#   6. requires one root for the interval: the audit's at A, B and C and
#      each archive's interval file.
# Each run adds seqs and an interval, so a second run exercises the same
# path on a stream that already has history. Exit 0 only when every step held.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
env_file="$here/plane-demo.env"
while getopts "c:" opt; do
	case $opt in
	c) env_file=$OPTARG ;;
	*) exit 2 ;;
	esac
done
shift $((OPTIND - 1))
mode=${1:-}
case $mode in
setup | run) ;;
*)
	echo "usage: $0 [-c ENV_FILE] setup|run" >&2
	exit 2
	;;
esac

[ -r "$env_file" ] || {
	echo "plane-demo: no config at $env_file (copy scripts/plane-demo.env.example and fill it in)" >&2
	exit 2
}
set -a
# shellcheck disable=SC1090
. "$env_file"
set +a

need() {
	local k
	for k in "$@"; do
		[ -n "${!k:-}" ] || {
			echo "plane-demo: $k is not set in $env_file" >&2
			exit 2
		}
	done
}
need BLOGS_BIN DEMO_HOME DEMO_DIR BLOGS_NETWORK BLOGS_ASSET BLOGS_SETTLE BLOGS_FACADE BLOGS_HEADER_URL \
	ARCHIVE_A ARCHIVE_B ARCHIVE_C STREAM_NAME
export BLOGS_HOME=$DEMO_HOME
DEMO_LINES=${DEMO_LINES:-40}
DEMO_MAX_LINES=${DEMO_MAX_LINES:-8}
FUND_BLOCKS=${FUND_BLOCKS:-101}
DELIVERY_WAIT=${DELIVERY_WAIT:-180}
ANCHOR_WAIT=${ANCHOR_WAIT:-15m}
[ -x "$BLOGS_BIN" ] || {
	echo "plane-demo: $BLOGS_BIN is not an executable (make blogs)" >&2
	exit 2
}
mkdir -p "$DEMO_DIR"

say() { printf '%s  %s\n' "$(date -u +%H:%M:%S)" "$*"; }
fail() {
	say "FAIL: $*" >&2
	exit 1
}
blogs() { "$BLOGS_BIN" "$@"; }

# fund_if_needed mines to the fund address when the pool holds no spendable
# coin (a chain the operator runs answers generatetoaddress).
fund_if_needed() {
	local pool outputs immature
	pool=$(blogs doctor | sed -n 's/^pool  *//p')
	outputs=$(sed -n 's/^\([0-9][0-9]*\) output.*/\1/p' <<<"$pool")
	immature=$(sed -n 's/.* \([0-9][0-9]*\) immature.*/\1/p' <<<"$pool")
	outputs=${outputs:-0}
	immature=${immature:-0}
	if [ $((outputs - immature)) -gt 0 ]; then
		say "pool: $pool"
		return
	fi
	need BLOGS_RPC
	say "pool has no spendable coin ($pool); funding with $FUND_BLOCKS block(s)"
	blogs fund -blocks "$FUND_BLOCKS"
}

# stream_of finds the home's stream named STREAM_NAME_<suffix>, if any.
stream_of() {
	blogs stream list | awk -v n="$STREAM_NAME" '$1 ~ "^" n "_[a-z]+$" { print $1; exit }'
}

setup() {
	blogs init
	fund_if_needed
	local stream producer
	stream=$(stream_of)
	if [ -z "$stream" ]; then
		blogs stream new "$STREAM_NAME"
		stream=$(stream_of)
	fi
	[ -n "$stream" ] || fail "no stream $STREAM_NAME in $DEMO_HOME"
	producer=$(blogs init 2>/dev/null | sed -n 's/^identity  *//p')
	cat >"$DEMO_DIR/stream.env" <<EOF
STREAM=$stream
PRODUCER=$producer
TOPIC=tm_log_$stream
EOF
	say "stream     $stream"
	say "topic      tm_log_$stream"
	say "producer   $producer"
	say "each archive host needs:"
	say "  OVERLAY_TOPICS  += tm_log_$stream"
	say "  BLOGS_STREAMS   += $stream=$producer"
	say "and hosts A and B a plane subscription to tm_log_$stream"
	blogs -hosts "$ARCHIVE_A,$ARCHIVE_B,$ARCHIVE_C" doctor
}

# interval_root DIR STREAM PRODUCER K prints the root of interval K from an
# archive directory.
interval_root() {
	local f
	f=$(printf '%s/%s/%s/intervals/%016d.json' "$1" "$2" "$3" "$4")
	[ -r "$f" ] || return 0
	sed -n 's/^  "root": "\([0-9a-f]*\)",$/\1/p' "$f"
}

run() {
	[ -r "$DEMO_DIR/stream.env" ] || fail "no $DEMO_DIR/stream.env; run setup first"
	local STREAM PRODUCER TOPIC
	# shellcheck disable=SC1091
	. "$DEMO_DIR/stream.env"
	local id dir input out first last k host name code root want
	id=$(date -u +%Y%m%dT%H%M%SZ)-$$
	dir="$DEMO_DIR/runs/$id"
	mkdir -p "$dir"
	say "run $id on $STREAM (topic $TOPIC), logs in $dir"

	# 0. Every host answers ls_log; the pool can pay.
	out=$(blogs -hosts "$ARCHIVE_A,$ARCHIVE_B,$ARCHIVE_C" doctor | tee "$dir/doctor.txt")
	if grep -q 'NOT ANSWERING\|NOT CONFIGURED' <<<"$out"; then
		cat "$dir/doctor.txt" >&2
		fail "a host or the header source does not answer"
	fi
	fund_if_needed

	# 1. Produce and anchor.
	input="$dir/input.log"
	for i in $(seq 1 "$DEMO_LINES"); do
		printf 'blogs plane demo %s line %d of %d\n' "$id" "$i" "$DEMO_LINES"
	done >"$input"
	blogs produce "$STREAM" -max-lines "$DEMO_MAX_LINES" -anchor-wait "$ANCHOR_WAIT" <"$input" >"$dir/produce.out" 2>"$dir/produce.err" ||
		fail "produce exited $? (see $dir/produce.err)"
	read -r first last < <(sed -n 's/.*published seq \([0-9]*\) to \([0-9]*\)$/\1 \2/p' "$dir/produce.out")
	[ -n "${last:-}" ] || fail "produce published nothing (see $dir/produce.out)"
	blogs anchor "$STREAM" -wait "$ANCHOR_WAIT" >"$dir/anchor.out" 2>"$dir/anchor.err" || fail "anchor exited $?"
	k=$(sed -n 's/^anchored to seq \([0-9]*\): interval \([0-9]*\),.*/\1 \2/p' "$dir/anchor.out")
	[ -n "$k" ] || fail "nothing is anchored (see $dir/anchor.out)"
	[ "${k%% *}" -ge "$last" ] || fail "anchored to seq ${k%% *}, published to $last"
	k=${k##* }
	say "produced seqs $first to $last; anchored through interval $k"

	# 2. The plane delivers to A and B: wait until each verifies interval k.
	for host in "$ARCHIVE_A" "$ARCHIVE_B"; do
		local deadline=$((SECONDS + DELIVERY_WAIT))
		until blogs -hosts "$host" verify-interval "$STREAM" "$PRODUCER" "$k" >"$dir/verify-wait.out" 2>&1; do
			[ $SECONDS -lt $deadline ] || fail "$host did not verify interval $k within ${DELIVERY_WAIT}s: $(tail -1 "$dir/verify-wait.out")"
			sleep 5
		done
		say "$host holds interval $k"
	done

	# 3. Read the new lines from A and B.
	for host in "$ARCHIVE_A" "$ARCHIVE_B"; do
		blogs -hosts "$host" read "$STREAM" "$PRODUCER" -from "$first" -to "$last" >"$dir/read.out" 2>"$dir/read.err" ||
			fail "read at $host exited $? (see $dir/read.err)"
		cmp -s "$input" "$dir/read.out" || fail "read at $host differs from the input (see $dir/read.out)"
		say "read at $host: $DEMO_LINES lines, identical"
	done

	# 4. verify-interval k at A and B.
	declare -A roots
	for host in "$ARCHIVE_A" "$ARCHIVE_B"; do
		out=$(blogs -hosts "$host" verify-interval "$STREAM" "$PRODUCER" "$k") || fail "verify-interval $k at $host exited $?"
		roots[verify $host]=$(sed -n 's/.*, root \([0-9a-f]*\), committed.*/\1/p' <<<"$out")
		say "$(tail -1 <<<"$out")"
	done

	# 5. C is off the plane: interval k is missing there until backfilled.
	code=0
	blogs -hosts "$ARCHIVE_C" verify-interval "$STREAM" "$PRODUCER" "$k" >"$dir/verify-c-before.out" 2>&1 || code=$?
	case $code in
	3) say "$ARCHIVE_C: interval $k INCOMPLETE before backfill, as expected" ;;
	0) say "$ARCHIVE_C: interval $k already VERIFIED (a previous run filled it)" ;;
	*) fail "verify-interval at $ARCHIVE_C exited $code before backfill (see $dir/verify-c-before.out)" ;;
	esac
	for i in $(seq 1 "$k"); do
		blogs backfill "$STREAM" "$PRODUCER" -from "$ARCHIVE_A" -to "$ARCHIVE_C" -interval "$i" >>"$dir/backfill.out" 2>&1 ||
			fail "backfill of interval $i into $ARCHIVE_C exited $? (see $dir/backfill.out)"
	done
	say "$(grep '^backfill ' "$dir/backfill.out" | tail -1)"
	blogs -hosts "$ARCHIVE_C" read "$STREAM" "$PRODUCER" -from "$first" -to "$last" >"$dir/read-c.out" 2>"$dir/read-c.err" ||
		fail "read at $ARCHIVE_C exited $?"
	cmp -s "$input" "$dir/read-c.out" || fail "read at $ARCHIVE_C differs from the input"
	out=$(blogs -hosts "$ARCHIVE_C" verify-interval "$STREAM" "$PRODUCER" "$k") || fail "verify-interval $k at $ARCHIVE_C exited $? after backfill"
	roots[verify $ARCHIVE_C]=$(sed -n 's/.*, root \([0-9a-f]*\), committed.*/\1/p' <<<"$out")
	say "$(tail -1 <<<"$out")"

	# 6. One archive directory per host.
	for name in a b c; do
		case $name in
		a) host=$ARCHIVE_A ;;
		b) host=$ARCHIVE_B ;;
		c) host=$ARCHIVE_C ;;
		esac
		blogs archive "$STREAM" "$PRODUCER" -dir "$DEMO_DIR/archive-$name" -host "$host" >"$dir/archive-$name.out" 2>&1 ||
			fail "archive of $host exited $? (see $dir/archive-$name.out)"
		roots[archive $name]=$(interval_root "$DEMO_DIR/archive-$name" "$STREAM" "$PRODUCER" "$k")
		say "archive $name: $(grep '^latest interval' "$dir/archive-$name.out")"
	done

	# 7. One root.
	want=${roots[verify $ARCHIVE_A]}
	[ -n "$want" ] || fail "no root from verify-interval at $ARCHIVE_A"
	[ ${#roots[@]} -eq 6 ] || fail "interval $k: ${#roots[@]} roots collected, want 6 (three audits, three archives)"
	for name in "${!roots[@]}"; do
		root=${roots[$name]}
		[ "$root" = "$want" ] || fail "interval $k: $name has root ${root:-none}, $ARCHIVE_A's audit has $want"
	done
	say "interval $k root $want: verify-interval at A, B and C and archives a, b and c agree"
	say "PASS run $id"
}

"$mode"
