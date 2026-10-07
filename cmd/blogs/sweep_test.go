package main

import (
	"net/http/httptest"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/lightwebinc/blogs/internal/state"
	"github.com/lightwebinc/blogs/internal/testchain"
)

func TestSweepRetractsNamedBatchesAndLeavesTheirPositions(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	stream, producer := h.producer()
	h.produce(linesOf(0, 4), stream, "-max-lines", "1")
	// Without -yes nothing changes.
	r := h.must("", "sweep", stream, "-retract-seqs", "2")
	if !strings.Contains(r.stdout, "retract seqs 2: tree ") || !strings.Contains(r.stdout, "nothing done") {
		t.Fatalf("dry run:\n%s\n%s", r.stdout, r.stderr)
	}
	if st, _ := state.Load(h.home, stream); len(st.Sweeps) != 0 {
		t.Fatal("a dry run recorded a sweep")
	}
	r = h.must("", append([]string{"sweep", stream, "-retract-seqs", "2", "-yes"}, fast...)...)
	if !strings.Contains(r.stderr, "seqs 2 retracted") || h.host.Retracted != 1 {
		t.Fatalf("retract:\n%s\n%s", r.stdout, r.stderr)
	}
	st, _ := state.Load(h.home, stream)
	if len(st.Sweeps) != 1 || !st.Sweeps[0].Done || st.Sweeps[0].Kind != state.SweepRetract || st.Sweeps[0].Height == 0 {
		t.Fatalf("recorded sweeps: %+v", st.Sweeps)
	}
	// The seq is a hole the anchor still commits: a reader finds it missing,
	// and the audit proves the interval across it from seq 3's prev.
	h.want(h.run("", "read", stream, producer, "-to", "4"), exitIncomplete, "seq 2: MISSING")
	if v := h.must("", "verify-interval", stream, producer, "1"); !strings.Contains(v.stdout, "unserved seqs 2: no host serves them (retracted, or not delivered)") || !strings.Contains(v.stdout, "VERIFIED interval 1") {
		t.Fatalf("verify-interval:\n%s", v.stdout)
	}
	// A sibling filled from this host: seq 2 is served by neither, and
	// seq 3's prev names it, so the backfill counts it unserved.
	sibling := httptest.NewServer(testchain.NewHost(h.chain))
	t.Cleanup(sibling.Close)
	if b := h.must("", "backfill", stream, producer, "-from", h.hostS.URL, "-to", sibling.URL, "-interval", "1"); !strings.Contains(b.stdout, "0 missing, 0 refused, 1 unserved") {
		t.Fatalf("backfill across the retraction:\n%s", b.stdout)
	}
	// Retracted already: the hosts no longer hold its carrier.
	h.want(h.run("", append([]string{"sweep", stream, "-retract-seqs", "2", "-yes"}, fast...)...), exitIncomplete, "no host holds its carrier")
	// The stream goes on.
	h.produce("after\n", stream)
	r = h.must("", "read", stream, producer, "-from", "5")
	if r.stdout != "after\n" {
		t.Fatalf("read after the sweep: %q", r.stdout)
	}
	// With no flags: the sweeps recorded.
	r = h.must("", append([]string{"sweep", stream}, fast...)...)
	if !strings.Contains(r.stdout, "retract  tree ") {
		t.Fatalf("list:\n%s", r.stdout)
	}
}

func TestSweepRetractTreeAndRetire(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	stream, producer := h.producer()
	// Two runs, two anchors: the anchor fee tree runs low and the next is
	// minted ahead, and two batch trees are used.
	h.produce(linesOf(0, 5), stream, "-max-lines", "1")
	h.produce(linesOf(10, 2), stream, "-max-lines", "1")
	st, _ := state.Load(h.home, stream)
	if len(st.Trees) < 2 {
		t.Fatalf("%d batch trees", len(st.Trees))
	}
	first := st.Trees[0].Txid
	r := h.must("", append([]string{"sweep", stream, "-retract-tree", first, "-yes"}, fast...)...)
	if !strings.Contains(r.stderr, "every batch funded by outputs 0 to 3 of tree "+first+" is retracted") || h.host.Retracted != 4 {
		t.Fatalf("retract tree:\n%s\n%s", r.stdout, r.stderr)
	}
	h.want(h.run("", "read", stream, producer, "-to", "2"), exitIncomplete, "seq 1: MISSING")
	h.want(h.run("", append([]string{"sweep", stream, "-retract-tree", strings.Repeat("0", 64)}, fast...)...), exitUsage, "is not a batch funding tree")

	r = h.must("", append([]string{"sweep", stream, "-retire"}, fast...)...)
	if !strings.Contains(r.stdout, "nothing done") {
		t.Fatalf("retire dry run:\n%s", r.stdout)
	}
	pool := func() string {
		out := h.must("", "doctor").stdout
		for _, l := range strings.Split(out, "\n") {
			if strings.HasPrefix(l, "pool ") {
				return l
			}
		}
		return ""
	}
	before := pool()
	r = h.must("", append([]string{"sweep", stream, "-retire", "-yes"}, fast...)...)
	if !strings.Contains(r.stderr, "retired to the pool") {
		t.Fatalf("retire:\n%s\n%s", r.stdout, r.stderr)
	}
	if !strings.Contains(r.stdout, "left: the unused outputs cannot pay the fee") {
		t.Fatalf("a one-satoshi batch tree was not left:\n%s", r.stdout)
	}
	if after := pool(); after == before {
		t.Fatalf("the pool did not change: %s", after)
	}
	// A retired tree minted ahead is no longer one to switch to: the
	// record that would have the next run hold it is gone with its outputs.
	st, _ = state.Load(h.home, stream)
	retired := 0
	for _, a := range st.Ahead {
		if a.Remaining() == 0 {
			retired++
			if st.PendingTree(a.Txid) != nil {
				t.Fatalf("the retired tree %s minted ahead kept its record", a.Txid)
			}
		}
	}
	if retired == 0 {
		t.Fatalf("no tree minted ahead was retired: %+v", st.Ahead)
	}
	// Retired trees have no unused output left; a second retire finds only
	// what cannot pay its fee.
	r = h.must("", append([]string{"sweep", stream, "-retire", "-yes"}, fast...)...)
	if strings.Contains(r.stderr, "retired to the pool") {
		t.Fatalf("retired twice:\n%s\n%s", r.stdout, r.stderr)
	}
	h.produce("still going\n", stream)
}

// On the plane a sweep goes to the facade, which the plane carries to its
// subscribers, and directly to every other configured host, so a host off
// the plane stops serving what it retracts. A host left out of hosts is
// given the sweep later by backfill -sweeps.
func TestPlaneSweepReachesHostsOffThePlane(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	stream, producer := h.producer()
	h.produce(linesOf(0, 4), stream, "-max-lines", "1")
	off := testchain.NewHost(h.chain)
	offS := httptest.NewServer(off)
	t.Cleanup(offS.Close)
	h.must("", "backfill", stream, producer, "-from", h.hostS.URL, "-to", offS.URL, "-interval", "1")

	// Off the plane and in hosts: the sweep reaches it directly.
	h.env["BLOGS_HOSTS"] = h.hostS.URL + "," + offS.URL
	r := h.must("", append([]string{"sweep", stream, "-retract-seqs", "2", "-yes"}, fast...)...)
	if !strings.Contains(r.stderr, "seqs 2 retracted") || h.host.Retracted != 1 || off.Retracted != 1 {
		t.Fatalf("sweep: facade host %d, off-plane host %d\n%s\n%s", h.host.Retracted, off.Retracted, r.stdout, r.stderr)
	}

	// Off the plane and not in hosts: the facade alone takes it, and the
	// run says so.
	h.env["BLOGS_HOSTS"] = h.hostS.URL
	r = h.must("", append([]string{"sweep", stream, "-retract-seqs", "3", "-yes"}, fast...)...)
	if !strings.Contains(r.stderr, "goes to the facade only") || off.Retracted != 1 {
		t.Fatalf("facade only: off-plane host %d\n%s", off.Retracted, r.stderr)
	}
	// A backfill without -sweeps says it carries none; with it, the sweep
	// is found from the node and submitted.
	b := h.must("", "backfill", stream, producer, "-from", h.hostS.URL, "-to", offS.URL, "-seqs", "3-3")
	if !strings.Contains(b.stderr, "not sweeps") || !strings.Contains(b.stdout, "0 sweep(s) carried") || off.Retracted != 1 {
		t.Fatalf("backfill without -sweeps:\n%s\n%s", b.stdout, b.stderr)
	}
	b = h.must("", "backfill", stream, producer, "-from", h.hostS.URL, "-to", offS.URL, "-seqs", "1-4", "-sweeps")
	if !strings.Contains(b.stdout, "1 sweep(s) carried") || !strings.Contains(b.stdout, "seq 3: carrier ") || off.Retracted != 2 {
		t.Fatalf("backfill -sweeps: off-plane host %d\n%s\n%s", off.Retracted, b.stdout, b.stderr)
	}
	h.env["BLOGS_HOSTS"] = offS.URL
	h.want(h.run("", "read", stream, producer, "-from", "3", "-to", "3"), exitIncomplete, "seq 3: MISSING")
	// -sweeps needs a source that names a spend.
	delete(h.env, "BLOGS_ASSET")
	h.want(h.run("", "backfill", stream, producer, "-from", h.hostS.URL, "-to", offS.URL, "-seqs", "1-4", "-sweeps"), exitUsage, "-sweeps needs chain")
	h.env["BLOGS_ASSET"] = h.chainS.URL

	// A host named but down: the sweep is mined and published to the
	// facade, the run stops, and the next sweep run finishes it.
	h.env["BLOGS_HOSTS"] = h.hostS.URL + "," + offS.URL
	off.SetDown(true)
	h.want(h.run("", append([]string{"sweep", stream, "-retract-seqs", "4", "-yes"}, fast...)...), exitUsage, "not to every host directly")
	off.SetDown(false)
	r = h.must("", append([]string{"sweep", stream}, fast...)...)
	if !strings.Contains(r.stderr, "seqs 4 retracted") || off.Retracted != 3 {
		t.Fatalf("finish: off-plane host %d\n%s", off.Retracted, r.stderr)
	}
}

// A sweep the network refuses for good, because the node names another
// transaction as the spender of one of its inputs, is marked failed and is
// not in flight any more: a later sweep of the stream goes on. Its fee coin
// is unspent, so it goes back to the pool.
func TestSweepRefusedForGoodDoesNotBlockTheNext(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	stream, _ := h.producer()
	h.produce(linesOf(0, 4), stream, "-max-lines", "1")
	plan := regexp.MustCompile(`retract seqs 2: tree ([0-9a-f]{64}) outputs (\d+)`).FindStringSubmatch(h.must("", "sweep", stream, "-retract-seqs", "2").stdout)
	if plan == nil {
		t.Fatal("no plan for seq 2")
	}
	vout, _ := strconv.ParseUint(plan[2], 10, 32)
	other := strings.Repeat("ab", 32)
	h.chain.SpendElsewhere(plan[1], uint32(vout), other)

	first := h.run("", append([]string{"sweep", stream, "-retract-seqs", "2", "-yes"}, fast...)...)
	if first.code == 0 || h.host.Retracted != 0 {
		t.Fatalf("a sweep whose input is spent elsewhere retracted:\n%s\n%s", first.stdout, first.stderr)
	}

	// The later sweep is not blocked by the one refused.
	r := h.run("", append([]string{"sweep", stream, "-retract-seqs", "3", "-yes"}, fast...)...)
	if r.code != 0 || !strings.Contains(r.stderr, "seqs 3 retracted") || h.host.Retracted != 1 {
		t.Fatalf("the sweep after one refused for good: exit %d\n%s\n%s", r.code, r.stdout, r.stderr)
	}

	// How the refusal was worded, and what became of the fee coin.
	h.want(first, exitRefused, "refused by the network")
	if !strings.Contains(first.stderr, "is spent by "+other) || !strings.Contains(first.stderr, "is back in the pool") {
		t.Fatalf("refused sweep:\n%s\n%s", first.stdout, first.stderr)
	}
	st, _ := state.Load(h.home, stream)
	if len(st.Sweeps) != 2 || st.Sweeps[0].Done || !st.Sweeps[1].Done {
		t.Fatalf("recorded sweeps: %+v", st.Sweeps)
	}
	d := h.must("", "doctor").stdout
	if !strings.Contains(d, "FAILED") || strings.Contains(d, "in flight") {
		t.Fatalf("doctor:\n%s", d)
	}
	h.produce("still going\n", stream)
}

// Only the node naming another spender gives a sweep up. One that has not
// mined while its inputs are unspent stays in flight and is finished by the
// next sweep; one whose fee coin is spent elsewhere is given up, and that
// coin is not given back.
func TestSweepGivenUpOnlyForAnotherSpender(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	stream, _ := h.producer()
	h.produce(linesOf(0, 4), stream, "-max-lines", "1")

	h.chain.SetHold(true)
	r := h.run("", append([]string{"sweep", stream, "-retract-seqs", "2", "-yes", "-wait", "300ms"}, fast...)...)
	h.want(r, exitUsage, "the next sweep resumes it")
	if sw := h.state(stream).Sweeps; len(sw) != 1 || !sw[0].InFlight() {
		t.Fatalf("recorded sweeps: %+v", sw)
	}
	if d := h.must("", "doctor").stdout; !strings.Contains(d, "in flight") || strings.Contains(d, "FAILED") {
		t.Fatalf("doctor:\n%s", d)
	}
	h.chain.SetHold(false)
	h.chain.Mine()
	r = h.must("", append([]string{"sweep", stream}, fast...)...)
	if !strings.Contains(r.stderr, "seqs 2 retracted") || h.host.Retracted != 1 {
		t.Fatalf("finish:\n%s\n%s", r.stdout, r.stderr)
	}

	other := strings.Repeat("cd", 32)
	for op := range h.poolCoins() {
		txid, vout, _ := strings.Cut(op, ".")
		n, _ := strconv.ParseUint(vout, 10, 32)
		h.chain.SpendElsewhere(txid, uint32(n), other)
	}
	r = h.run("", append([]string{"sweep", stream, "-retract-seqs", "3", "-yes"}, fast...)...)
	h.want(r, exitRefused, "refused by the network")
	if !strings.Contains(r.stderr, "is spent by "+other+" and was not given back") || h.host.Retracted != 1 {
		t.Fatalf("refused sweep:\n%s\n%s", r.stdout, r.stderr)
	}
	sw := h.state(stream).Sweeps
	if len(sw) != 2 || sw[1].Failed == "" || sw[1].Fee == nil || h.poolCoins()[sw[1].Fee.Outpoint()] {
		t.Fatalf("recorded sweeps: %+v", sw)
	}
	if l := h.must("", append([]string{"sweep", stream}, fast...)...).stdout; !strings.Contains(l, "FAILED: input") {
		t.Fatalf("list:\n%s", l)
	}
}

// An anchor the settlement leg refuses because the node names another
// spender of its fee output is given up and built again from the next
// output, as one refused after it was accepted is; it does not stay in
// flight to be sent again until the wait runs out.
func TestAnchorRefusedBeforeAcceptanceIsBuiltAgain(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	stream, producer := h.producer()
	h.produce(linesOf(0, 2), stream, "-max-lines", "1")
	at := h.state(stream).AnchorTree
	other := strings.Repeat("ef", 32)
	h.chain.SpendElsewhere(at.Txid, at.Next, other)
	h.produce(linesOf(2, 2), stream, "-max-lines", "1", "-anchor-wait", "0")
	r := h.run("", append([]string{"anchor", stream, "-wait", "5s"}, fast...)...)
	if r.code != 0 || !strings.Contains(r.stderr, "is spent by "+other) || !strings.Contains(r.stderr, "seqs 3 to 4 anchored") {
		t.Fatalf("anchor: exit %d\n%s\n%s", r.code, r.stdout, r.stderr)
	}
	if v := h.must("", "verify-interval", stream, producer, "2"); !strings.Contains(v.stdout, "VERIFIED interval 2") {
		t.Fatalf("verify-interval:\n%s", v.stdout)
	}
}

// An anchor whose predecessor token another transaction spends can never
// be followed: it is not rebuilt, which would only spend fee outputs, and
// stays in flight with a warning while batches go on.
func TestAnchorWithPredecessorSpentElsewhereStaysInFlight(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	stream, _ := h.producer()
	h.produce(linesOf(0, 2), stream, "-max-lines", "1")
	st := h.state(stream)
	other := strings.Repeat("ef", 32)
	h.chain.SpendElsewhere(st.Anchor.Txid, st.Anchor.Vout, other)
	next := st.AnchorTree.Next
	h.produce(linesOf(2, 2), stream, "-max-lines", "1", "-anchor-wait", "0")
	r := h.run("", append([]string{"anchor", stream, "-wait", "3s"}, fast...)...)
	if r.code == 0 || !strings.Contains(r.stderr, "is spent by "+other+", not by this stream's next anchor") {
		t.Fatalf("anchor: exit %d\n%s\n%s", r.code, r.stdout, r.stderr)
	}
	st = h.state(stream)
	if st.Inflight == nil || st.AnchorTree.Next != next+1 {
		t.Fatalf("in flight %+v, anchor tree next %d (was %d)", st.Inflight, st.AnchorTree.Next, next)
	}
	h.produce("still going\n", stream, "-anchor-wait", "0")
}
