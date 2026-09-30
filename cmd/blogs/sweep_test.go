package main

import (
	"net/http/httptest"
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
	h.want(h.run("", "backfill", stream, producer, "-from", h.hostS.URL, "-to", offS.URL, "-seqs", "1-4", "-sweeps"), exitUsage, "-sweeps needs asset or tx_source")
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
