package main

import (
	"encoding/hex"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"github.com/lightwebinc/blogs/internal/state"
	"github.com/lightwebinc/blogs/internal/testchain"
)

// unicast turns the harness into a unicast producer with two hosts: the
// harness's own and a second, returned.
func (h *harness) unicast() (*testchain.Host, string) {
	h.t.Helper()
	b := testchain.NewHost(h.chain)
	bs := httptest.NewServer(b)
	h.t.Cleanup(bs.Close)
	delete(h.env, "BLOGS_FACADE")
	h.env["BLOGS_MODE"] = "unicast"
	h.env["BLOGS_HOSTS"] = h.hostS.URL + "," + bs.URL
	return b, bs.URL
}

var repairLine = regexp.MustCompile(`(?m)copy them from the others: blogs (backfill .*)$`)

func TestUnicastPublishesToEveryHost(t *testing.T) {
	h := newHarness(t)
	stream, producer := h.producer()
	p, _ := hex.DecodeString(producer)
	b, bURL := h.unicast()

	r := h.produce(linesOf(0, 6), stream, "-max-lines", "2")
	if !strings.Contains(r.stdout, "published seq 1 to 3") || !strings.Contains(r.stdout, "anchored to seq 3: interval 1") {
		t.Fatalf("produce:\n%s\n%s", r.stdout, r.stderr)
	}
	for _, host := range []string{h.hostS.URL, bURL} {
		if !regexp.MustCompile(`host ` + regexp.QuoteMeta(host) + `: took \d+ object\(s\), missed 0`).MatchString(r.stdout) {
			t.Fatalf("no clean tally for %s:\n%s", host, r.stdout)
		}
	}
	for seq := uint64(1); seq <= 3; seq++ {
		if !h.host.Holds(p, stream, seq) || !b.Holds(p, stream, seq) {
			t.Fatalf("seq %d is not on both hosts", seq)
		}
	}
	// Each host alone proves the interval.
	for _, host := range []string{h.hostS.URL, bURL} {
		r = h.must("", "-hosts", host, "verify-interval", stream, producer, "1")
		if !strings.Contains(r.stdout, "VERIFIED interval 1") {
			t.Fatalf("verify at %s:\n%s", host, r.stdout)
		}
	}
	r = h.must("", "doctor")
	if !strings.Contains(r.stdout, "mode        unicast: every object to each of 2 host(s), published once 2 take it (quorum all)") {
		t.Fatalf("doctor:\n%s", r.stdout)
	}
}

func TestUnicastQuorumAndRepair(t *testing.T) {
	h := newHarness(t)
	stream, producer := h.producer()
	p, _ := hex.DecodeString(producer)
	b, bURL := h.unicast()
	defer retriesForTest()()
	h.produce("a\nb\n", stream)

	// Host b is down. With the default quorum, all, the carrier is not
	// published: the run stops with it persisted, naming the host.
	b.SetDown(true)
	r := h.run("c\n", append([]string{"produce", stream}, fast...)...)
	h.want(r, exitUsage, "the carrier is persisted")
	if !strings.Contains(r.stderr, "seq 2: host "+bURL+" MISSED it after 2 tries") || !strings.Contains(r.stderr, "1 of 2 host(s) took it and the quorum is 2") {
		t.Fatalf("stderr:\n%s", r.stderr)
	}
	if st, _ := state.Load(h.home, stream); st.Seq != 2 || st.Pending == "" {
		t.Fatalf("seq %d pending %t", st.Seq, st.Pending != "")
	}

	// With quorum one, the persisted carrier and the next are published to
	// the host that is up, and the run says what b missed and how to
	// repair it.
	r = h.must("d\ne\n", append([]string{"-quorum", "one", "produce", stream, "-max-lines", "1"}, fast...)...)
	if !strings.Contains(r.stderr, "seq 2: publishing the carrier a previous run persisted") || !strings.Contains(r.stdout, "anchored to seq 4: interval 2") {
		t.Fatalf("quorum one:\n%s\n%s", r.stdout, r.stderr)
	}
	if !strings.Contains(r.stdout, "host "+bURL+": took 0 object(s), missed") || !strings.Contains(r.stdout, "seq 2, seq 3, seq 4") {
		t.Fatalf("tally:\n%s", r.stdout)
	}
	m := repairLine.FindStringSubmatch(r.stdout)
	if m == nil || !strings.Contains(m[1], "backfill "+stream+" "+producer+" -seqs 2-4 -anchors 2") {
		t.Fatalf("no repair command:\n%s", r.stdout)
	}
	if b.Holds(p, stream, 2) || !h.host.Holds(p, stream, 4) {
		t.Fatal("the hosts do not hold what the tally says")
	}

	// b is back, without what it missed. The printed backfill, with no
	// -from or -to, brings the configured hosts into step, and b alone then
	// proves interval 2.
	b.SetDown(false)
	h.want(h.run("", "-hosts", bURL, "verify-interval", stream, producer, "2"), exitIncomplete, "INCOMPLETE")
	r = h.must("", strings.Fields(m[1])...)
	if !strings.Contains(r.stdout, "backfill "+stream+" to "+bURL+": ") || !strings.Contains(r.stdout, "0 missing, 0 refused") ||
		!strings.Contains(r.stdout, "seq 3: filled from the sibling "+h.hostS.URL) {
		t.Fatalf("repair:\n%s", r.stdout)
	}
	r = h.must("", "-hosts", bURL, "verify-interval", stream, producer, "2")
	if !strings.Contains(r.stdout, "VERIFIED interval 2") {
		t.Fatalf("verify at b:\n%s", r.stdout)
	}
	r = h.must("", "-hosts", bURL, "read", stream, producer)
	if r.stdout != "a\nb\nc\nd\ne\n" {
		t.Fatalf("read at b: %q", r.stdout)
	}
	// A second repair finds every position held.
	r = h.must("", strings.Fields(m[1])...)
	if strings.Contains(r.stdout, "filled") && !strings.Contains(r.stdout, "0 filled") {
		t.Fatalf("second repair filled something:\n%s", r.stdout)
	}

	// A retraction must reach every host: with b down the sweep is not
	// done, whatever the quorum, and the next sweep finishes it.
	b.SetDown(true)
	r = h.run("", append([]string{"-quorum", "one", "sweep", stream, "-retract-seqs", "3", "-yes"}, fast...)...)
	h.want(r, exitUsage, "the next sweep publishes it to every host")
	b.SetDown(false)
	r = h.must("", append([]string{"sweep", stream}, fast...)...)
	if !strings.Contains(r.stderr, "seqs 3 retracted") {
		t.Fatalf("sweep resumed:\n%s\n%s", r.stdout, r.stderr)
	}
	if h.host.Holds(p, stream, 3) || b.Holds(p, stream, 3) {
		t.Fatal("seq 3 is still served after the retraction")
	}
}

func TestUnicastConfigurationIsChecked(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	stream, _ := h.producer()
	h.env["BLOGS_MODE"] = "unicast"
	h.want(h.run("a\n", "produce", stream), exitUsage, "facade is for mode plane")
	delete(h.env, "BLOGS_FACADE")
	h.want(h.run("a\n", "-quorum", "2", "produce", stream), exitUsage, "quorum 2 is more than the 1 configured host(s)")
	delete(h.env, "BLOGS_HOSTS")
	h.want(h.run("a\n", "produce", stream), exitUsage, "hosts must be configured")
	h.want(h.run("", "-mode", "broadcast", "doctor"), exitUsage, "mode must be plane or unicast")
	h.want(h.run("", "-quorum", "most", "doctor"), exitUsage, "quorum \"most\" is not all, majority, one or a positive number")
	delete(h.env, "BLOGS_MODE")
	h.want(h.run("a\n", "produce", stream), exitUsage, "facade must be configured")
}
