package main

import (
	"fmt"
	"strings"
	"testing"
)

// The limits of docs/limits.md: a hard cap is a usage error naming the
// limit, a recommendation a warning.
func TestLimitsAreEnforced(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	stream, producer := h.producer()
	for _, tc := range []struct {
		flags []string
		want  string
	}{
		{[]string{"-max-lines", "65537"}, "-max-lines 65537 is over the limit of 65536"},
		{[]string{"-max-bytes", "262145"}, "-max-bytes 262145 is over the limit of 262144"},
		{[]string{"-max-wait", "1ms"}, "-max-wait 1ms is under the limit"},
		{[]string{"-interval", "30s"}, "-interval 30s is under the limit"},
		{[]string{"-anchor-batches", "65537"}, "-anchor-batches 65537 is over the limit"},
		{[]string{"-max-rate", "51"}, "-max-rate 51 is over the limit of 50"},
		{[]string{"-tree-count", "1001"}, "-tree-count 1001 is over the limit of 1000"},
		{[]string{"-anchor-tree-count", "1001"}, "-anchor-tree-count 1001 is over the limit of 1000"},
	} {
		h.want(h.run("x\n", append([]string{"produce", stream}, tc.flags...)...), exitUsage, tc.want)
	}
	h.want(h.run("", "read", stream, producer, "-from", "1", "-to", "65537"), exitUsage, "over the limit of 65536 per read")
	h.want(h.run("", "-timeout", "100ms", "doctor"), exitUsage, "outside the limits")

	// On the plane, a batch far over 64 packets is published with a
	// warning, before the run and when the carrier is built.
	big := strings.Repeat(strings.Repeat("y", 1000)+"\n", 120)
	r := h.produce(big, stream, "-max-bytes", "200000")
	if !strings.Contains(r.stderr, "warning: -max-bytes 200000 on the plane") || !strings.Contains(r.stderr, "packets on the plane") {
		t.Fatalf("plane size warnings:\n%s", r.stderr)
	}
	// The same in unicast mode is the default.
	h.env["BLOGS_MODE"], h.env["BLOGS_FACADE"] = "unicast", ""
	r = h.produce(big, stream, "-max-bytes", "200000")
	if strings.Contains(r.stderr, "warning:") {
		t.Fatalf("unicast warned:\n%s", r.stderr)
	}
}

func TestAHomeHoldsAtMost64Streams(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.must("", "init")
	for i := range 64 {
		h.must("", "stream", "new", fmt.Sprintf("s%c", 'a'+i%26))
	}
	h.want(h.run("", "stream", "new", "one"), exitUsage, "holds 64 streams, the limit")
}
