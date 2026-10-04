package limits

import (
	"strings"
	"testing"
	"time"

	"github.com/lightwebinc/bcommon/mint"
)

func base(plane bool) Producer {
	p := Producer{Plane: plane, MaxLines: DefaultMaxLines, MaxWait: DefaultMaxWait, Interval: DefaultInterval,
		AnchorBatches: DefaultAnchorBatches, Rate: DefaultRate, TreeCount: DefaultTreeCount, AnchorTreeCount: 8, ObjectBound: OpenObjectBound}
	p.MaxBytes = DefaultMaxBytes(plane, false, false)
	if !plane {
		p.Hosts, p.Need = 2, 2
	}
	return p
}

func TestTheDefaultsPass(t *testing.T) {
	for _, plane := range []bool{true, false} {
		if w, err := base(plane).Check(); err != nil || len(w) != 0 {
			t.Errorf("plane %v: %v %v", plane, w, err)
		}
	}
	for _, tc := range []struct {
		plane, digest, gzip bool
		want                int
	}{
		{true, false, false, PlaneMaxBytes}, {true, false, true, PlaneGzipMaxBytes}, {true, true, false, LargeMaxBytes},
		{false, false, false, UnicastMaxBytes}, {false, false, true, LargeMaxBytes}, {false, true, true, LargeMaxBytes},
	} {
		if got := DefaultMaxBytes(tc.plane, tc.digest, tc.gzip); got != tc.want {
			t.Errorf("DefaultMaxBytes(%v, %v, %v) = %d, want %d", tc.plane, tc.digest, tc.gzip, got, tc.want)
		}
	}
}

func TestHardCapsRefuseNamingTheLimit(t *testing.T) {
	for name, change := range map[string]func(*Producer){
		"-max-bytes":         func(p *Producer) { p.MaxBytes = UnicastMaxBytes + 1 },
		"-max-lines":         func(p *Producer) { p.MaxLines = MaxLines + 1 },
		"-max-wait":          func(p *Producer) { p.MaxWait = time.Millisecond },
		"-interval":          func(p *Producer) { p.Interval = 25 * time.Hour },
		"-anchor-batches":    func(p *Producer) { p.AnchorBatches = MaxAnchorBatches + 1 },
		"-max-rate":          func(p *Producer) { p.Rate = MaxRate + 1 },
		"-tree-count":        func(p *Producer) { p.TreeCount = MaxTreeCount + 1 },
		"-anchor-tree-count": func(p *Producer) { p.AnchorTreeCount = MaxTreeCount + 1 },
		"object_bound":       func(p *Producer) { p.ObjectBound = MaxObjectBound + 1 },
		"hosts":              func(p *Producer) { p.Hosts, p.Need = MaxHosts+1, 1 },
	} {
		p := base(false)
		change(&p)
		_, err := p.Check()
		if err == nil || !strings.HasPrefix(err.Error(), name+" ") || !strings.Contains(err.Error(), "limit") {
			t.Errorf("%s: %v", name, err)
		}
	}
	// Gzip and digest-only batches are bounded by the decoded size.
	p := base(false)
	p.Gzip, p.MaxBytes = true, 1<<20
	if _, err := p.Check(); err != nil {
		t.Error(err)
	}
}

// Both funding trees take up to MaxTreeCount outputs, which bcommon mints:
// the bound is under its mint.MaxFundingOutputs, so a tree the limits pass
// never fails at its mint with mint.ErrTreeTooLarge.
func TestTreeCountsAtTheBoundPass(t *testing.T) {
	if MaxTreeCount > mint.MaxFundingOutputs {
		t.Fatalf("MaxTreeCount %d is over mint.MaxFundingOutputs %d", MaxTreeCount, mint.MaxFundingOutputs)
	}
	for _, plane := range []bool{true, false} {
		p := base(plane)
		p.TreeCount, p.AnchorTreeCount = MaxTreeCount, MaxTreeCount
		if _, err := p.Check(); err != nil {
			t.Errorf("plane %v: %v", plane, err)
		}
	}
}

func TestRecommendationsWarn(t *testing.T) {
	p := base(true)
	p.MaxBytes, p.TreeCount, p.ObjectBound = 128<<10, 400, 2<<20
	w, err := p.Check()
	if err != nil || len(w) != 3 {
		t.Fatalf("%v %q", err, w)
	}
	u := base(false)
	u.Hosts, u.Need = 8, 1
	if w, err := u.Check(); err != nil || len(w) != 2 || !strings.Contains(w[0], "8 hosts") || !strings.Contains(w[1], "below a majority") {
		t.Fatalf("%v %q", err, w)
	}
	// The plane's size advice is not the unicast producer's.
	u = base(false)
	u.MaxBytes = UnicastMaxBytes
	if w, _ := u.Check(); len(w) != 0 {
		t.Fatalf("%q", w)
	}
	if Gap(2) != 500*time.Millisecond || Gap(0) != 0 {
		t.Fatal(Gap(2))
	}
}
