package gateway

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/lightwebinc/blogs/internal/archive"
	"github.com/lightwebinc/blogs/internal/logrec"
)

const (
	tStream   = "app_abcdefghij"
	tProducer = "0324653eac434488002cc06bbfb7f10fe18991e35f9fe4302dbea6d2353dc0ab1c"
)

// interval builds a verifying interval file over commitments.
func interval(t *testing.T, k, first uint64, cs [][32]byte, token string) *archive.IntervalFile {
	t.Helper()
	an := &logrec.Anchor{Interval: k, First: first, Count: uint64(len(cs)), Root: logrec.IntervalRoot(cs)}
	f := &archive.IntervalFile{Interval: k, First: first, Count: an.Count, Root: hex.EncodeToString(an.Root[:]), Token: token, Status: "proven"}
	for i := range cs {
		seq := first + uint64(i)
		path, err := logrec.ProveBatch(an, cs, seq)
		if err != nil {
			t.Fatal(err)
		}
		l := archive.Leaf{Seq: seq, Commitment: chainhash.Hash(cs[i]).String(), Held: true}
		for _, st := range path {
			h := hex.EncodeToString(st.Hash[:])
			if st.Left {
				h = "L:" + h
			}
			l.Path = append(l.Path, h)
		}
		f.Leaves = append(f.Leaves, l)
	}
	if err := f.Verify(); err != nil {
		t.Fatal(err)
	}
	return f
}

func commitments(tag string, n int) [][32]byte {
	out := make([][32]byte, n)
	for i := range out {
		out[i] = sha256.Sum256([]byte(tag + string(rune('a'+i))))
	}
	return out
}

func writeInterval(t *testing.T, dir string, f *archive.IntervalFile) {
	t.Helper()
	d := filepath.Join(dir, tStream, tProducer, "intervals")
	if err := os.MkdirAll(d, 0o755); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(f)
	if err := os.WriteFile(filepath.Join(d, pad(f.Interval)+".json"), raw, 0o644); err != nil {
		t.Fatal(err)
	}
}

// The second archive gates verified: agreement passes; a missing interval
// is pending; a file that does not verify, or one that verifies but
// commits other batches or under another token, is unreadable. Coverage
// says which.
func TestCrossArchive(t *testing.T) {
	cs1, cs2, cs3, cs4 := commitments("one", 4), commitments("two", 3), commitments("three", 2), commitments("four", 2)
	a, b := t.TempDir(), t.TempDir()
	tok := strings.Repeat("ab", 32)
	i1, i2, i3, i4 := interval(t, 1, 1, cs1, tok), interval(t, 2, 5, cs2, tok), interval(t, 3, 8, cs3, tok), interval(t, 4, 10, cs4, tok)
	for _, f := range []*archive.IntervalFile{i1, i2, i3, i4} {
		writeInterval(t, a, f)
	}
	// b: interval 1 agrees; 2 commits other batches (and so another
	// root); 3 is the same but under another token; 4 is missing.
	writeInterval(t, b, i1)
	writeInterval(t, b, interval(t, 2, 5, commitments("other", 3), tok))
	writeInterval(t, b, interval(t, 3, 8, cs3, strings.Repeat("cd", 32)))
	v := &Verifier{Dir: a, Cross: b}
	for _, c := range []struct {
		seq  uint64
		want string
	}{{1, ""}, {4, ""}, {5, ReasonUnreadable}, {7, ReasonUnreadable}, {8, ReasonUnreadable}, {10, ReasonPending}, {11, ReasonPending}} {
		f := v.list(a, tStream, tProducer, c.seq).covering(c.seq)
		if got := v.crossDisagrees(f, tStream, tProducer, c.seq); got != c.want {
			t.Fatalf("seq %d: %q, want %q", c.seq, got, c.want)
		}
		out := v.crossCheck(Verdict{State: Verified, Interval: f.Interval}, f, tStream, tProducer, c.seq)
		if (c.want == "") != (out.State == Verified) || out.Reason != c.want {
			t.Fatalf("seq %d: %+v", c.seq, out)
		}
	}
	// One step of interval 1's path changed at b: that file no longer
	// verifies, so its lines are unreadable, not pending.
	bad := *i1
	bad.Leaves = append([]archive.Leaf(nil), i1.Leaves...)
	bad.Leaves[0].Path = append([]string{strings.Repeat("00", 32)}, bad.Leaves[0].Path[1:]...)
	writeInterval(t, b, &bad)
	v = &Verifier{Dir: a, Cross: b}
	if got := v.crossDisagrees(i1, tStream, tProducer, 2); got != ReasonUnreadable {
		t.Fatalf("a file that does not verify: %q", got)
	}
	cov, err := v.Coverage()
	if err != nil || len(cov) != 1 || cov[0].Cross == nil {
		t.Fatalf("coverage %+v %v", cov, err)
	}
	x := cov[0].Cross
	if x.Agree != 0 || len(x.Missing) != 1 || x.Missing[0] != 4 || len(x.Disagree) != 3 ||
		!strings.Contains(x.Disagree["1"], "path") || !strings.Contains(x.Disagree["2"], "root") || !strings.Contains(x.Disagree["3"], "token") {
		t.Fatalf("cross coverage %+v", x)
	}
	// With no second archive nothing changes.
	v = &Verifier{Dir: a}
	if out := v.crossCheck(Verdict{State: Verified}, i4, tStream, tProducer, 10); out.State != Verified {
		t.Fatalf("no cross: %+v", out)
	}
	if cov, _ := v.Coverage(); cov[0].Cross != nil {
		t.Fatal("coverage names a second archive it does not have")
	}
}

// Files that verify on their own can still disagree on the path at a seq
// while agreeing on the root: the leaf there commits another batch.
func TestDisagree(t *testing.T) {
	cs := commitments("x", 3)
	f := interval(t, 1, 1, cs, "t")
	g := interval(t, 1, 1, cs, "t")
	for seq := uint64(1); seq <= 3; seq++ {
		if d := disagree(f, g, seq); d != "" {
			t.Fatalf("seq %d: %s", seq, d)
		}
	}
	g.Leaves[1].Path = append([]string(nil), g.Leaves[1].Path...)
	g.Leaves[1].Path[0] = strings.Repeat("11", 32)
	if d := disagree(f, g, 2); !strings.Contains(d, "paths differ") {
		t.Fatalf("path: %q", d)
	}
	g.Leaves[1].Commitment = strings.Repeat("22", 32)
	if d := disagree(f, g, 2); !strings.Contains(d, "commitment") {
		t.Fatalf("commitment: %q", d)
	}
	if d := disagree(f, interval(t, 1, 1, commitments("x", 4), "t"), 1); !strings.Contains(d, "seqs 1 to 4") {
		t.Fatalf("bounds: %q", d)
	}
}
