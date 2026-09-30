package reader

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/lightwebinc/blogs/internal/logrec"
)

func TestFetchHonoursTheAllowListAndTheOrigin(t *testing.T) {
	content := []byte("x\ny\n")
	d := sha256.Sum256(content)
	name := hex.EncodeToString(d[:])
	other := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(content) }))
	defer other.Close()
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/c/" + name:
			_, _ = w.Write(content)
		case "/moved/" + name:
			http.Redirect(w, r, "/c/"+name, http.StatusFound)
		case "/away/" + name:
			http.Redirect(w, r, other.URL+"/c/"+name, http.StatusFound)
		case "/wrong/" + name:
			_, _ = w.Write([]byte("z\nz\n"))
		}
	}))
	defer srv.Close()
	loc := func(path string) *logrec.Locator {
		return &logrec.Locator{Digest: d, Length: uint64(len(content)), URLs: []string{srv.URL + path + name}}
	}
	ctx := context.Background()
	f := Fetch{Allow: []string{"127.0.0.1"}, HTTP: srv.Client()}
	for path, ok := range map[string]bool{"/c/": true, "/moved/": true, "/away/": false, "/wrong/": false} {
		got, err := f.Get(ctx, loc(path))
		if ok != (err == nil) || (ok && string(got) != string(content)) {
			t.Errorf("%s: %q %v", path, got, err)
		}
		if !ok && !errors.Is(err, ErrPending) {
			t.Errorf("%s: %v is not pending", path, err)
		}
	}
	// Not on the list: never fetched.
	if _, err := (Fetch{Allow: []string{"logs.example.com"}, HTTP: srv.Client()}).Get(ctx, loc("/c/")); !errors.Is(err, ErrPending) {
		t.Errorf("fetched from a host not allowed: %v", err)
	}
	// A local directory, checked like any other source.
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, name), content, 0o600); err != nil {
		t.Fatal(err)
	}
	if got, err := (Fetch{Dirs: []string{dir}}).Get(ctx, loc("/none/")); err != nil || string(got) != string(content) {
		t.Errorf("from a directory: %q %v", got, err)
	}
}

func cand(seq uint64, c, prev byte) *Candidate {
	b := &logrec.Batch{Seq: seq}
	b.Prev[0] = prev
	k := &logrec.Carrier{Batch: b}
	k.Commitment[0] = c
	return &Candidate{Carrier: k}
}

func TestTheNextBatchChoosesAForkCandidate(t *testing.T) {
	a, b := cand(2, 0xa, 1), cand(2, 0xb, 1)
	next := cand(3, 0xc, 0xb)
	if got, ok := Link([]*Candidate{a, b}, next); !ok || got != b {
		t.Fatal("next's prev did not choose")
	}
	if _, ok := Link([]*Candidate{a, b}, nil); ok {
		t.Fatal("a fork chosen with nothing to choose by")
	}
	if got, ok := Link([]*Candidate{a}, nil); !ok || got != a {
		t.Fatal("a single candidate not chosen")
	}
	if !Linked(b, next) || Linked(a, next) {
		t.Fatal("Linked")
	}
	// choose tries each candidate at the last seq and keeps the one whose
	// chain reproduces the root.
	first := cand(1, 0x1, 0)
	an := &logrec.Anchor{First: 1, Count: 2, Root: logrec.IntervalRoot([][32]byte{first.Carrier.Commitment, b.Carrier.Commitment})}
	held := map[uint64][]*Candidate{1: {first}, 2: {a, b}}
	got, holes, ok := choose(held, an)
	if !ok || got[1] != b || len(holes) != 0 {
		t.Fatal("choose missed the candidate the root names")
	}
	an.Root[0] ^= 1
	if _, _, ok := choose(held, an); ok {
		t.Fatal("choose matched a root nothing makes")
	}
}

// A seq no host serves takes its commitment from the next batch's prev, as
// an archive proves across a retraction; the last seq, or two holes in a
// row, cannot be recovered.
func TestChooseRecoversAHoleFromTheNextPrev(t *testing.T) {
	one, three, four := cand(1, 0x1, 0), cand(3, 0x3, 0x2), cand(4, 0x4, 0x3)
	var two [32]byte
	two[0] = 0x2
	an := &logrec.Anchor{First: 1, Count: 4, Root: logrec.IntervalRoot([][32]byte{one.Carrier.Commitment, two, three.Carrier.Commitment, four.Carrier.Commitment})}
	got, holes, ok := choose(map[uint64][]*Candidate{1: {one}, 3: {three}, 4: {four}}, an)
	if !ok || len(holes) != 1 || holes[0] != 2 || got[1] != nil || got[0] != one {
		t.Fatalf("hole at 2: ok %v holes %v", ok, holes)
	}
	// Before a hole, a fork cannot be chosen: nothing names a candidate.
	fork := cand(1, 0x9, 0)
	if _, _, ok := choose(map[uint64][]*Candidate{1: {one, fork}, 3: {three}, 4: {four}}, an); ok {
		t.Fatal("a fork before a hole was chosen")
	}
	if _, _, ok := choose(map[uint64][]*Candidate{1: {one}, 4: {four}}, an); ok {
		t.Fatal("two holes in a row were recovered")
	}
	if _, _, ok := choose(map[uint64][]*Candidate{1: {one}, 3: {three}}, an); ok {
		t.Fatal("a hole at the last seq was recovered")
	}
}
