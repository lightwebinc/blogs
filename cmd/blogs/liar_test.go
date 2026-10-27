package main

import (
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lightwebinc/blogs/internal/logrec"
	"github.com/lightwebinc/blogs/internal/testchain"
)

// lie answers the classes named with one byte of every answered BEEF
// changed: a host that substitutes bytes for what it holds. What it answers
// no longer verifies as what was asked.
func lie(classes ...string) func(class string, beef []byte) []byte {
	return func(class string, beef []byte) []byte {
		for _, c := range classes {
			if c == class {
				beef[len(beef)*6/10] ^= 1
			}
		}
		return beef
	}
}

// liars is a unicast producer's stream of three batches and one anchor on
// two hosts, a and b.
func liars(t *testing.T) (h *harness, stream, producer string, b *testchain.Host, bURL string) {
	t.Helper()
	h = newHarness(t)
	stream, producer = h.producer()
	b, bURL = h.unicast()
	h.produce(linesOf(0, 6), stream, "-max-lines", "2")
	return h, stream, producer, b, bURL
}

// A host that substitutes bytes in a batch it answers is named, and what it
// answered is not used: beside an honest host the batch is read from that
// one, whichever is asked first, and the read is incomplete (exit 3), never
// refused. When every host does, the seq is not read and is not called
// absent; the header source is named.
func TestReadNamesAHostThatSubstitutesBytes(t *testing.T) {
	t.Parallel()
	h, stream, producer, b, bURL := liars(t)
	a, aURL := h.host, h.hostS.URL
	want := linesOf(0, 6)
	for _, liar := range []struct {
		host        *testchain.Host
		url, honest string
	}{{b, bURL, aURL}, {a, aURL, bURL}} {
		liar.host.SetRewrite(lie(logrec.ClassBatch))
		r := h.run("", "read", stream, producer)
		h.want(r, exitIncomplete, "host "+liar.url+": REFUSED seq 1: ")
		if r.stdout != want || strings.Contains(r.stderr, "host "+liar.honest+": REFUSED") || !strings.Contains(r.stderr, "host "+liar.url+": REFUSED seq 3: ") {
			t.Fatalf("read, %s substituting bytes:\n%s\n%s", liar.url, r.stdout, r.stderr)
		}
		liar.host.SetRewrite(nil)
	}
	a.SetRewrite(lie(logrec.ClassBatch))
	b.SetRewrite(lie(logrec.ClassBatch))
	r := h.run("", "read", stream, producer, "-to", "3")
	h.want(r, exitIncomplete, "seq 1: NOT READ: the hosts answered it 2 time(s), each answer named above, and none verifies against the header source "+h.chainS.URL+"; it is not absent")
	if r.stdout != "" || strings.Contains(r.stderr, "MISSING") || strings.Contains(r.stderr, "no host holds") {
		t.Fatalf("read, every host substituting bytes:\n%s\n%s", r.stdout, r.stderr)
	}
	// The producer's retraction plan says the same, and builds nothing.
	r = h.run("", "sweep", stream, "-retract-seqs", "1")
	h.want(r, exitIncomplete, "seq 1: the hosts answered its carrier only with what does not verify against the header source "+h.chainS.URL)
	a.SetRewrite(nil)
	b.SetRewrite(nil)

	// A header source that does not answer is named; no host is blamed.
	down := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "down", http.StatusServiceUnavailable) }))
	t.Cleanup(down.Close)
	h.env["BLOGS_HEADER_URL"] = down.URL
	r = h.run("", "read", stream, producer)
	h.want(r, exitUsage, "the header source "+down.URL+" did not answer")
	if strings.Contains(r.stderr, "REFUSED") {
		t.Fatalf("read, header source down:\n%s", r.stderr)
	}
}

// The audit asks every host every question (the interval class, the batch
// class and the tree class): a host that substitutes bytes in any answer is
// named, the interval is proven from what verified, and the audit is
// incomplete (exit 3). A seq every host answers only with what does not
// verify is not called unserved.
func TestVerifyIntervalNamesAHostThatSubstitutesBytes(t *testing.T) {
	t.Parallel()
	h, stream, producer, b, bURL := liars(t)
	b.SetRewrite(lie(logrec.ClassBatch, logrec.ClassInterval, logrec.ClassTree))
	r := h.run("", "verify-interval", stream, producer, "1")
	h.want(r, exitIncomplete, "the interval is proven from what did")
	for _, what := range []string{"interval 1: ", "seq 1: ", "seq 3: ", "funding tree "} {
		if !strings.Contains(r.stderr, "host "+bURL+": REFUSED "+what) {
			t.Fatalf("verify-interval, b substituting bytes: %q not named:\n%s\n%s", what, r.stdout, r.stderr)
		}
	}
	if !strings.Contains(r.stdout, "VERIFIED interval 1") || strings.Contains(r.stderr, "host "+h.hostS.URL+": REFUSED") {
		t.Fatalf("verify-interval, b substituting bytes:\n%s\n%s", r.stdout, r.stderr)
	}
	// Every host substitutes the tokens: nothing is proven, and the header
	// source is named.
	h.host.SetRewrite(lie(logrec.ClassInterval))
	b.SetRewrite(lie(logrec.ClassInterval))
	h.want(h.run("", "verify-interval", stream, producer, "1"), exitIncomplete,
		"INCOMPLETE interval 1: the hosts answered 2 time(s), each answer named, and none verifies against the header source "+h.chainS.URL)
	h.host.SetRewrite(nil)
	b.SetRewrite(nil)

	// An archive whose file for seq 2 was altered, and no host: the seq is
	// unverified, the archive named, never "unserved".
	dir := t.TempDir()
	h.must("", "archive", stream, producer, "-dir", dir)
	root := filepath.Join(dir, stream, producer)
	files, _ := filepath.Glob(filepath.Join(root, "batches", "0000000000000002-*.beef"))
	if len(files) != 1 {
		t.Fatalf("batch files: %v", files)
	}
	beef, _ := os.ReadFile(files[0])
	beef[len(beef)*6/10] ^= 1
	if err := os.WriteFile(files[0], beef, 0o600); err != nil {
		t.Fatal(err)
	}
	h.env["BLOGS_HOSTS"] = ""
	r = h.run("", "verify-interval", stream, producer, "1", "-archive", dir)
	h.want(r, exitIncomplete, "the archive "+root+": REFUSED seq 2: ")
	if !strings.Contains(r.stdout, "unverified seqs 2: ") || strings.Contains(r.stdout, "unserved") || !strings.Contains(r.stdout, "VERIFIED interval 1") {
		t.Fatalf("verify-interval -archive, an altered file:\n%s\n%s", r.stdout, r.stderr)
	}
}

// Backfill takes a position from the first sibling whose answer verifies:
// a sibling that substitutes bytes is named and the next is asked, and a
// repair that filled every position exits 0 whatever a sibling answered. A
// header source that does not answer stops it, named, rather than reading
// as a sibling's refusal.
func TestBackfillNamesASiblingThatSubstitutesBytes(t *testing.T) {
	t.Parallel()
	h, stream, producer, b, bURL := liars(t)
	p, _ := hex.DecodeString(producer)
	c := testchain.NewHost(h.chain)
	cs := httptest.NewServer(c)
	t.Cleanup(cs.Close)
	h.must("", "backfill", stream, producer, "-from", h.hostS.URL, "-to", cs.URL, "-seqs", "1-3", "-anchors", "1")
	b.Forget(p, stream, 2)
	h.host.SetRewrite(lie(logrec.ClassBatch))
	r := h.run("", "-hosts", h.hostS.URL+","+cs.URL+","+bURL, "backfill", stream, producer, "-to", bURL, "-seqs", "1-3")
	if r.code != exitOK || !strings.Contains(r.stdout, "seq 2: the sibling "+h.hostS.URL+" answered something REFUSED: ") ||
		!strings.Contains(r.stdout, "seq 2: filled from the sibling "+cs.URL) || !strings.Contains(r.stdout, "2 held, 1 filled, 0 missing, 1 refused") {
		t.Fatalf("backfill, the first sibling substituting bytes: exit %d\n%s\n%s", r.code, r.stdout, r.stderr)
	}
	h.host.SetRewrite(nil)
	b.Forget(p, stream, 2)
	down := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "down", http.StatusServiceUnavailable) }))
	t.Cleanup(down.Close)
	h.env["BLOGS_HEADER_URL"] = down.URL
	r = h.run("", "backfill", stream, producer, "-from", h.hostS.URL, "-to", bURL, "-seqs", "2")
	h.want(r, exitUsage, "the header source "+down.URL+" did not answer")
	if strings.Contains(r.stdout, "REFUSED") {
		t.Fatalf("backfill, header source down:\n%s\n%s", r.stdout, r.stderr)
	}
}

// An archive follows one host and asks no other: what that host answers
// that does not verify is named without -v, with the header source it was
// checked against, and the pass is refused (exit 1).
func TestArchiveNamesAHostThatSubstitutesBytes(t *testing.T) {
	t.Parallel()
	h, stream, producer, _, _ := liars(t)
	h.host.SetRewrite(lie(logrec.ClassBatch))
	r := h.run("", "archive", stream, producer, "-dir", t.TempDir(), "-host", h.hostS.URL)
	h.want(r, exitRefused, "the host "+h.hostS.URL+" answered 4 time(s) what does not verify against the header source "+h.chainS.URL)
	if !strings.Contains(r.stderr, "host "+h.hostS.URL+": REFUSED seq 1: ") {
		t.Fatalf("archive, the host substituting bytes:\n%s\n%s", r.stdout, r.stderr)
	}
}
