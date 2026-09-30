package main

import (
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/lightwebinc/blogs/internal/archive"
	"github.com/lightwebinc/blogs/internal/testchain"
)

var latestLine = regexp.MustCompile(`(?m)^latest interval (\d+) root ([0-9a-f]{64}) token ([0-9a-f]{64})$`)

func readInterval(t *testing.T, root string, k int) (*archive.IntervalFile, []byte) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(root, "intervals", fmt.Sprintf("%016d.json", k)))
	if err != nil {
		t.Fatal(err)
	}
	var f archive.IntervalFile
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatal(err)
	}
	if err := f.Verify(); err != nil {
		t.Fatal(err)
	}
	return &f, raw
}

func TestArchiveStoresBatchesAndProvesIntervals(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	stream, producer := h.producer()
	h.produce(linesOf(0, 6), stream, "-max-lines", "2")
	h.produce(linesOf(10, 2), stream, "-max-lines", "1")
	dirA := t.TempDir()
	r := h.must("", "archive", stream, producer, "-dir", dirA)
	if !strings.Contains(r.stdout, "interval 1  proven  seqs 1 to 3") || !strings.Contains(r.stdout, "interval 2  proven  seqs 4 to 5") ||
		!strings.Contains(r.stdout, "5 batch(es) stored, walked to seq 5, intervals 1 to 2 proven") {
		t.Fatalf("archive:\n%s\n%s", r.stdout, r.stderr)
	}
	m := latestLine.FindStringSubmatch(r.stdout)
	if m == nil || m[1] != "2" {
		t.Fatalf("no latest line:\n%s", r.stdout)
	}
	v := h.must("", "verify-interval", stream, producer, "2")
	if !strings.Contains(v.stdout, "root "+m[2]) {
		t.Fatalf("the archive's root %s is not the audit's:\n%s", m[2], v.stdout)
	}
	rootA := filepath.Join(dirA, stream, producer)
	f, rawA := readInterval(t, rootA, 1)
	if f.Status != "proven" || len(f.Leaves) != 3 || f.Height == 0 {
		t.Fatalf("interval 1: %+v", f)
	}
	tampered := *f
	tampered.Leaves = append([]archive.Leaf(nil), f.Leaves...)
	tampered.Leaves[1].Commitment = f.Leaves[0].Commitment
	if tampered.Verify() == nil {
		t.Fatal("an interval file with a leaf swapped verified")
	}
	content, err := os.ReadFile(filepath.Join(rootA, "content", "0000000000000001-"+f.Leaves[0].Commitment+".log"))
	if err != nil || string(content) != linesOf(0, 2) {
		t.Fatalf("content of seq 1: %q %v", content, err)
	}
	for _, sub := range []string{"batches/0000000000000005-*.beef", "batches/0000000000000005-*.json", "anchors/0000000000000002-" + m[3] + ".beef"} {
		if got, _ := filepath.Glob(filepath.Join(rootA, sub)); len(got) != 1 {
			t.Fatalf("%s: %v", sub, got)
		}
	}
	// A second pass stores nothing and proves nothing new.
	r = h.must("", "archive", stream, producer, "-dir", dirA)
	if !strings.Contains(r.stdout, "0 batch(es) stored") || strings.Contains(r.stdout, "  proven  ") || !strings.Contains(r.stdout, "latest interval 2 root "+m[2]) {
		t.Fatalf("second pass:\n%s", r.stdout)
	}

	// A second archive, of a sibling filled by backfill, holds the same proof.
	sibling := httptest.NewServer(testchain.NewHost(h.chain))
	t.Cleanup(sibling.Close)
	for _, k := range []string{"1", "2"} {
		h.must("", "backfill", stream, producer, "-from", h.hostS.URL, "-to", sibling.URL, "-interval", k)
	}
	dirB := t.TempDir()
	r = h.must("", "archive", stream, producer, "-dir", dirB, "-host", sibling.URL)
	if !strings.Contains(r.stdout, "latest interval 2 root "+m[2]+" token "+m[3]) {
		t.Fatalf("sibling archive:\n%s", r.stdout)
	}
	if _, rawB := readInterval(t, filepath.Join(dirB, stream, producer), 1); string(rawB) != string(rawA) {
		t.Fatalf("the two archives' interval 1 differ:\n%s\n%s", rawA, rawB)
	}
}

func TestArchiveProvesAnIntervalWithARetractedHole(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	stream, producer := h.producer()
	h.produce(linesOf(0, 4), stream, "-max-lines", "1")
	held := t.TempDir()
	h.must("", "archive", stream, producer, "-dir", held)
	fh, _ := readInterval(t, filepath.Join(held, stream, producer), 1)
	h.must("", append([]string{"sweep", stream, "-retract-seqs", "2", "-yes"}, fast...)...)

	// The archive that stored seq 2 finds it retracted on its next pass:
	// it keeps a receipt and the sweep, and drops the batch's bytes.
	r := h.must("", "archive", stream, producer, "-dir", held)
	if !strings.Contains(r.stdout, "retracted seqs 2: receipt and sweep kept") {
		t.Fatalf("recheck:\n%s\n%s", r.stdout, r.stderr)
	}
	rootH := filepath.Join(held, stream, producer)
	name := "0000000000000002-" + fh.Leaves[1].Commitment
	for _, gone := range []string{"batches/" + name + ".beef", "batches/" + name + ".json", "content/" + name + ".log"} {
		if _, err := os.Stat(filepath.Join(rootH, gone)); !os.IsNotExist(err) {
			t.Fatalf("%s was not dropped: %v", gone, err)
		}
	}
	var rc archive.Receipt
	raw, err := os.ReadFile(filepath.Join(rootH, "retracted", name+".json"))
	if err == nil {
		err = json.Unmarshal(raw, &rc)
	}
	if err != nil || rc.Seq != 2 || rc.Commitment != fh.Leaves[1].Commitment || rc.Sweep == "" || rc.SweepHeight == 0 || rc.Content != "dropped" {
		t.Fatalf("receipt: %+v %v", rc, err)
	}
	if _, err := os.Stat(filepath.Join(rootH, "retracted", name+".sweep.beef")); err != nil {
		t.Fatal(err)
	}
	// A further pass finds nothing new, and seq 3 stays stored.
	r = h.must("", "archive", stream, producer, "-dir", held)
	if strings.Contains(r.stdout, "retracted seqs") {
		t.Fatalf("retracted twice:\n%s", r.stdout)
	}
	if got, _ := filepath.Glob(filepath.Join(rootH, "batches", "0000000000000003-*.beef")); len(got) != 1 {
		t.Fatalf("seq 3: %v", got)
	}

	// An archive started after the retraction recovers seq 2's commitment
	// from seq 3's prev.
	late := t.TempDir()
	r = h.must("", "archive", stream, producer, "-dir", late)
	if !strings.Contains(r.stdout, "interval 1  proven-with-holes  seqs 1 to 4") || !strings.Contains(r.stdout, "holes 2") {
		t.Fatalf("late archive:\n%s\n%s", r.stdout, r.stderr)
	}
	f, _ := readInterval(t, filepath.Join(late, stream, producer), 1)
	if f.Root != fh.Root || f.Leaves[1].Held || !fh.Leaves[1].Held || f.Leaves[1].Commitment != fh.Leaves[1].Commitment {
		t.Fatalf("hole: %+v\nheld: %+v", f.Leaves[1], fh.Leaves[1])
	}
	// The auditor proves it the same way and names the seq retracted.
	v := h.must("", "verify-interval", stream, producer, "1")
	if !strings.Contains(v.stdout, "retracted seqs 2: no host serves them") || !strings.Contains(v.stdout, "VERIFIED interval 1") || !strings.Contains(v.stdout, "root "+fh.Root) {
		t.Fatalf("verify-interval:\n%s", v.stdout)
	}

	// A second interval whose last seq is retracted: nothing names its
	// commitment, so neither a fresh archive nor the auditor can recover
	// it, but an archive holding the receipt proves the interval.
	h.produce(linesOf(10, 4), stream, "-max-lines", "1")
	h.must("", "archive", stream, producer, "-dir", held)
	f2, _ := readInterval(t, rootH, 2)
	h.must("", append([]string{"sweep", stream, "-retract-seqs", "8", "-yes"}, fast...)...)
	h.want(h.run("", "archive", stream, producer, "-dir", t.TempDir()), exitIncomplete, "seq 8, the interval's last, is held by no file")
	h.want(h.run("", "verify-interval", stream, producer, "2"), exitIncomplete, "cannot be recovered")
	// Nothing names the interval's last seq, so a backfill finds it missing.
	sib := httptest.NewServer(testchain.NewHost(h.chain))
	t.Cleanup(sib.Close)
	h.want(h.run("", "backfill", stream, producer, "-from", h.hostS.URL, "-to", sib.URL, "-interval", "2"), exitIncomplete, "1 missing, 0 refused, 0 retracted")
	r = h.must("", "archive", stream, producer, "-dir", held)
	if !strings.Contains(r.stdout, "retracted seqs 8") {
		t.Fatalf("recheck of seq 8:\n%s", r.stdout)
	}
	// Proving interval 2 again from these files (as a rebuilt index would):
	// the receipt completes the root.
	if err := os.Remove(filepath.Join(rootH, "intervals", fmt.Sprintf("%016d.json", 2))); err != nil {
		t.Fatal(err)
	}
	a := &archive.Archive{Dir: held, Stream: stream}
	a.Producer = mustHex(producer)
	cur, err := a.LoadCursor()
	if err != nil {
		t.Fatal(err)
	}
	cur.Intervals = 1
	curRaw, _ := json.Marshal(cur)
	if err := os.WriteFile(filepath.Join(rootH, "archive.json"), curRaw, 0o644); err != nil {
		t.Fatal(err)
	}
	r = h.must("", "archive", stream, producer, "-dir", held)
	if !strings.Contains(r.stdout, "interval 2  proven  seqs 5 to 8  root "+f2.Root) || !strings.Contains(r.stdout, "retracted 8") {
		t.Fatalf("receipt proof:\n%s\n%s", r.stdout, r.stderr)
	}
	g, _ := readInterval(t, rootH, 2)
	if len(g.Retracted) != 1 || g.Retracted[0] != 8 || !g.Leaves[3].Retracted || g.Leaves[3].Held || len(g.Holes) != 0 {
		t.Fatalf("interval 2 from the receipt: %+v", g)
	}
}
