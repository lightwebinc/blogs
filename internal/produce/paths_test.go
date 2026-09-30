package produce_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/blogs/internal/logrec"
	"github.com/lightwebinc/blogs/internal/produce"
	"github.com/lightwebinc/blogs/internal/reader"
	"github.com/lightwebinc/blogs/internal/state"
)

func isAnchor(tx *transaction.Transaction) bool {
	k, _, _ := logrec.Classify(tx)
	return k == logrec.KindAnchor
}

func TestARefusedAnchorIsBuiltAgain(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	ctx := context.Background()
	refusals := 0
	r.chain.Refuse = func(tx *transaction.Transaction) string {
		if isAnchor(tx) && refusals == 0 {
			refusals++
			return "a policy this test has"
		}
		return ""
	}
	e := r.engine(r.options())
	if err := e.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := e.Publish(ctx, lines(2, 0)); err != nil {
		t.Fatal(err)
	}
	// The settlement leg refuses the first anchor: it stays in flight,
	// unsubmitted, and is sent again on the next tick.
	if err := e.Tick(ctx, true); err != nil {
		t.Fatal(err)
	}
	if e.St.Inflight == nil || e.St.Inflight.Submitted {
		t.Fatalf("after a refused submit: %+v", e.St.Inflight)
	}
	dctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := e.Drain(dctx); err != nil {
		t.Fatalf("%v\n%s", err, r.notes)
	}
	if e.St.Anchor == nil || e.St.Anchor.Interval != 1 {
		t.Fatalf("anchor: %+v", e.St.Anchor)
	}
	if !strings.Contains(r.notes.String(), "settle via") {
		t.Fatalf("no note of the failed settle:\n%s", r.notes)
	}
}

func TestAnAnchorInFlightSurvivesARestart(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	ctx := context.Background()
	r.chain.HoldIf = isAnchor
	o := r.options()
	e := r.engine(o)
	if err := e.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := e.Publish(ctx, lines(2, 0)); err != nil {
		t.Fatal(err)
	}
	if err := e.Tick(ctx, true); err != nil {
		t.Fatal(err)
	}
	if err := e.Tick(ctx, false); err != nil {
		t.Fatal(err)
	}
	if e.St.Inflight == nil || !e.St.Inflight.Submitted || r.chain.Mined(e.St.Inflight.Txid) || e.St.Anchor != nil {
		t.Fatalf("want an anchor submitted, unmined and unpublished: %+v", e.St.Inflight)
	}
	inflight := e.St.Inflight.Txid
	_ = e.Close(ctx)
	// A new run over the saved state collects it once it mines, and
	// publishes it, and only then.
	e2 := r.engine(o)
	if err := e2.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := e2.Tick(ctx, false); err != nil || e2.St.Anchor != nil {
		t.Fatalf("published before it mined: %v %+v", err, e2.St.Anchor)
	}
	r.chain.Mine()
	if err := e2.Tick(ctx, false); err != nil {
		t.Fatal(err)
	}
	if e2.St.Anchor == nil || e2.St.Anchor.Txid != inflight || e2.St.Inflight != nil || e2.St.NextFirst != 2 {
		t.Fatalf("after mining: anchor %+v inflight %+v\n%s", e2.St.Anchor, e2.St.Inflight, r.notes)
	}
	producer, _ := hex.DecodeString(e2.St.Identity)
	rep, err := r.reader().VerifyInterval(ctx, r.stream, producer, 1)
	if err != nil || rep.Outcome != reader.Verified {
		t.Fatalf("verify: %v %+v", err, rep)
	}
}

func TestTheIntervalSetsTheCadence(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	ctx := context.Background()
	o := r.options()
	o.Interval = time.Minute
	e := r.engine(o)
	now := time.UnixMilli(1_000_000)
	e.Now = func() time.Time { return now }
	if err := e.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := e.Publish(ctx, lines(1, 0)); err != nil {
		t.Fatal(err)
	}
	now = now.Add(59 * time.Second)
	if err := e.Tick(ctx, false); err != nil || e.St.Inflight != nil {
		t.Fatalf("anchored before the interval: %v %+v", err, e.St.Inflight)
	}
	now = now.Add(time.Second)
	if err := e.Tick(ctx, false); err != nil || e.St.Inflight == nil {
		t.Fatalf("not anchored at the interval: %v", err)
	}
	if err := e.Tick(ctx, false); err != nil || e.St.Anchor == nil {
		t.Fatalf("not published once mined: %v\n%s", err, r.notes)
	}
	// Nothing new: no anchor however long it waits.
	now = now.Add(time.Hour)
	if err := e.Tick(ctx, false); err != nil || e.St.Inflight != nil {
		t.Fatalf("an empty interval was anchored: %+v", e.St.Inflight)
	}
}

func TestRunCutsBatchesBySizeCountAndTime(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	ctx := context.Background()
	e := r.engine(r.options())
	if err := e.Start(ctx); err != nil {
		t.Fatal(err)
	}
	src := make(chan produce.Line)
	errc := make(chan error)
	done := make(chan error, 1)
	go func() {
		done <- e.Run(ctx, src, errc, produce.Cut{MaxBytes: 20, MaxLines: 3, MaxWait: 50 * time.Millisecond}, time.Second)
	}()
	send := func(s string) { src <- produce.Line{B: []byte(s)} }
	// Three short lines: cut by count.
	send("a\n")
	send("b\n")
	send("c\n")
	// Two lines over 20 bytes together: the first is cut by size.
	send("0123456789abcdef\n")
	send("0123456789\n")
	// A pause: cut by time, whatever the publish before it took.
	producer := r.wallet.Signer().Identity.Compressed()
	for i := 0; !r.host.Holds(producer, r.stream, 3); i++ {
		if i > 2000 {
			t.Fatal("the last line was not cut by time")
		}
		time.Sleep(5 * time.Millisecond)
	}
	send("late\n")
	close(src)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	st, _ := state.Load(r.home, r.stream)
	if st.Seq != 4 {
		t.Fatalf("%d batches, want 4\n%s", st.Seq, r.notes)
	}
	want := []string{"a\nb\nc\n", "0123456789abcdef\n", "0123456789\n", "late\n"}
	for i, w := range want {
		cands, _, err := r.reader().Batch(ctx, r.stream, producer, uint64(i+1), false)
		if err != nil || len(cands) != 1 {
			t.Fatalf("seq %d: %v", i+1, err)
		}
		got, err := reader.Content(ctx, cands[0].Carrier.Batch, reader.Fetch{})
		if err != nil || string(got) != w {
			t.Fatalf("seq %d: %q %v, want %q", i+1, got, err, w)
		}
	}
}

func TestAnOversizedEncodingIsSplit(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	ctx := context.Background()
	o := r.options()
	o.Encoding = logrec.EncodingLinesGzip
	e := r.engine(o)
	if err := e.Start(ctx); err != nil {
		t.Fatal(err)
	}
	// Incompressible lines: 600 KiB of random hex gzips to well over the
	// 256 KiB content bound.
	var b strings.Builder
	n := 0
	for b.Len() < 600<<10 {
		buf := make([]byte, 1000)
		_, _ = rand.Read(buf)
		b.WriteString(hex.EncodeToString(buf)[:1999] + "\n")
		n++
	}
	if err := e.Publish(ctx, produce.Batch{Raw: []byte(b.String()), Count: n}); err != nil {
		t.Fatal(err)
	}
	if e.St.Seq < 2 {
		t.Fatalf("split into %d batches", e.St.Seq)
	}
	producer, _ := hex.DecodeString(e.St.Identity)
	var all []byte
	for s := uint64(1); s <= e.St.Seq; s++ {
		cands, _, err := r.reader().Batch(ctx, r.stream, producer, s, false)
		if err != nil || len(cands) != 1 {
			t.Fatalf("seq %d: %v", s, err)
		}
		bt := cands[0].Carrier.Batch
		if len(bt.Content) > logrec.MaxContent {
			t.Fatalf("seq %d carries %d bytes", s, len(bt.Content))
		}
		// The gzip header carries no time and no name.
		zr, err := gzip.NewReader(bytes.NewReader(bt.Content))
		if err != nil || !zr.ModTime.IsZero() || zr.Name != "" || zr.Comment != "" {
			t.Fatalf("seq %d gzip header: %v %+v", s, err, zr.Header)
		}
		got, _ := reader.Content(ctx, bt, reader.Fetch{})
		all = append(all, got...)
	}
	if string(all) != b.String() {
		t.Fatal("the split batches do not rejoin to the input")
	}
}

func TestReadLinesSplitsLongLinesAndEndsTheLast(t *testing.T) {
	t.Parallel()
	long := strings.Repeat("z", produce.MaxLine+10)
	ch, errc := produce.ReadLines(strings.NewReader("a\n" + long + "\nlast"))
	var got []produce.Line
	for l := range ch {
		got = append(got, l)
	}
	select {
	case err := <-errc:
		t.Fatal(err)
	default:
	}
	if len(got) != 4 || string(got[0].B) != "a\n" || string(got[3].B) != "last\n" {
		t.Fatalf("%d lines", len(got))
	}
	if !got[1].Split || !got[2].Split || got[0].Split || len(got[1].B)+len(got[2].B) != len(long)+2 {
		t.Fatalf("long line: %d + %d bytes, split %t %t", len(got[1].B), len(got[2].B), got[1].Split, got[2].Split)
	}
}

func TestFollowReadsATruncatedFileAgain(t *testing.T) {
	t.Parallel()
	f := filepath.Join(t.TempDir(), "x.log")
	if err := os.WriteFile(f, []byte("one\ntwo\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var notes bytes.Buffer
	ch, _, err := produce.Follow(ctx, f, 4, time.Millisecond, true, func(s string, a ...any) { notes.WriteString(s) })
	if err != nil {
		t.Fatal(err)
	}
	l := <-ch
	if string(l.B) != "two\n" || l.Tail.Offset != 8 {
		t.Fatalf("from the offset: %q %d", l.B, l.Tail.Offset)
	}
	if err := os.WriteFile(f, []byte("new\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	l = <-ch
	if string(l.B) != "new\n" || l.Tail.Offset != 4 {
		t.Fatalf("after truncation: %q %d", l.B, l.Tail.Offset)
	}
	cancel()
	for range ch {
	}
	_, _ = io.Discard.Write(nil)
}

func TestURLPrefixes(t *testing.T) {
	t.Parallel()
	for p, ok := range map[string]bool{
		"https://logs.example.com/c/":                 true,
		"http://logs.example.com/":                    false,
		"https://logs.example.com/a b/":               false,
		"https://" + strings.Repeat("a", 190) + "/c/": false,
	} {
		if (produce.ValidURLPrefix(p) == nil) != ok {
			t.Errorf("%q: want ok %t", p, ok)
		}
	}
}
