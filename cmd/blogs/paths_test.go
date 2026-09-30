package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/blogs/internal/logrec"
	"github.com/lightwebinc/blogs/internal/produce"
	"github.com/lightwebinc/blogs/internal/state"
	"github.com/lightwebinc/blogs/internal/testchain"
)

func TestRestartContinuesTheStream(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	stream, producer := h.producer()
	first, second := linesOf(0, 4), linesOf(100, 3)
	h.produce(first, stream, "-max-lines", "2")
	r := h.produce(second, stream, "-max-lines", "2")
	if !strings.Contains(r.stdout, "published seq 3 to 4") || !strings.Contains(r.stdout, "interval 2") {
		t.Fatalf("second run:\n%s\n%s", r.stdout, r.stderr)
	}
	r = h.must("", "read", stream, producer, "-v")
	if r.stdout != first+second || strings.Contains(r.stderr, "unchecked") || strings.Contains(r.stderr, "BROKEN") {
		t.Fatalf("read across runs:\n%s\n%s", r.stdout, r.stderr)
	}
	h.must("", "verify-interval", stream, producer, "2")
}

func TestAPersistedCarrierIsPublishedAgainNotRebuilt(t *testing.T) {
	h := newHarness(t)
	stream, producer := h.producer()
	h.produce("a\nb\n", stream)
	// The host goes away while seq 2 is being published: the run stops with
	// the carrier persisted.
	h.host.SetDown(true)
	old := retriesForTest()
	defer old()
	r := h.run("c\n", append([]string{"produce", stream}, fast...)...)
	h.want(r, exitUsage, "the carrier is persisted")
	st, _ := state.Load(h.home, stream)
	if st.Seq != 2 || st.Pending == "" {
		t.Fatalf("after the failed publish: seq %d pending %t", st.Seq, st.Pending != "")
	}
	persisted := st.Head
	h.host.SetDown(false)
	r = h.must("", append([]string{"anchor", stream}, fast...)...)
	if !strings.Contains(r.stderr, "seq 2: publishing the carrier a previous run persisted") || !strings.Contains(r.stdout, "anchored to seq 2: interval 2") {
		t.Fatalf("anchor:\n%s\n%s", r.stdout, r.stderr)
	}
	st, _ = state.Load(h.home, stream)
	if st.Head != persisted || st.Pending != "" {
		t.Fatalf("the persisted carrier was replaced")
	}
	r = h.must("", "read", stream, producer)
	if r.stdout != "a\nb\nc\n" {
		t.Fatalf("read: %q", r.stdout)
	}
	h.must("", "verify-interval", stream, producer, "2")
}

func TestDigestOnlyAndGzip(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	stream, producer := h.producer()
	dir := filepath.Join(t.TempDir(), "content")
	in := linesOf(0, 20)
	h.produce(in, stream, "-profile", "digest", "-encoding", "gzip", "-content-dir", dir, "-url", "https://logs.example.com/c/", "-max-lines", "7")
	ents, _ := os.ReadDir(dir)
	if len(ents) != 3 {
		t.Fatalf("%d content files, want 3", len(ents))
	}
	// Without the content the batches verify and the content is pending.
	r := h.must("", "read", stream, producer, "-meta")
	if strings.Count(r.stdout, "content pending") != 3 || strings.Contains(r.stdout, "event") {
		t.Fatalf("read without content:\n%s", r.stdout)
	}
	// A hint whose host is not allowed is never fetched.
	r = h.must("", "read", stream, producer, "-allow-host", "other.example.com", "-meta")
	if strings.Count(r.stdout, "content pending") != 3 {
		t.Fatalf("read with a host not allowed:\n%s", r.stdout)
	}
	r = h.must("", "read", stream, producer, "-content-dir", dir)
	if r.stdout != in {
		t.Fatalf("read with the content:\n%s", r.stdout)
	}
	// Bytes that do not match the digest are discarded.
	for _, e := range ents {
		if err := os.WriteFile(filepath.Join(dir, e.Name()), []byte("tampered\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	r = h.must("", "read", stream, producer, "-content-dir", dir, "-meta")
	if strings.Count(r.stdout, "content pending") != 3 {
		t.Fatalf("read with tampered content:\n%s", r.stdout)
	}
	h.must("", "verify-interval", stream, producer, "1")
	// Full-bytes gzip on the same stream: profiles mix batch by batch.
	more := linesOf(50, 5)
	h.produce(more, stream, "-encoding", "gzip")
	r = h.must("", "read", stream, producer, "-from", "4")
	if r.stdout != more {
		t.Fatalf("gzip batch: %q", r.stdout)
	}
}

func TestFileIsReadFromWhereTheLastRunStopped(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	stream, producer := h.producer()
	f := filepath.Join(t.TempDir(), "app.log")
	if err := os.WriteFile(f, []byte("l1\nl2\npartial"), 0o600); err != nil {
		t.Fatal(err)
	}
	h.produce("", stream, "-file", f)
	fh, _ := os.OpenFile(f, os.O_APPEND|os.O_WRONLY, 0)
	_, _ = fh.WriteString("l3\nl4\n")
	fh.Close()
	r := h.produce("", stream, "-file", f)
	if !strings.Contains(r.stdout, "published seq 2 to 2") {
		t.Fatalf("second run:\n%s\n%s", r.stdout, r.stderr)
	}
	r = h.must("", "read", stream, producer)
	if r.stdout != "l1\nl2\npartial\nl3\nl4\n" {
		t.Fatalf("read: %q", r.stdout)
	}
}

func TestFollowPublishesAsTheFileGrows(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	stream, producer := h.producer()
	f := filepath.Join(t.TempDir(), "app.log")
	if err := os.WriteFile(f, []byte("start\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan result, 1)
	go func() {
		done <- h.runCtx(ctx, "", append([]string{"produce", stream, "-file", f, "-follow", "-max-wait", "20ms"}, fast...)...)
	}()
	waitFor(t, func() bool { return h.host.Holds(mustHex(producer), stream, 1) })
	fh, _ := os.OpenFile(f, os.O_APPEND|os.O_WRONLY, 0)
	_, _ = fh.WriteString("more\n")
	fh.Close()
	waitFor(t, func() bool { return h.host.Holds(mustHex(producer), stream, 2) })
	// Rotation: a new file under the same name is followed from its start.
	if err := os.Rename(f, f+".1"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(f, []byte("rotated\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return h.host.Holds(mustHex(producer), stream, 3) })
	cancel()
	r := <-done
	if r.code != 0 || !strings.Contains(r.stdout, "published seq 1 to 3") {
		t.Fatalf("follow: exit %d\n%s\n%s", r.code, r.stdout, r.stderr)
	}
	r = h.must("", "anchor", stream, "-poll", "5ms")
	r = h.must("", "read", stream, producer)
	if r.stdout != "start\nmore\nrotated\n" {
		t.Fatalf("read: %q", r.stdout)
	}
}

func waitFor(t *testing.T, ok func() bool) {
	t.Helper()
	for i := 0; i < 1000; i++ {
		if ok() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out")
}

func mustHex(s string) []byte {
	b, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	}
	return b
}

func TestReadFiltersWhatReachesTheTerminal(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	stream, producer := h.producer()
	hostile := "title\x1b]0;owned\x07 \x1b[31mred\x1b[0m rev\u202eersed\x1b[2J\n" + strings.Repeat("w", 700) + "\n"
	r := h.produce(hostile, stream)
	if !strings.Contains(r.stderr, "2 line(s) held something a terminal must be protected from") && !strings.Contains(r.stderr, "1 line(s) held") {
		t.Fatalf("produce did not warn:\n%s", r.stderr)
	}
	r = h.must("", "read", stream, producer)
	if strings.ContainsAny(r.stdout, "\x1b\x07\u202e") {
		t.Fatalf("read let a control through: %q", r.stdout)
	}
	if !strings.Contains(r.stdout, "title red reversed\n") {
		t.Fatalf("read: %q", r.stdout)
	}
	// A 700-column line is wrapped, not cut.
	if strings.Count(r.stdout, "w") != 700 {
		t.Fatalf("long line: %d of 700", strings.Count(r.stdout, "w"))
	}
	r = h.must("", "read", stream, producer, "-ansi")
	if !strings.Contains(r.stdout, "\x1b[31mred") || strings.Contains(r.stdout, "\x1b]") {
		t.Fatalf("read -ansi: %q", r.stdout)
	}
	// -unsafe drop keeps such lines off the plane.
	r = h.produce("ok\n\x1b[2Jbad\n", stream, "-unsafe", "drop")
	if !strings.Contains(r.stderr, "1 line(s) held") || !strings.Contains(r.stderr, "dropped") {
		t.Fatalf("drop: %s", r.stderr)
	}
	r = h.must("", "read", stream, producer, "-from", "2")
	if r.stdout != "ok\n" {
		t.Fatalf("after drop: %q", r.stdout)
	}
}

// lying is a host whose lookup answers every question with one fixed
// answer, taken from the real host's answer to base.
func lying(t *testing.T, real string, base map[string]any) *httptest.Server {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"service": "ls_log", "query": base})
	resp, err := http.Post(real+"/lookup", "application/json", strings.NewReader(string(body)))
	if err != nil {
		t.Fatal(err)
	}
	fixed, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(fixed)
	}))
	t.Cleanup(s.Close)
	return s
}

func TestReadRefusesAnAnswerForAnotherPosition(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	stream, producer := h.producer()
	h.produce("a\nb\n", stream, "-max-lines", "1")
	liar := lying(t, h.hostS.URL, map[string]any{"stream": stream, "producer": producer, "seq": 1})
	h.env["BLOGS_HOSTS"] = liar.URL
	r := h.run("", "read", stream, producer, "-to", "2")
	h.want(r, exitRefused, "REFUSED-SEQ")
	// Another producer's key: the lying host's carrier is refused too.
	other := "02" + strings.Repeat("11", 32)
	for !isKey(other) {
		other = "02" + strings.Repeat("12", 32)
	}
	h.want(h.run("", "read", stream, other, "-to", "1"), exitRefused, "REFUSED-KEY")
	// A header source without the funding tree's block refuses the proof.
	empty := httptest.NewServer(testchain.New(1))
	t.Cleanup(empty.Close)
	h.env["BLOGS_HOSTS"], h.env["BLOGS_HEADER_URL"] = h.hostS.URL, empty.URL
	h.want(h.run("", "read", stream, producer, "-to", "1"), exitRefused, "REFUSED-BUMP")
	h.want(h.run("", "verify-interval", stream, producer, "1"), exitIncomplete, "no answered token")
}

func isKey(h string) bool {
	b, err := hex.DecodeString(h)
	return err == nil && logrec.CheckProducer(b) == nil
}

func TestBackfillFillsAHoleFromASibling(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	stream, producer := h.producer()
	h.produce(linesOf(0, 6), stream, "-max-lines", "2")
	sibling := testchain.NewHost(h.chain)
	sib := httptest.NewServer(sibling)
	t.Cleanup(sib.Close)
	// The sibling holds nothing: verify-interval against it is incomplete.
	h.env["BLOGS_HOSTS"] = sib.URL
	h.want(h.run("", "verify-interval", stream, producer, "1"), exitIncomplete, "no host answered")
	r := h.must("", "backfill", stream, producer, "-from", h.hostS.URL, "-to", sib.URL, "-interval", "1")
	if !strings.Contains(r.stdout, "interval 1: filled from the sibling") || !strings.Contains(r.stdout, "0 held, 4 filled, 0 missing") {
		t.Fatalf("backfill:\n%s\n%s", r.stdout, r.stderr)
	}
	h.must("", "verify-interval", stream, producer, "1")
	// Again: everything is held.
	r = h.must("", "backfill", stream, producer, "-from", h.hostS.URL, "-to", sib.URL, "-seqs", "1-3")
	if !strings.Contains(r.stdout, "3 held, 0 filled") {
		t.Fatalf("second backfill:\n%s", r.stdout)
	}
	// A seq neither holds stays missing.
	h.want(h.run("", "backfill", stream, producer, "-from", h.hostS.URL, "-to", sib.URL, "-seqs", "4"), exitIncomplete, "seq 4: MISSING")
}

func TestVerifyIntervalRefusesARootTheBatchesDoNotMake(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	stream, producer := h.producer()
	h.produce("a\nb\n", stream, "-max-lines", "1", "-anchor-wait", "0")
	// The producer anchors a commitment it never published.
	st, _ := state.Load(h.home, stream)
	st.Unanchored[1] = strings.Repeat("ab", 32)
	if err := st.Save(); err != nil {
		t.Fatal(err)
	}
	h.must("", append([]string{"anchor", stream}, fast...)...)
	h.want(h.run("", "verify-interval", stream, producer, "1"), exitRefused, "root-mismatch")
}

func TestUsageErrorsAndRefusals(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	stream, producer := h.producer()
	for _, c := range []struct {
		args []string
		want string
	}{
		{[]string{"nosuch"}, `unknown command "nosuch"`},
		{[]string{"produce"}, "produce <stream>"},
		{[]string{"produce", "app"}, "is not a stream identifier"},
		{[]string{"produce", "app_bbbbbbbbbb", "-poll", "5ms"}, "holds no stream app_bbbbbbbbbb"},
		{[]string{"produce", stream, "-profile", "digest"}, "needs -content-dir"},
		{[]string{"produce", stream, "-profile", "digest", "-content-dir", t.TempDir(), "-url", "http://x.example/"}, "https://"},
		{[]string{"produce", stream, "-encoding", "zstd"}, "-encoding is lines or gzip"},
		{[]string{"produce", stream, "-follow"}, "-follow needs -file"},
		{[]string{"read", stream, "zz"}, "not a producer identity key"},
		{[]string{"read", "app", producer}, "readable name alone"},
		{[]string{"verify-interval", stream, producer, "0"}, "is not a number from 1"},
		{[]string{"backfill", stream, producer, "-from", "http://a", "-to", "http://b"}, "give -seqs, -interval or -anchors"},
		{[]string{"stream", "new", "Bad-Name"}, "lowercase letters"},
		{[]string{"-network", "mainnet", "doctor"}, "network must be main, test or regtest"},
		{[]string{"-header-url", "woc:main", "doctor"}, "header source woc:main is the main network but network is regtest"},
	} {
		h.want(h.run("", c.args...), exitUsage, c.want)
	}
	for _, help := range [][]string{{"help"}, {"-h"}, {"read", "-h"}, {"produce", "-help"}} {
		if r := h.run("", help...); r.code != exitOK {
			t.Fatalf("%v: exit %d", help, r.code)
		}
	}
	if r := h.must("", "version"); !strings.HasPrefix(r.stdout, "blogs ") {
		t.Fatalf("version: %q", r.stdout)
	}
	// Missing configuration names the key.
	delete(h.env, "BLOGS_HEADER_URL")
	h.want(h.run("", "read", stream, producer), exitUsage, "no header source")
	delete(h.env, "BLOGS_FACADE")
	h.want(h.run("a\n", "produce", stream), exitUsage, "facade must be configured")
	h.env["BLOGS_FACADE"], h.env["BLOGS_SETTLE"] = h.hostS.URL, "udp:x"
	h.want(h.run("a\n", "produce", stream), exitUsage, "settle must be")
	delete(h.env, "BLOGS_HOSTS")
	h.want(h.run("", "verify-interval", stream, producer, "1"), exitUsage, "no overlay host configured")
}

func TestFund(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.must("", "init")
	h.env["BLOGS_NETWORK"] = "main"
	r := h.must("", "fund", "-blocks", "1")
	if !strings.Contains(r.stdout, "fund address 1") {
		t.Fatalf("main: %s", r.stdout)
	}
	h.env["BLOGS_NETWORK"] = "test"
	r = h.must("", "fund", "-blocks", "3", "-batch", "2")
	if !strings.Contains(r.stdout, "pool after:  4 output(s), 20000000000 sat (+3); immature 4") || !strings.Contains(r.stdout, "fund address m") && !strings.Contains(r.stdout, "fund address n") {
		t.Fatalf("fund:\n%s", r.stdout)
	}
	r = h.must("", "fund", "-rescan", "-blocks", "10")
	if !strings.Contains(r.stdout, "(+0)") {
		t.Fatalf("rescan found what the pool holds:\n%s", r.stdout)
	}
	for addr, want := range map[string]string{"1BgGZ9tcN4rm9KBzDn7KprQz87SZ26SAMH": "main", "mipcBbFg9gMiCh81Kj8tqqdgoZub1ZJRfn": "test"} {
		if got, err := addressNetwork(addr); err != nil || got != want {
			t.Fatalf("%s: %s %v", addr, got, err)
		}
	}
	if _, err := addressNetwork("not-an-address"); err == nil {
		t.Fatal("a bad address decoded")
	}
	for addr, network := range map[string]string{"1BgGZ9tcN4rm9KBzDn7KprQz87SZ26SAMH": "test", "mipcBbFg9gMiCh81Kj8tqqdgoZub1ZJRfn": "main"} {
		if err := checkFundAddress(addr, network); err == nil || !strings.Contains(err.Error(), "refusing to mine") {
			t.Fatalf("%s on %s: %v", addr, network, err)
		}
	}
	if err := checkFundAddress("mipcBbFg9gMiCh81Kj8tqqdgoZub1ZJRfn", "regtest"); err != nil {
		t.Fatal(err)
	}
}

func TestProduceNeedsCoinAndTheHomeLock(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.must("", "init")
	r := h.must("", "stream", "new", "empty")
	stream := streamLine.FindStringSubmatch(r.stdout)[1]
	h.want(h.run("a\n", append([]string{"produce", stream}, fast...)...), exitUsage, "run `blogs fund`")
	unlock, err := state.Lock(filepath.Join(h.home, "lock"))
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	h.want(h.run("a\n", append([]string{"produce", stream}, fast...)...), exitUsage, "another blogs command is using the home")
	h.want(h.run("", "fund", "-blocks", "1"), exitUsage, "another blogs command is using the home")
}

// retriesForTest shortens the publish retry schedule and returns its
// restore.
func retriesForTest() func() {
	old := produce.Retries
	produce.Retries = []time.Duration{time.Millisecond}
	return func() { produce.Retries = old }
}

// copyFile copies src over dst.
func copyFile(t *testing.T, src, dst string) {
	t.Helper()
	b, err := os.ReadFile(src)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dst, b, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestTheFirstMinedTokenIsCanonical(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	stream, producer := h.producer()
	h.produce("a\nb\n", stream, "-max-lines", "1", "-anchor-wait", "0")
	file := filepath.Join(state.Dir(h.home, stream), "state.json")
	saved := filepath.Join(t.TempDir(), "state.json")
	copyFile(t, file, saved)
	h.must("", append([]string{"anchor", stream}, fast...)...)
	st, _ := state.Load(h.home, stream)
	first := st.Anchor.Txid
	// The producer equivocates: the same batches anchored again from the
	// state before the first anchor, as a second interval 1.
	copyFile(t, saved, file)
	r := h.must("", append([]string{"anchor", stream}, fast...)...)
	st, _ = state.Load(h.home, stream)
	if st.Anchor.Txid == first || st.Anchor.Interval != 1 {
		t.Fatalf("no second interval 1:\n%s", r.stdout)
	}
	r = h.must("", "verify-interval", stream, producer, "1")
	if !strings.Contains(r.stdout, "interval 1  token "+first) {
		t.Fatalf("the first mined token is not canonical:\n%s", r.stdout)
	}
	// Interval 2 built on the second token continues no canonical chain.
	h.produce("c\n", stream)
	h.want(h.run("", "verify-interval", stream, producer, "2"), exitIncomplete, "no answered token continues the chain")
}

func TestVerifyIntervalRefusesAnAnchorThatDoesNotJoin(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	stream, producer := h.producer()
	h.produce("a\nb\n", stream, "-max-lines", "1")
	h.produce("c\nd\n", stream, "-max-lines", "1", "-anchor-wait", "0")
	// Interval 2 skips seq 3.
	st, _ := state.Load(h.home, stream)
	st.NextFirst, st.Unanchored = 4, st.Unanchored[1:]
	if err := st.Save(); err != nil {
		t.Fatal(err)
	}
	h.must("", append([]string{"anchor", stream}, fast...)...)
	h.want(h.run("", "verify-interval", stream, producer, "2"), exitRefused, "anchor-join")
}

func TestAnInterruptDuringTheFinalWaitLeavesTheAnchorInFlight(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	stream, producer := h.producer()
	h.chain.HoldIf = func(tx *transaction.Transaction) bool {
		k, _, _ := logrec.Classify(tx)
		return k == logrec.KindAnchor
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan result, 1)
	go func() { done <- h.runCtx(ctx, "a\nb\n", append([]string{"produce", stream}, fast...)...) }()
	waitFor(t, func() bool { return h.chain.Waiting() == 1 })
	cancel()
	r := <-done
	if r.code != exitOK || !strings.Contains(r.stdout, "anchor interval 1 (seq 1 to 1) in flight") {
		t.Fatalf("interrupted: exit %d\n%s\n%s", r.code, r.stdout, r.stderr)
	}
	h.chain.Mine()
	r = h.must("", append([]string{"anchor", stream}, fast...)...)
	if !strings.Contains(r.stdout, "anchored to seq 1: interval 1") {
		t.Fatalf("anchor:\n%s\n%s", r.stdout, r.stderr)
	}
	h.must("", "verify-interval", stream, producer, "1")
}
