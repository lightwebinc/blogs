package module

import (
	"context"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/lightwebinc/blogs/internal/logrec"
)

// Retention, the twin of retention.test.ts: proven carriers older than the
// policy leave the index and the engine's storage, oldest intervals first,
// and never anything else; what was dropped stays dropped across a
// restart; the configuration is refused rather than guessed.

const day = 24 * 60 * 60 * 1000

func anchorTime(t *testing.T, name string) int64 {
	t.Helper()
	a, _, err := logrec.DecodeToken(vectors(t).byName(t, name).tx(t))
	if err != nil {
		t.Fatal(err)
	}
	return int64(a.Timestamp) //nolint:gosec // a timestamp
}

func env(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

func TestParseRetention(t *testing.T) {
	if r, err := ParseRetention(env(nil)); r != nil || err != nil {
		t.Fatal("off by default")
	}
	if r, err := ParseRetention(env(map[string]string{"BLOGS_KEEP_DAYS": " "})); r != nil || err != nil {
		t.Fatal("blank")
	}
	r, err := ParseRetention(env(map[string]string{"BLOGS_KEEP_DAYS": "7", "BLOGS_STATE_DIR": "/var/lib/blogs"}))
	if err != nil || r.KeepMs != 7*day || r.StateDir != "/var/lib/blogs" {
		t.Fatal(r, err)
	}
	if r, err := ParseRetention(env(map[string]string{"BLOGS_KEEP_DAYS": "1.5", "BLOGS_STATE_DIR": "/s"})); err != nil || r.KeepMs != 1.5*day {
		t.Fatal(r, err)
	}
	for _, bad := range []map[string]string{
		{"BLOGS_STATE_DIR": "/s"},
		{"BLOGS_KEEP_DAYS": "0.5", "BLOGS_STATE_DIR": "/s"},
		{"BLOGS_KEEP_DAYS": "0", "BLOGS_STATE_DIR": "/s"},
		{"BLOGS_KEEP_DAYS": "-3", "BLOGS_STATE_DIR": "/s"},
		{"BLOGS_KEEP_DAYS": "7d", "BLOGS_STATE_DIR": "/s"},
		{"BLOGS_KEEP_DAYS": "7"},
		{"BLOGS_KEEP_DAYS": "7", "BLOGS_STATE_DIR": "relative/dir"},
	} {
		if _, err := ParseRetention(env(bad)); err == nil {
			t.Fatal(bad)
		}
	}
}

func TestStateFile(t *testing.T) {
	v := vectors(t)
	dir := t.TempDir()
	st := FileStore{Dir: dir}
	if s, err := st.Load(); err != nil || len(s.Marks)+len(s.Trees) != 0 {
		t.Fatal("no file is the empty state")
	}
	state := RetentionState{Marks: []Mark{{Producer: v.IdentityKey, Stream: v.Stream, Interval: 2, Seq: 3, C: strings.Repeat("ab", 32)}}, Trees: []TreeLow{{Key: v.Topic + "|" + strings.Repeat("cd", 32), Vout: 2}}}
	if err := st.Save(state); err != nil {
		t.Fatal(err)
	}
	if got, err := (FileStore{Dir: dir}).Load(); err != nil || !reflect.DeepEqual(got, state) {
		t.Fatal(got, err)
	}
	// The TypeScript module's file reads the same.
	ts := `{"v":1,"marks":[{"producer":"` + v.IdentityKey + `","stream":"` + v.Stream + `","interval":2,"seq":3,"c":"` + strings.Repeat("ab", 32) + `"}],"trees":[["` + v.Topic + "|" + strings.Repeat("cd", 32) + `",2]]}`
	if raw, _ := os.ReadFile(filepath.Join(dir, StateFile)); string(raw) != ts {
		t.Fatalf("not the TypeScript layout:\n%s\n%s", raw, ts)
	}
	for _, bad := range []string{"{", `{"v":2,"marks":[],"trees":[]}`, `{"v":1,"marks":[{"producer":"p","stream":"s","interval":1,"seq":-1,"c":""}],"trees":[]}`} {
		if err := os.WriteFile(filepath.Join(dir, StateFile), []byte(bad), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := (FileStore{Dir: dir}).Load(); err == nil {
			t.Fatal(bad)
		}
	}
}

type retSetup struct {
	*setup
	store *MemoryStore
}

// retained is the module with retention over st, restored and passed once,
// as the host starts it.
func retained(t *testing.T, store *MemoryStore, now *int64, st *memStorage, noDelete bool) *retSetup {
	t.Helper()
	host := newFakeHost()
	m := mount(t, host, &RetentionConfig{KeepMs: day, StateDir: "/unused"}, store, func() int64 { return *now })
	if st == nil {
		st = newMemStorage()
	}
	e := newEngine(m.Module, st)
	if noDelete {
		e.seam = st
	}
	v := vectors(t)
	if _, err := m.LS.Restore(context.Background(), st.unspent(v.Topic, true), e.seam); err != nil {
		t.Fatal(err)
	}
	m.LS.Retain(context.Background())
	return &retSetup{setup: &setup{host: host, mod: m, st: st, e: e}, store: store}
}

func (s *retSetup) submitAll(t *testing.T, names ...string) {
	for _, n := range names {
		s.submit(t, n)
	}
}

func TestRetentionDropsProvenIntervalsOldestFirst(t *testing.T) {
	v := vectors(t)
	store := &MemoryStore{}
	now := anchorTime(t, "anchor-interval-1") + day - 1
	s := retained(t, store, &now, nil, false)
	l := s.mod.LS
	ctx := context.Background()
	s.submitAll(t, "funding-tree", "carrier-seq-1", "carrier-seq-2", "carrier-seq-3", "anchor-interval-1")

	// Not old enough: nothing.
	if p := l.Retain(ctx); p != (RetentionPass{}) || l.Counts().Batches != 3 || store.Saved != 0 {
		t.Fatal(p)
	}
	// Interval 1 (seqs 1 and 2) is old enough and complete; seq 3 has no
	// anchor yet, so it is unproven and stays.
	now = anchorTime(t, "anchor-interval-2") + day
	if p := l.Retain(ctx); p != (RetentionPass{Intervals: 1, Carriers: 2, Deleted: 2}) {
		t.Fatal(p)
	}
	if l.Counts().Batches != 1 || len(mustAsk(t, l, map[string]any{"stream": v.Stream, "producer": v.IdentityKey, "seq": 1})) != 0 {
		t.Fatal("seq 1 answered")
	}
	if got := mustAsk(t, l, map[string]any{"stream": v.Stream, "producer": v.IdentityKey, "seq": 3}); len(got) != 1 || got[0].Txid != v.txid(t, "carrier-seq-3") {
		t.Fatal(got)
	}
	if status(t, l, v.txid(t, "carrier-seq-3")) != BatchLinked {
		t.Fatal("its prev is the dropped seq 2")
	}
	if s.st.has(v.Topic, v.txid(t, "carrier-seq-1"), 0) != nil || !s.st.has(v.Topic, v.txid(t, "funding-tree"), 0).spent {
		t.Fatal("the carrier left storage, its funding output did not")
	}
	if s.host.count("blogs_retired_total", nil) != 2 {
		t.Fatal("retired")
	}
	c2 := hashOrder(v.txid(t, "carrier-seq-2"))
	if want := []Mark{{Producer: v.IdentityKey, Stream: v.Stream, Interval: 1, Seq: 2, C: hex.EncodeToString(c2[:])}}; !reflect.DeepEqual(store.State.Marks, want) {
		t.Fatal(store.State.Marks)
	}
	r := l.Report(v.Stream)[0]
	if len(r.Holes) != 0 || !reflect.DeepEqual(r.Intervals, []IntervalReport{{Interval: 1, First: 1, Count: 2, Txid: v.txid(t, "anchor-interval-1"), Status: IntervalComplete, Missing: []uint64{}, Retired: true}}) {
		t.Fatalf("%+v", r)
	}
	if c := l.Counts(); c.Missing != 0 || c.Retired != 2 {
		t.Fatal(c)
	}

	// Interval 2 arrives and is dropped at the next pass; the anchors and
	// the tree are still answered.
	s.submit(t, "anchor-interval-2")
	if p := l.Retain(ctx); p != (RetentionPass{Intervals: 1, Carriers: 1, Deleted: 1}) || l.Counts().Batches != 0 {
		t.Fatal(p)
	}
	r = l.Report(v.Stream)[0]
	if len(r.Intervals) != 2 || !r.Intervals[0].Retired || !r.Intervals[1].Retired || r.Highest != 3 {
		t.Fatalf("%+v", r)
	}
	for _, k := range []int{1, 2} {
		if a, _ := s.e.lookup(t, map[string]any{"stream": v.Stream, "producer": v.IdentityKey, "interval": k}); len(a) != 1 {
			t.Fatal("interval still answered", k)
		}
	}
	tree := map[string]any{"stream": v.Stream, "tree": v.txid(t, "funding-tree")}
	if a, _ := s.e.lookup(t, tree); len(a) != 1 || a[0].vout != 0 {
		t.Fatal(a)
	}
	for _, o := range s.st.ofTx(v.txid(t, "funding-tree")) {
		if !o.spent {
			t.Fatal("funding outputs stay, spent")
		}
	}

	// A dropped carrier offered again is a duplicate to the engine.
	s.submit(t, "carrier-seq-1")
	if l.Counts().Batches != 0 || s.st.has(v.Topic, v.txid(t, "carrier-seq-1"), 0) != nil {
		t.Fatal("a dropped carrier came back")
	}

	// A restart: restore from the engine's storage and the state, and the
	// index reads the same, the tree still answered.
	live := l.Report("")
	again := retained(t, store, &now, s.st, false)
	if got := again.mod.LS.Report(""); !reflect.DeepEqual(got, live) || again.mod.LS.Counts().Batches != 0 {
		t.Fatalf("%+v\n%+v", got, live)
	}
	if a, _ := again.e.lookup(t, tree); len(a) != 1 || a[0].vout != 0 {
		t.Fatal(a)
	}
	if again.mod.LS.Counts().Trees != l.Counts().Trees {
		t.Fatal("trees")
	}
}

func TestWithoutRetentionEverythingStays(t *testing.T) {
	s := newSetup(t, true)
	for _, n := range []string{"funding-tree", "carrier-seq-1", "carrier-seq-2", "carrier-seq-3", "anchor-interval-1", "anchor-interval-2"} {
		s.submit(t, n)
	}
	if p := s.mod.LS.Retain(context.Background()); p != (RetentionPass{}) || s.mod.LS.Counts().Batches != 3 || s.host.count("blogs_retired_total", nil) != 0 {
		t.Fatal(p)
	}
}

func TestStoreThatCannotDelete(t *testing.T) {
	v := vectors(t)
	store := &MemoryStore{}
	now := int64(maxSafe)
	st := newMemStorage()
	first := retained(t, store, &now, st, true)
	first.submitAll(t, "funding-tree", "carrier-seq-1", "carrier-seq-2", "carrier-seq-3", "anchor-interval-1", "anchor-interval-2")
	if p := first.mod.LS.Retain(context.Background()); p != (RetentionPass{Intervals: 2, Carriers: 3}) {
		t.Fatal(p)
	}
	if !first.host.logged("no DeleteOutput") || st.has(v.Topic, v.txid(t, "carrier-seq-1"), 0) == nil {
		t.Fatal("still in storage, and said so")
	}
	// The restart with a store that deletes: restore does not index them,
	// and the pass after it deletes them.
	again := retained(t, store, &now, st, false)
	if again.mod.LS.Counts().Batches != 0 || !reflect.DeepEqual(again.mod.LS.Report(""), first.mod.LS.Report("")) {
		t.Fatal("restore indexed a dropped carrier")
	}
	for _, n := range []string{"carrier-seq-1", "carrier-seq-2", "carrier-seq-3"} {
		if st.has(v.Topic, v.txid(t, n), 0) != nil {
			t.Fatal(n)
		}
	}
}

func TestStateWrittenBeforeStorage(t *testing.T) {
	v := vectors(t)
	store := &MemoryStore{Fail: errors.New("disk full")}
	now := int64(maxSafe)
	s := retained(t, store, &now, nil, false)
	s.submitAll(t, "funding-tree", "carrier-seq-1", "carrier-seq-2", "carrier-seq-3", "anchor-interval-1")
	if p := s.mod.LS.Retain(context.Background()); p != (RetentionPass{Intervals: 1, Carriers: 2}) {
		t.Fatal(p)
	}
	if !s.host.logged("could not write the retention state") || s.st.has(v.Topic, v.txid(t, "carrier-seq-1"), 0) == nil {
		t.Fatal("deleted before the state was written")
	}
	store.mu.Lock()
	store.Fail = nil
	store.mu.Unlock()
	if p := s.mod.LS.Retain(context.Background()); p != (RetentionPass{Deleted: 2}) || store.State.Marks[0].Seq != 2 {
		t.Fatal(p)
	}
	if s.st.has(v.Topic, v.txid(t, "carrier-seq-1"), 0) != nil {
		t.Fatal("not deleted")
	}
}

func TestPolicyFromTheEnvironment(t *testing.T) {
	v := vectors(t)
	dir := t.TempDir()
	e := map[string]string{"BLOGS_STREAMS": v.Stream + "=open", "OVERLAY_TOPICS": v.Topic, "BLOGS_KEEP_DAYS": "0.5", "BLOGS_STATE_DIR": dir}
	if _, err := New(newFakeHost(), env(e)); err == nil || !strings.Contains(err.Error(), "floor") {
		t.Fatal(err)
	}
	e["BLOGS_KEEP_DAYS"] = "3"
	host := newFakeHost()
	if _, err := New(host, env(e)); err != nil {
		t.Fatal(err)
	}
	if got := host.extra("blogs module mounted", "retention"); got != "3 days" {
		t.Fatal(got)
	}
	if err := os.WriteFile(filepath.Join(dir, StateFile), []byte("not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := New(newFakeHost(), env(e)); err == nil {
		t.Fatal("a state file that does not parse")
	}
	if raw, _ := os.ReadFile(filepath.Join(dir, StateFile)); string(raw) != "not json" {
		t.Fatal("the file was touched")
	}
}
