package module

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/overlay"
	"github.com/bsv-blockchain/go-sdk/transaction"

	bmodule "github.com/lightwebinc/bcommon/module"
)

// The module inside an engine's ordinary admission path over an in-memory
// store, the twin of engine.test.ts: what the engine records for a
// refusal, the out-of-order anchor, a delivery in scrambled order,
// retraction, and a restore from the engine's own storage.

type setup struct {
	host *fakeHost
	mod  *Mounted
	st   *memStorage
	e    *engine
}

func newSetup(t *testing.T, restore bool) *setup {
	t.Helper()
	host := newFakeHost()
	m := mount(t, host, nil, nil, nil)
	st := newMemStorage()
	e := newEngine(m.Module, st)
	if restore {
		if _, err := m.LS.Restore(context.Background(), nil, e.seam); err != nil {
			t.Fatal(err)
		}
	}
	return &setup{host: host, mod: m, st: st, e: e}
}

func (s *setup) submit(t *testing.T, name string) []uint32 {
	t.Helper()
	v := vectors(t)
	got, err := s.e.submit(t, v.byName(t, name).beef(t), v.Topic)
	var u *Unrecorded
	if errors.As(err, &u) {
		return []uint32{}
	}
	if err != nil {
		t.Fatal(err)
	}
	return got
}

// naive wraps a manager so that every raised refusal is returned as empty
// instructions, as a manager that does not raise would.
type naive struct {
	bmodule.TopicManager
}

func (n naive) IdentifyAdmissibleOutputs(ctx context.Context, beef *transaction.Beef, txid *chainhash.Hash, prev []uint32) (overlay.AdmittanceInstructions, error) {
	ai, err := n.TopicManager.IdentifyAdmissibleOutputs(ctx, beef, txid, prev)
	var u *Unrecorded
	if errors.As(err, &u) {
		return overlay.AdmittanceInstructions{OutputsToAdmit: []uint32{}, CoinsToRetain: append([]uint32{}, prev...)}, nil
	}
	return ai, err
}

func fundingOut(t *testing.T, s *setup, vout uint32) *row {
	v := vectors(t)
	return s.st.has(v.Topic, v.txid(t, "funding-tree"), vout)
}

func TestEngineRecordsAReturnedRefusal(t *testing.T) {
	v := vectors(t)
	s := newSetup(t, true)
	s.e.mod.Topics[v.Topic] = naive{s.e.mod.Topics[v.Topic]}
	if got := s.submit(t, "funding-tree"); !reflect.DeepEqual(got, []uint32{0, 1, 2, 3, 4}) {
		t.Fatal(got)
	}
	// Interval 2 before interval 1: its predecessor is not held, its fee
	// input spends held funding output 4.
	if got := s.submit(t, "anchor-interval-2"); len(got) != 0 {
		t.Fatal(got)
	}
	if !s.st.applied[v.Topic+"|"+v.txid(t, "anchor-interval-2")] || !fundingOut(t, s, 4).spent {
		t.Fatal("recorded as applied, the held output it spends marked spent")
	}
	if got := s.submit(t, "anchor-interval-1"); !reflect.DeepEqual(got, []uint32{0}) {
		t.Fatal(got)
	}
	before := s.e.calls
	if got := s.submit(t, "anchor-interval-2"); len(got) != 0 || s.e.calls != before {
		t.Fatal("a duplicate: the topic manager is not asked")
	}
	if a, _ := s.e.lookup(t, map[string]any{"stream": v.Stream, "producer": v.IdentityKey, "interval": 2}); len(a) != 0 {
		t.Fatal("interval 2 is lost to this host")
	}
}

func TestOutOfOrderAnchor(t *testing.T) {
	v := vectors(t)
	s := newSetup(t, true)
	s.submit(t, "funding-tree")
	if got := s.submit(t, "anchor-interval-2"); len(got) != 0 {
		t.Fatal(got)
	}
	if s.st.applied[v.Topic+"|"+v.txid(t, "anchor-interval-2")] || fundingOut(t, s, 4).spent {
		t.Fatal("recorded, or the held funding output touched")
	}
	if s.host.count("blogs_refused_total", map[string]string{"reason": "anchor-input", "recorded": "no"}) != 1 {
		t.Fatal("not counted")
	}
	if got := s.submit(t, "anchor-interval-1"); !reflect.DeepEqual(got, []uint32{0}) {
		t.Fatal(got)
	}
	if got := s.submit(t, "anchor-interval-2"); !reflect.DeepEqual(got, []uint32{0}) {
		t.Fatal("decided afresh and admitted")
	}
	a, _ := s.e.lookup(t, map[string]any{"stream": v.Stream, "producer": v.IdentityKey, "interval": 2})
	if !reflect.DeepEqual(txidsOf(a), []string{v.txid(t, "anchor-interval-2")}) || !fundingOut(t, s, 4).spent {
		t.Fatal(a)
	}
}

func TestCoinsFromTheIndexBeforeStorage(t *testing.T) {
	s := newSetup(t, false)
	s.submit(t, "funding-tree")
	if len(s.submit(t, "anchor-interval-2")) != 0 {
		t.Fatal("interval 2 first")
	}
	if !reflect.DeepEqual(s.submit(t, "anchor-interval-1"), []uint32{0}) || !reflect.DeepEqual(s.submit(t, "anchor-interval-2"), []uint32{0}) {
		t.Fatal("the same order without storage")
	}
}

func TestScrambledDeliveryRetractionRestore(t *testing.T) {
	v := vectors(t)
	s := newSetup(t, true)
	l := s.mod.LS
	p := v.IdentityKey
	q := func(k string, n int) map[string]any { return map[string]any{"stream": v.Stream, "producer": p, k: n} }
	answer := func(query map[string]any) []string {
		a, err := s.e.lookup(t, query)
		if err != nil {
			t.Fatal(err)
		}
		return txidsOf(a)
	}
	s.submit(t, "funding-tree")
	if !reflect.DeepEqual(s.submit(t, "carrier-seq-3"), []uint32{0}) || !reflect.DeepEqual(s.submit(t, "carrier-seq-1"), []uint32{0}) {
		t.Fatal("carriers")
	}
	if r := l.Report(v.Stream)[0]; !reflect.DeepEqual(r.Holes, [][2]uint64{{2, 2}}) {
		t.Fatal(r.Holes)
	}
	if status(t, l, v.txid(t, "carrier-seq-3")) != BatchGap {
		t.Fatal("gap")
	}
	s.submit(t, "anchor-interval-1")
	if r := l.Report(v.Stream)[0]; r.Intervals[0].Status != IntervalIncomplete || !reflect.DeepEqual(r.Intervals[0].Missing, []uint64{2}) {
		t.Fatalf("%+v", r.Intervals)
	}
	s.submit(t, "carrier-seq-2")
	s.submit(t, "anchor-interval-2")
	r := l.Report(v.Stream)[0]
	if len(r.Holes) != 0 || status(t, l, v.txid(t, "carrier-seq-3")) != BatchLinked || r.Intervals[0].Status != IntervalComplete || r.Intervals[1].Status != IntervalComplete {
		t.Fatalf("%+v", r)
	}
	for i, n := range []string{"carrier-seq-1", "carrier-seq-2", "carrier-seq-3"} {
		if got := answer(q("seq", i+1)); !reflect.DeepEqual(got, []string{v.txid(t, n)}) {
			t.Fatal(n, got)
		}
	}
	if got := answer(q("interval", 1)); !reflect.DeepEqual(got, []string{v.txid(t, "anchor-interval-1")}) {
		t.Fatal("a spent token is still answered")
	}
	// The tree class: every funding output of the tree is spent by now, and
	// the host still answers it, with the proof it was admitted with.
	for _, o := range s.st.ofTx(v.txid(t, "funding-tree")) {
		if !o.spent {
			t.Fatal("every funding output is retained, spent")
		}
	}
	tree, _ := s.e.lookup(t, map[string]any{"stream": v.Stream, "tree": v.txid(t, "funding-tree")})
	if !reflect.DeepEqual(tree, []hydrated{{v.txid(t, "funding-tree"), true, 0}}) {
		t.Fatal(tree)
	}
	if a, _ := s.e.lookup(t, map[string]any{"stream": v.Stream, "tree": v.txid(t, "anchor-interval-1")}); len(a) != 0 {
		t.Fatal("an anchor token is not a tree")
	}
	// The sweep retracts seq 1 and 2: nothing answers them, their receipts
	// still complete interval 1.
	if !reflect.DeepEqual(s.submit(t, "sweep"), []uint32{0}) {
		t.Fatal("sweep")
	}
	r = l.Report(v.Stream)[0]
	if !reflect.DeepEqual(r.Retracted, []uint64{1, 2}) || len(answer(q("seq", 1))) != 0 || !reflect.DeepEqual(answer(q("seq", 3)), []string{v.txid(t, "carrier-seq-3")}) {
		t.Fatalf("%+v", r)
	}
	if r.Intervals[0].Status != IntervalComplete || r.Intervals[1].Status != IntervalComplete {
		t.Fatal("receipts")
	}

	live := l.Report("")
	rows := s.st.unspent(v.Topic, true)
	reversed := make([]bmodule.Output, len(rows))
	for i := range rows {
		reversed[len(rows)-1-i] = rows[i]
	}
	// A host that hands the rows without their BEEF: each is read again.
	bare := s.st.unspent(v.Topic, false)
	for _, order := range [][]bmodule.Output{rows, reversed, bare} {
		again := mount(t, newFakeHost(), nil, nil, nil).LS
		n, err := again.Restore(context.Background(), order, s.e.seam)
		if err != nil || n == 0 {
			t.Fatal(n, err)
		}
		if got := again.Report(""); !reflect.DeepEqual(got, live) {
			t.Fatalf("restore reaches other statuses:\n%+v\n%+v", got, live)
		}
		for _, c := range []map[string]any{q("seq", 1), q("seq", 2), q("seq", 3), q("interval", 1), q("interval", 2),
			{"stream": v.Stream, "tree": v.txid(t, "funding-tree")}, {"stream": v.Stream, "tree": v.txid(t, "sweep")}} {
			a, b := mustAsk(t, again, c), mustAsk(t, l, c)
			if !reflect.DeepEqual(a, b) {
				t.Fatal(c, a, b)
			}
		}
		for _, name := range []string{"funding-tree", "sweep"} {
			if a := mustAsk(t, again, map[string]any{"stream": v.Stream, "tree": v.txid(t, name)}); !reflect.DeepEqual(a, []Formula{{Txid: v.txid(t, name), Vout: 0}}) {
				t.Fatal(name, a)
			}
		}
		if again.Counts().Trees != l.Counts().Trees {
			t.Fatal("tree count")
		}
	}
}
