package produce_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/lightwebinc/bcommon/nodeapi"

	"github.com/lightwebinc/blogs/internal/state"
)

func (r *rig) state() *state.Stream {
	r.t.Helper()
	st, err := state.Load(r.home, r.stream)
	if err != nil {
		r.t.Fatal(err)
	}
	return st
}

// unminedTree leaves the stream with the record of a batch tree the chain
// took and has not mined: a publish whose tree does not mine within the
// wait fails, and the failure does not drop the record.
func (r *rig) unminedTree(wait time.Duration) string {
	r.t.Helper()
	r.chain.SetHold(true)
	o := r.options()
	o.Wait = wait
	e := r.engine(o)
	ctx := context.Background()
	if err := e.Start(ctx); err != nil {
		r.t.Fatal(err)
	}
	if err := e.Publish(ctx, lines(1, 0)); err == nil || !strings.Contains(err.Error(), "waiting for a proof") {
		r.t.Fatalf("a publish whose tree does not mine: %v", err)
	}
	st := r.state()
	if len(st.PendingTrees) != 1 || st.PendingTrees[0].Kind != "batch" || st.Tree != nil || st.Seq != 0 {
		r.t.Fatalf("after the failed mint: records %+v, tree %+v, seq %d", st.PendingTrees, st.Tree, st.Seq)
	}
	if r.chain.Waiting() != 1 {
		r.t.Fatalf("%d transaction(s) waiting, want the tree", r.chain.Waiting())
	}
	return st.PendingTrees[0].Tree.Txid
}

// A tree on the chain and not yet mined is waited for at the start, for no
// longer than the wait for one proof. A tree still unmined then keeps its
// record, the start goes on, and a later start adopts the tree.
func TestTheStartWaitsForAnUnminedTreeNoLongerThanTheProofWait(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	ctx := context.Background()
	id := r.unminedTree(150 * time.Millisecond)
	r.notes.Reset()

	o := r.options()
	o.Wait = 150 * time.Millisecond
	e := r.engine(o)
	began := time.Now()
	if err := e.Start(ctx); err != nil {
		t.Fatalf("a start with an unmined tree: %v", err)
	}
	if d := time.Since(began); d < 100*time.Millisecond || d > 5*time.Second {
		t.Fatalf("the start waited %s", d)
	}
	for _, has := range []string{"waited for, 150ms at most", "funding tree " + id + ": what became of it could not be settled now", "this command goes on without the tree"} {
		if !strings.Contains(r.notes.String(), has) {
			t.Fatalf("no %q in:\n%s", has, r.notes)
		}
	}
	if st := r.state(); len(st.PendingTrees) != 1 || st.Tree != nil {
		t.Fatalf("after the bounded wait: records %+v, tree %+v", st.PendingTrees, st.Tree)
	}

	// An interrupt ends the wait and the start, and keeps the record.
	o.Wait = time.Minute
	e = r.engine(o)
	ictx, cancel := context.WithCancel(ctx)
	time.AfterFunc(50*time.Millisecond, cancel)
	began = time.Now()
	err := e.Start(ictx)
	if err == nil || !strings.Contains(err.Error(), "interrupted while asking what became of it") || time.Since(began) > 5*time.Second {
		t.Fatalf("an interrupted start: %v after %s", err, time.Since(began))
	}
	if st := r.state(); len(st.PendingTrees) != 1 {
		t.Fatalf("after the interrupt: records %+v", st.PendingTrees)
	}

	// Mined: the next start adopts it, and the publish spends from it.
	r.chain.SetHold(false)
	r.chain.Mine()
	e = r.engine(r.options())
	if err := e.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := e.Publish(ctx, lines(1, 0)); err != nil {
		t.Fatalf("publish: %v\n%s", err, r.notes)
	}
	if st := r.state(); st.Tree == nil || st.Tree.Txid != id || len(st.Trees) != 1 || len(st.PendingTrees) != 0 || st.Seq != 1 {
		t.Fatalf("after the tree mined: tree %+v, %d trees, records %+v, seq %d", st.Tree, len(st.Trees), st.PendingTrees, st.Seq)
	}
}

// A node that cannot answer decides nothing: the record stays and the
// recovery returns no error.
func TestANodeThatCannotAnswerKeepsTheRecord(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	ctx := context.Background()
	id := r.unminedTree(100 * time.Millisecond)
	broken := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "broken", http.StatusInternalServerError)
	}))
	t.Cleanup(broken.Close)
	e := r.engine(r.options())
	e.Legs.Asset.Base = broken.URL
	r.notes.Reset()
	if err := e.RecoverTrees(ctx); err != nil {
		t.Fatalf("recovery against a node that cannot answer: %v", err)
	}
	if !strings.Contains(r.notes.String(), "funding tree "+id+": what became of it could not be settled now") {
		t.Fatalf("notes:\n%s", r.notes)
	}
	if st := r.state(); len(st.PendingTrees) != 1 || st.PendingTrees[0].ReturnedOnce {
		t.Fatalf("records %+v", st.PendingTrees)
	}
}

// A tree the lists already hold is never asked about, which would adopt it
// a second time: its record is dropped.
func TestARecordOfAnAdoptedTreeIsDroppedUnasked(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	ctx := context.Background()
	e := r.engine(r.options())
	if err := e.Start(ctx); err != nil {
		t.Fatal(err)
	}
	for i := range 5 {
		if err := e.Publish(ctx, lines(1, i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.Close(ctx); err != nil {
		t.Fatal(err)
	}
	st := r.state()
	if len(st.Trees) != 2 || st.Tree.Txid != st.Trees[1].Txid {
		t.Fatalf("%d batch trees", len(st.Trees))
	}
	old := st.Trees[0]
	st.KeepPendingTree(state.PendingTree{Kind: "batch", Tree: old})
	if err := st.Save(); err != nil {
		t.Fatal(err)
	}
	e = r.engine(r.options())
	e.Legs.Asset = &nodeapi.Asset{Base: "http://127.0.0.1:1"}
	if err := e.RecoverTrees(ctx); err != nil {
		t.Fatal(err)
	}
	back := r.state()
	if back.PendingTree(old.Txid) != nil || back.Tree.Txid != st.Tree.Txid || len(back.Trees) != 2 {
		t.Fatalf("after recovery: record %+v, tree %s, %d trees", back.PendingTree(old.Txid), back.Tree.Txid, len(back.Trees))
	}
}

// A record that cannot be saved aborts the mint: nothing reaches the chain
// and the coin stays in the pool.
func TestATreeWhoseRecordCannotBeSavedIsNotSent(t *testing.T) {
	t.Parallel()
	if os.Geteuid() == 0 {
		t.Skip("root writes to a read-only directory")
	}
	r := newRig(t)
	ctx := context.Background()
	e := r.engine(r.options())
	if err := e.Start(ctx); err != nil {
		t.Fatal(err)
	}
	coins, sent := r.wallet.Pool.Count(), r.chain.Sent
	dir := state.Dir(r.home, r.stream)
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	err := e.Publish(ctx, lines(1, 0))
	if cerr := os.Chmod(dir, 0o700); cerr != nil {
		t.Fatal(cerr)
	}
	if err == nil || !strings.Contains(err.Error(), "recording the tree before it is sent") {
		t.Fatalf("a mint whose record cannot be saved: %v", err)
	}
	if r.wallet.Pool.Count() != coins || r.chain.Sent != sent {
		t.Fatalf("the pool holds %d coin(s), was %d; the chain took %d transaction(s)", r.wallet.Pool.Count(), coins, r.chain.Sent-sent)
	}
	if st := r.state(); len(st.PendingTrees) != 0 {
		t.Fatalf("records %+v", st.PendingTrees)
	}
	if err := e.Publish(ctx, lines(1, 0)); err != nil {
		t.Fatalf("the publish after: %v", err)
	}
}
