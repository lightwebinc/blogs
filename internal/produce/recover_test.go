package produce_test

import (
	"context"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bcommon/nodeapi"

	"github.com/lightwebinc/blogs/internal/produce"
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

// twoTreesOnChain leaves the stream with a current batch tree of six
// outputs, three of them spent, and the records of two more batch trees
// that are on the chain and mined, each signed by a run that stopped before
// it adopted the tree: the home two crashed runs leave. It returns the two
// in the order they were signed, and the outputs spent so far.
func (r *rig) twoTreesOnChain(o produce.Options) (first, second string, spent map[string]bool) {
	r.t.Helper()
	spent = map[string]bool{}
	// The chain keeps every batch tree after the first unmined.
	r.chain.SetHoldIf(func(tx *transaction.Transaction) bool {
		st, err := state.Load(r.home, r.stream)
		if err != nil || st.Tree == nil {
			return false
		}
		p := st.PendingTree(tx.TxID().String())
		return p != nil && p.Kind == "batch"
	})
	// Each run mints the next tree ahead and stops before it mines.
	for n, batches := range []int{2, 1} {
		ro := o
		ro.Wait = 150 * time.Millisecond
		ctx, cancel := context.WithCancel(context.Background())
		e := r.engine(ro)
		if err := e.Start(ctx); err != nil {
			r.t.Fatalf("run %d: %v\n%s", n, err, r.notes)
		}
		for range batches {
			r.publish(e, spent)
		}
		for end := time.Now().Add(10 * time.Second); r.chain.Waiting() != n+1; time.Sleep(time.Millisecond) {
			if time.Now().After(end) {
				r.t.Fatalf("run %d: %d tree(s) waiting on the chain\n%s", n, r.chain.Waiting(), r.notes)
			}
		}
		cancel()
	}
	st := r.state()
	if len(st.PendingTrees) != 2 || len(st.Trees) != 1 || st.Tree.Remaining() != 3 || len(st.Ahead) != 0 {
		r.t.Fatalf("after two stopped runs: records %v, %d trees, %d left, ahead %d", records(st), len(st.Trees), st.Tree.Remaining(), len(st.Ahead))
	}
	r.chain.SetHoldIf(nil)
	if n := r.chain.Mine(); n != 2 {
		r.t.Fatalf("mined %d", n)
	}
	r.notes.Reset()
	return st.PendingTrees[0].Tree.Txid, st.PendingTrees[1].Tree.Txid, spent
}

// records and aheads are the txids of the trees signed and not adopted, and
// of the trees minted ahead.
func records(st *state.Stream) []string {
	var out []string
	for _, p := range st.PendingTrees {
		out = append(out, p.Tree.Txid)
	}
	return out
}

func aheads(st *state.Stream) []string {
	var out []string
	for _, t := range st.Ahead {
		out = append(out, t.Txid)
	}
	return out
}

// publish publishes one batch and records the funding output its carrier
// spent, which no other carrier may spend.
func (r *rig) publish(e *produce.Engine, spent map[string]bool) {
	r.t.Helper()
	if err := e.Publish(context.Background(), lines(1, len(spent))); err != nil {
		r.t.Fatalf("publish: %v\n%s", err, r.notes)
	}
	cur := e.St.Tree
	out := fmt.Sprintf("%s.%d", cur.Txid, cur.Next-1)
	if spent[out] {
		r.t.Fatalf("funding output %s is spent a second time\n%s", out, r.notes)
	}
	spent[out] = true
}

// trees checks the batch trees the state adopted, in order, each once, and
// how many outputs of each are spent.
func (r *rig) trees(want []string, next []uint32) {
	r.t.Helper()
	st := r.state()
	if len(st.Trees) != len(want) {
		r.t.Fatalf("%d batch trees, want %d\n%s", len(st.Trees), len(want), r.notes)
	}
	for i, tr := range st.Trees {
		if tr.Txid != want[i] || tr.Next != next[i] {
			r.t.Fatalf("batch tree %d is %s with %d spent, want %s with %d\n%s", i, tr.Txid, tr.Next, want[i], next[i], r.notes)
		}
	}
	if st.Tree.Txid != want[len(want)-1] || st.Tree.Next != next[len(next)-1] {
		r.t.Fatalf("the current tree is %s with %d spent", st.Tree.Txid, st.Tree.Next)
	}
}

func twoTreeOptions(r *rig) produce.Options {
	o := r.options()
	o.TreeCount, o.Ahead = 6, 4
	return o
}

// Two recovered trees are held, in the order they were signed, and one
// command spends the current tree to its end, then the first held tree,
// then the second: each adopted once, at its switch, from output 0, and no
// funding output spent twice. Nothing is minted while a tree is held.
func TestTwoRecoveredTreesAreHeldAndSpentInOrderWithinOneCommand(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	ctx := context.Background()
	o := twoTreeOptions(r)
	first, second, spent := r.twoTreesOnChain(o)
	current := r.state().Tree.Txid

	e := r.engine(o)
	if err := e.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(r.notes.String(), "is recovered and waits for the switch"); n != 2 {
		t.Fatalf("%d tree(s) held:\n%s", n, r.notes)
	}
	st := r.state()
	if len(st.Ahead) != 2 || st.Ahead[0].Txid != first || st.Ahead[1].Txid != second || len(st.PendingTrees) != 2 || st.Tree.Txid != current {
		t.Fatalf("after the start: ahead %v, records %v", aheads(st), records(st))
	}
	sent := r.chain.Sent
	for range 3 {
		r.publish(e, spent)
	}
	r.trees([]string{current}, []uint32{6})
	// The switch to the first held tree: the second keeps its place.
	r.publish(e, spent)
	r.trees([]string{current, first}, []uint32{6, 1})
	st = r.state()
	if len(st.PendingTrees) != 1 || st.PendingTrees[0].Tree.Txid != second || len(st.Ahead) != 1 || st.Ahead[0].Txid != second {
		t.Fatalf("after the first switch: records %v, ahead %v", records(st), aheads(st))
	}
	for range 5 {
		r.publish(e, spent)
	}
	r.trees([]string{current, first}, []uint32{6, 6})
	// The switch to the second.
	r.publish(e, spent)
	r.trees([]string{current, first, second}, []uint32{6, 6, 1})
	if err := e.Close(ctx); err != nil {
		t.Fatal(err)
	}
	st = r.state()
	if len(st.PendingTrees) != 0 || len(st.Ahead) != 0 || st.Seq != 13 || len(spent) != 13 {
		t.Fatalf("at the end: records %v, ahead %v, seq %d, %d outputs spent", records(st), aheads(st), st.Seq, len(spent))
	}
	if r.chain.Sent != sent {
		t.Fatalf("%d transaction(s) were sent while trees were held", r.chain.Sent-sent)
	}
	producer, _ := hex.DecodeString(st.Identity)
	for seq := uint64(1); seq <= st.Seq; seq++ {
		if !r.host.Holds(producer, r.stream, seq) {
			t.Fatalf("the host does not hold seq %d", seq)
		}
	}
	for _, id := range []string{first, second} {
		if !r.host.HoldsTree(id) {
			t.Fatalf("the host does not hold the tree %s", id)
		}
	}
}

// The same across commands: one command switches to the first held tree
// and ends, and the next holds the second again, and only the second, and
// goes on from where the first tree was spent to.
func TestTwoRecoveredTreesAreHeldAndSpentInOrderAcrossCommands(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	ctx := context.Background()
	o := twoTreeOptions(r)
	first, second, spent := r.twoTreesOnChain(o)
	current := r.state().Tree.Txid

	// The first command: to the end of the current tree, and one output of
	// the first held tree.
	e := r.engine(o)
	if err := e.Start(ctx); err != nil {
		t.Fatal(err)
	}
	for range 4 {
		r.publish(e, spent)
	}
	if err := e.Close(ctx); err != nil {
		t.Fatal(err)
	}
	r.trees([]string{current, first}, []uint32{6, 1})
	st := r.state()
	if len(st.PendingTrees) != 1 || st.PendingTrees[0].Tree.Txid != second || len(st.Ahead) != 1 || st.Ahead[0].Txid != second {
		t.Fatalf("after the first command: records %v, ahead %v", records(st), aheads(st))
	}

	// The second: the tree in use is not held again, the second one is.
	r.notes.Reset()
	e = r.engine(o)
	if err := e.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(r.notes.String(), first) || !strings.Contains(r.notes.String(), "batch funding tree "+second+", minted ahead by an earlier run, waits for the switch") {
		t.Fatalf("the second command's start:\n%s", r.notes)
	}
	r.trees([]string{current, first}, []uint32{6, 1})
	for range 2 {
		r.publish(e, spent)
	}
	if err := e.Close(ctx); err != nil {
		t.Fatal(err)
	}
	r.trees([]string{current, first}, []uint32{6, 3})

	// The third: to the end of the first tree and into the second.
	e = r.engine(o)
	if err := e.Start(ctx); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		r.publish(e, spent)
	}
	r.trees([]string{current, first}, []uint32{6, 6})
	r.publish(e, spent)
	if err := e.Close(ctx); err != nil {
		t.Fatal(err)
	}
	r.trees([]string{current, first, second}, []uint32{6, 6, 1})
	if st = r.state(); len(st.PendingTrees) != 0 || len(st.Ahead) != 0 || st.Seq != 13 || len(spent) != 13 {
		t.Fatalf("at the end: records %v, ahead %v, seq %d, %d outputs spent", records(st), aheads(st), st.Seq, len(spent))
	}

	// The fourth holds nothing and spends the next output of the second.
	r.notes.Reset()
	e = r.engine(o)
	if err := e.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(r.notes.String(), "funding tree") {
		t.Fatalf("a start with nothing to settle:\n%s", r.notes)
	}
	r.publish(e, spent)
	r.trees([]string{current, first, second}, []uint32{6, 6, 2})
}
