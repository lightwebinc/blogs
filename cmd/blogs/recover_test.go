package main

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bcommon/bwallet"

	"github.com/lightwebinc/blogs/internal/produce"
	"github.com/lightwebinc/blogs/internal/state"
)

// A run that stops is a home as its files stood at that moment. These
// tests copy the home at the moment a funding tree the state recorded
// reaches the settlement leg, before anything else, and go on in the copy:
// the home the next command finds.

// stopper refuses, from the first funding tree the stream recorded on, what
// a run sends to the chain, and copies the home as it stood when that tree
// arrived.
type stopper struct {
	mu     sync.Mutex
	armed  bool
	stream string
	from   string
	to     string
	sent   *transaction.Transaction
	failed error
}

// stopAtTree runs run, which stops at the first funding tree it signs: the
// home is copied as it stood when the tree reached the settlement leg, the
// harness is moved to the copy, and the tree is returned. The chain has
// not taken the tree; land sends it.
func (h *harness) stopAtTree(stream string, run func()) *transaction.Transaction {
	h.t.Helper()
	s := &stopper{armed: true, stream: stream, from: h.home, to: filepath.Join(h.t.TempDir(), "stopped")}
	h.chain.Refuse = func(tx *transaction.Transaction) string {
		s.mu.Lock()
		defer s.mu.Unlock()
		if !s.armed {
			return ""
		}
		if s.sent != nil {
			return "the run has stopped"
		}
		st, err := state.Load(s.from, s.stream)
		if err != nil || st.PendingTree(tx.TxID().String()) == nil {
			return ""
		}
		s.sent = tx
		s.failed = stoppedHome(s.from, s.to)
		return "the run has stopped"
	}
	run()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.armed = false
	if s.sent == nil || s.failed != nil {
		h.t.Fatalf("the run sent %v to the settlement leg: %v", s.sent, s.failed)
	}
	h.home = s.to
	h.env["BLOGS_HOME"] = s.to
	return s.sent
}

// land has the chain take a tree a stopped run had sent.
func (h *harness) land(tx *transaction.Transaction) {
	h.t.Helper()
	if err := h.chain.Send(tx); err != nil {
		h.t.Fatal(err)
	}
	if !h.chain.Mined(tx.TxID().String()) {
		h.t.Fatal("the tree did not mine")
	}
}

// stoppedHome copies a home a command is running in: the lock and the
// temporary files of a save under way are left out.
func stoppedHome(from, to string) error {
	return filepath.WalkDir(from, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		rel, _ := filepath.Rel(from, p)
		dst := filepath.Join(to, rel)
		if d.IsDir() {
			return os.MkdirAll(dst, 0o700)
		}
		if d.Name() == "lock" || strings.HasPrefix(d.Name(), ".") {
			return nil
		}
		b, err := os.ReadFile(p)
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		return os.WriteFile(dst, b, 0o600)
	})
}

func (h *harness) state(stream string) *state.Stream {
	h.t.Helper()
	st, err := state.Load(h.home, stream)
	if err != nil {
		h.t.Fatal(err)
	}
	return st
}

// poolCoins are the outpoints the home's pool holds.
func (h *harness) poolCoins() map[string]bool {
	h.t.Helper()
	w, err := bwallet.Open(h.home, produce.Profile)
	if err != nil {
		h.t.Fatal(err)
	}
	out := map[string]bool{}
	for _, o := range w.Pool.Outputs() {
		out[o.Outpoint()] = true
	}
	return out
}

// poolHoldsTx reports whether the pool holds an output of txid.
func (h *harness) poolHoldsTx(txid string) bool {
	h.t.Helper()
	for op := range h.poolCoins() {
		if strings.HasPrefix(op, txid) {
			return true
		}
	}
	return false
}

// stopped is a funded producer whose first produce stopped as its batch
// funding tree reached the settlement leg: the stream, the producer, the
// tree, and the outpoint of the coin the tree spends.
func (h *harness) stopped() (stream, producer string, tree *transaction.Transaction, coin string) {
	h.t.Helper()
	stream, producer = h.producer()
	tree = h.stopAtTree(stream, func() {
		h.want(h.run("a\n", append([]string{"produce", stream}, fast...)...), exitUsage, "batch funding tree")
	})
	st := h.state(stream)
	if len(st.PendingTrees) != 1 || st.PendingTrees[0].Kind != "batch" || st.PendingTrees[0].Tree.Txid != tree.TxID().String() ||
		st.PendingTrees[0].ReturnedOnce || st.Tree != nil {
		h.t.Fatalf("the stopped home's record: %+v", st.PendingTrees)
	}
	coin = st.PendingTrees[0].Coin.Outpoint()
	if h.poolCoins()[coin] {
		h.t.Fatalf("the stopped home's pool holds the coin %s the tree spends", coin)
	}
	return stream, producer, tree, coin
}

// The run stopped after the tree was recorded and before the chain took
// it. The coin goes back, and the record is kept for one more command and
// dropped by the second that finds nothing.
func TestATreeThatNeverReachedTheChainGivesItsCoinBack(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	stream, producer, tree, coin := h.stopped()
	id := tree.TxID().String()

	// A command that only reads asks nothing and changes nothing.
	r := h.must("", "doctor")
	if !strings.Contains(r.stdout, "batch tree "+id+" signed for coin "+coin+" and not adopted") {
		t.Fatalf("doctor:\n%s", r.stdout)
	}
	h.must("", "stream", "list")
	if st := h.state(stream); len(st.PendingTrees) != 1 || st.PendingTrees[0].ReturnedOnce || h.poolCoins()[coin] {
		t.Fatalf("a reading command settled the record: %+v", st.PendingTrees)
	}

	// The first command that spends: the coin is back, the record kept.
	r = h.must("", append([]string{"anchor", stream}, fast...)...)
	for _, has := range []string{"batch funding tree " + id + " was signed by a run that stopped before it adopted the tree",
		"never reached the chain: its fee coin " + coin + " is unspent and back in the pool", "its record is kept for one more command"} {
		if !strings.Contains(r.stderr, has) {
			t.Fatalf("no %q in:\n%s", has, r.stderr)
		}
	}
	if st := h.state(stream); len(st.PendingTrees) != 1 || !st.PendingTrees[0].ReturnedOnce || !h.poolCoins()[coin] {
		t.Fatalf("after the first answer: %+v, coin in the pool %t", st.PendingTrees, h.poolCoins()[coin])
	}
	// The second: the record is dropped, the coin stays.
	r = h.must("", append([]string{"anchor", stream}, fast...)...)
	if !strings.Contains(r.stderr, "the chain view still does not know it, so its record is dropped") {
		t.Fatalf("the second answer:\n%s", r.stderr)
	}
	if st := h.state(stream); len(st.PendingTrees) != 0 || !h.poolCoins()[coin] {
		t.Fatalf("after the second answer: %+v, coin in the pool %t", st.PendingTrees, h.poolCoins()[coin])
	}
	r = h.must("", append([]string{"anchor", stream}, fast...)...)
	if strings.Contains(r.stderr, id) {
		t.Fatalf("a dropped record was asked about:\n%s", r.stderr)
	}
	h.produce("a\n", stream)
	if r = h.must("", "read", stream, producer); r.stdout != "a\n" {
		t.Fatalf("read: %q", r.stdout)
	}
}

// The run stopped after the chain took the tree and before the state
// adopted it. The next command adopts the tree and takes its change.
func TestATreeOnTheChainIsAdoptedByTheNextCommand(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	stream, producer, tree, coin := h.stopped()
	id := tree.TxID().String()
	h.land(tree)
	r := h.produce("a\nb\n", stream)
	for _, has := range []string{"was signed by a run that stopped before it adopted the tree", "funding tree " + id + " is recovered", "funding tree published: admitted 4 output(s)"} {
		if !strings.Contains(r.stderr, has) {
			t.Fatalf("no %q in:\n%s", has, r.stderr)
		}
	}
	st := h.state(stream)
	if st.Tree == nil || st.Tree.Txid != id || len(st.Trees) != 1 || len(st.PendingTrees) != 0 || st.Tree.BumpHex == "" {
		t.Fatalf("the tree was not adopted: tree %+v, %d trees, records %+v", st.Tree, len(st.Trees), st.PendingTrees)
	}
	if h.poolCoins()[coin] || !h.poolHoldsTx(id) {
		t.Fatalf("the pool: spent coin held %t, the tree's change held %t", h.poolCoins()[coin], h.poolHoldsTx(id))
	}
	if !h.host.HoldsTree(id) {
		t.Fatal("the host does not hold the recovered tree")
	}
	if r = h.must("", "read", stream, producer); r.stdout != "a\nb\n" {
		t.Fatalf("read: %q", r.stdout)
	}
	h.must("", "verify-interval", stream, producer, "1")
}

// The tree reaches the node after a command found nothing and put the coin
// back: the kept record lets the next command adopt it and take the coin,
// now spent, out of the pool.
func TestATreeThatLandsLateIsStillAdopted(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	stream, producer, tree, coin := h.stopped()
	id := tree.TxID().String()
	h.must("", append([]string{"anchor", stream}, fast...)...)
	if !h.poolCoins()[coin] {
		t.Fatal("the coin did not go back")
	}
	h.land(tree)
	r := h.produce("a\n", stream)
	for _, has := range []string{"its fee coin " + coin + " is spent and is taken out of the pool", "funding tree " + id + " is recovered"} {
		if !strings.Contains(r.stderr, has) {
			t.Fatalf("no %q in:\n%s", has, r.stderr)
		}
	}
	st := h.state(stream)
	if st.Tree == nil || st.Tree.Txid != id || len(st.Trees) != 1 || len(st.PendingTrees) != 0 || h.poolCoins()[coin] {
		t.Fatalf("the late tree: tree %+v, records %+v, spent coin held %t", st.Tree, st.PendingTrees, h.poolCoins()[coin])
	}
	if r = h.must("", "read", stream, producer); r.stdout != "a\n" {
		t.Fatalf("read: %q", r.stdout)
	}
}

// The tree lands after two commands found nothing and dropped its record:
// the tree is lost to the stream. The command that next takes the coin is
// refused, and the refusal takes the spent coin out of the pool. The one
// after it drops the refused tree's record and goes on.
func TestATreeThatLandsAfterItsRecordIsDroppedCostsOneRefusedCommand(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	stream, producer, tree, coin := h.stopped()
	h.must("", append([]string{"anchor", stream}, fast...)...)
	h.must("", append([]string{"anchor", stream}, fast...)...)
	if st := h.state(stream); len(st.PendingTrees) != 0 || !h.poolCoins()[coin] {
		t.Fatalf("records %+v, coin held %t", st.PendingTrees, h.poolCoins()[coin])
	}
	h.land(tree)
	// The pool is left with that coin alone, so the next tree takes it.
	w, err := bwallet.Open(h.home, produce.Profile)
	if err != nil {
		t.Fatal(err)
	}
	var others []bwallet.Output
	for _, o := range w.Pool.Outputs() {
		if o.Outpoint() != coin {
			others = append(others, o)
			if _, err := w.Pool.Remove(o); err != nil {
				t.Fatal(err)
			}
		}
	}
	// A tree of another size, so it is not the landed transaction signed
	// again, which the chain would answer as one it already holds.
	refused := h.run("a\n", append(append([]string{"produce", stream}, fast...), "-tree-count", "5")...)
	h.want(refused, exitUsage, "batch funding tree")
	st := h.state(stream)
	if len(st.PendingTrees) != 1 || st.PendingTrees[0].Coin.Outpoint() != coin || st.Seq != 0 {
		t.Fatalf("after the refused tree: records %+v, seq %d", st.PendingTrees, st.Seq)
	}
	// The refusal names the tree that landed as the coin's spender, so the
	// coin is out of the pool already and is not handed out again.
	if !strings.Contains(refused.stderr, "its fee coin "+coin+" is spent by "+tree.TxID().String()+", so it is out of the pool and is not returned") || h.poolCoins()[coin] {
		t.Fatalf("the refused command: coin held %t\n%s", h.poolCoins()[coin], refused.stderr)
	}
	if w, err = bwallet.Open(h.home, produce.Profile); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Pool.Add(others...); err != nil {
		t.Fatal(err)
	}
	r := h.produce("a\n", stream)
	if !strings.Contains(r.stderr, "its fee coin "+coin+" is spent by "+tree.TxID().String()) {
		t.Fatalf("the command after:\n%s", r.stderr)
	}
	st = h.state(stream)
	if len(st.PendingTrees) != 0 || h.poolCoins()[coin] || st.Tree == nil || st.Tree.Txid == tree.TxID().String() {
		t.Fatalf("after the coin was found spent: records %+v, coin held %t, tree %+v", st.PendingTrees, h.poolCoins()[coin], st.Tree)
	}
	if r = h.must("", "read", stream, producer); r.stdout != "a\n" {
		t.Fatalf("read: %q", r.stdout)
	}
}

// A coin another transaction spent: the record is dropped at once.
func TestATreeWhoseCoinWasSpentElsewhereIsForgotten(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	stream, _, tree, coin := h.stopped()
	in := tree.Inputs[0]
	other := strings.Repeat("5e", 32)
	h.chain.SpendElsewhere(in.SourceTXID.String(), in.SourceTxOutIndex, other)
	r := h.must("", append([]string{"anchor", stream}, fast...)...)
	if !strings.Contains(r.stderr, "its fee coin "+coin+" is spent by "+other) {
		t.Fatalf("anchor:\n%s", r.stderr)
	}
	if st := h.state(stream); len(st.PendingTrees) != 0 || h.poolCoins()[coin] {
		t.Fatalf("after a coin spent elsewhere: %+v, coin held %t", st.PendingTrees, h.poolCoins()[coin])
	}
}

// A tree the node took and that then lost a double spend never mines,
// though the node goes on serving it. The next command drops its record at
// once, as for any coin another transaction spent, takes the coin out of
// the pool and adopts nothing; the stream goes on with a tree of its own.
func TestATreeThatLostADoubleSpendIsForgotten(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	stream, producer, tree, coin := h.stopped()
	id := tree.TxID().String()
	// The node takes the tree and never mines it.
	h.chain.SetHoldIf(func(tx *transaction.Transaction) bool { return tx.TxID().String() == id })
	if err := h.chain.Send(tree); err != nil {
		t.Fatal(err)
	}
	if h.chain.Tx(id) == nil || h.chain.Mined(id) {
		t.Fatalf("the tree on the node %v, mined %v", h.chain.Tx(id) != nil, h.chain.Mined(id))
	}
	in := tree.Inputs[0]
	other := strings.Repeat("5e", 32)
	h.chain.SpendElsewhere(in.SourceTXID.String(), in.SourceTxOutIndex, other)
	began := time.Now()
	r := h.must("", append([]string{"anchor", stream}, fast...)...)
	if !strings.Contains(r.stderr, "funding tree "+id+" lost a double spend: the node still serves it, without a proof, and its fee coin "+coin+" is spent by "+other) ||
		strings.Contains(r.stderr, "could not be settled now") {
		t.Fatalf("anchor:\n%s", r.stderr)
	}
	if d := time.Since(began); d > 30*time.Second {
		t.Fatalf("the command waited %s for a tree that never mines", d)
	}
	if st := h.state(stream); len(st.PendingTrees) != 0 || st.Tree != nil || len(st.Ahead) != 0 || h.poolCoins()[coin] {
		t.Fatalf("after the lost double spend: records %+v, tree %+v, ahead %d, coin held %t", st.PendingTrees, st.Tree, len(st.Ahead), h.poolCoins()[coin])
	}
	// The command after says nothing of it and mints a tree of its own.
	r = h.produce("a\n", stream)
	if strings.Contains(r.stderr, id) {
		t.Fatalf("the command after:\n%s", r.stderr)
	}
	if st := h.state(stream); st.Tree == nil || st.Tree.Txid == id || len(st.PendingTrees) != 0 {
		t.Fatalf("after the next produce: tree %+v, records %+v", st.Tree, st.PendingTrees)
	}
	if r = h.must("", "read", stream, producer); r.stdout != "a\n" {
		t.Fatalf("read: %q", r.stdout)
	}
}

// A tree minted ahead is on its way to a block when the run ends. The next
// run holds it once it has mined, and switches to it when the current tree
// runs out: no third tree is minted.
func TestATreeMintedAheadAndNotCollectedIsHeldByTheNextRun(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	stream, producer := h.producer()
	home := h.home
	// The chain keeps the second batch tree unmined: the one minted ahead.
	h.chain.HoldIf = func(tx *transaction.Transaction) bool {
		st, err := state.Load(home, stream)
		if err != nil || st.Tree == nil {
			return false
		}
		p := st.PendingTree(tx.TxID().String())
		return p != nil && p.Kind == "batch"
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan result, 1)
	go func() {
		done <- h.runCtx(ctx, linesOf(0, 3), append([]string{"produce", stream, "-max-rate", "50", "-max-lines", "1", "-anchor-wait", "0"}, fast...)...)
	}()
	waitFor(t, func() bool { return h.chain.Waiting() == 1 && h.host.Holds(mustHex(producer), stream, 3) })
	cancel()
	if r := <-done; r.code != exitOK {
		t.Fatalf("the first run: exit %d\n%s\n%s", r.code, r.stdout, r.stderr)
	}
	st := h.state(stream)
	if len(st.PendingTrees) != 1 || st.PendingTrees[0].Kind != "batch" || len(st.Ahead) != 0 || len(st.Trees) != 1 {
		t.Fatalf("after the run: records %+v, ahead %d, trees %d", st.PendingTrees, len(st.Ahead), len(st.Trees))
	}
	ahead := st.PendingTrees[0].Tree.Txid
	r := h.must("", "doctor")
	if !strings.Contains(r.stdout, "batch tree "+ahead+" signed for coin") {
		t.Fatalf("doctor:\n%s", r.stdout)
	}
	h.chain.Mine()
	r = h.produce(linesOf(3, 2), stream, "-max-lines", "1")
	for _, has := range []string{"funding tree " + ahead + " is recovered and waits for the switch", "switching to funding tree " + ahead + ", minted ahead"} {
		if !strings.Contains(r.stderr, has) {
			t.Fatalf("no %q in:\n%s", has, r.stderr)
		}
	}
	st = h.state(stream)
	if st.Tree == nil || st.Tree.Txid != ahead || len(st.Trees) != 2 || len(st.PendingTrees) != 0 || len(st.Ahead) != 0 {
		t.Fatalf("after the switch: tree %+v, %d trees, records %+v, ahead %d", st.Tree, len(st.Trees), st.PendingTrees, len(st.Ahead))
	}
	if !h.poolHoldsTx(ahead) || !h.host.HoldsTree(ahead) {
		t.Fatalf("the tree's change in the pool %t, the tree at the host %t", h.poolHoldsTx(ahead), h.host.HoldsTree(ahead))
	}
	if r = h.must("", "read", stream, producer); r.stdout != linesOf(0, 3)+linesOf(3, 2) {
		t.Fatalf("read: %q", r.stdout)
	}
}

// A tree minted ahead and collected before the run ended is recorded as
// minted ahead and keeps its record: the next run holds it again, quietly,
// and switches to it.
func TestATreeMintedAheadIsUsedByTheNextRun(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	stream, producer := h.producer()
	h.produce(linesOf(0, 3), stream, "-max-lines", "1", "-anchor-wait", "0")
	st := h.state(stream)
	if len(st.Ahead) != 1 || len(st.PendingTrees) != 1 || st.PendingTrees[0].Tree.Txid != st.Ahead[0].Txid {
		t.Fatalf("after the run: ahead %+v, records %+v", st.Ahead, st.PendingTrees)
	}
	ahead := st.Ahead[0].Txid
	r := h.must("", "doctor")
	if !strings.Contains(r.stdout, "batch tree "+ahead+" minted ahead") {
		t.Fatalf("doctor:\n%s", r.stdout)
	}
	r = h.produce(linesOf(3, 2), stream, "-max-lines", "1")
	if strings.Contains(r.stderr, "stopped before") || strings.Contains(r.stderr, "is recovered") ||
		!strings.Contains(r.stderr, "switching to funding tree "+ahead+", minted ahead") {
		t.Fatalf("the second run:\n%s", r.stderr)
	}
	st = h.state(stream)
	if st.Tree == nil || st.Tree.Txid != ahead || len(st.Trees) != 2 || len(st.PendingTrees) != 0 || len(st.Ahead) != 0 {
		t.Fatalf("after the switch: tree %+v, %d trees, records %+v, ahead %d", st.Tree, len(st.Trees), st.PendingTrees, len(st.Ahead))
	}
	if r = h.must("", "read", stream, producer); r.stdout != linesOf(0, 3)+linesOf(3, 2) {
		t.Fatalf("read: %q", r.stdout)
	}
}

// The host is away when a new tree is published: the tree is adopted, the
// publish is recorded as pending, and the next command publishes the tree
// before anything else. No second tree is minted.
func TestATreeAdoptedAndNotPublishedIsPublishedByTheNextCommand(t *testing.T) {
	h := newHarness(t)
	stream, producer := h.producer()
	defer retriesForTest()()
	h.host.SetDown(true)
	r := h.run("a\n", append([]string{"produce", stream}, fast...)...)
	h.want(r, exitUsage, "the tree is adopted and recorded as not published, and the next produce, anchor or sweep publishes it")
	st := h.state(stream)
	if st.Tree == nil || len(st.Unpublished) != 1 || st.Unpublished[0] != st.Tree.Txid || len(st.PendingTrees) != 0 || st.Seq != 0 {
		t.Fatalf("after the failed publish: tree %+v, unpublished %v, records %+v, seq %d", st.Tree, st.Unpublished, st.PendingTrees, st.Seq)
	}
	id := st.Tree.Txid
	if !strings.Contains(r.stderr, "funding tree "+id+" is adopted, and its publish failed") {
		t.Fatalf("the failed publish:\n%s", r.stderr)
	}
	r = h.must("", "doctor")
	if !strings.Contains(r.stdout, "tree "+id+" adopted and not confirmed published") {
		t.Fatalf("doctor:\n%s", r.stdout)
	}
	// Still away: the next command stops at the same publish.
	h.want(h.run("", append([]string{"anchor", stream}, fast...)...), exitUsage, "funding tree "+id+": ")
	if st = h.state(stream); len(st.Unpublished) != 1 || h.host.HoldsTree(id) {
		t.Fatalf("while the host is away: unpublished %v", st.Unpublished)
	}
	h.host.SetDown(false)
	r = h.produce("a\n", stream)
	for _, has := range []string{"funding tree " + id + " is adopted and not yet published: publishing it", "funding tree published: admitted 4 output(s)"} {
		if !strings.Contains(r.stderr, has) {
			t.Fatalf("no %q in:\n%s", has, r.stderr)
		}
	}
	st = h.state(stream)
	if len(st.Unpublished) != 0 || st.Tree.Txid != id || len(st.Trees) != 1 || !h.host.HoldsTree(id) {
		t.Fatalf("after the publish: unpublished %v, tree %s, %d trees, at the host %t", st.Unpublished, st.Tree.Txid, len(st.Trees), h.host.HoldsTree(id))
	}
	if r = h.must("", "read", stream, producer); r.stdout != "a\n" {
		t.Fatalf("read: %q", r.stdout)
	}
}

// The same when the tree is one a stopped run left on the chain: recovery
// adopts it, its publish fails, and the next command publishes it.
func TestARecoveredTreeThatCannotBePublishedIsPublishedLater(t *testing.T) {
	h := newHarness(t)
	stream, _, tree, _ := h.stopped()
	id := tree.TxID().String()
	h.land(tree)
	defer retriesForTest()()
	h.host.SetDown(true)
	h.want(h.run("", append([]string{"anchor", stream}, fast...)...), exitUsage, "the tree is adopted and recorded as not published")
	st := h.state(stream)
	if st.Tree == nil || st.Tree.Txid != id || len(st.Unpublished) != 1 || st.Unpublished[0] != id || len(st.PendingTrees) != 0 {
		t.Fatalf("after recovery: tree %+v, unpublished %v, records %+v", st.Tree, st.Unpublished, st.PendingTrees)
	}
	h.host.SetDown(false)
	h.must("", append([]string{"anchor", stream}, fast...)...)
	if st = h.state(stream); len(st.Unpublished) != 0 || !h.host.HoldsTree(id) {
		t.Fatalf("after the publish: unpublished %v, at the host %t", st.Unpublished, h.host.HoldsTree(id))
	}
}
