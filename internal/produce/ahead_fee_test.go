package produce_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/blogs/internal/produce"
)

// oneCoin leaves the home with one spendable coin, as a home that has
// spent the others is.
func (r *rig) oneCoin() {
	r.t.Helper()
	kept := false
	for _, o := range r.wallet.Pool.Outputs() {
		if !kept && !o.Unproven && o.Spendable(r.chain.Height()) {
			kept = true
			continue
		}
		if _, err := r.wallet.Pool.Remove(o); err != nil {
			r.t.Fatal(err)
		}
	}
	if !kept || r.wallet.Pool.Count() != 1 {
		r.t.Fatalf("the home holds %d coins, one spendable %v", r.wallet.Pool.Count(), kept)
	}
}

// mintingAhead is a producer whose batch tree minted ahead took the home's
// one coin and is held unmined by the chain: the first batch tree is mined
// and has two outputs left. It returns the engine, the first tree, and a
// channel closed when a take says it waits for the mint.
func (r *rig) mintingAhead(o produce.Options) (e *produce.Engine, first string, asked chan struct{}) {
	r.t.Helper()
	r.oneCoin()
	ctx := context.Background()
	e = r.engine(o)
	asked = make(chan struct{})
	var once sync.Once
	note := e.Note
	e.Note = func(format string, args ...any) {
		if strings.HasPrefix(format, "no coin is spendable") {
			once.Do(func() { close(asked) })
		}
		note(format, args...)
	}
	if err := e.Start(ctx); err != nil {
		r.t.Fatal(err)
	}
	if err := e.Publish(ctx, lines(1, 0)); err != nil {
		r.t.Fatal(err)
	}
	first = e.St.Tree.Txid
	// From here the chain holds what the stream signs next: the batch tree
	// minted ahead.
	r.chain.SetHoldIf(func(tx *transaction.Transaction) bool {
		return tx.TxID().String() != first && !isAnchor(tx)
	})
	if err := e.Publish(ctx, lines(1, 1)); err != nil {
		r.t.Fatal(err)
	}
	if !strings.Contains(r.notes.String(), "so the next is minted ahead") || r.wallet.Pool.Count() != 0 || len(e.St.PendingTrees) != 1 {
		r.t.Fatalf("no tree is being minted ahead from the one coin: %d coin(s), records %+v\n%s", r.wallet.Pool.Count(), e.St.PendingTrees, r.notes)
	}
	return e, first, asked
}

// A funding tree minted ahead takes a coin when the mint starts, and its
// change reaches the pool only when the mint is collected. A home with one
// coin has none in between. A fee asked for in that time, by the first
// anchor's fee tree or by a sweep, waits for the mint and is paid from the
// tree's change: the run goes on.
func TestAFeeWaitsForATreeMintedAhead(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		name string
		pay  func(ctx context.Context, e *produce.Engine, first string) error
	}{
		{"anchor fee tree", func(ctx context.Context, e *produce.Engine, _ string) error {
			if err := e.Tick(ctx, true); err != nil {
				return err
			}
			if e.St.AnchorTree == nil || e.St.Inflight == nil {
				return fmt.Errorf("no anchor was built: fee tree %+v, in flight %+v", e.St.AnchorTree, e.St.Inflight)
			}
			return nil
		}},
		{"sweep", func(ctx context.Context, e *produce.Engine, first string) error {
			if _, err := e.Retract(ctx, first, []uint32{0}, []uint64{1}); err != nil {
				return err
			}
			return e.FinishSweeps(ctx)
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			r := newRig(t)
			e, first, asked := r.mintingAhead(r.options())
			// The tree mines once the fee has been asked for.
			go func() {
				select {
				case <-asked:
				case <-time.After(20 * time.Second):
				}
				time.Sleep(100 * time.Millisecond)
				r.chain.SetHoldIf(nil)
				r.chain.Mine()
			}()
			began := time.Now()
			if err := c.pay(context.Background(), e, first); err != nil {
				t.Fatalf("%v\n%s", err, r.notes)
			}
			if d := time.Since(began); d < 100*time.Millisecond {
				t.Fatalf("the fee was paid after %s, before the tree mined", d)
			}
			for _, has := range []string{"1 funding tree(s) minted ahead hold one: waiting for the change", "is minted ahead and waits for the switch"} {
				if !strings.Contains(r.notes.String(), has) {
					t.Fatalf("no %q in:\n%s", has, r.notes)
				}
			}
		})
	}
}

// The wait for a tree minted ahead is bounded by the wait for one proof.
// A tree that does not mine in time ends it: the fee is refused in words
// that say where the coin is, the tree's record is kept, and the next
// command, once the tree has mined, recovers it and pays the fee. An
// interrupt ends the wait at once.
func TestAFeeWaitsForATreeMintedAheadNoLongerThanTheProofWait(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	ctx := context.Background()
	o := r.options()
	o.Wait = 300 * time.Millisecond
	e, first, _ := r.mintingAhead(o)
	began := time.Now()
	_, err := e.Retract(ctx, first, []uint32{0}, []uint64{1})
	// The tree began its own wait first, so what is left of it bounds this
	// one.
	if d := time.Since(began); d > 5*time.Second {
		t.Fatalf("the sweep's fee waited %s", d)
	}
	if err == nil || !strings.Contains(err.Error(), "1 funding tree(s) signed for a coin of this home are not settled") || strings.Contains(err.Error(), "(run `blogs fund`:") {
		t.Fatalf("a fee whose coin is in an unmined tree: %v\n%s", err, r.notes)
	}
	for _, has := range []string{"1 funding tree(s) minted ahead hold one: waiting for the change", "the next funding tree was not minted ahead"} {
		if !strings.Contains(r.notes.String(), has) {
			t.Fatalf("no %q in:\n%s", has, r.notes)
		}
	}
	if len(e.St.PendingTrees) != 1 || e.InFlight() {
		t.Fatalf("after the bounded wait: records %+v, a sweep in flight %v", e.St.PendingTrees, e.InFlight())
	}
	ahead := e.St.PendingTrees[0].Tree.Txid
	if err := e.Close(ctx); err != nil {
		t.Fatal(err)
	}

	// Mined: the next command holds the tree, takes its change, and the
	// sweep is paid.
	r.chain.SetHoldIf(nil)
	r.chain.Mine()
	e = r.engine(r.options())
	if err := e.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Retract(ctx, first, []uint32{0}, []uint64{1}); err != nil {
		t.Fatalf("the sweep after the tree mined: %v\n%s", err, r.notes)
	}
	if err := e.FinishSweeps(ctx); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if st := r.state(); len(st.Ahead) != 1 || st.Ahead[0].Txid != ahead || len(st.Sweeps) != 1 || !st.Sweeps[0].Done {
		t.Fatalf("after the recovery: ahead %v, sweeps %+v", aheads(st), st.Sweeps)
	}
}

// An interrupt ends the wait for a tree minted ahead at once, and the
// refusal counts the mint still in flight.
func TestAnInterruptEndsTheWaitForATreeMintedAhead(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	o := r.options()
	o.Wait = time.Minute
	e, first, asked := r.mintingAhead(o)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-asked
		cancel()
	}()
	began := time.Now()
	_, err := e.Retract(ctx, first, []uint32{0}, []uint64{1})
	if d := time.Since(began); d > 5*time.Second {
		t.Fatalf("the interrupted fee waited %s", d)
	}
	if err == nil || !strings.Contains(err.Error(), "1 funding tree(s) minted ahead hold a coin and have not settled") {
		t.Fatalf("an interrupted fee: %v\n%s", err, r.notes)
	}
	if len(e.St.PendingTrees) != 1 || e.InFlight() {
		t.Fatalf("after the interrupt: records %+v, a sweep in flight %v", e.St.PendingTrees, e.InFlight())
	}
}
