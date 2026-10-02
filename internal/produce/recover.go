package produce

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bcommon/bwallet"
	"github.com/lightwebinc/bcommon/funding"
	"github.com/lightwebinc/bcommon/producer"

	"github.com/lightwebinc/blogs/internal/state"
)

// A funding tree's fee coin leaves the pool, which is saved without it,
// when the tree is signed, and the tree is adopted into the state only once
// it has mined; a tree minted ahead is adopted later still, at the switch.
// A run that stops in between leaves a tree that may be on the chain and a
// pool without the coin. The record closes that window: the library hands
// the tree and its coin over before the tree reaches the settlement leg
// (producer.Trees.Prepare), the state saves both with the tree's kind
// (state.PendingTrees), and the next command that spends asks the node what
// became of each (RecoverTrees) before it spends anything. Adopting a tree
// drops its record in the same save.
//
// A tree is adopted before it is published, so a publish can fail with the
// tree adopted. Its txid is then saved as publish pending
// (state.Unpublished) and published again: at once, and, while that fails,
// at the start of every later command (PublishTrees), as a persisted
// carrier is.

// prepareTree is the Prepare of the trees of one kind: it saves the record
// of a signed tree and its fee coin before the tree reaches the settlement
// leg. A record that cannot be saved aborts the mint.
func (e *Engine) prepareTree(kind TreeKind) func(funding.Tree, bwallet.Output) error {
	return func(tree funding.Tree, coin bwallet.Output) error {
		e.St.KeepPendingTree(state.PendingTree{Kind: string(kind), Tree: tree, Coin: coin})
		if err := e.St.Save(); err != nil {
			e.St.DropPendingTree(tree.Txid)
			return fmt.Errorf("recording the tree before it is sent: %w", err)
		}
		return nil
	}
}

// lib is the library's trees of one kind, or nil.
func (e *Engine) lib(kind TreeKind) *producer.Trees {
	switch kind {
	case BatchTree:
		return e.trees
	case AnchorTree:
		return e.anchorTrees
	}
	return nil
}

// adopted reports whether the tree lists hold txid: a tree the stream
// adopted, now or before.
func (e *Engine) adopted(txid string) bool {
	st := e.St
	for _, t := range []*funding.Tree{st.Tree, st.AnchorTree} {
		if t != nil && t.Txid == txid {
			return true
		}
	}
	has := func(t funding.Tree) bool { return t.Txid == txid }
	return slices.ContainsFunc(st.Trees, has) || slices.ContainsFunc(st.AnchorTrees, has)
}

// sweptAhead reports whether txid is a tree minted ahead with no output
// left: a retirement spent them.
func (e *Engine) sweptAhead(txid string) bool {
	return slices.ContainsFunc(e.St.Ahead, func(t funding.Tree) bool { return t.Txid == txid && t.Remaining() == 0 })
}

// recoverWait bounds what RecoverTrees may wait, for all its records
// together: the wait for one proof.
func (e *Engine) recoverWait() time.Duration {
	if e.Opts.Wait > 0 {
		return e.Opts.Wait
	}
	return producer.DefaultTimeout
}

// RecoverTrees settles every tree an earlier run signed for a coin and did
// not adopt, before anything is spent:
//
//   - a tree the chain holds is adopted and published, or, while the
//     current tree of its kind has outputs left, held for the switch, and
//     its change is taken. A held tree is recorded among the trees minted
//     ahead, and its record stays until the switch adopts it;
//   - a tree the node does not know whose coin another transaction spent is
//     gone, and its record is dropped;
//   - a tree the node does not know whose coin is unspent never reached the
//     chain as far as the node can say now. The coin goes back to the pool,
//     and the record is kept for one more command: a tree handed to the leg
//     an instant before the run stopped can reach the node later. The
//     second such answer drops the record;
//   - a node that cannot answer, or a tree that has not mined in time,
//     decides nothing: the record stays, the next command asks again, and
//     this one goes on.
//
// Payer.Async is off, so a tree found on the chain and not yet mined is
// waited for. The wait for all the records together is bounded by the wait
// for one proof (Options.Wait), so a start never waits without end; a tree
// still unmined then keeps its record. When ctx itself ends, the error says
// so and the command stops. Otherwise only a failure to save the state is
// returned.
func (e *Engine) RecoverTrees(ctx context.Context) error {
	if len(e.St.PendingTrees) == 0 {
		return nil
	}
	wait := e.recoverWait()
	rctx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	for _, p := range slices.Clone(e.St.PendingTrees) {
		id := p.Tree.Txid
		// A tree the lists hold has nothing left to settle, and asking
		// about one that is no longer current would adopt it a second time.
		// So would holding a tree minted ahead whose outputs a sweep spent.
		if e.adopted(id) || e.sweptAhead(id) {
			e.St.DropPendingTree(id)
			if err := e.St.Save(); err != nil {
				return err
			}
			continue
		}
		lib := e.lib(TreeKind(p.Kind))
		if lib == nil {
			e.note("funding tree %s is recorded as signed and not adopted, of a kind this blogs does not know (%q): its record is kept", id, p.Kind)
			continue
		}
		// A tree an earlier run settled and recorded as minted ahead is
		// held again for the switch, which is no news.
		ahead := e.St.MintedAhead(id)
		var held []string
		note := e.payer.Note
		if ahead {
			e.payer.Note = func(format string, args ...any) { held = append(held, fmt.Sprintf(format, args...)) }
		} else {
			e.note("%s funding tree %s was signed by a run that stopped before it adopted the tree: asking the node what became of it (a tree on the chain and not yet mined is waited for, %s at most)", p.Kind, id, wait)
		}
		r, err := lib.Recover(rctx, p.Tree, p.Coin)
		e.payer.Note = note
		switch {
		case err != nil || (r.Outcome != producer.TreeHeld && r.Outcome != producer.TreeAdopted):
			for _, l := range held {
				e.note("%s", l)
			}
		case !ahead:
		case r.Outcome == producer.TreeAdopted:
			e.note("switching to %s funding tree %s, minted ahead by an earlier run; it is published", p.Kind, id)
		case e.Verbose:
			e.note("%s funding tree %s, minted ahead by an earlier run, waits for the switch", p.Kind, id)
		}
		if errors.Is(err, producer.ErrPublish) {
			// Adopted, its record dropped by Adopt: only the publish is
			// left, and PublishTrees repeats it.
			if err := e.markUnpublished(id, err); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			if ctx.Err() != nil {
				return fmt.Errorf("funding tree %s: interrupted while asking what became of it (%w); its record is kept and the next produce, anchor or sweep asks again", id, ctx.Err())
			}
			e.note("funding tree %s: what became of it could not be settled now (%s): its record is kept, the next produce, anchor or sweep asks again, and this command goes on without the tree", id, oneLine(err))
			continue
		}
		switch r.Outcome {
		case producer.TreeAdopted, producer.CoinSpent:
			e.St.DropPendingTree(id)
		case producer.TreeHeld:
			e.keepAhead(r.Tree)
		case producer.CoinReturned:
			if p.ReturnedOnce {
				e.St.DropPendingTree(id)
				e.note("funding tree %s: the node still does not know it, so its record is dropped", id)
			} else if rec := e.St.PendingTree(id); rec != nil {
				rec.ReturnedOnce = true
				e.note("funding tree %s: its record is kept for one more command, in case it reaches the node after all", id)
			}
		}
		if err := e.St.Save(); err != nil {
			return err
		}
	}
	return nil
}

func oneLine(err error) string {
	s, _, _ := strings.Cut(err.Error(), "\n")
	return s
}

// spend is the library's Spend with the one error that leaves a tree
// adopted handled: a tree whose publish failed is saved as publish pending
// and published again, and the spend is asked again, which then answers
// the tree, now current, without publishing it.
func (e *Engine) spend(ctx context.Context, lib *producer.Trees, need uint32) (*transaction.Transaction, uint32, error) {
	tree, vout, err := lib.Spend(ctx, need)
	if !errors.Is(err, producer.ErrPublish) {
		return tree, vout, err
	}
	var pe *producer.PublishError
	if !errors.As(err, &pe) {
		return nil, 0, err
	}
	if err := e.markUnpublished(pe.Tree.Txid, err); err != nil {
		return nil, 0, err
	}
	if err := e.PublishTrees(ctx); err != nil {
		return nil, 0, err
	}
	return lib.Spend(ctx, need)
}

// markUnpublished saves the adopted tree txid as publish pending. cause is
// why its publish failed.
func (e *Engine) markUnpublished(txid string, cause error) error {
	if !slices.Contains(e.St.Unpublished, txid) {
		e.St.Unpublished = append(e.St.Unpublished, txid)
	}
	// The library's Adopt dropped the record; a tree answered as already
	// current kept it.
	e.St.DropPendingTree(txid)
	if err := e.St.Save(); err != nil {
		return err
	}
	e.note("funding tree %s is adopted, and its publish failed (%s)", txid, oneLine(cause))
	return nil
}

// PublishTrees publishes every adopted tree whose publish failed, the same
// bytes, so a host that already holds one answers a duplicate. A tree that
// still cannot be published stays publish pending and is the error: the
// run stops, as it does for a carrier it cannot publish, and the next
// command starts here.
func (e *Engine) PublishTrees(ctx context.Context) error {
	for _, id := range slices.Clone(e.St.Unpublished) {
		drop := func() error {
			e.St.Unpublished = slices.DeleteFunc(e.St.Unpublished, func(s string) bool { return s == id })
			return e.St.Save()
		}
		info, ok := e.Tree(id)
		lib := e.lib(info.Kind)
		if !ok || lib == nil {
			e.note("funding tree %s is recorded as not published, and is not a tree this stream adopted: the note is dropped", id)
			if err := drop(); err != nil {
				return err
			}
			continue
		}
		what := "funding tree " + id
		e.note("%s is adopted and not yet published: publishing it", what)
		try := func() error { return lib.Publish(ctx, info.Tree) }
		var err error
		if e.Legs.Hosts != nil {
			// Unicast: every host is retried on its own.
			err = try()
		} else {
			_, err = e.retry(ctx, what, try)
		}
		if err != nil {
			return fmt.Errorf("%s: %w; the tree is adopted and recorded as not published, and the next produce, anchor or sweep publishes it", what, err)
		}
		if err := drop(); err != nil {
			return err
		}
	}
	return nil
}
