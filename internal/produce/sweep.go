package produce

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bcommon/carrier"
	"github.com/lightwebinc/bcommon/funding"
	"github.com/lightwebinc/bcommon/mint"

	"github.com/lightwebinc/blogs/internal/state"
)

// Sweeps (docs/spec.md section 6.4). A retraction sweeps outputs of a batch
// funding tree with bcommon carrier.Sweep: once it mines and is published,
// every carrier that spent one of those outputs is a double spend, and hosts
// stop serving it while keeping its seq as a numbered position. A retirement
// spends only the unused outputs of a tree no longer in use, paying their
// value back to the coin pool; it retracts nothing. Each sweep is one
// transaction per tree, persisted in the state before it is settled and
// finished (mined, its change pooled, published) by FinishSweeps, so a run
// that stops part way is resumed rather than rebuilt.

// ErrSweepInFlight is a new sweep asked for while one is still in flight.
var ErrSweepInFlight = errors.New("a sweep is still in flight; finish it first")

// TreeKind is which of the stream's lists a tree is recorded in.
type TreeKind string

// The tree kinds.
const (
	BatchTree  TreeKind = "batch"
	AnchorTree TreeKind = "anchor"
	AheadTree  TreeKind = "ahead"
)

// TreeInfo is one funding tree the stream recorded.
type TreeInfo struct {
	Kind    TreeKind
	Current bool
	Tree    funding.Tree
}

// Trees lists every funding tree the stream recorded, once each, in the
// order batch trees, anchor fee trees, trees minted ahead.
func (e *Engine) Trees() []TreeInfo {
	st := e.St
	var out []TreeInfo
	seen := map[string]bool{}
	add := func(k TreeKind, cur *funding.Tree, list []funding.Tree) {
		for _, t := range list {
			if seen[t.Txid] {
				continue
			}
			seen[t.Txid] = true
			isCur := cur != nil && cur.Txid == t.Txid
			if isCur {
				t = *cur
			}
			out = append(out, TreeInfo{Kind: k, Current: isCur, Tree: t})
		}
		if cur != nil && !seen[cur.Txid] {
			seen[cur.Txid] = true
			out = append(out, TreeInfo{Kind: k, Current: true, Tree: *cur})
		}
	}
	add(BatchTree, st.Tree, st.Trees)
	add(AnchorTree, st.AnchorTree, st.AnchorTrees)
	add(AheadTree, nil, st.Ahead)
	return out
}

// Tree finds one recorded tree by txid.
func (e *Engine) Tree(txid string) (TreeInfo, bool) {
	for _, t := range e.Trees() {
		if t.Tree.Txid == txid {
			return t, true
		}
	}
	return TreeInfo{}, false
}

// InFlight reports whether a sweep is built and not yet finished.
func (e *Engine) InFlight() bool {
	for _, s := range e.St.Sweeps {
		if !s.Done {
			return true
		}
	}
	return false
}

// Retract builds and persists one sweep of the batch tree txid over vouts
// (every output of the tree when vouts is empty), with its fee from the
// coin pool. seqs, when known, are the batches it retracts.
func (e *Engine) Retract(ctx context.Context, txid string, vouts []uint32, seqs []uint64) (*state.Sweep, error) {
	if e.InFlight() {
		return nil, ErrSweepInFlight
	}
	info, ok := e.Tree(txid)
	switch {
	case !ok:
		return nil, fmt.Errorf("tree %s is not one this stream recorded", txid)
	case info.Kind != BatchTree:
		return nil, fmt.Errorf("tree %s is an %s tree, which funds no carrier; retire it instead", txid, info.Kind)
	}
	vouts, err := outputs(info.Tree, vouts)
	if err != nil {
		return nil, err
	}
	tree, err := e.kept.Tx(txid)
	if err != nil {
		return nil, err
	}
	payer := e.newPayer()
	fee, err := payer.Take(ctx)
	if err != nil {
		return nil, FeeError(fmt.Errorf("sweep fee: %w", err))
	}
	change, err := e.Signer.FundScript()
	if err != nil {
		payer.GiveBack()
		return nil, err
	}
	tx, err := carrier.Sweep(ctx, e.Signer, e.Signer.Originator, Params, tree, vouts, fee.Tx, fee.Vout, fee.Unlocker, change,
		e.Opts.Fees.SatPerByte, e.Opts.Fees.Floor)
	if err != nil {
		payer.GiveBack()
		return nil, err
	}
	sw, err := e.record(tx, state.SweepRetract, info, vouts, seqs, true)
	if err != nil {
		payer.GiveBack()
	}
	return sw, err
}

// ErrNotWorthIt is a retirement whose outputs cannot pay its own fee.
var ErrNotWorthIt = errors.New("the unused outputs cannot pay the fee to spend them")

// Retirable lists the trees a retirement would sweep: every tree not in use
// with unused outputs.
func (e *Engine) Retirable() []TreeInfo {
	var out []TreeInfo
	for _, t := range e.Trees() {
		if !t.Current && t.Tree.Remaining() > 0 {
			out = append(out, t)
		}
	}
	return out
}

// Retire builds and persists one sweep of the unused outputs of the tree
// txid, which must not be in use, paying their value less the fee back to
// the coin pool. ErrNotWorthIt means the value cannot pay the fee.
func (e *Engine) Retire(ctx context.Context, txid string) (*state.Sweep, error) {
	if e.InFlight() {
		return nil, ErrSweepInFlight
	}
	info, ok := e.Tree(txid)
	switch {
	case !ok:
		return nil, fmt.Errorf("tree %s is not one this stream recorded", txid)
	case info.Current:
		return nil, fmt.Errorf("tree %s is in use; only a tree no longer in use is retired", txid)
	case info.Tree.Remaining() == 0:
		return nil, fmt.Errorf("tree %s has no unused output", txid)
	}
	var vouts []uint32
	for v := info.Tree.Next; v < info.Tree.Count; v++ {
		vouts = append(vouts, v)
	}
	tree, err := e.kept.Tx(txid)
	if err != nil {
		return nil, err
	}
	pay, err := e.Signer.FundScript()
	if err != nil {
		return nil, err
	}
	tx, err := retireTx(tree, vouts, Params.Derivation.Unlocker(ctx, e.Signer, e.Signer.Originator), pay, e.Opts.Fees)
	if err != nil {
		return nil, err
	}
	// A tree minted ahead was never published, so the hosts hold none of
	// what its retirement spends.
	return e.record(tx, state.SweepRetire, info, vouts, nil, info.Kind != AheadTree)
}

// retireTx spends vouts of tree to pay, the fee coming out of their value,
// by the same fee rule as carrier.Sweep.
func retireTx(tree *transaction.Transaction, vouts []uint32, unlocker transaction.UnlockingScriptTemplate, pay *script.Script, fees mint.Fees) (*transaction.Transaction, error) {
	var in uint64
	for _, v := range vouts {
		if uint64(v) >= uint64(len(tree.Outputs)) {
			return nil, fmt.Errorf("output %d is out of range", v)
		}
		in += tree.Outputs[v].Satoshis
	}
	fee := fees.Floor
	for pass := 0; pass < 6; pass++ {
		if in <= fee {
			return nil, fmt.Errorf("%w: %d sat in %d output(s), fee %d", ErrNotWorthIt, in, len(vouts), fee)
		}
		tx := transaction.NewTransaction()
		for _, v := range vouts {
			tx.AddInputFromTx(tree, v, unlocker)
		}
		tx.AddOutput(&transaction.TransactionOutput{Satoshis: in - fee, LockingScript: pay})
		if err := tx.Sign(); err != nil {
			return nil, err
		}
		need := max(uint64(tx.Size())*fees.SatPerByte, fees.Floor) //nolint:gosec // a serialised length
		if need <= fee {
			return tx, nil
		}
		fee = max((uint64(tx.Size())+2*uint64(len(tx.Inputs)))*fees.SatPerByte, fees.Floor) //nolint:gosec // a serialised length
	}
	return nil, errors.New("retirement fee did not converge")
}

// outputs checks vouts against the tree, sorted and without repeats; none
// means every output.
func outputs(t funding.Tree, vouts []uint32) ([]uint32, error) {
	if len(vouts) == 0 {
		for v := uint32(0); v < t.Count; v++ {
			vouts = append(vouts, v)
		}
		return vouts, nil
	}
	out := slices.Clone(vouts)
	slices.Sort(out)
	out = slices.Compact(out)
	if out[len(out)-1] >= t.Count {
		return nil, fmt.Errorf("tree %s has %d outputs; output %d does not exist", t.Txid, t.Count, out[len(out)-1])
	}
	return out, nil
}

// record persists a sweep before it is settled, and marks the swept outputs
// as used in every record of the tree, so no carrier or anchor spends them.
func (e *Engine) record(tx *transaction.Transaction, kind string, info TreeInfo, vouts []uint32, seqs []uint64, publish bool) (*state.Sweep, error) {
	beef, err := funding.KeepBEEF(tx, nil)
	if err != nil {
		return nil, err
	}
	st := e.St
	next := vouts[len(vouts)-1] + 1
	for _, t := range []*funding.Tree{st.Tree, st.AnchorTree} {
		if t != nil && t.Txid == info.Tree.Txid && t.Next < next {
			t.Next = next
		}
	}
	for _, list := range [][]funding.Tree{st.Trees, st.AnchorTrees, st.Ahead} {
		for i := range list {
			if list[i].Txid == info.Tree.Txid && list[i].Next < next {
				list[i].Next = next
			}
		}
	}
	// A tree minted ahead whose outputs are swept is no longer one to switch
	// to: without its record the next start does not hold it again.
	if info.Kind == AheadTree {
		st.DropPendingTree(info.Tree.Txid)
	}
	st.Sweeps = append(st.Sweeps, state.Sweep{Kind: kind, Tree: info.Tree.Txid, Vouts: vouts, Seqs: seqs,
		Txid: tx.TxID().String(), RawHex: tx.Hex(), BeefHex: beef, Publish: publish})
	if err := st.Save(); err != nil {
		return nil, err
	}
	sw := &st.Sweeps[len(st.Sweeps)-1]
	e.note("sweep %s: %s tree %s, %d output(s), built", sw.Txid, kind, sw.Tree, len(vouts))
	return sw, nil
}

// rebuildSweep rebuilds a sweep from the stream's state through
// funding.Rebuild: a mined sweep from its raw bytes and proof, an unmined
// one from the BEEF kept, and holds it to the txid recorded beside it.
//
// A sweep kept by an earlier build can hold a BEEF whose fee input's parent
// is a stand-in of no inputs, which the guard refuses on reading back. Once
// such a sweep is submitted, nothing but its txid and its own bytes is
// needed to wait for its proof and publish it, so it is read from its raw
// bytes alone. One not yet submitted still needs its ancestry to settle,
// and stays refused.
func rebuildSweep(sw *state.Sweep) (*transaction.Transaction, error) {
	tx, err := funding.Rebuild(sw.RawHex, sw.BumpHex, sw.BeefHex)
	if err != nil && sw.Submitted && sw.BumpHex == "" && sw.BeefHex != "" {
		tx, err = funding.Rebuild(sw.RawHex, "", "")
	}
	if err != nil {
		return nil, err
	}
	if tx.TxID().String() != sw.Txid {
		return nil, errors.New("the kept record does not hold the sweep")
	}
	return tx, nil
}

// FinishSweeps settles every sweep in flight, waits for it to mine, adds
// its change to the pool, and publishes it once, with its proof, to the
// stream's topic when the hosts hold what it spends. An error leaves the
// sweep persisted for the next run.
func (e *Engine) FinishSweeps(ctx context.Context) error {
	st := e.St
	for i := range st.Sweeps {
		sw := &st.Sweeps[i]
		if sw.Done {
			continue
		}
		tx, err := rebuildSweep(sw)
		if err != nil {
			return fmt.Errorf("sweep %s: %w", sw.Txid, err)
		}
		what := "sweep (" + sw.Kind + ")"
		if sw.BumpHex == "" {
			mp, height, perr := e.Legs.Asset.Proof(ctx, sw.Txid)
			if perr != nil {
				if !sw.Submitted {
					if err := e.Legs.Settler.Submit(ctx, tx); err != nil {
						return fmt.Errorf("sweep %s: settle via %s: %w; it is persisted and the next sweep resumes it", sw.Txid, e.Legs.Settler.Name(), err)
					}
					sw.Submitted = true
					if err := st.Save(); err != nil {
						return err
					}
				}
				if mp, height, err = e.payer.Await(ctx, what, tx); err != nil {
					return fmt.Errorf("sweep %s: %w; it is persisted and the next sweep resumes it", sw.Txid, err)
				}
			}
			tx.MerklePath = mp
			sw.BumpHex, sw.Height, sw.BeefHex, sw.Submitted = mp.Hex(), height, "", true
			e.payer.Change(tx, height, mp)
			if err := st.Save(); err != nil {
				return err
			}
		}
		if sw.Publish {
			beef, err := tx.AtomicBEEF(false)
			if err != nil {
				return err
			}
			if e.Legs.Hosts != nil {
				// No lookup class names the sweep of a batch, so no backfill repairs a
				// host that missed one: every host must take it.
				o := e.Legs.Hosts.Send(ctx, "sweep "+sw.Txid, e.topic, beef, len(e.Legs.Hosts.Hosts))
				if err := o.Err(); err != nil {
					return fmt.Errorf("sweep %s: publish: %w; it is mined and persisted, and the next sweep publishes it to every host", sw.Txid, err)
				}
			} else {
				if _, err := e.Legs.Facade.Submit(ctx, e.topic, beef); err != nil {
					return fmt.Errorf("sweep %s: publish: %w; it is mined and persisted, and the next sweep publishes it", sw.Txid, err)
				}
				// The plane carries it to every subscribed host; a host off
				// the plane receives it only directly, since no lookup
				// class serves a sweep. Every host named must take it.
				if d := e.Legs.SweepHosts; d != nil && len(d.Hosts) > 0 {
					o := d.Send(ctx, "sweep "+sw.Txid, e.topic, beef, len(d.Hosts))
					if err := o.Err(); err != nil {
						return fmt.Errorf("sweep %s: published to the facade, but not to every host directly: %w; it is mined and persisted, and the next sweep sends it again", sw.Txid, err)
					}
				} else if !e.warnedSweep {
					e.warnedSweep = true
					e.note("note: sweep %s goes to the facade only: a host off the plane receives a sweep only when it is in hosts (docs/usage.md, Sweeping)", sw.Txid)
				}
			}
		}
		sw.Done = true
		if err := st.Save(); err != nil {
			return err
		}
		switch {
		case sw.Kind == state.SweepRetract && len(sw.Seqs) > 0:
			e.note("sweep %s: mined at height %d and published; seqs %s retracted", sw.Txid, sw.Height, seqWords(sw.Seqs))
		case sw.Kind == state.SweepRetract:
			e.note("sweep %s: mined at height %d and published; every batch funded by outputs %s of tree %s is retracted", sw.Txid, sw.Height, voutWords(sw.Vouts), sw.Tree)
		case sw.Publish:
			e.note("sweep %s: mined at height %d and published; %d unused output(s) of tree %s retired to the pool", sw.Txid, sw.Height, len(sw.Vouts), sw.Tree)
		default:
			e.note("sweep %s: mined at height %d; %d output(s) of tree %s, never published, retired to the pool", sw.Txid, sw.Height, len(sw.Vouts), sw.Tree)
		}
	}
	return nil
}

func seqWords(s []uint64) string {
	out := ""
	for i, n := range s {
		if i > 0 {
			out += " "
		}
		out += fmt.Sprint(n)
	}
	return out
}

func voutWords(v []uint32) string {
	if len(v) > 1 && int(v[len(v)-1]-v[0]) == len(v)-1 {
		return fmt.Sprintf("%d to %d", v[0], v[len(v)-1])
	}
	out := ""
	for i, n := range v {
		if i > 0 {
			out += ","
		}
		out += fmt.Sprint(n)
	}
	return out
}
