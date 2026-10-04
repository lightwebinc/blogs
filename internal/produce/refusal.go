package produce

import (
	"context"
	"errors"
	"fmt"

	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bcommon/chainview"
	"github.com/lightwebinc/bcommon/termsafe"

	"github.com/lightwebinc/blogs/internal/state"
)

// A sweep the network will not mine is not in flight any more: waiting for
// it, or sending the same bytes again, can never finish it, and while it
// stayed in flight no later sweep of the stream could run. Only the node
// naming another transaction as the spender of one of its inputs marks it
// failed; anything else (a leg that refuses, cannot be reached or says
// busy, no verdict yet) leaves it in flight to be tried again.

// SweepRefusedError is a sweep the network refused. The sweep is marked
// failed in the stream's state, its fee coin is back in the pool when the
// node shows it unspent, and the next sweep of the stream goes on.
type SweepRefusedError struct {
	Txid string
	Kind string
	// Why is the refusal, as the evidence said it.
	Why string
	// Coin is what became of the fee coin.
	Coin string
}

func (e *SweepRefusedError) Error() string {
	what := "retracts nothing"
	if e.Kind == state.SweepRetire {
		what = "retires nothing"
	}
	return fmt.Sprintf("sweep %s refused by the network: %s; it is marked failed and %s; %s; sweep again to build a new one",
		e.Txid, termsafe.Text(e.Why), what, termsafe.Text(e.Coin))
}

// refusal is why the network will never mine tx, or "" while it still
// might. err is the settlement leg's answer, if any. The only evidence a
// persisted transaction is given up on is the node naming ANOTHER
// transaction as the spender of one of its inputs: then these bytes cannot
// mine. A transaction that has mined is never refused. Nor is one whose
// inputs the node shows unspent, spent by this very transaction, or cannot
// answer for, whatever a leg said: a leg's refusal (a node's verify error,
// arcade's REJECTED or DOUBLE_SPEND_ATTEMPTED) is also what a second
// submission of a transaction already accepted is answered with, and
// giving up on that would mark failed a sweep that then retracts what the
// stream believes it did not.
func (e *Engine) refusal(ctx context.Context, tx *transaction.Transaction, err error) string {
	txid := tx.TxID().String()
	if _, _, perr := e.Legs.Asset.Proof(ctx, txid); perr == nil {
		return ""
	}
	spent := chainview.SpentElsewhere(ctx, e.Legs.Asset, tx)
	if spent == "" {
		return ""
	}
	if why, ok := chainview.RefusedAnswer(err); ok {
		return spent + " (" + why + ")"
	}
	if a := e.Legs.Arcade; a != nil {
		if st, serr := a.Status(ctx, txid); serr == nil && st.Refused() {
			return spent + " (arcade reports " + st.Why() + ")"
		}
	}
	return spent
}

// failSweep marks sw failed for why, and gives its fee coin back to the
// pool if the node shows that coin unspent.
func (e *Engine) failSweep(ctx context.Context, sw *state.Sweep, why string) error {
	sw.Failed = why
	coin := "it spent no pool coin"
	switch f := sw.Fee; {
	case f != nil:
		by, err := e.Legs.Asset.Spender(ctx, f.TxID, f.Vout)
		switch {
		case err != nil:
			coin = fmt.Sprintf("its fee coin %s was not given back: the node could not say whether it is spent (%v)", f.Outpoint(), err)
		case by != "":
			coin = fmt.Sprintf("its fee coin %s is spent by %s and was not given back", f.Outpoint(), by)
		default:
			if err := e.Pool.Return(*f); err != nil {
				coin = fmt.Sprintf("its fee coin %s could not be given back: %v", f.Outpoint(), err)
			} else {
				coin = fmt.Sprintf("its fee coin %s (%d sat) is back in the pool", f.Outpoint(), f.Satoshis)
			}
		}
	case sw.Kind == state.SweepRetract:
		coin = "its fee coin was not recorded (a sweep built by an earlier version); `blogs doctor` shows the pool"
	}
	if err := e.St.Save(); err != nil {
		return err
	}
	e.note("sweep %s: refused by the network, marked failed", sw.Txid)
	return &SweepRefusedError{Txid: sw.Txid, Kind: sw.Kind, Why: why, Coin: coin}
}

// IsSweepRefused reports whether err is a sweep the network refused.
func IsSweepRefused(err error) bool {
	var r *SweepRefusedError
	return errors.As(err, &r)
}
