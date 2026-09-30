package reader

import (
	"context"
	"encoding/hex"
	"fmt"

	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bcommon/publish"

	"github.com/lightwebinc/blogs/internal/logrec"
)

// Backfill fills holes in one host (To) from a sibling (From): the anchor
// tokens of intervals 1 to Intervals and the batches at Seqs, each asked of
// To first and fetched from From only when To holds nothing, checked, and
// submitted to To's submit route (docs/spec.md section 12, Archive).
type Backfill struct {
	From, To string
	Stream   string
	Producer []byte
	// Intervals, when above zero, backfills the anchor chain 1 to
	// Intervals, in order, since a host admits an anchor only once it
	// holds its predecessor.
	Intervals uint64
	Seqs      []uint64
	// Note receives one line per position.
	Note func(format string, args ...any)
}

// Result counts what a backfill did.
type Result struct {
	Held, Filled, Missing, Refused int
}

// Run runs the backfill. An error is a transport failure; positions the
// sibling held nothing for, or that To did not admit, are counted.
func (c *Client) Run(ctx context.Context, b Backfill) (Result, error) {
	var r Result
	note := b.Note
	if note == nil {
		note = func(string, ...any) {}
	}
	topic, err := logrec.Topic(b.Stream)
	if err != nil {
		return r, err
	}
	to := &publish.Facade{Base: b.To, HTTP: c.HTTP}
	p := hex.EncodeToString(b.Producer)
	for k := uint64(1); k <= b.Intervals; k++ {
		what := fmt.Sprintf("interval %d", k)
		q := Query{Stream: b.Stream, Producer: p, Interval: k}
		if err := c.fill(ctx, b, to, topic, q, what, &r, note, func(a Answer) (string, bool, error) {
			_, tx, _, err := transaction.ParseBeef(a.Beef)
			if err != nil || tx == nil {
				return "BEEF does not parse", false, nil
			}
			an, _, err := logrec.DecodeToken(tx)
			if err != nil || an.Interval != k || an.Stream != b.Stream || string(an.Producer) != string(b.Producer) {
				return "not this producer's token for this interval", false, nil
			}
			if c.Headers != nil {
				if v := spv(ctx, tx, c.Headers); !v.OK() || !logrec.Mined(ctx, tx, c.Headers) {
					return "not mined in the header source", false, nil
				}
			}
			return "", true, nil
		}, func(a Answer) (bool, error) { return c.feedFunding(ctx, to, topic, a, note) }); err != nil {
			return r, err
		}
	}
	for _, seq := range b.Seqs {
		what := fmt.Sprintf("seq %d", seq)
		q := Query{Stream: b.Stream, Producer: p, Seq: seq}
		if err := c.fill(ctx, b, to, topic, q, what, &r, note, func(a Answer) (string, bool, error) {
			_, v := c.CheckCarrier(ctx, a, b.Stream, b.Producer, seq)
			if !v.OK() {
				return string(v.Code) + " " + v.Reason, false, nil
			}
			return "", true, nil
		}, nil); err != nil {
			return r, err
		}
	}
	return r, nil
}

// feedFunding submits to the host the funding trees an anchor token's fee
// inputs spend, read from the node with their proofs, since no lookup class
// serves a funding tree and a host admits an anchor only once it holds the
// outputs it spends. It reports whether it submitted any.
func (c *Client) feedFunding(ctx context.Context, to *publish.Facade, topic string, a Answer, note func(string, ...any)) (bool, error) {
	_, tx, _, err := transaction.ParseBeef(a.Beef)
	if err != nil || tx == nil {
		return false, nil
	}
	an, _, err := logrec.DecodeToken(tx)
	if err != nil {
		return false, nil
	}
	fed := false
	for _, in := range tx.Inputs {
		if in.SourceTXID == nil || [32]byte(*in.SourceTXID) == an.Prev {
			continue
		}
		id := in.SourceTXID.String()
		if c.Asset == nil {
			note("interval %d spends funding tree %s, which no lookup class serves; configure asset to read it from a node", an.Interval, id)
			continue
		}
		raw, err := c.Asset.TxRaw(ctx, id)
		if err != nil {
			note("interval %d: funding tree %s: %v", an.Interval, id, err)
			continue
		}
		src, err := transaction.NewTransactionFromBytes(raw)
		if err != nil || src.TxID().String() != id {
			return false, fmt.Errorf("the node answered funding tree %s with other bytes", id)
		}
		if src.MerklePath, _, err = c.Asset.Proof(ctx, id); err != nil {
			note("interval %d: funding tree %s: its proof: %v", an.Interval, id, err)
			continue
		}
		beef, err := src.AtomicBEEF(false)
		if err != nil {
			return false, err
		}
		if _, err := to.Submit(ctx, topic, beef); err != nil {
			return false, fmt.Errorf("submitting funding tree %s: %w", id, err)
		}
		note("interval %d: funding tree %s submitted from the node", an.Interval, id)
		fed = true
	}
	return fed, nil
}

func (c *Client) fill(ctx context.Context, b Backfill, to *publish.Facade, topic string, q Query, what string, r *Result, note func(string, ...any),
	check func(Answer) (string, bool, error), prepare func(Answer) (bool, error)) error {
	have, err := c.Ask(ctx, b.To, q)
	if err != nil {
		return fmt.Errorf("%s: asking %s: %w", what, b.To, err)
	}
	if len(have) > 0 {
		r.Held++
		note("%s: held", what)
		return nil
	}
	got, err := c.Ask(ctx, b.From, q)
	if err != nil {
		return fmt.Errorf("%s: asking %s: %w", what, b.From, err)
	}
	if len(got) == 0 {
		r.Missing++
		note("%s: MISSING: the sibling holds nothing either", what)
		return nil
	}
	admitted := 0
	for _, a := range got {
		why, ok, err := check(a)
		if err != nil {
			return err
		}
		if !ok {
			r.Refused++
			note("%s: the sibling's answer is REFUSED: %s", what, why)
			continue
		}
		res, err := to.Submit(ctx, topic, a.Beef)
		if err != nil {
			return fmt.Errorf("%s: submitting to %s: %w", what, b.To, err)
		}
		if len(res.Admitted) == 0 && prepare != nil {
			fed, err := prepare(a)
			if err != nil {
				return err
			}
			if fed {
				if res, err = to.Submit(ctx, topic, a.Beef); err != nil {
					return fmt.Errorf("%s: submitting to %s: %w", what, b.To, err)
				}
			}
		}
		if len(res.Admitted) > 0 {
			admitted++
		}
	}
	if admitted == 0 {
		r.Missing++
		note("%s: MISSING: fetched from the sibling, not admitted", what)
		return nil
	}
	r.Filled++
	note("%s: filled from the sibling", what)
	return nil
}
