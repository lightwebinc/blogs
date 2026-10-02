package reader

import (
	"context"
	"encoding/hex"
	"fmt"

	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bcommon/publish"

	"github.com/lightwebinc/blogs/internal/logrec"
)

// Backfill fills holes in one host (To) from its siblings (From): the
// anchor tokens of intervals 1 to Intervals and the batches at Seqs, each
// asked of To first and fetched from the first sibling that holds it only
// when To holds nothing, checked, and submitted to To's submit route
// (docs/spec.md section 12, Archive). It is also how a host that missed a
// unicast producer's submissions is repaired from the hosts that took them.
type Backfill struct {
	// From are the siblings, asked in order.
	From     []string
	To       string
	Stream   string
	Producer []byte
	// Intervals, when above zero, backfills the anchor chain 1 to
	// Intervals, in order, since a host admits an anchor only once it
	// holds its predecessor.
	Intervals uint64
	Seqs      []uint64
	// Sweeps carries sweeps to To: for each seq To holds, every candidate
	// it serves is checked for a retraction (Client.Retraction: a mined
	// spend of its funding output by another transaction, named by Txs,
	// which must answer spends), and a sweep found is submitted to To. No
	// lookup class names the sweep of a batch, so this is the only way a
	// backfill can carry one; it costs one spend read per held carrier.
	Sweeps bool
	// Note receives one line per position.
	Note func(format string, args ...any)
}

// Result counts what a backfill did. Unserved are seqs no host serves
// whose commitment the next seq filled or held names (its prev), as an
// archive and verify-interval prove an interval across a retraction
// (docs/spec.md section 8.4): retracted, or never delivered to any host
// asked, which a lookup cannot tell apart. A missing seq nothing names
// stays Missing. Sweeps counts sweeps carried to the host (Backfill.Sweeps).
type Result struct {
	Held, Filled, Missing, Refused, Unserved, Sweeps int
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
		if _, _, err := c.fill(ctx, b, to, topic, q, what, &r, note, func(a Answer) (string, bool, error) {
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
		}, func(a Answer) (bool, error) { return c.feedFunding(ctx, b, to, topic, a, note) }); err != nil {
			return r, err
		}
	}
	// unserved is the previous seq when no host served it; it is counted
	// Unserved instead of Missing once the seq after it is in To.
	var unserved uint64
	for _, seq := range b.Seqs {
		what := fmt.Sprintf("seq %d", seq)
		q := Query{Stream: b.Stream, Producer: p, Seq: seq}
		st, have, err := c.fill(ctx, b, to, topic, q, what, &r, note, func(a Answer) (string, bool, error) {
			_, v := c.CheckCarrier(ctx, a, b.Stream, b.Producer, seq)
			if !v.OK() {
				return string(v.Code) + " " + v.Reason, false, nil
			}
			return "", true, nil
		}, nil)
		if err != nil {
			return r, err
		}
		if b.Sweeps && st == held {
			if err := c.carrySweeps(ctx, b, to, topic, seq, have, &r, note); err != nil {
				return r, err
			}
		}
		if unserved != 0 && unserved+1 == seq && (st == held || st == filled) {
			r.Missing--
			r.Unserved++
			note("seq %d: served by no host (retracted, or not delivered); seq %d's prev names it: unserved", unserved, seq)
		}
		unserved = 0
		if st == unservedAnywhere {
			unserved = seq
		}
	}
	return r, nil
}

// carrySweeps checks each carrier To serves at seq for a retraction and
// submits the sweep it finds, which To then admits and so stops serving the
// carrier (docs/spec.md section 8.4).
func (c *Client) carrySweeps(ctx context.Context, b Backfill, to *publish.Facade, topic string, seq uint64, have []Answer, r *Result, note func(string, ...any)) error {
	for _, a := range have {
		cand, v := c.CheckCarrier(ctx, a, b.Stream, b.Producer, seq)
		if !v.OK() {
			continue
		}
		sweep, _, err := c.Retraction(ctx, cand)
		if err != nil {
			return err
		}
		if sweep == nil {
			continue
		}
		beef, err := sweep.AtomicBEEF(false)
		if err != nil {
			return err
		}
		if _, err := to.Submit(ctx, topic, beef); err != nil {
			return fmt.Errorf("seq %d: submitting sweep %s to %s: %w", seq, sweep.TxID(), b.To, err)
		}
		r.Sweeps++
		note("seq %d: carrier %s was retracted by the mined sweep %s, which the host lacked: sweep submitted", seq, cand.Tx.TxID(), sweep.TxID())
	}
	return nil
}

// feedFunding submits to the host the funding trees an anchor token's fee
// inputs spend, since a host admits an anchor only once it holds the
// outputs it spends. Each tree is asked of the siblings by the tree class
// and, when none serves it, read from Txs; either way it comes with its
// proof, checked against the headers when there are any, and the host
// checks it again. It reports whether it submitted any.
func (c *Client) feedFunding(ctx context.Context, b Backfill, to *publish.Facade, topic string, a Answer, note func(string, ...any)) (bool, error) {
	siblings := *c
	siblings.Hosts, siblings.Local, siblings.ProveTrees = nil, nil, true
	for _, from := range b.From {
		if from != b.To {
			siblings.Hosts = append(siblings.Hosts, from)
		}
	}
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
		src, from, missing, err := siblings.FundingTree(ctx, b.Stream, id)
		if err != nil {
			return false, err
		}
		if src == nil {
			note("interval %d spends funding tree %s, which nothing asked serves: %s", an.Interval, id, missing)
			continue
		}
		beef, err := src.AtomicBEEF(false)
		if err != nil {
			return false, err
		}
		if _, err := to.Submit(ctx, topic, beef); err != nil {
			return false, fmt.Errorf("submitting funding tree %s: %w", id, err)
		}
		note("interval %d: funding tree %s submitted from %s", an.Interval, id, from)
		fed = true
	}
	return fed, nil
}

// fillStatus is how one position ended.
type fillStatus int

const (
	held fillStatus = iota
	filled
	unservedAnywhere // neither To nor any sibling served it
	notFilled        // refused or not admitted
)

func (c *Client) fill(ctx context.Context, b Backfill, to *publish.Facade, topic string, q Query, what string, r *Result, note func(string, ...any),
	check func(Answer) (string, bool, error), prepare func(Answer) (bool, error)) (fillStatus, []Answer, error) {
	have, err := c.Ask(ctx, b.To, q)
	if err != nil {
		return notFilled, nil, fmt.Errorf("%s: asking %s: %w", what, b.To, err)
	}
	if len(have) > 0 {
		r.Held++
		note("%s: held", what)
		return held, have, nil
	}
	var got []Answer
	var askErr error
	source := ""
	for _, from := range b.From {
		if from == b.To {
			continue
		}
		as, err := c.Ask(ctx, from, q)
		if err != nil {
			// A sibling that cannot be asked is skipped; the others may
			// hold the position.
			askErr = fmt.Errorf("%s: asking %s: %w", what, from, err)
			continue
		}
		if len(as) > 0 {
			got, source = as, from
			break
		}
	}
	if len(got) == 0 {
		if askErr != nil {
			return notFilled, nil, askErr
		}
		r.Missing++
		note("%s: MISSING: no sibling holds it either", what)
		return unservedAnywhere, nil, nil
	}
	admitted := 0
	for _, a := range got {
		why, ok, err := check(a)
		if err != nil {
			return notFilled, nil, err
		}
		if !ok {
			r.Refused++
			note("%s: the sibling %s answered something REFUSED: %s", what, source, why)
			continue
		}
		res, err := to.Submit(ctx, topic, a.Beef)
		if err != nil {
			return notFilled, nil, fmt.Errorf("%s: submitting to %s: %w", what, b.To, err)
		}
		if len(res.Admitted) == 0 && prepare != nil {
			fed, err := prepare(a)
			if err != nil {
				return notFilled, nil, err
			}
			if fed {
				if res, err = to.Submit(ctx, topic, a.Beef); err != nil {
					return notFilled, nil, fmt.Errorf("%s: submitting to %s: %w", what, b.To, err)
				}
			}
		}
		if len(res.Admitted) > 0 {
			admitted++
		}
	}
	if admitted == 0 {
		r.Missing++
		note("%s: MISSING: fetched from the sibling %s, not admitted", what, source)
		return notFilled, nil, nil
	}
	r.Filled++
	note("%s: filled from the sibling %s", what, source)
	return filled, nil, nil
}
