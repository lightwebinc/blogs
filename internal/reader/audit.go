package reader

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bcommon/verify"

	"github.com/lightwebinc/blogs/internal/logrec"
)

// Outcome is an audit's verdict.
type Outcome int

const (
	// Verified: the interval is complete and proven.
	Verified Outcome = iota
	// Refused: the evidence contradicts the claim.
	Refused
	// Incomplete: some evidence was not found; nothing is proven either
	// way, since an empty answer proves nothing.
	Incomplete
)

// Token is one canonical anchor token of the chain walked.
type Token struct {
	Anchor *logrec.Anchor
	Tx     *transaction.Transaction
	Txid   string
	Vout   int
	Height uint32
	Index  uint64 // position in its block
}

// Report is what verify-interval found.
type Report struct {
	Outcome Outcome
	Reason  string
	// Chain is the canonical token of each interval walked, 1 to n.
	Chain []*Token
	// Batches are the interval's batches in seq order, as chosen by the
	// root.
	Batches []*Candidate
	// Missing are seqs of the interval no host answered.
	Missing []uint64
	// Refusals are answers refused along the way, for the trace.
	Refusals []string
}

func (r *Report) refuse(format string, args ...any) *Report {
	r.Outcome, r.Reason = Refused, fmt.Sprintf(format, args...)
	return r
}

func (r *Report) incomplete(format string, args ...any) *Report {
	r.Outcome, r.Reason = Incomplete, fmt.Sprintf(format, args...)
	return r
}

// VerifyInterval is the auditor's check of docs/spec.md section 13 for
// interval n of producer's stream: it walks the interval class from 1 to n,
// keeping each interval's canonical token (interval 1: the one mined first;
// each later one: the one that spends its predecessor), holds every token to
// the anchor rules and its proof to the reader's headers, then asks the
// batch class for every seq interval n covers and requires the root over
// their commitments to equal the token's. The outputs a token's inputs
// spend are read from the predecessor token's transaction, from the funding
// trees the interval's carriers carry, or from Asset.
//
// An error is no verdict: a host or the header source could not be asked.
func (c *Client) VerifyInterval(ctx context.Context, stream string, producer []byte, n uint64) (*Report, error) {
	if c.Headers == nil {
		return nil, errors.New("no header source: a proof cannot be checked")
	}
	if n < 1 || n > logrec.MaxSafe {
		return nil, fmt.Errorf("interval %d is out of range", n)
	}
	r := &Report{}
	pHex := hex.EncodeToString(producer)
	var prev *Token
	for k := uint64(1); k <= n; k++ {
		as, err := c.AskAny(ctx, Query{Stream: stream, Producer: pHex, Interval: k}, true)
		if err != nil {
			return nil, err
		}
		var cands []*Token
		for _, a := range as {
			t, why, err := c.candidate(ctx, a, stream, producer, k)
			if err != nil {
				return nil, err
			}
			if why != "" {
				r.Refusals = append(r.Refusals, fmt.Sprintf("interval %d: %s", k, why))
				continue
			}
			cands = append(cands, t)
		}
		canon := canonical(cands, prev)
		if canon == nil {
			if len(as) == 0 {
				return r.incomplete("interval %d: no host answered; an empty answer proves nothing", k), nil
			}
			return r.incomplete("interval %d: no answered token continues the chain (%d answered)", k, len(as)), nil
		}
		if prev != nil && canon.Anchor.First != prev.Anchor.First+prev.Anchor.Count {
			return r.refuse("interval %d: anchor-join: first %d does not continue interval %d, which ends at seq %d",
				k, canon.Anchor.First, k-1, prev.Anchor.Last()), nil
		}
		r.Chain = append(r.Chain, canon)
		prev = canon
	}
	last := r.Chain[len(r.Chain)-1].Anchor
	// Every batch the interval covers, from every host: forks included.
	held := map[uint64][]*Candidate{}
	for seq := last.First; seq <= last.Last(); seq++ {
		cands, refused, err := c.Batch(ctx, stream, producer, seq, true)
		if err != nil {
			return nil, err
		}
		for _, v := range refused {
			r.Refusals = append(r.Refusals, fmt.Sprintf("seq %d: %s %s", seq, v.Code, v.Reason))
		}
		if len(cands) == 0 {
			r.Missing = append(r.Missing, seq)
			continue
		}
		sort.Slice(cands, func(i, j int) bool {
			return string(cands[i].Carrier.Commitment[:]) < string(cands[j].Carrier.Commitment[:])
		})
		if len(cands) > 8 {
			cands = cands[:8]
		}
		held[seq] = cands
	}
	// The anchor rules, now that the carriers' funding trees are in hand.
	evidence := map[string]*transaction.Transaction{}
	for _, cs := range held {
		for _, cand := range cs {
			if src := cand.Tx.Inputs[0].SourceTransaction; src != nil {
				evidence[src.TxID().String()] = src
			}
		}
	}
	for i, t := range r.Chain {
		var before *Token
		if i > 0 {
			before = r.Chain[i-1]
		}
		coins, missing, err := c.coins(ctx, t.Tx, before, evidence)
		if err != nil {
			return nil, err
		}
		if missing != "" {
			return r.incomplete("interval %d: %s", t.Anchor.Interval, missing), nil
		}
		if _, err := logrec.Admit(ctx, t.Tx, coins, logrec.Host{Stream: stream, Policy: logrec.Policy{Producers: [][]byte{producer}}, Headers: c.Headers}); err != nil {
			return r.refuse("interval %d token %s: %s: %v", t.Anchor.Interval, t.Txid, logrec.Reason(err), err), nil
		}
	}
	if len(r.Missing) > 0 {
		return r.incomplete("interval %d: %d of %d seqs answered by no host", n, len(r.Missing), last.Count), nil
	}
	chosen, ok := choose(held, last)
	if !ok {
		return r.refuse("interval %d: root-mismatch: no choice of the answered batches reproduces the anchor's root", n), nil
	}
	r.Batches = chosen
	r.Outcome = Verified
	return r, nil
}

// candidate reads one answered token for interval k: the record, the
// producer and stream, and a proof the headers hold. A refusal is returned
// as its reason; an error means the headers could not be asked.
func (c *Client) candidate(ctx context.Context, a Answer, stream string, producer []byte, k uint64) (*Token, string, error) {
	_, tx, _, err := transaction.ParseBeef(a.Beef)
	if err != nil || tx == nil {
		return nil, fmt.Sprintf("REFUSED-DECODE BEEF does not parse: %v", err), nil
	}
	an, at, err := logrec.DecodeToken(tx)
	if err != nil {
		return nil, fmt.Sprintf("REFUSED-DECODE %s: %v", logrec.Reason(err), err), nil
	}
	switch {
	case string(an.Producer) != string(producer):
		return nil, "REFUSED-KEY another producer's token", nil
	case an.Stream != stream:
		return nil, "REFUSED-DECODE another stream's token", nil
	case an.Interval != k:
		return nil, fmt.Sprintf("REFUSED-SEQ asked for interval %d, answered interval %d", k, an.Interval), nil
	case uint32(at) != a.Output: //nolint:gosec // an output index
		return nil, fmt.Sprintf("REFUSED-DECODE the answered output %d is not the anchor output %d", a.Output, at), nil
	}
	v := spv(ctx, tx, c.Headers)
	if v.Code == verify.Error {
		return nil, "", errors.New(v.Reason)
	}
	if !v.OK() || tx.MerklePath == nil || !logrec.Mined(ctx, tx, c.Headers) {
		return nil, "REFUSED-BUMP the token is not mined in the header source", nil
	}
	t := &Token{Anchor: an, Tx: tx, Txid: tx.TxID().String(), Vout: at, Height: tx.MerklePath.BlockHeight}
	id := tx.TxID()
	for _, leaf := range tx.MerklePath.Path[0] {
		if leaf.Hash != nil && leaf.Hash.Equal(*id) {
			t.Index = leaf.Offset
		}
	}
	return t, "", nil
}

// canonical is interval 1's first mined token, or the token that spends
// prev's.
func canonical(cands []*Token, prev *Token) *Token {
	var best *Token
	for _, t := range cands {
		if prev != nil {
			pid, _ := chainhash.NewHashFromHex(prev.Txid)
			if t.Anchor.Prev != [32]byte(*pid) {
				continue
			}
			for _, in := range t.Tx.Inputs {
				if in.SourceTXID != nil && in.SourceTXID.Equal(*pid) && in.SourceTxOutIndex == uint32(prev.Vout) { //nolint:gosec // an output index
					return t
				}
			}
			continue
		}
		if best == nil || t.Height < best.Height || (t.Height == best.Height && t.Index < best.Index) {
			best = t
		}
	}
	return best
}

// coins are the outputs tx's inputs spend: from the predecessor token's
// transaction, a transaction in evidence, or Asset. missing names an input
// whose transaction was found nowhere.
func (c *Client) coins(ctx context.Context, tx *transaction.Transaction, prev *Token, evidence map[string]*transaction.Transaction) (logrec.Coins, string, error) {
	coins := logrec.Coins{}
	for i, in := range tx.Inputs {
		if in.SourceTXID == nil {
			return nil, fmt.Sprintf("input %d names no transaction", i), nil
		}
		id := in.SourceTXID.String()
		var src *transaction.Transaction
		switch {
		case prev != nil && prev.Txid == id:
			src = prev.Tx
		case evidence[id] != nil:
			src = evidence[id]
		case c.Asset != nil:
			raw, err := c.Asset.TxRaw(ctx, id)
			if err != nil {
				return nil, fmt.Sprintf("input %d spends %s:%d, whose transaction the node did not answer (%v)", i, id, in.SourceTxOutIndex, err), nil
			}
			t, err := transaction.NewTransactionFromBytes(raw)
			if err != nil || t.TxID().String() != id {
				return nil, "", fmt.Errorf("the node answered transaction %s with other bytes", id)
			}
			evidence[id], src = t, t
		default:
			return nil, fmt.Sprintf("input %d spends %s:%d, a funding tree no answered carrier carries; configure asset to read it from a node", i, id, in.SourceTxOutIndex), nil
		}
		if int(in.SourceTxOutIndex) >= len(src.Outputs) {
			return nil, fmt.Sprintf("input %d spends output %d of %s, which has %d", i, in.SourceTxOutIndex, id, len(src.Outputs)), nil
		}
		o := src.Outputs[in.SourceTxOutIndex]
		coins[transaction.Outpoint{Txid: *in.SourceTXID, Index: in.SourceTxOutIndex}] = logrec.Coin{LockingScript: *o.LockingScript, Satoshis: o.Satoshis}
	}
	return coins, "", nil
}

// choose picks one candidate per seq so that the root is the anchor's: the
// next batch's prev selects a candidate, and each candidate at the last
// seq is tried.
func choose(held map[uint64][]*Candidate, a *logrec.Anchor) ([]*Candidate, bool) {
	for _, tail := range held[a.Last()] {
		chosen := make([]*Candidate, a.Count)
		chosen[a.Count-1] = tail
		ok := true
		for i := int(a.Count) - 2; i >= 0; i-- {
			next := chosen[i+1]
			pick, found := Link(held[a.First+uint64(i)], next)
			if !found {
				ok = false
				break
			}
			chosen[i] = pick
		}
		if !ok {
			continue
		}
		cs := make([][32]byte, len(chosen))
		for i, cand := range chosen {
			cs[i] = cand.Carrier.Commitment
		}
		if logrec.IntervalRoot(cs) == a.Root {
			return chosen, true
		}
	}
	return nil, false
}

// Token reads one answered anchor token for interval k of producer's
// stream: its record, its producer and stream, and a proof the headers
// hold. why names a refusal; an error means the headers could not be asked.
func (c *Client) Token(ctx context.Context, a Answer, stream string, producer []byte, k uint64) (t *Token, why string, err error) {
	if c.Headers == nil {
		return nil, "", errors.New("no header source: a proof cannot be checked")
	}
	return c.candidate(ctx, a, stream, producer, k)
}

// Next is one step of the walk VerifyInterval makes, for a verifier that
// keeps the chain between runs: it finds interval k's canonical token among
// the hosts' answers, given the canonical token of interval k - 1 (nil for
// k = 1), and holds it to the join and the anchor rules. The outputs its
// inputs spend are read from prev, from evidence (transactions by txid),
// or from Asset. An error is no verdict.
func (c *Client) Next(ctx context.Context, stream string, producer []byte, k uint64, prev *Token, evidence map[string]*transaction.Transaction) (*Token, *Report, error) {
	if c.Headers == nil {
		return nil, nil, errors.New("no header source: a proof cannot be checked")
	}
	r := &Report{}
	as, err := c.AskAny(ctx, Query{Stream: stream, Producer: hex.EncodeToString(producer), Interval: k}, true)
	if err != nil {
		return nil, nil, err
	}
	var cands []*Token
	for _, a := range as {
		t, why, err := c.candidate(ctx, a, stream, producer, k)
		if err != nil {
			return nil, nil, err
		}
		if why != "" {
			r.Refusals = append(r.Refusals, fmt.Sprintf("interval %d: %s", k, why))
			continue
		}
		cands = append(cands, t)
	}
	canon := canonical(cands, prev)
	switch {
	case canon == nil && len(as) == 0:
		return nil, r.incomplete("interval %d: no host answered; an empty answer proves nothing", k), nil
	case canon == nil:
		return nil, r.incomplete("interval %d: no answered token continues the chain (%d answered)", k, len(as)), nil
	case prev != nil && canon.Anchor.First != prev.Anchor.First+prev.Anchor.Count:
		return nil, r.refuse("interval %d: anchor-join: first %d does not continue interval %d, which ends at seq %d",
			k, canon.Anchor.First, k-1, prev.Anchor.Last()), nil
	}
	if evidence == nil {
		evidence = map[string]*transaction.Transaction{}
	}
	coins, missing, err := c.coins(ctx, canon.Tx, prev, evidence)
	if err != nil {
		return nil, nil, err
	}
	if missing != "" {
		return nil, r.incomplete("interval %d: %s", k, missing), nil
	}
	if _, err := logrec.Admit(ctx, canon.Tx, coins, logrec.Host{Stream: stream, Policy: logrec.Policy{Producers: [][]byte{producer}}, Headers: c.Headers}); err != nil {
		return nil, r.refuse("interval %d token %s: %s: %v", k, canon.Txid, logrec.Reason(err), err), nil
	}
	r.Chain = []*Token{canon}
	r.Outcome = Verified
	return canon, r, nil
}
