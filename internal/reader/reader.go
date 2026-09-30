// Package reader asks overlay hosts the two ls_log questions and checks
// every answer itself (docs/spec.md sections 7.2 and 13): a batch carrier's
// rules, its funding parent's proof against the reader's own headers, an
// anchor token's rules and its proof, the chain of anchors and an
// interval's root. A host is trusted for nothing: an answer is either
// verified here or reported as refused, and an empty answer means only that
// the host holds nothing that answers.
package reader

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/go-sdk/transaction/chaintracker"

	"github.com/lightwebinc/bcommon/hostset"
	"github.com/lightwebinc/bcommon/lookup"
	"github.com/lightwebinc/bcommon/nodeapi"
	"github.com/lightwebinc/bcommon/verify"

	"github.com/lightwebinc/blogs/internal/logrec"
)

// Client asks hosts and checks their answers.
type Client struct {
	// Hosts are the overlay hosts' base URLs, asked in order.
	Hosts []string
	// Headers is the reader's own header source. Nil skips the proof
	// checks, which only backfill allows: the host it submits to checks
	// proofs itself.
	Headers chaintracker.ChainTracker
	// Asset, when set, answers raw transactions by txid: the funding trees
	// an anchor's fee inputs spend, which no lookup class serves.
	Asset *nodeapi.Asset
	// Timeout bounds each request; HTTP, when set, is the client used (tests).
	Timeout time.Duration
	HTTP    *http.Client
}

// Query is one ls_log question: exactly one of Seq and Interval is set.
type Query struct {
	Stream   string `json:"stream"`
	Producer string `json:"producer"`
	Seq      uint64 `json:"seq,omitempty"`
	Interval uint64 `json:"interval,omitempty"`
}

// Answer is one output a host answered, and which host.
type Answer struct {
	Host   string
	Beef   []byte
	Output uint32
}

// Ask asks one host one question.
func (c *Client) Ask(ctx context.Context, base string, q Query) ([]Answer, error) {
	hs := &hostset.Client{Timeout: c.Timeout, HTTP: c.HTTP}
	res, err := lookup.Query(ctx, hs, base, lookup.Question{Service: logrec.LookupService, Query: q})
	if err != nil {
		return nil, err
	}
	var out []Answer
	for _, r := range res {
		for _, o := range r.Answer.Outputs {
			out = append(out, Answer{Host: base, Beef: o.Beef, Output: o.OutputIndex})
		}
	}
	return out, nil
}

// AskAny asks the hosts in order and returns the first non-empty answer.
// all asks every host and returns the union, the first copy of each
// transaction kept. Hosts that fail are skipped; when every host failed,
// the first failure is returned.
func (c *Client) AskAny(ctx context.Context, q Query, all bool) ([]Answer, error) {
	if len(c.Hosts) == 0 {
		return nil, errors.New("no overlay host configured (config key hosts, or -host)")
	}
	var first error
	failed := 0
	var out []Answer
	seen := map[string]bool{}
	for _, h := range c.Hosts {
		as, err := c.Ask(ctx, h, q)
		if err != nil {
			failed++
			if first == nil {
				first = err
			}
			continue
		}
		for _, a := range as {
			id := subjectID(a.Beef)
			if seen[id] {
				continue
			}
			seen[id] = true
			out = append(out, a)
		}
		if len(out) > 0 && !all {
			return out, nil
		}
	}
	if failed == len(c.Hosts) {
		return nil, first
	}
	return out, nil
}

func subjectID(beef []byte) string {
	_, _, id, err := transaction.ParseBeef(beef)
	if err != nil || id == nil {
		return hex.EncodeToString(beef)
	}
	return id.String()
}

// Verdict is the outcome of checking one answer.
type Verdict struct {
	Code   verify.Code
	Reason string
}

// OK reports a verified answer.
func (v Verdict) OK() bool { return v.Code == verify.Verified }

// Candidate is a verified batch carrier.
type Candidate struct {
	Carrier *logrec.Carrier
	Tx      *transaction.Transaction
	Beef    []byte
	Host    string
}

func codeFor(err error) verify.Code {
	switch logrec.Reason(err) {
	case "producer-policy":
		return verify.RefusedKey
	case "signature":
		return verify.RefusedSig
	case "lock":
		return verify.RefusedKeyDerive
	case "mineable":
		return verify.RefusedMineable
	case "unlock":
		return verify.RefusedUnlocking
	case "unmined":
		return verify.RefusedBump
	}
	return verify.RefusedDecode
}

// CheckCarrier holds one answered output to what the batch at seq of
// producer on stream must be: the carrier rules of section 8.1 (the
// producer set being just this producer), the asked seq, output 0, and SPV
// through its funding parent against the reader's headers.
func (c *Client) CheckCarrier(ctx context.Context, a Answer, stream string, producer []byte, seq uint64) (*Candidate, Verdict) {
	_, tx, _, err := transaction.ParseBeef(a.Beef)
	if err != nil || tx == nil {
		return nil, Verdict{verify.RefusedDecode, fmt.Sprintf("BEEF does not parse: %v", err)}
	}
	adm, err := logrec.Admit(ctx, tx, nil, logrec.Host{Stream: stream, Policy: logrec.Policy{Producers: [][]byte{producer}}})
	if err != nil {
		return nil, Verdict{codeFor(err), logrec.Reason(err) + ": " + err.Error()}
	}
	if adm.Kind != logrec.KindBatch {
		return nil, Verdict{verify.RefusedDecode, "not a batch carrier"}
	}
	if a.Output != 0 {
		return nil, Verdict{verify.RefusedDecode, fmt.Sprintf("the answered output %d is not the record output 0", a.Output)}
	}
	if adm.Carrier.Batch.Seq != seq {
		return nil, Verdict{verify.RefusedSeq, fmt.Sprintf("asked for seq %d, answered seq %d", seq, adm.Carrier.Batch.Seq)}
	}
	if c.Headers != nil {
		if v := spv(ctx, tx, c.Headers); !v.OK() {
			return nil, v
		}
	}
	return &Candidate{Carrier: adm.Carrier, Tx: tx, Beef: a.Beef, Host: a.Host}, Verdict{Code: verify.Verified}
}

func spv(ctx context.Context, tx *transaction.Transaction, t chaintracker.ChainTracker) Verdict {
	switch k, err := verify.Check(ctx, tx, t); k {
	case verify.Passed:
		return Verdict{Code: verify.Verified}
	case verify.ProofRefused, verify.AncestryMissing:
		return Verdict{verify.RefusedBump, "the proof is not in the header source: " + err.Error()}
	case verify.ScriptRefused:
		return Verdict{verify.RefusedSig, "an input does not satisfy the output it spends: " + err.Error()}
	default:
		return Verdict{verify.Error, "could not verify: " + err.Error()}
	}
}

// Batch asks for the batch at seq and checks every answer. all asks every
// host (a fork check); otherwise the first host that answers is used.
func (c *Client) Batch(ctx context.Context, stream string, producer []byte, seq uint64, all bool) ([]*Candidate, []Verdict, error) {
	as, err := c.AskAny(ctx, Query{Stream: stream, Producer: hex.EncodeToString(producer), Seq: seq}, all)
	if err != nil {
		return nil, nil, err
	}
	var cands []*Candidate
	var refused []Verdict
	for _, a := range as {
		cand, v := c.CheckCarrier(ctx, a, stream, producer, seq)
		if v.Code == verify.Error {
			return nil, nil, errors.New(v.Reason)
		}
		if !v.OK() {
			refused = append(refused, v)
			continue
		}
		cands = append(cands, cand)
	}
	return cands, refused, nil
}

// Link chooses, among the candidates at a seq, the one the next batch's
// prev names. With no next batch, or none named, a single candidate is
// chosen; several are a fork the caller reports.
func Link(cands []*Candidate, next *Candidate) (*Candidate, bool) {
	if next != nil {
		for _, c := range cands {
			if c.Carrier.Commitment == next.Carrier.Batch.Prev {
				return c, true
			}
		}
	}
	if len(cands) == 1 {
		return cands[0], true
	}
	return nil, false
}

// Linked reports whether cand's prev is prev's commitment.
func Linked(prev, cand *Candidate) bool {
	return prev != nil && bytes.Equal(prev.Carrier.Commitment[:], cand.Carrier.Batch.Prev[:])
}
