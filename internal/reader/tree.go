package reader

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bcommon/verify"
)

// Source answers ls_log questions from somewhere other than a host: an
// archive's files. Its answers are checked exactly as a host's are.
type Source interface {
	Answer(q Query) ([]Answer, error)
	// Name names the source in messages.
	Name() string
}

// CheckTree holds one answer of the tree class to the question: the BEEF's
// subject is the transaction asked for (its double SHA-256 is txid), the
// answered output exists, and the proof the BEEF carries for it verifies
// against the reader's headers. A conforming host holds only mined trees,
// so an answer with no proof is refused. With no header source (which only
// backfill allows) the proof must be present and is left to the host the
// tree is submitted to.
//
// why names a refusal; an error means the headers could not be asked.
func (c *Client) CheckTree(ctx context.Context, a Answer, txid string) (tx *transaction.Transaction, why string, err error) {
	_, tx, _, perr := transaction.ParseBeef(a.Beef)
	if perr != nil || tx == nil {
		return nil, fmt.Sprintf("REFUSED-DECODE BEEF does not parse: %v", perr), nil
	}
	if got := tx.TxID().String(); got != txid {
		return nil, fmt.Sprintf("REFUSED-DECODE asked for transaction %s, answered %s", txid, got), nil
	}
	if int(a.Output) >= len(tx.Outputs) {
		return nil, fmt.Sprintf("REFUSED-DECODE the answered output %d is not one of the transaction's %d", a.Output, len(tx.Outputs)), nil
	}
	if tx.MerklePath == nil {
		return nil, "REFUSED-BUMP the tree comes with no proof", nil
	}
	if c.Headers == nil {
		return tx, "", nil
	}
	v := spv(ctx, tx, c.Headers)
	if v.Code == verify.Error {
		return nil, "", errors.New(v.Reason)
	}
	if !v.OK() {
		return nil, "REFUSED-BUMP the tree's proof is not in the header source", nil
	}
	return tx, "", nil
}

// Tree asks for the mined funding tree txid of stream by the tree class
// (docs/spec.md section 7.2): the local sources first, then the hosts in
// order, each answer held to CheckTree. The first answer that passes is
// returned with the name of what answered it. With none, tried says what
// each source did: held nothing, answered something refused, or could not
// be asked (a host that predates the tree class refuses the question).
func (c *Client) Tree(ctx context.Context, stream, txid string) (tx *transaction.Transaction, from string, tried []string, err error) {
	q := Query{Stream: stream, Tree: txid}
	check := func(name string, as []Answer, askErr error) (*transaction.Transaction, error) {
		if askErr != nil {
			tried = append(tried, fmt.Sprintf("%s did not answer the tree class (%v)", name, askErr))
			return nil, nil
		}
		if len(as) == 0 {
			tried = append(tried, name+" holds no such tree")
			return nil, nil
		}
		for _, a := range as {
			t, why, err := c.CheckTree(ctx, a, txid)
			if err != nil {
				return nil, err
			}
			if why == "" {
				return t, nil
			}
			tried = append(tried, name+" answered something refused: "+why)
		}
		return nil, nil
	}
	for _, s := range c.Local {
		as, askErr := s.Answer(q)
		if tx, err = check(s.Name(), as, askErr); tx != nil || err != nil {
			return tx, s.Name(), tried, err
		}
	}
	for _, h := range c.Hosts {
		as, askErr := c.Ask(ctx, h, q)
		if tx, err = check(h, as, askErr); tx != nil || err != nil {
			return tx, h, tried, err
		}
	}
	return nil, "", tried, nil
}

// FundingTree finds the transaction txid whose output an anchor's input
// spends: by the tree class from the local sources and the hosts, then from
// Txs (a copy of the chain). A tree from the tree class always comes with a
// proof checked against the headers; one from Txs is held to the txid, and
// comes with a checked proof when ProveTrees is set. from names what
// answered it; missing says where it was looked for when nothing did.
func (c *Client) FundingTree(ctx context.Context, stream, txid string) (tx *transaction.Transaction, from, missing string, err error) {
	tx, from, tried, err := c.Tree(ctx, stream, txid)
	if err != nil || tx != nil {
		return tx, from, "", err
	}
	if c.Txs != nil {
		t, ferr := c.Fetch(ctx, txid, c.ProveTrees)
		switch {
		case errors.Is(ferr, ErrOtherBytes):
			return nil, "", "", ferr
		case ferr != nil:
			tried = append(tried, fmt.Sprintf("%s did not answer (%v)", c.Txs.Name(), ferr))
		case c.ProveTrees && c.Headers != nil:
			v := spv(ctx, t, c.Headers)
			if v.Code == verify.Error {
				return nil, "", "", errors.New(v.Reason)
			}
			if v.OK() {
				return t, c.Txs.Name(), "", nil
			}
			tried = append(tried, c.Txs.Name()+" answered a proof that is not in the header source")
		default:
			return t, c.Txs.Name(), "", nil
		}
	} else {
		tried = append(tried, "no copy of the chain is configured (asset, or tx_source woc:main or woc:test)")
	}
	return nil, "", strings.Join(tried, "; "), nil
}
