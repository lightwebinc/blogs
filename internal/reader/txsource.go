package reader

import (
	"context"
	"errors"
	"fmt"

	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bcommon/nodeapi"
)

// TxSource is a copy of the chain answering a mined transaction by its
// txid: a funding tree an anchor's input spends that no host asked serves
// by the tree class, a sweep, a payment. Its answer is never trusted. The
// caller holds the bytes to the txid it asked for, and a proof, when it
// needs one, to its own header source.
type TxSource interface {
	// Tx returns the transaction; with proof, carrying its merkle path.
	Tx(ctx context.Context, txid string, proof bool) (*transaction.Transaction, error)
	// Name names the source in messages.
	Name() string
}

// ChainSource is a chain view (nodeapi.ParseChain): a node, WhatsOnChain,
// or both. Its transactions are held to the txid and its proofs to the
// header source the view was built with (nodeapi.Checked).
type ChainSource struct {
	Chain nodeapi.Chain
	// Label names the view in messages, such as its specification.
	Label string
}

// Name names the view.
func (s ChainSource) Name() string { return s.Label }

// Tx reads the raw transaction and, with proof, its merkle path.
func (s ChainSource) Tx(ctx context.Context, txid string, proof bool) (*transaction.Transaction, error) {
	raw, err := s.Chain.TxRaw(ctx, txid)
	if err != nil {
		return nil, err
	}
	tx, err := transaction.NewTransactionFromBytes(raw)
	if err != nil {
		return nil, err
	}
	if proof {
		if tx.MerklePath, _, err = s.Chain.Proof(ctx, txid); err != nil {
			return nil, fmt.Errorf("its proof: %w", err)
		}
	}
	return tx, nil
}

// Spender is the view's answer (nodeapi.SpendSource): the spender of
// output vout of txid, or "" with a nil error only on the view's evidence
// that the output is unspent. Every other answer is an error wrapping
// nodeapi.ErrSpendUnknown.
func (s ChainSource) Spender(ctx context.Context, txid string, vout uint32) (string, error) {
	return s.Chain.Spender(ctx, txid, vout)
}

// Fetch asks the source for txid and holds the answer to it: the bytes must
// hash to the txid asked for, and with proof a merkle path must come too.
// The path itself is the caller's to check against its headers.
func (c *Client) Fetch(ctx context.Context, txid string, proof bool) (*transaction.Transaction, error) {
	if c.Txs == nil {
		return nil, errNoTxSource
	}
	tx, err := c.Txs.Tx(ctx, txid, proof)
	if err != nil {
		return nil, err
	}
	if tx.TxID().String() != txid {
		return nil, fmt.Errorf("%w: %s answered transaction %s with %s", ErrOtherBytes, c.Txs.Name(), txid, tx.TxID())
	}
	if proof && tx.MerklePath == nil {
		return nil, fmt.Errorf("%s answered transaction %s with no proof", c.Txs.Name(), txid)
	}
	return tx, nil
}

// ErrOtherBytes is a source that answered a txid with another
// transaction: a source that lies, which is an error rather than missing
// evidence.
var ErrOtherBytes = errors.New("other bytes")

var errNoTxSource = errors.New("no chain view: configure chain (woc:main, woc:test or asset:URL)")

// SpendSource answers which transaction spent an output, for an archive
// checking whether a batch it stored was retracted (nodeapi.SpendSource).
// Its answer is never trusted: Client.Retraction fetches the spender with
// its proof and checks that it spends the output and is mined in the
// reader's headers.
type SpendSource = nodeapi.SpendSource
