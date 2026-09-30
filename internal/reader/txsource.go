package reader

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bcommon/nodeapi"
)

// TxSource answers a mined transaction by its txid: the funding trees an
// anchor's fee inputs spend, which no lookup class serves. Its answer is
// never trusted. The caller holds the bytes to the txid it asked for, and a
// proof, when it needs one, to its own header source.
type TxSource interface {
	// Tx returns the transaction; with proof, carrying its merkle path.
	Tx(ctx context.Context, txid string, proof bool) (*transaction.Transaction, error)
	// Name names the source in messages.
	Name() string
}

// AssetSource is a node's asset API.
type AssetSource struct{ Asset *nodeapi.Asset }

// Name names the node.
func (s AssetSource) Name() string { return "the node " + s.Asset.Base }

// Tx reads the raw transaction and, with proof, its merkle path.
func (s AssetSource) Tx(ctx context.Context, txid string, proof bool) (*transaction.Transaction, error) {
	raw, err := s.Asset.TxRaw(ctx, txid)
	if err != nil {
		return nil, err
	}
	tx, err := transaction.NewTransactionFromBytes(raw)
	if err != nil {
		return nil, err
	}
	if proof {
		if tx.MerklePath, _, err = s.Asset.Proof(ctx, txid); err != nil {
			return nil, fmt.Errorf("its proof: %w", err)
		}
	}
	return tx, nil
}

// WhatsOnChain is the public WhatsOnChain API, for an auditor with no node
// of its own. Network is "main" or "test".
type WhatsOnChain struct {
	Network string
	// Base is the API root; empty is the public one.
	Base    string
	Timeout time.Duration
	HTTP    *http.Client
}

// WhatsOnChainBase is the public API root.
const WhatsOnChainBase = "https://api.whatsonchain.com/v1/bsv/"

// maxTxAnswer bounds an answer before it is parsed.
const maxTxAnswer = 16 << 20

// Name names the source.
func (w WhatsOnChain) Name() string { return "WhatsOnChain (" + w.Network + ")" }

// Tx reads the raw transaction (/tx/<txid>/hex) or, with proof, its BEEF
// (/tx/<txid>/beef), which WhatsOnChain serves only once it has mined.
func (w WhatsOnChain) Tx(ctx context.Context, txid string, proof bool) (*transaction.Transaction, error) {
	base := w.Base
	if base == "" {
		base = WhatsOnChainBase
	}
	what := "hex"
	if proof {
		what = "beef"
	}
	url := strings.TrimRight(base, "/") + "/" + w.Network + "/tx/" + txid + "/" + what
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	c := w.HTTP
	if c == nil {
		// No proxy from the environment: the auditor names its sources.
		c = &http.Client{Timeout: w.Timeout, Transport: &http.Transport{Proxy: nil}}
	}
	resp, err := c.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxTxAnswer+1))
	if err != nil {
		return nil, err
	}
	switch {
	case resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusInternalServerError:
		// WhatsOnChain answers an unknown transaction with a 500.
		return nil, fmt.Errorf("WhatsOnChain (%s) has no transaction %s (status %d)", w.Network, txid, resp.StatusCode)
	case resp.StatusCode == http.StatusUnprocessableEntity:
		return nil, fmt.Errorf("WhatsOnChain (%s): %s is not mined yet", w.Network, txid)
	case resp.StatusCode != http.StatusOK:
		return nil, fmt.Errorf("WhatsOnChain (%s): %s: status %d", w.Network, txid, resp.StatusCode)
	case len(body) > maxTxAnswer:
		return nil, fmt.Errorf("WhatsOnChain (%s): %s: answer over %d bytes", w.Network, txid, maxTxAnswer)
	}
	text := string(bytes.TrimSpace(body))
	if proof {
		tx, err := transaction.NewTransactionFromBEEFHex(text)
		if err != nil {
			return nil, err
		}
		if tx.MerklePath == nil {
			return nil, errors.New("the BEEF carries no proof of the transaction")
		}
		return tx, nil
	}
	return transaction.NewTransactionFromHex(text)
}

// fetch asks the source for txid and holds the answer to it: the bytes must
// hash to the txid asked for, and with proof a merkle path must come too.
func (c *Client) fetch(ctx context.Context, txid string, proof bool) (*transaction.Transaction, error) {
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

var errNoTxSource = errors.New("no transaction source: configure asset (a node's asset API) or tx_source (woc:main or woc:test)")
