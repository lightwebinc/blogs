package reader

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bcommon/nodeapi"
)

// TxSource is a copy of the chain answering a mined transaction by its
// txid: a funding tree an anchor's input spends that no host asked serves
// by the tree class, a sweep, a payment. Its answer is never trusted. The caller holds the bytes to the txid it asked for, and a
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

var errNoTxSource = errors.New("no transaction source: configure asset (a node's asset API) or tx_source (woc:main or woc:test)")

// SpendSource answers which transaction spent an output, for an archive
// checking whether a batch it stored was retracted. Its answer is never
// trusted: Client.Retraction fetches the spender with its proof and checks
// that it spends the output and is mined in the reader's headers.
type SpendSource interface {
	// Spender is the txid of the transaction that spent txid:vout, or ""
	// with a nil error only on the source's evidence that the output is
	// unspent. A source that cannot say answers an error, never "".
	Spender(ctx context.Context, txid string, vout uint32) (string, error)
}

// getBounded fetches url with a bounded body; a 404 is (nil, nil).
func getBounded(ctx context.Context, c *http.Client, timeout time.Duration, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	if c == nil {
		c = &http.Client{Timeout: timeout, Transport: &http.Transport{Proxy: nil}}
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
	case resp.StatusCode == http.StatusNotFound:
		return nil, nil
	case resp.StatusCode != http.StatusOK:
		return nil, fmt.Errorf("GET %s: status %d", url, resp.StatusCode)
	case len(body) > maxTxAnswer:
		return nil, fmt.Errorf("GET %s: answer over %d bytes", url, maxTxAnswer)
	}
	return body, nil
}

// Spender is the node's answer (nodeapi.Asset.Spender): the spender of
// output vout of txid, or "" with a nil error only when the node shows the
// output unspent (status OK). Every other answer, a status that is neither
// OK nor SPENT, a SPENT naming no spender, a transaction the node does not
// serve or a failed read, is an error wrapping nodeapi.ErrSpendUnknown.
func (s AssetSource) Spender(ctx context.Context, txid string, vout uint32) (string, error) {
	return s.Asset.Spender(ctx, txid, vout)
}

// Spender asks WhatsOnChain (/tx/<txid>/<vout>/spent), which answers 404
// for an unspent output.
func (w WhatsOnChain) Spender(ctx context.Context, txid string, vout uint32) (string, error) {
	base := w.Base
	if base == "" {
		base = WhatsOnChainBase
	}
	body, err := getBounded(ctx, w.HTTP, w.Timeout, fmt.Sprintf("%s/%s/tx/%s/%d/spent", strings.TrimRight(base, "/"), w.Network, txid, vout))
	if err != nil || body == nil {
		return "", err
	}
	var sp struct {
		TxID string `json:"txid"`
	}
	if err := json.Unmarshal(body, &sp); err != nil {
		return "", fmt.Errorf("WhatsOnChain (%s): spender of %s:%d: %w", w.Network, txid, vout, err)
	}
	return sp.TxID, nil
}
