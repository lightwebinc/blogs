// Package testchain is a local stand-in for everything outside a blogs
// process that is not an overlay host: a chain that mines what it is sent,
// served as a node's JSON-RPC (generatetoaddress, sendrawtransaction,
// getinfo) and asset API, and as a header source in the overlay bridge's
// native shape (/v1/root/<height>, /v1/tip). It checks what it is sent as a
// node would in the ways that matter here: inputs exist and are unspent,
// coinbase is mature, scripts verify, and nothing non-final is mined. Host
// is a stand-in overlay host running the blogs admission rules.
//
// Each transaction is mined in a block of its own, beside a fixed sibling,
// so every proof is one level deep and every header is a root the chain
// computed. Nothing here is for real use.
package testchain

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/script/interpreter"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/go-sdk/transaction/template/p2pkh"
)

// CoinbaseValue is what each mined block pays.
const CoinbaseValue = 5_000_000_000

// Maturity is the depth at which coinbase may be spent.
const Maturity = 100

// Chain is the local chain.
type Chain struct {
	mu      sync.Mutex
	height  uint32
	roots   map[uint32]chainhash.Hash
	txs     map[string]*transaction.Transaction
	mined   map[string]uint32
	cbAt    map[string]uint32 // coinbase txid to height
	utxo    map[transaction.Outpoint]*transaction.TransactionOutput
	spent   map[transaction.Outpoint]string // outpoint to the txid spending it
	pending []string
	// Hold keeps accepted transactions unmined until Mine; otherwise each
	// is mined as it is accepted. HoldIf holds only those it answers true
	// for.
	Hold   bool
	HoldIf func(tx *transaction.Transaction) bool
	// Refuse, when set, is asked about each transaction sent; a non-empty
	// answer refuses it with that reason.
	Refuse func(tx *transaction.Transaction) string
	// Sent counts the transactions accepted.
	Sent int
}

// New is a chain whose tip is at height start.
func New(start uint32) *Chain {
	return &Chain{height: start, roots: map[uint32]chainhash.Hash{},
		txs: map[string]*transaction.Transaction{}, mined: map[string]uint32{}, cbAt: map[string]uint32{},
		utxo: map[transaction.Outpoint]*transaction.TransactionOutput{}, spent: map[transaction.Outpoint]string{}}
}

func blockHash(h uint32) string { return fmt.Sprintf("%064x", uint64(0xb10c)<<32|uint64(h)) }

// blockAt is the height of a block hash at or below the tip: every height
// has a block, those below the start height empty ones.
func (c *Chain) blockAt(hash string) (uint32, bool) {
	var tag, h uint64
	if len(hash) != 64 || !strings.HasPrefix(hash, strings.Repeat("0", 52)) {
		return 0, false
	}
	if _, err := fmt.Sscanf(hash[52:], "%04x%08x", &tag, &h); err != nil || tag != 0xb10c || h > uint64(c.height) {
		return 0, false
	}
	return uint32(h), true
}

// Height is the tip.
func (c *Chain) Height() uint32 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.height
}

// proof places txid at offset 1 of a two-transaction block at height.
func proof(txid *chainhash.Hash, height uint32) *transaction.MerklePath {
	sibling := chainhash.Hash(bytes.Repeat([]byte{0x33}, 32))
	isTxid := true
	return transaction.NewMerklePath(height, [][]*transaction.PathElement{{
		{Offset: 0, Hash: &sibling},
		{Offset: 1, Hash: txid, Txid: &isTxid},
	}})
}

// mineLocked puts txid in a block of its own at the next height.
func (c *Chain) mineLocked(txid string) {
	c.height++
	id, _ := chainhash.NewHashFromHex(txid)
	root, err := proof(id, c.height).ComputeRoot(id)
	if err != nil {
		panic(err)
	}
	c.roots[c.height] = *root
	c.mined[txid] = c.height
}

// Mine mines every accepted transaction still waiting, each in its own
// block, and returns how many.
func (c *Chain) Mine() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := len(c.pending)
	for _, id := range c.pending {
		c.mineLocked(id)
	}
	c.pending = nil
	return n
}

// Waiting is how many accepted transactions are not yet mined.
func (c *Chain) Waiting() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.pending)
}

// Proof is txid's proof and height, if it mined.
func (c *Chain) Proof(txid string) (*transaction.MerklePath, uint32, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	h, ok := c.mined[txid]
	if !ok {
		return nil, 0, false
	}
	id, _ := chainhash.NewHashFromHex(txid)
	return proof(id, h), h, true
}

// Mined reports whether txid is in a block.
func (c *Chain) Mined(txid string) bool {
	_, _, ok := c.Proof(txid)
	return ok
}

// Tx is a transaction the chain knows.
func (c *Chain) Tx(txid string) *transaction.Transaction {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.txs[txid]
}

// Txids are the txids of every transaction the chain holds, sorted.
func (c *Chain) Txids() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	ids := make([]string, 0, len(c.txs))
	for id := range c.txs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// Generate mines n blocks whose coinbase pays addr, and returns their
// hashes. Waiting transactions are mined first.
func (c *Chain) Generate(n int, addr string) ([]string, error) {
	a, err := script.NewAddressFromString(addr)
	if err != nil {
		return nil, err
	}
	lock, err := p2pkh.Lock(a)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, id := range c.pending {
		c.mineLocked(id)
	}
	c.pending = nil
	var hashes []string
	for range n {
		cb := transaction.NewTransaction()
		var h [4]byte
		copy(h[:], []byte{byte(c.height + 1), byte((c.height + 1) >> 8), byte((c.height + 1) >> 16), byte((c.height + 1) >> 24)})
		us := script.Script(append([]byte{4}, h[:]...))
		zero := chainhash.Hash{}
		cb.AddInput(&transaction.TransactionInput{SourceTXID: &zero, SourceTxOutIndex: 0xffffffff, UnlockingScript: &us, SequenceNumber: transaction.MaxTxInSequenceNum})
		cb.AddOutput(&transaction.TransactionOutput{Satoshis: CoinbaseValue, LockingScript: lock})
		id := cb.TxID().String()
		c.txs[id] = cb
		c.mineLocked(id)
		c.cbAt[id] = c.height
		c.utxo[transaction.Outpoint{Txid: *cb.TxID(), Index: 0}] = cb.Outputs[0]
		hashes = append(hashes, blockHash(c.height))
	}
	return hashes, nil
}

// Send takes a transaction as a node's sendrawtransaction does.
func (c *Chain) Send(tx *transaction.Transaction) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	id := tx.TxID().String()
	if _, ok := c.txs[id]; ok {
		return nil
	}
	if c.Refuse != nil {
		if why := c.Refuse(tx); why != "" {
			return fmt.Errorf("refused: %s", why)
		}
	}
	for _, in := range tx.Inputs {
		if tx.LockTime != 0 && in.SequenceNumber != transaction.MaxTxInSequenceNum {
			return fmt.Errorf("non-final: locktime %d with a non-final input", tx.LockTime)
		}
	}
	for i, in := range tx.Inputs {
		op := transaction.Outpoint{Txid: *in.SourceTXID, Index: in.SourceTxOutIndex}
		prev, ok := c.utxo[op]
		if !ok {
			return fmt.Errorf("missing or spent input %d: %s", i, op.String())
		}
		if at, cb := c.cbAt[in.SourceTXID.String()]; cb && c.height+1-at < Maturity {
			return fmt.Errorf("input %d spends immature coinbase", i)
		}
		if err := interpreter.NewEngine().Execute(
			interpreter.WithTx(tx, i, prev),
			interpreter.WithForkID(),
			interpreter.WithAfterGenesis(),
			interpreter.WithAfterChronicle(),
		); err != nil {
			return fmt.Errorf("input %d: script: %w", i, err)
		}
	}
	for _, in := range tx.Inputs {
		op := transaction.Outpoint{Txid: *in.SourceTXID, Index: in.SourceTxOutIndex}
		delete(c.utxo, op)
		c.spent[op] = tx.TxID().String()
	}
	for i, o := range tx.Outputs {
		c.utxo[transaction.Outpoint{Txid: *tx.TxID(), Index: uint32(i)}] = o //nolint:gosec // an output index
	}
	c.txs[id] = tx
	c.Sent++
	if c.Hold || (c.HoldIf != nil && c.HoldIf(tx)) {
		c.pending = append(c.pending, id)
	} else {
		c.mineLocked(id)
	}
	return nil
}

// IsValidRootForHeight is the chain as a chain tracker.
func (c *Chain) IsValidRootForHeight(_ context.Context, root *chainhash.Hash, height uint32) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	want, ok := c.roots[height]
	return ok && root != nil && *root == want, nil
}

// CurrentHeight is the tip.
func (c *Chain) CurrentHeight(context.Context) (uint32, error) { return c.Height(), nil }

// ServeHTTP serves the node's JSON-RPC at /rpc, its asset API under
// /api/v1/, and the header source at /v1/root/<height> and /v1/tip.
func (c *Chain) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p := r.URL.Path
	switch {
	case r.Method == http.MethodPost && p == "/rpc":
		c.rpc(w, r)
	case p == "/v1/tip":
		h := c.Height()
		writeJSON(w, map[string]any{"height": h, "hash": blockHash(h)})
	case strings.HasPrefix(p, "/v1/root/"):
		h, err := strconv.ParseUint(strings.TrimPrefix(p, "/v1/root/"), 10, 32)
		c.mu.Lock()
		root, ok := c.roots[uint32(h)]
		c.mu.Unlock()
		if err != nil || !ok {
			http.NotFound(w, r)
			return
		}
		writeJSON(w, map[string]any{"height": h, "merkleRoot": root.String()})
	case strings.HasPrefix(p, "/api/v1/"):
		c.asset(w, r, strings.TrimPrefix(p, "/api/v1/"))
	default:
		http.NotFound(w, r)
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func (c *Chain) rpc(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID     any    `json:"id"`
		Method string `json:"method"`
		Params []any  `json:"params"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 8<<20)).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	reply := func(result any, err error) {
		if err != nil {
			writeJSON(w, map[string]any{"id": req.ID, "result": nil, "error": map[string]any{"code": -26, "message": err.Error()}})
			return
		}
		writeJSON(w, map[string]any{"id": req.ID, "result": result, "error": nil})
	}
	switch req.Method {
	case "getinfo":
		reply(map[string]any{"blocks": c.Height(), "connections": 0, "version": 1}, nil)
	case "generatetoaddress":
		n, _ := req.Params[0].(float64)
		addr, _ := req.Params[1].(string)
		hashes, err := c.Generate(int(n), addr)
		reply(hashes, err)
	case "sendrawtransaction":
		s, _ := req.Params[0].(string)
		tx, err := transaction.NewTransactionFromHex(s)
		if err != nil {
			reply(nil, err)
			return
		}
		if err := c.Send(tx); err != nil {
			reply(nil, err)
			return
		}
		reply(tx.TxID().String(), nil)
	default:
		reply(nil, fmt.Errorf("method %q not found", req.Method))
	}
}

func (c *Chain) asset(w http.ResponseWriter, r *http.Request, p string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch {
	case p == "bestblockheader/json":
		writeJSON(w, map[string]any{"hash": blockHash(c.height), "height": c.height, "merkleroot": c.roots[c.height].String()})
	case strings.HasPrefix(p, "txmeta/"):
		id := strings.TrimSuffix(strings.TrimPrefix(p, "txmeta/"), "/json")
		h, ok := c.mined[id]
		if !ok {
			http.NotFound(w, r)
			return
		}
		writeJSON(w, map[string]any{"blockHashes": []string{blockHash(h)}, "blockHeights": []uint32{h}, "subtreeIdxs": []int{0}, "mainChainIndex": 0})
	case strings.HasPrefix(p, "merkle_proof/"):
		id := strings.TrimPrefix(p, "merkle_proof/")
		h, ok := c.mined[id]
		if !ok {
			http.NotFound(w, r)
			return
		}
		txid, _ := chainhash.NewHashFromHex(id)
		_, _ = w.Write(proof(txid, h).Bytes())
	case strings.HasPrefix(p, "utxos/"):
		// The node's UTXO view of a transaction: each output's status, and
		// its spender once spent.
		tx, ok := c.txs[strings.TrimSuffix(strings.TrimPrefix(p, "utxos/"), "/json")]
		if !ok {
			http.NotFound(w, r)
			return
		}
		outs := []map[string]any{}
		for i := range tx.Outputs {
			o := map[string]any{"txid": tx.TxID().String(), "vout": i, "status": "OK"}
			if by, ok := c.spent[transaction.Outpoint{Txid: *tx.TxID(), Index: uint32(i)}]; ok { //nolint:gosec // an output index
				o["status"], o["spendingData"] = "SPENT", map[string]any{"txId": by, "vin": 0}
			}
			outs = append(outs, o)
		}
		writeJSON(w, outs)
	case strings.HasPrefix(p, "tx/"):
		tx, ok := c.txs[strings.TrimPrefix(p, "tx/")]
		if !ok {
			http.NotFound(w, r)
			return
		}
		// As a Teranode asset API answers: Extended Format, coinbases
		// included.
		_, _ = w.Write(c.ef(tx))
	case strings.HasPrefix(p, "block/"):
		hash := strings.TrimSuffix(strings.TrimPrefix(p, "block/"), "/json")
		h, ok := c.blockAt(hash)
		if !ok {
			http.NotFound(w, r)
			return
		}
		type out struct {
			Satoshis      uint64 `json:"satoshis"`
			LockingScript string `json:"lockingScript"`
		}
		cb := map[string]any{"txid": "", "outputs": []out{}}
		for id, at := range c.cbAt {
			if at == h {
				tx := c.txs[id]
				outs := []out{}
				for _, o := range tx.Outputs {
					outs = append(outs, out{o.Satoshis, hex.EncodeToString(*o.LockingScript)})
				}
				cb = map[string]any{"txid": id, "outputs": outs}
			}
		}
		writeJSON(w, map[string]any{"hash": hash, "height": h, "coinbase_tx": cb})
	case p == "blocks":
		off, _ := strconv.ParseUint(r.URL.Query().Get("offset"), 10, 32)
		h := c.height - uint32(off)
		writeJSON(w, map[string]any{"data": []map[string]any{{"height": h, "hash": blockHash(h)}}})
	case strings.HasPrefix(p, "header/"):
		hash := strings.TrimSuffix(strings.TrimPrefix(p, "header/"), "/json")
		h, ok := c.blockAt(hash)
		if !ok {
			http.NotFound(w, r)
			return
		}
		writeJSON(w, map[string]any{"hash": hash, "height": h, "merkleroot": c.roots[h].String()})
	default:
		http.NotFound(w, r)
	}
}

// ef is tx in Extended Format (BRC-30), each input carrying the output it
// spends, or a zero output where the chain holds none, as for a coinbase.
// The caller holds c.mu.
func (c *Chain) ef(tx *transaction.Transaction) []byte {
	cp, err := transaction.NewTransactionFromBytes(tx.Bytes())
	if err != nil {
		panic(err)
	}
	for _, in := range cp.Inputs {
		out := &transaction.TransactionOutput{LockingScript: &script.Script{}}
		if src, ok := c.txs[in.SourceTXID.String()]; ok && int(in.SourceTxOutIndex) < len(src.Outputs) {
			out = src.Outputs[in.SourceTxOutIndex]
		}
		in.SetSourceTxOutput(out)
	}
	b, err := cp.EF()
	if err != nil {
		panic(err)
	}
	return b
}
