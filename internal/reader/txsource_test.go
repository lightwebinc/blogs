package reader

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/blogs/internal/testchain"
)

// woc serves a chain's mined transactions in WhatsOnChain's shape:
// /main/tx/<txid>/hex, and /main/tx/<txid>/beef once mined; an unknown
// txid is a 500, as WhatsOnChain answers it. swap, when set, answers every
// hex request with that transaction instead.
func woc(t *testing.T, chain *testchain.Chain, swap *transaction.Transaction) *httptest.Server {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := strings.Split(strings.TrimPrefix(r.URL.Path, "/"), "/")
		if len(p) != 4 || p[0] != "main" || p[1] != "tx" {
			http.NotFound(w, r)
			return
		}
		tx := chain.Tx(p[2])
		if tx == nil {
			http.Error(w, "unknown", http.StatusInternalServerError)
			return
		}
		switch p[3] {
		case "hex":
			if swap != nil {
				tx = swap
			}
			_, _ = w.Write([]byte(tx.Hex()))
		case "beef":
			mp, _, ok := chain.Proof(p[2])
			if !ok {
				http.Error(w, "not mined", http.StatusUnprocessableEntity)
				return
			}
			c := *tx
			c.MerklePath = mp
			b, err := c.BEEFHex()
			if err != nil {
				t.Error(err)
			}
			_, _ = w.Write([]byte(b))
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestWhatsOnChainIsHeldToTheTxid(t *testing.T) {
	chain := testchain.New(700)
	key, _ := ec.PrivateKeyFromBytes([]byte(strings.Repeat("\x42", 32)))
	addr, _ := script.NewAddressFromPublicKey(key.PubKey(), false)
	ids, err := chain.Generate(2, addr.AddressString)
	if err != nil || len(ids) != 2 {
		t.Fatal(err)
	}
	var txids []string
	for h := uint32(701); h <= 702; h++ {
		for _, id := range chainCoinbase(t, chain, h) {
			txids = append(txids, id)
		}
	}
	ctx := context.Background()
	c := &Client{Txs: WhatsOnChain{Network: "main", Base: woc(t, chain, nil).URL}}
	tx, err := c.Fetch(ctx, txids[0], false)
	if err != nil || tx.TxID().String() != txids[0] {
		t.Fatalf("hex: %v", err)
	}
	tx, err = c.Fetch(ctx, txids[0], true)
	if err != nil || tx.MerklePath == nil {
		t.Fatalf("beef: %v", err)
	}
	if _, err := c.Fetch(ctx, strings.Repeat("ab", 32), false); err == nil || errors.Is(err, ErrOtherBytes) {
		t.Fatalf("an unknown txid: %v", err)
	}
	// A source that answers with another transaction is caught.
	c.Txs = WhatsOnChain{Network: "main", Base: woc(t, chain, chain.Tx(txids[1])).URL}
	if _, err := c.Fetch(ctx, txids[0], false); !errors.Is(err, ErrOtherBytes) {
		t.Fatalf("other bytes: %v", err)
	}
	var none Client
	if _, err := none.Fetch(ctx, txids[0], false); err == nil || !strings.Contains(err.Error(), "tx_source") {
		t.Fatalf("no source: %v", err)
	}
}

// chainCoinbase finds the coinbase mined at height h by walking the
// chain's transactions through the proof lookup.
func chainCoinbase(t *testing.T, chain *testchain.Chain, h uint32) []string {
	t.Helper()
	for _, id := range chain.Txids() {
		if _, at, ok := chain.Proof(id); ok && at == h {
			return []string{id}
		}
	}
	t.Fatalf("no transaction at height %d", h)
	return nil
}
