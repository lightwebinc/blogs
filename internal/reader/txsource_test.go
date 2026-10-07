package reader

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bcommon/nodeapi"

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

// wocAt is WhatsOnChain (nodeapi.WoC) at a test server, unthrottled.
func wocAt(t *testing.T, base string) ChainSource {
	t.Helper()
	w, err := nodeapi.NewWoC("main", "")
	if err != nil {
		t.Fatal(err)
	}
	w.Base, w.Rate = strings.TrimRight(base, "/")+"/main", 1000
	return ChainSource{Chain: w, Label: "WhatsOnChain (main)"}
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
		txids = append(txids, chainCoinbase(t, chain, h)...)
	}
	ctx := context.Background()
	c := &Client{Txs: wocAt(t, woc(t, chain, nil).URL)}
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
	// A source that answers with another transaction is caught: by the
	// chain view, before Fetch holds the bytes to the txid itself.
	c.Txs = wocAt(t, woc(t, chain, chain.Tx(txids[1])).URL)
	if tx, err := c.Fetch(ctx, txids[0], false); err == nil || !strings.Contains(err.Error(), "answered") {
		t.Fatalf("other bytes: %v %v", tx, err)
	}
	var none Client
	if _, err := none.Fetch(ctx, txids[0], false); err == nil || !strings.Contains(err.Error(), "configure chain") {
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

// Both sources name an output's spender, and "" while it is unspent.
func TestSpenderFromTheNodeAndWhatsOnChain(t *testing.T) {
	spent := "ab" + strings.Repeat("0", 62)
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/utxos/"+strings.Repeat("1", 64)+"/json" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(`[{"vout":0,"status":"OK"},{"vout":1,"status":"SPENT","spendingData":{"txId":"` + spent + `","vin":0}}]`))
	}))
	defer node.Close()
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/main/tx/"+strings.Repeat("1", 64)+"/1/spent" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(`{"txid":"` + spent + `","vin":0}`))
	}))
	defer api.Close()
	ctx := context.Background()
	id := strings.Repeat("1", 64)
	for _, src := range []SpendSource{ChainSource{Chain: &nodeapi.Asset{Base: node.URL}}, wocAt(t, api.URL)} {
		if got, err := src.Spender(ctx, id, 1); err != nil || got != spent {
			t.Fatalf("%T spent: %q %v", src, got, err)
		}
		if got, err := src.Spender(ctx, id, 0); err != nil || got != "" {
			t.Fatalf("%T unspent: %q %v", src, got, err)
		}
	}
	if _, err := (ChainSource{Chain: &nodeapi.Asset{Base: node.URL}}).Spender(ctx, strings.Repeat("2", 64), 0); err == nil {
		t.Fatal("an unknown transaction answered")
	}
}

// A retraction is looked for only on the node's evidence: an output the
// node shows OK is noted unspent, and every answer that is not evidence
// (a status other than OK or SPENT, a SPENT naming no spender, a
// transaction the node does not serve) is noted as undecided and never as
// unspent, as nodeapi.Asset.Spender answers it.
func TestRetractionNotesUnspentOnlyOnTheNodesWord(t *testing.T) {
	fund := strings.Repeat("1", 64)
	answers := map[uint32]string{
		0: `{"vout":0,"status":"OK"}`,
		1: `{"vout":1,"status":"NOT_FOUND"}`,
		2: `{"vout":2,"status":"IMMATURE"}`,
		3: `{"vout":3,"status":"SPENT"}`,
		4: `{"vout":4,"status":"FROZEN","spendingData":{"txId":"` + strings.Repeat("ab", 32) + `"}}`,
	}
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/utxos/"+fund+"/json" {
			http.NotFound(w, r)
			return
		}
		body := "["
		for v := uint32(0); v < uint32(len(answers)); v++ {
			if v > 0 {
				body += ","
			}
			body += answers[v]
		}
		_, _ = w.Write([]byte(body + "]"))
	}))
	defer node.Close()
	src := ChainSource{Chain: &nodeapi.Asset{Base: node.URL}}
	c := &Client{Headers: testchain.New(100), Txs: src}
	ctx := context.Background()
	retraction := func(txid string, vout uint32) string {
		t.Helper()
		k := cand(1, 1, 0)
		h, err := chainhash.NewHashFromHex(txid)
		if err != nil {
			t.Fatal(err)
		}
		k.Carrier.Funding = transaction.Outpoint{Txid: *h, Index: vout}
		k.Tx = transaction.NewTransaction()
		sweep, why, err := c.Retraction(ctx, k)
		if err != nil || sweep != nil {
			t.Fatalf("%s:%d: %v %v", txid, vout, sweep, err)
		}
		return why
	}
	if why := retraction(fund, 0); !strings.Contains(why, "is unspent") {
		t.Fatalf("status OK: %q", why)
	}
	for _, at := range []struct {
		txid string
		vout uint32
	}{{fund, 1}, {fund, 2}, {fund, 3}, {fund, 4}, {fund, 9}, {strings.Repeat("2", 64), 0}} {
		if _, err := src.Spender(ctx, at.txid, at.vout); !errors.Is(err, nodeapi.ErrSpendUnknown) {
			t.Fatalf("%s:%d: Spender answered %v, not ErrSpendUnknown", at.txid, at.vout, err)
		}
		why := retraction(at.txid, at.vout)
		if strings.Contains(why, "unspent") || !strings.Contains(why, "does not say whether") {
			t.Fatalf("%s:%d: %q", at.txid, at.vout, why)
		}
	}
}
