package main

import (
	"cmp"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/go-sdk/transaction/template/p2pkh"

	"github.com/lightwebinc/bcommon/nodeapi"

	"github.com/lightwebinc/blogs/internal/testchain"
)

// fakeWoC serves the local chain in WhatsOnChain's shape under /test: raw
// transactions, BEEF with the proof once mined (422 before), spends (404
// unspent, 400 unknown) and known transactions. Spends are read from the
// chain's own UTXO view.
func fakeWoC(t *testing.T, chain *testchain.Chain, node string) *httptest.Server {
	t.Helper()
	asset := &nodeapi.Asset{Base: node}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := strings.Split(strings.TrimPrefix(r.URL.Path, "/test/tx/"), "/")
		if !strings.HasPrefix(r.URL.Path, "/test/tx/") {
			http.NotFound(w, r)
			return
		}
		if len(p) == 2 && p[0] == "hash" {
			if chain.Tx(p[1]) == nil {
				http.NotFound(w, r)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]string{"txid": p[1]})
			return
		}
		tx := chain.Tx(p[0])
		switch {
		case len(p) == 2 && p[1] == "hex":
			if tx == nil {
				http.NotFound(w, r)
				return
			}
			_, _ = w.Write([]byte(tx.Hex()))
		case len(p) == 2 && p[1] == "beef":
			if tx == nil {
				http.Error(w, "No such mempool or blockchain transaction", http.StatusInternalServerError)
				return
			}
			mp, _, ok := chain.Proof(p[0])
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
		case len(p) == 3 && p[2] == "spent":
			vout, err := strconv.ParseUint(p[1], 10, 32)
			if err != nil || tx == nil || vout >= uint64(len(tx.Outputs)) {
				http.Error(w, "unknown", http.StatusBadRequest)
				return
			}
			by, err := asset.Spender(r.Context(), p[0], uint32(vout))
			switch {
			case err != nil:
				http.Error(w, "unknown", http.StatusBadRequest)
			case by == "":
				http.NotFound(w, r)
			default:
				_ = json.NewEncoder(w).Encode(map[string]any{"txid": by, "vin": 0, "status": "confirmed"})
			}
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

// noNode turns a funded harness into the zero-infrastructure profile on
// testnet: no asset, no rpc, the chain view WhatsOnChain (here the fake),
// and the broadcaster an arcade (the local chain's). wocBase is a package
// variable, so a test that calls this is not parallel.
func (h *harness) noNode() {
	h.t.Helper()
	srv := fakeWoC(h.t, h.chain, h.chainS.URL)
	old := wocBase
	wocBase = srv.URL
	h.t.Cleanup(func() { wocBase = old })
	delete(h.env, "BLOGS_ASSET")
	delete(h.env, "BLOGS_RPC")
	h.env["BLOGS_NETWORK"] = "test"
	h.env["BLOGS_CHAIN"] = "woc:test"
	h.env["BLOGS_SETTLE"] = "arcade:" + h.chainS.URL + "/arcade"
}

// A producer with no node: the chain view is WhatsOnChain and the
// broadcaster arcade, and everything a producer does works, from the
// first funding tree to an anchor read back and verified.
func TestAProducerNeedsNoNode(t *testing.T) {
	h := newHarness(t)
	stream, producer := h.producer()
	h.noNode()
	r := h.must("", "doctor")
	for _, want := range []string{"node        none", "chain       woc:test", "fees        100/1000 sat/bytes, floor 100, static"} {
		if !strings.Contains(r.stdout, want) {
			t.Fatalf("doctor lacks %q:\n%s", want, r.stdout)
		}
	}
	// A payment imported from its BEEF before it mined is released by the
	// next run once the chain view has its proof.
	key, keyAddr := fundKey(t)
	before := map[string]bool{}
	for _, id := range h.chain.Txids() {
		before[id] = true
	}
	if _, err := h.chain.Generate(101, keyAddr); err != nil {
		t.Fatal(err)
	}
	coins := newTxids(h.chain, before)
	slices.SortFunc(coins, func(a, b string) int {
		_, ha, _ := h.chain.Proof(a)
		_, hb, _ := h.chain.Proof(b)
		return cmp.Compare(ha, hb)
	})
	m := fundAddrLine.FindStringSubmatch(h.must("", "init").stdout)
	payment := pay(t, h.chain, coins[0], key, m[1])
	b, err := payment.BEEFHex()
	if err != nil {
		t.Fatal(err)
	}
	h.must(b, "fund", "-beef", "-")
	if err := h.chain.Send(payment); err != nil {
		t.Fatal(err)
	}
	h.chain.Mine()
	r = h.produce(linesOf(0, 6), stream)
	if !strings.Contains(r.stderr, "change from "+payment.TxID().String()+": mined at height") {
		t.Fatalf("produce did not release the imported payment:\n%s", r.stderr)
	}
	h.must("", "anchor", stream)
	r = h.must("", "read", stream, producer)
	if !strings.Contains(r.stdout, "event ") {
		t.Fatalf("read:\n%s\n%s", r.stdout, r.stderr)
	}
	h.must("", "verify-interval", stream, producer, "1")
	// The coinbase path still names the node it needs.
	h.want(h.run("", "fund", "-blocks", "1"), exitUsage, "coinbase needs a node")
}

// fundKey is a key the tests pay coinbase to and spend from.
func fundKey(t *testing.T) (*ec.PrivateKey, string) {
	t.Helper()
	k, _ := ec.PrivateKeyFromBytes([]byte(strings.Repeat("\x42", 32)))
	a, err := script.NewAddressFromPublicKey(k.PubKey(), false)
	if err != nil {
		t.Fatal(err)
	}
	return k, a.AddressString
}

// newTxids are the transactions the chain holds that before did not.
func newTxids(chain *testchain.Chain, before map[string]bool) []string {
	var out []string
	for _, id := range chain.Txids() {
		if !before[id] {
			out = append(out, id)
			before[id] = true
		}
	}
	return out
}

// pay builds a transaction spending output 0 of parent (paid to key) to
// addr, with parent's proof in its BEEF.
func pay(t *testing.T, chain *testchain.Chain, parentID string, key *ec.PrivateKey, addr string) *transaction.Transaction {
	t.Helper()
	parent := chain.Tx(parentID)
	mp, _, ok := chain.Proof(parentID)
	if parent == nil || !ok {
		t.Fatalf("parent %s is not mined", parentID)
	}
	p := *parent
	p.MerklePath = mp
	unlock, err := p2pkh.Unlock(key, nil)
	if err != nil {
		t.Fatal(err)
	}
	tx := transaction.NewTransaction()
	tx.AddInputFromTx(&p, 0, unlock)
	lock, err := p2pkh.Lock(mustAddress(t, addr))
	if err != nil {
		t.Fatal(err)
	}
	tx.AddOutput(&transaction.TransactionOutput{Satoshis: 40000, LockingScript: lock})
	if err := tx.Sign(); err != nil {
		t.Fatal(err)
	}
	return tx
}

func mustAddress(t *testing.T, addr string) *script.Address {
	t.Helper()
	a, err := script.NewAddressFromString(addr)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

// fund -beef takes the payment as the wallet hands it over, with no
// lookup: a mined one at once, an unmined one whose parent is proven held
// back until its proof arrives (or refused with -mined-only), and one
// that pays someone else refused.
func TestFundFromBEEF(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	r := h.must("", "init")
	m := fundAddrLine.FindStringSubmatch(r.stdout)
	if m == nil {
		t.Fatalf("init printed no fund address:\n%s", r.stdout)
	}
	fundAddr := m[1]
	key, keyAddr := fundKey(t)
	before := map[string]bool{}
	for _, id := range h.chain.Txids() {
		before[id] = true
	}
	// Mature coinbase to spend: the two oldest of 102.
	if _, err := h.chain.Generate(102, keyAddr); err != nil {
		t.Fatal(err)
	}
	coins := newTxids(h.chain, before)
	slices.SortFunc(coins, func(a, b string) int {
		_, ha, _ := h.chain.Proof(a)
		_, hb, _ := h.chain.Proof(b)
		return cmp.Compare(ha, hb)
	})
	coin, other := coins[0], coins[1]
	dir := t.TempDir()
	write := func(name string, tx *transaction.Transaction) string {
		b, err := tx.BEEF()
		if err != nil {
			t.Fatal(err)
		}
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, b, 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	// Unmined, its parent proven: accepted and held.
	payment := pay(t, h.chain, coin, key, fundAddr)
	file := write("unmined.beef", payment)
	h.want(h.run("", "fund", "-beef", file, "-mined-only"), exitRefused, "has not mined yet")
	r = h.must("", "fund", "-beef", file)
	if !strings.Contains(r.stdout, "imported 1 of 1 output(s) paying "+fundAddr+", 40000 sat, not mined yet") {
		t.Fatalf("fund -beef unmined:\n%s\n%s", r.stdout, r.stderr)
	}
	if r = h.must("", "doctor"); !strings.Contains(r.stdout, "change from 1 transaction(s) held until mined") {
		t.Fatalf("doctor:\n%s", r.stdout)
	}
	// The same payment mined, as hex on stdin.
	if err := h.chain.Send(payment); err != nil {
		t.Fatal(err)
	}
	h.chain.Mine()
	mp, _, ok := h.chain.Proof(payment.TxID().String())
	if !ok {
		t.Fatal("the payment did not mine")
	}
	payment.MerklePath = mp
	hexBEEF, err := payment.BEEFHex()
	if err != nil {
		t.Fatal(err)
	}
	r = h.must(hexBEEF+"\n", "fund", "-beef", "-")
	if !strings.Contains(r.stdout, "40000 sat, mined at height") || !strings.Contains(r.stderr, "1 output(s) held until mined are spendable now") {
		t.Fatalf("fund -beef mined:\n%s\n%s", r.stdout, r.stderr)
	}
	if r = h.must("", "doctor"); strings.Contains(r.stdout, "held until mined") {
		t.Fatalf("doctor:\n%s", r.stdout)
	}
	// A payment to someone else.
	h.want(h.run("", "fund", "-beef", write("other.beef", pay(t, h.chain, other, key, keyAddr))), exitRefused,
		"pays nothing to this wallet's fund address")
	h.want(h.run("", "fund", "-beef", filepath.Join(dir, "absent")), exitUsage, "absent")
	h.want(h.run("", "fund", "-beef", file, "-txid", coin), exitUsage, "exclusive")
}

// fund -txid with no node: the payment and its proof come from
// WhatsOnChain (here the fake), the proof checked against the headers.
func TestFundByTxidWithNoNode(t *testing.T) {
	h := newHarness(t)
	r := h.must("", "init")
	h.noNode()
	before := map[string]bool{}
	for _, id := range h.chain.Txids() {
		before[id] = true
	}
	m := fundAddrLine.FindStringSubmatch(r.stdout)
	if _, err := h.chain.Generate(1, m[1]); err != nil {
		t.Fatal(err)
	}
	paid := newTxids(h.chain, before)[0]
	r = h.must("", "fund", "-txid", paid)
	if !strings.Contains(r.stdout, "imported 1 of 1 output(s) paying "+m[1]+", 5000000000 sat, mined at height 701") {
		t.Fatalf("fund -txid:\n%s\n%s", r.stdout, r.stderr)
	}
	// Coinbase keeps its maturity.
	if r = h.must("", "doctor"); !strings.Contains(r.stdout, "1 output(s), 5000000000 sat, 1 immature") {
		t.Fatalf("doctor:\n%s", r.stdout)
	}
	_, keyAddr := fundKey(t)
	if _, err := h.chain.Generate(1, keyAddr); err != nil {
		t.Fatal(err)
	}
	h.want(h.run("", "fund", "-txid", newTxids(h.chain, before)[0]), exitRefused, "pays nothing to this wallet's fund address")
	h.want(h.run("", "fund", "-txid", strings.Repeat("ab", 32)), exitUsage, "not known")
}

// The chain view, header source and broadcaster default by network, and
// the older keys still name a node.
func TestChainDefaults(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.must("", "init")
	for _, k := range []string{"BLOGS_ASSET", "BLOGS_RPC", "BLOGS_SETTLE", "BLOGS_HEADER_URL"} {
		delete(h.env, k)
	}
	h.env["BLOGS_NETWORK"] = "test"
	h.env["BLOGS_TIMEOUT"] = "1s"
	h.env["BLOGS_CHAIN"] = "woc:main"
	h.want(h.run("", "doctor"), exitUsage, "names WhatsOnChain's main network but network is test")
	h.env["BLOGS_CHAIN"] = "nonsense:x"
	h.want(h.run("", "doctor"), exitUsage, "bad chain specification")
	delete(h.env, "BLOGS_CHAIN")
	h.env["BLOGS_FEE_RATE"] = "1/0"
	h.want(h.run("", "doctor"), exitUsage, "fee_rate")
	h.env["BLOGS_FEE_RATE"] = "50/1000"
	h.env["BLOGS_FEE_SOURCE"] = "oracle"
	h.want(h.run("", "doctor"), exitUsage, "fee_source must be static or arc")
	delete(h.env, "BLOGS_FEE_SOURCE")
	// The defaults are reported without asking anything of the network:
	// the header source and broadcaster lines may fail offline.
	r := h.run("", "-header-url", h.chainS.URL, "-settle", "tcp:127.0.0.1:9", "doctor")
	for _, want := range []string{"chain       woc:test", "fees        50/1000 sat/bytes, floor 100, static", "settle      tcp:127.0.0.1:9"} {
		if !strings.Contains(r.stdout, want) {
			t.Fatalf("doctor lacks %q:\n%s\n%s", want, r.stdout, r.stderr)
		}
	}
	h.env["BLOGS_ASSET"] = h.chainS.URL
	if r = h.run("", "-header-url", h.chainS.URL, "doctor"); !strings.Contains(r.stdout, "chain       asset:"+h.chainS.URL) {
		t.Fatalf("asset is the chain view:\n%s", r.stdout)
	}
}
