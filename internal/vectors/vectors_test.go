package vectors

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/blogs/internal/logrec"
)

const dir = "../../testdata/vectors"

// TestVectors regenerates every vector and compares it byte for byte with
// the checked-in file, and refuses a checked-in file nothing generates.
func TestVectors(t *testing.T) {
	files, err := Generate()
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range Names(files) {
		have, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatalf("%s: %v (run: GOWORK=off go run ./cmd/vectors)", name, err)
		}
		if !bytes.Equal(have, files[name]) {
			t.Errorf("%s differs from what the codec generates", name)
		}
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if _, ok := files[e.Name()]; !ok {
			t.Errorf("%s: nothing generates it", e.Name())
		}
	}
}

// TestDeterministic runs the generator twice.
func TestDeterministic(t *testing.T) {
	a, err := Generate()
	if err != nil {
		t.Fatal(err)
	}
	b, _ := Generate()
	for k := range a {
		if !bytes.Equal(a[k], b[k]) {
			t.Errorf("%s differs between runs", k)
		}
	}
}

// TestTransactionVectorsFromBEEF admits every transaction case from its
// BEEF bytes, as a host receives it, with the previous coins the case
// lists, against the headers the file lists.
func TestTransactionVectorsFromBEEF(t *testing.T) {
	var f struct {
		Stream  string `json:"stream"`
		Headers []struct {
			Height uint32 `json:"height"`
			Root   string `json:"merkleRoot"`
		} `json:"headers"`
		Transactions []struct {
			Name   string          `json:"name"`
			Policy json.RawMessage `json:"policy"`
			Txid   string          `json:"txid"`
			BEEF   string          `json:"beef"`
			Coins  []struct {
				Txid          string `json:"txid"`
				Vout          uint32 `json:"vout"`
				LockingScript string `json:"lockingScript"`
				Satoshis      uint64 `json:"satoshis"`
			} `json:"previousCoins"`
			Verdict string `json:"verdict"`
			Reason  string `json:"reason"`
			Admits  *struct {
				Kind       string `json:"kind"`
				Commitment string `json:"commitment"`
				Funding    []int  `json:"funding"`
			} `json:"admits"`
		} `json:"transactions"`
	}
	b, err := os.ReadFile(filepath.Join(dir, "transaction-v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &f); err != nil {
		t.Fatal(err)
	}
	headers := Headers{}
	for _, h := range f.Headers {
		r, err := chainhash.NewHashFromHex(h.Root)
		if err != nil {
			t.Fatal(err)
		}
		headers[h.Height] = *r
	}
	for _, c := range f.Transactions {
		beef, _ := hex.DecodeString(c.BEEF)
		tx, err := transaction.NewTransactionFromBEEF(beef)
		if err != nil {
			t.Fatalf("%s: %v", c.Name, err)
		}
		if tx.TxID().String() != c.Txid {
			t.Fatalf("%s: txid", c.Name)
		}
		pol := logrec.Policy{Open: true}
		if string(c.Policy) != `"open"` {
			var ks []string
			if err := json.Unmarshal(c.Policy, &ks); err != nil {
				t.Fatal(err)
			}
			pol = logrec.Policy{}
			for _, k := range ks {
				kb, _ := hex.DecodeString(k)
				pol.Producers = append(pol.Producers, kb)
			}
		}
		coins := logrec.Coins{}
		for _, c := range c.Coins {
			id, err := chainhash.NewHashFromHex(c.Txid)
			if err != nil {
				t.Fatal(err)
			}
			ls, _ := hex.DecodeString(c.LockingScript)
			coins[transaction.Outpoint{Txid: *id, Index: c.Vout}] = logrec.Coin{LockingScript: ls, Satoshis: c.Satoshis}
		}
		a, err := logrec.Admit(context.Background(), tx, coins, logrec.Host{Stream: f.Stream, Policy: pol, Headers: headers})
		if c.Verdict == "refuse" {
			if got := logrec.Reason(err); got != c.Reason {
				t.Errorf("%s: reason %s, want %s (%v)", c.Name, got, c.Reason, err)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: %v", c.Name, err)
			continue
		}
		switch c.Admits.Kind {
		case "batch":
			if a.Carrier == nil || hex.EncodeToString(a.Carrier.Commitment[:]) != c.Admits.Commitment {
				t.Errorf("%s: commitment", c.Name)
			}
		case "none":
			if fmt.Sprint(a.Funding) != fmt.Sprint(c.Admits.Funding) && len(a.Funding)+len(c.Admits.Funding) > 0 {
				t.Errorf("%s: funding %v, want %v", c.Name, a.Funding, c.Admits.Funding)
			}
		}
	}
}

// TestQueryVectorsFromFile reads the checked-in question vectors and holds
// the codec to each verdict.
func TestQueryVectorsFromFile(t *testing.T) {
	var f struct {
		Queries []struct {
			Name    string `json:"name"`
			Query   string `json:"query"`
			Class   string `json:"class"`
			Verdict string `json:"verdict"`
		} `json:"queries"`
	}
	raw, err := os.ReadFile(filepath.Join(dir, "query-v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &f); err != nil || len(f.Queries) == 0 {
		t.Fatalf("query-v1.json: %v", err)
	}
	classes := map[string]bool{}
	for _, q := range f.Queries {
		got, err := logrec.ParseQuery([]byte(q.Query))
		switch {
		case q.Verdict == "refuse" && err == nil:
			t.Errorf("%s: accepted", q.Name)
		case q.Verdict == "answer" && (err != nil || got.Class != q.Class):
			t.Errorf("%s: %v %v", q.Name, got, err)
		case q.Verdict == "answer":
			classes[got.Class] = true
		}
	}
	if len(classes) != 3 {
		t.Errorf("classes accepted: %v, want the three", classes)
	}
}
