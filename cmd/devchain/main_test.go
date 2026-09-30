package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-sdk/script"

	"github.com/lightwebinc/blogs/internal/testchain"
)

func rpc(t *testing.T, h http.Handler, body string) string {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/rpc", strings.NewReader(body)))
	return rec.Body.String()
}

func TestTheJournalReplaysTheChain(t *testing.T) {
	key, _ := ec.PrivateKeyFromBytes([]byte(strings.Repeat("\x42", 32)))
	addr, err := script.NewAddressFromPublicKey(key.PubKey(), false)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "journal")
	c1 := testchain.New(700)
	j1, err := newJournaled(c1, path)
	if err != nil {
		t.Fatal(err)
	}
	if out := rpc(t, j1, `{"id":1,"method":"generatetoaddress","params":[3,"`+addr.AddressString+`"]}`); !strings.Contains(out, `"error":null`) {
		t.Fatal(out)
	}
	rpc(t, j1, `{"id":2,"method":"getinfo","params":[]}`)
	rpc(t, j1, `{"id":3,"method":"sendrawtransaction","params":["00"]}`)
	raw, _ := os.ReadFile(path)
	if n := strings.Count(string(raw), "\n"); n != 1 {
		t.Fatalf("journal holds %d line(s), want only the accepted mining call:\n%s", n, raw)
	}
	c2 := testchain.New(700)
	if _, err := newJournaled(c2, path); err != nil {
		t.Fatal(err)
	}
	if c2.Height() != 703 || c2.Height() != c1.Height() {
		t.Fatalf("replayed tip %d, want %d", c2.Height(), c1.Height())
	}
	// A journal another chain wrote (a different start) is refused, not
	// half applied.
	if err := os.WriteFile(path, []byte(`{"id":1,"method":"sendrawtransaction","params":["00"]}`+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := newJournaled(testchain.New(700), path); err == nil {
		t.Fatal("a journal line the chain refuses was accepted")
	}
}
