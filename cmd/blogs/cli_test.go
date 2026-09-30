package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/lightwebinc/blogs/internal/state"
	"github.com/lightwebinc/blogs/internal/testchain"
)

// harness is a home directory, a local chain and a stand-in host, and the
// environment that points the command at them.
type harness struct {
	t      *testing.T
	home   string
	chain  *testchain.Chain
	host   *testchain.Host
	chainS *httptest.Server
	hostS  *httptest.Server
	env    map[string]string
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	h := &harness{t: t, home: filepath.Join(t.TempDir(), "home"), chain: testchain.New(700)}
	h.host = testchain.NewHost(h.chain)
	h.chainS = httptest.NewServer(h.chain)
	h.hostS = httptest.NewServer(h.host)
	t.Cleanup(h.chainS.Close)
	t.Cleanup(h.hostS.Close)
	h.env = map[string]string{
		"BLOGS_HOME":       h.home,
		"BLOGS_NETWORK":    "regtest",
		"BLOGS_ASSET":      h.chainS.URL,
		"BLOGS_RPC":        h.chainS.URL + "/rpc",
		"BLOGS_SETTLE":     "rpc:" + h.chainS.URL + "/rpc",
		"BLOGS_FACADE":     h.hostS.URL,
		"BLOGS_HOSTS":      h.hostS.URL,
		"BLOGS_HEADER_URL": h.chainS.URL,
		"LANG":             "C.UTF-8",
	}
	return h
}

type result struct {
	code           int
	stdout, stderr string
}

func (h *harness) runCtx(ctx context.Context, stdin string, args ...string) result {
	h.t.Helper()
	var out, errb bytes.Buffer
	code := run(ctx, args, strings.NewReader(stdin), &out, &errb, func(k string) string { return h.env[k] })
	return result{code, out.String(), errb.String()}
}

func (h *harness) run(stdin string, args ...string) result {
	h.t.Helper()
	return h.runCtx(context.Background(), stdin, args...)
}

// must runs a command that must exit 0.
func (h *harness) must(stdin string, args ...string) result {
	h.t.Helper()
	r := h.run(stdin, args...)
	if r.code != 0 {
		h.t.Fatalf("blogs %s: exit %d\nstdout:\n%s\nstderr:\n%s", strings.Join(args, " "), r.code, r.stdout, r.stderr)
	}
	return r
}

func (h *harness) want(r result, code int, stderrHas string) {
	h.t.Helper()
	if r.code != code || !strings.Contains(r.stderr+r.stdout, stderrHas) {
		h.t.Fatalf("exit %d, want %d with %q\nstdout:\n%s\nstderr:\n%s", r.code, code, stderrHas, r.stdout, r.stderr)
	}
}

var streamLine = regexp.MustCompile(`(?m)^stream   ([a-z_]+)$`)
var identityLine = regexp.MustCompile(`(?m)^identity     ([0-9a-f]{66})$`)

// producer initialises and funds the home, creates a stream, and returns
// the stream and the producer's key.
func (h *harness) producer() (string, string) {
	h.t.Helper()
	r := h.must("", "init")
	id := identityLine.FindStringSubmatch(r.stdout)
	if id == nil {
		h.t.Fatalf("init printed no identity:\n%s", r.stdout)
	}
	h.must("", "fund", "-blocks", "102")
	r = h.must("", "stream", "new", "app")
	s := streamLine.FindStringSubmatch(r.stdout)
	if s == nil {
		h.t.Fatalf("stream new printed no stream:\n%s", r.stdout)
	}
	return s[1], id[1]
}

// fast are produce flags for a test: poll often, small trees.
var fast = []string{"-poll", "5ms", "-tree-count", "4", "-ahead", "1", "-anchor-tree-count", "3", "-anchor-ahead", "1"}

func (h *harness) produce(stdin, stream string, extra ...string) result {
	h.t.Helper()
	return h.must(stdin, append(append([]string{"produce", stream, "-max-rate", "50"}, fast...), extra...)...)
}

func linesOf(from, n int) string {
	var b strings.Builder
	for i := range n {
		b.WriteString("event ")
		b.WriteString(strings.Repeat("x", i%3))
		b.WriteString(" ")
		b.WriteString(strconv.Itoa(from + i))
		b.WriteString("\n")
	}
	return b.String()
}

func TestInitProduceReadVerify(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	stream, producer := h.producer()
	if !regexp.MustCompile(`^app_[a-z]{10}$`).MatchString(stream) {
		t.Fatalf("stream %q", stream)
	}
	fi, err := os.Stat(h.home)
	if err != nil || fi.Mode().Perm() != 0o700 {
		t.Fatalf("home mode %v %v", fi.Mode(), err)
	}
	// init again leaves the identity alone.
	r := h.must("", "init")
	if !strings.Contains(r.stdout, producer) || !strings.Contains(r.stderr, "exists") {
		t.Fatalf("second init:\n%s%s", r.stdout, r.stderr)
	}
	in := "one\ntwo\nthree\nfour\nfive\n"
	r = h.produce(in, stream, "-max-lines", "2")
	if !strings.Contains(r.stdout, "published seq 1 to 3") || !strings.Contains(r.stdout, "anchored to seq 3: interval 1") {
		t.Fatalf("produce summary:\n%s\n%s", r.stdout, r.stderr)
	}
	r = h.must("", "read", stream, producer)
	if r.stdout != in {
		t.Fatalf("read printed %q, want %q\n%s", r.stdout, in, r.stderr)
	}
	r = h.must("", "read", stream, producer, "-meta", "-from", "2", "-to", "3")
	if !strings.Contains(r.stdout, "# seq 2 ") || !strings.Contains(r.stdout, "VERIFIED  linked") || strings.Contains(r.stdout, "one") {
		t.Fatalf("read -meta:\n%s", r.stdout)
	}
	r = h.must("", "read", stream, producer, "-json")
	var got []batchJSON
	dec := json.NewDecoder(strings.NewReader(r.stdout))
	for dec.More() {
		var b batchJSON
		if err := dec.Decode(&b); err != nil {
			t.Fatal(err)
		}
		got = append(got, b)
	}
	if len(got) != 3 || got[0].Link != "linked" || got[2].Lines[0] != "five" || got[1].Verdict != "VERIFIED" {
		t.Fatalf("read -json: %+v", got)
	}
	r = h.must("", "verify-interval", stream, producer, "1")
	if !strings.Contains(r.stdout, "VERIFIED interval 1") || !strings.Contains(r.stdout, "seqs 1 to 3") {
		t.Fatalf("verify-interval:\n%s", r.stdout)
	}
	h.want(h.run("", "verify-interval", stream, producer, "2"), exitIncomplete, "INCOMPLETE interval 2")
	r = h.must("", "stream", "list")
	if !strings.Contains(r.stdout, stream+"  seq 3  anchored to seq 3 (interval 1)") {
		t.Fatalf("stream list:\n%s", r.stdout)
	}
	r = h.must("", "doctor")
	for _, want := range []string{"identity    " + producer, "stream      " + stream + " seq 3", "headers     " + h.chainS.URL + " tip", "host        " + h.hostS.URL + " answering"} {
		if !strings.Contains(r.stdout, want) {
			t.Fatalf("doctor lacks %q:\n%s", want, r.stdout)
		}
	}
	st, err := state.Load(h.home, stream)
	if err != nil || st.Seq != 3 || st.Pending != "" {
		t.Fatalf("state: %+v %v", st, err)
	}
}

func TestATransactionSourceMustMatchTheNetwork(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	h.env["BLOGS_TX_SOURCE"] = "woc:main"
	h.want(h.run("", "doctor"), exitUsage, "tx_source woc:main is the main network but network is regtest")
	h.env["BLOGS_TX_SOURCE"] = "https://api.example.com"
	h.want(h.run("", "doctor"), exitUsage, "tx_source must be woc:main or woc:test")
}

var fundAddrLine = regexp.MustCompile(`(?m)^fund address (\S+) `)

func TestFundImportsAPayment(t *testing.T) {
	t.Parallel()
	h := newHarness(t)
	r := h.must("", "init")
	m := fundAddrLine.FindStringSubmatch(r.stdout)
	if m == nil {
		t.Fatalf("init printed no fund address:\n%s", r.stdout)
	}
	before := map[string]bool{}
	for _, id := range h.chain.Txids() {
		before[id] = true
	}
	if _, err := h.chain.Generate(1, m[1]); err != nil {
		t.Fatal(err)
	}
	var paid string
	for _, id := range h.chain.Txids() {
		if !before[id] {
			paid = id
		}
	}
	r = h.must("", "fund", "-txid", paid)
	if !strings.Contains(r.stdout, "imported 1 of 1 output(s) paying "+m[1]+", 5000000000 sat, mined at height 701") {
		t.Fatalf("fund -txid:\n%s\n%s", r.stdout, r.stderr)
	}
	r = h.must("", "fund", "-txid", paid)
	if !strings.Contains(r.stderr, "in the pool already") {
		t.Fatalf("a second import:\n%s\n%s", r.stdout, r.stderr)
	}
	// Coinbase keeps its maturity: nothing is spendable yet.
	r = h.must("", "doctor")
	if !strings.Contains(r.stdout, "1 output(s), 5000000000 sat, 1 immature") {
		t.Fatalf("doctor:\n%s", r.stdout)
	}
	other := strings.Repeat("ab", 32)
	h.want(h.run("", "fund", "-txid", other), exitUsage, other)
	h.want(h.run("", "fund", "-txid", "nothex"), exitUsage, "is not a transaction id")
	// A transaction that pays someone else.
	if _, err := h.chain.Generate(1, "mfWxJ45yp2SFn7UciZyNpvDKrzbhyfKrY8"); err != nil {
		t.Fatal(err)
	}
	for _, id := range h.chain.Txids() {
		if !before[id] && id != paid {
			h.want(h.run("", "fund", "-txid", id), exitUsage, "pays nothing to the fund address")
		}
	}
}
