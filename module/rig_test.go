package module

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/overlay/lookup"
	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"

	bmodule "github.com/lightwebinc/bcommon/module"

	"github.com/lightwebinc/blogs/internal/logrec"
)

// The module and an engine together, for tests: the transaction vectors,
// a host that records what the module counts and logs, and a stand-in for
// the Go engine's storage and ordinary admission path (the TypeScript
// tests' testutil.ts MemoryStorage and the @bsv/overlay Engine they run).

const vecDir = "../testdata/vectors/"

type txVector struct {
	Name          string          `json:"name"`
	Policy        json.RawMessage `json:"policy"`
	Txid          string          `json:"txid"`
	Beef          string          `json:"beef"`
	PreviousCoins []struct {
		Txid          string `json:"txid"`
		Vout          uint32 `json:"vout"`
		LockingScript string `json:"lockingScript"`
		Satoshis      uint64 `json:"satoshis"`
	} `json:"previousCoins"`
	Verdict string `json:"verdict"`
	Reason  string `json:"reason"`
	Admits  *struct {
		Kind        string   `json:"kind"`
		OutputIndex *uint32  `json:"outputIndex"`
		Funding     []uint32 `json:"funding"`
	} `json:"admits"`
}

type txVectors struct {
	Stream       string `json:"stream"`
	Topic        string `json:"topic"`
	IdentityKey  string `json:"identityKey"`
	Transactions []txVector
	Headers      []struct {
		Height     uint32 `json:"height"`
		MerkleRoot string `json:"merkleRoot"`
	} `json:"headers"`
}

func readJSON(t testing.TB, name string, v any) {
	t.Helper()
	b, err := os.ReadFile(vecDir + name)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, v); err != nil {
		t.Fatal(err)
	}
}

var (
	vecOnce sync.Once
	vec     txVectors
)

func vectors(t testing.TB) *txVectors {
	vecOnce.Do(func() { readJSON(t, "transaction-v1.json", &vec) })
	return &vec
}

func (v *txVectors) byName(t testing.TB, name string) *txVector {
	t.Helper()
	for i := range v.Transactions {
		if v.Transactions[i].Name == name {
			return &v.Transactions[i]
		}
	}
	t.Fatalf("no transaction vector %s", name)
	return nil
}

func (v *txVectors) txid(t testing.TB, name string) string { return v.byName(t, name).Txid }

func (x *txVector) beef(t testing.TB) []byte {
	t.Helper()
	b, err := hex.DecodeString(x.Beef)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func (x *txVector) tx(t testing.TB) *transaction.Transaction {
	t.Helper()
	_, tx, _, err := transaction.ParseBeef(x.beef(t))
	if err != nil {
		t.Fatal(err)
	}
	return tx
}

func (x *txVector) policy(t testing.TB) logrec.Policy {
	t.Helper()
	var s string
	if json.Unmarshal(x.Policy, &s) == nil && s == "open" {
		return logrec.Policy{Open: true}
	}
	var keys []string
	if err := json.Unmarshal(x.Policy, &keys); err != nil {
		t.Fatal(err)
	}
	var p logrec.Policy
	for _, k := range keys {
		b, _ := hex.DecodeString(k)
		p.Producers = append(p.Producers, b)
	}
	return p
}

// coins is the vector's previous coins as the engine hands them: input
// indexes, and a source for their outputs.
func (x *txVector) coins(t testing.TB) ([]uint32, CoinSource) {
	held := map[string]logrec.Coin{}
	for _, c := range x.PreviousCoins {
		s, _ := hex.DecodeString(c.LockingScript)
		held[opKey(c.Txid, c.Vout)] = logrec.Coin{LockingScript: s, Satoshis: c.Satoshis}
	}
	prev := []uint32{}
	for i, in := range x.tx(t).Inputs {
		if in.SourceTXID != nil {
			if _, ok := held[opKey(in.SourceTXID.String(), in.SourceTxOutIndex)]; ok {
				prev = append(prev, uint32(i)) //nolint:gosec // an input index
			}
		}
	}
	return prev, func(_ context.Context, _ string, op transaction.Outpoint) (logrec.Coin, bool, error) {
		c, ok := held[opKey(op.Txid.String(), op.Index)]
		return c, ok, nil
	}
}

func noCoins(context.Context, string, transaction.Outpoint) (logrec.Coin, bool, error) {
	return logrec.Coin{}, false, nil
}

// headersOf is the vector headers as a chain tracker.
type headersOf map[uint32]string

func (h headersOf) IsValidRootForHeight(_ context.Context, root *chainhash.Hash, height uint32) (bool, error) {
	return h[height] == root.String(), nil
}

func (h headersOf) CurrentHeight(context.Context) (uint32, error) {
	var top uint32
	for k := range h {
		top = max(top, k)
	}
	return top, nil
}

func (v *txVectors) headers() headersOf {
	h := headersOf{}
	for _, x := range v.Headers {
		h[x.Height] = x.MerkleRoot
	}
	return h
}

// fakeHost records what the module counts and logs.
type fakeHost struct {
	mu       sync.Mutex
	counters map[string]float64
	gauges   map[string]func() float64
	lines    []logLine
}

type logLine struct {
	msg   string
	extra map[string]any
}

func newFakeHost() *fakeHost {
	return &fakeHost{counters: map[string]float64{}, gauges: map[string]func() float64{}}
}

func mkey(name string, labels map[string]string) string {
	ks := make([]string, 0, len(labels))
	for k := range labels {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	parts := make([]string, 0, len(ks))
	for _, k := range ks {
		parts = append(parts, fmt.Sprintf("%s=%q", k, labels[k]))
	}
	if len(parts) == 0 {
		return name
	}
	return name + "{" + strings.Join(parts, ",") + "}"
}

func (h *fakeHost) Log(msg string, extra map[string]any) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.lines = append(h.lines, logLine{msg, extra})
}

func (h *fakeHost) Metrics() bmodule.Metrics { return h }

func (h *fakeHost) Inc(name string, labels map[string]string, by float64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.counters[mkey(name, labels)] += by
}

func (h *fakeHost) Preset(name string, labels map[string]string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.counters[mkey(name, labels)]; !ok {
		h.counters[mkey(name, labels)] = 0
	}
}

func (h *fakeHost) Gauge(name string, fn func() float64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.gauges[name] = fn
}

func (h *fakeHost) count(name string, labels map[string]string) float64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.counters[mkey(name, labels)]
}

func (h *fakeHost) logged(sub string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, l := range h.lines {
		if strings.Contains(l.msg, sub) {
			return true
		}
	}
	return false
}

func (h *fakeHost) extra(msg, key string) any {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, l := range h.lines {
		if l.msg == msg {
			return l.extra[key]
		}
	}
	return nil
}

// row is one stored output.
type row struct {
	op              transaction.Outpoint
	topic           string
	script          []byte
	satoshis        uint64
	spent           bool
	outputsConsumed []*transaction.Outpoint
	consumedBy      []*transaction.Outpoint
	beef            []byte
}

// memStorage is the engine's storage in memory, as the engine depends on
// it: FindOutput matches spent and unspent outputs, with the stored BEEF.
type memStorage struct {
	mu      sync.Mutex
	rows    map[string]*row
	applied map[string]bool
	deletes int
}

func newMemStorage() *memStorage {
	return &memStorage{rows: map[string]*row{}, applied: map[string]bool{}}
}

func rowKey(topic string, op transaction.Outpoint) string { return topic + "|" + op.String() }

func (m *memStorage) output(r *row, withBEEF bool) *bmodule.Output {
	s := script.Script(append([]byte(nil), r.script...))
	o := &bmodule.Output{Outpoint: r.op, OutputScript: &s, Satoshis: r.satoshis, Topic: r.topic, Spent: r.spent,
		OutputsConsumed: append([]*transaction.Outpoint(nil), r.outputsConsumed...), ConsumedBy: append([]*transaction.Outpoint(nil), r.consumedBy...)}
	if withBEEF {
		o.Beef = r.beef
	}
	return o
}

// FindOutput implements bmodule.RestoreStorage.
func (m *memStorage) FindOutput(_ context.Context, op *transaction.Outpoint, topic string) (*bmodule.Output, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	keys := make([]string, 0, len(m.rows))
	for k := range m.rows {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		r := m.rows[k]
		if r.op == *op && (topic == "" || r.topic == topic) {
			return m.output(r, true), nil
		}
	}
	return nil, nil
}

// unspent is FindUTXOsForTopic.
func (m *memStorage) unspent(topic string, withBEEF bool) []bmodule.Output {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []bmodule.Output
	keys := make([]string, 0, len(m.rows))
	for k := range m.rows {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if r := m.rows[k]; r.topic == topic && !r.spent {
			out = append(out, *m.output(r, withBEEF))
		}
	}
	return out
}

func (m *memStorage) ofTx(txid string) []*row {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []*row
	for _, r := range m.rows {
		if r.op.Txid.String() == txid {
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].op.Index < out[j].op.Index })
	return out
}

func (m *memStorage) has(topic, txid string, vout uint32) *row {
	h, _ := chainhash.NewHashFromHex(txid)
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.rows[rowKey(topic, transaction.Outpoint{Txid: *h, Index: vout})]
}

// deleter is memStorage with DeleteOutput; memStorage alone has none.
type deleter struct{ *memStorage }

func (d deleter) DeleteOutput(_ context.Context, op *transaction.Outpoint, topic string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.rows, rowKey(topic, *op))
	d.deletes++
	return nil
}

// engine is the ordinary admission path of the Go engine over memStorage,
// with the module mounted: duplicates answer empty, the previous coins are
// the held outputs the inputs spend, a manager error stores nothing, held
// inputs not retained are deleted, admitted outputs stored and announced,
// retained ones marked spent and consumed by the new outputs, the
// transaction recorded as applied.
type engine struct {
	st    *memStorage
	seam  bmodule.RestoreStorage
	mod   bmodule.Module
	calls int
}

func newEngine(mod bmodule.Module, st *memStorage) *engine {
	return &engine{st: st, seam: deleter{st}, mod: mod}
}

func (e *engine) submit(t testing.TB, raw []byte, topic string) ([]uint32, error) {
	t.Helper()
	ctx := context.Background()
	b, tx, txid, err := transaction.ParseBeef(raw)
	if err != nil {
		t.Fatal(err)
	}
	e.st.mu.Lock()
	dup := e.st.applied[topic+"|"+txid.String()]
	e.st.mu.Unlock()
	if dup {
		return []uint32{}, nil
	}
	var prev []uint32
	held := map[uint32]*row{}
	for i, in := range tx.Inputs {
		op := transaction.Outpoint{Txid: *in.SourceTXID, Index: in.SourceTxOutIndex}
		e.st.mu.Lock()
		r := e.st.rows[rowKey(topic, op)]
		e.st.mu.Unlock()
		if r != nil {
			prev = append(prev, uint32(i)) //nolint:gosec // an input index
			held[uint32(i)] = r            //nolint:gosec // an input index
		}
	}
	e.calls++
	ai, err := e.mod.Topics[topic].IdentifyAdmissibleOutputs(bmodule.WithSubmission(ctx, raw), b, txid, prev)
	if err != nil {
		return nil, err
	}
	atomic, err := b.AtomicBytes(txid)
	if err != nil {
		t.Fatal(err)
	}
	retain := map[uint32]bool{}
	for _, c := range ai.CoinsToRetain {
		retain[c] = true
	}
	var consumed []*transaction.Outpoint
	e.st.mu.Lock()
	for vin, r := range held {
		r.spent = true
		if retain[vin] {
			op := r.op
			consumed = append(consumed, &op)
			continue
		}
		delete(e.st.rows, rowKey(topic, r.op))
	}
	var fresh []*transaction.Outpoint
	for _, v := range ai.OutputsToAdmit {
		o := tx.Outputs[v]
		op := transaction.Outpoint{Txid: *txid, Index: v}
		e.st.rows[rowKey(topic, op)] = &row{op: op, topic: topic, script: append([]byte(nil), *o.LockingScript...), satoshis: o.Satoshis, outputsConsumed: consumed, beef: atomic}
		fresh = append(fresh, &op)
	}
	for _, c := range consumed {
		if r := e.st.rows[rowKey(topic, *c)]; r != nil {
			r.consumedBy = append(r.consumedBy, fresh...)
		}
	}
	e.st.applied[topic+"|"+txid.String()] = true
	e.st.mu.Unlock()
	for _, v := range ai.OutputsToAdmit {
		for _, ls := range e.mod.Lookups {
			if err := ls.OutputAdmittedByTopic(ctx, &bmodule.OutputAdmittedByTopic{Mode: bmodule.AdmitWholeTx, Outpoint: transaction.Outpoint{Txid: *txid, Index: v}, Topic: topic, AtomicBEEF: atomic}); err != nil {
				t.Fatal(err)
			}
		}
	}
	if ai.OutputsToAdmit == nil {
		return []uint32{}, nil
	}
	return ai.OutputsToAdmit, nil
}

// lookup asks the engine and hydrates the formulas from storage: each
// answered transaction's txid, whether its BEEF carries a proof, and the
// output.
func (e *engine) lookup(t testing.TB, query map[string]any) ([]hydrated, error) {
	t.Helper()
	q, _ := json.Marshal(query)
	ans, err := e.mod.Lookups[logrec.LookupService].Lookup(context.Background(), &lookup.LookupQuestion{Service: logrec.LookupService, Query: q})
	if err != nil {
		return nil, err
	}
	if ans.Type != lookup.AnswerTypeFormula {
		t.Fatalf("answer type %s", ans.Type)
	}
	out := []hydrated{}
	for _, f := range ans.Formulas {
		o, err := e.st.FindOutput(context.Background(), f.Outpoint, "")
		if err != nil {
			t.Fatal(err)
		}
		if o == nil || o.Beef == nil {
			continue
		}
		_, tx, _, err := transaction.ParseBeef(o.Beef)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, hydrated{txid: tx.TxID().String(), proof: tx.MerklePath != nil, vout: f.Outpoint.Index})
	}
	return out, nil
}

type hydrated struct {
	txid  string
	proof bool
	vout  uint32
}

func txidsOf(h []hydrated) []string {
	out := []string{}
	for _, x := range h {
		out = append(out, x.txid)
	}
	return out
}

// mount is the module for the vector stream, open, as a host builds it.
func mount(t testing.TB, host *fakeHost, r *RetentionConfig, store StateStore, now func() int64) *Mounted {
	t.Helper()
	v := vectors(t)
	m, err := Mount(host, &Config{Streams: []StreamConfig{{Stream: v.Stream, Policy: logrec.Policy{Open: true}}}, Retention: r}, nil, store, v.headers(), now)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

// ask answers a question as the service names it.
func ask(t testing.TB, s *Service, query any) ([]Formula, error) {
	t.Helper()
	q, err := json.Marshal(query)
	if err != nil {
		t.Fatal(err)
	}
	return s.Answer(context.Background(), logrec.LookupService, q)
}

func askRaw(s *Service, query string) ([]Formula, error) {
	return s.Answer(context.Background(), logrec.LookupService, []byte(query))
}
