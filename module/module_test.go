package module

import (
	"context"
	"encoding/json"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/overlay/lookup"

	bmodule "github.com/lightwebinc/bcommon/module"
)

// The factory as a host calls it, the twin of module.test.ts: its
// configuration refused rather than guessed, and what it mounts.

const key2 = "0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798"

func TestParseStreams(t *testing.T) {
	v := vectors(t)
	got, err := ParseStreams(v.Stream + "=open")
	if err != nil || len(got) != 1 || got[0].Stream != v.Stream || !got[0].Policy.Open {
		t.Fatal(got, err)
	}
	got, err = ParseStreams(" " + v.Stream + "=" + v.IdentityKey + "+" + key2 + " ")
	if err != nil || len(got[0].Policy.Producers) != 2 || got[0].Policy.Open {
		t.Fatal(got, err)
	}
	for _, bad := range []string{
		"", v.Stream, v.Stream + "=", v.Stream + "=closed", v.Stream + "=open," + v.Stream + "=open", "example_app=open",
		v.Stream + "=" + strings.ToUpper(v.IdentityKey), v.Stream + "=04" + strings.Repeat("00", 32), v.Stream + "=" + v.IdentityKey + "+",
	} {
		if _, err := ParseStreams(bad); err == nil {
			t.Fatalf("%q", bad)
		}
	}
}

func TestNewMountsEachStreamAndTheService(t *testing.T) {
	v := vectors(t)
	host := newFakeHost()
	e := map[string]string{
		"BLOGS_STREAMS":  v.Stream + "=open,audit_mkvbqzxtrw=" + v.IdentityKey,
		"OVERLAY_TOPICS": "tm_anytx," + v.Topic + ",tm_log_audit_mkvbqzxtrw",
	}
	m, err := New(host, env(e))
	if err != nil {
		t.Fatal(err)
	}
	var topics []string
	for k := range m.Module.Topics {
		topics = append(topics, k)
	}
	sort.Strings(topics)
	if !reflect.DeepEqual(topics, []string{"tm_log_audit_mkvbqzxtrw", v.Topic}) || len(m.Module.Lookups) != 1 || m.Module.Lookups["ls_log"] == nil {
		t.Fatal(topics, m.Module.Lookups)
	}
	if _, ok := m.Module.Lookups["ls_log"].(bmodule.Restorer); !ok {
		t.Fatal("the index is in memory, so the module must restore")
	}
	if _, ok := host.counters[`blogs_refused_total{reason="anchor-input",recorded="no"}`]; !ok {
		t.Fatal("counters are preset")
	}
	if host.gauges["blogs_missing_positions"] == nil {
		t.Fatal("gauges")
	}
	if got := host.extra("blogs module mounted", "retention"); got != "off" {
		t.Fatal(got)
	}
	// A tm_log_ topic the host carries but the module does not configure
	// would fall to the admit-everything default: refused.
	e["OVERLAY_TOPICS"] = v.Topic + ",tm_log_other_mkvbqzxtrw"
	if _, err := New(newFakeHost(), env(e)); err == nil || !strings.Contains(err.Error(), "tm_log_other_mkvbqzxtrw") {
		t.Fatal(err)
	}
	delete(e, "BLOGS_STREAMS")
	if _, err := New(newFakeHost(), env(e)); err == nil || !strings.Contains(err.Error(), "BLOGS_STREAMS") {
		t.Fatal(err)
	}
	if _, err := New(newFakeHost(), nil); err == nil {
		t.Fatal("no environment")
	}
}

func TestLookupIsAFormulaAnswer(t *testing.T) {
	v := vectors(t)
	s := service(t, newFakeHost())
	admitVector(t, s, "carrier-seq-1")
	q, _ := json.Marshal(map[string]any{"stream": v.Stream, "producer": v.IdentityKey, "seq": 1})
	a, err := s.Lookup(context.Background(), &lookup.LookupQuestion{Service: "ls_log", Query: q})
	if err != nil || a.Type != lookup.AnswerTypeFormula || len(a.Formulas) != 1 || a.Formulas[0].Outpoint.Txid.String() != v.txid(t, "carrier-seq-1") {
		t.Fatal(a, err)
	}
	if _, err := s.Lookup(context.Background(), &lookup.LookupQuestion{Service: "ls_log", Query: []byte(`{"stream":1}`)}); err == nil {
		t.Fatal("a bad question answered")
	}
	if s.GetMetaData().Name != "ls_log" || !strings.HasPrefix(s.GetDocumentation(), "# ls_log") {
		t.Fatal("metadata")
	}
}

func TestStartRunsRetentionOnceRestored(t *testing.T) {
	v := vectors(t)
	store := &MemoryStore{}
	now := int64(maxSafe)
	s := retained(t, store, &now, nil, false)
	s.submitAll(t, "funding-tree", "carrier-seq-1", "carrier-seq-2", "carrier-seq-3", "anchor-interval-1")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() { done <- s.mod.Start(ctx) }()
	deadline := time.Now().Add(5 * time.Second)
	for s.mod.LS.Counts().Batches != 1 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if s.mod.LS.Counts().Batches != 1 || s.st.has(v.Topic, v.txid(t, "carrier-seq-1"), 0) != nil {
		t.Fatal("the first pass did not run")
	}
	// Without retention Start only waits.
	plain := mount(t, newFakeHost(), nil, nil, nil)
	ctx, cancel = context.WithCancel(context.Background())
	cancel()
	if err := plain.Start(ctx); err != nil {
		t.Fatal(err)
	}
}
