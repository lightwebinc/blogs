package module

import (
	"context"
	"encoding/hex"
	"errors"
	"reflect"
	"slices"
	"sort"
	"testing"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/blogs/internal/logrec"
)

// The topic manager at the engine seam, the twin of tm_log.test.ts: every
// transaction vector with its previous coins, every record refusal vector
// inside a transaction, and the rule for which refusals the engine may
// record. Together they refuse for every reason the spec names.

func manager(t *testing.T, p logrec.Policy, coins CoinSource, host *fakeHost) *TopicManager {
	t.Helper()
	v := vectors(t)
	tm, err := NewTopicManager(v.Topic, p, coins, host, v.headers())
	if err != nil {
		t.Fatal(err)
	}
	return tm
}

type outcome struct {
	admit  []uint32
	retain []uint32
	raised string
}

// run is one submission; a raised refusal comes back as its reason.
func run(t *testing.T, tm *TopicManager, beef []byte, prev []uint32) outcome {
	t.Helper()
	ai, err := tm.Admit(context.Background(), beef, prev)
	var u *Unrecorded
	if errors.As(err, &u) {
		return outcome{raised: u.Reason}
	}
	if err != nil {
		t.Fatal(err)
	}
	return outcome{admit: ai.OutputsToAdmit, retain: ai.CoinsToRetain}
}

func TestTransactionVectors(t *testing.T) {
	v := vectors(t)
	if len(v.Transactions) != 54 {
		t.Fatalf("%d transaction vectors, want 54", len(v.Transactions))
	}
	admitted, recorded, raised := 0, 0, 0
	exercised := map[string]bool{}
	for i := range v.Transactions {
		x := &v.Transactions[i]
		host := newFakeHost()
		prev, coins := x.coins(t)
		got := run(t, manager(t, x.policy(t), coins, host), x.beef(t), prev)
		if x.Verdict == "admit" {
			if got.raised != "" {
				t.Fatalf("%s: raised %s", x.Name, got.raised)
			}
			want := x.Admits.Funding
			if x.Admits.Kind == "batch" || x.Admits.Kind == "anchor" {
				want = []uint32{*x.Admits.OutputIndex}
			}
			if want == nil {
				want = []uint32{}
			}
			if !reflect.DeepEqual(got.admit, want) {
				t.Fatalf("%s: admits %v, want %v", x.Name, got.admit, want)
			}
			if !reflect.DeepEqual(got.retain, prev) {
				t.Fatalf("%s: retains %v, want every held output it spends %v", x.Name, got.retain, prev)
			}
			admitted++
			continue
		}
		exercised[x.Reason] = true
		mustRaise := len(prev) > 0 || slices.Contains(DeferredReasons, x.Reason)
		if mustRaise {
			if got.raised != x.Reason {
				t.Fatalf("%s: want %s raised, got %+v", x.Name, x.Reason, got)
			}
			raised++
		} else {
			if got.raised != "" || len(got.admit) != 0 || len(got.retain) != 0 || got.admit == nil {
				t.Fatalf("%s: want %s recorded, got %+v", x.Name, x.Reason, got)
			}
			recorded++
		}
		rec := "yes"
		if mustRaise {
			rec = "no"
		}
		if n := host.count("blogs_refused_total", map[string]string{"reason": x.Reason, "recorded": rec}); n != 1 {
			t.Fatalf("%s: counted %v", x.Name, n)
		}
	}
	if admitted+recorded+raised != 54 || recorded == 0 || raised == 0 {
		t.Fatalf("admitted %d recorded %d raised %d", admitted, recorded, raised)
	}
	// The record refusals (below) and these exercise every reason.
	for _, r := range recordRefusals(t) {
		exercised[r] = true
	}
	exercised["magic"] = true
	var missing []string
	for _, r := range Reasons {
		if !exercised[r] {
			missing = append(missing, r)
		}
	}
	if len(missing) > 0 {
		t.Fatalf("reasons never exercised: %v", missing)
	}
}

func TestDeferredRefusals(t *testing.T) {
	v := vectors(t)
	// An anchor whose predecessor is not held is raised, not recorded, even
	// though it spends a held funding output.
	x := v.byName(t, "anchor-interval-2-token-not-held")
	prev, coins := x.coins(t)
	if len(prev) != 1 {
		t.Fatalf("the fee input spends a held funding output: %v", prev)
	}
	if got := run(t, manager(t, logrec.Policy{Open: true}, coins, newFakeHost()), x.beef(t), prev); got.raised != "anchor-input" {
		t.Fatalf("%+v", got)
	}
	// An unmined anchor is raised, so that its mined publication is decided afresh.
	x = v.byName(t, "anchor-unmined")
	prev, coins = x.coins(t)
	if got := run(t, manager(t, logrec.Policy{Open: true}, coins, newFakeHost()), x.beef(t), prev); got.raised != "unmined" {
		t.Fatalf("%+v", got)
	}
}

func TestUnminedTreeAndSweep(t *testing.T) {
	v := vectors(t)
	host := newFakeHost()
	got := run(t, manager(t, logrec.Policy{Open: true}, noCoins, host), v.byName(t, "funding-tree-unmined").beef(t), nil)
	if len(got.admit) != 0 || got.raised != "" || host.count("blogs_admitted_total", map[string]string{"kind": "none"}) != 1 {
		t.Fatalf("an unmined funding tree: %+v", got)
	}
	host = newFakeHost()
	x := v.byName(t, "sweep")
	prev, coins := x.coins(t)
	got = run(t, manager(t, logrec.Policy{Open: true}, coins, host), x.beef(t), prev)
	if !reflect.DeepEqual(got.admit, []uint32{0}) || !reflect.DeepEqual(got.retain, prev) || host.count("blogs_admitted_total", map[string]string{"kind": "funding"}) != 1 {
		t.Fatalf("a sweep: %+v", got)
	}
}

// carrying is a transaction whose one output carries the byte string as a
// batch record (a carrier) or as an anchor record (a token). Its input
// spends a parent carried in its BEEF, of the same value, so carrier-shape
// passes and the record's own rules are the first to decide.
func carrying(t *testing.T, kind string, record []byte) []byte {
	t.Helper()
	v := vectors(t)
	kb, _ := hex.DecodeString(v.IdentityKey)
	key, err := ec.PublicKeyFromBytes(kb)
	if err != nil {
		t.Fatal(err)
	}
	sig := []byte{0x30, 0x06, 0x02, 0x01, 0x01, 0x02, 0x01, 0x01}
	fields := [][]byte{record}
	if kind != "batch" {
		fields = [][]byte{logrec.TagAnchor, record}
	}
	parent := transaction.NewTransaction()
	one := script.Script([]byte{0x51})
	parent.AddOutput(&transaction.TransactionOutput{LockingScript: &one, Satoshis: 1})
	tx := transaction.NewTransaction()
	empty := script.Script([]byte{})
	tx.AddInput(&transaction.TransactionInput{SourceTXID: parent.TxID(), SourceTransaction: parent, SourceTxOutIndex: 0, UnlockingScript: &empty, SequenceNumber: 0})
	lock := script.Script(logrec.PushDropScript(key, fields, sig))
	tx.AddOutput(&transaction.TransactionOutput{LockingScript: &lock, Satoshis: 1})
	tx.LockTime = 4102444800
	b, err := tx.BEEF()
	if err != nil {
		t.Fatal(err)
	}
	return b
}

type refusalVector struct {
	Name        string `json:"name"`
	Kind        string `json:"kind"`
	Hex         string `json:"hex"`
	Reason      string `json:"reason"`
	AtAdmission string `json:"atAdmission"`
}

// recordRefusals runs the 53 record refusal vectors inside transactions:
// refused for the record reason and recorded, or (a batch whose magic does
// not claim) accepted as claiming nothing. It answers the reasons seen.
func recordRefusals(t *testing.T) []string {
	t.Helper()
	var r struct {
		Refusals []refusalVector `json:"refusals"`
	}
	readJSON(t, "refusal-v1.json", &r)
	if len(r.Refusals) != 53 {
		t.Fatalf("%d refusal vectors, want 53", len(r.Refusals))
	}
	var seen []string
	for _, x := range r.Refusals {
		rec, _ := hex.DecodeString(x.Hex)
		host := newFakeHost()
		got := run(t, manager(t, logrec.Policy{Open: true}, noCoins, host), carrying(t, x.Kind, rec), nil)
		if got.raised != "" || len(got.admit) != 0 || len(got.retain) != 0 {
			t.Fatalf("%s: %+v", x.Name, got)
		}
		if x.Kind == "batch" && x.AtAdmission == "skip" {
			if host.count("blogs_admitted_total", map[string]string{"kind": "none"}) != 1 {
				t.Fatalf("%s: claims nothing", x.Name)
			}
			continue
		}
		if host.count("blogs_refused_total", map[string]string{"reason": x.Reason, "recorded": "yes"}) != 1 {
			t.Fatalf("%s: not refused %s", x.Name, x.Reason)
		}
		seen = append(seen, x.Reason)
	}
	return seen
}

func TestRecordRefusalVectors(t *testing.T) { recordRefusals(t) }

func TestAnchorWithWrongMagic(t *testing.T) {
	// A token whose anchor record has the wrong magic is refused magic (a
	// batch record with it only fails to claim).
	var r struct {
		Refusals []refusalVector `json:"refusals"`
	}
	readJSON(t, "refusal-v1.json", &r)
	var b struct {
		Batches []struct {
			Record string `json:"record"`
		} `json:"batches"`
	}
	readJSON(t, "batch-v1.json", &b)
	var wrong string
	for _, x := range r.Refusals {
		if x.Name == "wrong-magic" {
			wrong = x.Hex
		}
	}
	for _, h := range []string{wrong, b.Batches[0].Record} {
		rec, _ := hex.DecodeString(h)
		host := newFakeHost()
		got := run(t, manager(t, logrec.Policy{Open: true}, noCoins, host), carrying(t, "anchor", rec), nil)
		if got.raised != "" || len(got.admit) != 0 || host.count("blogs_refused_total", map[string]string{"reason": "magic", "recorded": "yes"}) != 1 {
			t.Fatalf("%+v", got)
		}
	}
}

func TestManagerRefusesBadMounts(t *testing.T) {
	v := vectors(t)
	for _, c := range []struct {
		topic string
		p     logrec.Policy
	}{
		{"tm_log_app", logrec.Policy{Open: true}},
		{"tm_other_app_qzxkvbmwtr", logrec.Policy{Open: true}},
		{v.Topic, logrec.Policy{}},
	} {
		if _, err := NewTopicManager(c.topic, c.p, noCoins, newFakeHost(), nil); err == nil {
			t.Fatalf("%s mounted", c.topic)
		}
	}
}

func TestReasonsMatchTheCodec(t *testing.T) {
	// Every label the codec gives is one the counter is preset with, and
	// the deferred ones are among them.
	r := append([]string(nil), Reasons...)
	sort.Strings(r)
	for _, d := range DeferredReasons {
		if !slices.Contains(r, d) {
			t.Fatal(d)
		}
	}
	if len(r) != 23 {
		t.Fatalf("%d reasons", len(r))
	}
}
