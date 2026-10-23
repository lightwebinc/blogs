package module

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"

	bmodule "github.com/lightwebinc/bcommon/module"

	"github.com/lightwebinc/blogs/internal/logrec"
)

// ls_log on its own, the twin of ls_log.test.ts: the question rules, and
// the statuses of spec sections 8.2 to 8.4 over admitted outputs delivered
// in any order. The admissions are the transaction vectors and, for the
// cases no vector covers (a fork, a second chain, a failed join, a wrong
// root, the caps), transactions built here. The lookup service trusts
// admission, so those need only the shape it reads.

const maxSafe = 1<<53 - 1

func service(t *testing.T, host *fakeHost) *Service {
	t.Helper()
	s, err := NewService([]string{vectors(t).Topic}, host, nil)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// admitTx indexes one output of a transaction built here, as an admission
// does once its BEEF is parsed.
func admitTx(s *Service, tx *transaction.Transaction, vout uint32, topic string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.topics[topic] {
		if ok, _ := s.index(topic, tx, vout); ok {
			s.noteSpends(tx)
		}
	}
}

// admitVector announces a vector's admitted outputs through the contract.
func admitVector(t *testing.T, s *Service, name string) {
	t.Helper()
	admitVectorOn(t, s, name, vectors(t).Topic)
}

func admitVectorOn(t *testing.T, s *Service, name, topic string) {
	t.Helper()
	x := vectors(t).byName(t, name)
	b, _, txid, err := transaction.ParseBeef(x.beef(t))
	if err != nil {
		t.Fatal(err)
	}
	atomic, err := b.AtomicBytes(txid)
	if err != nil {
		t.Fatal(err)
	}
	outs := x.Admits.Funding
	if x.Admits.Kind == "batch" || x.Admits.Kind == "anchor" {
		outs = []uint32{*x.Admits.OutputIndex}
	}
	for _, i := range outs {
		if err := s.OutputAdmittedByTopic(context.Background(), &bmodule.OutputAdmittedByTopic{Mode: bmodule.AdmitWholeTx, Outpoint: transaction.Outpoint{Txid: *txid, Index: i}, Topic: topic, AtomicBEEF: atomic}); err != nil {
			t.Fatal(err)
		}
	}
}

func producerKey(t *testing.T) (*ec.PublicKey, []byte) {
	kb, _ := hex.DecodeString(vectors(t).IdentityKey)
	k, err := ec.PublicKeyFromBytes(kb)
	if err != nil {
		t.Fatal(err)
	}
	return k, kb
}

var someSig = []byte{0x30, 0x06, 0x02, 0x01, 0x01, 0x02, 0x01, 0x01}

// carrier is a carrier of a one-line batch at seq with prev, spending
// funding-tree output vout.
func carrier(t *testing.T, seq uint64, prev [32]byte, line string, vout uint32) *transaction.Transaction {
	t.Helper()
	v := vectors(t)
	key, kb := producerKey(t)
	content := []byte(line + "\n")
	rec, err := (&logrec.Batch{Producer: kb, Stream: v.Stream, Seq: seq, Prev: prev, Timestamp: 1, Count: 1, Encoding: logrec.EncodingLines, Profile: logrec.ProfileFull, Content: content, Size: uint64(len(content))}).Encode()
	if err != nil {
		t.Fatal(err)
	}
	tree := v.byName(t, "funding-tree").tx(t)
	tx := transaction.NewTransaction()
	empty := script.Script([]byte{})
	tx.AddInput(&transaction.TransactionInput{SourceTXID: tree.TxID(), SourceTransaction: tree, SourceTxOutIndex: vout, UnlockingScript: &empty})
	lock := script.Script(logrec.PushDropScript(key, [][]byte{rec}, someSig))
	tx.AddOutput(&transaction.TransactionOutput{LockingScript: &lock, Satoshis: 1})
	tx.LockTime = 4102444800
	return tx
}

type spend struct {
	txid string
	vout uint32
}

// token is a mined anchor token spending the given outpoints, at a block
// height and position.
func token(t *testing.T, a logrec.Anchor, spends []spend, height uint32, index uint64) *transaction.Transaction {
	t.Helper()
	v := vectors(t)
	key, kb := producerKey(t)
	a.Producer, a.Stream, a.Timestamp = kb, v.Stream, 1
	rec, err := a.Encode()
	if err != nil {
		t.Fatal(err)
	}
	tx := transaction.NewTransaction()
	for _, s := range spends {
		h, _ := chainhash.NewHashFromHex(s.txid)
		empty := script.Script([]byte{})
		tx.AddInput(&transaction.TransactionInput{SourceTXID: h, SourceTxOutIndex: s.vout, UnlockingScript: &empty, SequenceNumber: 0xffffffff})
	}
	lock := script.Script(logrec.PushDropScript(key, [][]byte{logrec.TagAnchor, rec}, someSig))
	tx.AddOutput(&transaction.TransactionOutput{LockingScript: &lock, Satoshis: 1})
	yes := true
	tx.MerklePath = &transaction.MerklePath{BlockHeight: height, Path: [][]*transaction.PathElement{{{Offset: index, Hash: tx.TxID(), Txid: &yes}, {Offset: index ^ 1, Hash: &chainhash.Hash{}}}}}
	return tx
}

func hashOrder(display string) [32]byte {
	h, _ := chainhash.NewHashFromHex(display)
	return [32]byte(*h)
}

func fill(b byte) [32]byte { return [32]byte(bytes.Repeat([]byte{b}, 32)) }

func formulaTxids(f []Formula) []string {
	out := []string{}
	for _, x := range f {
		out = append(out, x.Txid)
	}
	return out
}

func mustAsk(t *testing.T, s *Service, q any) []Formula {
	t.Helper()
	f, err := ask(t, s, q)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func status(t *testing.T, s *Service, txid string) string {
	t.Helper()
	st, _ := s.BatchStatusOf(vectors(t).IdentityKey, vectors(t).Stream, txid)
	return st
}

func TestQuestionRules(t *testing.T) {
	v := vectors(t)
	s := service(t, newFakeHost())
	p := v.IdentityKey
	ok := map[string]any{"stream": v.Stream, "producer": p, "seq": 1}
	with := func(k string, val any) map[string]any {
		m := map[string]any{"stream": v.Stream, "producer": p, "seq": 1}
		m[k] = val
		return m
	}
	if f := mustAsk(t, s, ok); len(f) != 0 {
		t.Fatal("a stream this host holds nothing of answers empty")
	}
	if _, err := askRaw(s, fmt.Sprintf(`{"stream":%q,"producer":%q,"seq":1.0}`, v.Stream, p)); err != nil {
		t.Fatalf("a double with an integer value: %v", err)
	}
	if _, err := askRaw(s, fmt.Sprintf(`{"stream":%q,"producer":%q,"seq":9,"seq":1}`, v.Stream, p)); err != nil {
		t.Fatalf("a member given twice: %v", err)
	}
	bad := []any{
		nil, []any{}, "seq=1", map[string]any{},
		map[string]any{"stream": v.Stream, "producer": p},
		map[string]any{"stream": v.Stream, "seq": 1},
		map[string]any{"producer": p, "seq": 1},
		with("interval", 1), with("extra", true),
		with("stream", "example_app"), with("stream", "Example_app_qzxkvbmwtr"), with("stream", 7),
		with("producer", strings.ToUpper(p)), with("producer", p[2:]), with("producer", "02"+strings.Repeat("ff", 32)),
		with("seq", 0), with("seq", 1.5), with("seq", float64(1<<53)), with("seq", "1"), with("seq", -1),
	}
	for _, q := range bad {
		if _, err := ask(t, s, q); err == nil || !strings.Contains(err.Error(), "ls_log") {
			t.Fatalf("%v: %v", q, err)
		}
	}
	if _, err := s.Answer(context.Background(), "ls_anytx", []byte(`{}`)); err == nil || !strings.Contains(err.Error(), "ls_log") {
		t.Fatal("another service's question")
	}
	tr := strings.Repeat("ab", 32)
	badTree := []any{
		map[string]any{"tree": tr},
		map[string]any{"stream": v.Stream, "tree": tr, "producer": p},
		map[string]any{"stream": v.Stream, "tree": tr, "seq": 1},
		map[string]any{"stream": v.Stream, "tree": tr, "vout": 0},
		map[string]any{"stream": "example_app", "tree": tr},
		map[string]any{"stream": v.Stream, "tree": strings.ToUpper(tr)},
		map[string]any{"stream": v.Stream, "tree": tr[2:]},
		map[string]any{"stream": v.Stream, "tree": strings.Repeat("zz", 32)},
		map[string]any{"stream": v.Stream, "tree": 1},
	}
	for _, q := range badTree {
		if _, err := ask(t, s, q); err == nil {
			t.Fatalf("%v answered", q)
		}
	}
	if f := mustAsk(t, s, map[string]any{"stream": v.Stream, "tree": tr}); len(f) != 0 {
		t.Fatal("a tree this host does not hold answers empty")
	}
}

func TestTreeClass(t *testing.T) {
	v := vectors(t)
	s := service(t, newFakeHost())
	id := v.txid(t, "funding-tree")
	q := map[string]any{"stream": v.Stream, "tree": id}
	if len(mustAsk(t, s, q)) != 0 {
		t.Fatal("before")
	}
	admitVector(t, s, "funding-tree")
	want := []Formula{{Txid: id, Vout: 0}}
	if got := mustAsk(t, s, q); !reflect.DeepEqual(got, want) {
		t.Fatalf("the lowest held funding output: %v", got)
	}
	if s.Counts().Trees != 1 {
		t.Fatal("tree count")
	}
	// Spends do not change the answer: every admitted output is retained.
	for _, n := range []string{"carrier-seq-1", "carrier-seq-2", "carrier-seq-3", "anchor-interval-1", "anchor-interval-2"} {
		admitVector(t, s, n)
	}
	if got := mustAsk(t, s, q); !reflect.DeepEqual(got, want) {
		t.Fatal(got)
	}
	// A carrier and an anchor token are held coins, but not funding outputs.
	for _, n := range []string{"carrier-seq-1", "anchor-interval-1"} {
		if len(mustAsk(t, s, map[string]any{"stream": v.Stream, "tree": v.txid(t, n)})) != 0 {
			t.Fatal(n)
		}
	}
	// A sweep's tombstone is a funding output of a mined transaction.
	admitVector(t, s, "sweep")
	if got := mustAsk(t, s, map[string]any{"stream": v.Stream, "tree": v.txid(t, "sweep")}); !reflect.DeepEqual(got, []Formula{{Txid: v.txid(t, "sweep"), Vout: 0}}) {
		t.Fatal(got)
	}
	// Another stream's topic holds nothing of it.
	if len(mustAsk(t, s, map[string]any{"stream": "example_app_hdjvnwpxle", "tree": id})) != 0 {
		t.Fatal("another stream")
	}
	// An output the engine no longer holds leaves the answer; the last one empties it.
	h, _ := chainhash.NewHashFromHex(id)
	ctx := context.Background()
	_ = s.OutputNoLongerRetainedInHistory(ctx, &transaction.Outpoint{Txid: *h, Index: 0}, v.Topic)
	if got := mustAsk(t, s, q); !reflect.DeepEqual(got, []Formula{{Txid: id, Vout: 1}}) {
		t.Fatal(got)
	}
	for _, i := range []uint32{1, 2, 3, 4} {
		_ = s.OutputEvicted(ctx, &transaction.Outpoint{Txid: *h, Index: i})
	}
	if len(mustAsk(t, s, q)) != 0 {
		t.Fatal("after every output left")
	}
}

func TestGapClosedInEitherOrder(t *testing.T) {
	v := vectors(t)
	for _, order := range [][]string{{"carrier-seq-1", "carrier-seq-3", "carrier-seq-2"}, {"carrier-seq-3", "carrier-seq-2", "carrier-seq-1"}} {
		s := service(t, newFakeHost())
		admitVector(t, s, "funding-tree")
		admitVector(t, s, order[0])
		admitVector(t, s, order[1])
		mid := s.Report("")[0]
		if mid.Highest != 3 || len(mid.Holes) != 1 {
			t.Fatalf("%+v", mid)
		}
		admitVector(t, s, order[2])
		if r := s.Report("")[0]; len(r.Holes) != 0 {
			t.Fatalf("%+v", r.Holes)
		}
		for _, n := range order {
			if st := status(t, s, v.txid(t, n)); st != BatchLinked {
				t.Fatalf("%s: %s", n, st)
			}
		}
	}
	s := service(t, newFakeHost())
	admitVector(t, s, "carrier-seq-3")
	if st := status(t, s, v.txid(t, "carrier-seq-3")); st != BatchGap {
		t.Fatal(st)
	}
	if h := s.Report("")[0].Holes; !reflect.DeepEqual(h, [][2]uint64{{1, 2}}) {
		t.Fatal(h)
	}
	// A producer may number a batch 2^53 - 1; the report walks held seqs only.
	admitTx(s, carrier(t, maxSafe, fill(3), "far", 5), 0, v.Topic)
	if h := s.Report("")[0].Holes; !reflect.DeepEqual(h, [][2]uint64{{1, 2}, {4, maxSafe - 1}}) {
		t.Fatal(h)
	}
	if m := s.Counts().Missing; m != maxSafe-2 {
		t.Fatal(m)
	}
}

func TestForkKeepsBothCandidates(t *testing.T) {
	v := vectors(t)
	s := service(t, newFakeHost())
	for _, n := range []string{"carrier-seq-1", "carrier-seq-2", "carrier-seq-3", "anchor-interval-1"} {
		admitVector(t, s, n)
	}
	other := carrier(t, 2, hashOrder(v.txid(t, "carrier-seq-1")), "another second batch", 5)
	admitTx(s, other, 0, v.Topic)
	want := []string{v.txid(t, "carrier-seq-2"), other.TxID().String()}
	sort.Slice(want, func(i, j int) bool {
		a, b := hashOrder(want[i]), hashOrder(want[j])
		return hex.EncodeToString(a[:]) < hex.EncodeToString(b[:])
	})
	if got := formulaTxids(mustAsk(t, s, map[string]any{"stream": v.Stream, "producer": v.IdentityKey, "seq": 2})); !reflect.DeepEqual(got, want) {
		t.Fatalf("%v, want %v", got, want)
	}
	if status(t, s, other.TxID().String()) != BatchFork || status(t, s, v.txid(t, "carrier-seq-2")) != BatchFork {
		t.Fatal("both candidates are forks")
	}
	if status(t, s, v.txid(t, "carrier-seq-3")) != BatchLinked {
		t.Fatal("its prev names a held candidate")
	}
	r := s.Report("")[0]
	if !reflect.DeepEqual(r.Forks, []uint64{2}) || len(r.Intervals) != 1 || r.Intervals[0].Status != IntervalComplete {
		t.Fatalf("the root selects the real second batch: %+v", r)
	}
	if s.Counts().ForkSeqs != 1 {
		t.Fatal("fork seqs")
	}
	// A batch whose prev names no held batch at seq - 1 is a fork too.
	stray := carrier(t, 4, fill(7), "stray", 5)
	admitTx(s, stray, 0, v.Topic)
	if status(t, s, stray.TxID().String()) != BatchFork {
		t.Fatal("stray")
	}
}

func TestRootMismatchAndIncomplete(t *testing.T) {
	v := vectors(t)
	s := service(t, newFakeHost())
	for _, n := range []string{"carrier-seq-1", "carrier-seq-2", "carrier-seq-3", "anchor-interval-1", "anchor-interval-2"} {
		admitVector(t, s, n)
	}
	c3 := hashOrder(v.txid(t, "carrier-seq-3"))
	b4 := carrier(t, 4, c3, "four", 5)
	admitTx(s, b4, 0, v.Topic)
	t2 := v.txid(t, "anchor-interval-2")
	t3 := token(t, logrec.Anchor{Interval: 3, Prev: hashOrder(t2), First: 4, Count: 1, Root: fill(1)}, []spend{{t2, 0}}, 900, 0)
	admitTx(s, t3, 0, v.Topic)
	var got [][2]any
	for _, i := range s.Report("")[0].Intervals {
		got = append(got, [2]any{i.Interval, i.Status})
	}
	if !reflect.DeepEqual(got, [][2]any{{uint64(1), IntervalComplete}, {uint64(2), IntervalComplete}, {uint64(3), IntervalRootMismatch}}) {
		t.Fatal(got)
	}
	// Missing positions make it incomplete, not a mismatch.
	t4 := token(t, logrec.Anchor{Interval: 4, Prev: hashOrder(t3.TxID().String()), First: 5, Count: 2, Root: fill(1)}, []spend{{t3.TxID().String(), 0}}, 901, 0)
	admitTx(s, t4, 0, v.Topic)
	want := IntervalReport{Interval: 4, First: 5, Count: 2, Txid: t4.TxID().String(), Status: IntervalIncomplete, Missing: []uint64{5, 6}}
	if r := s.Report("")[0].Intervals[3]; !reflect.DeepEqual(r, want) {
		t.Fatalf("%+v", r)
	}
}

func TestBranchJoinPending(t *testing.T) {
	v := vectors(t)
	s := service(t, newFakeHost())
	admitVector(t, s, "anchor-interval-1")
	t1 := v.txid(t, "anchor-interval-1")
	tree := v.txid(t, "funding-tree")
	later := token(t, logrec.Anchor{Interval: 1, First: 1, Count: 9, Root: fill(1)}, []spend{{tree, 5}}, 5000, 0)
	admitTx(s, later, 0, v.Topic)
	badJoin := token(t, logrec.Anchor{Interval: 2, Prev: hashOrder(t1), First: 7, Count: 1, Root: fill(1)}, []spend{{t1, 0}}, 5001, 0)
	admitTx(s, badJoin, 0, v.Topic)
	orphan := token(t, logrec.Anchor{Interval: 6, Prev: fill(9), First: 50, Count: 1, Root: fill(1)}, []spend{{strings.Repeat("ab", 32), 0}}, 5002, 0)
	admitTx(s, orphan, 0, v.Topic)
	r := s.Report("")[0]
	if len(r.Intervals) != 1 || r.Intervals[0].Txid != t1 {
		t.Fatalf("the chain stops before the failed join: %+v", r.Intervals)
	}
	want := []TokenReport{
		{1, later.TxID().String(), TokenBranch},
		{2, badJoin.TxID().String(), TokenJoin},
		{6, orphan.TxID().String(), TokenPending},
	}
	if !reflect.DeepEqual(r.Tokens, want) {
		t.Fatalf("%+v", r.Tokens)
	}
	q := map[string]any{"stream": v.Stream, "producer": v.IdentityKey, "interval": 1}
	if got := formulaTxids(mustAsk(t, s, q)); !reflect.DeepEqual(got, []string{t1, later.TxID().String()}) {
		t.Fatalf("canonical first: %v", got)
	}
	// A token mined earlier than the vector's interval 1 becomes canonical.
	earlier := token(t, logrec.Anchor{Interval: 1, First: 1, Count: 2, Root: fill(1)}, []spend{{tree, 5}}, 10, 0)
	admitTx(s, earlier, 0, v.Topic)
	if got := formulaTxids(mustAsk(t, s, q)); got[0] != earlier.TxID().String() {
		t.Fatal(got)
	}
}

func TestCaps(t *testing.T) {
	v := vectors(t)
	host := newFakeHost()
	s := service(t, host)
	type made struct{ id, k string }
	var all []made
	for i := 0; i <= MaxCandidates; i++ {
		tx := carrier(t, 5, fill(1), fmt.Sprintf("line %d", i), 5)
		admitTx(s, tx, 0, v.Topic)
		all = append(all, made{tx.TxID().String(), hex.EncodeToString(tx.TxID()[:])})
	}
	sort.Slice(all, func(i, j int) bool { return all[i].k < all[j].k })
	var want []string
	for _, m := range all[:MaxCandidates] {
		want = append(want, m.id)
	}
	if got := formulaTxids(mustAsk(t, s, map[string]any{"stream": v.Stream, "producer": v.IdentityKey, "seq": 5})); !reflect.DeepEqual(got, want) {
		t.Fatalf("%v", got)
	}
	if host.count("blogs_candidates_dropped_total", nil) != 1 {
		t.Fatal("candidates dropped")
	}
	for i := 0; i <= MaxCandidates; i++ {
		tok := token(t, logrec.Anchor{Interval: 3, Prev: fill(2), First: 9, Count: uint64(1 + i), Root: fill(1)}, []spend{{strings.Repeat("cd", 32), uint32(i)}}, uint32(700+i), 0) //nolint:gosec // small
		admitTx(s, tok, 0, v.Topic)
	}
	if n := len(mustAsk(t, s, map[string]any{"stream": v.Stream, "producer": v.IdentityKey, "interval": 3})); n != MaxCandidates {
		t.Fatal(n)
	}
	if host.count("blogs_tokens_dropped_total", nil) != 1 {
		t.Fatal("tokens dropped")
	}
}

func TestOnlyAMinedSpendRetracts(t *testing.T) {
	v := vectors(t)
	s := service(t, newFakeHost())
	admitVector(t, s, "carrier-seq-1")
	// A second carrier on the same funding output (never mined) is not a retraction.
	second := carrier(t, 1, [32]byte{}, "same funding output", 0)
	admitTx(s, second, 0, v.Topic)
	if status(t, s, v.txid(t, "carrier-seq-1")) != BatchFork || s.Counts().Retracted != 0 {
		t.Fatal("an unmined spend retracted")
	}
	admitVector(t, s, "sweep")
	if status(t, s, v.txid(t, "carrier-seq-1")) != BatchRetracted || status(t, s, second.TxID().String()) != BatchRetracted {
		t.Fatal("the sweep did not retract")
	}
	if len(mustAsk(t, s, map[string]any{"stream": v.Stream, "producer": v.IdentityKey, "seq": 1})) != 0 {
		t.Fatal("a retracted batch is answered")
	}
	// A carrier that arrives after the sweep is retracted on arrival.
	late := service(t, newFakeHost())
	admitVector(t, late, "sweep")
	admitVector(t, late, "carrier-seq-2")
	if status(t, late, v.txid(t, "carrier-seq-2")) != BatchRetracted {
		t.Fatal("late carrier")
	}
}

func TestOtherTopicsAndEviction(t *testing.T) {
	v := vectors(t)
	s := service(t, newFakeHost())
	admitVectorOn(t, s, "carrier-seq-1", "tm_anytx")
	if s.Counts().Batches != 0 {
		t.Fatal("an undeclared topic was indexed")
	}
	admitVector(t, s, "carrier-seq-1")
	if s.Counts().Batches != 1 {
		t.Fatal("not indexed")
	}
	h, _ := chainhash.NewHashFromHex(v.txid(t, "carrier-seq-1"))
	_ = s.OutputEvicted(context.Background(), &transaction.Outpoint{Txid: *h, Index: 0})
	if s.Counts().Batches != 0 || len(mustAsk(t, s, map[string]any{"stream": v.Stream, "producer": v.IdentityKey, "seq": 1})) != 0 {
		t.Fatal("an evicted output is answered")
	}
}

func TestCoinsFromTheIndexUntilRestore(t *testing.T) {
	v := vectors(t)
	s := service(t, newFakeHost())
	admitVector(t, s, "funding-tree")
	tree := v.byName(t, "funding-tree").tx(t)
	ctx := context.Background()
	c, ok, _ := s.Coin(ctx, v.Topic, transaction.Outpoint{Txid: *tree.TxID(), Index: 3})
	if !ok || !bytes.Equal(c.LockingScript, *tree.Outputs[3].LockingScript) {
		t.Fatal("output 3")
	}
	if _, ok, _ := s.Coin(ctx, v.Topic, transaction.Outpoint{Txid: *tree.TxID(), Index: 5}); ok {
		t.Fatal("change is not admitted")
	}
	if _, ok, _ := s.Coin(ctx, "tm_anytx", transaction.Outpoint{Txid: *tree.TxID(), Index: 3}); ok {
		t.Fatal("another topic")
	}
}
