package produce_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/lightwebinc/bcommon/bwallet"
	"github.com/lightwebinc/bcommon/mint"
	"github.com/lightwebinc/bcommon/nodeapi"
	"github.com/lightwebinc/bcommon/publish"

	"github.com/lightwebinc/blogs/internal/logrec"
	"github.com/lightwebinc/blogs/internal/produce"
	"github.com/lightwebinc/blogs/internal/reader"
	"github.com/lightwebinc/blogs/internal/state"
	"github.com/lightwebinc/blogs/internal/testchain"
)

// rig is a funded producer home, a local chain and a stand-in host.
type rig struct {
	t      *testing.T
	home   string
	chain  *testchain.Chain
	host   *testchain.Host
	chainS *httptest.Server
	hostS  *httptest.Server
	wallet *bwallet.Embedded
	stream string
	notes  *bytes.Buffer
}

func newRig(t *testing.T) *rig {
	t.Helper()
	r := &rig{t: t, home: t.TempDir(), chain: testchain.New(700), notes: &bytes.Buffer{}}
	r.host = testchain.NewHost(r.chain)
	r.chainS = httptest.NewServer(r.chain)
	r.hostS = httptest.NewServer(r.host)
	t.Cleanup(r.chainS.Close)
	t.Cleanup(r.hostS.Close)
	w, err := bwallet.Create(r.home, produce.Profile)
	if err != nil {
		t.Fatal(err)
	}
	w.Originator = "blogs"
	r.wallet = w
	rpc := &nodeapi.RPC{URL: r.chainS.URL + "/rpc", ID: "blogs"}
	if _, _, err := bwallet.FundFromCoinbase(context.Background(), w.Signer(), w.Pool, rpc, r.asset(), 102, 0); err != nil {
		t.Fatal(err)
	}
	r.stream, err = logrec.NewStream("app", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.Create(r.home, r.stream, w.Signer().IdentityHex()); err != nil {
		t.Fatal(err)
	}
	return r
}

func (r *rig) asset() *nodeapi.Asset { return &nodeapi.Asset{Base: r.chainS.URL} }

func (r *rig) options() produce.Options {
	return produce.Options{
		Profile: logrec.ProfileFull, Encoding: logrec.EncodingLines,
		Interval: time.Hour, Poll: 5 * time.Millisecond, ObjectBound: 1 << 20,
		TreeCount: 4, TreeSats: 1, AnchorTreeCount: 3, AnchorTreeSats: 2000, Ahead: 2, AnchorAhead: 1,
		Fees: mint.DefaultFees, Wait: 5 * time.Second,
	}
}

func (r *rig) engine(o produce.Options) *produce.Engine {
	r.t.Helper()
	st, err := state.Load(r.home, r.stream)
	if err != nil {
		r.t.Fatal(err)
	}
	legs := produce.Legs{
		Settler: &publish.RPCSettler{RPC: &nodeapi.RPC{URL: r.chainS.URL + "/rpc", ID: "blogs"}},
		Asset:   r.asset(), Facade: &publish.Facade{Base: r.hostS.URL},
	}
	e, err := produce.New(st, r.wallet.Signer(), r.wallet.Pool, legs, o)
	if err != nil {
		r.t.Fatal(err)
	}
	e.Note = func(f string, a ...any) { fmt.Fprintf(r.notes, f+"\n", a...) }
	e.Verbose = true
	return e
}

func (r *rig) reader() *reader.Client {
	return &reader.Client{Hosts: []string{r.hostS.URL}, Headers: r.chain, Asset: r.asset(), Timeout: 5 * time.Second}
}

func lines(n, from int) produce.Batch {
	var b strings.Builder
	for i := range n {
		fmt.Fprintf(&b, "line %d\n", from+i)
	}
	return produce.Batch{Raw: []byte(b.String()), Count: n}
}

func TestProduceAnchorAndAudit(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	ctx := context.Background()
	e := r.engine(r.options())
	if err := e.Start(ctx); err != nil {
		t.Fatal(err)
	}
	for i := range 5 {
		if err := e.Publish(ctx, lines(3, i*3)); err != nil {
			t.Fatalf("batch %d: %v\n%s", i, err, r.notes)
		}
	}
	dctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := e.Drain(dctx); err != nil {
		t.Fatalf("drain: %v\n%s", err, r.notes)
	}
	for i := range 3 {
		if err := e.Publish(ctx, lines(2, 100+i*2)); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.Drain(dctx); err != nil {
		t.Fatalf("drain 2: %v\n%s", err, r.notes)
	}
	if err := e.Close(ctx); err != nil {
		t.Fatal(err)
	}
	st, _ := state.Load(r.home, r.stream)
	if st.Seq != 8 || st.Anchor == nil || st.Anchor.Interval != 2 || st.NextFirst != 9 || len(st.Unanchored) != 0 || st.Pending != "" {
		t.Fatalf("state after two intervals: seq %d next %d unanchored %d anchor %+v", st.Seq, st.NextFirst, len(st.Unanchored), st.Anchor)
	}
	producer, _ := hex.DecodeString(st.Identity)
	for n, want := range map[uint64][2]uint64{1: {1, 5}, 2: {6, 3}} {
		rep, err := r.reader().VerifyInterval(ctx, r.stream, producer, n)
		if err != nil {
			t.Fatal(err)
		}
		if rep.Outcome != reader.Verified {
			t.Fatalf("interval %d: %d %s %v\n%s", n, rep.Outcome, rep.Reason, rep.Refusals, r.notes)
		}
		last := rep.Chain[len(rep.Chain)-1].Anchor
		if last.First != want[0] || last.Count != want[1] || len(rep.Batches) != int(want[1]) {
			t.Fatalf("interval %d covers %d+%d, want %v", n, last.First, last.Count, want)
		}
	}
	t.Log(r.notes)
}
