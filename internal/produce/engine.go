// Package produce is a blogs producer: it turns batches of log lines into
// signed carriers on one stream, publishes each once, and anchors the
// stream's batches in one mined token per interval (docs/spec.md sections
// 3 to 6 and 12).
//
// It is built on bcommon's producer package. A Payer pays for funding trees
// from the home's coin pool. Two Trees mint them, both with Ahead: one of
// small outputs that carriers spend, one of larger outputs that pay each
// anchor's fee (an anchor spends only its predecessor and funding outputs,
// so its fee comes from a funding output). Payer.Async is off: a tree is
// settled and waited for, and published only once it has mined, because a
// host admits funding outputs only from mined transactions. A Collector
// collects each anchor's proof, and the anchor is published once, after it
// mines. Kept holds the one copy of every tree and anchor in use.
//
// Every tree is recorded with its fee coin before it reaches the settlement
// leg (Trees.Prepare), and a run's start settles the records an earlier run
// left (recover.go), so a run that stops between a tree's signing and its
// adoption loses neither the coin nor the tree.
//
// The state file (package state) is written before every publish, so a
// crash never makes the producer build a second carrier for a seq it may
// already have published: the persisted carrier is published again instead.
package produce

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bcommon/bwallet"
	"github.com/lightwebinc/bcommon/carrier"
	"github.com/lightwebinc/bcommon/funding"
	"github.com/lightwebinc/bcommon/mint"
	"github.com/lightwebinc/bcommon/nodeapi"
	"github.com/lightwebinc/bcommon/producer"
	"github.com/lightwebinc/bcommon/publish"

	"github.com/lightwebinc/blogs/internal/limits"
	"github.com/lightwebinc/blogs/internal/logrec"
	"github.com/lightwebinc/blogs/internal/state"
	"github.com/lightwebinc/blogs/internal/unicast"
)

// Params are the carrier parameters of every blogs carrier.
var Params = carrier.Params{
	Derivation: logrec.BatchDerivation,
	FundingTag: logrec.TagFunding,
	ValidatePayload: func(p []byte) error {
		_, err := logrec.DecodeBatch(p)
		return err
	},
}

// Profile is the embedded wallet's profile: the fund key under [1, "blogs"]
// key id fund, and the registered basket.
var Profile = bwallet.Profile{
	FundProtocol:   logrec.Protocol,
	FundKeyID:      logrec.KeyFund,
	FundBasket:     "blogs fund",
	Version:        "blogs-1",
	LegacyPoolFile: "blogs-pool.json",
}

// Options are a producer's choices. None of them is frozen (docs/frozen.md
// section 8).
type Options struct {
	// Profile is logrec.ProfileFull or logrec.ProfileDigest; Encoding is
	// logrec.EncodingLines or logrec.EncodingLinesGzip.
	Profile  uint64
	Encoding uint64
	// ContentDir is where a digest-only batch's content is written, named
	// by its SHA-256 in hex; URLs are prefixes the digest's hex is appended
	// to for the locator's fetch hints. Digest-only only.
	ContentDir string
	URLs       []string
	// Interval is the anchor cadence: an anchor is built once Interval has
	// passed since the last one and a batch is unanchored.
	Interval time.Duration
	// AnchorBatches anchors early, once this many batches are unanchored,
	// and is the most one anchor covers; zero is the record's bound,
	// logrec.MaxIntervalBatches.
	AnchorBatches int
	// Poll is how often proofs are asked for.
	Poll time.Duration
	// ObjectBound is the plane's object bound, in bytes.
	ObjectBound int
	// TreeCount and TreeSats size the batch funding trees; AnchorTreeCount
	// and AnchorTreeSats the anchor fee trees. Ahead and AnchorAhead are
	// how few outputs a tree has left when the next is minted ahead.
	TreeCount       int
	TreeSats        uint64
	AnchorTreeCount int
	AnchorTreeSats  uint64
	Ahead           uint32
	AnchorAhead     uint32
	// Fees is the fee policy of trees and anchors.
	Fees mint.Fees
	// Wait bounds a wait for one proof.
	Wait time.Duration
}

// Legs are the endpoints a producer talks to. Objects go to exactly one of
// Facade (plane mode: one submit, which the plane delivers to every
// subscribed host) and Hosts (unicast mode: one submit to each host).
// SweepHosts, in plane mode, are hosts every sweep is also submitted to
// directly: no lookup class names the sweep of a batch, so a host off the plane
// receives one only this way.
type Legs struct {
	Settler    publish.Settler
	Asset      *nodeapi.Asset
	Arcade     *publish.Arcade
	Facade     *publish.Facade
	Hosts      *unicast.Set
	SweepHosts *unicast.Set
}

// treeFacade is where a new funding tree is published: the facade, or in
// unicast mode a facade that submits to every host.
func (l Legs) treeFacade(what string) *publish.Facade {
	if l.Hosts != nil {
		return l.Hosts.Facade(what)
	}
	return l.Facade
}

// Engine is one producer run on one stream.
type Engine struct {
	St     *state.Stream
	Signer *bwallet.Signer
	Pool   *bwallet.Pool
	Legs   Legs
	Opts   Options
	// Now is the clock records are stamped with.
	Now func() time.Time
	// Note receives progress lines; Verbose adds one per batch.
	Note    func(format string, args ...any)
	Verbose bool

	topic       string
	payer       *producer.Payer
	kept        *producer.Kept
	trees       *producer.Trees
	anchorTrees *producer.Trees
	collector   *producer.Collector
	// warnedSize is set once a carrier over the plane's packet aim was
	// reported, so a run reports it once.
	warnedSize bool
	// warnedSweep is set once a sweep went to the facade alone.
	warnedSweep bool
}

// New builds an engine over a stream's state.
func New(st *state.Stream, signer *bwallet.Signer, pool *bwallet.Pool, legs Legs, o Options) (*Engine, error) {
	topic, err := logrec.Topic(st.Stream)
	if err != nil {
		return nil, err
	}
	if st.Identity != signer.IdentityHex() {
		return nil, fmt.Errorf("stream %s belongs to identity %s, not this home's %s", st.Stream, st.Identity, signer.IdentityHex())
	}
	if legs.Settler == nil || (legs.Facade == nil) == (legs.Hosts == nil) || legs.Asset == nil {
		return nil, errors.New("a producer needs a settlement leg, a node, and either a facade or hosts")
	}
	e := &Engine{St: st, Signer: signer, Pool: pool, Legs: legs, Opts: o, Now: time.Now, topic: topic}
	e.kept = &producer.Kept{Load: e.load}
	e.payer = e.newPayer()
	lock := func(ctx context.Context) (*script.Script, error) {
		return carrier.FundingLock(ctx, signer, signer.Originator, Params)
	}
	e.trees = &producer.Trees{Payer: e.payer, State: treeState{e, false}, Identity: signer.IdentityHex(),
		Count: o.TreeCount, Sats: o.TreeSats, Funder: "pool", Lock: lock, Change: signer.FundScript,
		Facade: legs.treeFacade("batch funding tree"), Topic: topic, Ahead: o.Ahead, Prepare: e.prepareTree(BatchTree)}
	e.anchorTrees = &producer.Trees{Payer: e.payer, State: treeState{e, true}, Identity: signer.IdentityHex(),
		Count: o.AnchorTreeCount, Sats: o.AnchorTreeSats, Funder: "pool", Lock: lock, Change: signer.FundScript,
		Facade: legs.treeFacade("anchor fee tree"), Topic: topic, Ahead: o.AnchorAhead, Prepare: e.prepareTree(AnchorTree)}
	e.collector = &producer.Collector{Proofs: producer.Proofs{Arcade: legs.Arcade, Asset: legs.Asset},
		Kept: e.kept, Pool: pool, Save: st.Save, Note: e.note}
	return e, nil
}

// newPayer is a Payer over the engine's pool and legs. A sweep takes its
// fee through a Payer of its own, so that giving back its coin gives back
// nothing another transaction took.
func (e *Engine) newPayer() *producer.Payer {
	p := &producer.Payer{
		Pool: e.Pool, Keys: map[string]*bwallet.Signer{e.Signer.IdentityHex(): e.Signer},
		Kept: e.kept, Settler: e.Legs.Settler, Asset: e.Legs.Asset, Fees: e.Opts.Fees,
		Timeout: e.Opts.Wait, Poll: e.Opts.Poll, Note: e.note,
	}
	if e.payer != nil {
		p.Tip = e.payer.Tip
	}
	return p
}

func (e *Engine) note(format string, args ...any) {
	if e.Note != nil {
		e.Note(format, args...)
	}
}

// Topic is the stream's topic.
func (e *Engine) Topic() string { return e.topic }

// treeState is one of the stream's two tree lists as a producer.TreeState.
type treeState struct {
	e      *Engine
	anchor bool
}

func (t treeState) Current() *funding.Tree {
	if t.anchor {
		return t.e.St.AnchorTree
	}
	return t.e.St.Tree
}

// Adopt makes tr the current tree of its kind. The same save drops the
// record made when the tree was signed, and the tree's entry among those
// minted ahead: from here on the tree lists hold it.
func (t treeState) Adopt(tr funding.Tree) error {
	st := t.e.St
	if t.anchor {
		st.AnchorTree = &tr
		st.AnchorTrees = append(st.AnchorTrees, tr)
	} else {
		st.Tree = &tr
		st.Trees = append(st.Trees, tr)
	}
	st.DropPendingTree(tr.Txid)
	st.Ahead = slices.DeleteFunc(st.Ahead, func(a funding.Tree) bool { return a.Txid == tr.Txid })
	return st.Save()
}

// load rebuilds a transaction the stream keeps: a funding tree or an anchor.
func (e *Engine) load(txid string) (*transaction.Transaction, error) {
	st := e.St
	var trees []funding.Tree
	for _, t := range []*funding.Tree{st.Tree, st.AnchorTree} {
		if t != nil {
			trees = append(trees, *t)
		}
	}
	trees = append(append(append(trees, st.Trees...), st.AnchorTrees...), st.Ahead...)
	for _, t := range trees {
		if t.Txid == txid {
			return funding.Rebuild(t.RawHex, t.BumpHex, t.BeefHex)
		}
	}
	for _, a := range []*state.Anchor{st.Anchor, st.Inflight} {
		if a != nil && a.Txid == txid {
			return funding.Rebuild(a.RawHex, a.BumpHex, a.BeefHex)
		}
	}
	return nil, fmt.Errorf("transaction %s is not one this stream keeps", txid)
}

// FeeError words the library's refusals to find a fee input for an
// operator. Every other error passes as it is.
func FeeError(err error) error {
	var nc *producer.NoCoinError
	var nk *producer.NoKeyError
	switch {
	case errors.As(err, &nc):
		switch {
		case nc.Held > 0 && nc.Minting > 0:
			return fmt.Errorf("fee input: %w: the other coins are change from %d transaction(s) whose proofs have not arrived, and %d funding tree(s) minted ahead hold a coin and have not settled; they become spendable once mined, so run the command again", nc.Err, nc.Held, nc.Minting)
		case nc.Held > 0:
			return fmt.Errorf("fee input: %w: the other coins are change from %d transaction(s) whose proofs have not arrived; they become spendable once mined", nc.Err, nc.Held)
		case nc.Minting > 0:
			return fmt.Errorf("fee input: %w: %d funding tree(s) minted ahead hold a coin and have not settled; the change returns once one does, so run the command again", nc.Err, nc.Minting)
		}
		return fmt.Errorf("fee input: %w (run `blogs fund`: pay the fund address from your own wallet, then `blogs fund -txid TXID`)", nc.Err)
	case errors.As(err, &nk):
		return fmt.Errorf("wallet output %s is locked to a key this home does not hold", nk.Outpoint)
	}
	return err
}

// feeError is FeeError for a fee this engine asked for. A tree minted ahead
// is waited for by the take (producer.Payer.Take), for as long as the tree
// itself waits for its block, so a tree that does not mine in time ends
// both waits: the take then finds no coin and no mint in flight, while the
// coin is in a tree on its way to a block. The record made when the tree
// was signed says so, and the words follow it.
func (e *Engine) feeError(err error) error {
	var nc *producer.NoCoinError
	if errors.As(err, &nc) && nc.Held == 0 && nc.Minting == 0 {
		if n := e.unsettled(); n > 0 {
			return fmt.Errorf("fee input: %w: %d funding tree(s) signed for a coin of this home are not settled, and the change of one on its way to a block returns once it mines; run the command again, which asks the node about them first, or run `blogs fund -txid TXID` so the pool holds a second coin", nc.Err, n)
		}
	}
	return FeeError(err)
}

// unsettled counts the trees signed for a coin that are neither adopted nor
// held for the switch: the ones whose change the pool has not got.
func (e *Engine) unsettled() int {
	n := 0
	for _, p := range e.St.PendingTrees {
		id := p.Tree.Txid
		held := e.St.MintedAhead(id)
		for _, t := range []*producer.Trees{e.trees, e.anchorTrees} {
			held = held || slices.ContainsFunc(t.Held(), func(h funding.Tree) bool { return h.Txid == id })
		}
		if !held && !e.adopted(id) {
			n++
		}
	}
	return n
}

// Start reads the chain tip for coinbase maturity, settles the funding
// trees an earlier run signed and did not adopt, publishes the trees an
// earlier run adopted and could not publish, and publishes the carrier a
// previous run persisted and may not have published. Nothing is spent
// before it returns.
func (e *Engine) Start(ctx context.Context) error {
	if err := e.tip(ctx); err != nil {
		return err
	}
	if err := e.RecoverTrees(ctx); err != nil {
		return err
	}
	if err := e.PublishTrees(ctx); err != nil {
		return err
	}
	return e.Resume(ctx)
}

func (e *Engine) tip(ctx context.Context) error {
	h, err := e.Legs.Asset.BestHeader(ctx)
	if err != nil {
		return fmt.Errorf("node tip: %w", err)
	}
	e.payer.Tip = h.Height
	return nil
}

// Resume publishes the persisted carrier, if there is one: the same bytes,
// so a host that already holds it answers a duplicate.
func (e *Engine) Resume(ctx context.Context) error {
	if e.St.Pending == "" {
		return nil
	}
	beef, err := hex.DecodeString(e.St.Pending)
	if err != nil {
		return fmt.Errorf("persisted carrier of seq %d: %w", e.St.Seq, err)
	}
	e.note("seq %d: publishing the carrier a previous run persisted", e.St.Seq)
	if err := e.publishCarrier(ctx, e.St.Seq, beef, true); err != nil {
		return err
	}
	e.St.Pending = ""
	return e.St.Save()
}

// Batch is one batch's content before encoding: LF-terminated lines.
type Batch struct {
	Raw   []byte
	Count int
	// Tail is the followed file's position after the batch's last line.
	Tail *state.Tail
}

// Publish encodes b, splits it while its encoding is over the record's
// bound, and publishes each part as one batch.
func (e *Engine) Publish(ctx context.Context, b Batch) error {
	content, err := encode(b.Raw, e.Opts.Encoding)
	if err != nil {
		return err
	}
	limit := logrec.MaxContent
	if e.Opts.Profile == logrec.ProfileDigest {
		limit = logrec.MaxLocated
	}
	if len(content) > limit || len(b.Raw) > logrec.MaxExpanded {
		first, second, ok := split(b.Raw)
		if !ok {
			return fmt.Errorf("one line encodes to %d bytes, over the bound of %d", len(content), limit)
		}
		if err := e.Publish(ctx, Batch{Raw: first, Count: countLines(first)}); err != nil {
			return err
		}
		return e.Publish(ctx, Batch{Raw: second, Count: countLines(second), Tail: b.Tail})
	}
	return e.publishOne(ctx, b, content)
}

func (e *Engine) publishOne(ctx context.Context, b Batch, content []byte) error {
	if e.St.Pending != "" {
		if err := e.Resume(ctx); err != nil {
			return err
		}
	}
	st := e.St
	rec := &logrec.Batch{
		Producer: e.Signer.Identity.Compressed(), Stream: st.Stream, Seq: st.Seq + 1,
		Timestamp: uint64(e.Now().UnixMilli()), Count: uint64(b.Count), //nolint:gosec // a clock after 1970
		Encoding: e.Opts.Encoding, Profile: e.Opts.Profile, Size: uint64(len(b.Raw)),
	}
	if st.Head != "" {
		h, err := hex.DecodeString(st.Head)
		if err != nil || len(h) != 32 {
			return fmt.Errorf("state head %q is not a commitment", st.Head)
		}
		rec.Prev = [32]byte(h)
	}
	switch e.Opts.Profile {
	case logrec.ProfileFull:
		rec.Content = content
	case logrec.ProfileDigest:
		loc, err := e.store(content)
		if err != nil {
			return err
		}
		rec.Locator = loc
	default:
		return fmt.Errorf("profile %d is not one this producer writes", e.Opts.Profile)
	}
	payload, err := rec.Encode()
	if err != nil {
		return fmt.Errorf("seq %d: %w", rec.Seq, err)
	}
	tree, vout, err := e.spend(ctx, e.trees, 1)
	if err != nil {
		return e.feeError(fmt.Errorf("batch funding tree: %w", err))
	}
	k, err := carrier.Mint(ctx, e.Signer, e.Signer.Originator, Params, payload, tree, vout)
	if err != nil {
		return fmt.Errorf("seq %d: carrier: %w", rec.Seq, err)
	}
	// The producer's own copy of the host's rules: a carrier a host would
	// refuse is never persisted or published.
	adm, err := logrec.Admit(ctx, k, nil, logrec.Host{Stream: st.Stream, Policy: logrec.Policy{Open: true}})
	if err != nil {
		return fmt.Errorf("seq %d: the carrier breaks the host's rules (%s): %w", rec.Seq, logrec.Reason(err), err)
	}
	beef, err := k.AtomicBEEF(false)
	if err != nil {
		return fmt.Errorf("seq %d: BEEF: %w", rec.Seq, err)
	}
	if len(beef) > e.Opts.ObjectBound {
		return fmt.Errorf("seq %d: the carrier's BEEF is %d bytes, over the object bound of %d (shrink the batch or the funding tree)", rec.Seq, len(beef), e.Opts.ObjectBound)
	}
	if e.Legs.Facade != nil && len(beef) > limits.PlaneObjectAim && !e.warnedSize {
		e.warnedSize = true
		e.note("warning: seq %d: the carrier's BEEF is %d bytes, about %d packets on the plane; objects over %d packets are lost far more often under loss and distance (lower -max-bytes or -tree-count; docs/limits.md)",
			rec.Seq, len(beef), (len(beef)+limits.FragmentPayload-1)/limits.FragmentPayload, limits.PlanePackets)
	}
	c := adm.Carrier.Commitment
	st.Seq, st.Head, st.Pending = rec.Seq, hex.EncodeToString(c[:]), hex.EncodeToString(beef)
	st.Unanchored = append(st.Unanchored, st.Head)
	st.Tree.Next = vout + 1
	st.Trees = producer.Index(st.Trees, st.Tree)
	if b.Tail != nil {
		st.Tail = b.Tail
	}
	if st.LastAnchorMs == 0 {
		st.LastAnchorMs = e.Now().UnixMilli()
	}
	if err := st.Save(); err != nil {
		return err
	}
	if err := e.publishCarrier(ctx, rec.Seq, beef, false); err != nil {
		return err
	}
	st.Pending = ""
	if err := st.Save(); err != nil {
		return err
	}
	if e.Verbose {
		e.note("seq %d: %d line(s), %d byte(s), carrier %s", rec.Seq, b.Count, len(b.Raw), chainhash.Hash(c))
	}
	return nil
}

// Retries are the waits between the tries of a publish before the run
// stops with the carrier persisted.
var Retries = []time.Duration{time.Second, 2 * time.Second, 4 * time.Second}

func (e *Engine) publishCarrier(ctx context.Context, seq uint64, beef []byte, resumed bool) error {
	if e.Legs.Hosts != nil {
		// Unicast: every host is sent the carrier and retried on its own.
		o := e.Legs.Hosts.Send(ctx, fmt.Sprintf("seq %d", seq), e.topic, beef, 0)
		if err := o.Err(); err != nil {
			return fmt.Errorf("seq %d: publish: %w; the carrier is persisted and a later run publishes it", seq, err)
		}
		if !resumed {
			for _, h := range nothingAdmitted(o) {
				e.note("seq %d: host %s admitted nothing: it holds this carrier already, or refused it", seq, h)
			}
		}
		return nil
	}
	var res publish.Result
	interrupted, err := e.retry(ctx, fmt.Sprintf("seq %d", seq), func() (err error) {
		res, err = e.Legs.Facade.Submit(ctx, e.topic, beef)
		return err
	})
	if interrupted {
		return err
	}
	if err != nil {
		return fmt.Errorf("seq %d: publish: %w; the carrier is persisted and a later run publishes it", seq, err)
	}
	if res.Duplicate && !resumed {
		e.note("seq %d: the host admitted nothing: it holds this carrier already, or refused it", seq)
	}
	return nil
}

// retry runs one publish to the facade until it succeeds, waiting Retries
// between the tries, and returns the last failure. When ctx ends during a
// wait it reports that, with ctx's error.
func (e *Engine) retry(ctx context.Context, what string, try func() error) (interrupted bool, err error) {
	for i := 0; ; i++ {
		if err = try(); err == nil {
			return false, nil
		}
		if i == len(Retries) || ctx.Err() != nil {
			return false, err
		}
		e.note("%s: publish failed (%v); retrying", what, err)
		select {
		case <-ctx.Done():
			return true, ctx.Err()
		case <-time.After(Retries[i]):
		}
	}
}

// nothingAdmitted are the hosts that answered with nothing admitted: they
// hold the object already, or their topic manager refused it. The answer is
// the same for both, so a producer cannot tell them apart.
func nothingAdmitted(o unicast.Outcome) []string {
	var hs []string
	for _, r := range o.Results {
		if r.Took() && len(r.Result.Admitted) == 0 {
			hs = append(hs, r.Host)
		}
	}
	return hs
}

// Unanchored is how many batches no published anchor covers yet.
func (e *Engine) Unanchored() int { return len(e.St.Unanchored) }

// Tick advances the anchor chain by what can be done without waiting: it
// collects the proof of the anchor in flight and publishes it once mined,
// and builds and settles the next anchor when none is in flight, a batch is
// unanchored and the interval has passed (or force is set).
func (e *Engine) Tick(ctx context.Context, force bool) error {
	if err := e.tip(ctx); err != nil {
		e.note("note: %v", err)
	}
	if e.St.Inflight != nil {
		return e.advance(ctx)
	}
	if len(e.St.Unanchored) == 0 {
		return nil
	}
	if !force && e.Now().Sub(time.UnixMilli(e.St.LastAnchorMs)) < e.Opts.Interval && len(e.St.Unanchored) < e.anchorBatches() {
		return nil
	}
	return e.anchor(ctx)
}

// anchorBatches is the most batches one anchor covers.
func (e *Engine) anchorBatches() int {
	if n := e.Opts.AnchorBatches; n > 0 && n < logrec.MaxIntervalBatches {
		return n
	}
	return logrec.MaxIntervalBatches
}

// Settled reports whether every batch is anchored and nothing is in flight.
func (e *Engine) Settled() bool { return e.St.Inflight == nil && len(e.St.Unanchored) == 0 }

// Drain anchors every batch built so far and waits until the anchors are
// mined and published, or ctx ends.
func (e *Engine) Drain(ctx context.Context) error {
	for {
		if err := e.Tick(ctx, true); err != nil {
			return err
		}
		if e.Settled() {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("anchoring: %w (the anchor in flight is collected by the next run)", ctx.Err())
		case <-time.After(e.Opts.Poll):
		}
	}
}

// anchor builds the next anchor over the unanchored batches, persists it,
// and hands it to the settlement leg.
func (e *Engine) anchor(ctx context.Context) error {
	st := e.St
	count := min(len(st.Unanchored), e.anchorBatches())
	cs := make([][32]byte, count)
	for i := range cs {
		b, err := hex.DecodeString(st.Unanchored[i])
		if err != nil || len(b) != 32 {
			return fmt.Errorf("state: unanchored commitment %q", st.Unanchored[i])
		}
		cs[i] = [32]byte(b)
	}
	a := &logrec.Anchor{Producer: e.Signer.Identity.Compressed(), Stream: st.Stream, Interval: 1,
		First: st.NextFirst, Count: uint64(count), Root: logrec.IntervalRoot(cs),
		Timestamp: uint64(e.Now().UnixMilli())} //nolint:gosec // a clock after 1970
	var prev *mint.Input
	if st.Anchor != nil {
		a.Interval = st.Anchor.Interval + 1
		h, err := chainhash.NewHashFromHex(st.Anchor.Txid)
		if err != nil {
			return err
		}
		a.Prev = [32]byte(*h)
		ptx, err := e.kept.Tx(st.Anchor.Txid)
		if err != nil {
			return fmt.Errorf("anchor interval %d: %w", st.Anchor.Interval, err)
		}
		prev = &mint.Input{Tx: ptx, Vout: st.Anchor.Vout, Unlocker: logrec.AnchorDerivation.Unlocker(ctx, e.Signer, e.Signer.Originator)}
	}
	rec, err := a.Encode()
	if err != nil {
		return fmt.Errorf("anchor interval %d: %w", a.Interval, err)
	}
	lock, err := logrec.AnchorDerivation.Lock(ctx, e.Signer, e.Signer.Originator, [][]byte{logrec.TagAnchor, rec}, true)
	if err != nil {
		return err
	}
	tree, vout, err := e.spend(ctx, e.anchorTrees, 1)
	if err != nil {
		return e.feeError(fmt.Errorf("anchor funding tree: %w", err))
	}
	change, err := e.Signer.FundScript()
	if err != nil {
		return err
	}
	fee := mint.Input{Tx: tree, Vout: vout, Unlocker: logrec.BatchDerivation.Unlocker(ctx, e.Signer, e.Signer.Originator)}
	tok, err := mint.Transition(lock, 1, prev, fee, change, e.Opts.Fees)
	if err != nil {
		return fmt.Errorf("anchor interval %d: %w (the anchor funding trees' outputs must cover an anchor's fee)", a.Interval, err)
	}
	beefHex, err := funding.KeepBEEF(tok, nil)
	if err != nil {
		return err
	}
	st.Inflight = &state.Anchor{Interval: a.Interval, First: a.First, Count: a.Count,
		Txid: tok.TxID().String(), Vout: 0, RawHex: tok.Hex(), BeefHex: beefHex}
	st.AnchorTree.Next = vout + 1
	st.AnchorTrees = producer.Index(st.AnchorTrees, st.AnchorTree)
	st.LastAnchorMs = e.Now().UnixMilli()
	if err := st.Save(); err != nil {
		return err
	}
	e.note("anchor interval %d: seqs %d to %d, token %s", a.Interval, a.First, a.Last(), tok.TxID())
	return e.submit(ctx, tok)
}

// submit hands the anchor in flight to the settlement leg. A failure is a
// note: the next tick sends the same transaction again. A refusal for good
// is not: when the node names another transaction as the spender of the
// anchor's fee output, these bytes can never mine, and the anchor is given
// up and built again from the next output, as one refused after the leg
// accepted it is (advance). When the node names another spender of the
// previous anchor token, no anchor can follow it: a rebuild would only
// spend fee outputs, so the anchor stays in flight with a warning, and
// batches go on being published.
func (e *Engine) submit(ctx context.Context, tok *transaction.Transaction) error {
	inf := e.St.Inflight
	if err := e.Legs.Settler.Submit(ctx, tok); err != nil {
		var se *nodeapi.SpentError
		if _, _, perr := e.Legs.Asset.Proof(ctx, inf.Txid); perr != nil && errors.As(e.Legs.Asset.SpentElsewhere(ctx, tok), &se) {
			if a := e.St.Anchor; a == nil || se.Outpoint != fmt.Sprintf("%s.%d", a.Txid, a.Vout) {
				return e.giveUpAnchor(fmt.Errorf("%w: %w (%w)", producer.ErrRefused, se, err))
			}
			e.note("WARNING: anchor interval %d: the previous anchor token %s is spent by %s, not by this stream's next anchor, so no anchor can follow it; it stays in flight and is sent again on the next tick", inf.Interval, se.Outpoint, se.By)
			return nil
		}
		e.note("anchor interval %d: settle via %s failed (%v); sent again on the next tick", inf.Interval, e.Legs.Settler.Name(), err)
		return nil
	}
	e.St.Inflight.Submitted = true
	e.payer.Change(tok, 0, nil)
	return e.St.Save()
}

// advance collects the proof of the anchor in flight and publishes it once
// it has mined.
func (e *Engine) advance(ctx context.Context) error {
	st := e.St
	inf := st.Inflight
	if !inf.Submitted {
		tok, err := e.kept.Tx(inf.Txid)
		if err != nil {
			return err
		}
		return e.submit(ctx, tok)
	}
	var refused error
	e.collector.Collect(ctx, []producer.Pending{{
		What: "anchor", Txid: inf.Txid, RawHex: inf.RawHex, BeefHex: inf.BeefHex,
		Proven: func(mp *transaction.MerklePath, height uint32) {
			inf.BumpHex, inf.Height, inf.BeefHex = mp.Hex(), height, ""
		},
		Refused: func(err error) { refused = err },
	}}, inf.Txid)
	if refused != nil {
		return e.giveUpAnchor(refused)
	}
	if inf.BumpHex == "" {
		return nil
	}
	tok, err := e.kept.Tx(inf.Txid)
	if err != nil {
		return err
	}
	if tok.MerklePath == nil {
		if tok.MerklePath, err = transaction.NewMerklePathFromHex(inf.BumpHex); err != nil {
			return err
		}
	}
	beef, err := tok.AtomicBEEF(false)
	if err != nil {
		return err
	}
	if e.Legs.Hosts != nil {
		o := e.Legs.Hosts.Send(ctx, fmt.Sprintf("anchor interval %d", inf.Interval), e.topic, beef, 0)
		if err := o.Err(); err != nil {
			e.note("anchor interval %d: publish failed (%v); published on the next tick", inf.Interval, err)
			return nil
		}
		for _, h := range nothingAdmitted(o) {
			e.note("anchor interval %d: host %s admitted nothing: it holds the anchor already, or does not yet hold what it spends", inf.Interval, h)
		}
	} else {
		res, err := e.Legs.Facade.Submit(ctx, e.topic, beef)
		if err != nil {
			e.note("anchor interval %d: publish failed (%v); published on the next tick", inf.Interval, err)
			return nil
		}
		if res.Duplicate {
			e.note("anchor interval %d: the host admitted nothing: it holds the anchor already, or does not yet hold what it spends", inf.Interval)
		}
	}
	st.Anchor, st.Inflight = inf, nil
	st.Unanchored = st.Unanchored[inf.Count:]
	st.NextFirst += inf.Count
	if err := st.Save(); err != nil {
		return err
	}
	e.note("anchor interval %d: mined at height %d and published; seqs %d to %d anchored", inf.Interval, inf.Height, inf.First, inf.First+inf.Count-1)
	return nil
}

// giveUpAnchor drops the anchor in flight, which the network refused for
// good. Never mined, so never published: the next anchor is built again
// over the same batches with the same interval number, from the next fee
// output. Change the pool took from it when the leg accepted it is
// forgotten, since it will never exist.
func (e *Engine) giveUpAnchor(why error) error {
	st := e.St
	inf := st.Inflight
	e.note("WARNING: anchor interval %d %s: %v; it is built again", inf.Interval, inf.Txid, why)
	for _, o := range e.Pool.Outputs() {
		if o.TxID == inf.Txid {
			if _, err := e.Pool.Remove(o); err != nil {
				return err
			}
		}
	}
	st.Inflight = nil
	return st.Save()
}

// Close collects any tree still being minted ahead, so its change reaches
// the pool, and records every tree that is settled and was never adopted
// (minted ahead, or recovered while the current tree had outputs left) in
// the state's Ahead list, so a sweep finds it. Such a tree keeps the record
// made when it was signed, and the next run's start holds it again.
func (e *Engine) Close(ctx context.Context) error {
	for _, t := range []*producer.Trees{e.trees, e.anchorTrees} {
		_ = t.Wait(ctx)
		for _, h := range t.Held() {
			e.keepAhead(h)
		}
	}
	return e.St.Save()
}

// keepAhead records a settled tree that is not adopted among the trees
// minted ahead, once. An entry already there is left as it is: a sweep may
// have advanced its Next.
func (e *Engine) keepAhead(t funding.Tree) {
	if !e.St.MintedAhead(t.Txid) {
		e.St.Ahead = append(e.St.Ahead, t)
	}
}
