package vectors

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/big"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/spv"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/go-sdk/transaction/template/p2pkh"
	"github.com/bsv-blockchain/go-sdk/util"
	"github.com/bsv-blockchain/go-sdk/wallet"

	"github.com/lightwebinc/bcommon/carrier"
	"github.com/lightwebinc/bcommon/mint"
	"github.com/lightwebinc/bcommon/pushdrop"

	"github.com/lightwebinc/blogs/internal/logrec"
)

// originator is the wallet originator the vectors sign under.
const originator = "vectors.example.com"

// OtherKey is a second test key, 32 bytes of 0x43, for producer-policy and
// other-producer cases. Public, never for real use.
var OtherKey = bytes.Repeat([]byte{0x43}, 32)

// Headers is the test chain's block headers: a merkle root per height. It
// is the chain tracker the vectors are admitted against.
type Headers map[uint32]chainhash.Hash

// IsValidRootForHeight holds root to the one the test chain has at height.
func (h Headers) IsValidRootForHeight(_ context.Context, root *chainhash.Hash, height uint32) (bool, error) {
	want, ok := h[height]
	return ok && root != nil && *root == want, nil
}

// CurrentHeight is the highest height held.
func (h Headers) CurrentHeight(context.Context) (uint32, error) {
	var top uint32
	for k := range h {
		top = max(top, k)
	}
	return top, nil
}

// chain mines transactions into blocks of two at successive heights: the
// transaction at offset 1 beside a fixed sibling, the way bcommon's test
// chain proves its coins.
type chain struct {
	height  uint32
	headers Headers
}

func (c *chain) mine(tx *transaction.Transaction) {
	c.height++
	sibling := chainhash.Hash(bytes.Repeat([]byte{0x33}, 32))
	isTxid := true
	tx.MerklePath = transaction.NewMerklePath(c.height, [][]*transaction.PathElement{{
		{Offset: 0, Hash: &sibling},
		{Offset: 1, Hash: tx.TxID(), Txid: &isTxid},
	}})
	root, err := tx.MerklePath.ComputeRoot(tx.TxID())
	if err != nil {
		panic(err)
	}
	c.headers[c.height] = *root
}

// unproven is a fixed height nothing is mined at: a proof there verifies
// against no header the test chain holds.
const unproven = 999

// txEnv is everything the transaction vectors are built from.
type txEnv struct {
	ctx      context.Context
	w        wallet.Interface
	params   carrier.Params
	producer *ec.PublicKey
	other    []byte
	feeKey   *ec.PrivateKey
	feeLock  *script.Script
	c        *chain
	coin     *transaction.Transaction
	tree     *transaction.Transaction
	misc     *transaction.Transaction
	records  [][]byte
	carriers []*transaction.Transaction
	anchors  []*transaction.Transaction
	anchorAs []*logrec.Anchor
}

func newTxEnv() (*txEnv, error) {
	ctx := context.Background()
	priv, _ := ec.PrivateKeyFromBytes(FixedKey)
	w, err := wallet.NewCompletedProtoWallet(priv)
	if err != nil {
		return nil, err
	}
	otherPriv, _ := ec.PrivateKeyFromBytes(OtherKey)
	// The fee coins pay the embedded wallet's funding key: [1, "blogs"],
	// key id fund, counterparty anyone, the producer's side.
	feeKey, err := wallet.NewKeyDeriver(priv).DerivePrivateKey(logrec.Protocol, logrec.KeyFund, pushdrop.Anyone())
	if err != nil {
		return nil, err
	}
	want, err := logrec.FundDerivation.ExpectedLockingKey(priv.PubKey())
	if err != nil || !want.IsEqual(feeKey.PubKey()) {
		return nil, fmt.Errorf("the fund key does not match its reader derivation: %v", err)
	}
	addr, err := script.NewAddressFromPublicKey(feeKey.PubKey(), false)
	if err != nil {
		return nil, err
	}
	feeLock, err := p2pkh.Lock(addr)
	if err != nil {
		return nil, err
	}
	e := &txEnv{
		ctx: ctx, w: w, producer: priv.PubKey(), other: otherPriv.PubKey().Compressed(),
		feeKey: feeKey, feeLock: feeLock, c: &chain{height: 800, headers: Headers{}},
		params: carrier.Params{
			Derivation: logrec.BatchDerivation, FundingTag: logrec.TagFunding,
			ValidatePayload: func(p []byte) error { _, err := logrec.DecodeBatch(p); return err },
		},
	}
	return e, e.build()
}

func (e *txEnv) feeUnlocker() transaction.UnlockingScriptTemplate {
	u, err := p2pkh.Unlock(e.feeKey, nil)
	if err != nil {
		panic(err)
	}
	return u
}

func (e *txEnv) fee(vout uint32) mint.Input {
	return mint.Input{Tx: e.coin, Vout: vout, Unlocker: e.feeUnlocker()}
}

func (e *txEnv) batchUnlocker() transaction.UnlockingScriptTemplate {
	return logrec.BatchDerivation.Unlocker(e.ctx, e.w, originator)
}

func (e *txEnv) anchorUnlocker() transaction.UnlockingScriptTemplate {
	return logrec.AnchorDerivation.Unlocker(e.ctx, e.w, originator)
}

// lock writes a signed PushDrop under d through the wallet.
func (e *txEnv) lock(d pushdrop.Derivation, fields ...[]byte) *script.Script {
	s, err := d.Lock(e.ctx, e.w, originator, fields, true)
	if err != nil {
		panic(err)
	}
	return s
}

// build mines the coin, the funding tree and a parent of odd outputs, mints
// three carriers and anchors them in two intervals.
func (e *txEnv) build() error {
	// The coin: five outputs paying the fund key, from nowhere, mined.
	e.coin = transaction.NewTransaction()
	nowhere := chainhash.Hash(bytes.Repeat([]byte{0x11}, 32))
	e.coin.AddInput(&transaction.TransactionInput{SourceTXID: &nowhere, UnlockingScript: &script.Script{},
		SequenceNumber: transaction.MaxTxInSequenceNum})
	for range 5 {
		e.coin.AddOutput(&transaction.TransactionOutput{Satoshis: 50000, LockingScript: e.feeLock})
	}
	e.c.mine(e.coin)

	// The funding tree: four outputs of 1000.
	flock, err := carrier.FundingLock(e.ctx, e.w, originator, e.params)
	if err != nil {
		return err
	}
	if !bytes.Equal(*flock, logrec.FundingScript(mustKey(logrec.BatchDerivation, e.producer))) {
		return fmt.Errorf("bcommon's funding lock is not the funding script")
	}
	if e.tree, err = mint.FundingTree(flock, 4, 1000, e.fee(0), e.feeLock, mint.DefaultFees); err != nil {
		return err
	}
	e.c.mine(e.tree)

	// misc: a mined parent of outputs a producer does not fund carriers
	// from: a funding-shaped output under the anchor key, a fee-key P2PKH,
	// and an anyone-can-spend OP_1.
	e.misc = transaction.NewTransaction()
	e.misc.AddInputFromTx(e.coin, 3, e.feeUnlocker())
	wrong := logrec.FundingScript(mustKey(logrec.AnchorDerivation, e.producer))
	e.misc.AddOutput(&transaction.TransactionOutput{Satoshis: 1000, LockingScript: (*script.Script)(&wrong)})
	e.misc.AddOutput(&transaction.TransactionOutput{Satoshis: 1000, LockingScript: e.feeLock})
	e.misc.AddOutput(&transaction.TransactionOutput{Satoshis: 1000, LockingScript: &script.Script{script.Op1}})
	e.misc.AddOutput(&transaction.TransactionOutput{Satoshis: 46000, LockingScript: e.feeLock})
	if err := e.misc.Sign(); err != nil {
		return err
	}
	e.c.mine(e.misc)

	// Three carriers on tree outputs 0 to 2: full-bytes lines, full-bytes
	// gzip, digest-only, each prev the one before's commitment.
	gz, _ := hex.DecodeString(gzipContent)
	digest := sha256.Sum256([]byte(linesContent))
	batches := []*logrec.Batch{
		{Seq: 1, Timestamp: t0, Count: 3, Encoding: logrec.EncodingLines, Profile: logrec.ProfileFull,
			Content: []byte(linesContent), Size: uint64(len(linesContent))},
		{Seq: 2, Timestamp: t0 + 1000, Count: 3, Encoding: logrec.EncodingLinesGzip, Profile: logrec.ProfileFull,
			Content: gz, Size: uint64(len(gzipExpanded))},
		{Seq: 3, Timestamp: t0 + 2000, Count: 3, Encoding: logrec.EncodingLines, Profile: logrec.ProfileDigest,
			Locator: &logrec.Locator{Digest: digest, Length: uint64(len(linesContent)), URLs: []string{}},
			Size:    uint64(len(linesContent))},
	}
	var prev [32]byte
	for i, b := range batches {
		b.Producer, b.Stream, b.Prev = e.producer.Compressed(), Stream, prev
		rec, err := b.Encode()
		if err != nil {
			return err
		}
		k, err := carrier.Mint(e.ctx, e.w, originator, e.params, rec, e.tree, uint32(i))
		if err != nil {
			return err
		}
		e.records = append(e.records, rec)
		e.carriers = append(e.carriers, k)
		prev = carrier.Commitment(k)
	}

	// Interval 1 covers seq 1 and 2 and spends tree output 3; interval 2
	// covers seq 3 and spends interval 1's token.
	a1 := e.anchorRecord(1, [32]byte{}, 1, 2, t0+60000)
	t1, err := e.token(a1, &mint.Input{Tx: e.tree, Vout: 3, Unlocker: e.batchUnlocker()}, 1)
	if err != nil {
		return err
	}
	e.c.mine(t1)
	a2 := e.anchorRecord(2, [32]byte(*t1.TxID()), 3, 1, t0+120000)
	t2, err := e.token(a2, &mint.Input{Tx: t1, Vout: 0, Unlocker: e.anchorUnlocker()}, 2)
	if err != nil {
		return err
	}
	e.c.mine(t2)
	e.anchors = []*transaction.Transaction{t1, t2}
	e.anchorAs = []*logrec.Anchor{a1, a2}
	return nil
}

func mustKey(d pushdrop.Derivation, identity *ec.PublicKey) *ec.PublicKey {
	k, err := d.ExpectedLockingKey(identity)
	if err != nil {
		panic(err)
	}
	return k
}

func (e *txEnv) anchorRecord(interval uint64, prev [32]byte, first, count uint64, ts uint64) *logrec.Anchor {
	cs := make([][32]byte, count)
	for i := range cs {
		cs[i] = carrier.Commitment(e.carriers[first-1+uint64(i)])
	}
	return &logrec.Anchor{Producer: e.producer.Compressed(), Stream: Stream, Interval: interval, Prev: prev,
		First: first, Count: count, Root: logrec.IntervalRoot(cs), Timestamp: ts}
}

// token builds an anchor token for a, spending prev (when not nil) and the
// fee coin's output feeVout, signed through the wallet.
func (e *txEnv) token(a *logrec.Anchor, prev *mint.Input, feeVout uint32) (*transaction.Transaction, error) {
	rec, err := a.Encode()
	if err != nil {
		return nil, err
	}
	return mint.Transition(e.lock(logrec.AnchorDerivation, logrec.TagAnchor, rec), 1, prev, e.fee(feeVout), e.feeLock, mint.DefaultFees)
}

// resign signs every input of tx again after a change to it.
func resign(tx *transaction.Transaction) *transaction.Transaction {
	if err := tx.Sign(); err != nil {
		panic(err)
	}
	return tx
}

// carrierFrom builds a carrier by hand: one input spending src:vout, the
// given outputs, the carrier's locktime and sequence, signed.
func carrierFrom(src *transaction.Transaction, vout uint32, u transaction.UnlockingScriptTemplate, outs ...*transaction.TransactionOutput) *transaction.Transaction {
	tx := transaction.NewTransaction()
	tx.LockTime = carrier.LockTime
	tx.AddInputFromTx(src, vout, u)
	tx.Inputs[0].SequenceNumber = carrier.Sequence
	for _, o := range outs {
		tx.AddOutput(o)
	}
	return resign(tx)
}

// derInt is a DER INTEGER's content for v.
func derInt(v *big.Int) []byte {
	b := v.Bytes()
	if len(b) == 0 || b[0]&0x80 != 0 {
		b = append([]byte{0}, b...)
	}
	return b
}

// highS rewrites a strict low-S DER signature to its high-S twin, which
// verifies all the same: S becomes n - S.
func highS(der []byte) []byte {
	sig, err := ec.ParseDERSignature(der)
	if err != nil {
		panic(err)
	}
	r, s := derInt(sig.R), derInt(new(big.Int).Sub(ec.S256().N, sig.S))
	body := append(append([]byte{0x02, byte(len(r))}, r...), append([]byte{0x02, byte(len(s))}, s...)...)
	return append([]byte{0x30, byte(len(body))}, body...)
}

// cloneTx copies tx, keeping each input's source transaction so the copy's
// BEEF carries the same ancestry.
func cloneTx(tx *transaction.Transaction) *transaction.Transaction {
	cp := &transaction.Transaction{Version: tx.Version, LockTime: tx.LockTime, MerklePath: tx.MerklePath}
	for _, in := range tx.Inputs {
		c := *in
		cp.Inputs = append(cp.Inputs, &c)
	}
	for _, o := range tx.Outputs {
		c := *o
		cp.Outputs = append(cp.Outputs, &c)
	}
	return cp
}

// rewriteUnlock replaces input i's unlocking script with f of its pushes.
func rewriteUnlock(tx *transaction.Transaction, i int, f func(sig []byte, rest [][]byte) []byte) *transaction.Transaction {
	cp := cloneTx(tx)
	ch, err := tx.Inputs[i].UnlockingScript.Chunks()
	if err != nil {
		panic(err)
	}
	var rest [][]byte
	for _, c := range ch[1:] {
		rest = append(rest, c.Data)
	}
	s := script.Script(f(ch[0].Data, rest))
	cp.Inputs[i].UnlockingScript = &s
	cp.MerklePath = nil
	return cp
}

func push(d []byte) []byte {
	s := &script.Script{}
	if err := s.AppendPushData(d); err != nil {
		panic(err)
	}
	return *s
}

// fieldSig rebuilds a signed PushDrop script with its field signature
// replaced by f(signature), canonically otherwise.
func fieldSig(s *script.Script, key *ec.PublicKey, nfields int, f func([]byte) []byte) *script.Script {
	ch, err := s.Chunks()
	if err != nil {
		panic(err)
	}
	var fields [][]byte
	for _, c := range ch[2 : 2+nfields] {
		fields = append(fields, c.Data)
	}
	out := script.Script(logrec.PushDropScript(key, fields, f(ch[2+nfields].Data)))
	return &out
}

func out(sats uint64, s *script.Script) *transaction.TransactionOutput {
	return &transaction.TransactionOutput{Satoshis: sats, LockingScript: s}
}

type admitJSON struct {
	Kind          string `json:"kind"`
	OutputIndex   *int   `json:"outputIndex,omitempty"`
	Commitment    string `json:"commitment,omitempty"`
	Record        string `json:"record,omitempty"`
	Seq           uint64 `json:"seq,omitempty"`
	FundingTxid   string `json:"fundingTxid,omitempty"`
	FundingVout   *int   `json:"fundingVout,omitempty"`
	Interval      uint64 `json:"interval,omitempty"`
	Funding       []int  `json:"funding,omitempty"`
	TokenTxidHash string `json:"tokenTxid,omitempty"`
}

type txJSON struct {
	Name   string `json:"name"`
	Note   string `json:"note"`
	Rule   string `json:"rule,omitempty"`
	Policy any    `json:"policy"`
	Txid   string `json:"txid"`
	RawTx  string `json:"rawTx"`
	BEEF   string `json:"beef"`
	// Scripts says whether every input of the transaction itself passes
	// the script interpreter: a refusal whose scripts verify is one only
	// the topic manager's rules catch.
	Scripts bool       `json:"scriptsVerify"`
	Verdict string     `json:"verdict"`
	Reason  string     `json:"reason,omitempty"`
	Admits  *admitJSON `json:"admits,omitempty"`
}

type txCase struct {
	name, note, rule string
	tx               *transaction.Transaction
	policy           logrec.Policy
	reason           string // "" admits
}

// beefOf is the BEEF a producer publishes. For an unmined transaction it is
// the usual one, the ancestry down to proven transactions. A mined
// transaction's BEEF would stop at its own proof, but admission reads the
// outputs its inputs spend (the anchor rules 7 and 9), so the BEEF of a
// mined transaction also carries the transactions it spends, each with
// its own proof or ancestry.
func beefOf(tx *transaction.Transaction) ([]byte, error) {
	if tx.MerklePath == nil {
		return tx.BEEF()
	}
	// Parents before children, each once, in input order: the order is
	// fixed so the bytes are.
	var order []*transaction.Transaction
	seen := map[chainhash.Hash]bool{}
	var visit func(t *transaction.Transaction, root bool)
	visit = func(t *transaction.Transaction, root bool) {
		id := *t.TxID()
		if seen[id] {
			return
		}
		if root || t.MerklePath == nil {
			for _, in := range t.Inputs {
				if in.SourceTransaction != nil {
					visit(in.SourceTransaction, false)
				}
			}
		}
		seen[id] = true
		order = append(order, t)
	}
	visit(tx, true)
	var bumps []*transaction.MerklePath
	at := map[uint32]int{}
	for _, t := range order {
		if t.MerklePath == nil {
			continue
		}
		if i, ok := at[t.MerklePath.BlockHeight]; ok {
			if err := bumps[i].Combine(t.MerklePath); err != nil {
				return nil, err
			}
			continue
		}
		at[t.MerklePath.BlockHeight] = len(bumps)
		bumps = append(bumps, t.MerklePath)
	}
	b := binary.LittleEndian.AppendUint32(nil, transaction.BEEF_V1)
	b = append(b, util.VarInt(len(bumps)).Bytes()...)
	for _, m := range bumps {
		b = append(b, m.Bytes()...)
	}
	b = append(b, util.VarInt(len(order)).Bytes()...)
	for _, t := range order {
		b = append(b, t.Bytes()...)
		if t.MerklePath == nil {
			b = append(b, 0)
			continue
		}
		b = append(b, 1)
		b = append(b, util.VarInt(at[t.MerklePath.BlockHeight]).Bytes()...)
	}
	return b, nil
}

// scriptsVerify runs the interpreter over tx's own inputs.
func scriptsVerify(ctx context.Context, tx *transaction.Transaction) bool {
	cp := tx.ShallowClone()
	cp.MerklePath = nil
	ok, err := spv.Verify(ctx, cp, &spv.GullibleHeadersClient{}, nil)
	return err == nil && ok
}

func (e *txEnv) admitJSON(a *logrec.Admission) *admitJSON {
	switch a.Kind {
	case logrec.KindBatch:
		idx, fv := 0, int(a.Carrier.Funding.Index)
		return &admitJSON{Kind: "batch", OutputIndex: &idx, Commitment: h(a.Carrier.Commitment[:]),
			Record: h(a.Carrier.Record), Seq: a.Carrier.Batch.Seq, FundingTxid: h(a.Carrier.Funding.Txid[:]), FundingVout: &fv}
	case logrec.KindAnchor:
		idx := a.Token.OutputIndex
		return &admitJSON{Kind: "anchor", OutputIndex: &idx, Record: h(a.Token.Record), Interval: a.Token.Anchor.Interval,
			TokenTxidHash: h(a.Token.Txid[:])}
	}
	return &admitJSON{Kind: "none", Funding: a.Funding}
}

// transactionVectors builds every transaction case and admits it, and
// fails unless each is admitted or refused exactly as the case says.
func transactionVectors() (map[string]any, error) {
	e, err := newTxEnv()
	if err != nil {
		return nil, err
	}
	open := logrec.Policy{Open: true}
	closed := logrec.Policy{Producers: [][]byte{e.other}}
	bkey := mustKey(logrec.BatchDerivation, e.producer)
	akey := mustKey(logrec.AnchorDerivation, e.producer)
	k1 := e.carriers[0]
	rec1 := e.records[0]
	lock1 := k1.Outputs[0].LockingScript
	sig1 := func() []byte { ch, _ := lock1.Chunks(); return ch[3].Data }()
	t1, t2 := e.anchors[0], e.anchors[1]
	a1rec, _ := e.anchorAs[0].Encode()
	a2rec, _ := e.anchorAs[1].Encode()

	var cases []txCase
	add := func(c txCase) { cases = append(cases, c) }

	// Admitted.
	for i, k := range e.carriers {
		add(txCase{fmt.Sprintf("carrier-seq-%d", i+1), []string{
			"a batch carrier: full-bytes lines, spending funding-tree output 0",
			"a batch carrier: full-bytes lines+gzip, spending funding-tree output 1; prev is carrier-seq-1's commitment",
			"a batch carrier: digest-only, spending funding-tree output 2; prev is carrier-seq-2's commitment",
		}[i], "", k, open, ""})
	}
	add(txCase{"anchor-interval-1", "the anchor token of interval 1 (seq 1 to 2), spending funding-tree output 3 and a fee coin; mined", "", t1, open, ""})
	add(txCase{"anchor-interval-2", "the anchor token of interval 2 (seq 3), spending interval 1's token and a fee coin; mined", "", t2, open, ""})
	add(txCase{"funding-tree", "a mined funding tree: outputs 0 to 3 are funding outputs and are admitted; output 4 is change", "", e.tree, open, ""})
	unminedTree := cloneTx(e.tree)
	unminedTree.MerklePath = nil
	add(txCase{"funding-tree-unmined", "the same funding tree with no proof: it claims nothing and admits nothing", "", unminedTree, open, ""})
	sweep, err := carrier.Sweep(e.ctx, e.w, originator, e.params, e.tree, []uint32{0, 1}, nil, 0, nil, e.feeLock, 1, 250)
	if err != nil {
		return nil, err
	}
	e.c.mine(sweep)
	add(txCase{"sweep", "a mined sweep of funding-tree outputs 0 and 1: its tombstone, output 0, is admitted as a funding output; it retracts carrier-seq-1 and carrier-seq-2", "", sweep, open, ""})
	add(txCase{"carrier-closed-policy-admits", "carrier-seq-1 under a closed policy that names its producer", "", k1,
		logrec.Policy{Producers: [][]byte{e.other, e.producer.Compressed()}}, ""})

	// Carrier shape.
	two := carrierFrom(e.tree, 0, e.batchUnlocker(), out(1000, lock1))
	two.AddInputFromTx(e.tree, 1, e.batchUnlocker())
	add(txCase{"carrier-two-inputs", "carrier-seq-1's output with a second funding input", "carrier shape", resign(two), open, "carrier-shape"})
	add(txCase{"carrier-two-outputs", "carrier-seq-1's output with 999 and a second output of 1", "carrier shape",
		carrierFrom(e.tree, 0, e.batchUnlocker(), out(999, lock1), out(1, e.feeLock)), open, "carrier-shape"})
	add(txCase{"carrier-value-short", "the one output carries 999 of the input's 1000", "carrier shape",
		carrierFrom(e.tree, 0, e.batchUnlocker(), out(999, lock1)), open, "carrier-shape"})
	add(txCase{"carrier-two-batch-outputs", "two outputs claim a batch", "carrier shape",
		carrierFrom(e.tree, 0, e.batchUnlocker(), out(500, lock1), out(500, e.carriers[1].Outputs[0].LockingScript)), open, "carrier-shape"})
	add(txCase{"carrier-batch-and-anchor-outputs", "one output claims a batch and another an anchor", "carrier shape",
		carrierFrom(e.tree, 0, e.batchUnlocker(), out(999, lock1), out(1, t1.Outputs[0].LockingScript)), open, "carrier-shape"})

	// The record, the stream and the policy.
	badRec := enc1(set(validBatchMap(e.producer.Compressed()), 3, uint64(0)))
	add(txCase{"carrier-record-refused", "the record has seq 0; the record's own rules refuse it before any transaction rule", "",
		carrierFrom(e.tree, 0, e.batchUnlocker(), out(1000, e.lock(logrec.BatchDerivation, badRec))), open, "range"})
	otherRec := enc1(set(validBatchMap(e.producer.Compressed()), 2, OtherStream))
	add(txCase{"carrier-other-stream", "a well-formed carrier whose record names another stream than the topic's", "",
		carrierFrom(e.tree, 0, e.batchUnlocker(), out(1000, e.lock(logrec.BatchDerivation, otherRec))), open, "stream"})
	add(txCase{"carrier-producer-policy", "carrier-seq-1 under a closed policy that does not name its producer", "", k1, closed, "producer-policy"})

	// Mineability.
	lowLock := carrierFrom(e.tree, 0, e.batchUnlocker(), out(1000, lock1))
	lowLock.LockTime = carrier.LockTime - 1
	add(txCase{"carrier-locktime-below", "nLockTime one second before 2100", "carrier shape", resign(lowLock), open, "mineable"})
	final := carrierFrom(e.tree, 0, e.batchUnlocker(), out(1000, lock1))
	final.Inputs[0].SequenceNumber = transaction.MaxTxInSequenceNum
	add(txCase{"carrier-final-input", "the input's nSequence is 0xFFFFFFFF", "carrier shape", resign(final), open, "mineable"})

	// Script canonicality.
	add(txCase{"carrier-lock-anchor-key", "the record output locked and signed under the anchor key", "script canonicality",
		carrierFrom(e.tree, 0, e.batchUnlocker(), out(1000, e.lock(logrec.AnchorDerivation, rec1))), open, "lock"})
	if len(rec1) <= 255 || len(rec1) > 65535 {
		return nil, fmt.Errorf("carrier-seq-1's record is %d bytes; the non-minimal push case assumes OP_PUSHDATA2", len(rec1))
	}
	wide := script.Script(append(append(append([]byte{33}, bkey.Compressed()...), script.OpCHECKSIG, script.OpPUSHDATA4, byte(len(rec1)), byte(len(rec1)>>8), 0, 0),
		append(append(append([]byte{}, rec1...), push(sig1)...), script.Op2DROP)...))
	add(txCase{"carrier-lock-non-minimal-push", "the record pushed with OP_PUSHDATA4 where OP_PUSHDATA2 is minimal; same fields, same signature", "script canonicality",
		carrierFrom(e.tree, 0, e.batchUnlocker(), out(1000, &wide)), open, "lock"})
	dd := script.Script(append(append([]byte{}, (*lock1)[:len(*lock1)-1]...), script.OpDROP, script.OpDROP))
	add(txCase{"carrier-lock-drop-drop", "OP_DROP OP_DROP where OP_2DROP is the fewest", "script canonicality",
		carrierFrom(e.tree, 0, e.batchUnlocker(), out(1000, &dd)), open, "lock"})
	unsigned := script.Script(logrec.PushDropScript(bkey, [][]byte{rec1}, nil))
	add(txCase{"carrier-lock-unsigned", "the record output carries no field signature", "script canonicality",
		carrierFrom(e.tree, 0, e.batchUnlocker(), out(1000, &unsigned)), open, "lock"})

	// Signatures.
	add(txCase{"carrier-field-signature-high-s", "the field signature's high-S twin, which verifies; the script is otherwise canonical", "signatures",
		carrierFrom(e.tree, 0, e.batchUnlocker(), out(1000, fieldSig(lock1, bkey, 1, highS))), open, "signature"})
	other := func([]byte) []byte {
		ok, _ := ec.PrivateKeyFromBytes(OtherKey)
		d := sha256.Sum256(rec1)
		s, _ := ok.Sign(d[:])
		return s.Serialize()
	}
	add(txCase{"carrier-field-signature-wrong-key", "a strict signature over the record by another key", "signatures",
		carrierFrom(e.tree, 0, e.batchUnlocker(), out(1000, fieldSig(lock1, bkey, 1, other))), open, "signature"})

	// Funding.
	add(txCase{"carrier-funding-p2pkh", "spends a fee-key P2PKH output instead of a funding output", "carrier shape",
		carrierFrom(e.misc, 1, e.feeUnlocker(), out(1000, lock1)), open, "funding"})
	add(txCase{"carrier-funding-anchor-key", "spends a funding-shaped output locked to the producer's anchor key", "carrier shape",
		carrierFrom(e.misc, 0, e.anchorUnlocker(), out(1000, lock1)), open, "funding"})

	// Unlocking.
	add(txCase{"carrier-unlock-high-s", "carrier-seq-1 with its input signature's high-S twin: another txid, same record", "signatures",
		rewriteUnlock(k1, 0, func(s []byte, _ [][]byte) []byte {
			return push(append(highS(s[:len(s)-1]), s[len(s)-1]))
		}), open, "unlock"})
	add(txCase{"carrier-unlock-pushdata1", "carrier-seq-1 with its signature pushed by OP_PUSHDATA1", "carrier shape",
		rewriteUnlock(k1, 0, func(s []byte, _ [][]byte) []byte {
			return append([]byte{script.OpPUSHDATA1, byte(len(s))}, s...)
		}), open, "unlock"})
	add(txCase{"carrier-unlock-extra-push", "carrier-seq-1 with OP_0 pushed before its signature", "carrier shape",
		rewriteUnlock(k1, 0, func(s []byte, _ [][]byte) []byte { return append([]byte{script.Op0}, push(s)...) }), open, "unlock"})

	// Anchor tokens. Each is mined unless the case is about that.
	mined := func(tx *transaction.Transaction) *transaction.Transaction { e.c.mine(tx); return tx }
	tokenWith := func(outs []*transaction.TransactionOutput, prev *mint.Input, feeVout uint32) *transaction.Transaction {
		tx := transaction.NewTransaction()
		if prev != nil {
			tx.AddInputFromTx(prev.Tx, prev.Vout, prev.Unlocker)
		}
		tx.AddInputFromTx(e.coin, feeVout, e.feeUnlocker())
		for _, o := range outs {
			tx.AddOutput(o)
		}
		tx.AddOutput(out(40000, e.feeLock))
		return mined(resign(tx))
	}
	treeIn := &mint.Input{Tx: e.tree, Vout: 3, Unlocker: e.batchUnlocker()}
	t1lock := t1.Outputs[0].LockingScript
	add(txCase{"anchor-two-token-outputs", "two outputs claim an anchor", "anchor token",
		tokenWith([]*transaction.TransactionOutput{out(1, t1lock), out(1, t1lock)}, treeIn, 1), open, "anchor-shape"})
	add(txCase{"anchor-two-satoshis", "the token output holds 2 satoshis", "anchor token",
		tokenWith([]*transaction.TransactionOutput{out(2, t1lock)}, treeIn, 1), open, "anchor-shape"})
	unsignedTok := script.Script(logrec.PushDropScript(akey, [][]byte{logrec.TagAnchor, a1rec}, nil))
	add(txCase{"anchor-unsigned", "the tag and the record with no field signature", "anchor token",
		tokenWith([]*transaction.TransactionOutput{out(1, &unsignedTok)}, treeIn, 1), open, "anchor-shape"})
	badA := enc1(set(validAnchorMap(e.producer.Compressed()), 6, uint64(0)))
	add(txCase{"anchor-record-refused", "the anchor record has count 0", "",
		tokenWith([]*transaction.TransactionOutput{out(1, e.lock(logrec.AnchorDerivation, logrec.TagAnchor, badA))}, treeIn, 1), open, "range"})
	otherA := *e.anchorAs[0]
	otherA.Stream = OtherStream
	otherArec, _ := otherA.Encode()
	add(txCase{"anchor-other-stream", "a well-formed token whose record names another stream", "",
		tokenWith([]*transaction.TransactionOutput{out(1, e.lock(logrec.AnchorDerivation, logrec.TagAnchor, otherArec))}, treeIn, 1), open, "stream"})
	add(txCase{"anchor-producer-policy", "anchor-interval-1 under a closed policy that does not name its producer", "", t1, closed, "producer-policy"})
	add(txCase{"anchor-lock-batch-key", "the token locked and signed under the batch key", "script canonicality",
		tokenWith([]*transaction.TransactionOutput{out(1, e.lock(logrec.BatchDerivation, logrec.TagAnchor, a1rec))}, treeIn, 1), open, "lock"})
	ch, _ := t1lock.Chunks()
	asig := ch[4].Data
	wideTok := script.Script(append(append(append([]byte{33}, akey.Compressed()...), script.OpCHECKSIG, script.OpPUSHDATA1, 3),
		append(append(append(append([]byte{}, logrec.TagAnchor...), push(a1rec)...), push(asig)...), script.Op2DROP, script.OpDROP)...))
	add(txCase{"anchor-lock-non-minimal-push", "the tag pushed with OP_PUSHDATA1 where a direct push is minimal", "script canonicality",
		tokenWith([]*transaction.TransactionOutput{out(1, &wideTok)}, treeIn, 1), open, "lock"})
	add(txCase{"anchor-field-signature-high-s", "the field signature's high-S twin", "signatures",
		tokenWith([]*transaction.TransactionOutput{out(1, fieldSig(t1lock, akey, 2, highS))}, treeIn, 1), open, "signature"})
	add(txCase{"anchor-interval-1-no-funding", "interval 1 spending only a fee coin", "anchor token",
		tokenWith([]*transaction.TransactionOutput{out(1, t1lock)}, nil, 1), open, "anchor-input"})
	add(txCase{"anchor-interval-2-spends-funding", "interval 2 naming interval 1's txid but spending a funding output instead of its token", "anchor token",
		tokenWith([]*transaction.TransactionOutput{out(1, t2.Outputs[0].LockingScript)}, treeIn, 2), open, "anchor-input"})
	a2x := *e.anchorAs[1]
	a2x.Prev = anchorTxid(1)
	a2xrec, _ := a2x.Encode()
	add(txCase{"anchor-interval-2-prev-mismatch", "interval 2 spending interval 1's token while prev names another txid", "anchor token",
		tokenWith([]*transaction.TransactionOutput{out(1, e.lock(logrec.AnchorDerivation, logrec.TagAnchor, a2xrec))},
			&mint.Input{Tx: t1, Vout: 0, Unlocker: e.anchorUnlocker()}, 2), open, "anchor-input"})
	a3 := *e.anchorAs[1]
	a3.Interval = 3
	a3rec, _ := a3.Encode()
	add(txCase{"anchor-interval-3-spends-interval-1", "interval 3 spending interval 1's token, skipping interval 2", "anchor token",
		tokenWith([]*transaction.TransactionOutput{out(1, e.lock(logrec.AnchorDerivation, logrec.TagAnchor, a3rec))},
			&mint.Input{Tx: t1, Vout: 0, Unlocker: e.anchorUnlocker()}, 2), open, "anchor-input"})
	unminedT1 := cloneTx(t1)
	unminedT1.MerklePath = nil
	add(txCase{"anchor-unmined", "anchor-interval-1 with no proof", "anchor token", unminedT1, open, "unmined"})
	strayT1 := cloneTx(t1)
	strayT1.MerklePath = transaction.NewMerklePath(unproven, t1.MerklePath.Path)
	add(txCase{"anchor-proof-unknown-height", "anchor-interval-1 with a proof at a height the headers do not hold", "anchor token", strayT1, open, "unmined"})
	add(txCase{"anchor-unlock-fee-high-s", "anchor-interval-1 with its fee input's signature rewritten to high S; mined as rewritten", "anchor token",
		mined(rewriteUnlock(t1, 1, func(s []byte, rest [][]byte) []byte {
			return append(push(append(highS(s[:len(s)-1]), s[len(s)-1])), push(rest[0])...)
		})), open, "unlock"})
	add(txCase{"anchor-unlock-token-pushdata1", "anchor-interval-2 with its token input's signature pushed by OP_PUSHDATA1; mined as rewritten", "anchor token",
		mined(rewriteUnlock(t2, 0, func(s []byte, _ [][]byte) []byte {
			return append([]byte{script.OpPUSHDATA1, byte(len(s))}, s...)
		})), open, "unlock"})
	odd := transaction.NewTransaction()
	odd.AddInputFromTx(e.tree, 3, e.batchUnlocker())
	odd.AddInputFromTx(e.coin, 1, e.feeUnlocker())
	odd.AddInput(&transaction.TransactionInput{SourceTXID: e.misc.TxID(), SourceTxOutIndex: 2, SourceTransaction: e.misc,
		UnlockingScript: &script.Script{0x02, 0xab, 0xcd}, SequenceNumber: transaction.MaxTxInSequenceNum})
	odd.AddOutput(out(1, t1lock))
	odd.AddOutput(out(40000, e.feeLock))
	add(txCase{"anchor-unlock-unknown-template", "anchor-interval-1's token with a third input spending an OP_1 output: push-only, but which pushes are signatures cannot be told from the spent script", "anchor token",
		mined(resign(odd)), open, "unlock"})

	var outList []txJSON
	for _, c := range cases {
		a, err := logrec.Admit(e.ctx, c.tx, logrec.Host{Stream: Stream, Policy: c.policy, Headers: e.c.headers})
		j := txJSON{Name: c.name, Note: c.note, Rule: c.rule, Txid: c.tx.TxID().String(), RawTx: c.tx.Hex(), Scripts: scriptsVerify(e.ctx, c.tx)}
		if c.policy.Open {
			j.Policy = "open"
		} else {
			var ks []string
			for _, k := range c.policy.Producers {
				ks = append(ks, h(k))
			}
			j.Policy = ks
		}
		beef, berr := beefOf(c.tx)
		if berr != nil {
			return nil, fmt.Errorf("%s: beef: %w", c.name, berr)
		}
		j.BEEF = h(beef)
		back, berr := transaction.NewTransactionFromBEEF(beef)
		if berr != nil || back.TxID().String() != j.Txid {
			return nil, fmt.Errorf("%s: the BEEF does not read back: %v", c.name, berr)
		}
		switch {
		case c.reason == "" && err != nil:
			return nil, fmt.Errorf("%s: refused: %w", c.name, err)
		case c.reason != "" && err == nil:
			return nil, fmt.Errorf("%s: admitted, want %s", c.name, c.reason)
		case c.reason != "" && logrec.Reason(err) != c.reason:
			return nil, fmt.Errorf("%s: reason %s, want %s (%v)", c.name, logrec.Reason(err), c.reason, err)
		case err == nil:
			j.Verdict = "admit"
			j.Admits = e.admitJSON(a)
		default:
			j.Verdict = "refuse"
			j.Reason = c.reason
		}
		outList = append(outList, j)
	}
	var headers []map[string]any
	for ht := uint32(801); ht <= e.c.height; ht++ {
		headers = append(headers, map[string]any{"height": ht, "merkleRoot": e.c.headers[ht].String()})
	}
	return map[string]any{
		"description":      "Transactions admitted or refused by the topic manager's rules (docs/spec.md section 8.1), in the order given there. Each case is the transaction as raw hex and as BEEF with its ancestry; the BEEF of a mined transaction also carries the transactions its inputs spend, because the anchor rules read their outputs; admission runs against the headers below, the topic's stream, and the case's producer policy. scriptsVerify says whether the transaction's own inputs pass the script interpreter: the refusals where it is true are spends anyone could make without the key, which only these rules catch. rule names the frozen-list row a refusal exercises. Signatures are deterministic (RFC 6979), so the bytes are reproducible.",
		"privateKey":       h(FixedKey),
		"identityKey":      h(e.producer.Compressed()),
		"otherIdentityKey": h(e.other),
		"originator":       originator,
		"batchLockingKey":  h(bkey.Compressed()),
		"anchorLockingKey": h(akey.Compressed()),
		"fundKey":          h(e.feeKey.PubKey().Compressed()),
		"fundingScript":    h(logrec.FundingScript(bkey)),
		"stream":           Stream,
		"topic":            logrec.TopicPrefix + Stream,
		"headersNote":      "merkleRoot is in display byte order, as a txid is printed",
		"headers":          headers,
		"anchorRecords":    []string{h(a1rec), h(a2rec)},
		"transactions":     outList,
	}, nil
}
