package logrec

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/go-sdk/transaction/chaintracker"

	"github.com/lightwebinc/bcommon/carrier"
)

// The transaction refusals of docs/spec.md section 8.1, beside the record
// refusals in logrec.go. A transaction is refused for the first rule it
// breaks. A carrier or token whose record's stream is not the topic's is
// refused with ErrStream, the same label as a record whose stream breaks
// the grammar.
var (
	ErrCarrierShape   = errors.New("logrec: carrier has the wrong shape")
	ErrAnchorShape    = errors.New("logrec: anchor token has the wrong shape")
	ErrProducerPolicy = errors.New("logrec: producer is not in the stream's producer set")
	ErrMineable       = errors.New("logrec: carrier is mineable")
	ErrLock           = errors.New("logrec: output script is not the canonical one for the producer")
	ErrSignature      = errors.New("logrec: field signature is not strict or does not verify")
	ErrFunding        = errors.New("logrec: carrier does not spend a funding output of the producer")
	ErrUnlock         = errors.New("logrec: unlocking script is not in canonical form")
	ErrAnchorInput    = errors.New("logrec: anchor token does not spend its predecessor or a funding output")
	ErrUnmined        = errors.New("logrec: transaction has no proof that verifies")
)

// Policy is a host's producer policy for one stream. Openness is stated,
// never inferred: a closed policy with no producers is a configuration
// error, not an open stream.
type Policy struct {
	// Open admits any producer.
	Open bool
	// Producers is the set a closed stream admits, as compressed keys.
	Producers [][]byte
}

// ErrPolicy is a policy that is neither open nor names a producer, or is
// both.
var ErrPolicy = errors.New("logrec: a policy is open or names producers, exactly one")

// Validate refuses a policy that does not say which it is.
func (p Policy) Validate() error {
	if p.Open == (len(p.Producers) > 0) {
		return ErrPolicy
	}
	return nil
}

func (p Policy) admits(producer []byte) bool {
	if p.Open {
		return true
	}
	for _, k := range p.Producers {
		if bytes.Equal(k, producer) {
			return true
		}
	}
	return false
}

// Kind is what a transaction's outputs claim to be.
type Kind int

const (
	// KindNone claims nothing: a funding tree, a sweep or a spend.
	KindNone Kind = iota
	// KindBatch has an output that claims a batch.
	KindBatch
	// KindAnchor has an output that claims an anchor.
	KindAnchor
)

// Classify finds the one claiming output. An output claims a batch when it
// is a PushDrop whose first field starts with a CBOR map head, key 0 and
// the batch magic; it claims an anchor when its first field is 62 6c 01.
// More than one claiming output refuses the transaction: carrier-shape when
// any of them claims a batch, else anchor-shape.
func Classify(tx *transaction.Transaction) (Kind, int, error) {
	kind, at, batches, anchors := KindNone, -1, 0, 0
	for i, out := range tx.Outputs {
		if out == nil || out.LockingScript == nil {
			continue
		}
		p, ok := readPushDrop(*out.LockingScript)
		if !ok {
			continue
		}
		switch {
		case IsBatch(p.fields[0]):
			batches++
			kind, at = KindBatch, i
		case bytes.Equal(p.fields[0], TagAnchor):
			anchors++
			if kind != KindBatch {
				kind, at = KindAnchor, i
			}
		}
	}
	switch {
	case batches > 0 && batches+anchors > 1:
		return KindBatch, -1, fmt.Errorf("%w: %d claiming outputs", ErrCarrierShape, batches+anchors)
	case anchors > 1:
		return KindAnchor, -1, fmt.Errorf("%w: %d claiming outputs", ErrAnchorShape, anchors)
	}
	return kind, at, nil
}

// Carrier is an admitted batch carrier.
type Carrier struct {
	Batch *Batch
	// Record is the batch record exactly as carried.
	Record []byte
	// Commitment is C = txid(K), hash byte order.
	Commitment [32]byte
	// Funding is the outpoint the carrier spends; a mined spend of it by
	// another transaction retracts the batch.
	Funding transaction.Outpoint
}

// Token is an admitted anchor token.
type Token struct {
	Anchor *Anchor
	// Record is the anchor record exactly as carried.
	Record []byte
	// OutputIndex is the anchor output.
	OutputIndex int
	// Txid is the token transaction's txid, hash byte order: what the next
	// interval's prev names.
	Txid [32]byte
}

// Host is what admission needs from the host beyond the object: the
// topic's stream, its producer policy and a chain tracker over the host's
// own block headers.
type Host struct {
	Stream  string
	Policy  Policy
	Headers chaintracker.ChainTracker
}

// Admission is what a host admits from one transaction: a carrier, a
// token, or funding outputs. A transaction that claims nothing and admits
// no funding output has all three empty; whether it spends something the
// topic holds is the host's question, not the codec's.
type Admission struct {
	Kind    Kind
	Carrier *Carrier
	Token   *Token
	// Funding lists the admitted funding outputs of a mined funding tree or
	// sweep.
	Funding []int
}

// Admit applies docs/spec.md section 8.1 to tx, whose inputs carry their
// source transactions from the BEEF. SPV of the BEEF is the engine's and is
// assumed to have run; Admit checks what the topic manager checks, in the
// order given, and refuses for the first rule broken.
func Admit(ctx context.Context, tx *transaction.Transaction, h Host) (*Admission, error) {
	if err := h.Policy.Validate(); err != nil {
		return nil, err
	}
	if CheckStream(h.Stream) != nil {
		return nil, fmt.Errorf("logrec: host stream %q breaks the grammar", h.Stream)
	}
	kind, at, err := Classify(tx)
	if err != nil {
		return nil, err
	}
	switch kind {
	case KindBatch:
		c, err := checkCarrier(tx, at, h)
		if err != nil {
			return nil, err
		}
		return &Admission{Kind: kind, Carrier: c}, nil
	case KindAnchor:
		t, err := checkToken(ctx, tx, at, h)
		if err != nil {
			return nil, err
		}
		return &Admission{Kind: kind, Token: t}, nil
	}
	return &Admission{Kind: KindNone, Funding: fundingOutputs(ctx, tx, h.Headers)}, nil
}

func producerKey(k []byte) *ec.PublicKey {
	pub, err := ec.PublicKeyFromBytes(k)
	if err != nil {
		// The record's producer rule already held it to a canonical key.
		panic(err)
	}
	return pub
}

// checkCarrier is the batch carrier's rules 1 to 9.
func checkCarrier(tx *transaction.Transaction, at int, h Host) (*Carrier, error) {
	// 1. carrier-shape.
	if len(tx.Inputs) != 1 || len(tx.Outputs) != 1 {
		return nil, fmt.Errorf("%w: %d inputs, %d outputs", ErrCarrierShape, len(tx.Inputs), len(tx.Outputs))
	}
	in, out := tx.Inputs[0], tx.Outputs[0]
	src := in.SourceTxOutput()
	if src == nil || src.LockingScript == nil || in.SourceTXID == nil {
		return nil, fmt.Errorf("%w: the spent output is not in the BEEF", ErrCarrierShape)
	}
	if src.Satoshis != out.Satoshis {
		return nil, fmt.Errorf("%w: output %d of input %d", ErrCarrierShape, out.Satoshis, src.Satoshis)
	}
	pd, _ := readPushDrop(*out.LockingScript)
	record := pd.fields[0]
	// 2. the record's own rules.
	b, err := DecodeBatch(record)
	if err != nil {
		return nil, err
	}
	// 3. stream; 4. producer-policy.
	if b.Stream != h.Stream {
		return nil, fmt.Errorf("%w: not this topic's stream", ErrStream)
	}
	if !h.Policy.admits(b.Producer) {
		return nil, ErrProducerPolicy
	}
	// 5. mineable.
	if tx.LockTime < carrier.LockTime || in.SequenceNumber == transaction.MaxTxInSequenceNum {
		return nil, ErrMineable
	}
	// 6. lock.
	key, err := BatchDerivation.ExpectedLockingKey(producerKey(b.Producer))
	if err != nil {
		return nil, err
	}
	if len(pd.fields) != 2 {
		return nil, fmt.Errorf("%w: %d pushes, want the record and a signature", ErrLock, len(pd.fields))
	}
	sig := pd.fields[1]
	if !bytes.Equal(*out.LockingScript, PushDropScript(key, [][]byte{record}, sig)) {
		return nil, ErrLock
	}
	// 7. signature.
	if !verifyField(key, record, sig) {
		return nil, ErrSignature
	}
	// 8. funding.
	if !bytes.Equal(*src.LockingScript, FundingScript(key)) {
		return nil, ErrFunding
	}
	// 9. unlock, bcommon's check.
	if err := carrier.CheckUnlocking(tx); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrUnlock, err)
	}
	return &Carrier{
		Batch: b, Record: record, Commitment: carrier.Commitment(tx),
		Funding: transaction.Outpoint{Txid: *in.SourceTXID, Index: in.SourceTxOutIndex},
	}, nil
}

// verifyField checks a field signature: strict low-S DER, over
// SHA-256(fields concatenated), under key.
func verifyField(key *ec.PublicKey, signed, der []byte) bool {
	if !StrictSignature(der) {
		return false
	}
	s, err := ec.ParseDERSignature(der)
	if err != nil {
		return false
	}
	d := sha256.Sum256(signed)
	return s.Verify(d[:], key)
}

// readToken reads s as a well-formed anchor token: a PushDrop of the tag, a
// record that decodes, and a signature, whose script is the one rebuilt
// under the record producer's anchor key. It does not verify the signature.
func readToken(s []byte) (*Anchor, []byte, []byte, *ec.PublicKey, error) {
	pd, ok := readPushDrop(s)
	if !ok || !bytes.Equal(pd.fields[0], TagAnchor) || len(pd.fields) != 3 {
		return nil, nil, nil, nil, ErrAnchorShape
	}
	a, err := DecodeAnchor(pd.fields[1])
	if err != nil {
		return nil, nil, nil, nil, err
	}
	key, err := AnchorDerivation.ExpectedLockingKey(producerKey(a.Producer))
	if err != nil {
		return nil, nil, nil, nil, err
	}
	if !bytes.Equal(s, PushDropScript(key, [][]byte{TagAnchor, pd.fields[1]}, pd.fields[2])) {
		return a, pd.fields[1], pd.fields[2], key, ErrLock
	}
	return a, pd.fields[1], pd.fields[2], key, nil
}

// checkToken is the anchor token's rules 1 to 9.
func checkToken(ctx context.Context, tx *transaction.Transaction, at int, h Host) (*Token, error) {
	out := tx.Outputs[at]
	pd, _ := readPushDrop(*out.LockingScript)
	// 1. anchor-shape.
	if len(pd.fields) != 3 {
		return nil, fmt.Errorf("%w: %d pushes, want the tag, the record and a signature", ErrAnchorShape, len(pd.fields))
	}
	if out.Satoshis != 1 {
		return nil, fmt.Errorf("%w: %d satoshis", ErrAnchorShape, out.Satoshis)
	}
	record, sig := pd.fields[1], pd.fields[2]
	// 2. the record's own rules.
	a, err := DecodeAnchor(record)
	if err != nil {
		return nil, err
	}
	// 3. stream; 4. producer-policy.
	if a.Stream != h.Stream {
		return nil, fmt.Errorf("%w: not this topic's stream", ErrStream)
	}
	if !h.Policy.admits(a.Producer) {
		return nil, ErrProducerPolicy
	}
	// 5. lock.
	producer := producerKey(a.Producer)
	key, err := AnchorDerivation.ExpectedLockingKey(producer)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(*out.LockingScript, PushDropScript(key, [][]byte{TagAnchor, record}, sig)) {
		return nil, ErrLock
	}
	// 6. signature.
	if !verifyField(key, append(append([]byte{}, TagAnchor...), record...), sig) {
		return nil, ErrSignature
	}
	// 7. anchor-input.
	if !anchorInput(tx, a, producer) {
		return nil, ErrAnchorInput
	}
	// 8. unmined.
	if !Mined(ctx, tx, h.Headers) {
		return nil, ErrUnmined
	}
	// 9. unlock.
	for i, in := range tx.Inputs {
		src := in.SourceTxOutput()
		if in.UnlockingScript == nil || src == nil || src.LockingScript == nil ||
			!canonicalUnlock(*in.UnlockingScript, *src.LockingScript) {
			return nil, fmt.Errorf("%w: input %d", ErrUnlock, i)
		}
	}
	return &Token{Anchor: a, Record: record, OutputIndex: at, Txid: [32]byte(*tx.TxID())}, nil
}

// anchorInput is rule 7: interval k > 1 spends the anchor token of k - 1
// in transaction prev; interval 1 spends a funding output of the producer.
func anchorInput(tx *transaction.Transaction, a *Anchor, producer *ec.PublicKey) bool {
	var funding []byte
	if a.Interval == 1 {
		key, err := BatchDerivation.ExpectedLockingKey(producer)
		if err != nil {
			return false
		}
		funding = FundingScript(key)
	}
	for _, in := range tx.Inputs {
		src := in.SourceTxOutput()
		if src == nil || src.LockingScript == nil {
			continue
		}
		if a.Interval == 1 {
			if bytes.Equal(*src.LockingScript, funding) {
				return true
			}
			continue
		}
		if in.SourceTXID == nil || [32]byte(*in.SourceTXID) != a.Prev {
			continue
		}
		p, _, _, _, err := readToken(*src.LockingScript)
		if err == nil && bytes.Equal(p.Producer, a.Producer) && p.Stream == a.Stream && p.Interval == a.Interval-1 {
			return true
		}
	}
	return false
}

// Mined reports whether tx carries a proof that verifies against the
// headers. No headers means nothing is mined.
func Mined(ctx context.Context, tx *transaction.Transaction, headers chaintracker.ChainTracker) bool {
	if tx.MerklePath == nil || headers == nil {
		return false
	}
	txid := chainhash.Hash(*tx.TxID())
	ok, err := tx.MerklePath.Verify(ctx, &txid, headers)
	return err == nil && ok
}

// fundingOutputs is the funding tree and sweep rule: a transaction whose
// outputs claim nothing admits its well-formed funding outputs when it is
// mined, and nothing when it is not.
func fundingOutputs(ctx context.Context, tx *transaction.Transaction, headers chaintracker.ChainTracker) []int {
	var outs []int
	for i, o := range tx.Outputs {
		if o != nil && o.LockingScript != nil && IsFundingShape(*o.LockingScript) {
			outs = append(outs, i)
		}
	}
	if len(outs) == 0 || !Mined(ctx, tx, headers) {
		return nil
	}
	return outs
}
