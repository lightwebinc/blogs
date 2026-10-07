package produce

import (
	"encoding/hex"
	"errors"
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bcommon/funding"
	"github.com/lightwebinc/bcommon/guard"
	"github.com/lightwebinc/bcommon/mint"

	"github.com/lightwebinc/blogs/internal/state"
)

func fill(b byte) chainhash.Hash {
	var h chainhash.Hash
	for i := range h {
		h[i] = b
	}
	return h
}

func proofAt(txid *chainhash.Hash, height uint32) *transaction.MerklePath {
	sibling := fill(0x33)
	isTxid := true
	return transaction.NewMerklePath(height, [][]*transaction.PathElement{{
		{Offset: 0, Hash: &sibling},
		{Offset: 1, Hash: txid, Txid: &isTxid},
	}})
}

// spender is a transaction spending output 0 of parent.
func spender(parent *transaction.Transaction) *transaction.Transaction {
	tx := transaction.NewTransaction()
	tx.AddInputFromTx(parent, 0, nil)
	tx.Inputs[0].UnlockingScript = &script.Script{script.OpTRUE}
	tx.AddOutput(&transaction.TransactionOutput{Satoshis: 900, LockingScript: &script.Script{script.OpTRUE}})
	return tx
}

// A parent of one input spending nothing real, proven at height 90.
func realParent() *transaction.Transaction {
	p := transaction.NewTransaction()
	nowhere := fill(0x51)
	p.AddInput(&transaction.TransactionInput{SourceTXID: &nowhere, UnlockingScript: &script.Script{},
		SequenceNumber: transaction.MaxTxInSequenceNum})
	p.AddOutput(&transaction.TransactionOutput{Satoshis: 1000, LockingScript: &script.Script{script.OpTRUE}})
	p.MerklePath = proofAt(p.TxID(), 90)
	return p
}

// The stand-in of no inputs an earlier build wrote for a coinbase fee
// coin's parent.
func placeholderParent() *transaction.Transaction {
	p := transaction.NewTransaction()
	p.AddOutput(&transaction.TransactionOutput{Satoshis: 1000, LockingScript: &script.Script{script.OpTRUE}})
	return p
}

func TestRebuildSweepUnminedAndMined(t *testing.T) {
	tx := spender(realParent())
	beef, err := funding.KeepBEEF(tx, nil)
	if err != nil {
		t.Fatal(err)
	}
	sw := &state.Sweep{Txid: tx.TxID().String(), RawHex: tx.Hex(), BeefHex: beef}
	back, err := rebuildSweep(sw)
	if err != nil {
		t.Fatalf("unmined: %v", err)
	}
	if back.Inputs[0].SourceTransaction == nil {
		t.Fatal("unmined: the fee input's parent was not carried")
	}

	mp := proofAt(tx.TxID(), 101)
	sw = &state.Sweep{Txid: tx.TxID().String(), RawHex: tx.Hex(), BumpHex: mp.Hex(), Submitted: true, Done: true}
	back, err = rebuildSweep(sw)
	if err != nil || back.MerklePath == nil {
		t.Fatalf("mined: %v", err)
	}

	sw.Txid = fill(0x77).String()
	if _, err := rebuildSweep(sw); err == nil {
		t.Fatal("a record holding another transaction was accepted")
	}
}

// A sweep kept by an earlier build with a placeholder parent in its BEEF:
// submitted, it is read from its raw bytes; not yet submitted, it is
// refused, since settling it needs the ancestry.
func TestRebuildSweepLegacyPlaceholder(t *testing.T) {
	tx := spender(placeholderParent())
	b, err := tx.AtomicBEEF(false)
	if err != nil {
		t.Fatal(err)
	}
	legacy := hex.EncodeToString(b)
	if _, err := funding.Rebuild(tx.Hex(), "", legacy); !errors.Is(err, guard.ErrBEEF) {
		t.Fatalf("the guard should refuse the legacy BEEF: %v", err)
	}

	sw := &state.Sweep{Txid: tx.TxID().String(), RawHex: tx.Hex(), BeefHex: legacy, Submitted: true}
	back, err := rebuildSweep(sw)
	if err != nil {
		t.Fatalf("submitted legacy sweep: %v", err)
	}
	if back.TxID().String() != sw.Txid {
		t.Fatalf("read back %s", back.TxID())
	}

	sw.Submitted = false
	if _, err := rebuildSweep(sw); !errors.Is(err, guard.ErrBEEF) {
		t.Fatalf("an unsubmitted legacy sweep was not refused: %v", err)
	}

	sw.Submitted, sw.RawHex = true, spender(realParent()).Hex()
	if _, err := rebuildSweep(sw); err == nil {
		t.Fatal("raw bytes of another transaction were accepted")
	}
}

// fixedUnlock signs with a fixed 107-byte script, the size of a P2PKH
// signature and key.
type fixedUnlock struct{}

func (fixedUnlock) Sign(*transaction.Transaction, uint32) (*script.Script, error) {
	s := script.Script(make([]byte, 107))
	return &s, nil
}

func (fixedUnlock) EstimateLength(*transaction.Transaction, uint32) uint32 { return 107 }

// A retirement pays what the fee policy asks for its signed size, at a
// rate below a satoshi a byte as at a whole one, and at least the floor;
// a fee over the policy's maximum is refused.
func TestRetirementFeeFollowsThePolicy(t *testing.T) {
	tree := transaction.NewTransaction()
	nowhere := fill(0x52)
	tree.AddInput(&transaction.TransactionInput{SourceTXID: &nowhere, UnlockingScript: &script.Script{}, SequenceNumber: transaction.MaxTxInSequenceNum})
	var vouts []uint32
	for i := range 600 {
		tree.AddOutput(&transaction.TransactionOutput{Satoshis: 1000, LockingScript: &script.Script{script.OpTRUE}})
		vouts = append(vouts, uint32(i))
	}
	pay := &script.Script{script.OpTRUE}
	for _, tc := range []struct {
		name  string
		fees  mint.Fees
		vouts []uint32
	}{
		{"legacy, small", mint.LegacyFees, vouts[:2]},
		{"legacy, large", mint.LegacyFees, vouts},
		{"default, small", mint.DefaultFees, vouts[:2]},
		{"default, large", mint.DefaultFees, vouts},
		{"network, large", mint.NetworkFees, vouts},
	} {
		tx, err := retireTx(tree, tc.vouts, fixedUnlock{}, pay, tc.fees)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		fee := uint64(len(tc.vouts))*1000 - tx.Outputs[0].Satoshis
		lo, _ := tc.fees.For(tx.Size())
		hi, _ := tc.fees.For(tx.Size() + 2*len(tx.Inputs))
		if fee < lo || fee > hi {
			t.Errorf("%s: fee %d for %d bytes, want %d to %d", tc.name, fee, tx.Size(), lo, hi)
		}
	}
	capped := mint.NetworkFees
	capped.Max = 100
	if _, err := retireTx(tree, vouts, fixedUnlock{}, pay, capped); !errors.Is(err, mint.ErrFeeTooHigh) {
		t.Fatalf("over the maximum: %v", err)
	}
}
