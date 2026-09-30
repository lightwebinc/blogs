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
