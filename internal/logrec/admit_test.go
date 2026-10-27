package logrec

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"math/big"
	"testing"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/go-sdk/wallet"
)

func TestPolicyValidate(t *testing.T) {
	k := []byte{0x02}
	for _, c := range []struct {
		p  Policy
		ok bool
	}{
		{Policy{Open: true}, true},
		{Policy{Producers: [][]byte{k}}, true},
		{Policy{}, false}, // empty is a configuration error, never open
		{Policy{Open: true, Producers: [][]byte{k}}, false},
	} {
		if err := c.p.Validate(); (err == nil) != c.ok {
			t.Errorf("%+v: %v", c.p, err)
		}
	}
	if _, err := Admit(context.Background(), transaction.NewTransaction(), nil, Host{Stream: "example_app_qzxkvbmwtr"}); !errors.Is(err, ErrPolicy) {
		t.Fatalf("an unset policy admitted: %v", err)
	}
}

// TestPushDropScriptMatchesSDK: the rebuilt script is byte for byte what
// bcommon pushdrop writes through the wallet, for a short field, a field on
// each push-opcode boundary and a record past 65535 bytes.
func TestPushDropScriptMatchesSDK(t *testing.T) {
	priv, _ := ec.PrivateKeyFromBytes(bytes.Repeat([]byte{0x42}, 32))
	w, err := wallet.NewCompletedProtoWallet(priv)
	if err != nil {
		t.Fatal(err)
	}
	key, err := AnchorDerivation.ExpectedLockingKey(priv.PubKey())
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range []int{3, 75, 76, 255, 256, 65535, 65536, 70000} {
		f := bytes.Repeat([]byte{0xa5}, n)
		for _, sign := range []bool{false, true} {
			s, err := AnchorDerivation.Lock(context.Background(), w, "x", [][]byte{TagAnchor, f}, sign)
			if err != nil {
				t.Fatal(err)
			}
			var sig []byte
			if sign {
				p, ok := readPushDrop(*s)
				if !ok || len(p.fields) != 3 {
					t.Fatal("read back")
				}
				sig = p.fields[2]
			}
			if !bytes.Equal(*s, PushDropScript(key, [][]byte{TagAnchor, f}, sig)) {
				t.Errorf("field of %d bytes, signed %v: rebuilt script differs", n, sign)
			}
		}
	}
}

func TestParseScriptRefusesTruncation(t *testing.T) {
	for _, s := range [][]byte{
		{0x05, 1, 2},
		{script.OpPUSHDATA1},
		{script.OpPUSHDATA1, 3, 1},
		{script.OpPUSHDATA2, 0xff},
		{script.OpPUSHDATA4, 0xff, 0xff, 0xff, 0xff, 0},
	} {
		if _, ok := parseScript(s); ok {
			t.Errorf("%x parsed", s)
		}
	}
	// A truncated record output claims nothing, so the transaction is not
	// a carrier.
	k, _ := ec.PrivateKeyFromBytes(bytes.Repeat([]byte{0x42}, 32))
	s := script.Script(append(append([]byte{33}, k.PubKey().Compressed()...), script.OpCHECKSIG, 0x4c, 0xff, 0xa1, 0x00, 0x44))
	tx := transaction.NewTransaction()
	tx.AddOutput(&transaction.TransactionOutput{Satoshis: 1, LockingScript: &s})
	if kind, _, err := Classify(tx); kind != KindNone || err != nil {
		t.Fatalf("truncated script classified %v %v", kind, err)
	}
}

func lowSig(t *testing.T) []byte {
	k, _ := ec.PrivateKeyFromBytes(bytes.Repeat([]byte{0x42}, 32))
	d := sha256.Sum256([]byte("m"))
	s, err := k.Sign(d[:])
	if err != nil {
		t.Fatal(err)
	}
	return s.Serialize()
}

func TestStrictSignature(t *testing.T) {
	der := lowSig(t)
	if !StrictSignature(der) {
		t.Fatal("a go-sdk signature is not strict")
	}
	sig, _ := ec.ParseDERSignature(der)
	hs := new(big.Int).Sub(ec.S256().N, sig.S).Bytes()
	if hs[0]&0x80 != 0 {
		hs = append([]byte{0}, hs...)
	}
	r := sig.R.Bytes()
	if r[0]&0x80 != 0 {
		r = append([]byte{0}, r...)
	}
	body := append(append([]byte{2, byte(len(r))}, r...), append([]byte{2, byte(len(hs))}, hs...)...)
	high := append([]byte{0x30, byte(len(body))}, body...)
	padded := append([]byte{0x30, der[1] + 1, 0x02, der[3] + 1, 0x00}, der[4:]...)
	for name, b := range map[string][]byte{
		"high S": high, "R padded": padded, "trailing byte": append(append([]byte{}, der...), 0),
		"empty": {}, "wrong tag": append([]byte{0x31}, der[1:]...),
	} {
		if StrictSignature(b) {
			t.Errorf("%s accepted", name)
		}
	}
}

func TestIsFundingShape(t *testing.T) {
	k, _ := ec.PrivateKeyFromBytes(bytes.Repeat([]byte{0x42}, 32))
	f := FundingScript(k.PubKey())
	if !IsFundingShape(f) {
		t.Fatal("the funding script is not funding-shaped")
	}
	signed := PushDropScript(k.PubKey(), [][]byte{TagFunding}, lowSig(t))
	wrongTag := PushDropScript(k.PubKey(), [][]byte{TagAnchor}, nil)
	xIsP := append([]byte{}, f...)
	fieldP.FillBytes(xIsP[2:34])
	for name, s := range map[string][]byte{"signed": signed, "anchor tag": wrongTag, "x = p": xIsP, "empty": nil} {
		if IsFundingShape(s) {
			t.Errorf("%s is funding-shaped", name)
		}
	}
}

// FuzzAdmit: no transaction panics admission, and a refusal always has a
// reason label.
func FuzzAdmit(f *testing.F) {
	k, _ := ec.PrivateKeyFromBytes(bytes.Repeat([]byte{0x42}, 32))
	f.Add(PushDropScript(k.PubKey(), [][]byte{goodRecord(f)}, lowSigF(f)))
	f.Add(PushDropScript(k.PubKey(), [][]byte{TagAnchor, {0xa0}}, lowSigF(f)))
	f.Add(FundingScript(k.PubKey()))
	f.Fuzz(func(t *testing.T, s []byte) {
		src := transaction.NewTransaction()
		src.AddOutput(&transaction.TransactionOutput{Satoshis: 1, LockingScript: (*script.Script)(&s)})
		tx := transaction.NewTransaction()
		tx.LockTime = 4102444800
		tx.AddInputFromTx(src, 0, nil)
		tx.AddOutput(&transaction.TransactionOutput{Satoshis: 1, LockingScript: (*script.Script)(&s)})
		us := script.Script(s)
		tx.Inputs[0].UnlockingScript = &us
		_, err := Admit(context.Background(), tx, Coins{{Txid: *src.TxID(), Index: 0}: {LockingScript: s, Satoshis: 1}}, Host{Stream: "example_app_qzxkvbmwtr", Policy: Policy{Open: true}})
		if err != nil && Reason(err) == "other" {
			t.Fatalf("unlabeled refusal: %v", err)
		}
	})
}

func goodRecord(t testing.TB) []byte {
	b := goodBatch(t)
	b.Seq, b.Prev = 1, [32]byte{}
	enc, err := b.Encode()
	if err != nil {
		t.Fatal(err)
	}
	return enc
}

func lowSigF(f *testing.F) []byte {
	k, _ := ec.PrivateKeyFromBytes(bytes.Repeat([]byte{0x42}, 32))
	d := sha256.Sum256([]byte("m"))
	s, _ := k.Sign(d[:])
	return s.Serialize()
}
