package logrec

import (
	"bytes"
	"encoding/binary"
	"math/big"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-sdk/script"
	sdkpushdrop "github.com/bsv-blockchain/go-sdk/transaction/template/pushdrop"
)

// op is one parsed script element: an opcode, and for a data push the
// bytes pushed.
type op struct {
	code byte
	data []byte
	push bool // a data push (OP_0, a direct push or OP_PUSHDATA1/2/4)
}

// parseScript splits s into opcodes. A push whose declared length runs past
// the end refuses the whole script, so a truncated script claims nothing.
// The reader is this package's own rather than the SDK's, so that the Go
// and TypeScript codecs classify every byte string the same way.
func parseScript(s []byte) ([]op, bool) {
	var out []op
	for i := 0; i < len(s); {
		c := s[i]
		i++
		var n int
		switch {
		case c == script.Op0:
			out = append(out, op{code: c, data: []byte{}, push: true})
			continue
		case c >= 1 && c <= 75:
			n = int(c)
		case c == script.OpPUSHDATA1:
			if i+1 > len(s) {
				return nil, false
			}
			n = int(s[i])
			i++
		case c == script.OpPUSHDATA2:
			if i+2 > len(s) {
				return nil, false
			}
			n = int(binary.LittleEndian.Uint16(s[i:]))
			i += 2
		case c == script.OpPUSHDATA4:
			if i+4 > len(s) {
				return nil, false
			}
			m := uint64(binary.LittleEndian.Uint32(s[i:]))
			i += 4
			if m > uint64(len(s)-i) {
				return nil, false
			}
			n = int(m)
		default:
			out = append(out, op{code: c})
			continue
		}
		if n > len(s)-i {
			return nil, false
		}
		out = append(out, op{code: c, data: s[i : i+n], push: true})
		i += n
	}
	return out, true
}

// pushDrop is a lock-before PushDrop read leniently: a 33-byte push, then
// OP_CHECKSIG, then the data pushes up to the first opcode that is not one.
// Whether it is the canonical script is decided by rebuilding it
// (PushDropScript) and comparing bytes; this reading only classifies.
type pushDrop struct {
	key    []byte
	fields [][]byte // every push after OP_CHECKSIG, the signature included
}

func readPushDrop(s []byte) (*pushDrop, bool) {
	ops, ok := parseScript(s)
	if !ok || len(ops) < 3 {
		return nil, false
	}
	if !ops[0].push || len(ops[0].data) != 33 || (ops[0].data[0] != 0x02 && ops[0].data[0] != 0x03) {
		return nil, false
	}
	if ops[1].code != script.OpCHECKSIG {
		return nil, false
	}
	p := &pushDrop{key: ops[0].data}
	for _, o := range ops[2:] {
		if !o.push {
			break
		}
		p.fields = append(p.fields, o.data)
	}
	if len(p.fields) == 0 {
		return nil, false
	}
	return p, true
}

// PushDropScript is the one canonical lock-before PushDrop for key, fields
// and an optional field signature, exactly as bcommon pushdrop (through the
// SDK) writes it: the 33-byte compressed key, OP_CHECKSIG, each field and
// then the signature as one minimal push, and the fewest OP_2DROP then
// OP_DROP that clear them. Every script a host checks is compared with this
// byte for byte.
func PushDropScript(key *ec.PublicKey, fields [][]byte, sig []byte) []byte {
	ops := []*script.ScriptChunk{{Op: 33, Data: key.Compressed()}, {Op: script.OpCHECKSIG}}
	all := append(append([][]byte{}, fields...), sig)
	if sig == nil {
		all = all[:len(all)-1]
	}
	for _, f := range all {
		ops = append(ops, sdkpushdrop.CreateMinimallyEncodedScriptChunk(f))
	}
	for n := len(all); n > 0; n -= 2 {
		if n == 1 {
			ops = append(ops, &script.ScriptChunk{Op: script.OpDROP})
		} else {
			ops = append(ops, &script.ScriptChunk{Op: script.Op2DROP})
		}
	}
	s, err := script.NewScriptFromScriptOps(ops)
	if err != nil {
		// Every chunk above is well formed; this cannot happen.
		panic(err)
	}
	return *s
}

// FundingScript is a funding-tree output locked to key:
// <key> OP_CHECKSIG <62 6c 02> OP_DROP.
func FundingScript(key *ec.PublicKey) []byte {
	return PushDropScript(key, [][]byte{TagFunding}, nil)
}

// IsFundingShape reports whether s is a well-formed funding output under
// some canonical compressed key: exactly the funding script rebuilt from
// its own key. Whose key it is cannot be read from the script (the
// derivation is one way); a carrier's rule 8 compares with the producer's.
func IsFundingShape(s []byte) bool {
	if len(s) != 1+33+1+1+3+1 || s[0] != 33 || CheckProducer(s[1:34]) != nil {
		return false
	}
	k, err := ec.PublicKeyFromBytes(s[1:34])
	if err != nil {
		return false
	}
	return bytes.Equal(s, FundingScript(k))
}

// curveN is the order of secp256k1's group.
var curveN = ec.S256().N

// StrictSignature reports whether der is a strict DER ECDSA signature
// (BIP 66) with R in [1, n-1] and S in [1, n/2]: the one encoding of a
// signature a host accepts, so nobody can re-encode it into another byte
// string that still verifies.
func StrictSignature(der []byte) bool {
	if len(der) < 8 || len(der) > 72 || der[0] != 0x30 || int(der[1]) != len(der)-2 {
		return false
	}
	lenR := int(der[3])
	if der[2] != 0x02 || lenR == 0 || 6+lenR > len(der) {
		return false
	}
	lenS := int(der[5+lenR])
	if der[4+lenR] != 0x02 || lenS == 0 || 6+lenR+lenS != len(der) {
		return false
	}
	r, s := der[4:4+lenR], der[6+lenR:]
	for _, v := range [][]byte{r, s} {
		if v[0]&0x80 != 0 || (len(v) > 1 && v[0] == 0 && v[1]&0x80 == 0) {
			return false
		}
	}
	R, S := new(big.Int).SetBytes(r), new(big.Int).SetBytes(s)
	half := new(big.Int).Rsh(curveN, 1)
	return R.Sign() > 0 && R.Cmp(curveN) < 0 && S.Sign() > 0 && S.Cmp(half) <= 0
}

// minimalPush reports whether o pushes its data with the shortest opcode:
// OP_0 for nothing, OP_1 to OP_16 or OP_1NEGATE for the one-byte values
// they stand for, a direct push up to 75 bytes, then OP_PUSHDATA1, 2 and 4.
func minimalPush(o op) bool {
	n := len(o.data)
	switch {
	case n == 0:
		return o.code == script.Op0
	case n == 1 && (o.data[0] >= 1 && o.data[0] <= 16 || o.data[0] == 0x81):
		return false // OP_1 to OP_16 or OP_1NEGATE would be shorter
	case n <= 75:
		return int(o.code) == n
	case n <= 255:
		return o.code == script.OpPUSHDATA1
	case n <= 65535:
		return o.code == script.OpPUSHDATA2
	default:
		return o.code == script.OpPUSHDATA4
	}
}

// sighashTypes are the defined BSV sighash bytes: ALL, NONE or SINGLE, with
// FORKID, with or without ANYONECANPAY.
func sighashType(b byte) bool {
	switch b {
	case 0x41, 0x42, 0x43, 0xc1, 0xc2, 0xc3:
		return true
	}
	return false
}

// signaturePush reports whether d is a strict low-S DER signature followed
// by a defined sighash byte.
func signaturePush(d []byte) bool {
	return len(d) >= 9 && sighashType(d[len(d)-1]) && StrictSignature(d[:len(d)-1])
}

// isP2PKH reports whether s is OP_DUP OP_HASH160 <20 bytes> OP_EQUALVERIFY
// OP_CHECKSIG.
func isP2PKH(s []byte) bool {
	return len(s) == 25 && s[0] == script.OpDUP && s[1] == script.OpHASH160 && s[2] == 20 &&
		s[23] == script.OpEQUALVERIFY && s[24] == script.OpCHECKSIG
}

// isP2PKFirst reports whether s starts <33-byte key> OP_CHECKSIG: a bare
// pay-to-public-key, and every lock-before PushDrop (funding outputs and
// anchor tokens among them), which one signature push spends.
func isP2PKFirst(s []byte) bool {
	return len(s) >= 35 && s[0] == 33 && s[34] == script.OpCHECKSIG
}

// canonicalUnlock is the section 5.3 form of one input's unlocking script,
// given the locking script it spends: push-only with minimal pushes, and
// every signature strict low-S DER with a defined sighash byte. The spent
// script says which pushes are signatures: a script that starts with
// <key> OP_CHECKSIG takes exactly one push, a signature; a P2PKH takes
// exactly a signature and then a compressed or uncompressed key. Any other
// spent script is refused, because which of its pushes are signatures, and
// so whether someone else could re-encode them, cannot be told.
func canonicalUnlock(unlocking, spent []byte) bool {
	ops, ok := parseScript(unlocking)
	if !ok || len(ops) == 0 {
		return false
	}
	for _, o := range ops {
		if !o.push || !minimalPush(o) {
			return false
		}
	}
	switch {
	case isP2PKFirst(spent):
		return len(ops) == 1 && signaturePush(ops[0].data)
	case isP2PKH(spent):
		if len(ops) != 2 || !signaturePush(ops[0].data) {
			return false
		}
		k := ops[1].data
		return (len(k) == 33 && (k[0] == 0x02 || k[0] == 0x03)) || (len(k) == 65 && k[0] == 0x04)
	}
	return false
}
