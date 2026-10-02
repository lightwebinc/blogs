package logrec

import (
	"bytes"
	"encoding/binary"
	"math/big"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-sdk/script"

	"github.com/lightwebinc/bcommon/pushdrop"
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
// and an optional field signature (bcommon pushdrop.Script).
func PushDropScript(key *ec.PublicKey, fields [][]byte, sig []byte) []byte {
	return pushdrop.Script(key, fields, sig)
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
