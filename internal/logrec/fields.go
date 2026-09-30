package logrec

import (
	"crypto/rand"
	"fmt"
	"io"
	"math/big"
	"unicode/utf8"

	"github.com/lightwebinc/bcommon/cbor"
)

// fieldP is the secp256k1 field prime.
var fieldP, _ = new(big.Int).SetString("fffffffffffffffffffffffffffffffffffffffffffffffffffffffefffffc2f", 16)

// CheckProducer refuses anything but a canonical compressed secp256k1 key:
// 33 bytes, prefix 0x02 or 0x03, an x-coordinate below the field prime, and
// a point on the curve. A reader that accepted x >= p would accept two
// encodings of one key, so two byte strings would name one producer.
func CheckProducer(k []byte) error {
	if len(k) != 33 || (k[0] != 0x02 && k[0] != 0x03) {
		return ErrProducer
	}
	x := new(big.Int).SetBytes(k[1:])
	if x.Cmp(fieldP) >= 0 {
		return ErrProducer
	}
	// y^2 = x^3 + 7 must have a root.
	rhs := new(big.Int).Exp(x, big.NewInt(3), fieldP)
	rhs.Add(rhs, big.NewInt(7))
	rhs.Mod(rhs, fieldP)
	if new(big.Int).ModSqrt(rhs, fieldP) == nil {
		return ErrProducer
	}
	return nil
}

// checkName applies the readable part's grammar: 1 to max bytes of
// lowercase ASCII letters and single underscores, starting and ending with
// a letter.
func checkName(s string, max int) error {
	if len(s) == 0 || len(s) > max {
		return ErrStream
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z':
		case c == '_':
			if i == 0 || i == len(s)-1 || s[i-1] == '_' {
				return ErrStream
			}
		default:
			return ErrStream
		}
	}
	return nil
}

// CheckStreamName applies the grammar of a stream's readable part: 1 to
// MaxStreamName bytes of lowercase ASCII letters and single underscores,
// starting and ending with a letter.
func CheckStreamName(name string) error { return checkName(name, MaxStreamName) }

// CheckStream applies the stream identifier grammar: <name>_<suffix>, where
// name meets CheckStreamName and suffix is exactly SuffixLen lowercase ASCII
// letters. "tm_log_" + stream is then a BRC-87 topic manager name. The
// suffix holds no underscore, so the split is the last underscore.
func CheckStream(s string) error {
	if len(s) < MinStream || len(s) > MaxStream {
		return ErrStream
	}
	cut := len(s) - SuffixLen - 1
	if s[cut] != '_' {
		return ErrStream
	}
	for i := cut + 1; i < len(s); i++ {
		if s[i] < 'a' || s[i] > 'z' {
			return ErrStream
		}
	}
	return CheckStreamName(s[:cut])
}

// SplitStream returns a stream identifier's readable part and suffix. The
// readable part alone never identifies a stream.
func SplitStream(s string) (name, suffix string, err error) {
	if err := CheckStream(s); err != nil {
		return "", "", err
	}
	cut := len(s) - SuffixLen - 1
	return s[:cut], s[cut+1:], nil
}

// NewStream creates a stream identifier for name: name, an underscore, and
// SuffixLen letters drawn uniformly at random from a to z by r
// (crypto/rand.Reader when r is nil). A producer calls it once, when it
// creates the stream, and keeps the result for the stream's life.
func NewStream(name string, r io.Reader) (string, error) {
	if err := CheckStreamName(name); err != nil {
		return "", err
	}
	if r == nil {
		r = rand.Reader
	}
	suffix := make([]byte, 0, SuffixLen)
	var buf [16]byte
	for len(suffix) < SuffixLen {
		if _, err := io.ReadFull(r, buf[:]); err != nil {
			return "", err
		}
		for _, c := range buf {
			// 234 = 9 * 26: rejecting 234 to 255 keeps every letter
			// equally likely.
			if c < 234 && len(suffix) < SuffixLen {
				suffix = append(suffix, 'a'+c%26)
			}
		}
	}
	return name + "_" + string(suffix), nil
}

// Topic is the topic manager name of a stream identifier.
func Topic(stream string) (string, error) {
	if err := CheckStream(stream); err != nil {
		return "", err
	}
	return TopicPrefix + stream, nil
}

// decodeMap bounds b, decodes one canonical CBOR item and requires a map.
func decodeMap(b []byte, max int) (cbor.Map, error) {
	if len(b) > max {
		return nil, ErrTooLarge
	}
	v, err := cbor.DecodeValue(b)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCBOR, err)
	}
	m, ok := v.(cbor.Map)
	if !ok {
		return nil, ErrCBOR
	}
	if len(m) > MaxKeys {
		return nil, fmt.Errorf("%w: more than %d keys", ErrTooLarge, MaxKeys)
	}
	return m, nil
}

// intKeys splits a record map into its known integer keys and the unknown
// ones above last, which are preserved. Any key that is not an unsigned
// integer refuses the record.
func intKeys(m cbor.Map, last uint64) (map[uint64]cbor.Value, cbor.Map, error) {
	known := make(map[uint64]cbor.Value, len(m))
	var extra cbor.Map
	for _, p := range m {
		k, ok := p.Key.(uint64)
		if !ok {
			return nil, nil, ErrKeyType
		}
		if k > last {
			extra = append(extra, p)
			continue
		}
		known[k] = p.Val
	}
	return known, extra, nil
}

// checkMagic applies step 5: key 0 present, bytes, and equal to magic.
func checkMagic(f map[uint64]cbor.Value, magic []byte) error {
	b, ok := f[0].([]byte)
	if !ok || string(b) != string(magic) {
		return ErrMagic
	}
	return nil
}

func getBytes(f map[uint64]cbor.Value, k uint64) ([]byte, error) {
	v, ok := f[k]
	if !ok {
		return nil, fmt.Errorf("%w: key %d", ErrMissing, k)
	}
	b, ok := v.([]byte)
	if !ok {
		return nil, fmt.Errorf("%w: key %d", ErrType, k)
	}
	return b, nil
}

func inRange(n, lo, hi uint64, what string) error {
	if n < lo || n > hi {
		return fmt.Errorf("%w: %s", ErrRange, what)
	}
	return nil
}

func needUint(f map[uint64]cbor.Value, k uint64, lo, hi uint64) (uint64, error) {
	v, ok := f[k]
	if !ok {
		return 0, fmt.Errorf("%w: key %d", ErrMissing, k)
	}
	n, ok := v.(uint64)
	if !ok {
		return 0, fmt.Errorf("%w: key %d", ErrType, k)
	}
	if n < lo || n > hi {
		return 0, fmt.Errorf("%w: key %d", ErrRange, k)
	}
	return n, nil
}

func needBytes(f map[uint64]cbor.Value, k uint64, lo, hi int) ([]byte, error) {
	v, ok := f[k]
	if !ok {
		return nil, fmt.Errorf("%w: key %d", ErrMissing, k)
	}
	b, ok := v.([]byte)
	if !ok {
		return nil, fmt.Errorf("%w: key %d", ErrType, k)
	}
	if len(b) < lo || len(b) > hi {
		return nil, fmt.Errorf("%w: key %d", ErrRange, k)
	}
	return b, nil
}

func needText(f map[uint64]cbor.Value, k uint64) (string, error) {
	v, ok := f[k]
	if !ok {
		return "", fmt.Errorf("%w: key %d", ErrMissing, k)
	}
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("%w: key %d", ErrType, k)
	}
	return s, nil
}

func hash32(b []byte) [32]byte {
	var h [32]byte
	copy(h[:], b)
	return h
}

func isZero(h [32]byte) bool { return h == [32]byte{} }

// checkExtra holds preserved keys to the rule that they sit above the
// known keys, so re-encoding cannot shadow a known field.
func checkExtra(extra cbor.Map, last uint64) error {
	for _, p := range extra {
		k, ok := p.Key.(uint64)
		if !ok || k <= last {
			return ErrKeyType
		}
	}
	return nil
}

func validURL(u string) bool {
	if len(u) < len("https://x") || len(u) > MaxURL || !utf8.ValidString(u) {
		return false
	}
	if u[:8] != "https://" {
		return false
	}
	for i := 0; i < len(u); i++ {
		if u[i] <= 0x20 || u[i] >= 0x7f {
			return false
		}
	}
	return true
}
