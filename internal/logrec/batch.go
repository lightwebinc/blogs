package logrec

import (
	"fmt"

	"github.com/lightwebinc/bcommon/cbor"
	"github.com/lightwebinc/bcommon/record"
)

// Batch is one batch record: the payload of one carrier.
type Batch struct {
	Producer  []byte   // key 1, compressed identity key, 33 bytes
	Stream    string   // key 2
	Seq       uint64   // key 3, 1 to MaxSafe
	Prev      [32]byte // key 4, the previous batch's commitment; zero on seq 1
	Timestamp uint64   // key 5, producer clock, Unix milliseconds
	Count     uint64   // key 6, log records in the content
	Encoding  uint64   // key 7
	Profile   uint64   // key 8
	Content   []byte   // key 9, ProfileFull only
	Locator   *Locator // key 10, ProfileDigest only
	Size      uint64   // key 11, decoded content length
	// Extra holds integer keys above 11, preserved verbatim and ignored.
	Extra cbor.Map
}

// Supported reports whether this version can read the batch's content: its
// profile and encoding are known and, for a digest-only batch, its locator
// has no member this version does not define. An unsupported batch is still
// a valid link in its stream.
func (b *Batch) Supported() bool {
	if b.Encoding != EncodingLines && b.Encoding != EncodingLinesGzip {
		return false
	}
	switch b.Profile {
	case ProfileFull:
		return true
	case ProfileDigest:
		return b.Locator != nil && !b.Locator.Unsupported()
	}
	return false
}

// Validate applies every rule that needs only the record itself, in the
// refusal order (steps 6 and 7). Encode and DecodeBatch both run it, so a
// producer cannot write what a reader refuses.
func (b *Batch) Validate() error {
	if err := checkExtra(b.Extra, bkLast); err != nil {
		return err
	}
	if len(b.Extra)+b.knownKeys() > MaxKeys {
		return fmt.Errorf("%w: more than %d keys", ErrTooLarge, MaxKeys)
	}
	if err := b.fieldRules(); err != nil {
		return err
	}
	return b.crossRules()
}

// knownKeys is how many defined keys the record encodes.
func (b *Batch) knownKeys() int {
	n := 10 // keys 0 to 8 and 11
	if b.Content != nil {
		n++
	}
	if b.Locator != nil {
		n++
	}
	return n
}

// fieldRules is step 6 for typed fields: each key's own rule, in key order.
func (b *Batch) fieldRules() error {
	if err := CheckProducer(b.Producer); err != nil {
		return err
	}
	if err := CheckStream(b.Stream); err != nil {
		return err
	}
	if err := inRange(b.Seq, 1, MaxSafe, "seq"); err != nil {
		return err
	}
	if err := inRange(b.Timestamp, 1, MaxSafe, "timestamp"); err != nil {
		return err
	}
	if err := inRange(b.Count, 1, MaxSafe, "count"); err != nil {
		return err
	}
	if err := inRange(b.Encoding, 1, MaxSafe, "encoding"); err != nil {
		return err
	}
	if err := inRange(b.Profile, 1, MaxSafe, "profile"); err != nil {
		return err
	}
	if b.Content != nil {
		if err := inRange(uint64(len(b.Content)), 1, MaxContent, "content"); err != nil {
			return err
		}
	}
	if b.Locator != nil {
		if err := b.Locator.validate(); err != nil {
			return err
		}
	}
	return inRange(b.Size, 1, MaxExpanded, "size")
}

// crossRules is step 7.
func (b *Batch) crossRules() error {
	if (b.Seq == 1) != isZero(b.Prev) {
		return ErrPrev
	}
	if b.Count > b.Size {
		return fmt.Errorf("%w: count above size", ErrRange)
	}
	switch b.Profile {
	case ProfileFull:
		if b.Content == nil || b.Locator != nil {
			return ErrProfileShape
		}
		if b.Encoding == EncodingLines && b.Size != uint64(len(b.Content)) {
			return ErrContent
		}
	case ProfileDigest:
		if b.Content != nil || b.Locator == nil {
			return ErrProfileShape
		}
		if b.Encoding == EncodingLines && b.Size != b.Locator.Length {
			return ErrContent
		}
	}
	return nil
}

// Encode writes the batch record in canonical CBOR.
func (b *Batch) Encode() ([]byte, error) {
	if err := b.Validate(); err != nil {
		return nil, err
	}
	m := cbor.Map{
		{Key: uint64(BKMagic), Val: MagicBatch},
		{Key: uint64(BKProducer), Val: b.Producer},
		{Key: uint64(BKStream), Val: b.Stream},
		{Key: uint64(BKSeq), Val: b.Seq},
		{Key: uint64(BKPrev), Val: b.Prev[:]},
		{Key: uint64(BKTimestamp), Val: b.Timestamp},
		{Key: uint64(BKCount), Val: b.Count},
		{Key: uint64(BKEncoding), Val: b.Encoding},
		{Key: uint64(BKProfile), Val: b.Profile},
		{Key: uint64(BKSize), Val: b.Size},
	}
	if b.Content != nil {
		m = append(m, cbor.Pair{Key: uint64(BKContent), Val: b.Content})
	}
	if b.Locator != nil {
		m = append(m, cbor.Pair{Key: uint64(BKLocator), Val: b.Locator.value()})
	}
	m = append(m, b.Extra...)
	out, err := cbor.Encode(m)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCBOR, err)
	}
	if len(out) > MaxBatchRecord {
		return nil, ErrTooLarge
	}
	return out, nil
}

// IsBatch reports whether payload claims to be a batch record: a CBOR map
// whose first entry is key 0 with the batch magic. It is the carrier
// classifier's test and allocates nothing.
func IsBatch(payload []byte) bool {
	return claims(payload, MagicBatch)
}

// claims looks for a definite-length map head (0xa0 to 0xbb), key 0, then a
// four-byte string equal to magic. Key 0 always sorts first in a canonical
// map with unsigned-integer keys.
func claims(p, magic []byte) bool { return record.Claims(p, magic) }

// DecodeBatch parses a batch record in the refusal order: the bound, then
// canonical CBOR and the map's shape, then the magic, then each defined key
// in ascending order (missing, type, the key's own rule), then the rules
// across fields.
func DecodeBatch(p []byte) (*Batch, error) {
	m, err := decodeMap(p, MaxBatchRecord)
	if err != nil {
		return nil, err
	}
	f, extra, err := intKeys(m, bkLast)
	if err != nil {
		return nil, err
	}
	if err := checkMagic(f, MagicBatch); err != nil {
		return nil, err
	}
	b := &Batch{Extra: extra}
	if b.Producer, err = getBytes(f, BKProducer); err != nil {
		return nil, err
	}
	if err := CheckProducer(b.Producer); err != nil {
		return nil, err
	}
	if b.Stream, err = needText(f, BKStream); err != nil {
		return nil, err
	}
	if err := CheckStream(b.Stream); err != nil {
		return nil, err
	}
	if b.Seq, err = needUint(f, BKSeq, 1, MaxSafe); err != nil {
		return nil, err
	}
	prev, err := needBytes(f, BKPrev, 32, 32)
	if err != nil {
		return nil, err
	}
	b.Prev = hash32(prev)
	if b.Timestamp, err = needUint(f, BKTimestamp, 1, MaxSafe); err != nil {
		return nil, err
	}
	if b.Count, err = needUint(f, BKCount, 1, MaxSafe); err != nil {
		return nil, err
	}
	if b.Encoding, err = needUint(f, BKEncoding, 1, MaxSafe); err != nil {
		return nil, err
	}
	if b.Profile, err = needUint(f, BKProfile, 1, MaxSafe); err != nil {
		return nil, err
	}
	if _, ok := f.Get(BKContent); ok {
		if b.Content, err = needBytes(f, BKContent, 1, MaxContent); err != nil {
			return nil, err
		}
	}
	if v, ok := f.Get(BKLocator); ok {
		if _, isMap := v.(cbor.Map); !isMap {
			return nil, fmt.Errorf("%w: key %d", ErrType, BKLocator)
		}
		if b.Locator, err = decodeLocator(v); err != nil {
			return nil, err
		}
	}
	if b.Size, err = needUint(f, BKSize, 1, MaxExpanded); err != nil {
		return nil, err
	}
	if err := b.crossRules(); err != nil {
		return nil, err
	}
	return b, nil
}
