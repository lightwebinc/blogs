package logrec

import (
	"fmt"

	"github.com/lightwebinc/bcommon/cbor"
	"github.com/lightwebinc/bcommon/commit"
)

// Anchor is one interval anchor record: the second field of a mined anchor
// token. It commits the batches First to First+Count-1 of one producer's
// stream under Root.
type Anchor struct {
	Producer  []byte   // key 1
	Stream    string   // key 2
	Interval  uint64   // key 3, 1 to MaxSafe, contiguous per producer and stream
	Prev      [32]byte // key 4, the previous anchor token's txid; zero on interval 1
	First     uint64   // key 5, the first seq covered
	Count     uint64   // key 6, batches covered
	Root      [32]byte // key 7, Root over the covered batches' commitments
	Timestamp uint64   // key 8, producer clock, Unix milliseconds
	// Extra holds integer keys above 8, preserved verbatim and ignored.
	Extra cbor.Map
}

// Last is the last seq the anchor covers.
func (a *Anchor) Last() uint64 { return a.First + a.Count - 1 }

// Covers reports whether seq falls in the anchor's range.
func (a *Anchor) Covers(seq uint64) bool { return seq >= a.First && seq <= a.Last() }

// Validate applies every rule that needs only the record itself, in the
// refusal order.
func (a *Anchor) Validate() error {
	if err := checkExtra(a.Extra, akLast); err != nil {
		return err
	}
	if len(a.Extra)+9 > MaxKeys {
		return fmt.Errorf("%w: more than %d keys", ErrTooLarge, MaxKeys)
	}
	if err := CheckProducer(a.Producer); err != nil {
		return err
	}
	if err := CheckStream(a.Stream); err != nil {
		return err
	}
	if err := inRange(a.Interval, 1, MaxSafe, "interval"); err != nil {
		return err
	}
	if err := inRange(a.First, 1, MaxSafe, "first"); err != nil {
		return err
	}
	if err := inRange(a.Count, 1, MaxIntervalBatches, "count"); err != nil {
		return err
	}
	if err := inRange(a.Timestamp, 1, MaxSafe, "timestamp"); err != nil {
		return err
	}
	return a.crossRules()
}

// crossRules: prev, then the range across first, count and interval.
func (a *Anchor) crossRules() error {
	if (a.Interval == 1) != isZero(a.Prev) {
		return ErrPrev
	}
	if a.Interval == 1 && a.First != 1 {
		return fmt.Errorf("%w: first on interval 1", ErrRange)
	}
	if a.First+a.Count-1 > MaxSafe {
		return fmt.Errorf("%w: last seq", ErrRange)
	}
	return nil
}

// Encode writes the anchor record in canonical CBOR.
func (a *Anchor) Encode() ([]byte, error) {
	if err := a.Validate(); err != nil {
		return nil, err
	}
	m := cbor.Map{
		{Key: uint64(AKMagic), Val: MagicAnchor},
		{Key: uint64(AKProducer), Val: a.Producer},
		{Key: uint64(AKStream), Val: a.Stream},
		{Key: uint64(AKInterval), Val: a.Interval},
		{Key: uint64(AKPrev), Val: a.Prev[:]},
		{Key: uint64(AKFirst), Val: a.First},
		{Key: uint64(AKCount), Val: a.Count},
		{Key: uint64(AKRoot), Val: a.Root[:]},
		{Key: uint64(AKTimestamp), Val: a.Timestamp},
	}
	m = append(m, a.Extra...)
	out, err := cbor.Encode(m)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCBOR, err)
	}
	if len(out) > MaxAnchorRecord {
		return nil, ErrTooLarge
	}
	return out, nil
}

// IsAnchor reports whether payload claims to be an anchor record.
func IsAnchor(payload []byte) bool { return claims(payload, MagicAnchor) }

// DecodeAnchor parses an anchor record: bounded first, then canonical
// CBOR, then the magic, then each key in ascending order, then the rules
// across fields.
func DecodeAnchor(p []byte) (*Anchor, error) {
	m, err := decodeMap(p, MaxAnchorRecord)
	if err != nil {
		return nil, err
	}
	f, extra, err := intKeys(m, akLast)
	if err != nil {
		return nil, err
	}
	if err := checkMagic(f, MagicAnchor); err != nil {
		return nil, err
	}
	a := &Anchor{Extra: extra}
	if a.Producer, err = getBytes(f, AKProducer); err != nil {
		return nil, err
	}
	if err := CheckProducer(a.Producer); err != nil {
		return nil, err
	}
	if a.Stream, err = needText(f, AKStream); err != nil {
		return nil, err
	}
	if err := CheckStream(a.Stream); err != nil {
		return nil, err
	}
	if a.Interval, err = needUint(f, AKInterval, 1, MaxSafe); err != nil {
		return nil, err
	}
	prev, err := needBytes(f, AKPrev, 32, 32)
	if err != nil {
		return nil, err
	}
	a.Prev = hash32(prev)
	if a.First, err = needUint(f, AKFirst, 1, MaxSafe); err != nil {
		return nil, err
	}
	if a.Count, err = needUint(f, AKCount, 1, MaxIntervalBatches); err != nil {
		return nil, err
	}
	root, err := needBytes(f, AKRoot, 32, 32)
	if err != nil {
		return nil, err
	}
	a.Root = hash32(root)
	if a.Timestamp, err = needUint(f, AKTimestamp, 1, MaxSafe); err != nil {
		return nil, err
	}
	if err := a.crossRules(); err != nil {
		return nil, err
	}
	return a, nil
}

// Leaf is the RFC 6962 leaf hash of one batch: SHA-256(0x00 || c), where c
// is the batch carrier's commitment (its txid in hash byte order).
func Leaf(c [32]byte) [32]byte { return commit.LeafHash(c) }

// IntervalRoot is the anchor root over the commitments of batches First to
// First+Count-1, in ascending seq order: bcommon's RFC 6962 root, whose
// leaves are the commitments themselves (it applies Leaf).
func IntervalRoot(commitments [][32]byte) [32]byte { return commit.Root(commitments) }

// ProveBatch returns the inclusion path of the batch at seq under an
// anchor, given every commitment the anchor covers in order.
func ProveBatch(a *Anchor, commitments [][32]byte, seq uint64) (commit.Path, error) {
	if uint64(len(commitments)) != a.Count || !a.Covers(seq) {
		return nil, commit.ErrIndex
	}
	return commit.Prove(commitments, int(seq-a.First))
}

// VerifyBatch checks that the batch with commitment c sits at seq under the
// anchor. The path's shape fixes the leaf's index, so a verifier also
// checks that index against seq - First; commit.Verify alone would accept a
// path for another position.
func VerifyBatch(a *Anchor, c [32]byte, seq uint64, path commit.Path) bool {
	if !a.Covers(seq) {
		return false
	}
	idx, ok := pathIndex(path, a.Count)
	if !ok || idx != seq-a.First {
		return false
	}
	return commit.Verify(c, path, a.Root)
}

// pathIndex recovers the leaf index an RFC 6962 path proves in a tree of n
// leaves, and refuses a path whose length or sides no leaf of that tree has.
func pathIndex(path commit.Path, n uint64) (uint64, bool) {
	// Walk from the root down: at each split the sibling's side says which
	// half holds the leaf. The steps run leaf to root, so read them in
	// reverse.
	lo, size := uint64(0), n
	for i := len(path) - 1; i >= 0; i-- {
		if size < 2 {
			return 0, false
		}
		k := uint64(1)
		for k*2 < size {
			k *= 2
		}
		if path[i].Left { // sibling on the left: the leaf is in the right part
			lo, size = lo+k, size-k
		} else {
			size = k
		}
	}
	if size != 1 {
		return 0, false
	}
	return lo, true
}
