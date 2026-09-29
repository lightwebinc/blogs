// Package logrec is the byte-level contract of a blogs log stream: the batch
// record a carrier holds, the digest-only locator, the interval anchor record
// a mined token holds, and the RFC 6962 leaf rule an anchor's root is
// computed by. docs/spec.md is the normative text; this package is the
// minimal codec that generates the golden vectors under testdata/vectors.
//
// Every record is canonical CBOR through bcommon's cbor package, so one
// value has one encoding a reader accepts. Every decoder bounds its input
// before the CBOR decoder runs, and the CBOR decoder bounds every declared
// length against the bytes present before it allocates.
package logrec

import (
	"errors"

	"github.com/bsv-blockchain/go-sdk/wallet"

	"github.com/lightwebinc/bcommon/pushdrop"
)

// Registry values (docs/frozen.md); frozen once the first record is
// published to a public host.
const (
	// ProtocolName is the BRC-43 protocol name, security level 1.
	ProtocolName = "blogs"
	// KeyBatch locks a batch carrier's record output, signs its field, and
	// locks the funding-tree outputs a carrier spends.
	KeyBatch = "batch"
	// KeyAnchor locks and signs the mined interval-anchor token.
	KeyAnchor = "anchor"
	// KeyFund is the embedded wallet's funding key.
	KeyFund = "fund"

	// TopicPrefix and LookupService are the overlay names (BRC-87).
	TopicPrefix   = "tm_log_"
	LookupService = "ls_log"
)

// Tags and magics. The tag prefix "bl" belongs to blogs.
var (
	TagPrefix = []byte("bl")
	// TagAnchor is the first field of an anchor token.
	TagAnchor = []byte{'b', 'l', 0x01}
	// TagFunding is the one field of a funding-tree output.
	TagFunding = []byte{'b', 'l', 0x02}
	// MagicBatch is key 0 of a batch record: "bl", 'b', version 1.
	MagicBatch = []byte{'b', 'l', 'b', 0x01}
	// MagicAnchor is key 0 of an anchor record: "bl", 'a', version 1.
	MagicAnchor = []byte{'b', 'l', 'a', 0x01}
)

// Derivations under [1, "blogs"].
var (
	Protocol         = wallet.Protocol{SecurityLevel: wallet.SecurityLevelEveryApp, Protocol: ProtocolName}
	BatchDerivation  = pushdrop.Derivation{Protocol: Protocol, KeyID: KeyBatch}
	AnchorDerivation = pushdrop.Derivation{Protocol: Protocol, KeyID: KeyAnchor}
	FundDerivation   = pushdrop.Derivation{Protocol: Protocol, KeyID: KeyFund}
)

// Bounds. A reader accepts every record within them; a later version may
// raise a bound but never lower it (docs/frozen.md).
const (
	// MaxSafe is 2^53 - 1: every integer a record carries fits a JSON
	// number and a JavaScript number exactly.
	MaxSafe = 1<<53 - 1
	// SuffixLen is the length of a stream's random suffix: lowercase
	// letters, about 47 bits.
	SuffixLen = 10
	// MaxStream is the longest stream identifier <name>_<suffix>: BRC-87
	// caps a name at 50 characters and the topic prefix takes 7.
	MaxStream = 50 - len(TopicPrefix)
	// MaxStreamName is the longest readable part: MaxStream less the
	// underscore and the suffix.
	MaxStreamName = MaxStream - 1 - SuffixLen
	// MinStream is the shortest stream identifier: one letter, the
	// underscore and the suffix.
	MinStream = 1 + 1 + SuffixLen
	// MaxContent bounds a full-bytes batch's content, in bytes as carried.
	MaxContent = 256 << 10
	// MaxExpanded bounds a batch's decoded content.
	MaxExpanded = 16 << 20
	// MaxLocated bounds the content a digest-only locator may name, as
	// fetched. It equals MaxExpanded: a batch's decoded size is bounded
	// there whatever its profile.
	MaxLocated = MaxExpanded
	// MaxBatchRecord bounds an encoded batch record, content included.
	MaxBatchRecord = MaxContent + 4096
	// MaxAnchorRecord bounds an encoded anchor record.
	MaxAnchorRecord = 1024
	// MaxIntervalBatches bounds the batches one anchor covers.
	MaxIntervalBatches = 1 << 16
	// MaxURLs and MaxURL bound a locator's fetch hints.
	MaxURLs = 4
	MaxURL  = 256
	// MaxKeys bounds the entries of a record map, unknown keys included.
	MaxKeys = 64
)

// Batch record keys.
const (
	BKMagic     = 0
	BKProducer  = 1
	BKStream    = 2
	BKSeq       = 3
	BKPrev      = 4
	BKTimestamp = 5
	BKCount     = 6
	BKEncoding  = 7
	BKProfile   = 8
	BKContent   = 9
	BKLocator   = 10
	BKSize      = 11
	bkLast      = BKSize
)

// Anchor record keys.
const (
	AKMagic     = 0
	AKProducer  = 1
	AKStream    = 2
	AKInterval  = 3
	AKPrev      = 4
	AKFirst     = 5
	AKCount     = 6
	AKRoot      = 7
	AKTimestamp = 8
	akLast      = AKTimestamp
)

// Content encodings.
const (
	// EncodingLines is UTF-8 text, one log record per line, each line
	// terminated by LF (0x0A).
	EncodingLines = 1
	// EncodingLinesGzip is EncodingLines compressed as one gzip member
	// (RFC 1952).
	EncodingLinesGzip = 2
)

// Profiles.
const (
	// ProfileFull carries the content in the record (key 9).
	ProfileFull = 1
	// ProfileDigest carries a locator (key 10) instead of the content.
	ProfileDigest = 2
)

// The refusals. Their texts name the rule broken; a host counts them by the
// Reason of each.
var (
	ErrTooLarge     = errors.New("logrec: record exceeds its bound")
	ErrCBOR         = errors.New("logrec: not a canonical CBOR map")
	ErrMagic        = errors.New("logrec: wrong magic")
	ErrKeyType      = errors.New("logrec: record key is not an unsigned integer")
	ErrMissing      = errors.New("logrec: required key missing")
	ErrType         = errors.New("logrec: field has the wrong type")
	ErrRange        = errors.New("logrec: field out of range")
	ErrProducer     = errors.New("logrec: producer key is not a canonical compressed key")
	ErrStream       = errors.New("logrec: stream identifier breaks the grammar")
	ErrPrev         = errors.New("logrec: prev must be zero exactly on the first")
	ErrProfileShape = errors.New("logrec: profile and content keys disagree")
	ErrLocator      = errors.New("logrec: locator is malformed")
	ErrContent      = errors.New("logrec: content does not match its declared size or count")
)

// The refusal order, which decides the reason when a record breaks several
// rules (docs/spec.md section 3.2):
//
//  1. too-large: the encoded length is over the bound;
//  2. cbor: not exactly one canonical item, or not a map;
//  3. too-large: more than MaxKeys entries;
//  4. key-type: a key that is not an unsigned integer;
//  5. magic: key 0 absent, not bytes, or not the magic;
//  6. each defined key in ascending order: missing, then type, then the
//     field's own rule (range, producer, stream, locator);
//  7. across fields: prev, then count against size (range), then
//     profile-shape, then content.

// Reason is the fixed label a host counts a refusal by.
func Reason(err error) string {
	for _, e := range []struct {
		err error
		s   string
	}{
		{ErrTooLarge, "too-large"}, {ErrCBOR, "cbor"}, {ErrMagic, "magic"},
		{ErrKeyType, "key-type"}, {ErrMissing, "missing"}, {ErrType, "type"},
		{ErrRange, "range"}, {ErrProducer, "producer"}, {ErrStream, "stream"},
		{ErrPrev, "prev"}, {ErrProfileShape, "profile-shape"},
		{ErrLocator, "locator"}, {ErrContent, "content"},
	} {
		if errors.Is(err, e.err) {
			return e.s
		}
	}
	return "other"
}
