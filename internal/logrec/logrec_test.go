package logrec

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"math/rand"
	"strings"
	"testing"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"

	"github.com/lightwebinc/bcommon/cbor"
	"github.com/lightwebinc/bcommon/commit"
)

func producer(t testing.TB) []byte {
	t.Helper()
	priv, _ := ec.PrivateKeyFromBytes(bytes.Repeat([]byte{0x42}, 32))
	return priv.PubKey().Compressed()
}

func goodBatch(t testing.TB) *Batch {
	c := []byte("one\ntwo\n")
	return &Batch{
		Producer: producer(t), Stream: "example_app_qzxkvbmwtr", Seq: 7, Prev: sha256.Sum256([]byte("p")),
		Timestamp: 1767225600000, Count: 2, Encoding: EncodingLines, Profile: ProfileFull,
		Content: c, Size: uint64(len(c)),
	}
}

func TestBatchRoundTrip(t *testing.T) {
	b := goodBatch(t)
	b.Extra = cbor.Map{{Key: uint64(40), Val: "future"}}
	enc, err := b.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if !IsBatch(enc) || IsAnchor(enc) {
		t.Fatal("classifier")
	}
	got, err := DecodeBatch(enc)
	if err != nil {
		t.Fatal(err)
	}
	again, err := got.Encode()
	if err != nil || !bytes.Equal(again, enc) {
		t.Fatalf("re-encode differs: %v", err)
	}
	if len(got.Extra) != 1 {
		t.Fatal("unknown key not preserved")
	}
}

// TestKeyLimitBothWays: a record at the key limit decodes and re-encodes,
// one over is refused both ways, and the classifier sees every map head.
func TestKeyLimitBothWays(t *testing.T) {
	b := goodBatch(t)
	for k := uint64(24); len(b.Extra)+b.knownKeys() < MaxKeys; k++ {
		b.Extra = append(b.Extra, cbor.Pair{Key: k, Val: uint64(0)})
	}
	enc, err := b.Encode()
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeBatch(enc)
	if err != nil {
		t.Fatal(err)
	}
	if again, err := got.Encode(); err != nil || !bytes.Equal(again, enc) {
		t.Fatalf("64-key record does not re-encode: %v", err)
	}
	b.Extra = append(b.Extra, cbor.Pair{Key: uint64(200), Val: uint64(0)})
	if _, err := b.Encode(); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("65 keys encoded: %v", err)
	}
	for _, head := range [][]byte{{0xb8, 0x40}, {0xb9, 0x00, 0x40}, {0xba, 0, 0, 0, 0x40}, {0xbb, 0, 0, 0, 0, 0, 0, 0, 0x40}} {
		p := append(append([]byte{}, head...), 0x00, 0x44)
		p = append(p, MagicBatch...)
		if !IsBatch(p) {
			t.Fatalf("head %x not classified", head)
		}
	}
}

func TestDigestBatchAndLocator(t *testing.T) {
	content := []byte("a\nb\n")
	l := &Locator{Digest: sha256.Sum256(content), Length: 4, URLs: []string{}}
	b := goodBatch(t)
	b.Profile, b.Content, b.Locator, b.Size = ProfileDigest, nil, l, 4
	enc, err := b.Encode()
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeBatch(enc)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Supported() {
		t.Fatal("known locator reported unsupported")
	}
	if err := CheckFetched(got.Locator, content); err != nil {
		t.Fatal(err)
	}
	if CheckFetched(got.Locator, []byte("a\nc\n")) == nil {
		t.Fatal("wrong bytes accepted")
	}
	if CheckFetched(got.Locator, []byte("a\n")) == nil {
		t.Fatal("short bytes accepted")
	}
	// An unknown member: preserved, and the batch is still valid but its
	// content is unsupported.
	l.Extra = cbor.Map{{Key: "zone", Val: "x"}}
	enc, err = b.Encode()
	if err != nil {
		t.Fatal(err)
	}
	got, err = DecodeBatch(enc)
	if err != nil {
		t.Fatal(err)
	}
	if got.Supported() || !got.Locator.Unsupported() {
		t.Fatal("unknown locator member not reported")
	}
	if again, _ := got.Encode(); !bytes.Equal(again, enc) {
		t.Fatal("unknown locator member not preserved")
	}
}

// TestLocatorUnknownMembersNeverRefuse: any number of members of any key
// type beyond the three known ones make the locator unsupported, never the
// batch refused.
func TestLocatorUnknownMembersNeverRefuse(t *testing.T) {
	l := &Locator{Digest: sha256.Sum256([]byte("a\n")), Length: 2, URLs: []string{"https://logs.example.com/a"}}
	for i := 0; i < 12; i++ {
		l.Extra = append(l.Extra, cbor.Pair{Key: uint64(i), Val: "x"})
	}
	l.Extra = append(l.Extra, cbor.Pair{Key: "", Val: []byte{1}}, cbor.Pair{Key: strings.Repeat("n", 100), Val: nil})
	enc, err := l.Encode()
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeLocator(enc)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Unsupported() {
		t.Fatal("unknown members not reported")
	}
	for _, u := range []string{"HTTPS://logs.example.com/a", "https://", "https://a b", "http://a"} {
		l := &Locator{Length: 1, URLs: []string{u}}
		if _, err := l.Encode(); !errors.Is(err, ErrLocator) {
			t.Errorf("%q accepted", u)
		}
	}
}

func TestUnknownProfileAndEncodingAreLinks(t *testing.T) {
	b := goodBatch(t)
	b.Profile = 9
	b.Encoding = 7
	enc, err := b.Encode()
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeBatch(enc)
	if err != nil {
		t.Fatal(err)
	}
	if got.Supported() {
		t.Fatal("unknown profile reported supported")
	}
}

func TestBoundBeforeDecode(t *testing.T) {
	// A byte string past the bound is refused as too large before the
	// CBOR decoder runs, whatever it declares.
	huge := make([]byte, MaxBatchRecord+1)
	huge[0] = 0xbf
	if _, err := DecodeBatch(huge); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("got %v", err)
	}
	// A declared length far past the bytes present is refused as
	// truncated, without allocating for it.
	lie := []byte{0xa1, 0x09, 0x5b, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	allocs := testing.AllocsPerRun(10, func() { _, _ = DecodeBatch(lie) })
	if _, err := DecodeBatch(lie); !errors.Is(err, ErrCBOR) {
		t.Fatalf("got %v", err)
	}
	if allocs > 16 {
		t.Fatalf("%v allocations for a lying length", allocs)
	}
	// Content at the bound is accepted; one byte over is refused on
	// encode and on decode.
	b := goodBatch(t)
	b.Content = bytes.Repeat([]byte("x"), MaxContent-1)
	b.Content = append(b.Content, '\n')
	b.Size, b.Count = uint64(len(b.Content)), 1
	enc, err := b.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeBatch(enc); err != nil {
		t.Fatal(err)
	}
	b.Content = append(b.Content, '\n')
	b.Size++
	if _, err := b.Encode(); !errors.Is(err, ErrRange) {
		t.Fatalf("got %v", err)
	}
}

func TestStreamGrammar(t *testing.T) {
	const x = "_qzxkvbmwtr"
	name32 := strings.Repeat("a", 32)
	for s, ok := range map[string]bool{
		"a" + x: true, "example_app" + x: true, name32 + x: true, "a_b_c" + x: true,
		"": false, "a": false, "example_app": false, strings.Repeat("a", 43): false,
		strings.Repeat("a", 33) + x: false, "_a" + x: false, "a_" + x: false, "a__b" + x: false,
		"A" + x: false, "a1" + x: false, "a-b" + x: false, "a b" + x: false, "é" + x: false,
		x: false, "a_qzxkvbmwt": false, "a_qzxkvbmwtrs": false, "a_qzxkvbmwtR": false,
		"a_qzxkvbmwt1": false, "a_qzxk_bmwtr": false,
	} {
		if (CheckStream(s) == nil) != ok {
			t.Errorf("%q: want ok=%v", s, ok)
		}
	}
	if MaxStreamName != 32 || MinStream != 12 || MaxStream != 43 {
		t.Fatal("bounds")
	}
	topic, _ := Topic(name32 + x)
	if len(topic) != 50 {
		t.Fatal("topic past the BRC-87 length")
	}
	name, suffix, err := SplitStream("example_app" + x)
	if err != nil || name != "example_app" || suffix != "qzxkvbmwtr" {
		t.Fatal("split")
	}
}

func TestNewStream(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 64; i++ {
		s, err := NewStream("example_app", nil)
		if err != nil || CheckStream(s) != nil || !strings.HasPrefix(s, "example_app_") {
			t.Fatalf("%q: %v", s, err)
		}
		seen[s] = true
	}
	if len(seen) != 64 {
		t.Fatal("suffixes repeat")
	}
	if _, err := NewStream(strings.Repeat("a", 33), nil); !errors.Is(err, ErrStream) {
		t.Fatal("long name accepted")
	}
	if _, err := NewStream("a_", nil); !errors.Is(err, ErrStream) {
		t.Fatal("bad name accepted")
	}
	// Bytes 234 to 255 are rejected, so every letter is equally likely.
	r := bytes.NewReader(append(bytes.Repeat([]byte{255}, 16), bytes.Repeat([]byte{0, 25, 26, 233}, 8)...))
	s, err := NewStream("a", r)
	if err != nil || s != "a_azazazazaz" {
		t.Fatalf("%q: %v", s, err)
	}
}

// TestProducerMatchesSDK holds CheckProducer to go-sdk's parser on random
// keys, and refuses the x >= p alias the SDK accepts.
func TestProducerMatchesSDK(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	for i := 0; i < 300; i++ {
		k := make([]byte, 33)
		k[0] = byte(2 + r.Intn(2))
		r.Read(k[1:])
		_, sdkErr := ec.ParsePubKey(k)
		if (CheckProducer(k) == nil) != (sdkErr == nil) {
			t.Fatalf("%x: CheckProducer and go-sdk disagree", k)
		}
	}
	alias, _ := hex.DecodeString("02fffffffffffffffffffffffffffffffffffffffffffffffffffffffefffffc30")
	if CheckProducer(alias) == nil {
		t.Fatal("x = p + 1 accepted")
	}
}

func TestAnchorAndPaths(t *testing.T) {
	cs := make([][32]byte, 7)
	for i := range cs {
		cs[i] = sha256.Sum256([]byte{byte(i)})
	}
	a := &Anchor{Producer: producer(t), Stream: "s_qzxkvbmwtr", Interval: 3, Prev: sha256.Sum256([]byte("a")),
		First: 20, Count: 7, Root: IntervalRoot(cs), Timestamp: 1}
	enc, err := a.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if !IsAnchor(enc) {
		t.Fatal("classifier")
	}
	if _, err := DecodeAnchor(enc); err != nil {
		t.Fatal(err)
	}
	for i := range cs {
		seq := a.First + uint64(i)
		p, err := ProveBatch(a, cs, seq)
		if err != nil {
			t.Fatal(err)
		}
		if !VerifyBatch(a, cs[i], seq, p) {
			t.Fatalf("seq %d", seq)
		}
		// The same path claimed for another seq fails, even though
		// commit.Verify alone would accept it.
		other := seq + 1
		if i == len(cs)-1 {
			other = seq - 1
		}
		if !commit.Verify(cs[i], p, a.Root) || VerifyBatch(a, cs[i], other, p) {
			t.Fatalf("seq %d: path accepted at seq %d", seq, other)
		}
	}
	if VerifyBatch(a, cs[0], 19, nil) || VerifyBatch(a, cs[0], 27, nil) {
		t.Fatal("seq outside the anchor accepted")
	}
	// A one-batch interval: the root is the leaf and the path is empty.
	one := &Anchor{Producer: producer(t), Stream: "s_qzxkvbmwtr", Interval: 1, First: 1, Count: 1,
		Root: IntervalRoot(cs[:1]), Timestamp: 1}
	if one.Root != Leaf(cs[0]) || !VerifyBatch(one, cs[0], 1, nil) {
		t.Fatal("one-batch interval")
	}
}

func gz(t *testing.T, s string, extra []byte) []byte {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	zw.Write([]byte(s))
	zw.Close()
	return append(buf.Bytes(), extra...)
}

func TestExpand(t *testing.T) {
	text := "x\ny\nz\n"
	b := &Batch{Encoding: EncodingLinesGzip, Size: uint64(len(text)), Count: 3}
	if out, err := Expand(b, gz(t, text, nil)); err != nil || string(out) != text {
		t.Fatalf("%q %v", out, err)
	}
	// A second member, or any byte after the first, is refused.
	if _, err := Expand(b, gz(t, text, gz(t, "w\n", nil))); err == nil {
		t.Fatal("second member accepted")
	}
	if _, err := Expand(b, gz(t, text, []byte{0})); err == nil {
		t.Fatal("trailing byte accepted")
	}
	// A bomb: 8 MiB of zeros declared as 6 bytes stops at 7.
	bomb := gz(t, strings.Repeat("\x00", 8<<20), nil)
	if _, err := Expand(b, bomb); !errors.Is(err, ErrContent) {
		t.Fatalf("bomb: %v", err)
	}
	// Count and the final LF are checked.
	b.Count = 2
	if _, err := Expand(b, gz(t, text, nil)); err == nil {
		t.Fatal("wrong count accepted")
	}
	lines := &Batch{Encoding: EncodingLines, Size: 3, Count: 1}
	if _, err := Expand(lines, []byte("abc")); err == nil {
		t.Fatal("missing final LF accepted")
	}
	if _, err := Expand(&Batch{Encoding: 5, Size: 1, Count: 1}, []byte("\n")); err == nil {
		t.Fatal("unknown encoding expanded")
	}
}

func TestReasonLabels(t *testing.T) {
	if Reason(errors.New("x")) != "other" || Reason(ErrStream) != "stream" {
		t.Fatal("labels")
	}
}

func TestDerivations(t *testing.T) {
	for _, d := range []interface{ Validate() error }{BatchDerivation, AnchorDerivation, FundDerivation} {
		if err := d.Validate(); err != nil {
			t.Fatal(err)
		}
	}
}

// FuzzDecodeBatch: whatever decodes re-encodes to the same bytes, so a
// reader never accepts two encodings of one record.
func FuzzDecodeBatch(f *testing.F) {
	enc, _ := goodBatch(f).Encode()
	f.Add(enc)
	f.Add([]byte{0xa0})
	f.Fuzz(func(t *testing.T, p []byte) {
		b, err := DecodeBatch(p)
		if err != nil {
			return
		}
		again, err := b.Encode()
		if err != nil || !bytes.Equal(again, p) {
			t.Fatalf("accepted a record that does not re-encode to itself: %x", p)
		}
	})
}

func FuzzDecodeAnchor(f *testing.F) {
	a := &Anchor{Producer: producer(f), Stream: "s_qzxkvbmwtr", Interval: 1, First: 1, Count: 1, Timestamp: 1}
	enc, _ := a.Encode()
	f.Add(enc)
	f.Fuzz(func(t *testing.T, p []byte) {
		a, err := DecodeAnchor(p)
		if err != nil {
			return
		}
		again, err := a.Encode()
		if err != nil || !bytes.Equal(again, p) {
			t.Fatalf("accepted an anchor that does not re-encode to itself: %x", p)
		}
	})
}
