// Package vectors generates the golden vectors under testdata/vectors from
// the logrec codec. Every input is fixed, so a run writes the same bytes
// every time; TestVectors regenerates them and compares byte for byte.
package vectors

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"

	"github.com/lightwebinc/bcommon/cbor"

	"github.com/lightwebinc/blogs/internal/logrec"
)

// FixedKey is the test key: 32 bytes of 0x42. It is public and must never
// hold anything real.
var FixedKey = bytes.Repeat([]byte{0x42}, 32)

// Stream is the vectors' stream identifier: the readable part
// "example_app" and a fixed example suffix. A real stream draws its suffix
// at random (logrec.NewStream); a vector fixes one so it is reproducible.
const Stream = "example_app_qzxkvbmwtr"

// OtherStream shares Stream's readable part under another suffix: a
// different stream, and a different topic.
const OtherStream = "example_app_hdjvnwpxle"

// commitment is a stand-in carrier txid (hash byte order) for batch i.
// Carrier vectors come with the transaction codec; the anchor arithmetic
// needs only the commitments.
func commitment(i int) [32]byte {
	return sha256.Sum256([]byte(fmt.Sprintf("blogs/vector/commitment/%d", i)))
}

// anchorTxid is a stand-in anchor-token txid for interval k.
func anchorTxid(k int) [32]byte {
	return sha256.Sum256([]byte(fmt.Sprintf("blogs/vector/anchor-txid/%d", k)))
}

// The lines content: three log records, the last carrying an ANSI escape
// that a renderer must strip (spec section 10); the record carries it
// verbatim.
const linesContent = "{\"level\":\"info\",\"msg\":\"started\",\"host\":\"logs.example.com\"}\n" +
	"{\"level\":\"info\",\"msg\":\"accepted\",\"peer\":\"192.0.2.10\"}\n" +
	"{\"level\":\"warn\",\"msg\":\"\x1b[2Jcleared?\",\"peer\":\"2001:db8::10\"}\n"

// gzipContent is one gzip member (header MTIME 0, no name) of three lines,
// fixed here so the vector does not move with a compressor's version.
const gzipContent = "1f8b08000000000002ffbccd4faac2301007e07d4ff1f8ad4398843ed4b94dd4f10f244dcda48e20bdbbe0c2b52b2ff07d4f64b94b06e33a9d2a1c8a9ec168725b443b1c2e553b1866e6e591ca9cc51f6a81c32cd2c08844818ffb2df3060eda535f141c89d6e137b6a5367d6ccdd5febe0fc22e7af2d18fe35b0087f84febf01a002ecc042615010000"

const gzipExpanded = "{\"level\":\"info\",\"msg\":\"request\",\"host\":\"www.example.com\",\"peer\":\"2001:db8::7\",\"status\":200}\n" +
	"{\"level\":\"info\",\"msg\":\"request\",\"host\":\"www.example.com\",\"peer\":\"2001:db8::7\",\"status\":200}\n" +
	"{\"level\":\"warn\",\"msg\":\"slow request\",\"host\":\"www.example.com\",\"peer\":\"192.0.2.44\",\"ms\":1250}\n"

// Base timestamp in Unix milliseconds.
const t0 = 1767225600000

func h(b []byte) string { return hex.EncodeToString(b) }

// Producer returns the fixed key's compressed identity key.
func Producer() []byte {
	priv, _ := ec.PrivateKeyFromBytes(FixedKey)
	return priv.PubKey().Compressed()
}

type batchJSON struct {
	Name      string         `json:"name"`
	Note      string         `json:"note"`
	Producer  string         `json:"producer"`
	Stream    string         `json:"stream"`
	Seq       uint64         `json:"seq"`
	Prev      string         `json:"prev"`
	Timestamp uint64         `json:"timestamp"`
	Count     uint64         `json:"count"`
	Encoding  uint64         `json:"encoding"`
	Profile   uint64         `json:"profile"`
	Size      uint64         `json:"size"`
	Content   string         `json:"content,omitempty"`
	Expanded  string         `json:"expanded"`
	Locator   *locatorJSON   `json:"locator,omitempty"`
	Topic     string         `json:"topic"`
	Record    string         `json:"record"`
	RecordLen int            `json:"recordLength"`
	Extra     []extraKeyJSON `json:"extra,omitempty"`
}

type extraKeyJSON struct {
	Key   uint64 `json:"key"`
	Value string `json:"valueCbor"`
}

type locatorJSON struct {
	Digest      string   `json:"digest"`
	Length      uint64   `json:"length"`
	URLs        []string `json:"urls"`
	Unsupported bool     `json:"unsupported"`
	Encoding    string   `json:"encoding,omitempty"`
}

func batchVector(name, note string, b *logrec.Batch, expanded string) (batchJSON, []byte, error) {
	rec, err := b.Encode()
	if err != nil {
		return batchJSON{}, nil, fmt.Errorf("%s: %w", name, err)
	}
	// The vector is only written if it reads back as itself.
	back, err := logrec.DecodeBatch(rec)
	if err != nil {
		return batchJSON{}, nil, fmt.Errorf("%s: decode: %w", name, err)
	}
	again, err := back.Encode()
	if err != nil || !bytes.Equal(again, rec) {
		return batchJSON{}, nil, fmt.Errorf("%s: does not round-trip", name)
	}
	topic, _ := logrec.Topic(b.Stream)
	j := batchJSON{
		Name: name, Note: note, Producer: h(b.Producer), Stream: b.Stream, Seq: b.Seq,
		Prev: h(b.Prev[:]), Timestamp: b.Timestamp, Count: b.Count, Encoding: b.Encoding,
		Profile: b.Profile, Size: b.Size, Expanded: h([]byte(expanded)), Topic: topic,
		Record: h(rec), RecordLen: len(rec),
	}
	if b.Content != nil {
		j.Content = h(b.Content)
	}
	if b.Locator != nil {
		j.Locator = locJSON(b.Locator)
	}
	for _, p := range b.Extra {
		v, _ := cbor.Encode(p.Val)
		j.Extra = append(j.Extra, extraKeyJSON{Key: p.Key.(uint64), Value: h(v)})
	}
	return j, rec, nil
}

func locJSON(l *logrec.Locator) *locatorJSON {
	enc, _ := l.Encode()
	return &locatorJSON{Digest: h(l.Digest[:]), Length: l.Length, URLs: l.URLs, Unsupported: l.Unsupported(), Encoding: h(enc)}
}

// Generate returns every vector file, keyed by its name under
// testdata/vectors.
func Generate() (map[string][]byte, error) {
	files := map[string][]byte{}
	prod := Producer()

	// Batches.
	full := &logrec.Batch{
		Producer: prod, Stream: Stream, Seq: 2, Prev: commitment(1), Timestamp: t0 + 1000,
		Count: 3, Encoding: logrec.EncodingLines, Profile: logrec.ProfileFull,
		Content: []byte(linesContent), Size: uint64(len(linesContent)),
	}
	gz, _ := hex.DecodeString(gzipContent)
	gzb := &logrec.Batch{
		Producer: prod, Stream: Stream, Seq: 3, Prev: commitment(2), Timestamp: t0 + 2000,
		Count: 3, Encoding: logrec.EncodingLinesGzip, Profile: logrec.ProfileFull,
		Content: gz, Size: uint64(len(gzipExpanded)),
	}
	if out, err := logrec.Expand(gzb, gz); err != nil || string(out) != gzipExpanded {
		return nil, fmt.Errorf("gzip vector does not expand: %v", err)
	}
	digestContent := []byte(linesContent)
	loc := &logrec.Locator{
		Digest: sha256.Sum256(digestContent), Length: uint64(len(digestContent)),
		URLs: []string{"https://logs.example.com/blobs/" + h(func() []byte { d := sha256.Sum256(digestContent); return d[:] }())},
	}
	dig := &logrec.Batch{
		Producer: prod, Stream: Stream, Seq: 4, Prev: commitment(3), Timestamp: t0 + 3000,
		Count: 3, Encoding: logrec.EncodingLines, Profile: logrec.ProfileDigest,
		Locator: loc, Size: uint64(len(digestContent)),
	}
	first := &logrec.Batch{
		Producer: prod, Stream: Stream, Seq: 1, Timestamp: t0,
		Count: 1, Encoding: logrec.EncodingLines, Profile: logrec.ProfileFull,
		Content: []byte("{\"level\":\"info\",\"msg\":\"stream opened\"}\n"), Size: 39,
		Extra: cbor.Map{{Key: uint64(24), Val: "a key this version does not define"}},
	}

	var batches []batchJSON
	for _, v := range []struct {
		name, note string
		b          *logrec.Batch
		exp        string
	}{
		{"batch-first", "seq 1: prev is all zero; carries unknown key 24, which a reader preserves and ignores", first, string(first.Content)},
		{"batch-lines", "full-bytes, lines; the third line carries ESC [2J, which the record carries verbatim and a renderer strips", full, linesContent},
		{"batch-gzip", "full-bytes, lines compressed as one gzip member", gzb, gzipExpanded},
		{"batch-digest", "digest-only: the locator names the lines content of batch-lines by SHA-256 and length", dig, linesContent},
	} {
		j, rec, err := batchVector(v.name, v.note, v.b, v.exp)
		if err != nil {
			return nil, err
		}
		batches = append(batches, j)
		files[v.name+".hex"] = []byte(h(rec) + "\n")
	}
	if err := putJSON(files, "batch-v1.json", map[string]any{
		"description": "Batch records (docs/spec.md section 3). record is the canonical CBOR a carrier's one field holds; expanded is the decoded content.",
		"batches":     batches,
	}); err != nil {
		return nil, err
	}

	// Locators alone.
	unk := &logrec.Locator{
		Digest: loc.Digest, Length: loc.Length, URLs: []string{},
		Extra: cbor.Map{{Key: "region", Val: "example"}},
	}
	if err := putJSON(files, "locator-v1.json", map[string]any{
		"description": "Digest-only locators (docs/spec.md section 4), encoded alone as they appear under batch key 10.",
		"locators": []any{
			map[string]any{"name": "locator-hints", "locator": locJSON(loc)},
			map[string]any{"name": "locator-unknown-member", "note": "an unknown member makes the locator unsupported, never the batch", "locator": locJSON(unk)},
		},
	}); err != nil {
		return nil, err
	}
	lenc, _ := loc.Encode()
	files["locator-hints.hex"] = []byte(h(lenc) + "\n")

	// Anchors.
	anchors, err := anchorVectors(files, prod)
	if err != nil {
		return nil, err
	}
	if err := putJSON(files, "anchor-v1.json", map[string]any{
		"description": "Interval anchors (docs/spec.md section 5). leaf = SHA-256(0x00 || commitment); the root is RFC 6962 over the commitments in seq order; each path runs leaf to root, left meaning the sibling is on the left. Commitments are stand-in carrier txids in hash byte order.",
		"anchors":     anchors,
	}); err != nil {
		return nil, err
	}

	// Derivations and tags.
	der, err := derivationVector(prod)
	if err != nil {
		return nil, err
	}
	if err := putJSON(files, "derivation-v1.json", der); err != nil {
		return nil, err
	}

	// Refusals.
	ref, err := refusalVectors(prod)
	if err != nil {
		return nil, err
	}
	if err := putJSON(files, "refusal-v1.json", map[string]any{
		"description": "Byte strings the record decoder refuses, each with its reason (docs/spec.md section 3.2). atAdmission says what a topic manager does with an output carrying them: refuse the transaction, or skip an output that does not claim to be a record (section 8.1). Every refusal is decided by the record alone. The prev refusals check prev against the record's own seq or interval (zero exactly on the first), never against another object: a break in the seq/prev link between batches (a missing predecessor, a prev naming no held batch, two batches at one seq) is a reported status, gap or fork (section 8.2), and never a refusal, so no vector here is one.",
		"refusals":    ref,
	}); err != nil {
		return nil, err
	}

	// Transactions.
	txv, err := transactionVectors()
	if err != nil {
		return nil, err
	}
	if err := putJSON(files, "transaction-v1.json", txv); err != nil {
		return nil, err
	}

	// Lookup questions.
	qv, err := queryVectors(prod)
	if err != nil {
		return nil, err
	}
	if err := putJSON(files, "query-v1.json", qv); err != nil {
		return nil, err
	}
	return files, nil
}

type stepJSON struct {
	Hash string `json:"hash"`
	Left bool   `json:"left"`
}

type leafJSON struct {
	Seq        uint64     `json:"seq"`
	Commitment string     `json:"commitment"`
	Leaf       string     `json:"leaf"`
	Path       []stepJSON `json:"path"`
}

func anchorVectors(files map[string][]byte, prod []byte) ([]any, error) {
	var out []any
	spans := []struct {
		interval    int
		first, n    int
		timestampMs uint64
	}{
		{1, 1, 5, t0 + 60000},
		{2, 6, 3, t0 + 120000},
	}
	for _, s := range spans {
		cs := make([][32]byte, s.n)
		for i := range cs {
			cs[i] = commitment(s.first + i)
		}
		a := &logrec.Anchor{
			Producer: prod, Stream: Stream, Interval: uint64(s.interval),
			First: uint64(s.first), Count: uint64(s.n), Root: logrec.IntervalRoot(cs),
			Timestamp: s.timestampMs,
		}
		if s.interval > 1 {
			a.Prev = anchorTxid(s.interval - 1)
		}
		rec, err := a.Encode()
		if err != nil {
			return nil, err
		}
		back, err := logrec.DecodeAnchor(rec)
		if err != nil {
			return nil, err
		}
		if again, _ := back.Encode(); !bytes.Equal(again, rec) {
			return nil, fmt.Errorf("anchor %d does not round-trip", s.interval)
		}
		var leaves []leafJSON
		for i, c := range cs {
			seq := uint64(s.first + i)
			p, err := logrec.ProveBatch(a, cs, seq)
			if err != nil {
				return nil, err
			}
			if !logrec.VerifyBatch(a, c, seq, p) {
				return nil, fmt.Errorf("anchor %d: path for seq %d does not verify", s.interval, seq)
			}
			lf := logrec.Leaf(c)
			lj := leafJSON{Seq: seq, Commitment: h(c[:]), Leaf: h(lf[:])}
			for _, st := range p {
				lj.Path = append(lj.Path, stepJSON{Hash: h(st.Hash[:]), Left: st.Left})
			}
			leaves = append(leaves, lj)
		}
		// The token's fields and what its field signature covers.
		signed := sha256.Sum256(append(append([]byte{}, logrec.TagAnchor...), rec...))
		name := fmt.Sprintf("anchor-interval-%d", s.interval)
		files[name+".hex"] = []byte(h(rec) + "\n")
		out = append(out, map[string]any{
			"name": name, "producer": h(prod), "stream": Stream, "interval": s.interval,
			"prev": h(a.Prev[:]), "first": s.first, "count": s.n, "timestamp": s.timestampMs,
			"root": h(a.Root[:]), "leaves": leaves, "record": h(rec), "recordLength": len(rec),
			"tokenFields":     []string{h(logrec.TagAnchor), h(rec)},
			"tokenSignedHash": h(signed[:]),
		})
	}
	return out, nil
}

func putJSON(files map[string][]byte, name string, v any) error {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		return err
	}
	files[name] = buf.Bytes()
	return nil
}

// Names returns the generated file names in order.
func Names(files map[string][]byte) []string {
	var n []string
	for k := range files {
		n = append(n, k)
	}
	sort.Strings(n)
	return n
}
