package vectors

import (
	"context"
	"fmt"
	"math/big"
	"strings"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-sdk/wallet"

	"github.com/lightwebinc/bcommon/cbor"
	"github.com/lightwebinc/bcommon/pushdrop"

	"github.com/lightwebinc/blogs/internal/logrec"
)

func derivationVector(prod []byte) (map[string]any, error) {
	priv, _ := ec.PrivateKeyFromBytes(FixedKey)
	w, err := wallet.NewProtoWallet(wallet.ProtoWalletArgs{Type: wallet.ProtoWalletArgsTypePrivateKey, PrivateKey: priv})
	if err != nil {
		return nil, err
	}
	forSelf := true
	var keys []map[string]string
	for _, d := range []pushdrop.Derivation{logrec.BatchDerivation, logrec.AnchorDerivation, logrec.FundDerivation} {
		if err := d.Validate(); err != nil {
			return nil, err
		}
		reader, err := d.ExpectedLockingKey(priv.PubKey())
		if err != nil {
			return nil, err
		}
		r, err := w.GetPublicKey(context.Background(), wallet.GetPublicKeyArgs{
			EncryptionArgs: wallet.EncryptionArgs{ProtocolID: d.Protocol, KeyID: d.KeyID, Counterparty: pushdrop.Anyone()},
			ForSelf:        &forSelf,
		}, "")
		if err != nil {
			return nil, err
		}
		if !r.PublicKey.IsEqual(reader) {
			return nil, fmt.Errorf("derivation %s: the producer's and the reader's keys differ", d.KeyID)
		}
		keys = append(keys, map[string]string{
			"keyId":      d.KeyID,
			"invoice":    fmt.Sprintf("%d-%s-%s", d.Protocol.SecurityLevel, d.Protocol.Protocol, d.KeyID),
			"lockingKey": h(reader.Compressed()),
		})
	}
	topic, _ := logrec.Topic(Stream)
	var streams []map[string]any
	for _, id := range []string{Stream, OtherStream} {
		name, suffix, err := logrec.SplitStream(id)
		if err != nil {
			return nil, err
		}
		t, _ := logrec.Topic(id)
		streams = append(streams, map[string]any{"stream": id, "name": name, "suffix": suffix, "topic": t})
	}
	return map[string]any{
		"description":   "Registry values and the keys they derive for the fixed test key (32 bytes of 0x42), counterparty anyone, forSelf true on the producer's side (docs/spec.md section 6).",
		"identityKey":   h(prod),
		"protocol":      []any{1, logrec.ProtocolName},
		"keys":          keys,
		"tagAnchor":     h(logrec.TagAnchor),
		"tagFunding":    h(logrec.TagFunding),
		"magicBatch":    h(logrec.MagicBatch),
		"magicAnchor":   h(logrec.MagicAnchor),
		"topic":         topic,
		"streams":       streams,
		"streamsNote":   "Two streams with one readable part and different suffixes: different streams, different topics. Suffixes here are fixed examples; a real stream draws its suffix at random when it is created.",
		"maxStreamName": logrec.MaxStreamName,
		"suffixLength":  logrec.SuffixLen,
		"lookupService": logrec.LookupService,
	}, nil
}

type refusalJSON struct {
	Name   string `json:"name"`
	Kind   string `json:"kind"`
	Hex    string `json:"hex"`
	Reason string `json:"reason"`
	// AtAdmission is what a topic manager does with an output whose field
	// is these bytes: "refuse" the transaction, or "skip" the output
	// because it does not claim to be a record of this kind.
	AtAdmission string `json:"atAdmission"`
}

// validBatchMap is the batch-first record as a raw map, for mutation.
func validBatchMap(prod []byte) cbor.Map {
	return cbor.Map{
		{Key: uint64(0), Val: logrec.MagicBatch},
		{Key: uint64(1), Val: prod},
		{Key: uint64(2), Val: Stream},
		{Key: uint64(3), Val: uint64(1)},
		{Key: uint64(4), Val: make([]byte, 32)},
		{Key: uint64(5), Val: uint64(t0)},
		{Key: uint64(6), Val: uint64(1)},
		{Key: uint64(7), Val: uint64(1)},
		{Key: uint64(8), Val: uint64(1)},
		{Key: uint64(9), Val: []byte("x\n")},
		{Key: uint64(11), Val: uint64(2)},
	}
}

func validAnchorMap(prod []byte) cbor.Map {
	return cbor.Map{
		{Key: uint64(0), Val: logrec.MagicAnchor},
		{Key: uint64(1), Val: prod},
		{Key: uint64(2), Val: Stream},
		{Key: uint64(3), Val: uint64(1)},
		{Key: uint64(4), Val: make([]byte, 32)},
		{Key: uint64(5), Val: uint64(1)},
		{Key: uint64(6), Val: uint64(1)},
		{Key: uint64(7), Val: make([]byte, 32)},
		{Key: uint64(8), Val: uint64(t0)},
	}
}

func set(m cbor.Map, k uint64, v cbor.Value) cbor.Map {
	out := append(cbor.Map{}, m...)
	for i := range out {
		if out[i].Key == k {
			out[i].Val = v
			return out
		}
	}
	return append(out, cbor.Pair{Key: k, Val: v})
}

func del(m cbor.Map, k uint64) cbor.Map {
	var out cbor.Map
	for _, p := range m {
		if p.Key != k {
			out = append(out, p)
		}
	}
	return out
}

func loc(digestLen int, urls []cbor.Value) cbor.Map {
	return cbor.Map{
		{Key: "digest", Val: make([]byte, digestLen)},
		{Key: "length", Val: uint64(2)},
		{Key: "urls", Val: urls},
	}
}

// offCurveKey is the first compressed key 02 || x, x = 1, 2, ..., that
// go-sdk refuses to parse: an x with no point on the curve.
func offCurveKey() []byte {
	for x := int64(1); ; x++ {
		k := make([]byte, 33)
		k[0] = 0x02
		big.NewInt(x).FillBytes(k[1:])
		if _, err := ec.ParsePubKey(k); err != nil {
			return k
		}
	}
}

func enc1(v cbor.Value) []byte {
	b, err := cbor.Encode(v)
	if err != nil {
		panic(err)
	}
	return b
}

func refusalVectors(prod []byte) ([]refusalJSON, error) {
	enc := func(m cbor.Map) []byte {
		b, err := cbor.Encode(m)
		if err != nil {
			panic(err)
		}
		return b
	}
	vb := validBatchMap(prod)
	va := validAnchorMap(prod)
	good := enc(vb)
	p, _ := new(big.Int).SetString("fffffffffffffffffffffffffffffffffffffffffffffffffffffffefffffc2f", 16)
	xIsP := make([]byte, 33)
	xIsP[0] = 0x02
	p.FillBytes(xIsP[1:])
	// x = p + 1 reduces to x = 1, which is on the curve: a parser that
	// reduces x accepts it as an alias of 02 || 1.
	xIsP1 := make([]byte, 33)
	xIsP1[0] = 0x02
	new(big.Int).Add(p, big.NewInt(1)).FillBytes(xIsP1[1:])
	if _, err := ec.ParsePubKey(xIsP1); err != nil {
		return nil, fmt.Errorf("the alias vector no longer parses in go-sdk: %v", err)
	}
	many := append(cbor.Map{}, vb...)
	for k := uint64(24); len(many) <= logrec.MaxKeys; k++ {
		many = append(many, cbor.Pair{Key: k, Val: uint64(0)})
	}
	digestOnly := set(del(vb, 9), 8, uint64(2))
	uncompressedPrefix := append([]byte{0x04}, prod[1:]...)

	// A non-canonical seq: 1 written with a one-byte argument (0x18 0x01).
	// The map is assembled pair by pair, in the canonical key order vb is
	// already in, so only that one item differs.
	ncb := []byte{0xa0 | byte(len(vb))}
	for _, pr := range vb {
		ncb = append(ncb, enc1(pr.Key)...)
		if pr.Key == uint64(3) {
			ncb = append(ncb, 0x18, 0x01)
			continue
		}
		ncb = append(ncb, enc1(pr.Val)...)
	}
	// The same record as an indefinite-length map.
	indefinite := append([]byte{0xbf}, append(append([]byte{}, good[1:]...), 0xff)...)

	cases := []struct {
		name, kind string
		b          []byte
		reason     string
	}{
		{"seq-non-canonical-integer", "batch", ncb, "cbor"},
		{"trailing-byte", "batch", append(append([]byte{}, good...), 0x00), "cbor"},
		{"indefinite-length-map", "batch", indefinite, "cbor"},
		{"array-not-map", "batch", enc1([]cbor.Value{logrec.MagicBatch}), "cbor"},
		{"text-key", "batch", enc(append(append(cbor.Map{}, vb...), cbor.Pair{Key: "x", Val: uint64(1)})), "key-type"},
		{"sixty-five-keys", "batch", enc(many), "too-large"},
		{"order-text-key-before-magic", "batch", enc(append(set(vb, 0, []byte("xxxx")), cbor.Pair{Key: "x", Val: uint64(1)})), "key-type"},
		{"order-producer-before-locator", "batch", enc(set(set(digestOnly, 1, xIsP1), 10, loc(31, []cbor.Value{}))), "producer"},
		{"order-type-key-1-before-missing-key-3", "batch", enc(del(set(vb, 1, "not bytes"), 3)), "type"},
		{"order-prev-before-count", "batch", enc(set(set(vb, 4, append([]byte{1}, make([]byte, 31)...)), 6, uint64(3))), "prev"},
		{"wrong-magic", "batch", enc(set(vb, 0, []byte{'b', 'f', 'r', 0x01})), "magic"},
		{"anchor-magic-in-batch", "batch", enc(set(vb, 0, logrec.MagicAnchor)), "magic"},
		{"count-missing", "batch", enc(del(vb, 6)), "missing"},
		{"stream-as-bytes", "batch", enc(set(vb, 2, []byte(Stream))), "type"},
		{"seq-zero", "batch", enc(set(vb, 3, uint64(0))), "range"},
		{"seq-past-2^53-1", "batch", enc(set(vb, 3, uint64(logrec.MaxSafe+1))), "range"},
		{"timestamp-zero", "batch", enc(set(vb, 5, uint64(0))), "range"},
		{"prev-nonzero-on-seq-1", "batch", enc(set(vb, 4, append([]byte{1}, make([]byte, 31)...))), "prev"},
		{"prev-zero-on-seq-2", "batch", enc(set(vb, 3, uint64(2))), "prev"},
		{"prev-31-bytes", "batch", enc(set(vb, 4, make([]byte, 31))), "range"},
		{"producer-x-equals-p", "batch", enc(set(vb, 1, xIsP)), "producer"},
		{"producer-x-is-p-plus-1-alias", "batch", enc(set(vb, 1, xIsP1)), "producer"},
		{"producer-off-curve", "batch", enc(set(vb, 1, offCurveKey())), "producer"},
		{"producer-prefix-04", "batch", enc(set(vb, 1, uncompressedPrefix)), "producer"},
		{"stream-uppercase", "batch", enc(set(vb, 2, "Example_app_qzxkvbmwtr")), "stream"},
		{"stream-double-underscore", "batch", enc(set(vb, 2, "example__app_qzxkvbmwtr")), "stream"},
		{"stream-leading-underscore", "batch", enc(set(vb, 2, "_example_qzxkvbmwtr")), "stream"},
		{"stream-digit", "batch", enc(set(vb, 2, "app2_qzxkvbmwtr")), "stream"},
		{"stream-name-33-bytes", "batch", enc(set(vb, 2, strings.Repeat("a", 33)+"_qzxkvbmwtr")), "stream"},
		{"stream-name-only", "batch", enc(set(vb, 2, "example_app")), "stream"},
		{"stream-suffix-9-letters", "batch", enc(set(vb, 2, "example_app_qzxkvbmwt")), "stream"},
		{"stream-suffix-11-letters", "batch", enc(set(vb, 2, "example_app_qzxkvbmwtrs")), "stream"},
		{"stream-suffix-uppercase", "batch", enc(set(vb, 2, "example_app_Qzxkvbmwtr")), "stream"},
		{"stream-suffix-digit", "batch", enc(set(vb, 2, "example_app_qzxkvbmwt7")), "stream"},
		{"stream-suffix-only", "batch", enc(set(vb, 2, "_qzxkvbmwtr")), "stream"},
		{"count-above-size", "batch", enc(set(vb, 6, uint64(3))), "range"},
		{"size-not-content-length", "batch", enc(set(set(vb, 11, uint64(3)), 6, uint64(1))), "content"},
		{"full-with-locator", "batch", enc(set(del(vb, 9), 10, loc(32, []cbor.Value{}))), "profile-shape"},
		{"full-with-both", "batch", enc(set(vb, 10, loc(32, []cbor.Value{}))), "profile-shape"},
		{"digest-without-locator", "batch", enc(set(vb, 8, uint64(2))), "profile-shape"},
		{"content-empty", "batch", enc(set(vb, 9, []byte{})), "range"},
		{"locator-digest-31", "batch", enc(set(digestOnly, 10, loc(31, []cbor.Value{}))), "locator"},
		{"locator-http-url", "batch", enc(set(digestOnly, 10, loc(32, []cbor.Value{"http://logs.example.com/x"}))), "locator"},
		{"locator-url-with-space", "batch", enc(set(digestOnly, 10, loc(32, []cbor.Value{"https://logs.example.com/a b"}))), "locator"},
		{"locator-five-urls", "batch", enc(set(digestOnly, 10, loc(32, []cbor.Value{"https://a.example", "https://b.example", "https://c.example", "https://d.example", "https://e.example"}))), "locator"},
		{"locator-urls-missing", "batch", enc(set(digestOnly, 10, cbor.Map{{Key: "digest", Val: make([]byte, 32)}, {Key: "length", Val: uint64(2)}})), "locator"},
		{"anchor-first-not-1-on-interval-1", "anchor", enc(set(va, 5, uint64(2))), "range"},
		{"anchor-count-zero", "anchor", enc(set(va, 6, uint64(0))), "range"},
		{"anchor-count-65537", "anchor", enc(set(va, 6, uint64(logrec.MaxIntervalBatches+1))), "range"},
		{"anchor-prev-zero-on-interval-2", "anchor", enc(set(set(va, 3, uint64(2)), 5, uint64(9))), "prev"},
		{"anchor-root-missing", "anchor", enc(del(va, 7)), "missing"},
		{"anchor-stream-name-only", "anchor", enc(set(va, 2, "example_app")), "stream"},
		{"anchor-over-1024-bytes", "anchor", enc(append(append(cbor.Map{}, va...), cbor.Pair{Key: uint64(9), Val: strings.Repeat("a", 1000)})), "too-large"},
	}
	var out []refusalJSON
	for _, c := range cases {
		var err error
		if c.kind == "batch" {
			_, err = logrec.DecodeBatch(c.b)
		} else {
			_, err = logrec.DecodeAnchor(c.b)
		}
		if err == nil {
			return nil, fmt.Errorf("refusal %s: accepted", c.name)
		}
		if got := logrec.Reason(err); got != c.reason {
			return nil, fmt.Errorf("refusal %s: reason %s, want %s (%v)", c.name, got, c.reason, err)
		}
		at := "refuse"
		if c.kind == "batch" && !logrec.IsBatch(c.b) {
			at = "skip"
		}
		out = append(out, refusalJSON{Name: c.name, Kind: c.kind, Hex: h(c.b), Reason: c.reason, AtAdmission: at})
	}
	return out, nil
}
