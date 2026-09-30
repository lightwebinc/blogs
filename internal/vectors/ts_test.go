package vectors

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"

	"github.com/lightwebinc/bcommon/cbor"
	"github.com/lightwebinc/bcommon/commit"

	"github.com/lightwebinc/blogs/internal/logrec"
)

// TestTypeScriptWrittenVectors reads what the TypeScript codec wrote
// (host/src/tsvectors.ts): every record decodes and re-encodes to the same
// bytes, the stream draw reproduces from the same random bytes, and the
// canonical script and the interval root rebuild identically.
func TestTypeScriptWrittenVectors(t *testing.T) {
	var v struct {
		StreamDraw struct {
			Name, Random, Stream string
		} `json:"streamDraw"`
		Batches []struct {
			Name      string   `json:"name"`
			Seq       uint64   `json:"seq"`
			Prev      string   `json:"prev"`
			Timestamp uint64   `json:"timestamp"`
			Count     uint64   `json:"count"`
			Profile   uint64   `json:"profile"`
			Size      uint64   `json:"size"`
			ExtraKeys []uint64 `json:"extraKeys"`
			Record    string   `json:"record"`
		} `json:"batches"`
		Locator struct {
			Record      string `json:"record"`
			Unsupported bool   `json:"unsupported"`
		} `json:"locator"`
		Anchor struct {
			Interval    uint64   `json:"interval"`
			First       uint64   `json:"first"`
			Count       uint64   `json:"count"`
			Root        string   `json:"root"`
			Commitments []string `json:"commitments"`
			Path        []struct {
				Hash string `json:"hash"`
				Left bool   `json:"left"`
			} `json:"path"`
			PathSeq   uint64   `json:"pathSeq"`
			ExtraKeys []uint64 `json:"extraKeys"`
			Record    string   `json:"record"`
		} `json:"anchor"`
		PushDrop struct {
			Key       string   `json:"key"`
			Fields    []string `json:"fields"`
			Signature string   `json:"signature"`
			Script    string   `json:"script"`
		} `json:"pushDrop"`
		CBORCheck string `json:"cborCheck"`
	}
	b, err := os.ReadFile("../../testdata/ts/ts-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &v); err != nil {
		t.Fatal(err)
	}
	unhex := func(s string) []byte {
		t.Helper()
		out, err := hex.DecodeString(s)
		if err != nil {
			t.Fatal(err)
		}
		return out
	}

	stream, err := logrec.NewStream(v.StreamDraw.Name, bytes.NewReader(unhex(v.StreamDraw.Random)))
	if err != nil || stream != v.StreamDraw.Stream {
		t.Fatalf("stream draw: %q %v, TypeScript drew %q", stream, err, v.StreamDraw.Stream)
	}

	if len(v.Batches) == 0 {
		t.Fatal("no batches")
	}
	for _, w := range v.Batches {
		rec := unhex(w.Record)
		d, err := logrec.DecodeBatch(rec)
		if err != nil {
			t.Fatalf("%s: %v", w.Name, err)
		}
		if d.Seq != w.Seq || hex.EncodeToString(d.Prev[:]) != w.Prev || d.Timestamp != w.Timestamp || d.Count != w.Count ||
			d.Profile != w.Profile || d.Size != w.Size || d.Stream != v.StreamDraw.Stream {
			t.Errorf("%s: fields differ: %+v", w.Name, d)
		}
		var keys []uint64
		for _, p := range d.Extra {
			keys = append(keys, p.Key.(uint64))
		}
		if len(keys) != len(w.ExtraKeys) || (len(keys) > 0 && keys[0] != w.ExtraKeys[0]) {
			t.Errorf("%s: extra keys %v, want %v", w.Name, keys, w.ExtraKeys)
		}
		again, err := d.Encode()
		if err != nil || !bytes.Equal(again, rec) {
			t.Errorf("%s: does not re-encode byte for byte: %v", w.Name, err)
		}
	}

	l, err := logrec.DecodeLocator(unhex(v.Locator.Record))
	if err != nil {
		t.Fatal(err)
	}
	if l.Unsupported() != v.Locator.Unsupported {
		t.Error("locator support")
	}
	if again, err := l.Encode(); err != nil || !bytes.Equal(again, unhex(v.Locator.Record)) {
		t.Errorf("locator does not re-encode: %v", err)
	}

	a, err := logrec.DecodeAnchor(unhex(v.Anchor.Record))
	if err != nil {
		t.Fatal(err)
	}
	var cs [][32]byte
	for _, c := range v.Anchor.Commitments {
		cs = append(cs, [32]byte(unhex(c)))
	}
	if a.Interval != v.Anchor.Interval || a.First != v.Anchor.First || a.Count != v.Anchor.Count ||
		hex.EncodeToString(a.Root[:]) != v.Anchor.Root || logrec.IntervalRoot(cs) != a.Root {
		t.Errorf("anchor fields or root differ: %+v", a)
	}
	if len(a.Extra) != 1 || a.Extra[0].Key.(uint64) != v.Anchor.ExtraKeys[0] {
		t.Error("anchor extra keys")
	}
	var path commit.Path
	for _, s := range v.Anchor.Path {
		path = append(path, commit.Step{Hash: [32]byte(unhex(s.Hash)), Left: s.Left})
	}
	if !logrec.VerifyBatch(a, cs[v.Anchor.PathSeq-a.First], v.Anchor.PathSeq, path) {
		t.Error("the TypeScript path does not verify")
	}
	if again, err := a.Encode(); err != nil || !bytes.Equal(again, unhex(v.Anchor.Record)) {
		t.Errorf("anchor does not re-encode: %v", err)
	}

	key, err := ec.PublicKeyFromBytes(unhex(v.PushDrop.Key))
	if err != nil {
		t.Fatal(err)
	}
	var fields [][]byte
	for _, f := range v.PushDrop.Fields {
		fields = append(fields, unhex(f))
	}
	if got := logrec.PushDropScript(key, fields, unhex(v.PushDrop.Signature)); !bytes.Equal(got, unhex(v.PushDrop.Script)) {
		t.Errorf("script differs:\n go %x\n ts %s", got, v.PushDrop.Script)
	}

	if got, _ := cbor.Encode(cbor.Map{{Key: uint64(1), Val: "a"}}); hex.EncodeToString(got) != v.CBORCheck {
		t.Error("cbor check")
	}
}
