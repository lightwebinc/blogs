package vectors

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

const dir = "../../testdata/vectors"

// TestVectors regenerates every vector and compares it byte for byte with
// the checked-in file, and refuses a checked-in file nothing generates.
func TestVectors(t *testing.T) {
	files, err := Generate()
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range Names(files) {
		have, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatalf("%s: %v (run: GOWORK=off go run ./cmd/vectors)", name, err)
		}
		if !bytes.Equal(have, files[name]) {
			t.Errorf("%s differs from what the codec generates", name)
		}
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if _, ok := files[e.Name()]; !ok {
			t.Errorf("%s: nothing generates it", e.Name())
		}
	}
}

// TestDeterministic runs the generator twice.
func TestDeterministic(t *testing.T) {
	a, err := Generate()
	if err != nil {
		t.Fatal(err)
	}
	b, _ := Generate()
	for k := range a {
		if !bytes.Equal(a[k], b[k]) {
			t.Errorf("%s differs between runs", k)
		}
	}
}
