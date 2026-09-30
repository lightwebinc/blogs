package produce

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/lightwebinc/blogs/internal/logrec"
	"github.com/lightwebinc/blogs/internal/state"
)

// encode writes raw content under an encoding. Gzip is one member with
// MTIME zero and no name, comment or extra field (docs/spec.md section 3.1).
func encode(raw []byte, encoding uint64) ([]byte, error) {
	switch encoding {
	case logrec.EncodingLines:
		return raw, nil
	case logrec.EncodingLinesGzip:
		var b bytes.Buffer
		zw, err := gzip.NewWriterLevel(&b, gzip.BestCompression)
		if err != nil {
			return nil, err
		}
		if _, err := zw.Write(raw); err != nil {
			return nil, err
		}
		if err := zw.Close(); err != nil {
			return nil, err
		}
		return b.Bytes(), nil
	}
	return nil, fmt.Errorf("encoding %d is not one this producer writes", encoding)
}

// split cuts LF-terminated content into two halves at a line boundary. It
// fails on a single line.
func split(raw []byte) ([]byte, []byte, bool) {
	mid := len(raw) / 2
	i := bytes.IndexByte(raw[mid:], '\n')
	if i >= 0 && mid+i+1 < len(raw) {
		return raw[:mid+i+1], raw[mid+i+1:], true
	}
	j := bytes.LastIndexByte(raw[:mid], '\n')
	if j >= 0 {
		return raw[:j+1], raw[j+1:], true
	}
	return nil, nil, false
}

func countLines(raw []byte) int { return bytes.Count(raw, []byte{'\n'}) }

// ValidURLPrefix checks that a locator hint built from prefix is one the
// locator rules accept: https, printable ASCII, and room for the digest.
func ValidURLPrefix(prefix string) error {
	u := prefix + strings.Repeat("0", 64)
	if len(u) > logrec.MaxURL || !strings.HasPrefix(u, "https://") || len(u) == len("https://") {
		return fmt.Errorf("url prefix %q: a hint is https:// and at most %d bytes with the 64-character digest", prefix, logrec.MaxURL)
	}
	for i := 0; i < len(u); i++ {
		if u[i] < 0x21 || u[i] > 0x7e {
			return fmt.Errorf("url prefix %q: printable ASCII only, no spaces", prefix)
		}
	}
	return nil
}

// store writes a digest-only batch's content to ContentDir under its
// SHA-256 in hex and returns its locator. The file is written before the
// record names it, so the bytes exist wherever the record reaches.
func (e *Engine) store(content []byte) (*logrec.Locator, error) {
	if e.Opts.ContentDir == "" {
		return nil, fmt.Errorf("digest-only batches need a content directory")
	}
	d := sha256.Sum256(content)
	name := hex.EncodeToString(d[:])
	p := filepath.Join(e.Opts.ContentDir, name)
	if _, err := os.Stat(p); err != nil {
		if err := state.WriteAtomic(p, content, 0o644); err != nil {
			return nil, fmt.Errorf("content %s: %w", name, err)
		}
	}
	urls := []string{}
	for _, u := range e.Opts.URLs {
		urls = append(urls, u+name)
	}
	return &logrec.Locator{Digest: d, Length: uint64(len(content)), URLs: urls}, nil
}
