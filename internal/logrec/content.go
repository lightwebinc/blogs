package logrec

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
)

// CheckFetched holds bytes fetched for a digest-only batch to its locator:
// the length first, then the digest. Bytes that fail are discarded, never
// stored.
func CheckFetched(l *Locator, content []byte) error {
	if uint64(len(content)) != l.Length {
		return fmt.Errorf("%w: fetched length", ErrContent)
	}
	if sha256.Sum256(content) != l.Digest {
		return fmt.Errorf("%w: fetched digest", ErrContent)
	}
	return nil
}

// Expand decodes a batch's content (carried or fetched) under its encoding
// and checks it against the record: exactly Size bytes, exactly Count
// LF-terminated lines; valid UTF-8 is NOT required here (a renderer
// filters text, docs/spec.md section 10). Decompression stops at Size+1
// bytes, so a compressed bomb costs at most MaxExpanded+1 bytes of output.
func Expand(b *Batch, content []byte) ([]byte, error) {
	var out []byte
	switch b.Encoding {
	case EncodingLines:
		out = content
	case EncodingLinesGzip:
		// bytes.Reader is an io.ByteReader, so the decompressor reads
		// from it directly and consumes exactly the one member; what is
		// left in br afterwards is trailing bytes.
		br := bytes.NewReader(content)
		zr, err := gzip.NewReader(br)
		if err != nil {
			return nil, fmt.Errorf("%w: gzip: %v", ErrContent, err)
		}
		zr.Multistream(false)
		out, err = io.ReadAll(io.LimitReader(zr, int64(b.Size)+1))
		if err != nil {
			return nil, fmt.Errorf("%w: gzip: %v", ErrContent, err)
		}
		if uint64(len(out)) != b.Size {
			return nil, fmt.Errorf("%w: expanded size", ErrContent)
		}
		// Read to the member's end so its CRC and length trailer are
		// checked, then refuse anything after it.
		if n, err := zr.Read(make([]byte, 1)); n != 0 || !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("%w: gzip trailer", ErrContent)
		}
		if br.Len() != 0 {
			return nil, fmt.Errorf("%w: bytes after the gzip member", ErrContent)
		}
	default:
		return nil, fmt.Errorf("%w: unknown encoding", ErrContent)
	}
	if uint64(len(out)) != b.Size {
		return nil, fmt.Errorf("%w: size", ErrContent)
	}
	if len(out) == 0 || out[len(out)-1] != '\n' || uint64(bytes.Count(out, []byte{'\n'})) != b.Count {
		return nil, fmt.Errorf("%w: count", ErrContent)
	}
	return out, nil
}
