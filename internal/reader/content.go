package reader

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/lightwebinc/blogs/internal/logrec"
)

// Fetch is how a reader obtains a digest-only batch's content (docs/spec.md
// section 9.2): from local directories holding files named by the digest in
// hex, or over HTTPS from a locator's hint whose host is on Allow. A hint
// whose host is not on the list is never fetched.
type Fetch struct {
	Dirs  []string
	Allow []string
	// HTTP is the client for hints; nil is a client with no proxy that
	// follows no redirect off the origin. Its CheckRedirect is replaced
	// either way.
	HTTP    *http.Client
	Timeout time.Duration
}

// ErrPending is content no configured source had: the batch is valid, its
// bytes are elsewhere.
var ErrPending = errors.New("content pending: no configured source holds it")

// Get returns the bytes a locator names, checked against its length and
// digest. Bytes that fail are discarded.
func (f Fetch) Get(ctx context.Context, l *logrec.Locator) ([]byte, error) {
	name := hex.EncodeToString(l.Digest[:])
	var last error
	for _, d := range f.Dirs {
		b, err := readAtMost(filepath.Join(d, name), int64(l.Length))
		if err != nil {
			continue
		}
		if err := logrec.CheckFetched(l, b); err != nil {
			last = err
			continue
		}
		return b, nil
	}
	for _, u := range l.URLs {
		p, err := url.Parse(u)
		if err != nil || p.Scheme != "https" || !slices.Contains(f.Allow, p.Hostname()) {
			continue
		}
		b, err := f.get(ctx, p, int64(l.Length))
		if err != nil {
			last = err
			continue
		}
		if err := logrec.CheckFetched(l, b); err != nil {
			last = err
			continue
		}
		return b, nil
	}
	if last != nil {
		return nil, fmt.Errorf("%w (last failure: %v)", ErrPending, last)
	}
	return nil, ErrPending
}

func readAtMost(path string, n int64) ([]byte, error) {
	fh, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer fh.Close()
	return io.ReadAll(io.LimitReader(fh, n+1))
}

func (f Fetch) get(ctx context.Context, u *url.URL, n int64) ([]byte, error) {
	c := &http.Client{Transport: &http.Transport{Proxy: nil}, Timeout: f.Timeout}
	if f.HTTP != nil {
		cp := *f.HTTP
		c = &cp
	}
	origin := u.Scheme + "://" + u.Host
	c.CheckRedirect = func(req *http.Request, _ []*http.Request) error {
		if req.URL.Scheme+"://"+req.URL.Host != origin {
			return fmt.Errorf("redirect off the origin to %s", req.URL.Host)
		}
		return nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s answered %d", u.Host, resp.StatusCode)
	}
	return io.ReadAll(io.LimitReader(resp.Body, n+1))
}

// Content returns a batch's decoded content: the carried bytes, or the
// fetched ones for a digest-only batch, decoded and held to size and count.
// An unsupported profile, encoding or locator is reported as such, never
// as a refusal: the batch is still a valid link.
func Content(ctx context.Context, b *logrec.Batch, f Fetch) ([]byte, error) {
	if !b.Supported() {
		return nil, ErrUnsupported
	}
	enc := b.Content
	if b.Profile == logrec.ProfileDigest {
		var err error
		if enc, err = f.Get(ctx, b.Locator); err != nil {
			return nil, err
		}
	}
	return logrec.Expand(b, enc)
}

// ErrUnsupported is a batch whose profile, encoding or locator this version
// does not read.
var ErrUnsupported = errors.New("content unsupported by this version")
