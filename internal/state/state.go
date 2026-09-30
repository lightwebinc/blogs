// Package state is a producer's durable record of one stream: its head
// (seq and commitment), the carrier persisted before it is published, the
// commitments not yet anchored, the anchor chain's head and the anchor in
// flight, and the funding trees carriers and anchors spend.
//
// A producer that loses this file loses its head, and a producer that has
// lost its head must not restart the stream (docs/spec.md section 12): it
// creates a new one. So the file is written atomically (a temporary file,
// fsync, rename, fsync of the directory) at mode 0600, and a stream's
// directory is locked while a command writes it.
package state

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"syscall"

	"github.com/lightwebinc/bcommon/funding"
)

// Version is the state file's format version.
const Version = 1

// Stream is one stream's state.
type Stream struct {
	Version int `json:"version"`
	// Stream is the full identifier <name>_<suffix>.
	Stream string `json:"stream"`
	// Identity is the producer's identity key, compressed hex.
	Identity string `json:"identity"`

	// Seq is the last seq built; Head its commitment, hash byte order hex.
	// Zero and empty before the first batch.
	Seq  uint64 `json:"seq"`
	Head string `json:"head,omitempty"`
	// Pending is the carrier of Seq while it may not have reached the
	// facade: persisted before the publish, cleared after it. A run that
	// finds it publishes these same bytes again, never a new carrier for
	// the same seq.
	Pending string `json:"pending,omitempty"`

	// Unanchored are the commitments of seq NextFirst to Seq, hash byte
	// order hex, in seq order: what the next anchors cover.
	Unanchored []string `json:"unanchored,omitempty"`
	// NextFirst is the first seq no mined and published anchor covers.
	NextFirst uint64 `json:"nextFirst"`

	// Anchor is the last anchor token mined and published: the head of the
	// anchor chain. Nil before the first.
	Anchor *Anchor `json:"anchor,omitempty"`
	// Inflight is the anchor built and handed to the settlement leg that
	// has not yet been published. At most one is in flight.
	Inflight *Anchor `json:"inflight,omitempty"`
	// LastAnchorMs is when the last anchor was built, Unix milliseconds.
	LastAnchorMs int64 `json:"lastAnchorMs,omitempty"`

	// Tree and Trees are the current batch funding tree and every one
	// minted; AnchorTree and AnchorTrees the same for the trees anchors
	// spend their fee from. Ahead are trees minted ahead and not adopted
	// when a run ended: on the chain, in no other list, kept so a sweep
	// finds them.
	Tree        *funding.Tree  `json:"tree,omitempty"`
	Trees       []funding.Tree `json:"trees,omitempty"`
	AnchorTree  *funding.Tree  `json:"anchorTree,omitempty"`
	AnchorTrees []funding.Tree `json:"anchorTrees,omitempty"`
	Ahead       []funding.Tree `json:"ahead,omitempty"`

	// Tail is where a file being followed was read to.
	Tail *Tail `json:"tail,omitempty"`

	path string
}

// Anchor is an anchor token the producer built.
type Anchor struct {
	Interval uint64 `json:"interval"`
	First    uint64 `json:"first"`
	Count    uint64 `json:"count"`
	Txid     string `json:"txid"`
	Vout     uint32 `json:"vout"`
	RawHex   string `json:"rawHex"`
	// BeefHex is kept while the token is unmined; BumpHex and Height once
	// it mined.
	BeefHex string `json:"beefHex,omitempty"`
	BumpHex string `json:"bumpHex,omitempty"`
	Height  uint32 `json:"height,omitempty"`
	// Submitted is set once the settlement leg accepted the token.
	Submitted bool `json:"submitted,omitempty"`
}

// Tail is a followed file's position.
type Tail struct {
	Path   string `json:"path"`
	Dev    uint64 `json:"dev"`
	Ino    uint64 `json:"ino"`
	Offset int64  `json:"offset"`
}

// Dir is the directory of a stream's state under home.
func Dir(home, stream string) string { return filepath.Join(home, "streams", stream) }

func file(home, stream string) string { return filepath.Join(Dir(home, stream), "state.json") }

// ErrNoStream is a stream this home holds no state for.
var ErrNoStream = errors.New("state: no such stream in this home")

// ErrExists is a stream this home already holds.
var ErrExists = errors.New("state: stream already exists in this home")

// Create writes the state of a new stream and refuses one that exists.
func Create(home, stream, identity string) (*Stream, error) {
	dir := Dir(home, stream)
	if _, err := os.Stat(file(home, stream)); err == nil {
		return nil, fmt.Errorf("%w: %s", ErrExists, stream)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	s := &Stream{Version: Version, Stream: stream, Identity: identity, NextFirst: 1, path: file(home, stream)}
	return s, s.Save()
}

// Load reads a stream's state.
func Load(home, stream string) (*Stream, error) {
	p := file(home, stream)
	raw, err := os.ReadFile(p)
	if errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("%w: %s", ErrNoStream, stream)
	}
	if err != nil {
		return nil, err
	}
	var s Stream
	if err := json.Unmarshal(raw, &s); err != nil {
		return nil, fmt.Errorf("state %s: %w", p, err)
	}
	if s.Version != Version {
		return nil, fmt.Errorf("state %s: version %d, this blogs reads %d", p, s.Version, Version)
	}
	if s.Stream != stream {
		return nil, fmt.Errorf("state %s: names stream %q", p, s.Stream)
	}
	if s.NextFirst+uint64(len(s.Unanchored)) != s.Seq+1 {
		return nil, fmt.Errorf("state %s: %d unanchored commitments from seq %d do not reach seq %d", p, len(s.Unanchored), s.NextFirst, s.Seq)
	}
	s.path = p
	return &s, nil
}

// List names the streams under home, sorted.
func List(home string) ([]string, error) {
	ents, err := os.ReadDir(filepath.Join(home, "streams"))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range ents {
		if e.IsDir() {
			out = append(out, e.Name())
		}
	}
	sort.Strings(out)
	return out, nil
}

// Save writes the state atomically at mode 0600.
func (s *Stream) Save() error {
	raw, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	return WriteAtomic(s.path, append(raw, '\n'), 0o600)
}

// WriteAtomic writes data to path through a temporary file in the same
// directory, synced and renamed, then syncs the directory, so a crash
// leaves either the old file or the new one.
func WriteAtomic(path string, data []byte, perm os.FileMode) (err error) {
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = os.Remove(f.Name())
		}
	}()
	if err = f.Chmod(perm); err != nil {
		f.Close()
		return err
	}
	if _, err = f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(f.Name(), path); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// ErrLocked is a lock another process holds.
var ErrLocked = errors.New("state: locked by another blogs process")

// Lock takes an exclusive lock on path (created at 0600), without waiting.
// The lock is the kernel's and ends with the process, so a crash never
// leaves it held. Release it with the returned function.
func Lock(path string) (func(), error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("%w: %s", ErrLocked, path)
		}
		return nil, err
	}
	return func() {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		f.Close()
	}, nil
}
