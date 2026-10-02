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
	"slices"
	"sort"
	"syscall"

	"github.com/lightwebinc/bcommon/bwallet"
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

	// PendingTrees are the funding trees signed for a pool coin that the
	// stream has not adopted yet, each with its kind and the coin it
	// spends: the record made before a tree reaches the settlement leg, so
	// a run that stops before the tree is adopted leaves what the next
	// command needs to ask the node what became of it. Adopting a tree
	// drops its record in the same save.
	PendingTrees []PendingTree `json:"pendingTrees,omitempty"`
	// Unpublished are the txids of trees adopted into Trees or AnchorTrees
	// whose publish to the hosts failed: publish pending. The next command
	// that spends publishes them before anything else.
	Unpublished []string `json:"unpublished,omitempty"`

	// Sweeps are the transactions built to spend funding-tree outputs, in
	// the order they were built: retractions and retirements. One not yet
	// Done is in flight, and the next sweep resumes it before anything else.
	Sweeps []Sweep `json:"sweeps,omitempty"`

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

// PendingTree is a funding tree signed for a pool coin and not yet adopted,
// keyed by the tree's txid.
type PendingTree struct {
	// Kind is which trees it belongs to: "batch" or "anchor".
	Kind string `json:"kind"`
	// Tree is the record the tree is adopted as, without a proof.
	Tree funding.Tree `json:"tree"`
	// Coin is the fee coin the tree spends, as the pool held it.
	Coin bwallet.Output `json:"coin"`
	// ReturnedOnce is set when a command found the tree absent from the
	// node and the coin unspent, and put the coin back: the record is kept
	// for one more command, in case the tree reaches the node after all.
	ReturnedOnce bool `json:"returned_once,omitempty"`
}

// KeepPendingTree records p, replacing a record of the same tree.
func (s *Stream) KeepPendingTree(p PendingTree) {
	if cur := s.PendingTree(p.Tree.Txid); cur != nil {
		*cur = p
		return
	}
	s.PendingTrees = append(s.PendingTrees, p)
}

// PendingTree finds the record of the tree txid, or nil.
func (s *Stream) PendingTree(txid string) *PendingTree {
	for i := range s.PendingTrees {
		if s.PendingTrees[i].Tree.Txid == txid {
			return &s.PendingTrees[i]
		}
	}
	return nil
}

// DropPendingTree drops the record of the tree txid, and reports whether
// there was one.
func (s *Stream) DropPendingTree(txid string) bool {
	n := len(s.PendingTrees)
	s.PendingTrees = slices.DeleteFunc(s.PendingTrees, func(p PendingTree) bool { return p.Tree.Txid == txid })
	return len(s.PendingTrees) != n
}

// MintedAhead reports whether txid is among the trees minted ahead and not
// adopted.
func (s *Stream) MintedAhead(txid string) bool {
	return slices.ContainsFunc(s.Ahead, func(t funding.Tree) bool { return t.Txid == txid })
}

// The kinds of sweep.
const (
	// SweepRetract spends outputs of a batch funding tree, used or not,
	// with bcommon carrier.Sweep: output 0 is a funding-shaped tombstone,
	// and every carrier that spent one of the outputs is retracted once it
	// mines (docs/spec.md section 6.4).
	SweepRetract = "retract"
	// SweepRetire spends only unused outputs of a tree no longer in use,
	// paying their value back to the coin pool. It retracts nothing.
	SweepRetire = "retire"
)

// Sweep is one mined transaction spending funding-tree outputs.
type Sweep struct {
	Kind  string   `json:"kind"`
	Tree  string   `json:"tree"`
	Vouts []uint32 `json:"vouts"`
	// Seqs are the batches it retracts, when they were named.
	Seqs   []uint64 `json:"seqs,omitempty"`
	Txid   string   `json:"txid"`
	RawHex string   `json:"rawHex"`
	// BeefHex is kept while it is unmined; BumpHex and Height once it
	// mined.
	BeefHex string `json:"beefHex,omitempty"`
	BumpHex string `json:"bumpHex,omitempty"`
	Height  uint32 `json:"height,omitempty"`
	// Publish is set when the hosts hold what it spends, so it goes to the
	// stream's topic once mined; a tree minted ahead and never published
	// is retired without it.
	Publish   bool `json:"publish,omitempty"`
	Submitted bool `json:"submitted,omitempty"`
	Done      bool `json:"done,omitempty"`
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
