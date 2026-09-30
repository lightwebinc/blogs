// Package archive is a storage adapter for one overlay host: it follows the
// host's ls_log answers for one producer's stream, checks every answer as a
// reader does, and writes the batches, the canonical anchor tokens and the
// proof of each interval to local files, so that the stream outlives the
// host and an interval can be proved from the files alone.
//
// On-disk layout, under Dir/<stream>/<producer>/ (docs/usage.md, Archive):
//
//	archive.json                    the cursor: the next seq to walk to and
//	                                the last interval proven
//	batches/<seq>-<carrier>.beef    each verified carrier, as the host
//	                                answered it: its BEEF carries the
//	                                funding tree it spends, with the tree's
//	                                proof
//	batches/<seq>-<carrier>.json    what was read from it
//	content/<seq>-<carrier>.log     its decoded lines, when the content was
//	                                read (full-bytes, or found for a
//	                                digest-only batch)
//	anchors/<interval>-<token>.beef each canonical anchor token, with its
//	                                proof
//	intervals/<interval>.json       the interval's proof: the token, its
//	                                height, the root, and every seq's
//	                                commitment with its RFC 6962 inclusion
//	                                path
//
// <seq> and <interval> are 16 decimal digits, zero padded, so a directory
// listing is in order; <carrier> and <token> are txids. Every file is
// written atomically and never rewritten, so a pass that stops part way is
// continued by the next, and two archives of the same stream hold the same
// bytes.
package archive

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/lightwebinc/bcommon/commit"

	"github.com/lightwebinc/blogs/internal/logrec"
	"github.com/lightwebinc/blogs/internal/reader"
	"github.com/lightwebinc/blogs/internal/state"
)

// Version is the cursor's format version.
const Version = 1

// Cursor is archive.json.
type Cursor struct {
	Version  int    `json:"version"`
	Stream   string `json:"stream"`
	Producer string `json:"producer"`
	// Next is the first seq the walk has not found held.
	Next uint64 `json:"next"`
	// Intervals is the last interval proven; 1 to Intervals are.
	Intervals uint64 `json:"intervals"`
	// Hosts are the hosts this archive has followed, in first-use order.
	Hosts []string `json:"hosts"`
}

// BatchFile is batches/<seq>-<carrier>.json.
type BatchFile struct {
	Seq       uint64 `json:"seq"`
	Carrier   string `json:"carrier"`
	Prev      string `json:"prev"`
	Timestamp uint64 `json:"timestamp"`
	Count     uint64 `json:"count"`
	Encoding  uint64 `json:"encoding"`
	Profile   uint64 `json:"profile"`
	Size      uint64 `json:"size"`
	// Funding is the outpoint the carrier spends, txid:vout.
	Funding string `json:"funding"`
	Host    string `json:"host"`
	// Content is ok (content/ holds the lines), pending, unsupported or
	// invalid.
	Content string `json:"content"`
}

// IntervalFile is intervals/<interval>.json.
type IntervalFile struct {
	Interval  uint64 `json:"interval"`
	First     uint64 `json:"first"`
	Count     uint64 `json:"count"`
	Root      string `json:"root"`
	Token     string `json:"token"`
	Vout      int    `json:"vout"`
	Height    uint32 `json:"height"`
	Position  uint64 `json:"position"`
	PrevToken string `json:"prevToken,omitempty"`
	// Status is proven (every batch held) or proven-with-holes (Holes are
	// seqs whose commitment was recovered from the next batch's prev, whose
	// bytes this archive does not hold: retracted, or never delivered).
	Status string   `json:"status"`
	Holes  []uint64 `json:"holes,omitempty"`
	Leaves []Leaf   `json:"leaves"`
}

// Leaf is one position of an interval.
type Leaf struct {
	Seq        uint64 `json:"seq"`
	Commitment string `json:"commitment"`
	Held       bool   `json:"held"`
	// Path is the RFC 6962 inclusion path, leaf to root: each step the
	// sibling's hash in hex, prefixed L: when it sits to the left.
	Path []string `json:"path"`
}

// Archive follows one host into one directory.
type Archive struct {
	// Dir is the archive's root; one stream's files are under
	// Dir/<stream>/<producer>.
	Dir      string
	Host     string
	Stream   string
	Producer []byte
	// Client asks Host and checks answers against its Headers; its Asset
	// answers the funding trees an anchor's fee inputs spend.
	Client *reader.Client
	// Fetch finds a digest-only batch's content.
	Fetch reader.Fetch
	// Note receives one line per thing done.
	Note func(format string, args ...any)
}

// Summary is what one pass did.
type Summary struct {
	Stored    int
	Next      uint64
	Intervals uint64
	// Proven are the intervals proven in this pass.
	Proven []*IntervalFile
	// Stopped says why the interval walk stopped, when it did short of
	// what the host holds.
	Stopped string
	// Refused counts answers refused.
	Refused int
}

func (a *Archive) note(format string, args ...any) {
	if a.Note != nil {
		a.Note(format, args...)
	}
}

// Root is the stream's directory.
func (a *Archive) Root() string {
	return filepath.Join(a.Dir, a.Stream, hex.EncodeToString(a.Producer))
}

func (a *Archive) path(parts ...string) string {
	return filepath.Join(append([]string{a.Root()}, parts...)...)
}

// LoadCursor reads the cursor, or starts one.
func (a *Archive) LoadCursor() (*Cursor, error) {
	raw, err := os.ReadFile(a.path("archive.json"))
	if errors.Is(err, os.ErrNotExist) {
		return &Cursor{Version: Version, Stream: a.Stream, Producer: hex.EncodeToString(a.Producer), Next: 1}, nil
	}
	if err != nil {
		return nil, err
	}
	var c Cursor
	if err := json.Unmarshal(raw, &c); err != nil {
		return nil, fmt.Errorf("%s: %w", a.path("archive.json"), err)
	}
	switch {
	case c.Version != Version:
		return nil, fmt.Errorf("%s: version %d, this blogs reads %d", a.path("archive.json"), c.Version, Version)
	case c.Stream != a.Stream || c.Producer != hex.EncodeToString(a.Producer):
		return nil, fmt.Errorf("%s: names another stream or producer", a.path("archive.json"))
	}
	return &c, nil
}

func writeJSON(path string, v any) error {
	raw, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return write(path, append(raw, '\n'))
}

func write(path string, b []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return state.WriteAtomic(path, b, 0o644)
}

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func pad(n uint64) string { return fmt.Sprintf("%016d", n) }

// Pass walks the host once: every seq from the cursor on until the host
// holds nothing, then every interval from the last proven on until one
// cannot be proven, filling the seqs each interval covers.
func (a *Archive) Pass(ctx context.Context) (*Summary, error) {
	cur, err := a.LoadCursor()
	if err != nil {
		return nil, err
	}
	if !contains(cur.Hosts, a.Host) {
		cur.Hosts = append(cur.Hosts, a.Host)
	}
	one := *a.Client
	one.Hosts = []string{a.Host}
	s := &Summary{}
	for seq := cur.Next; seq <= logrec.MaxSafe; seq++ {
		n, held, err := a.fetch(ctx, seq, s)
		if err != nil {
			return nil, err
		}
		s.Stored += n
		if !held {
			break
		}
		cur.Next = seq + 1
	}
	if err := writeJSON(a.path("archive.json"), cur); err != nil {
		return nil, err
	}
	var prev *reader.Token
	if cur.Intervals > 0 {
		if prev, err = a.token(ctx, cur.Intervals); err != nil {
			return nil, err
		}
	}
	for k := cur.Intervals + 1; k <= logrec.MaxSafe; k++ {
		tok, rep, err := one.Next(ctx, a.Stream, a.Producer, k, prev, nil)
		if err != nil {
			return nil, err
		}
		s.Refused += len(rep.Refusals)
		if rep.Outcome != reader.Verified {
			if rep.Outcome == reader.Refused || !strings.Contains(rep.Reason, "no host answered") {
				s.Stopped = rep.Reason
			}
			break
		}
		for seq := tok.Anchor.First; seq <= tok.Anchor.Last(); seq++ {
			if len(a.held(seq)) > 0 {
				continue
			}
			n, _, err := a.fetch(ctx, seq, s)
			if err != nil {
				return nil, err
			}
			s.Stored += n
		}
		f, why, err := a.prove(ctx, tok, prev)
		if err != nil {
			return nil, err
		}
		if f == nil {
			s.Stopped = fmt.Sprintf("interval %d: %s", k, why)
			break
		}
		beef, err := tok.Tx.AtomicBEEF(false)
		if err != nil {
			return nil, err
		}
		if err := write(a.path("anchors", pad(k)+"-"+tok.Txid+".beef"), beef); err != nil {
			return nil, err
		}
		if err := writeJSON(a.path("intervals", pad(k)+".json"), f); err != nil {
			return nil, err
		}
		cur.Intervals = k
		if err := writeJSON(a.path("archive.json"), cur); err != nil {
			return nil, err
		}
		s.Proven = append(s.Proven, f)
		a.note("interval %d: %s, seqs %d to %d, root %s, token %s at height %d", k, f.Status, f.First, f.First+f.Count-1, f.Root, f.Token, f.Height)
		prev = tok
	}
	s.Next, s.Intervals = cur.Next, cur.Intervals
	return s, nil
}

func contains(ss []string, s string) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
}

// fetch asks the host for seq and stores every verified candidate not yet
// stored. held reports whether the host answered anything verified.
func (a *Archive) fetch(ctx context.Context, seq uint64, s *Summary) (stored int, held bool, err error) {
	c := *a.Client
	c.Hosts = []string{a.Host}
	cands, refused, err := c.Batch(ctx, a.Stream, a.Producer, seq, false)
	if err != nil {
		return 0, false, err
	}
	for _, v := range refused {
		s.Refused++
		a.note("seq %d: %s %s", seq, v.Code, v.Reason)
	}
	for _, cand := range cands {
		n, err := a.store(ctx, cand)
		if err != nil {
			return stored, true, err
		}
		stored += n
	}
	return stored, len(cands) > 0, nil
}

// store writes one verified carrier, its record and its content.
func (a *Archive) store(ctx context.Context, cand *reader.Candidate) (int, error) {
	b := cand.Carrier.Batch
	id := cand.Tx.TxID().String()
	name := pad(b.Seq) + "-" + id
	if exists(a.path("batches", name+".json")) {
		return 0, nil
	}
	if err := write(a.path("batches", name+".beef"), cand.Beef); err != nil {
		return 0, err
	}
	status := "ok"
	content, err := reader.Content(ctx, b, a.Fetch)
	switch {
	case err == nil:
		if err := write(a.path("content", name+".log"), content); err != nil {
			return 0, err
		}
	case errors.Is(err, reader.ErrUnsupported):
		status = "unsupported"
	case errors.Is(err, reader.ErrPending):
		status = "pending"
	default:
		status = "invalid"
	}
	f := BatchFile{Seq: b.Seq, Carrier: id, Prev: chainhash.Hash(b.Prev).String(), Timestamp: b.Timestamp, Count: b.Count,
		Encoding: b.Encoding, Profile: b.Profile, Size: b.Size, Funding: cand.Carrier.Funding.String(), Host: cand.Host, Content: status}
	if err := writeJSON(a.path("batches", name+".json"), f); err != nil {
		return 0, err
	}
	a.note("seq %d: carrier %s stored, content %s", b.Seq, id, status)
	return 1, nil
}

// held reads back the carriers stored for seq, checked again by the
// carrier rules (their proofs were checked when they were stored).
func (a *Archive) held(seq uint64) []*reader.Candidate {
	names, _ := filepath.Glob(a.path("batches", pad(seq)+"-*.beef"))
	sort.Strings(names)
	c := &reader.Client{}
	var out []*reader.Candidate
	for _, n := range names {
		beef, err := os.ReadFile(n)
		if err != nil {
			continue
		}
		cand, v := c.CheckCarrier(context.Background(), reader.Answer{Host: "file", Beef: beef}, a.Stream, a.Producer, seq)
		if v.OK() {
			out = append(out, cand)
		}
	}
	return out
}

// Interval reads intervals/<k>.json.
func (a *Archive) Interval(k uint64) (*IntervalFile, error) {
	var f IntervalFile
	raw, err := os.ReadFile(a.path("intervals", pad(k)+".json"))
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(raw, &f); err != nil {
		return nil, fmt.Errorf("%s: %w", a.path("intervals", pad(k)+".json"), err)
	}
	return &f, nil
}

// token reads back interval k's stored canonical token, its proof checked
// against the headers again.
func (a *Archive) token(ctx context.Context, k uint64) (*reader.Token, error) {
	f, err := a.Interval(k)
	if err != nil {
		return nil, err
	}
	beef, err := os.ReadFile(a.path("anchors", pad(k)+"-"+f.Token+".beef"))
	if err != nil {
		return nil, err
	}
	t, why, err := a.Client.Token(ctx, reader.Answer{Host: "file", Beef: beef, Output: uint32(f.Vout)}, a.Stream, a.Producer, k) //nolint:gosec // an output index
	if err != nil {
		return nil, err
	}
	if why != "" {
		return nil, fmt.Errorf("interval %d: the stored token no longer verifies: %s", k, why)
	}
	return t, nil
}

// prove builds the interval's proof from the stored batches: one
// candidate per seq, chosen from the last seq backwards by each batch's
// prev, a seq held by no file taking its commitment from the next batch's
// prev (spec section 8.4), and the root over them equal to the token's.
// A nil file comes with the reason it could not be proven.
func (a *Archive) prove(_ context.Context, tok, prev *reader.Token) (*IntervalFile, string, error) {
	an := tok.Anchor
	held := map[uint64][]*reader.Candidate{}
	for seq := an.First; seq <= an.Last(); seq++ {
		held[seq] = a.held(seq)
	}
	tails := held[an.Last()]
	if len(tails) == 0 {
		return nil, fmt.Sprintf("seq %d, the interval's last, is held by no file and no later batch in the interval names it", an.Last()), nil
	}
	var why string
	for _, tail := range tails {
		cs := make([][32]byte, an.Count)
		heldAt := make([]bool, an.Count)
		cs[an.Count-1], heldAt[an.Count-1] = tail.Carrier.Commitment, true
		next := tail.Carrier.Batch.Prev
		nextKnown := true
		ok := true
		for i := int(an.Count) - 2; i >= 0; i-- {
			seq := an.First + uint64(i)
			var pick *reader.Candidate
			for _, c := range held[seq] {
				if nextKnown && c.Carrier.Commitment == next {
					pick = c
				}
			}
			switch {
			case pick == nil && nextKnown && len(held[seq]) == 0:
				// A hole: its commitment is the next batch's prev.
				cs[i], nextKnown = next, false
				continue
			case pick == nil && !nextKnown && len(held[seq]) == 1:
				pick = held[seq][0]
			case pick == nil:
				ok = false
				why = fmt.Sprintf("seq %d: no stored batch links to seq %d", seq, seq+1)
			}
			if !ok {
				break
			}
			cs[i], heldAt[i] = pick.Carrier.Commitment, true
			next, nextKnown = pick.Carrier.Batch.Prev, true
		}
		if !ok {
			continue
		}
		if logrec.IntervalRoot(cs) != an.Root {
			why = "root-mismatch: the stored batches do not reproduce the anchor's root"
			continue
		}
		f := &IntervalFile{Interval: an.Interval, First: an.First, Count: an.Count, Root: hex.EncodeToString(an.Root[:]),
			Token: tok.Txid, Vout: tok.Vout, Height: tok.Height, Position: tok.Index, Status: "proven"}
		if prev != nil {
			f.PrevToken = prev.Txid
		}
		for i := range cs {
			seq := an.First + uint64(i)
			path, err := logrec.ProveBatch(an, cs, seq)
			if err != nil {
				return nil, "", err
			}
			l := Leaf{Seq: seq, Commitment: chainhash.Hash(cs[i]).String(), Held: heldAt[i]}
			for _, st := range path {
				h := hex.EncodeToString(st.Hash[:])
				if st.Left {
					h = "L:" + h
				}
				l.Path = append(l.Path, h)
			}
			if !heldAt[i] {
				f.Holes = append(f.Holes, seq)
			}
			f.Leaves = append(f.Leaves, l)
		}
		if len(f.Holes) > 0 {
			f.Status = "proven-with-holes"
		}
		return f, "", nil
	}
	return nil, why, nil
}

// Anchor rebuilds the anchor record an interval file describes, enough to
// check its paths: the root and the covered range.
func (f *IntervalFile) Anchor() (*logrec.Anchor, error) {
	root, err := hex.DecodeString(f.Root)
	if err != nil || len(root) != 32 {
		return nil, errors.New("interval file: root is not 32 bytes of hex")
	}
	return &logrec.Anchor{Interval: f.Interval, First: f.First, Count: f.Count, Root: [32]byte(root)}, nil
}

// Verify checks every leaf's path against the file's root at its seq, so an
// interval file can be checked on its own, with no host and no network.
func (f *IntervalFile) Verify() error {
	an, err := f.Anchor()
	if err != nil {
		return err
	}
	if uint64(len(f.Leaves)) != f.Count {
		return fmt.Errorf("interval %d: %d leaves for a count of %d", f.Interval, len(f.Leaves), f.Count)
	}
	for i, l := range f.Leaves {
		h, err := chainhash.NewHashFromHex(l.Commitment)
		if err != nil || l.Seq != f.First+uint64(i) {
			return fmt.Errorf("interval %d: leaf %d is malformed", f.Interval, i)
		}
		var path commit.Path
		for _, st := range l.Path {
			left := strings.HasPrefix(st, "L:")
			b, err := hex.DecodeString(strings.TrimPrefix(st, "L:"))
			if err != nil || len(b) != 32 {
				return fmt.Errorf("interval %d: seq %d: a path step is not 32 bytes of hex", f.Interval, l.Seq)
			}
			path = append(path, commit.Step{Hash: [32]byte(b), Left: left})
		}
		if !logrec.VerifyBatch(an, [32]byte(*h), l.Seq, path) {
			return fmt.Errorf("interval %d: seq %d: the path does not lead to the root", f.Interval, l.Seq)
		}
	}
	return nil
}
