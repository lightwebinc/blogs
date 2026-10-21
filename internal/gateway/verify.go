// Package gateway is the read gateway: a Loki API in front of a stock Loki
// that marks every line it returns verified or unverified, checked against
// the interval roots an archive holds, and answers each stream's coverage
// (intervals proven, holes, retraction receipts). Grafana points at it as
// at Loki (docs/backend.md).
package gateway

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lightwebinc/blogs/internal/archive"
	"github.com/lightwebinc/blogs/internal/ingest"
	"github.com/lightwebinc/blogs/internal/reader"
)

// The marks.
const (
	Verified   = "verified"
	Unverified = "unverified"
)

// The reasons a line is unverified.
const (
	// ReasonPending: no interval proven in the archive covers the line's
	// batch yet.
	ReasonPending = "pending"
	// ReasonAltered: the line is not the one at its position in the batch
	// the root commits.
	ReasonAltered = "altered"
	// ReasonRelabeled: the line is, but the store's labels or time differ
	// from those the line carries.
	ReasonRelabeled = "relabeled"
	// ReasonRetracted: the batch was retracted; its receipt is in the
	// archive and the anchor still commits the position.
	ReasonRetracted = "retracted"
	// ReasonHole: the root commits the position and the archive never held
	// the batch.
	ReasonHole = "hole"
	// ReasonNoProvenance: the line carries no placement at all.
	ReasonNoProvenance = "no-provenance"
	// ReasonUnreadable: the archive's files for the batch could not be
	// read or did not verify, or, with a second archive, that archive's
	// interval file did not verify or disagrees on the root or the line's
	// path.
	ReasonUnreadable = "unreadable"
)

// Verdict is one line's mark.
type Verdict struct {
	State    string
	Reason   string
	Interval uint64
}

// Verifier checks lines against an archive directory (blogs archive -dir).
type Verifier struct {
	// Dir is the archive's root: streams are under Dir/<stream>/<producer>.
	Dir string
	// Fetch finds digest-only content.
	Fetch reader.Fetch
	// Cross, when set, is a second archive's root: a line is verified
	// only when Cross also holds a verified interval file covering its seq
	// that agrees with Dir's on the interval, its root and token, and the
	// line's commitment and inclusion path. Cross lacking the interval
	// marks the line pending; a file there that does not verify, or that
	// disagrees, marks it unreadable.
	Cross string
	// Rescan is how stale the interval list may be before a line past its
	// end makes it read again.
	Rescan time.Duration

	mu        sync.Mutex
	intervals map[string]*intervalList
	lines     map[string][]string
}

type intervalList struct {
	files   []*archive.IntervalFile
	scanned time.Time
	bad     map[uint64]string
}

// maxCachedBatches bounds the batches whose lines are kept between calls.
const maxCachedBatches = 4096

func (v *Verifier) root(stream, producer string) string {
	return filepath.Join(v.Dir, stream, producer)
}

// list returns the stream's proven intervals in the archive at dir, read
// again when seq is past the last and the list is older than Rescan.
func (v *Verifier) list(dir, stream, producer string, seq uint64) *intervalList {
	key := dir + "\x00" + stream + "/" + producer
	l := v.intervals[key]
	if l != nil {
		n := len(l.files)
		past := n == 0 || seq > l.files[n-1].First+l.files[n-1].Count-1
		if !past || time.Since(l.scanned) < v.Rescan {
			return l
		}
	}
	l = &intervalList{scanned: time.Now(), bad: map[uint64]string{}}
	names, _ := filepath.Glob(filepath.Join(dir, stream, producer, "intervals", "*.json"))
	sort.Strings(names)
	for _, n := range names {
		raw, err := os.ReadFile(n)
		if err != nil {
			continue
		}
		var f archive.IntervalFile
		if err := json.Unmarshal(raw, &f); err != nil {
			continue
		}
		if err := f.Verify(); err != nil {
			l.bad[f.Interval] = err.Error()
			continue
		}
		l.files = append(l.files, &f)
	}
	if v.intervals == nil {
		v.intervals = map[string]*intervalList{}
	}
	v.intervals[key] = l
	return l
}

func (l *intervalList) covering(seq uint64) *archive.IntervalFile {
	i := sort.Search(len(l.files), func(i int) bool { return l.files[i].First+l.files[i].Count-1 >= seq })
	if i < len(l.files) && l.files[i].First <= seq {
		return l.files[i]
	}
	return nil
}

func hexKey(s string) ([]byte, error) {
	b, err := hex.DecodeString(s)
	if err != nil || len(b) != 33 {
		return nil, fmt.Errorf("producer %q is not a compressed key in hex", s)
	}
	return b, nil
}

func pad(n uint64) string { return fmt.Sprintf("%016d", n) }

// Check marks one line the store answered: its placement (from structured
// metadata), its text, its time in nanoseconds as the store gives it, and
// the store's labels for it.
func (v *Verifier) Check(ctx context.Context, meta map[string]string, line, ts string, labels map[string]string) Verdict {
	stream, producer := meta[ingest.MetaStream], meta[ingest.MetaProducer]
	seq, err1 := strconv.ParseUint(meta[ingest.MetaSeq], 10, 64)
	idx, err2 := strconv.Atoi(meta[ingest.MetaLine])
	if stream == "" || producer == "" || err1 != nil || err2 != nil || idx < 1 ||
		strings.ContainsAny(stream+producer, "/\\.") {
		return Verdict{State: Unverified, Reason: ReasonNoProvenance}
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	root := v.root(stream, producer)
	l := v.list(v.Dir, stream, producer, seq)
	f := l.covering(seq)
	if f == nil {
		return Verdict{State: Unverified, Reason: ReasonPending}
	}
	leaf := f.Leaves[seq-f.First]
	out := Verdict{State: Unverified, Interval: f.Interval}
	if receipts, _ := filepath.Glob(filepath.Join(root, "retracted", pad(seq)+"-*.json")); len(receipts) > 0 || leaf.Retracted {
		out.Reason = ReasonRetracted
		return out
	}
	if !leaf.Held {
		out.Reason = ReasonHole
		return out
	}
	lines, err := v.batchLines(ctx, root, stream, producer, seq, leaf.Commitment)
	if err != nil {
		out.Reason = ReasonUnreadable
		return out
	}
	if idx > len(lines) || lines[idx-1] != line {
		out.Reason = ReasonAltered
		return out
	}
	rec, err := ingest.ParseLine([]byte(line))
	if err != nil {
		// A line not in the line form proves only its text.
		out.State, out.Reason = Verified, ""
		return v.crossCheck(out, f, stream, producer, seq)
	}
	if ts != "" && ts != strconv.FormatInt(rec.Ts, 10) {
		out.Reason = ReasonRelabeled
		return out
	}
	for k, want := range rec.Labels {
		if got, ok := labels[ingest.LabelName(k)]; ok && got != want {
			out.Reason = ReasonRelabeled
			return out
		}
	}
	out.State = Verified
	return v.crossCheck(out, f, stream, producer, seq)
}

// crossCheck holds a verified line to the second archive, when there is
// one: its interval file covering seq must verify and agree with f.
func (v *Verifier) crossCheck(out Verdict, f *archive.IntervalFile, stream, producer string, seq uint64) Verdict {
	if v.Cross == "" {
		return out
	}
	if why := v.crossDisagrees(f, stream, producer, seq); why != "" {
		out.State, out.Reason = Unverified, why
	}
	return out
}

// crossDisagrees is "" when the second archive agrees with f at seq, and
// otherwise the reason the line is not verified.
func (v *Verifier) crossDisagrees(f *archive.IntervalFile, stream, producer string, seq uint64) string {
	l := v.list(v.Cross, stream, producer, seq)
	if _, bad := l.bad[f.Interval]; bad {
		return ReasonUnreadable
	}
	g := l.covering(seq)
	if g == nil {
		return ReasonPending
	}
	if disagree(f, g, seq) != "" {
		return ReasonUnreadable
	}
	return ""
}

// disagree names what two archives' interval files covering seq differ
// on, or is "" when they agree on the interval, its root and token, and
// the commitment and inclusion path at seq.
func disagree(f, g *archive.IntervalFile, seq uint64) string {
	switch {
	case g.Interval != f.Interval || g.First != f.First || g.Count != f.Count:
		return fmt.Sprintf("seq %d: interval %d (seqs %d to %d) there, %d (seqs %d to %d) here", seq, g.Interval, g.First, g.First+g.Count-1, f.Interval, f.First, f.First+f.Count-1)
	case g.Root != f.Root:
		return fmt.Sprintf("interval %d: root %s there, %s here", f.Interval, g.Root, f.Root)
	case g.Token != f.Token:
		return fmt.Sprintf("interval %d: token %s there, %s here", f.Interval, g.Token, f.Token)
	}
	a, b := f.Leaves[seq-f.First], g.Leaves[seq-g.First]
	if a.Commitment != b.Commitment {
		return fmt.Sprintf("seq %d: commitment %s there, %s here", seq, b.Commitment, a.Commitment)
	}
	if strings.Join(a.Path, ",") != strings.Join(b.Path, ",") {
		return fmt.Sprintf("seq %d: the inclusion paths differ", seq)
	}
	return ""
}

// batchLines reads the batch the leaf commits from the archive, checks it
// by the carrier rules and that it is the committed carrier, and returns
// its lines.
func (v *Verifier) batchLines(ctx context.Context, root, stream, producer string, seq uint64, commitment string) ([]string, error) {
	key := root + "/" + pad(seq) + "-" + commitment
	if l, ok := v.lines[key]; ok {
		return l, nil
	}
	beef, err := os.ReadFile(filepath.Join(root, "batches", pad(seq)+"-"+commitment+".beef"))
	if err != nil {
		return nil, err
	}
	p, err := hexKey(producer)
	if err != nil {
		return nil, err
	}
	c := &reader.Client{}
	cand, verdict := c.CheckCarrier(ctx, reader.Answer{Host: "archive", Beef: beef}, stream, p, seq)
	if !verdict.OK() {
		return nil, fmt.Errorf("seq %d: %s", seq, verdict.Reason)
	}
	if cand.Tx.TxID().String() != commitment {
		return nil, fmt.Errorf("seq %d: the archived carrier is not the committed one", seq)
	}
	content, err := reader.Content(ctx, cand.Carrier.Batch, v.Fetch)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(strings.TrimSuffix(string(content), "\n"), "\n")
	if v.lines == nil || len(v.lines) >= maxCachedBatches {
		v.lines = map[string][]string{}
	}
	v.lines[key] = lines
	return lines, nil
}

// Coverage is one stream's state in the archive.
type Coverage struct {
	Stream    string    `json:"stream"`
	Producer  string    `json:"producer"`
	Intervals uint64    `json:"intervals"`
	Proven    []Span    `json:"proven"`
	Holes     []uint64  `json:"holes"`
	Retracted []Receipt `json:"retracted"`
	// Refused are interval files whose paths do not lead to their root.
	Refused map[string]string `json:"refused,omitempty"`
	// Cross is the second archive's agreement, when the gateway has one.
	Cross *Cross `json:"cross,omitempty"`
}

// Cross is how a second archive's interval files compare with the
// archive's, for the intervals the archive has proven.
type Cross struct {
	// Agree counts the intervals both hold and agree on.
	Agree uint64 `json:"agree"`
	// Missing are intervals the second archive has not proven: their
	// lines are pending.
	Missing []uint64 `json:"missing"`
	// Disagree are intervals whose files there do not verify or differ,
	// each with the first difference: their lines are unreadable.
	Disagree map[string]string `json:"disagree,omitempty"`
}

// Span is one proven interval.
type Span struct {
	Interval uint64 `json:"interval"`
	First    uint64 `json:"first"`
	Last     uint64 `json:"last"`
	Root     string `json:"root"`
	Token    string `json:"token"`
	Height   uint32 `json:"height"`
}

// Receipt is a retracted position and the sweep that retracted it.
type Receipt struct {
	Seq         uint64 `json:"seq"`
	Commitment  string `json:"commitment"`
	Sweep       string `json:"sweep"`
	SweepHeight uint32 `json:"sweepHeight"`
}

// Coverage reads every stream in the archive.
func (v *Verifier) Coverage() ([]Coverage, error) {
	dirs, err := filepath.Glob(filepath.Join(v.Dir, "*", "*", "intervals"))
	if err != nil {
		return nil, err
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	out := []Coverage{}
	for _, d := range dirs {
		root := filepath.Dir(d)
		producer, stream := filepath.Base(root), filepath.Base(filepath.Dir(root))
		delete(v.intervals, v.Dir+"\x00"+stream+"/"+producer)
		l := v.list(v.Dir, stream, producer, ^uint64(0))
		c := Coverage{Stream: stream, Producer: producer, Proven: []Span{}, Holes: []uint64{}, Retracted: []Receipt{}}
		for _, f := range l.files {
			c.Intervals = f.Interval
			c.Proven = append(c.Proven, Span{Interval: f.Interval, First: f.First, Last: f.First + f.Count - 1, Root: f.Root, Token: f.Token, Height: f.Height})
			c.Holes = append(c.Holes, f.Holes...)
		}
		for k, why := range l.bad {
			if c.Refused == nil {
				c.Refused = map[string]string{}
			}
			c.Refused[strconv.FormatUint(k, 10)] = why
		}
		if v.Cross != "" {
			c.Cross = v.crossCoverage(l, stream, producer)
		}
		names, _ := filepath.Glob(filepath.Join(root, "retracted", "*.json"))
		sort.Strings(names)
		for _, n := range names {
			raw, err := os.ReadFile(n)
			if err != nil {
				continue
			}
			var r archive.Receipt
			if json.Unmarshal(raw, &r) == nil {
				c.Retracted = append(c.Retracted, Receipt{Seq: r.Seq, Commitment: r.Commitment, Sweep: r.Sweep, SweepHeight: r.SweepHeight})
			}
		}
		out = append(out, c)
	}
	return out, nil
}

func (v *Verifier) crossCoverage(l *intervalList, stream, producer string) *Cross {
	delete(v.intervals, v.Cross+"\x00"+stream+"/"+producer)
	x := v.list(v.Cross, stream, producer, ^uint64(0))
	out := &Cross{Missing: []uint64{}}
	for _, f := range l.files {
		k := strconv.FormatUint(f.Interval, 10)
		if why, bad := x.bad[f.Interval]; bad {
			if out.Disagree == nil {
				out.Disagree = map[string]string{}
			}
			out.Disagree[k] = why
			continue
		}
		why := ""
		for seq := f.First; seq < f.First+f.Count && why == ""; seq++ {
			g := x.covering(seq)
			if g == nil {
				why = "missing"
				break
			}
			why = disagree(f, g, seq)
		}
		switch why {
		case "":
			out.Agree++
		case "missing":
			out.Missing = append(out.Missing, f.Interval)
		default:
			if out.Disagree == nil {
				out.Disagree = map[string]string{}
			}
			out.Disagree[k] = why
		}
	}
	return out
}

// Retained sums, per stream, the bytes the archive keeps for it in two
// kinds. content is the size of every batch's lines as recorded in the
// batch (the record's size field, so a digest-only batch counts the same
// whether or not this archive holds its content): what is billed.
// evidence is the carriers' BEEF on disk, each with the funding tree it
// spends: the cost of proof, measured and not billed. A retracted batch
// counts as neither once the archive has dropped it.
func (v *Verifier) Retained() map[string]map[string]int64 {
	out := map[string]map[string]int64{}
	dirs, _ := filepath.Glob(filepath.Join(v.Dir, "*", "*"))
	for _, root := range dirs {
		stream := filepath.Base(filepath.Dir(root))
		if out[stream] == nil {
			out[stream] = map[string]int64{"content": 0, "evidence": 0}
		}
		ents, _ := os.ReadDir(filepath.Join(root, "batches"))
		for _, e := range ents {
			name := e.Name()
			switch {
			case strings.HasSuffix(name, ".beef"):
				if fi, err := e.Info(); err == nil && fi.Mode().IsRegular() {
					out[stream]["evidence"] += fi.Size()
				}
			case strings.HasSuffix(name, ".json"):
				raw, err := os.ReadFile(filepath.Join(root, "batches", name))
				if err != nil {
					continue
				}
				var bf archive.BatchFile
				if json.Unmarshal(raw, &bf) == nil {
					out[stream]["content"] += int64(bf.Size) //nolint:gosec // a batch is bounded far below 2^63
				}
			}
		}
	}
	return out
}
