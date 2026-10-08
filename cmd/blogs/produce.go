package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/lightwebinc/bcommon/bwallet"
	"github.com/lightwebinc/bcommon/termsafe"

	"github.com/lightwebinc/blogs/internal/config"
	"github.com/lightwebinc/blogs/internal/limits"
	"github.com/lightwebinc/blogs/internal/logrec"
	"github.com/lightwebinc/blogs/internal/produce"
	"github.com/lightwebinc/blogs/internal/state"
)

const produceHelp = `usage: blogs produce <stream> [flags]

Read log lines from stdin (or -file), cut them into batches by size, line
count or time, and publish each batch once as a carrier on the stream's
topic, spending one output of a funding tree. Once -interval has passed
since the last anchor, anchor every published batch in one mined token,
published after it mines. At the end of the input the last batch is
published and, unless -anchor-wait is 0, anchored and waited for. An
interrupt publishes the batch being built and stops; an anchor in flight is
collected by the next produce or anchor.

A run persists each carrier before publishing it and republishes a carrier
it finds persisted, so a restart continues the stream at the next seq and
never builds two carriers for one. A funding tree an earlier run signed and
did not adopt is settled first: the chain view is asked what became of it, and a
tree on the chain and not yet mined is waited for, 10 minutes at most.

Needs where objects go: facade in mode plane, or hosts in mode unicast,
where each host is sent every object, retried on its own, and the run ends
with what each host took and missed and the backfill that repairs a host
that missed something. The broadcaster (settle), chain view (chain) and
header source (header_url) default to public services on mainnet and
testnet, so no node is needed.`

// repeated is a flag given several times.
type repeated []string

func (r *repeated) String() string     { return strings.Join(*r, ",") }
func (r *repeated) Set(v string) error { *r = append(*r, v); return nil }

// produceFlags are produce's flags, shared in part with anchor.
type produceFlags struct {
	file, profile, encoding, contentDir, unsafe string
	urls                                        repeated
	follow                                      bool
	maxBytes, maxLines, anchorBatches           int
	maxWait, interval, poll, anchorWait         time.Duration
	maxRate                                     float64
	treeCount, anchorTreeCount                  int
	treeSats, anchorTreeSats                    uint64
	ahead, anchorAhead                          uint
}

func engineFlags(fs *flag.FlagSet, f *produceFlags) {
	fs.DurationVar(&f.poll, "poll", 5*time.Second, "how often proofs are asked for")
	fs.IntVar(&f.treeCount, "tree-count", limits.DefaultTreeCount, fmt.Sprintf("outputs per batch funding tree (each carrier's BEEF carries its tree; at most %d)", limits.MaxTreeCount))
	fs.Uint64Var(&f.treeSats, "tree-sats", 1, "satoshis per batch funding output (a carrier carries its output's value)")
	fs.UintVar(&f.ahead, "ahead", 25, "mint the next batch tree when this few outputs are left")
	fs.IntVar(&f.anchorTreeCount, "anchor-tree-count", 8, fmt.Sprintf("outputs per anchor fee tree (at most %d)", limits.MaxTreeCount))
	fs.Uint64Var(&f.anchorTreeSats, "anchor-tree-sats", 1000, "satoshis per anchor fee output (one pays an anchor's fee)")
	fs.UintVar(&f.anchorAhead, "anchor-ahead", 2, "mint the next anchor fee tree when this few outputs are left")
	fs.IntVar(&f.anchorBatches, "anchor-batches", limits.DefaultAnchorBatches, fmt.Sprintf("anchor early once this many batches are unanchored; the most one anchor covers (at most %d)", limits.MaxAnchorBatches))
}

// checkLimits applies docs/limits.md to a producer's settings: a hard cap
// is a usage error naming the limit, a recommendation a warning. Settings a
// command does not take (anchor and sweep read no input) are checked at
// their defaults.
func (g *global) checkLimits(f *produceFlags, o produce.Options, legs produce.Legs) error {
	p := limits.Producer{Plane: legs.Hosts == nil, Digest: o.Profile == logrec.ProfileDigest, Gzip: o.Encoding == logrec.EncodingLinesGzip,
		MaxBytes: f.maxBytes, MaxLines: f.maxLines, MaxWait: f.maxWait, Interval: f.interval, AnchorBatches: f.anchorBatches,
		Rate: f.maxRate, TreeCount: f.treeCount, AnchorTreeCount: f.anchorTreeCount, ObjectBound: g.cfg.ObjectBound}
	if legs.Hosts != nil {
		p.Hosts, p.Need = len(legs.Hosts.Hosts), legs.Hosts.Need
	}
	if p.MaxBytes == 0 {
		p.MaxBytes = limits.DefaultMaxBytes(p.Plane, p.Digest, p.Gzip)
	}
	if p.MaxLines == 0 {
		p.MaxLines = limits.DefaultMaxLines
	}
	if p.MaxWait == 0 {
		p.MaxWait = limits.DefaultMaxWait
	}
	if p.Interval == 0 {
		p.Interval = limits.DefaultInterval
	}
	if p.Rate == 0 {
		p.Rate = limits.DefaultRate
	}
	warnings, err := p.Check()
	if err != nil {
		return usage("%v", err)
	}
	for _, w := range warnings {
		g.say("warning: %s", w)
	}
	return nil
}

func (g *global) engine(ctx context.Context, stream string, f *produceFlags, o produce.Options) (*produce.Engine, func(), error) {
	if err := checkEngineFlags(stream, f); err != nil {
		return nil, nil, err
	}
	legs, err := g.legs()
	if err != nil {
		return nil, nil, err
	}
	if err := g.checkLimits(f, o, legs); err != nil {
		return nil, nil, err
	}
	w, err := g.openWallet()
	if err != nil {
		return nil, nil, err
	}
	unlock, err := g.lockHome()
	if err != nil {
		return nil, nil, err
	}
	e, err := g.openEngine(ctx, stream, f, o, w, legs)
	if err != nil {
		unlock()
		return nil, nil, err
	}
	return e, unlock, nil
}

// checkEngineFlags checks what an engine's flags must be before anything
// is opened.
func checkEngineFlags(stream string, f *produceFlags) error {
	if err := logrec.CheckStream(stream); err != nil {
		return usage("%q is not a stream identifier <name>_<suffix>", stream)
	}
	if f.treeCount < 1 || f.anchorTreeCount < 1 || f.treeSats < 1 || f.anchorTreeSats < 1 {
		return usage("tree counts and satoshis must be at least 1")
	}
	if f.poll <= 0 {
		return usage("-poll must be positive")
	}
	return nil
}

// openEngine starts an engine on a stream of a home the caller has locked,
// with the wallet and legs the caller opened: one process may drive
// several streams of one home this way.
func (g *global) openEngine(ctx context.Context, stream string, f *produceFlags, o produce.Options, w *bwallet.Embedded, legs produce.Legs) (*produce.Engine, error) {
	st, err := state.Load(g.cfg.Home, stream)
	if err != nil {
		if errors.Is(err, state.ErrNoStream) {
			return nil, usage("this home holds no stream %s (create one with `blogs stream new <name>`; a producer that lost a stream's state starts a new stream, never the old one again)", stream)
		}
		return nil, err
	}
	fees, from, err := g.fees(ctx, legs.Arcade)
	if err != nil {
		return nil, err
	}
	if from != "" {
		g.say("fee rate %s sat/bytes from %s", fees.Rate, from)
	}
	o.Poll, o.ObjectBound, o.Fees = f.poll, g.cfg.ObjectBound, fees
	if o.Wait == 0 {
		o.Wait = 10 * time.Minute
	}
	o.TreeCount, o.TreeSats, o.Ahead = f.treeCount, f.treeSats, uint32(f.ahead)
	o.AnchorTreeCount, o.AnchorTreeSats, o.AnchorAhead = f.anchorTreeCount, f.anchorTreeSats, uint32(f.anchorAhead)
	o.AnchorBatches = f.anchorBatches
	e, err := produce.New(st, w.Signer(), w.Pool, legs, o)
	if err != nil {
		return nil, usage("%v", err)
	}
	e.Note, e.Verbose = g.say, g.verbose
	if err := e.Start(ctx); err != nil {
		return nil, err
	}
	return e, nil
}

func cmdProduce(ctx context.Context, g *global, args []string) error {
	fs := g.flagSet("produce", produceHelp)
	var f produceFlags
	fs.StringVar(&f.file, "file", "", "read this file instead of stdin, from where the last run stopped")
	fs.BoolVar(&f.follow, "follow", false, "with -file: keep reading as the file grows, across truncation and rotation")
	fs.StringVar(&f.profile, "profile", "full", "full (content in the record) or digest (content in -content-dir, a locator in the record)")
	fs.StringVar(&f.encoding, "encoding", "lines", "lines or gzip")
	fs.StringVar(&f.contentDir, "content-dir", "", "digest: where content files are written, named by SHA-256 hex")
	fs.Var(&f.urls, "url", "digest: an https URL prefix the content's SHA-256 hex is appended to, as a fetch hint (up to 4)")
	fs.IntVar(&f.maxBytes, "max-bytes", 0, "cut a batch at this many bytes of lines (default by mode and profile: docs/limits.md)")
	fs.IntVar(&f.maxLines, "max-lines", limits.DefaultMaxLines, fmt.Sprintf("cut a batch at this many lines (at most %d)", limits.MaxLines))
	fs.DurationVar(&f.maxWait, "max-wait", limits.DefaultMaxWait, fmt.Sprintf("cut a batch this long after its first line (%s to %s)", limits.MinMaxWait, limits.MaxMaxWait))
	fs.Float64Var(&f.maxRate, "max-rate", limits.DefaultRate, fmt.Sprintf("publish at most this many batches a second; a faster input waits (at most %d)", limits.MaxRate))
	fs.DurationVar(&f.interval, "interval", limits.DefaultInterval, fmt.Sprintf("anchor once this long has passed since the last anchor (%s to %s)", limits.MinInterval, limits.MaxInterval))
	fs.DurationVar(&f.anchorWait, "anchor-wait", 10*time.Minute, "at the end of the input, wait this long for the last anchor to mine (0: do not anchor at the end)")
	fs.StringVar(&f.unsafe, "unsafe", "warn", "lines a terminal would have to be protected from: warn (publish and count) or drop")
	engineFlags(fs, &f)
	pos, err := parse(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return usage("produce <stream> [flags]")
	}
	o := produce.Options{Interval: f.interval}
	switch f.profile {
	case "full":
		o.Profile = logrec.ProfileFull
	case "digest":
		o.Profile = logrec.ProfileDigest
		if f.contentDir == "" {
			return usage("-profile digest needs -content-dir, where the content is written")
		}
		if len(f.urls) > logrec.MaxURLs {
			return usage("at most %d -url hints", logrec.MaxURLs)
		}
		for _, u := range f.urls {
			if err := produce.ValidURLPrefix(u); err != nil {
				return usage("%v", err)
			}
		}
		if err := os.MkdirAll(f.contentDir, 0o755); err != nil {
			return err
		}
		var fsst syscall.Statfs_t
		if syscall.Statfs(f.contentDir, &fsst) == nil {
			if free := uint64(fsst.Bavail) * uint64(fsst.Bsize); free < limits.ContentDirFreeWarn { //nolint:unconvert // the field types differ across platforms
				g.say("warning: -content-dir %s has %d MiB free: every digest-only batch's content is written there and kept until you remove it (docs/limits.md)", f.contentDir, free>>20)
			}
		}
		o.ContentDir, o.URLs = f.contentDir, f.urls
	default:
		return usage("-profile is full or digest")
	}
	switch f.encoding {
	case "lines":
		o.Encoding = logrec.EncodingLines
	case "gzip":
		o.Encoding = logrec.EncodingLinesGzip
	default:
		return usage("-encoding is lines or gzip")
	}
	if f.maxBytes == 0 {
		f.maxBytes = limits.DefaultMaxBytes(g.cfg.Mode != config.ModeUnicast, o.Profile == logrec.ProfileDigest, o.Encoding == logrec.EncodingLinesGzip)
	}
	if f.unsafe != "warn" && f.unsafe != "drop" {
		return usage("-unsafe is warn or drop")
	}
	if f.follow && f.file == "" {
		return usage("-follow needs -file")
	}
	e, unlock, err := g.engine(ctx, pos[0], &f, o)
	if err != nil {
		return err
	}
	defer unlock()
	first := e.St.Seq + 1

	var src <-chan produce.Line
	var srcErr <-chan error
	if f.file != "" {
		path, err := filepath.Abs(f.file)
		if err != nil {
			return err
		}
		offset := int64(0)
		if t := e.St.Tail; t != nil && t.Path == path {
			if fi, err := os.Stat(path); err == nil {
				if dev, ino := produce.FileID(fi); dev == t.Dev && ino == t.Ino {
					offset = t.Offset
				} else {
					g.say("note: %s is not the file read before; reading it from the start", path)
				}
			}
		}
		if src, srcErr, err = produce.Follow(ctx, path, offset, 200*time.Millisecond, f.follow, g.say); err != nil {
			return err
		}
	} else {
		src, srcErr = produce.ReadLines(g.stdin)
	}
	checked, unsafeCount := screen(src, f.unsafe == "drop")
	runErr := e.Run(ctx, checked, srcErr, produce.Cut{MaxBytes: f.maxBytes, MaxLines: f.maxLines, MaxWait: f.maxWait, MinGap: limits.Gap(f.maxRate)}, 30*time.Second)
	if n := unsafeCount.Load(); n > 0 {
		verb := "published"
		if f.unsafe == "drop" {
			verb = "dropped"
		}
		g.say("note: %d line(s) held something a terminal must be protected from (termsafe); %s", n, verb)
	}
	// A followed file's reader closes its channel when ctx ends, so Run can
	// return nil for an interrupt too.
	interrupted := errors.Is(runErr, context.Canceled) || (runErr == nil && ctx.Err() != nil)
	if runErr != nil && !interrupted {
		closeEngine(e)
		g.hostSummary(e)
		return runErr
	}
	if !interrupted && f.anchorWait > 0 {
		dctx, cancel := context.WithTimeout(ctx, f.anchorWait)
		err := e.Drain(dctx)
		cancel()
		if err != nil && ctx.Err() == nil {
			closeEngine(e)
			g.summary(e, first)
			g.hostSummary(e)
			return err
		}
	}
	closeEngine(e)
	g.summary(e, first)
	g.hostSummary(e)
	return nil
}

// closeEngine collects a tree still being minted ahead, bounded.
func closeEngine(e *produce.Engine) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = e.Close(ctx)
}

func (g *global) summary(e *produce.Engine, first uint64) {
	st := e.St
	if st.Seq >= first {
		fmt.Fprintf(g.stdout, "stream %s: published seq %d to %d\n", st.Stream, first, st.Seq)
	} else {
		fmt.Fprintf(g.stdout, "stream %s: nothing new published (seq %d)\n", st.Stream, st.Seq)
	}
	if st.Anchor != nil {
		fmt.Fprintf(g.stdout, "anchored to seq %d: interval %d, token %s, mined at height %d\n", st.NextFirst-1, st.Anchor.Interval, st.Anchor.Txid, st.Anchor.Height)
	}
	if st.Inflight != nil {
		fmt.Fprintf(g.stdout, "anchor interval %d (seq %d to %d) in flight: token %s; the next produce or anchor collects it\n",
			st.Inflight.Interval, st.Inflight.First, st.Inflight.First+st.Inflight.Count-1, st.Inflight.Txid)
	}
	if n := len(st.Unanchored); n > 0 && st.Inflight == nil {
		fmt.Fprintf(g.stdout, "%d batch(es) not yet anchored; the next produce or anchor anchors them\n", n)
	}
}

// hostSummary prints, in unicast mode, what each host took and missed in
// this run, and the backfill that repairs the hosts that missed something.
func (g *global) hostSummary(e *produce.Engine) {
	if e.Legs.Hosts == nil {
		return
	}
	st := e.St
	var low uint64
	missed := false
	for _, t := range e.Legs.Hosts.Tallies() {
		line := fmt.Sprintf("host %s: took %d object(s), missed %d", t.Host, t.Took, t.Missed)
		if t.Missed > 0 {
			missed = true
			line += ": " + strings.Join(t.MissedWhat, ", ")
		}
		fmt.Fprintln(g.stdout, line)
		for _, w := range t.MissedWhat {
			var n uint64
			if _, err := fmt.Sscanf(w, "seq %d", &n); err == nil && (low == 0 || n < low) {
				low = n
			}
		}
	}
	if !missed {
		return
	}
	cmd := "blogs backfill " + st.Stream + " " + st.Identity
	if low > 0 && st.Seq >= low {
		low = max(low, st.Seq-min(st.Seq, logrec.MaxIntervalBatches-1))
		cmd += fmt.Sprintf(" -seqs %d-%d", low, st.Seq)
	}
	if st.Anchor != nil {
		cmd += fmt.Sprintf(" -anchors %d", st.Anchor.Interval)
	}
	if !strings.Contains(cmd, " -") {
		// Only funding trees: a carrier carries its batch tree, and
		// backfill feeds an anchor's fee tree from the node.
		fmt.Fprintln(g.stdout, "a host missed only funding trees, which needs no repair")
		return
	}
	fmt.Fprintf(g.stdout, "a host missed objects; once it is back, copy them from the others: %s\n", cmd)
}

// screen checks every line against the terminal-safety rules a reader
// applies (termsafe.Validate), counting the lines that fail and dropping
// them when drop is set.
func screen(in <-chan produce.Line, drop bool) (<-chan produce.Line, *atomic.Int64) {
	out := make(chan produce.Line, cap(in))
	n := new(atomic.Int64)
	go func() {
		defer close(out)
		for l := range in {
			if termsafe.Validate("line", string(l.B[:len(l.B)-1])) != nil {
				n.Add(1)
				if drop {
					continue
				}
			}
			out <- l
		}
	}()
	return out, n
}

const anchorHelp = `usage: blogs anchor <stream> [-wait DUR]

Settle a funding tree an earlier run signed and did not adopt, publish a
carrier a previous run persisted and may not have published, collect the
anchor in flight, anchor every published batch not yet
anchored, and wait until the anchors are mined and published. Needs what
produce needs.`

func cmdAnchor(ctx context.Context, g *global, args []string) error {
	fs := g.flagSet("anchor", anchorHelp)
	var f produceFlags
	wait := fs.Duration("wait", 10*time.Minute, "how long to wait for the anchors to mine")
	engineFlags(fs, &f)
	pos, err := parse(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return usage("anchor <stream> [-wait DUR]")
	}
	e, unlock, err := g.engine(ctx, pos[0], &f, produce.Options{Profile: logrec.ProfileFull, Encoding: logrec.EncodingLines, Interval: time.Hour})
	if err != nil {
		return err
	}
	defer unlock()
	dctx, cancel := context.WithTimeout(ctx, *wait)
	err = e.Drain(dctx)
	cancel()
	closeEngine(e)
	g.summary(e, e.St.Seq+1)
	g.hostSummary(e)
	if ctx.Err() != nil {
		// Interrupted: what is in flight is collected by the next run.
		return nil
	}
	return err
}
