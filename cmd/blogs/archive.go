package main

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/lightwebinc/bcommon/termsafe"

	"github.com/lightwebinc/blogs/internal/archive"
	"github.com/lightwebinc/blogs/internal/limits"
	"github.com/lightwebinc/blogs/internal/reader"
)

const archiveHelp = `usage: blogs archive <stream> <producer> -dir DIR [-host URL] [-follow] [-recheck N | -recheck-all] [flags]

Keep one host's copy of a stream in local files: ask the host for every seq
from where the last pass stopped until it holds nothing, check each carrier
as read does, and store it; then walk the interval class from the last
interval proven, keep each canonical anchor token, fetch any seq the
interval covers that is not yet stored, and write the interval's proof: its
token, its root, and every seq's commitment with its inclusion path. A seq
no file holds takes its commitment from the next batch's prev (a retracted
or undelivered batch), and the interval is written proven-with-holes.

Each pass first asks the host again about the seqs it has stored, up to
-recheck of them, continuing where the last pass stopped so the seqs asked
about longest ago come first (-recheck-all asks about every one). A stored
carrier the host no longer serves is checked for a retraction: a mined
transaction other than the carrier that spends its funding output (the
chain view names it; the archive checks it against header_url).
When one is found the archive keeps a receipt (the seq, the commitment,
its prev, and the sweep with its proof, under retracted/) and drops the
batch's bytes; the receipt still completes the interval's root. A carrier
the host stops serving with no such spend is kept.

Each anchor's fee tree is kept too, with its proof, under trees/, when the
anchor is first proven: the host answers it by txid (the tree class), and
the chain view (chain) is asked only when the host does not. An
archive begun before trees were kept catches up on its next pass. With its
trees an archive directory is a complete source for an audit:
blogs verify-interval -archive DIR.

The layout under DIR/<stream>/<producer> is in docs/usage.md. -host
defaults to the first configured host. Needs header_url, and the chain
view (chain) to find the spend that retracts a batch and a funding tree the
host does not serve; both default to WhatsOnChain on mainnet and testnet.

Prints what it stored and proved, then one line
"latest interval K root R token T" naming the last interval proven.

An archive follows one host and asks no other: an answer of that host that
does not verify is named, with the header source it was checked against,
and is not stored, and the pass exits 1.

Exit 0 when the pass is done, 1 when the host answered something that does
not verify or the chain or a root does not hold, 3 when an interval the
host answers cannot be proven from what it holds.`

func cmdArchive(ctx context.Context, g *global, args []string) error {
	fs := g.flagSet("archive", archiveHelp)
	dir := fs.String("dir", "", "the archive's directory")
	host := fs.String("host", "", "the host to follow (default: the first configured host)")
	follow := fs.Bool("follow", false, "keep following: a pass every -poll until interrupted")
	poll := fs.Duration("poll", 30*time.Second, "with -follow, the time between passes")
	recheck := fs.Int("recheck", limits.DefaultRecheck, fmt.Sprintf("stored seqs each pass asks the host about again, oldest checked first (0 to %d)", limits.MaxRecheck))
	recheckAll := fs.Bool("recheck-all", false, "ask the host again about every stored seq in this pass")
	var dirs, allow repeated
	fs.Var(&dirs, "content-dir", "a directory of digest-only content named by SHA-256 hex (repeatable)")
	fs.Var(&allow, "allow-host", "a host whose locator hints may be fetched over HTTPS (repeatable)")
	pos, err := parse(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 2 || *dir == "" {
		return usage("archive <stream> <producer> -dir DIR [-host URL]")
	}
	stream := pos[0]
	producer, err := streamAndProducer(stream, pos[1])
	if err != nil {
		return err
	}
	if *host == "" {
		if len(g.cfg.Hosts) == 0 {
			return usage("give -host, or configure hosts")
		}
		*host = g.cfg.Hosts[0]
	}
	if *poll <= 0 {
		return usage("-poll must be positive")
	}
	if *recheck < 0 || *recheck > limits.MaxRecheck {
		return usage("-recheck %d is outside the limits of 0 to %d: each is a lookup, so a pass is bounded (docs/limits.md); -recheck-all asks about every stored seq once", *recheck, limits.MaxRecheck)
	}
	abs, err := filepath.Abs(*dir)
	if err != nil {
		return err
	}
	c := readerFor(g, []string{*host}, g.cfg.Timeout)
	if c.Headers, err = g.headerClient(); err != nil {
		return err
	}
	a := &archive.Archive{Dir: abs, Host: *host, Stream: stream, Producer: producer, Client: c,
		Fetch: reader.Fetch{Dirs: dirs, Allow: allow, Timeout: g.cfg.Timeout, HTTP: httpClient}, Recheck: *recheck, RecheckAll: *recheckAll}
	if g.verbose {
		a.Note = g.say
	}
	for {
		s, err := a.Pass(ctx)
		if err != nil {
			if *follow && ctx.Err() != nil {
				return nil
			}
			if *follow {
				g.say("archive: %v; trying again in %s", err, *poll)
			} else {
				return err
			}
		} else if err := g.archiveSummary(a, s, !*follow); err != nil {
			return err
		}
		if !*follow {
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(*poll):
		}
	}
}

// archiveSummary prints one pass and, when final, turns its outcome into
// the exit code.
func (g *global) archiveSummary(a *archive.Archive, s *archive.Summary, final bool) error {
	out := g.stdout
	for _, f := range s.Proven {
		line := fmt.Sprintf("interval %d  %s  seqs %d to %d  root %s  token %s  height %d", f.Interval, f.Status, f.First, f.First+f.Count-1, f.Root, f.Token, f.Height)
		if len(f.Retracted) > 0 {
			line += "  retracted " + joinU64(f.Retracted)
		}
		if len(f.Holes) > 0 {
			line += "  holes " + joinU64(f.Holes)
		}
		fmt.Fprintln(out, line)
	}
	if len(s.Retracted) > 0 {
		fmt.Fprintf(out, "retracted seqs %s: receipt and sweep kept in retracted/, bytes dropped\n", joinU64(s.Retracted))
	}
	if s.StoredSeqs > 0 {
		fmt.Fprintf(out, "rechecked %d of %d stored seq(s)\n", s.Rechecked, s.StoredSeqs)
	}
	if s.Trees > 0 {
		fmt.Fprintf(out, "kept %d funding tree(s) with their proofs in trees/\n", s.Trees)
	}
	if s.TreesMissing != "" {
		g.say("archive: a funding tree is not kept yet: %s", termsafe.Text(s.TreesMissing))
	}
	fmt.Fprintf(out, "archive %s from %s: %d batch(es) stored, walked to seq %d, intervals 1 to %d proven, in %s\n",
		a.Stream, a.Host, s.Stored, s.Next-1, s.Intervals, a.Root())
	if s.Intervals > 0 {
		f, err := a.Interval(s.Intervals)
		if err != nil {
			return err
		}
		fmt.Fprintf(out, "latest interval %d root %s token %s\n", f.Interval, f.Root, f.Token)
	}
	// Every answer that did not verify is named, whether or not -v is set.
	for _, r := range s.Refusals {
		g.say("%s; what it answered does not verify, so it is not stored", termsafe.Text(r.String()))
	}
	if s.Stopped != "" {
		g.say("archive: stopped: %s", s.Stopped)
	}
	if !final {
		return nil
	}
	switch {
	case s.Refused > 0:
		// One host, and no other asked beside it: what it answered that
		// does not verify is a refusal of that host's copy.
		msg := fmt.Sprintf("the host %s answered %d time(s) what does not verify against the header source %s (each named above)", a.Host, s.Refused, g.cfg.HeaderURL)
		if s.Stopped != "" {
			msg += "; " + s.Stopped
		}
		return refused("%s", msg)
	case strings.Contains(s.Stopped, "anchor-join") || strings.Contains(s.Stopped, "root-mismatch") || strings.Contains(s.Stopped, "REFUSED"):
		return refused("%s", s.Stopped)
	case s.Stopped != "":
		return incomplete("%s", s.Stopped)
	}
	return nil
}
