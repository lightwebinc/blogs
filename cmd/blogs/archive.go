package main

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/lightwebinc/blogs/internal/archive"
	"github.com/lightwebinc/blogs/internal/reader"
)

const archiveHelp = `usage: blogs archive <stream> <producer> -dir DIR [-host URL] [-follow] [flags]

Keep one host's copy of a stream in local files: ask the host for every seq
from where the last pass stopped until it holds nothing, check each carrier
as read does, and store it; then walk the interval class from the last
interval proven, keep each canonical anchor token, fetch any seq the
interval covers that is not yet stored, and write the interval's proof: its
token, its root, and every seq's commitment with its inclusion path. A seq
no file holds takes its commitment from the next batch's prev (a retracted
or undelivered batch), and the interval is written proven-with-holes. The
layout under DIR/<stream>/<producer> is in docs/usage.md. -host defaults to
the first configured host. Needs header_url, and asset for the funding
trees anchors spend.

Prints what it stored and proved, then one line
"latest interval K root R token T" naming the last interval proven.

Exit 0 when the pass is done, 1 when the host answered something refused or
the chain or a root does not hold, 3 when an interval the host answers
cannot be proven from what it holds.`

func cmdArchive(ctx context.Context, g *global, args []string) error {
	fs := g.flagSet("archive", archiveHelp)
	dir := fs.String("dir", "", "the archive's directory")
	host := fs.String("host", "", "the host to follow (default: the first configured host)")
	follow := fs.Bool("follow", false, "keep following: a pass every -poll until interrupted")
	poll := fs.Duration("poll", 30*time.Second, "with -follow, the time between passes")
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
	abs, err := filepath.Abs(*dir)
	if err != nil {
		return err
	}
	c := readerFor(g, []string{*host}, g.cfg.Timeout)
	if c.Headers, err = g.headerClient(); err != nil {
		return err
	}
	a := &archive.Archive{Dir: abs, Host: *host, Stream: stream, Producer: producer, Client: c,
		Fetch: reader.Fetch{Dirs: dirs, Allow: allow, Timeout: g.cfg.Timeout, HTTP: httpClient}}
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
		if len(f.Holes) > 0 {
			line += "  holes " + joinU64(f.Holes)
		}
		fmt.Fprintln(out, line)
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
	if s.Stopped != "" {
		g.say("archive: stopped: %s", s.Stopped)
	}
	if !final {
		return nil
	}
	switch {
	case s.Refused > 0 || strings.Contains(s.Stopped, "anchor-join") || strings.Contains(s.Stopped, "root-mismatch") || strings.Contains(s.Stopped, "REFUSED"):
		return refused("%d answer(s) refused; %s", s.Refused, s.Stopped)
	case s.Stopped != "":
		return incomplete("%s", s.Stopped)
	}
	return nil
}
