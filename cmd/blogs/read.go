package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/lightwebinc/bcommon/nodeapi"
	"github.com/lightwebinc/bcommon/termsafe"

	"github.com/lightwebinc/blogs/internal/archive"
	"github.com/lightwebinc/blogs/internal/limits"
	"github.com/lightwebinc/blogs/internal/logrec"
	"github.com/lightwebinc/blogs/internal/reader"
)

// pingQuery is a question no conforming ls_log answers: seq 0 is out of
// range, so a live service refuses it with an error.
var pingQuery = reader.Query{Stream: "doctor_aaaaaaaaaa", Producer: strings.Repeat("0", 66)}

// httpClient, when set, is the client every host request uses (tests).
var httpClient *http.Client

// wocBase is the WhatsOnChain API root; empty is the public one (tests
// serve their own).
var wocBase string

func readerFor(g *global, hosts []string, timeout time.Duration) *reader.Client {
	c := &reader.Client{Hosts: hosts, Timeout: timeout, HTTP: httpClient}
	switch {
	case g == nil:
	case g.cfg.Asset != "":
		c.Txs = reader.AssetSource{Asset: &nodeapi.Asset{Base: g.cfg.Asset, Client: httpClient}}
	case g.cfg.TxSource != "":
		c.Txs = reader.WhatsOnChain{Network: strings.TrimPrefix(g.cfg.TxSource, "woc:"), Base: wocBase, Timeout: timeout, HTTP: httpClient}
	}
	return c
}

// streamAndProducer checks the two positionals every reader command takes.
func streamAndProducer(stream, producer string) ([]byte, error) {
	if logrec.CheckStream(stream) != nil {
		return nil, usage("%q is not a stream identifier <name>_<suffix> (the readable name alone never names a stream)", termsafe.Text(stream))
	}
	p, err := hex.DecodeString(producer)
	if err != nil || len(producer) != 66 || strings.ToLower(producer) != producer || logrec.CheckProducer(p) != nil {
		return nil, usage("%q is not a producer identity key (66 lowercase hex characters, a compressed key)", termsafe.Abbrev(producer))
	}
	return p, nil
}

func (g *global) hostsReader() (*reader.Client, error) {
	if len(g.cfg.Hosts) == 0 {
		return nil, usage("no overlay host configured (config key hosts, or -hosts)")
	}
	return readerFor(g, g.cfg.Hosts, g.cfg.Timeout), nil
}

const readHelp = `usage: blogs read <stream> <producer> [flags]

Ask the hosts for the producer's batches from -from on, check each answer
(the carrier rules, the producer's signature, the seq, the funding parent's
proof against the header source), follow the prev links, decode the content
and print its lines, filtered for the terminal. Without -to it reads until
no host holds the next seq, which may be the stream's end or a hole: an
empty answer proves nothing. One read covers at most 65536 seqs; without
-to it stops there and names the -from to continue with. A digest-only batch's content is read from
-content-dir, or fetched from a locator hint whose host is on -allow-host.
Needs hosts and header_url.

Exit 0 when every batch read verified and linked, 1 when an answer was
refused or the links broke, 3 when -to was given and a seq was missing.`

// renderer prints text from a record safely.
type renderer struct {
	opts termsafe.Options
}

// lines filters raw content for the terminal line by line, wrapping a line
// longer than termsafe.MaxCols rather than cutting it.
func (r renderer) lines(content []byte) []string {
	var out []string
	for _, l := range strings.SplitAfter(string(content), "\n") {
		l = strings.TrimSuffix(l, "\n")
		if l == "" && len(out) > 0 && len(content) > 0 {
			// SplitAfter's empty tail after the last LF.
			continue
		}
		rs := []rune(l)
		for len(rs) > termsafe.MaxCols {
			out = append(out, termsafe.Sanitize(string(rs[:termsafe.MaxCols]), r.opts))
			rs = rs[termsafe.MaxCols:]
		}
		out = append(out, termsafe.Sanitize(string(rs), r.opts))
	}
	return out
}

type batchJSON struct {
	Seq       uint64   `json:"seq"`
	Carrier   string   `json:"carrier"`
	Timestamp uint64   `json:"timestamp"`
	Count     uint64   `json:"count"`
	Profile   uint64   `json:"profile"`
	Encoding  uint64   `json:"encoding"`
	Verdict   string   `json:"verdict"`
	Link      string   `json:"link"`
	Content   string   `json:"content"`
	Lines     []string `json:"lines,omitempty"`
}

func cmdRead(ctx context.Context, g *global, args []string) error {
	fs := g.flagSet("read", readHelp)
	from := fs.Uint64("from", 1, "first seq")
	to := fs.Uint64("to", 0, "last seq (0: until no host holds the next)")
	meta := fs.Bool("meta", false, "print a line about each batch before its lines")
	jsonOut := fs.Bool("json", false, "one JSON object per batch, lines filtered as for a terminal")
	ansi := fs.Bool("ansi", false, "let colour (SGR) through to the terminal")
	ascii := fs.Bool("ascii", false, "print non-ASCII as ? (default when the locale is not UTF-8)")
	var dirs, allow repeated
	fs.Var(&dirs, "content-dir", "a directory of digest-only content named by SHA-256 hex (repeatable)")
	fs.Var(&allow, "allow-host", "a host whose locator hints may be fetched over HTTPS (repeatable)")
	pos, err := parse(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 2 {
		return usage("read <stream> <producer> [flags]")
	}
	stream := pos[0]
	producer, err := streamAndProducer(stream, pos[1])
	if err != nil {
		return err
	}
	if *from < 1 || (*to != 0 && *to < *from) {
		return usage("-from must be at least 1 and -to, when given, at least -from")
	}
	if *to != 0 && *to-*from >= limits.MaxReadBatches {
		return usage("-from %d -to %d is %d seqs, over the limit of %d per read: each seq is a lookup and a proof check (docs/limits.md); read it in parts", *from, *to, *to-*from+1, limits.MaxReadBatches)
	}
	c, err := g.hostsReader()
	if err != nil {
		return err
	}
	hc, err := g.headerClient()
	if err != nil {
		return err
	}
	c.Headers = hc
	r := renderer{termsafe.Options{ANSI: *ansi, ASCII: *ascii || !termsafe.UTF8Locale(g.getenv)}}
	fetch := reader.Fetch{Dirs: dirs, Allow: allow, Timeout: g.cfg.Timeout, HTTP: httpClient}
	enc := json.NewEncoder(g.stdout)

	var prev *reader.Candidate
	ahead := map[uint64][]*reader.Candidate{}
	bad, missing, read := 0, 0, 0
	get := func(seq uint64) ([]*reader.Candidate, error) {
		if cs, ok := ahead[seq]; ok {
			delete(ahead, seq)
			return cs, nil
		}
		cs, refusedAnswers, err := c.Batch(ctx, stream, producer, seq, false)
		for _, v := range refusedAnswers {
			bad++
			g.say("seq %d: %s %s", seq, v.Code, termsafe.Text(v.Reason))
		}
		return cs, err
	}
	for seq := *from; *to == 0 || seq <= *to; seq++ {
		if *to == 0 && seq-*from >= limits.MaxReadBatches {
			g.say("read stopped at the limit of %d seqs per read (docs/limits.md); continue with -from %d", limits.MaxReadBatches, seq)
			break
		}
		cands, err := get(seq)
		if err != nil {
			return err
		}
		if len(cands) == 0 {
			if *to == 0 {
				g.say("seq %d: no host holds it (the end of what the hosts hold, or a hole)", seq)
				break
			}
			missing++
			prev = nil
			g.say("seq %d: MISSING: no host holds it", seq)
			continue
		}
		cand := cands[0]
		if len(cands) > 1 {
			next, err := get(seq + 1)
			if err != nil {
				return err
			}
			ahead[seq+1] = next
			var nextOne *reader.Candidate
			if len(next) == 1 {
				nextOne = next[0]
			}
			var ok bool
			if cand, ok = reader.Link(cands, nextOne); !ok {
				bad++
				prev = nil
				g.say("seq %d: FORK: %d candidates and seq %d does not choose one", seq, len(cands), seq+1)
				continue
			}
			g.say("seq %d: FORK: %d candidates; seq %d's prev chooses %s", seq, len(cands), seq+1, chainhash.Hash(cand.Carrier.Commitment))
		}
		b := cand.Carrier.Batch
		link := "linked"
		switch {
		case b.Seq == 1:
		case prev == nil:
			link = "unchecked"
		case !reader.Linked(prev, cand):
			link = "BROKEN"
			bad++
			g.say("seq %d: BROKEN LINK: prev names %s, seq %d read is %s", seq, chainhash.Hash(b.Prev), seq-1, chainhash.Hash(prev.Carrier.Commitment))
		}
		prev = cand
		read++
		content, cerr := reader.Content(ctx, b, fetch)
		status := "ok"
		switch {
		case cerr == nil:
		case errors.Is(cerr, reader.ErrUnsupported):
			status = "unsupported"
		case errors.Is(cerr, reader.ErrPending):
			status = "pending"
		default:
			status = "INVALID"
			bad++
			g.say("seq %d: content INVALID: %v", seq, cerr)
		}
		id := cand.Tx.TxID().String()
		if g.verbose {
			g.say("seq %d: VERIFIED carrier %s, %s, content %s", seq, id, link, status)
		}
		if *jsonOut {
			j := batchJSON{Seq: b.Seq, Carrier: id, Timestamp: b.Timestamp, Count: b.Count, Profile: b.Profile,
				Encoding: b.Encoding, Verdict: "VERIFIED", Link: strings.ToLower(link), Content: status}
			if status == "ok" {
				j.Lines = r.lines(content)
			}
			if err := enc.Encode(j); err != nil {
				return err
			}
			continue
		}
		if *meta {
			fmt.Fprintf(g.stdout, "# seq %d  timestamp %d  %d line(s)  carrier %s  VERIFIED  %s  content %s\n", b.Seq, b.Timestamp, b.Count, id, link, status)
		}
		if status == "ok" {
			for _, l := range r.lines(content) {
				fmt.Fprintln(g.stdout, l)
			}
		}
	}
	g.say("read %d batch(es) of %s", read, stream)
	switch {
	case bad > 0:
		return refused("%d answer(s) refused, forked, broken or invalid", bad)
	case missing > 0:
		return incomplete("%d seq(s) missing", missing)
	}
	return nil
}

const verifyHelp = `usage: blogs verify-interval <stream> <producer> <n> [-archive DIR]

Audit interval n as an auditor holding block headers (specification section
13): walk the interval class from 1 to n keeping each interval's canonical
anchor token (interval 1 the first mined, each later one the token that
spends its predecessor); hold each to the anchor rules and its proof to the
header source; ask the batch class for every seq interval n covers, verify
each carrier, and require the RFC 6962 root over their commitments to equal
the token's. A seq no host serves takes its commitment from the next
batch's prev, as an archive proves across a retraction, and is reported
unserved (retracted, or not delivered: a host serves nothing for either,
so the audit names the position, not the cause; an archive that checks
the chain for the sweep names a retraction). One whose commitment nothing
names (the interval's last seq, or the first of two in a row) leaves the
interval INCOMPLETE. The outputs an anchor's inputs spend are read from the
predecessor token, from the funding trees the interval's carriers carry, or
from the funding tree itself: an anchor's fee tree is asked of the hosts by
its txid (the tree class), which answer it with its proof however long ago
its outputs were spent, and only when no host serves it of a copy of the
chain (asset or tx_source), which may have pruned it. Every tree is held to
the txid the anchor's input names, and a host's to its proof against the
header source. -v names what answered each tree.

-archive DIR adds an archive directory (blogs archive -dir DIR) as a source
asked before the hosts: its stored tokens, carriers and funding trees are
checked exactly as a host's answers are. With -archive and no host
configured the audit runs from the files and the header source alone.

Needs no producer home; needs header_url, and hosts or -archive.

Exit 0 VERIFIED, 1 REFUSED, 3 INCOMPLETE (a host held nothing where
evidence was needed), 2 when a host or the header source could not be asked.`

func cmdVerifyInterval(ctx context.Context, g *global, args []string) error {
	fs := g.flagSet("verify-interval", verifyHelp)
	dir := fs.String("archive", "", "an archive directory (blogs archive -dir) to ask before the hosts")
	pos, err := parse(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 3 {
		return usage("verify-interval <stream> <producer> <n> [-archive DIR]")
	}
	stream := pos[0]
	producer, err := streamAndProducer(stream, pos[1])
	if err != nil {
		return err
	}
	n, err := strconv.ParseUint(pos[2], 10, 64)
	if err != nil || n < 1 || n > logrec.MaxSafe {
		return usage("interval %q is not a number from 1 to %d", termsafe.Abbrev(pos[2]), uint64(logrec.MaxSafe))
	}
	var c *reader.Client
	if *dir == "" {
		if c, err = g.hostsReader(); err != nil {
			return err
		}
	} else {
		abs, err := filepath.Abs(*dir)
		if err != nil {
			return err
		}
		src := &archive.Archive{Dir: abs, Stream: stream, Producer: producer}
		if _, err := os.Stat(src.Root()); err != nil {
			return usage("-archive %s holds no archive of this stream and producer (%s is missing)", termsafe.Text(*dir), termsafe.Text(src.Root()))
		}
		c = readerFor(g, g.cfg.Hosts, g.cfg.Timeout)
		c.Local = []reader.Source{src}
	}
	hc, err := g.headerClient()
	if err != nil {
		return err
	}
	c.Headers = hc
	rep, err := c.VerifyInterval(ctx, stream, producer, n)
	if err != nil {
		return err
	}
	out := g.stdout
	if g.verbose {
		for _, t := range rep.Trees {
			g.say("interval %d: funding tree %s from %s", t.Interval, t.Txid, termsafe.Text(t.From))
		}
	}
	for _, t := range rep.Chain {
		fmt.Fprintf(out, "interval %d  token %s  height %d  seqs %d to %d\n", t.Anchor.Interval, t.Txid, t.Height, t.Anchor.First, t.Anchor.Last())
	}
	for _, b := range rep.Batches {
		fmt.Fprintf(out, "seq %d  carrier %s  timestamp %d  %d line(s)\n", b.Carrier.Batch.Seq, b.Tx.TxID(), b.Carrier.Batch.Timestamp, b.Carrier.Batch.Count)
	}
	if len(rep.Unserved) > 0 {
		fmt.Fprintf(out, "unserved seqs %s: no host serves them (retracted, or not delivered); each commitment is the next batch's prev\n", seqList(rep.Unserved))
	}
	if len(rep.Missing) > 0 {
		fmt.Fprintf(out, "missing seqs %s\n", seqList(rep.Missing))
	}
	if g.verbose {
		for _, s := range rep.Refusals {
			g.say("refused answer: %s", termsafe.Text(s))
		}
	}
	switch rep.Outcome {
	case reader.Verified:
		last := rep.Chain[len(rep.Chain)-1]
		fmt.Fprintf(out, "VERIFIED interval %d of %s by %s: seqs %d to %d, root %s, committed no later than block %d\n",
			n, stream, pos[1], last.Anchor.First, last.Anchor.Last(), hex.EncodeToString(last.Anchor.Root[:]), last.Height)
		return nil
	case reader.Refused:
		fmt.Fprintf(out, "REFUSED interval %d: %s\n", n, termsafe.Text(ownReason(rep.Reason, n)))
		return &exitError{exitRefused, ""}
	default:
		fmt.Fprintf(out, "INCOMPLETE interval %d: %s\n", n, termsafe.Text(ownReason(rep.Reason, n)))
		return &exitError{exitIncomplete, ""}
	}
}

// ownReason drops the interval a reason names when it is the interval
// audited, so the verdict line names it once. A reason about an earlier
// interval of the walk keeps its name.
func ownReason(reason string, n uint64) string {
	return strings.TrimPrefix(reason, fmt.Sprintf("interval %d: ", n))
}

func seqList(s []uint64) string {
	var parts []string
	for _, n := range s {
		parts = append(parts, strconv.FormatUint(n, 10))
	}
	return strings.Join(parts, " ")
}

const backfillHelp = `usage: blogs backfill <stream> <producer> [-from URL] [-to URL] [-sweeps] (-seqs A-B | -interval K | -anchors K)

Fill holes in a host from its siblings. Each position is asked of the host
first and fetched from the first sibling that holds it only when the host
holds nothing; the answer is checked (the carrier rules and, with
header_url set, the proof) and submitted to the host.

-to names the host to fill; without it every configured host is filled in
turn. -from names the sibling; without it the siblings are the configured
hosts other than the one being filled. With neither, the configured hosts
are brought into step with each other, which is how the hosts of a unicast
producer are repaired after one missed submissions.

-interval K fills the anchor chain 1 to K and the seqs interval K covers;
-seqs A-B fills a range of seqs; -anchors K fills the anchor chain 1 to K;
-seqs and -anchors combine. A host admits an anchor only once it holds the
fee tree the anchor spends, so when a host does not admit a token backfill
carries the tree first: it asks the siblings for it by txid (the tree
class), reads it from the node (asset) or tx_source only when no sibling
serves it, and submits it with its proof. Needs no producer home.

A seq that neither the host nor any sibling serves is missing, unless the
seq after it is held or filled: its prev then names the seq's commitment,
as for a retracted batch, and it is counted unserved instead (retracted,
or not delivered to any host asked; as verify-interval and archive prove
an interval across it).

No lookup class names the sweep of a batch, so a host that missed one still
serves the batches it retracted. -sweeps carries them: every carrier the host holds
at the seqs filled is checked for a mined spend of its funding output by
another transaction (asset or tx_source names it; it is checked against
header_url), and a sweep found is submitted to the host. It costs one
spend read per held carrier. Without -sweeps, backfill says it carried
none.

Exit 0 when every position is held (or unserved), 1 when a sibling
answered something refused, 3 when a position is still missing.`

func cmdBackfill(ctx context.Context, g *global, args []string) error {
	fs := g.flagSet("backfill", backfillHelp)
	from := fs.String("from", "", "the sibling to copy from (default: every configured host but the one filled)")
	to := fs.String("to", "", "the host to fill (default: every configured host in turn)")
	seqs := fs.String("seqs", "", "a seq range A-B, or one seq")
	interval := fs.Uint64("interval", 0, "fill the anchor chain to interval K and the seqs it covers")
	anchors := fs.Uint64("anchors", 0, "fill the anchor chain 1 to K")
	sweeps := fs.Bool("sweeps", false, "carry sweeps: check every held carrier for a mined spend of its funding output, and submit the sweep found (needs asset or tx_source, and header_url)")
	pos, err := parse(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 2 {
		return usage("backfill <stream> <producer> [-from URL] [-to URL] (-seqs A-B | -interval K | -anchors K)")
	}
	stream := pos[0]
	producer, err := streamAndProducer(stream, pos[1])
	if err != nil {
		return err
	}
	targets, sources := g.cfg.Hosts, g.cfg.Hosts
	if *to != "" {
		targets = []string{*to}
	}
	if *from != "" {
		sources = []string{*from}
	}
	if len(targets) == 0 || len(sources) == 0 || (len(targets) == 1 && len(sources) == 1 && targets[0] == sources[0]) {
		return usage("backfill needs a host to fill and another to copy from: -to and -from, or two or more configured hosts")
	}
	c := readerFor(g, nil, g.cfg.Timeout)
	if g.cfg.HeaderURL != "" {
		if c.Headers, err = g.headerClient(); err != nil {
			return err
		}
	}
	if *sweeps {
		if _, ok := c.Txs.(reader.SpendSource); !ok || c.Headers == nil {
			return usage("-sweeps needs asset or tx_source, to name the transaction that spent a batch's funding output, and header_url, to check it is mined")
		}
	}
	var seqList []uint64
	switch {
	case *seqs != "" && *interval != 0:
		return usage("-seqs and -interval are exclusive")
	case *seqs != "":
		a, z, err := seqRange(*seqs)
		if err != nil {
			return err
		}
		for s := a; s <= z; s++ {
			seqList = append(seqList, s)
		}
	case *interval != 0:
		first, last, err := intervalRange(ctx, c, unique(targets, sources), stream, producer, *interval)
		if err != nil {
			return err
		}
		for s := first; s <= last; s++ {
			seqList = append(seqList, s)
		}
	case *anchors == 0:
		return usage("give -seqs, -interval or -anchors")
	}
	var total reader.Result
	var runErr error
	for _, t := range targets {
		b := reader.Backfill{From: sources, To: t, Stream: stream, Producer: producer, Intervals: max(*anchors, *interval), Seqs: seqList,
			Sweeps: *sweeps, Note: func(f string, a ...any) { fmt.Fprintf(g.stdout, f+"\n", a...) }}
		if len(targets) > 1 {
			fmt.Fprintf(g.stdout, "filling %s\n", t)
		}
		res, err := c.Run(ctx, b)
		if err != nil {
			if len(targets) == 1 {
				return err
			}
			// One host down does not stop the others being filled.
			fmt.Fprintf(g.stdout, "backfill %s to %s: FAILED: %v\n", stream, t, err)
			if runErr == nil {
				runErr = err
			}
			continue
		}
		fmt.Fprintf(g.stdout, "backfill %s to %s: %d held, %d filled, %d missing, %d refused, %d unserved, %d sweep(s) carried\n", stream, t, res.Held, res.Filled, res.Missing, res.Refused, res.Unserved, res.Sweeps)
		total.Held += res.Held
		total.Filled += res.Filled
		total.Missing += res.Missing
		total.Refused += res.Refused
	}
	if !*sweeps && len(seqList) > 0 {
		g.say("note: backfill carried batches, anchors and their fee trees, not sweeps: no lookup class names the sweep of a batch, so a host that missed one still serves the batches it retracted; -sweeps checks each held batch and carries the sweep it finds")
	}
	switch {
	case total.Refused > 0:
		return &exitError{exitRefused, ""}
	case runErr != nil:
		return runErr
	case total.Missing > 0:
		return &exitError{exitIncomplete, ""}
	}
	return nil
}

// unique is a and then b, each host once, in order.
func unique(a, b []string) []string {
	var out []string
	seen := map[string]bool{}
	for _, h := range append(append([]string(nil), a...), b...) {
		if !seen[h] {
			seen[h] = true
			out = append(out, h)
		}
	}
	return out
}

func seqRange(s string) (uint64, uint64, error) {
	a, z, found := strings.Cut(s, "-")
	if !found {
		z = a
	}
	x, err1 := strconv.ParseUint(a, 10, 64)
	y, err2 := strconv.ParseUint(z, 10, 64)
	if err1 != nil || err2 != nil || x < 1 || y < x || y-x >= logrec.MaxIntervalBatches || y > logrec.MaxSafe {
		return 0, 0, usage("-seqs %q is not a range A-B of at most %d seqs", termsafe.Abbrev(s), logrec.MaxIntervalBatches)
	}
	return x, y, nil
}

// intervalRange reads the seqs interval k covers from a token the first of
// hosts that holds one answers. Several tokens for one interval (a producer
// that equivocated) are refused: the operator names the range with -seqs.
func intervalRange(ctx context.Context, c *reader.Client, hosts []string, stream string, producer []byte, k uint64) (uint64, uint64, error) {
	q := reader.Query{Stream: stream, Producer: hex.EncodeToString(producer), Interval: k}
	var askErr error
	for _, h := range hosts {
		as, err := c.Ask(ctx, h, q)
		if err != nil {
			askErr = err
			continue
		}
		var found []*logrec.Anchor
		for _, a := range as {
			if a2, ok := decodeAnswer(a); ok && a2.Interval == k && a2.Stream == stream && string(a2.Producer) == string(producer) {
				found = append(found, a2)
			}
		}
		switch len(found) {
		case 0:
			continue
		case 1:
			return found[0].First, found[0].Last(), nil
		default:
			return 0, 0, refused("%s answers %d tokens for interval %d; name the range with -seqs", h, len(found), k)
		}
	}
	if askErr != nil {
		return 0, 0, askErr
	}
	return 0, 0, incomplete("no host holds a token for interval %d", k)
}

func decodeAnswer(a reader.Answer) (*logrec.Anchor, bool) {
	tx, err := parseSubject(a.Beef)
	if err != nil {
		return nil, false
	}
	an, _, err := logrec.DecodeToken(tx)
	return an, err == nil
}

func parseSubject(beef []byte) (*transaction.Transaction, error) {
	_, tx, _, err := transaction.ParseBeef(beef)
	if err == nil && tx == nil {
		err = errors.New("the BEEF names no subject transaction")
	}
	return tx, err
}
