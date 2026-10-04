package main

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/lightwebinc/bcommon/termsafe"

	"github.com/lightwebinc/blogs/internal/logrec"
	"github.com/lightwebinc/blogs/internal/produce"
)

const sweepHelp = `usage: blogs sweep <stream> (-retract-seqs A-B | -retract-tree TXID | -retire) [-yes]

Spend funding-tree outputs, one mined transaction per tree (specification
section 6.4).

-retract-seqs A-B retracts those batches: it asks the hosts for their
carriers and sweeps exactly the funding outputs they spent, with bcommon's
carrier.Sweep, whose output 0 is a funding-shaped tombstone. -retract-tree
sweeps every output of one batch funding tree, used and unused, retracting
every batch it funded. Once a retraction mines and is published, every
carrier it names is a double spend: hosts stop serving the batch and keep
its seq as a numbered position, which the anchor still commits. A
retraction's fee comes from the coin pool.

-retire spends the unused outputs of every tree no longer in use (batch
trees, anchor fee trees and trees minted ahead) back to the coin pool, the
fee coming out of their value. It retracts nothing. A tree whose unused
outputs cannot pay the fee is left and named.

Without -yes it prints what it would sweep and changes nothing. Each sweep
is written to the stream's state before it is settled; a sweep in flight is
finished (settled, waited for, pooled, published) by the next sweep run,
with or without flags, before anything new is built. A sweep the network
refuses for good, because the node names another transaction as the
spender of one of its inputs, is marked failed: it retracts or retires
nothing, its fee coin goes back to the pool when the node shows it
unspent, the run exits 1, and the next sweep goes on. Any other failure
leaves the sweep in flight. Needs what produce needs; -retract-seqs needs
hosts.

No lookup class names a sweep, so a host receives one only when it is
submitted to it. In mode plane a sweep goes to the facade, which the plane
carries to every subscribed host, and directly to every other host in
hosts, so a host off the plane receives it too; in mode unicast it goes to
every host in hosts. Either way it counts as published only when every
host named took it, whatever the quorum. A host missing from hosts can be
given a sweep later by backfill -sweeps.`

func cmdSweep(ctx context.Context, g *global, args []string) error {
	fs := g.flagSet("sweep", sweepHelp)
	var f produceFlags
	seqs := fs.String("retract-seqs", "", "retract the batches at these seqs (A-B, or one seq)")
	tree := fs.String("retract-tree", "", "retract every batch funded by this batch tree (txid)")
	retire := fs.Bool("retire", false, "spend the unused outputs of every tree no longer in use back to the pool")
	yes := fs.Bool("yes", false, "build and settle the sweeps; without it, print them and change nothing")
	wait := fs.Duration("wait", 10*time.Minute, "how long to wait for each sweep to mine")
	engineFlags(fs, &f)
	pos, err := parse(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return usage("sweep <stream> (-retract-seqs A-B | -retract-tree TXID | -retire) [-yes]")
	}
	n := 0
	for _, set := range []bool{*seqs != "", *tree != "", *retire} {
		if set {
			n++
		}
	}
	if n > 1 {
		return usage("-retract-seqs, -retract-tree and -retire are exclusive")
	}
	if *wait <= 0 {
		return usage("-wait must be positive")
	}
	var lo, hi uint64
	if *seqs != "" {
		if lo, hi, err = seqRange(*seqs); err != nil {
			return err
		}
	}
	e, unlock, err := g.engine(ctx, pos[0], &f, produce.Options{Profile: logrec.ProfileFull, Encoding: logrec.EncodingLines, Interval: time.Hour, Wait: *wait})
	if err != nil {
		return err
	}
	defer unlock()
	defer closeEngine(e)
	var gaveUp error
	if e.InFlight() {
		g.say("finishing the sweep in flight first")
		if err := e.FinishSweeps(ctx); produce.IsSweepRefused(err) {
			// It failed for good, which frees this run to go on.
			g.say("%v", err)
			gaveUp = refused("%v", err)
		} else if err != nil {
			return err
		}
	}
	switch {
	case *retire:
		return g.retire(ctx, e, *yes)
	case *tree != "":
		info, ok := e.Tree(*tree)
		if !ok || info.Kind != produce.BatchTree {
			return usage("%s is not a batch funding tree of %s (blogs doctor lists them)", *tree, e.St.Stream)
		}
		fmt.Fprintf(g.stdout, "retract tree %s: all %d output(s), %d of them spent by carriers\n", *tree, info.Tree.Count, info.Tree.Next)
		if !*yes {
			fmt.Fprintln(g.stdout, "nothing done; add -yes to sweep")
			return nil
		}
		if _, err := e.Retract(ctx, *tree, nil, nil); err != nil {
			return err
		}
		return sweepErr(e.FinishSweeps(ctx))
	case *seqs != "":
		plan, err := g.retractPlan(ctx, e, lo, hi)
		if err != nil {
			return err
		}
		trees := make([]string, 0, len(plan))
		for t := range plan {
			trees = append(trees, t)
		}
		sort.Strings(trees)
		for _, t := range trees {
			p := plan[t]
			fmt.Fprintf(g.stdout, "retract seqs %s: tree %s outputs %s\n", joinU64(p.seqs), t, joinU32(p.vouts))
		}
		if !*yes {
			fmt.Fprintln(g.stdout, "nothing done; add -yes to sweep")
			return nil
		}
		for _, t := range trees {
			if _, err := e.Retract(ctx, t, plan[t].vouts, plan[t].seqs); err != nil {
				return err
			}
			if err := e.FinishSweeps(ctx); err != nil {
				return sweepErr(err)
			}
		}
		return nil
	case n == 0 && !e.InFlight():
		// Nothing asked and nothing in flight: say what is recorded.
		for _, sw := range e.St.Sweeps {
			if sw.Failed != "" {
				fmt.Fprintf(g.stdout, "sweep %s  %s  tree %s  %d output(s)  FAILED: %s\n", sw.Txid, sw.Kind, sw.Tree, len(sw.Vouts), termsafe.Text(sw.Failed))
				continue
			}
			fmt.Fprintf(g.stdout, "sweep %s  %s  tree %s  %d output(s)  height %d\n", sw.Txid, sw.Kind, sw.Tree, len(sw.Vouts), sw.Height)
		}
		if len(e.St.Sweeps) == 0 {
			fmt.Fprintln(g.stdout, "no sweeps; give -retract-seqs, -retract-tree or -retire")
		}
		return gaveUp
	}
	return nil
}

// sweepErr is a sweep's error, a refusal by the network exiting 1.
func sweepErr(err error) error {
	if produce.IsSweepRefused(err) {
		return refused("%v", err)
	}
	return err
}

func (g *global) retire(ctx context.Context, e *produce.Engine, yes bool) error {
	trees := e.Retirable()
	if len(trees) == 0 {
		fmt.Fprintln(g.stdout, "retire: no tree out of use has unused outputs")
		return nil
	}
	for _, t := range trees {
		fmt.Fprintf(g.stdout, "retire %s tree %s: %d unused output(s) of %d sat\n", t.Kind, t.Tree.Txid, t.Tree.Remaining(), t.Tree.Sats)
	}
	if !yes {
		fmt.Fprintln(g.stdout, "nothing done; add -yes to sweep")
		return nil
	}
	for _, t := range trees {
		if _, err := e.Retire(ctx, t.Tree.Txid); err != nil {
			if errors.Is(err, produce.ErrNotWorthIt) {
				fmt.Fprintf(g.stdout, "retire %s tree %s: left: %v\n", t.Kind, t.Tree.Txid, err)
				continue
			}
			return err
		}
		if err := e.FinishSweeps(ctx); err != nil {
			return sweepErr(err)
		}
	}
	return nil
}

type retraction struct {
	vouts []uint32
	seqs  []uint64
}

// retractPlan finds, for each seq, the funding output its carriers spent,
// by asking the hosts for them: every candidate at a seq (a fork included)
// that is this producer's and spends a batch tree this stream recorded.
func (g *global) retractPlan(ctx context.Context, e *produce.Engine, lo, hi uint64) (map[string]*retraction, error) {
	c, err := g.hostsReader()
	if err != nil {
		return nil, err
	}
	producer, err := streamAndProducer(e.St.Stream, e.St.Identity)
	if err != nil {
		return nil, err
	}
	plan := map[string]*retraction{}
	for seq := lo; seq <= hi; seq++ {
		cands, _, err := c.Batch(ctx, e.St.Stream, producer, seq, true)
		if err != nil {
			return nil, err
		}
		if len(cands) == 0 {
			return nil, incomplete("seq %d: no host holds its carrier, so the output it spent is not known (already retracted, or not yet delivered)", seq)
		}
		for _, cand := range cands {
			in := cand.Tx.Inputs[0]
			t := in.SourceTXID.String()
			info, ok := e.Tree(t)
			if !ok || info.Kind != produce.BatchTree {
				return nil, refused("seq %d: its carrier %s spends %s:%d, which is not a batch tree this stream recorded", seq, cand.Tx.TxID(), t, in.SourceTxOutIndex)
			}
			p := plan[t]
			if p == nil {
				p = &retraction{}
				plan[t] = p
			}
			p.vouts = append(p.vouts, in.SourceTxOutIndex)
			if len(p.seqs) == 0 || p.seqs[len(p.seqs)-1] != seq {
				p.seqs = append(p.seqs, seq)
			}
		}
	}
	return plan, nil
}

func joinU64(s []uint64) string {
	parts := make([]string, len(s))
	for i, n := range s {
		parts[i] = fmt.Sprint(n)
	}
	return strings.Join(parts, " ")
}

func joinU32(s []uint32) string {
	parts := make([]string, len(s))
	for i, n := range s {
		parts[i] = fmt.Sprint(n)
	}
	return strings.Join(parts, " ")
}
