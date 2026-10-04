package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	base58 "github.com/bsv-blockchain/go-sdk/compat/base58"

	"github.com/lightwebinc/bcommon/bwallet"
	"github.com/lightwebinc/bcommon/nodeapi"
	"github.com/lightwebinc/bcommon/publish"
	"github.com/lightwebinc/bcommon/termsafe"

	"github.com/lightwebinc/blogs/internal/config"
	"github.com/lightwebinc/blogs/internal/limits"
	"github.com/lightwebinc/blogs/internal/logrec"
	"github.com/lightwebinc/blogs/internal/produce"
	"github.com/lightwebinc/blogs/internal/reader"
	"github.com/lightwebinc/blogs/internal/state"
	"github.com/lightwebinc/blogs/internal/unicast"
)

// openWallet opens the home's embedded wallet.
func (g *global) openWallet() (*bwallet.Embedded, error) {
	e, err := bwallet.Open(g.cfg.Home, produce.Profile)
	if err != nil {
		return nil, fmt.Errorf("open the producer home %s: %w (run `blogs init`)", g.cfg.Home, err)
	}
	e.Originator = g.cfg.Originator
	e.Mainnet = g.cfg.Network == "main"
	return e, nil
}

// lockHome takes the home's lock: one command that spends from the pool at
// a time.
func (g *global) lockHome() (func(), error) {
	unlock, err := state.Lock(filepath.Join(g.cfg.Home, "lock"))
	if errors.Is(err, state.ErrLocked) {
		return nil, usage("another blogs command is using the home %s (one produce, anchor or fund at a time; run another producer from another home)", g.cfg.Home)
	}
	return unlock, err
}

// node is the configured node: its JSON-RPC and asset API.
func (g *global) node(needRPC bool) (*nodeapi.RPC, *nodeapi.Asset, error) {
	if g.cfg.Asset == "" || (needRPC && g.cfg.RPC == "") {
		if needRPC {
			return nil, nil, usage("rpc and asset must be configured (config keys rpc, asset)")
		}
		return nil, nil, usage("asset must be configured (config key asset): proofs are read from the node")
	}
	var rpc *nodeapi.RPC
	if g.cfg.RPC != "" {
		rpc = &nodeapi.RPC{URL: g.cfg.RPC, User: g.cfg.RPCUser, Pass: g.cfg.RPCPass, ID: "blogs"}
	}
	return rpc, &nodeapi.Asset{Base: g.cfg.Asset}, nil
}

// legs are the endpoints a producer publishes and settles through. None
// has a default.
func (g *global) legs() (produce.Legs, error) {
	var l produce.Legs
	_, asset, err := g.node(false)
	if err != nil {
		return l, err
	}
	l.Asset = asset
	switch g.cfg.Mode {
	case config.ModeUnicast:
		if g.cfg.Facade != "" {
			return l, usage("facade is for mode plane; mode unicast submits to every host in hosts, so leave facade unset")
		}
		if len(g.cfg.Hosts) == 0 {
			return l, usage("hosts must be configured (config key hosts, or -hosts): mode unicast submits to each of them")
		}
		need, err := config.Need(g.cfg.Quorum, len(g.cfg.Hosts))
		if err != nil {
			return l, usage("%v (config key quorum)", err)
		}
		l.Hosts = &unicast.Set{Hosts: g.cfg.Hosts, Need: need, Retries: produce.Retries, HTTP: httpClient,
			Note: g.say, Verbose: g.verbose}
	default:
		if g.cfg.Facade == "" {
			return l, usage("facade must be configured (config key facade): the overlay host a producer publishes to (or set mode = unicast to submit to every host in hosts)")
		}
		l.Facade = &publish.Facade{Base: g.cfg.Facade}
		// Every sweep also goes directly to the hosts other than the
		// facade, so a host off the plane is not left serving what a
		// sweep retracted.
		var direct []string
		for _, h := range g.cfg.Hosts {
			if strings.TrimRight(h, "/") != strings.TrimRight(g.cfg.Facade, "/") {
				direct = append(direct, h)
			}
		}
		if len(direct) > 0 {
			l.SweepHosts = &unicast.Set{Hosts: direct, Need: len(direct), Retries: produce.Retries, HTTP: httpClient,
				Note: g.say, Verbose: g.verbose}
		}
	}
	kind, addr, _ := strings.Cut(g.cfg.Settle, ":")
	switch kind {
	case "tcp":
		l.Settler = &publish.TCPIngress{Addr: addr}
	case "rpc":
		l.Settler = &publish.RPCSettler{RPC: &nodeapi.RPC{URL: addr, User: g.cfg.RPCUser, Pass: g.cfg.RPCPass, ID: "blogs"}}
	case "arcade":
		l.Arcade = &publish.Arcade{Base: addr, Key: g.cfg.ArcadeKey}
		l.Settler = l.Arcade
	default:
		return l, usage("settle must be tcp:<host:port>, rpc:<url> or arcade:<url> (config key settle)")
	}
	return l, nil
}

const initHelp = `usage: blogs init

Create the producer home (mode 0700): an identity key and an empty coin
pool. Print the identity key and the fund address for the configured
network. A home that exists is left alone and printed.`

func cmdInit(_ context.Context, g *global, args []string) error {
	fs := g.flagSet("init", initHelp)
	if pos, err := parse(fs, args); err != nil {
		return err
	} else if len(pos) > 0 {
		return usage("init takes no arguments")
	}
	e, err := bwallet.Create(g.cfg.Home, produce.Profile)
	if errors.Is(err, bwallet.ErrIdentityExists) {
		if e, err = g.openWallet(); err != nil {
			return err
		}
		g.say("the home %s exists; left as it is", g.cfg.Home)
	} else if err != nil {
		return err
	} else {
		fmt.Fprintln(g.stdout, "created", g.cfg.Home)
	}
	if err := os.Chmod(g.cfg.Home, 0o700); err != nil {
		return err
	}
	e.Originator = g.cfg.Originator
	e.Mainnet = g.cfg.Network == "main"
	addr, err := e.FundAddress(e.Mainnet)
	if err != nil {
		return err
	}
	fmt.Fprintf(g.stdout, "identity     %s\nfund address %s (%s)\n", e.Signer().IdentityHex(), addr, g.cfg.Network)
	return nil
}

const fundHelp = `usage: blogs fund [-blocks N] [-batch N] [-rescan]
       blogs fund -txid TXID

With -txid, import a payment: read the mined transaction TXID with its
proof from the node (asset) or, with no node, from tx_source; check the
proof against the header source; and add every output it pays to the
home's fund address to the pool. This is how a producer on a real network
is funded: send BSV to the fund address that init printed, wait for one
confirmation, and import it.

Without -txid, mine coinbase to the fund address through the node's
generatetoaddress, which only a chain you run answers, and add the outputs
to the pool. Coinbase can be spent 100 blocks after it is mined, so the
first fund mines 101 or more. -rescan re-reads recent blocks for coinbase
the pool lacks.

The address prefix follows the network (main: mainnet; test and regtest:
testnet), and an address for another network than the configured one,
above all a mainnet address on a test network, is refused before anything
is mined.`

// addressNetwork is the network a base58 P2PKH address is for.
func addressNetwork(addr string) (string, error) {
	b, err := base58.Decode(addr)
	if err != nil || len(b) != 25 {
		return "", fmt.Errorf("fund address %q does not decode", addr)
	}
	switch b[0] {
	case 0x00:
		return "main", nil
	case 0x6f:
		return "test", nil
	}
	return "", fmt.Errorf("fund address %q has version byte 0x%02x", addr, b[0])
}

// checkFundAddress refuses an address for another network than the
// configured one: above all a mainnet address on a test network, which a
// chain run for testing must never be asked to pay.
func checkFundAddress(addr, network string) error {
	n, err := addressNetwork(addr)
	if err != nil {
		return err
	}
	if (n == "main") != (network == "main") {
		return usage("the fund address %s is a %s address and network is %s; refusing to mine to it", addr, n, network)
	}
	return nil
}

func cmdFund(ctx context.Context, g *global, args []string) error {
	fs := g.flagSet("fund", fundHelp)
	blocks := fs.Int("blocks", 101, "blocks to mine to the fund address")
	batch := fs.Int("batch", bwallet.DefaultFundBatch, "blocks per generatetoaddress call")
	rescan := fs.Bool("rescan", false, "re-read the last -blocks blocks for coinbase the pool lacks instead of mining")
	txid := fs.String("txid", "", "import this mined payment to the fund address instead of mining")
	if pos, err := parse(fs, args); err != nil {
		return err
	} else if len(pos) > 0 {
		return usage("fund takes no arguments")
	}
	if *txid != "" {
		if *rescan {
			return usage("-txid and -rescan are exclusive")
		}
		return g.importPayment(ctx, *txid)
	}
	if *blocks < 1 {
		return usage("-blocks must be at least 1")
	}
	e, err := g.openWallet()
	if err != nil {
		return err
	}
	addr, err := e.FundAddress(e.Mainnet)
	if err != nil {
		return err
	}
	if err := checkFundAddress(addr, g.cfg.Network); err != nil {
		return err
	}
	rpc, asset, err := g.node(true)
	if err != nil {
		return err
	}
	unlock, err := g.lockHome()
	if err != nil {
		return err
	}
	defer unlock()
	tip, err := asset.BestHeader(ctx)
	if err != nil {
		return fmt.Errorf("node tip: %w", err)
	}
	fmt.Fprintf(g.stdout, "pool before: %d output(s), %d sat; node tip %d; fund address %s\n", e.Pool.Count(), e.Pool.Balance(), tip.Height, addr)
	var added int
	if *rescan {
		from := uint32(0)
		if uint32(*blocks) < tip.Height { //nolint:gosec // a flag value
			from = tip.Height - uint32(*blocks) //nolint:gosec // a flag value
		}
		added, err = bwallet.Rescan(ctx, e.Signer(), e.Pool, asset, from, tip.Height)
	} else {
		added, _, err = bwallet.FundFromCoinbase(ctx, e.Signer(), e.Pool, rpc, asset, *blocks, *batch)
	}
	if err != nil {
		return err
	}
	h := tip.Height
	if t2, err := asset.BestHeader(ctx); err == nil {
		h = t2.Height
	}
	fmt.Fprintf(g.stdout, "pool after:  %d output(s), %d sat (+%d); immature %d; node tip %d\n",
		e.Pool.Count(), e.Pool.Balance(), added, len(e.Pool.Immature(h)), h)
	return nil
}

// importPayment adds the outputs a mined payment pays to the fund address
// to the pool, once its proof holds against the header source.
func (g *global) importPayment(ctx context.Context, txid string) error {
	if h, err := chainhash.NewHashFromHex(txid); err != nil || h.String() != txid {
		return usage("-txid %q is not a transaction id (64 lowercase hex characters)", termsafe.Abbrev(txid))
	}
	e, err := g.openWallet()
	if err != nil {
		return err
	}
	hc, err := g.headerClient()
	if err != nil {
		return err
	}
	c := readerFor(g, nil, g.cfg.Timeout)
	if c.Txs == nil {
		return usage("fund -txid reads the payment from asset (a node) or tx_source (woc:main or woc:test); configure one")
	}
	tx, err := c.Fetch(ctx, txid, true)
	if err != nil {
		if errors.Is(err, reader.ErrOtherBytes) {
			return refused("%v", err)
		}
		return fmt.Errorf("payment %s: %w", txid, err)
	}
	ok, err := tx.MerklePath.Verify(ctx, tx.TxID(), hc)
	if err != nil {
		return fmt.Errorf("payment %s: checking its proof against %s: %w", txid, g.cfg.HeaderURL, err)
	}
	if !ok {
		return refused("payment %s: its proof does not hold against the header source %s", txid, g.cfg.HeaderURL)
	}
	fund, err := e.Signer().FundScript()
	if err != nil {
		return err
	}
	var outs []bwallet.Output
	var sats uint64
	for i, o := range tx.Outputs {
		if o.LockingScript == nil || !bytes.Equal(*o.LockingScript, *fund) {
			continue
		}
		outs = append(outs, bwallet.Output{TxID: txid, Vout: uint32(i), Satoshis: o.Satoshis, //nolint:gosec // an output index
			LockingScript: o.LockingScript.String(), Height: tx.MerklePath.BlockHeight, Coinbase: tx.IsCoinbase(),
			Raw: tx.Hex(), Bump: tx.MerklePath.Hex()})
		sats += o.Satoshis
	}
	addr, _ := e.FundAddress(e.Mainnet)
	if len(outs) == 0 {
		return usage("payment %s pays nothing to the fund address %s", txid, addr)
	}
	unlock, err := g.lockHome()
	if err != nil {
		return err
	}
	defer unlock()
	added, err := e.Pool.Add(outs...)
	if err != nil {
		return err
	}
	fmt.Fprintf(g.stdout, "imported %d of %d output(s) paying %s, %d sat, mined at height %d; pool %d output(s), %d sat\n",
		added, len(outs), addr, sats, tx.MerklePath.BlockHeight, e.Pool.Count(), e.Pool.Balance())
	if added < len(outs) {
		g.say("note: %d output(s) were in the pool already", len(outs)-added)
	}
	return nil
}

const streamHelp = `usage: blogs stream new <name>
       blogs stream list

new draws the stream's 10-letter suffix uniformly at random, creates its
state in the home, and prints the stream identifier <name>_<suffix>, its
topic, and the line a host's BLOGS_STREAMS takes to admit only this
producer. <name> is 1 to 32 lowercase letters and single underscores,
starting and ending with a letter. list prints the home's streams.`

func cmdStream(_ context.Context, g *global, args []string) error {
	fs := g.flagSet("stream", streamHelp)
	pos, err := parse(fs, args)
	if err != nil {
		return err
	}
	if len(pos) == 0 {
		return usage("stream new <name> | stream list")
	}
	e, err := g.openWallet()
	if err != nil {
		return err
	}
	id := e.Signer().IdentityHex()
	switch {
	case pos[0] == "new" && len(pos) == 2:
		if err := logrec.CheckStreamName(pos[1]); err != nil {
			return usage("stream name %q: 1 to %d lowercase letters and single underscores, starting and ending with a letter", pos[1], logrec.MaxStreamName)
		}
		if names, err := state.List(g.cfg.Home); err != nil {
			return err
		} else if len(names) >= limits.MaxStreams {
			return usage("this home holds %d streams, the limit: one produce runs per home at a time, and doctor and stream list read every stream's state (docs/limits.md); give another stream its own home", len(names))
		}
		s, err := logrec.NewStream(pos[1], nil)
		if err != nil {
			return err
		}
		if _, err := state.Create(g.cfg.Home, s, id); err != nil {
			return err
		}
		topic, _ := logrec.Topic(s)
		fmt.Fprintf(g.stdout, "stream   %s\ntopic    %s\nproducer %s\nhost     BLOGS_STREAMS=%s=%s\n", s, topic, id, s, id)
		return nil
	case pos[0] == "list" && len(pos) == 1:
		names, err := state.List(g.cfg.Home)
		if err != nil {
			return err
		}
		for _, n := range names {
			st, err := state.Load(g.cfg.Home, n)
			if err != nil {
				fmt.Fprintf(g.stdout, "%s  unreadable: %v\n", n, err)
				continue
			}
			fmt.Fprintf(g.stdout, "%s  seq %d  %s\n", n, st.Seq, anchorLine(st))
		}
		return nil
	}
	return usage("stream new <name> | stream list")
}

func anchorLine(st *state.Stream) string {
	s := "no anchor yet"
	if st.Anchor != nil {
		s = fmt.Sprintf("anchored to seq %d (interval %d)", st.NextFirst-1, st.Anchor.Interval)
	}
	if st.Inflight != nil {
		s += fmt.Sprintf(", interval %d in flight", st.Inflight.Interval)
	}
	if n := len(st.Unanchored); n > 0 {
		s += fmt.Sprintf(", %d unanchored", n)
	}
	return s
}

const doctorHelp = `usage: blogs doctor

Report the home (identity, pool, streams and their state) and whether the
configured node, header source and hosts answer. Reads only: it publishes
nothing and spends nothing.`

func cmdDoctor(ctx context.Context, g *global, args []string) error {
	fs := g.flagSet("doctor", doctorHelp)
	if pos, err := parse(fs, args); err != nil {
		return err
	} else if len(pos) > 0 {
		return usage("doctor takes no arguments")
	}
	out := g.stdout
	fmt.Fprintf(out, "home        %s\n", g.cfg.Home)
	fmt.Fprintf(out, "network     %s\n", g.cfg.Network)
	var tip uint32
	if g.cfg.Asset != "" {
		if h, err := (&nodeapi.Asset{Base: g.cfg.Asset}).BestHeader(ctx); err != nil {
			fmt.Fprintf(out, "node        %s: %v\n", g.cfg.Asset, err)
		} else {
			tip = h.Height
			fmt.Fprintf(out, "node        %s tip %d\n", g.cfg.Asset, h.Height)
		}
	} else {
		fmt.Fprintln(out, "node        not configured (asset)")
	}
	switch {
	case g.cfg.Asset != "":
		fmt.Fprintln(out, "tx source   the node (asset)")
	case g.cfg.TxSource != "":
		fmt.Fprintf(out, "tx source   %s\n", g.cfg.TxSource)
	default:
		fmt.Fprintln(out, "tx source   none: verify-interval, archive and backfill cannot read an anchor's fee tree (set asset or tx_source)")
	}
	if e, err := g.openWallet(); err != nil {
		fmt.Fprintf(out, "identity    absent (%v)\n", err)
	} else {
		fmt.Fprintf(out, "identity    %s\n", e.Signer().IdentityHex())
		fmt.Fprintf(out, "pool        %d output(s), %d sat", e.Pool.Count(), e.Pool.Balance())
		if tip > 0 {
			fmt.Fprintf(out, ", %d immature", len(e.Pool.Immature(tip)))
		}
		if n := len(e.Pool.UnprovenTxids()); n > 0 {
			fmt.Fprintf(out, ", change from %d transaction(s) held until mined", n)
		}
		fmt.Fprintln(out)
		names, err := state.List(g.cfg.Home)
		if err != nil {
			fmt.Fprintf(out, "streams     unreadable: %v\n", err)
		}
		for _, n := range names {
			st, err := state.Load(g.cfg.Home, n)
			if err != nil {
				fmt.Fprintf(out, "stream      %s UNREADABLE: %v\n", n, err)
				continue
			}
			fmt.Fprintf(out, "stream      %s seq %d, %s\n", n, st.Seq, anchorLine(st))
			if st.Pending != "" {
				fmt.Fprintf(out, "            seq %d persisted and not confirmed published; the next produce or anchor publishes it\n", st.Seq)
			}
			if st.Tree != nil {
				fmt.Fprintf(out, "            batch tree %s %d of %d left\n", st.Tree.Txid, st.Tree.Remaining(), st.Tree.Count)
			}
			if st.AnchorTree != nil {
				fmt.Fprintf(out, "            anchor tree %s %d of %d left\n", st.AnchorTree.Txid, st.AnchorTree.Remaining(), st.AnchorTree.Count)
			}
			for _, p := range st.PendingTrees {
				if st.MintedAhead(p.Tree.Txid) {
					fmt.Fprintf(out, "            %s tree %s minted ahead; the next produce, anchor or sweep holds it for the switch\n", p.Kind, p.Tree.Txid)
					continue
				}
				fmt.Fprintf(out, "            %s tree %s signed for coin %s and not adopted; the next produce, anchor or sweep asks the node what became of it\n", p.Kind, p.Tree.Txid, p.Coin.Outpoint())
			}
			for _, id := range st.Unpublished {
				fmt.Fprintf(out, "            tree %s adopted and not confirmed published; the next produce, anchor or sweep publishes it\n", id)
			}
			for _, sw := range st.Sweeps {
				switch {
				case sw.Failed != "":
					verb := "retracts"
					if sw.Kind == state.SweepRetire {
						verb = "retires"
					}
					fmt.Fprintf(out, "            sweep %s (%s) FAILED: %s; it %s nothing\n", sw.Txid, sw.Kind, termsafe.Text(sw.Failed), verb)
				case sw.InFlight():
					fmt.Fprintf(out, "            sweep %s (%s) in flight; the next sweep finishes it\n", sw.Txid, sw.Kind)
				}
			}
			if st.Tail != nil {
				fmt.Fprintf(out, "            following %s at offset %d\n", st.Tail.Path, st.Tail.Offset)
			}
		}
	}
	if g.cfg.HeaderURL == "" {
		fmt.Fprintln(out, "headers     NOT CONFIGURED (nothing can be verified)")
	} else if hc, err := g.headerClient(); err == nil {
		if n, err := hc.CurrentHeight(ctx); err != nil {
			fmt.Fprintf(out, "headers     %s: %v\n", g.cfg.HeaderURL, err)
		} else {
			fmt.Fprintf(out, "headers     %s tip %d\n", g.cfg.HeaderURL, n)
		}
	}
	switch {
	case g.cfg.Mode == config.ModeUnicast:
		need, err := config.Need(g.cfg.Quorum, len(g.cfg.Hosts))
		if err != nil {
			fmt.Fprintf(out, "mode        unicast: %v\n", err)
		} else {
			fmt.Fprintf(out, "mode        unicast: every object to each of %d host(s), published once %d take it (quorum %s)\n", len(g.cfg.Hosts), need, g.cfg.Quorum)
		}
		if g.cfg.Facade != "" {
			fmt.Fprintf(out, "facade      %s IGNORED: mode unicast submits to hosts, and produce refuses a facade\n", g.cfg.Facade)
		}
	case g.cfg.Facade != "":
		fmt.Fprintf(out, "mode        plane\n")
		fmt.Fprintf(out, "facade      %s\n", g.cfg.Facade)
	default:
		fmt.Fprintf(out, "mode        plane\n")
		fmt.Fprintln(out, "facade      not configured")
	}
	if kind, addr, _ := strings.Cut(g.cfg.Settle, ":"); kind == "arcade" {
		a := &publish.Arcade{Base: addr, Key: g.cfg.ArcadeKey}
		if err := a.Ping(ctx); err != nil {
			fmt.Fprintf(out, "settle      %s: %v\n", g.cfg.Settle, err)
		} else {
			fmt.Fprintf(out, "settle      %s answering\n", g.cfg.Settle)
		}
	} else if g.cfg.Settle != "" {
		fmt.Fprintf(out, "settle      %s\n", g.cfg.Settle)
	} else {
		fmt.Fprintln(out, "settle      not configured")
	}
	for _, h := range g.cfg.Hosts {
		fmt.Fprintf(out, "host        %s %s\n", h, ping(ctx, h, g.cfg.Timeout))
	}
	return nil
}

// ping asks a host a question no stream answers, which a live ls_log
// refuses with an error: the host is up and runs the service.
func ping(ctx context.Context, base string, timeout time.Duration) string {
	c := readerFor(nil, []string{base}, timeout)
	_, err := c.Ask(ctx, base, pingQuery)
	// The question is out of range, so a live ls_log refuses it (4xx): that
	// is the answer expected. A 5xx is a host that is up and failing.
	if err == nil || strings.Contains(err.Error(), "status 4") {
		return "answering"
	}
	if strings.Contains(err.Error(), "status 5") {
		return "answering with an ERROR (" + firstLine(err.Error()) + ")"
	}
	return "NOT ANSWERING: " + firstLine(err.Error())
}

func firstLine(s string) string {
	s, _, _ = strings.Cut(s, "\n")
	if len(s) > 160 {
		s = s[:160] + "..."
	}
	return s
}
