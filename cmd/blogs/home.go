package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	base58 "github.com/bsv-blockchain/go-sdk/compat/base58"

	"github.com/lightwebinc/bcommon/bwallet"
	"github.com/lightwebinc/bcommon/feepolicy"
	"github.com/lightwebinc/bcommon/guard"
	"github.com/lightwebinc/bcommon/headers"
	"github.com/lightwebinc/bcommon/mint"
	"github.com/lightwebinc/bcommon/nodeapi"
	"github.com/lightwebinc/bcommon/publish"
	"github.com/lightwebinc/bcommon/termsafe"

	"github.com/lightwebinc/bcommon/unicast"
	"github.com/lightwebinc/blogs/internal/config"
	"github.com/lightwebinc/blogs/internal/limits"
	"github.com/lightwebinc/blogs/internal/logrec"
	"github.com/lightwebinc/blogs/internal/produce"
	"github.com/lightwebinc/blogs/internal/state"
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

// node is the configured node, for coinbase on a regtest chain you run:
// its JSON-RPC and its asset API (asset, or a chain view that is one node).
func (g *global) node() (*nodeapi.RPC, *nodeapi.Asset, error) {
	base := g.cfg.Asset
	if kind, rest, _ := strings.Cut(g.cfg.Chain, ":"); base == "" && kind == "asset" && !strings.Contains(rest, ",") {
		base = rest
	}
	if base == "" || g.cfg.RPC == "" {
		return nil, nil, usage("coinbase needs a node: configure rpc and asset (config keys rpc, asset); on mainnet or testnet, pay the fund address from your own wallet and run `blogs fund -txid TXID` or `blogs fund -beef FILE`")
	}
	rpc := &nodeapi.RPC{URL: g.cfg.RPC, User: g.cfg.RPCUser, Pass: g.cfg.RPCPass, ID: "blogs"}
	return rpc, &nodeapi.Asset{Base: base}, nil
}

// chain is the configured chain view, every proof it answers checked
// against the header source, and that header source.
func (g *global) chain() (*nodeapi.Sources, *headers.Client, error) {
	hc, err := g.headerClient()
	if err != nil {
		return nil, nil, err
	}
	if g.cfg.Chain == "" {
		return nil, nil, usage("no chain view: set chain (or -chain): woc:main, woc:test, or asset:URL for a node")
	}
	src, err := nodeapi.ParseChain(g.cfg.Chain, nodeapi.ChainOptions{Headers: hc, WoCKey: g.cfg.WoCKey, WoCRate: g.cfg.WoCRate, Client: httpClient})
	if err != nil {
		return nil, nil, usage("chain %s: %v", g.cfg.Chain, err)
	}
	if wocBase != "" {
		for _, t := range src.Tx {
			if w, ok := t.(*nodeapi.WoC); ok {
				w.Base, w.Rate = strings.TrimRight(wocBase, "/")+"/"+w.Network, 1000
			}
		}
	}
	return src, hc, nil
}

// fees is the fee policy for this run (feepolicy): static at the
// configured rate, by default the network's 100 satoshis per 1000 bytes,
// or the broadcaster's published policy with fee_source = arc, whose URLs
// default to the arcade or ARC installation settle names. from says where
// a live rate came from, and is empty for a static one.
func (g *global) fees(ctx context.Context, arcade *publish.Arcade) (f mint.Fees, from string, err error) {
	fc, err := g.cfg.FeeConfig()
	if err != nil {
		return mint.Fees{}, "", usage("%v", err)
	}
	live := fc.Source != "" && fc.Source != feepolicy.SourceStatic
	if live && len(fc.PolicyURLs) == 0 && arcade != nil {
		fc.PolicyURLs = []string{arcade.Base}
	}
	src, err := fc.Build(mint.DefaultFees)
	if err != nil {
		return mint.Fees{}, "", usage("%v (config keys fee_*)", err)
	}
	if f, err = src.Fees(ctx); err != nil {
		return mint.Fees{}, "", err
	}
	if a, ok := src.(*feepolicy.ARC); ok {
		st := a.Status()
		from = fmt.Sprintf("%s (%s", strings.Join(fc.PolicyURLs, ", "), st.Source)
		if st.Err != nil {
			from += ": " + st.Err.Error()
		}
		from += ")"
	}
	return f, from, nil
}

// legs are the endpoints a producer publishes and settles through. The
// hosts and the facade have no default; the chain view, header source and
// broadcaster default to the public services on mainnet and testnet.
func (g *global) legs() (produce.Legs, error) {
	var l produce.Legs
	chain, hc, err := g.chain()
	if err != nil {
		return l, err
	}
	l.Chain, l.Tip = chain, hc
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
	if g.cfg.Settle == "" {
		return l, usage("settle must be configured (config key settle): arcade:URL, arc:URL, rpc:URL or tcp:host:port; a regtest chain has no public broadcaster")
	}
	// Arcade's verdict is held to the chain view's spends, so a
	// transaction whose input another already spent is refused whatever
	// arcade answered.
	l.Settler, l.Arcade, err = publish.ParseSettler(g.cfg.Settle, publish.SettleOptions{Key: g.cfg.ArcadeKey,
		RPCUser: g.cfg.RPCUser, RPCPass: g.cfg.RPCPass, RPCID: "blogs", Spends: chain})
	if err != nil {
		return l, usage("settle must be arcade:main, arcade:test, arcade:URL, arc:URL, rpc:URL or tcp:host:port (config key settle): %v", err)
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

const fundHelp = `usage: blogs fund -txid TXID
       blogs fund -beef FILE|-  [-mined-only]
       blogs fund [-blocks N] [-batch N] [-rescan]   (coinbase: only on a regtest chain you run)

Import a payment to the fund address that init printed, and add every
output it pays to that address to the pool. This is how a producer on
mainnet or testnet is funded, with no node: send BSV from your own wallet
to the fund address, then either

  blogs fund -beef payment.beef    the payment as BEEF, as your wallet
                                   hands it over (a file, or - for stdin)
  blogs fund -txid TXID            once it has one confirmation

-beef reads nothing from the network: the payment's proof, or for one not
mined yet its parents' proofs, are checked against the header source. A
payment not mined yet is held back until its proof arrives (fund_mined_only
or -mined-only refuses it). -txid reads the mined payment and its proof
from the chain view (chain: WhatsOnChain by default, or a node) and checks
the proof against the header source.

Without either (coinbase: only on a regtest chain you run, for development
and tests), mine coinbase to the fund address through the node's
generatetoaddress, which no public network answers, and add the outputs
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
	blocks := fs.Int("blocks", 101, "coinbase: only on a regtest chain you run (development and tests): blocks to mine to the fund address")
	batch := fs.Int("batch", bwallet.DefaultFundBatch, "coinbase: only on a regtest chain you run (development and tests): blocks per generatetoaddress call")
	rescan := fs.Bool("rescan", false, "coinbase: only on a regtest chain you run (development and tests): re-read the last -blocks blocks for coinbase the pool lacks instead of mining")
	txid := fs.String("txid", "", "import this mined payment to the fund address (mainnet or testnet: pay the fund address from your own wallet first)")
	beef := fs.String("beef", "", "import the payment to the fund address from this BEEF file, or - for stdin, as your wallet hands it over")
	minedOnly := fs.Bool("mined-only", g.cfg.FundMinedOnly, "with -beef: refuse a payment that has not mined yet (config key fund_mined_only)")
	if pos, err := parse(fs, args); err != nil {
		return err
	} else if len(pos) > 0 {
		return usage("fund takes no arguments")
	}
	if *txid != "" || *beef != "" {
		if *rescan || (*txid != "" && *beef != "") {
			return usage("-txid, -beef and -rescan are exclusive")
		}
		return g.importPayment(ctx, *txid, *beef, *minedOnly)
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
	rpc, asset, err := g.node()
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

// importPayment adds the outputs a payment pays to the fund address to
// the pool: from its BEEF (beefPath, - for stdin), checked against the
// header source with no lookup, or by txid from the chain view, its proof
// checked against the header source.
func (g *global) importPayment(ctx context.Context, txid, beefPath string, minedOnly bool) error {
	if txid != "" {
		if h, err := chainhash.NewHashFromHex(txid); err != nil || h.String() != txid {
			return usage("-txid %q is not a transaction id (64 lowercase hex characters)", termsafe.Abbrev(txid))
		}
	}
	e, err := g.openWallet()
	if err != nil {
		return err
	}
	fund, err := e.Signer().FundScript()
	if err != nil {
		return err
	}
	var im *bwallet.Import
	if beefPath != "" {
		beef, err := readBEEF(g, beefPath)
		if err != nil {
			return err
		}
		hc, err := g.headerClient()
		if err != nil {
			return err
		}
		im, err = bwallet.ImportBEEF(ctx, beef, fund, hc, bwallet.ImportOptions{RefuseUnmined: minedOnly})
		if err != nil {
			return g.importError(err, minedOnly)
		}
	} else {
		chain, hc, err := g.chain()
		if err != nil {
			return err
		}
		if im, err = bwallet.ImportTxid(ctx, txid, fund, chain, hc); err != nil {
			return g.importError(err, false)
		}
	}
	// A coinbase output keeps its maturity.
	if im.Tx.IsCoinbase() {
		for i := range im.Outputs {
			im.Outputs[i].Coinbase = true
		}
	}
	unlock, err := g.lockHome()
	if err != nil {
		return err
	}
	defer unlock()
	added, err := e.Pool.Add(im.Outputs...)
	if err != nil {
		return err
	}
	// A payment imported before it mined, imported again with its proof,
	// is spendable now.
	if im.Mined && added < len(im.Outputs) {
		if n, err := e.Pool.Prove(im.Txid, im.Tx.MerklePath.Hex(), im.Height); err != nil {
			return err
		} else if n > 0 {
			g.say("%d output(s) held until mined are spendable now", n)
		}
	}
	addr, _ := e.FundAddress(e.Mainnet)
	at := fmt.Sprintf("mined at height %d", im.Height)
	if !im.Mined {
		at = "not mined yet: held until its proof arrives, which the next produce, anchor or sweep collects"
	}
	fmt.Fprintf(g.stdout, "imported %d of %d output(s) paying %s, %d sat, %s; pool %d output(s), %d sat\n",
		added, len(im.Outputs), addr, im.Sats, at, e.Pool.Count(), e.Pool.Balance())
	if added < len(im.Outputs) {
		g.say("note: %d output(s) were in the pool already", len(im.Outputs)-added)
	}
	return nil
}

// importError words an import's refusal: a payment that is not for this
// wallet, or not mined, or whose proof does not hold, is refused (1); the
// rest are errors.
func (g *global) importError(err error, minedOnly bool) error {
	switch {
	case errors.Is(err, bwallet.ErrPaysNothing):
		e, oerr := g.openWallet()
		if oerr == nil {
			addr, _ := e.FundAddress(e.Mainnet)
			return refused("%v (the fund address is %s)", err, addr)
		}
		return refused("%v", err)
	case errors.Is(err, bwallet.ErrUnmined) && minedOnly:
		return refused("%v; fund_mined_only refuses it until it has a block", err)
	case errors.Is(err, bwallet.ErrUnmined):
		return refused("%v; import it once it has a block, or hand over its BEEF with -beef", err)
	case errors.Is(err, bwallet.ErrUnprovenParent), errors.Is(err, nodeapi.ErrProofRefused):
		return refused("%v", err)
	}
	return err
}

// readBEEF reads a payment's BEEF from a file or, for -, stdin: binary, or
// hex as wallets also print it.
func readBEEF(g *global, path string) ([]byte, error) {
	var b []byte
	var err error
	if path == "-" {
		b, err = io.ReadAll(io.LimitReader(g.stdin, maxBEEFFile+1))
	} else {
		var f *os.File
		if f, err = os.Open(path); err == nil {
			b, err = io.ReadAll(io.LimitReader(f, maxBEEFFile+1))
			f.Close()
		}
	}
	if err != nil {
		return nil, err
	}
	if len(b) > maxBEEFFile {
		return nil, usage("-beef %s: over %d bytes", path, maxBEEFFile)
	}
	if t := bytes.TrimSpace(b); len(t) > 0 && len(t)%2 == 0 {
		if raw, herr := hex.DecodeString(string(t)); herr == nil {
			return raw, nil
		}
	}
	return b, nil
}

// maxBEEFFile bounds a payment's BEEF as read, as hex; bwallet holds the
// decoded BEEF to its own bound.
const maxBEEFFile = 2 * guard.DefaultBound

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

Report the home (identity, pool, streams and their state), the chain view,
the fee policy, and whether the configured node, header source,
broadcaster and hosts answer. Reads only: it publishes
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
		fmt.Fprintln(out, "node        none (none is needed: the chain view, header source and broadcaster serve a producer)")
	}
	// The header source's tip, asked once: maturity is judged at it when
	// no node is configured.
	headersLine := "headers     NOT CONFIGURED (nothing can be verified)"
	if g.cfg.HeaderURL != "" {
		if hc, err := g.headerClient(); err == nil {
			if n, err := hc.CurrentHeight(ctx); err != nil {
				headersLine = fmt.Sprintf("headers     %s: %v", g.cfg.HeaderURL, err)
			} else {
				if tip == 0 {
					tip = n
				}
				headersLine = fmt.Sprintf("headers     %s tip %d", g.cfg.HeaderURL, n)
			}
		}
	}
	if g.cfg.Chain == "" {
		fmt.Fprintln(out, "chain       NOT CONFIGURED: fund -txid, produce, verify-interval, archive and backfill need a chain view (set chain)")
	} else {
		fmt.Fprintf(out, "chain       %s\n", g.cfg.Chain)
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
				fmt.Fprintf(out, "            %s tree %s signed for coin %s and not adopted; the next produce, anchor or sweep asks the chain view what became of it\n", p.Kind, p.Tree.Txid, p.Coin.Outpoint())
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
	fmt.Fprintln(out, headersLine)
	var arcade *publish.Arcade
	if g.cfg.Settle != "" {
		_, arcade, _ = publish.ParseSettler(g.cfg.Settle, publish.SettleOptions{Key: g.cfg.ArcadeKey})
	}
	if f, from, err := g.fees(ctx, arcade); err != nil {
		fmt.Fprintf(out, "fees        %v\n", err)
	} else if from != "" {
		fmt.Fprintf(out, "fees        %s sat/bytes, floor %d, arc: %s\n", f.Rate, f.Floor, from)
	} else {
		fmt.Fprintf(out, "fees        %s sat/bytes, floor %d, static\n", f.Rate, f.Floor)
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
	if g.cfg.Settle == "" {
		fmt.Fprintln(out, "settle      not configured")
	} else if _, a, err := publish.ParseSettler(g.cfg.Settle, publish.SettleOptions{Key: g.cfg.ArcadeKey}); err != nil {
		fmt.Fprintf(out, "settle      %s: %v\n", g.cfg.Settle, err)
	} else if a != nil {
		if err := a.Ping(ctx); err != nil {
			fmt.Fprintf(out, "settle      %s (%s): %v\n", g.cfg.Settle, a.Base, err)
		} else {
			fmt.Fprintf(out, "settle      %s (%s) answering\n", g.cfg.Settle, a.Base)
		}
	} else {
		fmt.Fprintf(out, "settle      %s\n", g.cfg.Settle)
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
