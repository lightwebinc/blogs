// Command blogs is a producer and reader of blogs log streams: it creates a
// producer home and a stream, turns lines from stdin or a followed file into
// signed batches published on the stream's topic, anchors them in one mined
// token per interval, retracts batches by sweeping their funding outputs,
// and reads, audits, backfills and archives streams from overlay hosts,
// checking every answer itself.
//
// Exit codes: 0 done or verified; 1 refused (what verified contradicts what
// was asked); 2 usage, configuration, local or transport error; 3
// incomplete (a host held nothing where evidence was needed, or answered
// something that does not verify, which is named and not used; it proves
// nothing either way). docs/usage.md is the reference.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/lightwebinc/bcommon/headers"
	"github.com/lightwebinc/bcommon/nodeapi"

	"github.com/lightwebinc/blogs/internal/config"
)

// version is stamped by the build; "dev" otherwise.
var version = "dev"

// The exit codes.
const (
	exitOK         = 0
	exitRefused    = 1
	exitUsage      = 2
	exitIncomplete = 3
)

const usageText = `usage: blogs [global flags] <command> [flags] [arguments]

producer commands (a producer home):
  init                         create the home: identity key and coin pool
  fund -beef FILE|-            import a payment to the fund address as BEEF, as
                               your wallet hands it over: how a producer on
                               mainnet or testnet is funded, with no node
  fund -txid TXID              the same for a mined payment, by its txid
  fund [-blocks N] [-rescan]   coinbase: only on a regtest chain you run
                               (development and tests)
  stream new <name>            create a stream: <name>_<random suffix>, and its topic
  stream list                  the streams this home produces
  produce <stream>             read lines from stdin or -file, publish batches, anchor intervals
  anchor <stream>              anchor every published batch now and wait for it
  sweep <stream> (-retract-seqs A-B | -retract-tree TXID | -retire) [-yes]
                               retract batches, or retire unused funding outputs
  doctor                       local state and reachability; reads only
  ingest -stream S [-confidential-stream S2] [doors] [-loki URL]
                               the logging sidecar: syslog, OTLP logs and Loki
                               push in, signed batches out, each line forwarded
                               to Loki with where it was published

reader commands (no home needed):
  read <stream> <producer>     fetch, verify and print a stream's batches
  verify-interval <stream> <producer> <n>
                               audit interval n: the anchor chain, its proof, its root
  backfill <stream> <producer> [-from URL] [-to URL] [-sweeps]
                               fill a host's holes from its siblings; with
                               neither flag, bring the configured hosts into
                               step; -sweeps also carries sweeps it lacks
  archive <stream> <producer> -dir DIR [-host URL] [-recheck N | -recheck-all]
                               keep a host's copy of a stream, with interval
                               proofs, in local files
  gateway -archive DIR -loki URL
                               the Loki API in front of Loki, marking each line
                               verified or unverified against the archive

  version                      print the version

global flags:
  -config PATH        config file (default $BLOGS_HOME/config,
                      $XDG_CONFIG_HOME/blogs/config, ~/.blogs/config)
  -home DIR           producer home (default ~/.blogs)
  -hosts URLS         overlay hosts, comma separated: the hosts a reader
                      asks, and in mode unicast every host a producer submits to
  -chain SPEC         chain view: woc:main or woc:test (WhatsOnChain, the
                      default on mainnet and testnet), asset:URL (a node),
                      or both, comma separated
  -header-url SOURCE  header source: woc:main or woc:test (the default on
                      mainnet and testnet), bhs:URL, arcade:URL,
                      chaintracks:URL or a bridge URL
  -settle SPEC        broadcaster: arcade:main or arcade:test (the default
                      on mainnet and testnet), arcade:URL, arc:URL,
                      rpc:URL or tcp:host:port
  -fee-rate SATS/BYTES  miner fee rate (default 100/1000)
  -fee-source SRC     static (default) or arc: the broadcaster's published rate
  -fee-floor SATS     least fee a transaction pays (default 100)
  -fee-max-rate SATS/BYTES  most a rate may be (default 100/1000)
  -mode MODE          plane (submit once to facade; the default) or unicast
                      (submit to each of hosts)
  -network NET        main, test or regtest (default main)
  -quorum Q           mode unicast: hosts that must take an object for it to
                      count as published: all (default), majority, one or N
  -timeout DUR        per request (default 15s)
  -v                  verbose
  -version            print the version and exit

Run 'blogs <command> -h' for a command's flags. Exit codes: 0 done or
verified, 1 refused, 2 usage or transport error, 3 incomplete.
`

// exitError carries an exit code with a message.
type exitError struct {
	code int
	msg  string
}

func (e *exitError) Error() string { return e.msg }

func usage(format string, args ...any) error {
	return &exitError{exitUsage, fmt.Sprintf(format, args...)}
}

func refused(format string, args ...any) error {
	return &exitError{exitRefused, fmt.Sprintf(format, args...)}
}

func incomplete(format string, args ...any) error {
	return &exitError{exitIncomplete, fmt.Sprintf(format, args...)}
}

func isHelp(arg string) bool {
	return arg == "help" || arg == "-h" || arg == "-help" || arg == "--help"
}

// helpOrUsage answers a flag-parsing failure: a help request exits 0 (the
// FlagSet has printed its text), anything else is a usage error.
func helpOrUsage(err error) error {
	if errors.Is(err, flag.ErrHelp) {
		return &exitError{exitOK, ""}
	}
	return &exitError{exitUsage, ""}
}

// global is the resolved configuration and the shared flags.
type global struct {
	cfg     config.Config
	verbose bool
	stdin   io.Reader
	stdout  io.Writer
	stderr  io.Writer
	getenv  func(string) string
}

func (g *global) say(format string, args ...any) { fmt.Fprintf(g.stderr, format+"\n", args...) }

type command func(ctx context.Context, g *global, args []string) error

var commands map[string]command

func init() {
	commands = map[string]command{
		"init":            cmdInit,
		"fund":            cmdFund,
		"stream":          cmdStream,
		"produce":         cmdProduce,
		"anchor":          cmdAnchor,
		"doctor":          cmdDoctor,
		"read":            cmdRead,
		"verify-interval": cmdVerifyInterval,
		"backfill":        cmdBackfill,
		"sweep":           cmdSweep,
		"archive":         cmdArchive,
		"ingest":          cmdIngest,
		"gateway":         cmdGateway,
	}
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr, os.Getenv)
	stop()
	os.Exit(code)
}

// run is the whole command, with its streams and environment passed in so
// tests drive it in process.
func run(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer, getenv func(string) string) int {
	fs := flag.NewFlagSet("blogs", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { fmt.Fprint(stderr, usageText) }
	var (
		cfgPath    = fs.String("config", "", "")
		home       = fs.String("home", "", "")
		hosts      = fs.String("hosts", "", "")
		chain      = fs.String("chain", "", "")
		feeFloor   = fs.String("fee-floor", "", "")
		feeMaxRate = fs.String("fee-max-rate", "", "")
		feeRate    = fs.String("fee-rate", "", "")
		feeSource  = fs.String("fee-source", "", "")
		headerURL  = fs.String("header-url", "", "")
		settle     = fs.String("settle", "", "")
		mode       = fs.String("mode", "", "")
		network    = fs.String("network", "", "")
		quorum     = fs.String("quorum", "", "")
		timeout    = fs.String("timeout", "", "")
		txSource   = fs.String("tx-source", "", "")
		verbose    = fs.Bool("v", false, "")
		showVer    = fs.Bool("version", false, "")
	)
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitUsage
	}
	if *showVer {
		fmt.Fprintln(stdout, "blogs", version)
		return exitOK
	}
	rest := fs.Args()
	if len(rest) == 0 {
		fs.Usage()
		return exitUsage
	}
	switch {
	case isHelp(rest[0]):
		fmt.Fprint(stdout, usageText)
		return exitOK
	case rest[0] == "version":
		fmt.Fprintln(stdout, "blogs", version)
		return exitOK
	}
	cmd, ok := commands[rest[0]]
	if !ok {
		fmt.Fprintf(stderr, "blogs: unknown command %q\n", rest[0])
		fs.Usage()
		return exitUsage
	}
	cfg, err := config.Load(config.Path(*cfgPath, getenv), getenv)
	if err != nil {
		fmt.Fprintln(stderr, "blogs:", err)
		return exitUsage
	}
	over := map[string]string{}
	for k, v := range map[string]string{"chain": *chain, "fee_floor": *feeFloor, "fee_max_rate": *feeMaxRate, "fee_rate": *feeRate,
		"fee_source": *feeSource, "home": *home, "hosts": *hosts, "header_url": *headerURL, "mode": *mode, "network": *network,
		"quorum": *quorum, "settle": *settle, "timeout": *timeout, "tx_source": *txSource} {
		if v != "" {
			over[k] = v
		}
	}
	if cfg, err = cfg.Apply(over); err != nil {
		fmt.Fprintln(stderr, "blogs:", err)
		return exitUsage
	}
	if n := strings.TrimPrefix(cfg.TxSource, "woc:"); cfg.TxSource != "" && n != cfg.Network {
		fmt.Fprintf(stderr, "blogs: tx_source %s is the %s network but network is %s\n", cfg.TxSource, n, cfg.Network)
		return exitUsage
	}
	cfg = cfg.WithDefaults()
	if err := checkHeaderSource(cfg.HeaderURL, cfg.Network); err != nil {
		fmt.Fprintln(stderr, "blogs:", err)
		return exitUsage
	}
	if err := checkChain(cfg.Chain, cfg.Network); err != nil {
		fmt.Fprintln(stderr, "blogs:", err)
		return exitUsage
	}
	g := &global{cfg: cfg, verbose: *verbose, stdin: stdin, stdout: stdout, stderr: stderr, getenv: getenv}
	err = cmd(ctx, g, rest[1:])
	var ee *exitError
	switch {
	case err == nil:
		return exitOK
	case errors.As(err, &ee):
		if ee.msg != "" {
			fmt.Fprintln(stderr, "blogs:", ee.msg)
		}
		return ee.code
	default:
		fmt.Fprintln(stderr, "blogs:", err)
		return exitUsage
	}
}

// checkHeaderSource refuses a header source that does not parse, or a
// WhatsOnChain source for another network than the configured one.
func checkHeaderSource(spec, network string) error {
	if spec == "" {
		return nil
	}
	kind, _, implied, err := headers.Parse(spec)
	if err != nil {
		return err
	}
	if kind == headers.WhatsOnChain && implied != network {
		return fmt.Errorf("header source %s is the %s network but network is %s", spec, implied, network)
	}
	return nil
}

// checkChain refuses a chain view that does not parse, or a WhatsOnChain
// backend for another network than the configured one.
func checkChain(spec, network string) error {
	if spec == "" {
		return nil
	}
	if _, err := nodeapi.ParseChain(spec, nodeapi.ChainOptions{Headers: headers.New("woc:main")}); err != nil {
		return fmt.Errorf("chain %s: %w", spec, err)
	}
	for _, item := range strings.Split(spec, ",") {
		item = strings.TrimSpace(item)
		if m, rest, ok := strings.Cut(item, "="); ok && !strings.Contains(m, ":") {
			item = rest
		}
		if n, ok := strings.CutPrefix(item, "woc:"); ok && n != network {
			return fmt.Errorf("chain %s names WhatsOnChain's %s network but network is %s", spec, n, network)
		}
	}
	return nil
}

// headerClient is the configured header source, held to the network's
// proof-of-work floor when it serves header fields.
func (g *global) headerClient() (*headers.Client, error) {
	if g.cfg.HeaderURL == "" {
		return nil, usage("no header source: set header_url (or -header-url; a regtest chain has no default); a proof is only as good as the headers it is checked against")
	}
	c := headers.New(g.cfg.HeaderURL)
	if c.Kind != headers.Native {
		c.Network = g.cfg.Network
	}
	c.Timeout = g.cfg.Timeout
	return c, nil
}

// flagSet is a command's FlagSet, printing its help text on -h.
func (g *global) flagSet(name, help string) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(g.stderr)
	fs.BoolVar(&g.verbose, "v", g.verbose, "verbose (also a global flag)")
	fs.Usage = func() {
		fmt.Fprintln(g.stderr, strings.TrimSpace(help))
		fmt.Fprintln(g.stderr, "\nflags:")
		fs.PrintDefaults()
	}
	return fs
}

// parse parses a command's flags, allowing them after its positional
// arguments too, and returns the positionals.
func parse(fs *flag.FlagSet, args []string) ([]string, error) {
	var pos []string
	for {
		if err := fs.Parse(args); err != nil {
			return nil, helpOrUsage(err)
		}
		args = fs.Args()
		if len(args) == 0 {
			return pos, nil
		}
		if args[0] == "--" {
			return append(pos, args[1:]...), nil
		}
		pos = append(pos, args[0])
		args = args[1:]
	}
}
