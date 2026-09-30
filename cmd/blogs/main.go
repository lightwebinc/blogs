// Command blogs is a producer and reader of blogs log streams: it creates a
// producer home and a stream, turns lines from stdin or a followed file into
// signed batches published on the stream's topic, anchors them in one mined
// token per interval, retracts batches by sweeping their funding outputs,
// and reads, audits, backfills and archives streams from overlay hosts,
// checking every answer itself.
//
// Exit codes: 0 done or verified; 1 refused (the evidence contradicts what
// was asked); 2 usage, configuration, local or transport error; 3
// incomplete (a host held nothing where evidence was needed, which proves
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
  fund [-blocks N]             mine coinbase to the fund address (a chain you run)
  stream new <name>            create a stream: <name>_<random suffix>, and its topic
  stream list                  the streams this home produces
  produce <stream>             read lines from stdin or -file, publish batches, anchor intervals
  anchor <stream>              anchor every published batch now and wait for it
  sweep <stream> (-retract-seqs A-B | -retract-tree TXID | -retire) [-yes]
                               retract batches, or retire unused funding outputs
  doctor                       local state and reachability; reads only

reader commands (no home needed):
  read <stream> <producer>     fetch, verify and print a stream's batches
  verify-interval <stream> <producer> <n>
                               audit interval n: the anchor chain, its proof, its root
  backfill <stream> <producer> [-from URL] [-to URL]
                               fill a host's holes from its siblings; with
                               neither flag, bring the configured hosts into step
  archive <stream> <producer> -dir DIR [-host URL]
                               keep a host's copy of a stream, with interval
                               proofs, in local files

  version                      print the version

global flags:
  -config PATH        config file (default $BLOGS_HOME/config,
                      $XDG_CONFIG_HOME/blogs/config, ~/.blogs/config)
  -home DIR           producer home (default ~/.blogs)
  -hosts URLS         overlay hosts, comma separated: the hosts a reader
                      asks, and in mode unicast every host a producer submits to
  -header-url SOURCE  header source: a bridge URL, woc:main, woc:test or
                      chaintracks:URL; required to verify, no default
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
		cfgPath   = fs.String("config", "", "")
		home      = fs.String("home", "", "")
		hosts     = fs.String("hosts", "", "")
		headerURL = fs.String("header-url", "", "")
		mode      = fs.String("mode", "", "")
		network   = fs.String("network", "", "")
		quorum    = fs.String("quorum", "", "")
		timeout   = fs.String("timeout", "", "")
		verbose   = fs.Bool("v", false, "")
		showVer   = fs.Bool("version", false, "")
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
	for k, v := range map[string]string{"home": *home, "hosts": *hosts, "header_url": *headerURL, "mode": *mode, "network": *network, "quorum": *quorum, "timeout": *timeout} {
		if v != "" {
			over[k] = v
		}
	}
	if cfg, err = cfg.Apply(over); err != nil {
		fmt.Fprintln(stderr, "blogs:", err)
		return exitUsage
	}
	if err := checkHeaderSource(cfg.HeaderURL, cfg.Network); err != nil {
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

// headerClient is the configured header source, held to the network's
// proof-of-work floor when it serves header fields.
func (g *global) headerClient() (*headers.Client, error) {
	if g.cfg.HeaderURL == "" {
		return nil, usage("no header source: set header_url (or -header-url); a proof is only as good as the headers it is checked against")
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
