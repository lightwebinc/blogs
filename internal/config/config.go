// Package config resolves the blogs command's settings: flags, then BLOGS_*
// in the environment, then the config file, then the built-in default.
//
// The overlay hosts and the facade have no default: a default there would
// quietly send a producer's objects or a reader's questions to somebody
// else's server, so a command that needs one and finds none is a usage
// error. The chain services do, on mainnet and testnet, so that a producer
// needs no infrastructure of its own (WithDefaults): WhatsOnChain for
// headers and the chain view, GorillaPool's public arcade to broadcast.
// Every one of them is a setting, and a node of one's own is the better
// choice where there is one. A regtest chain has no public services and
// so no defaults.
package config

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/lightwebinc/bcommon/feepolicy"
	"github.com/lightwebinc/bcommon/mint"
	"github.com/lightwebinc/bcommon/publish"

	"github.com/lightwebinc/blogs/internal/limits"
)

// Config is the resolved settings.
type Config struct {
	// ArcadeKey is the bearer token for the arcade or ARC installation in
	// Settle.
	ArcadeKey string
	// Asset is a node's asset API base URL. It is the chain view when Chain
	// is not set (asset:<Asset>), and the node coinbase is read from on a
	// regtest chain.
	Asset string
	// Chain is the chain view (nodeapi.ParseChain): where transactions,
	// proofs and spends are read. woc:main or woc:test (WhatsOnChain, no
	// node), asset:URL (a node), or a comma-separated combination.
	Chain string
	// Facade is the overlay host a producer publishes to.
	Facade string
	// The fee settings (feepolicy.Config), as written: fee_rate,
	// fee_min_rate and fee_max_rate are sats/bytes, the others whole
	// numbers, a source name or a URL list. Empty is the default.
	FeeDust, FeeFloor, FeeMaxRate, FeeMaxTx, FeeMinRate, FeePolicyURLs, FeeRate, FeeSource string
	// FundMinedOnly refuses a funding payment (fund -beef) that has not
	// mined yet.
	FundMinedOnly bool
	// HeaderURL is the header source every proof is checked against:
	// woc:main, woc:test, bhs:URL, arcade:URL, chaintracks:URL or a
	// bridge's /v1 base URL.
	HeaderURL string
	// Home is the producer's directory: identity, coin pool and streams.
	Home string
	// Hosts are the overlay hosts a reader asks, in order. In unicast mode
	// they are also every host a producer submits to.
	Hosts []string
	// Mode is how a producer's objects reach the hosts: "plane" (one
	// submit to Facade, which the multicast plane delivers to every
	// subscribed host) or "unicast" (one submit to each of Hosts).
	Mode string
	// Network is "main", "test" or "regtest". It picks the fund address's
	// prefix and the proof-of-work floor a header source is held to.
	Network string
	// ObjectBound is the largest object the plane takes, in bytes: a
	// carrier's BEEF over it is not published.
	ObjectBound int
	// Originator is the BRC-100 originator presented to the wallet.
	Originator string
	// Quorum is, in unicast mode, how many of Hosts must take an object
	// for it to count as published: "all", "majority", "one", or a number.
	Quorum string
	// RPC is the node's JSON-RPC URL; RPCUser and RPCPass its basic auth.
	RPC     string
	RPCPass string
	RPCUser string
	// Settle is the settlement leg (publish.ParseSettler): arcade:main,
	// arcade:test, arcade:<url>, arc:<url>, rpc:<url> or tcp:<host:port>.
	Settle string
	// Timeout bounds each request to a host, node or header source.
	Timeout time.Duration
	// TxSource is the older name of a WhatsOnChain chain view, woc:main or
	// woc:test: the chain view when neither Chain nor Asset is set.
	TxSource string
	// WoCKey is a WhatsOnChain API key, and WoCRate the requests a second
	// its plan allows; zero is the free tier's 3.
	WoCKey  string
	WoCRate float64
}

// WithDefaults fills in the chain services a network has defaults for:
// the chain view (asset:<Asset> when a node is configured, else tx_source,
// else woc:<network>), the header source (woc:<network>) and the
// settlement leg (publish.DefaultSettle: GorillaPool's public arcade). A
// regtest chain gets only the node as its chain view.
func (c Config) WithDefaults() Config {
	if c.Chain == "" {
		switch {
		case c.Asset != "":
			c.Chain = "asset:" + c.Asset
		case c.TxSource != "":
			c.Chain = c.TxSource
		case c.Network != "regtest":
			c.Chain = "woc:" + c.Network
		}
	}
	if c.Network == "regtest" {
		return c
	}
	if c.HeaderURL == "" {
		c.HeaderURL = "woc:" + c.Network
	}
	if c.Settle == "" {
		c.Settle, _ = publish.DefaultSettle(c.Network)
	}
	return c
}

// FeeConfig is the fee settings as a feepolicy.Config; Apply has checked
// each of them.
func (c Config) FeeConfig() (*feepolicy.Config, error) {
	fc := &feepolicy.Config{Source: c.FeeSource}
	rate := func(s string) (*mint.Rate, error) {
		if s == "" {
			return nil, nil
		}
		r, err := mint.ParseRate(s)
		return &r, err
	}
	num := func(s string) (*uint64, error) {
		if s == "" {
			return nil, nil
		}
		n, err := strconv.ParseUint(s, 10, 64)
		return &n, err
	}
	var err error
	if fc.Rate, err = rate(c.FeeRate); err != nil {
		return nil, fmt.Errorf("config: fee_rate: %w", err)
	}
	if fc.MinRate, err = rate(c.FeeMinRate); err != nil {
		return nil, fmt.Errorf("config: fee_min_rate: %w", err)
	}
	if fc.MaxRate, err = rate(c.FeeMaxRate); err != nil {
		return nil, fmt.Errorf("config: fee_max_rate: %w", err)
	}
	if fc.Floor, err = num(c.FeeFloor); err != nil {
		return nil, fmt.Errorf("config: fee_floor %q is not a whole number of satoshis", c.FeeFloor)
	}
	if fc.Dust, err = num(c.FeeDust); err != nil {
		return nil, fmt.Errorf("config: fee_dust %q is not a whole number of satoshis", c.FeeDust)
	}
	if m, err := num(c.FeeMaxTx); err != nil {
		return nil, fmt.Errorf("config: fee_max_tx %q is not a whole number of satoshis", c.FeeMaxTx)
	} else if m != nil {
		fc.MaxTx = *m
	}
	for _, u := range strings.Split(c.FeePolicyURLs, ",") {
		if u = strings.TrimSpace(u); u != "" {
			fc.PolicyURLs = append(fc.PolicyURLs, u)
		}
	}
	if _, err := fc.Fees(mint.DefaultFees); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	switch fc.Source {
	case "", feepolicy.SourceStatic, feepolicy.SourceARC, "arcade":
	default:
		return nil, fmt.Errorf("config: fee_source must be static or arc, got %q", fc.Source)
	}
	return fc, nil
}

// DefaultObjectBound is the object bound assumed when none is configured.
const DefaultObjectBound = 1 << 20

// The modes and the named quorums.
const (
	ModePlane      = "plane"
	ModeUnicast    = "unicast"
	QuorumAll      = "all"
	QuorumMajority = "majority"
	QuorumOne      = "one"
)

// Need is how many of n hosts the quorum q requires. q is one Apply
// accepted; a number above n is an error, because a quorum no host set can
// meet would refuse every publish.
func Need(q string, n int) (int, error) {
	if n < 1 {
		return 0, errors.New("no hosts")
	}
	switch q {
	case QuorumAll, "":
		return n, nil
	case QuorumMajority:
		return n/2 + 1, nil
	case QuorumOne:
		return 1, nil
	}
	k, err := strconv.Atoi(q)
	if err != nil || k < 1 {
		return 0, fmt.Errorf("quorum %q is not all, majority, one or a positive number", q)
	}
	if k > n {
		return 0, fmt.Errorf("quorum %d is more than the %d configured host(s)", k, n)
	}
	return k, nil
}

// DefaultHome is ~/.blogs, or .blogs when there is no home directory.
func DefaultHome() string {
	if h, err := os.UserHomeDir(); err == nil {
		return filepath.Join(h, ".blogs")
	}
	return ".blogs"
}

// Defaults are the built-in values for the keys that have one.
func Defaults() Config {
	return Config{
		Home:        DefaultHome(),
		Mode:        ModePlane,
		Network:     "main",
		ObjectBound: DefaultObjectBound,
		Originator:  "blogs",
		Quorum:      QuorumAll,
		RPCPass:     "bitcoin",
		RPCUser:     "bitcoin",
		Timeout:     15 * time.Second,
	}
}

// Keys is the file grammar's key list, in lexicographic order. A key not in
// it is an error: a misspelt key that was silently ignored would leave a
// setting at its default with no sign that the file was ever read.
var Keys = []string{"arcade_key", "asset", "chain", "facade", "fee_dust", "fee_floor", "fee_max_rate", "fee_max_tx",
	"fee_min_rate", "fee_policy_urls", "fee_rate", "fee_source", "fund_mined_only", "header_url", "home", "hosts", "mode",
	"network", "object_bound", "originator", "quorum", "rpc", "rpc_pass", "rpc_user", "settle", "timeout", "tx_source", "woc_key", "woc_rate"}

// ErrUnknownKey reports a key the grammar does not define.
var ErrUnknownKey = errors.New("config: unknown key")

// Path is the config file location: the -config flag, else
// $BLOGS_HOME/config, else $XDG_CONFIG_HOME/blogs/config, else
// ~/.blogs/config.
func Path(flagPath string, getenv func(string) string) string {
	if flagPath != "" {
		return flagPath
	}
	if h := getenv("BLOGS_HOME"); h != "" {
		return filepath.Join(h, "config")
	}
	if x := getenv("XDG_CONFIG_HOME"); x != "" {
		return filepath.Join(x, "blogs", "config")
	}
	return filepath.Join(DefaultHome(), "config")
}

// Parse reads the key = value grammar: one setting per line, '#' comments,
// blank lines ignored, a key at most once.
func Parse(r io.Reader) (map[string]string, error) {
	out := map[string]string{}
	sc := bufio.NewScanner(r)
	line := 0
	for sc.Scan() {
		line++
		t := strings.TrimSpace(sc.Text())
		if t == "" || strings.HasPrefix(t, "#") {
			continue
		}
		k, v, ok := strings.Cut(t, "=")
		if !ok {
			return nil, fmt.Errorf("config line %d: not key = value", line)
		}
		k, v = strings.TrimSpace(k), strings.TrimSpace(v)
		if !known(k) {
			return nil, fmt.Errorf("%w: line %d: %q", ErrUnknownKey, line, k)
		}
		if _, dup := out[k]; dup {
			return nil, fmt.Errorf("config line %d: %q set twice", line, k)
		}
		out[k] = v
	}
	return out, sc.Err()
}

func known(k string) bool {
	for _, x := range Keys {
		if x == k {
			return true
		}
	}
	return false
}

// Load resolves the file (a missing one is fine) and the environment over
// the defaults. Flags are applied by the caller afterwards with Apply,
// because only the caller knows which flags were set.
func Load(path string, getenv func(string) string) (Config, error) {
	c := Defaults()
	vals := map[string]string{}
	if f, err := os.Open(path); err == nil {
		defer f.Close()
		vals, err = Parse(f)
		if err != nil {
			return c, fmt.Errorf("%s: %w", path, err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return c, err
	}
	for _, k := range Keys {
		if v := getenv("BLOGS_" + strings.ToUpper(k)); v != "" {
			vals[k] = v
		}
	}
	return c.Apply(vals)
}

// Apply sets the given keys the same way for a file, the environment or
// flags, so all three parse alike.
func (c Config) Apply(vals map[string]string) (Config, error) {
	for _, k := range Keys {
		v, ok := vals[k]
		if !ok {
			continue
		}
		switch k {
		case "arcade_key":
			c.ArcadeKey = v
		case "asset":
			c.Asset = v
		case "chain":
			c.Chain = v
		case "facade":
			c.Facade = v
		case "fee_dust":
			c.FeeDust = v
		case "fee_floor":
			c.FeeFloor = v
		case "fee_max_rate":
			c.FeeMaxRate = v
		case "fee_max_tx":
			c.FeeMaxTx = v
		case "fee_min_rate":
			c.FeeMinRate = v
		case "fee_policy_urls":
			c.FeePolicyURLs = v
		case "fee_rate":
			c.FeeRate = v
		case "fee_source":
			c.FeeSource = v
		case "fund_mined_only":
			b, err := strconv.ParseBool(v)
			if err != nil {
				return c, fmt.Errorf("config: fund_mined_only must be true or false, got %q", v)
			}
			c.FundMinedOnly = b
		case "header_url":
			c.HeaderURL = v
		case "home":
			c.Home = v
		case "hosts":
			c.Hosts = nil
			for _, h := range strings.Split(v, ",") {
				if h = strings.TrimSpace(h); h != "" {
					c.Hosts = append(c.Hosts, h)
				}
			}
			if len(c.Hosts) > limits.MaxHosts {
				return c, fmt.Errorf("config: hosts names %d hosts, over the limit of %d: a reader or an auditor asks every host for every seq, and a unicast producer uploads every object to each (docs/limits.md)", len(c.Hosts), limits.MaxHosts)
			}
		case "mode":
			if v != ModePlane && v != ModeUnicast {
				return c, fmt.Errorf("config: mode must be plane or unicast, got %q", v)
			}
			c.Mode = v
		case "network":
			if v != "main" && v != "test" && v != "regtest" {
				return c, fmt.Errorf("config: network must be main, test or regtest, got %q", v)
			}
			c.Network = v
		case "object_bound":
			n, err := strconv.Atoi(v)
			if err != nil || n < 1 {
				return c, fmt.Errorf("config: object_bound %q is not a positive number of bytes", v)
			}
			if n > limits.MaxObjectBound {
				return c, fmt.Errorf("config: object_bound %d is over the limit of %d: no plane admits a larger object (BRC-149; docs/limits.md)", n, limits.MaxObjectBound)
			}
			c.ObjectBound = n
		case "originator":
			c.Originator = v
		case "quorum":
			if _, err := Need(v, 1<<30); err != nil {
				return c, fmt.Errorf("config: %w", err)
			}
			c.Quorum = v
		case "rpc":
			c.RPC = v
		case "rpc_pass":
			c.RPCPass = v
		case "rpc_user":
			c.RPCUser = v
		case "settle":
			c.Settle = v
		case "tx_source":
			if v != "woc:main" && v != "woc:test" {
				return c, fmt.Errorf("config: tx_source must be woc:main or woc:test, got %q", v)
			}
			c.TxSource = v
		case "timeout":
			d, err := time.ParseDuration(v)
			if err != nil || d <= 0 {
				return c, fmt.Errorf("config: timeout %q is not a positive duration", v)
			}
			if d < limits.MinTimeout || d > limits.MaxTimeout {
				return c, fmt.Errorf("config: timeout %s is outside the limits of %s to %s: every request is bounded by it, and a publish tries each host four times (docs/limits.md)", d, limits.MinTimeout, limits.MaxTimeout)
			}
			c.Timeout = d
		case "woc_key":
			c.WoCKey = v
		case "woc_rate":
			r, err := strconv.ParseFloat(v, 64)
			if err != nil || r <= 0 || r > 1000 {
				return c, fmt.Errorf("config: woc_rate %q is not a number of requests a second, above 0 and at most 1000", v)
			}
			c.WoCRate = r
		}
	}
	if _, err := c.FeeConfig(); err != nil {
		return c, err
	}
	for k := range vals {
		if !known(k) {
			return c, fmt.Errorf("%w: %q", ErrUnknownKey, k)
		}
	}
	return c, nil
}
