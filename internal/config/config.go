// Package config resolves the blogs command's settings: flags, then BLOGS_*
// in the environment, then the config file, then the built-in default.
//
// The deployment addresses have no default: the overlay hosts, the facade,
// the header source, the settlement leg and the node. A default there would
// quietly send a producer's objects or a reader's questions to somebody
// else's server, so a command that needs one and finds none is a usage
// error.
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
)

// Config is the resolved settings.
type Config struct {
	// ArcadeKey is the bearer token for an arcade installation in Settle.
	ArcadeKey string
	// Asset is the node's asset API base URL: proofs, the chain tip, blocks
	// and raw transactions.
	Asset string
	// Facade is the overlay host a producer publishes to.
	Facade string
	// HeaderURL is the header source every proof a reader checks is checked
	// against: a bridge's /v1 base URL, woc:main, woc:test or
	// chaintracks:URL. No default, ever.
	HeaderURL string
	// Home is the producer's directory: identity, coin pool and streams.
	Home string
	// Hosts are the overlay hosts a reader asks, in order.
	Hosts []string
	// Network is "main", "test" or "regtest". It picks the fund address's
	// prefix and the proof-of-work floor a header source is held to.
	Network string
	// ObjectBound is the largest object the plane takes, in bytes: a
	// carrier's BEEF over it is not published.
	ObjectBound int
	// Originator is the BRC-100 originator presented to the wallet.
	Originator string
	// RPC is the node's JSON-RPC URL; RPCUser and RPCPass its basic auth.
	RPC     string
	RPCPass string
	RPCUser string
	// Settle is the settlement leg: tcp:<host:port>, rpc:<url> or
	// arcade:<url>.
	Settle string
	// Timeout bounds each request to a host, node or header source.
	Timeout time.Duration
}

// DefaultObjectBound is the object bound assumed when none is configured.
const DefaultObjectBound = 1 << 20

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
		Network:     "main",
		ObjectBound: DefaultObjectBound,
		Originator:  "blogs",
		RPCPass:     "bitcoin",
		RPCUser:     "bitcoin",
		Timeout:     15 * time.Second,
	}
}

// Keys is the file grammar's key list, in lexicographic order. A key not in
// it is an error: a misspelt key that was silently ignored would leave a
// setting at its default with no sign that the file was ever read.
var Keys = []string{"arcade_key", "asset", "facade", "header_url", "home", "hosts", "network", "object_bound", "originator", "rpc", "rpc_pass", "rpc_user", "settle", "timeout"}

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
		case "facade":
			c.Facade = v
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
			c.ObjectBound = n
		case "originator":
			c.Originator = v
		case "rpc":
			c.RPC = v
		case "rpc_pass":
			c.RPCPass = v
		case "rpc_user":
			c.RPCUser = v
		case "settle":
			c.Settle = v
		case "timeout":
			d, err := time.ParseDuration(v)
			if err != nil || d <= 0 {
				return c, fmt.Errorf("config: timeout %q is not a positive duration", v)
			}
			c.Timeout = d
		}
	}
	for k := range vals {
		if !known(k) {
			return c, fmt.Errorf("%w: %q", ErrUnknownKey, k)
		}
	}
	return c, nil
}
