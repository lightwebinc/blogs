// Package module is the blogs overlay module in Go: `tm_log_<name>_<suffix>`
// for each configured stream and `ls_log` for all of them, written to
// bcommon's engine-neutral contract (github.com/lightwebinc/bcommon/module)
// so that any Go overlay host that carries the adapter mounts it. It is the
// twin of the TypeScript module under host/src (frozen), built from the Go
// core: the admission rules are logrec.Admit, the questions logrec.ParseQuery,
// the interval roots logrec.IntervalRoot.
//
// Configuration is the TypeScript module's, read from the environment when
// the host calls the factory (docs/host.md): BLOGS_STREAMS, and for
// retention BLOGS_KEEP_DAYS with BLOGS_STATE_DIR. Every mistake is an error,
// and the host refuses to start.
package module

import (
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/lightwebinc/blogs/internal/logrec"
)

// MinKeepDays is the least BLOGS_KEEP_DAYS a host may set.
const MinKeepDays = 1

// StreamConfig is one stream and its producer policy.
type StreamConfig struct {
	Stream string
	Policy logrec.Policy
}

// Config is the module's configuration.
type Config struct {
	Streams []StreamConfig
	// Retention is nil when BLOGS_KEEP_DAYS is unset.
	Retention *RetentionConfig
}

// RetentionConfig is BLOGS_KEEP_DAYS and BLOGS_STATE_DIR.
type RetentionConfig struct {
	// KeepMs is how long a proven carrier is kept, in milliseconds.
	KeepMs   float64
	StateDir string
}

var keyHex = regexp.MustCompile(`^[0-9a-f]{66}$`)

// ParseStreams parses BLOGS_STREAMS: a comma list of `<stream>=open` or
// `<stream>=<key>[+<key>...]`, each key 66 lowercase hex characters, a
// compressed secp256k1 key. Openness is stated, never inferred.
func ParseStreams(raw string) ([]StreamConfig, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, errors.New("BLOGS_STREAMS is required: name each stream and its producer policy")
	}
	var out []StreamConfig
	for _, entry := range strings.Split(raw, ",") {
		e := strings.TrimSpace(entry)
		eq := strings.IndexByte(e, '=')
		if eq < 0 {
			return nil, fmt.Errorf("BLOGS_STREAMS: %q is not <stream>=open or <stream>=<key>+<key>", e)
		}
		stream, value := e[:eq], e[eq+1:]
		if logrec.CheckStream(stream) != nil {
			return nil, fmt.Errorf("BLOGS_STREAMS: %q is not a stream identifier <name>_<suffix>", stream)
		}
		for _, s := range out {
			if s.Stream == stream {
				return nil, fmt.Errorf("BLOGS_STREAMS names %s twice", stream)
			}
		}
		var p logrec.Policy
		if value == "open" {
			p.Open = true
		} else {
			for _, k := range strings.Split(value, "+") {
				if !keyHex.MatchString(k) {
					return nil, fmt.Errorf("BLOGS_STREAMS: %s: %q is not 66 lowercase hex characters", stream, k)
				}
				b, _ := hex.DecodeString(k)
				if logrec.CheckProducer(b) != nil {
					return nil, fmt.Errorf("BLOGS_STREAMS: %s: %s is not a compressed secp256k1 key", stream, k)
				}
				p.Producers = append(p.Producers, b)
			}
		}
		if err := p.Validate(); err != nil {
			return nil, err
		}
		out = append(out, StreamConfig{Stream: stream, Policy: p})
	}
	return out, nil
}

var daysRe = regexp.MustCompile(`^[0-9]+(\.[0-9]+)?$`)

// ParseRetention reads BLOGS_KEEP_DAYS and BLOGS_STATE_DIR; nil when
// retention is off.
func ParseRetention(getenv func(string) string) (*RetentionConfig, error) {
	raw := strings.TrimSpace(getenv("BLOGS_KEEP_DAYS"))
	dir := getenv("BLOGS_STATE_DIR")
	if raw == "" {
		if dir != "" {
			return nil, errors.New("BLOGS_STATE_DIR is set but BLOGS_KEEP_DAYS is not: set both to keep carriers for a bounded time, or neither to keep everything")
		}
		return nil, nil
	}
	if !daysRe.MatchString(raw) {
		return nil, fmt.Errorf("BLOGS_KEEP_DAYS: %q is not a number of days", raw)
	}
	days, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return nil, fmt.Errorf("BLOGS_KEEP_DAYS: %q is not a number of days", raw)
	}
	if days < MinKeepDays {
		return nil, fmt.Errorf("BLOGS_KEEP_DAYS: %s is below the floor of %d day", raw, MinKeepDays)
	}
	if dir == "" || !filepath.IsAbs(dir) {
		return nil, errors.New("BLOGS_KEEP_DAYS needs BLOGS_STATE_DIR, an absolute directory for the retention state")
	}
	return &RetentionConfig{KeepMs: days * 24 * 60 * 60 * 1000, StateDir: dir}, nil
}

// ParseConfig reads the module's configuration.
func ParseConfig(getenv func(string) string) (*Config, error) {
	streams, err := ParseStreams(getenv("BLOGS_STREAMS"))
	if err != nil {
		return nil, err
	}
	r, err := ParseRetention(getenv)
	if err != nil {
		return nil, err
	}
	return &Config{Streams: streams, Retention: r}, nil
}
