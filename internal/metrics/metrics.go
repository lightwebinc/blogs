// Package metrics is a small set of counters and gauges written in the
// Prometheus text exposition format: what the sidecar and the read gateway
// count, including the metering counters billing reads (docs/backend.md).
package metrics

import (
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
)

// Set holds metrics by name and label set.
type Set struct {
	mu    sync.Mutex
	kinds map[string][2]string // name: type, help
	vals  map[string]map[string]float64
}

// New returns an empty set.
func New() *Set {
	return &Set{kinds: map[string][2]string{}, vals: map[string]map[string]float64{}}
}

// Counter declares a counter.
func (s *Set) Counter(name, help string) { s.declare(name, "counter", help) }

// Gauge declares a gauge.
func (s *Set) Gauge(name, help string) { s.declare(name, "gauge", help) }

func (s *Set) declare(name, kind, help string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.kinds[name] = [2]string{kind, help}
	if s.vals[name] == nil {
		s.vals[name] = map[string]float64{}
	}
}

// labels renders label pairs k1, v1, k2, v2 ... in the order given.
func labels(kv []string) string {
	if len(kv) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteByte('{')
	for i := 0; i+1 < len(kv); i += 2 {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "%s=%q", kv[i], kv[i+1])
	}
	b.WriteByte('}')
	return b.String()
}

// Add adds v to a declared metric's series named by label pairs.
func (s *Set) Add(name string, v float64, kv ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.vals[name]
	if !ok {
		panic("metrics: undeclared " + name)
	}
	m[labels(kv)] += v
}

// Set sets a gauge's series.
func (s *Set) Set(name string, v float64, kv ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.vals[name]
	if !ok {
		panic("metrics: undeclared " + name)
	}
	m[labels(kv)] = v
}

// Get reads a series: tests and the gateway's own accounting.
func (s *Set) Get(name string, kv ...string) float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.vals[name][labels(kv)]
}

// WriteTo writes every metric in the text format, in name order.
func (s *Set) WriteTo(w io.Writer) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	names := make([]string, 0, len(s.kinds))
	for n := range s.kinds {
		names = append(names, n)
	}
	sort.Strings(names)
	var b strings.Builder
	for _, n := range names {
		k := s.kinds[n]
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s %s\n", n, k[1], n, k[0])
		series := make([]string, 0, len(s.vals[n]))
		for l := range s.vals[n] {
			series = append(series, l)
		}
		sort.Strings(series)
		for _, l := range series {
			fmt.Fprintf(&b, "%s%s %v\n", n, l, s.vals[n][l])
		}
	}
	n, err := io.WriteString(w, b.String())
	return int64(n), err
}

// ServeHTTP serves the set at /metrics.
func (s *Set) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = s.WriteTo(w)
}
