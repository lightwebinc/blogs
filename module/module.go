package module

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/bsv-blockchain/go-sdk/transaction/chaintracker"

	bmodule "github.com/lightwebinc/bcommon/module"

	"github.com/lightwebinc/blogs/internal/logrec"
)

// Name is the module's name, as a host's module list names it.
const Name = "blogs"

// TickEvery is how often the background tick looks for a retention pass to
// run: the first once the index is restored, then every RetentionEvery.
const TickEvery = 30 * time.Second

// Build is the version the host logs; a binary sets it (the TypeScript
// bundle's BLOGS_BUILD banner).
var Build = "unversioned"

// Mounted is the module for one configuration: the contract's Module, the
// lookup service, and what Start runs.
type Mounted struct {
	Module bmodule.Module
	LS     *Service
	config *Config
}

// Mount builds the module for c. overlayTopics is OVERLAY_TOPICS, when
// known; store replaces the retention state file, headers the topic
// managers' engine-verified tracker and now the host's clock in Unix
// milliseconds, for tests.
func Mount(host bmodule.Host, c *Config, overlayTopics []string, store StateStore, headers chaintracker.ChainTracker, now func() int64) (*Mounted, error) {
	if len(c.Streams) == 0 {
		return nil, errors.New("blogs: no streams configured")
	}
	names := map[string]bool{}
	var topics []string
	for _, s := range c.Streams {
		t, err := logrec.Topic(s.Stream)
		if err != nil {
			return nil, err
		}
		names[t] = true
		topics = append(topics, t)
	}
	for _, t := range overlayTopics {
		if strings.HasPrefix(t, logrec.TopicPrefix) && !names[t] {
			return nil, fmt.Errorf("OVERLAY_TOPICS names %s but BLOGS_STREAMS does not configure it; it would be admitted by the default manager", t)
		}
	}
	m := host.Metrics()
	for _, kind := range AdmitKinds {
		m.Preset("blogs_admitted_total", map[string]string{"kind": kind})
	}
	for _, reason := range Reasons {
		m.Preset("blogs_refused_total", map[string]string{"reason": reason, "recorded": "yes"})
		m.Preset("blogs_refused_total", map[string]string{"reason": reason, "recorded": "no"})
	}
	for _, name := range []string{"blogs_retractions_total", "blogs_candidates_dropped_total", "blogs_tokens_dropped_total", "blogs_retired_total"} {
		m.Preset(name, nil)
	}
	var retention *Retention
	if c.Retention != nil {
		if store == nil {
			store = FileStore{Dir: c.Retention.StateDir}
		}
		if now == nil {
			now = func() int64 { return time.Now().UnixMilli() }
		}
		retention = &Retention{KeepMs: c.Retention.KeepMs, Store: store, Now: now}
	}
	ls, err := NewService(topics, host, retention)
	if err != nil {
		return nil, err
	}
	m.Gauge("blogs_streams", func() float64 { return float64(ls.Counts().Streams) })
	m.Gauge("blogs_batches", func() float64 { return float64(ls.Counts().Batches) })
	m.Gauge("blogs_anchor_tokens", func() float64 { return float64(ls.Counts().Tokens) })
	m.Gauge("blogs_funding_trees", func() float64 { return float64(ls.Counts().Trees) })
	m.Gauge("blogs_retracted_batches", func() float64 { return float64(ls.Counts().Retracted) })
	m.Gauge("blogs_missing_positions", func() float64 { return float64(ls.Counts().Missing) })
	m.Gauge("blogs_fork_positions", func() float64 { return float64(ls.Counts().ForkSeqs) })
	m.Gauge("blogs_retired_positions", func() float64 { return float64(ls.Counts().Retired) })
	managers := map[string]bmodule.TopicManager{}
	for _, s := range c.Streams {
		t, _ := logrec.Topic(s.Stream)
		tm, err := NewTopicManager(t, s.Policy, ls.Coin, host, headers)
		if err != nil {
			return nil, err
		}
		managers[t] = tm
	}
	sort.Strings(topics)
	ret := "off"
	if c.Retention != nil {
		ret = strconv.FormatFloat(c.Retention.KeepMs/86400000, 'f', -1, 64) + " days"
	}
	host.Log("blogs module mounted", map[string]any{"topics": strings.Join(topics, ","), "lookup": logrec.LookupService, "build": Build, "retention": ret})
	return &Mounted{Module: bmodule.Module{Topics: managers, Lookups: map[string]bmodule.LookupService{logrec.LookupService: ls}}, LS: ls, config: c}, nil
}

// Start runs the background tick until ctx ends: with retention, a pass
// once the index is restored and every RetentionEvery after. It returns
// when ctx ends.
func (m *Mounted) Start(ctx context.Context) error {
	if m.LS.retention == nil {
		<-ctx.Done()
		return nil
	}
	var last time.Time
	t := time.NewTicker(TickEvery)
	defer t.Stop()
	for {
		if m.LS.Restored() && (last.IsZero() || time.Since(last) >= RetentionEvery) {
			last = time.Now()
			m.LS.Retain(ctx)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}

// New is the factory a host calls: the configuration from getenv, checked
// against OVERLAY_TOPICS.
func New(host bmodule.Host, getenv func(string) string) (*Mounted, error) {
	if getenv == nil {
		return nil, errors.New("blogs: no environment")
	}
	var topics []string
	for _, t := range strings.Split(getenv("OVERLAY_TOPICS"), ",") {
		if t = strings.TrimSpace(t); t != "" {
			topics = append(topics, t)
		}
	}
	c, err := ParseConfig(getenv)
	if err != nil {
		return nil, err
	}
	return Mount(host, c, topics, nil, nil, nil)
}
