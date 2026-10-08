package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/lightwebinc/blogs/internal/config"
	"github.com/lightwebinc/blogs/internal/ingest"
	"github.com/lightwebinc/blogs/internal/limits"
	"github.com/lightwebinc/blogs/internal/logrec"
	"github.com/lightwebinc/blogs/internal/metrics"
	"github.com/lightwebinc/blogs/internal/produce"
)

const ingestHelp = `usage: blogs ingest -stream S [-confidential-stream S2] [doors] [-loki URL] [flags]

The logging sidecar. Log shippers send to its doors as they send to any
log backend:

  -syslog ADDR       syslog over TCP (RFC 6587 framing, octet counted or
                     newline), RFC 5424 messages
  -syslog-tls ADDR   the same over TLS (RFC 5425), with -tls-cert and
                     -tls-key, and mutual TLS with -tls-client-ca
  -http ADDR         OTLP logs over HTTP (POST /v1/logs, protobuf or JSON)
                     and Loki push (POST /loki/api/v1/push, protobuf with
                     snappy, or JSON); also GET /metrics and GET /healthz

Each record becomes one line in the line form (docs/backend.md): a canonical
JSON object with its time, severity, labels, attributes and body. Lines are
cut into batches by size, count or time and each batch is published as a
signed carrier on the stream, exactly as produce publishes; the stream is
anchored every -interval. An HTTP request is answered once its records are
published (at least once: a sender that retries after an error may publish
a record twice). Syslog has no acknowledgment; its records are queued.

Records carrying the -confidential-label go to -confidential-stream, which
is digest-only: the stream carries each batch's digest and length, and the
lines are written to -content-dir and nowhere on the plane.

With -loki, every published line is pushed to that Loki unchanged, with its
place as structured metadata: blogs_stream, blogs_producer, blogs_seq and
blogs_line (its 1-based position in the batch). blogs gateway reads them
back and checks each line against the archive's roots.

One process drives both streams from one home and holds the home's lock.`

// IngestTreeCount is ingest's default -tree-count.
const IngestTreeCount = 16

func cmdIngest(ctx context.Context, g *global, args []string) error {
	fs := g.flagSet("ingest", ingestHelp)
	var f produceFlags
	stream := fs.String("stream", "", "the stream records go to")
	conf := fs.String("confidential-stream", "", "the digest-only stream confidential records go to")
	confLabel := fs.String("confidential-label", "confidential=true", "the label NAME=VALUE that makes a record confidential")
	fs.StringVar(&f.contentDir, "content-dir", "", "where the confidential stream's content is written, named by SHA-256 hex")
	fs.Var(&f.urls, "url", "an https URL prefix the confidential content's SHA-256 hex is appended to, as a fetch hint (up to 4)")
	fs.StringVar(&f.encoding, "encoding", "lines", "lines or gzip")
	httpAddr := fs.String("http", "", "address for OTLP logs, Loki push, metrics and health")
	syslogAddr := fs.String("syslog", "", "address for syslog over TCP")
	syslogTLS := fs.String("syslog-tls", "", "address for syslog over TLS (RFC 5425)")
	cert := fs.String("tls-cert", "", "the TLS certificate (PEM)")
	key := fs.String("tls-key", "", "the TLS key (PEM)")
	clientCA := fs.String("tls-client-ca", "", "require client certificates signed by this CA (PEM)")
	loki := fs.String("loki", "", "the Loki every published line is pushed to")
	tenant := fs.String("loki-tenant", "", "X-Scope-OrgID for the pushes")
	fs.IntVar(&f.maxBytes, "max-bytes", 0, "cut a batch at this many bytes of lines (default by mode and profile: docs/limits.md)")
	fs.IntVar(&f.maxLines, "max-lines", limits.DefaultMaxLines, fmt.Sprintf("cut a batch at this many lines (at most %d)", limits.MaxLines))
	fs.DurationVar(&f.maxWait, "max-wait", limits.DefaultMaxWait, fmt.Sprintf("cut a batch this long after its first line (%s to %s)", limits.MinMaxWait, limits.MaxMaxWait))
	fs.Float64Var(&f.maxRate, "max-rate", limits.DefaultRate, fmt.Sprintf("publish at most this many batches a second per stream (at most %d)", limits.MaxRate))
	fs.DurationVar(&f.interval, "interval", limits.DefaultInterval, fmt.Sprintf("anchor once this long has passed since the last anchor (%s to %s)", limits.MinInterval, limits.MaxInterval))
	admin := fs.String("admin", "", "address for the admin API (POST /admin/retract); keep it on loopback or behind your own access control")
	engineFlags(fs, &f)
	// A logging stream publishes many small batches, and every carrier's
	// BEEF carries its whole funding tree: a smaller tree keeps the
	// evidence an archive stores near the size of the lines.
	for name, v := range map[string]string{"tree-count": strconv.Itoa(IngestTreeCount), "ahead": strconv.Itoa(IngestTreeCount / 2)} {
		fl := fs.Lookup(name)
		_ = fl.Value.Set(v)
		fl.DefValue = v
	}
	pos, err := parse(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 0 || *stream == "" {
		return usage("ingest -stream S [flags]")
	}
	if *httpAddr == "" && *syslogAddr == "" && *syslogTLS == "" {
		return usage("give at least one door: -http, -syslog or -syslog-tls")
	}
	if *syslogTLS != "" && (*cert == "" || *key == "") {
		return usage("-syslog-tls needs -tls-cert and -tls-key")
	}
	labelName, labelValue, ok := strings.Cut(*confLabel, "=")
	if *conf != "" && (!ok || labelName == "") {
		return usage("-confidential-label is NAME=VALUE")
	}
	var encoding uint64
	switch f.encoding {
	case "lines":
		encoding = logrec.EncodingLines
	case "gzip":
		encoding = logrec.EncodingLinesGzip
	default:
		return usage("-encoding is lines or gzip")
	}
	if *conf != "" {
		if f.contentDir == "" {
			return usage("-confidential-stream needs -content-dir, where its content is written")
		}
		if len(f.urls) > logrec.MaxURLs {
			return usage("at most %d -url hints", logrec.MaxURLs)
		}
		for _, u := range f.urls {
			if err := produce.ValidURLPrefix(u); err != nil {
				return usage("%v", err)
			}
		}
		if err := os.MkdirAll(f.contentDir, 0o755); err != nil {
			return err
		}
	}
	var tlsCfg *tls.Config
	if *syslogTLS != "" {
		var ca []byte
		if *clientCA != "" {
			if ca, err = os.ReadFile(*clientCA); err != nil {
				return err
			}
		}
		if tlsCfg, err = ingest.TLSConfig(*cert, *key, ca); err != nil {
			return usage("TLS: %v", err)
		}
	}
	streams := []string{*stream}
	if *conf != "" {
		streams = append(streams, *conf)
	}
	for _, s := range streams {
		if err := checkEngineFlags(s, &f); err != nil {
			return err
		}
	}
	legs, err := g.legs()
	if err != nil {
		return err
	}
	plane := g.cfg.Mode != config.ModeUnicast
	opts := make([]produce.Options, len(streams))
	cuts := make([]produce.Cut, len(streams))
	for i := range streams {
		o := produce.Options{Interval: f.interval, Encoding: encoding, Profile: logrec.ProfileFull}
		if i == 1 {
			o.Profile, o.ContentDir, o.URLs = logrec.ProfileDigest, f.contentDir, f.urls
		}
		lf := f
		if lf.maxBytes == 0 {
			lf.maxBytes = limits.DefaultMaxBytes(plane, o.Profile == logrec.ProfileDigest, o.Encoding == logrec.EncodingLinesGzip)
		}
		if err := g.checkLimits(&lf, o, legs); err != nil {
			return err
		}
		opts[i] = o
		cuts[i] = produce.Cut{MaxBytes: lf.maxBytes, MaxLines: lf.maxLines, MaxWait: lf.maxWait, MinGap: limits.Gap(lf.maxRate)}
	}
	w, err := g.openWallet()
	if err != nil {
		return err
	}
	unlock, err := g.lockHome()
	if err != nil {
		return err
	}
	defer unlock()
	m := metrics.New()
	ingest.Declare(m)
	ingest.DeclareDoors(m)
	ingest.DeclareForward(m)
	b := &ingest.Batcher{Poll: f.poll, Metrics: m, Note: g.say}
	var engines []*produce.Engine
	defer func() {
		for _, e := range engines {
			closeEngine(e)
		}
	}()
	for i, s := range streams {
		e, err := g.openEngine(ctx, s, &f, opts[i], w, legs)
		if err != nil {
			return err
		}
		engines = append(engines, e)
		b.Lanes = append(b.Lanes, &ingest.Lane{Engine: ingest.FromEngine(e), Cut: cuts[i]})
		g.say("stream %s: topic %s, producer %s, from seq %d", s, e.Topic(), e.St.Identity, e.St.Seq+1)
	}
	if *conf != "" {
		b.Route = func(r *ingest.Record) int {
			if r.Labels[labelName] == labelValue {
				return 1
			}
			return 0
		}
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if *loki != "" {
		fw := &ingest.Forwarder{URL: *loki, Tenant: *tenant, Metrics: m, Note: g.say, HTTP: &http.Client{Timeout: 30 * time.Second}}
		fw.Start(runCtx, 4096)
		b.Forward = fw.Forward
	}
	b.Start()
	var ready atomic.Bool
	doors := &ingest.Doors{Batcher: b, Metrics: m, Note: g.say, Ready: ready.Load}
	errs := make(chan error, 4)
	serve := func(addr string, h http.Handler, what string) error {
		l, err := net.Listen("tcp", addr)
		if err != nil {
			return err
		}
		srv := &http.Server{Handler: h, ReadHeaderTimeout: 10 * time.Second}
		go func() { <-runCtx.Done(); _ = srv.Close() }()
		go func() {
			if err := srv.Serve(l); err != nil && !errors.Is(err, http.ErrServerClosed) {
				errs <- err
			}
		}()
		g.say("%s on %s", what, l.Addr())
		return nil
	}
	// The HTTP doors open first and answer 503 until every stream has a
	// funding tree to spend, so a sender never waits for a block.
	if *httpAddr != "" {
		if err := serve(*httpAddr, doors.Handler(), "http: OTLP logs and Loki push"); err != nil {
			return err
		}
	}
	if err := b.Prime(runCtx); err != nil {
		return err
	}
	done := make(chan error, 1)
	go func() { done <- b.Run(runCtx, 30*time.Second) }()
	if *admin != "" {
		if err := serve(*admin, adminHandler(g, b, engines), "admin"); err != nil {
			cancel()
			<-done
			return err
		}
	}
	for _, d := range []struct {
		addr string
		tls  *tls.Config
		door string
	}{{*syslogAddr, nil, "syslog"}, {*syslogTLS, tlsCfg, "syslog-tls"}} {
		if d.addr == "" {
			continue
		}
		l, err := net.Listen("tcp", d.addr)
		if err != nil {
			cancel()
			<-done
			return err
		}
		if d.tls != nil {
			l = tls.NewListener(l, d.tls)
		}
		g.say("%s: on %s", d.door, l.Addr())
		go func() {
			if err := doors.ServeSyslog(runCtx, l, d.door); err != nil {
				errs <- err
			}
		}()
	}
	ready.Store(true)
	g.say("ready")
	select {
	case err := <-errs:
		cancel()
		<-done
		return err
	case err := <-done:
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	}
}
