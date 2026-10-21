package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"time"

	"github.com/lightwebinc/blogs/internal/gateway"
	"github.com/lightwebinc/blogs/internal/metrics"
	"github.com/lightwebinc/blogs/internal/reader"
)

const gatewayHelp = `usage: blogs gateway -archive DIR -loki URL [-cross-archive DIR] [-listen ADDR] [flags]

The read gateway: the Loki HTTP API in front of a stock Loki, for Grafana
and any other Loki client. Every request is passed to Loki; the lines of a
query or query_range answer are each marked, against the archive (blogs
archive -dir DIR), with blogs_state "verified" or "unverified" and, when
unverified, blogs_reason:

  pending       no interval the archive has proven covers the line yet
  altered       the line is not the one its batch holds at its position
  relabeled     the line is, but its time or a label in the store differ
  retracted     the batch was retracted; the archive holds its receipt
  hole          the root commits the position and the archive never held it
  no-provenance the line carries no blogs placement
  unreadable    the archive's files for the batch did not read or verify,
                or the second archive's interval file did not verify or
                disagrees

With -cross-archive DIR, a second archive (another host's, followed by its
own blogs archive) must agree before a line is verified: its interval file
covering the line verifies and has the same interval, root and token, and
the same commitment and inclusion path at the line's seq. A line whose
interval the second archive has not proven yet is pending.

A client asking for categorized labels (Grafana does) gets the marks as
structured metadata; any other gets them as stream labels.

GET /blogs/v1/coverage answers, per stream in the archive, the intervals
proven with their roots, the holes and the retraction receipts, and with
-cross-archive how the second archive's intervals compare. GET
/metrics serves the gateway's counters, among them the retained bytes per
stream integrated over time (blogs_retained_byte_seconds_total), for
GB-month metering.`

func cmdGateway(ctx context.Context, g *global, args []string) error {
	fs := g.flagSet("gateway", gatewayHelp)
	dir := fs.String("archive", "", "the archive directory")
	cross := fs.String("cross-archive", "", "a second archive directory whose interval files must agree before a line is verified")
	loki := fs.String("loki", "", "Loki's base URL")
	listen := fs.String("listen", "127.0.0.1:3101", "address to serve on")
	meter := fs.Duration("meter", time.Minute, "how often retained bytes are measured")
	var dirs, allow repeated
	fs.Var(&dirs, "content-dir", "a directory of digest-only content named by SHA-256 hex (repeatable)")
	fs.Var(&allow, "allow-host", "a host whose locator hints may be fetched over HTTPS (repeatable)")
	pos, err := parse(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 0 || *dir == "" || *loki == "" {
		return usage("gateway -archive DIR -loki URL")
	}
	u, err := url.Parse(*loki)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return usage("-loki %q is not an http or https URL", *loki)
	}
	if *meter <= 0 {
		return usage("-meter must be positive")
	}
	abs, err := filepath.Abs(*dir)
	if err != nil {
		return err
	}
	crossAbs := ""
	if *cross != "" {
		if crossAbs, err = filepath.Abs(*cross); err != nil {
			return err
		}
		if crossAbs == abs {
			return usage("-cross-archive must be another archive than -archive")
		}
	}
	m := metrics.New()
	gateway.DeclareMetrics(m)
	gw := &gateway.Gateway{Loki: u, Metrics: m, Note: g.say, Verifier: &gateway.Verifier{Dir: abs, Cross: crossAbs, Rescan: 2 * time.Second,
		Fetch: reader.Fetch{Dirs: dirs, Allow: allow, Timeout: g.cfg.Timeout, HTTP: httpClient}}}
	l, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	srv := &http.Server{Handler: gw.Handler(), ReadHeaderTimeout: 10 * time.Second}
	go func() { <-ctx.Done(); _ = srv.Close() }()
	go gw.Meter(ctx, *meter)
	if crossAbs != "" {
		g.say("gateway: Loki API on %s, in front of %s, archive %s, cross-checked against %s", l.Addr(), u, abs, crossAbs)
	} else {
		g.say("gateway: Loki API on %s, in front of %s, archive %s", l.Addr(), u, abs)
	}
	if err := srv.Serve(l); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
