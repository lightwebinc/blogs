package ingest

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/lightwebinc/blogs/internal/metrics"
)

// MaxBody bounds one HTTP request's body, before and after decompression.
const MaxBody = 16 << 20

// Doors serves the ingest doors in front of a batcher: OTLP logs and Loki
// push over HTTP, and syslog over TCP and TLS.
type Doors struct {
	Batcher *Batcher
	Metrics *metrics.Set
	Note    func(format string, args ...any)
	// Ready reports whether the sidecar is serving (the health endpoint).
	Ready func() bool
}

// DeclareDoors declares the doors' metrics.
func DeclareDoors(m *metrics.Set) {
	m.Counter("blogs_ingest_requests_total", "Requests and syslog connections, by door and outcome.")
	m.Counter("blogs_ingest_received_bytes_total", "Bytes received, by door, before decompression: the wire volume.")
	m.Counter("blogs_ingest_records_total", "Records accepted, by door.")
}

func (d *Doors) note(format string, args ...any) {
	if d.Note != nil {
		d.Note(format, args...)
	}
}

func (d *Doors) count(door, outcome string) {
	if d.Metrics != nil {
		d.Metrics.Add("blogs_ingest_requests_total", 1, "door", door, "outcome", outcome)
	}
}

// Handler serves the HTTP doors, the metrics and the health check.
func (d *Doors) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/logs", d.otlp)
	mux.HandleFunc("POST /loki/api/v1/push", d.loki)
	if d.Metrics != nil {
		mux.Handle("GET /metrics", d.Metrics)
	}
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		if d.Ready != nil && !d.Ready() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		_, _ = io.WriteString(w, "ok\n")
	})
	return mux
}

// body reads a request body, gunzipping it when it says so, within MaxBody.
func (d *Doors) body(door string, r *http.Request) ([]byte, error) {
	raw, err := io.ReadAll(io.LimitReader(r.Body, MaxBody+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > MaxBody {
		return nil, fmt.Errorf("a body over %d bytes", MaxBody)
	}
	if d.Metrics != nil {
		d.Metrics.Add("blogs_ingest_received_bytes_total", float64(len(raw)), "door", door)
	}
	if r.Header.Get("Content-Encoding") == "gzip" {
		zr, err := gzip.NewReader(bytes.NewReader(raw))
		if err != nil {
			return nil, err
		}
		out, err := io.ReadAll(io.LimitReader(zr, MaxBody+1))
		if err != nil {
			return nil, err
		}
		if len(out) > MaxBody {
			return nil, fmt.Errorf("a body expanding over %d bytes", MaxBody)
		}
		return out, nil
	}
	return raw, nil
}

func mediaType(r *http.Request) string {
	t, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
	return t
}

// submit hands records to the batcher and answers: 200 or 204 once they
// are published, 503 with Retry-After when the sidecar cannot take them
// now, 400 for records that do not encode.
func (d *Doors) submit(w http.ResponseWriter, r *http.Request, door string, recs []Record, ok func()) {
	err := d.Batcher.Submit(r.Context(), recs, true)
	switch {
	case err == nil:
		if d.Metrics != nil {
			d.Metrics.Add("blogs_ingest_records_total", float64(len(recs)), "door", door)
		}
		d.count(door, "ok")
		ok()
	case errors.Is(err, ErrBusy):
		d.count(door, "busy")
		w.Header().Set("Retry-After", "1")
		http.Error(w, "busy", http.StatusServiceUnavailable)
	case errors.Is(err, context.Canceled):
		d.count(door, "cancelled")
	default:
		var lineErr bool
		for i := range recs {
			if _, e := recs[i].Line(); e != nil {
				lineErr = true
				break
			}
		}
		if lineErr {
			d.count(door, "refused")
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		d.count(door, "unpublished")
		w.Header().Set("Retry-After", "5")
		http.Error(w, "not published: "+err.Error(), http.StatusServiceUnavailable)
	}
}

// notReady answers a request that came before the sidecar is ready (its
// streams' first funding trees are being minted) with 503 and
// Retry-After, which every shipper retries.
func (d *Doors) notReady(w http.ResponseWriter, door string) bool {
	if d.Ready == nil || d.Ready() {
		return false
	}
	d.count(door, "not-ready")
	w.Header().Set("Retry-After", "5")
	http.Error(w, "not ready: minting the streams' first funding trees", http.StatusServiceUnavailable)
	return true
}

func (d *Doors) otlp(w http.ResponseWriter, r *http.Request) {
	const door = "otlp"
	if d.notReady(w, door) {
		return
	}
	b, err := d.body(door, r)
	if err != nil {
		d.count(door, "refused")
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	proto := mediaType(r) == "application/x-protobuf"
	var recs []Record
	if proto {
		recs, err = DecodeOTLPProto(b)
	} else {
		recs, err = DecodeOTLPJSON(b)
	}
	if err != nil {
		d.count(door, "refused")
		http.Error(w, "OTLP logs: "+err.Error(), http.StatusBadRequest)
		return
	}
	d.submit(w, r, door, recs, func() {
		// An ExportLogsServiceResponse with nothing rejected: empty in
		// protobuf, {} in JSON.
		if proto {
			w.Header().Set("Content-Type", "application/x-protobuf")
			w.WriteHeader(http.StatusOK)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, "{}")
	})
}

func (d *Doors) loki(w http.ResponseWriter, r *http.Request) {
	const door = "loki"
	if d.notReady(w, door) {
		return
	}
	b, err := d.body(door, r)
	if err != nil {
		d.count(door, "refused")
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var recs []Record
	if mediaType(r) == "application/json" {
		recs, err = DecodeLokiJSON(b)
	} else {
		var raw []byte
		if raw, err = snappyDecode(b, MaxBody); err == nil {
			recs, err = DecodeLokiProto(raw)
		}
	}
	if err != nil {
		d.count(door, "refused")
		http.Error(w, "Loki push: "+err.Error(), http.StatusBadRequest)
		return
	}
	d.submit(w, r, door, recs, func() { w.WriteHeader(http.StatusNoContent) })
}

// ServeSyslog accepts syslog connections on l until ctx ends. Over TLS
// (RFC 5425), l is a TLS listener. Syslog has no acknowledgment, so a
// connection's messages are queued, not waited for; a full queue holds
// the connection back.
func (d *Doors) ServeSyslog(ctx context.Context, l net.Listener, door string) error {
	var wg sync.WaitGroup
	go func() {
		<-ctx.Done()
		_ = l.Close()
	}()
	for {
		c, err := l.Accept()
		if err != nil {
			wg.Wait()
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			d.syslogConn(ctx, c, door)
		}()
	}
}

func (d *Doors) syslogConn(ctx context.Context, c net.Conn, door string) {
	defer func() { _ = c.Close() }()
	stop := context.AfterFunc(ctx, func() { _ = c.Close() })
	defer stop()
	if tc, ok := c.(*tls.Conn); ok {
		if err := tc.HandshakeContext(ctx); err != nil {
			d.count(door, "handshake")
			d.note("%s: %s: handshake: %v", door, c.RemoteAddr(), err)
			return
		}
	}
	d.count(door, "connection")
	r := bufio.NewReaderSize(&countReader{r: c, d: d, door: door}, MaxSyslogFrame+16)
	for {
		frame, err := ReadFrame(r)
		if err != nil {
			if !errors.Is(err, io.EOF) && ctx.Err() == nil && !errors.Is(err, net.ErrClosed) {
				d.count(door, "refused")
				d.note("%s: %s: %v", door, c.RemoteAddr(), err)
			}
			return
		}
		if len(bytes.TrimSpace(frame)) == 0 {
			continue
		}
		rec, err := ParseSyslog(frame, time.Now())
		if err != nil {
			d.count(door, "refused")
			d.note("%s: %s: %v", door, c.RemoteAddr(), err)
			continue
		}
		if err := d.Batcher.Submit(ctx, []Record{rec}, false); err != nil {
			if ctx.Err() == nil {
				d.count(door, "refused")
				d.note("%s: %s: %v", door, c.RemoteAddr(), err)
			}
			continue
		}
		if d.Metrics != nil {
			d.Metrics.Add("blogs_ingest_records_total", 1, "door", door)
		}
	}
}

type countReader struct {
	r    io.Reader
	d    *Doors
	door string
}

func (c *countReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	if n > 0 && c.d.Metrics != nil {
		c.d.Metrics.Add("blogs_ingest_received_bytes_total", float64(n), "door", c.door)
	}
	return n, err
}

// TLSConfig is a server configuration for RFC 5425: the certificate and
// key, and, when clientCA is set, mutual TLS against it.
func TLSConfig(certFile, keyFile string, clientCA []byte) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	cfg := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	if len(clientCA) > 0 {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(clientCA) {
			return nil, errors.New("the client CA file holds no PEM certificate")
		}
		cfg.ClientCAs, cfg.ClientAuth = pool, tls.RequireAndVerifyClientCert
	}
	return cfg, nil
}
