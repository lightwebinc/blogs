package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/lightwebinc/blogs/internal/metrics"
)

// The names a returned line's mark is given: a stream label when Loki
// answers with labels merged, structured metadata when the client asks for
// categorized labels (Grafana does).
const (
	LabelState  = "blogs_state"
	LabelReason = "blogs_reason"
)

// Gateway is the Loki API in front of Loki.
type Gateway struct {
	Loki     *url.URL
	Verifier *Verifier
	Metrics  *metrics.Set
	Note     func(format string, args ...any)

	proxy *httputil.ReverseProxy
}

// DeclareMetrics declares the gateway's metrics.
func DeclareMetrics(m *metrics.Set) {
	m.Counter("blogs_gateway_lines_total", "Lines returned, by mark and reason.")
	m.Gauge("blogs_retained_bytes", "Bytes the archive keeps, by stream and kind: content (the lines, as each batch records their size; billed) or evidence (carriers with their funding trees; not billed).")
	m.Counter("blogs_retained_byte_seconds_total", "Retained content bytes integrated over time, by stream: the billed retention; divide by 2.592e15 for GB-months (30-day months).")
}

// Handler serves the Loki API, the coverage endpoint and the metrics.
func (g *Gateway) Handler() http.Handler {
	g.proxy = &httputil.ReverseProxy{
		Rewrite: func(r *httputil.ProxyRequest) {
			r.SetURL(g.Loki)
			r.Out.Host = g.Loki.Host
			// The answer is rewritten, so it must arrive uncompressed.
			r.Out.Header.Del("Accept-Encoding")
		},
		ModifyResponse: g.modify,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /blogs/v1/coverage", g.coverage)
	if g.Metrics != nil {
		mux.Handle("GET /metrics", g.Metrics)
	}
	mux.Handle("/", g.proxy)
	return mux
}

func (g *Gateway) coverage(w http.ResponseWriter, _ *http.Request) {
	c, err := g.Verifier.Coverage()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(c)
}

// Meter updates the retention gauges and integrates them every period
// until ctx ends.
func (g *Gateway) Meter(ctx context.Context, period time.Duration) {
	last := time.Now()
	t := time.NewTicker(period)
	defer t.Stop()
	for {
		now := time.Now()
		dt := now.Sub(last).Seconds()
		last = now
		for stream, kinds := range g.Verifier.Retained() {
			for kind, n := range kinds {
				g.Metrics.Set("blogs_retained_bytes", float64(n), "stream", stream, "kind", kind)
			}
			g.Metrics.Add("blogs_retained_byte_seconds_total", float64(kinds["content"])*dt, "stream", stream)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func annotated(path string) bool {
	return strings.HasSuffix(path, "/loki/api/v1/query_range") || strings.HasSuffix(path, "/loki/api/v1/query")
}

func (g *Gateway) modify(resp *http.Response) error {
	if resp.StatusCode != http.StatusOK || !annotated(resp.Request.URL.Path) || resp.Header.Get("Content-Encoding") != "" {
		return nil
	}
	raw, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return err
	}
	out, err := g.Annotate(resp.Request.Context(), raw)
	if err != nil {
		out = raw
	}
	resp.Body = io.NopCloser(bytes.NewReader(out))
	resp.ContentLength = int64(len(out))
	resp.Header.Set("Content-Length", strconv.Itoa(len(out)))
	return nil
}

type lokiStream struct {
	Stream map[string]string   `json:"stream"`
	Values [][]json.RawMessage `json:"values"`
}

// Annotate marks every line of a Loki query answer of streams; any other
// answer is returned as it is.
func (g *Gateway) Annotate(ctx context.Context, raw []byte) ([]byte, error) {
	var top map[string]json.RawMessage
	if err := json.Unmarshal(raw, &top); err != nil {
		return nil, err
	}
	var data map[string]json.RawMessage
	if err := json.Unmarshal(top["data"], &data); err != nil {
		return nil, err
	}
	var rt string
	_ = json.Unmarshal(data["resultType"], &rt)
	if rt != "streams" {
		return raw, nil
	}
	var result []lokiStream
	if err := json.Unmarshal(data["result"], &result); err != nil {
		return nil, err
	}
	var out []lokiStream
	for _, s := range result {
		split := map[string]*lokiStream{}
		var order []string
		for _, v := range s.Values {
			if len(v) < 2 {
				continue
			}
			var ts, line string
			_ = json.Unmarshal(v[0], &ts)
			_ = json.Unmarshal(v[1], &line)
			if len(v) >= 3 {
				// Categorized labels: structured metadata on the entry.
				var cat map[string]json.RawMessage
				_ = json.Unmarshal(v[2], &cat)
				if cat == nil {
					cat = map[string]json.RawMessage{}
				}
				md := metaOf(cat["structuredMetadata"])
				vd := g.Verifier.Check(ctx, md, line, ts, s.Stream)
				g.count(vd)
				md[LabelState] = vd.State
				if vd.Reason != "" {
					md[LabelReason] = vd.Reason
				}
				cat["structuredMetadata"], _ = json.Marshal(md)
				v[2], _ = json.Marshal(cat)
				key := ""
				if split[key] == nil {
					split[key] = &lokiStream{Stream: s.Stream}
					order = append(order, key)
				}
				split[key].Values = append(split[key].Values, v)
				continue
			}
			vd := g.Verifier.Check(ctx, s.Stream, line, ts, s.Stream)
			g.count(vd)
			key := vd.State + "/" + vd.Reason
			if split[key] == nil {
				lbl := make(map[string]string, len(s.Stream)+2)
				for k, x := range s.Stream {
					lbl[k] = x
				}
				lbl[LabelState] = vd.State
				if vd.Reason != "" {
					lbl[LabelReason] = vd.Reason
				}
				split[key] = &lokiStream{Stream: lbl}
				order = append(order, key)
			}
			split[key].Values = append(split[key].Values, v)
		}
		sort.Strings(order)
		for _, k := range order {
			out = append(out, *split[k])
		}
	}
	if out == nil {
		out = []lokiStream{}
	}
	var err error
	if data["result"], err = json.Marshal(out); err != nil {
		return nil, err
	}
	if top["data"], err = json.Marshal(data); err != nil {
		return nil, err
	}
	return json.Marshal(top)
}

// metaOf reads structured metadata as Loki writes it: an object of names
// to values, or a list of {name, value}.
func metaOf(raw json.RawMessage) map[string]string {
	m := map[string]string{}
	if len(raw) == 0 {
		return m
	}
	if json.Unmarshal(raw, &m) == nil {
		return m
	}
	var pairs []struct {
		Name  string `json:"name"`
		Value string `json:"value"`
	}
	m = map[string]string{}
	if json.Unmarshal(raw, &pairs) == nil {
		for _, p := range pairs {
			m[p.Name] = p.Value
		}
	}
	return m
}

func (g *Gateway) count(v Verdict) {
	if g.Metrics != nil {
		g.Metrics.Add("blogs_gateway_lines_total", 1, "state", v.State, "reason", v.Reason)
	}
}
