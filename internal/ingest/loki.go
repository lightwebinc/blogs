package ingest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/lightwebinc/blogs/internal/metrics"
)

// The structured metadata names every forwarded line carries: where it was
// published. A read gateway finds the line's batch by them.
const (
	MetaStream   = "blogs_stream"
	MetaProducer = "blogs_producer"
	MetaSeq      = "blogs_seq"
	MetaLine     = "blogs_line"
)

// Forwarder pushes published lines to a stock Loki, in order, retrying a
// push that fails. Its queue is bounded: a push that cannot be queued, or
// that fails for good, is counted and dropped, and the lines stay on the
// stream, where an archive holds them.
type Forwarder struct {
	// URL is Loki's base URL.
	URL string
	// Tenant, when set, is sent as X-Scope-OrgID.
	Tenant  string
	HTTP    *http.Client
	Metrics *metrics.Set
	Note    func(format string, args ...any)
	// Retries are the waits between tries of one push.
	Retries []time.Duration

	q chan []Placed
}

// DeclareForward declares the forwarder's metrics.
func DeclareForward(m *metrics.Set) {
	m.Counter("blogs_forward_lines_total", "Lines pushed to the log store.")
	m.Counter("blogs_forward_dropped_lines_total", "Lines not pushed to the log store: the queue was full or the store refused them for good.")
	m.Counter("blogs_forward_errors_total", "Pushes to the log store that failed and were tried again or dropped.")
	m.Gauge("blogs_forward_queue", "Pushes waiting for the log store.")
}

// DefaultRetries are a push's waits: about five minutes in all.
var DefaultRetries = []time.Duration{time.Second, 2 * time.Second, 5 * time.Second, 10 * time.Second, 30 * time.Second, 60 * time.Second, 60 * time.Second, 120 * time.Second}

// Start starts the pushing goroutine; it stops when ctx ends.
func (f *Forwarder) Start(ctx context.Context, queue int) {
	f.q = make(chan []Placed, queue)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case p := <-f.q:
				f.push(ctx, p)
				if f.Metrics != nil {
					f.Metrics.Set("blogs_forward_queue", float64(len(f.q)))
				}
			}
		}
	}()
}

// Forward queues lines for the store, or drops them when the queue is full.
func (f *Forwarder) Forward(p []Placed) {
	select {
	case f.q <- p:
	default:
		f.add("blogs_forward_dropped_lines_total", float64(len(p)))
		f.note("forward: the queue is full; %d line(s) not pushed to the log store", len(p))
	}
}

func (f *Forwarder) add(name string, v float64) {
	if f.Metrics != nil {
		f.Metrics.Add(name, v)
	}
}

func (f *Forwarder) note(format string, args ...any) {
	if f.Note != nil {
		f.Note(format, args...)
	}
}

// PushBody is the Loki JSON push for placed lines: one stream per label
// set, each line with its placement as structured metadata.
func PushBody(p []Placed) ([]byte, error) {
	type stream struct {
		Stream map[string]string `json:"stream"`
		Values [][]any           `json:"values"`
	}
	by := map[string]*stream{}
	var order []string
	for _, x := range p {
		lbl := map[string]string{}
		for k, v := range x.Rec.Labels {
			lbl[LabelName(k)] = v
		}
		if len(lbl) == 0 {
			lbl["service_name"] = "unknown_service"
		}
		var key strings.Builder
		for _, k := range sortedKeys(lbl) {
			fmt.Fprintf(&key, "%s=%q,", k, lbl[k])
		}
		s := by[key.String()]
		if s == nil {
			s = &stream{Stream: lbl}
			by[key.String()] = s
			order = append(order, key.String())
		}
		s.Values = append(s.Values, []any{strconv.FormatInt(x.Rec.Ts, 10), string(x.Line), map[string]string{
			MetaStream: x.Stream, MetaProducer: x.Producer, MetaSeq: strconv.FormatUint(x.Seq, 10), MetaLine: strconv.Itoa(x.Index),
		}})
	}
	sort.Strings(order)
	out := struct {
		Streams []*stream `json:"streams"`
	}{}
	for _, k := range order {
		out.Streams = append(out.Streams, by[k])
	}
	return json.Marshal(out)
}

func (f *Forwarder) push(ctx context.Context, p []Placed) {
	body, err := PushBody(p)
	if err != nil {
		f.add("blogs_forward_dropped_lines_total", float64(len(p)))
		f.note("forward: %v", err)
		return
	}
	retries := f.Retries
	if retries == nil {
		retries = DefaultRetries
	}
	client := f.HTTP
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	for try := 0; ; try++ {
		err, again := f.post(ctx, client, body)
		if err == nil {
			f.add("blogs_forward_lines_total", float64(len(p)))
			return
		}
		f.add("blogs_forward_errors_total", 1)
		if !again || try >= len(retries) || ctx.Err() != nil {
			f.add("blogs_forward_dropped_lines_total", float64(len(p)))
			f.note("forward: %d line(s) not pushed to the log store: %v", len(p), err)
			return
		}
		select {
		case <-ctx.Done():
		case <-time.After(retries[try]):
		}
	}
}

// post sends one push; again says whether a failure is worth repeating.
func (f *Forwarder) post(ctx context.Context, client *http.Client, body []byte) (error, bool) { //nolint:revive // two results read together
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(f.URL, "/")+"/loki/api/v1/push", bytes.NewReader(body))
	if err != nil {
		return err, false
	}
	req.Header.Set("Content-Type", "application/json")
	if f.Tenant != "" {
		req.Header.Set("X-Scope-OrgID", f.Tenant)
	}
	resp, err := client.Do(req)
	if err != nil {
		return err, true
	}
	defer resp.Body.Close()
	msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	switch {
	case resp.StatusCode/100 == 2:
		return nil, false
	case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
		return fmt.Errorf("status %d: %s", resp.StatusCode, bytes.TrimSpace(msg)), true
	}
	return fmt.Errorf("status %d: %s", resp.StatusCode, bytes.TrimSpace(msg)), false
}
