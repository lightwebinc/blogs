package ingest

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lightwebinc/blogs/internal/metrics"
	"github.com/lightwebinc/blogs/internal/produce"
)

// The test-side protobuf writer.
func pbTag(b []byte, num, wt int) []byte { return binary.AppendUvarint(b, uint64(num<<3|wt)) }
func pbBytes(b []byte, num int, v []byte) []byte {
	b = pbTag(b, num, 2)
	b = binary.AppendUvarint(b, uint64(len(v)))
	return append(b, v...)
}
func pbStr(b []byte, num int, s string) []byte { return pbBytes(b, num, []byte(s)) }
func pbVarint(b []byte, num int, v uint64) []byte {
	return binary.AppendUvarint(pbTag(b, num, 0), v)
}
func pbFixed64(b []byte, num int, v uint64) []byte {
	return binary.LittleEndian.AppendUint64(pbTag(b, num, 1), v)
}

// SnappyLiteral is a valid snappy block of literals only: what a test
// needs to send a protobuf Loki push without a snappy library.
func SnappyLiteral(src []byte) []byte {
	out := binary.AppendUvarint(nil, uint64(len(src)))
	for len(src) > 0 {
		n := min(len(src), 1<<16)
		if n <= 60 {
			out = append(out, byte(n-1)<<2)
		} else {
			out = append(out, 61<<2, byte(n-1), byte((n-1)>>8))
		}
		out = append(out, src[:n]...)
		src = src[n:]
	}
	return out
}

func TestLineFormIsCanonical(t *testing.T) {
	r := Record{Ts: 1791400000123456789, Sev: "INFO", Labels: map[string]string{"service_name": "api", "env": "prod"},
		Attrs: map[string]string{"trace_id": "ab", "a": "<&>"}, Body: "GET /health 200"}
	got, err := r.Line()
	if err != nil {
		t.Fatal(err)
	}
	want := `{"attrs":{"a":"<&>","trace_id":"ab"},"body":"GET /health 200","labels":{"env":"prod","service_name":"api"},"sev":"INFO","ts":"1791400000123456789","v":1}`
	if string(got) != want {
		t.Fatalf("line\n got %s\nwant %s", got, want)
	}
	back, err := ParseLine(got)
	if err != nil || back.Ts != r.Ts || back.Labels["env"] != "prod" || back.Body != r.Body {
		t.Fatalf("parse back: %+v %v", back, err)
	}
	empty, _ := (&Record{Ts: 1, Body: "x"}).Line()
	if string(empty) != `{"body":"x","labels":{},"ts":"1","v":1}` {
		t.Fatalf("empty record: %s", empty)
	}
	if _, err := (&Record{Body: strings.Repeat("z", MaxRecordLine)}).Line(); err == nil {
		t.Fatal("an over-long record encoded")
	}
}

func TestSyslog(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	in := "<165>1 2026-10-08T12:00:00.5Z web-1 nginx 123 ID47 [exampleSDID@32473 iut=\"3\" eventSource=\"App\\\"lication\"] \ufeffGET / 200\n" +
		"<13>Oct  8 12:00:00 host legacy: a bsd message\n"
	oc := "<14>1 2026-10-08T12:00:01Z - app - - - octet"
	framed := strconv.Itoa(len(oc)) + " " + oc + in
	r := bufio.NewReaderSize(strings.NewReader(framed), MaxSyslogFrame+16)
	var recs []Record
	for {
		f, err := ReadFrame(r)
		if err != nil {
			break
		}
		rec, err := ParseSyslog(f, now)
		if err != nil {
			t.Fatalf("%q: %v", f, err)
		}
		recs = append(recs, rec)
	}
	if len(recs) != 3 {
		t.Fatalf("got %d records", len(recs))
	}
	if recs[0].Body != "octet" || recs[0].Labels["service_name"] != "app" || recs[0].Sev != "info" {
		t.Fatalf("octet counted: %+v", recs[0])
	}
	r1 := recs[1]
	if r1.Sev != "notice" || r1.Attrs["facility"] != "local4" || r1.Labels["host"] != "web-1" || r1.Labels["service_name"] != "nginx" ||
		r1.Attrs["procid"] != "123" || r1.Attrs["msgid"] != "ID47" || r1.Attrs["sd.exampleSDID@32473.eventSource"] != `App"lication` ||
		r1.Body != "GET / 200" || r1.Ts != time.Date(2026, 10, 8, 12, 0, 0, 5e8, time.UTC).UnixNano() {
		t.Fatalf("RFC 5424: %+v", r1)
	}
	if recs[2].Body != "Oct  8 12:00:00 host legacy: a bsd message" || recs[2].Ts != now.UnixNano() || recs[2].Sev != "notice" {
		t.Fatalf("RFC 3164 carried whole: %+v", recs[2])
	}
	if _, err := ParseSyslog([]byte("no priority"), now); err == nil {
		t.Fatal("a message without a priority parsed")
	}
}

func TestOTLP(t *testing.T) {
	js := `{"resourceLogs":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"api"}},{"key":"host.name","value":{"stringValue":"h1"}}]},
	"scopeLogs":[{"scope":{"name":"lib"},"logRecords":[{"timeUnixNano":"1791400000000000001","severityNumber":9,"body":{"stringValue":"hello"},
	"attributes":[{"key":"n","value":{"intValue":"42"}},{"key":"m","value":{"kvlistValue":{"values":[{"key":"k","value":{"boolValue":true}}]}}}],
	"traceId":"0102","spanId":"03"},{"observedTimeUnixNano":"5","severityText":"Error","body":{"arrayValue":{"values":[{"doubleValue":1.5}]}}}]}]}]}`
	recs, err := DecodeOTLPJSON([]byte(js))
	if err != nil || len(recs) != 2 {
		t.Fatalf("%v %d", err, len(recs))
	}
	a := recs[0]
	if a.Ts != 1791400000000000001 || a.Sev != "INFO" || a.Body != "hello" || a.Labels["service_name"] != "api" ||
		a.Attrs["resource.host.name"] != "h1" || a.Attrs["scope"] != "lib" || a.Attrs["n"] != "42" || a.Attrs["m"] != `{"k":true}` ||
		a.Attrs["trace_id"] != "0102" || a.Attrs["span_id"] != "03" {
		t.Fatalf("json record: %+v", a)
	}
	if recs[1].Ts != 5 || recs[1].Sev != "Error" || recs[1].Body != "[1.5]" {
		t.Fatalf("json record 2: %+v", recs[1])
	}

	kv := func(k string, v []byte) []byte { return pbBytes(pbStr(nil, 1, k), 2, v) }
	resource := pbBytes(nil, 1, kv("service.name", pbStr(nil, 1, "api")))
	resource = pbBytes(resource, 1, kv("host.name", pbStr(nil, 1, "h1")))
	lr := pbFixed64(nil, 1, 1791400000000000001)
	lr = pbVarint(lr, 2, 17)
	lr = pbBytes(lr, 5, pbStr(nil, 1, "hello"))
	lr = pbBytes(lr, 6, kv("n", pbVarint(nil, 3, 42)))
	lr = pbBytes(lr, 6, kv("d", pbFixed64(nil, 4, math.Float64bits(2.5))))
	lr = pbBytes(lr, 9, []byte{1, 2})
	lr = pbVarint(lr, 99, 7) // an unknown field is skipped
	scope := pbBytes(pbBytes(nil, 1, pbStr(nil, 1, "lib")), 2, lr)
	rl := pbBytes(pbBytes(nil, 1, resource), 2, scope)
	req := pbBytes(nil, 1, rl)
	recs, err = DecodeOTLPProto(req)
	if err != nil || len(recs) != 1 {
		t.Fatalf("%v %d", err, len(recs))
	}
	p := recs[0]
	if p.Ts != 1791400000000000001 || p.Sev != "ERROR" || p.Body != "hello" || p.Labels["service_name"] != "api" ||
		p.Attrs["resource.host.name"] != "h1" || p.Attrs["n"] != "42" || p.Attrs["d"] != "2.5" || p.Attrs["trace_id"] != "0102" {
		t.Fatalf("proto record: %+v", p)
	}
	if _, err := DecodeOTLPProto(req[:len(req)-3]); err == nil {
		t.Fatal("a truncated message decoded")
	}
}

func TestLokiPush(t *testing.T) {
	js := `{"streams":[{"stream":{"service_name":"api","env":"prod"},"values":[["1791400000000000001","one"],["1791400000000000002","two",{"trace_id":"ab"}]]}]}`
	recs, err := DecodeLokiJSON([]byte(js))
	if err != nil || len(recs) != 2 || recs[1].Attrs["trace_id"] != "ab" || recs[0].Labels["env"] != "prod" || recs[1].Ts != 1791400000000000002 {
		t.Fatalf("json: %v %+v", err, recs)
	}
	ts := pbVarint(pbVarint(nil, 1, 1791400000), 2, 7)
	entry := pbBytes(nil, 1, ts)
	entry = pbStr(entry, 2, "a line")
	entry = pbBytes(entry, 3, pbStr(pbStr(nil, 1, "trace_id"), 2, "cd"))
	stream := pbStr(nil, 1, `{service_name="api", path="a \"q\" b"}`)
	stream = pbBytes(stream, 2, entry)
	req := pbBytes(nil, 1, stream)
	raw, err := snappyDecode(SnappyLiteral(req), MaxBody)
	if err != nil {
		t.Fatal(err)
	}
	recs, err = DecodeLokiProto(raw)
	if err != nil || len(recs) != 1 {
		t.Fatalf("%v %d", err, len(recs))
	}
	r := recs[0]
	if r.Ts != 1791400000000000007 || r.Body != "a line" || r.Attrs["trace_id"] != "cd" || r.Labels["path"] != `a "q" b` || r.Labels["service_name"] != "api" {
		t.Fatalf("proto: %+v", r)
	}
}

func TestSnappyCopies(t *testing.T) {
	// "abcd" literal, then a 1-byte-offset copy of 8 at offset 4, then a
	// 2-byte-offset copy of 4 at offset 12.
	block := []byte{16, 3 << 2, 'a', 'b', 'c', 'd', 1 | (8-4)<<2, 4, 2 | (4-1)<<2, 12, 0}
	got, err := snappyDecode(block, 100)
	if err != nil || string(got) != "abcdabcdabcdabcd" {
		t.Fatalf("%q %v", got, err)
	}
	if _, err := snappyDecode(block, 10); err == nil {
		t.Fatal("a block over the bound decoded")
	}
	if _, err := snappyDecode([]byte{4, 1 | 0<<2, 9}, 100); err == nil {
		t.Fatal("a copy before the start decoded")
	}
}

func TestParseLabels(t *testing.T) {
	m, err := ParseLabels(`{a="1",b="x\\y\nz"}`)
	if err != nil || m["a"] != "1" || m["b"] != "x\\y\nz" {
		t.Fatalf("%v %q", err, m)
	}
	for _, bad := range []string{`a="1"`, `{a=1}`, `{a="1}`, `{="1"}`} {
		if _, err := ParseLabels(bad); err == nil {
			t.Fatalf("%s parsed", bad)
		}
	}
}

// fakeEngine publishes in memory, splitting a batch in two when told to.
type fakeEngine struct {
	seq   uint64
	split bool
	fail  error
	ticks int
}

func (f *fakeEngine) Stream() string   { return "app_abcdefghij" }
func (f *fakeEngine) Producer() string { return "02aa" }
func (f *fakeEngine) Tick(context.Context) error {
	f.ticks++
	return nil
}
func (f *fakeEngine) Publish(_ context.Context, raw []byte, count int) ([]Published, error) {
	if f.fail != nil {
		return nil, f.fail
	}
	parts := [][]byte{raw}
	if f.split && count > 1 {
		i := strings.IndexByte(string(raw), '\n') + 1
		parts = [][]byte{raw[:i], raw[i:]}
	}
	var out []Published
	for _, p := range parts {
		f.seq++
		out = append(out, Published{Seq: f.seq, Carrier: "c", Raw: p})
	}
	return out, nil
}

func TestBatcherPlacesEveryLine(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e := &fakeEngine{split: true}
	var got []Placed
	m := metrics.New()
	Declare(m)
	b := &Batcher{Lanes: []*Lane{{Engine: e, Cut: produce.Cut{MaxBytes: 1 << 20, MaxLines: 3, MaxWait: 20 * time.Millisecond}}},
		Forward: func(p []Placed) { got = append(got, p...) }, Poll: 5 * time.Millisecond, Metrics: m}
	b.Start()
	done := make(chan error)
	go func() { done <- b.Run(ctx, time.Second) }()
	recs := make([]Record, 7)
	for i := range recs {
		recs[i] = Record{Ts: int64(i + 1), Body: "r"}
	}
	if err := b.Submit(ctx, recs, true); err != nil {
		t.Fatal(err)
	}
	cancel()
	<-done
	if len(got) != 7 {
		t.Fatalf("placed %d", len(got))
	}
	// Chunks of 3, 3 and 1 lines; each of more than one line published as
	// two batches, the first of one line.
	want := [][2]uint64{{1, 1}, {2, 1}, {2, 2}, {3, 1}, {4, 1}, {4, 2}, {5, 1}}
	for i, p := range got {
		if p.Seq != want[i][0] || uint64(p.Index) != want[i][1] || p.Rec.Ts != int64(i+1) {
			t.Fatalf("line %d placed at seq %d line %d, want %v", i, p.Seq, p.Index, want[i])
		}
	}
	if m.Get("blogs_ingest_lines_total", "stream", "app_abcdefghij") != 7 || e.ticks == 0 {
		t.Fatalf("metrics or ticks: %v %d", m.Get("blogs_ingest_lines_total", "stream", "app_abcdefghij"), e.ticks)
	}
}

func TestBatcherRefusesOnPublishError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e := &fakeEngine{fail: errors.New("no hosts")}
	b := &Batcher{Lanes: []*Lane{{Engine: e, Cut: produce.Cut{MaxBytes: 1 << 20, MaxLines: 100, MaxWait: 10 * time.Millisecond}}}}
	b.Start()
	go func() { _ = b.Run(ctx, time.Second) }()
	err := b.Submit(ctx, []Record{{Body: "a"}, {Body: "b"}}, true)
	if err == nil || !strings.Contains(err.Error(), "no hosts") {
		t.Fatalf("got %v", err)
	}
	if err := b.Submit(ctx, []Record{{Body: strings.Repeat("z", MaxRecordLine)}}, true); err == nil {
		t.Fatal("an over-long record was taken")
	}
}

// Before the sidecar is ready the HTTP doors answer 503 with Retry-After,
// and the health check says not ready.
func TestDoorsNotReady(t *testing.T) {
	d := &Doors{Batcher: &Batcher{}, Ready: func() bool { return false }}
	h := d.Handler()
	for _, path := range []string{"/v1/logs", "/loki/api/v1/push"} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, path, strings.NewReader("{}")))
		if w.Code != http.StatusServiceUnavailable || w.Header().Get("Retry-After") == "" {
			t.Fatalf("%s: %d", path, w.Code)
		}
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("healthz: %d", w.Code)
	}
}
