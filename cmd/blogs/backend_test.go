package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lightwebinc/blogs/internal/archive"
	"github.com/lightwebinc/blogs/internal/gateway"
	"github.com/lightwebinc/blogs/internal/ingest"
	"github.com/lightwebinc/blogs/internal/reader"
	"github.com/lightwebinc/blogs/internal/testchain"
)

// fakeLoki keeps what is pushed (JSON pushes, as the sidecar sends them)
// and answers query_range with all of it, in Loki's two answer shapes:
// labels and structured metadata merged into stream labels, or, when the
// client asks for categorize-labels, structured metadata on each entry.
type fakeLoki struct {
	mu      sync.Mutex
	entries []lokiEntry
}

type lokiEntry struct {
	labels map[string]string
	ts     string
	line   string
	meta   map[string]string
}

func (f *fakeLoki) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/loki/api/v1/push":
		var req struct {
			Streams []struct {
				Stream map[string]string `json:"stream"`
				Values [][]any           `json:"values"`
			} `json:"streams"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		f.mu.Lock()
		for _, s := range req.Streams {
			for _, v := range s.Values {
				e := lokiEntry{labels: s.Stream, ts: v[0].(string), line: v[1].(string), meta: map[string]string{}}
				if len(v) == 3 {
					for k, x := range v[2].(map[string]any) {
						e.meta[k] = x.(string)
					}
				}
				f.entries = append(f.entries, e)
			}
		}
		f.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	case "/loki/api/v1/query_range":
		cat := strings.Contains(r.Header.Get("X-Loki-Response-Encoding-Flags"), "categorize-labels")
		f.mu.Lock()
		var result []map[string]any
		for _, e := range f.entries {
			lbl := map[string]string{}
			for k, v := range e.labels {
				lbl[k] = v
			}
			val := []any{e.ts, e.line}
			if cat {
				val = append(val, map[string]any{"structuredMetadata": e.meta})
			} else {
				for k, v := range e.meta {
					lbl[k] = v
				}
			}
			result = append(result, map[string]any{"stream": lbl, "values": [][]any{val}})
		}
		f.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "success", "data": map[string]any{"resultType": "streams", "result": result, "stats": map[string]any{}}})
	case "/loki/api/v1/labels":
		_, _ = io.WriteString(w, `{"status":"success","data":["service_name"]}`)
	default:
		http.NotFound(w, r)
	}
}

func (f *fakeLoki) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.entries)
}

func (f *fakeLoki) find(body string) (lokiEntry, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, e := range f.entries {
		if strings.Contains(e.line, body) {
			return e, true
		}
	}
	return lokiEntry{}, false
}

func listenPort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

func waitUntil(t *testing.T, what string, ok func() bool) {
	t.Helper()
	for i := 0; i < 400; i++ {
		if ok() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

type marked struct {
	line   string
	state  string
	reason string
	labels map[string]string
}

// queryGateway asks the gateway for everything and returns each line with
// its mark.
func queryGateway(t *testing.T, base string, categorized bool) []marked {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, base+"/loki/api/v1/query_range?query="+url.QueryEscape(`{service_name=~".+"}`), nil)
	if categorized {
		req.Header.Set("X-Loki-Response-Encoding-Flags", "categorize-labels")
	}
	req.Header.Set("Accept-Encoding", "gzip")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("query_range: %d %s", resp.StatusCode, raw)
	}
	var ans struct {
		Data struct {
			Result []struct {
				Stream map[string]string   `json:"stream"`
				Values [][]json.RawMessage `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &ans); err != nil {
		t.Fatalf("%v: %s", err, raw)
	}
	var out []marked
	for _, s := range ans.Data.Result {
		for _, v := range s.Values {
			var m marked
			_ = json.Unmarshal(v[1], &m.line)
			m.labels = s.Stream
			if categorized {
				var cat struct {
					StructuredMetadata map[string]string `json:"structuredMetadata"`
				}
				_ = json.Unmarshal(v[2], &cat)
				m.state, m.reason = cat.StructuredMetadata[gateway.LabelState], cat.StructuredMetadata[gateway.LabelReason]
			} else {
				m.state, m.reason = s.Stream[gateway.LabelState], s.Stream[gateway.LabelReason]
			}
			out = append(out, m)
		}
	}
	return out
}

func post(t *testing.T, u, ctype string, body []byte, want int) {
	t.Helper()
	resp, err := http.Post(u, ctype, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	msg, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != want {
		t.Fatalf("POST %s: %d %s", u, resp.StatusCode, msg)
	}
}

func snappyLiteral(src []byte) []byte {
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

func lokiProtoPush(labels, line string, sec uint64) []byte {
	pb := func(b []byte, num int, v []byte) []byte {
		b = binary.AppendUvarint(b, uint64(num<<3|2))
		return append(binary.AppendUvarint(b, uint64(len(v))), v...)
	}
	ts := binary.AppendUvarint(binary.AppendUvarint(nil, 1<<3), sec)
	entry := pb(pb(nil, 1, ts), 2, []byte(line))
	stream := pb(pb(nil, 1, []byte(labels)), 2, entry)
	return snappyLiteral(pb(nil, 1, stream))
}

var rootLine = regexp.MustCompile(`(?m)^interval (\d+)  \S+  seqs \d+ to \d+  root ([0-9a-f]{64})`)

// The sidecar end to end on the local chain and three stand-in hosts in
// mode unicast: records in by every door, published on two streams (one
// digest-only), forwarded to a Loki with their placement; anchored; three
// archives, one per host, agree on every root; the gateway marks every line
// verified, an altered line in the store unverified, and a retracted
// batch's lines as a receipted hole.
func TestBackendSidecarToGateway(t *testing.T) {
	h := newHarness(t)
	stream, producer := h.producer()
	r := h.must("", "stream", "new", "sec")
	conf := streamLine.FindStringSubmatch(r.stdout)[1]
	hosts := []string{h.hostS.URL}
	for range 2 {
		s := httptest.NewServer(testchain.NewHost(h.chain))
		t.Cleanup(s.Close)
		hosts = append(hosts, s.URL)
	}
	backendFlow(t, h, stream, producer, conf, hosts)
}

// backendFlow runs the backend on a harness whose home holds stream and
// conf, publishing in mode unicast to hosts (three), one archive following
// each.
func backendFlow(t *testing.T, h *harness, stream, producer, conf string, hosts []string) {
	t.Helper()
	delete(h.env, "BLOGS_FACADE")
	h.env["BLOGS_MODE"] = "unicast"
	h.env["BLOGS_HOSTS"] = strings.Join(hosts, ",")
	content := t.TempDir()
	loki := &fakeLoki{}
	lokiS := httptest.NewServer(loki)
	defer lokiS.Close()

	httpAddr, syslogAddr := listenPort(t), listenPort(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan result, 1)
	go func() {
		done <- h.runCtx(ctx, "", append([]string{"ingest", "-stream", stream, "-confidential-stream", conf, "-content-dir", content,
			"-http", httpAddr, "-syslog", syslogAddr, "-loki", lokiS.URL, "-max-wait", "20ms", "-max-rate", "50", "-max-lines", "4"}, fast...)...)
	}()
	waitUntil(t, "the sidecar", func() bool {
		resp, err := http.Get("http://" + httpAddr + "/healthz")
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	})

	otlp := `{"resourceLogs":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"api"}}]},"scopeLogs":[{"logRecords":[` +
		`{"timeUnixNano":"1791400000000000001","severityText":"INFO","body":{"stringValue":"otlp one"}},` +
		`{"timeUnixNano":"1791400000000000002","severityText":"WARN","body":{"stringValue":"otlp two"}}]}]}]}`
	post(t, "http://"+httpAddr+"/v1/logs", "application/json", []byte(otlp), http.StatusOK)
	post(t, "http://"+httpAddr+"/loki/api/v1/push", "application/json",
		[]byte(`{"streams":[{"stream":{"service_name":"web"},"values":[["1791400000000000003","loki json"]]},`+
			`{"stream":{"service_name":"vault","confidential":"true"},"values":[["1791400000000000004","secret token issued"]]}]}`), http.StatusNoContent)
	post(t, "http://"+httpAddr+"/loki/api/v1/push", "application/x-protobuf", lokiProtoPush(`{service_name="web"}`, "loki proto", 1791400000), http.StatusNoContent)
	post(t, "http://"+httpAddr+"/loki/api/v1/push", "application/json", []byte(`{"streams":[`), http.StatusBadRequest)
	c, err := net.Dial("tcp", syslogAddr)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 6 {
		fmt.Fprintf(c, "<14>1 2026-10-08T12:00:0%dZ h1 cron %d - - syslog %d\n", i, 100+i, i)
	}
	c.Close()
	waitUntil(t, "every line in Loki", func() bool { return loki.count() == 11 })
	m, err := http.Get("http://" + httpAddr + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	metricsText, _ := io.ReadAll(m.Body)
	m.Body.Close()
	for _, want := range []string{`blogs_ingest_lines_total{stream="` + stream + `"} 10`, `blogs_ingest_lines_total{stream="` + conf + `"} 1`,
		`blogs_ingest_records_total{door="syslog"} 6`, `blogs_forward_lines_total 11`} {
		if !strings.Contains(string(metricsText), want) {
			t.Fatalf("metrics lack %q:\n%s", want, metricsText)
		}
	}
	cancel()
	if r := <-done; r.code != 0 {
		t.Fatalf("ingest exit %d\n%s\n%s", r.code, r.stdout, r.stderr)
	}
	secret, _ := loki.find("secret token")
	if secret.meta[ingest.MetaStream] != conf {
		t.Fatalf("the confidential record went to %q", secret.meta[ingest.MetaStream])
	}
	for _, s := range []string{stream, conf} {
		h.must("", append([]string{"anchor", s}, fast...)...)
	}

	// Three archives of the stream agree on every root.
	dirs := []string{t.TempDir(), t.TempDir(), t.TempDir()}
	roots := make([]string, 3)
	for i, d := range dirs {
		var all []string
		for _, s := range [][2]string{{stream, producer}, {conf, producer}} {
			args := []string{"archive", s[0], s[1], "-dir", d, "-host", hosts[i]}
			if i == 0 {
				// The first archive is the deployment's own and holds the
				// confidential content; the others hold its digests only.
				args = append(args, "-content-dir", content)
			}
			r := h.must("", args...)
			for _, m := range rootLine.FindAllStringSubmatch(r.stdout, -1) {
				all = append(all, s[0]+" "+m[1]+" "+m[2])
			}
		}
		sort.Strings(all)
		roots[i] = strings.Join(all, "\n")
		if len(all) < 2 {
			t.Fatalf("archive %d proved %d intervals", i, len(all))
		}
	}
	if roots[0] != roots[1] || roots[1] != roots[2] {
		t.Fatalf("archives disagree:\n%s\n--\n%s\n--\n%s", roots[0], roots[1], roots[2])
	}

	gw := &gateway.Gateway{Verifier: &gateway.Verifier{Dir: dirs[0], Fetch: reader.Fetch{Dirs: []string{content}}}}
	gw.Loki, _ = url.Parse(lokiS.URL)
	gwS := httptest.NewServer(gw.Handler())
	defer gwS.Close()
	// Retention metering: content is the batches' recorded sizes, the
	// same at an archive holding the confidential lines (a) and one holding
	// their digests only (b); evidence is what each keeps on disk.
	ret, rb := gw.Verifier.Retained(), (&gateway.Verifier{Dir: dirs[1]}).Retained()
	if ret[stream]["content"] == 0 || ret[stream]["evidence"] == 0 || ret[conf]["content"] == 0 || rb[conf]["content"] != ret[conf]["content"] {
		t.Fatalf("retained at archive a %v, b %v", ret, rb)
	}
	for _, cat := range []bool{false, true} {
		got := queryGateway(t, gwS.URL, cat)
		if len(got) != 11 {
			t.Fatalf("gateway answered %d lines", len(got))
		}
		for _, l := range got {
			if l.state != gateway.Verified {
				t.Fatalf("categorized %v: %q is %s (%s)", cat, l.line, l.state, l.reason)
			}
		}
	}

	// A second archive cross-checks: the second host's agrees, so every
	// line stays verified; one that has proven nothing makes every line
	// pending.
	gw.Verifier.Cross = dirs[1]
	for _, l := range queryGateway(t, gwS.URL, true) {
		if l.state != gateway.Verified {
			t.Fatalf("cross-checked: %q is %s (%s)", l.line, l.state, l.reason)
		}
	}
	gw.Verifier.Cross = t.TempDir()
	for _, l := range queryGateway(t, gwS.URL, true) {
		if l.state != gateway.Unverified || l.reason != gateway.ReasonPending {
			t.Fatalf("cross-checked against an empty archive: %q is %s (%s)", l.line, l.state, l.reason)
		}
	}
	gw.Verifier.Cross = ""

	// An altered line in the store: same placement, other text.
	orig, _ := loki.find("loki json")
	altered := lokiEntry{labels: orig.labels, ts: orig.ts, line: strings.Replace(orig.line, "loki json", "loki JSON", 1), meta: orig.meta}
	// A relabeled one: the right text under another service.
	relabeled := lokiEntry{labels: map[string]string{"service_name": "other"}, ts: orig.ts, line: orig.line, meta: orig.meta}
	loki.mu.Lock()
	loki.entries = append(loki.entries, altered, relabeled)
	loki.mu.Unlock()
	reasons := map[string]string{}
	for _, l := range queryGateway(t, gwS.URL, true) {
		if l.state == gateway.Unverified {
			reasons[l.line+"|"+l.reason] = l.reason
		}
	}
	if len(reasons) != 2 || reasons[altered.line+"|altered"] != "altered" || reasons[orig.line+"|relabeled"] != "relabeled" {
		t.Fatalf("tampering not caught as altered and relabeled: %v", reasons)
	}

	// Retract the batch holding "otlp one".
	one, _ := loki.find("otlp one")
	seq := one.meta[ingest.MetaSeq]
	// Through the sidecar's admin API, with the sidecar up and taking
	// records: no ingest gap.
	adminAddr, http2 := listenPort(t), listenPort(t)
	ctx2, cancel2 := context.WithCancel(context.Background())
	done2 := make(chan result, 1)
	go func() {
		done2 <- h.runCtx(ctx2, "", append([]string{"ingest", "-stream", stream, "-confidential-stream", conf, "-content-dir", content,
			"-http", http2, "-admin", adminAddr, "-loki", lokiS.URL, "-max-wait", "20ms", "-max-rate", "50"}, fast...)...)
	}()
	waitUntil(t, "the sidecar again", func() bool {
		resp, err := http.Get("http://" + http2 + "/healthz")
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	})
	resp, err := http.Post("http://"+adminAddr+"/admin/retract?stream="+stream+"&seqs="+seq, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	ansRaw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted || !strings.Contains(string(ansRaw), `"state":"submitted"`) {
		t.Fatalf("retract: %d %s", resp.StatusCode, ansRaw)
	}
	// Records keep flowing while the sweep is in flight.
	post(t, "http://"+http2+"/loki/api/v1/push", "application/json",
		[]byte(`{"streams":[{"stream":{"service_name":"web"},"values":[["1791400000000000009","during the retraction"]]}]}`), http.StatusNoContent)
	waitUntil(t, "the receipts", func() bool {
		for i, d := range dirs {
			r := h.run("", "archive", stream, producer, "-dir", d, "-host", hosts[i], "-recheck-all")
			if !strings.Contains(r.stdout, "retracted seqs "+seq+": receipt and sweep kept") {
				got, _ := filepath.Glob(filepath.Join(d, stream, producer, "retracted", "*.json"))
				if len(got) == 0 {
					return false
				}
			}
		}
		return true
	})
	cancel2()
	if r := <-done2; r.code != 0 {
		t.Fatalf("ingest exit %d\n%s\n%s", r.code, r.stdout, r.stderr)
	}
	retracted := 0
	for _, l := range queryGateway(t, gwS.URL, false) {
		if l.reason == gateway.ReasonRetracted {
			retracted++
			if l.labels[ingest.MetaSeq] != seq {
				t.Fatalf("a line of seq %s marked retracted", l.labels[ingest.MetaSeq])
			}
		}
	}
	if retracted == 0 {
		t.Fatal("no line marked retracted")
	}
	resp, err = http.Get(gwS.URL + "/blogs/v1/coverage")
	if err != nil {
		t.Fatal(err)
	}
	var cov []gateway.Coverage
	_ = json.NewDecoder(resp.Body).Decode(&cov)
	resp.Body.Close()
	found := false
	for _, c := range cov {
		for _, rc := range c.Retracted {
			if c.Stream == stream && fmt.Sprint(rc.Seq) == seq && rc.Sweep != "" {
				found = true
			}
		}
	}
	if !found {
		t.Fatalf("coverage shows no receipt for seq %s: %+v", seq, cov)
	}
	// The confidential stream's archive holds no line on its own: the
	// record carries a locator, and its content came from the content dir.
	recs, _ := filepath.Glob(filepath.Join(dirs[0], conf, producer, "batches", "*.json"))
	for _, n := range recs {
		raw, _ := os.ReadFile(n)
		var bf archive.BatchFile
		_ = json.Unmarshal(raw, &bf)
		if bf.Profile != 2 {
			t.Fatalf("the confidential batch %s has profile %d", n, bf.Profile)
		}
	}
	if len(recs) == 0 {
		t.Fatal("archive a holds no confidential batch")
	}
	for _, d := range dirs[1:] {
		if got, _ := filepath.Glob(filepath.Join(d, conf, producer, "content", "*")); len(got) > 0 {
			t.Fatalf("an archive without the content holds confidential lines: %v", got)
		}
	}
}
