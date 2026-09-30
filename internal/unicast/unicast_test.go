package unicast

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/transaction"
)

// host is a stand-in submit route: it answers each POST with a STEAK
// admitting output 0, or nothing once it holds the body, or fails the
// first fail requests with 503.
type host struct {
	*httptest.Server
	mu    sync.Mutex
	held  map[string]bool
	fail  int
	calls atomic.Int32
	topic atomic.Value
}

func newHost(t *testing.T, fail int) *host {
	h := &host{held: map[string]bool{}, fail: fail}
	h.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.calls.Add(1)
		h.topic.Store(r.Header.Get("x-topics"))
		b, _ := io.ReadAll(r.Body)
		h.mu.Lock()
		defer h.mu.Unlock()
		if r.URL.Path != "/submit" {
			http.NotFound(w, r)
			return
		}
		if h.fail != 0 {
			if h.fail > 0 {
				h.fail--
			}
			http.Error(w, "down", http.StatusServiceUnavailable)
			return
		}
		admit := "[0]"
		if h.held[string(b)] {
			admit = "[]"
		}
		h.held[string(b)] = true
		fmt.Fprintf(w, `{%q:{"outputsToAdmit":%s,"coinsToRetain":[]}}`, r.Header.Get("x-topics"), admit)
	}))
	t.Cleanup(h.Close)
	return h
}

func beef(n byte) []byte {
	b := binary.LittleEndian.AppendUint32(nil, transaction.ATOMIC_BEEF)
	return append(b, n)
}

func set(need int, hs ...*host) (*Set, *[]string) {
	var notes []string
	var mu sync.Mutex
	s := &Set{Need: need, Retries: []time.Duration{time.Millisecond, time.Millisecond}, Note: func(f string, a ...any) {
		mu.Lock()
		notes = append(notes, fmt.Sprintf(f, a...))
		mu.Unlock()
	}}
	for _, h := range hs {
		s.Hosts = append(s.Hosts, h.URL)
	}
	return s, &notes
}

func TestEveryHostIsSentTheObject(t *testing.T) {
	a, b := newHost(t, 0), newHost(t, 0)
	s, notes := set(2, a, b)
	o := s.Send(context.Background(), "seq 1", "tm_log_app_abcdefghij", beef(1), 0)
	if !o.OK() || o.Took() != 2 || len(*notes) != 0 {
		t.Fatalf("took %d, notes %v", o.Took(), *notes)
	}
	for _, h := range []*host{a, b} {
		if h.calls.Load() != 1 || h.topic.Load() != "tm_log_app_abcdefghij" {
			t.Fatalf("host %s: %d calls, topic %v", h.URL, h.calls.Load(), h.topic.Load())
		}
	}
	if m := o.Merged(); len(m.Admitted) != 1 || m.Duplicate {
		t.Fatalf("merged %+v", m)
	}
	// The same object again: every host answers, none admits.
	o = s.Send(context.Background(), "seq 1", "tm_log_app_abcdefghij", beef(1), 0)
	if !o.OK() || !o.Merged().Duplicate {
		t.Fatalf("resend: %+v", o)
	}
}

func TestEachHostIsRetriedOnItsOwn(t *testing.T) {
	a, b := newHost(t, 2), newHost(t, 0)
	s, notes := set(2, a, b)
	o := s.Send(context.Background(), "seq 1", "tm_x", beef(1), 0)
	if !o.OK() || o.Results[0].Tries != 3 || o.Results[1].Tries != 1 || len(*notes) != 0 {
		t.Fatalf("%+v %v", o, *notes)
	}
}

func TestTheQuorumDecides(t *testing.T) {
	up, down := newHost(t, 0), newHost(t, -1)
	for _, tc := range []struct {
		need int
		ok   bool
	}{{1, true}, {2, false}} {
		s, notes := set(tc.need, up, down)
		o := s.Send(context.Background(), "seq 4", "tm_x", beef(byte(tc.need)), 0)
		if o.OK() != tc.ok || o.Took() != 1 {
			t.Fatalf("need %d: ok %v took %d", tc.need, o.OK(), o.Took())
		}
		if o.Results[1].Tries != 3 || o.Results[1].Took() {
			t.Fatalf("need %d: the down host: %+v", tc.need, o.Results[1])
		}
		if len(*notes) != 1 || !strings.Contains((*notes)[0], "seq 4: host "+down.URL+" MISSED it after 3 tries") {
			t.Fatalf("need %d: notes %v", tc.need, *notes)
		}
		err := o.Err()
		if (err == nil) != tc.ok || (err != nil && !strings.Contains(err.Error(), "1 of 2 host(s) took it and the quorum is 2")) {
			t.Fatalf("need %d: err %v", tc.need, err)
		}
		tl := s.Tallies()
		if tl[0].Took != 1 || tl[0].Missed != 0 || tl[1].Missed != 1 || tl[1].MissedWhat[0] != "seq 4" {
			t.Fatalf("need %d: tallies %+v", tc.need, tl)
		}
	}
	// A per-object quorum overrides the Set's.
	s, _ := set(1, up, down)
	if o := s.Send(context.Background(), "sweep", "tm_x", beef(9), len(s.Hosts)); o.OK() {
		t.Fatal("a sweep that must reach every host counted as published")
	}
}

func TestSubmitAndTheFacadeAdapter(t *testing.T) {
	a, b := newHost(t, 0), newHost(t, 0)
	s, _ := set(2, a, b)
	res, err := s.Submit(context.Background(), "tm_x", beef(1))
	if err != nil || len(res.Admitted) != 1 {
		t.Fatalf("%+v %v", res, err)
	}
	// The adapter: one facade, every host sent the object.
	f := s.Facade("funding tree")
	res, err = f.Submit(context.Background(), "tm_x", beef(2))
	if err != nil || len(res.Admitted) != 1 || a.calls.Load() != 2 || b.calls.Load() != 2 {
		t.Fatalf("adapter: %+v %v, calls %d %d", res, err, a.calls.Load(), b.calls.Load())
	}
	res, err = f.Submit(context.Background(), "tm_x", beef(2))
	if err != nil || !res.Duplicate {
		t.Fatalf("adapter, again: %+v %v", res, err)
	}
	// Too few hosts: the facade's error names them.
	down := newHost(t, -1)
	s2, _ := set(2, a, down)
	if _, err := s2.Facade("funding tree").Submit(context.Background(), "tm_x", beef(3)); err == nil ||
		!strings.Contains(err.Error(), "502") || !strings.Contains(err.Error(), down.URL) {
		t.Fatalf("adapter below quorum: %v", err)
	}
	if tl := s2.Tallies(); tl[1].MissedWhat[0] != "funding tree" {
		t.Fatalf("tallies %+v", tl)
	}
}

func TestACancelledSendStops(t *testing.T) {
	down := newHost(t, -1)
	s, _ := set(1, down)
	s.Retries = []time.Duration{time.Hour}
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(20 * time.Millisecond); cancel() }()
	start := time.Now()
	if o := s.Send(ctx, "seq 1", "tm_x", beef(1), 0); o.OK() || time.Since(start) > 5*time.Second {
		t.Fatalf("%+v after %s", o, time.Since(start))
	}
}
