// Package unicast publishes a producer's objects to overlay hosts one by
// one, for a deployment with no multicast plane.
//
// On the plane a producer submits each object once, to one facade, and the
// plane delivers it to every host subscribed to the topic and repairs what
// is lost on the way. Without the plane the producer does that itself: a
// Set submits every object to each of its hosts individually, retries each
// host on its own, and reports each host's answer. The object counts as
// published when at least Need hosts took it. A host that missed it does
// not receive it from the others; blogs backfill copies it across later.
//
// "Took it" means the host answered the submit with 200 and a STEAK for the
// topic. A host that answers with nothing admitted has decided (it holds the
// object already, or its topic manager refused it); resending cannot change
// that, so it is reported and counted as an answer, never retried.
package unicast

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/lightwebinc/bcommon/publish"
)

// Set is the hosts a producer submits to.
type Set struct {
	// Hosts are the hosts' base URLs; each is POSTed <base>/submit.
	Hosts []string
	// Need is how many hosts must take an object for Submit to succeed.
	Need int
	// Retries are the waits between the tries of one host: a host is tried
	// 1+len(Retries) times before it is counted as missed.
	Retries []time.Duration
	// HTTP is the client each host's facade uses; nil is bcommon's default.
	HTTP *http.Client
	// Note receives a line per host that did not take an object, and with
	// Verbose a line per host that did.
	Note    func(format string, args ...any)
	Verbose bool

	mu    sync.Mutex
	tally map[string]*Tally
}

// Result is one host's answer to one submit.
type Result struct {
	Host string
	// Result is the host's STEAK entry when Err is nil.
	Result publish.Result
	// Err is the last failure when every try failed.
	Err error
	// Tries is how many times the host was asked.
	Tries int
}

// Took reports whether the host answered.
func (r Result) Took() bool { return r.Err == nil }

// Outcome is every host's answer to one submit.
type Outcome struct {
	What    string
	Results []Result
	Need    int
}

// Took is how many hosts answered.
func (o Outcome) Took() int {
	n := 0
	for _, r := range o.Results {
		if r.Took() {
			n++
		}
	}
	return n
}

// OK reports whether enough hosts answered.
func (o Outcome) OK() bool { return o.Took() >= o.Need }

// Err is nil when the outcome is OK, and otherwise names every host that
// missed the object and why.
func (o Outcome) Err() error {
	if o.OK() {
		return nil
	}
	var why []string
	for _, r := range o.Results {
		if !r.Took() {
			why = append(why, fmt.Sprintf("%s: %v", r.Host, r.Err))
		}
	}
	return fmt.Errorf("%d of %d host(s) took it and the quorum is %d (%s)", o.Took(), len(o.Results), o.Need, strings.Join(why, "; "))
}

// Merged is one publish.Result for callers that expect a single host's
// answer: the first host that admitted something, else the first that
// answered. Duplicate is then true only when no host admitted anything.
func (o Outcome) Merged() publish.Result {
	var first *publish.Result
	for i := range o.Results {
		r := &o.Results[i]
		if !r.Took() {
			continue
		}
		if len(r.Result.Admitted) > 0 {
			return r.Result
		}
		if first == nil {
			first = &r.Result
		}
	}
	if first == nil {
		return publish.Result{}
	}
	return *first
}

// Tally is what one host was sent in this process's lifetime.
type Tally struct {
	Host string
	// Took and Missed count the objects the host answered and did not.
	Took, Missed int
	// MissedWhat names the missed objects, in order, as Send was told.
	MissedWhat []string
}

// Send submits beef for topic to every host at once, each retried on its
// own, and returns every host's answer. what names the object in notes.
// need is the quorum for this object; zero means s.Need.
func (s *Set) Send(ctx context.Context, what, topic string, beef []byte, need int) Outcome {
	if need == 0 {
		need = s.Need
	}
	o := Outcome{What: what, Results: make([]Result, len(s.Hosts)), Need: need}
	var wg sync.WaitGroup
	for i, h := range s.Hosts {
		wg.Add(1)
		go func() {
			defer wg.Done()
			o.Results[i] = s.one(ctx, h, topic, beef)
		}()
	}
	wg.Wait()
	s.record(o)
	return o
}

// Submit is Send with the Set's quorum, as one result: the Merged answer,
// or an error naming the hosts that missed the object when too few took
// it. It is the publish.Facade Submit signature.
func (s *Set) Submit(ctx context.Context, topic string, beef []byte) (publish.Result, error) {
	o := s.Send(ctx, "object", topic, beef, 0)
	if err := o.Err(); err != nil {
		return publish.Result{}, err
	}
	return o.Merged(), nil
}

func (s *Set) one(ctx context.Context, host, topic string, beef []byte) Result {
	f := &publish.Facade{Base: host, HTTP: s.HTTP}
	r := Result{Host: host}
	for {
		r.Tries++
		res, err := f.Submit(ctx, topic, beef)
		if err == nil {
			r.Result, r.Err = res, nil
			return r
		}
		r.Err = err
		if r.Tries > len(s.Retries) || ctx.Err() != nil {
			return r
		}
		select {
		case <-ctx.Done():
			r.Err = ctx.Err()
			return r
		case <-time.After(s.Retries[r.Tries-1]):
		}
	}
}

func (s *Set) note(format string, args ...any) {
	if s.Note != nil {
		s.Note(format, args...)
	}
}

func (s *Set) record(o Outcome) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.tally == nil {
		s.tally = map[string]*Tally{}
	}
	for _, r := range o.Results {
		t := s.tally[r.Host]
		if t == nil {
			t = &Tally{Host: r.Host}
			s.tally[r.Host] = t
		}
		switch {
		case !r.Took():
			t.Missed++
			t.MissedWhat = append(t.MissedWhat, o.What)
			s.note("%s: host %s MISSED it after %d tries: %v", o.What, r.Host, r.Tries, r.Err)
		case s.Verbose && len(r.Result.Admitted) > 0:
			t.Took++
			s.note("%s: host %s admitted %d output(s)", o.What, r.Host, len(r.Result.Admitted))
		case s.Verbose:
			t.Took++
			s.note("%s: host %s admitted nothing: it holds it already, or refused it", o.What, r.Host)
		default:
			t.Took++
		}
	}
}

// Tallies are the per-host counts so far, in Hosts order.
func (s *Set) Tallies() []Tally {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Tally, 0, len(s.Hosts))
	for _, h := range s.Hosts {
		if t := s.tally[h]; t != nil {
			c := *t
			c.MissedWhat = append([]string(nil), t.MissedWhat...)
			out = append(out, c)
		} else {
			out = append(out, Tally{Host: h})
		}
	}
	return out
}

// Facade is a publish.Facade whose every Submit is a Send to the whole Set
// with the Set's quorum, for code that takes one facade (bcommon's
// producer.Trees publishes each funding tree it adopts through one). Its
// base URL is never dialled: its transport answers every request itself.
// what names the objects it carries in notes.
func (s *Set) Facade(what string) *publish.Facade {
	return &publish.Facade{Base: "http://unicast.invalid", HTTP: &http.Client{Transport: fanout{s, what}}}
}

// fanout is the round tripper behind Facade.
type fanout struct {
	s    *Set
	what string
}

func (f fanout) RoundTrip(req *http.Request) (*http.Response, error) {
	body, err := io.ReadAll(req.Body)
	_ = req.Body.Close()
	if err != nil {
		return nil, err
	}
	topic := req.Header.Get("x-topics")
	o := f.s.Send(req.Context(), f.what, topic, body, 0)
	if err := o.Err(); err != nil {
		return respond(req, http.StatusBadGateway, []byte("unicast: "+err.Error())), nil
	}
	m := o.Merged()
	if len(m.Raw) > 0 {
		return respond(req, http.StatusOK, m.Raw), nil
	}
	// Every host answered, none with a body worth passing on: build the
	// bare STEAK the facade reads.
	raw, _ := json.Marshal(map[string]any{topic: map[string]any{"outputsToAdmit": m.Admitted, "coinsToRetain": m.Retained}})
	return respond(req, http.StatusOK, raw), nil
}

func respond(req *http.Request, code int, body []byte) *http.Response {
	return &http.Response{
		StatusCode: code, Status: fmt.Sprintf("%d %s", code, http.StatusText(code)),
		Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1,
		Header:        http.Header{"Content-Type": {"application/json"}},
		Body:          io.NopCloser(bytes.NewReader(body)),
		ContentLength: int64(len(body)),
		Request:       req,
	}
}
