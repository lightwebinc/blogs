package ingest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/lightwebinc/bcommon/termsafe"

	"github.com/lightwebinc/blogs/internal/metrics"
	"github.com/lightwebinc/blogs/internal/produce"
)

// Engine is a producer on one stream, as the batcher drives it.
type Engine interface {
	Stream() string
	Producer() string
	// Publish publishes lines (each LF-terminated) as one batch, or as
	// several when its encoding is over the record's bound, and returns
	// each batch published, in order.
	Publish(ctx context.Context, raw []byte, count int) ([]Published, error)
	// Tick anchors when the interval is due and collects an anchor in
	// flight.
	Tick(ctx context.Context) error
}

// Published is one batch published.
type Published struct {
	Seq     uint64
	Carrier string
	Raw     []byte
}

// produceEngine adapts the producer engine.
type produceEngine struct {
	e   *produce.Engine
	got []Published
}

// FromEngine adapts a producer engine. It takes the engine's Published
// hook.
func FromEngine(e *produce.Engine) Engine {
	p := &produceEngine{e: e}
	e.Published = func(seq uint64, carrier string, raw []byte) {
		p.got = append(p.got, Published{Seq: seq, Carrier: carrier, Raw: raw})
	}
	return p
}

func (p *produceEngine) Stream() string   { return p.e.St.Stream }
func (p *produceEngine) Producer() string { return p.e.St.Identity }

func (p *produceEngine) Publish(ctx context.Context, raw []byte, count int) ([]Published, error) {
	p.got = nil
	err := p.e.Publish(ctx, produce.Batch{Raw: raw, Count: count})
	got := p.got
	p.got = nil
	return got, err
}

// Tick also moves a sweep in flight (a retraction) along, waiting a few
// seconds at most each time, so it never holds the batches up: a sweep
// not mined yet stays persisted for the next tick.
func (p *produceEngine) Tick(ctx context.Context) error {
	if p.e.InFlight() {
		sctx, cancel := context.WithTimeout(ctx, SweepStep)
		err := p.e.FinishSweeps(sctx)
		cancel()
		if err != nil && sctx.Err() == nil && ctx.Err() == nil {
			return fmt.Errorf("sweep: %w", err)
		}
	}
	return p.e.Tick(ctx, false)
}

// SweepStep bounds how long one tick waits on a sweep in flight.
var SweepStep = 3 * time.Second

// Prime makes sure every lane's stream has a batch funding tree to spend,
// minting and waiting for one when it has none, so that the first batch
// after a start does not wait for a block. A lane whose engine is not a
// producer engine is left as it is.
func (b *Batcher) Prime(ctx context.Context) error {
	for _, l := range b.Lanes {
		if p, ok := l.Engine.(*produceEngine); ok {
			if err := p.e.Prime(ctx); err != nil {
				return fmt.Errorf("stream %s: %w", p.Stream(), err)
			}
		}
	}
	return nil
}

// Placed is a record as it was published: the line, its stream and
// producer, and its batch's seq and its 1-based position in the batch.
type Placed struct {
	Rec      *Record
	Line     []byte
	Stream   string
	Producer string
	Seq      uint64
	Index    int
}

// Lane is one stream the batcher feeds, and when its batches are cut.
type Lane struct {
	Engine Engine
	Cut    produce.Cut
}

// Batcher owns the lanes' engines: one goroutine publishes and anchors
// for all of them, so engines sharing a home's coin pool never run at
// once.
type Batcher struct {
	Lanes []*Lane
	// Route picks a record's lane.
	Route func(*Record) int
	// Forward receives every record published, after its batch is.
	Forward func([]Placed)
	// Poll is how often anchors are ticked.
	Poll    time.Duration
	Metrics *metrics.Set
	Note    func(format string, args ...any)
	// Queue bounds the submissions waiting for the publishing goroutine.
	Queue int

	in    chan *job
	tasks chan task
	state []*laneState
}

type task struct {
	fn   func(context.Context) error
	done chan error
}

type job struct {
	items     []item
	remaining int
	err       error
	done      chan error
}

type item struct {
	lane int
	rec  *Record
	line []byte
	job  *job
}

type laneState struct {
	items []item
	bytes int
	first time.Time
	last  time.Time
}

// ErrBusy is a submission refused because the queue is full.
var ErrBusy = errors.New("ingest: busy")

// Declare declares the batcher's metrics.
func Declare(m *metrics.Set) {
	m.Counter("blogs_ingest_lines_total", "Lines published, by stream.")
	m.Counter("blogs_ingest_bytes_total", "Bytes of lines published, newline included, by stream: the metered ingest volume.")
	m.Counter("blogs_ingest_batches_total", "Batches published, by stream.")
	m.Counter("blogs_ingest_publish_errors_total", "Publishes that failed, by stream; their lines were refused to the sender.")
	m.Counter("blogs_ingest_anchor_errors_total", "Anchor ticks that failed, by stream; the next tick tries again.")
	m.Counter("blogs_ingest_unsafe_lines_total", "Lines a terminal must be protected from, published and counted, by stream.")
	m.Gauge("blogs_ingest_queue", "Submissions waiting for the publishing goroutine.")
}

func (b *Batcher) note(format string, args ...any) {
	if b.Note != nil {
		b.Note(format, args...)
	}
}

func (b *Batcher) init() {
	if b.in == nil {
		q := b.Queue
		if q <= 0 {
			q = 1024
		}
		b.in = make(chan *job, q)
		b.tasks = make(chan task)
		b.state = make([]*laneState, len(b.Lanes))
		for i := range b.state {
			b.state[i] = &laneState{}
		}
	}
}

// Start prepares the batcher; call it before Submit and Run.
func (b *Batcher) Start() { b.init() }

// Submit puts records on their lanes. With wait it returns once every
// record's batch is published, or with the error that stopped it; without,
// once the records are queued. A record that does not encode refuses the
// whole submission before anything is queued.
func (b *Batcher) Submit(ctx context.Context, recs []Record, wait bool) error {
	if len(recs) == 0 {
		return nil
	}
	j := &job{done: make(chan error, 1)}
	for i := range recs {
		line, err := recs[i].Line()
		if err != nil {
			return err
		}
		lane := 0
		if b.Route != nil {
			lane = b.Route(&recs[i])
		}
		j.items = append(j.items, item{lane: lane, rec: &recs[i], line: line, job: j})
	}
	j.remaining = len(j.items)
	select {
	case b.in <- j:
	case <-ctx.Done():
		return ctx.Err()
	default:
		if !wait {
			// A sender that cannot be told no waits for room.
			select {
			case b.in <- j:
			case <-ctx.Done():
				return ctx.Err()
			}
		} else {
			return ErrBusy
		}
	}
	if !wait {
		return nil
	}
	select {
	case err := <-j.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (j *job) resolve(n int, err error) {
	if err != nil && j.err == nil {
		j.err = err
	}
	j.remaining -= n
	if j.remaining == 0 {
		j.done <- j.err
	}
}

// Do runs fn on the publishing goroutine, between batches, and returns its
// error: how an operation on an engine (a retraction) runs while the
// sidecar keeps taking records.
func (b *Batcher) Do(ctx context.Context, fn func(context.Context) error) error {
	t := task{fn: fn, done: make(chan error, 1)}
	select {
	case b.tasks <- t:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-t.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Run publishes and anchors until ctx ends, then publishes what is queued
// under a context of its own bounded by grace.
func (b *Batcher) Run(ctx context.Context, grace time.Duration) error {
	b.init()
	poll := b.Poll
	if poll <= 0 {
		poll = 5 * time.Second
	}
	anchor := time.NewTicker(poll)
	defer anchor.Stop()
	wake := time.NewTicker(10 * time.Millisecond)
	defer wake.Stop()
	for {
		select {
		case j := <-b.in:
			b.take(ctx, j)
		case t := <-b.tasks:
			t.done <- t.fn(ctx)
		case <-wake.C:
			if b.Metrics != nil {
				b.Metrics.Set("blogs_ingest_queue", float64(len(b.in)))
			}
			for i := range b.Lanes {
				st := b.state[i]
				if len(st.items) > 0 && time.Since(st.first) >= b.Lanes[i].Cut.MaxWait {
					b.flush(ctx, i)
				}
			}
		case <-anchor.C:
			for _, l := range b.Lanes {
				if err := l.Engine.Tick(ctx); err != nil && ctx.Err() == nil {
					b.note("stream %s: anchor: %v", l.Engine.Stream(), err)
					b.count("blogs_ingest_anchor_errors_total", 1, l.Engine.Stream())
				}
			}
		case <-ctx.Done():
			gctx, cancel := context.WithTimeout(context.Background(), grace)
			defer cancel()
			for {
				select {
				case j := <-b.in:
					b.take(gctx, j)
					continue
				default:
				}
				break
			}
			for i := range b.Lanes {
				if len(b.state[i].items) > 0 {
					b.flush(gctx, i)
				}
			}
			return ctx.Err()
		}
	}
}

func (b *Batcher) count(name string, v float64, stream string) {
	if b.Metrics != nil {
		b.Metrics.Add(name, v, "stream", stream)
	}
}

func (b *Batcher) take(ctx context.Context, j *job) {
	for _, it := range j.items {
		if it.lane < 0 || it.lane >= len(b.Lanes) {
			j.resolve(1, fmt.Errorf("ingest: no lane %d", it.lane))
			continue
		}
		st := b.state[it.lane]
		if len(st.items) == 0 {
			st.first = time.Now()
		}
		st.items = append(st.items, it)
		st.bytes += len(it.line) + 1
		cut := b.Lanes[it.lane].Cut
		if len(st.items) >= cut.MaxLines || st.bytes >= cut.MaxBytes {
			b.flush(ctx, it.lane)
		}
	}
}

// flush publishes a lane's items in batches no larger than its cut.
func (b *Batcher) flush(ctx context.Context, lane int) {
	st := b.state[lane]
	l := b.Lanes[lane]
	items := st.items
	st.items, st.bytes = nil, 0
	stream := l.Engine.Stream()
	var failed error
	for len(items) > 0 {
		n, size := 0, 0
		for n < len(items) && n < l.Cut.MaxLines && (n == 0 || size+len(items[n].line)+1 <= l.Cut.MaxBytes) {
			size += len(items[n].line) + 1
			n++
		}
		chunk := items[:n]
		items = items[n:]
		if failed != nil {
			for _, it := range chunk {
				it.job.resolve(1, failed)
			}
			continue
		}
		if d := l.Cut.MinGap - time.Since(st.last); l.Cut.MinGap > 0 && !st.last.IsZero() && d > 0 {
			select {
			case <-ctx.Done():
			case <-time.After(d):
			}
		}
		st.last = time.Now()
		raw := make([]byte, 0, size)
		for _, it := range chunk {
			raw = append(append(raw, it.line...), '\n')
			if termsafe.Validate("line", string(it.line)) != nil {
				b.count("blogs_ingest_unsafe_lines_total", 1, stream)
			}
		}
		pubs, err := l.Engine.Publish(ctx, raw, len(chunk))
		placed, perr := place(chunk, pubs, stream, l.Engine.Producer())
		if err == nil {
			err = perr
		}
		if len(placed) > 0 && b.Forward != nil {
			b.Forward(placed)
		}
		if err != nil {
			failed = fmt.Errorf("stream %s: publish: %w", stream, err)
			b.note("%v", failed)
			b.count("blogs_ingest_publish_errors_total", 1, stream)
		} else {
			b.count("blogs_ingest_lines_total", float64(len(chunk)), stream)
			b.count("blogs_ingest_bytes_total", float64(len(raw)), stream)
			b.count("blogs_ingest_batches_total", float64(len(pubs)), stream)
		}
		for _, it := range chunk {
			it.job.resolve(1, failed)
		}
	}
}

// place gives each item of a chunk its seq and position from the batches
// it was published as, in order.
func place(chunk []item, pubs []Published, stream, producer string) ([]Placed, error) {
	var out []Placed
	i := 0
	for _, p := range pubs {
		n := bytes.Count(p.Raw, []byte{'\n'})
		for k := 1; k <= n; k++ {
			if i >= len(chunk) {
				return out, errors.New("ingest: a batch published more lines than were given")
			}
			out = append(out, Placed{Rec: chunk[i].rec, Line: chunk[i].line, Stream: stream, Producer: producer, Seq: p.Seq, Index: k})
			i++
		}
	}
	if len(pubs) > 0 && i != len(chunk) {
		return out, fmt.Errorf("ingest: %d lines given, %d published", len(chunk), i)
	}
	return out, nil
}
