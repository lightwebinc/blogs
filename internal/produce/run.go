package produce

import (
	"bufio"
	"context"
	"errors"
	"io"
	"os"
	"syscall"
	"time"

	"github.com/lightwebinc/blogs/internal/state"
)

// MaxLine is the longest line a batch carries, LF included; a longer input
// line is carried as several lines.
const MaxLine = 64 << 10

// Line is one input line, LF-terminated, and for a followed file the
// file's position just after it.
type Line struct {
	B    []byte
	Tail *state.Tail
	// Split is set on every part of an input line longer than MaxLine.
	Split bool
}

// Cut is when a batch is closed: at MaxBytes bytes or MaxLines lines,
// whichever comes first, or MaxWait after its first line.
type Cut struct {
	MaxBytes int
	MaxLines int
	MaxWait  time.Duration
}

// Run reads lines until src closes or ctx ends, cuts them into batches and
// publishes each, and ticks the anchor chain every Poll. When src closes,
// the last batch is published. When ctx ends, the batch being built is
// published under a context of its own, bounded by grace, and Run returns
// ctx's error. A batch of a followed file records the position its last
// line ends at.
func (e *Engine) Run(ctx context.Context, src <-chan Line, srcErr <-chan error, cut Cut, grace time.Duration) error {
	var raw []byte
	var count int
	var tail *state.Tail
	var timer <-chan time.Time
	split := false
	flush := func(ctx context.Context) error {
		if count == 0 {
			return nil
		}
		b := Batch{Raw: raw, Count: count, Tail: tail}
		if split {
			e.note("note: a line longer than %d bytes was carried as several lines", MaxLine)
		}
		raw, count, timer, split = nil, 0, nil, false
		return e.Publish(ctx, b)
	}
	tick := time.NewTicker(e.Opts.Poll)
	defer tick.Stop()
	for {
		select {
		case l, ok := <-src:
			if !ok {
				if err := flush(ctx); err != nil {
					return err
				}
				select {
				case err := <-srcErr:
					return err
				default:
					return nil
				}
			}
			if count > 0 && len(raw)+len(l.B) > cut.MaxBytes {
				if err := flush(ctx); err != nil {
					return err
				}
			}
			raw = append(raw, l.B...)
			count++
			tail = l.Tail
			split = split || l.Split
			if count == 1 {
				timer = time.After(cut.MaxWait)
			}
			if count >= cut.MaxLines || len(raw) >= cut.MaxBytes {
				if err := flush(ctx); err != nil {
					return err
				}
			}
		case <-timer:
			if err := flush(ctx); err != nil {
				return err
			}
		case <-tick.C:
			if err := e.Tick(ctx, false); err != nil {
				if ctx.Err() != nil {
					continue
				}
				return err
			}
		case <-ctx.Done():
			fctx, cancel := context.WithTimeout(context.Background(), grace)
			err := flush(fctx)
			cancel()
			if err != nil {
				return err
			}
			return ctx.Err()
		}
	}
}

// ReadLines sends r's lines on the returned channel, each LF-terminated
// (a last line without one gets one), parts of a line longer than MaxLine
// as lines of their own, and closes it at EOF. A read error is sent on the
// error channel before the close.
func ReadLines(r io.Reader) (<-chan Line, <-chan error) {
	out, errc := make(chan Line, 1024), make(chan error, 1)
	go func() {
		defer close(out)
		br := bufio.NewReaderSize(r, MaxLine)
		cont := false
		for {
			frag, err := br.ReadSlice('\n')
			if len(frag) > 0 {
				b := append([]byte(nil), frag...)
				full := b[len(b)-1] == '\n'
				if !full {
					b = append(b, '\n')
				}
				out <- Line{B: b, Split: cont || (!full && errors.Is(err, bufio.ErrBufferFull))}
				cont = !full && errors.Is(err, bufio.ErrBufferFull)
			}
			if err == nil || errors.Is(err, bufio.ErrBufferFull) {
				continue
			}
			if !errors.Is(err, io.EOF) {
				errc <- err
			}
			return
		}
	}()
	return out, errc
}

// FileID is a file's device and inode.
func FileID(fi os.FileInfo) (dev, ino uint64) {
	if st, ok := fi.Sys().(*syscall.Stat_t); ok {
		return uint64(st.Dev), st.Ino //nolint:unconvert // Dev differs in type across platforms
	}
	return 0, 0
}

// Follow reads path from offset, each line carrying the file's position
// after it. Without follow it stops at the end of the file, a partial last
// line given an LF. With follow it tails the file: every complete line as it
// is appended, waiting poll between reads at the end; a file that shrinks
// below the offset (truncated) is read again from the start, and a path that
// names a new file (rotated) is switched to, from the new file's start, once
// the old one is read to its end. A partial line waits for its LF. The
// channel closes at the end, or when ctx ends.
func Follow(ctx context.Context, path string, offset int64, poll time.Duration, follow bool, note func(string, ...any)) (<-chan Line, <-chan error, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	dev, ino := FileID(fi)
	if offset > fi.Size() {
		note("note: %s is shorter than the offset read to before; reading it from the start", path)
		offset = 0
	}
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		f.Close()
		return nil, nil, err
	}
	out, errc := make(chan Line, 1024), make(chan error, 1)
	go func() {
		defer close(out)
		defer func() { f.Close() }()
		var pending []byte
		off := offset
		buf := make([]byte, 64<<10)
		for {
			n, rerr := f.Read(buf)
			if n > 0 {
				pending = append(pending, buf[:n]...)
				for {
					i := indexLF(pending)
					if i < 0 && len(pending) < MaxLine {
						break
					}
					cut := i + 1
					b := pending
					isSplit := false
					if i < 0 || cut > MaxLine {
						cut, isSplit = MaxLine-1, true
						b = append(append([]byte(nil), pending[:cut]...), '\n')
					} else {
						b = append([]byte(nil), pending[:cut]...)
					}
					off += int64(cut)
					pending = pending[cut:]
					select {
					case out <- Line{B: b, Split: isSplit, Tail: &state.Tail{Path: path, Dev: dev, Ino: ino, Offset: off}}:
					case <-ctx.Done():
						return
					}
				}
				continue
			}
			if rerr != nil && !errors.Is(rerr, io.EOF) {
				errc <- rerr
				return
			}
			if !follow {
				if len(pending) > 0 {
					off += int64(len(pending))
					b := append(append([]byte(nil), pending...), '\n')
					select {
					case out <- Line{B: b, Tail: &state.Tail{Path: path, Dev: dev, Ino: ino, Offset: off}}:
					case <-ctx.Done():
					}
				}
				return
			}
			// At the end: truncated, rotated, or just waiting.
			if fi, err := os.Stat(path); err == nil {
				if d, i := FileID(fi); d != dev || i != ino {
					nf, err := os.Open(path)
					if err == nil {
						note("note: %s was replaced; following the new file from its start", path)
						f.Close()
						f, dev, ino, off, pending = nf, d, i, 0, nil
						continue
					}
				} else if fi.Size() < off {
					note("note: %s was truncated; reading it from the start", path)
					if _, err := f.Seek(0, io.SeekStart); err == nil {
						off, pending = 0, nil
					}
				}
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(poll):
			}
		}
	}()
	return out, errc, nil
}

func indexLF(b []byte) int {
	for i, c := range b {
		if c == '\n' {
			return i
		}
	}
	return -1
}
