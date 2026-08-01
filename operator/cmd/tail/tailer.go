package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"regexp"
	"strconv"
	"time"
)

const (
	maxLineBytes    = 1 << 20 // 1MiB; nothing legitimate in EE.log comes close
	initialBufBytes = 64 << 10
	pollInterval    = 500 * time.Millisecond
)

var errLineTooLong = errors.New("log line too long")

type Tailer struct {
	path string
	sink Sink

	f      *os.File
	ino    uint64
	offset int64  // bytes consumed from the fd
	buf    []byte // unparsed bytes, len = valid, cap = room
	start  int    // prefix already emitted

	sessionID    string
	seq          uint64
	epoch        *time.Time // session start, from the header's UTC stamp
	lastGameTime *float64   // for continuation lines, which carry no timestamp
	head         []byte     // first bytes of the file; changes on relaunch
}

func NewTailer(path string, sink Sink) (*Tailer, error) {
	t := &Tailer{
		path: path,
		sink: sink,
		buf:  make([]byte, 0, initialBufBytes),
	}
	if err := t.open(); err != nil {
		return nil, err
	}
	return t, nil
}

func (t *Tailer) open() error {
	f, err := os.Open(t.path)
	if err != nil {
		return fmt.Errorf("open %s: %w", t.path, err)
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}
	t.f, t.ino = f, ino(fi)
	t.offset, t.buf, t.start = 0, t.buf[:0], 0
	t.head = nil
	t.newSession()
	if head, err := t.peekHead(); err == nil {
		t.head = head
		t.seedEpoch(head)
	}
	return nil
}

// seedEpoch pulls the session's launch time out of the file head so the very
// first envelopes carry the anchor too. Without it the lines preceding the
// "Current time:" entry ship with a null epoch, and the archive -- which is the
// replayable source of truth -- would be missing it for those lines.
func (t *Tailer) seedEpoch(head []byte) {
	if m := currentTimeRe.FindSubmatch(head); m != nil {
		if ts, err := time.Parse(wfTimeLayout, string(m[1])); err == nil {
			utc := ts.UTC()
			t.epoch = &utc
		}
	}
}

// reopen switches to the file now at t.path, discarding any buffered bytes from
// the old one so content can never splice across the boundary.
func (t *Tailer) reopen() error {
	if t.f != nil {
		t.f.Close()
	}
	return t.open()
}

func (t *Tailer) newSession() {
	var b [16]byte
	rand.Read(b[:])
	t.sessionID = hex.EncodeToString(b[:])
	t.seq = 0
	t.epoch = nil
	t.lastGameTime = nil
}

func (t *Tailer) Close() error {
	if t.f == nil {
		return nil
	}
	return t.f.Close()
}

// Poll reads whatever is currently available and emits any complete lines. It
// is safe to call on an idle file; it simply emits nothing.
func (t *Tailer) Poll(ctx context.Context) error {
	if err := t.checkRotate(); err != nil {
		return err
	}
	if t.f == nil {
		return nil // file missing right now; try again next tick
	}
	for {
		n, err := t.fill()
		if err != nil && !errors.Is(err, io.EOF) {
			return err // includes errLineTooLong, which must not be swallowed
		}
		if n > 0 {
			if derr := t.drain(ctx); derr != nil {
				return derr
			}
		}
		if errors.Is(err, io.EOF) || n == 0 {
			return nil
		}
	}
}

// Run polls on a ticker until ctx is cancelled. Cancellation is the expected
// way to stop, so it is reported as success; only a real read or sink failure
// comes back as an error.
func (t *Tailer) Run(ctx context.Context) error {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := t.Poll(ctx); err != nil {
				if errors.Is(err, context.Canceled) {
					return nil
				}
				return err
			}
			// A sink that batches keeps its records on a failed flush and
			// retries them on the next tick, so a transient outage must not
			// stop the tailer. The sink itself decides when being behind is
			// fatal, and reports that through Emit.
			if err := t.sink.Flush(ctx); err != nil && !errors.Is(err, context.Canceled) {
				slog.Warn("tick flush failed", "err", err)
			}
		}
	}
}

// Shutdown drains what arrived since the last tick and flushes the sink. It
// takes its own context because the one that drove Run is already cancelled by
// the time shutdown starts -- reusing it would cancel the final drain before it
// emitted anything, losing the tail of the session.
func (t *Tailer) Shutdown(ctx context.Context) error {
	err := t.Poll(ctx)
	if ferr := t.sink.Flush(ctx); err == nil {
		err = ferr // flush still runs even if the drain failed
	}
	return err
}

func (t *Tailer) fill() (int, error) {
	if len(t.buf) == cap(t.buf) {
		if t.start > 0 {
			n := copy(t.buf, t.buf[t.start:]) // compact
			t.buf, t.start = t.buf[:n], 0
		} else {
			if cap(t.buf) >= maxLineBytes {
				return 0, errLineTooLong
			}
			grown := make([]byte, len(t.buf), 2*cap(t.buf))
			copy(grown, t.buf)
			t.buf = grown
		}
	}
	n, err := t.f.Read(t.buf[len(t.buf):cap(t.buf)])
	t.buf = t.buf[:len(t.buf)+n]
	t.offset += int64(n)
	return n, err
}

func (t *Tailer) drain(ctx context.Context) error {
	for {
		// Emit can be a network round-trip. Checking here means shutdown is
		// bounded by one in-flight call rather than by the whole backlog.
		if err := ctx.Err(); err != nil {
			return err
		}
		rel := bytes.IndexByte(t.buf[t.start:], '\n')
		if rel < 0 {
			return nil // remainder is a partial line, hold it
		}
		line := t.buf[t.start : t.start+rel]
		line = bytes.TrimSuffix(line, []byte("\r"))
		if err := t.emit(ctx, line); err != nil {
			return err
		}
		t.start += rel + 1
	}
}

var (
	// "4051.223 Script [Info]: ..." — the leading seconds-since-launch.
	gameTimeRe = regexp.MustCompile(`^(\d+\.\d+) `)
	// The header's absolute launch time; the bracketed UTC value is authoritative.
	currentTimeRe = regexp.MustCompile(`Current time: .* \[UTC: (.+?)\]`)
)

const wfTimeLayout = "Mon Jan 2 15:04:05 2006"

func (t *Tailer) emit(ctx context.Context, line []byte) error {
	// Addresses are removed here, before the line is wrapped, so nothing
	// downstream -- including the S3 archive -- ever holds one.
	s := redactLine(string(line))
	t.noteHeader(s)

	e := Envelope{
		V:         envelopeVersion,
		Source:    sourceWarframe,
		SessionID: t.sessionID,
		Seq:       t.seq,
		Raw:       s,
	}
	if m := gameTimeRe.FindStringSubmatch(s); m != nil {
		if secs, err := strconv.ParseFloat(m[1], 64); err == nil {
			t.lastGameTime = &secs
		}
	}
	// Continuation lines (stack traces) inherit the preceding line's clock.
	if t.lastGameTime != nil {
		g := *t.lastGameTime
		e.GameTimeS = &g
		if t.epoch != nil {
			w := t.epoch.Add(time.Duration(g * float64(time.Second)))
			e.WallTimeUTC = &w
		}
	}
	e.SessionEpochUTC = t.epoch

	t.seq++
	return t.sink.Emit(ctx, e)
}

// noteHeader picks the session's wall-clock epoch out of the header. Every
// later line's timestamp is relative to it, so without this the session has no
// absolute time and envelopes carry a nil wall clock.
func (t *Tailer) noteHeader(s string) {
	if m := currentTimeRe.FindStringSubmatch(s); m != nil {
		if ts, err := time.Parse(wfTimeLayout, m[1]); err == nil {
			utc := ts.UTC()
			t.epoch = &utc
		}
	}
}

// checkRotate handles the two ways the file underneath us can change identity:
// a new inode at the same path (rename+create), and truncation in place, which
// is what Warframe actually does on every launch.
func (t *Tailer) checkRotate() error {
	fi, err := os.Stat(t.path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil // window between rename and create; try again next tick
	}
	if err != nil {
		return err
	}
	if t.f == nil {
		return t.reopen()
	}
	if ino(fi) != t.ino {
		return t.reopen() // rename+create: new inode at the same path
	}

	// Truncation in place, which is what Warframe does on every launch. Size is
	// not a sufficient signal: the game truncates and immediately writes the new
	// session, so by the time we look the file is often already longer than the
	// offset we had reached -- and reading from that stale offset would splice
	// fragments of the new session onto the old one.
	//
	// The file's head is the dependable signal. It changes on every launch (new
	// launch timestamp, new build ID) and costs one small pread per poll.
	head, err := t.peekHead()
	if err != nil {
		return err
	}
	if headChanged(t.head, head) {
		return t.restart(head)
	}
	if len(head) > len(t.head) {
		t.head = head // header still being written; keep the longest we've seen
	}

	pos, err := t.f.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	if fi.Size() < pos {
		return t.restart(head)
	}
	return nil
}

// headPeekBytes is enough to cover the launch timestamp and build ID, which
// together distinguish one session's header from the next.
const headPeekBytes = 512

// peekHead reads the first bytes of the file without disturbing our read
// position. The file's head is the one thing that reliably changes when the
// game truncates and starts a new session.
func (t *Tailer) peekHead() ([]byte, error) {
	buf := make([]byte, headPeekBytes)
	n, err := t.f.ReadAt(buf, 0)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	return buf[:n], nil
}

// headChanged reports whether the file's head is no longer the one we recorded.
// Only the overlapping prefix is compared: while the header is still being
// written the head grows, and growth alone is not a new session. A differing
// byte within that prefix, or a head that shrank, means the file was replaced.
func headChanged(old, cur []byte) bool {
	if len(old) == 0 {
		return false
	}
	if len(cur) < len(old) {
		return true
	}
	return !bytes.Equal(old, cur[:len(old)])
}

// restart rewinds to the top of the same inode and begins a new session. Any
// buffered bytes are dropped so no line can splice across the boundary.
func (t *Tailer) restart(head []byte) error {
	if _, err := t.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	t.offset, t.buf, t.start = 0, t.buf[:0], 0
	t.newSession()
	t.head = head
	t.seedEpoch(head)
	return nil
}
