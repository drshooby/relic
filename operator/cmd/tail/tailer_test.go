package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// collectSink records everything emitted so tests can assert on it.
type collectSink struct {
	lines    []string
	sessions []string
}

func (c *collectSink) Emit(e Envelope) error {
	c.lines = append(c.lines, e.Raw)
	c.sessions = append(c.sessions, e.SessionID)
	return nil
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func appendFile(t *testing.T, path, content string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteString(content); err != nil {
		t.Fatal(err)
	}
}

const testHeader = "0.050 Sys [Diag]: Process Command-line: -cluster:public -language:en\n" +
	"0.100 Sys [Diag]: Build Unique ID: 111\n" +
	"0.200 Sys [Diag]: Current time: Wed Jul 22 19:49:16 2026 [UTC: Thu Jul 23 00:49:16 2026]\n"

func newTestTailer(t *testing.T, path string, sink Sink) *Tailer {
	t.Helper()
	tl, err := NewTailer(path, sink)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tl.Close() })
	return tl
}

func TestEmitsCompleteLines(t *testing.T) {
	path := filepath.Join(t.TempDir(), "EE.log")
	writeFile(t, path, testHeader+"1.000 Script [Info]: alpha\n2.000 Script [Info]: beta\n")

	sink := &collectSink{}
	tl := newTestTailer(t, path, sink)
	if err := tl.Poll(); err != nil {
		t.Fatal(err)
	}

	want := []string{"1.000 Script [Info]: alpha", "2.000 Script [Info]: beta"}
	got := sink.lines[len(sink.lines)-2:]
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("line %d = %q, want %q", i, got[i], want[i])
		}
	}
}

// A line still being written must not be emitted until its newline arrives.
func TestHoldsPartialLineUntilComplete(t *testing.T) {
	path := filepath.Join(t.TempDir(), "EE.log")
	writeFile(t, path, testHeader)

	sink := &collectSink{}
	tl := newTestTailer(t, path, sink)
	if err := tl.Poll(); err != nil {
		t.Fatal(err)
	}
	before := len(sink.lines)

	appendFile(t, path, "3.000 Script [Info]: half")
	if err := tl.Poll(); err != nil {
		t.Fatal(err)
	}
	if len(sink.lines) != before {
		t.Fatalf("emitted a partial line: %q", sink.lines[before:])
	}

	appendFile(t, path, "-written\n")
	if err := tl.Poll(); err != nil {
		t.Fatal(err)
	}
	if got, want := sink.lines[len(sink.lines)-1], "3.000 Script [Info]: half-written"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

// Warframe truncates EE.log in place on launch. The tailer must start over and
// mint a new session, even when the new content is already longer than the
// offset we had consumed.
func TestDetectsInPlaceTruncation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "EE.log")
	writeFile(t, path, testHeader+"1.000 Script [Info]: old session line\n")

	sink := &collectSink{}
	tl := newTestTailer(t, path, sink)
	if err := tl.Poll(); err != nil {
		t.Fatal(err)
	}
	firstSession := sink.sessions[len(sink.sessions)-1]

	// Relaunch: truncate and immediately write a longer new session.
	newHeader := "0.050 Sys [Diag]: Process Command-line: -cluster:public -language:en\n" +
		"0.100 Sys [Diag]: Build Unique ID: 222\n" +
		"0.200 Sys [Diag]: Current time: Wed Jul 22 21:00:00 2026 [UTC: Thu Jul 23 02:00:00 2026]\n"
	writeFile(t, path, newHeader+"1.000 Script [Info]: new session line\n"+strings.Repeat("2.000 Sys [Info]: filler\n", 50))

	if err := tl.Poll(); err != nil {
		t.Fatal(err)
	}

	for _, l := range sink.lines {
		if strings.Contains(l, "old session line") && len(sink.lines) > 0 {
			continue // fine in the pre-truncate batch
		}
	}
	last := sink.lines[len(sink.lines)-1]
	if !strings.Contains(last, "filler") {
		t.Fatalf("did not resume reading after truncation; last line = %q", last)
	}
	newSession := sink.sessions[len(sink.sessions)-1]
	if newSession == firstSession {
		t.Errorf("session id unchanged across truncation (%s); a relaunch must start a new session", newSession)
	}
	// No line may be a splice of old and new content.
	for _, l := range sink.lines {
		if strings.Contains(l, "old session line") && strings.Contains(l, "new session") {
			t.Errorf("spliced line across truncation boundary: %q", l)
		}
	}
}

// Rename+create (logrotate style) is not what Warframe does, but the tailer
// should follow the path rather than hold a deleted inode.
func TestFollowsNewInodeAtSamePath(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "EE.log")
	writeFile(t, path, testHeader+"1.000 Script [Info]: first\n")

	sink := &collectSink{}
	tl := newTestTailer(t, path, sink)
	if err := tl.Poll(); err != nil {
		t.Fatal(err)
	}

	if err := os.Rename(path, filepath.Join(dir, "EE.log.1")); err != nil {
		t.Fatal(err)
	}
	writeFile(t, path, testHeader+"1.000 Script [Info]: second\n")
	if err := tl.Poll(); err != nil {
		t.Fatal(err)
	}

	if got := sink.lines[len(sink.lines)-1]; !strings.Contains(got, "second") {
		t.Errorf("did not follow new inode; last line = %q", got)
	}
}

// A line longer than the buffer's starting size must still be emitted whole.
func TestGrowsBufferForLongLine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "EE.log")
	long := strings.Repeat("x", 200_000)
	writeFile(t, path, testHeader+"1.000 Script [Info]: "+long+"\n")

	sink := &collectSink{}
	tl := newTestTailer(t, path, sink)
	if err := tl.Poll(); err != nil {
		t.Fatal(err)
	}

	last := sink.lines[len(sink.lines)-1]
	if !strings.HasSuffix(last, long) {
		t.Errorf("long line truncated: got %d bytes, want suffix of %d", len(last), len(long))
	}
}

// A pathological line beyond maxLineBytes must not grow the buffer forever.
func TestRejectsLineBeyondMax(t *testing.T) {
	path := filepath.Join(t.TempDir(), "EE.log")
	writeFile(t, path, testHeader+"1.000 "+strings.Repeat("y", maxLineBytes+1_000)+"\n")

	sink := &collectSink{}
	tl := newTestTailer(t, path, sink)
	err := tl.Poll()
	if err == nil {
		t.Fatal("expected an error for an over-long line")
	}
	if !strings.Contains(err.Error(), "too long") {
		t.Errorf("unexpected error: %v", err)
	}
}

// The header's UTC stamp anchors relative timestamps to wall-clock time.
func TestEnvelopeCarriesWallClockAndSeq(t *testing.T) {
	path := filepath.Join(t.TempDir(), "EE.log")
	writeFile(t, path, testHeader+"1.500 Script [Info]: anchored\n")

	sink := &envSink{}
	tl := newTestTailer(t, path, sink)
	if err := tl.Poll(); err != nil {
		t.Fatal(err)
	}

	last := sink.envs[len(sink.envs)-1]
	if last.GameTimeS == nil || *last.GameTimeS != 1.5 {
		t.Errorf("GameTimeS = %v, want 1.5", last.GameTimeS)
	}
	want := time.Date(2026, 7, 23, 0, 49, 17, 500_000_000, time.UTC)
	if last.WallTimeUTC == nil || !last.WallTimeUTC.Equal(want) {
		t.Errorf("WallTimeUTC = %v, want %v", last.WallTimeUTC, want)
	}
	// seq is monotonic within a session, starting at 0 for the first line.
	for i, e := range sink.envs {
		if e.Seq != uint64(i) {
			t.Errorf("envs[%d].Seq = %d, want %d", i, e.Seq, i)
		}
	}
}

// Continuation lines (stack traces) have no leading timestamp; they ship as-is
// and inherit the previous line's clock rather than being dropped.
func TestContinuationLineInheritsClock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "EE.log")
	writeFile(t, path, testHeader+"1.500 Script [Error]: boom\n    at SomeFunction()\n")

	sink := &envSink{}
	tl := newTestTailer(t, path, sink)
	if err := tl.Poll(); err != nil {
		t.Fatal(err)
	}

	last := sink.envs[len(sink.envs)-1]
	if last.Raw != "    at SomeFunction()" {
		t.Fatalf("continuation line altered: %q", last.Raw)
	}
	if last.GameTimeS == nil || *last.GameTimeS != 1.5 {
		t.Errorf("continuation GameTimeS = %v, want inherited 1.5", last.GameTimeS)
	}
}

// EE.log is CRLF (it is a Windows binary running under CrossOver). The carriage
// return must be stripped from every emitted line, and the fixture has to keep
// its CRLF endings for this to be worth anything.
func TestStripsCarriageReturns(t *testing.T) {
	fixture, err := os.ReadFile("../../testdata/session_sample.log")
	if err != nil {
		t.Skip("fixture not present")
	}
	if !bytes.Contains(fixture, []byte("\r\n")) {
		t.Fatal("fixture lost its CRLF endings; it no longer covers the Windows line-ending path")
	}

	path := filepath.Join(t.TempDir(), "EE.log")
	writeFile(t, path, string(fixture))

	sink := &collectSink{}
	tl := newTestTailer(t, path, sink)
	if err := tl.Poll(); err != nil {
		t.Fatal(err)
	}

	for i, l := range sink.lines {
		if strings.HasSuffix(l, "\r") {
			t.Fatalf("line %d kept its carriage return: %q", i, l)
		}
	}
}

// The real (sanitized) log must flow through without error.
func TestReplaysRealFixture(t *testing.T) {
	fixture, err := os.ReadFile("../../testdata/session_sample.log")
	if err != nil {
		t.Skip("fixture not present")
	}
	path := filepath.Join(t.TempDir(), "EE.log")
	writeFile(t, path, string(fixture))

	sink := &collectSink{}
	tl := newTestTailer(t, path, sink)
	if err := tl.Poll(); err != nil {
		t.Fatal(err)
	}

	wantLines := strings.Count(string(fixture), "\n")
	if len(sink.lines) != wantLines {
		t.Errorf("emitted %d lines, want %d", len(sink.lines), wantLines)
	}
	for _, s := range sink.sessions {
		if s == "" {
			t.Fatal("empty session id in fixture replay")
		}
	}
}

// The session epoch comes from a header line that is not the first line, but
// every envelope in the session -- including the ones before it -- must carry
// it, since S3 is the replayable source of truth.
func TestFirstEnvelopesCarrySessionEpoch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "EE.log")
	writeFile(t, path, testHeader+"1.000 Script [Info]: later\n")

	sink := &envSink{}
	tl := newTestTailer(t, path, sink)
	if err := tl.Poll(); err != nil {
		t.Fatal(err)
	}

	for i, e := range sink.envs {
		if e.SessionEpochUTC == nil {
			t.Errorf("envs[%d] (%q) has nil session epoch", i, e.Raw)
		}
	}
}

// StdoutSink buffers; envelopes are lost unless Flush runs before exit. This
// cost 12 lines off the end of the first real run.
func TestStdoutSinkFlushEmitsAllLines(t *testing.T) {
	path := filepath.Join(t.TempDir(), "EE.log")
	writeFile(t, path, testHeader+"1.000 Script [Info]: a\n2.000 Script [Info]: b\n")

	var out strings.Builder
	sink := NewStdoutSink(&out)
	tl := newTestTailer(t, path, sink)
	if err := tl.Poll(); err != nil {
		t.Fatal(err)
	}
	if err := sink.Flush(); err != nil {
		t.Fatal(err)
	}

	got := strings.Count(strings.TrimSpace(out.String()), "\n") + 1
	if want := 5; got != want {
		t.Errorf("flushed %d envelopes, want %d", got, want)
	}
}

type envSink struct{ envs []Envelope }

func (e *envSink) Emit(env Envelope) error {
	e.envs = append(e.envs, env)
	return nil
}
