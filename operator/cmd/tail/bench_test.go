package main

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// discardSink measures the tailer itself, without JSON encoding cost.
type discardSink struct{ n int }

func (d *discardSink) Emit(Envelope) error { d.n++; return nil }
func (d *discardSink) Flush() error        { return nil }

func benchLog(b *testing.B, lines int) string {
	b.Helper()
	var sb strings.Builder
	sb.WriteString("0.050 Sys [Diag]: Process Command-line: -cluster:public\r\n")
	sb.WriteString("0.200 Sys [Diag]: Current time: Wed Jul 22 19:49:16 2026 [UTC: Thu Jul 23 00:49:16 2026]\r\n")
	for i := 0; i < lines; i++ {
		sb.WriteString("1234.567 Sys [Info]: Spot-loading /Lotus/Types/Game/MissionDecks/SomeReward\r\n")
	}
	path := filepath.Join(b.TempDir(), "EE.log")
	if err := os.WriteFile(path, []byte(sb.String()), 0o644); err != nil {
		b.Fatal(err)
	}
	return path
}

// BenchmarkTailerThroughput measures lines/sec through the tailer alone.
func BenchmarkTailerThroughput(b *testing.B) {
	path := benchLog(b, 50_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sink := &discardSink{}
		tl, err := NewTailer(path, sink)
		if err != nil {
			b.Fatal(err)
		}
		if err := tl.Poll(); err != nil {
			b.Fatal(err)
		}
		tl.Close()
		b.SetBytes(int64(sink.n))
	}
}

// BenchmarkWithJSONSink measures the full path, including envelope encoding,
// which is what actually runs in production.
func BenchmarkWithJSONSink(b *testing.B) {
	path := benchLog(b, 50_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sink := NewStdoutSink(io.Discard)
		tl, err := NewTailer(path, sink)
		if err != nil {
			b.Fatal(err)
		}
		if err := tl.Poll(); err != nil {
			b.Fatal(err)
		}
		sink.Flush()
		tl.Close()
	}
}

// BenchmarkIdlePoll measures the cost of a poll when nothing has been written,
// which is what happens 99% of the time while tailing a live file.
func BenchmarkIdlePoll(b *testing.B) {
	path := benchLog(b, 100)
	sink := &discardSink{}
	tl, err := NewTailer(path, sink)
	if err != nil {
		b.Fatal(err)
	}
	defer tl.Close()
	if err := tl.Poll(); err != nil { // drain first
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := tl.Poll(); err != nil {
			b.Fatal(err)
		}
	}
}
