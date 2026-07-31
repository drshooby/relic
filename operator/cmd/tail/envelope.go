package main

import (
	"bufio"
	"encoding/json"
	"io"
	"time"
)

// envelopeVersion is bumped whenever the envelope's shape changes. Old raw data
// must stay replayable, so changes here have to be backward-compatible.
const envelopeVersion = 1

const sourceWarframe = "warframe.ee_log"

// Envelope wraps a single raw log line with the metadata that only the producer
// can know: which session it belongs to, its order within that session, and the
// wall-clock time it happened. Parsing the line itself is deliberately left to
// the hot path downstream.
type Envelope struct {
	V               int        `json:"v"`
	Source          string     `json:"source"`
	SessionID       string     `json:"session_id"`
	Seq             uint64     `json:"seq"`
	GameTimeS       *float64   `json:"game_time_s"`
	WallTimeUTC     *time.Time `json:"wall_time_utc"`
	SessionEpochUTC *time.Time `json:"session_epoch_utc"`
	Raw             string     `json:"raw"`
}

// Sink consumes envelopes. M1 ships them to stdout; M2 swaps in a Kinesis
// implementation without the tailer knowing the difference.
type Sink interface {
	Emit(Envelope) error
	Flush() error
}

// StdoutSink writes newline-delimited JSON.
type StdoutSink struct {
	w   *bufio.Writer
	enc *json.Encoder
}

func NewStdoutSink(w io.Writer) *StdoutSink {
	bw := bufio.NewWriter(w)
	return &StdoutSink{w: bw, enc: json.NewEncoder(bw)}
}

func (ss *StdoutSink) Emit(e Envelope) error {
	return ss.enc.Encode(e)
}

func (ss *StdoutSink) Flush() error {
	return ss.w.Flush()
}

type KinesisSink struct {
	w   *bufio.Writer
	enc *json.Encoder
}

func NewKinesisSink(w io.Writer) *KinesisSink {
	bw := bufio.NewWriter(w)
	return &KinesisSink{w: bw, enc: json.NewEncoder(bw)}
}

func (ks *KinesisSink) Emit(e Envelope) error {
	return ks.enc.Encode(e)
}

func (ks *KinesisSink) Flush() error {
	return ks.w.Flush()
}
