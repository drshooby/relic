package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
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
	Emit(context.Context, Envelope) error
	Flush(context.Context) error
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

func (sink *StdoutSink) Emit(ctx context.Context, e Envelope) error {
	return sink.enc.Encode(e)
}

func (sink *StdoutSink) Flush(ctx context.Context) error {
	return sink.w.Flush()
}

type KinesisSink struct {
	client     *kinesis.Client
	streamName string
	buf        []types.PutRecordsRequestEntry
	bufBytes   int
}

func NewKinesisSink(cfg aws.Config, streamName string) *KinesisSink {
	client := kinesis.NewFromConfig(cfg)
	return &KinesisSink{client: client, streamName: streamName}
}

func (sink *KinesisSink) Emit(ctx context.Context, e Envelope) error {
	jsonData, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("failed to marshal envelope json: %w", err)
	}

	sink.buf = append(sink.buf, types.PutRecordsRequestEntry{
		Data:         jsonData,
		PartitionKey: aws.String(e.SessionID),
	})

	sink.bufBytes += len(jsonData)

	if len(sink.buf) >= 500 || sink.bufBytes >= 4<<20 {
		return sink.Flush(ctx)
	}
	return nil
}

func (sink *KinesisSink) Flush(ctx context.Context) error {
	if len(sink.buf) == 0 {
		return nil
	}
	putCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	_, err := sink.client.PutRecords(putCtx, &kinesis.PutRecordsInput{
		StreamName: aws.String(sink.streamName),
		Records:    sink.buf,
	})
	if err != nil {
		return fmt.Errorf("failed to put record in kinesis: %w", err)
	}

	sink.buf, sink.bufBytes = sink.buf[:0], 0
	return nil
}
