package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"os"
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

// PutRecords caps a single call at 500 records and 5MB. The byte ceiling is set
// below the hard limit because the API counts the partition key toward it too.
const (
	maxRecordsPerCall = 500
	maxBytesPerCall   = 4 << 20
)

// maxBufferedRecords bounds how far behind the sink may fall while Kinesis is
// refusing records. At a few hundred bytes per line this is roughly 15MB, or
// minutes of gameplay. Past it the operator exits rather than growing without
// limit or silently dropping lines -- the archive is the replayable source of
// truth, so losing records unnoticed is the worst available outcome.
const maxBufferedRecords = 50_000

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

	if len(sink.buf) >= maxRecordsPerCall || sink.bufBytes >= maxBytesPerCall {
		// A failed flush leaves the batch buffered for the next attempt. Kinesis
		// being briefly unavailable is normal backpressure and must not kill the
		// tailer, so the error is reported and swallowed here.
		if err := sink.Flush(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "kinesis flush failed, %d records buffered: %v\n", len(sink.buf), err)
		}
	}

	if len(sink.buf) > maxBufferedRecords {
		return fmt.Errorf("kinesis sink backed up: %d records buffered, giving up", len(sink.buf))
	}
	return nil
}

func (sink *KinesisSink) Flush(ctx context.Context) error {
	if len(sink.buf) == 0 {
		return nil
	}
	if err := sink.fire(ctx); err != nil {
		return err
	}
	sink.buf, sink.bufBytes = sink.buf[:0], 0
	return nil
}

const maxAttempts = 5

func (sink *KinesisSink) fire(ctx context.Context) error {
	pending := sink.buf
	for attempt := range maxAttempts {
		if attempt > 0 {
			backoff := time.Duration(1<<attempt) * 100 * time.Millisecond
			jitter := time.Duration(rand.Int63n(int64(backoff)))
			select {
			case <-time.After(backoff + jitter):
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		putCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		out, err := sink.client.PutRecords(putCtx, &kinesis.PutRecordsInput{
			StreamName: aws.String(sink.streamName),
			Records:    pending,
		})
		cancel()
		if err != nil {
			return fmt.Errorf("put records: %w", err)
		}
		if aws.ToInt32(out.FailedRecordCount) == 0 {
			return nil
		}

		next := pending[:0:0] // shorthand for making a new slice
		for i, r := range out.Records {
			if r.ErrorCode != nil {
				next = append(next, pending[i])
			}
		}
		pending = next
	}

	return fmt.Errorf("%d records still failing after %d attempts", len(pending), maxAttempts)
}
