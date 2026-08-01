package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

// fakeKinesis stands in for the real client. Each element of results is the
// response to the corresponding call, so a test can spell out a sequence like
// "two records fail, then everything succeeds".
type fakeKinesis struct {
	results []fakeResult
	calls   [][]types.PutRecordsRequestEntry
}

type fakeResult struct {
	// failIdx are positions within that call's batch that come back failed.
	failIdx []int
	err     error
}

func (f *fakeKinesis) PutRecords(_ context.Context, in *kinesis.PutRecordsInput, _ ...func(*kinesis.Options)) (*kinesis.PutRecordsOutput, error) {
	batch := make([]types.PutRecordsRequestEntry, len(in.Records))
	copy(batch, in.Records)
	f.calls = append(f.calls, batch)

	if len(f.calls) > len(f.results) {
		return nil, errors.New("fakeKinesis: unexpected extra call")
	}
	res := f.results[len(f.calls)-1]
	if res.err != nil {
		return nil, res.err
	}

	failed := make(map[int]bool, len(res.failIdx))
	for _, i := range res.failIdx {
		failed[i] = true
	}
	out := &kinesis.PutRecordsOutput{
		FailedRecordCount: aws.Int32(int32(len(res.failIdx))),
		Records:           make([]types.PutRecordsResultEntry, len(in.Records)),
	}
	for i := range in.Records {
		if failed[i] {
			out.Records[i].ErrorCode = aws.String("ProvisionedThroughputExceededException")
		} else {
			out.Records[i].SequenceNumber = aws.String("seq")
		}
	}
	return out, nil
}

// newTestSink builds a sink wired to the fake, with backoff short enough that
// retry tests do not spend seconds sleeping.
func newTestSink(f *fakeKinesis) *KinesisSink {
	return &KinesisSink{
		client:      f,
		streamName:  "test-stream",
		baseBackoff: time.Microsecond,
	}
}

func envelopeN(n int) Envelope {
	return Envelope{V: envelopeVersion, Source: sourceWarframe, SessionID: "sess", Seq: uint64(n), Raw: "line"}
}

// Emit must not send anything on its own; that is the entire point of batching.
func TestEmitBuffersWithoutSending(t *testing.T) {
	f := &fakeKinesis{}
	sink := newTestSink(f)

	for i := range 10 {
		if err := sink.Emit(context.Background(), envelopeN(i)); err != nil {
			t.Fatalf("Emit: %v", err)
		}
	}

	if len(f.calls) != 0 {
		t.Fatalf("expected no PutRecords calls, got %d", len(f.calls))
	}
}

func TestFlushSendsBufferedRecordsInOneCall(t *testing.T) {
	f := &fakeKinesis{results: []fakeResult{{}}}
	sink := newTestSink(f)

	for i := range 10 {
		if err := sink.Emit(context.Background(), envelopeN(i)); err != nil {
			t.Fatalf("Emit: %v", err)
		}
	}
	if err := sink.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	if len(f.calls) != 1 {
		t.Fatalf("expected 1 PutRecords call, got %d", len(f.calls))
	}
	if got := len(f.calls[0]); got != 10 {
		t.Fatalf("expected 10 records in the call, got %d", got)
	}
}

// Reaching the API's per-call record cap must send without waiting for a tick.
func TestEmitFlushesAtRecordCap(t *testing.T) {
	f := &fakeKinesis{results: []fakeResult{{}}}
	sink := newTestSink(f)

	for i := range maxRecordsPerCall {
		if err := sink.Emit(context.Background(), envelopeN(i)); err != nil {
			t.Fatalf("Emit: %v", err)
		}
	}

	if len(f.calls) != 1 {
		t.Fatalf("expected 1 PutRecords call at the record cap, got %d", len(f.calls))
	}
	if got := len(f.calls[0]); got != maxRecordsPerCall {
		t.Fatalf("expected %d records, got %d", maxRecordsPerCall, got)
	}
}

func TestFlushOnEmptyBufferDoesNothing(t *testing.T) {
	f := &fakeKinesis{}
	sink := newTestSink(f)

	if err := sink.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if len(f.calls) != 0 {
		t.Fatalf("expected no calls, got %d", len(f.calls))
	}
}

// PutRecords answers 200 with per-record failures. Only the failures may be
// resent -- resending the whole batch would duplicate records that succeeded.
func TestFlushRetriesOnlyFailedRecords(t *testing.T) {
	f := &fakeKinesis{results: []fakeResult{
		{failIdx: []int{1, 3}},
		{},
	}}
	sink := newTestSink(f)

	for i := range 5 {
		if err := sink.Emit(context.Background(), envelopeN(i)); err != nil {
			t.Fatalf("Emit: %v", err)
		}
	}
	if err := sink.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	if len(f.calls) != 2 {
		t.Fatalf("expected 2 calls (initial + retry), got %d", len(f.calls))
	}
	retry := f.calls[1]
	if len(retry) != 2 {
		t.Fatalf("expected 2 records in the retry, got %d", len(retry))
	}
	// Envelope 1 and 3 are the ones that failed; the retry must carry exactly
	// those, matched by position against the original batch.
	for i, want := range []string{`"seq":1`, `"seq":3`} {
		if got := string(retry[i].Data); !strings.Contains(got, want) {
			t.Errorf("retry[%d] = %s, want it to contain %s", i, got, want)
		}
	}
}

// A record that keeps failing must not be silently dropped.
func TestFlushReturnsErrorWhenRecordsNeverSucceed(t *testing.T) {
	results := make([]fakeResult, maxAttempts)
	for i := range results {
		results[i] = fakeResult{failIdx: []int{0}}
	}
	f := &fakeKinesis{results: results}
	sink := newTestSink(f)

	if err := sink.Emit(context.Background(), envelopeN(0)); err != nil {
		t.Fatalf("Emit: %v", err)
	}
	err := sink.Flush(context.Background())
	if err == nil {
		t.Fatal("expected an error when records never succeed")
	}
	if len(f.calls) != maxAttempts {
		t.Fatalf("expected %d attempts, got %d", maxAttempts, len(f.calls))
	}
}

// The buffer is the only copy of those records, so a failed flush must keep it.
func TestBufferSurvivesFailedFlush(t *testing.T) {
	f := &fakeKinesis{results: []fakeResult{{err: errors.New("kinesis down")}}}
	sink := newTestSink(f)

	if err := sink.Emit(context.Background(), envelopeN(0)); err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if err := sink.Flush(context.Background()); err == nil {
		t.Fatal("expected Flush to report the failure")
	}
	if len(sink.buf) != 1 {
		t.Fatalf("expected the record to stay buffered, got %d", len(sink.buf))
	}
}

// A successful flush must clear the buffer, or the next one resends everything.
func TestBufferClearedAfterSuccessfulFlush(t *testing.T) {
	f := &fakeKinesis{results: []fakeResult{{}}}
	sink := newTestSink(f)

	if err := sink.Emit(context.Background(), envelopeN(0)); err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if err := sink.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if len(sink.buf) != 0 || sink.bufBytes != 0 {
		t.Fatalf("expected an empty buffer, got %d records / %d bytes", len(sink.buf), sink.bufBytes)
	}
}

// Kinesis being unavailable is normal backpressure: Emit reports it but keeps
// accepting lines, so a blip mid-session cannot kill the tailer.
func TestEmitSurvivesFlushFailureAtCap(t *testing.T) {
	results := make([]fakeResult, maxAttempts)
	for i := range results {
		results[i] = fakeResult{err: errors.New("kinesis down")}
	}
	f := &fakeKinesis{results: results}
	sink := newTestSink(f)

	for i := range maxRecordsPerCall {
		if err := sink.Emit(context.Background(), envelopeN(i)); err != nil {
			t.Fatalf("Emit returned an error on a transient failure: %v", err)
		}
	}
	if len(sink.buf) != maxRecordsPerCall {
		t.Fatalf("expected records to stay buffered, got %d", len(sink.buf))
	}
}

// Past the ceiling the operator must fail loudly rather than buffer forever.
func TestEmitFailsOnceBufferCeilingExceeded(t *testing.T) {
	f := &fakeKinesis{}
	sink := newTestSink(f)
	// Pre-fill past the ceiling so the test does not have to emit 50k lines
	// through the flush path.
	sink.buf = make([]types.PutRecordsRequestEntry, maxBufferedRecords)

	err := sink.Emit(context.Background(), envelopeN(0))
	if err == nil {
		t.Fatal("expected Emit to fail once the buffer ceiling is exceeded")
	}
}

// countingSink records how many times the tailer flushed it, standing in for a
// batching sink that only reaches the network when Flush runs.
type countingSink struct {
	emitted int
	flushes int
	// pending mirrors a batching sink's buffer: Emit adds, Flush drains.
	pending int
	sent    int
}

func (c *countingSink) Emit(_ context.Context, _ Envelope) error {
	c.emitted++
	c.pending++
	return nil
}

func (c *countingSink) Flush(context.Context) error {
	c.flushes++
	c.sent += c.pending
	c.pending = 0
	return nil
}

// A batching sink only reaches the network on Flush, so Run must flush on its
// own -- otherwise records sit in the buffer until shutdown and a crash loses
// the whole session.
func TestRunFlushesOnEachTick(t *testing.T) {
	path := filepath.Join(t.TempDir(), "EE.log")
	writeFile(t, path, testHeader+"1.000 Script [Info]: alpha\n")

	sink := &countingSink{}
	tl := newTestTailer(t, path, sink)

	// Long enough for several ticks, short enough to keep the test quick.
	ctx, cancel := context.WithTimeout(context.Background(), 3*pollInterval)
	defer cancel()
	if err := tl.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if sink.flushes < 2 {
		t.Errorf("Run flushed %d times over ~3 ticks, want at least 2", sink.flushes)
	}
	if sink.pending != 0 {
		t.Errorf("%d records left unflushed after Run", sink.pending)
	}
}

// failingFlushSink fails every flush, standing in for Kinesis being unavailable.
type failingFlushSink struct {
	emitted int
	flushes int
}

func (s *failingFlushSink) Emit(_ context.Context, _ Envelope) error {
	s.emitted++
	return nil
}

func (s *failingFlushSink) Flush(context.Context) error {
	s.flushes++
	return errors.New("kinesis down")
}

// A batching sink keeps its records when a flush fails and retries on the next
// tick, so a transient outage must not stop the tailer.
func TestRunSurvivesFlushFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "EE.log")
	writeFile(t, path, testHeader+"1.000 Script [Info]: alpha\n")

	sink := &failingFlushSink{}
	tl := newTestTailer(t, path, sink)

	ctx, cancel := context.WithTimeout(context.Background(), 3*pollInterval)
	defer cancel()
	if err := tl.Run(ctx); err != nil {
		t.Fatalf("Run returned an error on a transient flush failure: %v", err)
	}
	if sink.flushes < 2 {
		t.Errorf("Run stopped flushing after a failure: %d flushes", sink.flushes)
	}
}

// Firehose concatenates record payloads verbatim, so each record has to carry
// its own newline or the S3 object arrives as one unbroken line -- unreadable
// to Athena, Glue, and every other newline-delimited-JSON reader.
func TestEmittedRecordsAreNewlineDelimited(t *testing.T) {
	f := &fakeKinesis{results: []fakeResult{{}}}
	sink := newTestSink(f)

	for i := range 3 {
		if err := sink.Emit(context.Background(), envelopeN(i)); err != nil {
			t.Fatalf("Emit: %v", err)
		}
	}
	if err := sink.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	var joined []byte
	for _, r := range f.calls[0] {
		joined = append(joined, r.Data...)
	}
	if got := bytes.Count(joined, []byte("\n")); got != 3 {
		t.Errorf("concatenated payloads hold %d newlines, want 3", got)
	}
	// Concatenated records must parse as one JSON document per line.
	for i, line := range bytes.Split(bytes.TrimRight(joined, "\n"), []byte("\n")) {
		var e Envelope
		if err := json.Unmarshal(line, &e); err != nil {
			t.Errorf("line %d does not parse as an envelope: %v", i, err)
		}
	}
}

// A successful flush must report what moved, so a live session shows progress
// instead of silence.
func TestFlushLogsRecordsSent(t *testing.T) {
	f := &fakeKinesis{results: []fakeResult{{}}}
	sink := newTestSink(f)

	var logged bytes.Buffer
	sink.log = slog.New(slog.NewTextHandler(&logged, nil))

	for i := range 3 {
		if err := sink.Emit(context.Background(), envelopeN(i)); err != nil {
			t.Fatalf("Emit: %v", err)
		}
	}
	if err := sink.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	got := logged.String()
	if !strings.Contains(got, "records=3") {
		t.Errorf("flush log does not report the record count: %q", got)
	}
}

// Retries are the interesting failure mode, so a partial failure must say so
// rather than being absorbed into a silent success.
func TestPartialFailureIsLogged(t *testing.T) {
	f := &fakeKinesis{results: []fakeResult{
		{failIdx: []int{1}},
		{},
	}}
	sink := newTestSink(f)

	var logged bytes.Buffer
	sink.log = slog.New(slog.NewTextHandler(&logged, nil))

	for i := range 3 {
		if err := sink.Emit(context.Background(), envelopeN(i)); err != nil {
			t.Fatalf("Emit: %v", err)
		}
	}
	if err := sink.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	if got := logged.String(); !strings.Contains(got, "failed=1") {
		t.Errorf("partial failure not logged: %q", got)
	}
}

// A sink built without an explicit logger must still work rather than panic on
// a nil *slog.Logger.
func TestSinkWithoutLoggerDoesNotPanic(t *testing.T) {
	f := &fakeKinesis{results: []fakeResult{{}}}
	sink := &KinesisSink{client: f, streamName: "test-stream", baseBackoff: time.Microsecond}

	if err := sink.Emit(context.Background(), envelopeN(0)); err != nil {
		t.Fatalf("Emit: %v", err)
	}
	if err := sink.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}
