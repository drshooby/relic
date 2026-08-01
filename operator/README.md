# operator

Go producer that tails Warframe's `EE.log` and emits one JSON envelope per line.

It is deliberately dumb: it wraps lines, it does not parse them. Everything that
interprets a line's contents lives downstream in the hot path, so this binary
rarely has to change. See the
[phase 1 design](../docs/superpowers/specs/2026-07-21-relic-phase1-design.md)
for where it sits in the pipeline.

**Status:** M1 and M2 complete. `-sink=kinesis` batches envelopes to the stream
with `PutRecords`, retries partial failures, and has been verified end to end —
an 11,629-line session landed in S3 intact. The spec's disk spool was
deliberately cut; see
[Deviation from the spec](#deviation-from-the-spec-no-disk-spool).

## Prerequisites

- Go 1.25+
- macOS (arm64) — the game runs here under CrossOver

## Build

From this directory:

```sh
go build -o tail ./cmd/tail/
```

## Run

Defaults to the CrossOver bottle location, so no arguments are needed on this
machine:

```sh
go run ./cmd/tail/          # or ./tail after building
```

Envelopes go to stdout, status messages to stderr, so the stream pipes cleanly:

```sh
./tail | jq -r '.raw'                 # watch the log live
./tail > session.ndjson               # capture a session
```

Stop it with Ctrl-C; buffered envelopes are flushed on the way out.

### Flags

| Flag | Purpose |
|------|---------|
| `-path` | Tail a different file (a captured log, a test fixture) |
| `-once` | Emit what is currently in the file and exit, instead of following it |
| `-sink` | `stdout` (default) or `kinesis` |

`-sink=kinesis` needs the pipeline stack applied and AWS credentials in the
environment; it reads the standard AWS config chain. The stream name comes from
the SSM parameter `/relic/pipeline/kinesis_stream_name`, so the operator also
needs `ssm:GetParameter` on it — with the pipeline destroyed that lookup fails
and the operator exits at startup rather than buffering toward a stream that is
not there. Because the Kinesis shard bills while it exists, the stack is
normally destroyed between sessions — so `stdout` is the everyday mode and
`kinesis` is opt-in.

`-once` is what replays a finite file, and is the seed of the end-to-end replay
harness the design calls for:

```sh
./tail -path testdata/session_sample.log -once | jq .
```

## Output

One envelope per line (newline-delimited JSON):

```json
{
  "v": 1,
  "source": "warframe.ee_log",
  "session_id": "216fc8f367235ebfb1b56a0007d059c0",
  "seq": 9,
  "game_time_s": 0.357,
  "wall_time_utc": "2026-07-23T00:49:16.357Z",
  "session_epoch_utc": "2026-07-23T00:49:16Z",
  "raw": "0.357 Sys [Diag]: Address Space: 128TB / 128TB free"
}
```

- `session_id` is minted per game launch; `seq` counts lines within that
  session. Together they are the ordering and idempotency key — delivery is
  at-least-once, so a duplicate must be harmless, and `(session_id, seq)` is
  what makes it so.
- `game_time_s` is the line's own seconds-since-launch. `session_epoch_utc`
  comes from the header's `Current time:` entry, and `wall_time_utc` is the two
  added together. That absolute timestamp is what will let EEG samples and game
  events land on one timeline in phase 3.
- Continuation lines (stack traces, which carry no timestamp of their own) are
  emitted as-is and inherit the preceding line's clock.
- `raw` is the line as written, minus the line ending and any IP address (see
  below). Nothing else is dropped or rewritten — the archive downstream is meant
  to be replayable.
- `v` is the envelope version. Changing the shape means bumping it, and old
  data has to stay readable.

## Behavior worth knowing

**Relaunches.** Warframe truncates `EE.log` every time it starts. The operator
notices, starts a new session, and resets `seq`. Detection compares the file's
head rather than its size: the game truncates and immediately writes the new
session, so by the time we look the file is usually *longer* than the offset we
had reached, and a size check would miss it and splice the new session onto the
old one.

**Partial lines.** The engine buffers writes (a line can lag ~10s and can be
half-written when we read it). Incomplete lines are held until their newline
arrives, so a truncated line is never emitted.

**Reading is read-only.** The game owns the file; the operator opens it for
reading and never locks, moves, or writes to it.

**IP addresses are stripped before anything is emitted.** `EE.log` records the
owner's public address, the LAN address, and squadmates' addresses (matchmaking
is peer-to-peer) — in `Net` *and* `Game` lines, throughout the session, not just
the header. `redactLine` replaces them with `<ip>` in [redact.go](cmd/tail/redact.go),
so no address ever reaches the stream or the archive. Ports, player ids, and
display names are kept: they are gameplay data.

Dropping whole subsystems was considered and rejected — `Net` alone is 7.8% of
the log and would still miss the addresses in `Game` lines, and a subsystem
denylist fails silently when a game update starts logging addresses somewhere
new. Matching the address itself keeps every line and its signal.

One deliberate false negative: `Phys [Info]: PhysX Core Version: 4.1.1.0` is a
syntactically valid address, so it is recognised by context (the word "version")
rather than by pattern. Anything without that context is redacted, so an
unrecognised version line gets mangled rather than an address getting through.

## Performance

The workload is small — Warframe writes on the order of 10–100 lines/sec — so
the operator is not close to being the bottleneck. Baseline on an M4 Pro
(2026-07-24, commit after M1):

| Benchmark | Result | Meaning |
|-----------|--------|---------|
| `WithJSONSink` | 71ms / 50k lines | ~700k lines/sec through the full path, encoding and redaction included |
| `TailerThroughput` | 20ms / 50k lines | the tailer alone, without JSON |
| `IdlePoll` | ~4.0µs, 3 allocs | one poll when nothing was written |

Replaying a real ~11k-line session takes **~73ms** end to end. Idle polling twice
a second costs roughly 0.0008% of one core, which is the number that matters:
the game must never feel this process.

The synthetic benchmark is pessimistic about redaction — every generated line
contains a dotted `/Lotus/...` path, so almost none take the no-dots fast path,
whereas a real log is mostly short lines that exit immediately.

Known inefficiency, deliberately left alone: ~8 allocations per line (copying
each line out of the buffer, the two pointer fields in the envelope, and
`encoding/json` reflection). At this volume it buys nothing to fix.

IP redaction costs roughly 2x on the synthetic benchmark. Adding it naively cost
**15x** until a dot-counting fast path was put in front of the regexes — worth
knowing before adding any other per-line work.

Run them with:

```sh
go test ./cmd/tail/ -run XXX -bench . -benchmem
```

`IdlePoll` is the regression guard worth watching now that the Kinesis sink has
landed — a flush on an empty buffer returns immediately, but if an idle poll
ever starts doing network work, that number moves first.

The remaining performance question is not CPU. It is what happens to the local
spool when the network drops mid-session, which is still unbuilt.

## Batching and delivery

`KinesisSink` buffers envelopes and ships them with `PutRecords`. `Emit` only
appends; nothing reaches the network until a flush, which fires on whichever
comes first:

| Trigger | Value | Why |
|---------|-------|-----|
| Record count | 500 | The API's hard per-call cap |
| Payload bytes | 4MB | Under the 5MB cap; partition keys count toward it |
| Poll tick | 500ms | Bounds staleness when the log goes quiet between missions |

The time trigger is not an optimization. With a buffering sink, `Flush` is the
only thing that moves records off the machine, so `Run` calls it every tick —
without that, envelopes would sit in memory until Ctrl-C and a crash would lose
the session. (`StdoutSink` hid this: `bufio.Writer` self-flushes when full.)

**Partial failures.** `PutRecords` can return HTTP 200 while individual records
failed — usually `ProvisionedThroughputExceededException` on one record and not
its neighbour. `err == nil` is therefore not success; `FailedRecordCount` has to
be checked separately, and the AWS SDK's built-in retry does not help because
the *call* succeeded. Response entries are positionally matched to the request
and carry only an error code, so the retry batch is rebuilt by index from the
input still in hand, and only the failed subset is resent — resending everything
would duplicate records that already landed. Retries use exponential backoff
with jitter, bounded at 5 attempts.

Duplicates are survivable regardless: `(session_id, seq)` is the idempotency
key, and delivery is at-least-once by design.

**Ordering is not preserved.** Neither `PutRecords` nor Firehose guarantees it,
and retries reorder further — a verified session's S3 object holds every `seq`
exactly once but not in ascending order. Consumers sort by `(session_id, seq)`;
file position means nothing.

**Backpressure.** A failed flush keeps the batch buffered and reports the error
without killing the tailer — a brief Kinesis outage is backpressure, not a
reason to stop capturing. Because "never give up" would mean an unbounded
buffer, `Emit` returns a fatal error past 50,000 buffered records and the
operator exits loudly. Losing records unnoticed is worse than stopping.

**Newline delimiting.** Each record carries a trailing `\n`. Firehose
concatenates payloads verbatim, so without it the S3 object arrives as one
unbroken line that no NDJSON reader can parse.

## Deviation from the spec: no disk spool

The [phase 1 design](../docs/superpowers/specs/2026-07-21-relic-phase1-design.md)
calls for "exponential backoff + spool batches to local disk; drain spool on
recovery." The backoff exists; the spool was deliberately cut.

Reaching the case it protects against takes a network outage that outlasts the
retry budget *and* runs long enough to buffer 50,000 records — on the order of
fifteen minutes mid-session — with the operator then exiting on the ceiling. The
spec's assumption was that the memory buffer held the only copy of those lines.
It does not: `EE.log` is a durable local file the game keeps writing regardless,
so recovery from any such failure is one command, `./tail -once -sink=kinesis`,
which re-reads the session from the top. At-least-once makes the resulting
duplicates harmless.

So the spool buys automatic recovery, from a rare event, that already has a
manual fix — at the cost of a file format, crash-safe appends, drain ordering
against live tailing, and a disk ceiling.

The other reason is scope. Durable local buffering is an **agent** problem, not
a data-engineering one: it is the same work that goes into Fluent Bit or Vector,
and it is usually owned by SRE or observability engineers rather than by the
people who own what happens once records are in the stream. This project exists
to learn streaming data infrastructure, so the effort belongs downstream —
consumer checkpointing, partial failure inside a consumer batch, hot/cold path
divergence, replay. Those are the concepts the producer was built to reach.

Worth revisiting if a real session is ever lost this way. So far none has been.

`IdlePoll` is the benchmark to watch: an idle poll must not become network work.

## Tests

```sh
go test ./... -race
```

Tests run against `testdata/session_sample.log`, a **sanitized** sample of a real
session — identifiers, hostnames, and machine details are replaced with
placeholders. Its CRLF line endings are deliberate (`EE.log` is written by a
Windows binary) and asserted by a test, since normalizing them would silently
stop covering that path.

The last few lines are **synthetic**, not captured: real address-bearing line
shapes (`NAT bound`, `HandleSquadMessage`, `Registered to hub`, `public
address`, and the `PhysX Core Version` false-positive case) rewritten with
[RFC 5737](https://datatracker.ietf.org/doc/html/rfc5737) documentation
addresses — `192.0.2.x`, `198.51.100.x`, `203.0.113.x` — which are reserved for
examples and route nowhere. Without them the redaction test would pass against
a completely broken redactor, since the captured portion contains no addresses.

Never commit raw log content: real `EE.log` headers carry the machine's IP and
hostname. Any new fixture gets scrubbed and reviewed first — see
[CLAUDE.md](../CLAUDE.md).
