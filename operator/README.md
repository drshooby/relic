# operator

Go producer that tails Warframe's `EE.log` and emits one JSON envelope per line.

It is deliberately dumb: it wraps lines, it does not parse them. Everything that
interprets a line's contents lives downstream in the hot path, so this binary
rarely has to change. See the
[phase 1 design](../docs/superpowers/specs/2026-07-21-relic-phase1-design.md)
for where it sits in the pipeline.

**Status:** M1 complete. M2 in progress — `-sink=kinesis` produces to the stream
with one `PutRecord` per line. Batching and the offline spool are not built yet
(see [Next: batching](#next-batching)).

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
environment; it reads the standard AWS config chain. Because the Kinesis shard
bills while it exists, the stack is normally destroyed between sessions — so
`stdout` is the everyday mode and `kinesis` is opt-in.

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

`IdlePoll` is the regression guard worth watching when the Kinesis sink lands
at M2 — if it starts doing per-poll work (a network call, a stat storm), that
number moves first.

The real M2 performance questions are not CPU. They are how long to buffer
before a `PutRecords` call (latency vs. API cost, 500 records / 5MB per call,
1,000 records/sec per shard) and what happens to the local spool when the
network drops mid-session.

## Next: batching

`KinesisSink.Emit` currently makes one `PutRecord` call per log line. That was
the deliberate first step — it proves credentials, region, partition key, and
the whole path to S3 without any buffering logic to debug at the same time — but
it is not what ships.

At 10–100 lines/sec, one synchronous HTTPS round trip per line means the tailer
blocks on the network for every line it reads, and each call carries its own
request overhead against the shard's 1,000 records/sec limit. `PutRecords` takes
up to 500 records (5MB) per call.

What has to be decided:

- **Flush trigger.** Record count, payload bytes, or a time bound — in practice
  all three, whichever comes first. A time bound is what keeps the last few
  lines of an idle session from sitting in the buffer indefinitely.
- **Partial failure.** `PutRecords` returns `FailedRecordCount` with a
  per-record status; the call can succeed overall while individual records fail
  (usually `ProvisionedThroughputExceededException`). Only the failed subset may
  be retried — resending the whole batch duplicates records that already
  landed. Duplicates are survivable, since `(session_id, seq)` makes them
  idempotent, but manufacturing them is still wrong.
- **Backpressure.** What happens when the buffer fills faster than it drains.
  Blocking the tailer risks falling behind the log; dropping loses data.
- **Durability.** Today an `Emit` error propagates up and stops the tailer, so a
  transient network blip ends the session capture. The design calls for
  exponential backoff plus a local disk spool drained on recovery — never lose
  lines, never block on the network.

`Flush` becomes load-bearing at that point: with a buffer, records exist that
the tailer considers emitted but that have not left the machine, and only the
shutdown flush saves them. The context plumbing for that is already in place —
`main` cancels on signal and hands the final drain and flush a separate 10s
budget derived from `context.Background()`, so cleanup still has a live context
after everything else has been cancelled.

`IdlePoll` is the benchmark to watch: batching must not turn an idle poll into
network work.

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
