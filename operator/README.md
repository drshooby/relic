# operator

Go producer that tails Warframe's `EE.log` and emits one JSON envelope per line.

It is deliberately dumb: it wraps lines, it does not parse them. Everything that
interprets a line's contents lives downstream in the hot path, so this binary
rarely has to change. See the
[phase 1 design](../docs/superpowers/specs/2026-07-21-relic-phase1-design.md)
for where it sits in the pipeline.

**Status:** M1 complete — tails the live log and writes envelopes to stdout.
Kinesis delivery arrives at M2.

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
- `raw` is the line exactly as written, minus the line ending. Nothing is
  dropped or rewritten — the archive downstream is meant to be replayable.
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

## Performance

The workload is small — Warframe writes on the order of 10–100 lines/sec — so
the operator is not close to being the bottleneck. Baseline on an M4 Pro
(2026-07-24, commit after M1):

| Benchmark | Result | Meaning |
|-----------|--------|---------|
| `WithJSONSink` | 32ms / 50k lines | ~1.5M lines/sec through the full path, envelope encoding included |
| `TailerThroughput` | 9.6ms / 50k lines | the tailer alone, without JSON |
| `IdlePoll` | ~1.9µs, 3 allocs | one poll when nothing was written |

A whole 4-hour session (~34k lines) is ~22ms of work. Idle polling twice a
second costs roughly 0.0004% of one core, which is the number that matters:
the game must never feel this process.

Known inefficiency, deliberately left alone: ~8 allocations per line (copying
each line out of the buffer, the two pointer fields in the envelope, and
`encoding/json` reflection). At this volume it buys nothing to fix.

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

## Tests

```sh
go test ./... -race
```

Tests run against `testdata/session_sample.log`, a **sanitized** sample of a real
session — identifiers, hostnames, and machine details are replaced with
placeholders. Its CRLF line endings are deliberate (`EE.log` is written by a
Windows binary) and asserted by a test, since normalizing them would silently
stop covering that path.

Never commit raw log content: real `EE.log` headers carry the machine's IP and
hostname. Any new fixture gets scrubbed and reviewed first — see
[CLAUDE.md](../CLAUDE.md).
