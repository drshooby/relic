# relic — Phase 1 Design: Streaming Ingestion Backbone

**Date:** 2026-07-21
**Status:** Approved in brainstorm; pending implementation plan
**Scope:** Phase 1 only (Warframe log → live dashboard). Phases 2–4 are roadmap context in the README and get their own specs later.

## Goal

Tail Warframe's `EE.log` on the local Mac, stream every line to AWS in near-real-time, archive raw data permanently in S3, and show parsed events on a custom dashboard within seconds of them happening in-game. Every design choice also prepares (cheaply) for phase 2 (lakehouse/replay), phase 3 (EEG fusion), and phase 4 (consumer swap).

## Verified facts this design rests on

- `EE.log` exists at `~/Library/Application Support/CrossOver/Bottles/Steam Library/drive_c/users/crossover/AppData/Local/Warframe/EE.log` (game runs on this Mac via CrossOver).
- Line format: `<seconds-since-launch> <subsystem> [<level>]: <message>`. Some entries continue across multiple lines (continuation lines have no leading timestamp).
- The header logs absolute launch time (`Current time: ... [UTC: ...]`) → every relative timestamp is convertible to UTC.
- The file is written continuously during play (engine buffering can lag ~10s) and is **truncated on every game launch**.
- PII is **IP addresses**, and they are not confined to the header. Verified against a real session: the owner's public address, the LAN address, and squadmates' addresses (matchmaking is peer-to-peer) appear throughout, in `Net` and `Game` lines. No email is logged, despite the community wiki's warning — that claim shaped the original draft of this spec and did not survive contact with real data.

## Constraints

- Near-real-time (seconds of lag is fine; EE.log's ~10s write buffer is the floor).
- Minimal steady-state cost; everything except stored data must be `terraform destroy`-able. No always-on compute.
- Languages: Go (operator), Python (Lambdas), TypeScript (dashboard).
- Single user, single producer machine (for now).

## Architecture

```
EE.log ──(Go operator, local)──▶ Kinesis Data Stream (1 provisioned shard)
                                     ├─▶ Firehose ──▶ S3 raw archive       [cold path]
                                     └─▶ Lambda parse/clean ──▶ DynamoDB   [hot path]
                                                                    ▲
                                       Dashboard (Next.js + API GW/Lambda)─┘
```

Classic hot/cold (lambda-architecture) split: the cold path is the immutable source of truth; the hot path is a disposable live view. EEG (phase 3) becomes a second producer into the same stream with a different `source`; nothing else changes.

## Components

### 1. Operator (`operator/`, Go)

A local daemon that tails EE.log and ships lines to Kinesis. Deliberately dumb — no parsing beyond envelope needs — so it almost never changes.

Responsibilities:
- Tail the file read-only; never interfere with the game's writes.
- **Redact IP addresses before the line is wrapped.** The one transformation the operator performs, and a deliberate exception to "cold path stores raw lines": the invariant exists so history stays replayable, and no parser needs an IP to reconstruct a mission. Doing it here means no address ever reaches Kinesis or S3, so there is nothing to clean up later. Ports and player ids are kept. A dotted quad in version context (`PhysX Core Version: 4.1.1.0`) is left alone — it is indistinguishable from an address by pattern, so context decides, and the failure mode is a mangled version string rather than a leaked address.
- Detect truncation (size shrink) → new session: mint a new `session_id` (UUID), reset `seq`, re-parse the header for the wall-clock anchor.
- Wrap each line in the envelope (below); batch into `PutRecords` (up to 500 records / call), partition key = `session_id`.
- On Kinesis failure: exponential backoff + spool batches to local disk; drain spool on recovery. Never lose lines, never block on the network.
- Config file: bottle path, stream name, AWS region/profile. Started manually when playing (launchd agent is a later nicety). Not terraformed.

### 2. Event envelope

One JSON record per log line:

```json
{
  "v": 1,
  "source": "warframe.ee_log",
  "session_id": "9f3c…",
  "seq": 48211,
  "game_time_s": 4051.223,
  "wall_time_utc": "2026-07-18T21:27:39.223Z",
  "session_epoch_utc": "2026-07-18T20:20:08Z",
  "raw": "4051.223 Script [Info]: …"
}
```

- `wall_time_utc = session_epoch_utc + game_time_s` — the cross-source join key for EEG fusion.
- `(session_id, seq)` — ordering + idempotency key; also lets downstream reassemble multi-line entries (continuation lines ship as their own records with `game_time_s`/`wall_time_utc` carried from the preceding timestamped line, or null if none seen yet).
- `v` — schema version for painless evolution.

### 3. Stream (Kinesis Data Streams)

- 1 provisioned shard (~$0.015/hr; cheaper than on-demand at this volume, and teaches shard mechanics). 24h retention.
- Partition key `session_id` preserves per-session ordering.

### 4. Cold path (Firehose → S3)

- Firehose attached to the stream; buffer ~60s / 1MB; gzip; **zero transformation** (the operator's IP redaction is the only change ever applied to a line, and it happens before the stream).
- S3 layout (Hive-style, Athena-ready):
  `raw/source=warframe.ee_log/dt=YYYY-MM-DD/session_id=<uuid>/<firehose-object>.gz`
  (dt from `wall_time_utc`, via Firehose dynamic partitioning on the envelope.)
- Delivery failures land under `errors/` in the same bucket rather than vanishing.

### 5. Hot path (`pipeline/hot-path/`, Python Lambda)

- Event source mapping on the stream (batch size ~100, `bisect_batch_on_function_error`, SQS DLQ, `maximum_retry_attempts` bounded so a poison record can't wedge the shard).
- Parses `raw` → `{event_type, subsystem, level, attrs}`. Starting vocabulary: `session.start`, `mission.start`, `mission.end`, and `log.line` as the catch-all — **a bad line is data, not an error**; the parser vocabulary grows over time (and phase 2's replay tool re-derives history with each new parser version).
- Observed from a real session, for whoever builds the parser:
  - Void relic runs live in the `VoidProjections:` and `Projection*.lua` lines. Rewards are stated outright — `VoidProjections: <player_id> gets reward /Lotus/StoreItems/.../VorunaPrimeHelmetBlueprint` — so they do not have to be inferred from icon loads.
  - Item paths use **legacy internal names**: the engine says `Helmet` where the UI says *Neuroptics*. A path → display-name mapping is needed, and it cannot be built by transliterating the path.
  - `Script [Info]: EndOfMatch.lua:` carries mission outcome (`Mission Succeeded`), reward tags (`NotifyTagMultiple(MISSION_REWARD_CREDITS): 4400`), and squad roster lines.
  - `Sys [Info]: Weapon in slot <SLOT> with ID <id> has gained <n> XP` is the per-weapon XP breakdown.
- Writes:
  - `relic-events` table — PK `session_id`, SK `seq` (zero-padded string), attrs + envelope fields, TTL ~24h. Overwrite-on-duplicate = idempotent under at-least-once delivery.
  - `relic-sessions` table — one item per session (`session_id`, `started_at`, `last_seen_at`, counts), TTL ~7d, so the dashboard can find the active/recent sessions cheaply.
- DynamoDB is a **rolling cache**, not the archive. On-demand capacity; effectively $0 at this volume.

### 6. Dashboard (`dashboard/` + `pipeline/api/`)

- Read API: API Gateway (HTTP API) + Python Lambda. Endpoints: list recent sessions; get events for a session since a given `seq`.
- Next.js app polls the API ~2s during a live session; renders the session timeline / event feed. WebSockets are a possible later upgrade; polling is fine for a single user.
- No auth in phase 1 (single user, API is destroyable and holds only game telemetry from the rolling cache). Revisit if the dashboard is ever hosted publicly.

## Terraform

Two stacks, decoupled by design (the point: destroy the pipeline freely, keep the data forever):

- **`infra/data` (persistent, applied once, local state):**
  - Data bucket: versioned, lifecycle to Glacier after ~90d. No hard delete protection — deletion is simply an owner-only action (see CLAUDE.md).
  - TF state bucket for other stacks.
  - SSM parameter `/relic/data-bucket-name`.
- **`infra/pipeline` (ephemeral, S3 backend):**
  - Modules: `stream` (Kinesis), `cold-path` (Firehose + IAM), `hot-path` (Lambda + DDB + DLQ), `api` (API GW + Lambda). Module boundaries exist so phase 4 swaps `hot-path` without surgery.
  - Reads the data bucket name from SSM — no cross-stack state references.
- `scripts/up.sh` / `scripts/down.sh` wrap apply/destroy for the play-session lifecycle. `down.sh` waits ~2 minutes past the last operator activity before destroying, so records still buffered in Firehose flush to S3 first.

## Error handling summary

| Failure | Behavior |
|---|---|
| Game relaunch (log truncated) | Operator detects shrink → new session, offset 0 |
| Network/Kinesis outage | Operator backoff + disk spool; drain on recovery |
| Duplicate delivery (retries anywhere) | `(session_id, seq)` overwrite in DDB; dedupe on replay |
| Unparseable log line | Becomes `log.line` event — never dropped, never an error |
| Poison record crashes Lambda | Bisect batch + bounded retries + SQS DLQ |
| Firehose delivery failure | `errors/` prefix in bucket |
| Operator crash | Restart re-reads from last spooled position; worst case duplicates (harmless), never gaps within a running session |

## Testing

TDD throughout.

- **Fixtures:** a sanitized sample of the real 12MB EE.log (PII scrubbed before committing).
- **Operator (Go):** unit tests for tail-through-truncation, partial-line writes, multi-line entries, header parsing, spool/drain; integration test replaying the fixture end-to-end to stdout (M1 exit criterion).
- **Parser (Python):** golden-file tests — real lines in, expected structured events out.
- **E2E (M2/M3):** `terraform apply` → replay fixture through the operator against the real stack → assert objects in S3 and rows in DynamoDB → `terraform destroy`. This harness is the seed of phase 2's replay tool.
- **Dashboard:** light component tests; not the phase's focus.

## Milestones

- **M1 — Operator, offline.** Tails the real EE.log through a live play session; truncation-safe; envelopes to stdout/file. No AWS.
- **M2 — First byte to cloud.** Both stacks apply cleanly; play a mission → find it gzipped in S3.
- **M3 — Hot path.** Events queryable in DynamoDB seconds after they happen in-game.
- **M4 — Dashboard.** Live session feed in the browser. Phase 1 complete.

## Out of scope (phase 1)

Glue/Athena/Parquet (phase 2), replay tool beyond the E2E harness (phase 2), EEG anything (phase 3), Flink/KCL (phase 4), auth on the dashboard, WebSockets, launchd packaging of the operator.

## Cost model

Steady state (pipeline destroyed): S3 storage only — cents. During active use: 1 Kinesis shard $0.015/hr, Firehose/Lambda/DynamoDB/API GW pennies at single-user volume. No VPC, no NAT, no always-on compute anywhere.
