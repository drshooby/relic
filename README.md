# relic

A personal data-infrastructure project: stream and fuse **Warframe gameplay telemetry** and **EEG headset data** into a live dashboard, built on AWS streaming primitives and Terraform.

## Why

Three interests colliding:

- **Human sciences** — what does my brain actually do during a tense defense wave vs. cracking relics? EEG + game telemetry on one timeline can answer that.
- **Data infrastructure** — I work in cloud infrastructure and want real, hands-on depth in streaming systems: producers/consumers, at-least-once delivery, hot/cold paths, lakehouse patterns, replay/backfill.
- **Warframe** — one of my favorite games ever, and (conveniently) it writes a continuously-updated engine log.

## How it works (phase 1)

Warframe's PC client writes an engine log (`EE.log`, in `%LOCALAPPDATA%\Warframe\` — here, inside a CrossOver bottle on macOS) continuously during a session. A small Go **operator** tails it and ships every line, wrapped in a JSON envelope, to **Kinesis Data Streams**. The stream fans out two ways:

```
EE.log ──(Go operator, local)──▶ Kinesis Data Stream
                                     ├─▶ Firehose ──▶ S3 raw archive   (cold path: source of truth, replayable)
                                     └─▶ Lambda (parse/clean) ──▶ DynamoDB rolling cache   (hot path: live view)
                                                                       ▲
                                     Dashboard (Next.js + API) ────────┘  ~2s polling during a session
```

- **Cold path** stores every raw line untouched in Hive-partitioned S3 — the permanent, replayable record.
- **Hot path** parses lines into structured events for a near-real-time dashboard; DynamoDB is a TTL'd rolling cache, not the archive.
- Delivery is **at-least-once**; `(session_id, seq)` is the idempotency key that makes duplicates harmless.

Full design: [docs/superpowers/specs/2026-07-21-relic-phase1-design.md](docs/superpowers/specs/2026-07-21-relic-phase1-design.md)

## Infrastructure philosophy

Two Terraform stacks, deliberately decoupled:

- **`infra/data`** — persistent. The S3 data bucket (plus TF state bucket). Applied once, never destroyed; test-run data survives everything.
- **`infra/pipeline`** — ephemeral. Kinesis, Firehose, Lambdas, DynamoDB, API Gateway. `terraform destroy`'d whenever not in use; steady-state cost is essentially S3 storage alone.

The stacks share nothing but an SSM parameter (the data bucket's name), so the pipeline can be rebuilt from scratch without touching stored data.

## Roadmap

### Phase 1 — Ingestion (current)
The serverless streaming backbone described above.
- **M1** — Go operator tails the real EE.log through a live play session (truncation-safe, offline, envelopes to stdout)
- **M2** — Terraform stacks up; operator → Kinesis → Firehose → raw lines land in S3
- **M3** — Hot-path Lambda parses into DynamoDB; events queryable seconds after they happen in-game
- **M4** — Custom dashboard shows the live session feed

### Phase 2 — Lakehouse & replay
Turn the raw S3 archive into a real analytical layer: Glue catalog, JSON→Parquet compaction, date/session partitioning, Athena. Build a **replay tool** to reprocess historical raw logs through new parser versions — the payoff of the "store raw everything" decision.

### Phase 3 — EEG fusion
Second producer: EEG headset (hardware TBD). High-frequency continuous signal alongside sparse discrete game events — clock alignment across sources, windowed aggregation (e.g., 1s band-power buckets), and a dashboard timeline that overlays brain state on gameplay events.

### Phase 4 — Consumer upgrade (optional)
Swap the hot-path Lambda for a real stream-processing consumer (Flink or KCL), spun up per-session to keep costs near zero. Same data, heavier machinery — a deliberate compare-and-contrast with the serverless consumer model.

## Notes & constraints

- EE.log facts (verified locally): truncated on every game launch; writes can lag ~10s (engine buffering); header contains the absolute launch time, which anchors every relative timestamp to UTC — the key to cross-source fusion later.
- The log header contains PII (IP, email). Data lands only in a private bucket; committed test fixtures are sanitized.
- Languages: Go (operator), Python (stream processing), TypeScript (dashboard) — right tool per layer.
