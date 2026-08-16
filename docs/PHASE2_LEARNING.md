# Phase 2 — what to learn before building the lakehouse

A study guide, written with [Claude](https://claude.ai/code) partway through phase 1. It assumes
cloud-infra fluency (IAM, Terraform, S3) and **no data-engineering background**. It is not a design
doc — no decisions are made here. It explains the vocabulary you'll meet, why each idea exists, and
which parts of this repo already depend on them.

Read the roadmap entry first: [README.md](../README.md#phase-2--lakehouse--replay). Phase 2 is two
deliverables:

1. **Lakehouse** — Glue catalog, JSON→Parquet compaction, partitioning, Athena over the S3 archive.
2. **Replay tool** — reprocess historical raw logs through new parser versions.

Everything downstream of the archive is new. The archive itself already exists and is verified.

---

## 0. The one-paragraph version

You have a bucket full of gzipped JSON lines. Phase 2 makes it *queryable* — `SELECT` over it with
SQL — and *reprocessable* — run a newer parser over old data and get better events out. The first
half is mostly metadata and file formats. The second half is the payoff for a decision you made back
in M1: the cold path stores raw, untransformed lines, so history can always be re-derived. Almost
every term below is in service of one of those two things.

---

## 1. Lakehouse

### The word itself

Two older things, and a compromise between them:

- **Data warehouse** — you load data *into* a proprietary system (Redshift, Snowflake, BigQuery). It
  owns the storage, enforces schemas, and gives you fast SQL and transactions. Loading is a
  commitment: it costs money and time, and the system's format is the only format.
- **Data lake** — you dump files in object storage (S3) in whatever format, and figure out meaning
  later. Cheap and flexible, historically a mess to query. The failure mode has a name: **data
  swamp**, a lake nobody can find anything in.

A **lakehouse** keeps files in S3 but adds warehouse-like semantics on top: schemas, SQL, sometimes
transactions. Your bucket *becomes* the table. Nothing is loaded anywhere.

The property that makes it work is **separation of storage and compute**. Storage is S3, sitting
there at ~$0.023/GB/month whether or not anyone queries. Compute is Athena, existing only for the
seconds a query runs. A warehouse couples the two — that is why an idle Redshift cluster still bills.
This separation is also the reason phase 2 fits this project's no-standing-cost rule, and the reason
it's the dominant architecture in industry right now.

> **Why this project is a genuine lakehouse and not just "files in a bucket":** the raw archive is
> immutable and append-only, partitioned by time, and read by an engine that never mutates it. That
> is the shape. What's missing is only the metadata layer — which is exactly what phase 2 adds.

### Glue Data Catalog

**A metadata store, not a data store.** Nothing of yours is copied into it. It records: *this S3
prefix is a table named `raw_events`, here are its columns and their types, here is how it's
partitioned, here is the file format.*

This matters because Athena has no storage of its own. It is a query engine and nothing else. When
you run a query, Athena asks the catalog "where are the bytes and what shape are they?", then reads
S3 directly. Swap Athena for Redshift Spectrum or EMR and they ask the same catalog the same
question. **The catalog is the shared contract**; the engines are interchangeable. That decoupling is
the whole point, and it's why "define the table once, query it from anywhere" is a real property and
not marketing.

Two ways to populate it:

| | **Crawler** | **Terraform (`aws_glue_catalog_table`)** |
|---|---|---|
| How | Samples your files, infers schema and partitions | You declare columns and types by hand |
| Effort | Near zero | You write the schema out |
| Correctness | Guesses. Can type a column wrong, or change its guess when new data arrives | Exactly what you wrote |
| Cost | Billed per crawler-run (DPU-hour, 10-min minimum) | Free |
| Reviewable | No — schema appears in the console | Yes — it's in the diff |

For this repo the second is the obvious fit: everything goes through Terraform, nothing bills while
idle, and you already know your schema because you wrote the producer. Learn what a crawler is
anyway — you'll be asked about it, and it's the right tool when you *don't* control the producer.

### Partitioning, and why your Firehose prefix looks like that

Open [infra/pipeline/firehose.tf](../infra/pipeline/firehose.tf#L25-L31). The prefix is:

```
raw/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/
```

Those `key=value` path segments are **Hive-style partitioning** — a convention from Apache Hive that
Glue, Athena, Spark, Trino and friends all understand. Encoding a column's value *in the path* means
a query engine can decide whether to open a file without opening it.

That is **partition pruning**: `WHERE year='2026' AND month='08'` reads one prefix instead of the
whole bucket. It is the single biggest lever on Athena cost, because **Athena bills per byte
scanned** (~$5/TB, no idle charge, no per-query minimum for S3 sources). A query that scans 10 GB
costs five cents; the same query pruned to one partition scanning 50 MB costs a hundredth of that.
Pruning is not an optimization you add later — it's determined by the layout you chose at write time,
which is why the comment in `firehose.tf` was written during phase 1 for a phase-2 benefit.

Contrast with Firehose's **default** prefix, `2026/07/31/00/` — same information, positional rather
than named. Glue can't infer that `2026` means `year`, so every prefix needs a manual
`ALTER TABLE ADD PARTITION`. You avoided that.

**Partition projection** is the Athena feature that removes partition bookkeeping entirely. Instead
of registering partitions, you declare their *shape* in table properties — "the `year` column is an
integer from 2026 to 2030, `month` is 01–12, and the path template is this." Athena computes which
partitions could exist and goes straight to S3, skipping the `GetPartitions` catalog call. No
crawler, no `ALTER TABLE`, new days just work.

Read the limitations before committing to it, because one bites hobby projects specifically:

- If a projected partition **doesn't exist in S3, Athena projects it anyway** — no error, zero rows.
- **If more than half your projected partitions are empty, AWS recommends traditional partitions
  instead**, because Athena wastes time probing prefixes that aren't there.

You generate data only on days you play. An hour-granularity projection over a year is ~8,760
partitions, of which maybe a few dozen are non-empty. That is *far* past the "more than half empty"
line. Worth measuring rather than assuming, but don't reach for projection reflexively just because
every blog post recommends it — your data is sparse in exactly the way it dislikes.

### Columnar formats — Parquet

Today the archive is gzipped **NDJSON**: newline-delimited JSON, one envelope per line, row-oriented.
To answer "what's the average `game_time_s` across all sessions?", an engine must read every byte of
every line, parse each JSON object, and throw away every field but one.

**Parquet** stores data by *column* instead of by row. All the `game_time_s` values sit contiguously,
all the `session_id` values sit contiguously. Three consequences, and they compound:

1. **Read only what you asked for.** A query touching 2 of 12 columns reads roughly 2/12 of the
   bytes. On a per-TB-scanned pricing model that's a direct discount.
2. **Far better compression.** A column is homogeneous — a thousand similar timestamps, or the same
   `session_id` repeated — so run-length and dictionary encoding do enormous work that gzip over
   mixed-type JSON text cannot.
3. **Predicate pushdown.** Parquet files are split into **row groups**, each carrying per-column
   **min/max statistics**. For `WHERE game_time_s > 5000`, the engine reads a row group's stats,
   sees `max = 900`, and skips the entire group without decompressing it. Same idea as partition
   pruning, one level further down.

Parquet also carries its own schema, so the file is self-describing — the catalog and the file agree
without a separate contract. Know **ORC** exists as the other major columnar format with the same
core ideas; the practical answer for AWS is Parquet, because everything reads it.

> **The tension you'll hit immediately, and it's the interesting problem in phase 2.** Columnar wins
> come from having *many typed columns*. Your raw archive is an envelope wrapping one big opaque
> `raw` string — the log line. Converting that to Parquet as-is gives you a one-string-column table
> and almost none of the benefit.
>
> So useful compaction means **parsing** `raw` into typed columns (`event_type`, `subsystem`,
> `reward_item`, …). But parsing is what the hot-path Lambda does, and re-parsing history is
> precisely what the replay tool is for. The two deliverables are much more coupled than the roadmap
> makes them look, and **"is the Parquet table produced by the replay tool, or is it a separate
> straight compaction of raw strings?"** is the question that decides whether phase 2 is one project
> or two. Resolve it before writing a spec.

### The small-files problem, and compaction

Firehose flushes on a 60-second buffer ([firehose.tf:23](../infra/pipeline/firehose.tf#L23)), so a
one-hour session leaves ~60 small gzip objects.

Query engines pay a **fixed cost per file** — an S3 `GET`, HTTP round-trip, metadata read, opening a
decompressor. Below a certain size that overhead dominates and you're paying latency for
bookkeeping rather than data. Thousands of tiny files can make a query slower than the same bytes in
one file by an order of magnitude. In industry this is *the* classic data-lake complaint.

**Compaction** is the fix: periodically rewrite many small files into fewer large ones — and, in
practice, convert format at the same time. Typical targets are 128 MB–1 GB per file, mostly inherited
from HDFS block sizing. Your volume is nowhere near that, which is worth noting honestly: **at this
scale compaction is a learning exercise, not a performance necessity.** That's a fine reason to build
it, but be clear-eyed that the win is understanding the pattern, not query speed.

Also note gzip is **non-splittable** — a single gzip file can't be divided across parallel readers,
because you can't start decompressing in the middle. One huge .gz is a bottleneck. Parquet with
Snappy or Zstd internally is splittable, which is another reason compaction and format conversion
travel together.

### Medallion architecture (bronze / silver / gold)

The naming convention nearly every data team uses for lakehouse layers:

- **Bronze** — raw, exactly as ingested, never modified. *You already have this: `raw/` in S3.*
- **Silver** — cleaned, typed, deduplicated, parsed. One row per event with real columns.
- **Gold** — aggregated and shaped for consumption. Per-session summaries, reward-rate tables.

Nothing magic; it's shared vocabulary. It's worth adopting because it makes the coupling above
obvious: your silver layer *is* parser output, so silver and the replay tool are the same machinery.
The term is also common enough in interviews and design docs that not knowing it reads as a gap.

---

## 2. The replay tool

### Kappa vs Lambda architecture

Two names you should be able to compare, because your project is an argument for one of them.

**Lambda architecture** (no relation to AWS Lambda — the naming collision is unfortunate and
predates the service in common use): run a *batch layer* over all history for correctness, and a
*speed layer* over the live stream for freshness, then merge. The known flaw is that you implement
your business logic **twice**, in two systems with different semantics, and must keep them agreeing
forever.

**Kappa architecture**: keep only the stream. If the raw log is durable and replayable, you don't
need a separate batch layer — to recompute history you just re-run the stream processor from the
beginning. One codebase, one set of semantics.

**relic is a small Kappa.** One raw immutable archive, one parser, replay to re-derive. The
justification is in [README.md](../README.md#lessons-learned) — "in a streaming pipeline, ask which
component owns durability," and every stage downstream of it is reprocessable.

Worth reading: Jay Kreps' *Questioning the Lambda Architecture* (2014), the essay that named Kappa.
Short, opinionated, and it's the intellectual basis for your entire cold path.

### Backfill and reprocessing

**Backfill** — computing results for a past period you couldn't compute before, usually because the
logic didn't exist yet. **Reprocessing** — recomputing results you already had, because the logic
changed or was wrong.

Your parser vocabulary is deliberately small today (`session.start`, `mission.start`, `mission.end`,
and `log.line` as catch-all, per the phase-1 spec). Every future parser improvement makes old
`log.line` rows re-parseable into something meaningful. That is the concrete payoff of storing raw,
and it only exists because you resisted transforming the cold path.

### The ordering trap — read this one twice

From [CLAUDE.md](../CLAUDE.md): records land in S3 **out of `seq` order**, because neither
`PutRecords` nor Firehose preserves order. The object is valid NDJSON with every `seq` present
exactly once, but **file position means nothing.**

Any replay reader must sort by `(session_id, seq)` and never by line number or file order. A naive
line-by-line reader will look like it works — the data is all there — and silently produce a
mis-ordered session. This is verified behavior on real data, not a theoretical concern.

### Idempotency and determinism

You already have the idempotency key: `(session_id, seq)`. Delivery is at-least-once everywhere, so
duplicates are normal and harmless — a re-delivered record overwrites itself.

The question replay *forces* is one at-least-once doesn't answer: when parser v2 reprocesses a
session that parser v1 already wrote, does it **overwrite** v1's output or write to a **new
versioned table**? Overwriting is simpler and keeps one source of truth. But it destroys the ability
to diff parser versions against the same input — arguably the entire reason to have a replay tool.
Think about this before you build; it's a design decision, not a detail.

Related: **determinism**. Replay is only trustworthy if the same input always yields the same output.
A parser that reads the wall clock, iterates a Python `set`, or depends on an external lookup that
changes is not deterministic, and replay results won't be reproducible.

### Schema evolution

Your envelope carries `v`, and [CLAUDE.md](../CLAUDE.md) requires bumps to stay backward-compatible
so old raw data remains replayable. **The replay tool is where that promise gets tested** — it must
handle every `v` ever written, forever. That's a stronger obligation than the live pipeline has,
since the live pipeline only ever sees current records.

Learn the general rules, which hold in Parquet, Avro, and Protobuf alike:

- **Adding a nullable column** — safe. Old files lack it; readers see null.
- **Removing a column** — usually safe for readers that don't ask for it.
- **Renaming** — not safe; it's a delete plus an add, and old data won't follow.
- **Retyping** — the dangerous one. Widening (int32→int64) is often fine; anything narrowing or
  changing kind will either fail or silently corrupt.

The `Decimal`/float rule in CLAUDE.md is the same family of problem one layer down: a
representational choice that fails silently rather than loudly.

### Event time vs processing time

**The most important streaming concept in this document**, and phase 2 is where it first bites.

- **Event time** — when the thing actually happened. Here: `wall_time_utc` / `game_time_s`, parsed
  from the log line.
- **Processing time** — when your system saw it. Here: Firehose's ingestion timestamp.

They differ whenever anything is delayed, retried, buffered, or replayed. Your current partitioning
uses **processing time** — the comment at
[firehose.tf:28-29](../infra/pipeline/firehose.tf#L28-L29) says so explicitly. Consequences:

- A session spanning midnight UTC splits across two day-partitions by *arrival*, not by when you
  played.
- Replaying an old log through the operator lands 2026-07 events in a 2026-08 partition. **Replay
  and processing-time partitioning actively fight each other** — worth sitting with, since replay is
  the other half of phase 2.
- `WHERE day='15'` answers "what arrived on the 15th", not "what happened on the 15th." Fine until
  it isn't, and the failure is silent.

This also foreshadows **phase 3**: fusing EEG with game events is entirely a clock-alignment problem
across two sources with independent notions of time. Understanding event vs processing time now pays
twice.

The general vocabulary — **watermarks** (how a stream engine decides "probably no more events older
than T will arrive"), **late-arriving data**, **windowing** (tumbling, sliding, session) — belongs to
phase 4's Flink/KCL work, but read it now. It's the conceptual core of stream processing.

---

## 3. The first real decision phase 2 has to make

Two documents in this repo disagree about the S3 layout, and reconciling them is step one.

**The spec** ([phase-1 design](superpowers/specs/2026-07-21-relic-phase1-design.md), §4) calls for:

```
raw/source=warframe.ee_log/dt=YYYY-MM-DD/session_id=<uuid>/<object>.gz
```

partitioned by **event date** (from the envelope's `wall_time_utc`) and by **session**, using
Firehose **dynamic partitioning**.

**What's deployed** ([firehose.tf:30](../infra/pipeline/firehose.tf#L30)):

```
raw/year=YYYY/month=MM/day=DD/hour=HH/
```

partitioned by **ingestion time**, with **no `session_id` partition at all**.

What follows:

- **No `session_id` partition** means "give me session X" scans every partition in the time range
  rather than one prefix. Sessions are your natural query unit, so this is the layout's main cost.
- **Ingestion time ≠ event time** — see above.
- **Firehose dynamic partitioning** (deriving partition keys from record *content* via a jq
  expression) is what would close the gap. It has its own per-GB charge and requires Firehose to
  parse every record, which is in mild tension with the zero-transformation cold path — it doesn't
  modify the record, but it does stop being a blind passthrough.
- The alternative is **fixing partitioning during compaction**: leave the raw bronze layer exactly as
  it is and let the silver layer be partitioned by event time and session. This is the more common
  industry answer, and it preserves bronze's untouched-passthrough property.

I'd lean toward the second, but it's a real trade-off and it deserves the brainstorm → spec flow the
rest of the project uses. Note that changing the Firehose prefix does **not** rewrite existing
objects, so whatever you decide, the catalog has to cope with a layout change partway through
history — itself a good lesson.

---

## 4. Cost check

The rule from [CLAUDE.md](../CLAUDE.md): no always-on compute, nothing that bills while idle.

**Free or near-free while idle:**

- **Athena** — per TB scanned (~$5/TB), no idle charge, no per-query minimum for S3 sources. At your
  volume, effectively free.
- **Glue Data Catalog** — first 1,000,000 objects and 1,000,000 requests/month free; then $1 per
  100k objects. Note an "object" is a table, *partition*, table version, or database — hour-grain
  partitions accumulate faster than you'd expect, though a million is still far away.
- **S3 storage** — what you already pay.

**Would break the rule:**

- **Glue crawlers on a schedule** — billed per DPU-hour with a 10-minute minimum per run.
- **Glue ETL jobs** for compaction — DPU-hour billing plus startup overhead; a real standing cost if
  scheduled.
- Anything provisioned: Redshift, EMR clusters, OpenSearch.

A **Lambda-based compactor** (triggered on demand or by a low-frequency EventBridge rule) stays
within the philosophy, and at your data volume it's genuinely sufficient — Glue ETL exists for scales
you don't have. Worth deciding before design, since it constrains the compaction approach.

Unchanged from phase 1: **the Kinesis shard is still the thing that bills while idle** (~$11/month
from apply to destroy). Phase 2 works entirely on data already in S3, so most of it can be built and
tested with the pipeline stack destroyed. That's a nice property — take advantage of it.

---

## 5. Reading list, in order

**Do first — in this repo:**

1. [README.md](../README.md#phase-2--lakehouse--replay) — the roadmap entry.
2. [README.md](../README.md#lessons-learned) — the durability-ownership lesson; it's why replay works.
3. [phase-1 design spec](superpowers/specs/2026-07-21-relic-phase1-design.md), §4 — the *intended* S3 layout.
4. [infra/pipeline/firehose.tf](../infra/pipeline/firehose.tf#L20-L35) — the layout you *shipped*. Compare with §3 above.

**AWS docs — narrow and directly applicable:**

- [Athena partition projection](https://docs.aws.amazon.com/athena/latest/ug/partition-projection.html) — read the limitations section carefully.
- `aws_glue_catalog_table` in the Terraform AWS provider docs — the resource you'll actually write.
- Firehose dynamic partitioning — to judge the §3 decision.
- Athena's "Top 10 performance tuning tips" post — dated but still the best concise summary of why layout drives cost.

**Concepts — the ones that generalize past AWS:**

- [Parquet format docs](https://parquet.apache.org/docs/) on row groups, column chunks, and page statistics. Enough to understand *why* predicate pushdown works, not just that it does.
- Jay Kreps, *Questioning the Lambda Architecture* — short, and the basis of your cold path.
- Kleppmann, *Designing Data-Intensive Applications*, ch. 10–11 (batch and stream processing). The best single treatment of the batch/stream duality that phases 2 and 4 both circle. Ch. 11's "the log is the source of truth" argument is your project in book form.

**Skip for now:** Iceberg, Delta Lake, and Hudi — the "open table format" generation that adds ACID
transactions, time travel, and schema evolution on top of Parquet. They are where the industry is
heading and absolutely worth knowing eventually, but they solve concurrent-writer and mutation
problems you don't have with a single append-only producer. Learn plain Parquet + Glue first; the
table formats make far more sense once you've felt what they fix. (S3 Tables — managed Iceberg — is
the AWS-native version if you revisit this.)

---

## 6. Self-check

You're ready to spec phase 2 when you can answer these without looking:

- Why does the S3 prefix use `year=2026/` instead of `2026/`?
- Athena bills per byte scanned. Name two independent mechanisms that reduce bytes scanned, operating at different levels.
- Why doesn't converting the raw archive straight to Parquet buy much?
- Your archive is valid NDJSON with every `seq` present exactly once. Why can't a replay tool read it top to bottom?
- What breaks if you partition by ingestion time and then replay a two-month-old log?
- Which phase-2 components would bill while you're not playing?

---

*Written with Claude (Opus 5) via Claude Code, August 2026. Facts on Athena/Glue pricing and
partition-projection limits were verified against AWS docs at time of writing; pricing changes, so
re-check before relying on the numbers.*
