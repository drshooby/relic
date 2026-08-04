# infra

Two Terraform stacks with deliberately different lifecycles. The split is the whole point: the pipeline is destroyed whenever it is idle, and the data it produced survives that.

```
infra/
  data/        persistent  -- applied once, essentially never destroyed
  pipeline/    ephemeral   -- destroyed between play sessions
```

## Why two stacks

A single provisioned Kinesis shard bills ~$0.015/hour from apply until destroy, regardless of traffic — about $11/month to leave running for a hobby project used a few hours a week. Everything else here is on-demand and costs effectively nothing while idle. So the pipeline exists only while the game is running, and the archive it wrote has to outlive it.

That is only safe if destroying the pipeline cannot take the data with it. Hence the boundary: **anything holding data lives in `data/`, anything that merely processes it lives in `pipeline/`.**

## `data/` — persistent

| Resource | Purpose |
|---|---|
| `aws_s3_bucket.data_bucket` | The archive. Versioned, public access blocked, TLS-only bucket policy. |
| `aws_ssm_parameter.data_bucket_name` | `/relic/data/bucket_name` — how the pipeline finds the bucket. |

Deleting this bucket or its contents is an **owner-only decision**. There is no `prevent_destroy` lifecycle block; the guardrail is a rule, not a lock. See the root [CLAUDE.md](../CLAUDE.md).

## `pipeline/` — ephemeral

Kinesis in, two paths out.

```
operator (Go, this Mac)
        │  PutRecords, partition key = session_id
        ▼
  aws_kinesis_stream.relic_stream          1 shard, PROVISIONED, 24h retention
        │
        ├─────────────► Firehose ──────► S3  (cold path)
        │               buffer 60s        raw NDJSON, Hive-partitioned
        │               NO transform      the replayable source of truth
        │
        └─────────────► Lambda ────────► DynamoDB  (hot path)
                        event source      relic-events   (24h TTL)
                        mapping           relic-sessions (7d TTL)
                              │
                              └── SQS DLQ on exhausted retries
```

**Both consumers read the same shard.** They are standard (shared-throughput) consumers, so they split the shard's 2 MB/s read budget. Invisible at single-user volume; the fix if it ever isn't is enhanced fan-out (`aws_kinesis_stream_consumer`), which gives each consumer its own 2 MB/s.

### Cold path — `firehose.tf`, `s3.tf`

Zero transformation, deliberately. Firehose has no processing configuration attached: what lands in S3 is the exact line the operator emitted. That is what makes history replayable when the parser vocabulary grows, and it is why phase 2's replay tool is possible at all. The operator's IP redaction is the single exception, and it happens *before* the stream.

CloudWatch logging is enabled because delivery failures are otherwise silent — Firehose retries for up to 24h and surfaces nothing.

### Hot path — `hot_path.tf`, `lambda.tf`, `dynamodb.tf`

An `aws_lambda_event_source_mapping` polls the stream and invokes `relic-hot-path`. Source lives in [`pipeline/lambda/hot-path/`](pipeline/lambda/hot-path/) — see its own README for the handler's design.

Settings that are load-bearing rather than defaults:

- `starting_position = "LATEST"` — the stack is destroyed and re-applied between sessions and the stream retains 24h, so `TRIM_HORIZON` would replay the previous session on every apply. Harmless (writes are idempotent on `(session_id, seq)`) but it burns invocations and makes "is this live?" unreadable during development.
- `bisect_batch_on_function_error` + `maximum_retry_attempts = 2` + a DLQ — Kinesis is ordered, so a batch that always throws blocks its shard until the records age out. Bisecting narrows toward the single bad record; the retry cap lets the shard advance; the DLQ records what was given up on. Without the DLQ, exhausted records vanish with no signal.
- `maximum_batching_window_in_seconds = 5` — cuts invocations on a sparse log without adding latency the dashboard would notice.

**The DLQ holds metadata, not payloads.** For stream sources, SQS receives the shard id, sequence-number range, and error — never the record body. That is the right shape here: S3 already holds every raw line, so the DLQ says *which* records to re-derive and the archive says *what they were*.

### DynamoDB

Two tables, both `PAY_PER_REQUEST`, both a **rolling cache** rather than an archive — anything here can be rebuilt by replaying S3.

| Table | Key | TTL |
|---|---|---|
| `relic-events` | PK `session_id`, SK `seq` | ~24h |
| `relic-sessions` | PK `session_id` | ~7d |

`relic-sessions` exists because a partition key can only be matched *exactly*, never enumerated — "what sessions exist?" is unanswerable against `relic-events` without scanning every item. The dashboard resolves a `session_id` from the small table, then queries the big one.

Two traps worth knowing, both silent when wrong:

- **`seq` is a zero-padded string, width 20.** String sort keys compare lexicographically, so `"10"` sorts before `"2"` unpadded. Width 20 is exactly `uint64` max, so overflow is impossible by construction. Nothing in DynamoDB or Terraform enforces the padding — an unpadded write succeeds and lands in the wrong sort position with no error anywhere.
- **`expires_at` is UNIX seconds.** Milliseconds — the natural reflex — put expiry ~50,000 years out and the item is never swept. TTL deletion is also *not prompt*: AWS sweeps within ~48h, and expired-but-unswept items still come back in queries. TTL is a storage-cost control, not a correctness guarantee.

## The stacks talk only through SSM

No cross-stack state references, no remote state data sources. Each parameter is owned by the stack whose lifecycle matches the value:

| Parameter | Written by | Read by |
|---|---|---|
| `/relic/data/bucket_name` | `data` | `pipeline` (as a data source) |
| `/relic/pipeline/kinesis_stream_name` | `pipeline` | the operator, at startup |

The stream name lives in the ephemeral stack because the stream does. With the pipeline destroyed, `./tail -sink=kinesis` fails fast at startup — which is the intended behavior, not a bug.

The pipeline reads the bucket as a **data source, never a managed resource**. An `import` block or an `aws_s3_bucket` resource here would put the persistent bucket inside the destroyable stack.

## Order of operations

**Apply:** `data` first, then `pipeline`. The pipeline's data-source lookup fails otherwise.

**Destroy:** `pipeline` only, and **wait ~2 minutes after the last operator activity** so Firehose flushes its buffer — otherwise the last records are lost.

**Destroy order matters and nothing enforces it.** Destroying `data` first strands the pipeline: its data sources can no longer resolve, so even `terraform destroy` fails to plan. Recovery is to recreate the bucket and SSM parameter (any bucket with the right name resolves — a data source is a lookup, not a reference), then destroy the pipeline normally.

## Conventions

- All AWS resources go through Terraform. Nothing created in the console or by CLI.
- `terraform validate` before claiming a stack works — it catches bad references without touching AWS. It does **not** contact AWS, so a clean validate says nothing about whether data actually flows.
- Any `Principal` that is an AWS service (`firehose.amazonaws.com`, `lambda.amazonaws.com`) is global, not account-scoped. Every such trust policy here carries an `aws:SourceAccount` condition to prevent the confused-deputy problem — see the entry in the root [README](../README.md#aws-service-principals-are-global--the-confused-deputy).
- Never commit `terraform.tfvars`, state files, or credentials.
