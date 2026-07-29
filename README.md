# relic

A personal data-infrastructure project: stream and fuse **Warframe gameplay telemetry** and **EEG headset data** into a live dashboard, built on AWS streaming primitives and Terraform.

## Why

Three interests colliding:

- **Human sciences** — what does my brain actually do during a long survival mission vs. cracking a void relic and getting a 1% chance gold reward? EEG + game telemetry on one timeline can answer that.
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

What's actually deployed today:

```
infra/data/                        infra/pipeline/
  s3.tf      data bucket             kinesis.tf    1 provisioned shard
             versioned, closed       firehose.tf   Kinesis source → S3, 60s buffer
  ssm.tf     /relic/data/            s3.tf         data source (read-only)
             bucket_name             ssm.tf        reads the shared param
  iam.tf     (no principals)         iam.tf        one Firehose role, two policies
```

The pipeline holds the bucket as a **data source**, never a managed resource. That's the load-bearing detail: an `import` block or a `resource "aws_s3_bucket"` here would enroll the persistent bucket in the stack that gets destroyed between sessions.

The Kinesis shard is the only thing that bills while idle — $0.015/shard-hour, ~$11/month. Everything else is per-request or storage, which is what makes the teardown habit worth keeping.

## Roadmap

### Phase 1 — Ingestion (current)

The serverless streaming backbone described above.

- **M1** ✅ — Go operator tails the real EE.log through a live play session (truncation-safe, offline, envelopes to stdout, IPs redacted at the source)
- **M2** _(in progress)_ — Terraform stacks up; operator → Kinesis → Firehose → raw lines land in S3. Both stacks apply and the Kinesis → Firehose → S3 path is wired; not yet verified end-to-end, and the operator still writes to stdout rather than Kinesis.
- **M3** — Hot-path Lambda parses into DynamoDB; events queryable seconds after they happen in-game
- **M4** — Custom dashboard shows the live session feed

### Phase 2 — Lakehouse & replay

Turn the raw S3 archive into a real analytical layer: Glue catalog, JSON→Parquet compaction, date/session partitioning, Athena. Build a **replay tool** to reprocess historical raw logs through new parser versions — the payoff of the "store raw everything" decision.

### Phase 3 — EEG fusion

Second producer: EEG headset (hardware TBD). High-frequency continuous signal alongside sparse discrete game events — clock alignment across sources, windowed aggregation (e.g., 1s band-power buckets), and a dashboard timeline that overlays brain state on gameplay events.

### Phase 4 — Consumer upgrade (optional)

Swap the hot-path Lambda for a real stream-processing consumer (Flink or KCL), spun up per-session to keep costs near zero. Same data, heavier machinery — a deliberate compare-and-contrast with the serverless consumer model.

## Lessons learned

Things this project taught me that I didn't know going in.

### AWS service principals are global — the confused deputy

A trust policy naming a service looks account-scoped. It isn't:

```hcl
Principal = { Service = "firehose.amazonaws.com" }
```

There is one `firehose.amazonaws.com` shared by every AWS account on Earth. That statement means _any_ Firehose stream, in _anyone's_ account, may assume this role.

The attack: someone creates a delivery stream in their own account and sets the role ARN to mine. Their Firehose calls `sts:AssumeRole`; my trust policy asks only "is the caller Firehose?" — and it genuinely is. STS issues credentials, and their stream now writes into my bucket. Nothing was spoofed, no credentials leaked. The service was simply _confused_ about whose behalf it was acting on — the same shape as CSRF, or a SUID binary that takes a path argument.

There's no "are you allowed to reference this role?" check anywhere, because the role lives in my account and their config lives in theirs. **The trust policy is the only gate.** Closing it means adding a condition:

```hcl
Condition = {
  StringEquals = { "aws:SourceAccount" = data.aws_caller_identity.current.account_id }
}
```

Details worth getting right, all of which I got wrong first:

- **`aws:SourceAccount`, not `sts:ExternalId`.** `sts:` keys only exist during the AssumeRole call; `ExternalId` is for cross-account third-party access, where a human on the vendor's side was told the value. Nothing populates it for a service principal — so that condition matches nothing and fails closed, which looks like a mysterious access-denied at delivery time rather than an apply error.
- **On the trust policy, not the permission policy.** Same reason: by the time the role is calling `s3:PutObject`, there is no `sts:` context left in the request.

The same bug has a second face: **resource** policies. An `aws_lambda_permission` with `principal = "s3.amazonaws.com"` and no `source_account` lets anyone's bucket invoke my function. Trust policies gate who can _wear_ an identity; resource policies gate who can _invoke_ me. Most write-ups only cover the first.

Lambda raises the stakes for a different reason than I expected. `iam:PassRole` actually makes the classic version _harder_ — you can't point your Lambda at my execution role. The real escalation is that a Lambda role sits behind arbitrary code: any dependency vulnerability or injection bug becomes full use of that role, and credentials are readable straight out of the execution environment's env vars. So a compromised Lambda doesn't just act as the role, it hands the role's credentials to someone else's shell. Firehose can't be made to run code; that's the whole difference.

General rule: **a service principal in any policy — trust or resource — is an open door until a condition narrows it.**

## Notes & constraints

- EE.log facts (verified against a real session, which corrected several claims the community wiki makes): truncated on every game launch; writes can lag ~10s (engine buffering); header contains the absolute launch time, which anchors every relative timestamp to UTC — the key to cross-source fusion later.
- **PII is IP addresses, not email.** The wiki warns about an email in the header; this client logs none. It does log the owner's public address, the LAN address, and — because matchmaking is peer-to-peer — squadmates' addresses, and they appear mid-session in `Net` _and_ `Game` lines, not just the header. The operator strips them at the source, so nothing downstream ever stores one. Squad display names and player ids are kept: they are gameplay data, not identity.
- Data lands only in a private bucket; committed test fixtures are sanitized.
- Languages: Go (operator), Python (stream processing), TypeScript (dashboard) — right tool per layer.
