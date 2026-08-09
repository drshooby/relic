# CLAUDE.md — relic

Personal data-infra project: stream Warframe `EE.log` (+ EEG later) → Kinesis → S3/DynamoDB → live dashboard. Read [README.md](README.md) for the story and roadmap; the authoritative phase-1 design is [docs/superpowers/specs/2026-07-21-relic-phase1-design.md](docs/superpowers/specs/2026-07-21-relic-phase1-design.md).

**Status:** Phase 1, M3 complete. Both paths are **verified end-to-end** against live sessions. Cold path: the operator tails `EE.log`, redacts IPs, batches envelopes with `PutRecords`, and sessions land in S3 as valid newline-delimited JSON with every `seq` present exactly once — no loss, no duplicates. **Redaction is confirmed against real peer addresses** (481 redactions across 384 lines in one matchmade session; 217 in another, none surviving either time). Hot path: a live fissure session produced 6,332 items in `relic-events`, an empty DLQ, and a correctly parsed `reward.relic`. Records arrive out of `seq` order in the object (expected; `PutRecords` and Firehose do not preserve order), so consumers must sort by `(session_id, seq)` and never by file position. Next: M4's read API and dashboard.

**Hot-path invariants that fail silently — do not "simplify" these:**

- `seq` is a zero-padded string of width 20. Unpadded writes succeed and land in the wrong sort position with no error anywhere.
- `game_time_s` must be `Decimal(str(x))`. boto3 rejects Python floats outright, and `Decimal(x)` reintroduces the binary artifact. Getting this wrong DLQs every batch deterministically while the pipeline looks healthy.
- `expires_at` is UNIX **seconds**. Milliseconds put expiry ~50,000 years out and the item is never swept.
- Items must be deduped on `(session_id, seq)` before `BatchWriteItem` — a duplicate key fails all 25 items in the chunk, and at-least-once delivery makes duplicates normal.
- DynamoDB write failures must **raise**. That is what triggers bisect/retry/DLQ; swallowing them loses data silently.

## PII — treat game logs as sensitive

`EE.log` contains **real PII: IP addresses and machine names**. Verified against a real session (the community wiki also warns about an email in the header — this client logs none, so don't repeat that claim):

- The owner's **public IP**, the LAN address, and — matchmaking is peer-to-peer — **squadmates' IPs**. These appear *mid-session* in `Net` and `Game` lines, not only in the header, so header-only scanning is not enough.
- Machine name and Windows username in the header.
- Squad display names, player ids, and clan ids are treated as gameplay data, not identity, and are kept.

The operator redacts IP addresses at the source (`operator/cmd/tail/redact.go`) so nothing downstream ever stores one. That is a deliberate, documented exception to "cold path stores raw lines" — the invariant exists to keep history replayable, and no parser needs an IP to reconstruct a mission.

**Verifying redaction: `<ip>` does not appear literally in the archive.** Go's `encoding/json` escapes `<` and `>` unconditionally, so the placeholder is stored as `<ip>`. Grepping raw NDJSON for `<ip>` returns zero matches on a correctly-redacted file, which reads exactly like a redaction failure. Always unmarshal before checking, or grep for the escaped form — a bad grep here previously produced a confident, wrong "redaction never fired" conclusion.

Rules, no exceptions:

- **Never commit raw log content.** Any fixture derived from a real log must be sanitized first (scrub IP/email/hostnames) and reviewed before `git add`.
- **Never paste raw log lines/headers** into PRs, issues, artifacts, web content, or any external service.
- Raw data lands **only** in the private S3 data bucket. Never repoint the pipeline at a shared/public bucket; never widen bucket ACLs/policies.
- The live log lives outside the repo at
  `~/Library/Application Support/CrossOver/Bottles/Steam Library/drive_c/users/crossover/AppData/Local/Warframe/EE.log` — read-only access only; never lock, move, or write to it (the game is writing it).

## Workflow

- This project uses the superpowers flow: brainstorm → spec (`docs/superpowers/specs/`) → implementation plan → TDD. Follow it for every phase/feature.
- The owner is learning streaming data infra — when touching streaming concepts (serving layer, checkpointing, at-least-once, etc.), briefly explain the vocabulary rather than assuming it.
- Design invariants to preserve (see spec for rationale):
  - Operator stays dumb: envelope only, no parsing. Parsing lives in the hot path.
  - Cold path stores raw, untransformed lines — it is the replayable source of truth.
  - `(session_id, seq)` is the ordering/idempotency key; delivery is at-least-once everywhere.
  - Envelope changes must bump `v` and stay backward-compatible (old raw data must remain replayable).
- **Deliberate deviation from the spec: no operator disk spool.** The design calls for spooling batches to disk on Kinesis failure; only the backoff was built. `EE.log` is itself durable, so recovery is `./tail -once -sink=kinesis` and at-least-once makes the duplicates harmless. The deeper reason is scope — local durable buffering is an agent/SRE concern, and this project's learning goal is the data-engineering work downstream of the stream. Don't "fix" this by building the spool; see [operator/README.md](operator/README.md#deviation-from-the-spec-no-disk-spool).

## Terraform & AWS hygiene

- Two stacks: `infra/data` (persistent; holds the data bucket, TF state bucket, SSM param) and `infra/pipeline` (ephemeral — destroyed whenever idle; the owner does this habitually).
- Deleting the data bucket or its contents is an **owner-only decision**. Agents must never destroy `infra/data`, empty the data bucket, or weaken its protections on their own — only on David's explicit instruction. (No hard `prevent_destroy` required; the guardrail is "only David deletes data," not "data is undeletable.")
- Destroy of the pipeline must wait for Firehose to flush (~2 min after last activity) before `terraform destroy`, or the last buffered records are lost.
- The stacks communicate only through SSM parameters, each owned by the stack whose lifecycle matches the value:
  - `/relic/data/bucket_name` — written by `infra/data`, **read** by the pipeline as a data source.
  - `/relic/pipeline/kinesis_stream_name` — written by `infra/pipeline`, read by the operator at startup. It lives in the ephemeral stack because the stream does; with the pipeline destroyed, `-sink=kinesis` fails fast at startup, which is the intended behavior.
- The pipeline reads the bucket as a **data source**, never a managed resource — an `import` block or `resource "aws_s3_bucket"` in `infra/pipeline` would put the persistent bucket in the destroyable stack. `infra/data` must be applied first or the pipeline's data source lookup fails.
- **Destroy order matters and nothing enforces it.** Destroying `infra/data` first strands the pipeline: its data sources can no longer resolve, so even `terraform destroy` fails to plan. Recovery is to recreate the bucket and SSM param (any bucket with the right name resolves — a data source is a lookup, not a reference), then destroy the pipeline normally.
- All AWS resources go through Terraform — no console/CLI-created resources.
- Cost discipline: no always-on compute, no VPC/NAT, nothing that bills while idle. If a change adds standing cost, flag it to the owner before applying.
- **The Kinesis shard is the one thing that bills while idle** — provisioned mode, 1 shard, $0.015/shard-hour (~$11/month) from apply until destroy, regardless of traffic. This is why the pipeline gets torn down between sessions.
- IAM: any `Principal` that is an AWS service (`firehose.amazonaws.com`, `lambda.amazonaws.com`, …) is global, not account-scoped — it must be narrowed with an `aws:SourceAccount`/`aws:SourceArn` condition on the trust policy, and `source_account` on any `aws_lambda_permission`. See the confused-deputy entry in [README.md](README.md#lessons-learned).
- Run `terraform validate` before claiming a stack works; it catches bad references without touching AWS. Note it does **not** contact AWS, so a clean validate says nothing about whether data actually flows.
- Never commit: `terraform.tfvars`, state files, AWS credentials, real log data.

## Conventions

- Languages: Go (`operator/`), Python (`pipeline/`), TypeScript (`dashboard/`). Keep each component in its directory; they communicate only via the stream/tables/API, never by importing each other.
- TDD: tests first; sanitized fixtures only.
- The game runs on **this Mac** via CrossOver — operator code must build/run natively on macOS (arm64).