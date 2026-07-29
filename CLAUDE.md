# CLAUDE.md — relic

Personal data-infra project: stream Warframe `EE.log` (+ EEG later) → Kinesis → S3/DynamoDB → live dashboard. Read [README.md](README.md) for the story and roadmap; the authoritative phase-1 design is [docs/superpowers/specs/2026-07-21-relic-phase1-design.md](docs/superpowers/specs/2026-07-21-relic-phase1-design.md).

**Status:** Phase 1, M2 in progress. M1 (Go operator) is built — tails `EE.log`, redacts IPs, emits versioned envelopes to stdout. Both Terraform stacks apply cleanly and the Kinesis → Firehose → S3 path is wired, but **nothing has been verified end-to-end**: no record has flowed through the stream yet. The operator does not write to Kinesis (still stdout only). Next: manual `put-record` smoke test, then the operator's Kinesis producer.

## PII — treat game logs as sensitive

`EE.log` contains **real PII: IP addresses and machine names**. Verified against a real session (the community wiki also warns about an email in the header — this client logs none, so don't repeat that claim):

- The owner's **public IP**, the LAN address, and — matchmaking is peer-to-peer — **squadmates' IPs**. These appear *mid-session* in `Net` and `Game` lines, not only in the header, so header-only scanning is not enough.
- Machine name and Windows username in the header.
- Squad display names, player ids, and clan ids are treated as gameplay data, not identity, and are kept.

The operator redacts IP addresses at the source (`operator/cmd/tail/redact.go`) so nothing downstream ever stores one. That is a deliberate, documented exception to "cold path stores raw lines" — the invariant exists to keep history replayable, and no parser needs an IP to reconstruct a mission.

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

## Terraform & AWS hygiene

- Two stacks: `infra/data` (persistent; holds the data bucket, TF state bucket, SSM param) and `infra/pipeline` (ephemeral — destroyed whenever idle; the owner does this habitually).
- Deleting the data bucket or its contents is an **owner-only decision**. Agents must never destroy `infra/data`, empty the data bucket, or weaken its protections on their own — only on David's explicit instruction. (No hard `prevent_destroy` required; the guardrail is "only David deletes data," not "data is undeletable.")
- Destroy of the pipeline must wait for Firehose to flush (~2 min after last activity) before `terraform destroy`, or the last buffered records are lost.
- The stacks share exactly one string: the SSM parameter `/relic/data/bucket_name`. The pipeline reads the bucket as a **data source**, never a managed resource — an `import` block or `resource "aws_s3_bucket"` in `infra/pipeline` would put the persistent bucket in the destroyable stack. `infra/data` must be applied first or the pipeline's data source lookup fails.
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