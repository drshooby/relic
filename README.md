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
                                     Dashboard (Vite/React + API) ─────┘  ~2s polling during a session
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
  iam.tf     (no principals)         dynamodb.tf   events + sessions, TTL'd
                                     hot_path.tf   event source mapping + DLQ
                                     api.tf        HTTP API, 2 routes
                                     lambda.tf     hot-path + api functions
                                     iam.tf        one role each, all scoped
```

The pipeline holds the bucket as a **data source**, never a managed resource. That's the load-bearing detail: an `import` block or a `resource "aws_s3_bucket"` here would enroll the persistent bucket in the stack that gets destroyed between sessions.

The Kinesis shard is the only thing that bills while idle — $0.015/shard-hour, ~$11/month. Everything else is per-request or storage, which is what makes the teardown habit worth keeping.

## Roadmap

### Phase 1 — Ingestion (current)

The serverless streaming backbone described above.

- **M1** ✅ — Go operator tails the real EE.log through a live play session (truncation-safe, offline, envelopes to stdout, IPs redacted at the source)
- **M2** ✅ — operator → Kinesis → Firehose → S3, verified end-to-end against two live sessions (11,629 and 7,876 lines). Every `seq` present exactly once in both: no loss, no duplicates, every line independently parseable. IP redaction is confirmed against real peer addresses — a matchmade session produced 481 redactions across 384 lines with none surviving. The operator batches with `PutRecords` (500 records / 4MB / one poll tick, whichever comes first), retries only the individually-failed records with exponential backoff, and reads its stream name from SSM. Records arrive out of `seq` order in the object — `PutRecords` and Firehose preserve neither, which is exactly why `(session_id, seq)` is the ordering key rather than file position.
- **M3** ✅ — Hot-path Lambda parses into DynamoDB, verified end-to-end against a live fissure session: 6,332 events in `relic-events`, an empty DLQ, and the relic reward parsed correctly (`reward.relic`, `GyrePrimeSystemsBlueprint`, `game_time_s` 186.318). Both paths agree — the same session landed in S3 as 4,278 valid NDJSON lines, seq 0–4277 with zero gaps and 217 redactions with none surviving. A `seq BETWEEN` query returned the full 21-line reveal window in order, separating stage one (your roll, +109ms) from stage two (squadmates' rolls, +149ms) — the two-stage split the per-line design exists to preserve. `event_count` read 6,372 against 6,332 stored: the documented at-least-once drift, ~0.6%, observed rather than assumed.
- **M4** ✅ — Read API (API Gateway HTTP API + read-only Lambda) and a Vite + React dashboard polling it every 2s. Verified against the live stack: `GET /sessions` returns 200, an unknown session 404, and a session whose events aged out returns 204 with no body — the three codes the UI's live/recent/expired states are inferred from, with no extra stored field. The dashboard borrows the game's downed/dead mechanic for the case a log goes quiet: after 30s it spends one silent auto-revive, then prompts, and a session confirmed finished stops polling entirely rather than hammering the API while you have walked away.

### Phase 2 — Lakehouse & replay

Turn the raw S3 archive into a real analytical layer: Glue catalog, JSON→Parquet compaction, date/session partitioning, Athena. Build a **replay tool** to reprocess historical raw logs through new parser versions — the payoff of the "store raw everything" decision.

Background reading before the spec: [docs/PHASE2_LEARNING.md](docs/PHASE2_LEARNING.md) — the lakehouse/replay vocabulary from a cloud-infra starting point, and the open question of whether the deployed S3 layout (ingestion-time, no `session_id`) or the spec's (event-time, session-partitioned) is the one phase 2 builds on.

### Phase 3 — EEG fusion

Second producer: EEG headset (hardware TBD). High-frequency continuous signal alongside sparse discrete game events — clock alignment across sources, windowed aggregation (e.g., 1s band-power buckets), and a dashboard timeline that overlays brain state on gameplay events.

The relic reward reveal is the anchor event, and it turns out to be **two** events ~113–285ms apart: your own roll landing (absolute evaluation — "is my drop good?") and squadmates' rolls loading (relative re-evaluation — "is mine good *compared to theirs*?"). Same item, potentially opposite valence, independently randomized. That is a natural 2×2 the game generates for free.

Stage one is fully labelled today. Stage two has **timing but not labels**: `EE.log` records when squadmates' rewards arrive, not what they were.

That gap matters less than it first appears. The dashboard needs only the timestamps — two response spikes of differing magnitude, against a reveal you just watched, tells the story without any rarity annotation. Labels are needed for *averaging across trials*, which is a later concern.

Nothing in the log states a reward's rarity. Relic **projection** paths (`.../T2VoidProjectionProteaPrimeABronze`) look like they do — four of them, one per player, each naming a Prime item and a tier — but they name the relics the squad *equipped*, not what those relics rolled. The run that ended with a Gauss Prime Chassis lists no Gauss projection at all. Labels need an outside source: manual annotation, Warframe API inventory diffing, or OCR of the reveal screen.

The hot path stores one event per line specifically so labels from any of those sources can be joined onto timestamps already in the archive.

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

### Ask which component owns durability

First end-to-end smoke test: two records `put-record`'d into Kinesis, nothing in S3. My read was "Firehose never received them." Wrong, and the wrong question.

The cause was mundane — `iam.tf` defined an S3 delivery policy and never attached it to the Firehose role. The Kinesis read policy had an `aws_iam_role_policy_attachment`; the S3 one didn't. An unattached policy is a perfectly valid resource, so `terraform validate` and `plan` are both silent about it. Two symptoms made it hard to see:

- **Firehose fails silently by design.** On `AccessDenied` it neither errors nor drops — it retries for up to 24h. Correct for a transient S3 blip, but a permanent permission error looks identical to nothing happening. Fixing that is a config flag: `cloudwatch_logging_options` turns silence into a readable `AccessDenied`. It's off by default, which means the default posture is undebuggable.
- **Records were still readable in the stream after Firehose had read them.** I took that as proof Firehose hadn't consumed anything. It proves nothing — reads don't consume in Kinesis.

That last one is the actual lesson. Firehose *had* received both records, buffered them, and was retrying. But its buffer is short-lived working memory — no API to inspect it, no dead-letter queue here, and a bounded retry window after which the data is simply gone. Firehose was never where the records were safe.

Kinesis was. 24h retention, replicated across three AZs, and a consumer reading a record doesn't remove it — each consumer tracks its own position, so many can read the same stream independently. Firehose is just one consumer with one cursor.

```
Kinesis (durable, 24h)  ← the records were always here
   └─▶ Firehose (buffered, retrying, nothing durable)
          └─▶ S3  ✗ AccessDenied
```

So S3 was empty while Kinesis looked healthy, and no data was ever lost — the retry loop was stalled, not failing. Attaching the policy inside the retention window let the original records land on their own; no third `put-record` needed. At-least-once delivery recovering on its own, which is the same property `(session_id, seq)` exists to make safe.

General rule: **in a streaming pipeline, ask which component owns durability.** Every stage downstream of it is reprocessable as long as you're inside the retention window; every stage upstream of an empty destination is worth checking before assuming data is gone. That's the same property Phase 2's replay tool depends on, and the reason the cold path stores raw lines. A day later, both the Kinesis retention and the Firehose retry window would have expired and the records really would have been gone.

### A 200 from `PutRecords` does not mean the records landed

Batching the operator's writes meant moving from `PutRecord` to `PutRecords`, which I read as "same call, takes a slice." It isn't. `PutRecords` returns **HTTP 200 with individual records failed inside the response body**:

```
{ FailedRecordCount: 47, Records: [ {SequenceNumber: "..."}, {ErrorCode: "ProvisionedThroughputExceededException"}, ... ] }
```

A shard can throttle one record while accepting its neighbour, so partial success is the *normal* case under load, not an edge case. Three things follow, and I'd have got all three wrong:

- **`err == nil` is not success.** You have to check `FailedRecordCount` separately. Code that only checks the error silently drops records — the worst possible failure for a pipeline whose whole premise is a replayable archive.
- **The SDK's built-in retry doesn't help.** It retries failed *calls*. This call succeeded. The gap is invisible unless you know to look for it.
- **The response says which records failed, not what they were.** `Records[i]` in the response is positionally matched to `Records[i]` in the request, and carries only an error code — no data. The retry batch has to be rebuilt by index against the input you still hold.

So the retry loop resends only the entries whose `ErrorCode` is set, with exponential backoff and jitter, and gives up after a bounded number of attempts rather than looping forever. Duplicates from a retry whose ack was lost are fine — `(session_id, seq)` is the idempotency key, and that is exactly what it's for.

General rule: **for any batch API, find out what a partial failure looks like before trusting the status code.** The single-record version of the call taught me nothing about the batch version.

### Adding a buffer moves the flush responsibility to the caller

The operator originally wrote one record per line, straight to the network. Batching it — accumulate in `Emit`, send in `Flush` — is an obvious win: `PutRecords` bills per 25KB payload unit, so single-record puts of a few hundred bytes waste ~98% of a paid unit, and the per-shard limit is 1000 records/sec but only 200 API calls/sec.

What I missed is that batching changes the contract. `Flush` had exactly one caller: `Shutdown`. That was fine while the only sink was stdout, because `bufio.Writer` self-flushes when its buffer fills — the sink quietly guaranteed progress on its own. A batching network sink makes no such promise. Nothing would have reached Kinesis until Ctrl-C, and a crash would have lost the entire session.

The fix is small — flush on each poll tick — but it isn't an optimization for the idle case, which is how I first framed it. Without it the design is simply incorrect. Two consequences worth writing down:

- **Three flush triggers, not one.** Record count and payload bytes have to fire inside `Emit` (the API caps a call at 500 records / 5MB), while the timer handles the log going quiet between missions. The tick alone can't bound batch size; the size caps alone can't bound staleness.
- **A failed flush must not kill the tailer.** Kinesis being briefly unavailable is backpressure, not an outage — the records stay buffered and go out on the next tick. But "never give up" means an unbounded buffer, so there's a ceiling past which the operator exits loudly. Losing records unnoticed is worse than stopping.

General rule: **when a component starts buffering, ask who is now responsible for making it drain** — and what happens when draining fails.

### Verify the verification — a bad grep invented a bug

The operator replaces IP addresses with `<ip>` before anything leaves the machine. Checking that in the S3 archive looked trivial:

```sh
grep -c '<ip>' session.ndjson    # 0
```

Zero. On a session with 828 `Net` lines, NAT traffic, and squad messages. I had a tidy explanation ready — peer addresses only appear once matchmaking connects, so a session that never squadded up would have nothing to redact — and it was completely wrong.

Go's `encoding/json` escapes `<` and `>` unconditionally, as an XSS precaution for JSON embedded in HTML. The placeholder is stored as `<ip>`. Searching for the literal string returns zero on a *perfectly redacted* file, which is indistinguishable from redaction never having run. The real count was 481 redactions across 384 lines.

Two things went wrong, and the second is the worse one:

- **The measurement was broken in the direction that looks like a real finding.** A false "0 matches" reads as a bug in the thing being tested, not a bug in the test. Had it come back with an inflated number I'd have questioned the grep immediately.
- **I explained the bad number instead of checking it.** A plausible story arrived before verification did, and it was persuasive enough to stop the investigation. The tell was available the whole time: 828 `Net` lines and 58 "public address" entries are not what "no network activity" looks like.

The fix is to check the pipeline end to end on data where the answer is already known — mask the digits and *look* at a line rather than counting matches:

```sh
grep -o '"raw":"[^"]*public address[^"]*"' session.ndjson | head -1 | sed -E 's/[0-9]/N/g'
# "raw":"NN.NNN Net [Info]: ... public address \uNNNcip\uNNNe:NNNN"
```

General rule: **when a check reports that something is broken, confirm the check works before believing it.** A verification step is code too, and a plausible explanation for a bad measurement is not evidence — it is the thing that stops you from taking the measurement again.

### An empty grep measures the session, not the format

Designing the hot path's parser meant confirming what a relic reward actually looks like in `EE.log`. The spec quotes the line, so this was a formality:

```sh
grep -c VoidProjections EE.log    # 0
```

Zero, in an 8,217-line log with three completed missions. I concluded the spec's example was stale — written from an older client, or transcribed from the wiki — and started redesigning the parser around `EndOfMatch.lua`, which was abundantly present.

Wrong. Relic rewards only appear in **Void Fissure** missions. Those three completed missions were ordinary ones, so the log was correct and complete; there was simply nothing to log. Running a single fissure produced 15 `VoidProjections` lines matching the spec's format exactly, down to the `Sys [Info]` subsystem.

This is the same failure as the redaction grep above, with the roles reversed: there the *measurement* was broken, here the measurement was fine and the **sample** didn't contain the phenomenon. Both produce a confident zero, and both invite a story that explains the zero instead of questioning it.

What made it recoverable was cheap: play one fissure. Ten minutes of gameplay settled a question I had been answering by inference. When the data is *generated on demand*, generating more of it beats reasoning about why it's missing.

Two durable notes for anyone touching the parser:

- **Absence of a game-specific line is not evidence about its format** unless the session actually contained that activity. Check what the session *did* before concluding what the log *can't* do.
- `EE.log` is truncated on every game launch, so the evidence is perishable. The 8,217-line session is gone; copy the file somewhere outside the repo before relaunching if it matters.

### A test double that models the API's shape but not its contracts

The hot path shipped with 35 passing tests and would have written **zero events** in production.

`game_time_s` was passed to DynamoDB as a Python float. boto3's resource layer rejects that outright — `TypeError: Float types are not supported. Use Decimal types instead.` — because DynamoDB's Number type is exact decimal and boto3 refuses to guess what precision a binary float was supposed to mean. Almost every log line carries a game clock, so almost every record would have raised. Worse, it fails *deterministically*: the event source mapping's `bisect_batch_on_function_error` exists to isolate one poison record, but when every record is poison, bisecting just subdivides its way to the DLQ. From the operator's side everything looks healthy — records leave, Firehose archives them to S3, and the serving layer is simply always empty.

A second bug hid the same way: two copies of one `(session_id, seq)` in a single batch make `BatchWriteItem` reject **all 25 items in the chunk**, not just the duplicate. That is reachable in normal operation, since Kinesis is at-least-once and the operator re-sends a whole buffered batch after a failed flush.

Both bugs had the same root cause, and it was not the production code. The hand-rolled DynamoDB fake modelled the *shape* of boto3's API — the right method names, the right call signatures — but none of its *contracts*:

| Real boto3 | The fake |
|---|---|
| raises `TypeError` on any float | stored it happily |
| rejects duplicate keys in one batch | silently overwrote (dict-keyed!) |
| buffers, flushes at 25 or `__exit__` | raised on the first `put_item` |

The fake's dict-keyed storage is the detail worth staring at. Keying stored items by `(session_id, seq)` looks like faithfully modelling a DynamoDB table — and it silently implemented the *opposite* of the real API's duplicate behaviour. A test asserting "3 records in, 3 items stored" passed for the wrong reason.

There was even a test named `test_build_event_item_preserves_float_precision` asserting `item["game_time_s"] == 240.623`. It didn't just miss the bug; it **pinned it in place**. Any attempt to fix the type would have broken a green test.

The fix was to make the fake enforce all three contracts, then watch eleven previously-passing tests fail. Two general rules came out of it:

- **A fake is only as good as the contract it enforces, not the interface it mimics.** When faking a third-party client, encode what it *rejects*, not just what it accepts. The rejections are the part your code has never been tested against.
- **Verify one real call before trusting a suite of fake ones.** A single `TypeSerializer().serialize(item)` against real boto3 — no AWS account, no network — would have caught this on day one. The E2E harness would have caught it too; it was deferred, which is exactly why the cheap local check mattered.

## Notes & constraints

- EE.log facts (verified against a real session, which corrected several claims the community wiki makes): truncated on every game launch; writes can lag ~10s (engine buffering); header contains the absolute launch time, which anchors every relative timestamp to UTC — the key to cross-source fusion later.
- **Relic rewards (`VoidProjections`) — verified against a live fissure run.** The reveal is a ~464ms burst on the `Sys` subsystem, not `Script`:

  ```
  240.444  VoidProjections: OpenVoidProjectionRewardScreenRMI       reveal screen opens
  240.623  VoidProjections: <player_id> gets reward /Lotus/StoreItems/.../FulminPrimeBarrel
  240.736  VoidProjections: Client got reward info from <player_id>  (squadmate, no item path)
  240.908  VoidProjections: Client has reward info for all players now
  ```

  Two limits worth knowing before building anything on this. **Only your own item path is logged** — squadmates' rolls arrive as `Client got reward info from <id>` with no item, so the four-way reward table on screen is not recoverable from the log. And **the selection is never logged**: Warframe lets you take any squadmate's roll, so what you actually walk away with does not appear. A run that rolled a Fulmin Prime Barrel and ended with a Gauss Prime Chassis leaves only the Fulmin in the log. Acquisitions need a second source (inventory diffing, or manual annotation).

  For EEG correlation this is still a clean anchor: screen-open is `t=0`, your own outcome lands ~180ms later, and the full reveal completes ~464ms in. Item paths use legacy internal names (`FulminPrimeBarrel`), so a path → display-name mapping is needed and cannot be produced by transliterating the path.

- **PII is IP addresses, not email.** The wiki warns about an email in the header; this client logs none. It does log the owner's public address, the LAN address, and — because matchmaking is peer-to-peer — squadmates' addresses, and they appear mid-session in `Net` _and_ `Game` lines, not just the header. The operator strips them at the source, so nothing downstream ever stores one. Squad display names and player ids are kept: they are gameplay data, not identity.
- Data lands only in a private bucket; committed test fixtures are sanitized.
- Languages: Go (operator), Python (stream processing), TypeScript (dashboard) — right tool per layer.
