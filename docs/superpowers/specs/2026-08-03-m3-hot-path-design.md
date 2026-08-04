# M3 — Hot path handler design

**Status:** designed, not implemented.
**Supersedes:** the hot-path event vocabulary in [phase-1 design §5](2026-07-21-relic-phase1-design.md).
**Depends on:** the M3 infrastructure already applied in `infra/pipeline` (event source mapping, DynamoDB tables, SQS DLQ, IAM).

The goal is the smallest handler that turns a Kinesis record into a queryable DynamoDB row, plus exactly one parsed event type verified against real gameplay.

## Why the vocabulary changed

The phase-1 design proposed `session.start`, `mission.start`, `mission.end`, `log.line`. Two of those are dropped.

The project's purpose is correlating EEG against gameplay, and that needs **moments with an expected neural signature** — a discrete instant you can window around. Mission boundaries are state transitions: useful for segmenting a session into intervals, but nothing happens neurologically when a loading screen ends. Designing the first parser around them would have produced correct code aimed at the wrong target.

The relic reward reveal is the opposite: a randomized outcome with real emotional valence, resolving in under half a second, with a precise timestamp. That is a textbook reward-positivity paradigm, and the game generates the trials for free.

Mission boundaries remain worth capturing eventually — not as correlation targets but as **segmentation**, so EEG can be sliced per mission. They are recoverable from the raw lines in S3 whenever needed, so deferring costs nothing.

## Verified line formats

Confirmed against a live Void Fissure run on 2026-08-03. The reveal is a ~464ms burst on the `Sys` subsystem (**not** `Script`, which the phase-1 design's prose implied):

```
240.444  Sys [Info]: VoidProjections: OpenVoidProjectionRewardScreenRMI
240.458  Sys [Info]: VoidProjections: GetVoidProjectionRewards
240.623  Sys [Info]: VoidProjections: <player_id> gets reward /Lotus/StoreItems/Types/Recipes/Weapons/WeaponParts/FulminPrimeBarrel
240.623  Sys [Info]: VoidProjections: Client got reward info from <player_id>
240.623  Sys [Info]: VoidProjections: Still waiting on response from <player_id>   (x3)
240.736  Sys [Info]: VoidProjections: Client got reward info from <player_id>
240.908  Sys [Info]: VoidProjections: Client has reward info for all players now
```

Two limits that constrain what can ever be built on this:

- **Only your own item path is logged.** Squadmates' rolls appear as `Client got reward info from <id>` with no item. The four-way reward table visible on screen is not recoverable from `EE.log`.
- **The selection is never logged.** Warframe lets you take any squadmate's roll, so what you actually acquire does not appear. The verifying run rolled a Fulmin Prime Barrel and ended with a Gauss Prime Chassis; only the Fulmin is in the log. Acquisitions need a second source (inventory diffing or manual annotation) — out of scope for phase 1.

Relic rewards appear **only in Void Fissure missions**. Their absence from an ordinary session says nothing about the format; see the README lesson on this, which cost a redesign.

## Architecture

Three modules under `infra/pipeline/lambda/hot-path/`. The location is a deliberate deviation from phase-1 §5's `pipeline/hot-path/`: keeping the source beside the Terraform that packages it lets `archive_file` use a clean relative path, where a sibling top-level directory would need `${path.module}/../../`. Revisit if the hot path ever becomes its own Terraform module.

```
main.py     handler: decode batch -> parse -> write. The only module importing boto3.
parser.py   raw line -> (event_type, attrs). Pure functions.
items.py    envelope + parse result -> DynamoDB item shapes. Pure functions.
```

The split exists so the logic worth testing is testable without AWS. `parser.py` and `items.py` are pure functions over strings and dicts — no mocking, no moto. `main.py` holds everything AWS-facing and stays thin enough to verify by reading.

### parser.py

An ordered matcher table; first match wins, no match falls through to `log.line`:

Each entry is `(pattern, event_type, enrich)`, where `enrich` is an optional pure function refining the regex's named groups into the final `attrs`:

```python
def _relic_reward(g: dict) -> dict:
    return g | {"item_name": g["item_path"].rsplit("/", 1)[-1]}

MATCHERS = [
    (re.compile(r'VoidProjections: (?P<player_id>\w+) gets reward (?P<item_path>/Lotus/\S+)'),
     "reward.relic", _relic_reward),
]

def parse(raw: str) -> tuple[str, dict]:
    for pattern, event_type, enrich in MATCHERS:
        if m := pattern.search(raw):
            attrs = m.groupdict()
            return event_type, enrich(attrs) if enrich else attrs
    return "log.line", {}
```

Adding an event type is appending a tuple, with `enrich = None` when the named groups suffice. This is the growth path phase-1 §5 asks for, and phase 2's replay tool re-derives history against each new parser version.

`_relic_reward` derives `item_name` as the final path segment (`FulminPrimeBarrel`). It is trivially re-derivable, so storing it loses nothing and saves every consumer a string split. The **display** name (`Fulmin Prime Barrel`) requires the path -> name mapping phase-1 §5 warns about — item paths use legacy internal names (`Helmet` where the UI says *Neuroptics*) — and stays out of the hot path entirely.

### Event granularity: one event per line

Every log line becomes its own DynamoDB item with its own timestamp. The reveal sequence is **not** collapsed into a single synthesized `reward.reveal` event.

Aggregation is a one-way door. Collapsing asserts the 464ms of choreography is noise; if that is wrong, recovering it means reprocessing from S3. Keeping per-line events is observation rather than judgment, and the dashboard can group them in a query. The ~113ms between your own roll landing and the next squadmate's arriving is the window where you react to your own outcome alone — plausibly real signal, and untestable if discarded.

Synthesizing a single event would also require the handler to hold state across records, which is unsound: records arrive out of order and batches can be retried.

## DynamoDB item shape

```python
{
  "session_id":    "a1b2...",              # PK, from envelope
  "seq":           "00000000000000000042", # SK, zero-padded to width 20
  "event_type":    "reward.relic",
  "raw":           "240.623 Sys [Info]: VoidProjections: ...",
  "attrs":         {"player_id": "...", "item_path": "/Lotus/...",
                    "item_name": "FulminPrimeBarrel"},
  "game_time_s":   240.623,                # omitted when null
  "wall_time_utc": "2026-08-03T19:12:44Z", # omitted when null
  "v":             1,                      # envelope version, carried through
  "expires_at":    1754341234,             # now + 24h, UNIX SECONDS
}
```

- **`seq` pads to width 20.** String sort keys compare lexicographically, so unpadded values order wrongly (`"10"` before `"2"`). Width 20 is exactly `uint64` max, so the ceiling is unreachable by construction. Nothing enforces this but the code and its tests — an unpadded write succeeds and lands in the wrong sort position silently.
- **`raw` is stored on every item**, duplicating S3. It makes the dashboard useful for lines the parser does not recognize, and DynamoDB is a 24h rolling cache, so the cost is negligible.
- **Null clocks are omitted, not stored as null.** Lines emitted before the header is parsed carry no timestamp. DynamoDB distinguishes absent from null; absent is the honest encoding.
- **`game_time_s` stays a float.** DynamoDB Numbers are exact decimal, so `240.623` round-trips without IEEE754 error. This is the EEG alignment key.
- **`expires_at` is UNIX seconds.** Milliseconds — the natural reflex — yields a date ~50,000 years out and the item is never swept, with no error.

`relic-sessions` gets one `UpdateItem` per batch: an atomic `ADD event_count`, `SET last_seen_at`, and `started_at` via `if_not_exists`. Atomic counters prevent concurrent batches clobbering each other. A retried batch double-counts `event_count`, making it **approximate** — acceptable for a display counter, and documented here so nobody later mistakes it for exact.

## Error handling

The governing rule from phase-1 §5: **a bad line is data, not an error.** Three failure classes, treated differently on purpose.

| Failure | Handling | Why |
|---|---|---|
| Unparseable line | Falls through to `log.line`, empty `attrs` | Normal. A weird line must never wedge a shard. |
| Malformed envelope (bad JSON, missing `session_id`/`seq`) | Log at ERROR, skip the record | Deterministically malformed — retry reaches the same conclusion. The raw record is still in S3. |
| DynamoDB write failure | Raise | Transient or systemic. Retry is correct, and an exhausted retry belongs in the DLQ. |

So the DLQ catches **infrastructure failure, not data weirdness**. That is the distinction the `bisect_batch_on_function_error` / `maximum_retry_attempts` / `destination_config` settings exist to draw: if one record in a batch fails to write, bisecting isolates it rather than discarding the other 99.

`BatchWriteItem` writes in chunks of 25 (the API limit). It returns `UnprocessedItems` on partial throttling **rather than failing**, so those are retried with backoff and only a persistent failure raises. This is the same trap the operator hit with `PutRecords` — a 200 response does not mean the records landed.

## Testing

TDD, tests first, sanitized fixtures only.

- **`parser.py`** — table-driven over real line shapes: the `gets reward` line; `Client got reward info` and `Still waiting` (must fall through to `log.line`); a continuation line; an empty line.
- **`items.py`** — the silent failures: `seq` pads to exactly 20 characters and sorts correctly across the 1 -> 10 -> 100 boundary; `expires_at` is an `int` in seconds; null `game_time_s` is omitted rather than null; `item_name` derives from the final path segment.
- **`main.py`** — a dict-backed fake DynamoDB client (not moto) driving: a normal batch; a malformed envelope mid-batch; a write failure raising; an `UnprocessedItems` retry. This mirrors how the operator uses a narrow interface to test Kinesis retry paths without AWS.

Fixtures live in `infra/pipeline/lambda/hot-path/testdata/`, derived from a real session with player ids and any surviving addresses scrubbed, reviewed before `git add`.

Not covered here: real AWS. That is the E2E harness from phase-1 §"Testing" — apply, replay a fixture through the operator, assert rows in DynamoDB, destroy — worth doing as its own step once the handler works.

## Out of scope

- Mission boundary events (`mission.start` / `mission.end`) — deferred, re-derivable from S3.
- Item path -> display name mapping — a dashboard concern.
- Squad reward table and reward selection — not present in `EE.log` at all.
- The read API and dashboard — phase-1 §6.
