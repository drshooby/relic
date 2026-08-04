# M3 Hot-Path Handler Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn Kinesis records carrying raw Warframe log lines into queryable DynamoDB rows, parsing exactly one verified event type (`reward.relic`).

**Architecture:** Three modules under `infra/pipeline/lambda/hot-path/`. `parser.py` and `items.py` are pure functions over strings and dicts — no AWS imports, so they are testable without mocks. `main.py` is the only module importing boto3 and holds the batch/retry/error logic. Tests use a dict-backed fake DynamoDB client rather than moto, mirroring how the Go operator tests Kinesis retry paths through a narrow interface.

**Tech Stack:** Python 3.12 (Lambda runtime), pytest, uv for env management, boto3 (provided by the Lambda runtime, not vendored).

## Global Constraints

- **Python 3.12** — matches `runtime = "python3.12"` in `infra/pipeline/lambda.tf:13`. Local Python is 3.14; tests MUST run under 3.12 via `uv run --python 3.12` or behavior can diverge from deployed.
- **boto3 is NOT vendored.** The Lambda runtime provides it. It is a dev-only dependency for tests.
- **`seq` pads to width 20** — zero-padded decimal string. Exactly `uint64` max width.
- **`expires_at` is UNIX SECONDS as an `int`** — never milliseconds, never float.
- **Events TTL ~24h; sessions TTL ~7d.**
- **Envelope version `v` is carried through unchanged**, never rewritten.
- **A bad line is data, not an error** — unparseable lines become `log.line`, never raise.
- **Fixtures must be sanitized** — player ids and any surviving addresses scrubbed, reviewed before `git add`. Never commit raw log content (CLAUDE.md, no exceptions).
- **`archive_file` packages a single file today** (`source_file` in `lambda.tf:1-5`). Adding modules requires switching to `source_dir`; Task 6 covers this. Until then the Lambda deploys with only `main.py` and would fail at import.

---

## File Structure

| File | Responsibility |
|---|---|
| `infra/pipeline/lambda/hot-path/parser.py` | raw line → `(event_type, attrs)`. Pure. |
| `infra/pipeline/lambda/hot-path/items.py` | envelope + parse result → DynamoDB item dicts. Pure. |
| `infra/pipeline/lambda/hot-path/main.py` | Lambda handler: decode → parse → batch write. Only boto3 consumer. |
| `infra/pipeline/lambda/hot-path/tests/test_parser.py` | Matcher table behavior. |
| `infra/pipeline/lambda/hot-path/tests/test_items.py` | Padding, TTL units, null omission. |
| `infra/pipeline/lambda/hot-path/tests/test_main.py` | Batch handling, error classes, retries. |
| `infra/pipeline/lambda/hot-path/tests/conftest.py` | Fake DynamoDB client fixture. |
| `infra/pipeline/lambda/hot-path/pyproject.toml` | pytest config, dev deps, Python pin. |
| `infra/pipeline/lambda.tf` | Switch `archive_file` to `source_dir`, exclude tests. |

---

### Task 1: Project scaffolding and test harness

**Files:**
- Create: `infra/pipeline/lambda/hot-path/pyproject.toml`
- Create: `infra/pipeline/lambda/hot-path/tests/__init__.py` (empty)
- Modify: `.gitignore`

**Interfaces:**
- Consumes: nothing.
- Produces: a working `uv run --python 3.12 pytest` invocation used by every later task.

- [ ] **Step 1: Create `pyproject.toml`**

```toml
[project]
name = "relic-hot-path"
version = "0.1.0"
description = "Kinesis -> DynamoDB hot path for relic"
requires-python = "==3.12.*"

[dependency-groups]
dev = [
    "pytest>=8.0",
    "boto3>=1.35",
]

[tool.pytest.ini_options]
testpaths = ["tests"]
pythonpath = ["."]
```

`pythonpath = ["."]` lets tests `import parser` without a src layout — the Lambda handler resolves modules from the deployment root, so this mirrors production.

- [ ] **Step 2: Create the empty tests package**

```bash
mkdir -p infra/pipeline/lambda/hot-path/tests
touch infra/pipeline/lambda/hot-path/tests/__init__.py
```

- [ ] **Step 3: Verify the toolchain resolves Python 3.12**

Run: `cd infra/pipeline/lambda/hot-path && uv run --python 3.12 python -V`
Expected: `Python 3.12.x` (uv downloads it if absent). NOT 3.14.

- [ ] **Step 4: Add Python artifacts to `.gitignore`**

Append to the repo-root `.gitignore`:

```gitignore
# Python
__pycache__/
*.pyc
.pytest_cache/
.venv/
```

Note: `main.zip` (built by `archive_file`) is already untracked; confirm with `git status` after a `terraform plan`.

- [ ] **Step 5: Commit**

```bash
git add infra/pipeline/lambda/hot-path/pyproject.toml \
        infra/pipeline/lambda/hot-path/tests/__init__.py .gitignore
git commit -m "chore(hot-path): python test scaffolding pinned to 3.12"
```

---

### Task 2: Parser — matcher table and fallthrough

**Files:**
- Create: `infra/pipeline/lambda/hot-path/parser.py`
- Test: `infra/pipeline/lambda/hot-path/tests/test_parser.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `parse(raw: str) -> tuple[str, dict]`. Returns `("log.line", {})` when nothing matches. Used by `items.build_event_item` (Task 4) and `main.lambda_handler` (Task 5).

- [ ] **Step 1: Write the failing tests**

Create `tests/test_parser.py`. Player ids below are fabricated, not from a real session.

```python
import pytest
from parser import parse


def test_relic_reward_extracts_player_item_and_name():
    raw = ("240.623 Sys [Info]: VoidProjections: aaaa1111bbbb2222cccc3333 "
           "gets reward /Lotus/StoreItems/Types/Recipes/Weapons/WeaponParts/FulminPrimeBarrel")
    event_type, attrs = parse(raw)
    assert event_type == "reward.relic"
    assert attrs["player_id"] == "aaaa1111bbbb2222cccc3333"
    assert attrs["item_path"] == (
        "/Lotus/StoreItems/Types/Recipes/Weapons/WeaponParts/FulminPrimeBarrel")
    assert attrs["item_name"] == "FulminPrimeBarrel"


@pytest.mark.parametrize("raw", [
    # Squadmate arrival: same subsystem, no item path. MUST NOT match.
    "240.736 Sys [Info]: VoidProjections: aaaa1111bbbb2222cccc3333 gets reward",
    "240.736 Sys [Info]: VoidProjections: Client got reward info from aaaa1111",
    "240.623 Sys [Info]: VoidProjections: Still waiting on response from aaaa1111",
    "240.908 Sys [Info]: VoidProjections: Client has reward info for all players now",
    "240.444 Sys [Info]: VoidProjections: OpenVoidProjectionRewardScreenRMI",
    # Projection resource load -- looks reward-shaped, is not a reward.
    "63.761 Sys [Info]: ResourceLoader (/Lotus/Types/Game/Projections/T2VoidProjectionGyrePrimeFBronze) starting",
    # Continuation line (no timestamp, no subsystem).
    "  at Script.lua:42",
    "",
])
def test_non_reward_lines_fall_through_to_log_line(raw):
    event_type, attrs = parse(raw)
    assert event_type == "log.line"
    assert attrs == {}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd infra/pipeline/lambda/hot-path && uv run --python 3.12 pytest tests/test_parser.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'parser'`.

Note: Python had a stdlib `parser` module removed in 3.10, so on 3.12 there is no shadowing risk.

- [ ] **Step 3: Write the implementation**

Create `parser.py`:

```python
"""Raw log line -> (event_type, attrs).

Pure functions only: no AWS, no I/O. Adding an event type means appending a
tuple to MATCHERS. A line that matches nothing is not an error -- it becomes
log.line, because a bad line is data (see the M3 design spec).
"""

import re


def _relic_reward(groups: dict) -> dict:
    # item_path uses legacy internal names (FulminPrimeBarrel). The display
    # name needs a mapping that deliberately lives outside the hot path.
    return groups | {"item_name": groups["item_path"].rsplit("/", 1)[-1]}


# (pattern, event_type, enrich). enrich may be None when named groups suffice.
MATCHERS = [
    (
        re.compile(
            r"VoidProjections: (?P<player_id>\w+) gets reward (?P<item_path>/Lotus/\S+)"
        ),
        "reward.relic",
        _relic_reward,
    ),
]


def parse(raw: str) -> tuple[str, dict]:
    for pattern, event_type, enrich in MATCHERS:
        if m := pattern.search(raw):
            attrs = m.groupdict()
            return event_type, enrich(attrs) if enrich else attrs
    return "log.line", {}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd infra/pipeline/lambda/hot-path && uv run --python 3.12 pytest tests/test_parser.py -v`
Expected: PASS — 9 tests (1 + 8 parametrized).

- [ ] **Step 5: Commit**

```bash
git add infra/pipeline/lambda/hot-path/parser.py \
        infra/pipeline/lambda/hot-path/tests/test_parser.py
git commit -m "feat(hot-path): parser matcher table with reward.relic"
```

---

### Task 3: Items — seq padding

**Files:**
- Create: `infra/pipeline/lambda/hot-path/items.py`
- Test: `infra/pipeline/lambda/hot-path/tests/test_items.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `pad_seq(seq: int) -> str`. Used by `build_event_item` (Task 4).

This gets its own task because it is the silent-failure case: an unpadded write succeeds and lands in the wrong sort position with no error.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_items.py`:

```python
from items import pad_seq

SEQ_WIDTH = 20


def test_pad_seq_produces_fixed_width():
    assert pad_seq(0) == "0" * SEQ_WIDTH
    assert pad_seq(42) == "0" * (SEQ_WIDTH - 2) + "42"
    assert len(pad_seq(999)) == SEQ_WIDTH


def test_pad_seq_sorts_lexicographically_like_integers():
    # The whole reason for padding: "10" < "2" as strings, but 10 > 2.
    values = [1, 2, 10, 11, 100, 999, 1000]
    padded = [pad_seq(v) for v in values]
    assert padded == sorted(padded)


def test_pad_seq_handles_uint64_max_without_overflowing_width():
    uint64_max = 18446744073709551615
    assert pad_seq(uint64_max) == str(uint64_max)
    assert len(pad_seq(uint64_max)) == SEQ_WIDTH
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd infra/pipeline/lambda/hot-path && uv run --python 3.12 pytest tests/test_items.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'items'`.

- [ ] **Step 3: Write the implementation**

Create `items.py`:

```python
"""Envelope + parse result -> DynamoDB item dicts. Pure functions only."""

# uint64 max is 18446744073709551615 -- exactly 20 digits -- so a seq from the
# operator can never exceed this width and silently break sort order.
SEQ_WIDTH = 20


def pad_seq(seq: int) -> str:
    """Zero-pad seq so string sort order matches integer order.

    DynamoDB string sort keys compare lexicographically, so "10" sorts before
    "2" unpadded. Nothing enforces this but this function -- an unpadded write
    succeeds and lands in the wrong place with no error.
    """
    return f"{seq:0{SEQ_WIDTH}d}"
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd infra/pipeline/lambda/hot-path && uv run --python 3.12 pytest tests/test_items.py -v`
Expected: PASS — 3 tests.

- [ ] **Step 5: Commit**

```bash
git add infra/pipeline/lambda/hot-path/items.py \
        infra/pipeline/lambda/hot-path/tests/test_items.py
git commit -m "feat(hot-path): zero-padded seq sort keys"
```

---

### Task 4: Items — event and session item builders

**Files:**
- Modify: `infra/pipeline/lambda/hot-path/items.py`
- Test: `infra/pipeline/lambda/hot-path/tests/test_items.py`

**Interfaces:**
- Consumes: `pad_seq` (Task 3), `parse` (Task 2).
- Produces:
  - `build_event_item(envelope: dict, now: int) -> dict` — a `relic-events` item.
  - `build_session_update(session_id: str, count: int, last_seen: str, now: int) -> dict` — kwargs for `UpdateItem`, with keys `Key`, `UpdateExpression`, `ExpressionAttributeNames`, `ExpressionAttributeValues`.
  - `EVENTS_TTL_SECONDS = 86400`, `SESSIONS_TTL_SECONDS = 604800`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_items.py`:

```python
from items import (
    build_event_item,
    build_session_update,
    EVENTS_TTL_SECONDS,
    SESSIONS_TTL_SECONDS,
)

NOW = 1754341234

REWARD_ENVELOPE = {
    "v": 1,
    "source": "warframe.ee_log",
    "session_id": "abc123",
    "seq": 42,
    "game_time_s": 240.623,
    "wall_time_utc": "2026-08-03T19:12:44Z",
    "session_epoch_utc": "2026-08-03T19:08:44Z",
    "raw": ("240.623 Sys [Info]: VoidProjections: aaaa1111 gets reward "
            "/Lotus/StoreItems/Types/Recipes/Weapons/WeaponParts/FulminPrimeBarrel"),
}


def test_build_event_item_keys_and_parse_result():
    item = build_event_item(REWARD_ENVELOPE, NOW)
    assert item["session_id"] == "abc123"
    assert item["seq"] == "0" * 18 + "42"
    assert item["event_type"] == "reward.relic"
    assert item["attrs"]["item_name"] == "FulminPrimeBarrel"
    assert item["raw"] == REWARD_ENVELOPE["raw"]
    assert item["v"] == 1


def test_build_event_item_ttl_is_int_seconds_not_millis():
    item = build_event_item(REWARD_ENVELOPE, NOW)
    assert item["expires_at"] == NOW + EVENTS_TTL_SECONDS
    assert isinstance(item["expires_at"], int)
    # Milliseconds would be ~1000x larger and never swept.
    assert item["expires_at"] < 10_000_000_000


def test_build_event_item_preserves_float_precision():
    item = build_event_item(REWARD_ENVELOPE, NOW)
    assert item["game_time_s"] == 240.623


def test_build_event_item_omits_null_clocks_rather_than_storing_none():
    envelope = REWARD_ENVELOPE | {"game_time_s": None, "wall_time_utc": None}
    item = build_event_item(envelope, NOW)
    # DynamoDB distinguishes absent from null; absent is the honest encoding
    # for a line emitted before the header was parsed.
    assert "game_time_s" not in item
    assert "wall_time_utc" not in item


def test_build_event_item_unparsed_line_gets_empty_attrs():
    envelope = REWARD_ENVELOPE | {"raw": "6.170 Script [Info]: UISTYLE: initialized 1"}
    item = build_event_item(envelope, NOW)
    assert item["event_type"] == "log.line"
    assert item["attrs"] == {}


def test_build_session_update_uses_atomic_add_and_if_not_exists():
    update = build_session_update("abc123", 5, "2026-08-03T19:12:44Z", NOW)
    assert update["Key"] == {"session_id": "abc123"}
    expr = update["UpdateExpression"]
    assert "ADD" in expr and "event_count" in expr
    assert "if_not_exists(started_at" in expr
    values = update["ExpressionAttributeValues"]
    assert values[":inc"] == 5
    assert values[":ttl"] == NOW + SESSIONS_TTL_SECONDS
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd infra/pipeline/lambda/hot-path && uv run --python 3.12 pytest tests/test_items.py -v`
Expected: FAIL — `ImportError: cannot import name 'build_event_item'`.

- [ ] **Step 3: Write the implementation**

Append to `items.py`:

```python
from parser import parse

EVENTS_TTL_SECONDS = 24 * 60 * 60        # 24h -- rolling cache, S3 is the archive
SESSIONS_TTL_SECONDS = 7 * 24 * 60 * 60  # 7d  -- outlives its own events


def build_event_item(envelope: dict, now: int) -> dict:
    """Build a relic-events item from an envelope.

    `now` is injected rather than read from the clock so tests are deterministic.
    """
    event_type, attrs = parse(envelope["raw"])

    item = {
        "session_id": envelope["session_id"],
        "seq": pad_seq(envelope["seq"]),
        "event_type": event_type,
        "attrs": attrs,
        "raw": envelope["raw"],
        "v": envelope["v"],
        # UNIX SECONDS. Milliseconds put expiry ~50,000 years out and the item
        # is never swept -- silently, with no error.
        "expires_at": now + EVENTS_TTL_SECONDS,
    }

    # Lines emitted before the header is parsed carry no clock. DynamoDB
    # distinguishes absent from null, and absent is the honest encoding.
    for field in ("game_time_s", "wall_time_utc"):
        if envelope.get(field) is not None:
            item[field] = envelope[field]

    return item


def build_session_update(
    session_id: str, count: int, last_seen: str, now: int
) -> dict:
    """UpdateItem kwargs for relic-sessions: one call per batch, not per record.

    ADD is atomic, so concurrent batches do not clobber each other. A retried
    batch double-counts, making event_count APPROXIMATE -- acceptable for a
    display counter, documented so nobody mistakes it for exact.
    """
    return {
        "Key": {"session_id": session_id},
        "UpdateExpression": (
            "ADD event_count :inc "
            "SET last_seen_at = :seen, "
            "expires_at = :ttl, "
            "started_at = if_not_exists(started_at, :seen)"
        ),
        "ExpressionAttributeValues": {
            ":inc": count,
            ":seen": last_seen,
            ":ttl": now + SESSIONS_TTL_SECONDS,
        },
    }
```

Note: no `ExpressionAttributeNames` is needed — none of `event_count`, `last_seen_at`, `expires_at`, `started_at` is a DynamoDB reserved word. The interface block lists it as optional; omit it.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd infra/pipeline/lambda/hot-path && uv run --python 3.12 pytest tests/test_items.py -v`
Expected: PASS — 9 tests total (3 from Task 3 + 6 new).

- [ ] **Step 5: Commit**

```bash
git add infra/pipeline/lambda/hot-path/items.py \
        infra/pipeline/lambda/hot-path/tests/test_items.py
git commit -m "feat(hot-path): event and session item builders"
```

---

### Task 5: Handler — decode, batch write, error classes

**Files:**
- Create: `infra/pipeline/lambda/hot-path/main.py` (replaces the existing stub)
- Create: `infra/pipeline/lambda/hot-path/tests/conftest.py`
- Test: `infra/pipeline/lambda/hot-path/tests/test_main.py`

**Interfaces:**
- Consumes: `build_event_item`, `build_session_update` (Task 4).
- Produces: `lambda_handler(event, context)` — the Lambda entrypoint named in `lambda.tf:12` as `main.lambda_handler`.

- [ ] **Step 1: Write the fake DynamoDB client**

Create `tests/conftest.py`:

```python
"""A dict-backed fake DynamoDB resource.

Not moto: this keeps tests fast and dependency-free, and mirrors how the Go
operator drives Kinesis retry paths through a narrow interface.

It mimics the boto3 *resource* surface the handler actually uses:
    resource.Table(name).batch_writer() -> context manager with put_item
    resource.Table(name).update_item(**kwargs)
"""

import pytest


class FakeBatchWriter:
    def __init__(self, table):
        self.table = table

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        # Real batch_writer flushes here; nothing buffered in the fake.
        return False

    def put_item(self, Item):
        if self.table.fail_with is not None:
            raise self.table.fail_with
        self.table.writes += 1
        self.table.items[(Item["session_id"], Item["seq"])] = Item


class FakeTable:
    def __init__(self, name, *, fail_with=None):
        self.name = name
        self.items = {}
        self.updates = []
        self.writes = 0
        self.fail_with = fail_with

    def batch_writer(self):
        return FakeBatchWriter(self)

    def update_item(self, **kwargs):
        self.updates.append(kwargs)
        return {}


class FakeDynamoResource:
    def __init__(self, *, fail_with=None):
        self._tables = {}
        self.fail_with = fail_with

    def Table(self, name):
        if name not in self._tables:
            self._tables[name] = FakeTable(name, fail_with=self.fail_with)
        return self._tables[name]


@pytest.fixture
def fake_ddb():
    return FakeDynamoResource()
```

**Note on retry coverage:** `batch_writer` handles chunking at 25 and
`UnprocessedItems` retries inside boto3, so those paths are boto3's
responsibility and are not re-tested here. What the handler owns — and what
Task 5 tests — is that a write error propagates rather than being swallowed.

- [ ] **Step 2: Write the failing tests**

Create `tests/test_main.py`:

```python
import base64
import json

import pytest

import main
# Package-qualified: pythonpath is the project root, so a bare
# `from conftest import ...` raises ModuleNotFoundError.
from tests.conftest import FakeDynamoResource

EVENTS_TABLE = "relic-events"
SESSIONS_TABLE = "relic-sessions"


def _record(envelope: dict) -> dict:
    payload = json.dumps(envelope).encode()
    return {"kinesis": {"data": base64.b64encode(payload).decode()}}


def _envelope(seq: int, raw: str = "1.0 Sys [Info]: something") -> dict:
    return {
        "v": 1,
        "source": "warframe.ee_log",
        "session_id": "abc123",
        "seq": seq,
        "game_time_s": 1.0,
        "wall_time_utc": "2026-08-03T19:12:44Z",
        "session_epoch_utc": "2026-08-03T19:08:44Z",
        "raw": raw,
    }


@pytest.fixture(autouse=True)
def env(monkeypatch):
    monkeypatch.setenv("EVENTS_TABLE", EVENTS_TABLE)
    monkeypatch.setenv("SESSIONS_TABLE", SESSIONS_TABLE)


def test_writes_every_record_to_events_table(fake_ddb, monkeypatch):
    monkeypatch.setattr(main, "_resource", lambda: fake_ddb)
    event = {"Records": [_record(_envelope(i)) for i in range(3)]}

    main.lambda_handler(event, None)

    stored = fake_ddb.Table(EVENTS_TABLE).items
    assert len(stored) == 3
    assert ("abc123", "0" * 19 + "0") in stored


def test_seq_is_padded_in_the_stored_key(fake_ddb, monkeypatch):
    monkeypatch.setattr(main, "_resource", lambda: fake_ddb)
    main.lambda_handler({"Records": [_record(_envelope(7))]}, None)

    (key,) = fake_ddb.Table(EVENTS_TABLE).items
    assert key == ("abc123", "0" * 19 + "7")
    assert len(key[1]) == 20


def test_updates_session_once_per_batch_not_per_record(fake_ddb, monkeypatch):
    monkeypatch.setattr(main, "_resource", lambda: fake_ddb)
    event = {"Records": [_record(_envelope(i)) for i in range(5)]}

    main.lambda_handler(event, None)

    updates = fake_ddb.Table(SESSIONS_TABLE).updates
    assert len(updates) == 1
    assert updates[0]["ExpressionAttributeValues"][":inc"] == 5


def test_malformed_envelope_is_skipped_not_raised(fake_ddb, monkeypatch):
    monkeypatch.setattr(main, "_resource", lambda: fake_ddb)
    bad = {"kinesis": {"data": base64.b64encode(b"not json at all").decode()}}
    missing_key = _record({"v": 1, "raw": "x"})  # no session_id/seq
    event = {"Records": [_record(_envelope(1)), bad, missing_key]}

    main.lambda_handler(event, None)  # must not raise

    # The one good record still landed.
    assert len(fake_ddb.Table(EVENTS_TABLE).items) == 1


@pytest.mark.parametrize("missing_field", ["session_id", "seq", "raw", "v"])
def test_envelope_missing_any_required_field_is_skipped(
    fake_ddb, monkeypatch, missing_field
):
    monkeypatch.setattr(main, "_resource", lambda: fake_ddb)
    incomplete = _envelope(1)
    del incomplete[missing_field]
    event = {"Records": [_record(incomplete), _record(_envelope(2))]}

    # build_event_item indexes raw and v directly, so validating only
    # session_id/seq would let a KeyError escape and crash the whole batch --
    # sending good records to the DLQ because one was malformed.
    main.lambda_handler(event, None)

    assert len(fake_ddb.Table(EVENTS_TABLE).items) == 1


def test_all_records_malformed_writes_nothing_and_does_not_raise(fake_ddb, monkeypatch):
    monkeypatch.setattr(main, "_resource", lambda: fake_ddb)
    bad = {"kinesis": {"data": base64.b64encode(b"garbage").decode()}}

    main.lambda_handler({"Records": [bad, bad]}, None)

    assert fake_ddb.Table(EVENTS_TABLE).items == {}
    assert fake_ddb.Table(SESSIONS_TABLE).updates == []


def test_large_batch_writes_every_item(fake_ddb, monkeypatch):
    monkeypatch.setattr(main, "_resource", lambda: fake_ddb)
    event = {"Records": [_record(_envelope(i)) for i in range(60)]}

    main.lambda_handler(event, None)

    # Chunking at 25 is boto3's batch_writer's job; what matters here is that
    # nothing is lost across the chunk boundaries.
    assert len(fake_ddb.Table(EVENTS_TABLE).items) == 60


def test_write_failure_raises_so_the_batch_retries(monkeypatch):
    fake = FakeDynamoResource(fail_with=RuntimeError("ProvisionedThroughputExceeded"))
    monkeypatch.setattr(main, "_resource", lambda: fake)
    event = {"Records": [_record(_envelope(1))]}

    # Infrastructure failure MUST raise: that is what triggers bisect/retry/DLQ.
    with pytest.raises(RuntimeError):
        main.lambda_handler(event, None)


def test_empty_batch_does_not_touch_dynamodb(fake_ddb, monkeypatch):
    monkeypatch.setattr(main, "_resource", lambda: fake_ddb)

    main.lambda_handler({"Records": []}, None)

    assert fake_ddb.Table(EVENTS_TABLE).writes == 0
    assert fake_ddb.Table(SESSIONS_TABLE).updates == []
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cd infra/pipeline/lambda/hot-path && uv run --python 3.12 pytest tests/test_main.py -v`
Expected: FAIL — `AttributeError: module 'main' has no attribute '_resource'` (the stub has only `lambda_handler`).

- [ ] **Step 4: Write the implementation**

Replace `main.py` entirely:

```python
"""Hot path: Kinesis -> parse -> DynamoDB.

The only module importing boto3. Parsing and item shaping live in parser.py
and items.py, which are pure and tested without AWS.

Error philosophy (see the M3 design spec):
  - unparseable line   -> log.line, never raises. A bad line is data.
  - malformed envelope -> logged and skipped. Deterministically bad; retrying
                          reaches the same conclusion. The raw record is in S3.
  - DynamoDB failure   -> raises. Transient or systemic, so retry is correct
                          and an exhausted retry belongs in the DLQ.
"""

import base64
import json
import logging
import os
import time

import boto3

from items import build_event_item, build_session_update

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Every field build_event_item indexes directly. Validating all of them here
# means the builder can use plain [] lookups without raising mid-batch.
REQUIRED_FIELDS = ("session_id", "seq", "raw", "v")

_ddb = None


def _resource():
    """The DynamoDB service resource.

    boto3.resource, NOT boto3.client: the resource's Table objects serialize
    plain Python dicts automatically. The low-level client would demand typed
    attribute values -- {"S": "abc"}, {"N": "42"} -- and items.py deliberately
    builds untyped dicts. Lazily built and cached across warm invocations;
    tests monkeypatch this function.
    """
    global _ddb
    if _ddb is None:
        _ddb = boto3.resource("dynamodb")
    return _ddb


def _decode(record: dict) -> dict | None:
    """Kinesis record -> envelope dict, or None if it is unusable."""
    try:
        payload = base64.b64decode(record["kinesis"]["data"])
        envelope = json.loads(payload)
    except (KeyError, ValueError) as err:
        logger.error("undecodable record, skipping: %s", err)
        return None

    if not isinstance(envelope, dict):
        logger.error("envelope is not an object, skipping: %s", type(envelope))
        return None

    # All four are required: build_event_item indexes raw and v directly, so a
    # missing one would raise KeyError mid-batch and crash records that are
    # otherwise fine -- turning a bad record into an infrastructure failure and
    # burning DLQ retries. Validating here keeps the "malformed -> log and
    # skip" contract in one place.
    missing = [k for k in REQUIRED_FIELDS if k not in envelope]
    if missing:
        logger.error("envelope missing %s, skipping", ",".join(missing))
        return None
    return envelope


def lambda_handler(event, context):
    records = event.get("Records", [])
    if not records:
        return

    now = int(time.time())
    ddb = _resource()
    events_table = ddb.Table(os.environ["EVENTS_TABLE"])
    sessions_table = ddb.Table(os.environ["SESSIONS_TABLE"])

    items = []
    skipped = 0
    for record in records:
        envelope = _decode(record)
        if envelope is None:
            skipped += 1
            continue
        items.append(build_event_item(envelope, now))

    if not items:
        logger.warning("batch of %d records yielded nothing writable", len(records))
        return

    # batch_writer chunks at 25 and retries UnprocessedItems itself. Rolling
    # that by hand is the classic way to silently drop records: BatchWriteItem
    # returns 200 with UnprocessedItems on partial throttling, so treating the
    # 200 as success loses data. Any error it cannot resolve propagates, which
    # is what we want -- infrastructure failure must raise so the event source
    # mapping bisects, retries, and finally routes to the DLQ.
    with events_table.batch_writer() as batch:
        for item in items:
            batch.put_item(Item=item)

    # One session update per batch, not per record. Records arrive out of order
    # within a batch, so last_seen_at takes the max rather than the last item's.
    session_id = items[-1]["session_id"]
    last_seen = max(
        (i["wall_time_utc"] for i in items if "wall_time_utc" in i),
        default=None,
    )
    sessions_table.update_item(**build_session_update(session_id, len(items), last_seen, now))

    logger.info(
        "wrote %d events, skipped %d, session=%s", len(items), skipped, session_id
    )
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd infra/pipeline/lambda/hot-path && uv run --python 3.12 pytest -v`
Expected: PASS — all tests across the three files.

- [ ] **Step 6: Commit**

```bash
git add infra/pipeline/lambda/hot-path/main.py \
        infra/pipeline/lambda/hot-path/tests/conftest.py \
        infra/pipeline/lambda/hot-path/tests/test_main.py
git commit -m "feat(hot-path): handler with batch writes and error classes"
```

---

### Task 6: Package multiple modules into the Lambda zip

**Files:**
- Modify: `infra/pipeline/lambda.tf:1-5`

**Interfaces:**
- Consumes: the three modules from Tasks 2-5.
- Produces: a deployable zip containing `main.py`, `parser.py`, `items.py` and excluding tests.

**Without this task the Lambda deploys with only `main.py` and fails at import.** `archive_file` currently uses `source_file`, which packages exactly one file.

- [ ] **Step 1: Switch the archive to a directory**

Replace the `data "archive_file"` block in `infra/pipeline/lambda.tf`:

```hcl
data "archive_file" "hot_path_func_files" {
  type        = "zip"
  source_dir  = "${path.module}/lambda/hot-path"
  output_path = "${path.module}/lambda/hot-path.zip"

  # Tests, tooling, and caches must not ship to Lambda. output_path sits
  # outside source_dir so the zip never tries to include itself.
  excludes = [
    "tests",
    "pyproject.toml",
    "uv.lock",
    "__pycache__",
    ".pytest_cache",
    ".venv",
  ]
}
```

Note `output_path` moved out of `lambda/hot-path/` — an archive written inside its own `source_dir` produces nondeterministic hashes.

- [ ] **Step 2: Verify the config parses**

Run: `cd infra/pipeline && terraform fmt && terraform validate`
Expected: `Success! The configuration is valid.`

- [ ] **Step 3: Verify the zip contains the right files**

Run:
```bash
cd infra/pipeline && terraform plan -out=/dev/null >/dev/null 2>&1; \
  unzip -l lambda/hot-path.zip
```
Expected: `main.py`, `parser.py`, `items.py`. NO `tests/`, no `pyproject.toml`, no `__pycache__`.

If `terraform plan` cannot run (no credentials/state), build the zip directly instead:
```bash
cd infra/pipeline/lambda/hot-path && \
  zip -r /tmp/check.zip . -x 'tests/*' 'pyproject.toml' '__pycache__/*' && \
  unzip -l /tmp/check.zip
```

- [ ] **Step 4: Confirm the built zip is not tracked by git**

Run: `git status --short infra/pipeline/`
Expected: `lambda/hot-path.zip` does not appear as untracked. If it does, add `*.zip` under the Python section of `.gitignore` and re-check.

- [ ] **Step 5: Commit**

```bash
git add infra/pipeline/lambda.tf .gitignore
git commit -m "build(hot-path): package all modules, exclude tests from the zip"
```

---

### Task 7: Sanitized fixture and a real-line regression test

**Files:**
- Create: `infra/pipeline/lambda/hot-path/tests/testdata/reward_sequence.jsonl`
- Modify: `infra/pipeline/lambda/hot-path/tests/test_parser.py`

**Interfaces:**
- Consumes: `parse` (Task 2).
- Produces: nothing consumed by later tasks.

**PII gate:** the source lines contain real player ids. They MUST be replaced before the file is written, and the file MUST be read back and reviewed before `git add`. Never commit raw log content (CLAUDE.md).

- [ ] **Step 1: Create the sanitized fixture by hand**

Write `tests/testdata/reward_sequence.jsonl` — one envelope per line, ids replaced with obvious fakes. This is the verified 2026-08-03 sequence with `<player_N>` substituted:

```jsonl
{"v":1,"source":"warframe.ee_log","session_id":"fixture-session","seq":1,"game_time_s":240.444,"wall_time_utc":"2026-08-03T19:12:44Z","session_epoch_utc":"2026-08-03T19:08:44Z","raw":"240.444 Sys [Info]: VoidProjections: OpenVoidProjectionRewardScreenRMI"}
{"v":1,"source":"warframe.ee_log","session_id":"fixture-session","seq":2,"game_time_s":240.458,"wall_time_utc":"2026-08-03T19:12:44Z","session_epoch_utc":"2026-08-03T19:08:44Z","raw":"240.458 Sys [Info]: VoidProjections: GetVoidProjectionRewards"}
{"v":1,"source":"warframe.ee_log","session_id":"fixture-session","seq":3,"game_time_s":240.623,"wall_time_utc":"2026-08-03T19:12:44Z","session_epoch_utc":"2026-08-03T19:08:44Z","raw":"240.623 Sys [Info]: VoidProjections: player0000 gets reward /Lotus/StoreItems/Types/Recipes/Weapons/WeaponParts/FulminPrimeBarrel"}
{"v":1,"source":"warframe.ee_log","session_id":"fixture-session","seq":4,"game_time_s":240.623,"wall_time_utc":"2026-08-03T19:12:44Z","session_epoch_utc":"2026-08-03T19:08:44Z","raw":"240.623 Sys [Info]: VoidProjections: Client got reward info from player0000"}
{"v":1,"source":"warframe.ee_log","session_id":"fixture-session","seq":5,"game_time_s":240.623,"wall_time_utc":"2026-08-03T19:12:44Z","session_epoch_utc":"2026-08-03T19:08:44Z","raw":"240.623 Sys [Info]: VoidProjections: Still waiting on response from player0001"}
{"v":1,"source":"warframe.ee_log","session_id":"fixture-session","seq":6,"game_time_s":240.736,"wall_time_utc":"2026-08-03T19:12:44Z","session_epoch_utc":"2026-08-03T19:08:44Z","raw":"240.736 Sys [Info]: VoidProjections: Client got reward info from player0001"}
{"v":1,"source":"warframe.ee_log","session_id":"fixture-session","seq":7,"game_time_s":240.908,"wall_time_utc":"2026-08-03T19:12:44Z","session_epoch_utc":"2026-08-03T19:08:44Z","raw":"240.908 Sys [Info]: VoidProjections: Client has reward info for all players now"}
```

- [ ] **Step 2: Verify no real ids survived**

Run:
```bash
cd infra/pipeline/lambda/hot-path && \
  grep -oE '\b[0-9a-f]{16,}\b' tests/testdata/reward_sequence.jsonl || echo "clean"
```
Expected: `clean`. Any hex run of 16+ characters is a real player id that must be replaced before continuing.

- [ ] **Step 3: Write the failing test**

Append to `tests/test_parser.py`:

```python
import json
from pathlib import Path

FIXTURE = Path(__file__).parent / "testdata" / "reward_sequence.jsonl"


def test_full_reveal_sequence_yields_exactly_one_reward_event():
    envelopes = [json.loads(line) for line in FIXTURE.read_text().splitlines() if line]
    types = [parse(e["raw"])[0] for e in envelopes]

    # Only the player's own roll carries an item path. Every surrounding
    # coordination line must fall through -- squadmate arrivals included.
    assert types.count("reward.relic") == 1
    assert types.count("log.line") == len(envelopes) - 1

    reward_idx = types.index("reward.relic")
    _, attrs = parse(envelopes[reward_idx]["raw"])
    assert attrs["item_name"] == "FulminPrimeBarrel"
    # The reveal onset precedes the reward, so timing is preserved per line.
    assert envelopes[0]["game_time_s"] < envelopes[reward_idx]["game_time_s"]
```

- [ ] **Step 4: Run tests**

Run: `cd infra/pipeline/lambda/hot-path && uv run --python 3.12 pytest -v`
Expected: PASS — all tests including the new sequence test.

- [ ] **Step 5: Review the fixture before staging**

Run: `cat infra/pipeline/lambda/hot-path/tests/testdata/reward_sequence.jsonl`
Confirm by eye: no real player ids, no IP addresses, no machine or account names. This is a required manual gate, not a formality.

- [ ] **Step 6: Commit**

```bash
git add infra/pipeline/lambda/hot-path/tests/testdata/reward_sequence.jsonl \
        infra/pipeline/lambda/hot-path/tests/test_parser.py
git commit -m "test(hot-path): sanitized reveal-sequence regression fixture"
```

---

## Verification

After every task, the full suite must pass:

```bash
cd infra/pipeline/lambda/hot-path && uv run --python 3.12 pytest -v
```

Terraform must still validate:

```bash
cd infra/pipeline && terraform fmt -check && terraform validate
```

**What these do NOT prove:** neither command contacts AWS. A clean `terraform validate` says nothing about whether data flows, and the fake DynamoDB client is not DynamoDB. Confirming the real path — operator → Kinesis → Lambda → DynamoDB — requires the E2E harness from the phase-1 spec's Testing section, which is out of scope here and needs an applied pipeline.

## Out of scope

- The E2E harness (apply → replay → assert rows → destroy).
- `infra/README.md` (tracked separately).
- Mission boundary events, item display-name mapping, the read API and dashboard.
