# M4 Read API and Dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A dashboard that streams the live session feed while you play, and says plainly when a session's events have aged out.

**Architecture:** A read-only Python Lambda behind an HTTP API serves two endpoints from the M3 DynamoDB tables. A Next.js 16 app polls it client-side every 2s, with a downed/dead revive mechanic that stops polling a session that has gone quiet. All fetching is client-side; there is no server component to the dashboard.

**Tech Stack:** Python 3.12 (Lambda), Terraform (`aws_apigatewayv2_*`), Next.js 16.3 + React 19.2 + TypeScript (dashboard), bun (package manager), Vitest + React Testing Library, CSS Modules.

## Global Constraints

- **Python 3.12** — matches the Lambda runtime. Run tests via `uv run --python 3.12`. Local system Python is 3.14; behavior can diverge.
- **boto3 is NOT vendored.** The Lambda runtime provides it; it is a dev-only test dependency.
- **The API is read-only.** Its IAM role gets `dynamodb:Query`, `GetItem`, `Scan` on the two table ARNs and nothing else. The hot-path role stays write-only. Never widen either.
- **`Decimal` must be converted before `json.dumps`.** DynamoDB returns Numbers as `decimal.Decimal` and `json.dumps` raises `TypeError` on it. This is the mirror of M3's float bug.
- **`seq` is a zero-padded string of width 20**, passed through verbatim. Never parse it to an int and re-pad client-side.
- **CORS `allow_origins = ["http://localhost:3000"]`**, methods `["GET"]`. Not a wildcard.
- **Package manager is bun.** Not npm, not pnpm. `bun add`, `bun run`.
- **This Next.js is 16.3 with React 19.2 and differs from older versions.** `dashboard/node_modules/next/dist/docs/` is authoritative over training data. Note `app/layout.tsx` already uses the Next-16 global type `LayoutProps<"/">` — do not "fix" it to `{ children }: { children: React.ReactNode }`.
- **Every component is a directory**: `Component/Component.tsx` (named export), `Component/Component.module.css`, `Component/index.tsx` re-exporting. No exceptions, including one-off children.
- **`lib/` and `types.ts` live at the `dashboard/` root**, not inside `components/` — they are not components.
- **Never commit real log content.** Fixtures use fabricated player ids (`player0000`), never values from a real session.

---

## File Structure

| File | Responsibility |
|---|---|
| `infra/pipeline/lambda/api/queries.py` | DynamoDB reads. Table objects in, plain dicts out. Pure of HTTP. |
| `infra/pipeline/lambda/api/responses.py` | Status codes, JSON encoding, CORS headers, Decimal conversion. Pure. |
| `infra/pipeline/lambda/api/main.py` | Handler: route → query → response. Only module importing boto3. |
| `infra/pipeline/lambda/api/tests/` | conftest fake + one test file per module. |
| `infra/pipeline/api.tf` | HTTP API, routes, integration, stage, Lambda permission. |
| `infra/pipeline/iam.tf` (modify) | Read-only role and policy for the API Lambda. |
| `infra/pipeline/lambda.tf` (modify) | `archive_file` + `aws_lambda_function` for the API. |
| `infra/pipeline/outputs.tf` | The invoke URL, so the dashboard can read it without console digging. |
| `dashboard/types.ts` | `Session`, `RelicEvent`, `FeedState`. |
| `dashboard/lib/api.ts` | Typed fetch wrappers; 204/404 handled here. |
| `dashboard/lib/useSessionFeed.ts` | The polling hook and revive state machine. |
| `dashboard/components/*/` | Six components, each its own directory. |
| `dashboard/app/page.tsx` (replace) | Composes the layout. |

---

### Task 1: API scaffolding and the response layer

**Files:**
- Create: `infra/pipeline/lambda/api/pyproject.toml`
- Create: `infra/pipeline/lambda/api/tests/__init__.py` (empty)
- Create: `infra/pipeline/lambda/api/responses.py`
- Test: `infra/pipeline/lambda/api/tests/test_responses.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `ok(body: dict) -> dict`, `no_content() -> dict`, `not_found(message: str) -> dict`, `json_default(value)`. All return API Gateway v2 proxy response dicts with keys `statusCode`, `headers`, and (except `no_content`) `body`.

- [ ] **Step 1: Create `pyproject.toml`**

```toml
[project]
name = "relic-api"
version = "0.1.0"
description = "Read API for the relic dashboard"
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

- [ ] **Step 2: Create the tests package**

```bash
mkdir -p infra/pipeline/lambda/api/tests
touch infra/pipeline/lambda/api/tests/__init__.py
```

- [ ] **Step 3: Write the failing test**

Create `infra/pipeline/lambda/api/tests/test_responses.py`:

```python
import json
from decimal import Decimal

from responses import json_default, no_content, not_found, ok

CORS_ORIGIN = "http://localhost:3000"


def test_ok_returns_200_with_json_body_and_cors():
    resp = ok({"sessions": []})
    assert resp["statusCode"] == 200
    assert json.loads(resp["body"]) == {"sessions": []}
    assert resp["headers"]["Access-Control-Allow-Origin"] == CORS_ORIGIN
    assert resp["headers"]["Content-Type"] == "application/json"


def test_no_content_returns_204_with_no_body():
    resp = no_content()
    assert resp["statusCode"] == 204
    # 204 forbids a body. Returning one is a protocol violation, and API
    # Gateway may pass it through to the browser, where fetch's res.json()
    # would then behave inconsistently across runtimes.
    assert "body" not in resp
    assert resp["headers"]["Access-Control-Allow-Origin"] == CORS_ORIGIN


def test_not_found_returns_404_with_a_message():
    resp = not_found("no such session")
    assert resp["statusCode"] == 404
    assert json.loads(resp["body"]) == {"error": "no such session"}


def test_decimal_serializes_without_raising():
    # DynamoDB returns every Number as decimal.Decimal, and json.dumps raises
    # TypeError on it. This is the mirror image of the hot path's float bug:
    # the same type boundary, failing in the opposite direction.
    resp = ok({"game_time_s": Decimal("186.318")})
    assert json.loads(resp["body"])["game_time_s"] == 186.318


def test_integral_decimal_serializes_as_int_not_float():
    # event_count comes back as Decimal("6372"). Rendering it as 6372.0 in the
    # UI would look like a bug.
    resp = ok({"event_count": Decimal("6372")})
    assert json.loads(resp["body"])["event_count"] == 6372
    assert "6372.0" not in resp["body"]


def test_json_default_raises_on_genuinely_unserializable_types():
    # A silent fallback to str() would hide real bugs.
    with pytest.raises(TypeError):
        json_default(object())
```

Add `import pytest` at the top of the file.

- [ ] **Step 4: Run the test to verify it fails**

Run: `cd infra/pipeline/lambda/api && uv run --python 3.12 pytest tests/test_responses.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'responses'`

- [ ] **Step 5: Write the implementation**

Create `infra/pipeline/lambda/api/responses.py`:

```python
"""API Gateway v2 proxy responses. Pure: no AWS, no I/O.

Every response carries CORS headers. The browser calls this API from
http://localhost:3000, a different origin from execute-api, so without them
the fetch fails before the handler's status code is ever read.
"""

import json
from decimal import Decimal

# Phase 1 runs the dashboard locally. Not a wildcard: CORS is what stops other
# sites' JS from reading your session telemetry while the pipeline is applied.
CORS_ORIGIN = "http://localhost:3000"

_HEADERS = {
    "Content-Type": "application/json",
    "Access-Control-Allow-Origin": CORS_ORIGIN,
}


def json_default(value):
    """Convert types json.dumps cannot handle.

    DynamoDB returns every Number as decimal.Decimal. Integral values become
    int so event_count renders as 6372 rather than 6372.0; the rest become
    float. Anything else raises, because a silent str() fallback would hide
    real bugs.
    """
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def ok(body: dict) -> dict:
    return {
        "statusCode": 200,
        "headers": _HEADERS,
        "body": json.dumps(body, default=json_default),
    }


def no_content() -> dict:
    # No "body" key at all: 204 forbids one.
    return {"statusCode": 204, "headers": _HEADERS}


def not_found(message: str) -> dict:
    return {
        "statusCode": 404,
        "headers": _HEADERS,
        "body": json.dumps({"error": message}),
    }
```

- [ ] **Step 6: Run the test to verify it passes**

Run: `cd infra/pipeline/lambda/api && uv run --python 3.12 pytest tests/test_responses.py -v`
Expected: PASS — 6 tests.

- [ ] **Step 7: Report for review (DO NOT COMMIT)**

The user reviews every change before it is committed. Stop here and report what you changed. Do not run `git add` or `git commit`.

---

### Task 2: Query layer

**Files:**
- Create: `infra/pipeline/lambda/api/queries.py`
- Create: `infra/pipeline/lambda/api/tests/conftest.py`
- Test: `infra/pipeline/lambda/api/tests/test_queries.py`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces:
  - `list_sessions(table, limit: int = 20) -> list[dict]` — newest first by `last_seen_at`.
  - `get_events(table, session_id: str, since: str | None) -> list[dict]` — ascending by `seq`, exclusive of `since`.
  - `session_exists(table, session_id: str) -> bool`.
  - `MAX_SESSIONS = 20`.

- [ ] **Step 1: Write the fake**

Create `infra/pipeline/lambda/api/tests/conftest.py`:

```python
"""A dict-backed fake DynamoDB Table.

Models the read side of boto3's resource API: query(), get_item(), scan().
Deliberately enforces two real contracts, because M3 shipped two Critical
bugs behind a fake that modelled shape but not behaviour:
  - query() requires a KeyConditionExpression and returns items SORTED by
    sort key, which is what makes the padded-seq cursor meaningful.
  - Numbers come back as decimal.Decimal, exactly as the real client returns
    them, so a missing Decimal conversion fails here rather than in production.
"""

from decimal import Decimal

import pytest
from boto3.dynamodb.conditions import Key


class FakeTable:
    def __init__(self, name, items=None):
        self.name = name
        # list of plain dicts; the fake sorts on read the way DynamoDB does
        self.items = list(items or [])
        self.queries = []

    def query(self, **kwargs):
        self.queries.append(kwargs)
        cond = kwargs["KeyConditionExpression"]
        expr = cond.get_expression()
        # ("session_id", value) is always the leading equality condition
        sid = _extract_session_id(expr)
        rows = [i for i in self.items if i["session_id"] == sid]
        since = _extract_since(expr)
        if since is not None:
            rows = [r for r in rows if r["seq"] > since]
        rows.sort(key=lambda r: r["seq"])
        limit = kwargs.get("Limit")
        if limit:
            rows = rows[:limit]
        return {"Items": rows, "Count": len(rows)}

    def get_item(self, Key):
        for i in self.items:
            if all(i.get(k) == v for k, v in Key.items()):
                return {"Item": i}
        return {}

    def scan(self, **kwargs):
        rows = list(self.items)
        limit = kwargs.get("Limit")
        if limit:
            rows = rows[:limit]
        return {"Items": rows, "Count": len(rows)}


def _extract_session_id(expr):
    # Conditions nest as And(Key('session_id').eq(x), Key('seq').gt(y)).
    # Each leaf exposes get_expression() with 'operator' and 'values', where
    # values[0] is the Attr (with a .name) and values[1] is the bound value.
    return _walk(expr, "session_id", "=")


def _extract_since(expr):
    return _walk(expr, "seq", ">")


def _walk(expr, attr_name, operator):
    values = expr.get("values", ())
    if expr.get("operator") == operator:
        left = values[0]
        if getattr(left, "name", None) == attr_name:
            return values[1]
    for v in values:
        if hasattr(v, "get_expression"):
            found = _walk(v.get_expression(), attr_name, operator)
            if found is not None:
                return found
    return None


def event_item(session_id, seq, event_type="log.line", game_time_s="1.0"):
    """Build an item shaped exactly as DynamoDB returns it -- Decimal included."""
    return {
        "session_id": session_id,
        "seq": f"{seq:020d}",
        "event_type": event_type,
        "raw": f"{game_time_s} Sys [Info]: something",
        "attrs": {},
        "v": Decimal("1"),
        "game_time_s": Decimal(game_time_s),
        "wall_time_utc": "2026-08-09T02:00:00Z",
        "expires_at": Decimal("1786845741"),
    }


def session_item(session_id, last_seen_at, event_count=10):
    return {
        "session_id": session_id,
        "started_at": "2026-08-09T01:56:49.759Z",
        "last_seen_at": last_seen_at,
        "event_count": Decimal(str(event_count)),
        "expires_at": Decimal("1786845741"),
    }


@pytest.fixture
def events_table():
    return FakeTable("relic-events")


@pytest.fixture
def sessions_table():
    return FakeTable("relic-sessions")
```

- [ ] **Step 2: Write the failing test**

Create `infra/pipeline/lambda/api/tests/test_queries.py`:

```python
from tests.conftest import FakeTable, event_item, session_item

from queries import MAX_SESSIONS, get_events, list_sessions, session_exists


def test_get_events_returns_all_when_since_is_none(events_table):
    events_table.items = [event_item("s1", n) for n in (1, 2, 3)]
    rows = get_events(events_table, "s1", None)
    assert [r["seq"] for r in rows] == [f"{n:020d}" for n in (1, 2, 3)]


def test_get_events_since_is_exclusive(events_table):
    events_table.items = [event_item("s1", n) for n in (1, 2, 3)]
    rows = get_events(events_table, "s1", f"{2:020d}")
    # strictly greater: seq 2 itself must not come back, or every poll would
    # re-deliver the last event it already rendered
    assert [r["seq"] for r in rows] == [f"{3:020d}"]


def test_get_events_returns_ascending_by_seq(events_table):
    events_table.items = [event_item("s1", n) for n in (3, 1, 2)]
    rows = get_events(events_table, "s1", None)
    assert [r["seq"] for r in rows] == sorted(r["seq"] for r in rows)


def test_get_events_ignores_other_sessions(events_table):
    events_table.items = [event_item("s1", 1), event_item("s2", 1)]
    rows = get_events(events_table, "s1", None)
    assert len(rows) == 1
    assert rows[0]["session_id"] == "s1"


def test_get_events_empty_when_nothing_past_cursor(events_table):
    events_table.items = [event_item("s1", 1)]
    assert get_events(events_table, "s1", f"{1:020d}") == []


def test_list_sessions_newest_first(sessions_table):
    sessions_table.items = [
        session_item("old", "2026-08-09T01:00:00Z"),
        session_item("new", "2026-08-09T03:00:00Z"),
        session_item("mid", "2026-08-09T02:00:00Z"),
    ]
    rows = list_sessions(sessions_table)
    assert [r["session_id"] for r in rows] == ["new", "mid", "old"]


def test_list_sessions_caps_at_max(sessions_table):
    sessions_table.items = [
        session_item(f"s{n}", f"2026-08-09T{n:02d}:00:00Z") for n in range(25)
    ]
    assert len(list_sessions(sessions_table)) == MAX_SESSIONS


def test_session_exists_true_and_false(sessions_table):
    sessions_table.items = [session_item("s1", "2026-08-09T01:00:00Z")]
    assert session_exists(sessions_table, "s1") is True
    assert session_exists(sessions_table, "nope") is False
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `cd infra/pipeline/lambda/api && uv run --python 3.12 pytest tests/test_queries.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'queries'`

- [ ] **Step 4: Write the implementation**

Create `infra/pipeline/lambda/api/queries.py`:

```python
"""DynamoDB reads. Table objects in, plain dicts out.

No HTTP knowledge: this module does not know what a status code is. That
keeps it testable against a fake table with no request plumbing.
"""

from boto3.dynamodb.conditions import Key

# relic-sessions holds one row per play session, so this bounds the scan to a
# trivially small result set.
MAX_SESSIONS = 20


def get_events(table, session_id: str, since: str | None) -> list[dict]:
    """Events for one session, ascending by seq, exclusive of `since`.

    `since` is the zero-padded seq string the client was last given. It is
    passed straight through -- never parsed to an int and re-padded, because
    an unpadded comparison silently matches the wrong range.
    """
    condition = Key("session_id").eq(session_id)
    if since:
        condition = condition & Key("seq").gt(since)

    # ScanIndexForward=True is the default, but stated explicitly: the feed
    # depends on ascending order, and a silent flip would render backwards.
    response = table.query(KeyConditionExpression=condition, ScanIndexForward=True)
    return response.get("Items", [])


def list_sessions(table, limit: int = MAX_SESSIONS) -> list[dict]:
    """Recent sessions, newest first.

    A Scan, deliberately: a partition key cannot be enumerated, so there is no
    Query that answers "what sessions exist". It is cheap because the table
    holds one row per session and the cap bounds it.
    """
    rows = table.scan().get("Items", [])
    rows.sort(key=lambda r: r.get("last_seen_at", ""), reverse=True)
    return rows[:limit]


def session_exists(table, session_id: str) -> bool:
    """Whether the session row is still alive.

    This is what separates 404 (row aged out after ~7d, session gone) from
    204 (row alive, events aged out after ~24h).
    """
    return "Item" in table.get_item(Key={"session_id": session_id})
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `cd infra/pipeline/lambda/api && uv run --python 3.12 pytest -v`
Expected: PASS — 14 tests (6 from Task 1 + 8 here).

- [ ] **Step 6: Report for review (DO NOT COMMIT)**

Stop and report. Do not run `git add` or `git commit`.

---

### Task 3: Handler and routing

**Files:**
- Create: `infra/pipeline/lambda/api/main.py`
- Test: `infra/pipeline/lambda/api/tests/test_main.py`

**Interfaces:**
- Consumes: `ok`, `no_content`, `not_found` (Task 1); `get_events`, `list_sessions`, `session_exists` (Task 2).
- Produces: `lambda_handler(event, context) -> dict`. Terraform sets `handler = "main.lambda_handler"`.

- [ ] **Step 1: Write the failing test**

Create `infra/pipeline/lambda/api/tests/test_main.py`:

```python
import json

import pytest

import main
from tests.conftest import FakeTable, event_item, session_item


@pytest.fixture(autouse=True)
def env(monkeypatch):
    monkeypatch.setenv("EVENTS_TABLE", "relic-events")
    monkeypatch.setenv("SESSIONS_TABLE", "relic-sessions")


@pytest.fixture
def tables(monkeypatch):
    events = FakeTable("relic-events")
    sessions = FakeTable("relic-sessions")

    class FakeResource:
        def Table(self, name):
            return events if name == "relic-events" else sessions

    monkeypatch.setattr(main, "_resource", lambda: FakeResource())
    return events, sessions


def _req(path, params=None, query=None):
    return {
        "requestContext": {"http": {"method": "GET", "path": path}},
        "pathParameters": params or {},
        "queryStringParameters": query,
    }


def test_list_sessions_route(tables):
    events, sessions = tables
    sessions.items = [session_item("s1", "2026-08-09T01:00:00Z")]

    resp = main.lambda_handler(_req("/sessions"), None)

    assert resp["statusCode"] == 200
    assert json.loads(resp["body"])["sessions"][0]["session_id"] == "s1"


def test_list_sessions_empty_is_200_not_204(tables):
    # A list endpoint returning nothing is a real answer, not "no content".
    resp = main.lambda_handler(_req("/sessions"), None)
    assert resp["statusCode"] == 200
    assert json.loads(resp["body"]) == {"sessions": []}


def test_events_route_returns_events_and_last_seq(tables):
    events, sessions = tables
    sessions.items = [session_item("s1", "2026-08-09T01:00:00Z")]
    events.items = [event_item("s1", n) for n in (1, 2)]

    resp = main.lambda_handler(
        _req("/sessions/s1/events", {"id": "s1"}), None
    )

    body = json.loads(resp["body"])
    assert resp["statusCode"] == 200
    assert len(body["events"]) == 2
    # last_seq is the padded string of the final event, handed back as the
    # client's next cursor
    assert body["last_seq"] == f"{2:020d}"


def test_events_route_204_when_nothing_past_cursor(tables):
    events, sessions = tables
    sessions.items = [session_item("s1", "2026-08-09T01:00:00Z")]
    events.items = [event_item("s1", 1)]

    resp = main.lambda_handler(
        _req("/sessions/s1/events", {"id": "s1"}, {"since": f"{1:020d}"}), None
    )

    assert resp["statusCode"] == 204
    assert "body" not in resp


def test_events_route_204_when_session_alive_but_events_expired(tables):
    events, sessions = tables
    # session row survives 7d; its events TTL'd at 24h
    sessions.items = [session_item("s1", "2026-08-07T01:00:00Z")]

    resp = main.lambda_handler(_req("/sessions/s1/events", {"id": "s1"}), None)

    assert resp["statusCode"] == 204


def test_events_route_404_when_session_row_is_gone(tables):
    # No session row at all: aged out entirely, or never existed.
    resp = main.lambda_handler(
        _req("/sessions/ghost/events", {"id": "ghost"}), None
    )
    assert resp["statusCode"] == 404


def test_unknown_route_is_404(tables):
    resp = main.lambda_handler(_req("/nope"), None)
    assert resp["statusCode"] == 404


def test_since_is_passed_through_to_the_query(tables):
    events, sessions = tables
    sessions.items = [session_item("s1", "2026-08-09T01:00:00Z")]
    events.items = [event_item("s1", n) for n in (1, 2, 3)]

    resp = main.lambda_handler(
        _req("/sessions/s1/events", {"id": "s1"}, {"since": f"{1:020d}"}), None
    )

    body = json.loads(resp["body"])
    assert [e["seq"] for e in body["events"]] == [f"{2:020d}", f"{3:020d}"]


def test_missing_query_string_is_treated_as_no_cursor(tables):
    # API Gateway omits queryStringParameters entirely when there is no query
    # string -- it is None, not {}. Indexing it would raise TypeError.
    events, sessions = tables
    sessions.items = [session_item("s1", "2026-08-09T01:00:00Z")]
    events.items = [event_item("s1", 1)]

    resp = main.lambda_handler(_req("/sessions/s1/events", {"id": "s1"}), None)

    assert resp["statusCode"] == 200
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd infra/pipeline/lambda/api && uv run --python 3.12 pytest tests/test_main.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'main'`

- [ ] **Step 3: Write the implementation**

Create `infra/pipeline/lambda/api/main.py`:

```python
"""Read API: HTTP API -> DynamoDB.

The only module importing boto3. Querying lives in queries.py and response
shaping in responses.py, both pure and tested without AWS.

Read-only by construction: this function's IAM role grants Query, GetItem,
and Scan and nothing else, so a write bug cannot corrupt the serving layer.
"""

import logging
import os

import boto3

from queries import get_events, list_sessions, session_exists
from responses import no_content, not_found, ok

logger = logging.getLogger()
logger.setLevel(logging.INFO)

_ddb = None


def _resource():
    """The DynamoDB service resource.

    boto3.resource, not boto3.client: Table objects deserialize into plain
    Python values. The low-level client would hand back {"S": "..."} typed
    attribute maps that every caller would have to unwrap. Cached across warm
    invocations; tests monkeypatch this function.
    """
    global _ddb
    if _ddb is None:
        _ddb = boto3.resource("dynamodb")
    return _ddb


def lambda_handler(event, context):
    http = event.get("requestContext", {}).get("http", {})
    path = http.get("path", "")
    ddb = _resource()

    if path == "/sessions":
        table = ddb.Table(os.environ["SESSIONS_TABLE"])
        return ok({"sessions": list_sessions(table)})

    if path.startswith("/sessions/") and path.endswith("/events"):
        session_id = (event.get("pathParameters") or {}).get("id")
        if not session_id:
            return not_found("missing session id")

        sessions = ddb.Table(os.environ["SESSIONS_TABLE"])
        # 404 vs 204 is the whole expired-session story: no row means the
        # session aged out entirely (~7d), while a row with no events means
        # the events aged out (~24h) but the summary survives.
        if not session_exists(sessions, session_id):
            return not_found("no such session")

        # queryStringParameters is None (not {}) when there is no query string.
        since = (event.get("queryStringParameters") or {}).get("since")
        events = get_events(ddb.Table(os.environ["EVENTS_TABLE"]), session_id, since)
        if not events:
            return no_content()

        return ok({"events": events, "last_seq": events[-1]["seq"]})

    logger.warning("unrouted path: %s", path)
    return not_found("unknown route")
```

- [ ] **Step 4: Run the full suite**

Run: `cd infra/pipeline/lambda/api && uv run --python 3.12 pytest -v`
Expected: PASS — 23 tests (6 + 8 + 9).

- [ ] **Step 5: Report for review (DO NOT COMMIT)**

Stop and report. Do not run `git add` or `git commit`.

---

### Task 4: Terraform — API Lambda, IAM, HTTP API

**Files:**
- Create: `infra/pipeline/api.tf`
- Create: `infra/pipeline/outputs.tf`
- Modify: `infra/pipeline/lambda.tf` (append)
- Modify: `infra/pipeline/iam.tf` (append)

**Interfaces:**
- Consumes: `aws_dynamodb_table.events`, `aws_dynamodb_table.sessions`, `data.aws_caller_identity.current` (all already exist in the stack).
- Produces: `aws_apigatewayv2_api.relic_api`, and an output named `api_invoke_url`.

- [ ] **Step 1: Append the Lambda to `lambda.tf`**

```hcl
data "archive_file" "api_func_files" {
  type        = "zip"
  source_dir  = "${path.module}/lambda/api"
  output_path = "${path.module}/lambda/api.zip"

  excludes = [
    "tests",
    "pyproject.toml",
    "uv.lock",
    "__pycache__",
    ".pytest_cache",
    ".venv",
  ]
}

resource "aws_lambda_function" "api" {
  filename         = data.archive_file.api_func_files.output_path
  source_code_hash = data.archive_file.api_func_files.output_base64sha256
  function_name    = "relic-api"
  role             = aws_iam_role.api_lambda_role.arn
  handler          = "main.lambda_handler"
  runtime          = "python3.12"

  environment {
    variables = {
      EVENTS_TABLE   = aws_dynamodb_table.events.name
      SESSIONS_TABLE = aws_dynamodb_table.sessions.name
    }
  }
}
```

- [ ] **Step 2: Append IAM to `iam.tf`**

```hcl
resource "aws_iam_role" "api_lambda_role" {
  name = "relic-api-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
        # lambda.amazonaws.com is a global principal, not account-scoped.
        # Without this condition any account could assume the role.
        Condition = {
          StringEquals = { "aws:SourceAccount" = data.aws_caller_identity.current.account_id }
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "api_lambda_basic" {
  role       = aws_iam_role.api_lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# Read-only, deliberately. The hot-path role is write-only; neither function
# can do the other's job.
resource "aws_iam_policy" "api_ddb_read" {
  name        = "relic-api-ddb-read-policy"
  description = "Allows the read API to query events and sessions"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "dynamodb:Query",
          "dynamodb:GetItem",
          "dynamodb:Scan"
        ]
        Resource = [
          aws_dynamodb_table.events.arn,
          aws_dynamodb_table.sessions.arn
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "api_ddb_read_attach" {
  role       = aws_iam_role.api_lambda_role.name
  policy_arn = aws_iam_policy.api_ddb_read.arn
}
```

- [ ] **Step 3: Create `api.tf`**

```hcl
# HTTP API, not REST API: cheaper per request, and none of REST's extras
# (usage plans, API keys, request validators) are needed here.
resource "aws_apigatewayv2_api" "relic_api" {
  name          = "relic-api"
  protocol_type = "HTTP"

  # The browser calls this from http://localhost:3000, a different origin, so
  # without CORS the fetch fails before any status code is read. Not a
  # wildcard: this is what stops another site's JS from reading session
  # telemetry while the pipeline is applied. CORS is browser-only and is NOT
  # what makes the API private -- there is no auth, by design (phase-1 §6).
  cors_configuration {
    allow_origins = ["http://localhost:3000"]
    allow_methods = ["GET"]
    allow_headers = ["content-type"]
    max_age       = 300
  }
}

resource "aws_apigatewayv2_integration" "api" {
  api_id                 = aws_apigatewayv2_api.relic_api.id
  integration_type       = "AWS_PROXY"
  integration_uri        = aws_lambda_function.api.invoke_arn
  payload_format_version = "2.0"
}

resource "aws_apigatewayv2_route" "list_sessions" {
  api_id    = aws_apigatewayv2_api.relic_api.id
  route_key = "GET /sessions"
  target    = "integrations/${aws_apigatewayv2_integration.api.id}"
}

resource "aws_apigatewayv2_route" "session_events" {
  api_id    = aws_apigatewayv2_api.relic_api.id
  route_key = "GET /sessions/{id}/events"
  target    = "integrations/${aws_apigatewayv2_integration.api.id}"
}

# auto_deploy avoids a separate deployment resource that must be tainted on
# every change.
resource "aws_apigatewayv2_stage" "default" {
  api_id      = aws_apigatewayv2_api.relic_api.id
  name        = "$default"
  auto_deploy = true
}

resource "aws_lambda_permission" "api_gateway" {
  statement_id  = "AllowExecutionFromAPIGateway"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.api.function_name
  principal     = "apigateway.amazonaws.com"

  # apigateway.amazonaws.com is global. Without source_arn, any account's API
  # Gateway could invoke this function -- the confused-deputy problem.
  source_arn = "${aws_apigatewayv2_api.relic_api.execution_arn}/*/*"
}
```

- [ ] **Step 4: Create `outputs.tf`**

```hcl
output "api_invoke_url" {
  description = "Base URL for the read API. Put this in dashboard/.env.local as NEXT_PUBLIC_RELIC_API_URL."
  value       = aws_apigatewayv2_stage.default.invoke_url
}
```

- [ ] **Step 5: Verify the config parses**

Run: `cd infra/pipeline && terraform fmt && terraform validate`
Expected: `Success! The configuration is valid.`

Note this does not contact AWS. A clean validate says nothing about whether the API works.

- [ ] **Step 6: Verify the zip contains the right files**

Run:
```bash
cd infra/pipeline/lambda/api && rm -f /tmp/api-check.zip && \
  zip -rq /tmp/api-check.zip . -x 'tests/*' 'pyproject.toml' 'uv.lock' '__pycache__/*' '.venv/*' '.pytest_cache/*' && \
  unzip -l /tmp/api-check.zip
```
Expected: exactly `main.py`, `queries.py`, `responses.py`. No `tests/`, no `pyproject.toml`.

- [ ] **Step 7: Report for review (DO NOT COMMIT)**

Stop and report. Do not run `git add` or `git commit`.

---

### Task 5: Dashboard test harness and types

**Files:**
- Create: `dashboard/vitest.config.mts`
- Create: `dashboard/types.ts`
- Modify: `dashboard/package.json` (add `test` script and dev deps)

**Interfaces:**
- Consumes: nothing.
- Produces: `bun run test` works; and the types `Session`, `RelicEvent`, `FeedState`, `EventsResponse`, `SessionsResponse`.

- [ ] **Step 1: Install dev dependencies**

Run from `dashboard/`:
```bash
bun add -D vitest @vitejs/plugin-react jsdom @testing-library/react @testing-library/dom vite-tsconfig-paths
```

These are the exact packages Next 16's own Vitest guide lists (`node_modules/next/dist/docs/01-app/02-guides/testing/vitest.md`).

- [ ] **Step 2: Create `vitest.config.mts`**

```ts
import { defineConfig } from "vitest/config";
import react from "@vitejs/plugin-react";
import tsconfigPaths from "vite-tsconfig-paths";

export default defineConfig({
  // tsconfigPaths is what makes the "@/*" alias resolve in tests the same way
  // it does in the app.
  plugins: [tsconfigPaths(), react()],
  test: {
    environment: "jsdom",
  },
});
```

- [ ] **Step 3: Add the test script to `package.json`**

Add to `"scripts"`, keeping the existing entries:

```json
"test": "vitest run",
"test:watch": "vitest"
```

`vitest run` is non-watching, so it terminates in CI and in agent runs. Bare `vitest` watches forever.

- [ ] **Step 4: Create `types.ts`**

```ts
/** One play session, as returned by GET /sessions. */
export interface Session {
  session_id: string;
  started_at: string;
  last_seen_at: string;
  event_count: number;
}

/** One parsed log line. `attrs` is empty for log.line events. */
export interface RelicEvent {
  session_id: string;
  /** Zero-padded to width 20. Passed back to the API verbatim as the cursor. */
  seq: string;
  event_type: "log.line" | "reward.relic";
  raw: string;
  attrs: Record<string, string>;
  game_time_s?: number;
  wall_time_utc?: string;
  v: number;
}

export interface SessionsResponse {
  sessions: Session[];
}

export interface EventsResponse {
  events: RelicEvent[];
  last_seq: string;
}

/**
 * Where a session sits in the revive mechanic.
 *  alive   - polling, events arriving
 *  downed  - quiet past the threshold; auto-revive already spent
 *  dead    - a manual revive found nothing; polling stopped
 *  expired - events TTL'd out; only the summary survives
 *  error   - a request failed. NOT death: the session may be fine.
 */
export type FeedState = "alive" | "downed" | "dead" | "expired" | "error";
```

- [ ] **Step 5: Verify the harness runs**

Run: `cd dashboard && bun run test`
Expected: Vitest starts and reports "No test files found" (exit code may be non-zero — that is fine, it proves the runner works).

- [ ] **Step 6: Report for review (DO NOT COMMIT)**

Stop and report. Do not run `git add` or `git commit`.

---

### Task 6: API client (`lib/api.ts`)

**Files:**
- Create: `dashboard/lib/api.ts`
- Test: `dashboard/lib/api.test.ts`

**Interfaces:**
- Consumes: types from Task 5.
- Produces:
  - `fetchSessions(): Promise<Session[]>`
  - `fetchEvents(sessionId: string, since?: string): Promise<EventsResponse | null>` — `null` means 204.
  - `SessionGoneError` — thrown on 404.

- [ ] **Step 1: Write the failing test**

Create `dashboard/lib/api.test.ts`:

```ts
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { SessionGoneError, fetchEvents, fetchSessions } from "@/lib/api";

const BASE = "https://example.execute-api.us-east-1.amazonaws.com";

beforeEach(() => {
  vi.stubEnv("NEXT_PUBLIC_RELIC_API_URL", BASE);
});

afterEach(() => {
  vi.unstubAllEnvs();
  vi.restoreAllMocks();
});

function mockFetch(status: number, body?: unknown) {
  const res = {
    status,
    ok: status >= 200 && status < 300,
    json: async () => body,
  } as Response;
  const spy = vi.fn().mockResolvedValue(res);
  vi.stubGlobal("fetch", spy);
  return spy;
}

describe("fetchSessions", () => {
  it("returns the sessions array", async () => {
    mockFetch(200, { sessions: [{ session_id: "s1" }] });
    const sessions = await fetchSessions();
    expect(sessions).toHaveLength(1);
    expect(sessions[0].session_id).toBe("s1");
  });
});

describe("fetchEvents", () => {
  it("returns the body on 200", async () => {
    mockFetch(200, { events: [{ seq: "0".repeat(19) + "1" }], last_seq: "0".repeat(19) + "1" });
    const result = await fetchEvents("s1");
    expect(result?.events).toHaveLength(1);
  });

  it("returns null on 204 without parsing a body", async () => {
    // res.json() on a 204 throws in most runtimes -- the status must be
    // checked before any parse attempt.
    const res = {
      status: 204,
      ok: true,
      json: async () => {
        throw new Error("must not parse a 204 body");
      },
    } as unknown as Response;
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(res));

    await expect(fetchEvents("s1")).resolves.toBeNull();
  });

  it("throws SessionGoneError on 404", async () => {
    mockFetch(404, { error: "no such session" });
    await expect(fetchEvents("ghost")).rejects.toBeInstanceOf(SessionGoneError);
  });

  it("throws a plain Error on 500 so callers can distinguish it from death", async () => {
    mockFetch(500, { error: "boom" });
    const err = await fetchEvents("s1").catch((e) => e);
    expect(err).toBeInstanceOf(Error);
    expect(err).not.toBeInstanceOf(SessionGoneError);
  });

  it("sends since as a query param when given", async () => {
    const spy = mockFetch(204);
    await fetchEvents("s1", "0".repeat(19) + "5");
    expect(spy.mock.calls[0][0]).toContain("since=" + "0".repeat(19) + "5");
  });

  it("omits since entirely on first load", async () => {
    const spy = mockFetch(204);
    await fetchEvents("s1");
    expect(spy.mock.calls[0][0]).not.toContain("since");
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd dashboard && bun run test`
Expected: FAIL — cannot resolve `@/lib/api`.

- [ ] **Step 3: Write the implementation**

Create `dashboard/lib/api.ts`:

```ts
import type { EventsResponse, Session, SessionsResponse } from "@/types";

/**
 * Thrown when the API returns 404: the session row itself is gone (its ~7d
 * TTL elapsed). Distinct from a 204, which means the row is alive but its
 * events aged out, and distinct from a network failure, which says nothing
 * about the session at all.
 */
export class SessionGoneError extends Error {
  constructor(sessionId: string) {
    super(`session ${sessionId} no longer exists`);
    this.name = "SessionGoneError";
  }
}

function baseUrl(): string {
  const url = process.env.NEXT_PUBLIC_RELIC_API_URL;
  if (!url) {
    throw new Error(
      "NEXT_PUBLIC_RELIC_API_URL is not set. Get it from `terraform output api_invoke_url` in infra/pipeline and put it in dashboard/.env.local",
    );
  }
  return url.replace(/\/$/, "");
}

export async function fetchSessions(): Promise<Session[]> {
  const res = await fetch(`${baseUrl()}/sessions`);
  if (!res.ok) {
    throw new Error(`GET /sessions failed: ${res.status}`);
  }
  const body = (await res.json()) as SessionsResponse;
  return body.sessions;
}

/**
 * Events after `since`. Returns null for 204 -- "the session exists, nothing
 * is new" -- which is the signal the revive mechanic counts quiet time on.
 */
export async function fetchEvents(
  sessionId: string,
  since?: string,
): Promise<EventsResponse | null> {
  const url = new URL(`${baseUrl()}/sessions/${sessionId}/events`);
  if (since) {
    url.searchParams.set("since", since);
  }

  const res = await fetch(url.toString());

  // Check the status before touching the body: res.json() on a 204 throws.
  if (res.status === 204) {
    return null;
  }
  if (res.status === 404) {
    throw new SessionGoneError(sessionId);
  }
  if (!res.ok) {
    throw new Error(`GET events failed: ${res.status}`);
  }

  return (await res.json()) as EventsResponse;
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd dashboard && bun run test`
Expected: PASS — 7 tests.

- [ ] **Step 5: Report for review (DO NOT COMMIT)**

Stop and report. Do not run `git add` or `git commit`.

---

### Task 7: The polling hook and revive state machine

**Files:**
- Create: `dashboard/lib/useSessionFeed.ts`
- Test: `dashboard/lib/useSessionFeed.test.ts`

**Interfaces:**
- Consumes: `fetchEvents`, `SessionGoneError` (Task 6); types (Task 5).
- Produces: `useSessionFeed(sessionId: string | null)` returning `{ events, state, reviveCount, revive, error }`; and the constants `POLL_INTERVAL_MS = 2000`, `QUIET_THRESHOLD_MS = 30000`.

- [ ] **Step 1: Write the failing test**

Create `dashboard/lib/useSessionFeed.test.ts`:

```ts
import { act, renderHook, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { SessionGoneError } from "@/lib/api";
import { QUIET_THRESHOLD_MS, useSessionFeed } from "@/lib/useSessionFeed";

const seq = (n: number) => String(n).padStart(20, "0");

vi.mock("@/lib/api", async () => {
  const actual = await vi.importActual<typeof import("@/lib/api")>("@/lib/api");
  return { ...actual, fetchEvents: vi.fn() };
});

const { fetchEvents } = await import("@/lib/api");
const mockFetchEvents = vi.mocked(fetchEvents);

beforeEach(() => {
  vi.useFakeTimers({ shouldAdvanceTime: true });
  mockFetchEvents.mockReset();
});

afterEach(() => {
  vi.useRealTimers();
});

describe("useSessionFeed", () => {
  it("starts alive and appends events", async () => {
    mockFetchEvents.mockResolvedValue({
      events: [{ seq: seq(1), event_type: "log.line", raw: "x", attrs: {}, v: 1, session_id: "s1" }],
      last_seq: seq(1),
    });

    const { result } = renderHook(() => useSessionFeed("s1"));

    await waitFor(() => expect(result.current.events).toHaveLength(1));
    expect(result.current.state).toBe("alive");
  });

  it("advances the cursor so each poll asks for only what is new", async () => {
    mockFetchEvents.mockResolvedValueOnce({
      events: [{ seq: seq(5), event_type: "log.line", raw: "x", attrs: {}, v: 1, session_id: "s1" }],
      last_seq: seq(5),
    });
    mockFetchEvents.mockResolvedValue(null);

    renderHook(() => useSessionFeed("s1"));

    await waitFor(() => expect(mockFetchEvents).toHaveBeenCalledTimes(1));
    await act(async () => {
      await vi.advanceTimersByTimeAsync(2000);
    });

    expect(mockFetchEvents).toHaveBeenLastCalledWith("s1", seq(5));
  });

  it("goes downed after the quiet threshold, spending exactly one auto-revive", async () => {
    mockFetchEvents.mockResolvedValue(null);

    const { result } = renderHook(() => useSessionFeed("s1"));

    await act(async () => {
      await vi.advanceTimersByTimeAsync(QUIET_THRESHOLD_MS + 2000);
    });

    await waitFor(() => expect(result.current.state).toBe("downed"));
    // The auto-revive is spent silently before the prompt appears, so the
    // visible count stays 0 until the user clicks.
    expect(result.current.reviveCount).toBe(0);
  });

  it("returns to alive when a quiet stretch ends on its own", async () => {
    mockFetchEvents.mockResolvedValue(null);
    const { result } = renderHook(() => useSessionFeed("s1"));

    await act(async () => {
      await vi.advanceTimersByTimeAsync(10000);
    });

    mockFetchEvents.mockResolvedValue({
      events: [{ seq: seq(9), event_type: "log.line", raw: "x", attrs: {}, v: 1, session_id: "s1" }],
      last_seq: seq(9),
    });

    await act(async () => {
      await vi.advanceTimersByTimeAsync(2000);
    });

    await waitFor(() => expect(result.current.state).toBe("alive"));
  });

  it("manual revive from downed sends exactly one request", async () => {
    mockFetchEvents.mockResolvedValue(null);
    const { result } = renderHook(() => useSessionFeed("s1"));

    await act(async () => {
      await vi.advanceTimersByTimeAsync(QUIET_THRESHOLD_MS + 2000);
    });
    await waitFor(() => expect(result.current.state).toBe("downed"));

    mockFetchEvents.mockClear();
    await act(async () => {
      await result.current.revive();
    });

    expect(mockFetchEvents).toHaveBeenCalledTimes(1);
    expect(result.current.state).toBe("dead");
    expect(result.current.reviveCount).toBe(1);
  });

  it("stops polling once dead", async () => {
    mockFetchEvents.mockResolvedValue(null);
    const { result } = renderHook(() => useSessionFeed("s1"));

    await act(async () => {
      await vi.advanceTimersByTimeAsync(QUIET_THRESHOLD_MS + 2000);
    });
    await act(async () => {
      await result.current.revive();
    });
    await waitFor(() => expect(result.current.state).toBe("dead"));

    mockFetchEvents.mockClear();
    await act(async () => {
      await vi.advanceTimersByTimeAsync(10000);
    });

    // A dead session costs zero requests. That is the point of the mechanic.
    expect(mockFetchEvents).not.toHaveBeenCalled();
  });

  it("a network error does NOT kill the session", async () => {
    mockFetchEvents.mockRejectedValue(new Error("network down"));

    const { result } = renderHook(() => useSessionFeed("s1"));

    await waitFor(() => expect(result.current.state).toBe("error"));
    // Wifi hiccups are not session death: keep polling.
    mockFetchEvents.mockClear();
    await act(async () => {
      await vi.advanceTimersByTimeAsync(2000);
    });
    expect(mockFetchEvents).toHaveBeenCalled();
  });

  it("a 404 marks the session expired and stops polling", async () => {
    mockFetchEvents.mockRejectedValue(new SessionGoneError("s1"));

    const { result } = renderHook(() => useSessionFeed("s1"));

    await waitFor(() => expect(result.current.state).toBe("expired"));
    mockFetchEvents.mockClear();
    await act(async () => {
      await vi.advanceTimersByTimeAsync(5000);
    });
    expect(mockFetchEvents).not.toHaveBeenCalled();
  });

  it("does nothing without a session id", async () => {
    renderHook(() => useSessionFeed(null));
    await act(async () => {
      await vi.advanceTimersByTimeAsync(5000);
    });
    expect(mockFetchEvents).not.toHaveBeenCalled();
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd dashboard && bun run test`
Expected: FAIL — cannot resolve `@/lib/useSessionFeed`.

- [ ] **Step 3: Write the implementation**

Create `dashboard/lib/useSessionFeed.ts`:

```ts
"use client";

import { useCallback, useEffect, useRef, useState } from "react";

import { SessionGoneError, fetchEvents } from "@/lib/api";
import type { FeedState, RelicEvent } from "@/types";

export const POLL_INTERVAL_MS = 2000;

/**
 * How long a session may go without new events before it is questioned.
 * At a 2s poll that is ~15 consecutive empty responses -- long enough to sit
 * through a loading screen, short enough that a finished session stops
 * claiming to be live within half a minute.
 */
export const QUIET_THRESHOLD_MS = 30000;

interface FeedResult {
  events: RelicEvent[];
  state: FeedState;
  reviveCount: number;
  revive: () => Promise<void>;
  error: string | null;
}

export function useSessionFeed(sessionId: string | null): FeedResult {
  const [events, setEvents] = useState<RelicEvent[]>([]);
  const [state, setState] = useState<FeedState>("alive");
  const [reviveCount, setReviveCount] = useState(0);
  const [error, setError] = useState<string | null>(null);

  const cursor = useRef<string | undefined>(undefined);
  // Time of the last response that actually carried events -- NOT the last
  // request. Measuring from the last request would let a stream of 204s reset
  // the timer forever, and nothing would ever go downed.
  const lastEventAt = useRef<number>(Date.now());
  const autoRevived = useRef(false);

  const poll = useCallback(async (): Promise<boolean> => {
    if (!sessionId) return false;

    try {
      const result = await fetchEvents(sessionId, cursor.current);
      setError(null);

      if (result && result.events.length > 0) {
        setEvents((prev) => [...prev, ...result.events]);
        cursor.current = result.last_seq;
        lastEventAt.current = Date.now();
        autoRevived.current = false;
        setState("alive");
        return true;
      }
      return false;
    } catch (err) {
      if (err instanceof SessionGoneError) {
        setState("expired");
        return false;
      }
      // Any other failure is a transport problem, not a dead session. Surface
      // it and keep polling: the game may be running perfectly.
      setError(err instanceof Error ? err.message : String(err));
      setState("error");
      return false;
    }
  }, [sessionId]);

  const revive = useCallback(async () => {
    setReviveCount((n) => n + 1);
    const found = await poll();
    if (!found) {
      setState("dead");
    }
  }, [poll]);

  useEffect(() => {
    if (!sessionId) return;

    // Reset everything when the selected session changes.
    setEvents([]);
    setState("alive");
    setReviveCount(0);
    setError(null);
    cursor.current = undefined;
    lastEventAt.current = Date.now();
    autoRevived.current = false;
  }, [sessionId]);

  useEffect(() => {
    if (!sessionId) return;
    // dead and expired are terminal: no interval at all, so a finished
    // session costs nothing.
    if (state === "dead" || state === "expired") return;

    let cancelled = false;

    const tick = async () => {
      const found = await poll();
      if (cancelled || found) return;

      if (Date.now() - lastEventAt.current >= QUIET_THRESHOLD_MS) {
        if (!autoRevived.current) {
          // Spend one free revive before ever prompting, so a brief gap
          // self-heals without the user touching anything.
          autoRevived.current = true;
          const recovered = await poll();
          if (!cancelled && !recovered) {
            setState("downed");
          }
          return;
        }
        setState("downed");
      }
    };

    void tick();
    const id = setInterval(() => void tick(), POLL_INTERVAL_MS);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, [sessionId, state, poll]);

  return { events, state, reviveCount, revive, error };
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd dashboard && bun run test`
Expected: PASS — all tests including the 9 hook tests.

- [ ] **Step 5: Report for review (DO NOT COMMIT)**

Stop and report. Do not run `git add` or `git commit`.

---

### Task 8: Event display components

**Files:**
- Create: `dashboard/components/EventRow/{EventRow.tsx,EventRow.module.css,index.tsx}`
- Create: `dashboard/components/EventFeed/{EventFeed.tsx,EventFeed.module.css,index.tsx}`
- Test: `dashboard/components/EventRow/EventRow.test.tsx`

**Interfaces:**
- Consumes: `RelicEvent` (Task 5).
- Produces: `EventRow({ event }: { event: RelicEvent })`, `EventFeed({ events }: { events: RelicEvent[] })`.

- [ ] **Step 1: Write the failing test**

Create `dashboard/components/EventRow/EventRow.test.tsx`:

```tsx
import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { EventRow } from "@/components/EventRow";
import type { RelicEvent } from "@/types";

const base: RelicEvent = {
  session_id: "s1",
  seq: "0".repeat(20),
  event_type: "log.line",
  raw: "1.0 Sys [Info]: something happened",
  attrs: {},
  v: 1,
  game_time_s: 1.0,
};

describe("EventRow", () => {
  it("renders a log line's raw text", () => {
    render(<EventRow event={base} />);
    expect(screen.getByText(/something happened/)).toBeDefined();
  });

  it("shows the item name prominently for a relic reward", () => {
    const reward: RelicEvent = {
      ...base,
      event_type: "reward.relic",
      game_time_s: 186.318,
      attrs: {
        player_id: "player0000",
        item_path: "/Lotus/StoreItems/Types/Recipes/Weapons/WeaponParts/GyrePrimeSystemsBlueprint",
        item_name: "GyrePrimeSystemsBlueprint",
      },
      raw: "186.318 Sys [Info]: VoidProjections: player0000 gets reward /Lotus/...",
    };

    render(<EventRow event={reward} />);

    // The whole point of the parser made visible: one line out of thousands
    // looks different because it matters.
    expect(screen.getByText("GyrePrimeSystemsBlueprint")).toBeDefined();
    expect(screen.getByText(/186\.318/)).toBeDefined();
  });

  it("renders without a game clock", () => {
    // Lines logged before the header is parsed carry no timestamp at all.
    const { game_time_s, ...noClock } = base;
    render(<EventRow event={noClock as RelicEvent} />);
    expect(screen.getByText(/something happened/)).toBeDefined();
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd dashboard && bun run test`
Expected: FAIL — cannot resolve `@/components/EventRow`.

- [ ] **Step 3: Write `EventRow`**

`dashboard/components/EventRow/EventRow.tsx`:

```tsx
import type { RelicEvent } from "@/types";

import styles from "./EventRow.module.css";

export function EventRow({ event }: { event: RelicEvent }) {
  const isReward = event.event_type === "reward.relic";
  const clock =
    event.game_time_s === undefined ? "" : event.game_time_s.toFixed(3);

  if (isReward) {
    return (
      <li className={`${styles.row} ${styles.reward}`}>
        <span className={styles.clock}>{clock}</span>
        <span className={styles.label}>REWARD</span>
        <span className={styles.item}>{event.attrs.item_name}</span>
      </li>
    );
  }

  return (
    <li className={styles.row}>
      <span className={styles.clock}>{clock}</span>
      <span className={styles.raw}>{event.raw}</span>
    </li>
  );
}
```

`dashboard/components/EventRow/EventRow.module.css`:

```css
.row {
  display: flex;
  gap: 0.75rem;
  padding: 0.15rem 0.5rem;
  font-family: var(--font-geist-mono), monospace;
  font-size: 0.8rem;
  line-height: 1.5;
  border-left: 2px solid transparent;
}

.clock {
  flex: 0 0 5rem;
  text-align: right;
  opacity: 0.5;
  font-variant-numeric: tabular-nums;
}

.raw {
  opacity: 0.75;
  white-space: pre-wrap;
  word-break: break-word;
}

.reward {
  border-left-color: #f5c542;
  background: rgba(245, 197, 66, 0.08);
  padding-block: 0.4rem;
}

.label {
  flex: 0 0 auto;
  color: #f5c542;
  font-weight: 700;
  letter-spacing: 0.08em;
}

.item {
  font-weight: 600;
}
```

`dashboard/components/EventRow/index.tsx`:

```tsx
export { EventRow } from "./EventRow";
```

- [ ] **Step 4: Write `EventFeed`**

`dashboard/components/EventFeed/EventFeed.tsx`:

```tsx
import { EventRow } from "@/components/EventRow";
import type { RelicEvent } from "@/types";

import styles from "./EventFeed.module.css";

export function EventFeed({ events }: { events: RelicEvent[] }) {
  if (events.length === 0) {
    return <p className={styles.empty}>No events yet.</p>;
  }

  return (
    <ul className={styles.feed}>
      {events.map((event) => (
        <li key={`${event.session_id}:${event.seq}`} className={styles.item}>
          <EventRow event={event} />
        </li>
      ))}
    </ul>
  );
}
```

Note the key is `session_id:seq` — the project's idempotency key, and the only pair guaranteed unique.

`dashboard/components/EventFeed/EventFeed.module.css`:

```css
.feed {
  list-style: none;
  margin: 0;
  padding: 0;
  overflow-y: auto;
  max-height: 70vh;
}

.item {
  list-style: none;
}

.empty {
  opacity: 0.6;
  padding: 1rem;
}
```

`dashboard/components/EventFeed/index.tsx`:

```tsx
export { EventFeed } from "./EventFeed";
```

- [ ] **Step 5: Run the tests**

Run: `cd dashboard && bun run test`
Expected: PASS — 3 new tests plus everything prior.

- [ ] **Step 6: Report for review (DO NOT COMMIT)**

Stop and report. Do not run `git add` or `git commit`.

---

### Task 9: Session chrome components

**Files:**
- Create: `dashboard/components/SessionList/{SessionList.tsx,SessionList.module.css,index.tsx}`
- Create: `dashboard/components/SessionHeader/{SessionHeader.tsx,SessionHeader.module.css,index.tsx}`
- Create: `dashboard/components/ReviveButton/{ReviveButton.tsx,ReviveButton.module.css,index.tsx}`
- Create: `dashboard/components/ExpiredCard/{ExpiredCard.tsx,ExpiredCard.module.css,index.tsx}`
- Test: `dashboard/components/ReviveButton/ReviveButton.test.tsx`

**Interfaces:**
- Consumes: `Session`, `FeedState` (Task 5).
- Produces:
  - `SessionList({ sessions, selectedId, onSelect })`
  - `SessionHeader({ session, state })`
  - `ReviveButton({ state, reviveCount, onRevive })`
  - `ExpiredCard({ session })`

- [ ] **Step 1: Write the failing test**

Create `dashboard/components/ReviveButton/ReviveButton.test.tsx`:

```tsx
import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { ReviveButton } from "@/components/ReviveButton";

describe("ReviveButton", () => {
  it("renders nothing while the session is alive", () => {
    const { container } = render(
      <ReviveButton state="alive" reviveCount={0} onRevive={vi.fn()} />,
    );
    expect(container.firstChild).toBeNull();
  });

  it("prompts when downed", () => {
    render(<ReviveButton state="downed" reviveCount={0} onRevive={vi.fn()} />);
    expect(screen.getByRole("button")).toBeDefined();
  });

  it("still offers revive when dead", () => {
    // Unlimited revives: refusing to check a session that might be active
    // would be thematic but wrong.
    render(<ReviveButton state="dead" reviveCount={3} onRevive={vi.fn()} />);
    expect(screen.getByRole("button")).toBeDefined();
  });

  it("shows the bleed-out count once it is non-zero", () => {
    render(<ReviveButton state="dead" reviveCount={2} onRevive={vi.fn()} />);
    expect(screen.getByText(/2/)).toBeDefined();
  });

  it("calls onRevive when clicked", async () => {
    const onRevive = vi.fn();
    render(<ReviveButton state="downed" reviveCount={0} onRevive={onRevive} />);
    screen.getByRole("button").click();
    expect(onRevive).toHaveBeenCalledTimes(1);
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd dashboard && bun run test`
Expected: FAIL — cannot resolve `@/components/ReviveButton`.

- [ ] **Step 3: Write `ReviveButton`**

`dashboard/components/ReviveButton/ReviveButton.tsx`:

```tsx
"use client";

import type { FeedState } from "@/types";

import styles from "./ReviveButton.module.css";

interface Props {
  state: FeedState;
  reviveCount: number;
  onRevive: () => void;
}

export function ReviveButton({ state, reviveCount, onRevive }: Props) {
  if (state !== "downed" && state !== "dead") {
    return null;
  }

  return (
    <div className={styles.wrap}>
      <span className={styles.status}>
        {state === "downed" ? "Session downed" : "Session dead"}
      </span>
      <button type="button" className={styles.button} onClick={onRevive}>
        Revive
      </button>
      {reviveCount > 0 && (
        <span className={styles.count}>revived {reviveCount}x</span>
      )}
    </div>
  );
}
```

`dashboard/components/ReviveButton/ReviveButton.module.css`:

```css
.wrap {
  display: flex;
  align-items: center;
  gap: 0.75rem;
  padding: 0.5rem 0.75rem;
  border: 1px solid rgba(220, 80, 80, 0.5);
  border-radius: 6px;
  background: rgba(220, 80, 80, 0.08);
}

.status {
  font-weight: 600;
}

.button {
  padding: 0.3rem 0.9rem;
  border: 1px solid currentColor;
  border-radius: 4px;
  background: transparent;
  color: inherit;
  cursor: pointer;
  font: inherit;
}

.button:hover {
  background: rgba(255, 255, 255, 0.08);
}

.count {
  opacity: 0.6;
  font-size: 0.85rem;
}
```

`dashboard/components/ReviveButton/index.tsx`:

```tsx
export { ReviveButton } from "./ReviveButton";
```

- [ ] **Step 4: Write `SessionList`**

`dashboard/components/SessionList/SessionList.tsx`:

```tsx
"use client";

import type { Session } from "@/types";

import styles from "./SessionList.module.css";

interface Props {
  sessions: Session[];
  selectedId: string | null;
  onSelect: (sessionId: string) => void;
}

/** A session counts as live if it was updated within the quiet threshold. */
function isLive(session: Session): boolean {
  return Date.now() - Date.parse(session.last_seen_at) < 30000;
}

export function SessionList({ sessions, selectedId, onSelect }: Props) {
  if (sessions.length === 0) {
    return <p className={styles.empty}>No sessions. Is the pipeline applied?</p>;
  }

  return (
    <ul className={styles.list}>
      {sessions.map((session) => (
        <li key={session.session_id}>
          <button
            type="button"
            className={
              session.session_id === selectedId
                ? `${styles.item} ${styles.selected}`
                : styles.item
            }
            onClick={() => onSelect(session.session_id)}
          >
            <span className={styles.id}>{session.session_id.slice(0, 8)}</span>
            <span className={styles.count}>{session.event_count} events</span>
            {isLive(session) && <span className={styles.live}>LIVE</span>}
          </button>
        </li>
      ))}
    </ul>
  );
}
```

`dashboard/components/SessionList/SessionList.module.css`:

```css
.list {
  list-style: none;
  margin: 0;
  padding: 0;
}

.item {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  width: 100%;
  padding: 0.5rem 0.75rem;
  border: 0;
  border-radius: 4px;
  background: transparent;
  color: inherit;
  font: inherit;
  text-align: left;
  cursor: pointer;
}

.item:hover {
  background: rgba(255, 255, 255, 0.06);
}

.selected {
  background: rgba(255, 255, 255, 0.1);
}

.id {
  font-family: var(--font-geist-mono), monospace;
}

.count {
  opacity: 0.6;
  font-size: 0.85rem;
  margin-left: auto;
}

.live {
  color: #4ade80;
  font-size: 0.7rem;
  font-weight: 700;
  letter-spacing: 0.08em;
}

.empty {
  opacity: 0.6;
  padding: 1rem;
  font-size: 0.9rem;
}
```

`dashboard/components/SessionList/index.tsx`:

```tsx
export { SessionList } from "./SessionList";
```

- [ ] **Step 5: Write `SessionHeader`**

`dashboard/components/SessionHeader/SessionHeader.tsx`:

```tsx
import type { FeedState, Session } from "@/types";

import styles from "./SessionHeader.module.css";

const LABELS: Record<FeedState, string> = {
  alive: "LIVE",
  downed: "DOWNED",
  dead: "DEAD",
  expired: "EXPIRED",
  error: "CONNECTION ERROR",
};

export function SessionHeader({
  session,
  state,
}: {
  session: Session;
  state: FeedState;
}) {
  const durationMs =
    Date.parse(session.last_seen_at) - Date.parse(session.started_at);
  const minutes = Math.max(0, Math.round(durationMs / 60000));

  return (
    <header className={styles.header}>
      <h2 className={styles.id}>{session.session_id.slice(0, 12)}</h2>
      <span className={`${styles.chip} ${styles[state]}`}>{LABELS[state]}</span>
      <span className={styles.meta}>
        {minutes} min &middot; {session.event_count} events
      </span>
    </header>
  );
}
```

`dashboard/components/SessionHeader/SessionHeader.module.css`:

```css
.header {
  display: flex;
  align-items: center;
  gap: 0.75rem;
  padding: 0.75rem 0;
  border-bottom: 1px solid rgba(255, 255, 255, 0.12);
}

.id {
  margin: 0;
  font-family: var(--font-geist-mono), monospace;
  font-size: 1rem;
}

.chip {
  padding: 0.1rem 0.5rem;
  border-radius: 999px;
  font-size: 0.7rem;
  font-weight: 700;
  letter-spacing: 0.08em;
}

.alive {
  background: rgba(74, 222, 128, 0.15);
  color: #4ade80;
}

.downed,
.error {
  background: rgba(245, 197, 66, 0.15);
  color: #f5c542;
}

.dead,
.expired {
  background: rgba(220, 80, 80, 0.15);
  color: #e06c6c;
}

.meta {
  opacity: 0.6;
  font-size: 0.85rem;
  margin-left: auto;
}
```

`dashboard/components/SessionHeader/index.tsx`:

```tsx
export { SessionHeader } from "./SessionHeader";
```

- [ ] **Step 6: Write `ExpiredCard`**

`dashboard/components/ExpiredCard/ExpiredCard.tsx`:

```tsx
import type { Session } from "@/types";

import styles from "./ExpiredCard.module.css";

/**
 * Shown when a session's events have aged out of DynamoDB (~24h TTL) while
 * its summary row survives (~7d). The honest answer: this session happened,
 * here is how big it was, and its detail is gone from the hot path.
 */
export function ExpiredCard({ session }: { session: Session }) {
  return (
    <div className={styles.card}>
      <h3 className={styles.title}>Events expired</h3>
      <p className={styles.body}>
        This session recorded <strong>{session.event_count}</strong> events, but
        they have aged out of the 24-hour cache. The raw lines are still in the
        S3 archive.
      </p>
      <p className={styles.meta}>
        {session.started_at} &rarr; {session.last_seen_at}
      </p>
    </div>
  );
}
```

`dashboard/components/ExpiredCard/ExpiredCard.module.css`:

```css
.card {
  padding: 1.25rem;
  border: 1px dashed rgba(255, 255, 255, 0.2);
  border-radius: 8px;
  margin-top: 1rem;
}

.title {
  margin: 0 0 0.5rem;
  font-size: 1rem;
}

.body {
  margin: 0 0 0.75rem;
  opacity: 0.8;
  line-height: 1.5;
}

.meta {
  margin: 0;
  opacity: 0.5;
  font-size: 0.8rem;
  font-family: var(--font-geist-mono), monospace;
}
```

`dashboard/components/ExpiredCard/index.tsx`:

```tsx
export { ExpiredCard } from "./ExpiredCard";
```

- [ ] **Step 7: Run the tests**

Run: `cd dashboard && bun run test`
Expected: PASS — 5 new tests plus everything prior.

- [ ] **Step 8: Report for review (DO NOT COMMIT)**

Stop and report. Do not run `git add` or `git commit`.

---

### Task 10: Page composition and env wiring

**Files:**
- Replace: `dashboard/app/page.tsx`
- Replace: `dashboard/app/page.module.css`
- Create: `dashboard/.env.local.example`
- Modify: `dashboard/app/layout.tsx` (metadata only)
- Modify: `dashboard/.gitignore` (ensure `.env.local` is ignored)

**Interfaces:**
- Consumes: every component from Tasks 8-9, `useSessionFeed` (Task 7), `fetchSessions` (Task 6).
- Produces: the working page.

- [ ] **Step 1: Create `.env.local.example`**

```bash
# Base URL of the read API. Get it after applying infra/pipeline:
#   cd infra/pipeline && terraform output -raw api_invoke_url
# The URL changes on every apply, because the pipeline is destroyed between
# play sessions.
NEXT_PUBLIC_RELIC_API_URL=https://xxxxxxxxxx.execute-api.us-east-1.amazonaws.com
```

- [ ] **Step 2: Confirm `.env.local` is git-ignored**

Run: `cd dashboard && git check-ignore -v .env.local`
Expected: a match from the scaffold's `.gitignore` (it ships `.env*`). If there is no match, append `.env.local` to `dashboard/.gitignore`.

- [ ] **Step 3: Replace `app/page.tsx`**

```tsx
"use client";

import { useCallback, useEffect, useState } from "react";

import { EventFeed } from "@/components/EventFeed";
import { ExpiredCard } from "@/components/ExpiredCard";
import { ReviveButton } from "@/components/ReviveButton";
import { SessionHeader } from "@/components/SessionHeader";
import { SessionList } from "@/components/SessionList";
import { fetchSessions } from "@/lib/api";
import { useSessionFeed } from "@/lib/useSessionFeed";
import type { Session } from "@/types";

import styles from "./page.module.css";

export default function Home() {
  const [sessions, setSessions] = useState<Session[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [listError, setListError] = useState<string | null>(null);

  const loadSessions = useCallback(async () => {
    try {
      const rows = await fetchSessions();
      setSessions(rows);
      setListError(null);
      // Auto-select the newest session so the common case -- you just
      // finished playing -- needs no clicks.
      setSelectedId((current) => current ?? rows[0]?.session_id ?? null);
    } catch (err) {
      setListError(err instanceof Error ? err.message : String(err));
    }
  }, []);

  useEffect(() => {
    void loadSessions();
    // Refresh the list every 10s: far less often than the event feed, since a
    // new session_id only appears when the game restarts.
    const id = setInterval(() => void loadSessions(), 10000);
    return () => clearInterval(id);
  }, [loadSessions]);

  const { events, state, reviveCount, revive, error } =
    useSessionFeed(selectedId);

  const selected = sessions.find((s) => s.session_id === selectedId) ?? null;

  return (
    <div className={styles.page}>
      <aside className={styles.sidebar}>
        <h1 className={styles.brand}>relic</h1>
        {listError && <p className={styles.error}>{listError}</p>}
        <SessionList
          sessions={sessions}
          selectedId={selectedId}
          onSelect={setSelectedId}
        />
      </aside>

      <main className={styles.main}>
        {selected ? (
          <>
            <SessionHeader session={selected} state={state} />
            {error && <p className={styles.error}>{error}</p>}
            <ReviveButton
              state={state}
              reviveCount={reviveCount}
              onRevive={() => void revive()}
            />
            {state === "expired" ? (
              <ExpiredCard session={selected} />
            ) : (
              <EventFeed events={events} />
            )}
          </>
        ) : (
          <p className={styles.placeholder}>Select a session.</p>
        )}
      </main>
    </div>
  );
}
```

- [ ] **Step 4: Replace `app/page.module.css`**

```css
.page {
  display: grid;
  grid-template-columns: 260px 1fr;
  gap: 1.5rem;
  min-height: 100vh;
  padding: 1.5rem;
}

.sidebar {
  border-right: 1px solid rgba(255, 255, 255, 0.12);
  padding-right: 1rem;
}

.brand {
  margin: 0 0 1rem;
  font-size: 1.1rem;
  letter-spacing: 0.1em;
  text-transform: uppercase;
  opacity: 0.7;
}

.main {
  display: flex;
  flex-direction: column;
  gap: 0.75rem;
  min-width: 0;
}

.placeholder,
.error {
  opacity: 0.7;
  font-size: 0.9rem;
}

.error {
  color: #e06c6c;
}

@media (max-width: 700px) {
  .page {
    grid-template-columns: 1fr;
  }

  .sidebar {
    border-right: 0;
    border-bottom: 1px solid rgba(255, 255, 255, 0.12);
    padding-right: 0;
    padding-bottom: 1rem;
  }
}
```

- [ ] **Step 5: Update the page metadata in `app/layout.tsx`**

Replace only the `metadata` export, leaving fonts and `LayoutProps<"/">` untouched:

```ts
export const metadata: Metadata = {
  title: "relic",
  description: "Live Warframe session feed",
};
```

- [ ] **Step 6: Verify the build and tests**

Run:
```bash
cd dashboard && bun run test && bun run build
```
Expected: all tests pass, and the build succeeds. The build proves the components typecheck together, which the unit tests alone do not.

- [ ] **Step 7: Report for review (DO NOT COMMIT)**

Stop and report. Do not run `git add` or `git commit`.

---

## Verification

After each task:

```bash
cd infra/pipeline/lambda/api && uv run --python 3.12 pytest -v   # tasks 1-3
cd infra/pipeline && terraform fmt -check && terraform validate  # task 4
cd dashboard && bun run test                                      # tasks 5-10
cd dashboard && bun run build                                     # task 10
```

**What none of this proves:** that data flows. `terraform validate` never contacts AWS, and every test runs against a fake. M3 shipped a bug that would have written zero events behind 35 green tests — the fake enforced the API's shape but not its contracts. The E2E proof is applying the pipeline, running the operator during a live session, and watching events appear in the browser.

## Out of scope

- Deployment (S3, Vercel, containers). Phase 1 is `bun dev` locally.
- Auth on the API.
- Item path → display-name mapping (`GyrePrimeSystemsBlueprint` → "Gyre Prime Systems Blueprint").
- Historical browsing past the 24h/7d TTLs — phase 2's Athena work.
- WebSockets.
