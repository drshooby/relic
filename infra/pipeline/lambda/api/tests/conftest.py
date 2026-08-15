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
        self.get_item_calls = 0

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
        self.get_item_calls += 1
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
