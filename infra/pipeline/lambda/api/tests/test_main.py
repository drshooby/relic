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


def test_session_exists_is_not_called_when_events_are_present(tables):
    # The dashboard polls this route every 2s for the life of a session, so
    # the common case (events present) must cost one DynamoDB read, not two.
    # session_exists is only needed to disambiguate an empty events result.
    events, sessions = tables
    sessions.items = [session_item("s1", "2026-08-09T01:00:00Z")]
    events.items = [event_item("s1", 1)]

    resp = main.lambda_handler(_req("/sessions/s1/events", {"id": "s1"}), None)

    assert resp["statusCode"] == 200
    assert sessions.get_item_calls == 0
