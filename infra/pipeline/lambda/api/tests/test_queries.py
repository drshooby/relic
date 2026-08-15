from tests.conftest import FakeTable, event_item, session_item

from queries import EVENTS_PAGE_LIMIT, MAX_SESSIONS, get_events, list_sessions, session_exists


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


def test_get_events_caps_at_page_limit(events_table):
    # A real session hit 6,332 events with raw log lines attached -- well
    # past what fits in one 1MB DynamoDB page. get_events must bound the
    # response rather than silently truncating with no signal.
    events_table.items = [event_item("s1", n) for n in range(EVENTS_PAGE_LIMIT + 50)]

    rows = get_events(events_table, "s1", None)

    assert len(rows) == EVENTS_PAGE_LIMIT
    # The bound must land the client's next cursor exactly where the page
    # stopped -- the (n)th item, zero-indexed, is seq n.
    assert rows[-1]["seq"] == f"{EVENTS_PAGE_LIMIT - 1:020d}"


def test_get_events_query_passes_the_page_limit(events_table):
    events_table.items = [event_item("s1", 1)]
    get_events(events_table, "s1", None)
    assert events_table.queries[-1]["Limit"] == EVENTS_PAGE_LIMIT


def test_get_events_under_limit_returns_everything(events_table):
    events_table.items = [event_item("s1", n) for n in range(3)]
    rows = get_events(events_table, "s1", None)
    assert len(rows) == 3


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
