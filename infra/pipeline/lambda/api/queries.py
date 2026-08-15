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
