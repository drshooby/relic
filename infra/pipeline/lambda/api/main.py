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

        # queryStringParameters is None (not {}) when there is no query string.
        since = (event.get("queryStringParameters") or {}).get("since")
        events = get_events(ddb.Table(os.environ["EVENTS_TABLE"]), session_id, since)
        # Query events before checking session_exists: the dashboard polls
        # this route every 2s for the life of a session, so the common case
        # (events present) must cost one DynamoDB read, not two. The
        # existence check is only needed to disambiguate an EMPTY result --
        # 404 (session row itself aged out, ~7d) vs 204 (row alive, its
        # events aged out at ~24h) -- so it belongs behind that branch, not
        # in front of every request. Do not hoist it back above the query.
        if events:
            return ok({"events": events, "last_seq": events[-1]["seq"]})

        sessions = ddb.Table(os.environ["SESSIONS_TABLE"])
        if not session_exists(sessions, session_id):
            return not_found("no such session")

        return no_content()

    logger.warning("unrouted path: %s", path)
    return not_found("unknown route")
