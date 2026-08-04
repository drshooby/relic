"""Envelope + parse result -> DynamoDB item dicts. Pure functions only."""

from parser import parse

# uint64 max is 18446744073709551615 -- exactly 20 digits -- so a seq from the
# operator can never exceed this width and silently break sort order.
SEQ_WIDTH = 20

EVENTS_TTL_SECONDS = 24 * 60 * 60        # 24h -- rolling cache, S3 is the archive
SESSIONS_TTL_SECONDS = 7 * 24 * 60 * 60  # 7d  -- outlives its own events


def pad_seq(seq: int) -> str:
    """Zero-pad seq so string sort order matches integer order.

    DynamoDB string sort keys compare lexicographically, so "10" sorts before
    "2" unpadded. Nothing enforces this but this function -- an unpadded write
    succeeds and lands in the wrong place with no error.
    """
    return f"{seq:0{SEQ_WIDTH}d}"


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
    session_id: str, count: int, last_seen: str | None, now: int
) -> dict:
    """UpdateItem kwargs for relic-sessions: one call per session per batch.

    ADD is atomic, so concurrent batches do not clobber each other. A retried
    batch double-counts, making event_count APPROXIMATE -- acceptable for a
    display counter, documented so nobody mistakes it for exact.

    `last_seen` is None when none of this session's records in the batch
    carry a wall_time_utc (every line the operator emits before it parses the
    log header has no clock). boto3's TypeSerializer does not raise on
    None -- it happily serializes it to DynamoDB's NULL type, so a naive
    ":seen": None would succeed and leave last_seen_at/started_at explicitly
    NULL. Omitting the timestamp SET clauses entirely when there is nothing
    to record keeps a later batch free to set started_at correctly via
    if_not_exists.
    """
    if last_seen is None:
        return {
            "Key": {"session_id": session_id},
            "UpdateExpression": "ADD event_count :inc SET expires_at = :ttl",
            "ExpressionAttributeValues": {
                ":inc": count,
                ":ttl": now + SESSIONS_TTL_SECONDS,
            },
        }

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
