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
import collections
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

    # Keyed by (session_id, seq), the project's documented idempotency key,
    # not a plain list. Kinesis delivery is at-least-once (CLAUDE.md: "at
    # least once everywhere"), and the Go operator's own retry path
    # re-sends a whole buffered batch when it can't confirm a prior flush
    # succeeded (operator/cmd/tail/envelope.go) -- so the SAME (session_id,
    # seq) can legitimately appear twice in one Lambda invocation.
    # BatchWriteItem rejects any request containing two operations on the
    # same key ("ValidationException: Provided list of item keys contains
    # duplicates"), which fails every item in the batch, not just the
    # duplicate. Deduping here, last-write-wins, is correct and intended:
    # (session_id, seq) identifies one source log line, so two envelopes
    # sharing a key are the same event arriving twice, not two events.
    items_by_key = {}
    skipped = 0
    for record in records:
        envelope = _decode(record)
        if envelope is None:
            skipped += 1
            continue
        item = build_event_item(envelope, now)
        items_by_key[(item["session_id"], item["seq"])] = item

    if not items_by_key:
        logger.warning("batch of %d records yielded nothing writable", len(records))
        return

    items = list(items_by_key.values())

    # batch_writer chunks at 25 and retries UnprocessedItems itself. Rolling
    # that by hand is the classic way to silently drop records: BatchWriteItem
    # returns 200 with UnprocessedItems on partial throttling, so treating the
    # 200 as success loses data. Any error it cannot resolve propagates, which
    # is what we want -- infrastructure failure must raise so the event source
    # mapping bisects, retries, and finally routes to the DLQ.
    with events_table.batch_writer() as batch:
        for item in items:
            batch.put_item(Item=item)

    # One session update per SESSION in the batch, not one per batch and not
    # one per record. len(session_items) here counts DISTINCT (session_id,
    # seq) keys, not raw records, because by_session is built from the
    # already-deduped `items` list above -- a retried record that collapsed
    # for the events table also does not inflate event_count a second time.
    # (event_count can still drift across separate retried batches -- that
    # is the documented, accepted approximation -- this only removes the
    # extra, undocumented source of drift from duplicates within one batch.)
    # A Lambda event source mapping batches per shard, and
    # this stream runs a single shard, so a batch can hold records from more
    # than one session_id -- e.g. a new EE.log tail starting while the
    # previous session's records are still draining. Attributing the whole
    # batch's count to whichever session happened to be last in the list
    # would silently stop updating the other session's counter: no error, no
    # log, just a session that quietly stops advancing.
    by_session = collections.defaultdict(list)
    for item in items:
        by_session[item["session_id"]].append(item)

    for session_id, session_items in by_session.items():
        # Records arrive out of order within a batch, so last_seen_at takes
        # the max rather than any single item's value. Lines emitted before
        # the operator parses the log header carry no clock at all, so a
        # session's slice of the batch can legitimately have none with a
        # wall_time_utc -- default=None, and build_session_update knows to
        # omit the timestamp fields rather than write an explicit NULL.
        last_seen = max(
            (i["wall_time_utc"] for i in session_items if "wall_time_utc" in i),
            default=None,
        )
        sessions_table.update_item(
            **build_session_update(session_id, len(session_items), last_seen, now)
        )

    logger.info(
        "wrote %d events, skipped %d, sessions=%s",
        len(items),
        skipped,
        ",".join(by_session),
    )
