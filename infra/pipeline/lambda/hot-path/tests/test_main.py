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


def _envelope(
    seq: int,
    raw: str = "1.0 Sys [Info]: something",
    session_id: str = "abc123",
    wall_time_utc: str | None = "2026-08-03T19:12:44Z",
) -> dict:
    envelope = {
        "v": 1,
        "source": "warframe.ee_log",
        "session_id": session_id,
        "seq": seq,
        "game_time_s": 1.0,
        "wall_time_utc": wall_time_utc,
        "session_epoch_utc": "2026-08-03T19:08:44Z",
        "raw": raw,
    }
    return envelope


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


def test_mixed_session_batch_updates_each_session_with_its_own_count(
    fake_ddb, monkeypatch
):
    # A Lambda event source mapping batches per shard, not per partition key.
    # This stream is a single shard, so records from two sessions can land in
    # the same invocation -- e.g. a new EE.log tail starting while the
    # previous session's records are still draining. Attributing the whole
    # batch to one session_id would silently stop the other session's
    # counter from advancing.
    monkeypatch.setattr(main, "_resource", lambda: fake_ddb)
    event = {
        "Records": [
            _record(_envelope(i, session_id="session-a")) for i in range(3)
        ]
        + [_record(_envelope(i, session_id="session-b")) for i in range(2)]
    }

    main.lambda_handler(event, None)

    updates = fake_ddb.Table(SESSIONS_TABLE).updates
    assert len(updates) == 2

    by_key = {u["Key"]["session_id"]: u for u in updates}
    assert set(by_key) == {"session-a", "session-b"}
    assert by_key["session-a"]["ExpressionAttributeValues"][":inc"] == 3
    assert by_key["session-b"]["ExpressionAttributeValues"][":inc"] == 2


def test_batch_with_no_wall_time_utc_does_not_write_null_timestamp(
    fake_ddb, monkeypatch
):
    # boto3's TypeSerializer does not raise on None -- it serializes it to
    # DynamoDB's NULL type, so a naive max(..., default=None) flowing
    # straight into the update would succeed and write an explicit NULL
    # last_seen_at/started_at. Reachable with real data: every line the
    # operator emits before it parses the log header has no clock, so a
    # batch captured at session start can be entirely clockless.
    monkeypatch.setattr(main, "_resource", lambda: fake_ddb)
    event = {
        "Records": [
            _record(_envelope(i, wall_time_utc=None)) for i in range(3)
        ]
    }

    main.lambda_handler(event, None)

    (update,) = fake_ddb.Table(SESSIONS_TABLE).updates
    assert "last_seen_at" not in update["UpdateExpression"]
    assert "started_at" not in update["UpdateExpression"]
    assert ":seen" not in update["ExpressionAttributeValues"]
    # The count still advances -- only the timestamp is withheld.
    assert update["ExpressionAttributeValues"][":inc"] == 3


def test_batch_with_partial_wall_time_utc_writes_max_of_present_values(
    fake_ddb, monkeypatch
):
    monkeypatch.setattr(main, "_resource", lambda: fake_ddb)
    event = {
        "Records": [
            _record(_envelope(0, wall_time_utc=None)),
            _record(_envelope(1, wall_time_utc="2026-08-03T19:12:44Z")),
            _record(_envelope(2, wall_time_utc="2026-08-03T19:15:00Z")),
            _record(_envelope(3, wall_time_utc=None)),
        ]
    }

    main.lambda_handler(event, None)

    (update,) = fake_ddb.Table(SESSIONS_TABLE).updates
    assert update["ExpressionAttributeValues"][":seen"] == "2026-08-03T19:15:00Z"
