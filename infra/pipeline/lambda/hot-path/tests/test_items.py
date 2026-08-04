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
