import json
from pathlib import Path

import pytest
from parser import parse


def test_relic_reward_extracts_player_item_and_name():
    raw = ("240.623 Sys [Info]: VoidProjections: aaaa1111bbbb2222cccc3333 "
           "gets reward /Lotus/StoreItems/Types/Recipes/Weapons/WeaponParts/FulminPrimeBarrel")
    event_type, attrs = parse(raw)
    assert event_type == "reward.relic"
    assert attrs["player_id"] == "aaaa1111bbbb2222cccc3333"
    assert attrs["item_path"] == (
        "/Lotus/StoreItems/Types/Recipes/Weapons/WeaponParts/FulminPrimeBarrel")
    assert attrs["item_name"] == "FulminPrimeBarrel"


@pytest.mark.parametrize("raw", [
    # Squadmate arrival: same subsystem, no item path. MUST NOT match.
    "240.736 Sys [Info]: VoidProjections: aaaa1111bbbb2222cccc3333 gets reward",
    "240.736 Sys [Info]: VoidProjections: Client got reward info from aaaa1111",
    "240.623 Sys [Info]: VoidProjections: Still waiting on response from aaaa1111",
    "240.908 Sys [Info]: VoidProjections: Client has reward info for all players now",
    "240.444 Sys [Info]: VoidProjections: OpenVoidProjectionRewardScreenRMI",
    # Projection resource load -- looks reward-shaped, is not a reward.
    "63.761 Sys [Info]: ResourceLoader (/Lotus/Types/Game/Projections/T2VoidProjectionGyrePrimeFBronze) starting",
    # Continuation line (no timestamp, no subsystem).
    "  at Script.lua:42",
    "",
])
def test_non_reward_lines_fall_through_to_log_line(raw):
    event_type, attrs = parse(raw)
    assert event_type == "log.line"
    assert attrs == {}


FIXTURE = Path(__file__).parent / "testdata" / "reward_sequence.jsonl"


def test_full_reveal_sequence_yields_exactly_one_reward_event():
    envelopes = [json.loads(line) for line in FIXTURE.read_text().splitlines() if line]
    types = [parse(e["raw"])[0] for e in envelopes]

    # Only the player's own roll carries an item path. Every surrounding
    # coordination line must fall through -- squadmate arrivals included.
    assert types.count("reward.relic") == 1
    assert types.count("log.line") == len(envelopes) - 1

    reward_idx = types.index("reward.relic")
    _, attrs = parse(envelopes[reward_idx]["raw"])
    assert attrs["item_name"] == "FulminPrimeBarrel"
    # The reveal onset precedes the reward, so timing is preserved per line.
    assert envelopes[0]["game_time_s"] < envelopes[reward_idx]["game_time_s"]
