"""Raw log line -> (event_type, attrs).

Pure functions only: no AWS, no I/O. Adding an event type means appending a
tuple to MATCHERS. A line that matches nothing is not an error -- it becomes
log.line, because a bad line is data (see the M3 design spec).
"""

import re


def _relic_reward(groups: dict) -> dict:
    # item_path uses legacy internal names (FulminPrimeBarrel). The display
    # name needs a mapping that deliberately lives outside the hot path.
    return groups | {"item_name": groups["item_path"].rsplit("/", 1)[-1]}


# (pattern, event_type, enrich). enrich may be None when named groups suffice.
MATCHERS = [
    (
        re.compile(
            r"VoidProjections: (?P<player_id>\w+) gets reward (?P<item_path>/Lotus/\S+)"
        ),
        "reward.relic",
        _relic_reward,
    ),
]


def parse(raw: str) -> tuple[str, dict]:
    for pattern, event_type, enrich in MATCHERS:
        if m := pattern.search(raw):
            attrs = m.groupdict()
            return event_type, enrich(attrs) if enrich else attrs
    return "log.line", {}
