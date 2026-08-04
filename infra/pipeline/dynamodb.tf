# DynamoDB: the hot path's serving layer.
#
# These tables are a ROLLING CACHE, not an archive. S3 (cold path) is the
# durable source of truth; anything here can be re-derived by replaying raw
# lines from S3. That is why TTL is safe: losing an item costs nothing.
#
# Billing is PAY_PER_REQUEST (on-demand) — no provisioned capacity to size and
# nothing billed while idle, which matches the destroy-between-sessions habit.
# The alternative (PROVISIONED) bills per hour whether or not traffic exists.

resource "aws_dynamodb_table" "events" {
  name         = "relic-events"
  billing_mode = "PAY_PER_REQUEST"

  hash_key  = "session_id" # partition key: exact-match only
  range_key = "seq"        # sort key: ordered within one session

  # Only KEY attributes are declared. DynamoDB is schemaless for everything
  # else — event_type, raw, attrs, etc. are written without being declared
  # here. Attributes appear in this block only if they are part of a key or
  # an index; declaring an unused one is an error at apply time.
  attribute {
    name = "session_id"
    type = "S"
  }

  attribute {
    name = "seq"
    type = "S" # String, NOT Number — see the padding note below
  }

  # seq is a zero-padded decimal string of width 20.
  #
  # String sort keys compare lexicographically (character by character), so
  # unpadded values order wrongly: "10" sorts before "2" because '1' < '2'.
  # Fixed-width padding makes character order match numeric order.
  #
  # Width 20 is deliberate: uint64 max is 18446744073709551615 — exactly 20
  # digits — so the operator's seq can never exceed the width and silently
  # break ordering. Producers and consumers must both pad; an unpadded write
  # is not an error, it just lands in the wrong place in the sort order.
  #
  #   seq=1  -> "00000000000000000001"
  #   seq=42 -> "00000000000000000042"

  # (session_id, seq) is also the idempotency key. Delivery is at-least-once
  # everywhere, so the same record can arrive twice; PutItem on an identical
  # key overwrites rather than duplicating, which makes retries harmless.

  ttl {
    attribute_name = "expires_at"
    enabled        = true
  }

  # TTL behaviour worth knowing, because it surprises people:
  #
  #   - expires_at must be a Number holding a UNIX timestamp in SECONDS.
  #     Milliseconds (the usual reflex) yields a date ~50,000 years out and
  #     the item is simply never deleted.
  #   - Deletion is NOT prompt. AWS sweeps within ~48h of expiry, on its own
  #     schedule, for free. Until the sweep runs, EXPIRED ITEMS STILL APPEAR
  #     IN QUERIES. Consumers must filter on time, not assume presence means
  #     freshness.
  #
  # So TTL is a storage-cost control, not a correctness guarantee.

  tags = {
    Project = "relic"
    Path    = "hot"
  }
}

# One item per play session, so the dashboard can answer "what sessions exist?"
# cheaply. Without this it would have to Scan relic-events — reading every item
# in the table — since a partition key can only be matched exactly, never listed.
# This is the DynamoDB pattern: an access pattern the key design does not
# support gets its own table (or index), rather than a more complex query.
resource "aws_dynamodb_table" "sessions" {
  name         = "relic-sessions"
  billing_mode = "PAY_PER_REQUEST"

  # No range_key: session_id alone identifies an item, so this is a pure
  # key-value lookup. A table without a sort key holds exactly one item per
  # partition key.
  hash_key = "session_id"

  attribute {
    name = "session_id"
    type = "S"
  }

  ttl {
    attribute_name = "expires_at"
    enabled        = true
  }
  # Longer retention than relic-events (spec: ~7d vs ~24h) — the session list
  # stays browsable after the individual events have aged out.

  tags = {
    Project = "relic"
    Path    = "hot"
  }
}
