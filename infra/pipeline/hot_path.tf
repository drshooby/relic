# Hot path: Kinesis -> Lambda -> DynamoDB.
#
# This reads the same stream as Firehose, independently. Both are standard
# (shared-throughput) consumers, so they split the shard's 2 MB/s read budget.
# Fine at single-user volume; the fix if it ever isn't is enhanced fan-out
# (aws_kinesis_stream_consumer), which gives each consumer its own 2 MB/s.

resource "aws_sqs_queue" "hot_path_dlq" {
  name = "relic-hot-path-dlq"

  # 14 days (the SQS maximum) so a failure during a play session is still
  # visible days later. For stream sources the message holds *metadata*
  # (shard id, sequence-number range, error) — not the record payload.
  # The payload is recoverable from the raw line in S3.
  message_retention_seconds = 1209600
}

resource "aws_lambda_event_source_mapping" "hot_path" {
  event_source_arn = aws_kinesis_stream.relic_stream.arn
  function_name    = aws_lambda_function.hot_path.arn

  # LATEST, not TRIM_HORIZON: the pipeline is destroyed and re-applied between
  # sessions, and the stream retains 24h. TRIM_HORIZON would replay the previous
  # session on every apply — harmless (writes are idempotent on (session_id, seq))
  # but it burns invocations and makes "is this live?" hard to read during dev.
  starting_position = "LATEST"

  batch_size = 100

  # Wait up to 5s to fill a batch. Cuts invocations on a sparse log without
  # adding latency the dashboard would notice (it polls every ~2s).
  maximum_batching_window_in_seconds = 5

  # Poison-record containment. Kinesis is ordered, so a batch that always throws
  # blocks its shard until the records age out. These three bound the damage:
  #   - bisect: split a failing batch and retry the halves, narrowing toward the
  #     single bad record instead of discarding all 100 with it
  #   - retry attempts: give up after 2 rounds and let the shard advance
  #   - on_failure: record what was given up on, instead of dropping it silently
  bisect_batch_on_function_error = true
  maximum_retry_attempts         = 2

  destination_config {
    on_failure {
      destination_arn = aws_sqs_queue.hot_path_dlq.arn
    }
  }
}
