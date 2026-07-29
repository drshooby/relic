resource "aws_kinesis_stream" "relic_stream" {
  name        = "relic-events-stream"
  shard_count = 1

  shard_level_metrics = [
    "IncomingBytes",
    "OutgoingBytes",
  ]

  stream_mode_details {
    // alternative is ON_DEMAND but we want to specify shard count
    stream_mode = "PROVISIONED"
  }
}
