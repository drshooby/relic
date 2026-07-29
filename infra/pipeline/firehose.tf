resource "aws_kinesis_firehose_delivery_stream" "extended_s3_stream" {
  name        = "relic-kinesis-firehose-stream"
  destination = "extended_s3"

  kinesis_source_configuration {
    kinesis_stream_arn = aws_kinesis_stream.relic_stream.arn
    role_arn           = aws_iam_role.firehose_role.arn
  }

  extended_s3_configuration {
    role_arn           = aws_iam_role.firehose_role.arn
    bucket_arn         = data.aws_s3_bucket.data_bucket.arn
    buffering_interval = 60
  }
}
