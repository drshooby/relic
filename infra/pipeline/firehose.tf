resource "aws_cloudwatch_log_group" "firehose" {
  name              = "/aws/kinesisfirehose/relic-kinesis-firehose-stream"
  retention_in_days = 7
}

resource "aws_cloudwatch_log_stream" "firehose_s3_delivery" {
  name           = "S3Delivery"
  log_group_name = aws_cloudwatch_log_group.firehose.name
}

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

    # Hive-style partitioning so Athena/Glue can discover partitions
    # automatically in phase 2. Firehose's default (2026/07/31/00/) is
    # positional and needs manual ALTER TABLE ADD PARTITION per prefix.
    # Timestamps are UTC and refer to Firehose ingestion time, not the
    # in-game event time parsed from the log line.
    prefix              = "raw/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/"
    error_output_prefix = "errors/!{firehose:error-output-type}/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/"

    processing_configuration {
      enabled = "true"

      processors {
        type = "Lambda"

        parameters {
          parameter_name  = "LambdaArn"
          parameter_value = aws_lambda_function.lambda_processor.arn
        }
      }
    }

    # Without this, delivery failures (e.g. AccessDenied) are silent:
    # Firehose retries for up to 24h and surfaces nothing.
    cloudwatch_logging_options {
      enabled         = true
      log_group_name  = aws_cloudwatch_log_group.firehose.name
      log_stream_name = aws_cloudwatch_log_stream.firehose_s3_delivery.name
    }
  }
}
