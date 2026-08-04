resource "aws_iam_policy" "firehose_s3_delivery" {
  name        = "firehose-s3-delivery-policy"
  description = "Allows Amazon Data Firehose to write data to S3"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:AbortMultipartUpload",
          "s3:GetBucketLocation",
          "s3:ListBucket",
          "s3:ListBucketMultipartUploads",
          "s3:PutObject"
        ]
        Resource = [
          data.aws_s3_bucket.data_bucket.arn,
          "${data.aws_s3_bucket.data_bucket.arn}/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role" "firehose_role" {
  name = "relic-firehose-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "firehose.amazonaws.com"
        }
        Condition = {
          StringEquals = { "aws:SourceAccount" = data.aws_caller_identity.current.account_id }
        }
      }
    ]
  })
}

resource "aws_iam_policy" "kinesis_read" {
  name        = "relic-kinesis-read-policy"
  description = "Allows Amazon Data Firehose to read from Kinesis"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "kinesis:DescribeStream",
          "kinesis:GetShardIterator",
          "kinesis:GetRecords",
          "kinesis:ListShards"
        ]
        Resource = [
          aws_kinesis_stream.relic_stream.arn
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "kinesis_read_attach" {
  role       = aws_iam_role.firehose_role.name
  policy_arn = aws_iam_policy.kinesis_read.arn
}

resource "aws_iam_role_policy_attachment" "firehose_s3_delivery_attach" {
  role       = aws_iam_role.firehose_role.name
  policy_arn = aws_iam_policy.firehose_s3_delivery.arn
}

resource "aws_iam_policy" "firehose_logging" {
  name        = "relic-firehose-logging-policy"
  description = "Allows Amazon Data Firehose to write delivery errors to CloudWatch Logs"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["logs:PutLogEvents"]
        Resource = ["${aws_cloudwatch_log_group.firehose.arn}:*"]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "firehose_logging_attach" {
  role       = aws_iam_role.firehose_role.name
  policy_arn = aws_iam_policy.firehose_logging.arn
}

resource "aws_iam_role" "hot_path_lambda_role" {
  name = "relic-hot-path-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
        Condition = {
          StringEquals = { "aws:SourceAccount" = data.aws_caller_identity.current.account_id }
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "hot_path_lambda_basic" {
  role       = aws_iam_role.hot_path_lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# The event source mapping polls Kinesis using the *function's* role, so these
# permissions live here rather than on a separate poller identity.
resource "aws_iam_policy" "hot_path_kinesis_read" {
  name        = "relic-hot-path-kinesis-read-policy"
  description = "Allows the hot-path Lambda's event source mapping to consume the stream"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "kinesis:DescribeStream",
          "kinesis:DescribeStreamSummary",
          "kinesis:GetShardIterator",
          "kinesis:GetRecords",
          "kinesis:ListShards"
        ]
        Resource = [aws_kinesis_stream.relic_stream.arn]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "hot_path_kinesis_read_attach" {
  role       = aws_iam_role.hot_path_lambda_role.name
  policy_arn = aws_iam_policy.hot_path_kinesis_read.arn
}

# Writing a failure record to the DLQ is also done by the mapping under this role.
resource "aws_iam_policy" "hot_path_dlq_write" {
  name        = "relic-hot-path-dlq-write-policy"
  description = "Allows the hot-path event source mapping to report exhausted batches to SQS"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["sqs:SendMessage"]
        Resource = [aws_sqs_queue.hot_path_dlq.arn]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "hot_path_dlq_write_attach" {
  role       = aws_iam_role.hot_path_lambda_role.name
  policy_arn = aws_iam_policy.hot_path_dlq_write.arn
}
