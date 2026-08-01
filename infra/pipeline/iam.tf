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

resource "aws_iam_role" "firehose_lambda_role" {
  name = "relic-lambda-role"

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

resource "aws_iam_role_policy_attachment" "firehose_lambda_basic" {
  role       = aws_iam_role.firehose_lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}
