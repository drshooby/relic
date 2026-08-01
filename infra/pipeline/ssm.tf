data "aws_ssm_parameter" "data_bucket_name" {
  name = "/relic/data/bucket_name"
}

resource "aws_ssm_parameter" "kinesis_stream_name" {
  name        = "/relic/pipeline/kinesis_stream_name"
  description = "Name of the kinesis stream"
  type        = "String"
  value       = aws_kinesis_stream.relic_stream.name
}
