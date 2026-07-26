# The one thing the two stacks share.
#
# infra/pipeline is destroyed and rebuilt constantly, so it must not hold a
# terraform_remote_state reference to this stack. It reads the bucket name from
# here instead, which keeps the coupling to a single string and lets the
# pipeline be rebuilt from scratch without touching stored data.
#
# Pipeline side reads it with:
#   data "aws_ssm_parameter" "data_bucket" { name = "/relic/data/bucket_name" }
resource "aws_ssm_parameter" "data_bucket_name" {
  name        = "/relic/data/bucket_name"
  description = "Name of the persistent relic data bucket (cold path archive)"
  type        = "String"
  value       = aws_s3_bucket.data_bucket.bucket
}

resource "aws_ssm_parameter" "data_bucket_arn" {
  name        = "/relic/data/bucket_arn"
  description = "ARN of the persistent relic data bucket, for pipeline IAM policies"
  type        = "String"
  value       = aws_s3_bucket.data_bucket.arn
}
