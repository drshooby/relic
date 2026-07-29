data "aws_ssm_parameter" "data_bucket_name" {
  name = "/relic/data/bucket_name"
}
