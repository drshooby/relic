data "aws_s3_bucket" "data_bucket" {
  bucket = data.aws_ssm_parameter.data_bucket_name.value
}
