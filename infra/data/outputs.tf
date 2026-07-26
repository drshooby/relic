output "data_bucket_name" {
  description = "Name of the persistent data bucket"
  value       = aws_s3_bucket.data_bucket.bucket
}

output "data_bucket_arn" {
  description = "ARN of the persistent data bucket"
  value       = aws_s3_bucket.data_bucket.arn
}
