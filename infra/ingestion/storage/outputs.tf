output "aws_region" {
  value       = var.aws_region
  description = "AWS region used by the storage stack."
}

output "bucket_name" {
  value       = aws_s3_bucket.raw_data.bucket
  description = "Raw data bucket name."
}

output "bucket_arn" {
  value       = aws_s3_bucket.raw_data.arn
  description = "Raw data bucket ARN."
}
