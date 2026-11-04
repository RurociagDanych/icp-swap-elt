output "aws_region" {
  value = var.aws_region
}

output "processed_bucket_name" {
  value = aws_s3_bucket.processed.bucket
}

output "athena_results_bucket_name" {
  value = aws_s3_bucket.athena_results.bucket
}

output "access_logs_bucket_name" {
  value = aws_s3_bucket.access_logs.bucket
}

output "glue_database_name" {
  value = aws_glue_catalog_database.transformation.name
}

output "athena_workgroup_name" {
  value = aws_athena_workgroup.transformation.name
}
