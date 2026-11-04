output "aws_region" {
  value = var.aws_region
}

output "ecr_repository_url" {
  value = aws_ecr_repository.transformation.repository_url
}

output "silver_lambda_function_name" {
  value = aws_lambda_function.silver.function_name
}

output "quality_lambda_function_name" {
  value = aws_lambda_function.quality.function_name
}

output "silver_incremental_state_machine_arn" {
  value = aws_sfn_state_machine.silver_incremental_load.arn
}

output "silver_backfill_state_machine_arn" {
  value = aws_sfn_state_machine.silver_backfill.arn
}
