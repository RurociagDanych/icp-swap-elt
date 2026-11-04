output "aws_region" {
  value       = var.aws_region
  description = "AWS region used by the compute stack."
}

output "bucket_name" {
  value       = data.aws_s3_bucket.raw_data.bucket
  description = "Existing raw data bucket consumed by the compute stack."
}

output "ecr_repository_url" {
  value       = aws_ecr_repository.ingestion.repository_url
  description = "Push the ingestion container image here for Fargate runs."
}

output "ecs_cluster_name" {
  value       = aws_ecs_cluster.ingestion.name
  description = "ECS cluster that hosts the Fargate task."
}

output "task_definition_arn" {
  value       = aws_ecs_task_definition.ingestion.arn
  description = "Task definition ARN for ad hoc ECS runs."
}

output "task_security_group_id" {
  value       = aws_security_group.ingestion_task.id
  description = "Security group attached to the Fargate task ENI."
}

output "effective_vpc_id" {
  value       = local.effective_vpc_id
  description = "VPC ID actually used by the task networking."
}

output "effective_subnet_ids" {
  value       = local.effective_subnet_ids
  description = "Subnet IDs actually used by the task networking."
}

output "assign_public_ip" {
  value       = var.assign_public_ip
  description = "Whether awsvpc task ENIs are assigned a public IP."
}

output "cloudwatch_log_group_name" {
  value       = aws_cloudwatch_log_group.ingestion.name
  description = "CloudWatch log group for container logs."
}

output "alerts_topic_arn" {
  value       = aws_sns_topic.alerts.arn
  description = "SNS topic that receives failed-task alerts."
}

output "scheduler_name" {
  value       = try(aws_scheduler_schedule.ingestion[0].name, null)
  description = "EventBridge Scheduler name when schedule_expression is enabled."
}
