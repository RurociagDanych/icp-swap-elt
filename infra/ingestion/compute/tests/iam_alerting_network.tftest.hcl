# Offline checks (mock provider, no credentials) for the ingestion compute root: task IAM
# scoping, the alerts topic policy, scheduler alerting, network defaults and the task definition.

mock_provider "aws" {
  mock_data "aws_partition" {
    defaults = {
      partition = "aws"
    }
  }

  mock_data "aws_caller_identity" {
    defaults = {
      account_id = "123456789012"
    }
  }

  mock_data "aws_s3_bucket" {
    defaults = {
      arn    = "arn:aws:s3:::raw-bucket"
      bucket = "raw-bucket"
    }
  }

  mock_data "aws_iam_policy_document" {
    defaults = {
      json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}"
    }
  }

  mock_resource "aws_iam_role" {
    defaults = {
      arn = "arn:aws:iam::123456789012:role/mock"
    }
  }

  mock_resource "aws_iam_policy" {
    defaults = {
      arn = "arn:aws:iam::123456789012:policy/mock"
    }
  }

  mock_resource "aws_sns_topic" {
    defaults = {
      arn = "arn:aws:sns:us-east-1:123456789012:mock-alerts"
    }
  }

  mock_resource "aws_cloudwatch_event_rule" {
    defaults = {
      arn = "arn:aws:events:us-east-1:123456789012:rule/mock-task-failed"
    }
  }

  mock_resource "aws_ecs_cluster" {
    defaults = {
      arn = "arn:aws:ecs:us-east-1:123456789012:cluster/mock"
    }
  }

  mock_resource "aws_ecs_task_definition" {
    defaults = {
      arn = "arn:aws:ecs:us-east-1:123456789012:task-definition/mock:1"
    }
  }

  mock_resource "aws_ecr_repository" {
    defaults = {
      repository_url = "123456789012.dkr.ecr.us-east-1.amazonaws.com/mock"
    }
  }

  mock_resource "aws_cloudwatch_log_group" {
    defaults = {
      arn = "arn:aws:logs:us-east-1:123456789012:log-group:mock"
    }
  }

  mock_resource "aws_scheduler_schedule_group" {
    defaults = {
      arn = "arn:aws:scheduler:us-east-1:123456789012:schedule-group/mock"
    }
  }
}

variables {
  bucket_name = "raw-bucket"
  vpc_id      = "vpc-0123456789abcdef0"
  subnet_ids  = ["subnet-0123456789abcdef0"]
  image_tag   = "test-1"
}

# --- H1 / M4: S3 scoping ------------------------------------------------------------------------

run "delete_is_confined_to_the_lock_sub_path" {
  command = plan

  assert {
    condition = length([
      for s in data.aws_iam_policy_document.task_s3_access.statement : s
      if s.sid == "DeleteRunLockOnly" && toset(s.actions) == toset(["s3:DeleteObject"]) && toset(s.resources) == toset(["arn:aws:s3:::raw-bucket/icp-swap-elt/locks/*"])
    ]) == 1
    error_message = "s3:DeleteObject must be granted on <prefix>/locks/* only (apps/ingestion storageTarget.ts: <prefix>/locks/run.lock)"
  }

  assert {
    condition = length([
      for s in data.aws_iam_policy_document.task_s3_access.statement : s
      if s.sid != "DeleteRunLockOnly" && length([for a in tolist(s.actions) : a if startswith(a, "s3:DeleteObject")]) > 0
    ]) == 0
    error_message = "no other statement may grant delete"
  }
}

run "bucket_location_is_unconditioned_and_list_keeps_its_prefix_condition" {
  command = plan

  assert {
    condition = length([
      for s in data.aws_iam_policy_document.task_s3_access.statement : s
      if contains(tolist(s.actions), "s3:GetBucketLocation") && length(s.condition) == 0 && toset(s.resources) == toset(["arn:aws:s3:::raw-bucket"]) && length(tolist(s.actions)) == 1
    ]) == 1
    error_message = "s3:GetBucketLocation needs its own unconditioned statement on the bucket ARN (it carries no s3:prefix key)"
  }

  assert {
    condition = length([
      for s in data.aws_iam_policy_document.task_s3_access.statement : s
      if contains(tolist(s.actions), "s3:ListBucket") && !contains(tolist(s.actions), "s3:GetBucketLocation") && length([for c in s.condition : c if c.variable == "s3:prefix"]) == 1
    ]) == 1
    error_message = "s3:ListBucket stays confined by s3:prefix"
  }
}

# --- B2 (ingestion side) / M3: alerts topic ------------------------------------------------------

run "alerts_topic_events_publish_is_pinned_to_the_task_failed_rule_and_alarms_may_publish" {
  command = apply

  assert {
    condition = length([
      for s in data.aws_iam_policy_document.alerts_topic.statement : s
      if s.sid == "AllowEventBridgePublish" &&
      length([for c in s.condition : c if c.variable == "aws:SourceArn" && c.test == "ArnEquals" && toset(c.values) == toset([aws_cloudwatch_event_rule.task_failed.arn])]) == 1
    ]) == 1
    error_message = "the EventBridge publish grant must be an ArnEquals on aws:SourceArn of the task-failed rule"
  }

  assert {
    condition = length([
      for s in data.aws_iam_policy_document.alerts_topic.statement : s
      if s.sid == "AllowCloudWatchAlarmsPublish" &&
      contains(flatten([for p in s.principals : tolist(p.identifiers)]), "cloudwatch.amazonaws.com") &&
      length([for c in s.condition : c if c.variable == "aws:SourceAccount"]) == 1 &&
      length([for c in s.condition : c if c.variable == "aws:SourceArn" && c.test == "ArnLike"]) == 1
    ]) == 1
    error_message = "the new scheduler alarms need a CloudWatch publish statement with source conditions"
  }
}

# --- M5: scheduler alerting -------------------------------------------------------------------------

run "scheduler_failures_and_missed_runs_raise_alarms" {
  command = apply

  variables {
    schedule_expression = "cron(0 8 * * ? *)"
  }

  assert {
    condition = (
      aws_cloudwatch_metric_alarm.scheduler_target_errors[0].namespace == "AWS/Scheduler" &&
      aws_cloudwatch_metric_alarm.scheduler_target_errors[0].metric_name == "TargetErrorCount" &&
      aws_cloudwatch_metric_alarm.scheduler_target_errors[0].dimensions["ScheduleGroup"] == aws_scheduler_schedule_group.ingestion[0].name &&
      aws_cloudwatch_metric_alarm.scheduler_target_errors[0].comparison_operator == "GreaterThanOrEqualToThreshold"
    )
    error_message = "a TargetErrorCount alarm on the dedicated schedule group must exist"
  }

  assert {
    condition = (
      aws_cloudwatch_metric_alarm.scheduler_missed_run[0].namespace == "AWS/Scheduler" &&
      aws_cloudwatch_metric_alarm.scheduler_missed_run[0].metric_name == "InvocationAttemptCount" &&
      aws_cloudwatch_metric_alarm.scheduler_missed_run[0].comparison_operator == "LessThanThreshold" &&
      aws_cloudwatch_metric_alarm.scheduler_missed_run[0].treat_missing_data == "breaching" &&
      aws_cloudwatch_metric_alarm.scheduler_missed_run[0].evaluation_periods == 26 &&
      aws_cloudwatch_metric_alarm.scheduler_missed_run[0].datapoints_to_alarm == 26
    )
    error_message = "the missed-run alarm must breach when no InvocationAttemptCount datapoint arrives for the whole window"
  }

  assert {
    condition     = aws_scheduler_schedule.ingestion[0].group_name == aws_scheduler_schedule_group.ingestion[0].name
    error_message = "the schedule must live in the dedicated group so the group-level metrics are this schedule's"
  }
}

run "no_schedule_no_scheduler_alarms" {
  command = apply

  assert {
    condition     = length(aws_cloudwatch_metric_alarm.scheduler_target_errors) == 0 && length(aws_cloudwatch_metric_alarm.scheduler_missed_run) == 0
    error_message = "without a schedule there is nothing to alarm on"
  }
}

# --- M1: network defaults ---------------------------------------------------------------------------

run "defaults_are_private_and_need_explicit_networking" {
  command = plan

  assert {
    condition     = var.assign_public_ip == false && var.use_default_vpc == false
    error_message = "assign_public_ip and use_default_vpc must default to false"
  }
}

run "missing_vpc_is_rejected_by_default" {
  command = plan

  variables {
    vpc_id     = null
    subnet_ids = []
  }

  expect_failures = [var.vpc_id, var.subnet_ids]
}

run "default_vpc_without_public_ip_is_rejected" {
  command = plan

  variables {
    use_default_vpc  = true
    assign_public_ip = false
  }

  expect_failures = [var.assign_public_ip]
}

run "malformed_subnet_ids_are_rejected" {
  command = plan

  variables {
    subnet_ids = ["not-a-subnet"]
  }

  expect_failures = [var.subnet_ids]
}

# --- M2: SIGTERM grace and the renamed scheduler variable -----------------------------------------------

run "container_gets_a_120s_stop_timeout_and_scheduler_age_is_bounded" {
  command = apply

  assert {
    condition     = jsondecode(aws_ecs_task_definition.ingestion.container_definitions)[0].stopTimeout == 120
    error_message = "stopTimeout must be 120 s (the Fargate maximum) so SIGTERM handling can release the lock"
  }

  assert {
    condition     = var.scheduler_maximum_event_age_seconds == 3600
    error_message = "scheduler_maximum_event_age_seconds replaces task_timeout_seconds"
  }
}

run "scheduler_event_age_below_60s_is_rejected" {
  command = plan

  variables {
    scheduler_maximum_event_age_seconds = 10
  }

  expect_failures = [var.scheduler_maximum_event_age_seconds]
}

# --- H6: ECR and image tag -----------------------------------------------------------------------------

run "ecr_is_immutable_and_force_deletable_and_task_uses_the_unique_tag" {
  command = apply

  assert {
    condition     = aws_ecr_repository.ingestion.image_tag_mutability == "IMMUTABLE" && aws_ecr_repository.ingestion.force_delete == true
    error_message = "ingestion ECR repository: IMMUTABLE tags and force_delete"
  }

  assert {
    condition     = jsondecode(aws_ecs_task_definition.ingestion.container_definitions)[0].image == "123456789012.dkr.ecr.us-east-1.amazonaws.com/mock:test-1"
    error_message = "the task definition must reference the explicit image_tag"
  }
}

run "latest_is_rejected_as_image_tag" {
  command = plan

  variables {
    image_tag = "latest"
  }

  expect_failures = [var.image_tag]
}

# --- M6 ---------------------------------------------------------------------------------------------------

run "environment_tag_is_a_variable" {
  command = plan

  variables {
    environment = "prod"
  }

  assert {
    condition     = local.common_tags["Environment"] == "prod" && local.common_tags["Stage"] == "ingestion"
    error_message = "Environment follows var.environment; Stage stays ingestion"
  }
}
