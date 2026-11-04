# Offline plan-level checks (mock provider, no credentials) for IAM scoping and the alerts
# topic policy. Every run uses `command = plan`; no AWS API is called.

mock_provider "aws" {
  mock_data "aws_partition" {
    defaults = {
      partition = "aws"
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

  mock_resource "aws_sqs_queue" {
    defaults = {
      arn = "arn:aws:sqs:us-east-1:123456789012:mock-dlq"
    }
  }

  mock_resource "aws_sns_topic" {
    defaults = {
      arn = "arn:aws:sns:us-east-1:123456789012:mock-alerts"
    }
  }

  mock_resource "aws_lambda_function" {
    defaults = {
      arn = "arn:aws:lambda:us-east-1:123456789012:function:mock"
    }
  }

  mock_resource "aws_cloudwatch_event_rule" {
    defaults = {
      arn = "arn:aws:events:us-east-1:123456789012:rule/mock-ingestion-finished"
    }
  }

  mock_resource "aws_sfn_state_machine" {
    defaults = {
      arn = "arn:aws:states:us-east-1:123456789012:stateMachine:mock"
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
}

# Distinct role ARNs so "which function runs under which role" is assertable.
override_resource {
  target = aws_iam_role.quality
  values = {
    arn = "arn:aws:iam::123456789012:role/mock-quality"
  }
}

override_resource {
  target = aws_iam_role.lambda_execution
  values = {
    arn = "arn:aws:iam::123456789012:role/mock-lambda"
  }
}

variables {
  aws_account_id  = "123456789012"
  raw_bucket_name = "raw-bucket"
  # Pinned: terraform test also reads the local, gitignored terraform.tfvars, which sets raw_prefix to the
  # migrated prefix; this test asserts the module default, so it must not depend on that file.
  raw_prefix               = "icp-swap-elt"
  processed_bucket_name    = "processed-bucket"
  glue_database_name       = "icp_swap_elt"
  silver_target_generation = "g20261101"
  image_tag                = "test-1"
}

# --- B2: the alerts topic must accept CloudWatch alarm notifications ---------------------

run "alerts_topic_policy_lets_cloudwatch_alarms_publish_with_source_conditions" {
  command = plan

  assert {
    condition = length([
      for s in data.aws_iam_policy_document.alerts_topic.statement : s
      if s.sid == "AllowCloudWatchAlarmsPublish" &&
      contains(flatten([for p in s.principals : tolist(p.identifiers)]), "cloudwatch.amazonaws.com") &&
      contains(tolist(s.actions), "SNS:Publish") &&
      s.effect == "Allow"
    ]) == 1
    error_message = "the alerts topic policy must allow cloudwatch.amazonaws.com to SNS:Publish (alarm_actions otherwise fail silently)"
  }

  assert {
    condition = length([
      for s in data.aws_iam_policy_document.alerts_topic.statement : s
      if s.sid == "AllowCloudWatchAlarmsPublish" &&
      length([for c in s.condition : c if c.variable == "aws:SourceAccount" && c.test == "StringEquals" && contains(tolist(c.values), "123456789012")]) == 1 &&
      length([for c in s.condition : c if c.variable == "aws:SourceArn" && c.test == "ArnLike" && length(tolist(c.values)) == 1 && startswith(tolist(c.values)[0], "arn:aws:cloudwatch:us-east-1:123456789012:alarm:icp-swap-elt-transformation-")]) == 1
    ]) == 1
    error_message = "the CloudWatch statement must be confined by aws:SourceAccount and an aws:SourceArn alarm pattern of this stack"
  }

  assert {
    condition = length([
      for s in data.aws_iam_policy_document.alerts_topic.statement : s
      if s.sid == "AllowBudgetsPublish" && contains(flatten([for p in s.principals : tolist(p.identifiers)]), "budgets.amazonaws.com")
    ]) == 1
    error_message = "the AWS Budgets statement must be kept"
  }
}

# --- H2: quality is read-only and has its own role ---------------------------------------

run "quality_runs_under_its_own_read_only_role" {
  command = apply

  assert {
    condition     = aws_lambda_function.quality.role == aws_iam_role.quality.arn && aws_lambda_function.silver.role == aws_iam_role.lambda_execution.arn && aws_lambda_function.silver_backfill.role == aws_iam_role.lambda_execution.arn
    error_message = "quality needs a role of its own; silver and silver_backfill keep the writer role"
  }

  assert {
    condition     = aws_iam_role.quality.name != aws_iam_role.lambda_execution.name && aws_iam_role_policy_attachment.quality_permissions.role == aws_iam_role.quality.name
    error_message = "the quality policy must be attached to the quality role"
  }

  assert {
    condition = length([
      for a in flatten([for s in data.aws_iam_policy_document.quality_permissions.statement : tolist(s.actions)]) : a
      if can(regex("^(s3:(Put|Delete|Abort)|glue:(Create|Update|Delete|BatchDelete|BatchCreate)|sqs:)", a))
    ]) == 0
    error_message = "the quality role must not hold any S3 write/delete, Glue write or SQS action (quality_job.py only reads through PyIceberg and writes to /tmp)"
  }

  assert {
    condition     = contains(flatten([for s in data.aws_iam_policy_document.quality_permissions.statement : tolist(s.actions)]), "s3:GetObject") && contains(flatten([for s in data.aws_iam_policy_document.quality_permissions.statement : tolist(s.actions)]), "glue:GetTable")
    error_message = "the quality role must still be able to read the Iceberg tables"
  }

  assert {
    condition     = length([for s in data.aws_iam_policy_document.quality_permissions.statement : s if length(regexall("raw-bucket", join(",", tolist(s.resources)))) > 0]) == 0
    error_message = "the quality role has no business reading the raw bucket"
  }
}

# --- H2/H3: writer role scoping -----------------------------------------------------------

run "writer_role_writes_only_below_the_warehouse_prefix_and_lists_raw_by_prefix" {
  command = plan

  assert {
    condition = length([
      for s in data.aws_iam_policy_document.lambda_permissions.statement : s
      if s.sid == "ReadWriteIcebergWarehouse" &&
      contains(tolist(s.actions), "s3:PutObject") &&
      alltrue([for r in tolist(s.resources) : endswith(r, "/warehouse/*")])
    ]) == 1
    error_message = "object-level writes of silver/silver_backfill must be confined to the warehouse/ prefix of the processed bucket"
  }

  assert {
    condition = length([
      for s in data.aws_iam_policy_document.lambda_permissions.statement : s
      if contains(tolist(s.actions), "s3:ListBucket") && length(regexall("raw-bucket", join(",", tolist(s.resources)))) > 0 &&
      length([for c in s.condition : c if c.variable == "s3:prefix" && c.test == "StringLike" && contains(tolist(c.values), "icp-swap-elt/*")]) == 1
    ]) == 1
    error_message = "s3:ListBucket on the raw bucket must carry a StringLike s3:prefix condition limited to the raw prefix"
  }

  assert {
    condition = length([
      for s in data.aws_iam_policy_document.lambda_permissions.statement : s
      if contains(tolist(s.actions), "s3:ListBucket") && length(regexall("raw-bucket", join(",", tolist(s.resources)))) > 0 && length(s.condition) == 0
    ]) == 0
    error_message = "no unconditioned ListBucket on the raw bucket"
  }

  assert {
    condition = length([
      for s in data.aws_iam_policy_document.lambda_permissions.statement : s
      if length(regexall("raw-bucket", join(",", tolist(s.resources)))) > 0 &&
      length([for a in tolist(s.actions) : a if can(regex("^s3:(Put|Delete|Abort)", a))]) > 0
    ]) == 0
    error_message = "the transformation roles never write to the raw bucket"
  }
}
