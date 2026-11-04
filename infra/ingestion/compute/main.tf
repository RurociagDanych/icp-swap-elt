data "aws_partition" "current" {}
data "aws_caller_identity" "current" {}

data "aws_vpc" "default" {
  count   = var.use_default_vpc ? 1 : 0
  default = true
}

data "aws_subnets" "default_vpc" {
  count = var.use_default_vpc ? 1 : 0

  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default[0].id]
  }
}

data "aws_s3_bucket" "raw_data" {
  bucket = var.bucket_name
}

locals {
  # The managed repository is IMMUTABLE: the task references an explicit, unique tag (image_tag).
  image_uri            = var.container_image != "" ? var.container_image : "${aws_ecr_repository.ingestion.repository_url}:${var.image_tag}"
  effective_vpc_id     = var.use_default_vpc ? data.aws_vpc.default[0].id : var.vpc_id
  effective_subnet_ids = var.use_default_vpc ? data.aws_subnets.default_vpc[0].ids : var.subnet_ids
  common_tags = merge(
    {
      Project     = "icp-swap-elt"
      Stage       = "ingestion"
      ManagedBy   = "terraform"
      Environment = var.environment
      Layer       = "compute"
    },
    var.tags
  )
}

resource "aws_cloudwatch_log_group" "ingestion" {
  name              = "/aws/ecs/${var.name_prefix}"
  retention_in_days = var.log_retention_days
  tags              = local.common_tags
}

# Holds only build artifacts: force_delete lets `terraform destroy` remove it with images in it.
# Tags are immutable; push every build under a unique tag or reference it by digest (image_tag).
resource "aws_ecr_repository" "ingestion" {
  name                 = var.name_prefix
  image_tag_mutability = "IMMUTABLE"
  force_delete         = true

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = local.common_tags
}

resource "aws_ecs_cluster" "ingestion" {
  name = "${var.name_prefix}-cluster"
  tags = local.common_tags
}

resource "aws_security_group" "ingestion_task" {
  name        = "${var.name_prefix}-task"
  description = "Network policy for ICP ingestion Fargate tasks"
  vpc_id      = local.effective_vpc_id
  tags        = local.common_tags
}

resource "aws_vpc_security_group_egress_rule" "https" {
  security_group_id = aws_security_group.ingestion_task.id
  ip_protocol       = "tcp"
  from_port         = 443
  to_port           = 443
  cidr_ipv4         = "0.0.0.0/0"
  description       = "HTTPS egress for IC API, ECR, S3, and CloudWatch"
}

resource "aws_vpc_security_group_egress_rule" "dns_udp" {
  security_group_id = aws_security_group.ingestion_task.id
  ip_protocol       = "udp"
  from_port         = 53
  to_port           = 53
  cidr_ipv4         = "0.0.0.0/0"
  description       = "DNS UDP egress"
}

resource "aws_vpc_security_group_egress_rule" "dns_tcp" {
  security_group_id = aws_security_group.ingestion_task.id
  ip_protocol       = "tcp"
  from_port         = 53
  to_port           = 53
  cidr_ipv4         = "0.0.0.0/0"
  description       = "DNS TCP egress"
}

data "aws_iam_policy_document" "ecs_task_execution_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "ecs_task_execution" {
  name               = "${var.name_prefix}-execution"
  assume_role_policy = data.aws_iam_policy_document.ecs_task_execution_assume_role.json
  tags               = local.common_tags
}

resource "aws_iam_role_policy_attachment" "ecs_task_execution_managed" {
  role       = aws_iam_role.ecs_task_execution.name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role" "ecs_task" {
  name               = "${var.name_prefix}-task"
  assume_role_policy = data.aws_iam_policy_document.ecs_task_execution_assume_role.json
  tags               = local.common_tags
}

data "aws_iam_policy_document" "task_s3_access" {
  # GetBucketLocation has no s3:prefix context key; combined with the condition below it could
  # never be satisfied, so it gets its own unconditioned statement.
  statement {
    sid       = "ReadBucketLocation"
    effect    = "Allow"
    actions   = ["s3:GetBucketLocation"]
    resources = [data.aws_s3_bucket.raw_data.arn]
  }

  statement {
    sid       = "ListOnlyWithinPrefix"
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [data.aws_s3_bucket.raw_data.arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values = [
        "${var.s3_prefix}",
        "${var.s3_prefix}/*",
      ]
    }
  }

  statement {
    sid    = "ReadWriteObjectsWithinPrefix"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts",
    ]
    resources = ["${data.aws_s3_bucket.raw_data.arn}/${var.s3_prefix}/*"]
  }

  # Required to release the run lock on exit/SIGTERM; without it the lock leaks until its 6h TTL
  # and blocks the next run. The lock object is <prefix>/locks/run.lock (apps/ingestion
  # src/lib/storageTarget.ts, lockKey); that is the only object the application ever deletes.
  # Delete on the ingested data itself (canister/, rest/) is denied by infra/ingestion/storage.
  statement {
    sid       = "DeleteRunLockOnly"
    effect    = "Allow"
    actions   = ["s3:DeleteObject"]
    resources = ["${data.aws_s3_bucket.raw_data.arn}/${var.s3_prefix}/locks/*"]
  }
}

data "aws_iam_policy_document" "task_events" {
  count = var.event_bus_name != "" ? 1 : 0

  statement {
    sid       = "AnnounceFinishedRuns"
    effect    = "Allow"
    actions   = ["events:PutEvents"]
    resources = ["arn:${data.aws_partition.current.partition}:events:${var.aws_region}:${data.aws_caller_identity.current.account_id}:event-bus/${var.event_bus_name}"]
  }
}

resource "aws_iam_policy" "task_events" {
  count  = var.event_bus_name != "" ? 1 : 0
  name   = "${var.name_prefix}-task-events"
  policy = data.aws_iam_policy_document.task_events[0].json
  tags   = local.common_tags
}

resource "aws_iam_role_policy_attachment" "task_events" {
  count      = var.event_bus_name != "" ? 1 : 0
  role       = aws_iam_role.ecs_task.name
  policy_arn = aws_iam_policy.task_events[0].arn
}

resource "aws_iam_policy" "task_s3_access" {
  name   = "${var.name_prefix}-task-s3"
  policy = data.aws_iam_policy_document.task_s3_access.json
  tags   = local.common_tags
}

resource "aws_iam_role_policy_attachment" "task_s3_access" {
  role       = aws_iam_role.ecs_task.name
  policy_arn = aws_iam_policy.task_s3_access.arn
}

resource "aws_ecs_task_definition" "ingestion" {
  family                   = var.name_prefix
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = tostring(var.container_cpu)
  memory                   = tostring(var.container_memory)
  execution_role_arn       = aws_iam_role.ecs_task_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  container_definitions = jsonencode([
    {
      name      = "ingestion"
      image     = local.image_uri
      essential = true
      command   = var.container_command
      # SIGTERM grace (Fargate maximum): the app releases its run lock and flushes on SIGTERM.
      # Infra enforces no run-time limit of its own, see docs/RUNBOOK.md.
      stopTimeout = 120
      environment = concat(
        [
          {
            name  = "S3_BUCKET"
            value = data.aws_s3_bucket.raw_data.bucket
          },
          {
            name  = "S3_PREFIX"
            value = var.s3_prefix
          }
        ],
        var.event_bus_name != "" ? [{ name = "EVENT_BUS_NAME", value = var.event_bus_name }] : []
      )
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          awslogs-group         = aws_cloudwatch_log_group.ingestion.name
          awslogs-region        = var.aws_region
          awslogs-stream-prefix = "ingestion"
        }
      }
    }
  ])

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "X86_64"
  }

  tags = local.common_tags
}

resource "aws_sns_topic" "alerts" {
  name = "${var.name_prefix}-alerts"
  tags = local.common_tags
}

data "aws_iam_policy_document" "alerts_topic" {
  statement {
    sid    = "AllowEventBridgePublish"
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }

    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.alerts.arn]

    condition {
      test     = "ArnEquals"
      variable = "aws:SourceArn"
      values   = [aws_cloudwatch_event_rule.task_failed.arn]
    }
  }

  # The scheduler alarms below publish to this topic through CloudWatch.
  statement {
    sid    = "AllowCloudWatchAlarmsPublish"
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["cloudwatch.amazonaws.com"]
    }

    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.alerts.arn]

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }

    condition {
      test     = "ArnLike"
      variable = "aws:SourceArn"
      values   = ["arn:${data.aws_partition.current.partition}:cloudwatch:${var.aws_region}:${data.aws_caller_identity.current.account_id}:alarm:${var.name_prefix}-*"]
    }
  }
}

resource "aws_sns_topic_policy" "alerts" {
  arn    = aws_sns_topic.alerts.arn
  policy = data.aws_iam_policy_document.alerts_topic.json
}

resource "aws_sns_topic_subscription" "alert_email" {
  count     = var.alert_email != "" ? 1 : 0
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

resource "aws_cloudwatch_event_rule" "task_failed" {
  name        = "${var.name_prefix}-task-failed"
  description = "ICP ingestion Fargate task stopped abnormally (non-zero exit code or failed to start)"

  event_pattern = jsonencode({
    source      = ["aws.ecs"]
    detail-type = ["ECS Task State Change"]
    detail = {
      clusterArn = [aws_ecs_cluster.ingestion.arn]
      lastStatus = ["STOPPED"]
      "$or" = [
        { containers = { exitCode = [{ anything-but = 0 }] } },
        { stopCode = ["TaskFailedToStart"] },
      ]
    }
  })

  tags = local.common_tags
}

resource "aws_cloudwatch_event_target" "task_failed_sns" {
  rule = aws_cloudwatch_event_rule.task_failed.name
  arn  = aws_sns_topic.alerts.arn
}

data "aws_iam_policy_document" "scheduler_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "scheduler" {
  count              = var.schedule_expression != "" ? 1 : 0
  name               = "${var.name_prefix}-scheduler"
  assume_role_policy = data.aws_iam_policy_document.scheduler_assume_role.json
  tags               = local.common_tags
}

data "aws_iam_policy_document" "scheduler_run_task" {
  count = var.schedule_expression != "" ? 1 : 0

  statement {
    sid    = "RunEcsTask"
    effect = "Allow"
    actions = [
      "ecs:RunTask",
    ]
    resources = [aws_ecs_task_definition.ingestion.arn]
  }

  statement {
    sid    = "PassTaskRoles"
    effect = "Allow"
    actions = [
      "iam:PassRole",
    ]
    resources = [
      aws_iam_role.ecs_task_execution.arn,
      aws_iam_role.ecs_task.arn,
    ]
  }
}

resource "aws_iam_policy" "scheduler_run_task" {
  count  = var.schedule_expression != "" ? 1 : 0
  name   = "${var.name_prefix}-scheduler-run-task"
  policy = data.aws_iam_policy_document.scheduler_run_task[0].json
  tags   = local.common_tags
}

resource "aws_iam_role_policy_attachment" "scheduler_run_task" {
  count      = var.schedule_expression != "" ? 1 : 0
  role       = aws_iam_role.scheduler[0].name
  policy_arn = aws_iam_policy.scheduler_run_task[0].arn
}

# A dedicated group, so the AWS/Scheduler metrics (dimension ScheduleGroup only) below belong to
# this schedule alone and not to every schedule in the account's default group.
resource "aws_scheduler_schedule_group" "ingestion" {
  count = var.schedule_expression != "" ? 1 : 0
  name  = "${var.name_prefix}-schedules"
  tags  = local.common_tags
}

resource "aws_scheduler_schedule" "ingestion" {
  count       = var.schedule_expression != "" ? 1 : 0
  name        = "${var.name_prefix}-schedule"
  group_name  = aws_scheduler_schedule_group.ingestion[0].name
  description = "Scheduled ICP ingestion task"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression          = var.schedule_expression
  schedule_expression_timezone = "UTC"
  state                        = "ENABLED"

  target {
    arn      = aws_ecs_cluster.ingestion.arn
    role_arn = aws_iam_role.scheduler[0].arn

    ecs_parameters {
      task_definition_arn = aws_ecs_task_definition.ingestion.arn
      launch_type         = "FARGATE"
      platform_version    = "LATEST"
      task_count          = 1

      network_configuration {
        assign_public_ip = var.assign_public_ip
        subnets          = local.effective_subnet_ids
        security_groups  = [aws_security_group.ingestion_task.id]
      }
    }

    retry_policy {
      maximum_event_age_in_seconds = var.scheduler_maximum_event_age_seconds
      maximum_retry_attempts       = 2
    }
  }
}

# --- Scheduler alerting (M5) -----------------------------------------------------------------------
#
# The task-failed rule only sees tasks that started. These two alarms cover the scheduler itself,
# using the AWS/Scheduler metrics that exist (dimension ScheduleGroup):
#  - TargetErrorCount: the scheduler called ecs:RunTask and it returned an error (permissions,
#    throttling, invalid network configuration, ...).
#  - InvocationAttemptCount: emitted for every invocation attempt; no datapoint for the whole
#    window means the schedule did not fire at all (disabled, deleted, scheduler outage). Missing
#    data counts as breaching. The window must be longer than the schedule interval.
# CloudWatch metric delivery from the scheduler is best effort, so treat the missed-run alarm as a
# safety net rather than an exact SLA.

resource "aws_cloudwatch_metric_alarm" "scheduler_target_errors" {
  count               = var.schedule_expression != "" ? 1 : 0
  alarm_name          = "${var.name_prefix}-scheduler-target-errors"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "TargetErrorCount"
  namespace           = "AWS/Scheduler"
  period              = 300
  statistic           = "Sum"
  threshold           = 1
  treat_missing_data  = "notBreaching"
  alarm_description   = "EventBridge Scheduler could not start the ingestion task (ecs:RunTask returned an error)."
  dimensions = {
    ScheduleGroup = aws_scheduler_schedule_group.ingestion[0].name
  }
  alarm_actions = [aws_sns_topic.alerts.arn]
  tags          = local.common_tags
}

resource "aws_cloudwatch_metric_alarm" "scheduler_missed_run" {
  count               = var.schedule_expression != "" ? 1 : 0
  alarm_name          = "${var.name_prefix}-scheduler-missed-run"
  comparison_operator = "LessThanThreshold"
  evaluation_periods  = var.missed_run_window_hours
  datapoints_to_alarm = var.missed_run_window_hours
  metric_name         = "InvocationAttemptCount"
  namespace           = "AWS/Scheduler"
  period              = 3600
  statistic           = "Sum"
  threshold           = 1
  treat_missing_data  = "breaching"
  alarm_description   = "The ingestion schedule made no invocation attempt for ${var.missed_run_window_hours} hours."
  dimensions = {
    ScheduleGroup = aws_scheduler_schedule_group.ingestion[0].name
  }
  alarm_actions = [aws_sns_topic.alerts.arn]
  tags          = local.common_tags
}
