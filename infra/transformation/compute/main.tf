data "aws_partition" "current" {}

locals {
  # # The managed repository is IMMUTABLE: functions reference an explicit, unique tag (or digest)
  # # via image_tag; there is no ":latest" to overwrite.
  image_uri = var.container_image != "" ? var.container_image : "${aws_ecr_repository.transformation.repository_url}:${var.image_tag}"

  common_tags = merge(
    {
      Project     = "icp-swap-elt"
      Stage       = "transformation"
      ManagedBy   = "terraform"
      Environment = var.environment
      Layer       = "transformation-compute"
    },
    var.tags
  )

  glue_db_arn      = "arn:${data.aws_partition.current.partition}:glue:${var.aws_region}:${var.aws_account_id}:database/${var.glue_database_name}"
  glue_table_arn   = "arn:${data.aws_partition.current.partition}:glue:${var.aws_region}:${var.aws_account_id}:table/${var.glue_database_name}/${var.glue_table_name}"
  glue_dim_arn     = "arn:${data.aws_partition.current.partition}:glue:${var.aws_region}:${var.aws_account_id}:table/${var.glue_database_name}/${var.token_table_name}"
  glue_catalog_arn = "arn:${data.aws_partition.current.partition}:glue:${var.aws_region}:${var.aws_account_id}:catalog"

  raw_bucket_arn       = "arn:${data.aws_partition.current.partition}:s3:::${var.raw_bucket_name}"
  processed_bucket_arn = "arn:${data.aws_partition.current.partition}:s3:::${var.processed_bucket_name}"
  # # Iceberg tables live below the Glue database location s3://<processed>/warehouse/; every
  # # object-level grant on the processed bucket is confined to it.
  warehouse_objects_arn = "${local.processed_bucket_arn}/warehouse/*"
  # # Manifests written by the list step (docs/OPERATIONS.md, "Load state machine contract").
  manifest_objects_arn = "${local.processed_bucket_arn}/${var.manifest_prefix}/*"

  # # Constructed, not referenced: the backfill state machine's own role must be allowed to start
  # # child executions of that very state machine (Distributed Map), which would otherwise be a cycle.
  backfill_state_machine_arn = "arn:${data.aws_partition.current.partition}:states:${var.aws_region}:${var.aws_account_id}:stateMachine:${var.name_prefix}-backfill"
  backfill_execution_arns    = "arn:${data.aws_partition.current.partition}:states:${var.aws_region}:${var.aws_account_id}:execution:${var.name_prefix}-backfill/*"
  alarm_source_arn_pattern   = "arn:${data.aws_partition.current.partition}:cloudwatch:${var.aws_region}:${var.aws_account_id}:alarm:${var.name_prefix}-*"
}

resource "aws_cloudwatch_log_group" "silver" {
  name              = "/aws/lambda/${var.name_prefix}-silver"
  retention_in_days = var.log_retention_days
  tags              = local.common_tags
}

resource "aws_cloudwatch_log_group" "silver_backfill" {
  name              = "/aws/lambda/${var.name_prefix}-silver-backfill"
  retention_in_days = var.log_retention_days
  tags              = local.common_tags
}

resource "aws_cloudwatch_log_group" "quality" {
  name              = "/aws/lambda/${var.name_prefix}-quality"
  retention_in_days = var.log_retention_days
  tags              = local.common_tags
}

# # The repository holds only build artifacts (no data): force_delete lets `terraform destroy`
# # remove it with images in it. Tags are immutable, so every push needs a unique tag or digest.
resource "aws_ecr_repository" "transformation" {
  name                 = var.name_prefix
  image_tag_mutability = "IMMUTABLE"
  force_delete         = true

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = local.common_tags
}

resource "aws_sqs_queue" "silver_dlq" {
  name                      = "${var.name_prefix}-silver-dlq"
  message_retention_seconds = var.dlq_retention_days * 24 * 3600
  tags                      = local.common_tags
}

data "aws_iam_policy_document" "lambda_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lambda_execution" {
  name               = "${var.name_prefix}-lambda"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
  tags               = local.common_tags
}

resource "aws_iam_role_policy_attachment" "lambda_basic_execution" {
  role       = aws_iam_role.lambda_execution.name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# # `lambda_execution` is the WRITER role shared by silver and silver_backfill (same code path);
# # `quality` is READ-ONLY with its own role (quality_job.py only loads tables via PyIceberg and
# # writes to /tmp).
data "aws_iam_policy_document" "lambda_permissions" {
  statement {
    sid       = "ListRawBucketWithinPrefix"
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [local.raw_bucket_arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["${var.raw_prefix}/*"]
    }
  }

  statement {
    sid       = "ReadRawObjectsWithinPrefix"
    effect    = "Allow"
    actions   = ["s3:GetObject"]
    resources = ["${local.raw_bucket_arn}/${var.raw_prefix}/*"]
  }

  statement {
    sid    = "ReadWriteIcebergWarehouse"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload",
      "s3:ListMultipartUploadParts",
    ]
    resources = [local.warehouse_objects_arn]
  }

  # # Not prefix-conditioned on purpose: an s3:prefix condition makes bucket-level calls without a
  # # prefix parameter (HeadBucket, which the S3 filesystem layer may issue) fail. The bucket only
  # # holds the warehouse and list manifests, and all object access stays confined.
  statement {
    sid       = "ListProcessedBucket"
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [local.processed_bucket_arn]
  }

  statement {
    sid       = "WriteListManifests"
    effect    = "Allow"
    actions   = ["s3:PutObject"]
    resources = [local.manifest_objects_arn]
  }

  statement {
    sid    = "GlueCatalogReadWriteScopedToOwnDatabase"
    effect = "Allow"
    actions = [
      "glue:GetDatabase",
      "glue:GetTable",
      "glue:GetTables",
      "glue:CreateTable",
      "glue:UpdateTable",
      "glue:GetPartitions",
      "glue:BatchCreatePartition",
      "glue:BatchDeletePartition",
    ]
    resources = [
      local.glue_catalog_arn,
      local.glue_db_arn,
      local.glue_table_arn,
      local.glue_dim_arn,
    ]
  }

  statement {
    sid       = "SendToOwnDeadLetterQueue"
    effect    = "Allow"
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.silver_dlq.arn]
  }
}

resource "aws_iam_policy" "lambda_permissions" {
  name   = "${var.name_prefix}-lambda"
  policy = data.aws_iam_policy_document.lambda_permissions.json
  tags   = local.common_tags
}

resource "aws_iam_role_policy_attachment" "lambda_permissions" {
  role       = aws_iam_role.lambda_execution.name
  policy_arn = aws_iam_policy.lambda_permissions.arn
}

resource "aws_iam_role" "quality" {
  name               = "${var.name_prefix}-quality"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
  tags               = local.common_tags
}

resource "aws_iam_role_policy_attachment" "quality_basic_execution" {
  role       = aws_iam_role.quality.name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

data "aws_iam_policy_document" "quality_permissions" {
  statement {
    sid       = "ReadIcebergWarehouse"
    effect    = "Allow"
    actions   = ["s3:GetObject"]
    resources = [local.warehouse_objects_arn]
  }

  statement {
    sid       = "ListProcessedBucket"
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [local.processed_bucket_arn]
  }

  statement {
    sid    = "GlueCatalogReadOnlyOwnDatabase"
    effect = "Allow"
    actions = [
      "glue:GetDatabase",
      "glue:GetTable",
      "glue:GetTables",
      "glue:GetPartitions",
    ]
    resources = [
      local.glue_catalog_arn,
      local.glue_db_arn,
      local.glue_table_arn,
      local.glue_dim_arn,
    ]
  }
}

resource "aws_iam_policy" "quality_permissions" {
  name   = "${var.name_prefix}-quality"
  policy = data.aws_iam_policy_document.quality_permissions.json
  tags   = local.common_tags
}

resource "aws_iam_role_policy_attachment" "quality_permissions" {
  role       = aws_iam_role.quality.name
  policy_arn = aws_iam_policy.quality_permissions.arn
}

resource "aws_lambda_function" "silver" {
  function_name = "${var.name_prefix}-silver"
  role          = aws_iam_role.lambda_execution.arn
  package_type  = "Image"
  image_uri     = local.image_uri
  memory_size   = var.silver_memory_size_mb
  timeout       = var.silver_timeout_seconds

  # # Bounds parallel Iceberg commits from this function (optimistic-concurrency contention on the
  # # Glue table); see docs/OPERATIONS.md "Concurrency".
  reserved_concurrent_executions = var.silver_reserved_concurrency

  image_config {
    command = ["silver_job.handler"]
  }

  environment {
    variables = {
      RAW_BUCKET                              = var.raw_bucket_name
      RAW_PREFIX                              = "${var.raw_prefix}/"
      PROCESSED_BUCKET                        = var.processed_bucket_name
      GLUE_DATABASE                           = var.glue_database_name
      TABLE_NAME                              = var.glue_table_name
      SILVER_TARGET_GENERATION                = var.silver_target_generation
      SILVER_MAX_SOURCE_BYTES                 = tostring(var.silver_max_source_bytes)
      SILVER_MAX_SOURCE_ROWS                  = tostring(var.silver_max_source_rows)
      SILVER_DEADLINE_SECONDS                 = tostring(var.silver_deadline_seconds)
      TOKEN_METADATA_MAX_RESPONSE_BYTES       = tostring(var.token_metadata_max_response_bytes)
      SILVER_COMPACT_TARGET_FILE_ROWS         = tostring(var.silver_backfill_compact_target_file_rows)
      SILVER_COMPACT_MIN_INPUT_FILES          = tostring(var.silver_backfill_compact_min_input_files)
      SILVER_COMPACT_MAX_INPUT_FILES          = tostring(var.silver_backfill_compact_max_input_files)
      SILVER_COMPACT_MAX_INPUT_ROWS           = tostring(var.silver_compact_max_input_rows)
      SILVER_COMPACT_MAX_INPUT_BYTES          = tostring(var.silver_compact_max_input_bytes)
      SILVER_COMPACT_PARTITION_FILE_THRESHOLD = tostring(var.compact_partition_file_threshold)
      SILVER_SNAPSHOT_RETENTION_DAYS          = tostring(var.snapshot_retention_days)
      SILVER_SNAPSHOTS_TO_RETAIN              = tostring(var.snapshots_to_retain)
      SILVER_ORPHAN_GRACE_DAYS                = tostring(var.orphan_file_retention_days)
    }
  }

  dead_letter_config {
    target_arn = aws_sqs_queue.silver_dlq.arn
  }

  lifecycle {
    precondition {
      condition     = var.silver_deadline_seconds < var.silver_timeout_seconds
      error_message = "silver_deadline_seconds must stay strictly below silver_timeout_seconds so the soft deadline trips before Lambda kills the invocation."
    }

    precondition {
      condition     = var.silver_backfill_compact_target_file_rows <= var.silver_compact_max_input_rows
      error_message = "silver_compact_max_input_rows must be at least silver_backfill_compact_target_file_rows (the job refuses it too)."
    }
  }

  depends_on = [
    aws_iam_role_policy_attachment.lambda_basic_execution,
    aws_iam_role_policy_attachment.lambda_permissions,
    aws_cloudwatch_log_group.silver,
  ]

  tags = local.common_tags
}

# # Same image, entrypoint, env and role as `silver`; differs only in memory/timeout and soft deadline
# # (Lambda memory is per FUNCTION, so a second function). AWS accounts can be capped at 3008 MB
# # until a Service Quotas increase; that only surfaces at apply time.
resource "aws_lambda_function" "silver_backfill" {
  function_name = "${var.name_prefix}-silver-backfill"
  role          = aws_iam_role.lambda_execution.arn
  package_type  = "Image"
  image_uri     = local.image_uri
  memory_size   = var.silver_backfill_memory_size_mb
  timeout       = var.silver_backfill_timeout_seconds

  # # Hard ceiling on parallel invocations of the writer: two overlapping executions would otherwise
  # # double the Map bound (backfill_max_concurrency).
  reserved_concurrent_executions = var.silver_backfill_reserved_concurrency

  image_config {
    command = ["silver_job.handler"]
  }

  environment {
    variables = {
      RAW_BUCKET                              = var.raw_bucket_name
      RAW_PREFIX                              = "${var.raw_prefix}/"
      PROCESSED_BUCKET                        = var.processed_bucket_name
      GLUE_DATABASE                           = var.glue_database_name
      TABLE_NAME                              = var.glue_table_name
      SILVER_TARGET_GENERATION                = var.silver_target_generation
      SILVER_MAX_SOURCE_BYTES                 = tostring(var.silver_max_source_bytes)
      SILVER_MAX_SOURCE_ROWS                  = tostring(var.silver_max_source_rows)
      SILVER_DEADLINE_SECONDS                 = tostring(var.silver_backfill_deadline_seconds)
      SILVER_MAX_SELECTED_FILES               = tostring(var.silver_backfill_max_selected_files)
      SILVER_MAX_SELECTED_ROWS                = tostring(var.silver_backfill_max_selected_rows)
      SILVER_MAX_SELECTED_BYTES               = tostring(var.silver_backfill_max_selected_bytes)
      TOKEN_METADATA_MAX_RESPONSE_BYTES       = tostring(var.token_metadata_max_response_bytes)
      SILVER_COMPACT_TARGET_FILE_ROWS         = tostring(var.silver_backfill_compact_target_file_rows)
      SILVER_COMPACT_MIN_INPUT_FILES          = tostring(var.silver_backfill_compact_min_input_files)
      SILVER_COMPACT_MAX_INPUT_FILES          = tostring(var.silver_backfill_compact_max_input_files)
      SILVER_COMPACT_MAX_INPUT_ROWS           = tostring(var.silver_backfill_compact_max_input_rows)
      SILVER_COMPACT_MAX_INPUT_BYTES          = tostring(var.silver_backfill_compact_max_input_bytes)
      SILVER_COMPACT_TRIGGER_SELECTED_FILES   = tostring(var.silver_backfill_compact_trigger_selected_files)
      SILVER_COMPACT_PARTITION_FILE_THRESHOLD = tostring(var.compact_partition_file_threshold)
      SILVER_SNAPSHOT_RETENTION_DAYS          = tostring(var.snapshot_retention_days)
      SILVER_SNAPSHOTS_TO_RETAIN              = tostring(var.snapshots_to_retain)
      SILVER_ORPHAN_GRACE_DAYS                = tostring(var.orphan_file_retention_days)
    }
  }

  dead_letter_config {
    target_arn = aws_sqs_queue.silver_dlq.arn
  }

  lifecycle {
    precondition {
      condition     = var.silver_backfill_deadline_seconds < var.silver_backfill_timeout_seconds
      error_message = "silver_backfill_deadline_seconds must stay strictly below silver_backfill_timeout_seconds so the soft deadline trips before Lambda kills the invocation."
    }

    precondition {
      condition     = var.silver_backfill_reserved_concurrency == -1 || var.silver_backfill_reserved_concurrency >= var.backfill_max_concurrency
      error_message = "silver_backfill_reserved_concurrency must be at least backfill_max_concurrency (or -1 for unreserved), otherwise the Map's own parallelism is throttled."
    }

    precondition {
      condition     = var.silver_backfill_compact_trigger_selected_files < var.silver_backfill_max_selected_files
      error_message = "silver_backfill_compact_trigger_selected_files must stay below silver_backfill_max_selected_files, otherwise the lookup cap fails the object before the per-object compaction can help."
    }

    precondition {
      condition     = var.silver_backfill_compact_target_file_rows <= var.silver_backfill_compact_max_input_rows && var.silver_backfill_compact_min_input_files <= var.silver_backfill_compact_max_input_files
      error_message = "compaction limits are inconsistent: target_file_rows must not exceed max_input_rows and min_input_files must not exceed max_input_files (the job refuses them too)."
    }
  }

  depends_on = [
    aws_iam_role_policy_attachment.lambda_basic_execution,
    aws_iam_role_policy_attachment.lambda_permissions,
    aws_cloudwatch_log_group.silver_backfill,
  ]

  tags = local.common_tags
}

# # Same image, other entrypoint: dbt tests over the Silver tables. Error-severity failures raise,
# # so the error alarm notifies. No DLQ (accepted): a DLQ only receives async-invoke failures and
# # this function is only invoked synchronously, so it would stay empty.
resource "aws_lambda_function" "quality" {
  function_name = "${var.name_prefix}-quality"
  role          = aws_iam_role.quality.arn
  package_type  = "Image"
  image_uri     = local.image_uri
  memory_size   = var.quality_memory_size_mb
  timeout       = var.quality_timeout_seconds

  image_config {
    command = ["quality_job.handler"]
  }

  ephemeral_storage {
    size = var.quality_ephemeral_storage_mb
  }

  environment {
    variables = {
      GLUE_DATABASE                  = var.glue_database_name
      TABLE_NAME                     = var.glue_table_name
      TOKEN_TABLE_NAME               = var.token_table_name
      QUALITY_CHECK_DEADLINE_SECONDS = tostring(var.quality_deadline_seconds)
    }
  }

  lifecycle {
    precondition {
      condition     = var.quality_deadline_seconds < var.quality_timeout_seconds
      error_message = "quality_deadline_seconds must stay strictly below quality_timeout_seconds so the soft deadline trips before Lambda kills the invocation."
    }
  }

  depends_on = [
    aws_iam_role_policy_attachment.quality_basic_execution,
    aws_iam_role_policy_attachment.quality_permissions,
    aws_cloudwatch_log_group.quality,
  ]

  tags = local.common_tags
}

resource "aws_sns_topic" "alerts" {
  name = "${var.name_prefix}-alerts"
  tags = local.common_tags
}

data "aws_iam_policy_document" "alerts_topic" {
  statement {
    sid    = "AllowBudgetsPublish"
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["budgets.amazonaws.com"]
    }

    actions   = ["SNS:Publish"]
    resources = [aws_sns_topic.alerts.arn]

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [var.aws_account_id]
    }
  }

  # # The alarms below use this topic as alarm_actions; without this statement CloudWatch is not
  # # allowed to publish and every alarm notification is dropped silently.
  statement {
    sid    = "AllowCloudWatchAlarmsPublish"
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["cloudwatch.amazonaws.com"]
    }

    actions   = ["SNS:Publish"]
    resources = [aws_sns_topic.alerts.arn]

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [var.aws_account_id]
    }

    condition {
      test     = "ArnLike"
      variable = "aws:SourceArn"
      values   = [local.alarm_source_arn_pattern]
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

resource "aws_cloudwatch_metric_alarm" "silver_errors" {
  alarm_name          = "${var.name_prefix}-silver-errors"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 300
  statistic           = "Sum"
  threshold           = 1
  treat_missing_data  = "notBreaching"
  alarm_description   = "The transformation Lambda (incremental/list/token_metadata) raised an error."
  dimensions = {
    FunctionName = aws_lambda_function.silver.function_name
  }
  alarm_actions = [aws_sns_topic.alerts.arn]
  tags          = local.common_tags
}

resource "aws_cloudwatch_metric_alarm" "silver_backfill_errors" {
  alarm_name          = "${var.name_prefix}-silver-backfill-errors"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 300
  statistic           = "Sum"
  threshold           = 1
  treat_missing_data  = "notBreaching"
  alarm_description   = "The transformation backfill Lambda (10 GiB, historical reload) raised an error."
  dimensions = {
    FunctionName = aws_lambda_function.silver_backfill.function_name
  }
  alarm_actions = [aws_sns_topic.alerts.arn]
  tags          = local.common_tags
}

resource "aws_cloudwatch_metric_alarm" "quality_errors" {
  alarm_name          = "${var.name_prefix}-quality-errors"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 300
  statistic           = "Sum"
  threshold           = 1
  treat_missing_data  = "notBreaching"
  alarm_description   = "The data-quality Lambda failed: a test with error severity failed, or the check could not run."
  dimensions = {
    FunctionName = aws_lambda_function.quality.function_name
  }
  alarm_actions = [aws_sns_topic.alerts.arn]
  tags          = local.common_tags
}

resource "aws_cloudwatch_metric_alarm" "silver_dlq_depth" {
  alarm_name          = "${var.name_prefix}-silver-dlq-not-empty"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "ApproximateNumberOfMessagesVisible"
  namespace           = "AWS/SQS"
  period              = 300
  statistic           = "Maximum"
  threshold           = 1
  treat_missing_data  = "notBreaching"
  alarm_description   = "An transformation Lambda (silver or silver_backfill) invocation failed all retries and landed on the dead-letter queue."
  dimensions = {
    QueueName = aws_sqs_queue.silver_dlq.name
  }
  alarm_actions = [aws_sns_topic.alerts.arn]
  tags          = local.common_tags
}

data "aws_iam_policy_document" "sfn_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "sfn_execution" {
  name               = "${var.name_prefix}-sfn"
  assume_role_policy = data.aws_iam_policy_document.sfn_assume_role.json
  tags               = local.common_tags
}

data "aws_iam_policy_document" "sfn_invoke_silver" {
  statement {
    sid     = "InvokePipelineLambdas"
    effect  = "Allow"
    actions = ["lambda:InvokeFunction"]
    resources = [
      aws_lambda_function.silver.arn,
      aws_lambda_function.silver_backfill.arn,
      aws_lambda_function.quality.arn,
    ]
  }
}

resource "aws_iam_policy" "sfn_invoke_silver" {
  name   = "${var.name_prefix}-sfn-invoke-silver"
  policy = data.aws_iam_policy_document.sfn_invoke_silver.json
  tags   = local.common_tags
}

resource "aws_iam_role_policy_attachment" "sfn_invoke_silver" {
  role       = aws_iam_role.sfn_execution.name
  policy_arn = aws_iam_policy.sfn_invoke_silver.arn
}

# # The load Map reads its item list from the manifest object the list step wrote: read access to
# # that prefix and nothing else.
data "aws_iam_policy_document" "sfn_read_manifests" {
  statement {
    sid       = "ReadListManifests"
    effect    = "Allow"
    actions   = ["s3:GetObject"]
    resources = [local.manifest_objects_arn]
  }
}

resource "aws_iam_policy" "sfn_read_manifests" {
  name   = "${var.name_prefix}-sfn-read-manifests"
  policy = data.aws_iam_policy_document.sfn_read_manifests.json
  tags   = local.common_tags
}

resource "aws_iam_role_policy_attachment" "sfn_read_manifests" {
  role       = aws_iam_role.sfn_execution.name
  policy_arn = aws_iam_policy.sfn_read_manifests.arn
}

# # Distributed Map runs every item as a child execution of the load state machine itself, and
# # both state machines deliver execution logs to CloudWatch Logs.
data "aws_iam_policy_document" "sfn_runtime" {
  statement {
    sid       = "StartOwnChildExecutions"
    effect    = "Allow"
    actions   = ["states:StartExecution"]
    resources = [local.backfill_state_machine_arn, local.incremental_state_machine_arn]
  }

  statement {
    sid    = "ManageOwnChildExecutions"
    effect = "Allow"
    actions = [
      "states:DescribeExecution",
      "states:StopExecution",
      "states:RedriveExecution",
    ]
    resources = [local.backfill_execution_arns, local.incremental_execution_arns]
  }

  # # Log delivery management does not support resource-level permissions: AWS documents "*" for
  # # exactly these actions (Step Functions "Logging using CloudWatch Logs" IAM policy).
  statement {
    sid    = "DeliverExecutionLogs"
    effect = "Allow"
    actions = [
      "logs:CreateLogDelivery",
      "logs:GetLogDelivery",
      "logs:UpdateLogDelivery",
      "logs:DeleteLogDelivery",
      "logs:ListLogDeliveries",
      "logs:PutResourcePolicy",
      "logs:DescribeResourcePolicies",
      "logs:DescribeLogGroups",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_policy" "sfn_runtime" {
  name   = "${var.name_prefix}-sfn-runtime"
  policy = data.aws_iam_policy_document.sfn_runtime.json
  tags   = local.common_tags
}

resource "aws_iam_role_policy_attachment" "sfn_runtime" {
  role       = aws_iam_role.sfn_execution.name
  policy_arn = aws_iam_policy.sfn_runtime.arn
}

# # Execution logs (level ERROR, no execution data: payloads carry S3 keys and are not logged).
resource "aws_cloudwatch_log_group" "sfn_backfill" {
  name              = "/aws/vendedlogs/states/${var.name_prefix}-backfill"
  retention_in_days = var.log_retention_days
  tags              = local.common_tags
}

locals {
  # # Last step of both state machines. Only transient Lambda errors are retried; a failed quality
  # # test (error severity) is not, so the execution fails and the quality error alarm notifies.
  quality_state = {
    Type           = "Task"
    Resource       = "arn:aws:states:::lambda:invoke"
    TimeoutSeconds = var.quality_timeout_seconds + 60
    Parameters = {
      FunctionName = aws_lambda_function.quality.arn
      Payload      = {}
    }
    Retry = [{
      ErrorEquals     = ["Lambda.ServiceException", "Lambda.AWSLambdaException", "Lambda.SdkClientException", "Lambda.TooManyRequestsException"]
      IntervalSeconds = 30
      MaxAttempts     = 2
      BackoffRate     = 2.0
    }]
    End = true
  }
}

locals {
  # # In-job table maintenance (mode=compact), shared by both state machines. Glue table optimizers
  # # are not relied on (24 h cadence, concurrent second writer).
  compact_state_base = {
    Type     = "Task"
    Resource = "arn:aws:states:::lambda:invoke"
    ResultSelector = {
      "summary.$" = "$.Payload"
    }
    ResultPath = "$.compaction"
    Retry = [
      {
        ErrorEquals     = ["Lambda.ServiceException", "Lambda.AWSLambdaException", "Lambda.SdkClientException", "Lambda.TooManyRequestsException", "States.Timeout"]
        IntervalSeconds = 30
        MaxAttempts     = 3
        BackoffRate     = 2.0
      },
      # # Compaction committed bounded progress but work remains (time): each retry resumes.
      # # Any other error (cap refusal, duplicate event_id, verification) fails before the quality check.
      {
        ErrorEquals     = ["MaintenanceIncomplete"]
        IntervalSeconds = 5
        MaxAttempts     = var.maintenance_incomplete_max_attempts
        BackoffRate     = 1.0
      },
    ]
    Next = "CheckQuality"
  }
}

# # Load failure policy: one object that fails all retries fails the Map and the execution, which
# # raises the silver_backfill error alarm. Redrive re-runs only the failed items; loading is
# # idempotent (event_id dedup), so a redrive or restart never double-loads.
resource "aws_sfn_state_machine" "silver_backfill" {
  name     = "${var.name_prefix}-backfill"
  role_arn = aws_iam_role.sfn_execution.arn

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.sfn_backfill.arn}:*"
    include_execution_data = false
    level                  = "ERROR"
  }

  definition = local.load_backfill_definition

  depends_on = [aws_iam_role_policy_attachment.sfn_runtime]

  tags = local.common_tags
}

resource "aws_budgets_budget" "transformation" {
  name         = "${var.name_prefix}-monthly-cost-guard"
  budget_type  = "COST"
  limit_amount = tostring(var.monthly_budget_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  cost_filter {
    name   = "TagKeyValue"
    values = ["user:Stage$transformation"]
  }

  notification {
    comparison_operator       = "GREATER_THAN"
    threshold                 = 80
    threshold_type            = "PERCENTAGE"
    notification_type         = "ACTUAL"
    subscriber_sns_topic_arns = [aws_sns_topic.alerts.arn]
  }

  notification {
    comparison_operator       = "GREATER_THAN"
    threshold                 = 100
    threshold_type            = "PERCENTAGE"
    notification_type         = "FORECASTED"
    subscriber_sns_topic_arns = [aws_sns_topic.alerts.arn]
  }
}

# # Loading is idempotent, so a redelivered or re-started event changes nothing. Undeliverable
# # events land in the DLQ, whose depth alarm already notifies the alerts topic.
resource "aws_cloudwatch_event_rule" "ingestion_finished" {
  count          = var.trigger_on_ingestion_events ? 1 : 0
  name           = "${var.name_prefix}-ingestion-finished"
  description    = "Ingestion run finished: load the raw prefixes it wrote."
  event_bus_name = var.event_bus_name

  event_pattern = jsonencode({
    source        = ["icp-swap-elt.ingestion"]
    "detail-type" = ["IngestionRunFinished"]
    detail = {
      bucket   = [var.raw_bucket_name]
      prefixes = [{ prefix = "${var.raw_prefix}/" }]
    }
  })

  tags = local.common_tags
}

data "aws_iam_policy_document" "events_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "events_start_load" {
  count              = var.trigger_on_ingestion_events ? 1 : 0
  name               = "${var.name_prefix}-events"
  assume_role_policy = data.aws_iam_policy_document.events_assume_role.json
  tags               = local.common_tags
}

data "aws_iam_policy_document" "events_start_load" {
  statement {
    sid       = "StartLoadStateMachine"
    effect    = "Allow"
    actions   = ["states:StartExecution"]
    resources = [aws_sfn_state_machine.silver_incremental_load.arn]
  }
}

resource "aws_iam_policy" "events_start_load" {
  count  = var.trigger_on_ingestion_events ? 1 : 0
  name   = "${var.name_prefix}-events-start-load"
  policy = data.aws_iam_policy_document.events_start_load.json
  tags   = local.common_tags
}

resource "aws_iam_role_policy_attachment" "events_start_load" {
  count      = var.trigger_on_ingestion_events ? 1 : 0
  role       = aws_iam_role.events_start_load[0].name
  policy_arn = aws_iam_policy.events_start_load[0].arn
}

resource "aws_cloudwatch_event_target" "load" {
  count          = var.trigger_on_ingestion_events ? 1 : 0
  rule           = aws_cloudwatch_event_rule.ingestion_finished[0].name
  event_bus_name = var.event_bus_name
  target_id      = "silver-backfill-load"
  arn            = aws_sfn_state_machine.silver_incremental_load.arn
  role_arn       = aws_iam_role.events_start_load[0].arn

  input_transformer {
    input_paths = {
      prefixes = "$.detail.prefixes"
    }
    input_template = "{\"mode\":\"list\",\"prefixes\":<prefixes>}"
  }

  retry_policy {
    maximum_event_age_in_seconds = 3600
    maximum_retry_attempts       = 4
  }

  dead_letter_config {
    arn = aws_sqs_queue.silver_dlq.arn
  }
}

data "aws_iam_policy_document" "dlq_from_events" {
  count = var.trigger_on_ingestion_events ? 1 : 0

  statement {
    sid       = "AllowIngestionRuleToDeadLetter"
    effect    = "Allow"
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.silver_dlq.arn]

    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }

    condition {
      test     = "ArnEquals"
      variable = "aws:SourceArn"
      values   = [aws_cloudwatch_event_rule.ingestion_finished[0].arn]
    }
  }
}

resource "aws_sqs_queue_policy" "dlq_from_events" {
  count     = var.trigger_on_ingestion_events ? 1 : 0
  queue_url = aws_sqs_queue.silver_dlq.url
  policy    = data.aws_iam_policy_document.dlq_from_events[0].json
}

# # NOT relied on: compaction, retention and token_dim cleanup run in our own job (mode=compact).
# # When on, Glue is a second, concurrent writer on the same tables; keep enable_table_optimizers off
# # until the first load has created the tables (an optimizer needs an existing table).
locals {
  optimizers = var.enable_table_optimizers ? 1 : 0
}

data "aws_iam_policy_document" "optimizer_assume_role" {
  count = local.optimizers

  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "table_optimizer" {
  count              = local.optimizers
  name               = "${var.name_prefix}-table-optimizer"
  assume_role_policy = data.aws_iam_policy_document.optimizer_assume_role[0].json
  tags               = local.common_tags
}

data "aws_iam_policy_document" "table_optimizer" {
  count = local.optimizers

  statement {
    sid       = "LakeFormationDataAccess"
    effect    = "Allow"
    actions   = ["lakeformation:GetDataAccess"]
    resources = ["*"]
  }

  statement {
    sid       = "ReadAndUpdateOwnTables"
    effect    = "Allow"
    actions   = ["glue:GetTable", "glue:UpdateTable"]
    resources = [local.glue_catalog_arn, local.glue_db_arn, local.glue_table_arn, local.glue_dim_arn]
  }

  statement {
    sid       = "WarehouseObjects"
    effect    = "Allow"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = ["arn:${data.aws_partition.current.partition}:s3:::${var.processed_bucket_name}/warehouse/*"]
  }

  statement {
    sid       = "WarehouseListing"
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = ["arn:${data.aws_partition.current.partition}:s3:::${var.processed_bucket_name}"]
  }

  statement {
    sid     = "OptimizerLogs"
    effect  = "Allow"
    actions = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
    resources = [
      "arn:${data.aws_partition.current.partition}:logs:${var.aws_region}:${var.aws_account_id}:log-group:/aws-glue/iceberg-compaction/logs:*",
      "arn:${data.aws_partition.current.partition}:logs:${var.aws_region}:${var.aws_account_id}:log-group:/aws-glue/iceberg-retention/logs:*",
      "arn:${data.aws_partition.current.partition}:logs:${var.aws_region}:${var.aws_account_id}:log-group:/aws-glue/iceberg-orphan-file-deletion/logs:*",
    ]
  }
}

resource "aws_iam_policy" "table_optimizer" {
  count  = local.optimizers
  name   = "${var.name_prefix}-table-optimizer"
  policy = data.aws_iam_policy_document.table_optimizer[0].json
  tags   = local.common_tags
}

resource "aws_iam_role_policy_attachment" "table_optimizer" {
  count      = local.optimizers
  role       = aws_iam_role.table_optimizer[0].name
  policy_arn = aws_iam_policy.table_optimizer[0].arn
}

resource "aws_glue_catalog_table_optimizer" "events_compaction" {
  count         = local.optimizers
  catalog_id    = var.aws_account_id
  database_name = var.glue_database_name
  table_name    = var.glue_table_name
  type          = "compaction"

  configuration {
    role_arn = aws_iam_role.table_optimizer[0].arn
    enabled  = true

    compaction_configuration {
      iceberg_configuration {
        strategy        = "binpack"
        min_input_files = var.compaction_min_input_files
      }
    }
  }

  depends_on = [aws_iam_role_policy_attachment.table_optimizer]
}

# # No orphan-file deletion for icp_swap_events: the quarantine artifacts live inside the table
# # location (silver-quarantine/<generation>/) and are not referenced by Iceberg metadata, so the
# # orphan cleaner would delete them.
resource "aws_glue_catalog_table_optimizer" "events_retention" {
  count         = local.optimizers
  catalog_id    = var.aws_account_id
  database_name = var.glue_database_name
  table_name    = var.glue_table_name
  type          = "retention"

  configuration {
    role_arn = aws_iam_role.table_optimizer[0].arn
    enabled  = true

    retention_configuration {
      iceberg_configuration {
        snapshot_retention_period_in_days = var.snapshot_retention_days
        number_of_snapshots_to_retain     = var.snapshots_to_retain
        clean_expired_files               = true
        run_rate_in_hours                 = var.optimizer_run_rate_hours
      }
    }
  }

  depends_on = [aws_iam_role_policy_attachment.table_optimizer]
}

resource "aws_glue_catalog_table_optimizer" "dim_retention" {
  count         = local.optimizers
  catalog_id    = var.aws_account_id
  database_name = var.glue_database_name
  table_name    = var.token_table_name
  type          = "retention"

  configuration {
    role_arn = aws_iam_role.table_optimizer[0].arn
    enabled  = true

    retention_configuration {
      iceberg_configuration {
        snapshot_retention_period_in_days = var.snapshot_retention_days
        number_of_snapshots_to_retain     = var.snapshots_to_retain
        clean_expired_files               = true
        run_rate_in_hours                 = var.optimizer_run_rate_hours
      }
    }
  }

  depends_on = [aws_iam_role_policy_attachment.table_optimizer]
}

resource "aws_glue_catalog_table_optimizer" "dim_orphan_files" {
  count         = local.optimizers
  catalog_id    = var.aws_account_id
  database_name = var.glue_database_name
  table_name    = var.token_table_name
  type          = "orphan_file_deletion"

  configuration {
    role_arn = aws_iam_role.table_optimizer[0].arn
    enabled  = true

    orphan_file_deletion_configuration {
      iceberg_configuration {
        orphan_file_retention_period_in_days = var.orphan_file_retention_days
        run_rate_in_hours                    = var.optimizer_run_rate_hours
      }
    }
  }

  depends_on = [aws_iam_role_policy_attachment.table_optimizer]
}
