# Load state machine definition, shared by two state machines that differ only in the Lambda
# function they run on:
#   - silver_backfill: historical loads, large aws_lambda_function.silver_backfill (10 GB).
#   - silver_incremental_load: IngestionRunFinished events, small aws_lambda_function.silver (2048 MB).
locals {
  load_state_machine_template = jsonencode({
    Comment = "Load: list raw CSV keys under the given prefixes into an S3 manifest, process each file via backfill_file, run the in-job table maintenance (mode=compact), then run the data-quality tests."
    StartAt = "ListKeys"
    States = {
      ListKeys = {
        Type           = "Task"
        Resource       = "arn:aws:states:::lambda:invoke"
        TimeoutSeconds = "__LOAD_TIMEOUT__"
        Parameters = {
          FunctionName = "__LOAD_FUNCTION_ARN__"
          "Payload.$"  = "$"
        }
        # Contract with apps/transformation (mode=list): {manifest_bucket, manifest_key, count}.
        ResultSelector = {
          "manifest.$" = "$.Payload"
        }
        Retry = [{
          ErrorEquals     = ["Lambda.ServiceException", "Lambda.AWSLambdaException", "Lambda.SdkClientException", "Lambda.TooManyRequestsException", "States.Timeout"]
          IntervalSeconds = 30
          MaxAttempts     = 3
          BackoffRate     = 2.0
        }]
        Next = "EmptyListChoice"
      }
      EmptyListChoice = {
        Type = "Choice"
        Choices = [{
          Variable      = "$.manifest.count"
          NumericEquals = 0
          Next          = "CheckQuality"
        }]
        Default = "ProcessEachKey"
      }
      ProcessEachKey = {
        Type  = "Map"
        Label = "ProcessEachKey"
        ItemReader = {
          Resource = "arn:aws:states:::s3:getObject"
          ReaderConfig = {
            InputType = "JSON"
          }
          Parameters = {
            "Bucket.$" = "$.manifest.manifest_bucket"
            "Key.$"    = "$.manifest.manifest_key"
          }
        }
        MaxConcurrency = var.backfill_max_concurrency
        # One failed object fails the load; redrive re-runs only the failed items.
        ToleratedFailurePercentage = 0
        # Discard the per-object results: the aggregate of all iterations is ONE state output
        # and Step Functions fails the whole execution with States.DataLimitExceeded above
        # 256 KiB, which a few hundred per-object results exceed.
        # Per-object results stay in the execution history and in the Lambda log.
        ResultPath = null
        ItemProcessor = {
          ProcessorConfig = { Mode = "DISTRIBUTED", ExecutionType = "STANDARD" }
          StartAt         = "ProcessKey"
          States = {
            ProcessKey = {
              Type     = "Task"
              Resource = "arn:aws:states:::lambda:invoke"
              Parameters = {
                FunctionName = "__LOAD_FUNCTION_ARN__"
                Payload = {
                  mode    = "backfill_file"
                  "key.$" = "$"
                }
              }
              # Keep only the (already slimmed) Lambda payload, not the invoke SDK envelope.
              ResultSelector = {
                "result.$" = "$.Payload"
              }
              Retry = [{
                ErrorEquals     = ["States.ALL"]
                IntervalSeconds = 30
                MaxAttempts     = 2
                BackoffRate     = 2.0
              }]
              End = true
            }
          }
        }
        Next = "CompactTable"
      }
      # In-job maintenance, serially after the whole Map (no writer of this execution is
      # running; reserved concurrency 1 throttles any other one): compaction of
      # icp_swap_events, snapshot retention of both tables, orphan cleanup of token_dim only.
      # Glue table optimizers are not relied on (24 h cadence, concurrent second writer).
      CompactTable = merge(local.compact_state_base, {
        TimeoutSeconds = "__LOAD_TIMEOUT__"
        Parameters = {
          FunctionName = "__LOAD_FUNCTION_ARN__"
          Payload = {
            mode = "compact"
          }
        }
      })
      CheckQuality = local.quality_state
    }
  })

  load_backfill_definition    = replace(replace(local.load_state_machine_template, "\"__LOAD_TIMEOUT__\"", tostring(var.silver_backfill_timeout_seconds + 60)), "__LOAD_FUNCTION_ARN__", aws_lambda_function.silver_backfill.arn)
  load_incremental_definition = replace(replace(local.load_state_machine_template, "\"__LOAD_TIMEOUT__\"", tostring(var.silver_timeout_seconds + 60)), "__LOAD_FUNCTION_ARN__", aws_lambda_function.silver.arn)

  incremental_state_machine_arn = "arn:${data.aws_partition.current.partition}:states:${var.aws_region}:${var.aws_account_id}:stateMachine:${var.name_prefix}-incremental"
  incremental_execution_arns    = "arn:${data.aws_partition.current.partition}:states:${var.aws_region}:${var.aws_account_id}:execution:${var.name_prefix}-incremental/*"
}

resource "aws_cloudwatch_log_group" "sfn_incremental" {
  name              = "/aws/vendedlogs/states/${var.name_prefix}-incremental"
  retention_in_days = var.log_retention_days
  tags              = local.common_tags
}

# Event-driven incremental load: list the run's raw keys, process each file, compact, quality.
# Every Lambda task runs on the small silver function. A file that does not fit its timeout fails
# the execution; fix the cause and start a new silver_backfill execution with the same prefixes
# (see docs/RUNBOOK.md).
resource "aws_sfn_state_machine" "silver_incremental_load" {
  name     = "${var.name_prefix}-incremental"
  role_arn = aws_iam_role.sfn_execution.arn

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.sfn_incremental.arn}:*"
    include_execution_data = false
    level                  = "ERROR"
  }

  definition = local.load_incremental_definition

  depends_on = [aws_iam_role_policy_attachment.sfn_runtime]

  tags = local.common_tags
}
