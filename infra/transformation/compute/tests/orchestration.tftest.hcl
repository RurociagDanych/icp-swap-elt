# Offline checks (mock provider, no credentials) for concurrency limits, the
# manifest-based load state machine, logging, ECR and input validation.

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

variables {
  aws_account_id           = "123456789012"
  raw_bucket_name          = "raw-bucket"
  processed_bucket_name    = "processed-bucket"
  glue_database_name       = "icp_swap_elt"
  silver_target_generation = "g20261101"
  image_tag                = "test-1"
}

# --- H4: bounded parallelism ---------------------------------------------------------------

run "reserved_concurrency_bounds_parallel_iceberg_commits" {
  command = plan

  assert {
    condition     = aws_lambda_function.silver_backfill.reserved_concurrent_executions == 1
    error_message = "silver_backfill must default to reserved concurrency 1 (serialised Iceberg commits)"
  }

  assert {
    condition     = aws_lambda_function.silver.reserved_concurrent_executions == 1
    error_message = "silver must default to reserved concurrency 1 (serialised incremental commits)"
  }
}

run "backfill_max_concurrency_below_one_is_rejected" {
  command = plan

  variables {
    backfill_max_concurrency = 0
  }

  expect_failures = [var.backfill_max_concurrency]
}

run "map_concurrency_above_reserved_function_concurrency_is_rejected" {
  command = plan

  variables {
    backfill_max_concurrency             = 3
    silver_backfill_reserved_concurrency = 2
  }

  expect_failures = [aws_lambda_function.silver_backfill]
}

# --- H5: ListKeys contract + Map failure policy ---------------------------------------------

run "list_step_has_retry_timeout_and_returns_only_a_manifest_pointer" {
  command = apply

  assert {
    condition     = jsondecode(aws_sfn_state_machine.silver_backfill.definition).States.ListKeys.TimeoutSeconds == var.silver_backfill_timeout_seconds + 60
    error_message = "ListKeys needs a TimeoutSeconds just above the Lambda timeout"
  }

  assert {
    condition     = length(jsondecode(aws_sfn_state_machine.silver_backfill.definition).States.ListKeys.Retry) >= 1 && contains(jsondecode(aws_sfn_state_machine.silver_backfill.definition).States.ListKeys.Retry[0].ErrorEquals, "Lambda.TooManyRequestsException")
    error_message = "ListKeys needs a Retry that covers Lambda throttling and service errors"
  }

  assert {
    condition     = jsondecode(aws_sfn_state_machine.silver_backfill.definition).States.ListKeys.ResultSelector["manifest.$"] == "$.Payload"
    error_message = "ListKeys must keep only the small {manifest_bucket, manifest_key, count} payload"
  }
}

run "load_map_reads_keys_from_the_s3_manifest_and_tolerates_no_failures" {
  command = apply

  assert {
    condition = (
      jsondecode(aws_sfn_state_machine.silver_backfill.definition).States.ProcessEachKey.ItemReader.Resource == "arn:aws:states:::s3:getObject" &&
      jsondecode(aws_sfn_state_machine.silver_backfill.definition).States.ProcessEachKey.ItemReader.ReaderConfig.InputType == "JSON" &&
      jsondecode(aws_sfn_state_machine.silver_backfill.definition).States.ProcessEachKey.ItemReader.Parameters["Bucket.$"] == "$.manifest.manifest_bucket" &&
      jsondecode(aws_sfn_state_machine.silver_backfill.definition).States.ProcessEachKey.ItemReader.Parameters["Key.$"] == "$.manifest.manifest_key"
    )
    error_message = "the Map must read its items from the S3 JSON manifest named by ListKeys"
  }

  assert {
    condition     = jsondecode(aws_sfn_state_machine.silver_backfill.definition).States.ProcessEachKey.ItemProcessor.ProcessorConfig.Mode == "DISTRIBUTED" && jsondecode(aws_sfn_state_machine.silver_backfill.definition).States.ProcessEachKey.ItemProcessor.ProcessorConfig.ExecutionType == "STANDARD"
    error_message = "ItemReader from S3 is only available in Distributed mode (inline Map takes a JSON array only)"
  }

  assert {
    condition     = jsondecode(aws_sfn_state_machine.silver_backfill.definition).States.ProcessEachKey.ToleratedFailurePercentage == 0 && jsondecode(aws_sfn_state_machine.silver_backfill.definition).States.ProcessEachKey.MaxConcurrency == 1
    error_message = "a single failed object must fail the load (tolerated failure 0, redrive) and concurrency stays bounded"
  }

  assert {
    condition     = jsondecode(aws_sfn_state_machine.silver_backfill.definition).States.ProcessEachKey.Next == "CompactTable" && jsondecode(aws_sfn_state_machine.silver_backfill.definition).States.EmptyListChoice.Default == "ProcessEachKey"
    error_message = "ListKeys leads through the empty-list choice into the Map, which continues to the maintenance step and then the quality check"
  }
}

# --- In-job table maintenance after the Map --------------------------------------------------

run "compact_step_runs_after_the_map_and_before_the_quality_check" {
  command = apply

  assert {
    condition = (
      jsondecode(aws_sfn_state_machine.silver_backfill.definition).States.ProcessEachKey.Next == "CompactTable" &&
      jsondecode(aws_sfn_state_machine.silver_backfill.definition).States.CompactTable.Next == "CheckQuality" &&
      jsondecode(aws_sfn_state_machine.silver_backfill.definition).States.CompactTable.Type == "Task"
    )
    error_message = "CompactTable must sit between the ProcessEachKey Map and CheckQuality"
  }

  assert {
    condition = (
      jsondecode(aws_sfn_state_machine.silver_backfill.definition).States.CompactTable.Parameters.FunctionName == aws_lambda_function.silver_backfill.arn &&
      jsondecode(aws_sfn_state_machine.silver_backfill.definition).States.CompactTable.Parameters.Payload.mode == "compact"
    )
    error_message = "CompactTable must invoke silver_backfill with the compact payload"
  }

  assert {
    condition = (
      length(jsondecode(aws_sfn_state_machine.silver_backfill.definition).States.CompactTable.Retry) == 2 &&
      contains(jsondecode(aws_sfn_state_machine.silver_backfill.definition).States.CompactTable.Retry[0].ErrorEquals, "Lambda.ServiceException") &&
      contains(jsondecode(aws_sfn_state_machine.silver_backfill.definition).States.CompactTable.Retry[0].ErrorEquals, "Lambda.TooManyRequestsException") &&
      !contains(jsondecode(aws_sfn_state_machine.silver_backfill.definition).States.CompactTable.Retry[0].ErrorEquals, "States.ALL") &&
      jsondecode(aws_sfn_state_machine.silver_backfill.definition).States.CompactTable.Retry[1].ErrorEquals == ["MaintenanceIncomplete"] &&
      jsondecode(aws_sfn_state_machine.silver_backfill.definition).States.CompactTable.Retry[1].MaxAttempts == 5
    )
    error_message = "CompactTable needs a Retry on Lambda service errors/throttling and a bounded resume Retry on MaintenanceIncomplete only (never States.ALL: a refused compaction must fail the load)"
  }

  assert {
    condition     = jsondecode(aws_sfn_state_machine.silver_backfill.definition).States.CompactTable.TimeoutSeconds == var.silver_backfill_timeout_seconds + 60
    error_message = "CompactTable needs a TimeoutSeconds just above the Lambda timeout"
  }

  assert {
    condition     = jsondecode(aws_sfn_state_machine.silver_backfill.definition).States.CompactTable.ResultPath == "$.compaction"
    error_message = "CompactTable keeps only its slim summary under $.compaction"
  }

  assert {
    condition = (
      jsondecode(aws_sfn_state_machine.silver_backfill.definition).States.ProcessEachKey.MaxConcurrency == 1 &&
      aws_lambda_function.silver_backfill.reserved_concurrent_executions == 1
    )
    error_message = "the Map stays at MaxConcurrency 1 and silver_backfill at reserved concurrency 1 (serial writer)"
  }

  assert {
    condition = (
      aws_lambda_function.silver_backfill.environment[0].variables["SILVER_COMPACT_TRIGGER_SELECTED_FILES"] == "48" &&
      aws_lambda_function.silver_backfill.environment[0].variables["SILVER_COMPACT_MAX_INPUT_FILES"] == "256" &&
      aws_lambda_function.silver_backfill.environment[0].variables["SILVER_COMPACT_MAX_INPUT_ROWS"] == "2000000" &&
      aws_lambda_function.silver_backfill.environment[0].variables["SILVER_COMPACT_MAX_INPUT_BYTES"] == "536870912" &&
      aws_lambda_function.silver_backfill.environment[0].variables["SILVER_SNAPSHOT_RETENTION_DAYS"] == "7" &&
      aws_lambda_function.silver_backfill.environment[0].variables["SILVER_SNAPSHOTS_TO_RETAIN"] == "1" &&
      aws_lambda_function.silver_backfill.environment[0].variables["SILVER_ORPHAN_GRACE_DAYS"] == "3" &&
      !contains(keys(aws_lambda_function.silver.environment[0].variables), "SILVER_COMPACT_TRIGGER_SELECTED_FILES")
    )
    error_message = "maintenance caps go to silver_backfill only; the incremental silver function has no per-object compaction trigger"
  }

  assert {
    condition     = length(aws_glue_catalog_table_optimizer.events_compaction) == 0
    error_message = "the in-job maintenance must not create Glue table optimizers (enable_table_optimizers stays off by default)"
  }
}

run "incremental_state_machine_maps_each_file_on_silver_then_compacts_and_checks_quality" {
  command = apply

  assert {
    condition = (
      jsondecode(aws_sfn_state_machine.silver_incremental_load.definition).StartAt == "ListKeys" &&
      jsondecode(aws_sfn_state_machine.silver_incremental_load.definition).States.ListKeys.Parameters.FunctionName == aws_lambda_function.silver.arn &&
      jsondecode(aws_sfn_state_machine.silver_incremental_load.definition).States.ProcessEachKey.ItemProcessor.States.ProcessKey.Parameters.FunctionName == aws_lambda_function.silver.arn &&
      jsondecode(aws_sfn_state_machine.silver_incremental_load.definition).States.CompactTable.Next == "CheckQuality" &&
      jsondecode(aws_sfn_state_machine.silver_incremental_load.definition).States.CheckQuality.End == true
    )
    error_message = "incremental order must be ListKeys -> ProcessEachKey -> CompactTable -> CheckQuality, all on the small silver function"
  }

  assert {
    condition = (
      jsondecode(aws_sfn_state_machine.silver_incremental_load.definition).States.CompactTable.Parameters.FunctionName == aws_lambda_function.silver.arn &&
      jsondecode(aws_sfn_state_machine.silver_incremental_load.definition).States.CompactTable.Parameters.Payload.mode == "compact" &&
      jsondecode(aws_sfn_state_machine.silver_incremental_load.definition).States.CompactTable.TimeoutSeconds == var.silver_timeout_seconds + 60
    )
    error_message = "the incremental CompactTable invokes the small silver function with the compact payload"
  }

  assert {
    condition = (
      contains(jsondecode(aws_sfn_state_machine.silver_incremental_load.definition).States.CompactTable.Retry[0].ErrorEquals, "Lambda.ServiceException") &&
      !contains(jsondecode(aws_sfn_state_machine.silver_incremental_load.definition).States.CompactTable.Retry[0].ErrorEquals, "States.ALL") &&
      jsondecode(aws_sfn_state_machine.silver_incremental_load.definition).States.CompactTable.Retry[1].ErrorEquals == ["MaintenanceIncomplete"]
    )
    error_message = "the incremental CompactTable retries Lambda service errors and resumes on MaintenanceIncomplete only"
  }
}

run "compaction_trigger_at_or_above_the_lookup_cap_is_rejected" {
  command = plan

  variables {
    silver_backfill_compact_trigger_selected_files = 64
  }

  expect_failures = [aws_lambda_function.silver_backfill]
}

run "snapshot_retention_beyond_the_noncurrent_version_headroom_is_rejected" {
  command = plan

  variables {
    snapshot_retention_days = 61
  }

  expect_failures = [var.snapshot_retention_days]
}

run "state_machine_role_may_read_only_the_manifest_prefix" {
  command = plan

  assert {
    condition = length([
      for s in data.aws_iam_policy_document.sfn_read_manifests.statement : s
      if contains(tolist(s.actions), "s3:GetObject") && length(tolist(s.actions)) == 1 &&
      alltrue([for r in tolist(s.resources) : r == "arn:aws:s3:::processed-bucket/silver-manifests/*"])
    ]) == 1
    error_message = "the Step Functions role needs s3:GetObject on the manifest prefix and nothing wider"
  }

  assert {
    condition = length([
      for s in data.aws_iam_policy_document.lambda_permissions.statement : s
      if s.sid == "WriteListManifests" && alltrue([for r in tolist(s.resources) : r == "arn:aws:s3:::processed-bucket/silver-manifests/*"]) && !contains(tolist(s.actions), "s3:DeleteObject")
    ]) == 1
    error_message = "the list handler may put manifests below the manifest prefix only"
  }

}

# --- M8: state machine logging ----------------------------------------------------------------

run "both_state_machines_log_errors_without_execution_data" {
  command = apply

  assert {
    condition = (
      aws_sfn_state_machine.silver_backfill.logging_configuration[0].level == "ERROR" &&
      aws_sfn_state_machine.silver_backfill.logging_configuration[0].include_execution_data == false &&
      aws_sfn_state_machine.silver_incremental_load.logging_configuration[0].level == "ERROR" &&
      aws_sfn_state_machine.silver_incremental_load.logging_configuration[0].include_execution_data == false
    )
    error_message = "state machines must log level ERROR with include_execution_data = false"
  }

  assert {
    condition     = aws_cloudwatch_log_group.sfn_backfill.retention_in_days == 30 && aws_cloudwatch_log_group.sfn_incremental.retention_in_days == 30
    error_message = "state machine log groups need a retention period"
  }

  assert {
    condition     = endswith(aws_sfn_state_machine.silver_backfill.logging_configuration[0].log_destination, ":*")
    error_message = "log_destination must be the log group ARN with the :* suffix"
  }
}

# --- H6: ECR + image reference ------------------------------------------------------------------

run "ecr_repository_is_immutable_and_deletable_and_lambdas_use_a_unique_tag" {
  command = apply

  assert {
    condition     = aws_ecr_repository.transformation.image_tag_mutability == "IMMUTABLE" && aws_ecr_repository.transformation.force_delete == true
    error_message = "the image repository holds no data: force_delete true, tags immutable"
  }

  assert {
    condition     = aws_lambda_function.silver.image_uri == "123456789012.dkr.ecr.us-east-1.amazonaws.com/mock:test-1" && !endswith(aws_lambda_function.quality.image_uri, ":latest")
    error_message = "functions must reference the repository by the explicit image_tag, never :latest"
  }
}

run "image_tag_or_container_image_is_required" {
  command = plan

  variables {
    image_tag       = ""
    container_image = ""
  }

  expect_failures = [var.image_tag]
}

run "latest_tag_is_rejected_for_the_managed_repository" {
  command = plan

  variables {
    image_tag = "latest"
  }

  expect_failures = [var.image_tag]
}

# --- L4 -----------------------------------------------------------------------------------------

run "unsafe_target_generation_is_rejected" {
  command = plan

  variables {
    silver_target_generation = "../x"
  }

  expect_failures = [var.silver_target_generation]
}

# --- M6 -----------------------------------------------------------------------------------------

run "environment_tag_follows_the_environment_variable" {
  command = plan

  variables {
    environment = "prod"
  }

  assert {
    condition     = local.common_tags["Environment"] == "prod" && local.common_tags["Stage"] == "transformation"
    error_message = "Environment must come from var.environment; Stage stays the cost-allocation tag the budget filters on"
  }
}
