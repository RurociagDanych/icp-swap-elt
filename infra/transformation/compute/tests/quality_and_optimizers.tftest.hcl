# Offline checks (mock provider) for the quality Lambda and the Glue table optimizers.

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

  mock_resource "aws_cloudwatch_log_group" {
    defaults = {
      arn = "arn:aws:logs:us-east-1:123456789012:log-group:mock"
    }
  }

  mock_resource "aws_ecr_repository" {
    defaults = {
      repository_url = "123456789012.dkr.ecr.us-east-1.amazonaws.com/mock"
    }
  }
}

variables {
  aws_account_id        = "123456789012"
  raw_bucket_name       = "raw-bucket"
  processed_bucket_name = "processed-bucket"
  glue_database_name    = "icp_swap_elt"
  # Required inputs without module defaults.
  silver_target_generation = "g20261101"
  image_tag                = "test-1"
  # No schedules (the mock provider yields no valid ARNs for scheduler targets).
}

run "quality_lambda_runs_the_same_image_with_the_quality_entrypoint" {
  command = apply

  assert {
    condition     = aws_lambda_function.quality.image_config[0].command[0] == "quality_job.handler" && aws_lambda_function.quality.image_uri == aws_lambda_function.silver.image_uri
    error_message = "quality must run the shared image with the quality_job.handler entrypoint"
  }

  assert {
    condition     = aws_lambda_function.quality.function_name == "icp-swap-elt-transformation-quality"
    error_message = "unexpected quality function name"
  }

  assert {
    condition     = aws_lambda_function.quality.environment[0].variables["GLUE_DATABASE"] == "icp_swap_elt"
    error_message = "quality must read the transformation Glue database"
  }
}

run "quality_deadline_not_below_timeout_is_rejected" {
  command = plan

  variables {
    quality_deadline_seconds = 900
  }

  expect_failures = [aws_lambda_function.quality]
}

run "both_state_machines_end_with_the_quality_check" {
  command = apply

  assert {
    condition = (
      jsondecode(aws_sfn_state_machine.silver_backfill.definition).States.CompactTable.Next == "CheckQuality" &&
      jsondecode(aws_sfn_state_machine.silver_incremental_load.definition).States.CompactTable.Next == "CheckQuality" &&
      jsondecode(aws_sfn_state_machine.silver_backfill.definition).States.CheckQuality.End == true &&
      jsondecode(aws_sfn_state_machine.silver_incremental_load.definition).States.CheckQuality.End == true
    )
    error_message = "the quality check must be the last step of both the load and the incremental state machine"
  }

  assert {
    condition     = jsondecode(aws_sfn_state_machine.silver_backfill.definition).States.CheckQuality.Parameters.FunctionName == aws_lambda_function.quality.arn
    error_message = "the final step must invoke the quality Lambda"
  }

  assert {
    condition     = !contains(jsondecode(aws_sfn_state_machine.silver_backfill.definition).States.CheckQuality.Retry[0].ErrorEquals, "States.ALL")
    error_message = "a failed quality test must not be retried"
  }
}

run "optimizers_are_off_until_enabled" {
  command = apply

  assert {
    condition     = length(aws_glue_catalog_table_optimizer.events_compaction) == 0 && length(aws_iam_role.table_optimizer) == 0
    error_message = "no optimizer or optimizer role may exist before enable_table_optimizers is set"
  }
}

run "enabled_optimizers_cover_the_two_tables_without_orphan_cleanup_on_the_fact_table" {
  command = apply

  variables {
    enable_table_optimizers = true
  }

  assert {
    condition = (
      aws_glue_catalog_table_optimizer.events_compaction[0].type == "compaction" &&
      aws_glue_catalog_table_optimizer.events_compaction[0].table_name == "icp_swap_events" &&
      aws_glue_catalog_table_optimizer.events_compaction[0].configuration[0].compaction_configuration[0].iceberg_configuration[0].min_input_files == 10
    )
    error_message = "the fact table must be compacted (binpack, min 10 files)"
  }

  assert {
    condition = (
      aws_glue_catalog_table_optimizer.events_retention[0].type == "retention" &&
      aws_glue_catalog_table_optimizer.events_retention[0].table_name == "icp_swap_events" &&
      aws_glue_catalog_table_optimizer.dim_retention[0].table_name == "token_dim"
    )
    error_message = "both tables need snapshot retention"
  }

  assert {
    condition     = aws_glue_catalog_table_optimizer.dim_orphan_files[0].type == "orphan_file_deletion" && aws_glue_catalog_table_optimizer.dim_orphan_files[0].table_name == "token_dim"
    error_message = "orphan-file deletion is for token_dim only; the fact table's quarantine artifacts live in its location"
  }
}
