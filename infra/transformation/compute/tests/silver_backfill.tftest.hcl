# Offline checks (mock provider: no AWS credentials, no API calls) for the split transformation
# functions. Run from this root: `terraform init -backend=false && terraform test`.
# `command = apply` is used so that computed attributes (function ARNs) are known and the
# state-machine definitions can be inspected; with a mock provider nothing leaves the machine.

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
  # Pinned to the module defaults so the assertions below do not depend on the (gitignored,
  # auto-loaded) terraform.tfvars of whoever runs the test.
  silver_backfill_max_selected_files = 64
  silver_backfill_max_selected_rows  = 1000000
  silver_backfill_max_selected_bytes = 536870912
  # No schedules: the mock provider does not generate valid state-machine/Lambda ARNs for the
  # scheduler targets, and these checks are about the functions and state machines.
}

run "backfill_lambda_is_large_and_incremental_lambda_stays_small" {
  command = apply

  assert {
    condition     = aws_lambda_function.silver.memory_size == 2048 && aws_lambda_function.silver.timeout == 300
    error_message = "the incremental silver Lambda must stay at 2048 MB / 300 s"
  }

  assert {
    condition     = aws_lambda_function.silver_backfill.memory_size == 10240 && aws_lambda_function.silver_backfill.timeout == 900
    error_message = "the silver_backfill Lambda must default to 10240 MB / 900 s"
  }

  assert {
    condition     = aws_lambda_function.silver_backfill.image_config[0].command[0] == "silver_job.handler" && aws_lambda_function.silver_backfill.image_uri == aws_lambda_function.silver.image_uri
    error_message = "silver_backfill must run the same image and entrypoint as silver"
  }

  assert {
    condition     = aws_lambda_function.silver_backfill.function_name == "icp-swap-elt-transformation-silver-backfill"
    error_message = "unexpected silver_backfill function name"
  }

  assert {
    condition     = tonumber(aws_lambda_function.silver_backfill.environment[0].variables["SILVER_DEADLINE_SECONDS"]) < aws_lambda_function.silver_backfill.timeout
    error_message = "the backfill soft deadline must be strictly below its timeout"
  }

  assert {
    condition = (
      aws_lambda_function.silver_backfill.environment[0].variables["SILVER_MAX_SELECTED_ROWS"] == "1000000" &&
      aws_lambda_function.silver_backfill.environment[0].variables["SILVER_MAX_SELECTED_BYTES"] == "536870912" &&
      aws_lambda_function.silver_backfill.environment[0].variables["SILVER_MAX_SELECTED_FILES"] == "64" &&
      !contains(keys(aws_lambda_function.silver.environment[0].variables), "SILVER_MAX_SELECTED_ROWS")
    )
    error_message = "only silver_backfill gets the widened lookup caps; silver keeps the code defaults"
  }

  assert {
    condition     = tonumber(aws_lambda_function.silver.environment[0].variables["SILVER_DEADLINE_SECONDS"]) < aws_lambda_function.silver.timeout
    error_message = "the incremental soft deadline must be strictly below its timeout"
  }

  assert {
    condition     = aws_lambda_function.silver_backfill.dead_letter_config[0].target_arn == aws_lambda_function.silver.dead_letter_config[0].target_arn
    error_message = "both transformation functions share the dead-letter queue"
  }

  assert {
    condition     = aws_cloudwatch_log_group.silver_backfill.name == "/aws/lambda/icp-swap-elt-transformation-silver-backfill" && aws_cloudwatch_metric_alarm.silver_backfill_errors.dimensions["FunctionName"] == aws_lambda_function.silver_backfill.function_name
    error_message = "silver_backfill needs its own log group and error alarm"
  }
}

run "state_machines_invoke_the_matching_function" {
  command = apply

  assert {
    condition     = length(split(aws_lambda_function.silver_backfill.arn, aws_sfn_state_machine.silver_backfill.definition)) - 1 == 3
    error_message = "the list task, the per-key task and the compact task of the backfill state machine must target silver_backfill"
  }

  assert {
    condition     = !strcontains(aws_sfn_state_machine.silver_backfill.definition, aws_lambda_function.silver.arn)
    error_message = "the backfill state machine must NOT invoke the small silver function"
  }

  assert {
    condition     = strcontains(aws_sfn_state_machine.silver_incremental_load.definition, aws_lambda_function.silver.arn) && !strcontains(aws_sfn_state_machine.silver_incremental_load.definition, aws_lambda_function.silver_backfill.arn)
    error_message = "the incremental state machine must invoke the small silver function only"
  }
}

run "sfn_invoke_policy_covers_exactly_the_three_pipeline_functions" {
  command = apply

  assert {
    condition     = length(data.aws_iam_policy_document.sfn_invoke_silver.statement[0].resources) == 3 && contains(data.aws_iam_policy_document.sfn_invoke_silver.statement[0].resources, aws_lambda_function.silver.arn) && contains(data.aws_iam_policy_document.sfn_invoke_silver.statement[0].resources, aws_lambda_function.silver_backfill.arn) && contains(data.aws_iam_policy_document.sfn_invoke_silver.statement[0].resources, aws_lambda_function.quality.arn)
    error_message = "the state-machine invoke policy must list exactly silver, silver_backfill and quality"
  }
}

run "backfill_deadline_not_below_timeout_is_rejected" {
  command = plan

  variables {
    silver_backfill_deadline_seconds = 900
  }

  expect_failures = [aws_lambda_function.silver_backfill]
}

run "backfill_memory_above_lambda_maximum_is_rejected" {
  command = plan

  variables {
    silver_backfill_memory_size_mb = 10241
  }

  expect_failures = [var.silver_backfill_memory_size_mb]
}

run "backfill_map_discards_the_aggregate_so_it_cannot_exceed_the_256kib_state_output_limit" {
  command = apply

  assert {
    condition     = contains(keys(jsondecode(aws_sfn_state_machine.silver_backfill.definition).States.ProcessEachKey), "ResultPath") && jsondecode(aws_sfn_state_machine.silver_backfill.definition).States.ProcessEachKey.ResultPath == null
    error_message = "ProcessEachKey must set ResultPath null: 72 slim per-object results already approach the limit, ~300 (Dataset B) would exceed it (States.DataLimitExceeded)"
  }

  assert {
    condition     = jsondecode(aws_sfn_state_machine.silver_backfill.definition).States.ProcessEachKey.ItemProcessor.States.ProcessKey.ResultSelector["result.$"] == "$.Payload"
    error_message = "the per-key task must keep only the Lambda payload, not the invoke SDK envelope"
  }

}

run "ingestion_event_starts_the_load_state_machine_on_the_announced_prefixes" {
  command = apply

  assert {
    condition = (
      jsondecode(aws_cloudwatch_event_rule.ingestion_finished[0].event_pattern).source[0] == "icp-swap-elt.ingestion" &&
      jsondecode(aws_cloudwatch_event_rule.ingestion_finished[0].event_pattern)["detail-type"][0] == "IngestionRunFinished"
    )
    error_message = "the rule must match the event contract published by apps/ingestion (source and detail-type)"
  }

  assert {
    condition = (
      jsondecode(aws_cloudwatch_event_rule.ingestion_finished[0].event_pattern).detail.bucket[0] == "raw-bucket" &&
      jsondecode(aws_cloudwatch_event_rule.ingestion_finished[0].event_pattern).detail.prefixes[0].prefix == "${var.raw_prefix}/"
    )
    error_message = "the rule must only react to runs that wrote into raw_bucket_name under raw_prefix"
  }

  assert {
    condition     = aws_cloudwatch_event_target.load[0].arn == aws_sfn_state_machine.silver_incremental_load.arn
    error_message = "the event must start the load (silver_backfill) state machine"
  }

  assert {
    condition = (
      aws_cloudwatch_event_target.load[0].input_transformer[0].input_paths["prefixes"] == "$.detail.prefixes" &&
      jsondecode(replace(aws_cloudwatch_event_target.load[0].input_transformer[0].input_template, "<prefixes>", "[\"p/\"]")).mode == "list" &&
      jsondecode(replace(aws_cloudwatch_event_target.load[0].input_transformer[0].input_template, "<prefixes>", "[\"p/\"]")).prefixes[0] == "p/"
    )
    error_message = "the target input must be {mode: list, prefixes: <the announced prefixes>}"
  }

  assert {
    condition     = aws_cloudwatch_event_target.load[0].dead_letter_config[0].arn == aws_sqs_queue.silver_dlq.arn
    error_message = "undeliverable events must land in the DLQ that already has a depth alarm"
  }
}

run "event_trigger_can_be_disabled" {
  command = apply

  variables {
    trigger_on_ingestion_events = false
  }

  assert {
    condition     = length(aws_cloudwatch_event_rule.ingestion_finished) == 0 && length(aws_cloudwatch_event_target.load) == 0
    error_message = "trigger_on_ingestion_events = false must create no rule or target"
  }
}
