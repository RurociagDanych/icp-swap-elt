# Offline plan-level checks (mock provider, no credentials) for the processed-bucket lifecycle.

mock_provider "aws" {
  mock_data "aws_caller_identity" {
    defaults = {
      account_id = "123456789012"
    }
  }

  mock_data "aws_iam_policy_document" {
    defaults = {
      json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}"
    }
  }
}

run "noncurrent_versions_outlive_snapshot_retention_by_a_margin" {
  command = plan

  assert {
    condition     = var.processed_noncurrent_version_expiration_days >= var.snapshot_retention_days + 30
    error_message = "the default noncurrent-version expiry must keep rollback files for at least snapshot retention + 30 days"
  }

  assert {
    condition     = one([for r in aws_s3_bucket_lifecycle_configuration.processed.rule : r if r.id == "expire-noncurrent-versions"]).noncurrent_version_expiration[0].noncurrent_days == 90
    error_message = "processed bucket: noncurrent versions must be kept 90 days by default"
  }
}

run "noncurrent_expiry_shorter_than_snapshot_retention_plus_margin_is_rejected" {
  command = plan

  variables {
    snapshot_retention_days                      = 7
    processed_noncurrent_version_expiration_days = 30
  }

  expect_failures = [var.processed_noncurrent_version_expiration_days]
}

run "longer_snapshot_retention_raises_the_floor" {
  command = plan

  variables {
    snapshot_retention_days                      = 90
    processed_noncurrent_version_expiration_days = 100
  }

  expect_failures = [var.processed_noncurrent_version_expiration_days]
}

run "list_manifests_expire_and_do_not_touch_the_warehouse" {
  command = plan

  assert {
    condition = (
      one([for r in aws_s3_bucket_lifecycle_configuration.processed.rule : r if r.id == "expire-list-manifests"]).status == "Enabled" &&
      one([for r in aws_s3_bucket_lifecycle_configuration.processed.rule : r if r.id == "expire-list-manifests"]).filter[0].prefix == "silver-manifests/" &&
      one([for r in aws_s3_bucket_lifecycle_configuration.processed.rule : r if r.id == "expire-list-manifests"]).expiration[0].days == 14
    )
    error_message = "list manifests under silver-manifests/ must expire after 14 days, and only they"
  }
}

run "environment_tag_is_a_variable" {
  command = plan

  variables {
    environment = "prod"
  }

  assert {
    condition     = local.common_tags["Environment"] == "prod" && local.common_tags["Stage"] == "transformation"
    error_message = "Environment follows var.environment, Stage stays transformation"
  }
}
