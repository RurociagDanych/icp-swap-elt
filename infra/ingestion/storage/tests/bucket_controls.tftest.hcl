# Offline checks (mock provider, no credentials) for the raw bucket's controls.

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

  mock_resource "aws_s3_bucket" {
    defaults = {
      arn = "arn:aws:s3:::mock-raw"
    }
  }
}

run "canister_and_rest_data_cannot_be_deleted_except_by_the_admin_exemption" {
  command = apply

  variables {
    delete_exempt_principal_arns = ["arn:aws:iam::123456789012:role/data-admin"]
  }

  assert {
    condition = length([
      for s in data.aws_iam_policy_document.raw_data_protect.statement : s
      if s.sid == "DenyDeleteOfIngestedData" && s.effect == "Deny" &&
      toset(s.actions) == toset(["s3:DeleteObject*"]) &&
      toset(tolist(s.resources)) == toset(["arn:aws:s3:::mock-raw/icp-swap-elt/canister/*", "arn:aws:s3:::mock-raw/icp-swap-elt/rest/*"]) &&
      length([for c in s.condition : c if c.variable == "aws:PrincipalArn" && c.test == "ArnNotLike" && toset(c.values) == toset(["arn:aws:iam::123456789012:role/data-admin"])]) == 1
    ]) == 1
    error_message = "Deny s3:DeleteObject* on <prefix>/canister/* and <prefix>/rest/* unless the principal is in delete_exempt_principal_arns"
  }

  assert {
    condition = length([
      for s in data.aws_iam_policy_document.raw_data_protect.statement : s
      if s.sid == "DenyDeleteOfIngestedData" && length([for r in tolist(s.resources) : r if strcontains(r, "/locks/") || strcontains(r, "/state/")]) > 0
    ]) == 0
    error_message = "the run lock (locks/) must stay deletable by the task role"
  }
}

run "without_an_exemption_the_deny_applies_to_everybody" {
  command = apply

  assert {
    condition = length([
      for s in data.aws_iam_policy_document.raw_data_protect.statement : s
      if s.sid == "DenyDeleteOfIngestedData" && length(s.condition) == 0
    ]) == 1
    error_message = "an empty exemption list means no exemption at all"
  }
}

run "the_bucket_enforces_owner_ownership_and_blocks_public_access" {
  command = apply

  assert {
    condition     = aws_s3_bucket_ownership_controls.raw_data.rule[0].object_ownership == "BucketOwnerEnforced"
    error_message = "ACLs must be disabled (BucketOwnerEnforced)"
  }

  assert {
    condition     = aws_s3_bucket_public_access_block.raw_data.block_public_acls && aws_s3_bucket_public_access_block.raw_data.block_public_policy && aws_s3_bucket_public_access_block.raw_data.ignore_public_acls && aws_s3_bucket_public_access_block.raw_data.restrict_public_buckets
    error_message = "all four public access blocks must be on"
  }
}

run "access_logging_is_off_by_default_and_opt_in" {
  command = apply

  assert {
    condition     = length(aws_s3_bucket_logging.raw_data) == 0
    error_message = "server access logging must default to off"
  }
}

run "access_logging_can_be_enabled" {
  command = apply

  variables {
    access_log_bucket_name = "my-log-bucket"
  }

  assert {
    condition     = length(aws_s3_bucket_logging.raw_data) == 1 && aws_s3_bucket_logging.raw_data[0].target_bucket == "my-log-bucket"
    error_message = "access_log_bucket_name enables server access logging"
  }
}

run "environment_tag_is_a_variable" {
  command = apply

  variables {
    environment = "prod"
  }

  assert {
    condition     = local.common_tags["Environment"] == "prod" && local.common_tags["Stage"] == "ingestion"
    error_message = "Environment follows var.environment"
  }
}
