data "aws_caller_identity" "current" {}

locals {
  effective_processed_bucket_name      = var.processed_bucket_name != "" ? var.processed_bucket_name : "${var.name_prefix}-processed-${data.aws_caller_identity.current.account_id}-${var.aws_region}"
  effective_athena_results_bucket_name = var.athena_results_bucket_name != "" ? var.athena_results_bucket_name : "${var.name_prefix}-athena-results-${data.aws_caller_identity.current.account_id}-${var.aws_region}"
  effective_access_logs_bucket_name    = var.access_logs_bucket_name != "" ? var.access_logs_bucket_name : "${var.name_prefix}-access-logs-${data.aws_caller_identity.current.account_id}-${var.aws_region}"

  common_tags = merge(
    {
      Project     = "icp-swap-elt"
      Stage       = "transformation"
      ManagedBy   = "terraform"
      Environment = var.environment
      Layer       = "transformation-storage"
    },
    var.tags
  )
}

# --- Access-logs bucket (target for the other two buckets' server access logs) ----------------

resource "aws_s3_bucket" "access_logs" {
  bucket = local.effective_access_logs_bucket_name
  tags   = local.common_tags
}

resource "aws_s3_bucket_versioning" "access_logs" {
  bucket = aws_s3_bucket.access_logs.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_ownership_controls" "access_logs" {
  bucket = aws_s3_bucket.access_logs.id
  rule {
    object_ownership = "BucketOwnerPreferred"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "access_logs" {
  bucket = aws_s3_bucket.access_logs.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "access_logs" {
  bucket                  = aws_s3_bucket.access_logs.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "access_logs" {
  bucket = aws_s3_bucket.access_logs.id

  rule {
    id     = "expire-access-logs"
    status = "Enabled"
    filter {}
    expiration {
      days = var.access_logs_expiration_days
    }
  }

  rule {
    id     = "abort-incomplete-multipart-uploads"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }

  rule {
    id     = "expire-noncurrent-versions"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration {
      noncurrent_days = var.auxiliary_noncurrent_version_expiration_days
    }
  }
}

# --- Processed bucket: Iceberg warehouse (versioned; holds Silver data + quarantine) ----------

resource "aws_s3_bucket" "processed" {
  bucket = local.effective_processed_bucket_name
  tags   = local.common_tags
}

resource "aws_s3_bucket_versioning" "processed" {
  bucket = aws_s3_bucket.processed.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "processed" {
  bucket = aws_s3_bucket.processed.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "processed" {
  bucket                  = aws_s3_bucket.processed.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_logging" "processed" {
  bucket        = aws_s3_bucket.processed.id
  target_bucket = aws_s3_bucket.access_logs.id
  target_prefix = "processed/"
}

resource "aws_s3_bucket_lifecycle_configuration" "processed" {
  bucket = aws_s3_bucket.processed.id

  rule {
    id     = "expire-noncurrent-versions"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration {
      noncurrent_days = var.processed_noncurrent_version_expiration_days
    }
  }

  rule {
    id     = "abort-incomplete-multipart-uploads"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }

  # Temporary key manifests written by the load state machine's list step; never the warehouse.
  rule {
    id     = "expire-list-manifests"
    status = "Enabled"
    filter {
      prefix = "${var.manifest_prefix}/"
    }
    expiration {
      days = var.manifest_expiration_days
    }
    noncurrent_version_expiration {
      noncurrent_days = 1
    }
  }
}

# --- Athena query-results bucket ---------------------------------------------------------------

resource "aws_s3_bucket" "athena_results" {
  bucket = local.effective_athena_results_bucket_name
  tags   = local.common_tags
}

resource "aws_s3_bucket_versioning" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "athena_results" {
  bucket                  = aws_s3_bucket.athena_results.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_logging" "athena_results" {
  bucket        = aws_s3_bucket.athena_results.id
  target_bucket = aws_s3_bucket.access_logs.id
  target_prefix = "athena-results/"
}

resource "aws_s3_bucket_lifecycle_configuration" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id
  rule {
    id     = "expire-query-results"
    status = "Enabled"
    filter {}
    expiration {
      days = var.athena_results_expiration_days
    }
  }

  rule {
    id     = "abort-incomplete-multipart-uploads"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }

  rule {
    id     = "expire-noncurrent-versions"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration {
      noncurrent_days = var.auxiliary_noncurrent_version_expiration_days
    }
  }
}

# --- TLS-only bucket policies (every bucket this root owns) -----------------------------------

locals {
  tls_only_buckets = {
    (local.effective_processed_bucket_name)      = aws_s3_bucket.processed.arn
    (local.effective_athena_results_bucket_name) = aws_s3_bucket.athena_results.arn
    (local.effective_access_logs_bucket_name)    = aws_s3_bucket.access_logs.arn
  }
}

data "aws_iam_policy_document" "tls_only" {
  for_each = local.tls_only_buckets

  statement {
    sid     = "DenyInsecureTransport"
    effect  = "Deny"
    actions = ["s3:*"]

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    resources = [each.value, "${each.value}/*"]

    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "tls_only" {
  for_each = data.aws_iam_policy_document.tls_only
  bucket   = each.key
  policy   = each.value.json
}

# --- Glue Iceberg catalog + Athena workgroup ----------------------------------------------------
# Both are fresh resources owned by this root: the
# Iceberg TABLES themselves (icp_swap_events, token_dim) are never Terraform-managed — the
# registered transformation Lambda creates them lazily via pyiceberg's Glue catalog `create_table`
# (apps/transformation/src/silver_persistence.py), the same way the ingestion side treats its
# own S3 layout as application-owned rather than Terraform-owned.

resource "aws_glue_catalog_database" "transformation" {
  name = var.glue_database_name

  location_uri = "s3://${aws_s3_bucket.processed.bucket}/warehouse/"
  tags         = local.common_tags
}

resource "aws_athena_workgroup" "transformation" {
  name = var.athena_workgroup_name

  configuration {
    enforce_workgroup_configuration    = true
    bytes_scanned_cutoff_per_query     = var.athena_bytes_scanned_cutoff_bytes
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.bucket}/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }

  tags = local.common_tags
}
