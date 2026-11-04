data "aws_caller_identity" "current" {}

locals {
  effective_bucket_name = var.bucket_name != "" ? var.bucket_name : "${var.name_prefix}-${data.aws_caller_identity.current.account_id}-${var.aws_region}"
  common_tags = merge(
    {
      Project     = "icp-swap-elt"
      Stage       = "ingestion"
      ManagedBy   = "terraform"
      Environment = var.environment
      Layer       = "storage"
    },
    var.tags
  )
}

resource "aws_s3_bucket" "raw_data" {
  bucket = local.effective_bucket_name
  tags   = local.common_tags
}

resource "aws_s3_bucket_versioning" "raw_data" {
  bucket = aws_s3_bucket.raw_data.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "raw_data" {
  bucket = aws_s3_bucket.raw_data.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "raw_data" {
  bucket = aws_s3_bucket.raw_data.id

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
      noncurrent_days = var.noncurrent_version_expiration_days
    }
  }
}

resource "aws_s3_bucket_public_access_block" "raw_data" {
  bucket = aws_s3_bucket.raw_data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "raw_data" {
  bucket = aws_s3_bucket.raw_data.id

  # ACLs disabled: the bucket owner owns every object and access is governed by policies only.
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

# Optional server access logging, off by default. The target bucket must already exist and allow
# log delivery (logging.s3.amazonaws.com); it is not managed here.
resource "aws_s3_bucket_logging" "raw_data" {
  count         = var.access_log_bucket_name != "" ? 1 : 0
  bucket        = aws_s3_bucket.raw_data.id
  target_bucket = var.access_log_bucket_name
  target_prefix = "${local.effective_bucket_name}/"
}

data "aws_iam_policy_document" "raw_data_tls_only" {
  statement {
    sid    = "DenyInsecureTransport"
    effect = "Deny"

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    actions = ["s3:*"]

    resources = [
      aws_s3_bucket.raw_data.arn,
      "${aws_s3_bucket.raw_data.arn}/*",
    ]

    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

# Ingested data is append-only for everybody except an explicit admin. The ingestion task only
# ever deletes its run lock (<prefix>/locks/run.lock); canister/ and rest/ hold the raw history.
# Applies to DeleteObject and DeleteObjectVersion (s3:DeleteObject*). Lifecycle expiration of
# noncurrent versions is performed by S3 itself and is not affected by bucket policies.
data "aws_iam_policy_document" "raw_data_protect" {
  source_policy_documents = [data.aws_iam_policy_document.raw_data_tls_only.json]

  statement {
    sid    = "DenyDeleteOfIngestedData"
    effect = "Deny"

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    actions = ["s3:DeleteObject*"]

    resources = [
      "${aws_s3_bucket.raw_data.arn}/${var.s3_prefix}/canister/*",
      "${aws_s3_bucket.raw_data.arn}/${var.s3_prefix}/rest/*",
    ]

    dynamic "condition" {
      for_each = length(var.delete_exempt_principal_arns) > 0 ? [1] : []

      content {
        test     = "ArnNotLike"
        variable = "aws:PrincipalArn"
        values   = var.delete_exempt_principal_arns
      }
    }
  }
}

resource "aws_s3_bucket_policy" "raw_data" {
  bucket = aws_s3_bucket.raw_data.id
  policy = data.aws_iam_policy_document.raw_data_protect.json

  # The public access block and the ownership controls are applied before the policy.
  depends_on = [
    aws_s3_bucket_public_access_block.raw_data,
    aws_s3_bucket_ownership_controls.raw_data,
  ]
}
