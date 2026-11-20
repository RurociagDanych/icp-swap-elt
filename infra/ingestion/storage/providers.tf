terraform {
  # # >= 1.10 is required for the S3 backend's native state locking (use_lockfile); `terraform test`
  # # (mock providers) needs >= 1.7. CI pins an exact version, see .github/workflows/ci.yml.
  required_version = ">= 1.10.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.58"
    }
  }

  # # Partial on purpose: bucket and region come from an untracked backend.hcl (init -backend-config).
  # # Native S3 locking, no DynamoDB table. The key is fixed so the four roots can share one bucket.
  backend "s3" {
    key          = "icp-swap-elt/ingestion/storage/terraform.tfstate"
    encrypt      = true
    use_lockfile = true
  }
}

provider "aws" {
  region = var.aws_region

  # # Applied to every taggable resource, including ones without an explicit tags block.
  # # Stage is the AWS Billing cost-allocation tag the transformation budget filters on: it must
  # # be activated in the Billing console (docs/OPERATIONS.md, "Cost-allocation tag").
  default_tags {
    tags = {
      Project     = "icp-swap-elt"
      Stage       = "ingestion"
      ManagedBy   = "terraform"
      Environment = var.environment
    }
  }
}
