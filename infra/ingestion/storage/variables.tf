variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "name_prefix" {
  type    = string
  default = "icp-swap-elt-ingestion"
}

variable "environment" {
  type        = string
  default     = "shared"
  description = "Value of the Environment tag (resource tags and provider default_tags). Set it here, not through the tags map."
}

variable "s3_prefix" {
  type        = string
  default     = "icp-swap-elt"
  description = "Key prefix the ingestion task writes under. Must equal s3_prefix of infra/ingestion/compute; the delete protection of <prefix>/canister/ and <prefix>/rest/ is built from it."
}

variable "delete_exempt_principal_arns" {
  type        = list(string)
  default     = []
  description = "IAM role/user ARNs (wildcards allowed) exempt from the bucket-policy Deny on s3:DeleteObject* below <s3_prefix>/canister/ and <s3_prefix>/rest/. Set it to your data-admin role to allow deliberate deletion and the full teardown (docs/RUNBOOK.md, section 5). Empty means nobody can delete ingested data until the policy is changed."

  validation {
    condition     = alltrue([for a in var.delete_exempt_principal_arns : startswith(a, "arn:")])
    error_message = "delete_exempt_principal_arns must contain IAM ARNs (arn:...)."
  }
}

variable "access_log_bucket_name" {
  type        = string
  default     = ""
  description = "Optional existing bucket that receives S3 server access logs of the raw bucket. Empty (default) disables access logging."
}

variable "bucket_name" {
  type        = string
  default     = ""
  description = "Optional S3 bucket name. Leave empty to let Terraform create a deterministic name with account ID suffix."
}

variable "noncurrent_version_expiration_days" {
  type        = number
  default     = 30
  description = "Days to keep noncurrent object versions before expiring them. The bucket is versioned, so without this rule storage grows forever."
}

variable "tags" {
  type    = map(string)
  default = {}
}
