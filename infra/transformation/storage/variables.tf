# infra/transformation/storage — persistent storage + catalog layer for the
# transformation (Silver) stage: the Iceberg warehouse/processed bucket, the Athena
# query-results bucket, a shared access-logs bucket, the Glue database that owns the
# Iceberg tables, and the Athena workgroup used to query them.
#
# Mirrors infra/ingestion/storage's split (storage survives a compute teardown), but is a
# separate root with its own buckets: the transformation stage never reuses or references
# any bucket from the ingestion stage other than the raw bucket it reads from (that
# reference lives in infra/transformation/compute, not here).

variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "environment" {
  type        = string
  default     = "shared"
  description = "Value of the Environment tag (resource tags and provider default_tags)."
}

variable "name_prefix" {
  type        = string
  default     = "icp-swap-elt"
  description = "Prefix for every resource this root creates (buckets, Glue database, Athena workgroup)."
}

variable "processed_bucket_name" {
  type        = string
  default     = ""
  description = "Optional S3 bucket name for the Iceberg warehouse / processed data. Leave empty to let Terraform create a deterministic name with account ID + region suffix."
}

variable "athena_results_bucket_name" {
  type        = string
  default     = ""
  description = "Optional S3 bucket name for Athena query results. Leave empty for a deterministic generated name."
}

variable "access_logs_bucket_name" {
  type        = string
  default     = ""
  description = "Optional S3 bucket name for the shared server-access-log target. Leave empty for a deterministic generated name."
}

variable "snapshot_retention_days" {
  type        = number
  default     = 7
  description = "Iceberg snapshot retention of the compute root (its snapshot_retention_days; keep both equal). Only used to validate that noncurrent S3 versions outlive it."
}

variable "processed_noncurrent_version_expiration_days" {
  type        = number
  default     = 90
  description = "Days to keep noncurrent object versions in the processed bucket. Iceberg rewrites and deletes data/metadata files; a rollback to a retained snapshot (snapshot_retention_days) may need files that have since been deleted, which only the noncurrent versions can restore. Must be at least snapshot_retention_days + 30."

  validation {
    condition     = var.processed_noncurrent_version_expiration_days >= var.snapshot_retention_days + 30
    error_message = "processed_noncurrent_version_expiration_days must be at least snapshot_retention_days + 30, otherwise files needed for a snapshot rollback can be purged before the snapshot expires."
  }
}

variable "auxiliary_noncurrent_version_expiration_days" {
  type        = number
  default     = 30
  description = "Days to keep noncurrent object versions in the Athena-results and access-logs buckets (both disposable)."
}

variable "manifest_prefix" {
  type        = string
  default     = "silver-manifests"
  description = "Key prefix in the processed bucket where the compute root's list step writes its key manifests (must equal manifest_prefix of infra/transformation/compute). Objects below it are temporary."
}

variable "manifest_expiration_days" {
  type        = number
  default     = 14
  description = "Days after which list manifests (and their noncurrent versions) are expired. Long enough to redrive a failed load execution (Step Functions allows redrive for 14 days)."
}

variable "athena_results_expiration_days" {
  type    = number
  default = 30
}

variable "access_logs_expiration_days" {
  type    = number
  default = 90
}

variable "glue_database_name" {
  type    = string
  default = "icp_swap_elt"
}

variable "athena_workgroup_name" {
  type    = string
  default = "icp-swap-elt"
}

variable "athena_bytes_scanned_cutoff_bytes" {
  type        = number
  default     = 10737418240 # 10 GiB
  description = "Per-query Athena bytes-scanned cap enforced by the workgroup, as a cost guard against an unbounded ad hoc query."
}

variable "tags" {
  type    = map(string)
  default = {}
}
