# infra/transformation/compute — compute + orchestration for the transformation
# (Silver) stage: one container image serves three Lambda functions (the small incremental transformation
# `silver` and the large one-time-reload `silver_backfill`, both entrypoint silver_job.handler; and the
# the dbt data-quality checks, entrypoint quality_job.handler), fronted by an SQS dead-letter
# queue, least-privilege IAM, an EventBridge rule on IngestionRunFinished, and
# CloudWatch-alarm-driven SNS failure alerting — the same
# building blocks infra/ingestion/compute uses for the ingestion stage, adapted from ECS/
# Fargate to Lambda because this stage is short, bursty, and event-driven rather than a
# long-running daily job.
#
# Design invariants binding on anything built from this root:
#   1. icp_swap_events is only ever written by the registered transformation Lambdas (modes: list,
#      incremental, backfill_file, token_metadata; `silver_backfill` runs backfill_file for the
#      reload state machine). Dedup is by event_id against the table itself (no side index —
#      apps/transformation/README.md). Table maintenance (compaction, snapshot expiry, orphan
#      files) is done by Glue Data Catalog table optimizers, not by any function of ours; the
#      Lambda role has no glue:CreateTable/DeleteTable grant beyond what the load needs.
#   2. token_dim (one current-value row per ledger; latest observation wins under a single
#      `as_of` column, conflicts resolved deterministically and reported) is an
#      application-code invariant; its 6-column schema is created by the transformation, never by Terraform.
#   3. The hidden partition transform on icp_swap_events is YEAR, not DAY (see
#      apps/transformation/src/silver_persistence.py) — chosen to avoid the
#      small-files problem a daily partition produces at this table's write volume.
#
# The raw bucket is an EXPLICIT INPUT, never recreated: `raw_bucket_name` is a plain
# variable pointing at the pre-existing bucket managed by infra/ingestion/storage (the
# ingestion stage). This root creates no S3 bucket of its own.

variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "aws_account_id" {
  type        = string
  description = "AWS account ID the resources will be created in. Provided explicitly rather than discovered via a data \"aws_caller_identity\" lookup, so that no Terraform command in this root ever makes an sts:GetCallerIdentity call. Used only to build exact-account Glue/Athena ARNs for least-privilege IAM."

  validation {
    condition     = can(regex("^[0-9]{12}$", var.aws_account_id))
    error_message = "aws_account_id must be a 12-digit AWS account ID."
  }
}

variable "name_prefix" {
  type    = string
  default = "icp-swap-elt-transformation"
}

variable "environment" {
  type        = string
  default     = "shared"
  description = "Value of the Environment tag (resource tags and provider default_tags). The Stage tag, not this one, is what the AWS Budgets cost filter uses."

  validation {
    condition     = length(trimspace(var.environment)) > 0
    error_message = "environment must not be empty."
  }
}

# --- Preserved raw storage (input only — never recreated) --------------------------------------

variable "raw_bucket_name" {
  type        = string
  description = "Existing raw/source bucket owned by the ingestion stage (infra/ingestion/storage). Not managed by this root: no aws_s3_bucket resource, no data \"aws_s3_bucket\" lookup either (that would be an AWS API call/account discovery at plan time) — just a plain ARN-building input."

  validation {
    condition     = !startswith(var.raw_bucket_name, "REPLACE")
    error_message = "Set raw_bucket_name to the existing raw/source bucket name before any real plan."
  }
}

variable "raw_prefix" {
  type        = string
  default     = "icp-swap-elt"
  description = "Must match infra/ingestion/compute's s3_prefix (default \"icp-swap-elt\") - this is where the transformation Lambdas list and read raw CSV objects from. If you change it in one root, change it in the other."
}

# --- Inputs from infra/transformation/storage -------------------------------------------

variable "processed_bucket_name" {
  type = string
}

variable "glue_database_name" {
  type = string
}

variable "glue_table_name" {
  type    = string
  default = "icp_swap_events"
}

variable "token_table_name" {
  type    = string
  default = "token_dim"
}

# --- transformation Lambdas (container image; `silver` small / `silver_backfill` large) ---------------------------

variable "container_image" {
  type        = string
  default     = ""
  description = "Optional fully-qualified image URI, preferably pinned by digest (repo@sha256:...). Leave empty to use the managed ECR repository with image_tag - a reference only; this root never builds or pushes an image."
}

variable "image_tag" {
  type        = string
  default     = ""
  description = "Unique tag (for example the git commit SHA) of the image in the managed ECR repository. The repository is IMMUTABLE, so a tag can never be re-pushed; deploying a new image means pushing a new tag and applying with it. Required unless container_image is set."

  validation {
    condition     = var.container_image != "" || var.image_tag != ""
    error_message = "Set image_tag (a unique tag you pushed to the managed ECR repository) or container_image."
  }

  validation {
    condition     = var.image_tag != "latest"
    error_message = "image_tag must not be \"latest\": the repository is IMMUTABLE and a floating tag would defeat reproducible deploys."
  }
}

variable "silver_memory_size_mb" {
  type        = number
  default     = 2048
  description = "Memory of the small `silver` function (daily incremental, list, token_metadata). A realistic daily file is a few MB; the historical reload's large objects run on silver_backfill_memory_size_mb instead."
}

variable "silver_timeout_seconds" {
  type    = number
  default = 300
}

variable "silver_reserved_concurrency" {
  type        = number
  default     = 1
  description = "Reserved (and maximum) concurrent executions of the small `silver` function. 1 serialises incremental/ad-hoc writes so they cannot race each other's Iceberg commits. -1 leaves it unreserved. Reserved concurrency is carved out of the account's concurrency quota (AWS keeps at least 10 unreserved), so a new account with a quota of 10 cannot reserve any: raise the quota or set -1."

  validation {
    condition     = var.silver_reserved_concurrency == -1 || var.silver_reserved_concurrency >= 1
    error_message = "silver_reserved_concurrency must be -1 (unreserved) or at least 1."
  }
}

variable "silver_backfill_reserved_concurrency" {
  type        = number
  default     = 1
  description = "Reserved (and maximum) concurrent executions of `silver_backfill`. Caps parallel Iceberg commits even when several load executions overlap (each execution's Map is only bounded by backfill_max_concurrency on its own). Must be at least backfill_max_concurrency (checked by a precondition) or -1 for unreserved. Same account-quota caveat as silver_reserved_concurrency."

  validation {
    condition     = var.silver_backfill_reserved_concurrency == -1 || var.silver_backfill_reserved_concurrency >= 1
    error_message = "silver_backfill_reserved_concurrency must be -1 (unreserved) or at least 1."
  }
}

variable "silver_backfill_memory_size_mb" {
  type        = number
  default     = 10240
  description = "Memory of the dedicated `silver_backfill` function (backfill state machine only). backfill_file loads a whole source CSV fully in memory; 10240 MB is the AWS Lambda maximum and leaves wide margin over the in-memory footprint of the largest (~103 MB / 200k-row) object. Lambda memory is per FUNCTION, hence a second function. NOTE: new/low-usage accounts can be capped at 3008 MB until a Service Quotas increase is granted - that only surfaces at apply time."

  validation {
    condition     = var.silver_backfill_memory_size_mb >= 128 && var.silver_backfill_memory_size_mb <= 10240
    error_message = "silver_backfill_memory_size_mb must be between 128 and 10240 (the AWS Lambda memory range)."
  }
}

variable "silver_backfill_timeout_seconds" {
  type        = number
  default     = 900
  description = "Timeout of the `silver_backfill` function; 900 s is the AWS Lambda maximum."

  validation {
    condition     = var.silver_backfill_timeout_seconds >= 1 && var.silver_backfill_timeout_seconds <= 900
    error_message = "silver_backfill_timeout_seconds must be between 1 and 900 (the AWS Lambda timeout range)."
  }
}

variable "silver_target_generation" {
  type        = string
  description = "Permanent contract-version tag for the target table (apps/transformation/src/silver_persistence.py): stored as the table's silver.target.generation property and checked on every load. Required, deliberately without a default: it must be chosen once per deployment (for example g20261101) and never change once any row has been written under it - a different value is refused as a generation mismatch."

  validation {
    condition     = can(regex("^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$", var.silver_target_generation)) && !contains([".", ".."], var.silver_target_generation)
    error_message = "silver_target_generation must be a safe path component: letters, digits, '.', '_' or '-', starting with a letter or digit, at most 128 characters (the same rule apps/transformation enforces at runtime)."
  }
}

variable "silver_max_source_bytes" {
  type        = number
  description = "Guard rail on a single source object's size before an transformation Lambda will process it. Sized from the largest known source objects: ~103 MB (largest canister-archive CSV) and ~33 MB (largest daily REST-incremental CSV). 200 MiB gives margin over both."
  default     = 209715200 # 200 MiB
}

variable "silver_max_source_rows" {
  type        = number
  description = "Guard rail on a single source object's row count. The largest known source object has 200,000 rows (largest canister-archive CSV). 300,000 gives a 1.5x margin."
  default     = 300000
}

variable "silver_deadline_seconds" {
  type        = number
  description = "Soft processing deadline inside the small `silver` function; must stay strictly below silver_timeout_seconds (enforced by a precondition) so it trips before Lambda kills the invocation. A daily incremental file is a few MB, so 240s leaves wide headroom."
  default     = 240
}

variable "silver_backfill_deadline_seconds" {
  type        = number
  description = "Soft processing deadline inside the `silver_backfill` function; must stay strictly below silver_backfill_timeout_seconds (enforced by a precondition) so it trips before Lambda kills the invocation."
  default     = 840
}

# Bounds of the event_id lookup scan (apps/transformation/src/silver_persistence.py). The code
# defaults (64 files / 250,000 rows / 128 MiB) are sized for the small incremental function. A
# 200k-row object's lookup also reads the neighbouring objects that share its boundary event days,
# and an exact replay (e.g. a Step Functions retry after an acknowledgement loss) of one such object
# can select ~400k rows, so the backfill function gets wider caps. The scan
# is streamed batch by batch, so these caps bound work, not resident memory.
variable "silver_backfill_max_selected_files" {
  type        = number
  default     = 64
  description = "SILVER_MAX_SELECTED_FILES for the silver_backfill function: max data files one object's event_id lookup may select (fail-closed)."
}

variable "silver_backfill_max_selected_rows" {
  type        = number
  default     = 1000000
  description = "SILVER_MAX_SELECTED_ROWS for the silver_backfill function: max physical rows one object's event_id lookup may select (fail-closed). Sized for a ~200k-row object plus the neighbouring objects sharing its boundary event days."
}

variable "silver_backfill_max_selected_bytes" {
  type        = number
  default     = 536870912 # 512 MiB
  description = "SILVER_MAX_SELECTED_BYTES for the silver_backfill function: max parquet bytes one object's event_id lookup may select (fail-closed)."
}

variable "token_metadata_max_response_bytes" {
  type    = number
  default = 4194304 # 4 MiB, well under the 6 MiB Lambda synchronous-invoke response limit
}

# --- Quality Lambda (same image, different entrypoint: dbt tests on DuckDB; last step of both state machines) ---

variable "quality_memory_size_mb" {
  type        = number
  default     = 3008
  description = "Memory of the data-quality Lambda. The needed columns of the fact table are loaded into DuckDB, so size it for the table."
}

variable "quality_ephemeral_storage_mb" {
  type    = number
  default = 2048
}

variable "quality_timeout_seconds" {
  type    = number
  default = 900
}

variable "quality_deadline_seconds" {
  type        = number
  default     = 780
  description = "Soft deadline of the scan inside the handler; must stay below quality_timeout_seconds."
}

# --- Glue table optimizers (managed Iceberg maintenance) -----------------------------------

variable "enable_table_optimizers" {
  type        = bool
  default     = false
  description = "Create the Glue compaction / snapshot-retention / orphan-file-deletion optimizers. Leave false on the first apply: the tables only exist after the first load, and an optimizer needs an existing table."
}

variable "compaction_min_input_files" {
  type        = number
  default     = 10
  description = "Compaction runs when a partition holds at least this many data files."
}

variable "snapshots_to_retain" {
  type        = number
  default     = 1
  description = "Newest snapshots the snapshot retention always keeps (SILVER_SNAPSHOTS_TO_RETAIN for the in-job retention in mode=compact; also used by the Glue retention optimizer if enabled)."

  validation {
    condition     = var.snapshots_to_retain >= 1
    error_message = "snapshots_to_retain must be at least 1."
  }
}

variable "orphan_file_retention_days" {
  type        = number
  default     = 3
  description = "Grace period in days before an unreferenced file of token_dim is deleted (SILVER_ORPHAN_GRACE_DAYS for the in-job orphan cleanup in mode=compact; also used by the Glue optimizer if enabled). Files of a writer that has not committed yet look orphaned, so it must be at least 1 day."

  validation {
    condition     = var.orphan_file_retention_days >= 1
    error_message = "orphan_file_retention_days must be at least 1 (uncommitted writers' files look orphaned)."
  }
}

variable "optimizer_run_rate_hours" {
  type    = number
  default = 24
}

variable "snapshot_retention_days" {
  type        = number
  default     = 7
  description = "Iceberg snapshot/time-travel retention in days (SILVER_SNAPSHOT_RETENTION_DAYS for the in-job retention in mode=compact; also used by the Glue retention optimizer if enabled). Must match infra/transformation/storage's snapshot_retention_days and stay at least 30 days below its processed_noncurrent_version_expiration_days (90): a rollback to a retained snapshot may need deleted files back from noncurrent S3 versions."

  validation {
    condition     = var.snapshot_retention_days >= 1 && var.snapshot_retention_days <= 60
    error_message = "snapshot_retention_days must be between 1 and 60 (90-day noncurrent-version expiry minus 30 days of rollback headroom)."
  }
}

variable "log_retention_days" {
  type    = number
  default = 30
}

# --- Orchestration --------------------------------------------------------------------------

variable "trigger_on_ingestion_events" {
  type        = bool
  default     = true
  description = "Start the load state machine whenever ingestion publishes IngestionRunFinished for raw_bucket_name / raw_prefix (event contract: source icp-swap-elt.ingestion, see apps/ingestion/src/lib/events.ts)."
}

variable "event_bus_name" {
  type        = string
  default     = "default"
  description = "EventBridge bus that carries IngestionRunFinished; must equal event_bus_name of infra/ingestion/compute."
}

variable "alert_email" {
  type        = string
  default     = ""
  description = "Optional. Leave empty to skip the SNS email subscription (topic + alarms still created)."
}

variable "dlq_retention_days" {
  type    = number
  default = 14
}

variable "backfill_max_concurrency" {
  type        = number
  default     = 1
  description = "Map-state concurrency cap for the backfill Step Function's per-file processing on silver_backfill. Bounds memory pressure and Glue-catalog commit contention, not raw throughput. In Step Functions a MaxConcurrency of 0 means UNLIMITED, hence the lower bound of 1."

  validation {
    condition     = var.backfill_max_concurrency >= 1 && var.backfill_max_concurrency == floor(var.backfill_max_concurrency)
    error_message = "backfill_max_concurrency must be an integer >= 1 (0 would mean unlimited fan-out in Step Functions)."
  }
}

variable "manifest_prefix" {
  type        = string
  default     = "silver-manifests"
  description = "Key prefix (no leading or trailing slash) in the processed bucket under which the list step writes its JSON key manifests. Must equal manifest_prefix of infra/transformation/storage, which expires objects below it."

  validation {
    condition     = can(regex("^[A-Za-z0-9][A-Za-z0-9._/-]*[A-Za-z0-9]$", var.manifest_prefix)) && var.manifest_prefix != "warehouse"
    error_message = "manifest_prefix must be a plain key prefix without leading/trailing slash and must not be the warehouse prefix."
  }
}

variable "monthly_budget_usd" {
  type        = number
  default     = 100
  description = "Monthly AWS Budgets cost guardrail for everything tagged Stage=transformation. Alerts at 80% actual spend and 100% forecasted spend via the alerts SNS topic."
}

variable "tags" {
  type    = map(string)
  default = {}
}

# In-job compaction of icp_swap_events (apps/transformation/src/fact_compaction.py), run by the
# CompactTable step after the backfill Map and, above a threshold, by the per-object path.
variable "silver_backfill_compact_target_file_rows" {
  type        = number
  default     = 250000
  description = "SILVER_COMPACT_TARGET_FILE_ROWS: rows per compacted file; files with fewer rows are 'small' and compaction input. Output files are sorted by event time, so a lookup selects only the files whose days it touches."

  validation {
    condition     = var.silver_backfill_compact_target_file_rows >= 1
    error_message = "silver_backfill_compact_target_file_rows must be positive."
  }
}

variable "silver_backfill_compact_min_input_files" {
  type        = number
  default     = 4
  description = "SILVER_COMPACT_MIN_INPUT_FILES: a year partition is compacted once it holds at least this many small files (>= 2)."

  validation {
    condition     = var.silver_backfill_compact_min_input_files >= 2
    error_message = "silver_backfill_compact_min_input_files must be at least 2."
  }
}

variable "silver_backfill_compact_max_input_files" {
  type        = number
  default     = 256
  description = "SILVER_COMPACT_MAX_INPUT_FILES: max files one compaction transaction rewrites; a larger partition is compacted in bounded slices."
}

variable "silver_backfill_compact_max_input_rows" {
  type        = number
  default     = 2000000
  description = "SILVER_COMPACT_MAX_INPUT_ROWS: max rows one compaction transaction reads (held in memory about 3-4 times: input, sorted output, read-back verification)."
}

variable "silver_backfill_compact_max_input_bytes" {
  type        = number
  default     = 536870912
  description = "SILVER_COMPACT_MAX_INPUT_BYTES: max Parquet bytes one compaction transaction reads."
}

variable "silver_backfill_compact_trigger_selected_files" {
  type        = number
  default     = 48
  description = "SILVER_COMPACT_TRIGGER_SELECTED_FILES: when one object's event_id lookup would select more files than this, the object's year partitions are compacted first (cheap manifest-only check otherwise). Must stay below silver_backfill_max_selected_files (precondition)."

  validation {
    condition     = var.silver_backfill_compact_trigger_selected_files >= 1
    error_message = "silver_backfill_compact_trigger_selected_files must be positive."
  }
}

variable "maintenance_incomplete_max_attempts" {
  type        = number
  default     = 5
  description = "Retry attempts of the CompactTable step on MaintenanceIncomplete (bounded progress committed, work left); each attempt resumes where the previous one stopped."

  validation {
    condition     = var.maintenance_incomplete_max_attempts >= 1 && var.maintenance_incomplete_max_attempts <= 99
    error_message = "maintenance_incomplete_max_attempts must be between 1 and 99."
  }
}

variable "compact_partition_file_threshold" {
  type        = number
  default     = 7
  description = "SILVER_COMPACT_PARTITION_FILE_THRESHOLD on both functions: mode=compact compacts a year partition only when it holds MORE than this many data files (0 = whenever silver_backfill_compact_min_input_files small files exist). Default 7: a year partition is compacted once it accumulates more than 7 small files, so the table does not drift to dozens of files per year."

  validation {
    condition     = var.compact_partition_file_threshold >= 0
    error_message = "compact_partition_file_threshold must not be negative."
  }
}

variable "silver_compact_max_input_rows" {
  type        = number
  default     = 500000
  description = "SILVER_COMPACT_MAX_INPUT_ROWS for the 2048 MB silver function (daily maintenance); larger partitions are compacted in slices."
}

variable "silver_compact_max_input_bytes" {
  type        = number
  default     = 134217728
  description = "SILVER_COMPACT_MAX_INPUT_BYTES for the 2048 MB silver function (daily maintenance)."
}
