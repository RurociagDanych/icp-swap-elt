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

  validation {
    condition     = length(trimspace(var.environment)) > 0
    error_message = "environment must not be empty."
  }
}

variable "bucket_name" {
  type        = string
  description = "Existing S3 bucket created by the ingestion storage root (infra/ingestion/storage)."
}

variable "vpc_id" {
  type        = string
  default     = null
  description = "VPC of the task security group. Required unless use_default_vpc is true."

  validation {
    condition     = var.use_default_vpc || (var.vpc_id != null && can(regex("^vpc-[0-9a-f]{8,17}$", var.vpc_id)))
    error_message = "When use_default_vpc is false, provide vpc_id (vpc-...)."
  }
}

variable "use_default_vpc" {
  type        = bool
  default     = false
  description = "Evaluation only: use the account's default VPC and its (public) subnets instead of vpc_id/subnet_ids. Never set it in production."
}

variable "subnet_ids" {
  type        = list(string)
  default     = []
  description = "Subnets of the task ENIs. Required unless use_default_vpc is true. Private subnets need a route to the internet (NAT) because the task calls the Internet Computer and the ICPSwap API; see docs/RUNBOOK.md, \"Networking\"."

  validation {
    condition     = var.use_default_vpc || (length(var.subnet_ids) > 0 && alltrue([for s in var.subnet_ids : can(regex("^subnet-[0-9a-f]{8,17}$", s))]))
    error_message = "When use_default_vpc is false, provide at least one subnet ID (subnet-...), and only well-formed ones."
  }
}

variable "assign_public_ip" {
  type        = bool
  default     = false
  description = "Give the task ENI a public IP. Keep false in private subnets (egress through NAT/endpoints). Only needed in public subnets without NAT, such as the default VPC."

  validation {
    condition     = var.assign_public_ip || !var.use_default_vpc
    error_message = "The default VPC has no NAT: with use_default_vpc = true, assign_public_ip must be true."
  }
}

variable "container_image" {
  type        = string
  default     = ""
  description = "Optional image URI, preferably pinned by digest (repo@sha256:...). Leave empty to use the managed ECR repository with image_tag."
}

variable "image_tag" {
  type        = string
  default     = ""
  description = "Unique tag (for example the git commit SHA) of the image in the managed ECR repository. The repository is IMMUTABLE, so a tag can never be re-pushed. Required unless container_image is set."

  validation {
    condition     = var.container_image != "" || var.image_tag != ""
    error_message = "Set image_tag (a unique tag you pushed to the managed ECR repository) or container_image."
  }

  validation {
    condition     = var.image_tag != "latest"
    error_message = "image_tag must not be \"latest\": the repository is IMMUTABLE and a floating tag would defeat reproducible deploys."
  }
}

variable "container_cpu" {
  type    = number
  default = 1024
}

variable "container_memory" {
  type    = number
  default = 2048
}

variable "container_command" {
  type = list(string)
  # Scheduled runs use 'sync': backfill-if-needed then REST incremental. The
  # one-time canister archive is run manually (apps/ingestion/scripts/aws_build_push_and_run.sh --mode canister).
  default = ["node", "dist/index.js", "--mode", "sync"]
}

variable "s3_prefix" {
  type    = string
  default = "icp-swap-elt"
}

variable "log_retention_days" {
  type    = number
  default = 30
}

variable "event_bus_name" {
  type        = string
  default     = "default"
  description = "EventBridge bus that receives the IngestionRunFinished event at the end of every run (source icp-swap-elt.ingestion, with the S3 prefixes the run wrote). The transformation stack subscribes to it. Empty disables the event."
}

variable "schedule_expression" {
  type        = string
  default     = ""
  description = "Optional EventBridge Scheduler expression, for example rate(24 hour) or cron(0 * * * ? *). The transformation follows each run through the IngestionRunFinished event, so this is the only schedule the pipeline needs."
}

variable "scheduler_maximum_event_age_seconds" {
  type        = number
  default     = 3600
  description = "How long EventBridge Scheduler keeps retrying to START the task (retry policy maximum event age). This is NOT a task run-time limit: infrastructure enforces no timeout on a running task; the application's own run lock TTL (6 h) and deadlines apply."

  validation {
    condition     = var.scheduler_maximum_event_age_seconds >= 60 && var.scheduler_maximum_event_age_seconds <= 86400
    error_message = "scheduler_maximum_event_age_seconds must be between 60 and 86400 (EventBridge Scheduler limits)."
  }
}

variable "missed_run_window_hours" {
  type        = number
  default     = 26
  description = "The missed-run alarm fires when the schedule made no invocation attempt for this many hours. Must exceed the schedule interval (26 suits a daily schedule). Only used when schedule_expression is set."

  validation {
    condition     = var.missed_run_window_hours >= 2 && var.missed_run_window_hours <= 168 && var.missed_run_window_hours == floor(var.missed_run_window_hours)
    error_message = "missed_run_window_hours must be an integer between 2 and 168."
  }
}

variable "alert_email" {
  type        = string
  default     = ""
  description = "Optional email address subscribed to the failure alert SNS topic. Leave empty to skip the subscription (the topic and EventBridge rule are still created)."
}

variable "tags" {
  type    = map(string)
  default = {}
}
