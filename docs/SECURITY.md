# Security

> Purpose: Describe IAM scoping, networking, state handling and delete protection of the deployed pipeline.
> Audience: reviewers, operators
> Last reviewed: 2026-11-20

Sections:

- Ingestion storage and IAM
- Transformation IAM
- Networking
- State and delete protection

## Ingestion storage and IAM

The Fargate task role can list the bucket only within its configured S3 prefix, read, write and abort multipart uploads for objects under that prefix, and **delete only its run lock** (`<prefix>/locks/*`). A bucket-policy Deny on `s3:DeleteObject*` below `<prefix>/canister/` and `<prefix>/rest/` protects the ingested history against deletion by anyone except the principals listed in `delete_exempt_principal_arns`. The bucket is private, versioned, encrypted, ACL-free (`BucketOwnerEnforced`) and TLS-only.

## Transformation IAM

- `silver` and `silver_backfill` share one writer role: S3 object access only below `warehouse/`, `PutObject` on `silver-manifests/` for the list step, Glue read/write on the two tables.
- `quality` has its own read-only role: S3 `GetObject` below `warehouse/`, `ListBucket`, Glue `Get*`. `apps/transformation/src/quality_job.py` only loads and scans the tables and writes to `/tmp`.
- The raw bucket is read with `ListBucket` constrained to `s3:prefix = <raw_prefix>/*` and `GetObject` below `<raw_prefix>/`.
- Iceberg tables must live below the Glue database location `s3://<processed>/warehouse/`. A table created elsewhere fails with `AccessDenied` until the grant is widened deliberately.
- `quality` has no DLQ: a Lambda DLQ only receives asynchronous failures and `quality` is invoked synchronously. Failures show as a failed execution and via the `quality-errors` alarm.

Source: [OPERATIONS.md](OPERATIONS.md), section 6 "Day-2 operations".

## Networking

The Fargate task needs outbound HTTPS to the Internet Computer (`ic0.app`) and the ICPSwap REST API (`api.icpswap.com`), plus ECR, S3 and CloudWatch Logs. Defaults are private (`use_default_vpc = false`, `assign_public_ip = false`, explicit `vpc_id`/`subnet_ids`).

- **Production:** private subnets with a NAT gateway. VPC endpoints cannot replace NAT because the IC and ICPSwap APIs are on the public internet.
- **Dev profile (current deployment):** `use_default_vpc = true`, `assign_public_ip = true`. Chosen for cost: per the runbook in [OPERATIONS.md](OPERATIONS.md) a NAT gateway costs more per month than the project's whole annual budget. Tasks are exposed on public IPs; not for production.

Details: [OPERATIONS.md](OPERATIONS.md), "Networking".

## State and delete protection

- Terraform state is remote (S3 backend with native locking), configured from an untracked `backend.hcl` per root.
- The raw bucket policy denies `s3:DeleteObject*` below `<prefix>/canister/` and `<prefix>/rest/` except for `delete_exempt_principal_arns`. Teardown requires running as an exempt role ([OPERATIONS.md](OPERATIONS.md), runbook section 5).
