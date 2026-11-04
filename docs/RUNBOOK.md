# Runbook

Operational guide for the ICPSwap ELT pipeline: running the ingestion stage locally, testing, deploying both stages to AWS with Terraform, day-2 operations, and teardown. Sections 1–5 cover `apps/ingestion` (and its Terraform under `infra/ingestion/`); section 6 covers `apps/transformation` (`infra/transformation/`); section 7 describes how the two stages are wired together (an EventBridge event, not Terraform references).

## Prerequisites

| Tool | Needed for | Version |
| --- | --- | --- |
| Node.js + npm | local runs, tests, the helper script | Node 22+ |
| Terraform | AWS infrastructure | >= 1.10 (S3 backend native locking, `use_lockfile`); CI pins 1.11.4. `terraform test` with mock providers needs >= 1.7, which 1.10 satisfies |
| AWS CLI v2 | deploy + verification | any recent |
| Docker | building/pushing the container images | any recent |
| Python 3.12 | transformation tests (`make venv`) | 3.12 |

**Version policy.** All four roots declare `required_version = ">= 1.10.0"` (native S3 state locking needs 1.10; it is experimental in 1.10 and generally available from 1.11, which CI uses). Provider constraints are deliberately minor-range pins (`hashicorp/aws ~> 6.58`: any 6.x from 6.58, never 7.x); the exact provider build is fixed by each root's committed `.terraform.lock.hcl`. Bump the provider on purpose: `terraform init -upgrade`, review the plan, commit the lock file. Every root also sets provider `default_tags` (`Project`, `Stage`, `ManagedBy`, `Environment`).

AWS commands use the normal credential chain (`AWS_PROFILE`, env vars, or SSO). The IAM principal applying Terraform needs rights to manage S3, ECR, ECS, IAM, CloudWatch, EventBridge, and SNS; the transformation roots additionally need Lambda, Step Functions, SQS, Glue, Athena, and AWS Budgets.

---

## 1. Run locally

Local mode needs no AWS account — output goes to `./out/` (relative to `apps/ingestion`).

```bash
cd apps/ingestion
npm install

# canister: one-time deep-history archive of every storage canister
# (long; resumable if interrupted). Sets canisterArchiveComplete + canisterMaxTxTime.
npm run dev -- --mode canister --page-size 1000 --concurrency 5

# backfill: REST history newest->oldest down to the canister seam (resumable)
npm run dev -- --mode backfill

# incremental: new REST transactions since the stored watermark
npm run dev -- --mode incremental

# sync (default): backfill-if-needed then incremental; never runs canister
npm run dev -- --mode sync
```

Or run the compiled build (what the container runs):

```bash
npm run build
npm start -- --mode sync
```

### What you get in `./out/`

| File | Meaning |
| --- | --- |
| `NNNN_<canisterId>.csv` | canister-mode swap rows (28-column canister CSV) |
| `transactions_backfill_<endSnapshot>_p<NNNNNN>.csv` (one per page-batch) / `transactions_incremental_<runId>.csv` | REST rows (REST schema) |
| `manifest_<mode>_<runId>.json` | per-file rows/bytes/sha256 + totals for the run |
| `state.json` | resume state (canister seam, REST backfill cursor + incremental watermark) |
| `ingestion.log` | the same JSON log lines that went to stdout |
| `run.lock` | present only while a run is active |

### Useful flags

| Flag | Default | Notes |
| --- | --- | --- |
| `--mode` | `sync` | `sync`, `canister` (alias `full`), `backfill`, `incremental` |
| `--page-size` | `1000` | 1..1000, canister mode |
| `--concurrency` | `5` | 1..20, canister mode only |
| `--overlap` | `50` | canister-mode re-fetch window |
| `--rest-page-size` | `100` | 1..100 (REST API caps at 100) |
| `--backfill-overlap-ms` | `3600000` | overlap below the canister seam (ms) |
| `--incremental-overlap-ms` | `300000` | REST incremental re-fetch window (ms) |
| `--backfill-floor` | — | explicit floor (epoch ms) if `canisterMaxTxTime` is absent |
| `--out-dir` / `--state-file` / `--log-file` | under `./out` | local paths |

### Local gotchas

- **Interrupted canister archive**: just rerun the same command — completed canisters are skipped via `state.json`.
- **Interrupted backfill**: rerun `--mode backfill` — it resumes from the saved page cursor against the same `end` snapshot.
- **Cutover check**: once `state.json` shows `canisterArchiveComplete: true` and `rest.backfillComplete: true`, scheduled `sync` runs only the REST incremental.
- **Stale lock after a kill**: if a run was killed (Ctrl-C is handled, `kill -9` is not), `./out/run.lock` may remain. A new run within 6 hours will log `another run holds the lock` and exit. Delete it manually: `rm ./out/run.lock`.
- **Fresh start**: `rm -rf ./out` wipes data, state, and lock.

---

## 2. Run tests

```bash
cd apps/ingestion
npm test          # (in apps/ingestion) node:test suites in tests/ (config, csv, logger, state, storageTarget, restCsv, restClient, restPaging, restRuns)
npm run build     # strict TypeScript compile — treat failures as test failures
```

Transformation (Python, fully offline — local SQLite Iceberg catalog, no AWS calls):

```bash
cd apps/transformation
make venv PYTHON=python3.12
make test-python
```

Terraform checks (what CI also enforces; Terraform >= 1.10, CI pins 1.11.4):

```bash
for root in infra/ingestion/storage infra/ingestion/compute infra/transformation/storage infra/transformation/compute; do
  terraform -chdir=$root fmt -check -recursive
  terraform -chdir=$root init -backend=false -input=false   # -backend=false: no state bucket or credentials needed
  terraform -chdir=$root validate
  terraform -chdir=$root test                               # offline, mock provider, no credentials
done
```

The `tests/*.tftest.hcl` files of each root use a mock provider and never call AWS. They pin IAM scoping (including the SNS topic policies), concurrency, the load state machine definition, ECR settings and input validation. Where a value is only known after apply (ARNs of created resources), the run uses `command = apply` against the mock provider, which is still entirely in memory. What they **cannot** show is real AWS behaviour: that a policy is accepted and has the intended effect, that the account's Lambda concurrency quota permits the reservation, that a Distributed Map reads the manifest, and so on. Those are verified by the first real deployment (steps below).

All of the above runs automatically in GitHub Actions on every push/PR (`.github/workflows/ci.yml`).

---

## 3. Deploy to AWS

### Step 3.0 — remote state (once per account, before any `terraform init`)

Every root stores its state in S3 with native lockfile locking (`use_lockfile = true`, no DynamoDB table; Terraform >= 1.10). The `backend "s3"` block in each `providers.tf` is deliberately **partial**: the state key is fixed per root, bucket and region come from an untracked `backend.hcl`.

| Root | State key |
| --- | --- |
| `infra/ingestion/storage` | `icp-swap-elt/ingestion/storage/terraform.tfstate` |
| `infra/ingestion/compute` | `icp-swap-elt/ingestion/compute/terraform.tfstate` |
| `infra/transformation/storage` | `icp-swap-elt/transformation/storage/terraform.tfstate` |
| `infra/transformation/compute` | `icp-swap-elt/transformation/compute/terraform.tfstate` |

1. Create the state bucket **outside** these roots (it must outlive every destroy here), in the target region, with versioning, default encryption and Block Public Access, and grant the applying principal `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject` (the lock file `<key>.tflock` is created and deleted next to the state), `s3:ListBucket` on it. Prefer a dedicated bucket in the same account or a central tooling account.
2. In each root: `cp backend.hcl.example backend.hcl`, fill in `bucket` and `region` (`backend.hcl` is gitignored via `infra/.gitignore`), then

   ```bash
   terraform init -backend-config=backend.hcl
   ```

3. A root that already has local state: run the same command with `-migrate-state`, answer `yes`, check `terraform state list`, then delete the local `terraform.tfstate*` files.

State contains resource attributes; never commit it or `*.tfvars`. `terraform init -backend=false` (CI and offline validation) ignores the backend block.

### Deploying

The ingestion infrastructure is two independent Terraform roots, applied in order:

1. `infra/ingestion/storage` — the persistent raw-data bucket (survives compute teardown).
2. `infra/ingestion/compute` — ECR, ECS/Fargate, IAM, CloudWatch, scheduler, alerting.

### Step 3.1 — storage root

```bash
cd infra/ingestion/storage
cp terraform.tfvars.example terraform.tfvars
# edit terraform.tfvars:
#   aws_region                          - target region
#   bucket_name                         - leave "" for an auto-generated unique name
#   noncurrent_version_expiration_days  - default 30
#   environment                         - Environment tag value
#   s3_prefix                           - must equal infra/ingestion/compute's s3_prefix
#   delete_exempt_principal_arns        - your data-admin role (see "Delete protection" below)
terraform init -backend-config=backend.hcl
terraform apply
terraform output bucket_name        # <- needed in the next step
```

**Delete protection (raw data).** The bucket policy denies `s3:DeleteObject*` on `<s3_prefix>/canister/*` and `<s3_prefix>/rest/*` for everybody except the principals in `delete_exempt_principal_arns` (matched against `aws:PrincipalArn`, i.e. the role ARN, not the session ARN). The ingestion task role can only delete `<s3_prefix>/locks/*`. Lifecycle expiry of noncurrent versions is unaffected (S3 performs it itself). With an empty exemption list nobody can delete ingested data, including during teardown (section 5), until you set the variable and apply.

**Lock-path dependency.** The task role's `s3:DeleteObject` is limited to `<s3_prefix>/locks/*` because `apps/ingestion/src/lib/storageTarget.ts` keeps its run lock at `<prefix>/locks/run.lock` (`lockKey`) and that is the only object the application deletes (`grep DeleteObject apps/ingestion/src`: only `releaseRunLock`). If the lock key ever moves out of `<prefix>/locks/`, releasing the lock fails with a warning and the lock leaks until its 6-hour TTL; change the IAM statement `DeleteRunLockOnly` together with it.

### Step 3.2 — compute root

```bash
cd ../compute
cp terraform.tfvars.example terraform.tfvars
# edit terraform.tfvars:
#   bucket_name         - REQUIRED: the output from step 3.1
#   aws_region          - same region as storage
#   vpc_id, subnet_ids  - REQUIRED (use_default_vpc and assign_public_ip now default to false); see "Networking"
#   image_tag           - REQUIRED unless container_image is set: the unique tag you push in step 3.3
#   environment         - Environment tag value
#   schedule_expression - e.g. "cron(0 8 * * ? *)" (08:00 UTC daily); set "" to disable scheduled runs
#   alert_email         - your email for failure alerts (recommended)
#   container_command   - default runs 'sync' mode (REST backfill-if-needed + incremental)
#   event_bus_name      - bus for the IngestionRunFinished event (default "default"; "" disables it)
terraform init -backend-config=backend.hcl
terraform apply
```

If you set `alert_email`, **confirm the subscription** via the email AWS sends, or alerts go nowhere.

### Step 3.3 — build, push, and first run

The ECR repository is **IMMUTABLE**: a tag can be pushed once and never overwritten, so every build needs a unique tag (the git commit SHA works well) and Terraform points the task definition at it via `image_tag` (or at a digest via `container_image = "<repo>@sha256:..."`). There is no `latest` any more; `image_tag = "latest"` is rejected.

The helper script reads region, ECR URL, cluster, task definition and networking from the compute root's Terraform outputs. Always pass `--tag`:

```bash
cd ../../..   # repo root
TAG=$(git rev-parse --short=12 HEAD)

# 1. push the image only
apps/ingestion/scripts/aws_build_push_and_run.sh --build-only --tag "$TAG"

# 2. point the task definition at it
#    set image_tag = "<TAG>" in infra/ingestion/compute/terraform.tfvars, then
terraform -chdir=infra/ingestion/compute apply

# 3. one-time deep-history archive of all canisters (run this first)
apps/ingestion/scripts/aws_build_push_and_run.sh --mode canister --tag "$TAG"
```

> **Known gap (ingestion-owned script).** `aws_build_push_and_run.sh` defaults to `--tag latest`, documents `latest` as the safe path and prints a warning that the task definition "still references :latest". With an immutable repository a second push of `latest` fails, so its default must change to a unique tag (see the review's DELEGATED-OUT list). Until then, always pass `--tag`. The script's `run-task` uses the task definition from Terraform, so run step 2 before step 3, otherwise the task starts the previous image.

After `--mode canister` completes (`canisterArchiveComplete: true` in `state.json`), scheduled `sync` runs handle everything from the REST API and never re-touch the canisters.

### Networking

The Fargate task needs outbound HTTPS to the Internet Computer (`ic0.app`) and the ICPSwap REST API (`api.icpswap.com`), plus ECR, S3 and CloudWatch Logs. Defaults are private: `use_default_vpc = false`, `assign_public_ip = false`, explicit `vpc_id` and `subnet_ids` (validated).

- **Private subnets (recommended):** a NAT gateway route is required, because the Internet Computer and the ICPSwap API are on the public internet; VPC endpoints cannot replace it.
- **VPC endpoints (optional, reduce NAT traffic and cost for AWS-internal traffic):** an S3 **gateway** endpoint (free; image layers and the data bucket traffic), ECR **interface** endpoints `ecr.api` and `ecr.dkr` (image pull), and a `logs` interface endpoint (awslogs). Their security groups must allow 443 from the task security group (`task_security_group_id` output). The task security group allows egress 443 and DNS to `0.0.0.0/0`.
- **Public subnets without NAT:** set `assign_public_ip = true`. `use_default_vpc = true` (evaluation only, requires `assign_public_ip = true`) uses the account's default VPC and its public subnets.
- **Dev profile (what the current deployment uses):** `use_default_vpc = true` and `assign_public_ip = true`. The reason is cost: a NAT gateway costs more per month than the project's whole annual budget. This profile exposes the tasks on public IPs and must not be used for production. For production, use private subnets with NAT as above.

### Step 3.4 — verify the deployment

```bash
REGION=$(terraform -chdir=infra/ingestion/compute output -raw aws_region)
BUCKET=$(terraform -chdir=infra/ingestion/compute output -raw bucket_name)
LOG_GROUP=$(terraform -chdir=infra/ingestion/compute output -raw cloudwatch_log_group_name)

# data landed? newest entries should show <runId>/...csv and manifest_<mode>.json
aws s3 ls "s3://${BUCKET}/icp-swap-elt/canister/" --recursive | tail -5
aws s3 ls "s3://${BUCKET}/icp-swap-elt/rest/backfill/" --recursive | tail -5
aws s3 ls "s3://${BUCKET}/icp-swap-elt/rest/incremental/" --recursive | tail -5

# state object exists?
aws s3 ls "s3://${BUCKET}/icp-swap-elt/state/"

# logs (JSON lines; filter in CloudWatch Logs Insights with: fields ts, level, msg)
aws logs tail "$LOG_GROUP" --region "$REGION" --since 1h
```

A healthy run ends with a `full load done`, `backfill done`, or `rest incremental done` log line, and the run's `manifest_<mode>.json` totals match the CSV files next to it (a `sync` run has `manifest_backfill.json` and `manifest_incremental.json`).

---

## 4. Day-2 operations

- **A run failed**: you get an SNS email (if `alert_email` is set). Check CloudWatch logs for the `error` line; the run's prefix will have no `manifest_<mode>.json` — that's the marker of an incomplete run. Re-running is safe: state was only advanced for canisters that completed.
- **Stale S3 lock** (task was killed mid-run): new runs exit with `another run holds the lock` until the 6-hour TTL expires. To clear immediately:
  ```bash
  aws s3 rm "s3://${BUCKET}/icp-swap-elt/locks/run.lock"
  ```
- **Deploy a code change**: push the image under a new unique tag (`aws_build_push_and_run.sh --build-only --tag <TAG>`), set `image_tag = "<TAG>"` and `terraform apply`; the next scheduled run uses it. Tags are immutable, so a rollback is another apply with the previous tag.
- **Scheduler alarms**: besides the task-failed rule, `<name_prefix>-scheduler-target-errors` (CloudWatch metric `TargetErrorCount` of `AWS/Scheduler`: the scheduler could not start the task) and `<name_prefix>-scheduler-missed-run` (`InvocationAttemptCount`: no invocation attempt for `missed_run_window_hours`, default 26, with missing data counted as breaching) notify the alerts topic. Both are created only when `schedule_expression` is set and use a dedicated schedule group so the group-level metrics belong to this schedule. Scheduler metrics are best effort; the window must exceed the schedule interval.
- **Task run time**: infrastructure enforces **no** run-time limit on the task. `scheduler_maximum_event_age_seconds` (formerly `task_timeout_seconds`) only bounds how long the scheduler retries to *start* it. The container gets `stopTimeout = 120` (the Fargate maximum) so a SIGTERM (for example on a stop or a Spot/maintenance event) leaves two minutes to release the run lock and flush; any run-time bound must be implemented in the application (its run lock TTL is 6 h).
- **Cost allocation tag**: see "Cost-allocation tag" in section 6.
- **Change the schedule / alerting / resources**: edit `infra/ingestion/compute/terraform.tfvars`, `terraform apply`.
- **Pause scheduled runs**: set `schedule_expression = ""` and apply.

---

## 5. Teardown

Remove compute but **keep all collected data**:

```bash
cd infra/ingestion/compute
terraform destroy
```

The ECR repository is created with `force_delete = true`, so destroy removes it together with its images (build artifacts only, rebuildable from git).

Full teardown including data (irreversible). The bucket is versioned and its policy denies deletes below `canister/` and `rest/` for everybody but `delete_exempt_principal_arns`, so first make sure your admin role is listed there and applied:

```bash
cd infra/ingestion/storage
# 1. terraform.tfvars: delete_exempt_principal_arns = ["<your admin role ARN>"]; terraform apply
# 2. run the next commands AS that role
BUCKET=$(terraform output -raw bucket_name)
# 3. delete every object version and delete marker (a plain `aws s3 rm --recursive` leaves versions behind)
aws s3api list-object-versions --bucket "$BUCKET" --output json \
  --query '{Objects: [Versions[].{Key:Key,VersionId:VersionId}, DeleteMarkers[].{Key:Key,VersionId:VersionId}][]}' \
  > /tmp/versions.json
# delete-objects accepts 1000 keys per call; split /tmp/versions.json with jq for larger buckets, repeat until the listing is empty
aws s3api delete-objects --bucket "$BUCKET" --delete file:///tmp/versions.json
terraform destroy
```

Or empty the bucket with the console's "Empty bucket" action (also as the exempt role). Abort incomplete multipart uploads first if the listing is not empty afterwards.

---

## 6. Transformation stage (`apps/transformation`)

The transformation stage reads the raw CSVs that ingestion lands in S3 and writes the `icp_swap_events` and `token_dim` Iceberg tables (Glue catalog, queryable from Athena). It is deployed and destroyed independently of ingestion; the only coupling is the raw bucket name and prefix, passed as plain variables. Design details and operating limits: [`apps/transformation/README.md`](../apps/transformation/README.md).

### Step 6.1 — storage root

```bash
cd infra/transformation/storage
cp terraform.tfvars.example terraform.tfvars   # edit values
cp backend.hcl.example backend.hcl             # see step 3.0
terraform init -backend-config=backend.hcl
terraform apply
terraform output   # processed_bucket_name, athena_results_bucket_name, glue_database_name, athena_workgroup_name
```

Notes on this root:

- `processed_noncurrent_version_expiration_days` defaults to **90** and is validated to be at least `snapshot_retention_days + 30` (variable `snapshot_retention_days` here must equal the one in the compute root, default 7). Iceberg rewrites and deletes data and metadata files; a rollback to a retained snapshot may need files that only the noncurrent S3 versions still hold, so they must outlive the snapshot retention with a margin. The Athena-results and access-logs buckets use the separate `auxiliary_noncurrent_version_expiration_days` (30).
- Load manifests (see "Load state machine contract") are expired after `manifest_expiration_days` (14) by a lifecycle rule scoped to `manifest_prefix/`; `manifest_prefix` must equal the compute root's.

### Step 6.2 — compute root

```bash
cd ../compute
cp terraform.tfvars.example terraform.tfvars
# edit terraform.tfvars:
#   aws_account_id                  - 12-digit account ID (used to build exact-account Glue ARNs)
#   raw_bucket_name / raw_prefix    - the ingestion bucket; raw_prefix must equal infra/ingestion/compute's s3_prefix (both default to `icp-swap-elt`)
#   processed_bucket_name, glue_database_name
#                                   - the outputs of step 6.1
#   alert_email                     - optional; confirm the SNS subscription email
#   silver_target_generation        - REQUIRED, no default: choose once (e.g. g20261101), never change it afterwards
#   image_tag                       - REQUIRED unless container_image is set: the unique tag pushed below
#   environment                     - Environment tag value
terraform init -backend-config=backend.hcl
```

The Lambda functions run a container image that this root never builds or pushes, and Lambda needs the image to exist when it creates a function. So the ECR repository is created first with a targeted apply, then the image is pushed under a **unique** tag (the repository is IMMUTABLE, `latest` is rejected), then everything is applied:

```bash
terraform apply -target=aws_ecr_repository.transformation
ECR=$(terraform output -raw ecr_repository_url)
TAG=$(git rev-parse --short=12 HEAD)
aws ecr get-login-password --region <region> | docker login --username AWS --password-stdin "${ECR%%/*}"
docker build --provenance=false -f ../../../apps/transformation/Dockerfile -t "$ECR:$TAG" ../../../apps/transformation
docker push "$ECR:$TAG"
terraform apply -var "image_tag=$TAG"     # or set image_tag in terraform.tfvars
```

The `-target` apply stays: the ECR change (immutable tags, `force_delete`) does **not** remove the need for it, because the chicken-and-egg is between the functions and an image that has to be pushed first. It is a one-time bootstrap (a normal `terraform apply` later, with a new `image_tag`, needs no `-target`), and it only creates the repository. Later deploys: push a new unique tag, apply with the new `image_tag`; a rollback is an apply with the previous tag.

`--provenance=false` is required: BuildKit otherwise pushes an image index with an attestation manifest, which Lambda rejects (`image manifest, config or layer media type ... is not supported`).

Deploy `silver` and `silver_backfill` together: they run the same image and share the table schemas. After a schema-affecting image change, do not roll the image back (see the transformation README, "token_dim").

### Step 6.3 — load the history (one-time) and verify

The backfill state machine (name suffix `-backfill`, resource `silver_backfill`) takes the raw prefixes to load and fans out one `backfill_file` invocation per CSV on `silver_backfill` (10240 MB), with bounded concurrency (`backfill_max_concurrency`, default 1):

```bash
SM=$(terraform output -raw silver_backfill_state_machine_arn)
aws stepfunctions start-execution --state-machine-arn "$SM" \
  --input '{"mode":"list","prefixes":["<raw_prefix>/canister/","<raw_prefix>/rest/"]}'
```

The prefixes are full S3 key prefixes in the raw bucket; every `.csv` under them is processed.

#### Load state machine contract

Both load state machines share one definition, `ListKeys` → `EmptyListChoice` → `ProcessEachKey` (Map) → `CompactTable` → `CheckQuality`, and differ only in the Lambda function they invoke: `-backfill` (resource `silver_backfill`, historical loads) uses `silver_backfill`; `-incremental` (resource `silver_incremental_load`, started by the `IngestionRunFinished` rule) uses the small `silver` function (2048 MB, `silver_timeout_seconds`). The text below says `silver_backfill` for the function; on the `-incremental` machine read `silver`.

- **`ListKeys`** invokes `silver_backfill` with `{"mode":"list","prefixes":[...]}` (`TimeoutSeconds` = function timeout + 60, retried on Lambda service errors, throttling and timeouts). The list handler must **write the keys as one JSON array of strings** (`["<raw_prefix>/rest/incremental/<runId>/0001.csv", ...]`) to an S3 object and return only a small object: `{"manifest_bucket": "<processed bucket>", "manifest_key": "<manifest_prefix>/<unique name>.json", "count": <number of keys>}`. This keeps the state payload far below Step Functions' 256 KiB limit regardless of how many files a run wrote.
- The handler writes to the path hardcoded in `apps/transformation/src/silver_job.py` (`LIST_MANIFEST_PREFIX`, `silver-manifests/list/`) in the processed bucket; there are no `MANIFEST_BUCKET` / `MANIFEST_PREFIX` environment variables. Terraform grants `s3:PutObject` below `manifest_prefix` (default `silver-manifests`, which must stay equal to the hardcoded prefix) to the writer role and `s3:GetObject` below it to the state machine role. The contract (field names, array of strings) is asserted by `tests/orchestration.tftest.hcl`.
- **`EmptyListChoice`**: `count == 0` skips the Map and the maintenance step and goes straight to the quality check.
- **`ProcessEachKey`** is a *Distributed* Map: an `ItemReader` of `arn:aws:states:::s3:getObject` with `InputType: JSON` reads the manifest, and every key becomes a child execution that invokes `backfill_file` on `silver_backfill`. Distributed mode is required because the S3 `ItemReader` is not available in inline mode (inline accepts only a JSON array in the state input). Each child execution is a Standard workflow, so its own history is separate from the parent's.
- **Failure policy: `ToleratedFailurePercentage = 0` plus redrive.** If any object still fails after its two retries, the Map and the execution fail (and the `silver_backfill` error alarm has already fired). The alternative (a tolerated percentage) lets a load finish "successfully" while objects are missing and the quality check runs on partial data, which is the wrong default for a loader whose correctness is "every announced object is in the table". After fixing the cause (cap raised, bad object quarantined), use **redrive** on the failed execution: `aws stepfunctions redrive-execution --execution-arn <arn>` re-runs only the failed child executions (available for 14 days after the execution closed; the manifest expires after 14 days as well). Loading is idempotent (`event_id` dedup), so redriving, or starting a new execution with the same prefixes, never double-loads.
- **`CompactTable`** (in-job table maintenance, after the whole Map; the Map defaults to `MaxConcurrency` 1 and `silver_backfill` to reserved concurrency 1): invokes `silver_backfill` with `{"mode":"compact"}`, which runs, in this order, compaction of `icp_swap_events` (only partitions with more than `compact_partition_file_threshold` = 7 files), snapshot retention of both tables (`snapshot_retention_days` = 7, newest `snapshots_to_retain` = 1 kept) and orphan-file cleanup of `token_dim` only (grace `orphan_file_retention_days` = 3). `TimeoutSeconds` = function timeout + 60; Retry on Lambda service errors/throttling/`States.Timeout`, and a separate bounded Retry on `MaintenanceIncomplete` (`maintenance_incomplete_max_attempts`, 5): the compaction committed progress but ran out of time, each attempt resumes. Any other error (a cap refusal, a duplicate `event_id`, a failed verification, an exhausted commit-retry budget) fails the execution before the quality check; the table is either unchanged or fully committed per slice. The output keeps only a counts summary under `$.compaction`. Caps and the design: `apps/transformation/README.md`, "Bounded file count of `icp_swap_events`" and "Table maintenance".
- **Per-object compaction trigger** (inside `backfill_file`, `silver_backfill` only): when an object's event_id lookup would select more than `silver_backfill_compact_trigger_selected_files` (48 of the 64 cap) files, the object's year partitions are compacted before the lookup. Without it, one load of more than ~64 overlapping objects fails inside the Map before `CompactTable` runs. If a load still fails with `selected file/row/byte cap exceeded`: invoke `silver_backfill` once with `{"mode":"compact"}` (no other execution running), then `redrive-execution`.
- **Logging**: both state machines log at level `ERROR` without execution data to `/aws/vendedlogs/states/<name_prefix>-{incremental,backfill}` (retention `log_retention_days`). The log-delivery permissions the Step Functions role needs are an AWS-documented `Resource: "*"` case.
- **Real-AWS caveats (not verifiable offline)**: the exact `ItemReader`/`ToleratedFailurePercentage` syntax and the `states:StartExecution` / `DescribeExecution` / `StopExecution` / `RedriveExecution` permissions on the child executions are modelled on the AWS documentation and only proven by the first execution; watch it once after the first deploy.

#### Concurrency

Iceberg commits are optimistic: two writers racing on one table retry or fail. `silver_backfill` has reserved concurrency `silver_backfill_reserved_concurrency` (default 1) and `silver` has `silver_reserved_concurrency` (default 1). Each execution's Map is limited by `backfill_max_concurrency` (default 1, validated >= 1 because `0` means *unlimited* in Step Functions), but ingestion can start several executions at once (one per finished run), and without a function-level cap they would multiply. A precondition requires the reserved value to be at least `backfill_max_concurrency`. Throttled invocations are retried by the state machine, so overlapping executions slow each other down instead of racing. **Account quota:** reserved concurrency comes out of the account's concurrency quota and AWS keeps at least 10 unreserved; a new account whose quota is 10 cannot reserve any (the apply fails with `InvalidParameterValueException`), so raise the quota or set the variables to `-1` (unreserved, then only the Map limit applies).

Transformation runs are normally started by ingestion, not by hand: see section 7. The `IngestionRunFinished` rule starts the `-incremental` state machine (`silver_incremental_state_machine_arn`): `ListKeys` → `ProcessEachKey` → `CompactTable` → `CheckQuality`, all Lambda steps on the small `silver` function, with in-job maintenance (`{"mode":"compact"}`) using smaller compaction caps for its 2048 MB (500,000 rows / 128 MiB per transaction), threshold-gated like the backfill (a partition is compacted only above 7 files, so a quiet day reads only manifests). There is no daily state machine or schedule on the transformation side. An incremental and a backfill execution can overlap: every maintenance commit is snapshot-fenced (`AssertTableUUID`, `AssertRefSnapshotId(main)`), so the other writer wins and the maintenance re-plans; the orphan grace period protects files of uncommitted writers. To load a specific day or prefix, start a state machine with its prefixes (section 7). Both paths are idempotent: replaying an object or a prefix changes nothing.

Data quality runs in its own function, `quality` (dbt tests on DuckDB). It is the last step of both load state machines (section 7), so every ingestion run is followed by a quality check of the freshly loaded tables; an error-severity failure fails that execution and raises the alarm. To run it by hand: `aws lambda invoke --function-name "$(terraform output -raw quality_lambda_function_name)" --payload '{}' --cli-binary-format raw-in-base64-out out.json` (add `"fail_on_error": false` to the payload to get the report without a function error). `out.json` lists every check with its status; error-severity failures make the invocation fail and raise the alarm, warn-severity ones (missing days, flagged rows, stale data) are only reported. The checks are listed in `apps/transformation/README.md`.

Ad-hoc read-only check (invoke the `silver` function directly):

- `{"mode":"token_metadata","ledger_ids":["<ledger-id>", ...]}` — current `token_dim` rows for those ledgers.

### Day-2 operations

- **Failures**: Lambda errors and dead-letter-queue depth raise CloudWatch alarms that publish to the alerts SNS topic. A failed object leaves the table untouched or fully committed (never partial); re-run the same object or day.
- **A lookup or source cap was hit** (`selected file/row/byte cap exceeded`, `source byte cap exceeded`, `source row cap exceeded`): raise the matching Terraform variable (see the transformation README, "Operating limits"), apply, and re-run the object.
- **Table maintenance runs in our own job** (`mode=compact`, step `CompactTable` of BOTH state machines (`-backfill` and `-incremental`); see the load contract above): compaction of `icp_swap_events`, then snapshot retention of both tables, then orphan cleanup of `token_dim`. Read the result in the execution output under `$.compaction.summary` (counts only) and in the `silver_backfill` / `silver` log lines `compaction slice committed`, `snapshots expired`, `orphan files removed`, `maintenance complete`. A retention or orphan backlog larger than one run's bounds (`SILVER_MAINT_MAX_DELETE_FILES` = 5,000) is reported as `pending` and continued by the next run; it is not an error. To run it by hand: `aws lambda invoke --function-name <silver_backfill function> --payload '{"mode":"compact"}' --cli-binary-format raw-in-base64-out out.json` while no load execution runs.
- **Glue table optimizers are not relied on.** `enable_table_optimizers` (default `false`) can still create them, but **keep it `false`** (turn it off where it was enabled): they run every 24 h and are a second writer that commits concurrently with the loads (forcing commit retries), their compaction output is not time-sorted, and their retention runs independently of the bounded in-job retention. If you keep them on anyway, expect `snapshot fence conflict retry budget exhausted` failures during long loads.
- **Rollback window**: snapshots older than `snapshot_retention_days` (7) are expired and their unreachable files deleted. Deleted files remain restorable from noncurrent S3 versions for `processed_noncurrent_version_expiration_days` (90); `snapshot_retention_days` is validated to at most 60 to keep 30 days of headroom. Keep it equal in the storage and compute roots.
- **Pause schedules**: pause the pipeline by setting `schedule_expression` to `""` in `infra/ingestion/compute` (transformation and quality follow the ingestion event; the transformation roots have no schedule).
- **Teardown**: see "Teardown (transformation)" below.
- **Roles**: `silver` and `silver_backfill` share the writer role (S3 object access only below `warehouse/`, plus `silver-manifests/` puts for the list step; Glue read/write on the two tables). `quality` has its own **read-only** role (S3 `GetObject` below `warehouse/`, `ListBucket`, Glue `Get*`): `apps/transformation/src/quality_job.py` only calls `catalog.load_table` / `scan(...).to_arrow_batch_reader()` and writes its DuckDB file and report to `/tmp` (grep for append/overwrite/delete/create_table/update/put_object/boto3 finds nothing). The raw bucket is read with `ListBucket` constrained to `s3:prefix` = `<raw_prefix>/*` and `GetObject` below `<raw_prefix>/`. The warehouse prefix confinement assumes the Iceberg tables live below the Glue database location `s3://<processed>/warehouse/` (the default PyIceberg derives from it); if a table is ever created elsewhere, writes fail with `AccessDenied` and the grant must be widened deliberately.
- **No DLQ on `quality` (accepted)**: a Lambda dead-letter queue only receives failures of *asynchronous* invocations, and `quality` is only invoked synchronously (Step Functions, or `aws lambda invoke`). Its failures are visible as a failed execution and through the `quality-errors` alarm.
- **Cost-allocation tag**: see below.

### Cost-allocation tag

The monthly budget (`aws_budgets_budget.transformation`) filters on the cost-allocation tag `Stage` (`user:Stage$transformation`). AWS only reports a tag in Cost Explorer and Budgets after it has been **activated** once: Billing and Cost Management → Cost allocation tags → *User-defined cost allocation tags* → activate `Stage` (this needs the management/payer account in an Organization; activation takes up to 24 hours and is not retroactive). Until then the budget sees no cost and never alerts. Every resource gets `Stage` through provider `default_tags` as well as explicit tags; `Environment` comes from `var.environment` (default `shared`).

### Teardown (transformation)

Order: **compute first, then storage**. The Iceberg tables and their data live in the storage root, so compute can be destroyed and re-created without data loss.

```bash
# 1. compute (Lambdas, state machines, queues, alarms, optimizers, ECR repository with its images)
cd infra/transformation/compute
terraform destroy
```

If the optimizers were enabled, they are removed with it (`enable_table_optimizers = true` resources are part of this root). The ECR repository has `force_delete = true`, so it is removed with its images.

Storage (**irreversible**, data loss): `terraform destroy` in `infra/transformation/storage` fails by design while the buckets hold objects (they are versioned, so "empty" means every object **version** and delete marker), and the Glue database cannot be removed while it contains tables that Terraform does not know about (the Lambdas created `icp_swap_events` and `token_dim`, not Terraform).

```bash
cd infra/transformation/storage
terraform output                       # bucket names and glue_database_name
DB=$(terraform output -raw glue_database_name)

# 2. delete the Iceberg tables from the Glue catalog (metadata only; the data files go with the bucket)
for T in $(aws glue get-tables --database-name "$DB" --query 'TableList[].Name' --output text); do
  aws glue delete-table --database-name "$DB" --name "$T"
done

# 3. empty all three buckets including every version and delete marker
for B in <processed_bucket_name> <athena_results_bucket_name> <access_logs_bucket_name>; do
  aws s3api list-object-versions --bucket "$B" --output json \
    --query '{Objects: [Versions[].{Key:Key,VersionId:VersionId}, DeleteMarkers[].{Key:Key,VersionId:VersionId}][]}' > /tmp/v.json
  # delete-objects takes up to 1000 keys per call: split /tmp/v.json for larger buckets and repeat until the listing is empty
  aws s3api delete-objects --bucket "$B" --delete file:///tmp/v.json
done

# 4. destroy the buckets, the (now empty) Glue database and the Athena workgroup
terraform destroy
```

The console's *Empty bucket* action does step 3 in one go. The access-logs bucket receives new log objects from the other two buckets until they are gone; if `destroy` still reports a non-empty bucket, empty it once more.

---

## 7. How the stages are wired (EventBridge)

The two stages share no Terraform state and no schedule. At the end of every S3 run (`sync`, `canister`, `backfill`, `incremental`; scheduled or started by hand) the ingestion task publishes one event to the bus `event_bus_name` (default `default`):

```json
{
  "source": "icp-swap-elt.ingestion",
  "detail-type": "IngestionRunFinished",
  "detail": {
    "status": "succeeded",
    "runId": "2026-06-23T11-49-42-924Z",
    "mode": "sync",
    "bucket": "<raw bucket>",
    "prefixes": ["<raw_prefix>/rest/incremental/2026-06-23T11-49-42-924Z/"]
  }
}
```

`prefixes` are the S3 key prefixes the run created files under. A rule in `infra/transformation/compute` (`trigger_on_ingestion_events`, default on) matches events for `raw_bucket_name` with a prefix under `raw_prefix` and starts the `-incremental` state machine (`silver_incremental_load`) with `{"mode":"list","prefixes":[...]}`: it lists the CSVs under those prefixes once and loads each one (bounded concurrency). So the transformation always loads exactly what a run pulled, for the one-time canister archive and the REST backfill as well as for the incremental runs.

- **Failed runs** also publish (`"status": "failed"`) if they wrote anything, because files committed before the failure are complete and the resume cursor has already moved past them. A failed run is reported by the existing task-failure alert; its already-written files are still loaded. A run that wrote nothing publishes nothing. If publishing itself fails after a successful run, the task exits non-zero and the failure alert fires.
- **Idempotent**: loading dedups on `event_id`, so a redelivered event, a re-run or a manual replay changes nothing.
- **Undeliverable events** (rule target retries exhausted) go to the transformation DLQ, whose depth alarm publishes to the alerts topic.
- **Check that it fired**: `aws stepfunctions list-executions --state-machine-arn $(terraform -chdir=infra/transformation/compute output -raw silver_incremental_state_machine_arn) --max-results 3` shows one execution per ingestion run, whose input holds the announced prefixes.
- **Manual load / recovery** (event lost, or data landed by other means): start the same state machine yourself with the prefixes (`silver_backfill_state_machine_arn` instead if the files are too large for the 2048 MB `silver` function):
  ```bash
  SM=$(terraform -chdir=infra/transformation/compute output -raw silver_incremental_state_machine_arn)
  aws stepfunctions start-execution --state-machine-arn "$SM" \
    --input '{"mode":"list","prefixes":["<raw_prefix>/rest/incremental/<runId>/"]}'
  ```
- **Contract**: source, detail-type and the `detail` fields above are defined in `apps/ingestion/src/lib/events.ts` and matched in `infra/transformation/compute/main.tf`; change both together. Both stages must use the same `event_bus_name`, and the transformation's `raw_bucket_name` / `raw_prefix` must equal the ingestion bucket and `s3_prefix`.
- **Pause**: `trigger_on_ingestion_events = false` in the transformation compute root stops the automatic loads (raw data keeps accumulating and can be loaded later with a manual start).
