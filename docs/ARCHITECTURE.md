# Architecture

> Purpose: Describe the two pipeline stages, their components, the key design decisions and the configured limits.
> Audience: contributors, reviewers, hiring readers
> Last reviewed: 2026-11-20

Sections:

- Overview
- Components
- Key design decisions
- Scale and limits

## Overview

Two independently deployable stages (`apps/ingestion`, TypeScript on Fargate; `apps/transformation`, Python on Lambda + Step Functions), each with its own Terraform roots under `infra/`. They are wired by an event, not by Terraform references: a finished ingestion run publishes `IngestionRunFinished` with the S3 prefixes it wrote, and a rule in the transformation stack starts the load on exactly those prefixes.

The flowchart of both stages is in the [README](../README.md#architecture).

## Components

- `apps/ingestion/` (TypeScript, ES modules, NodeNext; own `package.json`, `tsconfig.json`, `Dockerfile`): `src/index.ts` is the entrypoint (mode dispatch plus the canister archive `runFull`). Reusable logic lives under `src/lib/`: `config.ts` parses and validates CLI arguments, `csv.ts` / `restCsv.ts` map canister / REST records to rows, `csvSink.ts` handles streaming CSV output, `storageTarget.ts` abstracts the storage backend (local or S3: state, sinks, manifest, run lock), `restClient.ts` / `restPaging.ts` / `restRuns.ts` implement the REST backfill, incremental and sync modes, `logger.ts` emits structured JSON logs, `retry.ts` provides bounded backoff, and `state.ts` manages resume state. Canister interfaces live in `src/idl/`. Tests live in `tests/`. Container: `Dockerfile`.
- `apps/transformation/` (Python 3.12): the Silver transformation Lambda. `src/silver_job.py` is the handler; see `apps/transformation/README.md` for the module map, dedup model and operating limits. Tests are standalone scripts in `test/test_*.py`. One `Dockerfile` serves all three Lambda functions.
- `infra/`: four independent Terraform roots. `infra/ingestion/{storage,compute}` belong to ingestion; `infra/transformation/{storage,compute}` belong to transformation. Storage roots hold data and survive compute teardown.

## Key design decisions

- **The Iceberg table is the only dedup state.** `event_id` is looked up in the fact table, pruned to the incoming rows' event days; there is no side index. Why: a side index grows with total row count and every write pays for it. Trade-off: an `event_id` that re-arrives on a different UTC day than the stored row is not detected (documented known limit, pinned by a test). See [transformation README](../apps/transformation/README.md), "Dedup model".
- **Event coupling instead of Terraform references between stages.** Stages share only a bucket name and prefix as plain variables, so either can be applied or destroyed alone. Trade-off: a lost event means a manual load ([OPERATIONS.md](OPERATIONS.md), section 7); loading is idempotent, so replays are safe.
- **Canister and REST feeds are kept raw and separate; the overlap is reconciled once.** The canister path stopped receiving new transactions on 2026-06-07 and the REST API lacks deep history, so both are used. The two feeds hash the same swap differently, so `event_id` dedup cannot catch the overlap window; a fixed, content-based fixup handles the one known object pair. Trade-off: it is deliberately not a general reconciliation mechanism.
- **Quarantine instead of failing.** Rows that fail validation are quarantined and the load continues; payload conflicts resolve deterministically (smaller payload hash wins, flagged `quality_status = 'conflict'`). Trade-off: `amount_usd` of `conflict` rows is unreliable, and degenerate source records stay as `unresolved`, so analyses filter on `quality_status = 'ok'`.
- **Own compaction job instead of Glue table optimizers.** Compaction, snapshot expiry and orphan cleanup run as the `CompactTable` step. Why: the optimizers are a second concurrent writer that forces commit retries and their retention runs independently of the bounded in-job retention. Trade-off: maintenance code and its caps are ours to maintain. `enable_table_optimizers` stays `false`.
- **Fail-closed caps, concurrency of one.** Lookup and source-size caps raise before anything is committed; Lambda and Map concurrency default to 1 because Iceberg commits are optimistic. Trade-off: throughput is traded for no partial commits; a cap hit needs a Terraform change and a replay.
- **Dev networking without NAT.** The current deployment runs the Fargate task in the default VPC with public IPs because a NAT gateway costs more per month than the project's whole annual budget ([OPERATIONS.md](OPERATIONS.md), "Networking"). Trade-off: tasks are publicly addressable; production should use private subnets with NAT. See [SECURITY.md](SECURITY.md).

## Scale and limits

Configured caps (not measurements):

| Limit | Value | Source |
| --- | --- | --- |
| Max source object, `silver` and `silver_backfill` | 200 MiB / 300,000 rows | `apps/transformation/README.md`, "Operating limits" |
| Dedup lookup per object, `silver` | 64 files / 250,000 rows / 128 MiB | same (code defaults) |
| Dedup lookup per object, `silver_backfill` | 64 files / 1,000,000 rows / 512 MiB | same |
| Lambda sizing | `silver` 2048 MB / 300 s; `silver_backfill` 10240 MB / 900 s; `quality` 3008 MB / 900 s | `apps/transformation/README.md`, "Deploy" |
| `quality` read bounds | 20,000 files / 30,000,000 rows / 8 GiB | `apps/transformation/README.md`, "Operating limits" |
| Snapshot retention | 7 days, newest 1 kept | same |

Observed on real data:

| Observation | Value | Source |
| --- | --- | --- |
| Largest canister-archive CSV | ~103 MB / 200k rows | `apps/transformation/README.md`, "Operating limits" |
| Largest daily REST-incremental CSV | ~33 MB | same |
| Canister/REST overlap window | ~6.25 h | `apps/transformation/README.md`, "Seam reconciliation" |
| `unresolved` rows on the full load | about 5,262 | `apps/transformation/README.md`, "Known source artefacts" |

The repo contains no throughput, latency or total-row-count measurements.
