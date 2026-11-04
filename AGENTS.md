# Repository Guidelines

## Project Structure & Module Organization
Monorepo with two independently deployable stages under `apps/`, plus shared infrastructure and docs.

- `apps/ingestion/` (TypeScript, ES modules, NodeNext; own `package.json`, `tsconfig.json`, `Dockerfile`): `src/index.ts` is the entrypoint (mode dispatch plus the canister archive `runFull`). Reusable logic lives under `src/lib/`: `config.ts` parses and validates CLI arguments, `csv.ts` / `restCsv.ts` map canister / REST records to rows, `csvSink.ts` handles streaming CSV output, `storageTarget.ts` abstracts the storage backend (local or S3: state, sinks, manifest, run lock), `restClient.ts` / `restPaging.ts` / `restRuns.ts` implement the REST backfill, incremental and sync modes, `logger.ts` emits structured JSON logs, `retry.ts` provides bounded backoff, and `state.ts` manages resume state. Canister interfaces live in `src/idl/`. Tests live in `tests/`. Container: `Dockerfile`.
- `apps/transformation/` (Python 3.12): the Silver transformation Lambda. `src/silver_job.py` is the handler; see `apps/transformation/README.md` for the module map, dedup model and operating limits. Tests are standalone scripts in `test/test_*.py`. One `Dockerfile` serves all three Lambda functions.
- `infra/`: four independent Terraform roots. `infra/ingestion/{storage,compute}` belong to ingestion; `infra/transformation/{storage,compute}` belong to transformation. Storage roots hold data and survive compute teardown. The stages are wired by an EventBridge event, not by Terraform references: ingestion publishes `IngestionRunFinished` with the S3 prefixes it wrote, and a rule in `infra/transformation/compute` starts the load state machine on exactly those prefixes.
- `apps/ingestion/scripts/aws_build_push_and_run.sh`: build/push/run helper for the ingestion Fargate task.
- `docs/RUNBOOK.md`: local runs, deployment of both stages, day-2 operations, teardown.
- `.github/workflows/ci.yml`: CI for the Node build/tests, the Python suite, `terraform fmt`/`validate` on all four roots, and the offline `terraform test` on all four roots.

Local output is written to `out/` at runtime and must stay untracked.

## Build, Test, and Development Commands
Ingestion (`cd apps/ingestion`):

- `npm install` (or `npm ci`) to install dependencies.
- `npm run dev -- --mode sync`: run the ingestion job directly with `tsx` (modes: `sync`, `canister`/`full`, `backfill`, `incremental`).
- `npm run build`: compile `apps/ingestion/src` to `apps/ingestion/dist`.
- `npm start -- --mode incremental`: run the compiled build.
- `npm test`: run the `node:test` suites in `apps/ingestion/tests`.

Transformation (`cd apps/transformation`):

- `make venv PYTHON=python3.12`: create `.venv` and install test dependencies.
- `make test-python`: run every `test/test_*.py` offline (SQLite-backed Iceberg catalog, no AWS calls).

Infrastructure:

- `terraform fmt -check` must pass for all four roots under `infra/` (CI enforces it); `terraform -chdir=<root> test` runs offline (CI runs it for all four roots).
- `apps/ingestion/scripts/aws_build_push_and_run.sh --mode sync`: build, push to ECR, and trigger a one-off Fargate run from Terraform outputs.
- Apply order per stage: storage root first, then compute root (see `docs/RUNBOOK.md`).

## Coding Style & Naming Conventions
TypeScript is strict: 2-space indentation, semicolons, single quotes, explicit types for exported helpers, `camelCase` for variables/functions, `PascalCase` for types/classes, short filenames like `csvSink.ts`, kebab-cased CLI flags (`--state-file`). Python follows the existing module style in `apps/transformation/src`; keep modules dependency-light and failure behavior fail-closed as documented in its README.

## Testing Guidelines
Ingestion tests use Node's built-in `node:test` runner via `tsx` and are named after the target module (`state.test.ts`). For ingestion changes run `npm test` and `npm run build` at minimum. Transformation changes must pass `make test-python`. Pure logic should be unit-tested; network and S3 interaction is verified by running the pipeline.

## Commit & Pull Request Guidelines
Use short imperative commit subjects, with a conventional type/scope when it improves clarity (`fix(lock): ...`, `feat(transformation): ...`). PRs should describe the behavior changed, list the validation performed, and include sample commands or output paths when the change affects runtime behavior, storage layout, or Terraform.

## Security & Configuration Tips
Do not commit `out/`, Terraform state, credentials, or filled-in `terraform.tfvars`. Use the `terraform.tfvars.example` files as starting points and prefer environment variables (`S3_BUCKET`, `S3_PREFIX`) for deployment-specific configuration. The Fargate task role and the Lambda roles are scoped to their buckets/prefixes - keep new IAM grants equally narrow.
