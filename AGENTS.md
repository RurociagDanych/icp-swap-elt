# AGENTS.md

## Purpose

Repository guidelines for contributors and AI agents. Monorepo with two independently deployable stages under `apps/` (ingestion to raw S3 CSV, transformation to Iceberg tables), their Terraform roots under `infra/`, and docs under `docs/`. Module maps are in [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md#components).

## Layout

- `.github/`: `workflows/ci.yml`, CI for the Node build/tests, the Python suite, `terraform fmt`/`validate` on all four roots, and the offline `terraform test` on all four roots.
- `apps/`: `apps/ingestion/` (TypeScript, ES modules, NodeNext; own `package.json`, `tsconfig.json`, `Dockerfile`; build/push/run helper `scripts/aws_build_push_and_run.sh`) and `apps/transformation/` (Python 3.12; `src/silver_job.py` is the handler, one `Dockerfile` serves all three Lambda functions).
- `docs/`: architecture, operations (including the runbook: local runs, deployment of both stages, day-2 operations, teardown) and security.
- `infra/`: four independent Terraform roots. `infra/ingestion/{storage,compute}` belong to ingestion; `infra/transformation/{storage,compute}` belong to transformation. Storage roots hold data and survive compute teardown. The stages are wired by an EventBridge event, not by Terraform references: ingestion publishes `IngestionRunFinished` with the S3 prefixes it wrote, and a rule in `infra/transformation/compute` starts the load state machine on exactly those prefixes.

## Commands

Ingestion (`cd apps/ingestion`):

- Install: `npm install` (or `npm ci`).
- Lint/compile: `npm run build` compiles `apps/ingestion/src` to `apps/ingestion/dist` (strict TypeScript).
- Test: `npm test` runs the `node:test` suites in `apps/ingestion/tests`.
- Run: `npm run dev -- --mode sync` runs the job with `tsx` (modes: `sync`, `canister`/`full`, `backfill`, `incremental`); `npm start -- --mode incremental` runs the compiled build.

Transformation (`cd apps/transformation`):

- Install: `make venv PYTHON=python3.12` creates `.venv` and installs test dependencies.
- Test: `make test-python` runs every `test/test_*.py` offline (SQLite-backed Iceberg catalog, no AWS calls).

Infrastructure:

- Lint: `terraform fmt -check` must pass for all four roots under `infra/` (CI enforces it).
- Test: `terraform -chdir=ROOT test` runs offline (CI runs it for all four roots).
- Deploy dry run: `terraform -chdir=ROOT init -backend=false -input=false` then `terraform -chdir=ROOT validate`.
- Deploy: apply order per stage is storage root first, then compute root (see the runbook in [docs/OPERATIONS.md](docs/OPERATIONS.md#runbook)). `apps/ingestion/scripts/aws_build_push_and_run.sh --mode sync` builds, pushes to ECR, and triggers a one-off Fargate run from Terraform outputs.

## Conventions

- Language: English for code, docs and commit messages.
- Public modules, classes and functions have docstrings. Comments explain why, not what. Tool directives such as # noqa are allowed.
- TypeScript: strict, 2-space indentation, semicolons, single quotes, explicit types for exported helpers, `camelCase` for variables/functions, `PascalCase` for types/classes, short filenames like `csvSink.ts`, kebab-cased CLI flags (`--state-file`).
- Python: follow the existing module style in `apps/transformation/src`; keep modules dependency-light and failure behavior fail-closed as documented in its README.
- Formatters: `terraform fmt` for Terraform; no other formatter is configured.
- Commit style: short imperative subjects, with a conventional type/scope when it improves clarity (`fix(lock): ...`, `feat(transformation): ...`). PRs describe the behavior changed, list the validation performed, and include sample commands or output paths when the change affects runtime behavior, storage layout, or Terraform.
- Branch naming: prefixed branches such as `docs/...`, `fix/...`, `feat/...`, `chore/...`.
- Test locations: ingestion tests use Node's built-in `node:test` runner via `tsx` in `apps/ingestion/tests` and are named after the target module (`state.test.ts`); transformation tests are standalone scripts in `apps/transformation/test/test_*.py`; Terraform tests are `tests/*.tftest.hcl` in each root. For ingestion changes run `npm test` and `npm run build` at minimum. Transformation changes must pass `make test-python`. Pure logic should be unit-tested; network and S3 interaction is verified by running the pipeline.

## Guardrails

- Secrets: do not commit Terraform state, credentials, filled-in `terraform.tfvars` or `backend.hcl`. Use the `terraform.tfvars.example` and `backend.hcl.example` files as starting points and prefer environment variables (`S3_BUCKET`, `S3_PREFIX`) for deployment-specific configuration. The Fargate task role and the Lambda roles are scoped to their buckets/prefixes; keep new IAM grants equally narrow.
- Git: no force-push and no history rewrite.
- Deletes: do not delete files, data or Terraform roots without owner approval.
- Generated paths: local output is written to `out/` at runtime and must stay untracked, as must `node_modules/`, `dist/`, `.venv/` and `.terraform/`.
- Lock files: `apps/ingestion/package-lock.json` and each root's committed `.terraform.lock.hcl` are authoritative. Bump the Terraform provider on purpose: `terraform init -upgrade`, review the plan, commit the lock file.
