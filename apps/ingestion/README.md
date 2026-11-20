# apps/ingestion

Stage 1 of the pipeline: a TypeScript job that reads ICPSwap swap transactions from Internet Computer storage canisters and the ICPSwap REST API and writes them as raw CSV, with manifests and resume state, to S3 or to a local directory. It runs as an ECS Fargate task.

## Layout

- `src/index.ts`: entrypoint (mode dispatch plus the canister archive `runFull`).
- `src/lib/`: `config.ts` (CLI parsing and validation), `csv.ts` / `restCsv.ts` (canister / REST records to rows), `csvSink.ts` (streaming CSV output), `storageTarget.ts` (local or S3 backend: state, sinks, manifest, run lock), `restClient.ts` / `restPaging.ts` / `restRuns.ts` (REST backfill, incremental and sync modes), `logger.ts` (structured JSON logs), `retry.ts` (bounded backoff), `state.ts` (resume state).
- `src/idl/`: canister interfaces.
- `tests/`: `node:test` suites, named after the target module (`state.test.ts`).
- `scripts/aws_build_push_and_run.sh`: build, push to ECR and trigger a one-off Fargate run from Terraform outputs.
- `Dockerfile`: container image of the task.

## Run modes

- `canister` (alias `full`): one-time deep-history archive of every storage canister.
- `backfill`: REST history newest to oldest, down to the canister seam.
- `incremental`: new REST transactions since the stored watermark.
- `sync` (default, scheduled): `backfill` if it has not completed, then `incremental`; never runs `canister`.

## Commands

Node.js 22 or newer.

```bash
npm ci                              # or npm install
npm run dev -- --mode sync          # run with tsx; local output under ./out/
npm run build                       # compile src/ to dist/
npm start -- --mode incremental     # run the compiled build
npm test                            # node:test suites in tests/
```

Local output goes to `./out/` and must stay untracked. Pass `--s3-bucket` (or set `S3_BUCKET`) to write to S3 instead.

## Further reading

- CLI flags, S3 layout and reliability behaviour: [docs/OPERATIONS.md](../../docs/OPERATIONS.md#cli-ingestion).
- Local runs, deployment and day-2 operations: [docs/OPERATIONS.md](../../docs/OPERATIONS.md#runbook).
- Terraform: `infra/ingestion/storage` and `infra/ingestion/compute`.
