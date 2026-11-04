# apps/transformation

The Transform stage of this repo's ELT pipeline: a Python Lambda job that normalizes the two
raw ICPSwap CSV feeds landed by `apps/ingestion` (a 28-column on-chain "canister" feed and a
37-column REST "incremental" feed) into one deduplicated Iceberg fact table, `icp_swap_events`,
plus a current-label `token_dim` dimension — queryable from Amazon Athena.

## Layout

```
src/                       application code (packaged into the container image)
  silver_job.py               Lambda handler; dispatches on event["mode"]:
                           list | incremental | backfill_file | token_metadata | compact
  silver_csv.py            dependency-free CSV -> normalized-record boundary
  silver_contract.py       per-row normalization: identity, decimals, amount_usd, issues
  silver_persistence.py    atomic per-object Silver publication: bounded ingestion,
                           event_id dedup against the fact table itself, quarantine-on-dirty-row,
                           Iceberg table creation (hidden YEAR partition on event_timestamp)
  fact_compaction.py       in-job compaction of icp_swap_events (time-sorted rewrite of small
                           files, snapshot-fenced, bounded, verified); also the optional
                           per-object threshold trigger called by silver_persistence
  table_maintenance.py     in-job snapshot retention (both tables) and orphan-file cleanup
                           (token_dim only); run by mode=compact after compaction
  iceberg_scan_safety.py   makes PyIceberg 0.8.1 scan planning thread-safe (imported by
                           silver_persistence before any scan is planned)
  token_dimension.py       token_dim merge: latest observation wins per attribute under ONE
                           as_of column; ties/ICP rules, conflicts reported not raised; the
                           table is created on demand (6 columns), never evolved
  seam_reconciliation.py   content-based cross-dataset dedup for the one historical seam
                           between the canister archive and the REST backfill; wired into
                           the backfill_file dispatch for exactly that one known object pair
  write_planning.py        incremental/backfill raw-object routing helpers (no dedup logic)
  quality_job.py           data-quality Lambda handler: Iceberg -> DuckDB, then `dbt test`
                           (dbt in a child process bounded by the remaining invocation time)
  quality_dbt/             the dbt project it runs (declared tests in models/sources.yml,
                           SQL tests in tests/, generic tests in macros/)
  utils/logging.py         structured (JSON) logging shared by both handlers
  utils/retry.py           jittered exponential backoff for Iceberg commit-conflict retries
test/                      test/test_*.py, run standalone (no pytest dependency)
Dockerfile                 one container image serves all Lambda functions (silver, silver_backfill,
                           quality); each picks its entrypoint via image_config.command in
                           infra/transformation/compute
Makefile                   make venv / test-python / docker-build
```

## Bronze/Silver contract

Bronze retains the raw source rows and immutable per-object occurrence lineage exactly as
ingested. Silver (`icp_swap_events`) is a flat fact table, contract version `silver-flat-v1`: one
row per deduplicated event, grain = source-aware semantic identity (REST feed: namespaced genuine
`tx_hash`; canister feed: genuine non-sentinel hash, else a frozen legacy composite key).
Persisted columns: `event_id`, `event_timestamp` (nullable), `action`, `pool_id` (nullable),
`token0_ledger_id`/`token1_ledger_id` (nullable, paired to the source row's slot), `token0_amount`/
`token1_amount` (`DECIMAL(38,18)`, nonnegative operation magnitudes, never `abs()`-repaired),
`amount_usd` (`DECIMAL(38,18)`, nonnegative USD magnitude — the canister feed's raw
`amount_usd` directly, the REST feed's raw tx-value columns if present else `price * amount` as a
fallback), `from_principal`/`to_principal`, `contract_version`, `lineage_ref`, `quality_status`,
`ingested_at`.

**Amount scale.** The canister feed serialises float64, so small magnitudes print with more than
18 fractional digits (e.g. `0.0001115933237495279`, `1.2345678901234567e-7`), and `price * amount`
almost always has more. Such a value is **quantized to 18 places with `ROUND_HALF_EVEN`** under an
explicit `decimal.Context` (`silver_contract._AMOUNT_CONTEXT`; the ambient 28-digit context is never
used, products are exact before rounding) and the row records the informational issue
`amount_rounded` (counted in `issue_count`; it does NOT make the row `unresolved`: the dropped
digits are below the column's 1e-18 quantum). Previously such values were rejected to `NULL`, so a
swap lost both amounts and its USD value. Integral overflow (more than 20 integral digits, also
after rounding carries) is still rejected to `NULL` with `invalid_amount`. The event identity is
computed from the raw lexeme, so rounding never merges two source rows. No nested structures or token metadata in the fact table — those live in the
separate `token_dim` dimension (keyed by `ledger_id`; the CURRENT label per ledger).

**Hidden partitioning is `year(event_timestamp)`**, not a materialized day column and not a daily
partition — a yearly transform avoids the small-files problem a per-day partition produces at
this table's write volume. The spec is passed to `create_table`, so the table is born
partitioned in one catalog commit. `prepare_target` validates it: an unpartitioned table that is
still empty (the crash window of the former two-commit create) is repaired in place; a populated
unpartitioned table, or one partitioned on anything but `event_timestamp`, is refused.

Rows that fail validation are quarantined to a content-addressed, per-object artifact under the
table's own `silver-quarantine/<generation>/` prefix with a reason, rather than failing the whole run.

**File-level defects fail closed (nothing is published).** A CSV error after a valid prefix
(`malformed_csv`, e.g. a truncated object or an unterminated quote), undecodable bytes
(`invalid_utf8`), an ambiguous header (`blank_header`, `duplicate_header`) or non-blank content
without a header line (`missing_header`) raise `IngestionError` before any write: the object HAS
rows that cannot be read, and committing the prefix (or quarantining the whole object and returning
success) silently lost them. The raw object stays replayable. An **empty or header-only** object has
no rows at all — the REST incremental sink writes its header before it knows whether any row is
new — so it stays a durable-quarantine success, logged at WARNING with `marker=SILVER_EMPTY_SOURCE`.
Row-level quarantines log a WARNING with `marker=SILVER_ROWS_QUARANTINED` (`rows_quarantined`,
`quarantine_uri`). **Alarm requirement:** a CloudWatch metric filter + alarm on these markers (and on
`TOKEN_DIM_UPDATE_FAILED`, see token_dim) is needed for anyone to be told; that is infrastructure
and is not part of this app.

The table carries a permanent generation tag (`silver.target.generation`, set from the
`silver_target_generation` Terraform variable and checked on every load). It must never change
once a row has been written: a different value is refused as a generation mismatch.

### Dedup model (`event_id` against the table — no side index)

`event_id` is the only dedup key and the Iceberg fact table itself is the only dedup state; there
is no cumulative lineage index, no shard and no root pointer. A side index would grow with the
total row count, and every write would pay for that growth; looking the incoming ids up in the
table keeps the cost proportional to the object being written. A table that carries lineage-index
properties was written under a different dedup model and is refused.

For every object, per `event_id`:

1. Rows are collapsed inside the object first. Equal payload hashes are one row (lowest ordinal
   wins); different hashes are a conflict.
2. The stored rows are looked up with `event_id IN (...)` restricted to the incoming rows' UTC event
   days (year partition + file statistics prune the scan) and compared by **payload hash**
   (`_payload_key`: every column except `lineage_ref`, `ingested_at` and the derived `quality_status`;
   decimals/timestamps are normalised so a row read back from Iceberg hashes like the incoming one).
   - absent -> append;
   - same hash -> no-op (an exact replay, from any object, changes nothing and keeps the stored
     `ingested_at`/`lineage_ref`);
   - different hash -> **conflict**: the payload with the *smaller* hash survives (so the result does
     not depend on arrival order) and the surviving row gets `quality_status = 'conflict'`. There is
     no other conflict record; replaying either side of a conflict is a no-op.
   - **Precedence for derived USD values (documented, not recency-based).** `amount_usd` is part
     of the payload hash. When the REST incremental fetch and the backfill fetch of the same
     `tx_hash` differ ONLY in the derived USD value (`token*_tx_value` / `price * amount`), the row
     is one fact flagged `conflict` and the smaller-hash payload's `amount_usd` survives — which is
     NOT necessarily the fresher fetch. Treat `amount_usd` of `conflict` rows as unreliable.
     Excluding `amount_usd` from the hash was rejected: the first-arriving value would then win
     silently (arrival-order dependent, no flag). Pinned by
     `test_m7_usd_only_difference_is_a_flagged_order_independent_conflict`. **Needs real-data
     confirmation:** check whether the REST API's USD/price fields change between fetches of one
     transaction (count `conflict` rows whose only differing column is `amount_usd`); if they do,
     the follow-up is a fetch-recency precedence (e.g. by the run timestamp in the raw key).
3. Everything the object changes is ONE Iceberg transaction (delete of exactly the stored rows being
   rewritten, if any, plus the append), fenced by the table snapshot. A concurrent commit fails with
   `CommitFailedException`; the retry re-refreshes and re-scans, so two interleaved writers with the
   same new `event_id` leave exactly one row. Retries (`SILVER_MAX_RETRIES`, default 6) wait an
   equal-jitter exponential backoff (0.5 s base, 8 s per wait, 30 s total, never past the
   deadline) so concurrent backfill workers do not re-scan in lock-step; exhausting any bound
   raises (nothing published). Files staged by an attempt that was never admitted (deadline or
   staging error) or was rejected (`CommitFailedException`) are deleted; after any OTHER commit
   error they are left alone, because the commit may have landed.
4. The lookup is fail-closed bounded by `SILVER_MAX_SELECTED_FILES` / `_ROWS` / `_BYTES`. A cap hit
   raises an ingestion error for that object before anything is committed; the raw source object
   stays replayable, so the fix is to raise the cap and run the object again. The code defaults
   are 64 files / 250,000 rows / 128 MiB, which suit the small `silver` function. `silver_backfill` is
   configured with 64 / 1,000,000 / 512 MiB (`silver_backfill_max_selected_*` in
   `infra/transformation/compute/variables.tf`) because a 200k-row object's lookup also
   reads the neighbouring objects that share its boundary event days, and an exact replay of such
   an object can select ~400k rows. If more than one stored row shares an `event_id` the object is
   refused rather than guessed at.
5. After the commit, a bounded post-commit verification re-reads the appended `event_id`s from a
   freshly loaded table (twice the lookup caps) and fails closed — it never repairs or deletes —
   if any of them is stored more than once or cannot be read back. The commit is already
   published at that point, so the error message says to replay the object; the replay is a no-op
   when the table is healthy.

Scan planning must be thread-safe for this lookup to be reliable: PyIceberg 0.8.1 evaluates
manifests on a thread pool but shares one metrics evaluator between them, which can silently
drop a data file from the plan (and so store its rows twice). `iceberg_scan_safety.py`
serializes that one in-memory evaluation; it fails closed at import time if the PyIceberg
internals it relies on change.

`lineage_ref` is the cheap string `s3://<bucket>/<key>#<ordinal>` (one-based logical CSV record
ordinal, header excluded); the raw CSVs in S3 remain the source of truth for provenance and history.
For the single seam B object the ordinal refers to the *reconciled* content (overlapping rows
removed before ingest), not the original file.

**Known limit (not detected, by design).** Because the lookup is pruned to the incoming rows' event
days, an `event_id` that re-arrives later with a timestamp on a *different* UTC day than the
committed row is not found and would produce a second row. In this data the timestamp is part of the
event identity (tx hash / composite key), so that is a corruption case rather than a normal
conflict; no cumulative structure is kept to cover it (pinned by
`test_known_limit_same_event_id_on_a_different_day_is_not_detected`).

## Setup and local tests

```bash
make venv PYTHON=python3.12          # creates .venv, installs test/requirements.txt
                                       # (pulls src/requirements.txt too: boto3, pyarrow, pyiceberg)
make test-python                      # every test/test_*.py as a standalone script, no AWS needed
```

The test suite uses a local SQLite-backed Iceberg catalog with a `file://` warehouse — no AWS
Glue/S3 calls, so it runs fully offline.

## Deploy

Two Terraform roots, applied in order (see the repo root `README.md` / `docs/RUNBOOK.md`):

1. `infra/transformation/storage` — the Iceberg warehouse/processed bucket, Athena
   query-results bucket, access-logs bucket, Glue database, and Athena workgroup.
2. `infra/transformation/compute` — one container image (built from this directory's
   `Dockerfile`) backing three Lambda functions, an SQS dead-letter queue, least-privilege IAM,
   two Step Functions state machines with one shared definition (list, fan-out with bounded
   concurrency `backfill_max_concurrency` = 1 by default, compact, quality check): `-incremental`,
   started by each `IngestionRunFinished` event through an EventBridge rule and running on the small
   `silver` function, and `-backfill` for historical loads, started by hand and running on
   `silver_backfill`; no schedule exists in this root,
   CloudWatch-alarm-driven SNS alerting, and a monthly AWS Budget cost guardrail:

   | function | memory / timeout | used by |
   | --- | --- | --- |
   | `silver` | 2048 MB / 300 s | every step of the `-incremental` state machine (`list`, `backfill_file`, `compact`), i.e. every ingestion-triggered load; direct invokes of `token_metadata` and `incremental` |
   | `silver_backfill` | 10240 MB / 900 s | `-backfill` state machine only (`list`, `backfill_file`, `compact`), i.e. historical loads |
   | `quality` | 3008 MB / 900 s | last step of both state machines (dbt data-quality tests); can also be invoked by hand |

   Lambda memory is configured per *function*, not per invocation, hence two transformation functions over
   the same image. Each has its own soft `SILVER_DEADLINE_SECONDS` strictly below its timeout
   (enforced by a Terraform precondition). Memory above 3008 MB needs the account's Lambda memory
   quota to allow it (new accounts can be capped at 3008 MB until a Service Quotas increase) -
   this only surfaces at apply time.

   Always deploy `silver` and `silver_backfill` together (they share the image and the table
   schemas).

`apps/transformation/src/requirements.txt` pins `pyiceberg[glue,pyarrow]`+`pyarrow`, which
exceed the Lambda zip/layer size limit — hence the container image, same reasoning as
`apps/ingestion`'s Fargate-over-zip choice for its own heavier dependencies.

## Operating limits

All of these are environment variables on the Lambda functions, set from Terraform variables
(`infra/transformation/compute/variables.tf`, whose descriptions give the sizing
rationale); read the variable descriptions before changing them.

| limit | `silver` | `silver_backfill` | env var / Terraform variable |
| --- | --- | --- | --- |
| max source object size | 200 MiB | 200 MiB | `SILVER_MAX_SOURCE_BYTES` / `silver_max_source_bytes` (code default 16 MiB) |
| max source rows | 300,000 | 300,000 | `SILVER_MAX_SOURCE_ROWS` / `silver_max_source_rows` (code default 100,000) |
| soft deadline (whole invocation) | 240 s | 840 s | `SILVER_DEADLINE_SECONDS` / `silver_deadline_seconds`, `silver_backfill_deadline_seconds` |
| margin kept before the Lambda timeout | 15 s (code default) | 15 s | `SILVER_DEADLINE_MARGIN_SECONDS` |
| commit-conflict attempts | 6 (code default) | 6 | `SILVER_MAX_RETRIES` |
| lookup: files / rows / bytes | 64 / 250,000 / 128 MiB (code defaults) | 64 / 1,000,000 / 512 MiB | `SILVER_MAX_SELECTED_FILES`, `_ROWS`, `_BYTES` / `silver_backfill_max_selected_*` |
| quarantine artifact size | 32 MiB (code default) | 32 MiB | `SILVER_MAX_QUARANTINE_BYTES` |
| `token_metadata` response | 4 MiB | — | `TOKEN_METADATA_MAX_RESPONSE_BYTES` / `token_metadata_max_response_bytes` |
| compaction: partition gate (more than N files) | 7 | 7 | `SILVER_COMPACT_PARTITION_FILE_THRESHOLD` / `compact_partition_file_threshold` (code default 0 = off) |
| compaction: per-object trigger (lookup selects more than N files) | — (off) | 48 | `SILVER_COMPACT_TRIGGER_SELECTED_FILES` / `silver_backfill_compact_trigger_selected_files` (code default off) |
| compaction: rows per output file / min small files | 250,000 / 4 | 250,000 / 4 | `SILVER_COMPACT_TARGET_FILE_ROWS`, `SILVER_COMPACT_MIN_INPUT_FILES` |
| compaction input per transaction: files / rows / bytes | 256 / 500,000 / 128 MiB | 256 / 2,000,000 / 512 MiB | `SILVER_COMPACT_MAX_INPUT_FILES`, `_ROWS`, `_BYTES` |
| snapshot retention: days / newest kept | 7 / 1 | 7 / 1 | `SILVER_SNAPSHOT_RETENTION_DAYS`, `SILVER_SNAPSHOTS_TO_RETAIN` / `snapshot_retention_days`, `snapshots_to_retain` |
| token_dim orphan grace period | 3 days | 3 days | `SILVER_ORPHAN_GRACE_DAYS` / `orphan_file_retention_days` |
| maintenance per run: files deleted / manifests read / files listed | 5,000 / 20,000 / 200,000 (code defaults) | same | `SILVER_MAINT_MAX_DELETE_FILES`, `_MANIFESTS`, `_LISTED_FILES` |

The source-size and row guards are sized from the largest known raw objects (largest
canister-archive CSV ~103 MB / 200k rows; largest daily REST-incremental CSV ~33 MB). The seam
reconciliation uses the same `SILVER_MAX_SOURCE_ROWS` bound (it used to hard-code 200,000).

**Deadline.** The handler computes ONE absolute deadline when it is invoked: the earlier of
`SILVER_DEADLINE_SECONDS` after the invocation start and the Lambda's remaining time
(`context.get_remaining_time_in_millis()`) minus `SILVER_DEADLINE_MARGIN_SECONDS`. It covers the
object read, the seam step, the fact publication and the token_dim phase, and an `incremental` run
shares it across ALL its keys (previously every key got a fresh budget that started only inside
`ingest_one`, after the read, so a day could run past the 300 s Lambda timeout). A run that hits
it fails closed; the Step Functions retry replays the day, and keys already loaded are no-ops.

The `quality` Lambda has its own read bounds (defaults in `quality_job.DEFAULT_LIMITS`, overridable
with `QUALITY_CHECK_MAX_SCAN_FILES` / `_ROWS` / `_BYTES` / `QUALITY_CHECK_DEADLINE_SECONDS` /
`QUALITY_CHECK_MIN_DBT_SECONDS` / `QUALITY_CHECK_DEADLINE_MARGIN_SECONDS`): 20,000 files,
30,000,000 rows, 8 GiB, 780 s for load AND dbt together, at least 30 s left to start dbt, 30 s kept
before the Lambda timeout.

The `token_dim` limits are in `token_dimension.DEFAULT_LIMITS`: 16 data files per lookup, 50,000
rows, 32 MiB, 1,024 ledgers per object, 6 commit attempts (jittered exponential backoff, 0.2 s
base, 4 s per wait) and a 45 s phase deadline (also capped by the invocation deadline); see
"Bounded file count" below for the compaction thresholds.

The backfill state machine discards each object's result (`ResultPath = null`) and the Lambda
returns a slimmed response (`silver_job._backfill_response`), because a Step Functions state output
is capped at 256 KiB. The structured `object complete` log line carries the same slim summary
(per-ledger lists replaced by counts, no readback): one CloudWatch event is capped at 256 KB and a
large object's full token_dim readback alone exceeded it. The full result is still what
`_process_key` returns to direct callers.

**`list` output contract (changed).** `list` validates `prefixes` (a list of nonblank strings, each
starting with `RAW_PREFIX`; an empty prefix used to list the whole raw bucket), writes the CSV key
list as a JSON array to `s3://$PROCESSED_BUCKET/silver-manifests/list/<sha256>.json` (content
addressed, so a retried list writes the same object) and returns only
`{"manifest_bucket": ..., "manifest_key": ..., "count": N}`. The inline `{"keys": [...]}` output hit
the 256 KiB state-output limit at roughly 2,400 keys. Both state machines
read the list with a Distributed Map whose S3 `ItemReader` points at `manifest_bucket`/`manifest_key`
(JSON array input), and their role has `s3:GetObject` on the manifest prefix.

## Seam reconciliation (one-time historical fixup)

The canister-archive feed and the REST feed key the same real swap on different hashes by
construction, so the normal per-file `event_id` dedup cannot catch the ~6.25h overlap window
where both feeds cover the same swaps. `silver_job._process_key` special-cases the exact one known
overlapping object pair (`SEAM_A_KEY`/`SEAM_B_KEY`/`SEAM_WINDOW_START`/`SEAM_WINDOW_END` constants
in `silver_job.py`): when the `backfill_file` mode is asked to ingest the known seam B-side object, it
also reads the known seam A-side object (read-only, never written) to build a content-based window
index, and ingests only the non-duplicate rows. (`event_id` dedup does *not* catch these: the two
feeds hash the same swap differently. The reconciliation runs before `ingest_one` and never touches
the fact table's dedup; `test_seam_end_to_end_through_new_ingest_path_...` covers seam A loaded,
seam B reconciled, replay no-op.) The two keys are fixed relative to `RAW_PREFIX` (Terraform `raw_prefix`). Any other object — including ones that merely
resemble the seam filename — is completely unaffected. This is deliberately a fixed, one-time
mechanism, not a general ongoing reconciliation step. A seam B row whose cell count differs from
the header is passed through with exactly its original cells (it used to lose extra cells or be
padded with empty strings, i.e. come out well-formed), so ingestion quarantines it as
`column_count`; the report counts them in `column_count_rows`.

## token_dim: current value per ledger, created on demand

`token_dim` holds ONE row per `ledger_id`: its **current** `symbol`, `name`, `standard` and
`decimals`. It is informational — fact rows carry ledger ids only, so facts never depend on it —
and therefore **an attribute conflict never stops a load**: it is resolved by the rules below and
reported in the result.

**Order and failure isolation.** `_process_key` commits the FACTS first and only then merges the
dimension. Any `MetadataError` in the dimension step (schema refusal, invalid values, caps,
retry/deadline exhaustion) is best-effort: the facts stay published, the result carries
`token_dim.error`, and a WARNING with `marker=TOKEN_DIM_UPDATE_FAILED` is logged (alarm on it).
Recovery: fix the cause and replay the object — the fact load is an exact-replay no-op and the
dimension merge is idempotent. Other (non-contract) exceptions still fail the invocation after
the facts are committed; the retry replays the facts as a no-op. Previously the dimension was
merged first and any `MetadataError` blocked the facts.

Columns (exactly these six; created by the transformation, nothing else): `ledger_id` (non-null string),
`symbol`, `name`, `standard` (string), `decimals` (int64), `as_of` (`timestamp(us, UTC)`, nullable).

`as_of` is the event timestamp of the **latest observation applied to the row** (`timestamp` seconds
for Dataset A, `tx_time` ms for B, parsed by `silver_contract._timestamp` — the parser of the fact
table's `event_timestamp`; NULL = unknown/undated = oldest).

**Merge rules** (`token_dimension.apply_metadata`, never raises on a conflict):

| Situation | Rule |
|---|---|
| Within one object, per ledger and attribute | the value of the **latest** observation that supplies the attribute wins (null/absent never counts). The object's candidate `as_of` = the max timestamp of its observations of that ledger. |
| Same timestamp, different values (in the object or vs the stored row) | the **lexicographically greater** value wins; `decimals` compare numerically. |
| Attribute observed **later** than the stored `as_of` (or stored `as_of` NULL) | replaces the stored value; attributes the object does not supply keep theirs; `as_of` advances to the object's latest timestamp. The comparison uses the timestamp of the observation that supplied THAT attribute, not the object's maximum. |
| Attribute observed **earlier** than the stored `as_of` (or a replay) | may only **fill** a NULL; never overwrites — even when the same object carries newer observations of other attributes; `as_of` unchanged by it. A replay is a no-op and creates no snapshot. |
| `standard` = `ICP` | `ICP` always wins over any other value, whatever the timestamps (the native ICP ledger is recorded both as `ICP` and `ICRC2`). The only special case. |

Everything else stays fail-closed: invalid value types/ranges, the ledger cap, the byte/row/file
caps, deadlines, schema drift, a changed table UUID, commit-retry exhaustion.

**Reporting.** The `token_dim` section of the `apply_metadata` / `silver_job` result carries
`conflicts_resolved` (exact count of ledger/attribute pairs where two different values had to be
settled) and `conflicts` (the first 50, sorted by ledger/attribute, each
`{ledger_id, attr, values, chosen}`), so a CloudWatch/Step Functions output shows what was decided.
A replay re-reports the disagreements it sees (and still changes nothing).

**Accepted limitation (known; minimal fix applied).** `as_of` is per ROW, not per attribute. The
merge now compares each attribute's own observation timestamp with the row's `as_of`, so an OLDER
value can never overwrite a newer one (previously an object whose max timestamp was newer replaced
every attribute it supplied, including ones observed earlier than the stored value; pinned by
`test_older_attribute_in_a_newer_object_never_overwrites_a_newer_value`). What remains: a value
observed after its own stored value but before the row's `as_of` is refused (fill-only), so a stale
value can survive. Exact per-attribute ordering needs one `as_of` column per attribute — a schema
change (this table is never evolved in place; it would be a drop-and-rebuild of `token_dim` plus a
reload). **Follow-up:** add `symbol_as_of`, `name_as_of`, `standard_as_of`, `decimals_as_of`,
compare per attribute in `_merge_row`, and rebuild the dimension by replaying the raw objects.
Concretely, one `as_of` cannot order Dataset A observations
(symbol/standard/decimals) against Dataset B observations (symbol/name) per attribute, so a stale
`name` can survive in rare cases: when all A objects are loaded first and then B, two *older*
B names arriving after a newer A observation leave the **first** one applied (pinned by
`test_planned_load_order_all_a_then_b_leaves_the_first_older_b_name`). Objects that supply every
attribute — Dataset A — are fully order-independent (permutation-tested); partially-supplying
objects can be order-dependent in the same way.

The dimension keeps only the current value. An exact, order-independent rename history (SCD2)
would need unbounded per-ledger state, so it is deliberately not kept.

**Creation — no migration** (`token_dimension.prepare_dim_target`): on a fresh deployment
`token_dim` does not exist yet and nothing but the transformation creates it
(`infra/transformation/storage/main.tf` documents this invariant for both Iceberg tables).
`prepare_dim_target` mirrors `silver_persistence.prepare_target` for the fact table: create-if-absent
and race-safe (a worker that loses the `create_table` race loads the winner's table), then the
schema is validated. There is **no in-place schema evolution**: a `token_dim` with any other schema
is refused fail-closed with a message naming the six expected columns, before any fact is written.
Drop such a table and the next object creates the 6-column one. Do not roll the image back to a
version that expects a different `token_dim` schema (it would refuse the table), and always
deploy `silver` and `silver_backfill` together.

**Bounded file count.** Every `token_dim` commit adds a data file and nothing else compacts this
table, so `apply_metadata` rewrites the WHOLE table into one file in the same snapshot-fenced
transaction as soon as it holds more than `compact_files` (8) data files; the steady state is at
most 9 files however many objects are processed. That path stays fail-closed on the rows/bytes
caps, the `max_compact_files` (256) ceiling (also applied by `read_metadata`), duplicate keys and
the output row cap (`max_rows`); a replay still commits nothing.

**Bounded file count of `icp_swap_events` (in-job compaction).** Every loaded object appends one
data file, and the event_id lookup (`silver_persistence._plan_changes`) selects every file whose
event-time statistics overlap the incoming object. Backfill objects span wide, overlapping time
ranges inside one year partition, so the selected-file count grew with every object until
`SILVER_MAX_SELECTED_FILES` (64 on `silver_backfill`) failed the load with
`selected file/row/byte cap exceeded`. The job now compacts the table itself
(`fact_compaction.py`); **Glue table optimizers are not relied on for this** (they run at most every
24 h, and they commit concurrently with our writers).

- *Approach.* PyIceberg 0.8.1 has no rewrite/compaction API (`Operation.REPLACE` is only an enum
  value). Compaction reads the small files of one year partition (`ArrowScan`), sorts the rows by
  `(event_timestamp, event_id)`, writes them as files of at most `SILVER_COMPACT_TARGET_FILE_ROWS`
  rows with the writer behind `Transaction.append`, and replaces exactly the input files in ONE
  overwrite snapshot (`delete_data_file` / `append_data_file`, the plumbing `Transaction.delete`
  uses for its file rewrites). The snapshot summary is computed incrementally (PyIceberg 0.8.1
  would summarize any OVERWRITE as a full-table truncate). Each snapshot carries
  `icp-swap.compaction = true`.
- *Commit discipline.* One transaction per slice (a partition within the caps is one slice),
  fenced by `AssertRefSnapshotId(main)` exactly like the ingest path: a concurrent commit rejects
  it (`CommitFailedException`), the staged files are deleted and the slice is re-planned (bounded
  jittered retries, `SILVER_MAX_RETRIES`). Any other commit error leaves the staged files alone.
- *Correctness.* The input must have unique `event_id` (a duplicate is refused, not collapsed:
  the table would violate the dedup invariant); the output row count, the written record counts
  and, after the commit, a read-back of the new files through a fresh catalog load (all columns,
  sorted by `event_id`) must equal the input. Tests prove the row set is unchanged, including
  conflict survivors and replays, and that the dedup rules still hold against compacted files.
- *Bounds.* Per transaction `SILVER_COMPACT_MAX_INPUT_FILES` / `_ROWS` / `_BYTES`; a partition
  above them is compacted in slices (small files in event-time order), a single file above a cap
  is refused (never truncated). One invocation deadline (`SILVER_DEADLINE_SECONDS`, remaining
  Lambda time minus the margin); a new slice starts only if 1.5 x the previous slice duration is
  left. When committed progress leaves work, the run raises `MaintenanceIncomplete` and the
  state machine retries exactly that error (`maintenance_incomplete_max_attempts`), each attempt
  resuming.
- *When it runs (cost).* (1) `mode=compact`, once after the Map of each state machine (`-backfill` and `-incremental`); it compacts a partition only when it holds more than
  `SILVER_COMPACT_PARTITION_FILE_THRESHOLD` (7) files, so a quiet day reads only the partitions that grew.
  (2) On `silver_backfill` only, a per-object trigger: before an object's lookup, one extra
  manifest-only planning pass counts the files the lookup would select; above
  `SILVER_COMPACT_TRIGGER_SELECTED_FILES` (48 of the 64 cap) the object's year partitions are
  compacted first. That pass is the only per-object cost; a rewrite happens roughly once per
  "trigger minus files after compaction" objects, not per object. Without (2) a single load of
  more than ~64 overlapping objects would still fail inside the Map, before (1) runs.
- *Known limit.* A wide object's lookup selects every file of the year partitions it spans,
  compacted or not: compaction bounds the FILE count, not the selected ROWS. Once a year
  partition holds more than `SILVER_MAX_SELECTED_ROWS` (1,000,000 on `silver_backfill`) rows, a
  wide object fails on the row/byte cap instead. Narrow (daily) objects profit from the
  time-sorted files and select only the files of their days.

## Modes

In the deployed pipeline the transformation is started by ingestion: each `IngestionRunFinished` event
starts the `-incremental` state machine (resource `silver_incremental_load`) with `{"mode":"list","prefixes":[...]}` for exactly the prefixes
the run wrote (see `docs/RUNBOOK.md`, section 7). The modes below are what that machinery and the ad-hoc
tools invoke.


- `incremental` (default): processes the raw objects of one UTC day (`date`, default yesterday),
  skipping the one-time `transactions_backfill_*.csv` bulk files.
- `list` / `backfill_file`: the load state machines list the raw CSV keys under the given
  prefixes once (output: an S3 manifest pointer, see "`list` output contract"), then invoke
  `backfill_file` once per key (on `silver` in `-incremental`, on `silver_backfill` in `-backfill`).
- `token_metadata`: read-only lookup of `token_dim` rows for `ledger_ids` (a list of non-blank
  strings, at most 1,024); the encoded response is bounded (see Operating limits).
- `compact` (no keys): post-load table maintenance, in this order: compaction of
  `icp_swap_events`, snapshot retention of `icp_swap_events` and `token_dim`, orphan-file cleanup
  of `token_dim`. Run by BOTH state machines (`CompactTable` step: after the Map, on `silver_backfill`
  or `silver` respectively), always before the quality check.
  Returns only counts (`files_before/after`, `rows_before/after`, `commits`, per-table
  `retention`, `orphans`), well below the 256 KiB state limit; a missing fact table returns
  `{"mode": "compact", "skipped": "table_absent"}`.
- Data quality is a separate Lambda (`quality_job.handler`, below), not a `silver_job` mode.

## Data quality (`quality` Lambda)

It is the last step (`CheckQuality`) of both load state machines, so it runs
right after the data it checks. `quality_job.handler` reads the two Silver tables through PyIceberg (Glue catalog, only the columns the
checks need), streams them into a DuckDB file under `/tmp` as `main.icp_swap_events` and `main.token_dim`,
and runs `dbt test` on the project in `src/quality_dbt/` (dbt-core + dbt-duckdb, pinned in
`src/requirements.txt`; no dbt packages, so nothing is downloaded at run time).

| check | where | severity |
| --- | --- | --- |
| `event_id` not null + unique, `action` / `contract_version` / `quality_status` accepted values, `ingested_at` not null | `models/sources.yml` | error |
| amounts non-negative, no timestamp in the future | `models/sources.yml`, `macros/generic_tests.sql` | error |
| content pairs inside the seam window (same content, timestamps ≤ 2 s apart) | `tests/no_seam_content_duplicates.sql` | warn |
| ICP ledger recorded with standard `ICP` | `tests/icp_ledger_standard.sql` | error |
| `token_dim.ledger_id` not null + unique | `models/sources.yml` | error |
| missing calendar days between first and last event | `tests/coverage_no_missing_days.sql` | warn |
| rows flagged `unresolved` / `conflict` | `tests/quality_status_flags.sql` | warn |
| data older than 72 h, NULL pool/ledger share above 1 %, unknown ledgers vs `token_dim` | `models/sources.yml` | warn |

The handler returns the report (`ok`, per-check status and failure counts). A failing `error`-severity
test raises `QualityCheckFailed`: the state machine execution fails (a failed test is not retried) and the
Lambda error alarm notifies the alerts topic; `warn` results are
reported but never fail the run. `{"fail_on_error": false}` in the event returns the report without
raising. A missing fact table is reported as skipped (nothing loaded yet). The scan is bounded from
manifest metadata before any data file is read (files, rows, bytes) and by a soft deadline.

**Seam check severity.** `no_seam_content_duplicates` applies exactly the reconciliation's match
rule (same pool, ledgers, action, amounts; timestamps within `DEFAULT_TOLERANCE_SECONDS` = 2 s; the
old check required an exact timestamp and missed pairs 1-2 s apart). It is `warn`, not `error`: the
reconciliation drops a B row only for a positive 1:1 match against an A row, so two genuine
content-identical trades seconds apart are legitimately kept; the loaded columns cannot tell them
from a missed A/B pair, and an `error` on immutable facts would fail every later run forever with no
remediation. A reported pair is a lead to investigate through `lineage_ref`.

**Time budget.** One deadline covers the load AND `dbt test` (the earlier of
`QUALITY_CHECK_DEADLINE_SECONDS` after the start and the Lambda's remaining time minus
`QUALITY_CHECK_DEADLINE_MARGIN_SECONDS`). dbt runs in a child process killed when the remaining
budget is spent, and is not started with less than `QUALITY_CHECK_MIN_DBT_SECONDS` left; both end in
a clear `QualityCheckError` instead of a hard Lambda timeout after a long load.

**Scale limit (known).** Every run reloads the whole fact table (selected columns) into DuckDB, so
cost grows with the table; the 30,000,000-row / 8 GiB / 20,000-file caps are the ceiling of this
design and a run above them fails with a message naming the measured size and the variable to
change. **Follow-up:** windowed checks (only partitions/days written since the last successful run,
plus a periodic full run on a larger runner).

Local tests (`test/test_quality_job.py`) run the real dbt project against small tables in a SQLite-backed
Iceberg catalog.

## Known source artefacts (`quality_status = 'unresolved'`)

Some canister swap records are degenerate and are kept as `unresolved` with NULL token amounts
(`invalid_swap_shape`), as the contract requires. They come from the source, not the transform.
Observed on the full load (about 5,262 rows): most come from one canister file where one leg is
`1e-8` (the smallest unit), the other leg is `0`, `amount_usd` and `token0_price` are `0`, and
liquidity fields are `0`. The sender is nearly always the same principal and the events are
dense in time. They look like dust or spam swaps recorded as `swap`. This is not verified
on-chain. Filter `quality_status = 'ok'` in any volume or count analysis. The rule is deliberate:
a swap needs two positive legs, and amounts are never repaired.

## Table maintenance (in-job; Glue table optimizers not relied on)

All Iceberg maintenance runs in our own job, `mode=compact` (see Modes), serially at the end of
BOTH state machines (`-backfill`: after the Map, on `silver_backfill`; `-incremental`: after
the Map, on `silver`), before the quality check. The order is fixed: compaction, then
snapshot retention, then orphan cleanup.

- **Compaction** (`icp_swap_events`): see "Bounded file count of `icp_swap_events`" above.
- **Snapshot retention** (both tables, `table_maintenance.expire_snapshots`). PyIceberg 0.8.1 has
  no expire-snapshots API: `RemoveSnapshotsUpdate` exists as a model but has no metadata-update
  handler, so the module registers one (drops the snapshots, prunes the snapshot log like Iceberg
  Java, refuses snapshots that a branch or tag references) and commits it through the public
  `Catalog.commit_table` with `AssertTableUUID` + `AssertRefSnapshotId(main)`. A snapshot expires
  when it is older than `snapshot_retention_days` (7) and not among the newest
  `snapshots_to_retain` (1), not current and not referenced by a ref. Only after a confirmed
  commit are the files deleted that only expired snapshots reach (manifest lists, manifests, data
  files no retained snapshot reads); a rejected commit re-plans, an unknown outcome deletes
  nothing. Bounded per run: at most `SILVER_MAINT_MAX_DELETE_FILES` deletions and
  `SILVER_MAINT_MAX_MANIFESTS` manifests read; a larger backlog is reported (`pending`) and
  continued by the next run (after every load); a single snapshot whose files alone
  exceed the delete cap is refused.
- **Orphan-file cleanup** (`token_dim` only, `table_maintenance.remove_orphan_files`). It lists
  only `<table>/data/` and `<table>/metadata/`, then loads the metadata, and deletes a file only if
  no snapshot (any manifest entry), the current metadata file or the metadata log references it
  AND it is older than `orphan_file_retention_days` (3, at least 1): files of a writer that has
  not committed yet look orphaned. `icp_swap_events` is excluded on purpose (its per-object
  quarantine artifacts live in the table location and are referenced by no Iceberg metadata); the
  function refuses any table whose location overlaps it. Never run from a per-object path.

**Concurrency.** An incremental and a backfill execution can overlap (different functions). Every
maintenance commit is optimistic and snapshot-fenced, so a writer that commits first makes the
maintenance commit fail and re-plan; it can never overwrite that writer's rows (tested with a real
concurrent ingest for compaction and for retention). Under sustained contention the bounded retry
budget is exhausted and the step fails closed (the data is untouched; the next run catches up).

**Rollback trade-off.** Retention deletes the files of expired snapshots, so time travel and
rollback reach back `snapshot_retention_days` (7). The processed bucket is versioned and keeps
noncurrent versions for `processed_noncurrent_version_expiration_days` (90): for 90 days after a
file was deleted it can still be restored from its noncurrent version (manually, object by
object). Retention is therefore capped at 60 days (Terraform validation and
`SILVER_NONCURRENT_VERSION_DAYS` check in the code) to keep 30 days of headroom. Longer retention
means more snapshots and files kept (storage cost, slower planning); shorter means a shorter
rollback window.

**Glue table optimizers (`enable_table_optimizers`).** The Terraform resources exist behind the flag (default `false`). Recommendation: **leave `enable_table_optimizers = false` (turn it off
where it was enabled).** With it on, Glue becomes a second writer on the same tables: its
compaction commits concurrently with our loads (each of its commits makes our fenced commit retry,
and under a long load it can exhaust our retry budget), its binpack output is not time-sorted (the
lookup pruning above relies on time-sorted files), and its retention would expire/delete on its
own schedule independently of our bounded per-run retention. Our job already covers compaction,
retention and the `token_dim` orphan cleanup.
