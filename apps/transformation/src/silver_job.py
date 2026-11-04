"""Lambda handler for the icp_swap_events Silver transformation (registered-mode only).

Every write path routes through ``silver_persistence`` (the flat
``silver-flat-v1`` contract: ``event_id`` dedup against the Iceberg fact table, fail-closed
bounded ingestion, quarantine-on-dirty-row) and ``token_dimension`` (current-label
metadata merge: latest-observed symbol/name win, decimals/standard fill-only). The same image
backs two Lambdas: ``silver`` (2048 MB; ``list``,
``incremental``, ``token_metadata``) and ``silver_backfill`` (10240 MB; only
``backfill_file`` from the backfill state machine); the data-quality checks run in their own
Lambda (``quality_job``). Modes: ``list`` (enumerate raw CSV keys under given
prefixes inside ``RAW_PREFIX``; writes them as a JSON-array manifest to the processed bucket and
returns ``{manifest_bucket, manifest_key, count}``), ``backfill_file`` (ingest one explicit S3 key — used by the
one-time historical reload), ``incremental`` (ingest one day's worth of
non-backfill objects under the configured raw prefix), ``token_metadata``
(bounded read-only lookup for a list of ledger IDs), ``compact`` (post-load table maintenance:
compaction, snapshot retention, token_dim orphan cleanup; last step before the quality check in
both state machines).
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from collections.abc import Iterable
from datetime import UTC, date, datetime, timedelta
from typing import Any

import boto3
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError

from utils.logging import get_logger, log_event
from write_planning import incremental_prefix, is_backfill_object

LOG = get_logger(__name__)

RAW_BUCKET = os.environ["RAW_BUCKET"]
PROCESSED_BUCKET = os.environ["PROCESSED_BUCKET"]
GLUE_DATABASE = os.environ["GLUE_DATABASE"]
TABLE_NAME = os.environ["TABLE_NAME"]
RAW_PREFIX = os.environ.get("RAW_PREFIX", "icp-swap-elt/")
REGION = os.environ.get("AWS_REGION_NAME", os.environ.get("AWS_REGION", "us-east-1"))

_s3 = boto3.client("s3")

# One-time Dataset A / Dataset B seam reconciliation (see seam_reconciliation.py for
# the full rationale). Deliberately scoped to the exact pair of raw objects that overlap
# — this is a one-time historical-reload fixup, not a general ongoing mechanism, so the
# keys and window are fixed constants rather than derived at runtime. Never applies to
# any other key. The two keys are fixed relative to RAW_PREFIX, so a deployment whose
# historical data lives under a different raw prefix (set via ``raw_prefix``) matches the
# same objects.
SEAM_B_KEY = RAW_PREFIX + (
    "rest/incremental/2026-06-23T11-49-42-924Z/"
    "transactions_backfill_1782215383463_p001001.csv"
)
SEAM_A_KEY = RAW_PREFIX + "canister/2026-06-23T10-25-45-642Z/0071_3326b-qiaaa-aaaag-qnlja-cai.csv"
SEAM_WINDOW_START = datetime(2026, 6, 7, 0, 22, 33, tzinfo=UTC)
SEAM_WINDOW_END = datetime(2026, 6, 7, 6, 37, 26, tzinfo=UTC)


#: Seconds kept free before the Lambda hard timeout: the publication deadline handed to
#: ingest_one is at most ``remaining_time - margin`` so a commit is never cut off mid-way.
DEFAULT_DEADLINE_MARGIN_SECONDS = 15
#: List-mode manifest objects (M8): written to the processed bucket under this prefix; the
#: state machine reads the key list from S3 instead of from the 256 KiB-capped state output.
LIST_MANIFEST_PREFIX = "silver-manifests/list/"
#: Structured log markers that an operator alarm must match (metric filter on ``marker``).
MARKER_EMPTY_SOURCE = "SILVER_EMPTY_SOURCE"
MARKER_ROWS_QUARANTINED = "SILVER_ROWS_QUARANTINED"
MARKER_TOKEN_DIM_FAILED = "TOKEN_DIM_UPDATE_FAILED"


def _catalog():
    return load_catalog("glue", **{"type": "glue", "s3.region": REGION})


def _positive_int_env(name: str, default: int) -> int:
    value = int(os.environ.get(name, str(default)))
    if value <= 0:
        raise ValueError(f"{name} must be positive")
    return value


def _invocation_deadline(context: Any, started: float) -> float:
    """ONE absolute ``time.monotonic()`` deadline for the whole invocation.

    The earlier of ``SILVER_DEADLINE_SECONDS`` after the invocation started and the Lambda's
    remaining time minus ``SILVER_DEADLINE_MARGIN_SECONDS``. Every key of an incremental run
    and every phase of ``_process_key`` share it (no per-key fresh budget).
    """
    soft = _positive_int_env("SILVER_DEADLINE_SECONDS", 45)
    deadline = started + soft
    remaining = getattr(context, "get_remaining_time_in_millis", None)
    if callable(remaining):
        margin = _positive_int_env(
            "SILVER_DEADLINE_MARGIN_SECONDS", DEFAULT_DEADLINE_MARGIN_SECONDS
        )
        deadline = min(deadline, started + remaining() / 1000.0 - margin)
    return deadline


def _validated_prefixes(prefixes: Any) -> list[str]:
    """Every listed prefix must be a nonblank string inside the configured RAW_PREFIX: an
    empty prefix would list (and then load) the whole raw bucket."""
    if not isinstance(prefixes, list):
        raise ValueError("prefixes must be a list of strings")
    checked = []
    for prefix in prefixes:
        if not isinstance(prefix, str) or not prefix.strip():
            raise ValueError("every prefix must be a nonblank string")
        if not RAW_PREFIX or not prefix.startswith(RAW_PREFIX):
            raise ValueError(f"prefix {prefix!r} is outside the configured raw prefix")
        checked.append(prefix)
    return checked


def _write_list_manifest(keys: list[str]) -> dict[str, Any]:
    """Persist the key list as a JSON array (content-addressed, so a retried list writes the
    same object) and return the small pointer the state machine consumes."""
    body = json.dumps(keys, separators=(",", ":")).encode("utf-8")
    key = f"{LIST_MANIFEST_PREFIX}{hashlib.sha256(body).hexdigest()}.json"
    _s3.put_object(
        Bucket=PROCESSED_BUCKET, Key=key, Body=body, ContentType="application/json"
    )
    return {"manifest_bucket": PROCESSED_BUCKET, "manifest_key": key, "count": len(keys)}


def _list_csv_keys(prefixes: Iterable[str]) -> list[str]:
    keys: list[str] = []
    paginator = _s3.get_paginator("list_objects_v2")
    for prefix in prefixes:
        for page in paginator.paginate(Bucket=RAW_BUCKET, Prefix=prefix):
            for obj in page.get("Contents", []):
                if obj["Key"].lower().endswith(".csv"):
                    keys.append(obj["Key"])
    return keys


def _process_key(key: str, deadline: float | None = None) -> dict[str, Any]:
    """Load one raw object: facts first (``ingest_one``), then the best-effort token_dim.

    ``deadline`` is the invocation's absolute monotonic deadline (``_invocation_deadline``);
    ``None`` (direct callers/tests) means ``SILVER_DEADLINE_SECONDS`` from now.
    """
    if deadline is None:
        deadline = time.monotonic() + _positive_int_env("SILVER_DEADLINE_SECONDS", 45)
    log_event(LOG, logging.INFO, "processing object", key=key)
    from silver_persistence import (
        IngestionError,
        _generation,
        ingest_one,
        prepare_target,
    )

    if time.monotonic() >= deadline:
        raise IngestionError(f"invocation deadline exceeded before processing {key}")
    generation = _generation()
    catalog = _catalog()
    identifier = f"{GLUE_DATABASE}.{TABLE_NAME}"
    table = prepare_target(catalog, identifier, generation)
    max_bytes = int(os.environ.get("SILVER_MAX_SOURCE_BYTES", str(16 * 1024 * 1024)))
    if max_bytes <= 0:
        raise IngestionError("SILVER_MAX_SOURCE_BYTES must be positive")
    max_rows = int(os.environ.get("SILVER_MAX_SOURCE_ROWS", "100000"))
    if max_rows <= 0:
        raise IngestionError("SILVER_MAX_SOURCE_ROWS must be positive")
    source = _s3.get_object(Bucket=RAW_BUCKET, Key=key)
    body = source["Body"].read(max_bytes + 1)
    if len(body) > max_bytes:
        raise IngestionError("source byte cap exceeded")
    version = source.get("VersionId") or source.get("ETag") or ""
    seam_report = None
    if key == SEAM_B_KEY:
        import seam_reconciliation

        a_source = _s3.get_object(Bucket=RAW_BUCKET, Key=SEAM_A_KEY)
        a_body = a_source["Body"].read(max_bytes + 1)
        if len(a_body) > max_bytes:
            raise IngestionError("seam dataset-A source byte cap exceeded")
        # Same row bound as every other object (SILVER_MAX_SOURCE_ROWS), not a literal.
        a_index = seam_reconciliation.dataset_a_window_index(
            a_body, SEAM_WINDOW_START, SEAM_WINDOW_END, max_records=max_rows
        )
        body, seam_report = seam_reconciliation.clean_dataset_b_overlap(
            body, a_index, SEAM_WINDOW_START, SEAM_WINDOW_END, max_records=max_rows
        )
        # The cleaned bytes no longer match the B object's own VersionId/ETag — fall
        # through to the sha256-of-content fallback below instead of mislabeling the
        # reconciled content with the original (now-wrong) source version.
        version = ""
        log_event(LOG, logging.INFO, "seam reconciliation applied", key=key, **seam_report)
    # Facts FIRST: they never depend on token_dim (README), so a dimension problem must not
    # block or roll back a fact load. A fact failure raises here and the dimension is not
    # touched for this object (a replay redoes both: facts as a no-op, dim idempotently).
    persisted = ingest_one(
        table,
        bucket=RAW_BUCKET,
        key=key,
        source_version=version,
        content=body,
        deadline=deadline,
    )
    dim_report = _apply_token_dim(catalog, key, body, version, max_rows, max_bytes, deadline)
    result = {"key": key, **persisted, "token_dim": dim_report}
    if seam_report is not None:
        result["seam_reconciliation"] = seam_report
    summary = _backfill_response(result)
    if persisted.get("completion") == "quarantine_durable_source_replayable":
        # header-only / empty object: no rows at all (the only file-level issues that are
        # not fatal); still alarm-worthy if it happens for a key that should have data.
        log_event(
            LOG,
            logging.WARNING,
            "object has no data rows; durable file-issue artifact written",
            marker=MARKER_EMPTY_SOURCE,
            **summary,
        )
    elif persisted.get("rows_quarantined"):
        log_event(
            LOG,
            logging.WARNING,
            "object rows quarantined",
            marker=MARKER_ROWS_QUARANTINED,
            key=key,
            rows_quarantined=persisted["rows_quarantined"],
            quarantine_uri=persisted.get("quarantine_uri"),
        )
    # Log the slim summary, never the full per-ledger lists: one CloudWatch event is capped
    # at 256 KB and a large object's token_dim readback alone can exceed it.
    log_event(LOG, logging.INFO, "object complete", **summary)
    return result


def _apply_token_dim(catalog, key, body, version, max_rows, max_bytes, deadline):
    """Best-effort token_dim merge for one object (runs AFTER the facts committed).

    A ``MetadataError`` (contract/bounds violation, schema refusal, retry or deadline
    exhaustion) is reported in the result and logged as a WARNING with
    ``marker=TOKEN_DIM_UPDATE_FAILED``; it never fails the invocation, because the facts are
    already published. Recovery: fix the cause and replay the object (``backfill_file``):
    the fact load is an exact-replay no-op and the dimension merge is idempotent.
    """
    from token_dimension import (
        DEFAULT_LIMITS,
        MetadataError,
        apply_metadata,
        extract_metadata,
        prepare_dim_target,
        read_metadata,
    )

    try:
        header = body.decode("utf-8").splitlines()[0].lower()
    except (UnicodeDecodeError, IndexError):
        header = ""
    dataset = "B" if "tx_hash" in header else "A"
    digest = hashlib.sha256(body).hexdigest()
    dim_report: dict[str, Any] = {
        "snapshot_id": None,
        "changed_ledgers": [],
        "missing_attributes": {},
        "conflicts_resolved": 0,
        "conflicts": [],
    }
    try:
        observations, metadata_issues = extract_metadata(
            body,
            dataset,
            {
                "bucket": RAW_BUCKET,
                "key": key,
                "source_version": version or f"sha256:{digest}",
                "content_sha256": digest,
            },
            max_records=max_rows,
            max_bytes=max_bytes,
        )
        if not observations:
            dim_report["issues"] = metadata_issues
            return dim_report
        dim_table = prepare_dim_target(catalog, f"{GLUE_DATABASE}.token_dim")
        # ONE phase deadline across apply_metadata + readback (never re-started inside each
        # call), additionally bounded by the invocation deadline.
        phase_deadline = min(
            time.monotonic() + DEFAULT_LIMITS["deadline_seconds"], deadline
        )
        dim_report = apply_metadata(dim_table, observations, deadline=phase_deadline)
        remaining_bytes = DEFAULT_LIMITS["max_bytes"] - dim_report.get("bytes_read", 0)
        dim_report["readback"] = read_metadata(
            dim_table,
            [item["ledger_id"] for item in observations],
            limits={"max_bytes": max(0, remaining_bytes)},
            deadline=phase_deadline,
        )
        return dim_report
    except MetadataError as exc:
        log_event(
            LOG,
            logging.WARNING,
            "token_dim update failed; facts are published, replay the object to retry",
            marker=MARKER_TOKEN_DIM_FAILED,
            key=key,
            error=str(exc),
        )
        return {**dim_report, "error": str(exc)}


_RESPONSE_MAX_CONFLICTS = 3


def _backfill_response(result: dict[str, Any]) -> dict[str, Any]:
    """Slim ``_process_key`` result for the Step Functions caller.

    Step Functions caps a state's output at 256 KiB and a backfill run collects one response
    per object, so the per-ledger ``token_dim`` lists (readback, changed/created ledgers,
    missing attributes, the full conflict list) are replaced by counts and the first few
    conflicts. Everything the reload verification reads (rows_written, replayed, lookup,
    verify, seam report, conflicts_resolved) is kept as is; the complete result is still in
    the structured ``object complete`` log line written by ``_process_key``.
    """
    slim = {k: v for k, v in result.items() if k != "token_dim"}
    dim = result.get("token_dim")
    if dim is not None:
        slim_dim = {
            k: v
            for k, v in dim.items()
            if k
            not in (
                "readback",
                "changed_ledgers",
                "created_ledgers",
                "missing_attributes",
                "conflicts",
                "issues",
            )
        }
        if "issues" in dim:
            slim_dim["issue_count"] = len(dim["issues"])
        for name in ("changed_ledgers", "created_ledgers", "missing_attributes"):
            if name in dim:
                slim_dim[name] = len(dim[name])
        if "conflicts" in dim:
            slim_dim["conflicts"] = list(dim["conflicts"])[:_RESPONSE_MAX_CONFLICTS]
        slim["token_dim"] = slim_dim
    return slim


def _maintain(deadline: float) -> dict[str, Any]:
    """``compact`` mode: fact-table compaction, then snapshot retention (both tables), then
    orphan-file cleanup (``token_dim`` only), in that order, under ONE invocation deadline.

    Returns a slim summary (counts only; the Step Functions state output is capped at
    256 KiB). ``MaintenanceIncomplete`` (bounded progress committed, work left) propagates so
    the state machine's Retry re-runs the step; every other error fails closed.
    """
    import fact_compaction

    catalog = _catalog()
    try:
        table = catalog.load_table(f"{GLUE_DATABASE}.{TABLE_NAME}")
    except NoSuchTableError:
        return {"mode": "compact", "skipped": "table_absent"}
    summary = fact_compaction.compact_table(table, deadline=deadline)
    result: dict[str, Any] = {
        "mode": "compact",
        **{
            key: summary[key]
            for key in (
                "complete",
                "commits",
                "partitions_compacted",
                "files_before",
                "files_after",
                "rows_before",
                "rows_after",
            )
        },
    }
    result.update(_maintain_snapshots_and_orphans(catalog, table, deadline))
    log_event(LOG, logging.INFO, "maintenance complete", **result)
    return result


def _maintain_snapshots_and_orphans(catalog, fact_table, deadline: float) -> dict[str, Any]:
    """Snapshot retention on both tables, then orphan cleanup on ``token_dim`` only (the fact
    table's location holds unreferenced quarantine artifacts and is passed as protected)."""
    import table_maintenance

    retention: dict[str, Any] = {}
    report = table_maintenance.expire_snapshots(fact_table, deadline=deadline)
    retention[TABLE_NAME] = {k: report[k] for k in ("expired", "pending", "files_deleted")}
    try:
        dim = catalog.load_table(f"{GLUE_DATABASE}.token_dim")
    except NoSuchTableError:
        return {"retention": retention}
    report = table_maintenance.expire_snapshots(dim, deadline=deadline)
    retention["token_dim"] = {k: report[k] for k in ("expired", "pending", "files_deleted")}
    orphans = table_maintenance.remove_orphan_files(
        dim, protected_locations=(fact_table.location(),), deadline=deadline
    )
    return {
        "retention": retention,
        "orphans": {
            "token_dim": {k: orphans[k] for k in ("listed", "orphans_deleted", "orphans_pending")}
        },
    }


def _process_incremental(target: date, deadline: float | None = None) -> dict[str, Any]:
    prefix = incremental_prefix(RAW_PREFIX, target.isoformat())
    # Skip one-time bulk backfill files: the ingestion stage drops them into the same date folder as
    # the daily incremental feed, and they are far too large for a single daily incremental
    # invocation (they are loaded once by the backfill state machine on the dedicated,
    # larger silver_backfill Lambda). See is_backfill_object().
    keys = [k for k in _list_csv_keys([prefix]) if not is_backfill_object(k)]
    log_event(
        LOG, logging.INFO, "incremental run", date=target.isoformat(), files=len(keys)
    )
    totals = {
        "date": target.isoformat(),
        "files": len(keys),
        "rows_written": 0,
        "rows_quarantined": 0,
        "rows_observed": 0,
        "issue_count": 0,
    }
    for key in keys:
        # ONE invocation deadline shared by every key (no fresh budget per key).
        r = _process_key(key, deadline)
        totals["rows_written"] += r["rows_written"]
        totals["rows_quarantined"] += r["rows_quarantined"]
        totals["rows_observed"] += r.get("rows_observed", 0)
        totals["issue_count"] += r.get("issue_count", 0)
    return totals


def handler(event: dict[str, Any], context: Any = None) -> dict[str, Any]:
    """Dispatch on ``event['mode']``: list | incremental | backfill_file | token_metadata |
    compact."""
    started = time.monotonic()
    mode = event.get("mode", "incremental")
    log_event(LOG, logging.INFO, "invoked", mode=mode)

    if mode == "list":
        # Output contract: {manifest_bucket, manifest_key, count}; the key list itself is a
        # JSON array in S3 (a state output is capped at 256 KiB, ~2,400 keys inline).
        prefixes = _validated_prefixes(event.get("prefixes", []))
        return _write_list_manifest(_list_csv_keys(prefixes))

    if mode == "token_metadata":
        from token_dimension import DEFAULT_LIMITS, MetadataError, read_metadata

        requested = event.get("ledger_ids")
        if not isinstance(requested, list) or any(
            not isinstance(item, str) or not item.strip() for item in requested
        ):
            raise ValueError("ledger_ids must be a list of nonblank strings")
        ledger_ids = list(dict.fromkeys(item.strip() for item in requested))
        if len(ledger_ids) > DEFAULT_LIMITS["max_ledgers"]:
            raise ValueError("metadata ledger cap exceeded")
        try:
            table = _catalog().load_table(f"{GLUE_DATABASE}.token_dim")
        except NoSuchTableError:
            table = None
        result = read_metadata(table, ledger_ids)
        # Public-response bound, distinct from the internal bounded readback budget used
        # above (physical parquet bytes read): bound the ENCODED response itself so a caller
        # never receives a payload that can exceed the Lambda synchronous invoke response
        # limit (6 MB), with margin. Oversize is rejected outright, never truncated.
        max_response_bytes = int(
            os.environ.get("TOKEN_METADATA_MAX_RESPONSE_BYTES", str(4 * 1024 * 1024))
        )
        encoded_bytes = len(json.dumps(result).encode("utf-8"))
        if encoded_bytes > max_response_bytes:
            raise MetadataError(
                "token_metadata public response exceeds encoded response byte bound"
            )
        return result

    if mode == "backfill_file":
        key = event["key"]
        return _backfill_response(
            _process_key(key, _invocation_deadline(context, started))
        )

    if mode == "compact":
        # Post-load table maintenance, run serially once the backfill Map has finished.
        return _maintain(_invocation_deadline(context, started))

    if mode == "incremental":
        raw_date = (event.get("date") or "").strip()
        target = (
            date.fromisoformat(raw_date)
            if raw_date
            else (datetime.now(UTC).date() - timedelta(days=1))
        )
        return _process_incremental(target, _invocation_deadline(context, started))

    raise ValueError(f"unknown mode: {mode}")
