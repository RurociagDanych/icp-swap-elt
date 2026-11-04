"""Bounded local/S3-compatible atomic flat-Silver publication for one CSV object.

Deduplication model: ``event_id`` is the only dedup key and the Iceberg fact table
itself is the only dedup state. There is no cumulative index, no shard, no root pointer.

For every incoming record the fact table is looked up by ``event_id`` (pruned to the incoming
rows' UTC event days) and compared by payload hash (``_payload_key``):

* absent                      -> plain append;
* identical payload hash      -> no-op (idempotent replay; the stored row is left untouched);
* different payload hash      -> conflict: the row with the SMALLER hash survives (so the
  outcome does not depend on arrival order) and carries ``quality_status = 'conflict'``.
  This is a deterministic tie-break, not a recency rule: when two fetches differ only in the
  derived ``amount_usd`` the surviving value is not necessarily the fresher one (README:
  "Precedence for derived USD values").

Everything the object changes is published in ONE Iceberg transaction (delete of the exact
rows being rewritten, if any, plus the append) fenced by the table snapshot, so two
interleaved writers can never both insert the same new ``event_id``.

Provenance is deliberately cheap: ``lineage_ref`` is ``s3://<bucket>/<key>#<ordinal>`` (the
one-based logical CSV record ordinal); the raw CSVs in S3 remain the source of truth.
"""

from __future__ import annotations

import copy
import hashlib
import json
import os
import posixpath
import random
import re
import time
from datetime import date, datetime, time as datetime_time, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

import pyarrow as pa
from pyiceberg.exceptions import (
    CommitFailedException,
    NoSuchTableError,
    TableAlreadyExistsError,
)
from pyiceberg.expressions import And, GreaterThanOrEqual, In, IsNull, LessThan, Or
from pyiceberg.expressions.literals import TimestampLiteral
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import assign_fresh_schema_ids
from pyiceberg.transforms import YearTransform
from pyiceberg.utils.datetime import timestamptz_to_micros

from silver_csv import normalize_csv
from utils.retry import backoff_delay

# Must be imported before any scan is planned: makes PyIceberg's threaded manifest planning
# race-free (see iceberg_scan_safety for the root cause).
import iceberg_scan_safety  # noqa: F401  (import installs the guard)

FIELDS = (
    "event_id",
    "event_timestamp",
    "action",
    "pool_id",
    "token0_ledger_id",
    "token1_ledger_id",
    "token0_amount",
    "token1_amount",
    "amount_usd",
    "from_principal",
    "to_principal",
    "contract_version",
    "lineage_ref",
    "quality_status",
    "ingested_at",
)


def _field(name, data_type, required=False):
    return pa.field(name, data_type, nullable=not required)


ARROW_SCHEMA = pa.schema(
    [
        _field("event_id", pa.string(), True),
        _field("event_timestamp", pa.timestamp("us", tz="UTC")),
        _field("action", pa.string(), True),
        _field("pool_id", pa.string()),
        _field("token0_ledger_id", pa.string()),
        _field("token1_ledger_id", pa.string()),
        _field("token0_amount", pa.decimal128(38, 18)),
        _field("token1_amount", pa.decimal128(38, 18)),
        _field("amount_usd", pa.decimal128(38, 18)),
        _field("from_principal", pa.string()),
        _field("to_principal", pa.string()),
        _field("contract_version", pa.string(), True),
        _field("lineage_ref", pa.string(), True),
        _field("quality_status", pa.string(), True),
        _field("ingested_at", pa.timestamp("us", tz="UTC"), True),
    ]
)
GENERATION_PROPERTY = "silver.target.generation"
SCHEMA_PROPERTY = "silver.schema.version"
SCHEMA_VERSION = "silver-flat-v1"
#: Properties of a cumulative lineage index, which this design does not use. A table that
#: carries one was written under a different dedup model and must not be appended to.
_LEGACY_INDEX_PROPERTIES = ("silver.lineage.root", "silver.index.version")
#: Per-file quarantine artifacts live beside the table, never inside a data partition.
QUARANTINE_DIR = "silver-quarantine"
CONFLICT = "conflict"
PARTITION_FIELD_NAME = "event_timestamp_year"

#: File-level CSV defects. ``FATAL_FILE_ISSUES`` mean the object HAS rows that could not be
#: read (a mid-file csv.Error after a valid prefix, undecodable bytes, an ambiguous header): it
#: is refused before anything is published, because quarantining it and returning success
#: would silently lose those rows. ``header_only`` / blank ``missing_header`` carry no rows at
#: all (the REST incremental sink writes its header before it knows whether any row is new),
#: so they stay a durable-quarantine success with a WARNING marker.
FILE_ISSUE_CODES = frozenset(
    {
        "invalid_utf8",
        "missing_header",
        "header_only",
        "blank_header",
        "duplicate_header",
        "malformed_csv",
    }
)
FATAL_FILE_ISSUES = frozenset(
    {"invalid_utf8", "blank_header", "duplicate_header", "malformed_csv"}
)

#: Commit-conflict retry policy (``CommitFailedException`` = another writer committed first).
#: Attempts default to ``DEFAULT_MAX_RETRIES`` (``SILVER_MAX_RETRIES``); between attempts an
#: equal-jitter exponential backoff (``utils.retry.backoff_delay``) waits, and the sum of all
#: waits is capped by ``_RETRY_MAX_TOTAL_SLEEP_SECONDS`` and by the publication deadline.
#: Exhausting any bound raises (fail-closed; nothing is published, the source is replayable).
DEFAULT_MAX_RETRIES = 6
_RETRY_BASE_SECONDS = 0.5
_RETRY_MAX_SLEEP_SECONDS = 8.0
_RETRY_MAX_TOTAL_SLEEP_SECONDS = 30.0
_sleep = time.sleep
_jitter = random.random


class IngestionError(RuntimeError):
    """Fail-closed bounded-ingestion error; caller must leave source replayable."""


def _limit(name: str, default: int) -> int:
    value = int(os.environ.get(name, str(default)))
    if value <= 0:
        raise IngestionError(f"{name} must be positive")
    return value


def _generation() -> str:
    value = os.environ.get("SILVER_TARGET_GENERATION", "").strip()
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", value) or value in {
        ".",
        "..",
    }:
        raise IngestionError(
            "SILVER_TARGET_GENERATION must be an explicit safe path component"
        )
    return value


def _json_default(value):
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat(timespec="microseconds")
    raise TypeError(f"unsupported quarantine-artifact value: {type(value).__name__}")


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
        default=_json_default,
    ).encode("utf-8")


def _quarantine_namespace(table, generation: str) -> str:
    return table.location().rstrip("/") + f"/{QUARANTINE_DIR}/{generation}"


def _validate_metadata_uri(table, uri: str, generation: str, kind: str):
    """Confine a quarantine artifact reference to this table's generation namespace."""
    if kind != "quarantine":
        raise IngestionError(f"unsupported metadata artifact kind: {kind}")
    root = urlparse(table.location())
    ref = urlparse(uri)
    if (
        ref.scheme != root.scheme
        or ref.netloc != root.netloc
        or ref.query
        or ref.fragment
        or ref.scheme not in {"file", "s3"}
    ):
        raise IngestionError("foreign quarantine reference")
    base_path = unquote(root.path).rstrip("/") + f"/{QUARANTINE_DIR}/{generation}"
    ref_path = unquote(ref.path)
    if not re.fullmatch(r"quarantine-[0-9a-f]{64}\.json", posixpath.basename(ref_path)):
        raise IngestionError("invalid quarantine reference")
    if ref.scheme == "file":
        expected = Path(base_path).resolve()
        candidate = Path(ref_path).resolve()
        try:
            candidate.relative_to(expected)
        except ValueError as exc:
            raise IngestionError(
                "quarantine path escaped generation namespace"
            ) from exc
        if candidate.parent != expected:
            raise IngestionError(
                "quarantine path is not directly inside generation namespace"
            )
    else:
        expected = posixpath.normpath(base_path)
        candidate = posixpath.normpath(ref_path)
        if posixpath.dirname(candidate) != expected:
            raise IngestionError("quarantine path escaped generation namespace")


def _metadata_read(
    table, uri: str, cap: int, generation: str, kind: str
) -> tuple[dict[str, Any], int]:
    if cap <= 0:
        raise IngestionError("quarantine byte cap exceeded before read")
    _validate_metadata_uri(table, uri, generation, kind)
    inp = table.io.new_input(uri)
    # Check the stored size first: never preallocate ``cap + 1`` bytes just to read.
    if len(inp) > cap:
        raise IngestionError("quarantine artifact exceeds byte cap")
    with inp.open() as stream:
        raw = stream.read()
    if len(raw) > cap:
        raise IngestionError("quarantine artifact exceeds byte cap")
    try:
        decoded = json.loads(raw)
    except (ValueError, UnicodeDecodeError) as exc:
        raise IngestionError("corrupt quarantine artifact") from exc
    if not isinstance(decoded, dict) or _canonical_bytes(decoded) != raw:
        raise IngestionError("noncanonical or non-object quarantine artifact")
    digest = uri.rsplit("-", 1)[-1].removesuffix(".json")
    if hashlib.sha256(raw).hexdigest() != digest:
        raise IngestionError("quarantine artifact content hash mismatch")
    return decoded, len(raw)


def _metadata_write(
    table,
    uri: str,
    payload: bytes,
    generation: str,
    kind: str,
    cap: int = 0,
) -> None:
    _validate_metadata_uri(table, uri, generation, kind)
    try:
        with table.io.new_output(uri).create(overwrite=False) as stream:
            stream.write(payload)
    except (FileExistsError, OSError):
        existing, _ = _metadata_read(table, uri, cap, generation, kind)
        if _canonical_bytes(existing) != payload:
            raise IngestionError(
                "content-addressed metadata object exists with different bytes"
            )


def _validate_schema(table) -> None:
    actual = table.schema().as_arrow()
    if tuple(actual.names) != FIELDS:
        raise IngestionError("existing Silver schema mismatch; refusing migration")
    for expected, found in zip(ARROW_SCHEMA, actual, strict=True):
        string_alias = expected.type == pa.string() and found.type == pa.large_string()
        if (
            expected.type != found.type and not string_alias
        ) or expected.nullable != found.nullable:
            raise IngestionError(
                f"existing Silver field type/nullability mismatch: {expected.name} "
                f"expected={expected.type}/{expected.nullable} actual={found.type}/{found.nullable}"
            )


def _target_schema_and_spec():
    """The fact table's Iceberg schema (fresh field ids) and its yearly partition spec.

    Built once so ``create_table`` receives the spec and the table is born partitioned in
    the SAME catalog commit (a separate ``update_spec`` commit could be lost to a crash and
    leave an unpartitioned table). ``Catalog._convert_schema_if_needed`` is the conversion
    PyIceberg 0.8.1 itself applies to a pyarrow schema in ``create_table`` (pinned in
    requirements.txt); ``_validate_schema`` re-checks the result after creation.
    """
    from pyiceberg.catalog import Catalog

    schema = assign_fresh_schema_ids(Catalog._convert_schema_if_needed(ARROW_SCHEMA))
    source_id = schema.find_field("event_timestamp").field_id
    spec = PartitionSpec(
        PartitionField(
            source_id=source_id,
            field_id=1000,
            transform=YearTransform(),
            name=PARTITION_FIELD_NAME,
        )
    )
    return schema, spec


def _ensure_partitioned(catalog, identifier: str, table):
    """Every partition field must be a transform of ``event_timestamp`` (the lookup prunes on
    it). An UNPARTITIONED table is repaired in place only while it holds no snapshot (the
    crash window of the old two-commit create); a populated unpartitioned table is refused."""
    fields = table.spec().fields
    if fields:
        names = {table.schema().find_column_name(field.source_id) for field in fields}
        if names != {"event_timestamp"}:
            raise IngestionError(
                f"unexpected Silver partition spec on {sorted(names)}; refusing to append"
            )
        return table
    if table.current_snapshot() is not None:
        raise IngestionError(
            "existing Silver table is unpartitioned and already holds data; refusing to "
            "append (expected hidden partition year(event_timestamp))"
        )
    try:
        with table.update_spec() as update:
            update.add_field("event_timestamp", YearTransform(), PARTITION_FIELD_NAME)
    except CommitFailedException:
        pass  # a concurrent worker repaired it first; re-validated below
    table = catalog.load_table(identifier)
    if not table.spec().fields:
        raise IngestionError("Silver partition spec repair not confirmed")
    return table


def prepare_target(catalog, identifier: str, generation: str):
    """Load/create only an empty exact-schema target for the explicit generation.

    Create-or-load: at ``backfill_max_concurrency`` > 1 against a fresh catalog,
    multiple workers can all miss on ``load_table`` and race ``create_table``. Only
    one wins the create (and adds the partition spec below); every loser must load
    the now-existing table instead of raising ``TableAlreadyExistsError`` (catching
    that specific exception only — any other catalog/IO error still propagates). A
    table obtained this way runs through exactly the same generation and
    schema-compatibility checks below as a table that already existed.

    Generation guard (the cumulative lineage index is not used): a table without a
    generation marker must be completely empty; a marked table must carry exactly this
    generation, the current schema version, and at least one committed snapshot.
    """
    try:
        table = catalog.load_table(identifier)
    except NoSuchTableError:
        # Hidden yearly partitioning: year(event_timestamp), not a materialized column. A
        # daily partition produces a severe small-files problem at this table's write
        # cadence (one object per ingested CSV); yearly buckets keep the file count per
        # partition low while still giving Athena/Trino partition pruning. Passed to
        # create_table so schema and spec land in ONE commit.
        schema, spec = _target_schema_and_spec()
        try:
            table = catalog.create_table(identifier, schema=schema, partition_spec=spec)
        except TableAlreadyExistsError:
            table = catalog.load_table(identifier)
    _validate_schema(table)
    if any(name in table.properties for name in _LEGACY_INDEX_PROPERTIES):
        raise IngestionError(
            "table carries a legacy lineage index; refusing to append under the "
            "event_id dedup model"
        )
    table_generation = table.properties.get(GENERATION_PROPERTY)
    if table_generation is None:
        if (
            table.current_snapshot() is not None
            or table.properties.get("write.metadata.delete-after-commit.enabled")
        ):
            raise IngestionError(
                "nonempty unindexed Silver table; refusing raw acquisition"
            )
        return _ensure_partitioned(catalog, identifier, table)
    if table_generation != generation:
        raise IngestionError("target generation mismatch")
    if table.properties.get(SCHEMA_PROPERTY) != SCHEMA_VERSION:
        raise IngestionError("target schema version mismatch")
    if table.current_snapshot() is None:
        raise IngestionError("initialized Silver target has no committed snapshot")
    return _ensure_partitioned(catalog, identifier, table)


#: Columns that never take part in the payload hash. ``lineage_ref`` / ``ingested_at`` are
#: load provenance; ``quality_status`` is DERIVED (``ok`` | ``unresolved`` from the row's
#: issues, or ``conflict`` once a second payload was seen for the event_id), so a stored
#: survivor that carries ``conflict`` must hash exactly like the incoming payload it came from.
_HASH_EXCLUDED = frozenset({"lineage_ref", "ingested_at", "quality_status"})


def _payload_key(record: dict[str, Any]) -> str:
    """SHA-256 of the canonical payload; identical for an incoming row and the same row
    read back from Iceberg (Decimal(38,18) trailing zeros and tz-aware micros normalised)."""

    def encode(value):
        if isinstance(value, Decimal):
            sign, digits, exponent = value.as_tuple()
            if not isinstance(exponent, int):
                raise ValueError("non-finite Decimal cannot identify a payload")
            digits = list(digits)
            if not any(digits):
                return {"decimal": [0, [0], 0]}
            while len(digits) > 1 and digits[-1] == 0:
                digits.pop()
                exponent += 1
            return {"decimal": [sign, digits, exponent]}
        if isinstance(value, datetime):
            return value.astimezone(timezone.utc).isoformat(timespec="microseconds")
        return value

    payload = {}
    for key, value in record.items():
        if key in _HASH_EXCLUDED:
            continue
        if key in {"token0_amount", "token1_amount", "amount_usd"} and value is not None:
            value = value if isinstance(value, Decimal) else Decimal(value)
        payload[key] = encode(value)
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode()).hexdigest()


def _event_day(record: dict[str, Any]) -> date | None:
    value = record.get("event_timestamp")
    if value is None:
        return None
    if value.tzinfo is None:
        raise IngestionError("event timestamp lacks timezone")
    return value.astimezone(timezone.utc).date()


def _day_range(first: date, last: date):
    start = datetime.combine(first, datetime_time.min, tzinfo=timezone.utc)
    end = datetime.combine(last, datetime_time.min, tzinfo=timezone.utc) + timedelta(
        days=1
    )
    return And(
        GreaterThanOrEqual(
            "event_timestamp", TimestampLiteral(timestamptz_to_micros(start.isoformat()))
        ),
        LessThan(
            "event_timestamp", TimestampLiteral(timestamptz_to_micros(end.isoformat()))
        ),
    )


def _balanced_or(predicates: list):
    # A balanced tree keeps expression depth O(log n) however many disjoint day runs a
    # sparse object spans (a left-nested chain would hit the recursion limit).
    if len(predicates) == 1:
        return predicates[0]
    middle = len(predicates) // 2
    return Or(_balanced_or(predicates[:middle]), _balanced_or(predicates[middle:]))


def _lookup_filter(event_ids: list[str], days: set[date | None]):
    """``event_id IN (...)`` restricted to the UTC event days of the rows being looked up.

    Consecutive days are merged into one half-open range so a bulk object spanning months
    is a handful of predicates, not one per day. Pruning by day is what keeps the lookup
    bounded by the object's own time span instead of the whole table (known limit: an
    ``event_id`` that later arrives with a timestamp on a DIFFERENT day is not found).
    """
    predicates = []
    runs: list[list[date]] = []
    for day in sorted(d for d in days if d is not None):
        if runs and day == runs[-1][1] + timedelta(days=1):
            runs[-1][1] = day
        else:
            runs.append([day, day])
    predicates = [_day_range(first, last) for first, last in runs]
    if None in days:
        predicates.append(IsNull("event_timestamp"))
    return And(In("event_id", event_ids), _balanced_or(predicates))


def _group_incoming(normalized: list[dict[str, Any]]):
    """Collapse one object to a single candidate per ``event_id``.

    Returns ``(best, conflicted, days)`` where ``best[event_id] = (hash, record)`` is the
    smallest-hash payload (first occurrence wins an exact tie, i.e. the lowest ordinal),
    ``conflicted`` is the set of ids that carried more than one distinct payload inside
    this object and ``days`` every UTC event day any candidate falls on.
    """
    best: dict[str, tuple[str, dict[str, Any]]] = {}
    conflicted: set[str] = set()
    days: set[date | None] = set()
    for record in normalized:
        digest = _payload_key(record)
        days.add(_event_day(record))
        event_id = record["event_id"]
        current = best.get(event_id)
        if current is None:
            best[event_id] = (digest, record)
            continue
        if digest != current[0]:
            conflicted.add(event_id)
        if digest < current[0]:
            best[event_id] = (digest, record)
    return best, conflicted, days


def _scan_batches(scan, tasks):
    """Read exactly the already planned ``tasks`` of ``scan`` (one plan per scan: the files
    whose caps were checked are the files that are read)."""
    from pyiceberg.io.pyarrow import ArrowScan

    return ArrowScan(
        scan.table_metadata,
        scan.io,
        scan.projection(),
        scan.row_filter,
        scan.case_sensitive,
        scan.limit,
    ).to_record_batches(tasks)


def _plan_changes(table, best, conflicted, days, caps, check_deadline):
    """Compare the object against the committed table; return what must be written.

    Returns ``(appends, deletes, lookup)``: ``appends`` are the full rows to append (new events
    and rewritten survivors), ``deletes`` the ``(event_id, day)`` of the exact stored rows those
    rewrites replace and ``lookup`` the diagnostics of what the lookup saw
    (``snapshot_id`` it ran against, ``files_selected``, ``rows_scanned`` = stored rows the
    filtered scan returned, ``existing_found`` = incoming ids that were already stored).
    Empty ``appends`` means the object is an exact replay.
    """
    max_files, max_rows, max_bytes = caps
    appends: list[dict[str, Any]] = []
    deletes: list[tuple[str, date | None]] = []
    existing_ids: set[str] = set()
    snapshot = table.current_snapshot()
    lookup = {
        "snapshot_id": None if snapshot is None else snapshot.snapshot_id,
        "files_selected": 0,
        "rows_scanned": 0,
        "existing_found": 0,
    }
    if snapshot is not None:
        scan = table.scan(row_filter=_lookup_filter(sorted(best), days))
        selected = list(scan.plan_files())
        lookup["files_selected"] = len(selected)
        if (
            len(selected) > max_files
            or sum(task.file.record_count for task in selected) > max_rows
            or sum(task.file.file_size_in_bytes for task in selected) > max_bytes
        ):
            raise IngestionError("selected file/row/byte cap exceeded")
        if selected:  # no candidate file => nothing stored to compare against
            for batch in _scan_batches(scan, selected):
                check_deadline()
                lookup["rows_scanned"] += batch.num_rows
                for stored in batch.to_pylist():
                    event_id = stored["event_id"]
                    candidate = best.get(event_id)
                    if candidate is None:
                        continue  # defensive: the filter only selects incoming ids
                    if event_id in existing_ids:
                        raise IngestionError(
                            "multiple stored rows share one event_id; refusing to guess"
                        )
                    existing_ids.add(event_id)
                    incoming_hash, incoming = candidate
                    stored_hash = _payload_key(stored)
                    stored_conflict = stored["quality_status"] == CONFLICT
                    is_conflict = (
                        event_id in conflicted
                        or incoming_hash != stored_hash
                        or stored_conflict
                    )
                    if incoming_hash < stored_hash:
                        survivor = dict(incoming)
                    elif is_conflict and not stored_conflict:
                        survivor = dict(stored)
                    else:
                        continue  # stored row survives unchanged: exact replay / loser
                    if is_conflict:
                        survivor["quality_status"] = CONFLICT
                    appends.append(survivor)
                    deletes.append((event_id, _event_day(stored)))
    for event_id, (_digest, record) in best.items():
        if event_id in existing_ids:
            continue
        row = dict(record)
        if event_id in conflicted:
            row["quality_status"] = CONFLICT
        appends.append(row)
    lookup["existing_found"] = len(existing_ids)
    return appends, deletes, lookup


def _verify_single_rows(table, appended, caps, check_deadline):
    """Read-your-write check after the commit: every event_id this object appended (new rows
    and rewritten survivors) must now be stored exactly ONCE.

    Reads through a brand-new table handle (new catalog load, new FileIO, no state shared with
    the handle that planned and committed), scans only the ``event_id`` column of the files the
    ids' own day ranges select, and fails closed (never repairs, never deletes) when an id is
    stored more than once or not at all. Bounded like the lookup: twice its file/row/byte caps
    (the lookup's candidates plus the files this very commit wrote).
    """
    max_files, max_rows, max_bytes = (2 * cap for cap in caps)
    ids = sorted(appended)
    days = set(appended.values())
    fresh = table.catalog.load_table(table.name())
    scan = fresh.scan(
        row_filter=_lookup_filter(ids, days), selected_fields=("event_id",)
    )
    selected = list(scan.plan_files())
    if (
        len(selected) > max_files
        or sum(task.file.record_count for task in selected) > max_rows
        or sum(task.file.file_size_in_bytes for task in selected) > max_bytes
    ):
        raise IngestionError(
            "post-commit verification cap exceeded; the commit is published, replay the object"
        )
    counts: dict[str, int] = {}
    rows_scanned = 0
    for batch in _scan_batches(scan, selected):
        check_deadline()
        rows_scanned += batch.num_rows
        for event_id in batch.column("event_id").to_pylist():
            counts[event_id] = counts.get(event_id, 0) + 1
    duplicated = sorted(event_id for event_id, n in counts.items() if n > 1)
    missing = [event_id for event_id in ids if event_id not in counts]
    if duplicated or missing:
        raise IngestionError(
            f"post-commit verification failed: duplicate rows, {len(duplicated)} event_id "
            f"stored more than once (first: {duplicated[:3]}); {len(missing)} committed "
            f"event_id not readable (first: {missing[:3]}); snapshot "
            f"{fresh.current_snapshot().snapshot_id}. The commit is published and nothing "
            f"was deleted: repair the table before replaying the object"
        )
    return {
        "snapshot_id": fresh.current_snapshot().snapshot_id,
        "ids": len(ids),
        "files_selected": len(selected),
        "rows_scanned": rows_scanned,
    }


class _StagedOutputs:
    """Record every file a transaction stages so an ABORTED attempt can remove them.

    ``table.io`` is swapped for a shallow copy whose ``new_output`` records each location
    (data files written by append/delete-rewrite, manifests, manifest lists). Only attempts
    whose outcome is known to be "not committed" are cleaned: an exception raised before
    ``commit_transaction`` or a ``CommitFailedException`` (the catalog rejected the commit).
    Any other commit error leaves the files alone - the commit may have landed, and deleting
    a referenced data file would corrupt the table.
    """

    def __init__(self, table):
        self.table = table
        self.original_io = table.io
        self.locations: list[str] = []

    def __enter__(self):
        original_new_output = self.original_io.new_output
        locations = self.locations

        def recording_new_output(location):
            locations.append(str(location))
            return original_new_output(location)

        staged_io = copy.copy(self.original_io)
        staged_io.new_output = recording_new_output
        self.table.io = staged_io
        return self

    def __exit__(self, *_exc):
        self.table.io = self.original_io
        return False

    def discard(self) -> list[str]:
        """Best-effort delete of everything staged; never masks the original error."""
        failed = []
        for location in dict.fromkeys(self.locations):
            try:
                self.original_io.delete(location)
            except FileNotFoundError:
                continue
            except Exception:  # noqa: BLE001 - cleanup must not hide the abort reason
                failed.append(location)
        self.locations.clear()
        return failed


def ingest_one(
    table,
    *,
    bucket: str,
    key: str,
    source_version: str,
    content: bytes,
    deadline: float | None = None,
) -> dict[str, Any]:
    """Normalize one bounded immutable object and atomically publish its new facts.

    ``deadline`` is an absolute ``time.monotonic()`` instant chosen by the caller (derived
    from the invocation start and the remaining Lambda time); the effective deadline is the
    earlier of it and ``SILVER_DEADLINE_SECONDS`` from now.
    """
    generation = _generation()
    max_source = _limit("SILVER_MAX_SOURCE_BYTES", 16 * 1024 * 1024)
    max_rows = _limit("SILVER_MAX_SOURCE_ROWS", 100_000)
    max_quarantine = _limit("SILVER_MAX_QUARANTINE_BYTES", 32 * 1024 * 1024)
    caps = (
        _limit("SILVER_MAX_SELECTED_FILES", 64),
        _limit("SILVER_MAX_SELECTED_ROWS", 250_000),
        _limit("SILVER_MAX_SELECTED_BYTES", 128 * 1024 * 1024),
    )
    retries = _limit("SILVER_MAX_RETRIES", DEFAULT_MAX_RETRIES)
    deadline_s = _limit("SILVER_DEADLINE_SECONDS", 45)
    # Optional per-object compaction trigger (off unless set; Terraform sets it on
    # silver_backfill only). It must leave head-room below the lookup's own file cap.
    compact_trigger = None
    if os.environ.get("SILVER_COMPACT_TRIGGER_SELECTED_FILES", "").strip():
        compact_trigger = _limit("SILVER_COMPACT_TRIGGER_SELECTED_FILES", 1)
        if compact_trigger >= caps[0]:
            raise IngestionError(
                "SILVER_COMPACT_TRIGGER_SELECTED_FILES must be below SILVER_MAX_SELECTED_FILES"
            )
    effective_deadline = time.monotonic() + deadline_s
    if deadline is not None:
        effective_deadline = min(effective_deadline, deadline)
    if time.monotonic() >= effective_deadline:
        raise IngestionError("publication deadline exceeded before CSV normalization")
    if len(content) > max_source:
        raise IngestionError("source byte cap exceeded")
    digest = hashlib.sha256(content).hexdigest()
    occurrence_version = source_version or f"sha256:{digest}"
    if not occurrence_version.strip():
        raise IngestionError("source object version/etag missing")
    try:
        decoded = content.decode("utf-8")
    except UnicodeDecodeError:
        decoded = None
    ingested_at = datetime.now(timezone.utc).isoformat()
    lineage_prefix = f"s3://{bucket}/{key}#"
    rows_observed = 0
    quarantined_observations: list[dict[str, Any]] = []

    def lineage(ordinal, row):
        return {"lineage_ref": f"{lineage_prefix}{ordinal}", "ingested_at": ingested_at}

    def observe(ordinal, physical_line, row, row_issues, record):
        # Only rows that could NOT be persisted need a durable locator; persisted rows
        # carry their raw-object position in ``lineage_ref`` and are not retained here.
        nonlocal rows_observed
        rows_observed += 1
        if record is None and row_issues:
            quarantined_observations.append(
                {
                    "logical_record_ordinal": ordinal,
                    "physical_end_line": physical_line,
                    "raw": dict(row),
                    "issues": [dict(issue) for issue in row_issues],
                }
            )

    header_line = (
        decoded.splitlines()[0].lower() if decoded and decoded.splitlines() else ""
    )
    dataset = "B" if decoded is None or "tx_hash" in header_line else "A"
    normalized, issues = normalize_csv(
        content if decoded is None else decoded,
        dataset,
        lineage,
        max_records=max_rows,
        observation_sink=observe,
    )
    if any(issue.get("code") == "row_limit_exceeded" for issue in issues):
        raise IngestionError("source row cap exceeded")
    file_issues = [issue for issue in issues if issue.get("code") in FILE_ISSUE_CODES]
    fatal = [
        issue
        for issue in file_issues
        if issue["code"] in FATAL_FILE_ISSUES
        # no header line but non-blank content: rows exist that cannot be mapped
        or (issue["code"] == "missing_header" and decoded is not None and decoded.strip())
    ]
    if fatal:
        # Refused BEFORE any write: a csv.Error mid-file used to commit the valid prefix as
        # a complete object (the rest silently lost), and an unreadable header/encoding used
        # to quarantine the whole object and return success.
        first = fatal[0]
        raise IngestionError(
            f"file-level CSV defect {sorted({i['code'] for i in fatal})} "
            f"(first at line {first.get('line')}: {first.get('detail', first['code'])}); "
            f"{len(normalized)} parsed rows NOT published; source remains replayable"
        )
    if time.monotonic() >= effective_deadline:
        raise IngestionError("publication deadline exceeded during CSV normalization")
    issue_count = len(issues)
    summary: dict[str, Any] = {
        "rows_quarantined": len(quarantined_observations),
        "rows_observed": rows_observed,
        "issue_count": issue_count,
    }
    if file_issues:
        summary["file_issue_codes"] = sorted({issue["code"] for issue in file_issues})
    quarantine_uri = None
    if quarantined_observations or file_issues:
        quarantine_payload = _canonical_bytes(
            {
                "generation": generation,
                "source": {
                    "bucket": bucket,
                    "key": key,
                    "source_version": occurrence_version,
                    "content_sha256": digest,
                },
                "observations": quarantined_observations,
                "file_issues": file_issues,
            }
        )
        if len(quarantine_payload) > max_quarantine:
            raise IngestionError("quarantine artifact exceeds quarantine byte cap")
        quarantine_uri = (
            _quarantine_namespace(table, generation)
            + f"/quarantine-{hashlib.sha256(quarantine_payload).hexdigest()}.json"
        )
        # Content-addressed and per-object: writing it before the facts is idempotent and
        # guarantees the returned locator exists even when every good row is a replay.
        _metadata_write(
            table,
            quarantine_uri,
            quarantine_payload,
            generation,
            "quarantine",
            cap=max_quarantine,
        )
        readback, _ = _metadata_read(
            table, quarantine_uri, max_quarantine, generation, "quarantine"
        )
        if _canonical_bytes(readback) != quarantine_payload:
            raise IngestionError(
                "quarantine readback mismatch; source remains replayable"
            )
    summary["quarantine_uri"] = quarantine_uri
    if not normalized:
        if quarantine_uri is None:
            raise IngestionError(
                "CSV has no persistable records or durable issues; source remains replayable"
            )
        return {
            "rows_written": 0,
            **summary,
            "replayed": False,
            "completion": "quarantine_durable_source_replayable",
        }
    best, conflicted, days = _group_incoming(normalized)
    del normalized

    def check_deadline():
        if time.monotonic() >= effective_deadline:
            raise IngestionError("publication deadline exceeded")

    compaction = None
    if compact_trigger is not None:
        # Cheap check (one manifest-only planning pass of this object's lookup); compacts the
        # touched year partitions only when the lookup would select more than the trigger.
        from fact_compaction import compact_for_lookup

        check_deadline()
        table.refresh()
        compaction = compact_for_lookup(
            table,
            _lookup_filter(sorted(best), days),
            compact_trigger,
            deadline=effective_deadline,
        )
        if compaction is not None:
            summary["compaction"] = compaction

    slept = 0.0
    for attempt in range(retries):
        check_deadline()
        # The table is fenced by its immutable current metadata snapshot on every retry:
        # re-refresh and RE-SCAN so a concurrent writer's committed event_id is seen.
        table.refresh()
        appends, deletes, lookup = _plan_changes(
            table, best, conflicted, days, caps, check_deadline
        )
        if not appends:
            return {"rows_written": 0, **summary, "replayed": True, "lookup": lookup}
        check_deadline()
        # id -> UTC day of every row this commit appends (new rows and rewritten survivors):
        # exactly what the post-commit read-your-write check must find once.
        appended = {row["event_id"]: _event_day(row) for row in appends}
        arrow = pa.Table.from_pylist(appends, schema=ARROW_SCHEMA)
        written = arrow.num_rows
        del appends
        check_deadline()
        staged = _StagedOutputs(table)
        try:
            with staged:
                tx = table.transaction()
                tx.set_properties(
                    {GENERATION_PROPERTY: generation, SCHEMA_PROPERTY: SCHEMA_VERSION}
                )
                if deletes:
                    tx.delete(
                        delete_filter=_lookup_filter(
                            sorted({event_id for event_id, _ in deletes}),
                            {day for _, day in deletes},
                        )
                    )
                tx.append(arrow)
                # Enforce the deadline after all delete/append staging and before the
                # catalog commit is admitted.
                check_deadline()
        except BaseException:
            staged.discard()  # never admitted to the catalog: staged files are orphans
            raise
        try:
            tx.commit_transaction()
        except CommitFailedException:
            # Definitive rejection by the catalog (snapshot fence): nothing references the
            # staged files, so remove them before re-planning.
            staged.discard()
            if attempt + 1 >= retries:
                raise IngestionError("snapshot fence conflict retry budget exhausted")
            delay = backoff_delay(
                attempt,
                base=_RETRY_BASE_SECONDS,
                cap=_RETRY_MAX_SLEEP_SECONDS,
                jitter=_jitter,
            )
            if (
                slept + delay > _RETRY_MAX_TOTAL_SLEEP_SECONDS
                or time.monotonic() + delay >= effective_deadline
            ):
                raise IngestionError(
                    "snapshot fence conflict retry budget exhausted (backoff bound or "
                    "deadline reached)"
                )
            _sleep(delay)
            slept += delay
            # Retry re-refreshes and re-plans against the winner's committed rows.
            continue
        table.refresh()
        if table.properties.get(GENERATION_PROPERTY) != generation:
            raise IngestionError("commit outcome not confirmed by generation readback")
        verify = _verify_single_rows(table, appended, caps, check_deadline)
        return {
            "rows_written": written,
            **summary,
            "replayed": False,
            "lookup": lookup,
            "verify": verify,
            **({"commit_attempts": attempt + 1} if attempt else {}),
        }
    raise IngestionError("publication failed without a confirmed commit")
