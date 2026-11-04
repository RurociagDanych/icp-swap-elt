"""Bounded extraction, merge and lookup for the existing token dimension."""

from __future__ import annotations

import csv
import io
import math
import random
import time
from collections import defaultdict
from typing import Any

from silver_contract import _timestamp as _event_timestamp
from utils.retry import backoff_delay

# token_dim scans plan files with the same PyIceberg planner (see iceberg_scan_safety).
import iceberg_scan_safety  # noqa: F401  (import installs the guard)

ATTRIBUTES = ("symbol", "name", "standard", "decimals")
# token_dim is the CURRENT value per ledger and is purely informational (fact rows carry the
# ledger ids), so an attribute conflict must never stop a reload: every conflict is resolved
# deterministically and REPORTED (``conflicts_resolved`` / ``conflicts``) instead of raised.
#
# ONE nullable ``as_of`` column per ledger row = the event timestamp of the latest observation
# applied to it (NULL = unknown, oldest). Rules (see ``_resolve_candidates``/``_merge_row``):
#   * within one object, per ledger and attribute, the LATEST observation that supplies the
#     attribute wins; the object's as_of is the max timestamp of the ledger's observations;
#   * against the stored row the decision is PER ATTRIBUTE, with the timestamp of the
#     observation that supplied that attribute: strictly NEWER than the stored as_of
#     replaces the stored value, OLDER may only FILL a NULL, EQUAL settles a differing value
#     by the greater value (decimals numerically). as_of advances to the object's latest
#     timestamp. So an older attribute can never overwrite a newer one, even when it comes in
#     an object whose OTHER observations are newer;
#   * ``standard``: ``ICP`` wins over any other value irrespective of timestamps.
# Accepted limitation: one as_of per ROW (not per attribute) cannot order A
# (symbol/standard/decimals) against B (symbol/name) per attribute, so an attribute observed
# AFTER its own stored value but BEFORE the row's as_of is refused (fill-only) and a stale
# value can survive. Never the reverse (older over newer). Objects that supply every
# attribute are fully order-independent. Exact per-attribute ordering needs one as_of column
# per attribute (a schema change; see README "token_dim" known limitation).
AS_OF_COLUMN = "as_of"
ICP_STANDARD = "ICP"
MAX_REPORTED_CONFLICTS = 50
EXPECTED_COLUMNS = frozenset({"ledger_id", *ATTRIBUTES, AS_OF_COLUMN})
DEFAULT_LIMITS = {
    # Normal path: data files selected by ``In(ledger_id, ...)`` for one object.
    "max_files": 16,
    # Compaction: every commit adds a data file and Iceberg never merges them. Once the
    # table holds more than ``compact_files`` files, the next commit rewrites the WHOLE
    # (tiny) table into one file, so the steady-state file count is <= compact_files + 1
    # however many commits happen. ``max_compact_files`` is the fail-closed ceiling on the
    # files such a whole-table read/rewrite (and ``read_metadata``) may open.
    "compact_files": 8,
    "max_compact_files": 256,
    "max_rows": 50_000,
    "max_bytes": 32 * 1024 * 1024,
    "max_ledgers": 1024,
    "max_output_rows": 1024,
    "max_staged_bytes": 32 * 1024 * 1024,
    # Commit attempts under CommitFailedException (concurrent backfill writers); between
    # attempts an equal-jitter exponential backoff waits, bounded by the phase deadline.
    "max_attempts": 6,
    "deadline_seconds": 45,
}
_RETRY_BASE_SECONDS = 0.2
_RETRY_MAX_SLEEP_SECONDS = 4.0
_sleep = time.sleep
_jitter = random.random


def _bounded_limits(limits):
    budget: dict[str, Any] = dict(DEFAULT_LIMITS)
    for key, value in (limits or {}).items():
        if key not in DEFAULT_LIMITS:
            raise MetadataError(f"unknown dimension limit: {key}")
        maximum = DEFAULT_LIMITS[key]
        if key == "deadline_seconds":
            valid_type = isinstance(value, (int, float)) and not isinstance(value, bool)
            if (
                not valid_type
                or not math.isfinite(value)
                or value < 0
                or value > maximum
            ):
                raise MetadataError(f"dimension limit exceeds bounded default: {key}")
        elif (
            type(value) is not int
            or value < (1 if key == "max_attempts" else 0)
            or value > maximum
        ):
            raise MetadataError(f"dimension limit exceeds bounded default: {key}")
        budget[key] = value
    return budget


class MetadataError(RuntimeError):
    """Metadata is malformed, contradictory, or outside the bounded contract."""


_UNSET: Any = object()


def extract_metadata(
    content: bytes,
    dataset: str,
    source: dict[str, Any],
    *,
    max_records: int,
    max_bytes: int = 16 * 1024 * 1024,
):
    """Extract only complete, valid-width CSV rows; retain a valid prefix on syntax errors.

    ``max_bytes`` mirrors ``max_records``: the caller that already read the bytes under its
    own bound (``silver_job._process_key`` derives this from ``SILVER_MAX_SOURCE_BYTES``) passes
    that same bound through here, so exactly one number governs how many source bytes this
    object may contain. The literal below is only the default for callers that don't pass one
    (e.g. direct/test callers) — it must never silently diverge from the bound the bytes were
    actually read under.
    """
    if dataset not in {"A", "B"}:
        raise MetadataError("dataset must be A or B")
    if len(content) > max_bytes:
        raise MetadataError("source byte cap exceeded")
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError:
        return [], [{"code": "invalid_utf8", "line": 0}]
    reader = csv.reader(io.StringIO(text, newline=""), strict=True)
    observations: list[dict[str, Any]] = []
    issues: list[dict[str, Any]] = []
    try:
        header = next(reader, None)
        if not header:
            return observations, [{"code": "missing_header", "line": 1}]
        names = [name.strip() for name in header]
        if any(not name for name in names) or len(set(names)) != len(names):
            return observations, [{"code": "invalid_header", "line": 1}]
        for ordinal, cells in enumerate(reader, start=1):
            if ordinal > max_records:
                raise MetadataError("source row cap exceeded")
            if len(cells) != len(names):
                issues.append(
                    {
                        "code": "column_count",
                        "logical_record_ordinal": ordinal,
                        "line": reader.line_num,
                    }
                )
                continue
            row = {name: value.strip() for name, value in zip(names, cells)}
            # One parse per row, shared by both slots (see the observation below).
            event_time: Any = _UNSET
            for slot in (0, 1):
                prefix = f"token{slot}_"
                id_names = (
                    (prefix + "ledger_id", prefix + "id")
                    if dataset == "B"
                    else (prefix + "id", prefix + "ledger_id")
                )
                ids = {row[name] for name in id_names if row.get(name)}
                if len(ids) > 1:
                    raise MetadataError(f"contradictory {prefix}ledger_id aliases")
                ledger_id = next(iter(ids), "")
                if not ledger_id:
                    issues.append(
                        {
                            "code": "missing_ledger_id",
                            "logical_record_ordinal": ordinal,
                            "slot": slot,
                        }
                    )
                    continue
                values: dict[str, Any] = {"ledger_id": ledger_id}
                fields = {
                    "symbol": (prefix + "symbol",),
                    "name": (prefix + "name",),
                    "standard": (prefix + "standard",),
                    "decimals": (prefix + "decimals",),
                }
                for attr, aliases in fields.items():
                    supplied = [
                        row[key] for key in aliases if key in row and row[key] != ""
                    ]
                    if attr == "decimals" and supplied:
                        value = supplied[0]
                        if not value.isascii() or not value.isdecimal():
                            raise MetadataError("invalid supplied decimals")
                        values[attr] = int(value)
                    else:
                        values[attr] = supplied[0] if supplied else None
                if any(values[attr] is not None for attr in ATTRIBUTES):
                    # Every observation is dated (the single as_of orders ALL attributes):
                    # the very same parser (and validity rules) silver_contract uses for the
                    # fact table's event_timestamp, once per row. Invalid/missing -> None
                    # (an "undated" observation, oldest).
                    if event_time is _UNSET:
                        event_time = _event_timestamp(row, dataset)
                    timestamp = event_time
                    observations.append(
                        {
                            **values,
                            "timestamp": timestamp,
                            "source": dict(source),
                            "logical_record_ordinal": ordinal,
                            "slot": slot,
                        }
                    )
    except csv.Error as exc:
        issues.append(
            {"code": "malformed_csv", "line": reader.line_num, "detail": str(exc)}
        )
    return observations, issues


def _validate_schema(table):
    """Accept ONLY the 6-column ``token_dim`` schema (``prepare_dim_target`` creates it
    fresh; there is no in-place evolution). Anything else is refused fail-closed."""
    schema = table.schema().as_arrow()
    if set(schema.names) != EXPECTED_COLUMNS or len(schema.names) != len(EXPECTED_COLUMNS):
        raise MetadataError(
            "incompatible token_dim schema: expected exactly the columns "
            "ledger_id, symbol, name, standard, decimals, as_of"
        )
    for name in ("ledger_id", "symbol", "name", "standard"):
        if str(schema.field(name).type) not in {"string", "large_string"}:
            raise MetadataError(
                f"incompatible token_dim field type: {name}={schema.field(name).type}"
            )
    if str(schema.field("decimals").type) not in {"int32", "int64"}:
        raise MetadataError(
            f"incompatible token_dim field type: decimals={schema.field('decimals').type}"
        )
    field_type = schema.field(AS_OF_COLUMN).type
    if str(field_type) != "timestamp[us, tz=UTC]":
        raise MetadataError(
            f"incompatible token_dim field type: {AS_OF_COLUMN}={field_type}"
        )
    return schema


def prepare_dim_target(catalog, identifier: str):
    """Load the ``token_dim`` table, creating an empty exact-schema table if it does not
    exist yet.

    Mirrors ``silver_persistence.prepare_target`` for the fact table: on a fresh
    deployment ``token_dim`` does not exist and nothing else creates it. An existing
    table is only schema-validated: it is never evolved, rebuilt or truncated, and a table
    with any other schema is refused.

    Create-or-load: at ``backfill_max_concurrency`` > 1 against a fresh catalog,
    multiple workers can all miss on ``load_table`` and race ``create_table``. Only
    one wins; every loser must load the now-existing table instead of raising
    ``TableAlreadyExistsError`` (catching that specific exception only — any other
    catalog/IO error still propagates). The winner's and every loser's table is
    schema-validated identically below either way.
    """
    from pyiceberg.exceptions import NoSuchTableError, TableAlreadyExistsError

    try:
        table = catalog.load_table(identifier)
    except NoSuchTableError:
        import pyarrow as pa

        schema = pa.schema(
            [
                pa.field("ledger_id", pa.string(), nullable=False),
                pa.field("symbol", pa.string(), nullable=True),
                pa.field("name", pa.string(), nullable=True),
                pa.field("standard", pa.string(), nullable=True),
                pa.field("decimals", pa.int64(), nullable=True),
                pa.field("as_of", pa.timestamp("us", tz="UTC"), nullable=True),
            ]
        )
        try:
            table = catalog.create_table(identifier, schema=schema)
        except TableAlreadyExistsError:
            table = catalog.load_table(identifier)
    _validate_schema(table)
    return table


def _validate_candidate_types(candidates, schema):
    decimal_max = (
        2**31 - 1 if str(schema.field("decimals").type) == "int32" else 2**63 - 1
    )
    for attrs in candidates.values():
        for attr, values in attrs.items():
            for value in values:
                if attr == "decimals":
                    if type(value) is not int or value < 0 or value > decimal_max:
                        raise MetadataError(
                            "invalid supplied decimals for registered integer type"
                        )
                elif not isinstance(value, str) or not value.strip():
                    raise MetadataError(f"invalid supplied {attr} value")


def _ts_key(timestamp):
    """Orders observation timestamps; ``None`` (unknown/undated) sorts oldest."""
    return (timestamp is not None, timestamp)


def _order_key(value):
    """Total order inside one attribute: ints compare numerically (decimals), strings
    lexicographically. The type tag only keeps a malformed mixed-type pair comparable."""
    return (isinstance(value, str), value)


def _resolve_candidates(observations, max_ledgers=DEFAULT_LIMITS["max_ledgers"]):
    """Reduce one object's observations to one candidate per ledger/attribute.

    Per ledger and attribute the LATEST observation that supplies the attribute wins (null
    values never count; undated observations are oldest). Observations tied at that latest
    timestamp that disagree are settled by the greater value (decimals numerically), and
    ``standard`` ``ICP`` wins over any other value irrespective of timestamps. Nothing here
    raises for a conflict.

    Returns ``(candidates, as_of, conflicts, observed, attr_as_of)``:
    ``candidates[ledger][attr]`` is a one-element set with the winner; ``as_of[ledger]`` is
    the object's candidate as_of (the max timestamp of the ledger's observations, None if all
    undated); ``conflicts[(ledger, attr)]`` holds the distinct values wherever the object
    supplied more than one; ``observed[ledger][attr]`` holds every supplied value (for
    type/range validation, so a losing malformed value still fails closed);
    ``attr_as_of[(ledger, attr)]`` is the timestamp of the observation(s) that supplied the
    winning value (what ``_merge_row`` compares against the stored as_of).
    """
    ledger_ids = {item["ledger_id"] for item in observations}
    if any(not isinstance(ledger, str) or not ledger.strip() for ledger in ledger_ids):
        raise MetadataError("metadata ledger_id must be nonblank")
    if len(ledger_ids) > max_ledgers:
        raise MetadataError("metadata ledger cap exceeded")
    best: dict[tuple[str, str], list[Any]] = {}
    observed: dict[str, dict[str, set[Any]]] = defaultdict(lambda: defaultdict(set))
    as_of: dict[str, Any] = {}
    for observation in observations:
        ledger = observation["ledger_id"]
        timestamp = observation.get("timestamp")
        key = _ts_key(timestamp)
        for attr in ATTRIBUTES:
            value = observation.get(attr)
            if value is None:
                continue
            observed[ledger][attr].add(value)
            current = best.get((ledger, attr))
            if current is None or key > current[0]:
                best[(ledger, attr)] = [key, {value}]
            elif key == current[0]:
                current[1].add(value)
            if ledger not in as_of or key > _ts_key(as_of[ledger]):
                as_of[ledger] = timestamp
    candidates: dict[str, dict[str, set[Any]]] = defaultdict(lambda: defaultdict(set))
    conflicts: dict[tuple[str, str], set[Any]] = {}
    attr_as_of: dict[tuple[str, str], Any] = {}
    for (ledger, attr), (key, tied) in best.items():
        attr_as_of[(ledger, attr)] = key[1]
        supplied = observed[ledger][attr]
        winner = max(tied, key=_order_key)
        if attr == "standard" and ICP_STANDARD in supplied:
            # The native ICP ledger genuinely exposes both a legacy
            # ICP-ledger and an ICRC-2 interface and different integrations record it
            # differently (ryjl3-tyaaa-aaaaa-aaaba-cai); "ICP" is canonical and wins
            # irrespective of timestamps. It is the only special case.
            winner = ICP_STANDARD
        candidates[ledger][attr] = {winner}
        if len(supplied) > 1:
            conflicts[(ledger, attr)] = set(supplied)
    return (
        candidates,
        {ledger: as_of[ledger] for ledger in candidates},
        conflicts,
        observed,
        attr_as_of,
    )


def _unique_candidates(observations, max_ledgers=DEFAULT_LIMITS["max_ledgers"]):
    return _resolve_candidates(observations, max_ledgers)[0]


def _merge_row(
    row, ledger, candidates, candidate_as_of, conflicts, attr_as_of=None
) -> bool:
    """Merge one object's resolved candidates for ``ledger`` into ``row`` (mutated in place).

    Returns whether the row changed. With ``stored`` = the row's ``as_of`` (None = oldest)
    and ``incoming`` = the timestamp of the observation that supplied THIS attribute
    (``attr_as_of[(ledger, attr)]``; falls back to the object's as_of):
      * a NULL stored attribute is always filled;
      * incoming strictly NEWER than stored -> the attribute replaces the stored one;
      * EQUAL -> a differing value is settled by the greater value (decimals numerically);
      * OLDER -> never overwrites (even if the same object carries newer observations of
        OTHER attributes);
      * ``standard`` ``ICP`` always wins, whatever the timestamps.
    ``as_of`` advances to the object's as_of when that is newer than stored.
    Every differing pair that had to be settled is added to ``conflicts`` (never raises).
    """
    stored = _ts_key(row.get(AS_OF_COLUMN))
    object_incoming = _ts_key(candidate_as_of)
    changed = False
    for attr in ATTRIBUTES:
        values = candidates.get(attr)
        if not values:
            continue
        incoming = (
            _ts_key(attr_as_of[(ledger, attr)])
            if attr_as_of is not None and (ledger, attr) in attr_as_of
            else object_incoming
        )
        candidate = next(iter(values))
        current = row[attr]
        if current is None:
            row[attr] = candidate
            changed = True
            continue
        if candidate == current:
            continue
        conflicts.setdefault((ledger, attr), set()).update((current, candidate))
        if attr == "standard" and ICP_STANDARD in (candidate, current):
            winner = ICP_STANDARD
        elif incoming > stored:
            winner = candidate
        elif incoming == stored:
            winner = max(candidate, current, key=_order_key)
        else:
            winner = current
        if winner != current:
            row[attr] = winner
            changed = True
    if object_incoming > stored:
        row[AS_OF_COLUMN] = candidate_as_of
        changed = True
    return changed


def _conflict_report(conflicts, rows):
    """``conflicts_resolved`` (exact) + the first MAX_REPORTED_CONFLICTS entries, sorted by
    (ledger_id, attr) so the report is deterministic."""
    entries = [
        {
            "ledger_id": ledger,
            "attr": attr,
            "values": sorted(values, key=_order_key),
            "chosen": rows[ledger][attr],
        }
        for (ledger, attr), values in sorted(conflicts.items())
    ]
    return {
        "conflicts_resolved": len(entries),
        "conflicts": entries[:MAX_REPORTED_CONFLICTS],
    }


def apply_metadata(dim_table, observations, *, limits=None, deadline=None):
    """Merge one object's metadata into the current-value dimension (single ``as_of``).

    Per ledger the object's candidates (latest supplying observation per attribute, see
    ``_resolve_candidates``) are merged into the stored row by ``_merge_row``: a strictly
    newer object replaces what it supplies, an equal/older one or a replay only fills NULLs
    (equal timestamp: greater value wins), ``ICP`` always wins ``standard``. No attribute
    conflict raises; each is reported as ``conflicts_resolved`` plus ``conflicts`` (capped at
    ``MAX_REPORTED_CONFLICTS``; a replay re-reports the disagreements it sees but changes
    nothing and creates no snapshot). A candidate ledger the dimension does not have yet gets
    a new row. Any schema other than the 6-column one is refused (no in-place evolution).

    File count is bounded: every commit adds a data file, so once the table holds more than
    ``compact_files`` files the commit rewrites the whole table (every file deleted, one new
    file) in the same guarded transaction. That path is fail-closed on the rows/bytes caps,
    the ``max_compact_files`` ceiling and duplicate keys; a replay (nothing changed) still
    commits nothing, compaction or not."""
    if not observations:
        return {
            "snapshot_id": None,
            "changed_ledgers": [],
            "missing_attributes": {},
            "conflicts_resolved": 0,
            "conflicts": [],
        }
    import pyarrow as pa
    from pyiceberg.exceptions import CommitFailedException
    from pyiceberg.expressions import AlwaysTrue, In
    from pyiceberg.io.pyarrow import ArrowScan

    budget = _bounded_limits(limits)
    candidates, candidate_as_of, object_conflicts, observed, attr_as_of = (
        _resolve_candidates(observations, budget["max_ledgers"])
    )
    ids = sorted(candidates)
    if len(ids) > budget["max_ledgers"]:
        raise MetadataError("metadata ledger cap exceeded")
    schema = _validate_schema(dim_table)
    _validate_candidate_types(observed, schema)
    started = time.monotonic()
    end = started + budget["deadline_seconds"]
    if deadline is not None:
        end = min(end, deadline)
    expected_table_uuid = dim_table.metadata.table_uuid
    read_bytes = 0
    last_snapshot = None
    staged_bytes = 0
    for attempt in range(budget["max_attempts"]):
        if time.monotonic() >= end:
            raise MetadataError("dimension deadline exceeded")
        dim_table.refresh()
        if dim_table.metadata.table_uuid != expected_table_uuid:
            raise MetadataError("token_dim UUID changed during retry")
        schema = _validate_schema(dim_table)
        _validate_candidate_types(observed, schema)
        snapshot = dim_table.current_snapshot()
        snapshot_id = snapshot.snapshot_id if snapshot else None
        predicate = In("ledger_id", ids)
        # token_dim files are never merged by anything else, so plan the whole (manifest
        # only) table: past ``compact_files`` this commit takes the compaction path and
        # rewrites the entire table in one transaction (see ``compacting`` below).
        all_tasks = list(dim_table.scan(row_filter=AlwaysTrue()).plan_files())
        compacting = len(all_tasks) > budget["compact_files"]
        if compacting:
            tasks, file_cap = all_tasks, budget["max_compact_files"]
        else:
            tasks = list(dim_table.scan(row_filter=predicate).plan_files())
            file_cap = budget["max_files"]
        file_bytes = sum(task.file.file_size_in_bytes for task in tasks)
        physical_rows = sum(task.file.record_count for task in tasks)
        if (
            len(tasks) > file_cap
            or physical_rows > budget["max_rows"]
            or read_bytes + file_bytes > budget["max_bytes"]
        ):
            raise MetadataError("dimension selected file/row/byte cap exceeded")
        read_bytes += file_bytes
        existing_rows = (
            ArrowScan(
                table_metadata=dim_table.metadata,
                io=dim_table.io,
                projected_schema=dim_table.metadata.schema(),
                row_filter=AlwaysTrue(),
            )
            .to_table(tasks=tasks)
            .to_pylist()
            if tasks
            else []
        )
        matches = [row for row in existing_rows if row["ledger_id"] in candidates]
        by_id: dict[str, dict[str, Any]] = {}
        for row in matches:
            ledger = row["ledger_id"]
            if ledger in by_id:
                raise MetadataError("duplicate affected token_dim key")
            by_id[ledger] = row
        untouched: list[dict[str, Any]] = []
        if compacting:
            # The whole table was read: every key must be unique (a duplicate anywhere
            # would be silently carried into, or collapsed by, the rewrite).
            seen = set()
            for row in existing_rows:
                if row["ledger_id"] in seen:
                    raise MetadataError("duplicate token_dim key")
                seen.add(row["ledger_id"])
            untouched = [r for r in existing_rows if r["ledger_id"] not in candidates]
        # A fully blank observation is deliberately a true no-op.
        if not any(attrs for attrs in candidates.values()):
            return {
                "snapshot_id": snapshot_id,
                "changed_ledgers": [],
                "missing_attributes": {},
                "conflicts_resolved": 0,
                "conflicts": [],
            }
        merged = []
        changed = []
        created = []
        missing_attributes = {}
        conflict_sets = {key: set(values) for key, values in object_conflicts.items()}
        for ledger in ids:
            # A candidate ledger absent from the dimension gets a brand-new row seeded
            # from the supplied attributes instead of permanently blocking the whole batch
            # (a newly listed token must not hard-fail every file that mentions it).
            is_new_row = ledger not in by_id
            row = (
                dict(by_id[ledger])
                if not is_new_row
                else {
                    "ledger_id": ledger,
                    **{attr: None for attr in ATTRIBUTES},
                    AS_OF_COLUMN: None,
                }
            )
            row_changed = _merge_row(
                row,
                ledger,
                candidates[ledger],
                candidate_as_of[ledger],
                conflict_sets,
                attr_as_of,
            )
            missing_attributes[ledger] = [
                attr for attr in ATTRIBUTES if row[attr] is None
            ]
            merged.append(row)
            if row_changed or is_new_row:
                changed.append(ledger)
            if is_new_row:
                created.append(ledger)
        report = _conflict_report(conflict_sets, {row["ledger_id"]: row for row in merged})
        if not changed:
            return {
                "snapshot_id": snapshot_id,
                "changed_ledgers": [],
                "created_ledgers": [],
                "missing_attributes": missing_attributes,
                "bytes_read": read_bytes,
                "bytes_staged": 0,
                **report,
            }
        output_rows = merged
        if len(output_rows) > budget["max_output_rows"]:
            raise MetadataError("dimension merged output row cap exceeded")
        overwrite_filter = predicate
        if compacting:
            # Whole-table rewrite: untouched rows are carried over unchanged and the
            # delete covers every file, so the table ends with exactly one data file.
            # Still one transaction guarded by AssertRefSnapshotId below, so a concurrent
            # writer makes it CommitFailed -> refresh -> re-merge, never a lost update.
            output_rows = sorted(untouched + merged, key=lambda r: r["ledger_id"])
            if len(output_rows) > budget["max_rows"]:
                raise MetadataError("dimension compaction output row cap exceeded")
            overwrite_filter = AlwaysTrue()
        output = pa.Table.from_pylist(output_rows, schema=schema)
        # PyIceberg 0.8.1 overwrite stages delete and append in one catalog transaction;
        # its delete predicate is the same affected-ID expression used for planning.
        import copy

        original_io = dim_table.io
        staged_outputs = []

        class CountingOutput:
            def __init__(self, output):
                self.output = output

            def create(self, *args, **kwargs):
                return self.output.create(*args, **kwargs)

            def __len__(self):
                return len(self.output)

            def __getattr__(self, name):
                return getattr(self.output, name)

        def counted_new_output(location):
            output_file = CountingOutput(original_io.new_output(location))
            staged_outputs.append(output_file)
            return output_file

        def discard_staged():
            # Never admitted to the catalog (or definitively rejected): the staged data
            # files and manifests are referenced by nothing; remove them (best effort).
            for output_file in staged_outputs:
                try:
                    original_io.delete(output_file.location)
                except Exception:  # noqa: BLE001 - must not mask the abort reason
                    pass

        dim_table.io = copy.copy(original_io)
        dim_table.io.new_output = counted_new_output
        try:
            # Snapshot fence via the PUBLIC overwrite path (no private Transaction state):
            # PyIceberg 0.8.1's delete/append snapshot producers add
            # AssertRefSnapshotId(main = the transaction's base snapshot). The transaction is
            # opened on ``dim_table`` WITHOUT a refresh since the planning scan above, so its
            # base snapshot is exactly ``snapshot_id`` (pinned by
            # test_commit_is_fenced_on_the_planned_snapshot; pyiceberg is pinned in
            # requirements.txt).
            if dim_table.metadata.current_snapshot_id != snapshot_id:
                raise MetadataError("token_dim changed between planning and staging")
            tx = dim_table.transaction()
            try:
                tx.overwrite(df=output, overwrite_filter=overwrite_filter)
                staged_bytes += sum(len(output_file) for output_file in staged_outputs)
                if staged_bytes > budget["max_staged_bytes"]:
                    raise MetadataError("dimension staged byte cap exceeded")
                if time.monotonic() >= end:
                    raise MetadataError("dimension deadline exceeded before commit")
            except BaseException:
                discard_staged()
                raise
            try:
                tx.commit_transaction()
            except CommitFailedException:
                discard_staged()
                if attempt + 1 >= budget["max_attempts"]:
                    raise MetadataError("dimension snapshot retry budget exhausted")
                delay = backoff_delay(
                    attempt,
                    base=_RETRY_BASE_SECONDS,
                    cap=_RETRY_MAX_SLEEP_SECONDS,
                    jitter=_jitter,
                )
                if time.monotonic() + delay >= end:
                    raise MetadataError(
                        "dimension snapshot retry budget exhausted (deadline reached)"
                    )
                _sleep(delay)
                continue
        finally:
            dim_table.io = original_io
        dim_table.refresh()
        if dim_table.metadata.table_uuid != expected_table_uuid:
            raise MetadataError("token_dim UUID changed during commit")
        current = dim_table.current_snapshot()
        last_snapshot = current.snapshot_id if current else None
        if last_snapshot is None:
            raise MetadataError("dimension commit not confirmed")
        return {
            "snapshot_id": last_snapshot,
            "changed_ledgers": changed,
            "created_ledgers": created,
            "missing_attributes": missing_attributes,
            "bytes_read": read_bytes,
            "bytes_staged": staged_bytes,
            **report,
            **({"compacted_files": len(all_tasks)} if compacting else {}),
        }
    raise MetadataError("dimension snapshot retry budget exhausted")


def read_metadata(dim_table, ledger_ids, *, limits=None, deadline=None):
    """Read requested metadata under the same physical-file limits; absent rows stay explicit."""
    from pyiceberg.expressions import AlwaysTrue, In
    from pyiceberg.io.pyarrow import ArrowScan

    ids = list(dict.fromkeys(ledger_ids))
    budget = _bounded_limits(limits)
    if len(ids) > budget["max_ledgers"]:
        raise MetadataError("metadata ledger cap exceeded")
    if not ids:
        return {"snapshot_id": None, "tokens": []}
    if dim_table is None:
        return {
            "snapshot_id": None,
            "tokens": [
                {
                    "ledger_id": ledger,
                    **{attr: None for attr in ATTRIBUTES},
                    "missing_attributes": list(ATTRIBUTES),
                }
                for ledger in ids
            ],
            "bytes_read": 0,
        }
    _validate_schema(dim_table)
    end = time.monotonic() + budget["deadline_seconds"]
    if deadline is not None:
        end = min(end, deadline)
    dim_table.refresh()
    snapshot = dim_table.current_snapshot()
    snapshot_id = snapshot.snapshot_id if snapshot else None
    predicate = In("ledger_id", ids)
    tasks = list(dim_table.scan(row_filter=predicate).plan_files())
    physical_rows = sum(task.file.record_count for task in tasks)
    size = sum(task.file.file_size_in_bytes for task in tasks)
    if (
        len(tasks) > budget["max_compact_files"]
        or physical_rows > budget["max_rows"]
        or size > budget["max_bytes"]
    ):
        raise MetadataError("dimension readback file/row/byte cap exceeded")
    if time.monotonic() >= end:
        raise MetadataError("dimension deadline exceeded")
    rows = (
        ArrowScan(
            table_metadata=dim_table.metadata,
            io=dim_table.io,
            projected_schema=dim_table.metadata.schema(),
            row_filter=AlwaysTrue(),
        )
        .to_table(tasks=tasks)
        .to_pylist()
        if tasks
        else []
    )
    found = {}
    for row in rows:
        ledger = row["ledger_id"]
        if ledger in found:
            raise MetadataError("duplicate affected token_dim key")
        found[ledger] = row
    tokens = []
    for ledger in ids:
        row = found.get(ledger)
        tokens.append(
            {
                "ledger_id": ledger,
                **(
                    {attr: row[attr] for attr in ATTRIBUTES}
                    if row
                    else {attr: None for attr in ATTRIBUTES}
                ),
                "missing_attributes": list(ATTRIBUTES)
                if row is None
                else [attr for attr in ATTRIBUTES if row[attr] is None],
            }
        )
    return {"snapshot_id": snapshot_id, "tokens": tokens, "bytes_read": size}
