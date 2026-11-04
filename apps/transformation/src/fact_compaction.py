"""In-job compaction of the Silver fact table (``icp_swap_events``).

Why: every loaded object appends one data file, and the event_id lookup in
``silver_persistence._plan_changes`` selects every file whose event-time statistics overlap the
incoming object. Wide, overlapping backfill objects therefore make the selected-file count grow
with every object until ``SILVER_MAX_SELECTED_FILES`` fails the load. Glue table optimizers are
not used for this: they run at most every 24 h and commit concurrently with our writers.

PyIceberg 0.8.1 (pinned in requirements.txt) has no rewrite/compaction API (``Operation.REPLACE``
exists as an enum value only, there is no ``rewrite_data_files``). Compaction is therefore built
from the plumbing PyIceberg's own ``Transaction.delete`` uses for copy-on-write rewrites:
read the input files (``ArrowScan``), write new files (``_dataframe_to_data_files``, the writer
behind ``Transaction.append``) and replace exactly the input files in ONE overwrite snapshot
(``update_snapshot().overwrite()`` + ``delete_data_file`` / ``append_data_file``). File-level
replacement (not a predicate delete) is what allows bounded slices of a partition.

Commit discipline (same as ``silver_persistence.ingest_one``):

* one transaction per slice (a slice lies inside ONE year partition; a partition within the
  caps is one slice, i.e. one transaction per partition), fenced by PyIceberg's
  ``AssertRefSnapshotId(main = planned snapshot)``: any concurrent commit makes ours fail with
  ``CommitFailedException`` and the slice is re-planned from the new snapshot;
* bounded retries with ``utils.retry.backoff_delay``; staged files are deleted only when the
  outcome is definitely "not committed" (exception before the commit, or ``CommitFailedException``);
  any other commit error leaves them alone (the commit may have landed);
* fail-closed: input caps (files/rows/bytes), a deadline, duplicate ``event_id`` in the input,
  delete files, a foreign partition spec, and a pre- and post-commit content check all raise.

Correctness: the output is the input re-ordered by ``(event_timestamp, event_id)`` and nothing
else; the commit is refused unless the output row count and the written files' record counts
equal the input, and after the commit the new files are read back through a fresh catalog load
and compared, sorted by ``event_id``, with the input (all columns). Sorting by event time gives
the new files narrow, mostly disjoint time ranges, so a lookup only selects the files whose days
it touches.
"""

from __future__ import annotations

import itertools
import logging
import os
import random
import time
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg.conversions import from_bytes
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.expressions import AlwaysTrue

# Must be imported before any scan is planned (thread-safe manifest evaluation).
import iceberg_scan_safety  # noqa: F401
from silver_persistence import (
    ARROW_SCHEMA,
    DEFAULT_MAX_RETRIES,
    GENERATION_PROPERTY,
    IngestionError,
    _RETRY_BASE_SECONDS,
    _RETRY_MAX_SLEEP_SECONDS,
    _RETRY_MAX_TOTAL_SLEEP_SECONDS,
    _StagedOutputs,
    _generation,
    _limit,
    _validate_schema,
)
from utils.logging import get_logger, log_event
from utils.retry import backoff_delay

LOG = get_logger(__name__)

#: Snapshot-summary marker of every compaction commit (audit trail in the table history).
SNAPSHOT_MARKER = "icp-swap.compaction"
#: A new slice is started only if ``now + factor * last slice duration`` is before the deadline.
_SLICE_TIME_FACTOR = 1.5

_monotonic = time.monotonic
_sleep = time.sleep
_jitter = random.random


class MaintenanceIncomplete(IngestionError):
    """Bounded progress was committed but work remains; re-running continues it.

    The Lambda reports this as error type ``MaintenanceIncomplete``; the load state machine
    retries exactly this error a bounded number of times (each attempt resumes).
    """


def compaction_limits() -> dict[str, int]:
    """Caps from the environment (Terraform sets them on ``silver_backfill``)."""
    limits = {
        "target_rows": _limit("SILVER_COMPACT_TARGET_FILE_ROWS", 250_000),
        "min_files": _limit("SILVER_COMPACT_MIN_INPUT_FILES", 4),
        "max_files": _limit("SILVER_COMPACT_MAX_INPUT_FILES", 256),
        "max_rows": _limit("SILVER_COMPACT_MAX_INPUT_ROWS", 2_000_000),
        "max_bytes": _limit("SILVER_COMPACT_MAX_INPUT_BYTES", 512 * 1024 * 1024),
        "retries": _limit("SILVER_MAX_RETRIES", DEFAULT_MAX_RETRIES),
        "deadline_seconds": _limit("SILVER_DEADLINE_SECONDS", 45),
        "partition_threshold": _threshold("SILVER_COMPACT_PARTITION_FILE_THRESHOLD"),
    }
    if limits["min_files"] < 2:
        raise IngestionError("SILVER_COMPACT_MIN_INPUT_FILES must be at least 2")
    if limits["min_files"] > limits["max_files"]:
        raise IngestionError(
            "SILVER_COMPACT_MIN_INPUT_FILES must not exceed SILVER_COMPACT_MAX_INPUT_FILES"
        )
    if limits["target_rows"] > limits["max_rows"]:
        raise IngestionError(
            "SILVER_COMPACT_MAX_INPUT_ROWS must be at least SILVER_COMPACT_TARGET_FILE_ROWS"
        )
    return limits


def _threshold(name: str) -> int:
    """Optional non-negative gate; unset or 0 disables it."""
    raw = os.environ.get(name, "").strip()
    value = int(raw) if raw else 0
    if value < 0:
        raise IngestionError(f"{name} must not be negative")
    return value


def _partition_key(data_file) -> tuple:
    partition = data_file.partition
    return tuple(partition[pos] for pos in range(len(partition)))


def _time_lower_bound(table, data_file) -> int:
    """Lower event_timestamp bound (micros) from the file statistics; files without one
    (all-NULL timestamps) sort last."""
    field = table.schema().find_field("event_timestamp")
    raw = (data_file.lower_bounds or {}).get(field.field_id)
    if raw is None:
        return 2**63 - 1
    return from_bytes(field.field_type, raw)


def _plan(table) -> tuple[int | None, list]:
    snapshot = table.current_snapshot()
    if snapshot is None:
        return None, []
    tasks = list(table.scan(row_filter=AlwaysTrue()).plan_files())
    spec_id = table.spec().spec_id
    for task in tasks:
        if task.delete_files:
            raise IngestionError(
                "data file with delete files found; compaction refuses merge-on-read tables "
                f"(file {task.file.file_path})"
            )
        if task.file.spec_id != spec_id:
            raise IngestionError(
                "data file written under another partition spec; refusing to compact"
            )
    return snapshot.snapshot_id, tasks


def _next_slice(table, tasks, limits, partitions):
    """The next bounded set of small files to rewrite, or ``None`` when nothing qualifies.

    Small = fewer rows than the target. A partition qualifies once it holds MORE than
    ``partition_threshold`` data files in total (``SILVER_COMPACT_PARTITION_FILE_THRESHOLD``,
    0 = no gate; keeps the daily run cheap: below the threshold nothing is read) and at least
    ``min_files`` small files; its small files are taken in event-time order (lower bound)
    while the file/row/byte caps allow. A single file above a cap is refused, never skipped
    or truncated; a slice of one file would make no progress and is refused as well.
    """
    by_partition: dict[tuple, list] = {}
    totals: dict[tuple, int] = {}
    for task in tasks:
        key = _partition_key(task.file)
        totals[key] = totals.get(key, 0) + 1
        if task.file.record_count < limits["target_rows"]:
            by_partition.setdefault(key, []).append(task)
    for key in sorted(by_partition, key=repr):
        if partitions is not None and key not in partitions:
            continue
        if totals[key] <= limits["partition_threshold"]:
            continue
        small = by_partition[key]
        if len(small) < limits["min_files"]:
            continue
        small.sort(key=lambda t: (_time_lower_bound(table, t.file), t.file.file_path))
        chosen, rows, size = [], 0, 0
        for task in small:
            if (
                len(chosen) + 1 > limits["max_files"]
                or rows + task.file.record_count > limits["max_rows"]
                or size + task.file.file_size_in_bytes > limits["max_bytes"]
            ):
                break
            chosen.append(task)
            rows += task.file.record_count
            size += task.file.file_size_in_bytes
        if len(chosen) < 2:
            raise IngestionError(
                f"compaction input cap exceeded by a single data file in partition {key} "
                f"({small[0].file.record_count} rows, {small[0].file.file_size_in_bytes} "
                "bytes); refusing (raise SILVER_COMPACT_MAX_INPUT_ROWS/_BYTES)"
            )
        return key, chosen
    return None


def _read(table, tasks, check_deadline) -> pa.Table:
    from pyiceberg.io.pyarrow import ArrowScan

    batches = []
    reader = ArrowScan(table.metadata, table.io, table.schema(), AlwaysTrue())
    for batch in reader.to_record_batches(tasks):
        check_deadline()
        batches.append(batch)
    if not batches:
        return ARROW_SCHEMA.empty_table()
    return pa.Table.from_batches(batches).cast(ARROW_SCHEMA)


def _by_event_id(data: pa.Table) -> pa.Table:
    return data.sort_by("event_id").combine_chunks()


def _new_rewrite(tx, io):
    """The OVERWRITE snapshot producer behind ``Transaction.delete``'s file rewrites, with
    correct summary totals.

    PyIceberg 0.8.1 summarizes every OVERWRITE as a full-table truncate
    (``update_snapshot_summaries(truncate_full_table=True)``): after a compaction of 4 of 10
    files the snapshot would claim ``deleted-data-files = 10`` and ``total-records`` = only the
    rewritten rows. The data and manifests are right, the summary is not. This subclass
    builds the summary exactly like ``_SnapshotProducer._summary`` but incrementally
    (pinned by test_partition_above_the_file_cap_is_compacted_in_bounded_slices).
    """
    from pyiceberg.table.snapshots import (
        Operation,
        SnapshotSummaryCollector,
        Summary,
        update_snapshot_summaries,
    )
    from pyiceberg.table.update.snapshot import _OverwriteFiles

    class _CompactionRewrite(_OverwriteFiles):
        def _summary(self, snapshot_properties=None):
            metadata = self._transaction.table_metadata
            collector = SnapshotSummaryCollector()
            for data_file in self._added_data_files:
                collector.add_file(
                    data_file=data_file, partition_spec=metadata.spec(), schema=metadata.schema()
                )
            specs = metadata.specs()
            for data_file in self._deleted_data_files:
                collector.remove_file(
                    data_file=data_file,
                    partition_spec=specs[data_file.spec_id],
                    schema=metadata.schema(),
                )
            previous = (
                metadata.snapshot_by_id(self._parent_snapshot_id)
                if self._parent_snapshot_id is not None
                else None
            )
            return update_snapshot_summaries(
                summary=Summary(
                    operation=self._operation, **collector.build(), **(snapshot_properties or {})
                ),
                previous_summary=previous.summary if previous is not None else None,
                truncate_full_table=False,
            )

    return _CompactionRewrite(
        operation=Operation.OVERWRITE,
        transaction=tx,
        io=io,
        snapshot_properties={SNAPSHOT_MARKER: "true"},
    )


def _rewrite_slice(table, planned_snapshot_id, key, tasks, limits, check_deadline):
    """Stage and commit ONE slice. Returns the commit report.

    Raises ``CommitFailedException`` (staged files already removed) when the catalog rejected
    the commit, so the caller can re-plan.
    """
    from pyiceberg.io.pyarrow import _dataframe_to_data_files

    expected_rows = sum(task.file.record_count for task in tasks)
    data = _read(table, tasks, check_deadline)
    if data.num_rows != expected_rows:
        raise IngestionError(
            f"compaction read {data.num_rows} rows but the manifests list {expected_rows}; "
            "refusing to compact"
        )
    distinct = pc.count_distinct(data.column("event_id")).as_py()
    if distinct != data.num_rows:
        raise IngestionError(
            f"duplicate event_id in compaction input: {data.num_rows - distinct} extra rows in "
            f"partition {key}; refusing to compact (the table violates the event_id dedup "
            "invariant; repair it first)"
        )
    order = pc.sort_indices(
        data,
        sort_keys=[("event_timestamp", "ascending"), ("event_id", "ascending")],
        null_placement="at_end",
    )
    output = data.take(order)
    del order
    check_deadline()
    if table.metadata.current_snapshot_id != planned_snapshot_id:
        raise IngestionError("fact table changed between compaction planning and staging")
    target = limits["target_rows"]
    staged = _StagedOutputs(table)
    new_files = []
    try:
        with staged:
            tx = table.transaction()
            rewrite = _new_rewrite(tx, table.io)
            counter = itertools.count(0)
            for offset in range(0, output.num_rows, target):
                check_deadline()
                for data_file in _dataframe_to_data_files(
                    table_metadata=tx.table_metadata,
                    df=output.slice(offset, target),
                    io=table.io,
                    write_uuid=rewrite.commit_uuid,
                    counter=counter,
                ):
                    new_files.append(data_file)
                    rewrite.append_data_file(data_file)
            for task in tasks:
                rewrite.delete_data_file(task.file)
            if sum(f.record_count for f in new_files) != expected_rows:
                raise IngestionError("compaction output row count differs from its input")
            if any(_partition_key(f) != key for f in new_files):
                raise IngestionError("compaction output left its partition")
            rewrite.commit()  # stages manifests + manifest list into the transaction
            check_deadline()
    except BaseException:
        staged.discard()  # never admitted to the catalog
        raise
    try:
        tx.commit_transaction()
    except CommitFailedException:
        staged.discard()  # definitively rejected by the snapshot fence
        raise
    _verify_commit(table, tasks, new_files, data, check_deadline)
    del output
    return {"input_files": len(tasks), "output_files": len(new_files), "rows": expected_rows}


def _verify_commit(table, old_tasks, new_files, data, check_deadline) -> None:
    """Read-your-write through a fresh catalog load: old files gone, new files present and
    holding exactly the input rows. Never repairs or deletes anything."""
    from pyiceberg.io.pyarrow import ArrowScan

    fresh = table.catalog.load_table(table.name())
    live = {task.file.file_path: task for task in fresh.scan(row_filter=AlwaysTrue()).plan_files()}
    old = {task.file.file_path for task in old_tasks}
    new = {f.file_path for f in new_files}
    problem = None
    if old & live.keys():
        problem = f"{len(old & live.keys())} replaced files still live"
    elif not new <= live.keys():
        problem = f"{len(new - live.keys())} new files not live"
    else:
        batches = []
        reader = ArrowScan(fresh.metadata, fresh.io, fresh.schema(), AlwaysTrue())
        for batch in reader.to_record_batches([live[path] for path in sorted(new)]):
            check_deadline()
            batches.append(batch)
        readback = (
            pa.Table.from_batches(batches).cast(ARROW_SCHEMA)
            if batches
            else ARROW_SCHEMA.empty_table()
        )
        if not _by_event_id(readback).equals(_by_event_id(data)):
            problem = "rows read back differ from the compaction input"
    if problem:
        raise IngestionError(
            f"post-commit compaction verification failed: {problem}; snapshot "
            f"{fresh.current_snapshot().snapshot_id}. The commit is published and nothing was "
            "deleted: inspect/roll back the table before the next load"
        )
    table.refresh()


def _counts(tasks) -> tuple[int, int]:
    return len(tasks), sum(task.file.record_count for task in tasks)


def compact_table(
    table,
    *,
    deadline: float | None = None,
    partitions: set[tuple] | None = None,
    limits: dict[str, int] | None = None,
) -> dict[str, Any]:
    """Compact the small files of every qualifying year partition (or only ``partitions``).

    ``deadline`` is an absolute ``time.monotonic()`` instant (the invocation deadline); the
    effective one is the earlier of it and ``SILVER_DEADLINE_SECONDS`` from now. Returns a
    slim summary; raises ``MaintenanceIncomplete`` after committing at least one slice when
    the remaining time does not allow the next one (re-run to continue).
    """
    limits = limits or compaction_limits()
    end = _monotonic() + limits["deadline_seconds"]
    if deadline is not None:
        end = min(end, deadline)

    def check_deadline():
        if _monotonic() >= end:
            raise IngestionError("compaction deadline exceeded")

    check_deadline()
    generation = _generation()
    _validate_schema(table)
    table.refresh()
    if table.current_snapshot() is not None and (
        table.properties.get(GENERATION_PROPERTY) != generation
    ):
        raise IngestionError("compaction target generation mismatch")
    _, tasks = _plan(table)
    files_before, rows_before = _counts(tasks)
    commits, rows_rewritten, attempt, slept = 0, 0, 0, 0.0
    compacted: set[tuple] = set()
    last_duration = 0.0
    while True:
        check_deadline()
        table.refresh()
        snapshot_id, tasks = _plan(table)
        work = _next_slice(table, tasks, limits, partitions)
        if work is None:
            break
        key, chosen = work
        if commits and _monotonic() + _SLICE_TIME_FACTOR * last_duration >= end:
            files_now, _ = _counts(tasks)
            log_event(
                LOG,
                logging.WARNING,
                "compaction incomplete; re-run to continue",
                commits=commits,
                files_before=files_before,
                files_now=files_now,
            )
            raise MaintenanceIncomplete(
                f"compaction committed {commits} slices ({files_before} -> {files_now} files) "
                "but the remaining time does not cover the next one; re-run to continue"
            )
        started = _monotonic()
        try:
            report = _rewrite_slice(table, snapshot_id, key, chosen, limits, check_deadline)
        except CommitFailedException:
            if attempt + 1 >= limits["retries"]:
                raise IngestionError("compaction snapshot fence conflict retry budget exhausted")
            delay = backoff_delay(
                attempt, base=_RETRY_BASE_SECONDS, cap=_RETRY_MAX_SLEEP_SECONDS, jitter=_jitter
            )
            if slept + delay > _RETRY_MAX_TOTAL_SLEEP_SECONDS or _monotonic() + delay >= end:
                raise IngestionError(
                    "compaction snapshot fence conflict retry budget exhausted (backoff bound "
                    "or deadline reached)"
                )
            _sleep(delay)
            slept += delay
            attempt += 1
            continue  # re-plan against the winner's snapshot
        attempt, slept = 0, 0.0
        commits += 1
        rows_rewritten += report["rows"]
        compacted.add(key)
        last_duration = _monotonic() - started
        log_event(LOG, logging.INFO, "compaction slice committed", partition=repr(key), **report)
    table.refresh()
    _, tasks = _plan(table)
    files_after, rows_after = _counts(tasks)
    current = table.current_snapshot()
    return {
        "complete": True,
        "commits": commits,
        "partitions_compacted": len(compacted),
        "files_before": files_before,
        "files_after": files_after,
        "rows_before": rows_before,
        "rows_after": rows_after,
        "rows_rewritten": rows_rewritten,
        "snapshot_id": None if current is None else current.snapshot_id,
    }


def compact_for_lookup(table, row_filter, trigger: int, *, deadline: float) -> dict | None:
    """Cheap per-object threshold check (``SILVER_COMPACT_TRIGGER_SELECTED_FILES``).

    Plans the object's lookup (manifest metadata only, no data read). At or below ``trigger``
    selected files it returns ``None`` and costs one extra planning pass; above it compacts
    only the year partitions those files belong to. An incomplete compaction is reported, not
    raised: the lookup's own cap check right after it stays the fail-closed guard.
    """
    selected = list(table.scan(row_filter=row_filter).plan_files())
    if len(selected) <= trigger:
        return None
    partitions = {_partition_key(task.file) for task in selected}
    try:
        summary = compact_table(table, deadline=deadline, partitions=partitions)
    except MaintenanceIncomplete as exc:
        log_event(LOG, logging.WARNING, "per-object compaction incomplete", error=str(exc))
        table.refresh()
        return {
            "trigger_files": len(selected),
            "commits": None,
            "files_before": None,
            "files_after": None,
            "complete": False,
        }
    return {
        "trigger_files": len(selected),
        "commits": summary["commits"],
        "files_before": summary["files_before"],
        "files_after": summary["files_after"],
        "complete": True,
    }
