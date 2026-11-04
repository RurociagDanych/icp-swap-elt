"""In-job compaction of the Silver fact table (``fact_compaction``).

The backfill load writes one Parquet file per raw object. The event_id lookup selects every
file whose event-time statistics overlap the incoming object, so the selected-file count grows
with every object until ``SILVER_MAX_SELECTED_FILES`` fails the load. ``compact_table`` rewrites
the small files of each year partition into fewer, time-sorted files, in snapshot-fenced
transactions, without changing a single row.

Offline: SQLite-backed Iceberg catalog, ``file://`` warehouse, no AWS calls.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.table import Transaction

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
for _name, _value in {
    "RAW_BUCKET": "synthetic-raw",
    "PROCESSED_BUCKET": "synthetic-processed",
    "GLUE_DATABASE": "synthetic",
    "TABLE_NAME": "silver",
    "AWS_DEFAULT_REGION": "us-east-1",
}.items():
    os.environ.setdefault(_name, _value)

B_HEADER = (
    "tx_hash,tx_time,action_type,pool_id,from_principal_id,to_principal_id,"
    "token0_ledger_id,token1_ledger_id,token0_amount_in,token1_amount_in,"
    "token0_amount_out,token1_amount_out"
)
#: 2025-03-09T00:00:00Z in epoch milliseconds.
BASE_MS = 1741478400000
DAY_MS = 86_400_000
MINUTE_MS = 60_000
#: Same instants one year earlier: a second (2024) year partition.
YEAR_2024_MS = BASE_MS - 365 * DAY_MS


def b_row(tx_hash, ts_ms, amount="1"):
    return f"{tx_hash},{ts_ms},Claim,pool,alice,pool,ledger0,ledger1,0,0,0,{amount}"


def b_object(rows):
    return (B_HEADER + "\n" + "\n".join(rows) + "\n").encode()


def wide_object(n, days=30, base=BASE_MS, prefix="h"):
    """One backfill-like object spanning ``days`` days: overlaps every other wide object."""
    return [b_row(f"{prefix}-{n}-{d}", base + d * DAY_MS + n * MINUTE_MS) for d in range(days)]


def sorted_rows(table):
    """Every stored row (all columns) sorted by event_id: the exact content of the table."""
    return table.scan().to_arrow().sort_by("event_id")


class CompactionBase(unittest.TestCase):
    ENV: dict[str, str] = {}

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="synthetic-compaction-")
        base = Path(self.tmp.name)
        self.warehouse = base / "warehouse"
        self.env = patch.dict(
            os.environ,
            {
                "SILVER_TARGET_GENERATION": "test-generation-1",
                "SILVER_DEADLINE_SECONDS": "120",
                "SILVER_COMPACT_TARGET_FILE_ROWS": "1000",
                "SILVER_COMPACT_MIN_INPUT_FILES": "2",
                **self.ENV,
            },
        )
        self.env.start()
        self.catalog = SqlCatalog(
            "compaction",
            uri=f"sqlite:///{base / 'catalog.sqlite'}",
            warehouse=self.warehouse.as_uri(),
        )
        self.catalog.create_namespace("synthetic")
        from silver_persistence import prepare_target

        prepare_target(self.catalog, "synthetic.silver", "test-generation-1")

    def tearDown(self):
        self.env.stop()
        self.tmp.cleanup()

    def table(self):
        from silver_persistence import prepare_target

        return prepare_target(self.catalog, "synthetic.silver", "test-generation-1")

    def ingest(self, key, rows):
        from silver_persistence import ingest_one

        return ingest_one(
            self.table(),
            bucket="synthetic-raw",
            key=key,
            source_version=f"v-{key}",
            content=b_object(rows),
        )

    def compact(self, **kwargs):
        from fact_compaction import compact_table

        return compact_table(self.table(), **kwargs)

    def data_files(self):
        return {task.file.file_path for task in self.table().scan().plan_files()}

    def parquet_on_disk(self):
        return {str(p) for p in self.warehouse.rglob("*.parquet")}

    def snapshot_id(self):
        return self.table().current_snapshot().snapshot_id


class RowSetPreservationTests(CompactionBase):
    def test_compaction_keeps_the_exact_row_set_including_replays_and_conflicts(self):
        for n in range(6):
            self.ingest(f"obj-{n}.csv", wide_object(n))
        # exact replay of an earlier object: no new rows, no new file
        self.assertTrue(self.ingest("obj-replay.csv", wide_object(2))["replayed"])
        # conflict: same tx_hash and instant, different payload -> delete + rewritten survivor
        conflict = self.ingest("obj-conflict.csv", [b_row("h-1-5", BASE_MS + 5 * DAY_MS + MINUTE_MS, "2")])
        self.assertEqual(1, conflict["rows_written"])
        before_rows = sorted_rows(self.table())
        before_files = self.data_files()
        self.assertGreaterEqual(len(before_files), 6)
        statuses = dict(zip(before_rows["event_id"].to_pylist(), before_rows["quality_status"].to_pylist()))
        self.assertIn("conflict", statuses.values())

        summary = self.compact()

        after_rows = sorted_rows(self.table())
        self.assertTrue(summary["complete"])
        self.assertEqual(len(before_files), summary["files_before"])
        self.assertEqual(1, summary["files_after"])
        self.assertEqual(1, len(self.data_files()))
        self.assertEqual(before_rows.num_rows, summary["rows_before"])
        self.assertEqual(before_rows.num_rows, summary["rows_after"])
        # The same event_id set AND the same content in every column (lineage_ref,
        # ingested_at and quality_status included).
        self.assertEqual(before_rows.schema, after_rows.schema)
        self.assertTrue(before_rows.equals(after_rows), "compaction changed the row set")
        ids = after_rows["event_id"].to_pylist()
        self.assertEqual(len(ids), len(set(ids)), "event_id must stay unique")

    def test_dedup_rules_still_hold_against_compacted_files(self):
        for n in range(4):
            self.ingest(f"obj-{n}.csv", wide_object(n))
        loser_ts = BASE_MS + 3 * DAY_MS + MINUTE_MS
        self.ingest("obj-conflict.csv", [b_row("h-1-3", loser_ts, "2")])
        self.compact()
        compacted = sorted_rows(self.table())
        # exact replay of an object that is now inside the compacted file: a no-op
        self.assertTrue(self.ingest("obj-1-again.csv", wide_object(1))["replayed"])
        # both conflicting payloads again: the stored conflict survivor stays, nothing is written
        again = self.ingest("obj-conflict-again.csv", [b_row("h-1-3", loser_ts, "2")])
        self.assertEqual(0, again["rows_written"])
        again = self.ingest("obj-original-again.csv", [b_row("h-1-3", loser_ts, "1")])
        self.assertEqual(0, again["rows_written"])
        self.assertTrue(compacted.equals(sorted_rows(self.table())))

    def test_two_year_partitions_are_compacted_separately(self):
        for n in range(3):
            self.ingest(f"a-{n}.csv", wide_object(n, base=BASE_MS))
            self.ingest(f"b-{n}.csv", wide_object(n, base=YEAR_2024_MS, prefix="y2024"))
        before = sorted_rows(self.table())
        summary = self.compact()
        self.assertEqual(6, summary["files_before"])
        self.assertEqual(2, summary["files_after"])
        self.assertEqual(2, summary["partitions_compacted"])
        self.assertEqual(2, summary["commits"])
        self.assertTrue(before.equals(sorted_rows(self.table())))

    def test_second_run_is_a_no_op(self):
        for n in range(3):
            self.ingest(f"obj-{n}.csv", wide_object(n))
        self.compact()
        snapshot = self.snapshot_id()
        summary = self.compact()
        self.assertEqual(0, summary["commits"])
        self.assertEqual(summary["files_before"], summary["files_after"])
        self.assertEqual(snapshot, self.snapshot_id())


class LookupBoundTests(CompactionBase):
    ENV = {"SILVER_MAX_SELECTED_FILES": "8", "SILVER_COMPACT_TARGET_FILE_ROWS": "60"}

    def test_compaction_lets_the_load_continue_past_the_selected_file_cap(self):
        from silver_persistence import IngestionError

        for n in range(9):
            self.ingest(f"obj-{n}.csv", wide_object(n))
        # 9 overlapping files: the next wide object's lookup selects all of them (> 8)
        with self.assertRaisesRegex(IngestionError, "selected file/row/byte cap exceeded"):
            self.ingest("obj-9.csv", wide_object(9))
        summary = self.compact()
        self.assertEqual(9, summary["files_before"])
        # 270 rows sorted by event time into files of at most 60 rows
        self.assertEqual(5, summary["files_after"])
        result = self.ingest("obj-9.csv", wide_object(9))
        self.assertEqual(30, result["rows_written"])
        self.assertLessEqual(result["lookup"]["files_selected"], 5)

    def test_time_sorted_output_lets_a_narrow_object_select_one_file(self):
        for n in range(8):
            self.ingest(f"obj-{n}.csv", wide_object(n))
        self.compact()
        # one day well inside the first compacted file's event-time range
        narrow = [b_row(f"n-{m}", BASE_MS + 3 * DAY_MS + (30 + m) * MINUTE_MS) for m in range(5)]
        result = self.ingest("narrow.csv", narrow)
        self.assertEqual(5, result["rows_written"])
        self.assertEqual(1, result["lookup"]["files_selected"])


def compaction_snapshots(table):
    return [s for s in table.metadata.snapshots if s.summary.get("icp-swap.compaction") == "true"]


class PerObjectTriggerTests(CompactionBase):
    """Optional threshold check inside ``ingest_one``: compacts only when the lookup of THIS
    object would select more than ``SILVER_COMPACT_TRIGGER_SELECTED_FILES`` files."""

    ENV = {
        "SILVER_MAX_SELECTED_FILES": "8",
        "SILVER_COMPACT_TARGET_FILE_ROWS": "60",
        "SILVER_COMPACT_TRIGGER_SELECTED_FILES": "6",
    }

    def test_trigger_keeps_a_long_load_below_the_cap_without_compacting_every_object(self):
        # Without the trigger the 10th object fails (9 overlapping files > cap 8, see
        # LookupBoundTests); with it all 12 load.
        results = [self.ingest(f"obj-{n}.csv", wide_object(n)) for n in range(12)]
        rows = sorted_rows(self.table())
        ids = rows["event_id"].to_pylist()
        self.assertEqual(12 * 30, len(ids))
        self.assertEqual(len(ids), len(set(ids)))
        triggered = [r for r in results if "compaction" in r]
        rewrites = [r for r in triggered if r["compaction"]["commits"]]
        self.assertGreaterEqual(len(rewrites), 1)
        # cheap check, rare rewrite: far fewer compactions than objects
        self.assertLessEqual(len(rewrites), 4)
        self.assertEqual(
            len(compaction_snapshots(self.table())), sum(r["compaction"]["commits"] for r in triggered)
        )
        for r in results:
            self.assertLessEqual(r["lookup"]["files_selected"], 8)
        first = triggered[0]["compaction"]
        self.assertEqual({"trigger_files", "commits", "files_before", "files_after", "complete"}, set(first))
        self.assertGreater(first["trigger_files"], 6)

    def test_trigger_is_off_unless_configured(self):
        with patch.dict(os.environ, {"SILVER_MAX_SELECTED_FILES": "64"}):
            os.environ.pop("SILVER_COMPACT_TRIGGER_SELECTED_FILES")
            results = [self.ingest(f"obj-{n}.csv", wide_object(n)) for n in range(9)]
        self.assertFalse(any("compaction" in r for r in results))
        self.assertEqual([], compaction_snapshots(self.table()))

    def test_trigger_at_or_above_the_lookup_cap_is_refused(self):
        from silver_persistence import IngestionError

        with patch.dict(os.environ, {"SILVER_COMPACT_TRIGGER_SELECTED_FILES": "8"}):
            with self.assertRaisesRegex(IngestionError, "SILVER_COMPACT_TRIGGER_SELECTED_FILES"):
                self.ingest("obj-0.csv", wide_object(0))


class PartitionThresholdTests(CompactionBase):
    """``SILVER_COMPACT_PARTITION_FILE_THRESHOLD`` keeps the daily run cheap: a partition with
    at most that many files is neither read nor rewritten."""

    ENV = {"SILVER_COMPACT_PARTITION_FILE_THRESHOLD": "5"}

    def test_partition_at_or_below_the_threshold_is_not_read(self):
        import fact_compaction

        for n in range(5):
            self.ingest(f"obj-{n}.csv", wide_object(n))
        snapshot = self.snapshot_id()
        with patch.object(fact_compaction, "_read", side_effect=AssertionError("read")):
            summary = self.compact()
        self.assertEqual(0, summary["commits"])
        self.assertEqual(snapshot, self.snapshot_id())
        self.ingest("obj-5.csv", wide_object(5))
        before = sorted_rows(self.table())
        summary = self.compact()
        self.assertEqual(1, summary["commits"])
        self.assertEqual(1, summary["files_after"])
        self.assertTrue(before.equals(sorted_rows(self.table())))

    def test_negative_threshold_is_refused(self):
        from silver_persistence import IngestionError

        with patch.dict(os.environ, {"SILVER_COMPACT_PARTITION_FILE_THRESHOLD": "-1"}):
            with self.assertRaisesRegex(IngestionError, "SILVER_COMPACT_PARTITION_FILE_THRESHOLD"):
                self.compact()


class FailClosedTests(CompactionBase):
    def test_duplicate_event_id_in_the_input_is_refused_and_nothing_changes(self):
        from silver_persistence import ARROW_SCHEMA, IngestionError

        for n in range(3):
            self.ingest(f"obj-{n}.csv", wide_object(n))
        table = self.table()
        one = table.scan().to_arrow().slice(0, 1).cast(ARROW_SCHEMA)
        table.append(one)  # bypasses the dedup: a broken table with one event_id twice
        snapshot = self.snapshot_id()
        on_disk = self.parquet_on_disk()
        with self.assertRaisesRegex(IngestionError, "duplicate event_id"):
            self.compact()
        self.assertEqual(snapshot, self.snapshot_id())
        self.assertEqual(on_disk, self.parquet_on_disk(), "no staged file may be left behind")

    def test_a_single_file_above_the_byte_cap_is_refused_not_truncated(self):
        from silver_persistence import IngestionError

        for n in range(3):
            self.ingest(f"obj-{n}.csv", wide_object(n))
        snapshot = self.snapshot_id()
        with patch.dict(os.environ, {"SILVER_COMPACT_MAX_INPUT_BYTES": "1"}):
            with self.assertRaisesRegex(IngestionError, "compaction input cap"):
                self.compact()
        self.assertEqual(snapshot, self.snapshot_id())

    def test_inconsistent_limits_are_refused(self):
        from silver_persistence import IngestionError

        with patch.dict(
            os.environ,
            {"SILVER_COMPACT_TARGET_FILE_ROWS": "500", "SILVER_COMPACT_MAX_INPUT_ROWS": "100"},
        ):
            with self.assertRaisesRegex(IngestionError, "SILVER_COMPACT_MAX_INPUT_ROWS"):
                self.compact()
        with patch.dict(os.environ, {"SILVER_COMPACT_MIN_INPUT_FILES": "1"}):
            with self.assertRaisesRegex(IngestionError, "SILVER_COMPACT_MIN_INPUT_FILES"):
                self.compact()

    def test_expired_deadline_commits_nothing(self):
        from silver_persistence import IngestionError

        for n in range(3):
            self.ingest(f"obj-{n}.csv", wide_object(n))
        snapshot = self.snapshot_id()
        on_disk = self.parquet_on_disk()
        with self.assertRaisesRegex(IngestionError, "deadline"):
            self.compact(deadline=time.monotonic() - 1)
        self.assertEqual(snapshot, self.snapshot_id())
        self.assertEqual(on_disk, self.parquet_on_disk())


class SlicingTests(CompactionBase):
    ENV = {"SILVER_COMPACT_MAX_INPUT_FILES": "4"}

    def test_partition_above_the_file_cap_is_compacted_in_bounded_slices(self):
        for n in range(10):
            self.ingest(f"obj-{n}.csv", wide_object(n))
        before = sorted_rows(self.table())
        summary = self.compact()
        self.assertTrue(summary["complete"])
        self.assertGreaterEqual(summary["commits"], 3)
        self.assertEqual(1, summary["partitions_compacted"])
        self.assertTrue(before.equals(sorted_rows(self.table())))
        # every compaction commit replaced at most SILVER_COMPACT_MAX_INPUT_FILES files
        table = self.table()
        compaction_snapshots = [
            s for s in table.metadata.snapshots if s.summary.get("icp-swap.compaction") == "true"
        ]
        self.assertEqual(summary["commits"], len(compaction_snapshots))
        for snap in compaction_snapshots:
            self.assertLessEqual(int(snap.summary["deleted-data-files"]), 4)
            # incremental totals (PyIceberg 0.8.1 would summarize an OVERWRITE as a truncate)
            self.assertEqual("300", snap.summary["total-records"])
            self.assertEqual(snap.summary["added-records"], snap.summary["deleted-records"])
        self.assertLess(summary["files_after"], 2)  # < SILVER_COMPACT_MIN_INPUT_FILES small files

    def test_deadline_between_slices_reports_incomplete_after_committing_progress(self):
        import fact_compaction
        from fact_compaction import MaintenanceIncomplete

        for n in range(10):
            self.ingest(f"obj-{n}.csv", wide_object(n))
        before = sorted_rows(self.table())
        clock = {"offset": 0.0}
        real_monotonic = time.monotonic
        real_commit = Transaction.commit_transaction

        def commit(tx):
            result = real_commit(tx)
            clock["offset"] += 600.0  # the slice "took" 600 s
            return result

        with patch.object(fact_compaction, "_monotonic", lambda: real_monotonic() + clock["offset"]), patch.object(
            Transaction, "commit_transaction", commit
        ), patch.dict(os.environ, {"SILVER_DEADLINE_SECONDS": "5000"}):
            with self.assertRaises(MaintenanceIncomplete) as raised:
                self.compact(deadline=real_monotonic() + 1000)
        self.assertIn("re-run", str(raised.exception))
        # the first slice is committed and correct; the rest is left for the next run
        self.assertTrue(before.equals(sorted_rows(self.table())))
        summary = self.compact()
        self.assertTrue(summary["complete"])
        self.assertTrue(before.equals(sorted_rows(self.table())))


class CommitDisciplineTests(CompactionBase):
    def test_rejected_commit_is_retried_and_its_staged_files_are_removed(self):
        for n in range(4):
            self.ingest(f"obj-{n}.csv", wide_object(n))
        before = sorted_rows(self.table())
        on_disk = self.parquet_on_disk()
        real_commit = Transaction.commit_transaction
        calls = {"n": 0}

        def commit(tx):
            calls["n"] += 1
            if calls["n"] == 1:
                raise CommitFailedException("another writer committed first")
            return real_commit(tx)

        with patch.object(Transaction, "commit_transaction", commit), patch(
            "fact_compaction._sleep"
        ) as sleep:
            summary = self.compact()
        self.assertEqual(2, calls["n"])
        self.assertEqual(1, sleep.call_count)
        self.assertEqual(1, summary["commits"])
        self.assertTrue(before.equals(sorted_rows(self.table())))
        # exactly the old files plus the committed output: the rejected attempt left nothing
        self.assertEqual(on_disk | self.data_files_paths(), self.parquet_on_disk())

    def test_a_real_concurrent_writer_makes_compaction_replan_and_lose_nothing(self):
        """Daily and backfill executions can overlap: the snapshot fence must reject the
        compaction commit that was planned before another writer's commit."""
        for n in range(4):
            self.ingest(f"obj-{n}.csv", wide_object(n))
        real_commit = Transaction.commit_transaction
        state = {"raced": False}

        def commit(tx):
            if not state["raced"]:
                state["raced"] = True
                self.ingest("racer.csv", wide_object(99))  # the other writer commits first
            return real_commit(tx)

        with patch.object(Transaction, "commit_transaction", commit), patch(
            "fact_compaction._sleep"
        ) as sleep:
            summary = self.compact()
        self.assertEqual(1, sleep.call_count, "the stale compaction commit must be rejected")
        rows = sorted_rows(self.table())
        ids = rows["event_id"].to_pylist()
        self.assertEqual(150, len(ids))
        self.assertEqual(len(ids), len(set(ids)))
        self.assertEqual(1, summary["files_after"])
        self.assertEqual(150, summary["rows_after"])

    def data_files_paths(self):
        from urllib.parse import urlparse

        return {urlparse(path).path for path in self.data_files()}

    def test_unknown_commit_outcome_keeps_staged_files(self):
        for n in range(3):
            self.ingest(f"obj-{n}.csv", wide_object(n))
        snapshot = self.snapshot_id()
        on_disk = self.parquet_on_disk()

        def commit(_tx):
            raise RuntimeError("connection reset during commit")

        with patch.object(Transaction, "commit_transaction", commit):
            with self.assertRaisesRegex(RuntimeError, "connection reset"):
                self.compact()
        self.assertEqual(snapshot, self.snapshot_id())
        # the commit may have landed: deleting what it references would corrupt the table
        self.assertGreater(len(self.parquet_on_disk() - on_disk), 0)

    def test_retry_budget_exhaustion_fails_closed(self):
        from silver_persistence import IngestionError

        for n in range(3):
            self.ingest(f"obj-{n}.csv", wide_object(n))
        snapshot = self.snapshot_id()
        on_disk = self.parquet_on_disk()

        def commit(_tx):
            raise CommitFailedException("always behind")

        with patch.dict(os.environ, {"SILVER_MAX_RETRIES": "2"}), patch.object(
            Transaction, "commit_transaction", commit
        ), patch("fact_compaction._sleep"):
            with self.assertRaisesRegex(IngestionError, "retry budget exhausted"):
                self.compact()
        self.assertEqual(snapshot, self.snapshot_id())
        self.assertEqual(on_disk, self.parquet_on_disk())


class HandlerTests(CompactionBase):
    def setUp(self):
        super().setUp()
        import boto3

        with patch.object(boto3, "client", return_value=MagicMock()):
            import silver_job
        self.silver_job = silver_job

    def test_compact_mode_returns_a_small_summary(self):
        for n in range(3):
            self.ingest(f"obj-{n}.csv", wide_object(n))
        with patch.object(self.silver_job, "_catalog", return_value=self.catalog):
            out = self.silver_job.handler({"mode": "compact"})
        self.assertEqual("compact", out["mode"])
        self.assertTrue(out["complete"])
        self.assertEqual(3, out["files_before"])
        self.assertEqual(1, out["files_after"])
        self.assertEqual(90, out["rows_before"])
        self.assertEqual(90, out["rows_after"])
        self.assertLess(len(json.dumps(out).encode()), 1024)

    def test_compact_mode_without_a_table_is_skipped(self):
        self.catalog.drop_table("synthetic.silver")
        with patch.object(self.silver_job, "_catalog", return_value=self.catalog):
            out = self.silver_job.handler({"mode": "compact"})
        self.assertEqual({"mode": "compact", "skipped": "table_absent"}, out)


if __name__ == "__main__":
    unittest.main(verbosity=2)
