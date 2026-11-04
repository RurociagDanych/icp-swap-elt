"""Cross-object lookup correctness: thread-safe PyIceberg planning, lookup diagnostics and the
post-commit single-row verification.

The hazard guarded against: PyIceberg 0.8.1
``DataScan.plan_files`` evaluates every manifest on a thread pool but shares ONE
``_InclusiveMetricsEvaluator`` whose per-file bounds live in instance attributes, so
concurrent files read each other's bounds and the file that holds the previous object's
boundary row is intermittently pruned from the lookup.
"""

from __future__ import annotations

import os
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from concurrent.futures import ThreadPoolExecutor

from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.utils.concurrent import ExecutorFactory

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

B_HEADER = (
    "tx_hash,tx_time,action_type,pool_id,from_principal_id,to_principal_id,"
    "token0_ledger_id,token1_ledger_id,token0_amount_in,token1_amount_in,"
    "token0_amount_out,token1_amount_out"
)
#: 2025-03-09T00:00:00Z in epoch milliseconds; every synthetic row lives in 2025.
BASE_MS = 1741478400000


def b_row(tx_hash, ts_ms):
    return f"{tx_hash},{ts_ms},Claim,pool,alice,pool,ledger0,ledger1,0,0,0,1"


def b_object(rows):
    return (B_HEADER + "\n" + "\n".join(rows) + "\n").encode()


class CrossObjectLookupBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="synthetic-cross-object-")
        base = Path(self.tmp.name)
        self.env = patch.dict(
            os.environ,
            {
                "SILVER_TARGET_GENERATION": "test-generation-1",
                "SILVER_DEADLINE_SECONDS": "120",
            },
        )
        self.env.start()
        self.catalog = SqlCatalog(
            "cross_object",
            uri=f"sqlite:///{base / 'catalog.sqlite'}",
            warehouse=(base / "warehouse").as_uri(),
        )
        self.catalog.create_namespace("synthetic")
        from silver_persistence import prepare_target

        # Empty yearly table, exactly like production.
        self.table = prepare_target(self.catalog, "synthetic.silver", "test-generation-1")

    def tearDown(self):
        self.env.stop()
        self.tmp.cleanup()

    def fresh_table(self):
        from silver_persistence import prepare_target

        return prepare_target(self.catalog, "synthetic.silver", "test-generation-1")

    def ingest(self, key, rows):
        from silver_persistence import ingest_one

        return ingest_one(
            self.fresh_table(),
            bucket="synthetic-raw",
            key=key,
            source_version=f"v-{key}",
            content=b_object(rows),
        )

    def stored_ids(self):
        table = self.catalog.load_table("synthetic.silver")
        return [row["event_id"] for row in table.scan().to_arrow().to_pylist()]


class ThreadSafePlanningTests(CrossObjectLookupBase):
    def seed_many_objects(self, count=24):
        """``count`` consecutive objects, newest-first like Dataset A, each its own commit
        (one manifest per snapshot), consecutive objects sharing one boundary row."""
        self.objects = []
        for n in range(count):
            lo = n * 299
            # 300 rows per object (> the 200-literal IN limit, like the 100k-id production
            # lookups, so only the timestamp bounds can prune). Object n covers minutes
            # lo..lo+299 and its oldest row (lo) is the previous object's newest: the real A
            # export lists that boundary row in BOTH consecutive objects.
            rows = [b_row(f"h-{m}", BASE_MS + m * 60_000) for m in range(lo + 299, lo - 1, -1)]
            self.objects.append(rows)
        return self.objects

    def test_concurrent_plan_files_never_prunes_the_file_holding_the_boundary_row(self):
        """Without the guard the evaluator is shared across the manifest pool."""
        from silver_persistence import (
            _group_incoming,
            _lookup_filter,
        )
        from silver_csv import normalize_csv

        objects = self.seed_many_objects(24)
        for n, rows in enumerate(objects[:-1]):
            self.ingest(f"obj-{n:04d}.csv", rows)
        table = self.fresh_table()
        last_file = {task.file.file_path for task in table.scan().plan_files()}
        self.assertEqual(23, len(last_file))
        # The lookup the NEXT object would run: ids/days of objects[-1] (which re-lists the
        # boundary row h-2299.. of the committed object).
        content = b_object(objects[-1])
        records, _ = normalize_csv(
            content.decode(),
            "B",
            lambda o, r: {"lineage_ref": f"x#{o}", "ingested_at": "2026-01-01T00:00:00+00:00"},
            max_records=10_000,
        )
        best, _conflicted, days = _group_incoming(records)
        lookup = _lookup_filter(sorted(best), days)
        previous = self.fresh_table()
        with patch.object(ExecutorFactory, "_instance", ThreadPoolExecutor(max_workers=1)):
            expected = {
                task.file.file_path
                for task in previous.scan(row_filter=lookup).plan_files()
            }
        self.assertGreaterEqual(len(expected), 2)
        old = sys.getswitchinterval()
        sys.setswitchinterval(1e-6)
        try:
            for attempt in range(60):
                got = {
                    task.file.file_path
                    for task in previous.scan(row_filter=lookup).plan_files()
                }
                self.assertEqual(expected, got, f"attempt {attempt}")
        finally:
            sys.setswitchinterval(old)


    def test_consecutive_objects_sharing_a_boundary_row_never_store_it_twice(self):
        """End to end through ``ingest_one`` (under forced thread interleaving):
        the Dataset A shape, 24 consecutive objects each re-listing the
        previous object's newest row as its oldest one, many manifests in the table."""
        objects = self.seed_many_objects(24)
        old = sys.getswitchinterval()
        sys.setswitchinterval(1e-6)
        try:
            for n, rows in enumerate(objects):
                self.ingest(f"obj-{n:04d}.csv", rows)
        finally:
            sys.setswitchinterval(old)
        ids = self.stored_ids()
        self.assertEqual(len(set(ids)), len(ids), "duplicate event_ids were stored")
        self.assertEqual(24 * 299 + 1, len(ids))

    def test_inclusive_metrics_evaluator_is_safe_when_two_threads_share_one_instance(self):
        """Deterministic version of the race: thread A is paused between loading file A's
        bounds and reading them while thread B evaluates file B on the SAME evaluator."""
        from pyiceberg.expressions import GreaterThanOrEqual
        from pyiceberg.expressions.visitors import _InclusiveMetricsEvaluator
        from pyiceberg.manifest import DataFile
        from pyiceberg.schema import Schema
        from pyiceberg.types import LongType, NestedField
        from pyiceberg.conversions import to_bytes
        from pyiceberg.table import StaticTable  # noqa: F401  (import check only)

        import silver_persistence  # noqa: F401  # installs the thread-safety guard

        schema = Schema(NestedField(1, "ts", LongType(), required=False))
        evaluator = _InclusiveMetricsEvaluator(schema, GreaterThanOrEqual("ts", 100))

        def data_file(lower, upper):
            return DataFile(
                record_count=10,
                value_counts={1: 10},
                null_value_counts={1: 0},
                nan_value_counts={},
                lower_bounds={1: to_bytes(LongType(), lower)},
                upper_bounds={1: to_bytes(LongType(), upper)},
            )

        newest = data_file(150, 200)  # must match ts >= 100
        oldest = data_file(1, 50)  # cannot match
        a_loaded = threading.Event()
        b_done = threading.Event()
        original = _InclusiveMetricsEvaluator.visit_greater_than_or_equal
        results = {}

        def paused(self_, term, literal):
            if threading.current_thread().name == "A":
                a_loaded.set()
                b_done.wait(timeout=1.0)  # B runs inside this window unless it is locked out
            return original(self_, term, literal)

        def run_a():
            results["A"] = evaluator.eval(newest)

        def run_b():
            a_loaded.wait(timeout=2.0)
            results["B"] = evaluator.eval(oldest)
            b_done.set()

        with patch.object(_InclusiveMetricsEvaluator, "visit_greater_than_or_equal", paused):
            ta = threading.Thread(target=run_a, name="A")
            tb = threading.Thread(target=run_b, name="B")
            ta.start()
            tb.start()
            ta.join(10)
            tb.join(10)
        self.assertIs(True, results["A"], "the newest file was evaluated against another file's bounds")
        self.assertIs(False, results["B"])


class LookupDiagnosticsAndVerificationTests(CrossObjectLookupBase):
    T0 = BASE_MS

    def test_result_carries_what_the_lookup_saw_at_the_boundary(self):
        first = self.ingest(
            "a.csv", [b_row("h-1", self.T0), b_row("h-shared", self.T0 + 60_000)]
        )
        self.assertEqual(
            {"snapshot_id": None, "files_selected": 0, "rows_scanned": 0, "existing_found": 0},
            first["lookup"],
        )
        snapshot = self.fresh_table().current_snapshot().snapshot_id
        second = self.ingest(
            "b.csv", [b_row("h-shared", self.T0 + 60_000), b_row("h-2", self.T0 + 120_000)]
        )
        self.assertEqual(1, second["rows_written"])
        self.assertEqual(
            {"snapshot_id": snapshot, "files_selected": 1, "rows_scanned": 1, "existing_found": 1},
            second["lookup"],
        )
        replay = self.ingest(
            "b.csv", [b_row("h-shared", self.T0 + 60_000), b_row("h-2", self.T0 + 120_000)]
        )
        self.assertTrue(replay["replayed"])
        self.assertEqual(2, replay["lookup"]["existing_found"])
        self.assertEqual(2, replay["lookup"]["rows_scanned"])
        self.assertNotEqual(snapshot, replay["lookup"]["snapshot_id"])

    def test_post_commit_verification_fails_closed_on_a_stored_duplicate(self):
        """A lookup that misses (whatever the cause) must not stay silent: the object that
        introduced the duplicate is failed, nothing is auto-deleted."""
        import silver_persistence
        from silver_persistence import IngestionError

        shared = b_row("h-shared", self.T0 + 60_000)
        self.ingest("a.csv", [b_row("h-1", self.T0), shared])
        before = sorted(self.stored_ids())
        original = silver_persistence._plan_changes

        def blind_lookup(table, best, conflicted, days, caps, check_deadline):
            # what a lookup that pruned the previous object's file would have returned
            appends = [dict(record) for _digest, record in best.values()]
            return appends, [], {
                "snapshot_id": None,
                "files_selected": 0,
                "rows_scanned": 0,
                "existing_found": 0,
            }

        with patch.object(silver_persistence, "_plan_changes", blind_lookup):
            with self.assertRaises(IngestionError) as caught:
                self.ingest("b.csv", [shared, b_row("h-2", self.T0 + 120_000)])
        self.assertIn("duplicate", str(caught.exception))
        self.assertIn("1 event_id", str(caught.exception))
        self.assertIs(original, silver_persistence._plan_changes)
        # fail closed, no repair: the committed duplicate is still there for the operator
        after = sorted(self.stored_ids())
        self.assertEqual(len(before) + 2, len(after))
        self.assertEqual(len(before) + 1, len(set(after)))

    def test_post_commit_verification_reads_through_a_fresh_table_handle(self):
        from pyiceberg.table import Transaction

        committed = {"done": False}
        loads_after_commit = []
        real_commit = Transaction.commit_transaction
        real_load = self.catalog.load_table

        def commit(tx_self, *args, **kwargs):
            result = real_commit(tx_self, *args, **kwargs)
            committed["done"] = True
            return result

        def load(identifier):
            handle = real_load(identifier)
            if committed["done"]:
                loads_after_commit.append(handle)
            return handle

        with patch.object(Transaction, "commit_transaction", commit), patch.object(
            self.catalog, "load_table", load
        ):
            result = self.ingest("a.csv", [b_row("h-1", self.T0), b_row("h-2", self.T0 + 60_000)])
        self.assertEqual(2, result["rows_written"])
        self.assertGreaterEqual(len(loads_after_commit), 1)
        self.assertEqual(2, result["verify"]["ids"])
        self.assertEqual(2, result["verify"]["rows_scanned"])
        self.assertEqual(1, result["verify"]["files_selected"])

    def test_verification_covers_rewritten_conflict_survivors(self):
        first = self.ingest("a.csv", [b_row("h-c", self.T0)])
        self.assertEqual(1, first["rows_written"])
        # same tx_hash and instant, different payload => conflict rewrite (delete + append)
        changed = f"h-c,{self.T0},Claim,pool,alice,pool,ledger0,ledger1,0,0,0,2"
        second = self.ingest("b.csv", [changed])
        self.assertEqual(1, second["rows_written"])
        self.assertEqual(1, second["verify"]["ids"])
        self.assertEqual(1, len(self.stored_ids()))


class PackagingTests(unittest.TestCase):
    def test_dockerfile_copies_every_source_module(self):
        """A module imported by the Lambda but missing from the image only fails at runtime."""
        dockerfile = (ROOT / "Dockerfile").read_text()
        for module in sorted((ROOT / "src").glob("*.py")):
            self.assertIn(f"COPY src/{module.name} ", dockerfile, module.name)


if __name__ == "__main__":
    unittest.main(verbosity=2)
