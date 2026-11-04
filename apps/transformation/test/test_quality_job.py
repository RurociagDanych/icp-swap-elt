"""Tests for the dbt-duckdb data-quality Lambda (quality_job).

Runs the real dbt project against small Iceberg tables in a local SQLite-backed catalog (no AWS).
Runnable without pytest: ``python3 test/test_quality_job.py``.
"""
import datetime as dt
import os
import sys
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pyarrow as pa  # noqa: E402
from pyiceberg.catalog.sql import SqlCatalog  # noqa: E402
from pyiceberg.transforms import YearTransform  # noqa: E402

os.environ.setdefault("GLUE_DATABASE", "quality_test")
os.environ["HOME"] = tempfile.mkdtemp(prefix="quality-home-")

import quality_job  # noqa: E402
from silver_persistence import ARROW_SCHEMA  # noqa: E402

ICP = "ryjl3-tyaaa-aaaaa-aaaba-cai"
NOW = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
DIM_SCHEMA = pa.schema(
    [
        pa.field("ledger_id", pa.string(), nullable=False),
        pa.field("symbol", pa.string()),
        pa.field("name", pa.string()),
        pa.field("standard", pa.string()),
        pa.field("decimals", pa.int64()),
        pa.field("as_of", pa.timestamp("us", tz="UTC")),
    ]
)


def event(i, *, ts=None, **overrides):
    row = {
        "event_id": f"ev-{i}",
        "event_timestamp": ts if ts is not None else NOW - dt.timedelta(hours=i),
        "action": "swap",
        "pool_id": "pool-a",
        "token0_ledger_id": ICP,
        "token1_ledger_id": "token-b",
        "token0_amount": Decimal("1.5"),
        "token1_amount": Decimal("2.5"),
        "amount_usd": Decimal("3.5"),
        "from_principal": "from",
        "to_principal": "to",
        "contract_version": "silver-flat-v1",
        "lineage_ref": f"s3://raw/x.csv#{i}",
        "quality_status": "ok",
        "ingested_at": NOW,
    }
    row.update(overrides)
    return row


def dim(ledger, standard="ICRC1"):
    return {"ledger_id": ledger, "symbol": "S", "name": "N", "standard": standard, "decimals": 8, "as_of": NOW}


class QualityJobTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="quality-test-")
        base = Path(self.tmp.name)
        self.catalog = SqlCatalog(
            "quality-test", uri=f"sqlite:///{base / 'catalog.sqlite'}", warehouse=(base / "wh").as_uri()
        )
        self.catalog.create_namespace("quality_test")
        self.fact = self.catalog.create_table("quality_test.icp_swap_events", schema=ARROW_SCHEMA)
        with self.fact.update_spec() as update:
            update.add_field("event_timestamp", YearTransform(), "event_timestamp_year")
        self.fact = self.catalog.load_table("quality_test.icp_swap_events")
        self.dim = None
        self.addCleanup(self.tmp.cleanup)
        for key in list(os.environ):
            if key.startswith("QUALITY_CHECK_"):
                self.addCleanup(os.environ.pop, key, None)

    def put_events(self, rows):
        self.fact.append(pa.Table.from_pylist(rows, schema=ARROW_SCHEMA))

    def put_dim(self, rows):
        self.dim = self.catalog.create_table("quality_test.token_dim", schema=DIM_SCHEMA)
        self.dim.append(pa.Table.from_pylist(rows, schema=DIM_SCHEMA))

    def clean_data(self):
        self.put_events([event(i) for i in range(1, 6)])
        self.put_dim([dim(ICP, "ICP"), dim("token-b")])

    def report(self):
        return quality_job.run_quality_checks(self.catalog)

    @staticmethod
    def statuses(report):
        return {c["name"]: c["status"] for c in report["checks"]}

    def failing(self, report):
        return sorted(n for n, s in self.statuses(report).items() if s in ("fail", "error"))

    # --- passing / reporting ---------------------------------------------------------------

    def test_clean_tables_pass_every_check(self):
        self.clean_data()
        report = self.report()
        self.assertTrue(report["ok"], report)
        self.assertEqual(0, report["summary"]["fail"] + report["summary"]["error"] + report["summary"]["warn"], report)
        self.assertGreaterEqual(report["summary"]["pass"], 20)
        self.assertEqual(5, report["tables"]["icp_swap_events"]["rows"])
        self.assertEqual(2, report["tables"]["token_dim"]["rows"])

    def test_missing_fact_table_is_skipped_not_failed(self):
        self.catalog.drop_table("quality_test.icp_swap_events")
        report = self.report()
        self.assertTrue(report["ok"])
        self.assertIn("skipped", report)

    def test_missing_token_dim_is_tolerated_and_reported_as_relationship_warnings(self):
        self.put_events([event(i) for i in range(1, 4)])
        report = self.report()
        self.assertTrue(report["ok"], report)
        self.assertTrue(report["tables"]["token_dim"]["missing"])
        self.assertGreaterEqual(report["summary"]["warn"], 2)

    # --- failures --------------------------------------------------------------------------

    def test_duplicate_event_id_fails_uniqueness_and_the_handler_raises(self):
        self.clean_data()
        self.put_events([event(2)])  # same event_id again
        report = self.report()
        self.assertFalse(report["ok"])
        self.assertTrue(any("unique" in n and "event_id" in n for n in self.failing(report)), self.failing(report))

        original = quality_job._catalog
        quality_job._catalog = lambda: self.catalog
        try:
            with self.assertRaises(quality_job.QualityCheckFailed):
                quality_job.handler({})
            self.assertFalse(quality_job.handler({"fail_on_error": False})["ok"])
        finally:
            quality_job._catalog = original

    def test_unknown_action_negative_amount_and_future_timestamp_fail(self):
        self.put_events(
            [
                event(1),
                event(2, action="teleport"),
                event(3, token0_amount=Decimal("-1")),
                event(4, ts=NOW + dt.timedelta(days=3)),
            ]
        )
        self.put_dim([dim(ICP, "ICP"), dim("token-b")])
        failing = self.failing(self.report())
        self.assertTrue(any("accepted_values" in n and "action" in n for n in failing), failing)
        self.assertTrue(any("non_negative" in n and "token0_amount" in n for n in failing), failing)
        self.assertTrue(any("not_in_future" in n for n in failing), failing)

    def test_icp_ledger_with_another_standard_fails(self):
        self.put_events([event(1)])
        self.put_dim([dim(ICP, "ICRC2"), dim("token-b")])
        self.assertIn("icp_ledger_standard", self.failing(self.report()))

    def test_content_duplicates_inside_the_seam_window_warn_with_the_2s_tolerance(self):
        """M13: the check mirrors seam_reconciliation's match rule (same content, timestamps
        within DEFAULT_TOLERANCE_SECONDS = 2 s). It is WARN: the loaded columns cannot tell a
        missed A/B pair from two genuine identical trades in the same seconds, and an error
        severity would fail every later run forever on such legitimate data."""
        seam = dt.datetime(2026, 6, 7, 3, 0, 0, tzinfo=dt.timezone.utc)
        self.put_events(
            [
                event(1),
                event(2, ts=seam),
                event(3, ts=seam),  # exact-timestamp pair
                event(4, ts=seam + dt.timedelta(hours=1)),
                event(5, ts=seam + dt.timedelta(hours=1, seconds=2)),  # within tolerance
                event(6, ts=seam + dt.timedelta(hours=2)),
                event(7, ts=seam + dt.timedelta(hours=2, seconds=3)),  # outside tolerance
            ]
        )
        self.put_dim([dim(ICP, "ICP"), dim("token-b")])
        report = self.report()
        self.assertTrue(report["ok"], report)
        check = next(c for c in report["checks"] if c["name"] == "no_seam_content_duplicates")
        self.assertEqual(("warn", "warn"), (check["status"], check["severity"]))
        self.assertEqual(2, check["failures"])  # the exact pair and the 2 s pair, not the 3 s one

    # --- warnings (reported, not fatal) ---------------------------------------------------

    def test_missing_days_and_flagged_rows_are_warnings_only(self):
        day = lambda n: NOW - dt.timedelta(days=n)  # noqa: E731
        self.put_events(
            [
                event(1, ts=day(0)),
                event(2, ts=day(1)),
                event(3, ts=day(4)),  # days 2 and 3 have no events
                event(4, ts=day(5), quality_status="conflict"),
                event(5, ts=day(5), quality_status="unresolved"),
            ]
        )
        self.put_dim([dim(ICP, "ICP"), dim("token-b")])
        report = self.report()
        self.assertTrue(report["ok"], report)
        warned = sorted(n for n, s in self.statuses(report).items() if s == "warn")
        self.assertIn("coverage_no_missing_days", warned)
        self.assertIn("quality_status_flags", warned)
        gaps = next(c for c in report["checks"] if c["name"] == "coverage_no_missing_days")
        self.assertEqual(2, gaps["failures"])

    def test_stale_data_only_warns(self):
        self.put_events([event(1, ts=NOW - dt.timedelta(days=10))])
        self.put_dim([dim(ICP, "ICP"), dim("token-b")])
        report = self.report()
        self.assertTrue(report["ok"], report)
        self.assertTrue(any("recent" in n and s == "warn" for n, s in self.statuses(report).items()))

    # --- bounds -----------------------------------------------------------------------------

    def test_scan_caps_fail_closed_before_any_data_is_loaded(self):
        self.clean_data()
        os.environ["QUALITY_CHECK_MAX_SCAN_ROWS"] = "2"
        with self.assertRaisesRegex(quality_job.QualityCheckError, "cap exceeded") as caught:
            self.report()
        # M10: the message names the measured size, the bound and the knob to change
        message = str(caught.exception)
        self.assertIn("rows=5", message)
        self.assertIn("QUALITY_CHECK_MAX_SCAN_ROWS=2", message)
        self.assertIn("whole table", message)

    # --- time budget (M9) ----------------------------------------------------------------

    def test_dbt_phase_fails_clearly_when_too_little_time_is_left(self):
        import time

        self.clean_data()
        with self.assertRaisesRegex(quality_job.QualityCheckError, "dbt test phase"):
            quality_job.run_quality_checks(self.catalog, deadline=time.monotonic() + 5)

    def test_dbt_phase_overrun_is_killed_and_reported(self):
        import subprocess
        from unittest.mock import patch

        self.clean_data()

        def overrun(*args, timeout=None, **kwargs):
            raise subprocess.TimeoutExpired(args[0], timeout)

        with (
            patch.object(quality_job.subprocess, "run", overrun),
            self.assertRaisesRegex(quality_job.QualityCheckError, "exceeded its time budget"),
        ):
            self.report()

    def test_handler_derives_the_deadline_from_the_remaining_lambda_time(self):
        class Context:
            def get_remaining_time_in_millis(self):
                return 10_000  # less than the safety margin

        self.clean_data()
        original = quality_job._catalog
        quality_job._catalog = lambda: self.catalog
        try:
            with self.assertRaisesRegex(quality_job.QualityCheckError, "deadline|dbt test phase"):
                quality_job.handler({}, Context())
        finally:
            quality_job._catalog = original

    def test_runs_without_posix_semaphores_like_lambda_has_no_dev_shm(self):
        import multiprocessing.synchronize as synchronize
        from unittest.mock import patch

        def no_shm(*_args, **_kwargs):
            raise FileNotFoundError(2, "No such file or directory")

        self.clean_data()
        # in-process: the patch must be visible to dbt (the isolated child applies the same
        # thread-lock substitution itself, via the same code path)
        with patch.object(synchronize.SemLock, "__init__", no_shm):
            report = quality_job.run_quality_checks(self.catalog, isolate_dbt=False)
        self.assertTrue(report["ok"], report)

    def test_dbt_project_dir_does_not_shadow_the_dbt_package(self):
        self.assertNotEqual("dbt", quality_job.DBT_PROJECT_DIR.name)
        self.assertTrue((quality_job.DBT_PROJECT_DIR / "dbt_project.yml").is_file())

    def test_scan_loads_only_the_columns_the_checks_need(self):
        self.assertNotIn("lineage_ref", quality_job.FACT_COLUMNS)
        self.assertNotIn("from_principal", quality_job.FACT_COLUMNS)
        self.assertTrue(set(quality_job.FACT_COLUMNS) <= set(ARROW_SCHEMA.names))


if __name__ == "__main__":
    unittest.main()
