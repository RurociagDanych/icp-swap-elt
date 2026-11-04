"""Snapshot retention (both Silver tables) and orphan-file cleanup (``token_dim`` only).

PyIceberg 0.8.1 has no expire-snapshots API, so ``table_maintenance`` commits a
``RemoveSnapshotsUpdate`` through the public ``Catalog.commit_table`` (snapshot-fenced) and then
deletes only the files that no retained snapshot reaches. The orphan cleanup deletes files below
``token_dim``'s ``data/`` and ``metadata/`` that no snapshot or metadata file references AND that
are older than a grace period. Offline: SQLite-backed catalog, ``file://`` warehouse.
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
from urllib.parse import urlparse

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import CommitFailedException

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "test"))
for _name, _value in {
    "RAW_BUCKET": "synthetic-raw",
    "PROCESSED_BUCKET": "synthetic-processed",
    "GLUE_DATABASE": "synthetic",
    "TABLE_NAME": "silver",
    "AWS_DEFAULT_REGION": "us-east-1",
}.items():
    os.environ.setdefault(_name, _value)

from test_fact_compaction import b_object, wide_object  # noqa: E402

DAY_MS = 86_400_000
#: "now" for the retention tests: every snapshot written by the test is 30 days old.
LATER_MS = int(time.time() * 1000) + 30 * DAY_MS
DIM_SCHEMA = pa.schema([pa.field("ledger_id", pa.string(), nullable=False), pa.field("symbol", pa.string())])


def local(uri: str) -> Path:
    return Path(urlparse(uri).path)


class MaintenanceBase(unittest.TestCase):
    ENV: dict[str, str] = {}

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="synthetic-maintenance-")
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
            "maintenance",
            uri=f"sqlite:///{base / 'catalog.sqlite'}",
            warehouse=self.warehouse.as_uri(),
        )
        self.catalog.create_namespace("synthetic")
        from silver_persistence import prepare_target

        prepare_target(self.catalog, "synthetic.silver", "test-generation-1")

    def tearDown(self):
        self.env.stop()
        self.tmp.cleanup()

    def facts(self):
        return self.catalog.load_table("synthetic.silver")

    def ingest(self, key, rows):
        from silver_persistence import ingest_one, prepare_target

        return ingest_one(
            prepare_target(self.catalog, "synthetic.silver", "test-generation-1"),
            bucket="synthetic-raw",
            key=key,
            source_version=f"v-{key}",
            content=b_object(rows),
        )

    def load_and_compact(self, objects=4):
        from fact_compaction import compact_table

        for n in range(objects):
            self.ingest(f"obj-{n}.csv", wide_object(n))
        old_files = [task.file.file_path for task in self.facts().scan().plan_files()]
        compact_table(self.facts())
        return old_files

    def dim(self, appends=4):
        table = self.catalog.create_table("synthetic.token_dim", schema=DIM_SCHEMA)
        for n in range(appends):
            table.append(pa.Table.from_pylist([{"ledger_id": f"l-{n}", "symbol": f"S{n}"}], schema=DIM_SCHEMA))
        return self.catalog.load_table("synthetic.token_dim")

    def rows(self, table):
        return table.scan().to_arrow().sort_by(table.schema().fields[0].name)


class SnapshotRetentionTests(MaintenanceBase):
    def test_expires_old_snapshots_keeps_the_newest_and_deletes_only_unreachable_files(self):
        from table_maintenance import expire_snapshots

        old_files = self.load_and_compact()
        before = self.rows(self.facts())
        current = self.facts().current_snapshot()
        self.assertEqual(5, len(self.facts().metadata.snapshots))  # 4 appends + 1 compaction
        with patch.dict(os.environ, {"SILVER_SNAPSHOTS_TO_RETAIN": "1"}):
            summary = expire_snapshots(self.facts(), now_ms=LATER_MS)
        table = self.facts()
        self.assertTrue(summary["complete"])
        self.assertEqual(4, summary["expired"])
        self.assertEqual([current.snapshot_id], [s.snapshot_id for s in table.metadata.snapshots])
        self.assertEqual(current.snapshot_id, table.current_snapshot().snapshot_id)
        self.assertTrue(before.equals(self.rows(table)), "retention changed the current rows")
        for path in old_files:  # only the expired snapshots reached the pre-compaction files
            self.assertFalse(local(path).exists(), path)
        for task in table.scan().plan_files():  # everything the current snapshot reads survives
            self.assertTrue(local(task.file.file_path).exists())
        self.assertTrue(local(current.manifest_list).exists())
        self.assertGreater(summary["files_deleted"], len(old_files))  # + manifest lists/manifests

    def test_files_of_a_retained_snapshot_are_kept_and_time_travel_still_works(self):
        from table_maintenance import expire_snapshots

        old_files = self.load_and_compact()
        snapshots = self.facts().metadata.snapshots
        last_append = snapshots[-2]
        with patch.dict(os.environ, {"SILVER_SNAPSHOTS_TO_RETAIN": "2"}):
            summary = expire_snapshots(self.facts(), now_ms=LATER_MS)
        self.assertEqual(3, summary["expired"])
        table = self.facts()
        self.assertEqual(2, len(table.metadata.snapshots))
        for path in old_files:  # still reachable from the retained pre-compaction snapshot
            self.assertTrue(local(path).exists(), path)
        rolled_back = table.scan(snapshot_id=last_append.snapshot_id).to_arrow()
        self.assertEqual(120, rolled_back.num_rows)

    def test_snapshots_inside_the_retention_window_are_kept(self):
        from table_maintenance import expire_snapshots

        self.load_and_compact()
        location = self.facts().metadata_location
        summary = expire_snapshots(self.facts())  # real now: everything is minutes old
        self.assertEqual(0, summary["expired"])
        self.assertEqual(location, self.facts().metadata_location, "no commit without work")

    def test_a_tagged_snapshot_is_never_expired(self):
        from table_maintenance import expire_snapshots

        old_files = self.load_and_compact()
        first = self.facts().metadata.snapshots[0]
        self.facts().manage_snapshots().create_tag(first.snapshot_id, "audit").commit()
        with patch.dict(os.environ, {"SILVER_SNAPSHOTS_TO_RETAIN": "1"}):
            expire_snapshots(self.facts(), now_ms=LATER_MS)
        ids = {s.snapshot_id for s in self.facts().metadata.snapshots}
        self.assertIn(first.snapshot_id, ids)
        self.assertEqual(30, self.facts().scan(snapshot_id=first.snapshot_id).to_arrow().num_rows)

    def test_token_dim_retention_uses_the_same_rules(self):
        from table_maintenance import expire_snapshots

        dim = self.dim(appends=5)
        before = self.rows(dim)
        with patch.dict(os.environ, {"SILVER_SNAPSHOTS_TO_RETAIN": "2"}):
            summary = expire_snapshots(dim, now_ms=LATER_MS)
        self.assertEqual(3, summary["expired"])
        dim = self.catalog.load_table("synthetic.token_dim")
        self.assertEqual(2, len(dim.metadata.snapshots))
        self.assertTrue(before.equals(self.rows(dim)))

    def test_delete_cap_bounds_each_run_and_the_next_run_continues(self):
        from table_maintenance import expire_snapshots

        old_files = self.load_and_compact(objects=6)
        before = self.rows(self.facts())
        runs = []
        with patch.dict(
            # The newest pre-compaction snapshot alone reaches 13 files (its manifest list, 6
            # manifests, 6 data files); all 6 expired together would delete 18.
            os.environ, {"SILVER_SNAPSHOTS_TO_RETAIN": "1", "SILVER_MAINT_MAX_DELETE_FILES": "13"}
        ):
            for _ in range(6):
                runs.append(expire_snapshots(self.facts(), now_ms=LATER_MS))
                if runs[-1]["complete"]:
                    break
        self.assertFalse(runs[0]["complete"])  # backlog reported, not raised
        self.assertGreater(runs[0]["pending"], 0)
        self.assertGreaterEqual(runs[0]["commits"], 2)  # bounded batches inside one run
        for run in runs:
            self.assertLessEqual(run["files_deleted"], 13)
        self.assertTrue(runs[-1]["complete"])
        self.assertEqual(6, sum(run["expired"] for run in runs))
        self.assertEqual(1, len(self.facts().metadata.snapshots))
        self.assertTrue(before.equals(self.rows(self.facts())))
        for path in old_files:
            self.assertFalse(local(path).exists(), path)

    def test_a_single_snapshot_above_the_delete_cap_is_refused(self):
        from silver_persistence import IngestionError
        from table_maintenance import expire_snapshots

        old_files = self.load_and_compact()
        before = self.rows(self.facts())
        current = self.facts().current_snapshot().snapshot_id
        last_append = self.facts().metadata.snapshots[-2].snapshot_id
        with patch.dict(
            os.environ, {"SILVER_SNAPSHOTS_TO_RETAIN": "1", "SILVER_MAINT_MAX_DELETE_FILES": "1"}
        ):
            # older snapshots (one manifest list each) fit the cap, one per run; the newest
            # pre-compaction one reaches 9 files alone and is refused
            with self.assertRaisesRegex(IngestionError, "SILVER_MAINT_MAX_DELETE_FILES"):
                for _ in range(6):
                    expire_snapshots(self.facts(), now_ms=LATER_MS)
        table = self.facts()
        self.assertEqual(current, table.current_snapshot().snapshot_id)
        self.assertIn(last_append, {s.snapshot_id for s in table.metadata.snapshots})
        self.assertTrue(before.equals(self.rows(table)))
        for path in old_files:
            self.assertTrue(local(path).exists(), path)

    def test_rejected_commit_is_retried_and_nothing_is_deleted_before_a_confirmed_commit(self):
        from table_maintenance import expire_snapshots

        old_files = self.load_and_compact()
        real_commit = SqlCatalog.commit_table
        calls = {"n": 0}

        def commit(catalog, table, requirements, updates):  # noqa: D401
            calls["n"] += 1
            if calls["n"] == 1:
                for path in old_files:  # nothing may be deleted before the commit landed
                    self.assertTrue(local(path).exists())
                raise CommitFailedException("another writer committed first")
            return real_commit(catalog, table, requirements, updates)

        with patch.object(SqlCatalog, "commit_table", commit), patch(
            "table_maintenance._sleep"
        ), patch.dict(os.environ, {"SILVER_SNAPSHOTS_TO_RETAIN": "1"}):
            summary = expire_snapshots(self.facts(), now_ms=LATER_MS)
        self.assertEqual(2, calls["n"])
        self.assertEqual(4, summary["expired"])

    def test_unknown_commit_outcome_deletes_nothing(self):
        from table_maintenance import expire_snapshots

        old_files = self.load_and_compact()

        def commit(*_args, **_kwargs):
            raise RuntimeError("connection reset during commit")

        with patch.object(SqlCatalog, "commit_table", commit), patch.dict(
            os.environ, {"SILVER_SNAPSHOTS_TO_RETAIN": "1"}
        ):
            with self.assertRaisesRegex(RuntimeError, "connection reset"):
                expire_snapshots(self.facts(), now_ms=LATER_MS)
        for path in old_files:
            self.assertTrue(local(path).exists(), path)

    def test_fence_rejects_expiry_planned_on_a_stale_snapshot(self):
        """A writer that commits between planning and commit makes the expiry re-plan."""
        from table_maintenance import expire_snapshots

        self.load_and_compact()
        real_commit = SqlCatalog.commit_table
        state = {"raced": False}

        def commit(catalog, table, requirements, updates):
            if not state["raced"]:
                state["raced"] = True
                self.ingest("racer.csv", wide_object(99))  # concurrent writer wins
            return real_commit(catalog, table, requirements, updates)

        with patch.object(SqlCatalog, "commit_table", commit), patch(
            "table_maintenance._sleep"
        ), patch.dict(os.environ, {"SILVER_SNAPSHOTS_TO_RETAIN": "1"}):
            expire_snapshots(self.facts(), now_ms=LATER_MS)
        table = self.facts()
        self.assertEqual(150, table.scan().to_arrow().num_rows)  # the racer's rows survive
        self.assertEqual(1, len(table.metadata.snapshots))

    def test_expired_deadline_and_bad_limits_are_refused(self):
        from silver_persistence import IngestionError
        from table_maintenance import expire_snapshots

        self.load_and_compact()
        location = self.facts().metadata_location
        with self.assertRaisesRegex(IngestionError, "deadline"):
            expire_snapshots(self.facts(), now_ms=LATER_MS, deadline=time.monotonic() - 1)
        with patch.dict(os.environ, {"SILVER_SNAPSHOT_RETENTION_DAYS": "61"}):
            with self.assertRaisesRegex(IngestionError, "noncurrent"):
                expire_snapshots(self.facts(), now_ms=LATER_MS)
        self.assertEqual(location, self.facts().metadata_location)


class OrphanCleanupTests(MaintenanceBase):
    OLD = time.time() - 10 * 86_400

    def age_everything(self, root: Path):
        for path in root.rglob("*"):
            if path.is_file():
                os.utime(path, (self.OLD, self.OLD))

    def junk(self, path: Path, old: bool):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"junk")
        if old:
            os.utime(path, (self.OLD, self.OLD))
        return path

    def test_deletes_only_unreferenced_files_older_than_the_grace_period(self):
        from table_maintenance import remove_orphan_files

        dim = self.dim()
        root = local(dim.location())
        before = self.rows(dim)
        self.age_everything(root)  # referenced files are old too: age alone never deletes
        old_data = self.junk(root / "data" / "00000-crashed-writer.parquet", old=True)
        old_meta = self.junk(root / "metadata" / "snap-crashed.avro", old=True)
        fresh = self.junk(root / "data" / "00000-uncommitted-writer.parquet", old=False)
        outside = self.junk(root / "side" / "notes.json", old=True)
        summary = remove_orphan_files(dim, protected_locations=(self.facts().location(),))
        self.assertEqual(2, summary["orphans_deleted"])
        self.assertFalse(old_data.exists())
        self.assertFalse(old_meta.exists())
        self.assertTrue(fresh.exists(), "a file younger than the grace period may be in flight")
        self.assertTrue(outside.exists(), "only data/ and metadata/ are ever listed")
        dim = self.catalog.load_table("synthetic.token_dim")
        self.assertTrue(before.equals(self.rows(dim)))
        for snapshot in dim.metadata.snapshots:  # every snapshot is still fully readable
            dim.scan(snapshot_id=snapshot.snapshot_id).to_arrow()

    def test_fact_table_is_refused(self):
        from silver_persistence import IngestionError
        from table_maintenance import remove_orphan_files

        facts = self.facts()
        with self.assertRaisesRegex(IngestionError, "icp_swap_events"):
            remove_orphan_files(facts, protected_locations=(facts.location(),))

    def test_listing_cap_is_fail_closed(self):
        from silver_persistence import IngestionError
        from table_maintenance import remove_orphan_files

        dim = self.dim()
        root = local(dim.location())
        orphan = self.junk(root / "data" / "old.parquet", old=True)
        with patch.dict(os.environ, {"SILVER_MAINT_MAX_LISTED_FILES": "3"}):
            with self.assertRaisesRegex(IngestionError, "listing cap"):
                remove_orphan_files(dim, protected_locations=())
        self.assertTrue(orphan.exists())

    def test_delete_cap_bounds_each_run_and_the_next_run_continues(self):
        from table_maintenance import remove_orphan_files

        dim = self.dim()
        root = local(dim.location())
        orphans = [self.junk(root / "data" / f"old-{n}.parquet", old=True) for n in range(5)]
        with patch.dict(os.environ, {"SILVER_MAINT_MAX_DELETE_FILES": "2"}):
            first = remove_orphan_files(dim, protected_locations=())
        self.assertFalse(first["complete"])
        self.assertEqual(2, first["orphans_deleted"])
        self.assertEqual(3, first["orphans_pending"])
        self.assertEqual(3, sum(p.exists() for p in orphans))
        summary = remove_orphan_files(self.catalog.load_table("synthetic.token_dim"), protected_locations=())
        self.assertEqual(3, summary["orphans_deleted"])

    def test_grace_period_below_one_day_is_refused(self):
        from silver_persistence import IngestionError
        from table_maintenance import remove_orphan_files

        dim = self.dim()
        with patch.dict(os.environ, {"SILVER_ORPHAN_GRACE_DAYS": "0"}):
            with self.assertRaisesRegex(IngestionError, "SILVER_ORPHAN_GRACE_DAYS"):
                remove_orphan_files(dim, protected_locations=())

    def test_orphan_cleanup_is_not_reachable_from_the_per_object_path(self):
        for module in ("silver_persistence.py", "fact_compaction.py", "token_dimension.py"):
            source = (ROOT / "src" / module).read_text()
            self.assertNotIn("remove_orphan_files", source, module)
            self.assertNotIn("table_maintenance", source, module)


class CompactModeTests(MaintenanceBase):
    def setUp(self):
        super().setUp()
        import boto3

        with patch.object(boto3, "client", return_value=MagicMock()):
            import silver_job
        self.silver_job = silver_job

    def test_compact_mode_runs_compaction_retention_and_dim_orphan_cleanup_in_order(self):
        import fact_compaction
        import table_maintenance

        for n in range(3):
            self.ingest(f"obj-{n}.csv", wide_object(n))
        dim = self.dim()
        fact_root = local(self.facts().location())
        fact_junk = self.junk_old(fact_root / "data" / "unreferenced.parquet")
        quarantine = self.junk_old(fact_root / "silver-quarantine" / "g" / "quarantine-x.json")
        dim_junk = self.junk_old(local(dim.location()) / "data" / "unreferenced.parquet")
        calls = []
        real = (fact_compaction.compact_table, table_maintenance.expire_snapshots, table_maintenance.remove_orphan_files)

        def spy(name, fn):
            def wrapper(table, **kwargs):
                calls.append((name, table.name()[-1]))
                return fn(table, **kwargs)

            return wrapper

        with patch.object(self.silver_job, "_catalog", return_value=self.catalog), patch.object(
            fact_compaction, "compact_table", spy("compact", real[0])
        ), patch.object(table_maintenance, "expire_snapshots", spy("expire", real[1])), patch.object(
            table_maintenance, "remove_orphan_files", spy("orphans", real[2])
        ):
            out = self.silver_job.handler({"mode": "compact"})
        self.assertEqual(
            [("compact", "silver"), ("expire", "silver"), ("expire", "token_dim"), ("orphans", "token_dim")],
            calls,
        )
        self.assertEqual(1, out["files_after"])
        self.assertEqual({"silver", "token_dim"}, set(out["retention"]))
        self.assertEqual(1, out["orphans"]["token_dim"]["orphans_deleted"])
        self.assertFalse(dim_junk.exists())
        self.assertTrue(fact_junk.exists(), "no orphan cleanup on the fact table")
        self.assertTrue(quarantine.exists(), "quarantine artifacts are never touched")
        self.assertLess(len(json.dumps(out).encode()), 1024)

    def junk_old(self, path: Path) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"junk")
        old = time.time() - 10 * 86_400
        os.utime(path, (old, old))
        return path


if __name__ == "__main__":
    unittest.main(verbosity=2)
