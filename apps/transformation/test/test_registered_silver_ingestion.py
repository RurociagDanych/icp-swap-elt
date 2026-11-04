"""Registered Silver handler integration tests with synthetic object storage and real Iceberg."""

from __future__ import annotations

import hashlib
import io
import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import unittest
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch
from urllib.parse import unquote, urlparse

import pyarrow as pa
import pyarrow.fs as pafs
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.expressions import In
from pyiceberg.table import Transaction

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
B_HEADER = (
    "tx_hash,tx_time,action_type,pool_id,from_principal_id,to_principal_id,"
    "token0_ledger_id,token1_ledger_id,token0_amount_in,token1_amount_in,"
    "token0_amount_out,token1_amount_out"
)
A_HEADER = (
    "hash,timestamp,action,pool_id,from,to,token0_id,token1_id,amount_token0,"
    "amount_token1,token0_change,token1_change,amount_usd"
)


class SyntheticBody(io.BytesIO):
    def read(self, *args, **kwargs):
        return super().read(*args, **kwargs)


class SyntheticS3:
    """Test-only local object boundary; bytes are labelled synthetic in the fixture."""

    def __init__(self, objects):
        self.objects = objects
        self.get_calls = []

    def get_object(self, *, Bucket, Key):
        self.get_calls.append((Bucket, Key))
        return {"Body": SyntheticBody(self.objects[Key])}


class LocalOnlyFileIO:
    """Guard schemes and resolved paths before delegating to real local PyIceberg IO."""

    def __init__(self, delegate, allowed_root):
        self.delegate = delegate
        self.allowed_root = Path(allowed_root).resolve()
        self.delegated_uris = []
        self.write_uris = []
        self._original_new_input = getattr(delegate, "new_input", None)
        self._original_new_output = getattr(delegate, "new_output", None)

    def install(self):
        # Preserve PyArrowFileIO's concrete type checks while guarding its IO entry points.
        self.delegate.new_input = self.new_input
        self.delegate.new_output = self.new_output
        return self.delegate

    def __getattr__(self, name):
        return getattr(self.delegate, name)

    def _check(self, uri):
        parsed = urlparse(str(uri))
        if parsed.scheme != "file" or parsed.netloc not in ("", "localhost"):
            raise AssertionError(f"nonlocal FileIO URI rejected: {uri}")
        path = Path(unquote(parsed.path)).resolve()
        try:
            path.relative_to(self.allowed_root)
        except ValueError as exc:
            raise AssertionError(f"FileIO path escaped fixture root: {uri}") from exc
        return str(uri)

    def new_input(self, uri):
        checked = self._check(uri)
        self.delegated_uris.append(checked)
        return self._original_new_input(checked)

    def new_output(self, uri):
        checked = self._check(uri)
        self.delegated_uris.append(checked)
        self.write_uris.append(checked)
        return self._original_new_output(checked)


class TracingLocalHandler(pafs.FileSystemHandler):
    """Guard and count real Arrow filesystem data opens in the local fixture."""

    def __init__(self, allowed_root):
        self.allowed_root = Path(allowed_root).resolve()
        self.local = pafs.LocalFileSystem()
        self.parquet_reads = []
        self.parquet_writes = []

    def _path(self, value):
        path = Path(value).resolve()
        path.relative_to(self.allowed_root)
        return str(path)

    def get_type_name(self):
        return "registered-local-only"

    def normalize_path(self, path):
        return self._path(path)

    def get_file_info(self, paths):
        return self.local.get_file_info([self._path(p) for p in paths])

    def get_file_info_selector(self, selector):
        self._path(selector.base_dir)
        return self.local.get_file_info(selector)

    def create_dir(self, path, recursive):
        return self.local.create_dir(self._path(path), recursive=recursive)

    def delete_dir(self, path):
        return self.local.delete_dir(self._path(path))

    def delete_dir_contents(self, path, missing_dir_ok=False):
        return self.local.delete_dir_contents(
            self._path(path), missing_dir_ok=missing_dir_ok
        )

    def delete_root_dir_contents(self):
        raise AssertionError("fixture root deletion is forbidden")

    def delete_file(self, path):
        return self.local.delete_file(self._path(path))

    def move(self, src, dest):
        return self.local.move(self._path(src), self._path(dest))

    def copy_file(self, src, dest):
        return self.local.copy_file(self._path(src), self._path(dest))

    def open_input_stream(self, path):
        path = self._path(path)
        if path.endswith(".parquet"):
            self.parquet_reads.append(path)
        return self.local.open_input_stream(path)

    def open_input_file(self, path):
        path = self._path(path)
        if path.endswith(".parquet"):
            self.parquet_reads.append(path)
        return self.local.open_input_file(path)

    def open_output_stream(self, path, metadata=None):
        path = self._path(path)
        if path.endswith(".parquet"):
            self.parquet_writes.append(path)
        self.local.create_dir(str(Path(path).parent), recursive=True)
        return self.local.open_output_stream(path, metadata=metadata)

    def open_append_stream(self, path, metadata=None):
        path = self._path(path)
        if path.endswith(".parquet"):
            self.parquet_writes.append(path)
        return self.local.open_append_stream(path, metadata=metadata)


class RegisteredSilverIngestionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="synthetic-t09-")
        if str(ROOT / "src") not in sys.path:
            sys.path.insert(0, str(ROOT / "src"))
        base = Path(self.tmp.name)
        self.allowed_root = base / "warehouse"
        self.tracing_fs = pafs.PyFileSystem(TracingLocalHandler(self.allowed_root))
        from pyiceberg.io.pyarrow import PyArrowFileIO

        self.local_fs_patch = patch.object(
            PyArrowFileIO,
            "_initialize_fs",
            lambda _self, scheme, netloc=None: self.tracing_fs,
        )
        self.local_fs_patch.start()
        self.catalog = SqlCatalog(
            "registered-test",
            uri=f"sqlite:///{base / 'catalog.sqlite'}",
            warehouse=self.allowed_root.as_uri(),
        )
        self.catalog.create_namespace("synthetic")
        from silver_persistence import ARROW_SCHEMA

        table = self.catalog.create_table("synthetic.silver", schema=ARROW_SCHEMA)
        with table.update_spec() as update:
            from pyiceberg.transforms import DayTransform

            update.add_field("event_timestamp", DayTransform(), "event_timestamp_day")
        original_load = self.catalog.load_table
        self.file_io_guards = []
        self.transaction_hook = None

        def guarded_load(identifier):
            loaded = original_load(identifier)
            guard = LocalOnlyFileIO(loaded.io, self.allowed_root)
            loaded.io = guard.install()
            self.file_io_guards.append(guard)
            if self.transaction_hook is not None:
                self.transaction_hook(loaded)
            return loaded

        self.catalog.load_table = guarded_load
        self.env_patch = patch.dict(
            os.environ,
            {
                "RAW_BUCKET": "synthetic-raw",
                "PROCESSED_BUCKET": "synthetic-processed",
                "GLUE_DATABASE": "synthetic",
                "TABLE_NAME": "silver",
                "SILVER_TARGET_GENERATION": "test-generation-1",
                "SILVER_CATALOG": "registered-test",
                "AWS_ACCESS_KEY_ID": "synthetic-test-access-key",
                "AWS_SECRET_ACCESS_KEY": "synthetic-test-secret-key",
                "AWS_SESSION_TOKEN": "synthetic-test-session-token",
                "AWS_EC2_METADATA_DISABLED": "true",
                "AWS_WEB_IDENTITY_TOKEN_FILE": "",
                "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI": "",
                "AWS_CONTAINER_CREDENTIALS_FULL_URI": "",
                "AWS_CONFIG_FILE": str(base / "no-aws-config"),
                "AWS_SHARED_CREDENTIALS_FILE": str(base / "no-aws-credentials"),
                "AWS_PROFILE": "synthetic-test-profile",
            },
        )
        self.env_patch.start()
        import boto3
        import botocore.endpoint

        self.external_attempts = []

        def deny_transport(*_args, **_kwargs):
            raise AssertionError(
                "network transport is forbidden in registered local tests"
            )

        self.transport_patch = patch.object(
            botocore.endpoint.Endpoint, "_send", deny_transport
        )
        self.transport_patch.start()

        def deny_socket(*_args, **_kwargs):
            self.external_attempts.append("socket")
            raise AssertionError("socket access is forbidden in registered local tests")

        self.socket_patches = [
            patch.object(socket.socket, "connect", deny_socket),
            patch.object(socket.socket, "connect_ex", deny_socket),
            patch.object(socket, "create_connection", deny_socket),
        ]
        for socket_patch in self.socket_patches:
            socket_patch.start()
        self.boto_client_patch = patch.object(
            boto3, "client", return_value=SyntheticS3({})
        )
        self.boto_client_patch.start()
        self.session_client_patch = patch.object(
            boto3.session.Session, "client", return_value=SyntheticS3({})
        )
        self.session_client_patch.start()
        import silver_job

        self.session_client_patch.stop()
        self.boto_client_patch.stop()
        self.silver = silver_job
        self.silver._catalog = lambda: self.catalog

    def create_dim(self, schema, ident="synthetic.token_dim", **create_kwargs):
        """Hand-built ``token_dim`` fixture: the caller's columns (id/nullability variations
        under test) plus the one nullable ``as_of`` column production creates."""
        from pyiceberg.schema import Schema
        from pyiceberg.types import NestedField, TimestamptzType

        fields = list(schema.fields)
        fields.append(
            NestedField(
                field_id=max(f.field_id for f in fields) + 1,
                name="as_of",
                field_type=TimestamptzType(),
                required=False,
            )
        )
        return self.catalog.create_table(ident, schema=Schema(*fields), **create_kwargs)

    def tearDown(self):
        sys.modules.pop("silver_job", None)
        if str(ROOT / "src") in sys.path:
            sys.path.remove(str(ROOT / "src"))
        self.env_patch.stop()
        self.transport_patch.stop()
        for socket_patch in reversed(self.socket_patches):
            socket_patch.stop()
        self.local_fs_patch.stop()
        self.tmp.cleanup()

    def test_local_fileio_guard_rejects_nonlocal_uri_before_delegation(self):
        class Delegate:
            called = False

            def new_output(self, uri):
                self.called = True
                raise AssertionError("delegate should not receive foreign URI")

        delegate = Delegate()
        guard = LocalOnlyFileIO(delegate, self.allowed_root)
        with self.assertRaisesRegex(AssertionError, "nonlocal FileIO URI"):
            guard.new_output("s3://synthetic-processed/warehouse/unexpected")
        self.assertFalse(delegate.called)

    def test_same_field_names_with_wrong_physical_type_refuse_before_source_read(self):
        from silver_persistence import ARROW_SCHEMA

        wrong_schema = pa.schema(
            [
                pa.field(
                    field.name,
                    pa.float64() if field.name == "token0_amount" else field.type,
                    nullable=field.nullable,
                )
                for field in ARROW_SCHEMA
            ]
        )
        self.catalog.create_table("synthetic.wrong_type", schema=wrong_schema)
        self.silver.TABLE_NAME = "wrong_type"
        key = "synthetic/not-read.csv"
        s3 = SyntheticS3({key: b"synthetic fixture"})
        with patch.object(self.silver, "_s3", s3), self.assertRaises(Exception):
            self.silver.handler({"mode": "backfill_file", "key": key})
        self.assertEqual([], s3.get_calls)

    def test_legacy_lineage_root_refuses_before_source_read(self):
        from silver_persistence import GENERATION_PROPERTY

        table = self.catalog.load_table("synthetic.silver")
        with table.transaction() as tx:
            tx.set_properties(
                {
                    GENERATION_PROPERTY: "test-generation-1",
                    # retired index pointer: literal on purpose, the constant is gone
                    "silver.lineage.root": "s3://foreign-bucket/unowned/root.json",
                }
            )
        key = "synthetic/not-read.csv"
        s3 = SyntheticS3({key: b"synthetic fixture"})
        with patch.object(self.silver, "_s3", s3), self.assertRaises(Exception):
            self.silver.handler({"mode": "backfill_file", "key": key})
        self.assertEqual([], s3.get_calls)

    def test_registered_backfill_persists_exact_flat_schema_and_replay_is_noop(self):
        key = "synthetic/one.csv"
        content = (
            B_HEADER + "\n"
            "synthetic-hash,1793363500000,Swap,pool-a,alice,pool,ledger-a,ledger-b,"
            "1.125,0,0,2.75\n"
        ).encode()
        s3 = SyntheticS3({key: content})
        with patch.object(self.silver, "_s3", s3):
            result = self.silver.handler({"mode": "backfill_file", "key": key})
            table = self.catalog.load_table("synthetic.silver")
            first_snapshot = table.current_snapshot().snapshot_id
            replay = self.silver.handler({"mode": "backfill_file", "key": key})
            table = self.catalog.load_table("synthetic.silver")
            rows = table.scan().to_arrow().to_pylist()
        expected = {
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
        }
        from silver_contract import silver_identity

        self.assertEqual(1, result["rows_written"])
        self.assertEqual(0, replay["rows_written"])
        self.assertEqual(first_snapshot, table.current_snapshot().snapshot_id)
        self.assertEqual(1, len(rows))
        self.assertEqual(expected, set(rows[0]))
        self.assertEqual(
            pa.decimal128(38, 18), table.schema().as_arrow().field("token0_amount").type
        )
        self.assertEqual(
            silver_identity({"tx_hash": "synthetic-hash"}, "B"), rows[0]["event_id"]
        )
        self.assertEqual(
            "s3://synthetic-raw/synthetic/one.csv#1", rows[0]["lineage_ref"]
        )
        self.assertEqual("ok", rows[0]["quality_status"])
        self.assertEqual(2, len(s3.get_calls))

    def test_row_cap_overflow_refuses_before_fact_or_root_publication(self):
        key = "synthetic/too-many.csv"
        line = "synthetic-hash,1793363500000,Swap,pool,alice,pool,ledger-a,ledger-b,1,0,0,2"
        content = (
            B_HEADER
            + "\n"
            + line
            + "\n"
            + line.replace("synthetic-hash", "second-hash")
            + "\n"
        ).encode()
        os.environ["SILVER_MAX_SOURCE_ROWS"] = "1"
        s3 = SyntheticS3({key: content})
        with (
            patch.object(self.silver, "_s3", s3),
            self.assertRaisesRegex(RuntimeError, "source row cap exceeded"),
        ):
            self.silver.handler({"mode": "backfill_file", "key": key})
        table = self.catalog.load_table("synthetic.silver")
        self.assertIsNone(table.current_snapshot())
        self.assertNotIn("silver.lineage.root", table.properties)
        self.assertEqual(1, len(s3.get_calls))

    def test_cross_file_conflict_with_in_file_duplicate_keeps_one_row_and_exact_replay(
        self,
    ):
        first_key = "synthetic/a.csv"
        second_key = "synthetic/b.csv"
        row1 = "shared-event,1793363500000,Swap,pool,alice,pool,ledger-a,ledger-b,1.125,0,0,2.75"
        row2 = "shared-event,1793363501000,Swap,pool,alice,pool,ledger-a,ledger-b,1.125,0,0,3.75"
        objects = {
            first_key: (B_HEADER + "\n" + row1 + "\n" + row1 + "\n").encode(),
            second_key: (B_HEADER + "\n" + row2 + "\n").encode(),
        }
        s3 = SyntheticS3(objects)
        with patch.object(self.silver, "_s3", s3):
            self.silver.handler({"mode": "backfill_file", "key": first_key})
            self.silver.handler({"mode": "backfill_file", "key": second_key})
            table = self.catalog.load_table("synthetic.silver")
            before_replay = table.current_snapshot().snapshot_id
            fact = table.scan().to_arrow().to_pylist()
            replay = self.silver.handler({"mode": "backfill_file", "key": first_key})
            after_replay = (
                self.catalog.load_table("synthetic.silver").current_snapshot().snapshot_id
            )
            facts_after = self._facts()

        self.assertEqual(1, len(fact))
        self.assertEqual("conflict", fact[0]["quality_status"])
        self.assertEqual(0, replay["rows_written"])
        self.assertTrue(replay["replayed"])
        self.assertEqual(before_replay, after_replay)
        self.assertEqual(fact, facts_after)

    def test_quality_status_is_isolated_per_row_within_one_object(self):
        key = "synthetic/row-metadata.csv"
        first = "issue-event,1793363500000,FutureAction,pool,alice,pool,ledger0,ledger1,1.2300,0,0,2.500"
        second = "clean-event,1793363500000,Claim,pool,alice,pool,ledger0,ledger1,0,0,0,0.000"
        s3 = SyntheticS3(
            {key: (B_HEADER + "\n" + first + "\n" + second + "\n").encode()}
        )
        with patch.object(self.silver, "_s3", s3):
            self.silver.handler({"mode": "backfill_file", "key": key})
            facts = {row["action"]: row for row in self._facts()}

        # the semantic issue marks ONLY its own row; raw lexemes stay in the raw CSV
        self.assertEqual("unresolved", facts["unknown"]["quality_status"])
        self.assertEqual("ok", facts["claim"]["quality_status"])
        self.assertEqual(
            "s3://synthetic-raw/synthetic/row-metadata.csv#1", facts["unknown"]["lineage_ref"]
        )
        self.assertEqual(
            "s3://synthetic-raw/synthetic/row-metadata.csv#2", facts["claim"]["lineage_ref"]
        )

    def test_new_occurrence_of_same_payload_retains_original_fact_ingested_at(self):
        row = "same-payload,1793363500000,Claim,pool,alice,pool,ledger0,ledger1,0,0,0,0"
        keys = ("synthetic/first.csv", "synthetic/repeat.csv")
        s3 = SyntheticS3({key: (B_HEADER + "\n" + row + "\n").encode() for key in keys})
        with patch.object(self.silver, "_s3", s3):
            self.silver.handler({"mode": "backfill_file", "key": keys[0]})
            before = self._facts()[0]
            snapshot = self._snapshot()
            self.silver.handler({"mode": "backfill_file", "key": keys[1]})
            after = self._facts()[0]
        self.assertEqual(before["ingested_at"], after["ingested_at"])
        # an identical payload from another object is a no-op: the first provenance stays
        self.assertEqual(before["lineage_ref"], after["lineage_ref"])
        self.assertEqual(snapshot, self._snapshot())

    def test_all_bad_rows_return_readback_verified_quarantine_without_root_or_completion(
        self,
    ):
        key = "synthetic/all-bad.csv"
        invalid = ",1793363500000,Claim,pool,alice,pool,ledger0,ledger1,0,0,0,0"
        s3 = SyntheticS3({key: (B_HEADER + "\n" + invalid + "\n").encode()})
        with patch.object(self.silver, "_s3", s3):
            result = self.silver.handler({"mode": "backfill_file", "key": key})
            table = self.catalog.load_table("synthetic.silver")
            quarantine = json.loads(
                table.io.new_input(result["quarantine_uri"]).open().read()
            )
        self.assertEqual(0, result["rows_written"])
        self.assertEqual(1, result["rows_quarantined"])
        self.assertGreaterEqual(result["issue_count"], 1)
        self.assertFalse(result["replayed"])
        self.assertIsNone(table.current_snapshot())
        self.assertNotIn("silver.lineage.root", table.properties)
        self.assertEqual(
            "identity_unresolved", quarantine["observations"][0]["issues"][0]["code"]
        )

    def test_empty_and_header_only_get_durable_file_issue_locators(self):
        # invalid UTF-8 is no longer a quarantine-and-succeed case: it fails the invocation
        # (test_h2_file_level_corruption_fails_the_invocation).
        cases = (
            ("empty", b"", "missing_header"),
            ("header-only", (B_HEADER + "\n").encode(), "header_only"),
        )
        s3 = SyntheticS3(
            {f"synthetic/{name}.csv": content for name, content, _ in cases}
        )
        with patch.object(self.silver, "_s3", s3):
            for name, _content, expected in cases:
                result = self.silver.handler(
                    {"mode": "backfill_file", "key": f"synthetic/{name}.csv"}
                )
                table = self.catalog.load_table("synthetic.silver")
                artifact = json.loads(
                    table.io.new_input(result["quarantine_uri"]).open().read()
                )
                self.assertEqual(0, result["rows_written"])
                self.assertEqual(0, result["rows_observed"])
                self.assertEqual(1, result["issue_count"])
                self.assertEqual(expected, artifact["file_issues"][0]["code"])
                self.assertFalse(result["replayed"])
        self.assertIsNone(
            self.catalog.load_table("synthetic.silver").current_snapshot()
        )

    def test_mixed_good_and_missing_identity_rows_separate_observations_and_issues(
        self,
    ):
        key = "synthetic/mixed.csv"
        good = (
            "good-event,1793363500000,Claim,pool,alice,pool,ledger0,ledger1,0,0,0,1.25"
        )
        bad = ",1793363500000,Claim,pool,alice,pool,ledger0,ledger1,0,0,0,0"
        s3 = SyntheticS3({key: (B_HEADER + "\n" + good + "\n" + bad + "\n").encode()})
        with patch.object(self.silver, "_s3", s3):
            result = self.silver.handler({"mode": "backfill_file", "key": key})
            table = self.catalog.load_table("synthetic.silver")
            facts = table.scan().to_arrow().to_pylist()
            quarantine = json.loads(
                table.io.new_input(result["quarantine_uri"]).open().read()
            )
        self.assertEqual(1, result["rows_written"])
        self.assertEqual(1, result["rows_quarantined"])
        self.assertEqual(2, result["rows_observed"])
        self.assertEqual(1, result["issue_count"])
        self.assertEqual(1, len(facts))
        self.assertEqual(1, len(quarantine["observations"]))
        self.assertEqual(2, quarantine["observations"][0]["logical_record_ordinal"])

    def test_mixed_semantic_issues_and_identity_quarantine_counts_are_distinct(self):
        key = "synthetic/mixed-counts.csv"
        semantic = "semantic-event,1793363500000,Claim,pool,alice,pool,ledger0,ledger1,-1,2,0,1.25"
        missing_identity = (
            ",1793363500000,Claim,pool,alice,pool,ledger0,ledger1,0,0,0,0"
        )
        content = (B_HEADER + "\n" + semantic + "\n" + missing_identity + "\n").encode()
        s3 = SyntheticS3({key: content})
        with patch.object(self.silver, "_s3", s3):
            result = self.silver.handler({"mode": "backfill_file", "key": key})
            table = self.catalog.load_table("synthetic.silver")
            facts = table.scan().to_arrow().to_pylist()
            quarantine = json.loads(
                table.io.new_input(result["quarantine_uri"]).open().read()
            )

        self.assertEqual(2, result["rows_observed"])
        self.assertEqual(1, result["rows_quarantined"])
        self.assertGreater(result["issue_count"], result["rows_quarantined"])
        self.assertEqual(1, len(facts))
        self.assertIsNone(facts[0]["token0_amount"])
        self.assertIsNone(facts[0]["token1_amount"])
        self.assertEqual("unresolved", facts[0]["quality_status"])
        self.assertEqual(1, len(quarantine["observations"]))
        self.assertEqual(
            "identity_unresolved", quarantine["observations"][0]["issues"][0]["code"]
        )

    def test_registered_same_table_writers_reload_and_merge_after_real_commit_conflict(
        self,
    ):
        from silver_contract import silver_identity
        from silver_persistence import _payload_key

        shared = (
            "race-shared,1793363500000,Claim,pool,alice,pool,ledger0,ledger1,0,0,0,1"
        )
        seed = (
            B_HEADER + "\n" + shared + "\n"
            "race-unrelated-a,1793363500000,Claim,pool,alice,pool,ledger0,ledger1,0,0,0,3\n"
            "race-unrelated-b,1793449900000,Claim,pool,alice,pool,ledger0,ledger1,0,0,0,4\n"
        ).encode()
        keys = ("synthetic/race-a.csv", "synthetic/race-b.csv")
        objects = {
            keys[0]: (B_HEADER + "\n" + shared[:-1] + "2\n").encode(),
            keys[1]: (B_HEADER + "\n" + shared[:-1] + "5\n").encode(),
        }
        self._ingest("synthetic/race-seed.csv", seed)
        start_snapshot = self._snapshot()
        results, seen, bases = self._race(objects)

        table = self.catalog.load_table("synthetic.silver")
        facts = table.scan().to_arrow().to_pylist()
        event_id = silver_identity({"tx_hash": "race-shared"}, "B")
        unrelated_ids = {
            silver_identity({"tx_hash": value}, "B")
            for value in ("race-unrelated-a", "race-unrelated-b")
        }
        fact = next(row for row in facts if row["event_id"] == event_id)
        expected = min(
            self._records(shared, shared[:-1] + "2", shared[:-1] + "5"),
            key=_payload_key,
        )

        self.assertEqual(2, len(results))
        self.assertEqual(
            [start_snapshot, start_snapshot], [b.snapshot_id for b in bases]
        )
        self.assertEqual(1, seen.count("CommitFailedException"))
        # The fenced writer's retry re-scans: it rewrites the row only when its payload
        # outranks the winner's (1 or 2 commits depending on who won the thread race),
        # but the final state below is identical either way.
        self.assertIn(seen.count("committed"), (1, 2))
        self.assertNotEqual(start_snapshot, table.current_snapshot().snapshot_id)
        self.assertEqual("conflict", fact["quality_status"])
        self.assertEqual(expected["token1_amount"], fact["token1_amount"])
        self.assertEqual(3, len(facts))
        self.assertEqual(
            unrelated_ids,
            {row["event_id"] for row in facts if row["event_id"] != event_id},
        )

    def test_commit_conflict_budget_exhaustion_keeps_old_snapshot_and_rows(self):
        from pyiceberg.exceptions import CommitFailedException

        from silver_persistence import IngestionError

        seed_key, incoming_key = (
            "synthetic/retry-seed.csv",
            "synthetic/retry-exhausted.csv",
        )
        seed_row = (
            "retry-seed,1793363500000,Claim,pool,alice,pool,ledger0,ledger1,0,0,0,1"
        )
        incoming = (
            "retry-new,1793363500000,Claim,pool,alice,pool,ledger0,ledger1,0,0,0,2"
        )
        s3 = SyntheticS3(
            {
                seed_key: (B_HEADER + "\n" + seed_row + "\n").encode(),
                incoming_key: (B_HEADER + "\n" + incoming + "\n").encode(),
            }
        )
        with patch.object(self.silver, "_s3", s3):
            self.silver.handler({"mode": "backfill_file", "key": seed_key})
        before = self.catalog.load_table("synthetic.silver")
        state = (before.current_snapshot().snapshot_id, dict(before.properties))
        admissions = []

        def reject(tx, *_args, **_kwargs):
            admissions.append(tx)
            raise CommitFailedException("injected bounded rejection")

        with (
            patch.object(Transaction, "commit_transaction", reject),
            patch.dict(os.environ, {"SILVER_MAX_RETRIES": "2"}),
            patch.object(self.silver, "_s3", s3),
            self.assertRaisesRegex(IngestionError, "retry budget exhausted"),
        ):
            self.silver.handler({"mode": "backfill_file", "key": incoming_key})
        after = self.catalog.load_table("synthetic.silver")
        self.assertEqual(2, len(admissions))
        self.assertEqual(
            state, (after.current_snapshot().snapshot_id, dict(after.properties))
        )
        from silver_contract import silver_identity

        self.assertEqual(
            [silver_identity({"tx_hash": "retry-seed"}, "B")],
            [row["event_id"] for row in after.scan().to_arrow().to_pylist()],
        )
        self.assertEqual(2, len(s3.get_calls))

    def test_precommit_failure_leaves_old_snapshot_then_exact_retry_publishes(self):
        seed_key, incoming_key = (
            "synthetic/precommit-seed.csv",
            "synthetic/precommit-new.csv",
        )
        seed_row = (
            "precommit-seed,1793363500000,Claim,pool,alice,pool,ledger0,ledger1,0,0,0,1"
        )
        incoming = (
            "precommit-new,1793363500000,Claim,pool,alice,pool,ledger0,ledger1,0,0,0,2"
        )
        s3 = SyntheticS3(
            {
                seed_key: (B_HEADER + "\n" + seed_row + "\n").encode(),
                incoming_key: (B_HEADER + "\n" + incoming + "\n").encode(),
            }
        )
        with patch.object(self.silver, "_s3", s3):
            self.silver.handler({"mode": "backfill_file", "key": seed_key})
        before = self.catalog.load_table("synthetic.silver")
        state = (before.current_snapshot().snapshot_id, dict(before.properties))
        original = Transaction.commit_transaction
        admissions = []

        def fail_once(tx, *args, **kwargs):
            admissions.append("attempt")
            if len(admissions) == 1:
                raise RuntimeError("injected precommit failure")
            return original(tx, *args, **kwargs)

        with (
            patch.object(Transaction, "commit_transaction", fail_once),
            patch.object(self.silver, "_s3", s3),
            self.assertRaisesRegex(RuntimeError, "precommit failure"),
        ):
            self.silver.handler({"mode": "backfill_file", "key": incoming_key})
        failed = self.catalog.load_table("synthetic.silver")
        self.assertEqual(
            state, (failed.current_snapshot().snapshot_id, dict(failed.properties))
        )
        self.assertEqual(1, len(self._facts()))
        with patch.object(self.silver, "_s3", s3):
            success = self.silver.handler({"mode": "backfill_file", "key": incoming_key})
        final = self.catalog.load_table("synthetic.silver")
        self.assertEqual(1, success["rows_written"])
        self.assertEqual(2, len(final.scan().to_arrow().to_pylist()))
        self.assertEqual(1, len(admissions))

    def test_unknown_commit_outcome_is_reconciled_by_fresh_catalog_and_exact_replay(
        self,
    ):
        key = "synthetic/unknown-outcome.csv"
        row = "unknown-outcome,1793363500000,Claim,pool,alice,pool,ledger0,ledger1,0,0,0,2"
        s3 = SyntheticS3({key: (B_HEADER + "\n" + row + "\n").encode()})
        original = Transaction.commit_transaction
        acknowledgements_lost = []

        def commit_then_lose_ack(tx, *args, **kwargs):
            original(tx, *args, **kwargs)
            acknowledgements_lost.append("underlying commit succeeded")
            raise RuntimeError("injected acknowledgement loss after underlying commit")

        with (
            patch.object(Transaction, "commit_transaction", commit_then_lose_ack),
            patch.object(self.silver, "_s3", s3),
            self.assertRaisesRegex(RuntimeError, "acknowledgement loss"),
        ):
            self.silver.handler({"mode": "backfill_file", "key": key})

        from silver_contract import silver_identity

        event_id = silver_identity({"tx_hash": "unknown-outcome"}, "B")
        child = subprocess.run(
            [
                sys.executable,
                "-I",
                "-B",
                str(ROOT / "test/registered_silver_readback_probe.py"),
                f"sqlite:///{Path(self.tmp.name) / 'catalog.sqlite'}",
                self.allowed_root.as_uri(),
                event_id,
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        )
        process_readback = json.loads(child.stdout)
        self.assertEqual(1, process_readback["fact_count"])
        self.assertEqual(
            "s3://synthetic-raw/synthetic/unknown-outcome.csv#1",
            process_readback["lineage_ref"],
        )

        fresh = SqlCatalog(
            "registered-test",
            uri=f"sqlite:///{Path(self.tmp.name) / 'catalog.sqlite'}",
            warehouse=self.allowed_root.as_uri(),
        )
        table = fresh.load_table("synthetic.silver")
        snapshot = table.current_snapshot().snapshot_id
        self.assertEqual(1, len(table.scan().to_arrow().to_pylist()))
        self.catalog = fresh
        with (
            patch.object(self.silver, "_catalog", lambda: fresh),
            patch.object(self.silver, "_s3", s3),
        ):
            replay = self.silver.handler({"mode": "backfill_file", "key": key})
        after = fresh.load_table("synthetic.silver")
        self.assertEqual(1, len(acknowledgements_lost))
        self.assertEqual(0, replay["rows_written"])
        self.assertTrue(replay["replayed"])
        self.assertEqual(snapshot, after.current_snapshot().snapshot_id)
        self.assertEqual(1, len(after.scan().to_arrow().to_pylist()))

    def test_same_day_unrelated_file_does_not_consume_selection_cap_or_parquet_io(self):
        target = (
            "target-event,1793363500000,Claim,pool,alice,pool,ledger0,ledger1,0,0,0,1"
        )
        sibling = (
            "sibling-event,1793363500000,Claim,pool,alice,pool,ledger0,ledger1,0,0,0,3"
        )
        updated = target[:-1] + "2"
        keys = ("synthetic/target.csv", "synthetic/sibling.csv", "synthetic/update.csv")
        objects = {
            keys[0]: (B_HEADER + "\n" + target + "\n").encode(),
            keys[1]: (B_HEADER + "\n" + sibling + "\n").encode(),
            keys[2]: (B_HEADER + "\n" + updated + "\n").encode(),
        }
        s3 = SyntheticS3(objects)
        with patch.object(self.silver, "_s3", s3):
            self.silver.handler({"mode": "backfill_file", "key": keys[0]})
            self.silver.handler({"mode": "backfill_file", "key": keys[1]})
            table = self.catalog.load_table("synthetic.silver")
            from silver_contract import silver_identity

            target_id = silver_identity({"tx_hash": "target-event"}, "B")
            sibling_id = silver_identity({"tx_hash": "sibling-event"}, "B")
            target_path = (
                table.scan(row_filter=In("event_id", [target_id]))
                .plan_files()[0]
                .file.file_path
            )
            self.tracing_fs.handler.parquet_reads.clear()
            self.tracing_fs.handler.parquet_writes.clear()
            os.environ["SILVER_MAX_SELECTED_FILES"] = "1"
            self.silver.handler({"mode": "backfill_file", "key": keys[2]})
            parquet_inputs = list(self.tracing_fs.handler.parquet_reads)
            parquet_outputs = list(self.tracing_fs.handler.parquet_writes)
            rows = (
                self.catalog.load_table("synthetic.silver")
                .scan()
                .to_arrow()
                .to_pylist()
            )
        # the lookup AND the delete read the target's file; the post-commit verification reads
        # the file this commit just wrote; the same-day sibling is never opened
        self.assertEqual(
            {unquote(urlparse(target_path).path)} | set(parquet_outputs),
            set(parquet_inputs),
        )
        self.assertEqual(1, len(parquet_outputs))
        self.assertEqual({target_id, sibling_id}, {row["event_id"] for row in rows})

    def test_deadline_is_rechecked_after_real_append_before_transaction_commit(self):
        import silver_persistence
        from silver_persistence import IngestionError

        clock = [0.0]
        admissions = []
        original_append = Transaction.append
        original_commit = Transaction.commit_transaction

        def append(tx, *args, **kwargs):
            result = original_append(tx, *args, **kwargs)
            clock[0] = 2.0
            return result

        def commit(tx, *args, **kwargs):
            admissions.append(clock[0])
            return original_commit(tx, *args, **kwargs)

        key = "synthetic/deadline.csv"
        row = (
            "deadline-event,1793363500000,Claim,pool,alice,pool,ledger0,ledger1,0,0,0,1"
        )
        s3 = SyntheticS3({key: (B_HEADER + "\n" + row + "\n").encode()})
        with (
            patch.dict(os.environ, {"SILVER_DEADLINE_SECONDS": "1"}),
            patch.object(silver_persistence.time, "monotonic", lambda: clock[0]),
            patch.object(Transaction, "append", append),
            patch.object(Transaction, "commit_transaction", commit),
            patch.object(self.silver, "_s3", s3),
            self.assertRaises(IngestionError),
        ):
            self.silver.handler({"mode": "backfill_file", "key": key})
        table = self.catalog.load_table("synthetic.silver")
        self.assertEqual([], admissions)
        self.assertIsNone(table.current_snapshot())
        self.assertNotIn("silver.lineage.root", table.properties)
        self.assertEqual(1, len(s3.get_calls))
        with patch.object(self.silver, "_s3", s3):
            self.assertEqual(
                1,
                self.silver.handler({"mode": "backfill_file", "key": key})["rows_written"],
            )

    def test_null_partition_sibling_does_not_consume_selection_cap_or_open(self):
        target = "target-null,,Claim,pool,alice,pool,ledger0,ledger1,0,0,0,1"
        sibling = "sibling-null,,Claim,pool,alice,pool,ledger0,ledger1,0,0,0,3"
        updated = target[:-1] + "2"
        keys = (
            "synthetic/null-target.csv",
            "synthetic/null-sibling.csv",
            "synthetic/null-update.csv",
        )
        s3 = SyntheticS3(
            {
                keys[0]: (B_HEADER + "\n" + target + "\n").encode(),
                keys[1]: (B_HEADER + "\n" + sibling + "\n").encode(),
                keys[2]: (B_HEADER + "\n" + updated + "\n").encode(),
            }
        )
        with patch.object(self.silver, "_s3", s3):
            self.silver.handler({"mode": "backfill_file", "key": keys[0]})
            self.silver.handler({"mode": "backfill_file", "key": keys[1]})
            table = self.catalog.load_table("synthetic.silver")
            from silver_contract import silver_identity

            target_id = silver_identity({"tx_hash": "target-null"}, "B")
            sibling_id = silver_identity({"tx_hash": "sibling-null"}, "B")
            target_path = (
                table.scan(row_filter=In("event_id", [target_id]))
                .plan_files()[0]
                .file.file_path
            )
            self.tracing_fs.handler.parquet_reads.clear()
            self.tracing_fs.handler.parquet_writes.clear()
            os.environ["SILVER_MAX_SELECTED_FILES"] = "1"
            self.silver.handler({"mode": "backfill_file", "key": keys[2]})
            reads = list(self.tracing_fs.handler.parquet_reads)
            writes = list(self.tracing_fs.handler.parquet_writes)
            rows = (
                self.catalog.load_table("synthetic.silver")
                .scan()
                .to_arrow()
                .to_pylist()
            )
        # lookup + the verification of the file this commit wrote; the null-partition sibling
        # is never opened
        self.assertEqual(
            {unquote(urlparse(target_path).path)} | set(writes), set(reads)
        )
        self.assertEqual(1, len(writes))
        self.assertEqual({target_id, sibling_id}, {row["event_id"] for row in rows})

    def test_affected_same_day_cofile_is_rewritten_and_sibling_fact_survives(self):
        target = "co-target,1793363500000,Claim,pool,alice,pool,ledger0,ledger1,0,0,0,1"
        sibling = (
            "co-sibling,1793363500000,Claim,pool,alice,pool,ledger0,ledger1,0,0,0,3"
        )
        updated = target[:-1] + "2"
        keys = ("synthetic/cofile.csv", "synthetic/cofile-update.csv")
        s3 = SyntheticS3(
            {
                keys[0]: (B_HEADER + "\n" + target + "\n" + sibling + "\n").encode(),
                keys[1]: (B_HEADER + "\n" + updated + "\n").encode(),
            }
        )
        with patch.object(self.silver, "_s3", s3):
            self.silver.handler({"mode": "backfill_file", "key": keys[0]})
            table = self.catalog.load_table("synthetic.silver")
            tasks = table.scan().plan_files()
            self.assertEqual(1, len(tasks))
            self.assertEqual(2, tasks[0].file.record_count)
            cofile_path = unquote(urlparse(tasks[0].file.file_path).path)
            os.environ["SILVER_MAX_SELECTED_FILES"] = "1"
            os.environ["SILVER_MAX_SELECTED_ROWS"] = "2"
            os.environ["SILVER_MAX_SELECTED_BYTES"] = str(
                tasks[0].file.file_size_in_bytes
            )
            self.tracing_fs.handler.parquet_reads.clear()
            self.tracing_fs.handler.parquet_writes.clear()
            self.silver.handler({"mode": "backfill_file", "key": keys[1]})
            reads = list(self.tracing_fs.handler.parquet_reads)
            writes = list(self.tracing_fs.handler.parquet_writes)
            rows = (
                self.catalog.load_table("synthetic.silver")
                .scan()
                .to_arrow()
                .to_pylist()
            )
        self.assertIn(cofile_path, reads)
        self.assertGreaterEqual(len(writes), 1)
        self.assertEqual(2, len(rows))
        from silver_csv import normalize_csv
        from silver_persistence import _payload_key

        def lineage(_ordinal, _row):
            return {"lineage_ref": "test", "ingested_at": "2026-10-30T00:00:00Z"}

        old_record, _ = normalize_csv(B_HEADER + "\n" + target, "B", lineage)
        new_record, _ = normalize_csv(B_HEADER + "\n" + updated, "B", lineage)
        selected_payload = min((old_record[0], new_record[0]), key=_payload_key)
        target_fact = next(
            row for row in rows if row["event_id"] == selected_payload["event_id"]
        )
        sibling_fact = next(
            row for row in rows if row["event_id"] != selected_payload["event_id"]
        )
        self.assertEqual(
            selected_payload["token1_amount"], target_fact["token1_amount"]
        )
        self.assertEqual(Decimal(3), sibling_fact["token1_amount"])

    def test_registered_a_sentinel_hash_identity_keeps_usd_and_full_occurrence_locator(
        self,
    ):
        key = "synthetic/a-composite.csv"
        first = "hash,1713229323,Claim,pool,alice,pool,ledger0,ledger1,1.000,2.00,1.000,2.00,10.00"
        second = "hash,1713229323,Claim,pool,alice,pool,ledger0,ledger1,1.000,2.00,1.000,2.00,11.00"
        content = (A_HEADER + "\n" + first + "\n" + second + "\n").encode()
        s3 = SyntheticS3({key: content})
        with patch.object(self.silver, "_s3", s3):
            result = self.silver.handler({"mode": "backfill_file", "key": key})
            table = self.catalog.load_table("synthetic.silver")
            facts = table.scan().to_arrow().to_pylist()
        from silver_contract import silver_identity

        self.assertEqual(2, result["rows_written"])
        self.assertEqual(2, len(facts))
        self.assertEqual(2, len({fact["event_id"] for fact in facts}))
        self.assertEqual(
            {
                silver_identity(
                    {
                        "hash": "hash",
                        "timestamp": "1713229323",
                        "action": "Claim",
                        "pool_id": "pool",
                        "from": "alice",
                        "to": "pool",
                        "amount_token0": "1.000",
                        "amount_token1": "2.00",
                        "token0_change": "1.000",
                        "token1_change": "2.00",
                        "amount_usd": "10.00",
                    },
                    "A",
                ),
                silver_identity(
                    {
                        "hash": "hash",
                        "timestamp": "1713229323",
                        "action": "Claim",
                        "pool_id": "pool",
                        "from": "alice",
                        "to": "pool",
                        "amount_token0": "1.000",
                        "amount_token1": "2.00",
                        "token0_change": "1.000",
                        "token1_change": "2.00",
                        "amount_usd": "11.00",
                    },
                    "A",
                ),
            },
            {fact["event_id"] for fact in facts},
        )
        self.assertEqual(
            {f"s3://synthetic-raw/{key}#1", f"s3://synthetic-raw/{key}#2"},
            {fact["lineage_ref"] for fact in facts},
        )
        # the two rows differ ONLY in amount_usd, which is part of the identity
        self.assertEqual(
            {Decimal("10"), Decimal("11")}, {fact["amount_usd"] for fact in facts}
        )

    def test_quarantine_exact_retry_reuses_locator_without_false_replay(self):
        key = "synthetic/quarantine-retry.csv"
        invalid = ",1793363500000,Claim,pool,alice,pool,ledger0,ledger1,0,0,0,0"
        content = (B_HEADER + "\n" + invalid + "\n").encode()
        s3 = SyntheticS3({key: content})
        with patch.object(self.silver, "_s3", s3):
            first = self.silver.handler({"mode": "backfill_file", "key": key})
            table = self.catalog.load_table("synthetic.silver")
            path = Path(unquote(urlparse(first["quarantine_uri"]).path))
            bytes_first = path.read_bytes()
            second = self.silver.handler({"mode": "backfill_file", "key": key})
            bytes_second = path.read_bytes()
        self.assertEqual(first["quarantine_uri"], second["quarantine_uri"])
        self.assertEqual(bytes_first, bytes_second)
        self.assertFalse(first["replayed"])
        self.assertFalse(second["replayed"])
        self.assertEqual(1, first["rows_quarantined"])
        self.assertEqual(1, second["rows_quarantined"])
        self.assertIsNone(table.current_snapshot())
        self.assertNotIn("silver.lineage.root", table.properties)

    def test_quarantine_write_and_readback_failures_never_return_completion(self):
        import silver_persistence

        key = "synthetic/quarantine-failure.csv"
        invalid = ",1793363500000,Claim,pool,alice,pool,ledger0,ledger1,0,0,0,0"
        s3 = SyntheticS3({key: (B_HEADER + "\n" + invalid + "\n").encode()})
        original_read = silver_persistence._metadata_read
        for failure in ("write", "readback"):
            with self.subTest(failure=failure), patch.object(self.silver, "_s3", s3):
                if failure == "write":
                    patcher = patch.object(
                        silver_persistence,
                        "_metadata_write",
                        side_effect=silver_persistence.IngestionError(
                            "injected quarantine write failure"
                        ),
                    )
                else:

                    def fail_quarantine_read(*args, **kwargs):
                        if len(args) > 4 and args[4] == "quarantine":
                            raise silver_persistence.IngestionError(
                                "injected quarantine readback failure"
                            )
                        return original_read(*args, **kwargs)

                    patcher = patch.object(
                        silver_persistence,
                        "_metadata_read",
                        side_effect=fail_quarantine_read,
                    )
                with patcher, self.assertRaises(silver_persistence.IngestionError):
                    self.silver.handler({"mode": "backfill_file", "key": key})
                table = self.catalog.load_table("synthetic.silver")
                self.assertIsNone(table.current_snapshot())
                self.assertNotIn("silver.lineage.root", table.properties)

    def test_duplicate_stored_event_id_refuses_without_guessing_or_writing(self):
        import silver_persistence as persistence

        seed_key = "synthetic/dup-seed.csv"
        row = "dup-stored,1793363500000,Claim,pool,alice,pool,ledger0,ledger1,0,0,0,1"
        self._ingest(seed_key, self._b_object(row))
        table = self.catalog.load_table("synthetic.silver")
        # corruption the dedup model must fail closed on: a second stored row for the
        # same event_id (e.g. written by another tool or by the known different-day limit)
        stored = table.scan().to_arrow()
        table.append(stored)
        self.assertEqual(2, len(self._facts()))
        state = (self._snapshot(), dict(table.refresh().properties))
        self.tracing_fs.handler.parquet_writes.clear()
        update_key = "synthetic/dup-update.csv"
        update = row[:-1] + "2"
        update_s3 = SyntheticS3({update_key: self._b_object(update)})
        with (
            patch.object(self.silver, "_s3", update_s3),
            self.assertRaisesRegex(persistence.IngestionError, "multiple stored rows"),
        ):
            self.silver.handler({"mode": "backfill_file", "key": update_key})
        after = self.catalog.load_table("synthetic.silver")
        self.assertEqual(state, (self._snapshot(), dict(after.properties)))
        self.assertEqual([], self.tracing_fs.handler.parquet_writes)
        self.assertEqual(2, len(self._facts()))

    def test_short_and_extra_csv_rows_are_losslessly_quarantined_and_replayable(self):
        import silver_persistence as persistence

        self.assertTrue(
            Path(persistence.__file__).resolve().is_relative_to(ROOT / "src")
        )
        key = "synthetic/malformed-widths.csv"
        good = "good-width,1793363500000,Claim,pool,alice,pool,ledger0,ledger1,0,0,0,1"
        short = "short-width,1793363500000,Claim,pool,alice,pool,ledger0"
        long = "long-width,1793363500000,Claim,pool,alice,pool,ledger0,ledger1,0,0,0,2,EXTRA-A,EXTRA-B"
        content = (B_HEADER + "\n" + good + "\n" + short + "\n" + long + "\n").encode()
        with patch.object(self.silver, "_s3", SyntheticS3({key: content})):
            first = self.silver.handler({"mode": "backfill_file", "key": key})
            quarantine_path = Path(unquote(urlparse(first["quarantine_uri"]).path))
            first_bytes = quarantine_path.read_bytes()
            second = self.silver.handler({"mode": "backfill_file", "key": key})
            second_bytes = quarantine_path.read_bytes()
        self.assertEqual(1, first["rows_written"])
        self.assertEqual(2, first["rows_quarantined"])
        self.assertEqual(first["quarantine_uri"], second["quarantine_uri"])
        self.assertEqual(first_bytes, second_bytes)
        quarantine = json.loads(first_bytes)
        observations = quarantine["observations"]
        self.assertEqual(
            [2, 3], [item["logical_record_ordinal"] for item in observations]
        )
        self.assertEqual("short-width", observations[0]["raw"]["values"][0])
        self.assertEqual("long-width", observations[1]["raw"]["values"][0])
        self.assertEqual(
            ["EXTRA-A", "EXTRA-B"], observations[1]["raw"]["extra_columns"]
        )
        self.assertEqual(
            1,
            len(
                self.catalog.load_table("synthetic.silver")
                .scan()
                .to_arrow()
                .to_pylist()
            ),
        )

    def test_numeric_equivalent_payloads_coalesce_but_different_amount_conflicts(self):
        import silver_persistence as persistence

        self.assertTrue(
            Path(persistence.__file__).resolve().is_relative_to(ROOT / "src")
        )
        key = "synthetic/numeric-equivalence.csv"
        rows = [
            "same-number,1793363500000,Claim,pool,alice,pool,ledger0,ledger1,0,0,0,1",
            "same-number,1793363500000,Claim,pool,alice,pool,ledger0,ledger1,0,0,0,1.0",
            "same-number,1793363500000,Claim,pool,alice,pool,ledger0,ledger1,0,0,0,1.000",
            "same-number,1793363500000,Claim,pool,alice,pool,ledger0,ledger1,0,0,0,10e-1",
        ]
        with patch.object(
            self.silver,
            "_s3",
            SyntheticS3({key: (B_HEADER + "\n" + "\n".join(rows) + "\n").encode()}),
        ):
            result = self.silver.handler({"mode": "backfill_file", "key": key})
        fact = self._facts()[0]
        self.assertEqual(1, result["rows_written"])
        # four numerically equivalent lexemes are ONE payload: no conflict, first ordinal
        self.assertEqual("ok", fact["quality_status"])
        self.assertEqual(
            "s3://synthetic-raw/synthetic/numeric-equivalence.csv#1", fact["lineage_ref"]
        )
        # the same numbers stored in another object are an exact replay (round trip stable)
        snapshot = self._snapshot()
        same = self._ingest(
            "synthetic/numeric-again.csv", self._b_object(rows[1], rows[3])
        )
        self.assertEqual(0, same["rows_written"])
        self.assertEqual(snapshot, self._snapshot())
        conflict_key = "synthetic/numeric-conflict.csv"
        conflict_row = (
            "same-number,1793363500000,Claim,pool,alice,pool,ledger0,ledger1,0,0,0,1.01"
        )
        with patch.object(
            self.silver,
            "_s3",
            SyntheticS3(
                {conflict_key: (B_HEADER + "\n" + conflict_row + "\n").encode()}
            ),
        ):
            self.silver.handler({"mode": "backfill_file", "key": conflict_key})
        conflicted = self._facts()[0]
        self.assertEqual("conflict", conflicted["quality_status"])

    def test_registered_a_b_complementary_reversed_slots_fresh_readback(self):
        """Metadata is durably merged from both slots before unchanged fact writes."""
        from pyiceberg.schema import Schema
        from pyiceberg.types import IntegerType, NestedField, StringType

        self.create_dim(
            schema=Schema(
                NestedField(
                    field_id=1, name="ledger_id", field_type=StringType(), required=True
                ),
                NestedField(
                    field_id=2, name="symbol", field_type=StringType(), required=False
                ),
                NestedField(
                    field_id=3, name="name", field_type=StringType(), required=False
                ),
                NestedField(
                    field_id=4, name="standard", field_type=StringType(), required=False
                ),
                NestedField(
                    field_id=5,
                    name="decimals",
                    field_type=IntegerType(),
                    required=False,
                ),
            ),
        )
        dim = self.catalog.load_table("synthetic.token_dim")
        dim.append(
            pa.Table.from_pylist(
                [
                    {
                        "ledger_id": "ledger-a",
                        "symbol": "A",
                        "name": None,
                        "standard": None,
                        "decimals": None,
                    },
                    {
                        "ledger_id": "ledger-b",
                        "symbol": "B",
                        "name": None,
                        "standard": None,
                        "decimals": None,
                    },
                    {
                        "ledger_id": "unrelated",
                        "symbol": "KEEP",
                        "name": "Keep",
                        "standard": "icrc1",
                        "decimals": 8,
                    },
                ],
                schema=dim.schema().as_arrow(),
            )
        )
        dim_uuid, dim_location, dim_schema = (
            dim.metadata.table_uuid,
            dim.location(),
            dim.schema(),
        )
        key_a, key_b = "synthetic/a.csv", "synthetic/b.csv"
        a_header = "hash,timestamp,action,pool_id,from,to,token0_id,token0_symbol,token0_standard,token0_decimals,token1_id,token1_symbol,token1_standard,token1_decimals,amount_token0,amount_token1,token0_change,token1_change,amount_usd"
        b_header = "tx_hash,tx_time,action_type,pool_id,from_principal_id,to_principal_id,token0_ledger_id,token0_symbol,token0_name,token1_ledger_id,token1_symbol,token1_name,token0_amount_in,token1_amount_in,token0_amount_out,token1_amount_out"
        a = (
            a_header
            + "\nha,1713229323,Swap,pool,alice,pool,ledger-a,A,icrc1,0,ledger-b,B,icrc1,8,1,2,1,2,4\n"
        ).encode()
        b = (
            b_header
            + "\nhb,1793363500000,Swap,pool,alice,pool,ledger-b,B,Token B,ledger-a,A,Token A,1,0,0,2\n"
        ).encode()
        s3 = SyntheticS3({key_a: a, key_b: b})
        with patch.object(self.silver, "_s3", s3):
            first = self.silver.handler({"mode": "backfill_file", "key": key_a})
            second = self.silver.handler({"mode": "backfill_file", "key": key_b})
            rows = (
                self.catalog.load_table("synthetic.silver")
                .scan()
                .to_arrow()
                .to_pylist()
            )
            dim_before_replay = (
                self.catalog.load_table("synthetic.token_dim")
                .current_snapshot()
                .snapshot_id
            )
            replay = self.silver.handler({"mode": "backfill_file", "key": key_a})
            dim_after_replay = (
                self.catalog.load_table("synthetic.token_dim")
                .current_snapshot()
                .snapshot_id
            )
        self.assertEqual(1, first["rows_written"])
        self.assertEqual(1, second["rows_written"])
        self.assertEqual(0, replay["rows_written"])
        self.assertEqual(dim_before_replay, dim_after_replay)
        self.assertEqual(2, len(rows))
        self.assertTrue(all(len(row) == 15 for row in rows))
        self.assertEqual(
            {"ledger-a", "ledger-b"},
            {r[f"token{i}_ledger_id"] for r in rows for i in (0, 1)},
        )
        self.assertEqual(
            {Decimal(1), Decimal(2)},
            {r[f"token{i}_amount"] for r in rows for i in (0, 1)},
        )
        child = subprocess.run(
            [
                sys.executable,
                "-I",
                "-B",
                str(ROOT / "test/registered_token_metadata_readback_probe.py"),
                f"sqlite:///{Path(self.tmp.name) / 'catalog.sqlite'}",
                str(self.allowed_root),
                "ledger-b,ledger-a,ledger-b",
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        )
        readback = json.loads(child.stdout.splitlines()[-1])
        tokens = readback["result"]["tokens"]
        self.assertEqual([], readback["external_attempts"])
        self.assertTrue(
            Path(readback["origins"]["silver_job"]).resolve().is_relative_to(ROOT / "src")
        )
        self.assertEqual(
            ["ledger-b", "ledger-a"], [token["ledger_id"] for token in tokens]
        )
        self.assertEqual("Token B", tokens[0]["name"])
        self.assertEqual("Token A", tokens[1]["name"])
        self.assertEqual("icrc1", tokens[0]["standard"])
        self.assertEqual(0, tokens[1]["decimals"])
        dim_after = self.catalog.load_table("synthetic.token_dim")
        self.assertEqual(dim_uuid, dim_after.metadata.table_uuid)
        self.assertEqual(dim_location, dim_after.location())
        self.assertEqual(dim_schema, dim_after.schema())
        self.assertEqual(
            "unrelated",
            dim_after.scan(row_filter=In("ledger_id", ["unrelated"]))
            .to_arrow()
            .to_pylist()[0]["ledger_id"],
        )
        self.assertEqual([], self.external_attempts)

    def test_registered_dimension_nullable_key_int64_schema_conflict_and_cofile_preservation(
        self,
    ):
        from pyiceberg.schema import Schema
        from pyiceberg.types import LongType, NestedField, StringType

        from token_dimension import MetadataError, apply_metadata

        self.create_dim(
            schema=Schema(
                NestedField(
                    field_id=11,
                    name="ledger_id",
                    field_type=StringType(),
                    required=False,
                ),
                NestedField(
                    field_id=12, name="symbol", field_type=StringType(), required=True
                ),
                NestedField(
                    field_id=13, name="name", field_type=StringType(), required=False
                ),
                NestedField(
                    field_id=14,
                    name="standard",
                    field_type=StringType(),
                    required=False,
                ),
                NestedField(
                    field_id=15, name="decimals", field_type=LongType(), required=False
                ),
            ),
        )
        dim = self.catalog.load_table("synthetic.token_dim")
        dim.append(
            pa.Table.from_pylist(
                [
                    {
                        "ledger_id": "ledger-a",
                        "symbol": "A",
                        "name": None,
                        "standard": None,
                        "decimals": None,
                    },
                    {
                        "ledger_id": None,
                        "symbol": "NULL-KEY",
                        "name": "Preserve",
                        "standard": None,
                        "decimals": None,
                    },
                ],
                schema=dim.schema().as_arrow(),
            )
        )
        seed_snapshot = dim.current_snapshot().snapshot_id
        data = b"token0_ledger_id,token0_symbol,token0_name\nledger-a,A,Token A\n"
        observations, issues = __import__("token_dimension").extract_metadata(
            data,
            "B",
            {
                "bucket": "synthetic-raw",
                "key": "dim.csv",
                "source_version": "v1",
                "content_sha256": "0" * 64,
            },
            max_records=10,
        )
        self.assertEqual(
            [{"code": "missing_ledger_id", "logical_record_ordinal": 1, "slot": 1}],
            issues,
        )
        applied = apply_metadata(dim, observations)
        self.assertIn("ledger-a", applied["changed_ledgers"])
        after = self.catalog.load_table("synthetic.token_dim")
        rows = after.scan().to_arrow().to_pylist()
        self.assertEqual(2, len(rows))
        self.assertEqual(
            "Preserve", next(row for row in rows if row["ledger_id"] is None)["name"]
        )
        self.assertEqual(
            "Token A",
            next(row for row in rows if row["ledger_id"] == "ledger-a")["name"],
        )
        snapshot = after.current_snapshot().snapshot_id
        equal = apply_metadata(after, observations)
        self.assertEqual([], equal["changed_ledgers"])
        self.assertEqual(snapshot, after.current_snapshot().snapshot_id)
        # An undated differing
        # symbol ties with the stored undated one and the greater value wins; nothing raises.
        conflict = [dict(observations[0], symbol="CONFLICT")]
        resolved = apply_metadata(after, conflict)
        self.assertEqual(["ledger-a"], resolved["changed_ledgers"])
        self.assertEqual(1, resolved["conflicts_resolved"])
        self.assertEqual("CONFLICT", resolved["conflicts"][0]["chosen"])
        winner = self.catalog.load_table("synthetic.token_dim")
        self.assertNotEqual(snapshot, winner.current_snapshot().snapshot_id)
        winner_snapshot = winner.current_snapshot().snapshot_id
        replay = apply_metadata(winner, observations)  # the lesser value cannot revert it
        self.assertEqual([], replay["changed_ledgers"])
        self.assertEqual(
            winner_snapshot,
            self.catalog.load_table("synthetic.token_dim")
            .current_snapshot()
            .snapshot_id,
        )
        self.assertNotEqual(seed_snapshot, snapshot)

    def test_staged_byte_cap_observes_real_dimension_parquet_before_commit(self):
        from pyiceberg.schema import Schema
        from pyiceberg.types import IntegerType, NestedField, StringType

        from token_dimension import MetadataError, apply_metadata, extract_metadata

        self.create_dim(
            schema=Schema(
                NestedField(
                    field_id=1, name="ledger_id", field_type=StringType(), required=True
                ),
                NestedField(
                    field_id=2, name="symbol", field_type=StringType(), required=False
                ),
                NestedField(
                    field_id=3, name="name", field_type=StringType(), required=False
                ),
                NestedField(
                    field_id=4, name="standard", field_type=StringType(), required=False
                ),
                NestedField(
                    field_id=5,
                    name="decimals",
                    field_type=IntegerType(),
                    required=False,
                ),
            ),
        )
        dim = self.catalog.load_table("synthetic.token_dim")
        dim.append(
            pa.Table.from_pylist(
                [
                    {
                        "ledger_id": "ledger-a",
                        "symbol": None,
                        "name": None,
                        "standard": None,
                        "decimals": None,
                    },
                    {
                        "ledger_id": "unrelated",
                        "symbol": "KEEP",
                        "name": "Keep",
                        "standard": "icrc1",
                        "decimals": 8,
                    },
                ],
                schema=dim.schema().as_arrow(),
            )
        )
        before = dim.current_snapshot().snapshot_id
        observations, _ = extract_metadata(
            b"token0_ledger_id,token0_symbol\nledger-a,A\n",
            "B",
            {
                "bucket": "synthetic-raw",
                "key": "cap.csv",
                "source_version": "v1",
                "content_sha256": "0" * 64,
            },
            max_records=10,
        )
        self.tracing_fs.handler.parquet_writes.clear()
        with self.assertRaisesRegex(MetadataError, "staged byte cap"):
            apply_metadata(dim, observations, limits={"max_staged_bytes": 1})
        self.assertGreaterEqual(len(self.tracing_fs.handler.parquet_writes), 1)
        after = self.catalog.load_table("synthetic.token_dim")
        self.assertEqual(before, after.current_snapshot().snapshot_id)
        self.assertEqual(2, len(after.scan().to_arrow().to_pylist()))

    def test_dimension_selection_caps_refuse_before_parquet_data_io(self):
        from pyiceberg.schema import Schema
        from pyiceberg.types import IntegerType, NestedField, StringType

        from token_dimension import MetadataError, apply_metadata, extract_metadata

        self.create_dim(
            schema=Schema(
                NestedField(
                    field_id=1, name="ledger_id", field_type=StringType(), required=True
                ),
                NestedField(
                    field_id=2, name="symbol", field_type=StringType(), required=False
                ),
                NestedField(
                    field_id=3, name="name", field_type=StringType(), required=False
                ),
                NestedField(
                    field_id=4, name="standard", field_type=StringType(), required=False
                ),
                NestedField(
                    field_id=5,
                    name="decimals",
                    field_type=IntegerType(),
                    required=False,
                ),
            ),
        )
        dim = self.catalog.load_table("synthetic.token_dim")
        dim.append(
            pa.Table.from_pylist(
                [
                    {
                        "ledger_id": "ledger-a",
                        "symbol": None,
                        "name": None,
                        "standard": None,
                        "decimals": None,
                    },
                ],
                schema=dim.schema().as_arrow(),
            )
        )
        observations, _ = extract_metadata(
            b"token0_ledger_id,token0_symbol\nledger-a,A\n",
            "B",
            {
                "bucket": "synthetic-raw",
                "key": "cap.csv",
                "source_version": "v1",
                "content_sha256": "0" * 64,
            },
            max_records=10,
        )
        before = dim.current_snapshot().snapshot_id
        for limits in ({"max_files": 0}, {"max_rows": 0}, {"max_bytes": 0}):
            self.tracing_fs.handler.parquet_reads.clear()
            with (
                self.subTest(limits=limits),
                self.assertRaisesRegex(MetadataError, "cap"),
            ):
                apply_metadata(dim, observations, limits=limits)
            self.assertEqual([], self.tracing_fs.handler.parquet_reads)
            self.assertEqual(
                before,
                self.catalog.load_table("synthetic.token_dim")
                .current_snapshot()
                .snapshot_id,
            )
        data_files = list(
            dim.scan(row_filter=In("ledger_id", ["ledger-a"])).plan_files()
        )
        self.assertEqual(1, len(data_files))
        exact_limits = {
            "max_files": 1,
            "max_rows": sum(task.file.record_count for task in data_files),
            "max_bytes": sum(task.file.file_size_in_bytes for task in data_files),
        }
        success = apply_metadata(dim, observations, limits=exact_limits)
        self.assertEqual(["ledger-a"], success["changed_ledgers"])
        self.assertEqual(1, len(self.tracing_fs.handler.parquet_reads))
        self.assertEqual(
            "A",
            self.catalog.load_table("synthetic.token_dim")
            .scan()
            .to_arrow()
            .to_pylist()[0]["symbol"],
        )

    def test_dimension_icp_standard_precedence_wins_both_directions_other_conflicts_resolve(
        self,
    ):
        """The ICP ledger is recorded with
        standard="ICP" in some rows and "ICRC2" in others (it genuinely exposes both
        a legacy ICP-ledger interface and an ICRC-2 interface). "ICP" must win over
        any conflicting value for this merge-time path too (existing dim row vs a
        newly supplied candidate), in both directions, while an unrelated standard
        conflict (neither side "ICP") is resolved deterministically instead of raising.
        """
        from pyiceberg.schema import Schema
        from pyiceberg.types import IntegerType, NestedField, StringType

        from token_dimension import MetadataError, apply_metadata

        self.create_dim(
            schema=Schema(
                NestedField(
                    field_id=1, name="ledger_id", field_type=StringType(), required=True
                ),
                NestedField(
                    field_id=2, name="symbol", field_type=StringType(), required=False
                ),
                NestedField(
                    field_id=3, name="name", field_type=StringType(), required=False
                ),
                NestedField(
                    field_id=4, name="standard", field_type=StringType(), required=False
                ),
                NestedField(
                    field_id=5,
                    name="decimals",
                    field_type=IntegerType(),
                    required=False,
                ),
            ),
        )
        dim = self.catalog.load_table("synthetic.token_dim")
        icp_ledger = "ryjl3-tyaaa-aaaaa-aaaba-cai"  # the real native ICP ledger canister
        unrelated_ledger = "ly36x-wiaaa-aaaai-aqj7q-cai"  # a real pool token, unrelated conflict
        dim.append(
            pa.Table.from_pylist(
                [
                    {
                        "ledger_id": icp_ledger,
                        "symbol": "ICP",
                        "name": None,
                        "standard": "ICRC2",  # existing row recorded via the ICRC-2 interface
                        "decimals": 8,
                    },
                    {
                        "ledger_id": unrelated_ledger,
                        "symbol": None,
                        "name": None,
                        "standard": "ICRC1",
                        "decimals": None,
                    },
                ],
                schema=dim.schema().as_arrow(),
            )
        )
        source = {"bucket": "synthetic", "key": "icp-precedence.csv"}

        def observation(ledger, standard):
            return {
                "ledger_id": ledger,
                "symbol": None,
                "name": None,
                "standard": standard,
                "decimals": None,
                "source": source,
            }

        # Direction 1: existing="ICRC2", new candidate="ICP" -> row is corrected to "ICP".
        result = apply_metadata(dim, [observation(icp_ledger, "ICP")])
        self.assertIn(icp_ledger, result["changed_ledgers"])
        after = self.catalog.load_table("synthetic.token_dim")
        row = next(
            r
            for r in after.scan().to_arrow().to_pylist()
            if r["ledger_id"] == icp_ledger
        )
        self.assertEqual("ICP", row["standard"])

        # Direction 2: existing="ICP" (just corrected above), new candidate="ICRC2"
        # -> stays "ICP", true no-op (not in changed_ledgers).
        result2 = apply_metadata(after, [observation(icp_ledger, "ICRC2")])
        self.assertNotIn(icp_ledger, result2["changed_ledgers"])
        still = self.catalog.load_table("synthetic.token_dim")
        row2 = next(
            r
            for r in still.scan().to_arrow().to_pylist()
            if r["ledger_id"] == icp_ledger
        )
        self.assertEqual("ICP", row2["standard"])

        # An unrelated conflict (neither side ICP): the observation is undated and the stored as_of is NULL -> a tie, the greater value
        # wins, nothing raises, and the resolved conflict is reported.
        before_snapshot = still.current_snapshot().snapshot_id
        resolved = apply_metadata(still, [observation(unrelated_ledger, "ICRC2")])
        self.assertEqual([unrelated_ledger], resolved["changed_ledgers"])
        self.assertEqual(
            {
                "ledger_id": unrelated_ledger,
                "attr": "standard",
                "values": ["ICRC1", "ICRC2"],
                "chosen": "ICRC2",
            },
            resolved["conflicts"][0],
        )
        self.assertNotEqual(
            before_snapshot,
            self.catalog.load_table("synthetic.token_dim")
            .current_snapshot()
            .snapshot_id,
        )

    def test_registered_dimension_int64_preserves_schema_identity_and_decimal_range(
        self,
    ):
        from pyiceberg.schema import Schema
        from pyiceberg.types import LongType, NestedField, StringType

        from token_dimension import apply_metadata, extract_metadata

        self.create_dim(
            schema=Schema(
                NestedField(
                    field_id=31,
                    name="ledger_id",
                    field_type=StringType(),
                    required=False,
                ),
                NestedField(
                    field_id=32, name="symbol", field_type=StringType(), required=False
                ),
                NestedField(
                    field_id=33, name="name", field_type=StringType(), required=False
                ),
                NestedField(
                    field_id=34,
                    name="standard",
                    field_type=StringType(),
                    required=False,
                ),
                NestedField(
                    field_id=35, name="decimals", field_type=LongType(), required=False
                ),
            ),
        )
        dim = self.catalog.load_table("synthetic.token_dim")
        dim.append(
            pa.Table.from_pylist(
                [
                    {
                        "ledger_id": "ledger-a",
                        "symbol": "A",
                        "name": None,
                        "standard": None,
                        "decimals": None,
                    },
                    {
                        "ledger_id": None,
                        "symbol": "NULL-KEY",
                        "name": "Keep",
                        "standard": None,
                        "decimals": None,
                    },
                ],
                schema=dim.schema().as_arrow(),
            )
        )
        identity = (dim.metadata.table_uuid, dim.location(), dim.schema())
        observations, issues = extract_metadata(
            b"token0_id,token0_symbol,token0_decimals\nledger-a,A,2147483648\n",
            "A",
            {
                "bucket": "synthetic-raw",
                "key": "int64.csv",
                "source_version": "v1",
                "content_sha256": "0" * 64,
            },
            max_records=10,
        )
        self.assertEqual(
            [{"code": "missing_ledger_id", "logical_record_ordinal": 1, "slot": 1}],
            issues,
        )
        result = apply_metadata(dim, observations)
        self.assertEqual(["ledger-a"], result["changed_ledgers"])
        fresh = self.catalog.load_table("synthetic.token_dim")
        token = next(
            row
            for row in fresh.scan().to_arrow().to_pylist()
            if row["ledger_id"] == "ledger-a"
        )
        self.assertEqual(2_147_483_648, token["decimals"])
        self.assertEqual(
            identity, (fresh.metadata.table_uuid, fresh.location(), fresh.schema())
        )
        self.assertEqual(2, len(fresh.scan().to_arrow().to_pylist()))

    def test_registered_dimension_conflict_is_reported_and_does_not_block_fact_publication(self):
        from pyiceberg.schema import Schema
        from pyiceberg.types import IntegerType, NestedField, StringType

        self.create_dim(
            schema=Schema(
                NestedField(
                    field_id=1, name="ledger_id", field_type=StringType(), required=True
                ),
                NestedField(
                    field_id=2, name="symbol", field_type=StringType(), required=False
                ),
                NestedField(
                    field_id=3, name="name", field_type=StringType(), required=False
                ),
                NestedField(
                    field_id=4, name="standard", field_type=StringType(), required=False
                ),
                NestedField(
                    field_id=5,
                    name="decimals",
                    field_type=IntegerType(),
                    required=False,
                ),
            ),
        )
        dim = self.catalog.load_table("synthetic.token_dim")
        dim.append(
            pa.Table.from_pylist(
                [
                    {
                        "ledger_id": "ledger-a",
                        "symbol": "CANON",
                        "name": None,
                        "standard": None,
                        "decimals": 6,
                    },
                ],
                schema=dim.schema().as_arrow(),
            )
        )
        before = dim.current_snapshot().snapshot_id
        key = "synthetic/conflicting-metadata.csv"
        # A differing decimals value never raises MetadataError or blocks the fact
        # publication: the dated observation
        # (stored as_of is NULL = oldest) replaces it, the FACT row is published and the
        # resolved conflict is reported in the result instead.
        header = B_HEADER + ",token0_symbol,token0_name,token0_decimals"
        row = "conflict-hash,1793363500000,Swap,pool,alice,pool,ledger-a,ledger-b,1,0,0,2,CANON,Token A,8"
        source = SyntheticS3({key: (header + "\n" + row + "\n").encode()})
        with patch.object(self.silver, "_s3", source):
            result = self.silver.handler({"mode": "backfill_file", "key": key})
        self.assertEqual(1, len(source.get_calls))
        self.assertEqual(1, result["rows_written"])
        self.assertIsNotNone(
            self.catalog.load_table("synthetic.silver").current_snapshot()
        )
        self.assertEqual(1, result["token_dim"]["conflicts_resolved"])
        self.assertEqual(
            {"ledger_id": "ledger-a", "attr": "decimals", "values": [6, 8], "chosen": 8},
            result["token_dim"]["conflicts"][0],
        )
        after = self.catalog.load_table("synthetic.token_dim")
        self.assertNotEqual(before, after.current_snapshot().snapshot_id)
        stored = after.scan(row_filter=In("ledger_id", ["ledger-a"])).to_arrow().to_pylist()[0]
        self.assertEqual(("CANON", 8), (stored["symbol"], stored["decimals"]))
        self.assertEqual([], self.external_attempts)

    def test_dimension_two_real_writers_reload_and_preserve_disjoint_null_fills(self):
        from concurrent.futures import ThreadPoolExecutor

        from pyiceberg.exceptions import CommitFailedException
        from pyiceberg.schema import Schema
        from pyiceberg.types import IntegerType, NestedField, StringType

        from token_dimension import apply_metadata

        self.create_dim(
            schema=Schema(
                NestedField(
                    field_id=1, name="ledger_id", field_type=StringType(), required=True
                ),
                NestedField(
                    field_id=2, name="symbol", field_type=StringType(), required=False
                ),
                NestedField(
                    field_id=3, name="name", field_type=StringType(), required=False
                ),
                NestedField(
                    field_id=4, name="standard", field_type=StringType(), required=False
                ),
                NestedField(
                    field_id=5,
                    name="decimals",
                    field_type=IntegerType(),
                    required=False,
                ),
            ),
        )
        seed = self.catalog.load_table("synthetic.token_dim")
        seed.append(
            pa.Table.from_pylist(
                [
                    {
                        "ledger_id": "ledger-a",
                        "symbol": "A",
                        "name": None,
                        "standard": None,
                        "decimals": None,
                    },
                    {
                        "ledger_id": "unrelated",
                        "symbol": "KEEP",
                        "name": "Keep",
                        "standard": "icrc1",
                        "decimals": 8,
                    },
                ],
                schema=seed.schema().as_arrow(),
            )
        )
        identity = (seed.metadata.table_uuid, seed.location(), seed.schema())
        first = self.catalog.load_table("synthetic.token_dim")
        second = self.catalog.load_table("synthetic.token_dim")
        self.assertEqual(
            first.current_snapshot().snapshot_id, second.current_snapshot().snapshot_id
        )
        source = {
            "bucket": "synthetic",
            "key": "concurrent.csv",
            "source_version": "v1",
            "content_sha256": "0" * 64,
        }
        observations = [
            [
                {
                    "ledger_id": "ledger-a",
                    "symbol": None,
                    "name": "Token A",
                    "standard": None,
                    "decimals": None,
                    "source": source,
                }
            ],
            [
                {
                    "ledger_id": "ledger-a",
                    "symbol": None,
                    "name": None,
                    "standard": "icrc1",
                    "decimals": None,
                    "source": source,
                }
            ],
        ]
        barrier = threading.Barrier(2)
        lock = threading.Lock()
        admissions = 0
        retries = []
        original_commit = self.catalog.commit_table

        def synchronized_commit(table, requirements, updates):
            nonlocal admissions
            if table.name() == ("synthetic", "token_dim"):
                with lock:
                    first_two = admissions < 2
                    admissions += 1
                if first_two:
                    barrier.wait(timeout=15)
                try:
                    return original_commit(table, requirements, updates)
                except CommitFailedException as exc:
                    retries.append(exc)
                    raise
            return original_commit(table, requirements, updates)

        with patch.object(self.catalog, "commit_table", synchronized_commit):
            with ThreadPoolExecutor(max_workers=2) as pool:
                futures = [
                    pool.submit(apply_metadata, table, batch)
                    for table, batch in zip((first, second), observations)
                ]
                results = [future.result(timeout=40) for future in futures]
        self.assertEqual(2, len(results))
        self.assertEqual(1, len(retries))
        self.assertEqual(3, admissions)
        final = self.catalog.load_table("synthetic.token_dim")
        rows = final.scan().to_arrow().to_pylist()
        target = next(row for row in rows if row["ledger_id"] == "ledger-a")
        self.assertEqual("Token A", target["name"])
        self.assertEqual("icrc1", target["standard"])
        self.assertEqual(
            "KEEP",
            next(row for row in rows if row["ledger_id"] == "unrelated")["symbol"],
        )
        self.assertEqual(
            identity, (final.metadata.table_uuid, final.location(), final.schema())
        )
        self.assertEqual([], self.external_attempts)

    def test_dimension_retry_budget_is_exactly_max_attempts_real_stale_admissions(self):
        from pyiceberg.exceptions import CommitFailedException
        from pyiceberg.schema import Schema
        from pyiceberg.types import IntegerType, NestedField, StringType

        from token_dimension import MetadataError, apply_metadata

        schema = Schema(
            NestedField(
                field_id=1, name="ledger_id", field_type=StringType(), required=True
            ),
            NestedField(
                field_id=2, name="symbol", field_type=StringType(), required=False
            ),
            NestedField(
                field_id=3, name="name", field_type=StringType(), required=False
            ),
            NestedField(
                field_id=4, name="standard", field_type=StringType(), required=False
            ),
            NestedField(
                field_id=5, name="decimals", field_type=IntegerType(), required=False
            ),
        )
        self.create_dim(schema=schema)
        seed = self.catalog.load_table("synthetic.token_dim")
        seed.append(
            pa.Table.from_pylist(
                [
                    {
                        "ledger_id": "ledger-a",
                        "symbol": "A",
                        "name": None,
                        "standard": None,
                        "decimals": None,
                    },
                    {
                        "ledger_id": "unrelated",
                        "symbol": "KEEP",
                        "name": "Keep",
                        "standard": "icrc1",
                        "decimals": 8,
                    },
                ],
                schema=seed.schema().as_arrow(),
            )
        )
        source = {
            "bucket": "synthetic",
            "key": "retry.csv",
            "source_version": "v1",
            "content_sha256": "0" * 64,
        }
        target = [
            {
                "ledger_id": "ledger-a",
                "symbol": None,
                "name": "Target",
                "standard": None,
                "decimals": None,
                "source": source,
            }
        ]
        original_commit = self.catalog.commit_table
        target_admissions = 0
        stale = []

        def invalidate_target(table, requirements, updates):
            nonlocal target_admissions
            if table.name() == ("synthetic", "token_dim"):
                target_admissions += 1
                competitor = target_admissions
                with patch.object(self.catalog, "commit_table", original_commit):
                    concurrent = self.catalog.load_table("synthetic.token_dim")
                    concurrent.append(
                        pa.Table.from_pylist(
                            [
                                {
                                    "ledger_id": f"competitor-{competitor}",
                                    "symbol": "C",
                                    "name": f"commit-{competitor}",
                                    "standard": None,
                                    "decimals": None,
                                },
                            ],
                            schema=concurrent.schema().as_arrow(),
                        )
                    )
                try:
                    return original_commit(table, requirements, updates)
                except CommitFailedException as exc:
                    stale.append(exc)
                    raise
            return original_commit(table, requirements, updates)

        import token_dimension

        sleeps = []
        with (
            patch.object(self.catalog, "commit_table", invalidate_target),
            patch.object(token_dimension, "_sleep", sleeps.append),
        ):
            with self.assertRaisesRegex(MetadataError, "retry budget exhausted"):
                apply_metadata(self.catalog.load_table("synthetic.token_dim"), target)
        attempts = token_dimension.DEFAULT_LIMITS["max_attempts"]
        self.assertEqual(attempts, target_admissions)
        self.assertEqual(attempts, len(stale))
        # M6: a jittered exponential backoff between attempts, none after the last one
        self.assertEqual(attempts - 1, len(sleeps))
        # equal jitter: step k waits in [c_k/2, c_k] and c_{k+1} = 2*c_k, so never shrinks
        self.assertTrue(all(b >= a for a, b in zip(sleeps, sleeps[1:])), sleeps)
        self.assertTrue(all(0 < d <= token_dimension._RETRY_MAX_SLEEP_SECONDS for d in sleeps))
        current = self.catalog.load_table("synthetic.token_dim")
        rows = current.scan().to_arrow().to_pylist()
        row = next(item for item in rows if item["ledger_id"] == "ledger-a")
        self.assertIsNone(row["name"])
        self.assertEqual(
            {f"commit-{n}" for n in range(1, attempts + 1)},
            {
                item["name"]
                for item in rows
                if item["ledger_id"].startswith("competitor-")
            },
        )
        self.assertEqual(
            "Keep",
            next(item for item in rows if item["ledger_id"] == "unrelated")["name"],
        )
        result = apply_metadata(current, target)
        self.assertEqual(["ledger-a"], result["changed_ledgers"])
        final = (
            self.catalog.load_table("synthetic.token_dim").scan().to_arrow().to_pylist()
        )
        self.assertEqual(
            "Target",
            next(item for item in final if item["ledger_id"] == "ledger-a")["name"],
        )
        self.assertEqual([], self.external_attempts)

    def test_dimension_selected_bytes_are_reserved_cumulatively_across_retries(self):
        from pyiceberg.exceptions import CommitFailedException
        from pyiceberg.schema import Schema
        from pyiceberg.types import IntegerType, NestedField, StringType

        from token_dimension import MetadataError, apply_metadata

        self.create_dim(
            schema=Schema(
                NestedField(
                    field_id=1, name="ledger_id", field_type=StringType(), required=True
                ),
                NestedField(
                    field_id=2, name="symbol", field_type=StringType(), required=False
                ),
                NestedField(
                    field_id=3, name="name", field_type=StringType(), required=False
                ),
                NestedField(
                    field_id=4, name="standard", field_type=StringType(), required=False
                ),
                NestedField(
                    field_id=5,
                    name="decimals",
                    field_type=IntegerType(),
                    required=False,
                ),
            ),
        )
        dim = self.catalog.load_table("synthetic.token_dim")
        dim.append(
            pa.Table.from_pylist(
                [
                    {
                        "ledger_id": "ledger-a",
                        "symbol": "A",
                        "name": None,
                        "standard": None,
                        "decimals": None,
                    },
                ],
                schema=dim.schema().as_arrow(),
            )
        )
        seed_files = list(
            dim.scan(row_filter=In("ledger_id", ["ledger-a"])).plan_files()
        )
        self.assertEqual(1, len(seed_files))
        max_bytes = seed_files[0].file.file_size_in_bytes
        source = {
            "bucket": "synthetic",
            "key": "cumulative.csv",
            "source_version": "v1",
            "content_sha256": "0" * 64,
        }
        observations = [
            {
                "ledger_id": "ledger-a",
                "symbol": None,
                "name": "Target",
                "standard": None,
                "decimals": None,
                "source": source,
            }
        ]
        original_commit = self.catalog.commit_table
        target_admissions = 0

        def add_real_competitor(table, requirements, updates):
            nonlocal target_admissions
            if table.name() == ("synthetic", "token_dim"):
                target_admissions += 1
                with patch.object(self.catalog, "commit_table", original_commit):
                    concurrent = self.catalog.load_table("synthetic.token_dim")
                    concurrent.append(
                        pa.Table.from_pylist(
                            [
                                {
                                    "ledger_id": "unrelated",
                                    "symbol": "KEEP",
                                    "name": "Keep",
                                    "standard": None,
                                    "decimals": None,
                                },
                            ],
                            schema=concurrent.schema().as_arrow(),
                        )
                    )
                try:
                    return original_commit(table, requirements, updates)
                except CommitFailedException:
                    raise
            return original_commit(table, requirements, updates)

        self.tracing_fs.handler.parquet_reads.clear()
        with patch.object(self.catalog, "commit_table", add_real_competitor):
            with self.assertRaisesRegex(MetadataError, "selected file/row/byte cap"):
                apply_metadata(dim, observations, limits={"max_bytes": max_bytes})
        self.assertEqual(1, target_admissions)
        self.assertEqual(1, len(self.tracing_fs.handler.parquet_reads))
        current = self.catalog.load_table("synthetic.token_dim")
        rows = current.scan().to_arrow().to_pylist()
        self.assertIsNone(
            next(row for row in rows if row["ledger_id"] == "ledger-a")["name"]
        )
        self.assertEqual(
            "Keep", next(row for row in rows if row["ledger_id"] == "unrelated")["name"]
        )
        self.assertEqual([], self.external_attempts)

    def test_dimension_limits_cannot_raise_contract_caps_or_retry_bound(self):
        from pyiceberg.schema import Schema
        from pyiceberg.types import IntegerType, NestedField, StringType

        from token_dimension import DEFAULT_LIMITS, MetadataError, apply_metadata

        self.create_dim(
            schema=Schema(
                NestedField(
                    field_id=1, name="ledger_id", field_type=StringType(), required=True
                ),
                NestedField(
                    field_id=2, name="symbol", field_type=StringType(), required=False
                ),
                NestedField(
                    field_id=3, name="name", field_type=StringType(), required=False
                ),
                NestedField(
                    field_id=4, name="standard", field_type=StringType(), required=False
                ),
                NestedField(
                    field_id=5,
                    name="decimals",
                    field_type=IntegerType(),
                    required=False,
                ),
            ),
        )
        dim = self.catalog.load_table("synthetic.token_dim")
        dim.append(
            pa.Table.from_pylist(
                [
                    {
                        "ledger_id": "ledger-a",
                        "symbol": "A",
                        "name": None,
                        "standard": None,
                        "decimals": None,
                    },
                ],
                schema=dim.schema().as_arrow(),
            )
        )
        observations = [
            {
                "ledger_id": "ledger-a",
                "symbol": None,
                "name": "Target",
                "standard": None,
                "decimals": None,
                "source": {"bucket": "synthetic", "key": "limits.csv"},
            }
        ]
        self.tracing_fs.handler.parquet_reads.clear()
        for limits in (
            {"max_attempts": DEFAULT_LIMITS["max_attempts"] + 1},
            {"max_files": DEFAULT_LIMITS["max_files"] + 1},
            {"max_staged_bytes": DEFAULT_LIMITS["max_staged_bytes"] + 1},
            {"deadline_seconds": float("inf")},
            {"deadline_seconds": float("nan")},
        ):
            with (
                self.subTest(limits=limits),
                self.assertRaisesRegex(MetadataError, "limit"),
            ):
                apply_metadata(dim, observations, limits=limits)
            self.assertEqual([], self.tracing_fs.handler.parquet_reads)
        self.assertIsNone(
            self.catalog.load_table("synthetic.token_dim")
            .scan()
            .to_arrow()
            .to_pylist()[0]["name"]
        )
        self.assertEqual([], self.external_attempts)

    def test_dimension_staged_output_bytes_are_cumulative_across_real_retry(self):
        from pyiceberg.exceptions import CommitFailedException
        from pyiceberg.schema import Schema
        from pyiceberg.types import IntegerType, NestedField, StringType

        from token_dimension import apply_metadata

        self.create_dim(
            schema=Schema(
                NestedField(
                    field_id=1, name="ledger_id", field_type=StringType(), required=True
                ),
                NestedField(
                    field_id=2, name="symbol", field_type=StringType(), required=False
                ),
                NestedField(
                    field_id=3, name="name", field_type=StringType(), required=False
                ),
                NestedField(
                    field_id=4, name="standard", field_type=StringType(), required=False
                ),
                NestedField(
                    field_id=5,
                    name="decimals",
                    field_type=IntegerType(),
                    required=False,
                ),
            ),
        )
        dim = self.catalog.load_table("synthetic.token_dim")
        dim.append(
            pa.Table.from_pylist(
                [
                    {
                        "ledger_id": ledger,
                        "symbol": "T",
                        "name": None,
                        "standard": None,
                        "decimals": None,
                    }
                    for ledger in ("ledger-a", "ledger-b", "unrelated")
                ],
                schema=dim.schema().as_arrow(),
            )
        )
        source = {
            "bucket": "synthetic",
            "key": "stage-retry.csv",
            "source_version": "v1",
            "content_sha256": "0" * 64,
        }
        baseline = apply_metadata(
            dim,
            [
                {
                    "ledger_id": "ledger-a",
                    "symbol": None,
                    "name": "A",
                    "standard": None,
                    "decimals": None,
                    "source": source,
                }
            ],
        )
        self.assertGreater(baseline["bytes_staged"], 0)
        max_staged_bytes = baseline["bytes_staged"] + baseline["bytes_staged"] // 2
        target = [
            {
                "ledger_id": "ledger-b",
                "symbol": None,
                "name": "B",
                "standard": None,
                "decimals": None,
                "source": source,
            }
        ]
        original_commit = self.catalog.commit_table
        target_admissions = 0
        stale = []

        def append_competitor_then_commit(table, requirements, updates):
            nonlocal target_admissions
            if table.name() == ("synthetic", "token_dim"):
                target_admissions += 1
                if target_admissions == 1:
                    with patch.object(self.catalog, "commit_table", original_commit):
                        concurrent = self.catalog.load_table("synthetic.token_dim")
                        concurrent.append(
                            pa.Table.from_pylist(
                                [
                                    {
                                        "ledger_id": "competitor",
                                        "symbol": "C",
                                        "name": "Keep",
                                        "standard": None,
                                        "decimals": None,
                                    },
                                ],
                                schema=concurrent.schema().as_arrow(),
                            )
                        )
                    try:
                        return original_commit(table, requirements, updates)
                    except CommitFailedException as exc:
                        stale.append(exc)
                        raise
            return original_commit(table, requirements, updates)

        self.tracing_fs.handler.parquet_writes.clear()
        with patch.object(self.catalog, "commit_table", append_competitor_then_commit):
            with self.assertRaisesRegex(Exception, "staged byte cap"):
                apply_metadata(
                    dim, target, limits={"max_staged_bytes": max_staged_bytes}
                )
        self.assertEqual(1, target_admissions)
        self.assertEqual(1, len(stale))
        self.assertGreaterEqual(len(self.tracing_fs.handler.parquet_writes), 3)
        rows = (
            self.catalog.load_table("synthetic.token_dim").scan().to_arrow().to_pylist()
        )
        self.assertIsNone(
            next(row for row in rows if row["ledger_id"] == "ledger-b")["name"]
        )
        self.assertEqual(
            "Keep",
            next(row for row in rows if row["ledger_id"] == "competitor")["name"],
        )
        success = apply_metadata(self.catalog.load_table("synthetic.token_dim"), target)
        self.assertEqual(["ledger-b"], success["changed_ledgers"])
        self.assertEqual(
            "B",
            next(
                row
                for row in self.catalog.load_table("synthetic.token_dim")
                .scan()
                .to_arrow()
                .to_pylist()
                if row["ledger_id"] == "ledger-b"
            )["name"],
        )
        self.assertEqual([], self.external_attempts)

    def test_dimension_deadline_after_real_staging_blocks_catalog_admission_then_retry_succeeds(
        self,
    ):
        from pyiceberg.schema import Schema
        from pyiceberg.types import IntegerType, NestedField, StringType

        import token_dimension

        schema = Schema(
            NestedField(
                field_id=1, name="ledger_id", field_type=StringType(), required=True
            ),
            NestedField(
                field_id=2, name="symbol", field_type=StringType(), required=False
            ),
            NestedField(
                field_id=3, name="name", field_type=StringType(), required=False
            ),
            NestedField(
                field_id=4, name="standard", field_type=StringType(), required=False
            ),
            NestedField(
                field_id=5, name="decimals", field_type=IntegerType(), required=False
            ),
        )
        self.create_dim(schema=schema)
        dim = self.catalog.load_table("synthetic.token_dim")
        dim.append(
            pa.Table.from_pylist(
                [
                    {
                        "ledger_id": "ledger-a",
                        "symbol": "A",
                        "name": None,
                        "standard": None,
                        "decimals": None,
                    },
                    {
                        "ledger_id": "unrelated",
                        "symbol": "KEEP",
                        "name": "Keep",
                        "standard": "icrc1",
                        "decimals": 8,
                    },
                ],
                schema=dim.schema().as_arrow(),
            )
        )
        source = {
            "bucket": "synthetic",
            "key": "deadline.csv",
            "source_version": "v1",
            "content_sha256": "0" * 64,
        }
        observations = [
            {
                "ledger_id": "ledger-a",
                "symbol": None,
                "name": "Token A",
                "standard": None,
                "decimals": None,
                "source": source,
            }
        ]
        before = dim.current_snapshot().snapshot_id
        self.tracing_fs.handler.parquet_writes.clear()
        original_overwrite = Transaction.overwrite
        original_commit = self.catalog.commit_table
        clock = [0.0]
        admissions = []

        def stage_then_expire(tx, *args, **kwargs):
            result = original_overwrite(tx, *args, **kwargs)
            clock[0] = 100.0
            return result

        def track_dimension_admission(table, requirements, updates):
            if table.name() == ("synthetic", "token_dim"):
                admissions.append(table.name())
            return original_commit(table, requirements, updates)

        with (
            patch.object(token_dimension.time, "monotonic", lambda: clock[0]),
            patch.object(Transaction, "overwrite", stage_then_expire),
            patch.object(self.catalog, "commit_table", track_dimension_admission),
            self.assertRaisesRegex(
                token_dimension.MetadataError, "deadline exceeded before commit"
            ),
        ):
            token_dimension.apply_metadata(dim, observations, deadline=999.0)
        self.assertGreaterEqual(len(self.tracing_fs.handler.parquet_writes), 1)
        self.assertEqual([], admissions)
        unchanged = self.catalog.load_table("synthetic.token_dim")
        self.assertEqual(before, unchanged.current_snapshot().snapshot_id)
        unchanged_rows = unchanged.scan().to_arrow().to_pylist()
        self.assertIsNone(
            next(row for row in unchanged_rows if row["ledger_id"] == "ledger-a")[
                "name"
            ]
        )
        self.assertEqual(
            "Keep",
            next(row for row in unchanged_rows if row["ledger_id"] == "unrelated")[
                "name"
            ],
        )
        succeeded = token_dimension.apply_metadata(unchanged, observations)
        self.assertEqual(["ledger-a"], succeeded["changed_ledgers"])
        final = (
            self.catalog.load_table("synthetic.token_dim").scan().to_arrow().to_pylist()
        )
        self.assertEqual(
            "Token A",
            next(row for row in final if row["ledger_id"] == "ledger-a")["name"],
        )
        self.assertEqual(
            "Keep",
            next(row for row in final if row["ledger_id"] == "unrelated")["name"],
        )
        self.assertEqual([], self.external_attempts)

    def test_dimension_incompatible_schema_and_blank_keys_fail_before_staging(self):
        from pyiceberg.schema import Schema
        from pyiceberg.types import DoubleType, NestedField, StringType

        from token_dimension import MetadataError, apply_metadata

        source = {
            "bucket": "synthetic",
            "key": "bad-dimension.csv",
            "source_version": "v1",
            "content_sha256": "0" * 64,
        }
        observation = [
            {
                "ledger_id": "ledger-a",
                "symbol": None,
                "name": "Target",
                "standard": None,
                "decimals": None,
                "source": source,
            }
        ]
        self.create_dim(
            schema=Schema(
                NestedField(
                    field_id=1, name="ledger_id", field_type=StringType(), required=True
                ),
                NestedField(
                    field_id=2, name="symbol", field_type=StringType(), required=False
                ),
                NestedField(
                    field_id=3, name="name", field_type=StringType(), required=False
                ),
                NestedField(
                    field_id=4, name="standard", field_type=StringType(), required=False
                ),
                NestedField(
                    field_id=5, name="decimals", field_type=DoubleType(), required=False
                ),
            ),
        )
        bad_schema = self.catalog.load_table("synthetic.token_dim")
        self.tracing_fs.handler.parquet_reads.clear()
        self.tracing_fs.handler.parquet_writes.clear()
        with self.assertRaisesRegex(
            MetadataError, "incompatible token_dim field type: decimals"
        ):
            apply_metadata(bad_schema, observation)
        self.assertEqual([], self.tracing_fs.handler.parquet_reads)
        self.assertEqual([], self.tracing_fs.handler.parquet_writes)
        self.assertIsNone(
            self.catalog.load_table("synthetic.token_dim").current_snapshot()
        )
        for bad_id in (None, " "):
            self.tracing_fs.handler.parquet_reads.clear()
            with self.assertRaisesRegex(MetadataError, "nonblank"):
                apply_metadata(bad_schema, [dict(observation[0], ledger_id=bad_id)])
            self.assertEqual([], self.tracing_fs.handler.parquet_reads)

        self.assertEqual([], self.external_attempts)

    def test_duplicate_affected_dimension_key_fails_before_staging(self):
        from pyiceberg.schema import Schema
        from pyiceberg.types import IntegerType, NestedField, StringType

        from token_dimension import MetadataError, apply_metadata

        self.create_dim(
            schema=Schema(
                NestedField(
                    field_id=1,
                    name="ledger_id",
                    field_type=StringType(),
                    required=False,
                ),
                NestedField(
                    field_id=2, name="symbol", field_type=StringType(), required=False
                ),
                NestedField(
                    field_id=3, name="name", field_type=StringType(), required=False
                ),
                NestedField(
                    field_id=4, name="standard", field_type=StringType(), required=False
                ),
                NestedField(
                    field_id=5,
                    name="decimals",
                    field_type=IntegerType(),
                    required=False,
                ),
            ),
        )
        dim = self.catalog.load_table("synthetic.token_dim")
        duplicate = {
            "ledger_id": "ledger-a",
            "symbol": "A",
            "name": None,
            "standard": None,
            "decimals": None,
        }
        dim.append(
            pa.Table.from_pylist([duplicate, duplicate], schema=dim.schema().as_arrow())
        )
        before = dim.current_snapshot().snapshot_id
        observations = [
            {
                "ledger_id": "ledger-a",
                "symbol": None,
                "name": "Target",
                "standard": None,
                "decimals": None,
                "source": {"bucket": "synthetic", "key": "dup.csv"},
            }
        ]
        self.tracing_fs.handler.parquet_reads.clear()
        self.tracing_fs.handler.parquet_writes.clear()
        with self.assertRaisesRegex(MetadataError, "duplicate affected token_dim key"):
            apply_metadata(dim, observations)
        self.assertGreaterEqual(len(self.tracing_fs.handler.parquet_reads), 1)
        self.assertEqual([], self.tracing_fs.handler.parquet_writes)
        self.assertEqual(
            before,
            self.catalog.load_table("synthetic.token_dim")
            .current_snapshot()
            .snapshot_id,
        )
        self.assertEqual([], self.external_attempts)

    def test_dimension_rejects_nonintegral_or_wrongly_typed_values_before_parquet_io(
        self,
    ):
        from pyiceberg.schema import Schema
        from pyiceberg.types import IntegerType, NestedField, StringType

        from token_dimension import MetadataError, apply_metadata

        self.create_dim(
            schema=Schema(
                NestedField(
                    field_id=1, name="ledger_id", field_type=StringType(), required=True
                ),
                NestedField(
                    field_id=2, name="symbol", field_type=StringType(), required=False
                ),
                NestedField(
                    field_id=3, name="name", field_type=StringType(), required=False
                ),
                NestedField(
                    field_id=4, name="standard", field_type=StringType(), required=False
                ),
                NestedField(
                    field_id=5,
                    name="decimals",
                    field_type=IntegerType(),
                    required=False,
                ),
            ),
        )
        dim = self.catalog.load_table("synthetic.token_dim")
        dim.append(
            pa.Table.from_pylist(
                [
                    {
                        "ledger_id": "ledger-a",
                        "symbol": None,
                        "name": None,
                        "standard": None,
                        "decimals": None,
                    },
                ],
                schema=dim.schema().as_arrow(),
            )
        )
        source = {"bucket": "synthetic", "key": "bad-value.csv"}
        base = {
            "ledger_id": "ledger-a",
            "symbol": None,
            "name": None,
            "standard": None,
            "decimals": None,
            "source": source,
        }
        self.tracing_fs.handler.parquet_reads.clear()
        for attr, value in (
            ("decimals", 1.5),
            ("decimals", True),
            ("decimals", -1),
            ("symbol", 42),
        ):
            with (
                self.subTest(attr=attr, value=value),
                self.assertRaisesRegex(MetadataError, "invalid supplied"),
            ):
                apply_metadata(dim, [dict(base, **{attr: value})])
            self.assertEqual([], self.tracing_fs.handler.parquet_reads)
        self.assertEqual([], self.external_attempts)

    def test_dimension_output_row_cap_has_exact_success_boundary_and_shared_delete_predicate(
        self,
    ):
        from pyiceberg.schema import Schema
        from pyiceberg.types import IntegerType, NestedField, StringType

        from token_dimension import MetadataError, apply_metadata

        schema = Schema(
            NestedField(
                field_id=1, name="ledger_id", field_type=StringType(), required=False
            ),
            NestedField(
                field_id=2, name="symbol", field_type=StringType(), required=False
            ),
            NestedField(
                field_id=3, name="name", field_type=StringType(), required=False
            ),
            NestedField(
                field_id=4, name="standard", field_type=StringType(), required=False
            ),
            NestedField(
                field_id=5, name="decimals", field_type=IntegerType(), required=False
            ),
        )
        self.create_dim(schema=schema)
        dim = self.catalog.load_table("synthetic.token_dim")
        dim.append(
            pa.Table.from_pylist(
                [
                    {
                        "ledger_id": ledger,
                        "symbol": None,
                        "name": None,
                        "standard": None,
                        "decimals": None,
                    }
                    for ledger in ("ledger-a", "ledger-b", "unrelated")
                ],
                schema=dim.schema().as_arrow(),
            )
        )
        source = {
            "bucket": "synthetic",
            "key": "output-cap.csv",
            "source_version": "v1",
            "content_sha256": "0" * 64,
        }
        obs_a = [
            {
                "ledger_id": "ledger-a",
                "symbol": None,
                "name": "A",
                "standard": None,
                "decimals": None,
                "source": source,
            }
        ]
        obs_b = [
            {
                "ledger_id": "ledger-b",
                "symbol": None,
                "name": "B",
                "standard": None,
                "decimals": None,
                "source": source,
            }
        ]
        planned, deleted = [], []
        original_scan, original_overwrite = dim.scan, Transaction.overwrite

        def trace_scan(*args, **kwargs):
            planned.append(kwargs["row_filter"])
            return original_scan(*args, **kwargs)

        def trace_overwrite(tx, *args, **kwargs):
            deleted.append(kwargs["overwrite_filter"])
            return original_overwrite(tx, *args, **kwargs)

        with (
            patch.object(dim, "scan", trace_scan),
            patch.object(Transaction, "overwrite", trace_overwrite),
        ):
            success = apply_metadata(dim, obs_a, limits={"max_output_rows": 1})
        self.assertEqual(["ledger-a"], success["changed_ledgers"])
        self.assertIs(planned[-1], deleted[-1])
        self.assertEqual(
            "A",
            next(
                row
                for row in self.catalog.load_table("synthetic.token_dim")
                .scan()
                .to_arrow()
                .to_pylist()
                if row["ledger_id"] == "ledger-a"
            )["name"],
        )
        before_b = (
            self.catalog.load_table("synthetic.token_dim")
            .current_snapshot()
            .snapshot_id
        )
        self.tracing_fs.handler.parquet_writes.clear()
        with self.assertRaisesRegex(MetadataError, "output row cap"):
            apply_metadata(
                self.catalog.load_table("synthetic.token_dim"),
                obs_b,
                limits={"max_output_rows": 0},
            )
        self.assertEqual([], self.tracing_fs.handler.parquet_writes)
        self.assertEqual(
            before_b,
            self.catalog.load_table("synthetic.token_dim")
            .current_snapshot()
            .snapshot_id,
        )
        self.assertIsNone(
            next(
                row
                for row in self.catalog.load_table("synthetic.token_dim")
                .scan()
                .to_arrow()
                .to_pylist()
                if row["ledger_id"] == "ledger-b"
            )["name"]
        )
        self.assertEqual([], self.external_attempts)

    def test_dimension_uuid_replacement_during_retry_fails_closed(self):
        from pyiceberg.schema import Schema
        from pyiceberg.types import IntegerType, NestedField, StringType

        from token_dimension import MetadataError, apply_metadata

        schema = Schema(
            NestedField(
                field_id=1, name="ledger_id", field_type=StringType(), required=True
            ),
            NestedField(
                field_id=2, name="symbol", field_type=StringType(), required=False
            ),
            NestedField(
                field_id=3, name="name", field_type=StringType(), required=False
            ),
            NestedField(
                field_id=4, name="standard", field_type=StringType(), required=False
            ),
            NestedField(
                field_id=5, name="decimals", field_type=IntegerType(), required=False
            ),
        )
        self.create_dim(schema=schema)
        original = self.catalog.load_table("synthetic.token_dim")
        original.append(
            pa.Table.from_pylist(
                [
                    {
                        "ledger_id": "ledger-a",
                        "symbol": "A",
                        "name": None,
                        "standard": None,
                        "decimals": None,
                    }
                ],
                schema=original.schema().as_arrow(),
            )
        )
        original_uuid = original.metadata.table_uuid
        source = {
            "bucket": "synthetic",
            "key": "replace.csv",
            "source_version": "v1",
            "content_sha256": "0" * 64,
        }
        observations = [
            {
                "ledger_id": "ledger-a",
                "symbol": None,
                "name": "Target",
                "standard": None,
                "decimals": None,
                "source": source,
            }
        ]
        original_commit = self.catalog.commit_table
        replaced = []

        def replace_before_admission(table, requirements, updates):
            if table.name() == ("synthetic", "token_dim") and not replaced:
                replaced.append(True)
                with patch.object(self.catalog, "commit_table", original_commit):
                    self.catalog.rename_table(
                        "synthetic.token_dim", "synthetic.token_dim_previous"
                    )
                    replacement = self.create_dim(
                        schema=schema,
                        location=(self.allowed_root / "replacement").as_uri(),
                    )
                    replacement.append(
                        pa.Table.from_pylist(
                            [
                                {
                                    "ledger_id": "ledger-a",
                                    "symbol": "A",
                                    "name": None,
                                    "standard": None,
                                    "decimals": None,
                                }
                            ],
                            schema=replacement.schema().as_arrow(),
                        )
                    )
            return original_commit(table, requirements, updates)

        with patch.object(self.catalog, "commit_table", replace_before_admission):
            with self.assertRaisesRegex(MetadataError, "UUID changed"):
                apply_metadata(original, observations)
        active = self.catalog.load_table("synthetic.token_dim")
        active_row = active.scan().to_arrow().to_pylist()[0]
        self.assertNotEqual(original_uuid, active.metadata.table_uuid)
        self.assertIsNone(active_row["name"])
        previous = self.catalog.load_table("synthetic.token_dim_previous")
        self.assertIsNone(previous.scan().to_arrow().to_pylist()[0]["name"])
        self.assertEqual([], self.external_attempts)

    def test_registered_dimension_acknowledgement_loss_replays_persisted_commit(self):
        from pyiceberg.schema import Schema
        from pyiceberg.types import IntegerType, NestedField, StringType

        from token_dimension import ATTRIBUTES

        self.create_dim(
            schema=Schema(
                NestedField(1, "ledger_id", StringType(), required=True),
                NestedField(2, "symbol", StringType(), required=False),
                NestedField(3, "name", StringType(), required=False),
                NestedField(4, "standard", StringType(), required=False),
                NestedField(5, "decimals", IntegerType(), required=False),
            ),
        )
        dim = self.catalog.load_table("synthetic.token_dim")
        dim.append(
            pa.Table.from_pylist(
                [
                    {"ledger_id": ledger, **{attr: None for attr in ATTRIBUTES}}
                    for ledger in ("ledger-a", "ledger-b")
                ],
                schema=dim.schema().as_arrow(),
            )
        )
        key = "synthetic/dim-ack-loss.csv"
        source = (
            b"tx_hash,tx_time,action_type,pool_id,from_principal_id,to_principal_id,"
            b"token0_ledger_id,token0_symbol,token0_name,token1_ledger_id,token1_symbol,"
            b"token1_name,token0_amount_in,token1_amount_in,token0_amount_out,token1_amount_out\n"
            b"ack-hash,1793363500000,Swap,pool,alice,pool,ledger-a,A,Token A,ledger-b,B,Token B,1,0,0,2\n"
        )
        original_commit = self.catalog.commit_table
        lost_dim_ack = []

        def commit_then_lose_dimension_ack(table, requirements, updates):
            result = original_commit(table, requirements, updates)
            if table.name() == ("synthetic", "token_dim") and not lost_dim_ack:
                lost_dim_ack.append(True)
                raise RuntimeError("injected dimension acknowledgement loss")
            return result

        with patch.object(self.silver, "_s3", SyntheticS3({key: source})):
            with patch.object(
                self.catalog, "commit_table", commit_then_lose_dimension_ack
            ):
                with self.assertRaisesRegex(RuntimeError, "acknowledgement loss"):
                    self.silver.handler({"mode": "backfill_file", "key": key})
        self.assertEqual([True], lost_dim_ack)
        persisted_dim = self.catalog.load_table("synthetic.token_dim")
        rows = persisted_dim.scan().to_arrow().to_pylist()
        self.assertEqual(
            "Token A", next(r for r in rows if r["ledger_id"] == "ledger-a")["name"]
        )
        # M5: facts are committed BEFORE the dimension, so they are already durable when the
        # (non-MetadataError) dimension acknowledgement loss fails the invocation.
        self.assertEqual(
            1,
            len(self.catalog.load_table("synthetic.silver").scan().to_arrow().to_pylist()),
        )
        durable_snapshot = persisted_dim.current_snapshot().snapshot_id
        with patch.object(self.silver, "_s3", SyntheticS3({key: source})):
            result = self.silver.handler({"mode": "backfill_file", "key": key})
        self.assertEqual(0, result["rows_written"])
        self.assertTrue(result["replayed"])
        self.assertEqual(
            durable_snapshot,
            self.catalog.load_table("synthetic.token_dim")
            .current_snapshot()
            .snapshot_id,
        )
        self.assertEqual(
            1,
            len(
                self.catalog.load_table("synthetic.silver")
                .scan()
                .to_arrow()
                .to_pylist()
            ),
        )

    def test_new_candidate_ledger_creates_token_dim_row_and_fact_still_publishes(self):
        """REG-04: a brand-new ledger_id in metadata must not block fact publication."""
        from pyiceberg.schema import Schema
        from pyiceberg.types import IntegerType, NestedField, StringType

        self.create_dim(
            schema=Schema(
                NestedField(
                    field_id=1, name="ledger_id", field_type=StringType(), required=True
                ),
                NestedField(
                    field_id=2, name="symbol", field_type=StringType(), required=False
                ),
                NestedField(
                    field_id=3, name="name", field_type=StringType(), required=False
                ),
                NestedField(
                    field_id=4, name="standard", field_type=StringType(), required=False
                ),
                NestedField(
                    field_id=5,
                    name="decimals",
                    field_type=IntegerType(),
                    required=False,
                ),
            ),
        )
        dim = self.catalog.load_table("synthetic.token_dim")
        dim.append(
            pa.Table.from_pylist(
                [
                    {
                        "ledger_id": "ledger-a",
                        "symbol": "A",
                        "name": None,
                        "standard": None,
                        "decimals": None,
                    }
                ],
                schema=dim.schema().as_arrow(),
            )
        )
        key = "synthetic/new-listed-token.csv"
        b_header = (
            "tx_hash,tx_time,action_type,pool_id,from_principal_id,to_principal_id,"
            "token0_ledger_id,token0_symbol,token1_ledger_id,token1_symbol,token1_name,"
            "token0_amount_in,token1_amount_in,token0_amount_out,token1_amount_out"
        )
        content = (
            b_header + "\n"
            "hash-new,1793363500000,Swap,pool,alice,pool,ledger-a,A,ledger-new,NEW,"
            "New Token,1,0,0,2\n"
        ).encode()
        s3 = SyntheticS3({key: content})
        with patch.object(self.silver, "_s3", s3):
            result = self.silver._process_key(key)
        self.assertEqual(1, result["rows_written"])
        self.assertEqual(["ledger-new"], result["token_dim"]["created_ledgers"])
        rows = self.catalog.load_table("synthetic.silver").scan().to_arrow().to_pylist()
        self.assertEqual(1, len(rows))
        dim_after = (
            self.catalog.load_table("synthetic.token_dim").scan().to_arrow().to_pylist()
        )
        created = next(row for row in dim_after if row["ledger_id"] == "ledger-new")
        self.assertEqual("NEW", created["symbol"])
        self.assertEqual("New Token", created["name"])
        self.assertIsNone(created["standard"])
        self.assertIsNone(created["decimals"])
        unaffected = next(row for row in dim_after if row["ledger_id"] == "ledger-a")
        self.assertEqual("A", unaffected["symbol"])
        self.assertEqual([], self.external_attempts)

    def test_registered_metadata_phase_deadline_is_shared_not_reset_per_call(self):
        """REG-03: apply_metadata + readback share one phase deadline, not a fresh one each."""
        from pyiceberg.schema import Schema
        from pyiceberg.types import IntegerType, NestedField, StringType

        import token_dimension

        self.create_dim(
            schema=Schema(
                NestedField(
                    field_id=1, name="ledger_id", field_type=StringType(), required=True
                ),
                NestedField(
                    field_id=2, name="symbol", field_type=StringType(), required=False
                ),
                NestedField(
                    field_id=3, name="name", field_type=StringType(), required=False
                ),
                NestedField(
                    field_id=4, name="standard", field_type=StringType(), required=False
                ),
                NestedField(
                    field_id=5,
                    name="decimals",
                    field_type=IntegerType(),
                    required=False,
                ),
            ),
        )
        dim = self.catalog.load_table("synthetic.token_dim")
        dim.append(
            pa.Table.from_pylist(
                [
                    {
                        "ledger_id": "ledger-a",
                        "symbol": None,
                        "name": None,
                        "standard": None,
                        "decimals": None,
                    },
                    {
                        "ledger_id": "ledger-b",
                        "symbol": None,
                        "name": None,
                        "standard": None,
                        "decimals": None,
                    },
                ],
                schema=dim.schema().as_arrow(),
            )
        )
        key = "synthetic/deadline-wiring.csv"
        b_header = (
            "tx_hash,tx_time,action_type,pool_id,from_principal_id,to_principal_id,"
            "token0_ledger_id,token0_symbol,token0_name,token1_ledger_id,token1_symbol,"
            "token1_name,token0_amount_in,token1_amount_in,token0_amount_out,"
            "token1_amount_out"
        )
        content = (
            b_header + "\n"
            "hb,1793363500000,Swap,pool,alice,pool,ledger-b,B,Token B,ledger-a,A,"
            "Token A,1,0,0,2\n"
        ).encode()
        s3 = SyntheticS3({key: content})
        clock = [0.0]
        original_apply_metadata = token_dimension.apply_metadata

        def jump_clock_after_apply(*args, **kwargs):
            # Simulate real elapsed time between the dimension commit and the readback: a
            # real "fresh budget per call" bug would mask this by starting a brand-new
            # internal 45s window instead of inheriting the already-spent phase deadline.
            result = original_apply_metadata(*args, **kwargs)
            clock[0] = 1000.0
            return result

        with (
            patch.object(token_dimension.time, "monotonic", lambda: clock[0]),
            patch.object(token_dimension, "apply_metadata", jump_clock_after_apply),
            patch.object(self.silver, "_s3", s3),
        ):
            result = self.silver.handler({"mode": "backfill_file", "key": key})
        # M5: facts are committed first and never depend on token_dim; the readback still
        # inherits the already-spent phase deadline, which is reported, not raised.
        self.assertRegex(result["token_dim"]["error"], "deadline exceeded")
        self.assertEqual(1, result["rows_written"])
        self.assertEqual([], self.external_attempts)

    def test_public_token_metadata_lookup_rejects_oversize_encoded_response(self):
        """REG-02: the public lookup bounds the ENCODED response, not just internal reads."""
        from pyiceberg.schema import Schema
        from pyiceberg.types import IntegerType, NestedField, StringType

        self.create_dim(
            schema=Schema(
                NestedField(
                    field_id=1, name="ledger_id", field_type=StringType(), required=True
                ),
                NestedField(
                    field_id=2, name="symbol", field_type=StringType(), required=False
                ),
                NestedField(
                    field_id=3, name="name", field_type=StringType(), required=False
                ),
                NestedField(
                    field_id=4, name="standard", field_type=StringType(), required=False
                ),
                NestedField(
                    field_id=5,
                    name="decimals",
                    field_type=IntegerType(),
                    required=False,
                ),
            ),
        )
        dim = self.catalog.load_table("synthetic.token_dim")
        long_name = "x" * 2000
        dim.append(
            pa.Table.from_pylist(
                [
                    {
                        "ledger_id": f"ledger-{i}",
                        "symbol": "T",
                        "name": long_name,
                        "standard": "icrc1",
                        "decimals": 8,
                    }
                    for i in range(20)
                ],
                schema=dim.schema().as_arrow(),
            )
        )
        ledger_ids = [f"ledger-{i}" for i in range(20)]
        # The physical read is one small Parquet file (well inside the internal 32 MiB
        # readback budget), but the encoded JSON for 20 long names is not tiny: this must
        # fail closed on the PUBLIC response bound alone, independent of internal caps.
        with (
            patch.dict(os.environ, {"TOKEN_METADATA_MAX_RESPONSE_BYTES": "4096"}),
            self.assertRaisesRegex(Exception, "response"),
        ):
            self.silver.handler({"mode": "token_metadata", "ledger_ids": ledger_ids})
        with patch.dict(os.environ, {"TOKEN_METADATA_MAX_RESPONSE_BYTES": "1048576"}):
            result = self.silver.handler(
                {"mode": "token_metadata", "ledger_ids": ledger_ids}
            )
        self.assertEqual(20, len(result["tokens"]))
        self.assertEqual([], self.external_attempts)

    # --- seam reconciliation wiring, token_dim creation ---

    def test_seam_overlap_b_side_object_is_reconciled_and_nonseam_object_untouched(self):
        """seam_reconciliation must actually be wired into the
        backfill_file dispatch. Uses the real sanitized A/B seam fixtures from
        test_seam_reconciliation.py: 10 of B_CSV's 12 rows duplicate real A_CSV rows
        inside the measured overlap window and must be dropped before ingestion; a
        plain non-seam object must be completely untouched by this logic."""
        test_dir = str(Path(__file__).resolve().parent)
        if test_dir not in sys.path:
            sys.path.insert(0, test_dir)
        from test_seam_reconciliation import A_CSV, B_CSV, WINDOW_END, WINDOW_START

        self.assertEqual(WINDOW_START, self.silver.SEAM_WINDOW_START)
        self.assertEqual(WINDOW_END, self.silver.SEAM_WINDOW_END)
        non_seam_key = "synthetic/non-seam.csv"
        non_seam_row = (
            "non-seam-hash,1793363500000,Swap,pool,alice,pool,ledger-a,ledger-b,1,0,0,2"
        )
        s3 = SyntheticS3(
            {
                self.silver.SEAM_A_KEY: A_CSV,
                self.silver.SEAM_B_KEY: B_CSV,
                non_seam_key: (B_HEADER + "\n" + non_seam_row + "\n").encode(),
            }
        )
        with patch.object(self.silver, "_s3", s3):
            seam_result = self.silver.handler(
                {"mode": "backfill_file", "key": self.silver.SEAM_B_KEY}
            )
            non_seam_result = self.silver.handler(
                {"mode": "backfill_file", "key": non_seam_key}
            )
            rows = (
                self.catalog.load_table("synthetic.silver")
                .scan()
                .to_arrow()
                .to_pylist()
            )
        # 10 real duplicate rows dropped, 2 real survivors (unique-in-window +
        # out-of-window) kept and actually written as facts.
        self.assertIn("seam_reconciliation", seam_result)
        self.assertEqual(10, seam_result["seam_reconciliation"]["dropped_duplicate_rows"])
        self.assertEqual(12, seam_result["seam_reconciliation"]["dataset_b_total_rows"])
        self.assertEqual(2, seam_result["rows_written"])
        # The non-seam object must never see the reconciliation key at all.
        self.assertNotIn("seam_reconciliation", non_seam_result)
        self.assertEqual(1, non_seam_result["rows_written"])
        self.assertEqual(3, len(rows))

    def test_seam_key_is_the_single_exact_known_pair_not_a_pattern(self):
        """Guard against an over-broad seam match: an object that merely resembles
        the seam filename but isn't the exact known key must be ingested normally,
        with no attempt to read a (nonexistent) Dataset-A companion object."""
        lookalike_key = (
            "icp-swap-elt/rest/incremental/2099-01-01T00-00-00-000Z/"
            "transactions_backfill_1782215383463_p001001.csv"
        )
        row = "lookalike-hash,1793363500000,Swap,pool,alice,pool,ledger-a,ledger-b,1,0,0,2"
        s3 = SyntheticS3({lookalike_key: (B_HEADER + "\n" + row + "\n").encode()})
        with patch.object(self.silver, "_s3", s3):
            result = self.silver.handler({"mode": "backfill_file", "key": lookalike_key})
        self.assertNotIn("seam_reconciliation", result)
        self.assertEqual(1, result["rows_written"])
        # Only the one requested object was ever read — no companion A-side fetch.
        self.assertEqual([("synthetic-raw", lookalike_key)], s3.get_calls)

    _DIM_METADATA_HEADER = (
        "tx_hash,tx_time,action_type,pool_id,from_principal_id,to_principal_id,"
        "token0_ledger_id,token0_symbol,token0_name,token1_ledger_id,token1_symbol,"
        "token1_name,token0_amount_in,token1_amount_in,token0_amount_out,token1_amount_out"
    )

    def test_handler_response_for_backfill_file_is_slim_but_keeps_verification_facts(self):
        """The Lambda response (what Step Functions collects per object) carries
        counts instead of the per-ledger lists, plus the lookup/verify diagnostics."""
        key = "synthetic/dim-slim.csv"
        row = (
            "hash-dim,1793363500000,Swap,pool,alice,pool,ledger-new,NEW,New Token,"
            "ledger-two,TWO,Token Two,1,0,0,2"
        )
        content = (self._DIM_METADATA_HEADER + "\n" + row + "\n").encode()
        with patch.object(self.silver, "_s3", SyntheticS3({key: content})):
            result = self.silver.handler({"mode": "backfill_file", "key": key})
        self.assertEqual(1, result["rows_written"])
        self.assertFalse(result["replayed"])
        self.assertEqual(1, result["verify"]["ids"])
        self.assertEqual(0, result["lookup"]["existing_found"])
        dim = result["token_dim"]
        self.assertEqual(2, dim["changed_ledgers"])
        self.assertEqual(2, dim["created_ledgers"])
        self.assertEqual(0, dim["conflicts_resolved"])
        self.assertNotIn("readback", dim)
        json.dumps(result)  # JSON-serialisable Lambda response

    def test_token_dim_is_created_on_demand_when_table_absent(self):
        """A fresh deployment has no token_dim table anywhere (Terraform does not create
        it) — the first backfill_file invoke that observes
        token metadata must fail-open into creating it (mirroring prepare_target
        for the fact table), not fail-closed with MetadataError."""
        with self.assertRaises(Exception):
            self.catalog.load_table("synthetic.token_dim")
        key = "synthetic/dim-create.csv"
        row = (
            "hash-dim,1793363500000,Swap,pool,alice,pool,ledger-new,NEW,New Token,"
            "ledger-two,TWO,Token Two,1,0,0,2"
        )
        content = (self._DIM_METADATA_HEADER + "\n" + row + "\n").encode()
        s3 = SyntheticS3({key: content})
        with patch.object(self.silver, "_s3", s3):
            result = self.silver._process_key(key)
        from token_dimension import _validate_schema

        dim = self.catalog.load_table("synthetic.token_dim")
        _validate_schema(dim)  # exact {ledger_id,symbol,name,standard,decimals} contract
        rows = {r["ledger_id"]: r for r in dim.scan().to_arrow().to_pylist()}
        self.assertEqual("NEW", rows["ledger-new"]["symbol"])
        self.assertEqual("TWO", rows["ledger-two"]["symbol"])
        self.assertIn("ledger-new", result["token_dim"]["changed_ledgers"])
        self.assertEqual([], self.external_attempts)

    def test_token_dim_is_left_alone_when_table_already_exists(self):
        """An existing token_dim table must never be rebuilt or
        truncated by the on-demand creation path — only loaded and filled."""
        from pyiceberg.schema import Schema
        from pyiceberg.types import IntegerType, NestedField, StringType

        self.create_dim(
            schema=Schema(
                NestedField(
                    field_id=1, name="ledger_id", field_type=StringType(), required=True
                ),
                NestedField(
                    field_id=2, name="symbol", field_type=StringType(), required=False
                ),
                NestedField(
                    field_id=3, name="name", field_type=StringType(), required=False
                ),
                NestedField(
                    field_id=4, name="standard", field_type=StringType(), required=False
                ),
                NestedField(
                    field_id=5,
                    name="decimals",
                    field_type=IntegerType(),
                    required=False,
                ),
            ),
        )
        dim = self.catalog.load_table("synthetic.token_dim")
        dim.append(
            pa.Table.from_pylist(
                [
                    {
                        "ledger_id": "unrelated",
                        "symbol": "KEEP",
                        "name": "Keep",
                        "standard": "icrc1",
                        "decimals": 8,
                    }
                ],
                schema=dim.schema().as_arrow(),
            )
        )
        seed_uuid, seed_location = dim.metadata.table_uuid, dim.location()
        key = "synthetic/dim-existing.csv"
        row = (
            "hash-dim2,1793363500000,Swap,pool,alice,pool,ledger-new,NEW,New Token,"
            "ledger-two,TWO,Token Two,1,0,0,2"
        )
        content = (self._DIM_METADATA_HEADER + "\n" + row + "\n").encode()
        s3 = SyntheticS3({key: content})
        with patch.object(self.silver, "_s3", s3):
            self.silver.handler({"mode": "backfill_file", "key": key})
        after = self.catalog.load_table("synthetic.token_dim")
        self.assertEqual(seed_uuid, after.metadata.table_uuid)
        self.assertEqual(seed_location, after.location())
        rows = {r["ledger_id"]: r for r in after.scan().to_arrow().to_pylist()}
        self.assertEqual("KEEP", rows["unrelated"]["symbol"])
        self.assertEqual("NEW", rows["ledger-new"]["symbol"])

    def test_process_key_wires_silver_max_source_bytes_into_extract_metadata(self):
        """Wiring: _process_key must pass the SAME bound it already
        derives from SILVER_MAX_SOURCE_BYTES into extract_metadata's max_bytes keyword,
        not leave extract_metadata on its own 16 MiB default. Asserted on the real
        silver_job.handler path (not just the token_dimension helper in isolation), so a helper that is
        never wired in cannot pass."""
        import token_dimension

        key = "synthetic/wiring.csv"
        row = "wiring-event,1793363500000,Claim,pool,alice,pool,ledger0,ledger1,0,0,0,1"
        content = (B_HEADER + "\n" + row + "\n").encode()
        other_key = "synthetic/wiring2.csv"
        other_row = (
            "wiring-event-2,1793363500000,Claim,pool,alice,pool,ledger0,ledger1,0,0,0,1"
        )
        other_content = (B_HEADER + "\n" + other_row + "\n").encode()
        s3 = SyntheticS3({key: content, other_key: other_content})
        captured = {}
        original = token_dimension.extract_metadata

        def spy(*args, **kwargs):
            captured["max_bytes"] = kwargs.get("max_bytes")
            return original(*args, **kwargs)

        with (
            patch.object(token_dimension, "extract_metadata", side_effect=spy),
            patch.dict(os.environ, {"SILVER_MAX_SOURCE_BYTES": "5242880"}),
            patch.object(self.silver, "_s3", s3),
        ):
            self.silver.handler({"mode": "backfill_file", "key": key})
        self.assertEqual(5242880, captured["max_bytes"])
        # Prove the wiring is live, not a coincidental equal default: a different
        # configured bound must show up unchanged too.
        captured.clear()
        with (
            patch.object(token_dimension, "extract_metadata", side_effect=spy),
            patch.dict(os.environ, {"SILVER_MAX_SOURCE_BYTES": "9999999"}),
            patch.object(self.silver, "_s3", s3),
        ):
            self.silver.handler({"mode": "backfill_file", "key": other_key})
        self.assertEqual(9999999, captured.get("max_bytes"))

    def test_prepare_target_create_or_load_survives_concurrent_table_race(self):
        """With backfill_max_concurrency > 1 against a fresh Glue
        database, multiple Lambdas all miss load_table and race create_table; only
        one wins. The losers must load the table the winner created instead of
        raising TableAlreadyExistsError. Reproduced with real concurrent threads
        against the real backing SqlCatalog, not an injected mock."""
        from concurrent.futures import ThreadPoolExecutor

        from silver_persistence import prepare_target

        identifier = "synthetic.race_events"
        workers = 5
        barrier = threading.Barrier(workers, timeout=10)
        original_create = self.catalog.create_table

        def synchronized_create(*args, **kwargs):
            barrier.wait()
            return original_create(*args, **kwargs)

        with patch.object(
            self.catalog, "create_table", side_effect=synchronized_create
        ):
            with ThreadPoolExecutor(max_workers=workers) as pool:
                futures = [
                    pool.submit(
                        prepare_target, self.catalog, identifier, "test-generation-1"
                    )
                    for _ in range(workers)
                ]
                tables = [future.result(timeout=30) for future in futures]
        self.assertEqual(workers, len(tables))
        uuids = {table.metadata.table_uuid for table in tables}
        self.assertEqual(1, len(uuids), "every worker must observe the ONE real table")
        reloaded = self.catalog.load_table(identifier)
        self.assertEqual(next(iter(uuids)), reloaded.metadata.table_uuid)
        # NOTE (reported, not asserted): a loser that loads the table immediately
        # after catching TableAlreadyExistsError can observe it BEFORE the winner's
        # own update_spec() commits the yearly partition spec - this is the same
        # "spec-id 0 (unpartitioned) + spec-id 1 (year transform) both present"
        # shape, which does not affect the generation/schema checks below it.

    def test_prepare_dim_target_create_or_load_survives_concurrent_table_race(self):
        """token_dim side: prepare_dim_target creates token_dim
        through the same create-on-first-use path as prepare_target, so the first
        real reload races identically on token_dim. Same real-thread reproduction."""
        from concurrent.futures import ThreadPoolExecutor

        from token_dimension import prepare_dim_target

        identifier = "synthetic.race_dim"
        workers = 5
        barrier = threading.Barrier(workers, timeout=10)
        original_create = self.catalog.create_table

        def synchronized_create(*args, **kwargs):
            barrier.wait()
            return original_create(*args, **kwargs)

        with patch.object(
            self.catalog, "create_table", side_effect=synchronized_create
        ):
            with ThreadPoolExecutor(max_workers=workers) as pool:
                futures = [
                    pool.submit(prepare_dim_target, self.catalog, identifier)
                    for _ in range(workers)
                ]
                tables = [future.result(timeout=30) for future in futures]
        self.assertEqual(workers, len(tables))
        uuids = {table.metadata.table_uuid for table in tables}
        self.assertEqual(1, len(uuids), "every worker must observe the ONE real table")
        reloaded = self.catalog.load_table(identifier)
        self.assertEqual(next(iter(uuids)), reloaded.metadata.table_uuid)

    def test_prepare_target_still_raises_on_other_catalog_errors(self):
        """Only TableAlreadyExistsError may be caught.
        Any other catalog/IO error raised from create_table must still propagate -
        this is not a bare except, and must not mask a genuine catalog failure."""
        from silver_persistence import prepare_target

        with (
            patch.object(
                self.catalog,
                "create_table",
                side_effect=RuntimeError("injected catalog outage"),
            ),
            self.assertRaisesRegex(RuntimeError, "injected catalog outage"),
        ):
            prepare_target(self.catalog, "synthetic.outage_events", "test-generation-1")

    def test_prepare_dim_target_still_raises_on_other_catalog_errors(self):
        """Same constraint on the token_dim side: NoSuchTableError and other
        catalog/IO errors must still propagate, never be swallowed."""
        from token_dimension import prepare_dim_target

        with (
            patch.object(
                self.catalog,
                "create_table",
                side_effect=RuntimeError("injected catalog outage"),
            ),
            self.assertRaisesRegex(RuntimeError, "injected catalog outage"),
        ):
            prepare_dim_target(self.catalog, "synthetic.outage_dim")

    # ------------------------------------------------------------------
    # event_id dedup against the fact table (no cumulative index)
    # ------------------------------------------------------------------
    @staticmethod
    def _b_row(tx_hash, out1="1", ts="1793363500000", in0="0", out0="0"):
        return (
            f"{tx_hash},{ts},Claim,pool,alice,pool,ledger0,ledger1,"
            f"{in0},0,{out0},{out1}"
        )

    def _b_object(self, *rows):
        return (B_HEADER + "\n" + "\n".join(rows) + "\n").encode()

    def _facts(self):
        return (
            self.catalog.load_table("synthetic.silver").scan().to_arrow().to_pylist()
        )

    def _ingest(self, key, content):
        # ``_process_key`` returns the FULL result (token_dim lists, readback, every conflict);
        # the handler slims it for Step Functions (see test_backfill_response_size.py).
        with patch.object(self.silver, "_s3", SyntheticS3({key: content})):
            return self.silver._process_key(key)

    def test_payload_hash_is_stable_across_iceberg_round_trip_and_ignores_quality_status(
        self,
    ):
        from silver_csv import normalize_csv
        from silver_persistence import _payload_key

        rows = [
            # trailing zeros, exponent lexeme, zero, huge-scale decimals, null timestamp,
            # sub-second timestamp precision
            self._b_row("rt-zeros", out1="1.2300", in0="10e-1", out0="0.000"),
            self._b_row("rt-big", out1="123456789012345678.123456789012345678"),
            self._b_row("rt-null-ts", ts=""),
            self._b_row("rt-micros", ts="1793363500123"),
            self._b_row("rt-tiny", out1="0.000000000000000001"),
        ]
        incoming, _ = normalize_csv(
            B_HEADER + "\n" + "\n".join(rows),
            "B",
            lambda _o, _r: {
                "lineage_ref": "test",
                "ingested_at": "2026-10-30T00:00:00Z",
            },
        )
        self._ingest("synthetic/round-trip.csv", self._b_object(*rows))
        stored = {row["event_id"]: row for row in self._facts()}
        self.assertEqual(len(rows), len(stored))
        for record in incoming:
            readback = stored[record["event_id"]]
            self.assertEqual(
                _payload_key(record), _payload_key(readback), record["event_id"]
            )
            # quality_status is DERIVED (ok|unresolved|conflict): a stored survivor may
            # carry 'conflict' and must still hash like the incoming payload.
            for status in ("ok", "unresolved", "conflict"):
                self.assertEqual(
                    _payload_key(record),
                    _payload_key(dict(readback, quality_status=status)),
                )
            self.assertNotEqual(
                _payload_key(record),
                _payload_key(dict(readback, token1_amount=Decimal("99"))),
            )


    def _facts_of(self, name="silver"):
        return (
            self.catalog.load_table(f"synthetic.{name}").scan().to_arrow().to_pylist()
        )

    def _snapshot(self, name="silver"):
        table = self.catalog.load_table(f"synthetic.{name}")
        snapshot = table.current_snapshot()
        return None if snapshot is None else snapshot.snapshot_id

    def _records(self, *rows):
        from silver_csv import normalize_csv

        records, _ = normalize_csv(
            B_HEADER + "\n" + "\n".join(rows),
            "B",
            lambda _o, _r: {
                "lineage_ref": "test",
                "ingested_at": "2026-10-30T00:00:00Z",
            },
        )
        return records

    def test_fresh_append_stamps_object_ordinal_lineage_and_no_index_artifacts(self):
        from silver_contract import silver_identity

        key = "synthetic/fresh.csv"
        result = self._ingest(
            key,
            self._b_object(
                self._b_row("fresh-a", out1="1"), self._b_row("fresh-b", out1="2")
            ),
        )
        table = self.catalog.load_table("synthetic.silver")
        facts = {row["event_id"]: row for row in self._facts()}
        self.assertEqual(2, result["rows_written"])
        self.assertFalse(result["replayed"])
        self.assertEqual(
            {
                silver_identity({"tx_hash": "fresh-a"}, "B"): "s3://synthetic-raw/synthetic/fresh.csv#1",
                silver_identity({"tx_hash": "fresh-b"}, "B"): "s3://synthetic-raw/synthetic/fresh.csv#2",
            },
            {event_id: row["lineage_ref"] for event_id, row in facts.items()},
        )
        self.assertEqual({"ok"}, {row["quality_status"] for row in facts.values()})
        self.assertEqual("test-generation-1", table.properties["silver.target.generation"])
        self.assertEqual("silver-flat-v1", table.properties["silver.schema.version"])
        # no cumulative index of any kind: no root/version properties, no shard files
        self.assertNotIn("silver.lineage.root", table.properties)
        self.assertNotIn("silver.index.version", table.properties)
        self.assertEqual(
            [], list(self.allowed_root.rglob("silver-index/**/shard-*")), "shards"
        )
        self.assertEqual([], list(self.allowed_root.rglob("*root-*.json")), "root")

    def test_exact_replay_is_a_noop_even_from_another_object(self):
        row = self._b_row("replay-me", out1="3")
        first = self._ingest("synthetic/replay-1.csv", self._b_object(row))
        before = self._facts()
        snapshot = self._snapshot()
        same_object = self._ingest("synthetic/replay-1.csv", self._b_object(row))
        other_object = self._ingest("synthetic/replay-2.csv", self._b_object(row))
        self.assertEqual(1, first["rows_written"])
        for replay in (same_object, other_object):
            self.assertEqual(0, replay["rows_written"])
            self.assertTrue(replay["replayed"])
        self.assertEqual(snapshot, self._snapshot())
        # the committed row is untouched: original lineage_ref and ingested_at survive
        self.assertEqual(before, self._facts())
        self.assertTrue(before[0]["lineage_ref"].endswith("synthetic/replay-1.csv#1"))


    def test_in_file_conflict_keeps_min_hash_survivor_marked_conflict(self):
        from silver_persistence import _payload_key

        rows = (
            self._b_row("in-file", out1="7"),
            self._b_row("in-file", out1="5"),
            self._b_row("in-file", out1="7"),  # exact duplicate of the first row
            self._b_row("other", out1="9"),
        )
        records = self._records(*rows[:2])
        survivor = min(records, key=_payload_key)
        ordinal = 1 + [r["token1_amount"] for r in records].index(
            survivor["token1_amount"]
        )
        result = self._ingest("synthetic/in-file.csv", self._b_object(*rows))
        facts = {row["event_id"]: row for row in self._facts()}
        row = facts[survivor["event_id"]]
        self.assertEqual(2, result["rows_written"])
        self.assertEqual(2, len(facts))
        self.assertEqual("conflict", row["quality_status"])
        self.assertEqual(survivor["token1_amount"], row["token1_amount"])
        self.assertEqual(
            f"s3://synthetic-raw/synthetic/in-file.csv#{ordinal}", row["lineage_ref"]
        )
        other = next(r for eid, r in facts.items() if eid != survivor["event_id"])
        self.assertEqual("ok", other["quality_status"])
        # replay of the whole conflicted object is a no-op
        snapshot = self._snapshot()
        replay = self._ingest("synthetic/in-file.csv", self._b_object(*rows))
        self.assertEqual(0, replay["rows_written"])
        self.assertEqual(snapshot, self._snapshot())

    def test_cross_file_conflict_survivor_is_order_independent_and_replays_are_noops(
        self,
    ):
        from silver_persistence import _payload_key

        low_or_high = [self._b_row("xf", out1="1"), self._b_row("xf", out1="2")]
        records = self._records(*low_or_high)
        winner_row = low_or_high[records.index(min(records, key=_payload_key))]
        keys = {low_or_high[0]: "synthetic/xf-a.csv", low_or_high[1]: "synthetic/xf-b.csv"}
        orders = {"silver": low_or_high, "silver_rev": low_or_high[::-1]}
        survivors = {}
        for table_name, order in orders.items():
            self.silver.TABLE_NAME = table_name
            for row in order:
                self._ingest(keys[row], self._b_object(row))
            survivors[table_name] = self._facts_of(table_name)
            snapshot = self._snapshot(table_name)
            for row in order:  # conflict row replay (winner AND loser) is a no-op
                replay = self._ingest(keys[row], self._b_object(row))
                self.assertEqual(0, replay["rows_written"], (table_name, row))
            self.assertEqual(snapshot, self._snapshot(table_name))
        for rows in survivors.values():
            self.assertEqual(1, len(rows))
            self.assertEqual("conflict", rows[0]["quality_status"])
        strip = lambda row: {k: v for k, v in row.items() if k != "ingested_at"}  # noqa: E731
        self.assertEqual(
            strip(survivors["silver"][0]), strip(survivors["silver_rev"][0])
        )
        winner_amount = self._records(winner_row)[0]["token1_amount"]
        self.assertEqual(winner_amount, survivors["silver"][0]["token1_amount"])
        self.assertTrue(
            survivors["silver"][0]["lineage_ref"].endswith(keys[winner_row] + "#1")
        )

    def test_in_file_conflict_against_matching_existing_row_marks_it_conflict(self):
        from silver_persistence import _payload_key

        rows = [self._b_row("mark", out1="1"), self._b_row("mark", out1="2")]
        records = self._records(*rows)
        winner = rows[records.index(min(records, key=_payload_key))]
        loser = rows[1 - records.index(min(records, key=_payload_key))]
        # existing row == the eventual survivor; the later object shows BOTH payloads
        first = self._ingest("synthetic/mark-1.csv", self._b_object(winner))
        before = self._facts()[0]
        self._ingest("synthetic/mark-2.csv", self._b_object(loser, winner))
        after = self._facts()
        self.assertEqual(1, first["rows_written"])
        self.assertEqual(1, len(after))
        self.assertEqual("conflict", after[0]["quality_status"])
        # existing row WON: its ingested_at and lineage_ref are kept
        self.assertEqual(before["ingested_at"], after[0]["ingested_at"])
        self.assertEqual(before["lineage_ref"], after[0]["lineage_ref"])

    def test_new_lower_hash_payload_replaces_conflict_survivor(self):
        from silver_persistence import _payload_key

        rows = [self._b_row("three", out1=str(n)) for n in (1, 2, 3)]
        records = self._records(*rows)
        ordered = sorted(range(3), key=lambda i: _payload_key(records[i]))
        # commit the two HIGHER hashes first, then the lowest: it must take over
        self._ingest("synthetic/three-a.csv", self._b_object(rows[ordered[2]]))
        self._ingest("synthetic/three-b.csv", self._b_object(rows[ordered[1]]))
        self.assertEqual(rows[ordered[1]][-1], self._amount_digit(self._facts()[0]))
        self._ingest("synthetic/three-c.csv", self._b_object(rows[ordered[0]]))
        facts = self._facts()
        self.assertEqual(1, len(facts))
        self.assertEqual("conflict", facts[0]["quality_status"])
        self.assertEqual(rows[ordered[0]][-1], self._amount_digit(facts[0]))
        self.assertTrue(facts[0]["lineage_ref"].endswith("synthetic/three-c.csv#1"))

    @staticmethod
    def _amount_digit(fact):
        return str(int(fact["token1_amount"]))


    def _race(self, objects):
        """Run two real handlers whose first two catalog commits are held at a barrier so
        both plan against the same base snapshot; returns (results, commit_outcomes)."""
        from concurrent.futures import ThreadPoolExecutor

        from pyiceberg.exceptions import CommitFailedException

        barrier = threading.Barrier(2, timeout=10)
        delegate = self.catalog.commit_table
        seen, bases, lock = [], [], threading.Lock()

        def synchronized_commit(*args, **kwargs):
            with lock:
                ordinal = len(seen)
                seen.append("entered")
                if ordinal < 2:
                    bases.append(args[0].current_snapshot())
            if ordinal < 2:
                barrier.wait()
            try:
                outcome = delegate(*args, **kwargs)
            except CommitFailedException:
                with lock:
                    seen[ordinal] = "CommitFailedException"
                raise
            with lock:
                seen[ordinal] = "committed"
            return outcome

        self.catalog.commit_table = synchronized_commit
        try:
            with (
                patch.object(self.silver, "_s3", SyntheticS3(objects)),
                ThreadPoolExecutor(max_workers=2) as pool,
            ):
                futures = [
                    pool.submit(self.silver.handler, {"mode": "backfill_file", "key": key})
                    for key in objects
                ]
                results = [future.result(timeout=30) for future in futures]
        finally:
            self.catalog.commit_table = delegate
        return results, seen, bases

    def test_interleaved_writers_same_new_event_id_leave_exactly_one_row(self):
        from silver_persistence import _payload_key

        # different payloads: fence must fail one writer; its retry re-scans and conflicts
        rows = [self._b_row("race-new", out1="2"), self._b_row("race-new", out1="5")]
        records = self._records(*rows)
        objects = {
            "synthetic/race-1.csv": self._b_object(rows[0]),
            "synthetic/race-2.csv": self._b_object(rows[1]),
        }
        results, seen, bases = self._race(objects)
        facts = self._facts()
        self.assertEqual(2, len(results))
        self.assertEqual([None, None], bases)  # both planned against the EMPTY table
        self.assertEqual(1, seen.count("CommitFailedException"))
        # winner (+ the retry's rewrite only when the fenced payload outranks the winner's)
        self.assertIn(seen.count("committed"), (1, 2))
        self.assertEqual(1, len(facts), "exactly one row, never two")
        self.assertEqual("conflict", facts[0]["quality_status"])
        self.assertEqual(
            min(records, key=_payload_key)["token1_amount"], facts[0]["token1_amount"]
        )

    def test_interleaved_writers_identical_payload_leave_one_ok_row(self):
        row = self._b_row("race-same", out1="2")
        objects = {
            "synthetic/race-same-1.csv": self._b_object(row),
            "synthetic/race-same-2.csv": self._b_object(row),
        }
        results, seen, _bases = self._race(objects)
        facts = self._facts()
        self.assertEqual(1, seen.count("CommitFailedException"))
        self.assertEqual(1, len(facts))
        self.assertEqual("ok", facts[0]["quality_status"])
        self.assertEqual([0, 1], sorted(r["rows_written"] for r in results))
        self.assertEqual([False, True], sorted(r["replayed"] for r in results))

    def test_lookup_selection_caps_fail_closed_before_any_write(self):
        first = [self._b_row("cap-a", out1="1"), self._b_row("cap-b", out1="1")]
        second = [self._b_row("cap-c", out1="1"), self._b_row("cap-d", out1="1")]
        self._ingest("synthetic/cap-1.csv", self._b_object(*first))
        self._ingest("synthetic/cap-2.csv", self._b_object(*second))
        tasks = list(self.catalog.load_table("synthetic.silver").scan().plan_files())
        self.assertEqual(2, len(tasks))
        total_bytes = sum(t.file.file_size_in_bytes for t in tasks)
        update = self._b_object(
            self._b_row("cap-a", out1="9"), self._b_row("cap-c", out1="9")
        )
        snapshot = self._snapshot()
        before = self._facts()
        for name, value in (
            ("SILVER_MAX_SELECTED_FILES", "1"),
            ("SILVER_MAX_SELECTED_ROWS", "3"),
            ("SILVER_MAX_SELECTED_BYTES", str(total_bytes - 1)),
        ):
            with self.subTest(cap=name), patch.dict(os.environ, {name: value}):
                with self.assertRaisesRegex(
                    RuntimeError, "selected file/row/byte cap exceeded"
                ):
                    self._ingest("synthetic/cap-update.csv", update)
                self.assertEqual(snapshot, self._snapshot())
                self.assertEqual(before, self._facts())
        # at the exact boundary the same object succeeds
        with patch.dict(
            os.environ,
            {
                "SILVER_MAX_SELECTED_FILES": "2",
                "SILVER_MAX_SELECTED_ROWS": "4",
                "SILVER_MAX_SELECTED_BYTES": str(total_bytes),
            },
        ):
            self.assertEqual(
                2, self._ingest("synthetic/cap-update.csv", update)["rows_written"]
            )

    def test_quarantine_artifact_byte_cap_fails_closed_before_fact_publication(self):
        good = self._b_row("qcap-good", out1="1")
        bad = ",1793363500000,Claim,pool,alice,pool,ledger0,ledger1,0,0,0,0"
        with patch.dict(os.environ, {"SILVER_MAX_QUARANTINE_BYTES": "128"}):
            with self.assertRaisesRegex(RuntimeError, "quarantine byte cap"):
                self._ingest("synthetic/qcap.csv", self._b_object(good, bad))
        table = self.catalog.load_table("synthetic.silver")
        self.assertIsNone(table.current_snapshot())
        self.assertNotIn("silver.target.generation", table.properties)

    def test_quarantine_artifact_is_written_even_when_every_good_row_is_a_replay(self):
        good = self._b_row("qreplay-good", out1="1")
        bad = ",1793363500000,Claim,pool,alice,pool,ledger0,ledger1,0,0,0,0"
        self._ingest("synthetic/qreplay-0.csv", self._b_object(good))
        snapshot = self._snapshot()
        result = self._ingest("synthetic/qreplay-1.csv", self._b_object(good, bad))
        self.assertEqual(0, result["rows_written"])
        self.assertEqual(1, result["rows_quarantined"])
        self.assertTrue(result["replayed"])
        self.assertEqual(snapshot, self._snapshot())
        self.assertTrue(Path(unquote(urlparse(result["quarantine_uri"]).path)).is_file())

    def test_prepare_target_generation_guard_needs_no_lineage_root_or_index_version(
        self,
    ):
        from silver_persistence import IngestionError, prepare_target

        # empty, never-initialised table is accepted
        prepare_target(self.catalog, "synthetic.silver", "test-generation-1")
        self._ingest("synthetic/guard.csv", self._b_object(self._b_row("guard")))
        table = self.catalog.load_table("synthetic.silver")
        self.assertNotIn("silver.lineage.root", table.properties)
        self.assertNotIn("silver.index.version", table.properties)
        # an initialised generation is accepted WITHOUT root/index properties
        prepare_target(self.catalog, "synthetic.silver", "test-generation-1")
        with self.assertRaisesRegex(IngestionError, "target generation mismatch"):
            prepare_target(self.catalog, "synthetic.silver", "other-generation")
        with table.transaction() as tx:
            tx.set_properties({"silver.schema.version": "silver-flat-v0"})
        with self.assertRaisesRegex(IngestionError, "target schema version mismatch"):
            prepare_target(self.catalog, "synthetic.silver", "test-generation-1")

    def test_prepare_target_refuses_nonempty_unindexed_and_legacy_index_tables(self):
        from silver_persistence import IngestionError, prepare_target

        # rows present but no generation marker (e.g. a table written by something else)
        table = self.catalog.create_table(
            "synthetic.foreign_rows", schema=self.catalog.load_table("synthetic.silver").schema()
        )
        table.append(
            pa.Table.from_pylist(
                [self._records(self._b_row("foreign"))[0] | {"lineage_ref": "x"}],
                schema=table.schema().as_arrow(),
            )
        )
        with self.assertRaisesRegex(IngestionError, "nonempty unindexed"):
            prepare_target(self.catalog, "synthetic.foreign_rows", "test-generation-1")
        # initialised generation but no committed snapshot
        empty = self.catalog.create_table(
            "synthetic.gen_no_snapshot", schema=table.schema()
        )
        with empty.transaction() as tx:
            tx.set_properties(
                {
                    "silver.target.generation": "test-generation-1",
                    "silver.schema.version": "silver-flat-v1",
                }
            )
        with self.assertRaisesRegex(IngestionError, "no committed snapshot"):
            prepare_target(self.catalog, "synthetic.gen_no_snapshot", "test-generation-1")
        # a table still carrying the retired lineage index properties is refused loudly
        legacy = self.catalog.create_table("synthetic.legacy_index", schema=table.schema())
        with legacy.transaction() as tx:
            tx.set_properties(
                {
                    "silver.target.generation": "test-generation-1",
                    "silver.lineage.root": "s3://foreign-bucket/unowned/root.json",
                }
            )
        with self.assertRaisesRegex(IngestionError, "legacy lineage index"):
            prepare_target(self.catalog, "synthetic.legacy_index", "test-generation-1")


    def test_seam_end_to_end_through_new_ingest_path_drops_overlap_keeps_rest_and_replays_noop(
        self,
    ):
        """event_id dedup does NOT catch Dataset A vs B duplicates (different identities
        by construction); seam_reconciliation does, BEFORE ingest_one.

        Seam A rows are loaded first, then the seam B object goes through the real
        silver_job._process_key seam branch: the 10 overlapping B rows are dropped, the 2
        non-overlapping B rows are kept, and a replay of the seam B object is a no-op."""
        test_dir = str(Path(__file__).resolve().parent)
        if test_dir not in sys.path:
            sys.path.insert(0, test_dir)
        from test_seam_reconciliation import A_CSV, B_CSV

        objects = {self.silver.SEAM_A_KEY: A_CSV, self.silver.SEAM_B_KEY: B_CSV}
        # CONTROL (proves the premise): the same B bytes under a NON-seam key skip the
        # reconciliation, and event_id dedup keeps all 12 B rows next to the 10 A rows.
        self.silver.TABLE_NAME = "silver_control"
        control_key = "synthetic/control-b.csv"
        with patch.object(
            self.silver, "_s3", SyntheticS3(objects | {control_key: B_CSV})
        ):
            self.silver.handler({"mode": "backfill_file", "key": self.silver.SEAM_A_KEY})
            control = self.silver.handler({"mode": "backfill_file", "key": control_key})
        self.assertEqual(12, control["rows_written"])
        self.assertEqual(22, len(self._facts_of("silver_control")))

        # The real seam path.
        self.silver.TABLE_NAME = "silver"
        s3 = SyntheticS3(objects)
        with patch.object(self.silver, "_s3", s3):
            a_result = self.silver.handler(
                {"mode": "backfill_file", "key": self.silver.SEAM_A_KEY}
            )
            self.assertEqual(10, a_result["rows_written"])
            seam = self.silver.handler(
                {"mode": "backfill_file", "key": self.silver.SEAM_B_KEY}
            )
            facts = self._facts()
            snapshot = self._snapshot()
            replay = self.silver.handler(
                {"mode": "backfill_file", "key": self.silver.SEAM_B_KEY}
            )
        self.assertEqual(10, seam["seam_reconciliation"]["dropped_duplicate_rows"])
        self.assertEqual(12, seam["seam_reconciliation"]["dataset_b_total_rows"])
        self.assertEqual(2, seam["rows_written"])
        self.assertEqual(12, len(facts))  # 10 A + the 2 surviving B rows
        by_source = {}
        for fact in facts:
            by_source.setdefault(
                fact["lineage_ref"].rsplit("#", 1)[0].rsplit("/", 1)[-1], []
            ).append(fact)
        self.assertEqual(10, len(by_source[self.silver.SEAM_A_KEY.rsplit("/", 1)[-1]]))
        survivors = by_source[self.silver.SEAM_B_KEY.rsplit("/", 1)[-1]]
        self.assertEqual(2, len(survivors))
        # the survivors are exactly the in-window row with no A match and the out-of-window row
        self.assertEqual(
            {1780814246, 1780858942},
            {int(row["event_timestamp"].timestamp()) for row in survivors},
        )
        # nothing was marked conflict: the overlap was removed, not merged
        self.assertEqual({"ok"}, {row["quality_status"] for row in facts})
        # replay of the (reconciled) seam B object is a no-op
        self.assertEqual(0, replay["rows_written"])
        self.assertTrue(replay["replayed"])
        self.assertEqual(snapshot, self._snapshot())
        self.assertEqual(facts, self._facts())


    def test_lookup_finds_stored_rows_across_disjoint_day_runs_and_null_timestamps(self):
        day = 86_400_000
        base = 1793363500000
        # two consecutive days, one far-away day and one null timestamp: the lookup filter
        # merges consecutive days into one range and keeps the rest disjoint
        stamps = [str(base), str(base + day), str(base + 40 * day), ""]
        first = [self._b_row(f"days-{i}", out1="1", ts=ts) for i, ts in enumerate(stamps)]
        second = [self._b_row(f"days-{i}", out1="2", ts=ts) for i, ts in enumerate(stamps)]
        self._ingest("synthetic/days-1.csv", self._b_object(*first))
        snapshot = self._snapshot()
        replay = self._ingest("synthetic/days-1b.csv", self._b_object(*first[::-1]))
        self.assertEqual(0, replay["rows_written"])
        self.assertEqual(snapshot, self._snapshot())
        update = self._ingest("synthetic/days-2.csv", self._b_object(*second))
        facts = self._facts()
        self.assertEqual(4, update["rows_written"])  # every stored row found and rewritten
        self.assertEqual(4, len(facts), "no stored row was missed, so none was duplicated")
        self.assertEqual({"conflict"}, {row["quality_status"] for row in facts})

    def test_known_limit_same_event_id_on_a_different_day_is_not_detected(self):
        """DOCUMENTED LIMIT (README 'Known limit'): the lookup is pruned to the incoming
        rows' UTC event days, so an event_id that re-arrives with a timestamp on another
        day is NOT matched and becomes a second row. In this data the timestamp is part of
        the identity, so this is a corruption case, not a normal conflict. This test pins
        the behaviour so widening or narrowing it is a conscious decision."""
        day = 86_400_000
        self._ingest("synthetic/limit-1.csv", self._b_object(self._b_row("limit")))
        self._ingest(
            "synthetic/limit-2.csv",
            self._b_object(self._b_row("limit", ts=str(1793363500000 + 3 * day))),
        )
        facts = self._facts()
        self.assertEqual(2, len(facts))
        self.assertEqual(1, len({row["event_id"] for row in facts}))


    # ------------------------------------------------------------------
    # token_dim current label (latest observed symbol/name win)
    # ------------------------------------------------------------------
    @staticmethod
    def _a_label_object(*rows):
        """rows: (hash, ts_seconds, ledger0, symbol0) -> A object with symbol columns."""
        header = (
            "hash,timestamp,action,pool_id,from,to,token0_id,token0_symbol,"
            "token1_id,token1_symbol,amount_token0,amount_token1,token0_change,"
            "token1_change,amount_usd"
        )
        lines = [
            f"{h},{ts},Swap,pool,alice,pool,{ledger},{symbol},quote-ledger,QUOTE,1,1,1,-1,1"
            for h, ts, ledger, symbol in rows
        ]
        return (header + "\n" + "\n".join(lines) + "\n").encode()

    def _dim_rows(self):
        table = self.catalog.load_table("synthetic.token_dim")
        return {r["ledger_id"]: r for r in table.scan().to_arrow().to_pylist()}

    def test_renamed_token_in_one_a_object_loads_and_replays_without_reverting(self):
        """Through the registered handler: one A object carrying the real rename
        01 -> LUV (6ohjj..., real timestamps) lands its facts and the current label, and
        replaying an OLDER object afterwards cannot revert the label."""
        luv = "6ohjj-4qaaa-aaaai-aapua-cai"
        both = self._a_label_object(
            ("h-old-1", 1683886069, luv, "01"),
            ("h-old-2", 1683886100, luv, "01"),
            ("h-new-1", 1695313142, luv, "LUV"),
        )
        result = self._ingest("synthetic/0000_pool-new.csv", both)
        self.assertEqual(3, result["rows_written"])
        self.assertEqual(
            sorted([luv, "quote-ledger"]), sorted(result["token_dim"]["changed_ledgers"])
        )
        row = self._dim_rows()[luv]
        self.assertEqual("LUV", row["symbol"])
        self.assertEqual(1695313142, int(row["as_of"].timestamp()))
        readback = {t["ledger_id"]: t for t in result["token_dim"]["readback"]["tokens"]}
        self.assertEqual("LUV", readback[luv]["symbol"])
        older = self._a_label_object(("h-old-3", 1683886200, luv, "01"))
        result = self._ingest("synthetic/0001_pool-old.csv", older)
        self.assertEqual(1, result["rows_written"])
        self.assertEqual([], result["token_dim"]["changed_ledgers"])
        self.assertEqual("LUV", self._dim_rows()[luv]["symbol"])
        replay = self._ingest("synthetic/0000_pool-new.csv", both)
        self.assertEqual(0, replay["rows_written"])
        self.assertEqual("LUV", self._dim_rows()[luv]["symbol"])

    def test_token_dim_with_a_different_schema_is_refused_but_facts_still_publish(self):
        """There is no in-place schema evolution. A token_dim of any other schema (here a
        5-column one) is refused fail-closed and left exactly as it was. Since M5 the facts
        are committed FIRST and do not depend on token_dim: they publish, and the refusal is
        reported in ``token_dim.error`` (WARNING marker TOKEN_DIM_UPDATE_FAILED)."""
        legacy = pa.schema(
            [
                pa.field("ledger_id", pa.string(), nullable=False),
                pa.field("symbol", pa.string()),
                pa.field("name", pa.string()),
                pa.field("standard", pa.string()),
                pa.field("decimals", pa.int64()),
            ]
        )
        table = self.catalog.create_table("synthetic.token_dim", schema=legacy)
        location = table.metadata_location
        obj = self._a_label_object(("h-1", 1695313142, "icp-ledger", "ICP"))
        result = self._ingest("synthetic/0000_pool-legacy.csv", obj)
        self.assertRegex(result["token_dim"]["error"], "expected exactly the columns")
        after = self.catalog.load_table("synthetic.token_dim")
        self.assertEqual(location, after.metadata_location)
        self.assertEqual(
            ["ledger_id", "symbol", "name", "standard", "decimals"], after.schema().as_arrow().names
        )
        self.assertEqual(1, result["rows_written"])
        self.assertEqual(1, len(self._facts()))

    def test_a_object_shaped_like_0001_publishes_facts_and_reports_dim_conflicts(self):
        """Through ``silver_job.handler``: real Dataset A attribute conflicts (ICP/ICRC2 on
        ryjl3, ICRC1/ICRC2 on mxzaz, DIP20/ICRC2 on ph45x, decimals 18/12 on pa53d, the
        01 -> LUV rename). The object loads, the FACT
        rows are published, the dim holds the resolved values and the conflicts are reported
        in the result (never raised)."""
        header = (
            "hash,timestamp,action,pool_id,from,to,token0_id,token0_symbol,token0_standard,"
            "token0_decimals,token1_id,token1_symbol,token1_standard,token1_decimals,"
            "amount_token0,amount_token1,token0_change,token1_change,amount_usd"
        )
        quote = "quote-ledger,QUOTE,ICRC1,8"
        rows = [
            ("ryjl3-tyaaa-aaaaa-aaaba-cai", 1702827074, "ICP", "ICRC2", 8),
            ("ryjl3-tyaaa-aaaaa-aaaba-cai", 1702827283, "ICP", "ICP", 8),
            ("mxzaz-hqaaa-aaaar-qaada-cai", 1701487132, "MXZ", "ICRC1", 8),
            ("mxzaz-hqaaa-aaaar-qaada-cai", 1702827259, "MXZ", "ICRC2", 8),
            ("ph45x-riaaa-aaaak-afabq-cai", 1702716039, "PH", "DIP20", 8),
            ("ph45x-riaaa-aaaak-afabq-cai", 1702823692, "PH", "ICRC2", 8),
            ("pa53d-4qaaa-aaaak-afaba-cai", 1699607630, "PA", "ICRC2", 18),
            ("pa53d-4qaaa-aaaak-afaba-cai", 1702082045, "PA", "ICRC2", 12),
            ("6ohjj-4qaaa-aaaai-aapua-cai", 1683886069, "01", "ICRC1", 8),
            ("6ohjj-4qaaa-aaaai-aapua-cai", 1695313142, "LUV", "ICRC1", 8),
        ]
        lines = [
            f"h-{n},{ts},Swap,pool,alice,pool,{ledger},{symbol},{standard},{decimals},"
            f"{quote},1,1,1,-1,1"
            for n, (ledger, ts, symbol, standard, decimals) in enumerate(rows)
        ]
        content = (header + "\n" + "\n".join(lines) + "\n").encode()
        result = self._ingest("synthetic/0001_j34oz-oiaaa-aaaag-qcl3q-cai.csv", content)
        self.assertEqual(len(rows), result["rows_written"])
        self.assertEqual(len(rows), len(self._facts()))  # fact rows published
        dim = self._dim_rows()
        self.assertEqual("ICP", dim["ryjl3-tyaaa-aaaaa-aaaba-cai"]["standard"])
        self.assertEqual("ICRC2", dim["mxzaz-hqaaa-aaaar-qaada-cai"]["standard"])
        self.assertEqual("ICRC2", dim["ph45x-riaaa-aaaak-afabq-cai"]["standard"])
        self.assertEqual(12, dim["pa53d-4qaaa-aaaak-afaba-cai"]["decimals"])  # latest observation
        self.assertEqual("LUV", dim["6ohjj-4qaaa-aaaai-aapua-cai"]["symbol"])
        report = result["token_dim"]
        found = {(c["ledger_id"], c["attr"]): c for c in report["conflicts"]}
        self.assertEqual(5, report["conflicts_resolved"])
        self.assertEqual(
            {
                ("ryjl3-tyaaa-aaaaa-aaaba-cai", "standard"),
                ("mxzaz-hqaaa-aaaar-qaada-cai", "standard"),
                ("ph45x-riaaa-aaaak-afabq-cai", "standard"),
                ("pa53d-4qaaa-aaaak-afaba-cai", "decimals"),
                ("6ohjj-4qaaa-aaaai-aapua-cai", "symbol"),
            },
            set(found),
        )
        self.assertEqual(
            {
                "ledger_id": "pa53d-4qaaa-aaaak-afaba-cai",
                "attr": "decimals",
                "values": [12, 18],
                "chosen": 12,
            },
            found[("pa53d-4qaaa-aaaak-afaba-cai", "decimals")],
        )
        # replay of the whole object: nothing new in either table
        dim_snapshot = self.catalog.load_table("synthetic.token_dim").current_snapshot().snapshot_id
        replay = self._ingest("synthetic/0001_j34oz-oiaaa-aaaag-qcl3q-cai.csv", content)
        self.assertEqual(0, replay["rows_written"])
        self.assertEqual([], replay["token_dim"]["changed_ledgers"])
        self.assertEqual(
            dim_snapshot, self.catalog.load_table("synthetic.token_dim").current_snapshot().snapshot_id
        )

    def test_token_dim_file_count_stays_bounded_over_many_objects(self):
        """Through the registered handler: 45 sequential objects, each with
        brand-new ledgers whose ids span the whole id range (so ``plan_files()`` cannot
        prune any earlier dim file) plus a shared quote ledger whose as-of advances every
        time. Without compaction the 18th object would stop with 'dimension selected
        file/row/byte cap exceeded' (the Map state has no Catch)."""
        from token_dimension import DEFAULT_LIMITS

        bound = DEFAULT_LIMITS["compact_files"] + 1
        for n in range(45):
            ledgers = [f"{p}-{n:03d}" for p in ("aaa", "mmm", "zzz")]
            obj = self._a_label_object(
                *[
                    (f"h-{n}-{i}", 1695313142 + n, ledger, f"S{n}")
                    for i, ledger in enumerate(ledgers)
                ]
            )
            result = self._ingest(f"synthetic/{n:04d}_pool-bound.csv", obj)
            self.assertEqual(3, result["rows_written"], f"object {n}")
            self.assertEqual(
                sorted([*ledgers, "quote-ledger"]),
                sorted(result["token_dim"]["changed_ledgers"]),
            )
            dim = self.catalog.load_table("synthetic.token_dim")
            self.assertLessEqual(len(list(dim.scan().plan_files())), bound, f"object {n}")
        rows = self._dim_rows()
        self.assertEqual(45 * 3 + 1, len(rows))
        self.assertEqual("S44", rows["zzz-044"]["symbol"])
        self.assertEqual(1695313142 + 44, int(rows["quote-ledger"]["as_of"].timestamp()))
        replay = self._ingest("synthetic/0003_pool-bound.csv", self._a_label_object(
            *[(f"h-3-{i}", 1695313145, f"{p}-003", "S3") for i, p in enumerate(("aaa", "mmm", "zzz"))]
        ))
        self.assertEqual([], replay["token_dim"]["changed_ledgers"])

    # ------------------------------------------------------------------
    # pre-deploy review fixes
    # ------------------------------------------------------------------
    def _warehouse_files(self, suffix):
        return sorted(str(p) for p in self.allowed_root.rglob(f"*{suffix}"))

    def _referenced_data_files(self, name="silver"):
        table = self.catalog.load_table(f"synthetic.{name}")
        if table.current_snapshot() is None:
            return set()
        return {
            str(Path(unquote(urlparse(task.file.file_path).path)).resolve())
            for task in table.scan().plan_files()
        }

    def test_h1_mid_file_csv_error_fails_closed_instead_of_committing_the_prefix(self):
        """H1: a csv.Error after valid rows used to return the prefix plus a
        ``malformed_csv`` issue; the prefix was committed as a COMPLETE object and every row
        after the error was silently lost. Now nothing is published."""
        from silver_persistence import IngestionError

        key = "synthetic/truncated.csv"
        content = (
            B_HEADER + "\n"
            "good-1,1793363500000,Claim,pool,alice,pool,ledger0,ledger1,0,0,0,1\n"
            "good-2,1793363500000,Claim,pool,alice,pool,ledger0,ledger1,0,0,0,2\n"
            'bad-3,1793363500000,Claim,pool,"unterminated,pool,ledger0,ledger1,0,0,0,3\n'
            "lost-4,1793363500000,Claim,pool,alice,pool,ledger0,ledger1,0,0,0,4\n"
        ).encode()
        s3 = SyntheticS3({key: content})
        with (
            patch.object(self.silver, "_s3", s3),
            self.assertRaisesRegex(IngestionError, "malformed_csv"),
        ):
            self.silver.handler({"mode": "backfill_file", "key": key})
        self.assertIsNone(self._snapshot())
        self.assertEqual([], list(self.allowed_root.rglob("quarantine-*.json")))

    def test_h2_file_level_corruption_fails_the_invocation(self):
        """H2: invalid UTF-8 and blank/duplicate headers mean the object HAS rows that cannot
        be read; quarantining the whole object and returning success lost them silently."""
        from silver_persistence import IngestionError

        row = "x,1793363500000,Claim,pool,alice,pool,ledger0,ledger1,0,0,0,1"
        cases = {
            "utf8": (B_HEADER + "\n" + row + "\n").encode() + b"\xff\n",
            "dup": (B_HEADER + ",tx_hash\n" + row + ",y\n").encode(),
            "blank": (B_HEADER + ",  \n" + row + ",y\n").encode(),
            "leading-blank": ("\n" + B_HEADER + "\n" + row + "\n").encode(),
        }
        s3 = SyntheticS3({f"synthetic/{name}.csv": body for name, body in cases.items()})
        expected = {
            "utf8": "invalid_utf8",
            "dup": "duplicate_header",
            "blank": "blank_header",
            "leading-blank": "missing_header",
        }
        with patch.object(self.silver, "_s3", s3):
            for name in cases:
                with (
                    self.subTest(name=name),
                    self.assertRaisesRegex(IngestionError, expected[name]),
                ):
                    self.silver.handler({"mode": "backfill_file", "key": f"synthetic/{name}.csv"})
        self.assertIsNone(self._snapshot())

    def test_h2_empty_sources_succeed_with_a_dedicated_warning_marker(self):
        """An empty or header-only object is a legitimate no-data run of the REST incremental
        sink (it writes the header before knowing whether any row is new). It stays a
        success, but logs a WARNING with ``marker=SILVER_EMPTY_SOURCE`` for the alarm."""
        s3 = SyntheticS3(
            {"synthetic/empty.csv": b"", "synthetic/header.csv": (B_HEADER + "\n").encode()}
        )
        with patch.object(self.silver, "_s3", s3):
            for name in ("empty", "header"):
                with self.assertLogs(self.silver.LOG, level="WARNING") as logs:
                    result = self.silver.handler(
                        {"mode": "backfill_file", "key": f"synthetic/{name}.csv"}
                    )
                self.assertEqual(0, result["rows_written"])
                markers = [
                    getattr(r, "extra_fields", {}).get("marker") for r in logs.records
                ]
                self.assertIn("SILVER_EMPTY_SOURCE", markers)

    def test_h2_row_level_quarantine_logs_a_warning_marker(self):
        key = "synthetic/one-bad-row.csv"
        good = "good,1793363500000,Claim,pool,alice,pool,ledger0,ledger1,0,0,0,1"
        bad = ",1793363500000,Claim,pool,alice,pool,ledger0,ledger1,0,0,0,0"
        s3 = SyntheticS3({key: (B_HEADER + "\n" + good + "\n" + bad + "\n").encode()})
        with (
            patch.object(self.silver, "_s3", s3),
            self.assertLogs(self.silver.LOG, level="WARNING") as logs,
        ):
            result = self.silver.handler({"mode": "backfill_file", "key": key})
        self.assertEqual(1, result["rows_quarantined"])
        fields = [getattr(r, "extra_fields", {}) for r in logs.records]
        marked = [f for f in fields if f.get("marker") == "SILVER_ROWS_QUARANTINED"]
        self.assertEqual(1, len(marked))
        self.assertEqual(1, marked[0]["rows_quarantined"])
        self.assertEqual(result["quarantine_uri"], marked[0]["quarantine_uri"])

    class _Context:
        def __init__(self, remaining_ms):
            self.remaining_ms = remaining_ms

        def get_remaining_time_in_millis(self):
            return self.remaining_ms

    def test_m4_deadline_is_derived_from_the_remaining_lambda_time(self):
        """M4: the soft deadline used to start inside ingest_one, after the object read and
        the metadata phase. With less Lambda time left than the safety margin nothing may
        be published."""
        from silver_persistence import IngestionError

        key = "synthetic/late.csv"
        row = "late,1793363500000,Claim,pool,alice,pool,ledger0,ledger1,0,0,0,1"
        s3 = SyntheticS3({key: (B_HEADER + "\n" + row + "\n").encode()})
        with (
            patch.object(self.silver, "_s3", s3),
            self.assertRaisesRegex(IngestionError, "deadline"),
        ):
            self.silver.handler(
                {"mode": "backfill_file", "key": key}, self._Context(remaining_ms=1_000)
            )
        self.assertIsNone(self._snapshot())

    def test_m4_one_deadline_from_invocation_start_is_passed_to_every_key(self):
        import time

        import silver_persistence

        prefix = "icp-swap-elt/rest/incremental/2026-06-22T00-00-00-000Z/"
        keys = [prefix + "transactions_incremental_a.csv", prefix + "transactions_incremental_b.csv"]
        objects = {
            key: (
                B_HEADER + "\n"
                f"inc-{i},1793363500000,Claim,pool,alice,pool,ledger0,ledger1,0,0,0,1\n"
            ).encode()
            for i, key in enumerate(keys)
        }
        s3 = ListingS3(objects)
        seen = []
        original = silver_persistence.ingest_one

        def spy(*args, **kwargs):
            seen.append(kwargs.get("deadline"))
            return original(*args, **kwargs)

        remaining_ms = 300_000
        started = time.monotonic()
        with (
            patch.object(self.silver, "_s3", s3),
            patch.object(silver_persistence, "ingest_one", spy),
            patch.dict(os.environ, {"SILVER_DEADLINE_SECONDS": "240"}),
        ):
            totals = self.silver.handler(
                {"mode": "incremental", "date": "2026-06-22"},
                self._Context(remaining_ms=remaining_ms),
            )
        self.assertEqual(2, totals["rows_written"])
        self.assertEqual(2, len(seen))
        self.assertIsNotNone(seen[0])
        self.assertEqual(seen[0], seen[1], "every key must share ONE invocation deadline")
        self.assertLessEqual(seen[0], started + 240 + 1)

    def test_m5_facts_are_committed_before_the_dimension_and_dim_failure_is_best_effort(self):
        """M5: token_dim used to be merged first, so any MetadataError blocked the facts
        (README: facts never depend on it). Facts now commit first; a dimension failure is
        logged with ``marker=TOKEN_DIM_UPDATE_FAILED`` and reported, never raised."""
        import silver_persistence
        import token_dimension

        order = []
        original_ingest = silver_persistence.ingest_one
        original_apply = token_dimension.apply_metadata

        def ingest(*args, **kwargs):
            order.append("facts")
            return original_ingest(*args, **kwargs)

        def apply(*args, **kwargs):
            order.append("token_dim")
            raise token_dimension.MetadataError("injected dimension failure")

        obj = self._a_label_object(("h-m5", 1695313142, "icp-ledger", "ICP"))
        with (
            patch.object(silver_persistence, "ingest_one", ingest),
            patch.object(token_dimension, "apply_metadata", apply),
            self.assertLogs(self.silver.LOG, level="WARNING") as logs,
        ):
            result = self._ingest("synthetic/m5.csv", obj)
        self.assertEqual(["facts", "token_dim"], order)
        self.assertEqual(1, result["rows_written"])
        self.assertEqual(1, len(self._facts()))
        self.assertIn("injected dimension failure", result["token_dim"]["error"])
        markers = [getattr(r, "extra_fields", {}).get("marker") for r in logs.records]
        self.assertIn("TOKEN_DIM_UPDATE_FAILED", markers)

    def test_m6_fact_commit_conflicts_back_off_exponentially_with_jitter(self):
        from pyiceberg.exceptions import CommitFailedException

        import silver_persistence

        key = "synthetic/backoff.csv"
        row = "backoff,1793363500000,Claim,pool,alice,pool,ledger0,ledger1,0,0,0,1"
        s3 = SyntheticS3({key: (B_HEADER + "\n" + row + "\n").encode()})
        original = Transaction.commit_transaction
        rejected = []
        sleeps = []

        def reject_twice(tx, *args, **kwargs):
            if len(rejected) < 2:
                rejected.append(tx)
                raise CommitFailedException("injected concurrent writer")
            return original(tx, *args, **kwargs)

        with (
            patch.object(Transaction, "commit_transaction", reject_twice),
            patch.object(silver_persistence, "_sleep", sleeps.append),
            patch.object(silver_persistence, "_jitter", lambda: 1.0),
            patch.object(self.silver, "_s3", s3),
        ):
            result = self.silver.handler({"mode": "backfill_file", "key": key})
        base = silver_persistence._RETRY_BASE_SECONDS
        self.assertEqual(1, result["rows_written"])
        self.assertEqual([base, 2 * base], sleeps)
        self.assertGreaterEqual(int(silver_persistence.DEFAULT_MAX_RETRIES), 5)

    def test_m6_fact_commit_backoff_total_is_bounded_and_stays_fail_closed(self):
        from pyiceberg.exceptions import CommitFailedException

        import silver_persistence
        from silver_persistence import IngestionError

        key = "synthetic/backoff-bounded.csv"
        row = "bounded,1793363500000,Claim,pool,alice,pool,ledger0,ledger1,0,0,0,1"
        s3 = SyntheticS3({key: (B_HEADER + "\n" + row + "\n").encode()})
        sleeps = []

        def reject(tx, *args, **kwargs):
            raise CommitFailedException("injected concurrent writer")

        with (
            patch.object(Transaction, "commit_transaction", reject),
            patch.object(silver_persistence, "_sleep", sleeps.append),
            patch.object(silver_persistence, "_jitter", lambda: 1.0),
            patch.object(silver_persistence, "_RETRY_MAX_TOTAL_SLEEP_SECONDS", 1.0),
            patch.dict(os.environ, {"SILVER_MAX_RETRIES": "50"}),
            patch.object(self.silver, "_s3", s3),
            self.assertRaisesRegex(IngestionError, "retry budget exhausted"),
        ):
            self.silver.handler({"mode": "backfill_file", "key": key})
        self.assertLessEqual(sum(sleeps), 1.0)
        self.assertLess(len(sleeps), 49)
        self.assertIsNone(self._snapshot())

    def test_m7_usd_only_difference_is_a_flagged_order_independent_conflict(self):
        """M7 (documented precedence, pinned): two fetches of the same tx_hash that differ
        ONLY in the derived USD value are one row, flagged ``conflict``, and the survivor does
        not depend on which fetch (incremental or backfill) arrived first."""
        header = B_HEADER + ",token0_tx_value"
        base = "usd-event,1793363500000,Swap,pool,alice,pool,ledger-a,ledger-b,1,0,0,2"
        first = (header + "\n" + base + ",10.5\n").encode()
        second = (header + "\n" + base + ",11.25\n").encode()
        survivors = []
        for order, name in (((first, second), "usd_ab"), ((second, first), "usd_ba")):
            self.silver.TABLE_NAME = name
            for n, content in enumerate(order):
                self._ingest(f"synthetic/{name}-{n}.csv", content)
            facts = self._facts_of(name)
            self.assertEqual(1, len(facts))
            self.assertEqual("conflict", facts[0]["quality_status"])
            survivors.append(facts[0]["amount_usd"])
        self.assertEqual(survivors[0], survivors[1])

    def test_m8_list_writes_a_manifest_and_returns_a_small_pointer(self):
        prefix = "icp-swap-elt/canister/2026-06-23T10-25-45-642Z/"
        keys = [f"{prefix}{i:05d}_pool-{i:05d}-aaaaa-aaaag-qnlja-cai.csv" for i in range(5000)]
        s3 = ListingS3({key: b"" for key in keys} | {prefix + "manifest.json": b"{}"})
        with patch.object(self.silver, "_s3", s3):
            out = self.silver.handler({"mode": "list", "prefixes": [prefix]})
        self.assertEqual({"manifest_bucket", "manifest_key", "count"}, set(out))
        self.assertEqual("synthetic-processed", out["manifest_bucket"])
        self.assertEqual(5000, out["count"])
        self.assertLess(len(json.dumps(out)), 1024)
        body = s3.puts[(out["manifest_bucket"], out["manifest_key"])]
        self.assertEqual(keys, json.loads(body))

    def test_l18_list_rejects_empty_or_foreign_prefixes(self):
        s3 = ListingS3({"icp-swap-elt/x/a.csv": b""})
        with patch.object(self.silver, "_s3", s3):
            for prefixes in ([""], ["   "], ["other-bucket-root/"], "icp-swap-elt/", [None]):
                with self.subTest(prefixes=prefixes), self.assertRaises(ValueError):
                    self.silver.handler({"mode": "list", "prefixes": prefixes})
        self.assertEqual([], s3.list_calls)

    def test_m12_seam_reconciliation_uses_the_configured_row_cap(self):
        import seam_reconciliation

        test_dir = str(Path(__file__).resolve().parent)
        if test_dir not in sys.path:
            sys.path.insert(0, test_dir)
        from test_seam_reconciliation import A_CSV, B_CSV

        captured = {}
        original_index = seam_reconciliation.dataset_a_window_index
        original_clean = seam_reconciliation.clean_dataset_b_overlap

        def index(*args, **kwargs):
            captured["a"] = kwargs.get("max_records")
            return original_index(*args, **kwargs)

        def clean(*args, **kwargs):
            captured["b"] = kwargs.get("max_records")
            return original_clean(*args, **kwargs)

        s3 = SyntheticS3({self.silver.SEAM_A_KEY: A_CSV, self.silver.SEAM_B_KEY: B_CSV})
        with (
            patch.object(seam_reconciliation, "dataset_a_window_index", index),
            patch.object(seam_reconciliation, "clean_dataset_b_overlap", clean),
            patch.dict(os.environ, {"SILVER_MAX_SOURCE_ROWS": "12345"}),
            patch.object(self.silver, "_s3", s3),
        ):
            self.silver.handler({"mode": "backfill_file", "key": self.silver.SEAM_B_KEY})
        self.assertEqual({"a": 12345, "b": 12345}, captured)

    def test_l14_new_target_is_created_partitioned_in_one_commit(self):
        from pyiceberg.transforms import YearTransform

        from silver_persistence import prepare_target

        table = prepare_target(self.catalog, "synthetic.fresh_events", "test-generation-1")
        self.assertEqual(1, len(table.metadata.partition_specs))
        fields = table.spec().fields
        self.assertEqual(1, len(fields))
        self.assertIsInstance(fields[0].transform, YearTransform)
        self.assertEqual(
            "event_timestamp", table.schema().find_column_name(fields[0].source_id)
        )

    def test_l14_prepare_target_repairs_empty_unpartitioned_and_refuses_populated(self):
        from pyiceberg.transforms import YearTransform

        from silver_persistence import (
            ARROW_SCHEMA,
            GENERATION_PROPERTY,
            SCHEMA_PROPERTY,
            SCHEMA_VERSION,
            IngestionError,
            prepare_target,
        )

        # crash between create_table and update_spec (old code path): empty, unpartitioned
        self.catalog.create_table("synthetic.half_created", schema=ARROW_SCHEMA)
        repaired = prepare_target(self.catalog, "synthetic.half_created", "test-generation-1")
        self.assertIsInstance(repaired.spec().fields[0].transform, YearTransform)

        populated = self.catalog.create_table("synthetic.unpartitioned", schema=ARROW_SCHEMA)
        with populated.transaction() as tx:
            tx.set_properties(
                {GENERATION_PROPERTY: "test-generation-1", SCHEMA_PROPERTY: SCHEMA_VERSION}
            )
        populated = self.catalog.load_table("synthetic.unpartitioned")
        populated.append(
            pa.Table.from_pylist(self._records(self._b_row("unpart")), schema=ARROW_SCHEMA)
        )
        with self.assertRaisesRegex(IngestionError, "partition"):
            prepare_target(self.catalog, "synthetic.unpartitioned", "test-generation-1")

    def test_l17_result_log_line_is_a_bounded_summary(self):
        captured = []
        original = self.silver.log_event

        def spy(logger, level, message, **fields):
            captured.append((message, fields))
            return original(logger, level, message, **fields)

        rows = [
            (f"h-{i}", 1695313142, f"ledger-{i:04d}-aaaaa-bbbbb-ccccc-cai", f"S{i}")
            for i in range(300)
        ]
        with patch.object(self.silver, "log_event", spy):
            result = self._ingest("synthetic/l17.csv", self._a_label_object(*rows))
        self.assertGreater(len(result["token_dim"]["readback"]["tokens"]), 300)
        logged = [fields for message, fields in captured if message == "object complete"]
        self.assertEqual(1, len(logged))
        self.assertNotIn("readback", logged[0]["token_dim"])
        self.assertEqual(301, logged[0]["token_dim"]["changed_ledgers"])
        self.assertLess(len(json.dumps(logged[0], default=str)), 8 * 1024)

    def test_l19_deadline_abort_inside_the_transaction_removes_staged_files(self):
        import silver_persistence
        from silver_persistence import IngestionError

        clock = [0.0]
        original_append = Transaction.append

        def append(tx, *args, **kwargs):
            result = original_append(tx, *args, **kwargs)
            clock[0] = 2.0
            return result

        key = "synthetic/orphan.csv"
        row = "orphan,1793363500000,Claim,pool,alice,pool,ledger0,ledger1,0,0,0,1"
        s3 = SyntheticS3({key: (B_HEADER + "\n" + row + "\n").encode()})
        with (
            patch.dict(os.environ, {"SILVER_DEADLINE_SECONDS": "1"}),
            patch.object(silver_persistence.time, "monotonic", lambda: clock[0]),
            patch.object(Transaction, "append", append),
            patch.object(self.silver, "_s3", s3),
            self.assertRaisesRegex(IngestionError, "deadline"),
        ):
            self.silver.handler({"mode": "backfill_file", "key": key})
        self.assertIsNone(self._snapshot())
        self.assertEqual([], self._warehouse_files(".parquet"))
        self.assertEqual([], self._warehouse_files(".avro"))

    def test_l19_rejected_commits_leave_no_orphaned_data_files(self):
        from pyiceberg.exceptions import CommitFailedException

        import silver_persistence
        from silver_persistence import IngestionError

        self._ingest("synthetic/orphan-seed.csv", self._b_object(self._b_row("seed")))
        key = "synthetic/orphan-retry.csv"

        def reject(tx, *args, **kwargs):
            raise CommitFailedException("injected concurrent writer")

        with (
            patch.object(Transaction, "commit_transaction", reject),
            patch.object(silver_persistence, "_sleep", lambda _s: None),
            patch.dict(os.environ, {"SILVER_MAX_RETRIES": "3"}),
            patch.object(self.silver, "_s3", SyntheticS3({key: self._b_object(self._b_row("new"))})),
            self.assertRaisesRegex(IngestionError, "retry budget exhausted"),
        ):
            self.silver._process_key(key)
        on_disk = {str(Path(p).resolve()) for p in self._warehouse_files(".parquet")}
        self.assertEqual(self._referenced_data_files(), on_disk)


class ListingS3(SyntheticS3):
    """SyntheticS3 plus the list/put calls the ``list`` and ``incremental`` modes use."""

    def __init__(self, objects):
        super().__init__(objects)
        self.puts = {}
        self.list_calls = []

    def get_paginator(self, name):
        assert name == "list_objects_v2", name
        outer = self

        class Paginator:
            def paginate(self, *, Bucket, Prefix):
                outer.list_calls.append((Bucket, Prefix))
                keys = sorted(k for k in outer.objects if k.startswith(Prefix))
                for start in range(0, max(len(keys), 1), 1000):
                    yield {"Contents": [{"Key": k} for k in keys[start : start + 1000]]}

        return Paginator()

    def put_object(self, *, Bucket, Key, Body, **_kwargs):
        self.puts[(Bucket, Key)] = Body if isinstance(Body, bytes) else Body.encode()
        return {}


if __name__ == "__main__":
    unittest.main(verbosity=2)
