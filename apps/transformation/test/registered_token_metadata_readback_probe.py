"""Guarded fresh-process readback probe for the registered local token metadata handler."""

from __future__ import annotations

import json
import os
import socket
import sys
from pathlib import Path
from unittest.mock import patch


def main() -> int:
    catalog_uri, warehouse, ledger_csv = sys.argv[1:4]
    allowed = Path(warehouse).resolve()
    os.environ.update(
        {
            "RAW_BUCKET": "synthetic-raw",
            "PROCESSED_BUCKET": "synthetic-processed",
            "GLUE_DATABASE": "synthetic",
            "TABLE_NAME": "silver",
            "AWS_ACCESS_KEY_ID": "synthetic",
            "AWS_SECRET_ACCESS_KEY": "synthetic",
            "AWS_SESSION_TOKEN": "synthetic",
            "AWS_EC2_METADATA_DISABLED": "true",
            "AWS_CONFIG_FILE": str(allowed / "absent-config"),
            "AWS_SHARED_CREDENTIALS_FILE": str(allowed / "absent-credentials"),
        }
    )
    attempts = []

    def deny(*_args, **_kwargs):
        attempts.append("external")
        raise AssertionError("external access forbidden in child readback")

    socket.socket.connect = deny
    socket.socket.connect_ex = deny
    socket.create_connection = deny
    test_dir = Path(__file__).resolve().parent
    sys.path.insert(0, str(test_dir))
    import boto3
    import botocore.endpoint

    boto3.client = lambda *_args, **_kwargs: object()
    botocore.endpoint.Endpoint._send = deny

    import pyarrow.fs as pafs
    from pyiceberg.io.pyarrow import PyArrowFileIO
    from test_registered_silver_ingestion import LocalOnlyFileIO, TracingLocalHandler

    tracing = TracingLocalHandler(allowed)
    local_fs = pafs.PyFileSystem(tracing)
    PyArrowFileIO._initialize_fs = lambda _self, scheme, netloc=None: local_fs
    from pyiceberg.catalog.sql import SqlCatalog

    catalog = SqlCatalog("registered-test", uri=catalog_uri, warehouse=allowed.as_uri())
    original_load = catalog.load_table

    def guarded_load(identifier):
        table = original_load(identifier)
        table.io = LocalOnlyFileIO(table.io, allowed).install()
        return table

    catalog.load_table = guarded_load
    with (
        patch.object(boto3, "client", return_value=object()),
        patch.object(botocore.endpoint.Endpoint, "_send", deny),
    ):
        import silver_job

    silver_job._catalog = lambda: catalog
    if not Path(silver_job.__file__).resolve().is_relative_to(test_dir.parent / "src"):
        raise AssertionError(f"unexpected transformation import origin: {silver_job.__file__}")
    result = silver_job.handler(
        {"mode": "token_metadata", "ledger_ids": ledger_csv.split(",")}
    )
    if attempts:
        raise AssertionError(f"external attempts: {attempts}")
    print(
        json.dumps(
            {
                "result": result,
                "origins": {"silver_job": silver_job.__file__},
                "external_attempts": attempts,
                "parquet_reads": len(tracing.parquet_reads),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
