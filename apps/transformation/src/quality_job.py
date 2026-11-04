"""Lambda handler for the data-quality checks (dbt tests on DuckDB over the Silver tables).

The read path is PyIceberg -> Arrow -> DuckDB: PyIceberg is what the transformation already uses
to write and read these tables (it applies delete files and the yearly partition spec), and it
streams record batches, so the fact table never has to fit in memory as Python objects. Only the
columns the checks need are loaded. The tables land in a DuckDB file under /tmp as
``main.icp_swap_events`` and ``main.token_dim``; ``dbt test`` then runs the project in ``quality_dbt/``
(declared tests in ``models/sources.yml`` and SQL tests in ``tests/``).

The scan is bounded from the manifest metadata before any data file is opened (file, row and
byte caps) and by ONE deadline covering BOTH the load and the ``dbt test`` phase (the earlier of
``QUALITY_CHECK_DEADLINE_SECONDS`` after the start and the Lambda's remaining time minus
``QUALITY_CHECK_DEADLINE_MARGIN_SECONDS``). dbt runs in a child process that is killed when the
remaining budget is spent, so the Lambda reports a clear ``QualityCheckError`` instead of being
cut off by its hard timeout. Everything fails closed like the other read paths in this package.
A run with failing (``error``/``fail``) tests raises ``QualityCheckFailed`` so Lambda reports an
error (alarm, SNS); ``warn`` severity tests are reported but do not fail the run.

Scale limit: every run reloads the WHOLE fact table (the selected columns) into DuckDB; the
row/byte caps (default 30,000,000 rows / 8 GiB) are the documented ceiling of this design.
Beyond them the run fails with a message naming the measured size; the follow-up is windowed
checks (see README "Data quality").
"""
from __future__ import annotations

import contextlib
import json
import logging
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import traceback
from pathlib import Path
from typing import Any

from pyiceberg.exceptions import NoSuchTableError

from utils.logging import get_logger, log_event

LOG = get_logger(__name__)

GLUE_DATABASE = os.environ.get("GLUE_DATABASE", "")
TABLE_NAME = os.environ.get("TABLE_NAME", "icp_swap_events")
TOKEN_TABLE_NAME = os.environ.get("TOKEN_TABLE_NAME", "token_dim")
REGION = os.environ.get("AWS_REGION_NAME", os.environ.get("AWS_REGION", "us-east-1"))

# Not named "dbt": in the Lambda image this directory sits next to the installed ``dbt`` package.
DBT_PROJECT_DIR = Path(__file__).resolve().parent / "quality_dbt"

#: Columns the checks read. lineage_ref and the principals are the widest strings and no
#: check needs them, so they are never loaded.
FACT_COLUMNS = (
    "event_id",
    "event_timestamp",
    "action",
    "pool_id",
    "token0_ledger_id",
    "token1_ledger_id",
    "token0_amount",
    "token1_amount",
    "amount_usd",
    "contract_version",
    "quality_status",
    "ingested_at",
)
DIM_COLUMNS = ("ledger_id", "symbol", "name", "standard", "decimals", "as_of")

#: Fail-closed bounds (overridable with the QUALITY_CHECK_* environment variables).
DEFAULT_LIMITS = {
    "max_files": 20_000,
    "max_rows": 30_000_000,
    "max_bytes": 8 * 1024 * 1024 * 1024,
    # Whole run (load + dbt), measured from the handler start.
    "deadline_seconds": 780,
    # The dbt phase is not started with less than this many seconds left.
    "min_dbt_seconds": 30,
    # Kept free before the Lambda hard timeout when a context is available.
    "deadline_margin_seconds": 30,
}
_LIMIT_ENV = {
    "max_files": "QUALITY_CHECK_MAX_SCAN_FILES",
    "max_rows": "QUALITY_CHECK_MAX_SCAN_ROWS",
    "max_bytes": "QUALITY_CHECK_MAX_SCAN_BYTES",
}


class QualityCheckError(RuntimeError):
    """Fail-closed bound on the read-only scan, or dbt itself could not run."""


class QualityCheckFailed(RuntimeError):
    """At least one test with error severity failed; the report is in ``args[1]``."""


def _limit(name: str, key: str) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return DEFAULT_LIMITS[key]
    value = int(raw)
    if value <= 0:
        raise ValueError(f"{name} must be positive")
    return value


def _catalog():
    from pyiceberg.catalog import load_catalog

    return load_catalog("glue", **{"type": "glue", "s3.region": REGION})


def _exec(con, sql: str):
    """Run a statement and drain its result (DuckDB refuses new work on a connection that
    still holds an unfinished result)."""
    return con.execute(sql).fetchall()


def _load_table(catalog, identifier: str, columns: tuple[str, ...], con, target: str, deadline: float):
    """Stream one Iceberg table into DuckDB; returns its scan statistics (None if missing)."""
    try:
        table = catalog.load_table(identifier)
    except NoSuchTableError:
        return None
    scan = table.scan(selected_fields=columns)
    tasks = list(scan.plan_files())
    physical_rows = sum(task.file.record_count for task in tasks)
    size = sum(task.file.file_size_in_bytes for task in tasks)
    measured = {"max_files": len(tasks), "max_rows": physical_rows, "max_bytes": size}
    exceeded = [
        key for key, value in measured.items() if value > _limit(_LIMIT_ENV[key], key)
    ]
    if exceeded:
        bounds = ", ".join(
            f"{_LIMIT_ENV[key]}={_limit(_LIMIT_ENV[key], key)}" for key in exceeded
        )
        raise QualityCheckError(
            f"{identifier}: scan file/row/byte cap exceeded (files={len(tasks)}, "
            f"rows={physical_rows}, bytes={size}; {bounds}). The quality check reloads the "
            "whole table into DuckDB on every run and does not scale past these bounds; "
            "raise them only within the Lambda's memory/time, otherwise move to windowed "
            "checks (README: Data quality, scale limit)"
        )
    if time.monotonic() >= deadline:
        raise QualityCheckError("quality deadline exceeded")

    reader = scan.to_arrow_batch_reader()
    first = True
    for batch in reader:
        if time.monotonic() >= deadline:
            raise QualityCheckError("quality deadline exceeded")
        con.register("incoming_batch", batch)
        if first:
            _exec(con, f"create or replace table main.{target} as select * from incoming_batch")
            first = False
        else:
            _exec(con, f"insert into main.{target} select * from incoming_batch")
        con.unregister("incoming_batch")
    if first:  # no batches: an empty table still needs its columns
        empty = scan.to_arrow().slice(0, 0)
        con.register("incoming_batch", empty)
        _exec(con, f"create or replace table main.{target} as select * from incoming_batch")
        con.unregister("incoming_batch")
    rows = _exec(con, f"select count(*) from main.{target}")[0][0]
    return {"files": len(tasks), "physical_rows": physical_rows, "bytes": size, "rows": rows}


def _prepare_environment(workdir: Path) -> Path:
    """Copy the dbt project next to a writable work dir (the Lambda filesystem is read-only
    except /tmp) and point everything dbt writes there."""
    project = workdir / "project"
    shutil.copytree(DBT_PROJECT_DIR, project)
    os.environ["HOME"] = str(workdir)  # dbt writes under ~/.dbt; only /tmp is writable in Lambda
    os.environ["DBT_PROFILES_DIR"] = str(project)
    os.environ["DBT_SEND_ANONYMOUS_USAGE_STATS"] = "False"
    (workdir / "db").mkdir(exist_ok=True)
    os.environ["QUALITY_DUCKDB_PATH"] = str(workdir / "db" / "quality.duckdb")
    return project


@contextlib.contextmanager
def _thread_locks_instead_of_semaphores():
    """dbt's adapters take ``multiprocessing`` locks, which are POSIX semaphores and need
    /dev/shm; Lambda has none (``FileNotFoundError`` at adapter start-up). dbt runs here in one
    process on a thread pool, so plain thread locks are equivalent. Patched only around the dbt
    run."""
    import multiprocessing.synchronize as synchronize

    # BaseContext.Lock/RLock import these classes at call time, so this also covers the
    # bound ``mp_context.Lock`` factories dbt captured at import.
    saved = synchronize.Lock, synchronize.RLock
    synchronize.Lock = lambda ctx=None: threading.Lock()
    synchronize.RLock = lambda ctx=None: threading.RLock()
    try:
        yield
    finally:
        synchronize.Lock, synchronize.RLock = saved


_DBT_CHILD = (
    "import json, sys; sys.path[:0] = json.loads(sys.argv[1]); import quality_job; "
    "quality_job._dbt_child(sys.argv[2], sys.argv[3], sys.argv[4])"
)


def _dbt_child(project: str, workdir: str, out: str) -> None:
    """Entry point of the isolated dbt process: run the tests, write the checks as JSON."""
    checks = run_dbt_tests(Path(project), Path(workdir))
    Path(out).write_text(json.dumps(checks, default=str))


def _run_dbt_isolated(project: Path, workdir: Path, budget: float) -> list[dict[str, Any]]:
    """Run ``dbt test`` in a child process bounded by ``budget`` seconds.

    dbt runs in-process threads that cannot be interrupted; a child process can be killed,
    so a slow dbt phase ends with a clear error before the Lambda hard timeout (which would
    otherwise surface only as a generic ``Sandbox.Timedout``).
    """
    out = workdir / "checks.json"
    try:
        proc = subprocess.run(
            [sys.executable, "-c", _DBT_CHILD, json.dumps(sys.path), str(project),
             str(workdir), str(out)],
            capture_output=True,
            text=True,
            timeout=budget,
            env=dict(os.environ),
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise QualityCheckError(
            f"dbt test phase exceeded its time budget of {budget:.0f}s (remaining invocation "
            "time) and was killed; no check result is available for this run"
        ) from exc
    if proc.returncode != 0 or not out.is_file():
        raise QualityCheckError(
            f"dbt could not run (exit {proc.returncode}): {(proc.stderr or '')[-1500:]}"
        )
    return json.loads(out.read_text())


def run_dbt_tests(project: Path, workdir: Path) -> list[dict[str, Any]]:
    from dbt.cli.main import dbtRunner

    with _thread_locks_instead_of_semaphores():
        result = dbtRunner().invoke(
            [
                "--log-path", str(workdir / "logs"),
                "--log-level", "warn",
                "test",
                "--project-dir", str(project),
                "--profiles-dir", str(project),
                "--target-path", str(workdir / "target"),
            ]
        )
    if result.exception is not None:
        detail = "".join(traceback.format_exception(result.exception))[-1500:]
        raise QualityCheckError(f"dbt could not run: {result.exception!r}\n{detail}")
    checks: list[dict[str, Any]] = []
    for node_result in result.result.results:
        node = node_result.node
        checks.append(
            {
                "name": node.name,
                "status": str(node_result.status.value if hasattr(node_result.status, "value") else node_result.status),
                "severity": str(node.config.severity).lower(),
                "failures": int(node_result.failures or 0),
                "message": node_result.message,
            }
        )
    return sorted(checks, key=lambda c: c["name"])


def run_quality_checks(
    catalog, *, deadline: float | None = None, isolate_dbt: bool = True
) -> dict[str, Any]:
    """Load the Silver tables into DuckDB, run the dbt tests and return the report.

    ``deadline`` (absolute ``time.monotonic()``) bounds the WHOLE run together with
    ``QUALITY_CHECK_DEADLINE_SECONDS``. ``isolate_dbt=False`` runs dbt in-process (no time
    bound on that phase; tests only).
    """
    import duckdb

    started = time.monotonic()
    soft = started + _limit("QUALITY_CHECK_DEADLINE_SECONDS", "deadline_seconds")
    deadline = soft if deadline is None else min(soft, deadline)
    with tempfile.TemporaryDirectory(prefix="quality-", dir=os.environ.get("QUALITY_WORK_DIR")) as tmp:
        workdir = Path(tmp)
        project = _prepare_environment(workdir)
        con = duckdb.connect(os.environ["QUALITY_DUCKDB_PATH"])
        try:
            _exec(con, "set TimeZone='UTC'")
            facts = _load_table(catalog, f"{GLUE_DATABASE}.{TABLE_NAME}", FACT_COLUMNS, con, "icp_swap_events", deadline)
            if facts is None:
                return {"ok": True, "skipped": "fact table does not exist yet", "tables": {}}
            dim = _load_table(catalog, f"{GLUE_DATABASE}.{TOKEN_TABLE_NAME}", DIM_COLUMNS, con, "token_dim", deadline)
            if dim is None:  # token_dim is created lazily; an empty one keeps the relationship tests meaningful
                _exec(
                    con,
                    "create table main.token_dim (ledger_id varchar, symbol varchar, name varchar, "
                    "standard varchar, decimals bigint, as_of timestamptz)",
                )
                dim = {"files": 0, "physical_rows": 0, "bytes": 0, "rows": 0, "missing": True}
        finally:
            con.close()
        load_seconds = round(time.monotonic() - started, 2)
        remaining = deadline - time.monotonic()
        minimum = _limit("QUALITY_CHECK_MIN_DBT_SECONDS", "min_dbt_seconds")
        if isolate_dbt and remaining < minimum:
            raise QualityCheckError(
                f"dbt test phase not started: {max(0.0, remaining):.0f}s left after a "
                f"{load_seconds:.0f}s load, need at least {minimum}s "
                "(QUALITY_CHECK_MIN_DBT_SECONDS); the tables are too large for one "
                "invocation's time budget"
            )
        if isolate_dbt:
            checks = _run_dbt_isolated(project, workdir, remaining)
        else:
            checks = run_dbt_tests(project, workdir)

    summary = {"pass": 0, "warn": 0, "fail": 0, "error": 0}
    for check in checks:
        summary[check["status"]] = summary.get(check["status"], 0) + 1
    report = {
        "ok": summary["fail"] == 0 and summary["error"] == 0,
        "summary": summary,
        "tables": {"icp_swap_events": facts, "token_dim": dim},
        "load_seconds": load_seconds,
        "checks": checks,
    }
    return report


def _invocation_deadline(context: Any, started: float) -> float | None:
    remaining = getattr(context, "get_remaining_time_in_millis", None)
    if not callable(remaining):
        return None
    margin = _limit("QUALITY_CHECK_DEADLINE_MARGIN_SECONDS", "deadline_margin_seconds")
    return started + remaining() / 1000.0 - margin


def handler(event: dict[str, Any] | None, context: Any = None) -> dict[str, Any]:
    started = time.monotonic()
    event = event or {}
    log_event(LOG, logging.INFO, "invoked", mode="quality")
    report = run_quality_checks(
        _catalog(), deadline=_invocation_deadline(context, started)
    )
    log_event(
        LOG,
        logging.INFO,
        "quality complete",
        ok=report["ok"],
        summary=report.get("summary"),
        failing=[c["name"] for c in report.get("checks", []) if c["status"] in ("fail", "error")],
        warning=[c["name"] for c in report.get("checks", []) if c["status"] == "warn"],
    )
    if not report["ok"] and event.get("fail_on_error", True):
        raise QualityCheckFailed("data-quality checks failed", report)
    return report
