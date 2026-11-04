"""Post-load Iceberg maintenance in our own job: snapshot retention and orphan-file cleanup.

Replaces what the Glue table optimizers used to do (they are not relied on: they run on their
own schedule and commit concurrently with our writers). Run only from the ``compact`` mode,
serially after the backfill Map, never from a per-object path.

Snapshot retention (``expire_snapshots``, both tables)
    PyIceberg 0.8.1 has no expire-snapshots API: ``RemoveSnapshotsUpdate`` exists as a model but
    no metadata-update handler is registered for it. ``install()`` registers one (removes the
    snapshots, prunes the snapshot log like Iceberg Java, refuses to remove a snapshot that a
    branch/tag references) and the expiry is committed through the public
    ``Catalog.commit_table`` with ``AssertTableUUID`` + ``AssertRefSnapshotId(main)``: a writer
    that commits in between makes the catalog reject it (``CommitFailedException``), and the
    expiry is re-planned. Expired = older than ``SILVER_SNAPSHOT_RETENTION_DAYS`` and not among
    the newest ``SILVER_SNAPSHOTS_TO_RETAIN``, not current, not referenced by a ref. Only AFTER a
    confirmed commit are the files deleted that the expired snapshots reach and no retained
    snapshot reaches (manifest lists, manifests, live data files); an unknown commit outcome
    deletes nothing.

Orphan cleanup (``remove_orphan_files``, ``token_dim`` only)
    Lists ``<location>/data/`` and ``<location>/metadata/`` (nothing else: the fact table keeps
    unreferenced quarantine artifacts beside its data and is refused outright), then loads the
    table metadata, and deletes a file only if no snapshot (every manifest entry, live or
    deleted), the current metadata file or the metadata log references it AND it is older than
    ``SILVER_ORPHAN_GRACE_DAYS``: a writer that has not committed yet has files that look
    orphaned, the grace period (>= 1 day, a Lambda runs <= 15 min) keeps them.

Both are bounded per run (manifests read, files listed, files deleted per run) and
deadline-checked. A backlog larger than one run's bounds is reported (``complete: false`` with a
pending count) and continued by the next run (daily and after every load); it is not an error.
A cap that a single unit of work exceeds (one snapshot, the listing) is refused fail-closed.
"""

from __future__ import annotations

import logging
import random
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from urllib.parse import urlparse

from pyiceberg.exceptions import CommitFailedException
from pyiceberg.table.update import (
    AssertRefSnapshotId,
    AssertTableUUID,
    RemoveSnapshotsUpdate,
    _apply_table_update,
)

from silver_persistence import (
    DEFAULT_MAX_RETRIES,
    IngestionError,
    _RETRY_BASE_SECONDS,
    _RETRY_MAX_SLEEP_SECONDS,
    _RETRY_MAX_TOTAL_SLEEP_SECONDS,
    _limit,
)
from utils.logging import get_logger, log_event
from utils.retry import backoff_delay

LOG = get_logger(__name__)
DAY_MS = 86_400_000
#: Noncurrent object versions in the processed bucket are kept this long (storage root,
#: ``processed_noncurrent_version_expiration_days``); a rollback to a retained snapshot whose
#: files were deleted needs them, so retention must stay at least 30 days below it.
_NONCURRENT_HEADROOM_DAYS = 30
_DELETE_WORKERS = 16
_BATCH_TIME_FACTOR = 1.5

_monotonic = time.monotonic
_sleep = time.sleep
_jitter = random.random
_now_ms = lambda: int(time.time() * 1000)  # noqa: E731


# --- RemoveSnapshotsUpdate handler (missing in PyIceberg 0.8.1) ------------------------------


def _remove_snapshots(update, base_metadata, context):
    remove = set(update.snapshot_ids)
    for name, ref in base_metadata.refs.items():
        if ref.snapshot_id in remove:
            raise ValueError(f"cannot expire snapshot {ref.snapshot_id}: referenced by {name}")
    if base_metadata.current_snapshot_id in remove:
        raise ValueError("cannot expire the current snapshot")
    if not remove & {s.snapshot_id for s in base_metadata.snapshots}:
        return base_metadata
    snapshots = [s for s in base_metadata.snapshots if s.snapshot_id not in remove]
    # Iceberg Java semantics: history before an expired snapshot is dropped with it.
    log = []
    for entry in base_metadata.snapshot_log:
        if entry.snapshot_id in remove:
            log = []
        else:
            log.append(entry)
    context.add_update(update)
    return base_metadata.model_copy(update={"snapshots": snapshots, "snapshot_log": log})


def install() -> None:
    """Register the handler unless PyIceberg already provides one (then use theirs)."""
    if _apply_table_update.dispatch(RemoveSnapshotsUpdate) is _apply_table_update.dispatch(object):
        _apply_table_update.register(RemoveSnapshotsUpdate)(_remove_snapshots)


install()


# --- limits ----------------------------------------------------------------------------------


def maintenance_limits() -> dict[str, int]:
    limits = {
        "retention_days": _limit("SILVER_SNAPSHOT_RETENTION_DAYS", 7),
        "keep": _limit("SILVER_SNAPSHOTS_TO_RETAIN", 1),
        "noncurrent_days": _limit("SILVER_NONCURRENT_VERSION_DAYS", 90),
        "grace_days": _limit("SILVER_ORPHAN_GRACE_DAYS", 3),
        "max_manifests": _limit("SILVER_MAINT_MAX_MANIFESTS", 20_000),
        "max_delete": _limit("SILVER_MAINT_MAX_DELETE_FILES", 5_000),
        "max_listed": _limit("SILVER_MAINT_MAX_LISTED_FILES", 200_000),
        "retries": _limit("SILVER_MAX_RETRIES", DEFAULT_MAX_RETRIES),
        "deadline_seconds": _limit("SILVER_DEADLINE_SECONDS", 45),
    }
    if limits["retention_days"] + _NONCURRENT_HEADROOM_DAYS > limits["noncurrent_days"]:
        raise IngestionError(
            "SILVER_SNAPSHOT_RETENTION_DAYS must stay at least 30 days below the processed "
            "bucket's noncurrent-version expiry (SILVER_NONCURRENT_VERSION_DAYS, 90): a rollback "
            "to a retained snapshot may need deleted files back from noncurrent versions"
        )
    return limits


def _deadline_checker(limits, deadline, what):
    end = _monotonic() + limits["deadline_seconds"]
    if deadline is not None:
        end = min(end, deadline)

    def check():
        if _monotonic() >= end:
            raise IngestionError(f"{what} deadline exceeded")

    return end, check


# --- reachability ----------------------------------------------------------------------------


class _Reach:
    """Files reachable from snapshots, with every manifest read at most once (bounded)."""

    def __init__(self, io, max_manifests, check_deadline, live_only):
        self.io = io
        self.max_manifests = max_manifests
        self.check_deadline = check_deadline
        self.live_only = live_only
        self.manifest_files: dict[str, frozenset[str]] = {}
        self.snapshot_manifests: dict[int, tuple[str, tuple[str, ...]]] = {}

    def _snapshot(self, snapshot):
        cached = self.snapshot_manifests.get(snapshot.snapshot_id)
        if cached is None:
            manifests = snapshot.manifests(self.io)
            for manifest in manifests:
                if manifest.manifest_path not in self.manifest_files:
                    if len(self.manifest_files) >= self.max_manifests:
                        raise IngestionError(
                            "maintenance manifest cap exceeded (SILVER_MAINT_MAX_MANIFESTS); "
                            "refusing"
                        )
                    self.check_deadline()
                    entries = manifest.fetch_manifest_entry(
                        self.io, discard_deleted=self.live_only
                    )
                    self.manifest_files[manifest.manifest_path] = frozenset(
                        entry.data_file.file_path for entry in entries
                    )
            cached = (snapshot.manifest_list, tuple(m.manifest_path for m in manifests))
            self.snapshot_manifests[snapshot.snapshot_id] = cached
        return cached

    def files(self, snapshots) -> set[str]:
        out: set[str] = set()
        for snapshot in snapshots:
            manifest_list, manifests = self._snapshot(snapshot)
            out.add(manifest_list)
            for path in manifests:
                out.add(path)
                out |= self.manifest_files[path]
        return out


def _delete_all(delete, paths) -> list[str]:
    failed = []

    def one(path):
        try:
            delete(path)
        except FileNotFoundError:
            return None
        except Exception:  # noqa: BLE001 - collected and reported below
            return path
        return None

    with ThreadPoolExecutor(max_workers=_DELETE_WORKERS) as pool:
        for result in pool.map(one, sorted(paths)):
            if result is not None:
                failed.append(result)
    return failed


# --- snapshot retention ----------------------------------------------------------------------


def _expiry_candidates(metadata, limits, now_ms):
    snapshots = sorted(metadata.snapshots, key=lambda s: (s.timestamp_ms, s.snapshot_id))
    protected = {ref.snapshot_id for ref in metadata.refs.values()}
    if metadata.current_snapshot_id is not None:
        protected.add(metadata.current_snapshot_id)
    protected |= {s.snapshot_id for s in snapshots[-limits["keep"] :]}
    cutoff = now_ms - limits["retention_days"] * DAY_MS
    candidates = [
        s for s in snapshots if s.snapshot_id not in protected and s.timestamp_ms < cutoff
    ]
    return snapshots, candidates


def _plan_batch(snapshots, candidates, reach, budget, max_delete):
    """Oldest-first prefix of ``candidates`` whose unreachable files fit the run's remaining
    ``budget``. ``None`` when not even the oldest one fits the remaining budget (the next run
    continues); a snapshot that alone exceeds the whole per-run cap is refused."""
    size = len(candidates)
    while True:
        batch = candidates[:size]
        batch_ids = {s.snapshot_id for s in batch}
        keep = reach.files([s for s in snapshots if s.snapshot_id not in batch_ids])
        doomed = reach.files(batch) - keep
        if len(doomed) <= budget:
            return batch, doomed
        if size == 1:
            if len(doomed) > max_delete:
                raise IngestionError(
                    f"expiring snapshot {batch[0].snapshot_id} alone would delete "
                    f"{len(doomed)} files, above SILVER_MAINT_MAX_DELETE_FILES; refusing"
                )
            return None
        size //= 2


def expire_snapshots(
    table, *, now_ms: int | None = None, deadline: float | None = None, limits=None
) -> dict[str, Any]:
    """Expire old snapshots of ``table`` and delete the files only they reached.

    Bounded per run: at most ``SILVER_MAINT_MAX_DELETE_FILES`` files are deleted and no new
    batch is started when the remaining time looks too short. Leftover candidates are
    reported (``complete: false``, ``pending``) and expired by the next run; this backlog is
    not an error, the data is unaffected.
    """
    limits = limits or maintenance_limits()
    end, check_deadline = _deadline_checker(limits, deadline, "snapshot retention")
    check_deadline()
    now_ms = _now_ms() if now_ms is None else now_ms
    expired = files_deleted = commits = attempt = pending = 0
    slept = last_duration = 0.0
    while True:
        check_deadline()
        table.refresh()
        metadata = table.metadata
        snapshots, candidates = _expiry_candidates(metadata, limits, now_ms)
        pending = len(candidates)
        if not candidates:
            break
        if commits and _monotonic() + _BATCH_TIME_FACTOR * last_duration >= end:
            break
        started = _monotonic()
        reach = _Reach(table.io, limits["max_manifests"], check_deadline, live_only=True)
        planned = _plan_batch(
            snapshots, candidates, reach, limits["max_delete"] - files_deleted, limits["max_delete"]
        )
        if planned is None:
            break  # per-run delete budget spent
        batch, doomed = planned
        check_deadline()
        try:
            table.catalog.commit_table(
                table,
                requirements=(
                    AssertTableUUID(uuid=metadata.table_uuid),
                    AssertRefSnapshotId(ref="main", snapshot_id=metadata.current_snapshot_id),
                ),
                updates=(RemoveSnapshotsUpdate(snapshot_ids=[s.snapshot_id for s in batch]),),
            )
        except CommitFailedException:
            # Definitively rejected: nothing changed and nothing was deleted; re-plan.
            if attempt + 1 >= limits["retries"]:
                raise IngestionError("snapshot retention fence conflict retry budget exhausted")
            delay = backoff_delay(
                attempt, base=_RETRY_BASE_SECONDS, cap=_RETRY_MAX_SLEEP_SECONDS, jitter=_jitter
            )
            if slept + delay > _RETRY_MAX_TOTAL_SLEEP_SECONDS or _monotonic() + delay >= end:
                raise IngestionError(
                    "snapshot retention retry budget exhausted (backoff bound or deadline)"
                )
            _sleep(delay)
            slept += delay
            attempt += 1
            continue
        table.refresh()
        remaining = {s.snapshot_id for s in table.metadata.snapshots}
        if remaining & {s.snapshot_id for s in batch}:
            raise IngestionError("snapshot expiry not confirmed by readback; nothing deleted")
        # Confirmed: the doomed files are referenced by no retained snapshot.
        failed = _delete_all(table.io.delete, doomed)
        if failed:
            raise IngestionError(
                f"snapshots expired but {len(failed)} unreachable files could not be deleted "
                f"(first: {failed[:3]})"
            )
        attempt, slept = 0, 0.0
        commits += 1
        expired += len(batch)
        files_deleted += len(doomed)
        last_duration = _monotonic() - started
        log_event(
            LOG,
            logging.INFO,
            "snapshots expired",
            table=table.name()[-1],
            expired=len(batch),
            files_deleted=len(doomed),
        )
    if pending:
        log_event(
            LOG,
            logging.WARNING,
            "snapshot retention backlog left for the next run",
            table=table.name()[-1],
            pending=pending,
        )
    return {
        "complete": pending == 0,
        "expired": expired,
        "pending": pending,
        "retained": len(table.metadata.snapshots),
        "files_deleted": files_deleted,
        "commits": commits,
    }


# --- orphan cleanup --------------------------------------------------------------------------


def _norm(uri: str) -> str:
    """Comparable form of a file location: ``bucket/key`` for S3, the path for local files
    (what ``pyarrow.fs`` listing returns)."""
    parsed = urlparse(uri)
    if parsed.scheme in {"s3", "s3a", "s3n", "gs"}:
        return f"{parsed.netloc}{parsed.path}"
    return parsed.path if parsed.scheme in {"file", ""} else uri


def _overlaps(a: str, b: str) -> bool:
    a, b = a.rstrip("/") + "/", b.rstrip("/") + "/"
    return a.startswith(b) or b.startswith(a)


def remove_orphan_files(
    table,
    *,
    protected_locations: tuple[str, ...],
    now_ms: int | None = None,
    deadline: float | None = None,
    limits=None,
) -> dict[str, Any]:
    """Delete unreferenced files older than the grace period below data/ and metadata/."""
    from pyarrow.fs import FileSelector, FileType

    limits = limits or maintenance_limits()
    _, check_deadline = _deadline_checker(limits, deadline, "orphan cleanup")
    check_deadline()
    location = table.location().rstrip("/")
    for other in protected_locations:
        if _overlaps(location, other):
            raise IngestionError(
                "orphan cleanup refused: the table location overlaps a protected table "
                "(icp_swap_events is excluded on purpose: its quarantine artifacts are not "
                "referenced by Iceberg metadata)"
            )
    now_ms = _now_ms() if now_ms is None else now_ms
    cutoff_ns = (now_ms - limits["grace_days"] * DAY_MS) * 1_000_000
    parsed = urlparse(location)
    fs_by_scheme = getattr(table.io, "fs_by_scheme", None)
    if not callable(fs_by_scheme):
        raise IngestionError("orphan cleanup needs a pyarrow FileIO; refusing")
    fs = fs_by_scheme(parsed.scheme or "file", parsed.netloc)
    base = _norm(location)
    listed: list[tuple[str, int | None]] = []
    for sub in ("data", "metadata"):
        selector = FileSelector(f"{base}/{sub}", recursive=True, allow_not_found=True)
        for info in fs.get_file_info(selector):
            if info.type != FileType.File:
                continue
            listed.append((info.path, info.mtime_ns))
            if len(listed) > limits["max_listed"]:
                raise IngestionError(
                    "orphan cleanup listing cap exceeded (SILVER_MAINT_MAX_LISTED_FILES); "
                    "nothing deleted"
                )
        check_deadline()
    # Metadata is read AFTER the listing: everything committed before this point is referenced.
    table.refresh()
    metadata = table.metadata
    reach = _Reach(table.io, limits["max_manifests"], check_deadline, live_only=False)
    referenced = {_norm(path) for path in reach.files(metadata.snapshots)}
    referenced.add(_norm(table.metadata_location))
    referenced |= {_norm(entry.metadata_file) for entry in metadata.metadata_log}
    orphans = sorted(
        (mtime, path)
        for path, mtime in listed
        if path not in referenced and mtime is not None and mtime < cutoff_ns
    )
    batch = [path for _, path in orphans[: limits["max_delete"]]]
    check_deadline()
    failed = _delete_all(fs.delete_file, batch)
    if failed:
        raise IngestionError(
            f"orphan cleanup could not delete {len(failed)} files (first: {failed[:3]})"
        )
    log_event(
        LOG,
        logging.INFO,
        "orphan files removed",
        table=table.name()[-1],
        listed=len(listed),
        deleted=len(batch),
    )
    pending = len(orphans) - len(batch)  # bounded per run; the next run continues
    return {
        "complete": pending == 0,
        "listed": len(listed),
        "orphans_deleted": len(batch),
        "orphans_pending": pending,
    }
