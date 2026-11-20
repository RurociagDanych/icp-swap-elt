"""One-time historical reconciliation for the Dataset A / Dataset B seam."""

from __future__ import annotations

import csv
import io
from datetime import datetime
from typing import Any

from silver_contract import normalize_silver

# Scope: ONLY the ~6.25 h overlap where Dataset A's canister export (ends 2026-06-07T06:37:26Z in
# 0071_3326b-qiaaa-aaaag-qnlja-cai.csv, SEAM_A_KEY in silver_job.py) and Dataset B's REST backfill
# (transactions_backfill_..._p001001.csv, starts 2026-06-07T00:22:33Z) hold the same on-chain swaps
# under different event_ids (A keys on the canister's event hash, B on tx_hash), so the event_id
# dedup cannot catch them. On a match the A row is kept and the B duplicate dropped. This is not
# an ongoing dedup: going forward only Dataset B exists.
#
# Real matched A/B pairs carry exactly equal timestamps. A small margin is kept
# defensively in case a re-run of this one-time reload meets a pair that isn't bit-exact.
DEFAULT_TOLERANCE_SECONDS = 2

_CONTENT_FIELDS = (
    "pool_id",
    "token0_ledger_id",
    "token1_ledger_id",
    "action",
    "token0_amount",
    "token1_amount",
)


def _dummy_lineage(ordinal: int) -> dict[str, Any]:
    """Placeholder lineage: normalize_silver requires one, but it is not part of the content key."""
    return {
        "lineage_ref": f"seam-reconciliation:{ordinal}",
        "ingested_at": "2026-01-01T00:00:00+00:00",
    }


def _content_key(record: dict[str, Any] | None) -> tuple[Any, ...] | None:
    """Business-content identity for cross-dataset matching.

    Excludes event_id, lineage_ref, ingested_at (occurrence, not content) and amount_usd
    (derived differently per dataset, so not a reliable identity field).
    """
    if record is None or any(record.get(f) is None for f in _CONTENT_FIELDS):
        return None
    return tuple(record[f] for f in _CONTENT_FIELDS)


def _iter_normalized(csv_bytes: bytes, dataset: str, max_records: int):
    text = csv_bytes.decode("utf-8")
    reader = csv.DictReader(io.StringIO(text, newline=""), strict=True)
    for ordinal, raw in enumerate(reader, start=1):
        if ordinal > max_records:
            raise ValueError("row_limit_exceeded")
        if None in raw or any(value is None for value in raw.values()):
            continue
        row = {str(k).strip(): v for k, v in raw.items() if k is not None}
        record, _issues = normalize_silver(row, dataset, _dummy_lineage(ordinal))
        yield row, record


def dataset_a_window_index(
    csv_bytes: bytes,
    window_start: datetime,
    window_end: datetime,
    *,
    max_records: int,
) -> list[tuple[tuple[Any, ...], datetime]]:
    """Index Dataset-A rows within [window_start, window_end] by content key.

    Rows with any normalization issue (no reliable content key) are skipped: they never justify
    dropping a Dataset-B row, which keeps the reconciliation fail-safe toward over-counting.
    """
    index: list[tuple[tuple[Any, ...], datetime]] = []
    for _row, record in _iter_normalized(csv_bytes, "A", max_records):
        if record is None:
            continue
        ts = record.get("event_timestamp")
        if ts is None or not (window_start <= ts <= window_end):
            continue
        key = _content_key(record)
        if key is None:
            continue
        index.append((key, ts))
    return index


def clean_dataset_b_overlap(
    csv_bytes: bytes,
    a_window_index: list[tuple[tuple[Any, ...], datetime]],
    window_start: datetime,
    window_end: datetime,
    *,
    tolerance_seconds: float = DEFAULT_TOLERANCE_SECONDS,
    max_records: int,
) -> tuple[bytes, dict[str, Any]]:
    """Drop Dataset-B rows in the window that content-match a Dataset-A row; keep all others byte-for-byte.

    Each Dataset-A entry is consumed at most once (N real duplicates drop exactly N B rows),
    matched to the nearest unconsumed A timestamp within ``tolerance_seconds``. Rows outside the
    window or without a clean content key are always kept: only positive matches are removed.

    A row whose cell count differs from the header is never re-shaped (extra cells kept, short
    row NOT padded), so the CSV boundary downstream quarantines it as ``column_count`` with its
    original values. ``max_records`` is the caller's row bound (``SILVER_MAX_SOURCE_ROWS``).

    Returns (cleaned_csv_bytes, report) with counts for auditing.
    """
    pending: dict[tuple[Any, ...], list[datetime]] = {}
    for key, ts in a_window_index:
        pending.setdefault(key, []).append(ts)

    text = csv_bytes.decode("utf-8")
    reader = csv.reader(io.StringIO(text, newline=""), strict=True)
    fieldnames = next(reader, None) or []
    out = io.StringIO(newline="")
    # Same dialect csv.DictWriter used before (QUOTE_MINIMAL, CRLF), but rows are written as
    # their ORIGINAL cell lists so no row is ever widened, truncated or padded.
    writer = csv.writer(out, lineterminator="\r\n")
    writer.writerow(fieldnames)

    total_rows = 0
    window_rows = 0
    dropped = 0
    column_count_rows = 0
    ordinal = 0
    for cells in reader:
        if not cells:
            continue  # blank line: skipped exactly like csv.DictReader does
        ordinal += 1
        if ordinal > max_records:
            raise ValueError("row_limit_exceeded")
        total_rows += 1
        if len(cells) != len(fieldnames):
            # Malformed column count: never a reliable content match; kept with exactly
            # its original cells so the ingestion CSV boundary quarantines it.
            column_count_rows += 1
            writer.writerow(cells)
            continue
        row = {str(k).strip(): v for k, v in zip(fieldnames, cells)}
        record, _issues = normalize_silver(row, "B", _dummy_lineage(ordinal))
        ts = record.get("event_timestamp") if record else None
        in_window = ts is not None and window_start <= ts <= window_end
        if in_window:
            window_rows += 1
        dropped_this_row = False
        if in_window and ts is not None:
            key = _content_key(record)
            candidates = pending.get(key) if key is not None else None
            if candidates:
                best_idx, best_diff = None, None
                for idx, candidate_ts in enumerate(candidates):
                    diff = abs((candidate_ts - ts).total_seconds())
                    if diff <= tolerance_seconds and (
                        best_diff is None or diff < best_diff
                    ):
                        best_idx, best_diff = idx, diff
                if best_idx is not None:
                    candidates.pop(best_idx)
                    dropped_this_row = True
                    dropped += 1
        if not dropped_this_row:
            writer.writerow(cells)

    cleaned = out.getvalue().encode("utf-8")
    report = {
        "dataset_b_total_rows": total_rows,
        "dataset_b_window_rows": window_rows,
        "dropped_duplicate_rows": dropped,
        "kept_rows": total_rows - dropped,
        "column_count_rows": column_count_rows,
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
        "tolerance_seconds": tolerance_seconds,
    }
    return cleaned, report
