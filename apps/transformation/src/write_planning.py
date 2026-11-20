"""Pure raw-object routing helpers (no boto3 / pyarrow / pyiceberg), unit-testable without the container deps."""

from __future__ import annotations


def incremental_prefix(raw_prefix: str, target_iso: str) -> str:
    """S3 prefix for one day of the ongoing ``rest/incremental`` feed.

    The feed lands in run-timestamped folders (``rest/incremental/<date>T<time>/``), so the
    prefix must be the bare date with NO trailing slash: a date is always followed by ``T``, so
    it matches only that day's folder(s). A trailing slash matched nothing.
    """
    return f"{raw_prefix}rest/incremental/{target_iso}"


def is_backfill_object(key: str) -> bool:
    """True for one-time historical bulk-export files (``transactions_backfill_*.csv``).

    The REST ingestion stage drops the entire historical backfill into the same run-timestamp
    folder as that day's incremental file, so the daily date-prefix listing sweeps it too. It
    must be skipped: the backfill state machine loads it once and it is too large for the
    daily Lambda.
    """
    name = key.rsplit("/", 1)[-1].lower()
    return name.startswith("transactions_backfill")
