"""Pure raw-object routing helpers (no boto3 / pyarrow / pyiceberg).

Kept dependency-free so they are unit-testable without the heavy container deps.
``silver_job`` imports from here. Deduplication is NOT planned here: the only dedup key is
``silver_contract.silver_identity`` (``event_id``), applied against the fact table by
``silver_persistence``.
"""
from __future__ import annotations


def incremental_prefix(raw_prefix: str, target_iso: str) -> str:
    """S3 prefix for one day of the ongoing ``rest/incremental`` feed.

    The feed lands in run-timestamped folders — ``rest/incremental/<date>T<time>/`` (e.g.
    ``2026-06-22T14-46-53-217Z/``) — so the prefix must be the bare date with **no trailing
    slash**: a calendar date is always followed by ``T``, so ``...incremental/2026-06-22``
    prefix-matches that day's folder(s) and only that day's. (A trailing slash matched nothing.)
    """
    return f"{raw_prefix}rest/incremental/{target_iso}"


def is_backfill_object(key: str) -> bool:
    """True for one-time historical bulk-export files (``transactions_backfill_*.csv``).

    The REST ingestion stage drops the *entire* historical backfill into the SAME run-timestamp
    folder as that day's ongoing incremental file (``rest/incremental/<date>T..Z/`` holds the
    backfill files alongside one small ``transactions_incremental_*.csv``).
    The daily run lists by date prefix and so sweeps the whole folder — it must skip these:
    they are loaded once by the backfill state machine and are far too large for the daily
    Lambda.
    """
    name = key.rsplit("/", 1)[-1].lower()
    return name.startswith("transactions_backfill")
