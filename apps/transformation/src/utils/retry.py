"""Bounded retry helpers shared by the fact and the token_dim commit loops.

Optimistic Iceberg commits fail with ``CommitFailedException`` when another writer committed
first. Retrying immediately makes every concurrent backfill worker re-plan (and re-scan) at
the same moment; an exponential delay with jitter spreads the retries out. Every caller keeps
its own hard bounds (attempt count, total sleep, deadline) so a retry storm still fails closed.
"""
from __future__ import annotations

from collections.abc import Callable


def backoff_delay(
    attempt: int, *, base: float, cap: float, jitter: Callable[[], float]
) -> float:
    """Delay before retry number ``attempt`` (0-based): "equal jitter" exponential backoff.

    ``ceiling = min(cap, base * 2**attempt)``; the delay is uniform in
    ``[ceiling / 2, ceiling]`` (``jitter()`` returns a float in ``[0, 1]``), so concurrent
    writers de-synchronise while every retry still waits at least half its step.
    """
    if attempt < 0 or base < 0 or cap < 0:
        raise ValueError("backoff parameters must be nonnegative")
    ceiling = min(cap, base * (2**attempt))
    return ceiling / 2 + ceiling / 2 * min(1.0, max(0.0, jitter()))
