"""Bounded retry helpers shared by the fact and token_dim commit loops."""

from __future__ import annotations

from collections.abc import Callable


def backoff_delay(
    attempt: int, *, base: float, cap: float, jitter: Callable[[], float]
) -> float:
    """Delay before retry ``attempt`` (0-based): equal-jitter exponential backoff.

    Optimistic Iceberg commits fail when another writer committed first; retrying at once makes
    concurrent workers re-plan together. The delay is uniform in ``[ceiling / 2, ceiling]`` with
    ``ceiling = min(cap, base * 2**attempt)``, so writers de-synchronise but still wait half a step.
    """
    if attempt < 0 or base < 0 or cap < 0:
        raise ValueError("backoff parameters must be nonnegative")
    ceiling = min(cap, base * (2**attempt))
    return ceiling / 2 + ceiling / 2 * min(1.0, max(0.0, jitter()))
