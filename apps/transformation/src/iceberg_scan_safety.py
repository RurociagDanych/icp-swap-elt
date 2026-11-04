"""Thread-safety guard for PyIceberg 0.8.1 scan planning.

``DataScan.plan_files`` builds ONE ``_InclusiveMetricsEvaluator`` and then evaluates every
manifest on a thread pool (``ExecutorFactory``). The evaluator keeps the bounds / counts of the
file it is currently testing in INSTANCE attributes (``eval`` assigns them, the ``visit_*``
methods read them), so two manifests evaluated concurrently overwrite each other's bounds: a
file is then judged against another file's min/max and is intermittently pruned from (or
wrongly added to) the plan. For the ``event_id`` lookup that means the data file written by
the previous object can silently be missing from the next object's lookup, and the rows it
holds would be stored twice.

The guard serializes only the CPU-cheap in-memory ``eval`` call behind one lock; manifest I/O,
the slow part, stays parallel. ``install()`` is idempotent and fails closed (``ImportError`` /
``AttributeError`` at import time) if the PyIceberg internals it relies on ever change.
"""

from __future__ import annotations

import functools
import threading

from pyiceberg.expressions.visitors import _InclusiveMetricsEvaluator

_MARK = "_icp_swap_thread_safe_eval"
_EVAL_LOCK = threading.RLock()


def install() -> None:
    original = _InclusiveMetricsEvaluator.eval
    if getattr(original, _MARK, False):
        return

    @functools.wraps(original)
    def locked_eval(self, file):
        with _EVAL_LOCK:
            return original(self, file)

    setattr(locked_eval, _MARK, True)
    _InclusiveMetricsEvaluator.eval = locked_eval


install()
