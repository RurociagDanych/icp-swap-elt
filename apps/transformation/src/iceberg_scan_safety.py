"""Thread-safety guard for PyIceberg 0.8.1 scan planning."""

from __future__ import annotations

import functools
import threading

from pyiceberg.expressions.visitors import _InclusiveMetricsEvaluator

# DataScan.plan_files builds ONE _InclusiveMetricsEvaluator and evaluates every manifest on a
# thread pool. The evaluator keeps the bounds / counts of the file it is testing in INSTANCE
# attributes, so concurrent manifests overwrite each other's bounds and a file is intermittently
# pruned from the plan. For the event_id lookup that means the data file written by the
# previous object can be missing, and its rows would be stored twice. Only the CPU-cheap
# in-memory eval is serialized; manifest I/O stays parallel.
_MARK = "_icp_swap_thread_safe_eval"
_EVAL_LOCK = threading.RLock()


def install() -> None:
    """Wrap the evaluator in a lock; idempotent, and fails closed at import if internals change."""
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
