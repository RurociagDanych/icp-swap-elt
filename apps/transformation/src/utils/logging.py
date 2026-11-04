"""Shared structured (JSON) logging utility.

Emits one JSON object per log line to stdout so CloudWatch Logs Insights can query fields
directly. Import ``get_logger`` everywhere instead of configuring logging ad hoc.
"""
from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any


class JsonFormatter(logging.Formatter):
    """Format log records as single-line JSON."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        # Merge any structured fields passed via logger.info("msg", extra={"extra_fields": {...}}).
        extra = getattr(record, "extra_fields", None)
        if isinstance(extra, dict):
            payload.update(extra)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def get_logger(name: str) -> logging.Logger:
    """Return a configured JSON logger.

    Args:
        name: Logger name, typically ``__name__``.

    Returns:
        A logger writing single-line JSON to stdout at the level from ``LOG_LEVEL`` (default INFO).
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(os.environ.get("LOG_LEVEL", "INFO").upper())
        logger.propagate = False
    return logger


def log_event(logger: logging.Logger, level: int, message: str, **fields: Any) -> None:
    """Log ``message`` at ``level`` with arbitrary structured ``fields``."""
    logger.log(level, message, extra={"extra_fields": fields})
