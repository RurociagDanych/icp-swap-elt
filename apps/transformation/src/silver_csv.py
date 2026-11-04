"""Real standard-library CSV boundary for flat-Silver normalization."""

from __future__ import annotations

import csv
import io
from collections.abc import Callable
from typing import Any

from silver_contract import normalize_silver

LineageFactory = Callable[[int, dict[str, Any]], dict[str, Any]]
ObservationSink = Callable[
    [int, int, dict[str, Any], list[dict[str, Any]], dict[str, Any] | None], None
]


def normalize_csv(
    content: str | bytes,
    dataset: str,
    lineage_for_row: LineageFactory,
    *,
    max_records: int | None = None,
    observation_sink: ObservationSink | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Parse CSV text/bytes and normalize each complete data row without cloud dependencies.

    The lineage callback receives a one-based logical CSV record ordinal (excluding the header)
    and the raw string-valued row. Issue ``line`` fields remain ending physical line numbers.
    """
    issues: list[dict[str, Any]] = []
    records: list[dict[str, Any]] = []
    if isinstance(content, bytes):
        try:
            content = content.decode("utf-8")
        except UnicodeDecodeError as exc:
            return [], [{"line": 0, "code": "invalid_utf8", "detail": str(exc)}]

    reader = csv.DictReader(io.StringIO(content, newline=""), strict=True)
    try:
        fieldnames = reader.fieldnames
        if not fieldnames:
            return [], [{"line": 1, "code": "missing_header"}]

        seen: set[str] = set()
        header_issues: list[dict[str, Any]] = []
        for column, raw_name in enumerate(fieldnames, start=1):
            name = raw_name.strip()
            if not name:
                header_issues.append(
                    {"line": 1, "code": "blank_header", "column": column}
                )
            elif name in seen:
                header_issues.append(
                    {
                        "line": 1,
                        "code": "duplicate_header",
                        "field": name,
                        "column": column,
                    }
                )
            else:
                seen.add(name)
        if header_issues:
            return [], header_issues

        data_rows_seen = 0
        for ordinal, raw in enumerate(reader, start=1):
            data_rows_seen += 1
            if max_records is not None and ordinal > max_records:
                return [], [{"line": reader.line_num, "code": "row_limit_exceeded"}]
            line = reader.line_num
            if None in raw or any(value is None for value in raw.values()):
                row_issue = {"line": line, "code": "column_count"}
                issues.append(row_issue)
                if observation_sink:
                    observation_sink(
                        ordinal,
                        line,
                        {
                            "columns": list(fieldnames),
                            "values": [raw.get(name) for name in fieldnames],
                            "extra_columns": raw.get(None, []),
                        },
                        [row_issue],
                        None,
                    )
                continue
            row = {
                str(key).strip(): value for key, value in raw.items() if key is not None
            }
            try:
                lineage = lineage_for_row(ordinal, row)
            except Exception as exc:  # caller boundary: keep other valid source rows
                issues.append(
                    {"line": line, "code": "lineage_unresolved", "detail": str(exc)}
                )
                continue
            record, row_issues = normalize_silver(row, dataset, lineage)
            issues.extend({"line": line, **issue} for issue in row_issues)
            if observation_sink:
                observation_sink(ordinal, line, row, row_issues, record)
            if record is not None:
                records.append(record)
    except csv.Error as exc:
        issues.append(
            {"line": reader.line_num, "code": "malformed_csv", "detail": str(exc)}
        )
    else:
        if data_rows_seen == 0:
            issues.append({"line": 1, "code": "header_only"})
    return records, issues
