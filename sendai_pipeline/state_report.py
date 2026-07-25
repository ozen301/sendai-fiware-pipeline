"""Presentation helpers for pipeline state reports."""

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from sendai_pipeline.metadata import MetadataLoadError, SensorPlace, load_metadata
from sendai_pipeline.state_tools import (
    StateDoctorReport,
    TargetIssueSummary,
    WindowDiagnosis,
)


@dataclass(frozen=True)
class SensorLabel:
    """Display label for a target from current sensor metadata."""

    entity_id: str
    place_number: int
    batch: str
    interval_min: int


def load_sensor_labels(path: Path) -> dict[str, SensorLabel]:
    """Load current sensor metadata for display enrichment."""
    return _sensor_labels(load_metadata(path))


def try_load_sensor_labels(path: Path) -> dict[str, SensorLabel]:
    """Best-effort metadata loader for read-only pretty output."""
    try:
        return load_sensor_labels(path)
    except (FileNotFoundError, MetadataLoadError, OSError, ValueError):
        return {}


def _sensor_labels(places: Iterable[SensorPlace]) -> dict[str, SensorLabel]:
    return {
        place.entity_id: SensorLabel(
            entity_id=place.entity_id,
            place_number=place.place_number,
            batch=place.batch,
            interval_min=place.interval_min,
        )
        for place in places
    }


def state_report_to_json(report: StateDoctorReport) -> str:
    """Serialize the full doctor report for CLI output."""
    data = {
        "product": report.product,
        "status_counts": report.status_counts,
        "total_windows": sum(report.status_counts.values()),
        "open_window_count": len(report.open_windows),
        "open_windows": [_diagnosis_to_row(item) for item in report.open_windows],
        "failed_targets": [
            _target_issue_to_row(item) for item in report.failed_targets
        ],
        "failed_http_status_counts": [
            {"http_status": http_status, "count": count}
            for http_status, count in report.failed_http_status_counts
        ],
    }
    return json.dumps(data, indent=2, sort_keys=True)


def state_report_to_pretty(
    report: StateDoctorReport,
    *,
    state_path: Path,
    state_size_bytes: int | None,
    sensor_labels: Mapping[str, SensorLabel],
    top: int | None,
    window_limit: int | None,
    ascii_only: bool,
) -> str:
    """Render a human-readable state doctor dashboard.

    Args:
        report: Report to render.
        state_path: State file path, shown in the header line.
        state_size_bytes: State file size in bytes, or ``None`` if unknown;
            rendered as "missing" in that case.
        sensor_labels: Metadata labels keyed by entity ID, used to annotate
            failed-target rows for products other than ``"direction"``.
        top: Maximum failed-target rows to show; ``None`` shows all.
        window_limit: Maximum open-window rows to show; ``None`` shows all.
        ascii_only: Use plain ASCII markers in the status overview bar
            instead of block-drawing characters.

    Returns:
        The rendered report as a multi-line string.
    """
    lines: list[str] = [
        f"State doctor: {report.product}",
        f"State file: {state_path} ({_format_size(state_size_bytes)})",
        f"Windows: {sum(report.status_counts.values())} retained, "
        f"{len(report.open_windows)} open",
        "",
        "Status overview",
    ]
    lines.extend(_status_overview_lines(report.status_counts, ascii_only=ascii_only))
    open_windows = _limited(report.open_windows, window_limit)
    lines.extend(["", "Open windows"])
    lines.extend(
        _table_lines(
            (
                "window",
                "status",
                "int",
                "ok",
                "fail",
                "retry",
            ),
            [
                (
                    item.window_key,
                    item.status,
                    str(item.interval_min),
                    str(item.ok_count),
                    str(item.failed_count),
                    "yes" if item.retry_reachable else "no",
                )
                for item in open_windows
            ],
        )
    )
    lines.extend(
        _hidden_hint(
            label="open windows",
            total=len(report.open_windows),
            shown=len(open_windows),
        )
    )

    failed_targets = _limited(report.failed_targets, top)
    if report.product == "direction":
        lines.extend(["", "Aggregate target failures"])
        lines.extend(_aggregate_target_issue_table_lines(failed_targets))
    else:
        lines.extend(["", _target_section_heading("failed", top)])
        lines.extend(
            _target_issue_table_lines(
                failed_targets,
                sensor_labels=sensor_labels,
            )
        )
    lines.extend(
        _hidden_hint(
            label="failed targets",
            total=len(report.failed_targets),
            shown=len(failed_targets),
        )
    )
    lines.extend(["", "Failed HTTP statuses"])
    lines.extend(_http_status_table_lines(report.failed_http_status_counts))
    return "\n".join(lines)


def _diagnosis_to_row(item: WindowDiagnosis) -> dict[str, object]:
    return {
        "window": item.window_key,
        "status": item.status,
        "interval_min": item.interval_min,
        "first_seen": item.first_seen.isoformat(),
        "source_window_start": item.source_window_start.isoformat(),
        "source_window_end": item.source_window_end.isoformat(),
        "expected_target_source": item.expected_target_source,
        "target_status_category": item.target_status_category,
        "target_count": item.target_count,
        "ok_count": item.ok_count,
        "failed_count": item.failed_count,
        "failed_http_statuses": item.failed_http_statuses,
        "failed_target_ids": item.failed_target_ids,
        "failed_target_http_statuses": item.failed_target_http_statuses,
        "retry_reachable": item.retry_reachable,
    }


def _target_issue_to_row(item: TargetIssueSummary) -> dict[str, object]:
    return {
        "entity_id": item.entity_id,
        "count": item.count,
        "oldest_window": item.oldest_window,
        "newest_window": item.newest_window,
    }


def _status_overview_lines(
    status_counts: Mapping[str, int],
    *,
    ascii_only: bool,
) -> list[str]:
    total = sum(status_counts.values())
    markers = (
        {
            "complete": "C",
            "partial": "P",
            "pending": "N",
            "dead_letter": "D",
            "unknown": "U",
        }
        if ascii_only
        else {
            "complete": "█",
            "partial": "▒",
            "pending": "◆",
            "dead_letter": "×",
            "unknown": "?",
        }
    )
    segments = _stacked_segments(status_counts, total=total, width=64)
    lines = ["[" + "".join(markers[status] * count for status, count in segments) + "]"]
    for status, count in status_counts.items():
        percentage = "0.0%" if total <= 0 else f"{count / total * 100:.1f}%"
        lines.append(
            f"{markers.get(status, 'U')} {status:<11} {count:>5} {percentage:>6}"
        )
    return lines


def _stacked_segments(
    status_counts: Mapping[str, int],
    *,
    total: int,
    width: int,
) -> list[tuple[str, int]]:
    if total <= 0:
        return [(status, 0) for status in status_counts]

    raw_segments = [
        (status, width * count / total) for status, count in status_counts.items()
    ]
    segments = [(status, int(raw_count)) for status, raw_count in raw_segments]
    allocated = sum(count for _status, count in segments)
    remainder = width - allocated
    fractions = sorted(
        (
            (raw_count - int(raw_count), status)
            for status, raw_count in raw_segments
            if status_counts[status] > 0
        ),
        reverse=True,
    )
    counts_by_status = dict(segments)
    for _fraction, status in fractions[:remainder]:
        counts_by_status[status] += 1
    return [(status, counts_by_status[status]) for status in status_counts]


def _table_lines(
    headers: tuple[str, ...],
    rows: Iterable[tuple[str, ...]],
) -> list[str]:
    materialized = list(rows)
    if not materialized:
        return ["(none)"]
    widths = [
        max(len(headers[index]), *(len(row[index]) for row in materialized))
        for index in range(len(headers))
    ]
    lines = [_format_table_row(headers, widths)]
    lines.append(_format_table_row(tuple("-" * width for width in widths), widths))
    lines.extend(_format_table_row(row, widths) for row in materialized)
    return lines


def _format_table_row(row: tuple[str, ...], widths: list[int]) -> str:
    return "  ".join(value.ljust(widths[index]) for index, value in enumerate(row))


def _target_issue_table_lines(
    issues: Iterable[TargetIssueSummary],
    *,
    sensor_labels: Mapping[str, SensorLabel],
) -> list[str]:
    return _table_lines(
        ("target", "place", "batch", "int", "count", "oldest", "newest"),
        [
            (
                item.entity_id,
                _label_field(item.entity_id, sensor_labels, "place_number"),
                _label_field(item.entity_id, sensor_labels, "batch"),
                _label_field(item.entity_id, sensor_labels, "interval_min"),
                str(item.count),
                item.oldest_window,
                item.newest_window,
            )
            for item in issues
        ],
    )


def _aggregate_target_issue_table_lines(
    issues: Iterable[TargetIssueSummary],
) -> list[str]:
    """Render aggregate target failures without per-place metadata columns."""
    return _table_lines(
        ("target", "count", "oldest", "newest"),
        [
            (
                item.entity_id,
                str(item.count),
                item.oldest_window,
                item.newest_window,
            )
            for item in issues
        ],
    )


def _http_status_table_lines(status_counts: Iterable[tuple[int, int]]) -> list[str]:
    return _table_lines(
        ("http_status", "count"),
        [(str(http_status), str(count)) for http_status, count in status_counts],
    )


def _limited[T](items: tuple[T, ...], limit: int | None) -> tuple[T, ...]:
    if limit is None:
        return items
    return items[:limit]


def _target_section_heading(issue_name: str, limit: int | None) -> str:
    if limit is None:
        return f"{issue_name.title()} targets"
    return f"Top {issue_name} targets (limit {limit})"


def _hidden_hint(*, label: str, total: int, shown: int) -> list[str]:
    hidden = total - shown
    if hidden <= 0:
        return []
    return [f"... {hidden} more {label} hidden; rerun with --all to show all rows."]


def _label_field(
    entity_id: str,
    sensor_labels: Mapping[str, SensorLabel],
    field: Literal["place_number", "batch", "interval_min"],
) -> str:
    label = sensor_labels.get(entity_id)
    if label is None:
        return "-"
    return str(getattr(label, field))


def _format_size(size_bytes: int | None) -> str:
    if size_bytes is None:
        return "missing"
    value = float(size_bytes)
    for unit in ("B", "KiB", "MiB", "GiB"):
        if value < 1024 or unit == "GiB":
            if unit == "B":
                return f"{int(value)} {unit}"
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{value:.1f} GiB"
