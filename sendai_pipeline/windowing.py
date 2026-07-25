"""Shared source-window identity and formatting helpers."""

from datetime import datetime, timedelta, timezone
from typing import Any

JST = timezone(timedelta(hours=9))

_INTERVAL_PREFIXES: dict[int, str] = {5: "per300", 60: "per3600"}
_PREFIX_INTERVALS: dict[str, int] = {
    prefix: interval for interval, prefix in _INTERVAL_PREFIXES.items()
}


def floor_datetime(value: datetime, interval_min: int) -> datetime:
    """Round a datetime down to the nearest interval boundary."""
    return value - timedelta(
        minutes=value.minute % interval_min,
        seconds=value.second,
        microseconds=value.microsecond,
    )


def coerce_jst_datetime(value: datetime) -> datetime:
    """Return ``value`` as a JST-aware datetime.

    Attaches JST when ``value`` is naive; converts when ``value`` is already
    timezone-aware.
    """
    if value.tzinfo is None:
        return value.replace(tzinfo=JST)
    return value.astimezone(JST)


def eligible_source_cutoff(
    run_started_at: datetime,
    interval_min: int,
    *,
    source_stability_delay_hours: int,
) -> datetime:
    """Return the newest source window eligible for publication."""
    return floor_datetime(
        run_started_at - timedelta(hours=source_stability_delay_hours),
        interval_min,
    )


def window_key(interval_min: int, startdate: str) -> str:
    """Return the state key for a supported source aggregation window.

    Args:
        interval_min: Aggregation interval in minutes.
        startdate: Source window timestamp in ``YYYYMMDD_HHMM`` format.

    Returns:
        State key such as ``"per300/20260724_1200"``.

    Raises:
        ValueError: If ``interval_min`` is not 5 or 60.
    """
    prefix = _INTERVAL_PREFIXES.get(interval_min)
    if prefix is None:
        raise ValueError(f"unsupported aggregation interval: {interval_min}")
    return f"{prefix}/{startdate}"


def parse_window_key(value: str) -> tuple[int, str] | None:
    """Return the ``(interval_min, startdate)`` pair encoded in a state key.

    Returns:
        The parsed pair, or ``None`` if ``value`` has no ``"/"`` separator or
        its prefix is not a recognized aggregation interval.
    """
    prefix, separator, startdate = value.partition("/")
    if not separator:
        return None
    interval = _PREFIX_INTERVALS.get(prefix)
    if interval is None:
        return None
    return interval, startdate


def parse_source_window_start(startdate: str) -> datetime:
    """Parse a MySQL source-window key into a JST datetime."""
    return datetime.strptime(startdate, "%Y%m%d_%H%M").replace(tzinfo=JST)


def format_sql_window_bound(value: datetime) -> str:
    """Format a source-window timestamp for the MySQL ``startdate`` column."""
    return value.strftime("%Y%m%d_%H%M")


def format_mysql_timestamp(value: datetime) -> str:
    """Format a revision cursor for MySQL ``aggregated_at`` comparison."""
    return (
        coerce_jst_datetime(value).replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
    )


def parse_revision_aggregated_at(value: Any) -> datetime:
    """Normalize a driver datetime or MySQL timestamp string to JST seconds."""
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value)
        if "T" in text:
            parsed = datetime.fromisoformat(text)
        else:
            parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
    return coerce_jst_datetime(parsed).replace(microsecond=0)
