from datetime import UTC, datetime, timedelta, timezone

import pytest

from sendai_pipeline.windowing import (
    JST,
    coerce_jst_datetime,
    eligible_source_cutoff,
    floor_datetime,
    format_mysql_timestamp,
    format_sql_window_bound,
    parse_revision_aggregated_at,
    parse_source_window_start,
    parse_window_key,
    window_key,
)


def test_floor_datetime_rounds_down_to_interval_boundary() -> None:
    value = datetime(2026, 7, 24, 12, 7, 59, 123456, tzinfo=JST)

    assert floor_datetime(value, 5) == datetime(2026, 7, 24, 12, 5, tzinfo=JST)


def test_coerce_jst_datetime_attaches_jst_to_naive_value() -> None:
    value = datetime(2026, 7, 24, 12, 0)

    assert coerce_jst_datetime(value) == value.replace(tzinfo=JST)


def test_coerce_jst_datetime_converts_aware_value_to_jst() -> None:
    source_timezone = timezone(timedelta(hours=-4))
    value = datetime(2026, 7, 23, 23, 0, tzinfo=source_timezone)

    assert coerce_jst_datetime(value) == datetime(2026, 7, 24, 12, 0, tzinfo=JST)


@pytest.mark.parametrize(
    ("interval_min", "expected"),
    [
        (5, "per300/20260724_1200"),
        (60, "per3600/20260724_1200"),
    ],
)
def test_window_key_formats_supported_intervals(
    interval_min: int,
    expected: str,
) -> None:
    assert window_key(interval_min, "20260724_1200") == expected


def test_window_key_rejects_unsupported_interval() -> None:
    with pytest.raises(ValueError, match="unsupported aggregation interval"):
        window_key(15, "20260724_1200")


def test_format_sql_window_bound_preserves_source_format() -> None:
    value = datetime(2026, 7, 24, 12, 5, 59, tzinfo=JST)

    assert format_sql_window_bound(value) == "20260724_1205"


def test_parse_window_key_returns_interval_and_startdate() -> None:
    assert parse_window_key("per3600/20260724_1200") == (60, "20260724_1200")


@pytest.mark.parametrize("value", ["missing-separator", "per900/20260724_1200"])
def test_parse_window_key_rejects_unknown_shape(value: str) -> None:
    assert parse_window_key(value) is None


def test_parse_source_window_start_attaches_jst() -> None:
    assert parse_source_window_start("20260724_1205") == datetime(
        2026,
        7,
        24,
        12,
        5,
        tzinfo=JST,
    )


def test_eligible_source_cutoff_applies_delay_then_floors() -> None:
    run_started_at = datetime(2026, 7, 24, 12, 17, 59, tzinfo=JST)

    assert eligible_source_cutoff(
        run_started_at,
        60,
        source_stability_delay_hours=3,
    ) == datetime(2026, 7, 24, 9, 0, tzinfo=JST)


def test_format_mysql_timestamp_normalizes_to_jst_and_seconds() -> None:
    value = datetime(
        2026,
        7,
        24,
        3,
        17,
        43,
        123456,
        tzinfo=UTC,
    )

    assert format_mysql_timestamp(value) == "2026-07-24 12:17:43"


@pytest.mark.parametrize(
    "value",
    [
        datetime(2026, 7, 24, 12, 17, 43, 123456),
        "2026-07-24 12:17:43",
        "2026-07-24T12:17:43",
    ],
)
def test_parse_revision_aggregated_at_normalizes_driver_shapes(value: object) -> None:
    assert parse_revision_aggregated_at(value) == datetime(
        2026,
        7,
        24,
        12,
        17,
        43,
        tzinfo=JST,
    )
