from datetime import datetime
from pathlib import Path

from sendai_pipeline.revision_sweep import (
    RevisionWorkItem,
    revision_retry_items,
    split_discovered_revisions,
)
from sendai_pipeline.state import WindowStateStore
from sendai_pipeline.windowing import JST


def _item(
    startdate: str,
    aggregated_at: datetime,
    *,
    interval_min: int = 60,
) -> RevisionWorkItem:
    prefix = "per300" if interval_min == 5 else "per3600"
    return RevisionWorkItem(
        interval_min=interval_min,
        startdate=startdate,
        window_key=f"{prefix}/{startdate}",
        aggregated_at=aggregated_at,
    )


def test_split_discovered_revisions_keeps_cursor_second_together() -> None:
    first_second = datetime(2026, 7, 24, 12, 0, 1, tzinfo=JST)
    second_second = datetime(2026, 7, 24, 12, 0, 2, tzinfo=JST)
    discovered = [
        _item("20260724_0900", first_second),
        _item("20260724_1000", first_second),
        _item("20260724_1100", second_second),
    ]

    selected, deferred = split_discovered_revisions(discovered, 1)

    assert selected == discovered[:2]
    assert deferred == discovered[2:]


def test_split_discovered_revisions_returns_all_below_cap() -> None:
    discovered = [
        _item(
            "20260724_0900",
            datetime(2026, 7, 24, 12, 0, 1, tzinfo=JST),
        )
    ]

    assert split_discovered_revisions(discovered, 2) == (discovered, [])


def test_revision_retry_items_respects_available_intervals_and_exclusions(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 7, 24, 12, 0, tzinfo=JST)
    store = WindowStateStore(tmp_path / "state.json", now=lambda: now)
    for window_key, interval_min in (
        ("per3600/20260720_0900", 60),
        ("per3600/20260720_1000", 60),
        ("per300/20260720_1000", 5),
    ):
        store.begin_window_attempt(
            window_key,
            interval_min=interval_min,
            expected_target_ids=["target"],
        )

    items = revision_retry_items(
        store,
        startdate_upper_by_interval={
            60: datetime(2026, 7, 21, 0, 0, tzinfo=JST),
        },
        excluded_window_keys={"per3600/20260720_1000"},
        limit=10,
    )

    assert items == [
        RevisionWorkItem(
            interval_min=60,
            startdate="20260720_0900",
            window_key="per3600/20260720_0900",
            retry=True,
        )
    ]


def test_revision_retry_items_stops_at_limit(tmp_path: Path) -> None:
    now = datetime(2026, 7, 24, 12, 0, tzinfo=JST)
    store = WindowStateStore(tmp_path / "state.json", now=lambda: now)
    for window_key in (
        "per3600/20260720_0900",
        "per3600/20260720_1000",
    ):
        store.begin_window_attempt(
            window_key,
            interval_min=60,
            expected_target_ids=["target"],
        )

    items = revision_retry_items(
        store,
        startdate_upper_by_interval={
            60: datetime(2026, 7, 21, 0, 0, tzinfo=JST),
        },
        excluded_window_keys=set(),
        limit=1,
    )

    assert [item.window_key for item in items] == ["per3600/20260720_0900"]


def test_revision_retry_items_returns_empty_when_limit_is_zero(tmp_path: Path) -> None:
    store = WindowStateStore(tmp_path / "state.json")

    assert (
        revision_retry_items(
            store,
            startdate_upper_by_interval={},
            excluded_window_keys=set(),
            limit=0,
        )
        == []
    )
