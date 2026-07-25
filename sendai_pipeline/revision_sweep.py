"""Shared revision-sweep work selection."""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime

from sendai_pipeline.state import WindowStateStore
from sendai_pipeline.windowing import parse_window_key


@dataclass(frozen=True)
class RevisionWorkItem:
    """One discovered or retained revision-sweep source window.

    Attributes:
        interval_min: Aggregation interval in minutes.
        startdate: Source window start in ``YYYYMMDD_HHMM`` format.
        window_key: Stable state key for this source window.
        aggregated_at: Source aggregation timestamp read from the database
            row. Set for freshly discovered items; ``None`` for items from
            ``revision_retry_items``, which carry forward an old open window
            instead of reading a fresh row.
        retry: Whether this item was carried forward by
            ``revision_retry_items`` rather than freshly discovered.
    """

    interval_min: int
    startdate: str
    window_key: str
    aggregated_at: datetime | None = None
    retry: bool = False


def split_discovered_revisions(
    discovered: list[RevisionWorkItem],
    max_windows: int,
) -> tuple[list[RevisionWorkItem], list[RevisionWorkItem]]:
    """Cap discovered items at ``max_windows`` without splitting one second.

    Splitting is avoided by keeping every item that shares the boundary
    item's ``aggregated_at`` together, even past the cap.

    Args:
        discovered: Discovered work items, sorted by ascending
            ``aggregated_at``.
        max_windows: Soft cap on how many items to keep for this run.

    Returns:
        ``(kept, deferred)``: ``kept`` is the items to process this run;
        ``deferred`` is the rest, left for a later run.
    """
    if len(discovered) <= max_windows:
        return discovered, []

    boundary = discovered[max_windows - 1].aggregated_at
    split_at = max_windows
    while split_at < len(discovered) and discovered[split_at].aggregated_at == boundary:
        split_at += 1
    return discovered[:split_at], discovered[split_at:]


def revision_retry_items(
    state_store: WindowStateStore,
    *,
    startdate_upper_by_interval: Mapping[int, datetime],
    excluded_window_keys: set[str],
    limit: int,
) -> list[RevisionWorkItem]:
    """Return old open windows that rolling lookback can no longer reach.

    Args:
        state_store: State store to read open (pending/partial) windows from.
        startdate_upper_by_interval: Per-interval exclusive upper bound on
            source window start. A window at or after its interval's bound
            is still reachable by the normal rolling lookback and is
            skipped; only older windows are returned.
        excluded_window_keys: Window keys to skip, e.g. windows already
            selected by discovery in the same run.
        limit: Maximum number of items to return. Returns an empty list when
            ``limit`` is 0 or negative.

    Returns:
        Retry work items (``retry=True``), in the same order as
        ``state_store``'s open windows, capped at ``limit``.
    """
    if limit <= 0:
        return []

    items: list[RevisionWorkItem] = []
    for window_key, window in state_store.iter_open_windows():
        if window_key in excluded_window_keys:
            continue
        parsed_key = parse_window_key(window_key)
        if parsed_key is None:
            continue
        interval_min, startdate = parsed_key
        startdate_upper = startdate_upper_by_interval.get(interval_min)
        if startdate_upper is None:
            continue
        if state_store.source_window_start(window_key, window) >= startdate_upper:
            continue
        items.append(
            RevisionWorkItem(
                interval_min=interval_min,
                startdate=startdate,
                window_key=window_key,
                retry=True,
            )
        )
        if len(items) >= limit:
            break
    return items
