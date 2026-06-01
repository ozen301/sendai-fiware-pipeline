"""Operator tools for inspecting and repairing window state."""

import fcntl
import json
import logging
import os
import shutil
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Literal, cast

from sendai_pipeline.metadata import MetadataLoadError, SensorPlace, load_metadata
from sendai_pipeline.state import JST, StateValidationError, WindowStateStore

logger = logging.getLogger(__name__)

ProductName = Literal["flow", "direction"]
RepairAction = Literal["recompute_complete", "dead_letter"]
FlowMigrationAction = Literal["recomputed", "dropped"]
ExpectedTargetSource = Literal["stored", "derived"]

PRODUCT_STATE_PATHS: dict[ProductName, Path] = {
    "flow": Path("state/flow.json"),
    "direction": Path("state/direction.json"),
}
PRODUCT_LOCK_PATHS: dict[ProductName, Path] = {
    "flow": Path("state/flow.lock"),
    "direction": Path("state/direction.lock"),
}
_DEFAULT_MAX_LOOKBACK_HOURS: dict[int, int] = {5: 72, 60: 72}
_FLOW_MIGRATION_TARGET_STATUSES: frozenset[str] = frozenset({"ok", "failed", "pending"})


@dataclass(frozen=True)
class WindowDiagnosis:
    """Read-only diagnosis for one retained open window."""

    window_key: str
    status: str
    interval_min: int
    first_seen: datetime
    source_window_start: datetime
    source_window_end: datetime
    expected_target_source: ExpectedTargetSource
    target_status_category: str
    target_count: int
    ok_count: int
    failed_count: int
    failed_http_statuses: tuple[int, ...]
    failed_target_ids: tuple[str, ...]
    failed_target_http_statuses: tuple[int | None, ...]
    retry_reachable: bool


@dataclass(frozen=True)
class TargetIssueSummary:
    """Aggregate issue count for one target across open windows."""

    entity_id: str
    count: int
    oldest_window: str
    newest_window: str


@dataclass(frozen=True)
class StateDoctorReport:
    """Read-only report for retained state and open-window issues."""

    product: ProductName
    status_counts: dict[str, int]
    open_windows: tuple[WindowDiagnosis, ...]
    failed_targets: tuple[TargetIssueSummary, ...]
    failed_http_status_counts: tuple[tuple[int, int], ...]


@dataclass(frozen=True)
class SensorLabel:
    """Display label for a target from current sensor metadata."""

    entity_id: str
    place_number: int
    batch: str
    interval_min: int


@dataclass(frozen=True)
class RepairChange:
    """One state mutation planned or applied by repair."""

    window_key: str
    action: RepairAction
    before_status: str
    after_status: str
    reason: str | None = None


@dataclass(frozen=True)
class RepairResult:
    """Summary of a dry-run or applied repair."""

    product: ProductName
    dry_run: bool
    backup_path: Path | None
    changes: tuple[RepairChange, ...]


@dataclass(frozen=True)
class FlowMigrationChange:
    """One flow state migration mutation planned or applied.

    Attributes:
        window_key: Stable key for the migrated source window.
        action: Migration action taken for the window.
        before_status: Aggregate status before migration.
        after_status: Aggregate status after recompute, or ``None`` when
            the window is dropped.
        expected_target_ids: Derived target IDs recorded for the window.
    """

    window_key: str
    action: FlowMigrationAction
    before_status: str
    after_status: str | None
    expected_target_ids: tuple[str, ...]


@dataclass(frozen=True)
class FlowMigrationResult:
    """Summary of a dry-run or applied flow state migration.

    Attributes:
        product: Product name. This migration is always flow-only.
        dry_run: Whether the state file was left unchanged.
        backup_path: Timestamped pre-migration backup path, when applied.
        changes: Planned or applied per-window migration changes.
    """

    product: ProductName
    dry_run: bool
    backup_path: Path | None
    changes: tuple[FlowMigrationChange, ...]


def load_product_state(product: ProductName) -> WindowStateStore:
    """Load one product's default state store."""
    return WindowStateStore.load(PRODUCT_STATE_PATHS[product])


def diagnose_state(
    store: WindowStateStore,
    *,
    product: ProductName,
    now: datetime | None = None,
    max_lookback_hours_by_interval: Mapping[int, int] | None = None,
) -> list[WindowDiagnosis]:
    """Return deterministic diagnostics for retained open windows."""
    checked_at = now or datetime.now(JST)
    max_lookback = (
        dict(max_lookback_hours_by_interval)
        if max_lookback_hours_by_interval is not None
        else max_lookback_hours_from_env()
    )
    diagnoses = [
        _diagnose_window(
            store,
            window_key=window_key,
            window=window,
            now=checked_at,
            max_lookback_hours_by_interval=max_lookback,
        )
        for window_key, window in store.iter_open_windows()
    ]
    diagnoses.sort(key=lambda item: (item.source_window_start, item.window_key))
    logger.info(
        "state doctor reported",
        extra={
            "event": "state_doctor_reported",
            "product": product,
            "windows_seen": len(diagnoses),
        },
    )
    return diagnoses


def build_state_report(
    store: WindowStateStore,
    *,
    product: ProductName,
    now: datetime | None = None,
    max_lookback_hours_by_interval: Mapping[int, int] | None = None,
) -> StateDoctorReport:
    """Return retained-window summary and open-window diagnostics."""
    open_windows = tuple(
        diagnose_state(
            store,
            product=product,
            now=now,
            max_lookback_hours_by_interval=max_lookback_hours_by_interval,
        )
    )
    return StateDoctorReport(
        product=product,
        status_counts=retained_status_counts(store),
        open_windows=open_windows,
        failed_targets=_summarize_target_issues(
            open_windows,
            target_ids=lambda item: item.failed_target_ids,
        ),
        failed_http_status_counts=_failed_http_status_counts(open_windows),
    )


def retained_status_counts(store: WindowStateStore) -> dict[str, int]:
    """Return retained window counts by aggregate status."""
    counts = {
        "complete": 0,
        "partial": 0,
        "pending": 0,
        "dead_letter": 0,
        "unknown": 0,
    }
    windows = store.as_dict().get("windows", {})
    if not isinstance(windows, Mapping):
        counts["unknown"] += 1
        return counts
    for window in windows.values():
        status = window.get("status") if isinstance(window, Mapping) else None
        if status in counts and status != "unknown":
            counts[str(status)] += 1
        else:
            counts["unknown"] += 1
    return counts


def repair_state(
    *,
    product: ProductName,
    window_keys: Iterable[str],
    action: RepairAction,
    reason: str | None = None,
    expected_target_ids: Iterable[str] | None = None,
    apply: bool = False,
    state_path: Path | None = None,
    lock_path: Path | None = None,
) -> RepairResult:
    """Repair explicitly selected windows, dry-run by default."""
    selected = tuple(window_keys)
    if not selected:
        raise StateValidationError("repair requires at least one explicit window key")
    if action == "dead_letter" and not reason:
        raise StateValidationError("dead_letter repair requires a reason")

    resolved_state_path = state_path or PRODUCT_STATE_PATHS[product]
    resolved_lock_path = lock_path or PRODUCT_LOCK_PATHS[product]

    if not apply:
        store = WindowStateStore.load(resolved_state_path)
        changes = tuple(
            _plan_repair_change(
                store,
                window_key,
                action=action,
                reason=reason,
                explicit_expected_target_ids=expected_target_ids,
            )
            for window_key in selected
        )
        logger.info(
            "state repair dry-run",
            extra={
                "event": "state_repair_dry_run",
                "product": product,
                "windows_seen": len(changes),
            },
        )
        return RepairResult(product, dry_run=True, backup_path=None, changes=changes)

    resolved_lock_path.parent.mkdir(parents=True, exist_ok=True)
    with resolved_lock_path.open("a", encoding="utf-8") as lock_file:
        fcntl.flock(lock_file, fcntl.LOCK_EX)
        store = WindowStateStore.load(resolved_state_path)
        backup_path = _backup_state_file(resolved_state_path)
        changes = tuple(
            _apply_repair_change(
                store,
                window_key,
                action=action,
                reason=reason,
                explicit_expected_target_ids=expected_target_ids,
            )
            for window_key in selected
        )
        store.save()
        reloaded = WindowStateStore.load(resolved_state_path)
        _verify_repair_applied(reloaded, changes)

    logger.info(
        "state repair applied",
        extra={
            "event": "state_repair_applied",
            "product": product,
            "path": str(resolved_state_path),
            "backup_path": str(backup_path),
            "windows_seen": len(changes),
        },
    )
    return RepairResult(
        product,
        dry_run=False,
        backup_path=backup_path,
        changes=changes,
    )


def migrate_flow_state(
    *,
    apply: bool = False,
    state_path: Path | None = None,
    lock_path: Path | None = None,
) -> FlowMigrationResult:
    """Re-derive retained flow windows from recorded deliverable targets.

    Args:
        apply: When false, plan without writing. When true, mutate the flow
            state file under the product lock.
        state_path: Optional flow state file override.
        lock_path: Optional flow lock file override.

    Returns:
        Summary of planned or applied migration changes.

    Raises:
        StateValidationError: If backup, save, reload, or verification fails.
    """
    product: ProductName = "flow"
    resolved_state_path = state_path or PRODUCT_STATE_PATHS[product]
    resolved_lock_path = lock_path or PRODUCT_LOCK_PATHS[product]

    if not apply:
        store = WindowStateStore.load(resolved_state_path)
        window_count = len(_state_windows(store))
        changes = tuple(_plan_flow_migration_changes(store))
        logger.info(
            "flow state migration dry-run",
            extra={
                "event": "flow_state_migration_dry_run",
                "product": product,
                "windows_seen": window_count,
                **_flow_migration_log_counts(changes),
            },
        )
        return FlowMigrationResult(
            product,
            dry_run=True,
            backup_path=None,
            changes=changes,
        )

    resolved_lock_path.parent.mkdir(parents=True, exist_ok=True)
    with resolved_lock_path.open("a", encoding="utf-8") as lock_file:
        fcntl.flock(lock_file, fcntl.LOCK_EX)
        store = WindowStateStore.load(resolved_state_path)
        window_count = len(_state_windows(store))
        backup_path = _backup_state_file(resolved_state_path)
        changes = tuple(_apply_flow_migration_changes(store))
        store.save()
        reloaded = WindowStateStore.load(resolved_state_path)
        _verify_flow_migration_applied(reloaded, changes)

    logger.info(
        "flow state migration applied",
        extra={
            "event": "flow_state_migration_applied",
            "product": product,
            "path": str(resolved_state_path),
            "backup_path": str(backup_path),
            "windows_seen": window_count,
            **_flow_migration_log_counts(changes),
        },
    )
    return FlowMigrationResult(
        product,
        dry_run=False,
        backup_path=backup_path,
        changes=changes,
    )


def max_lookback_hours_from_env(
    env: Mapping[str, str] | None = None,
) -> dict[int, int]:
    """Return doctor retry-horizon limits from runtime environment."""
    values = os.environ if env is None else env
    return {
        5: _positive_int_env(values, "MAX_LOOKBACK_HOURS_PER300", 72),
        60: _positive_int_env(values, "MAX_LOOKBACK_HOURS_PER3600", 72),
    }


def _diagnose_window(
    store: WindowStateStore,
    *,
    window_key: str,
    window: dict[str, Any],
    now: datetime,
    max_lookback_hours_by_interval: Mapping[int, int],
) -> WindowDiagnosis:
    expected, expected_source = _diagnostic_expected_target_ids(window)
    targets = window.get("targets", {})
    if not isinstance(targets, dict):
        targets = {}
    ok_count = 0
    failed_count = 0
    failed_http_statuses: set[int] = set()
    failed_target_ids: list[str] = []
    failed_target_http_statuses: list[int | None] = []
    for entity_id in expected:
        target = targets.get(entity_id)
        if not isinstance(target, dict):
            continue
        status = target.get("status")
        if status == "ok":
            ok_count += 1
        elif status == "failed":
            failed_count += 1
            failed_target_ids.append(entity_id)
            http_status = target.get("last_http_status")
            if isinstance(http_status, int):
                failed_http_statuses.add(http_status)
                failed_target_http_statuses.append(http_status)
            else:
                failed_target_http_statuses.append(None)
    if expected and ok_count == len(expected):
        category = "all_ok"
    elif expected and failed_count == len(expected):
        category = "all_failed"
    else:
        category = "mixed"

    interval_min = int(window.get("interval_min") or _interval_from_key(window_key))
    source_start = store.source_window_start(window_key, window)
    source_end = _source_window_end(window, source_start, interval_min)
    retry_anchor = store.retry_anchor(window_key, window)
    max_lookback_hours = max_lookback_hours_by_interval[interval_min]
    return WindowDiagnosis(
        window_key=window_key,
        status=str(window.get("status", "")),
        interval_min=interval_min,
        first_seen=datetime.fromisoformat(str(window["first_seen"])),
        source_window_start=source_start,
        source_window_end=source_end,
        expected_target_source=expected_source,
        target_status_category=category,
        target_count=len(expected),
        ok_count=ok_count,
        failed_count=failed_count,
        failed_http_statuses=tuple(sorted(failed_http_statuses)),
        failed_target_ids=tuple(failed_target_ids),
        failed_target_http_statuses=tuple(failed_target_http_statuses),
        retry_reachable=(now - retry_anchor) <= timedelta(hours=max_lookback_hours),
    )


def _plan_repair_change(
    store: WindowStateStore,
    window_key: str,
    *,
    action: RepairAction,
    reason: str | None,
    explicit_expected_target_ids: Iterable[str] | None,
) -> RepairChange:
    window = _window(store, window_key)
    after = _planned_after_status(
        store,
        window_key,
        window,
        action=action,
        explicit_expected_target_ids=explicit_expected_target_ids,
    )
    return RepairChange(
        window_key=window_key,
        action=action,
        before_status=str(window.get("status", "")),
        after_status=after,
        reason=reason,
    )


def _apply_repair_change(
    store: WindowStateStore,
    window_key: str,
    *,
    action: RepairAction,
    reason: str | None,
    explicit_expected_target_ids: Iterable[str] | None,
) -> RepairChange:
    window = _window(store, window_key)
    before = str(window.get("status", ""))
    if action == "recompute_complete":
        expected = _repair_expected_target_ids(
            window_key,
            window,
            explicit_expected_target_ids=explicit_expected_target_ids,
        )
        if not _all_expected_targets_ok(window, expected):
            raise StateValidationError(
                f"cannot recompute non-all-ok window: {window_key}"
            )
        window["expected_target_ids"] = expected
        after = store.recompute_status(window_key, expected_target_ids=expected)
    else:
        after = "dead_letter"
        window["status"] = after
        window["dead_letter_reason"] = reason
        window["dead_lettered_at"] = (
            datetime.now(JST).replace(microsecond=0).isoformat()
        )
    return RepairChange(
        window_key=window_key,
        action=action,
        before_status=before,
        after_status=after,
        reason=reason,
    )


def _planned_after_status(
    store: WindowStateStore,
    window_key: str,
    window: dict[str, Any],
    *,
    action: RepairAction,
    explicit_expected_target_ids: Iterable[str] | None,
) -> str:
    if action == "dead_letter":
        return "dead_letter"
    expected = _repair_expected_target_ids(
        window_key,
        window,
        explicit_expected_target_ids=explicit_expected_target_ids,
    )
    if not _all_expected_targets_ok(window, expected):
        raise StateValidationError(f"cannot recompute non-all-ok window: {window_key}")
    _validate_source_metadata(store, window_key, window)
    return "complete"


def _stored_expected_target_ids(window: dict[str, Any]) -> list[str] | None:
    expected = window.get("expected_target_ids")
    if isinstance(expected, list) and all(isinstance(item, str) for item in expected):
        return sorted(set(expected))
    return None


def _diagnostic_expected_target_ids(
    window: dict[str, Any],
) -> tuple[list[str], ExpectedTargetSource]:
    expected = _stored_expected_target_ids(window)
    if expected is not None:
        return expected, "stored"
    targets = window.get("targets", {})
    if isinstance(targets, dict):
        return sorted(key for key in targets if isinstance(key, str)), "derived"
    return [], "derived"


def _repair_expected_target_ids(
    window_key: str,
    window: dict[str, Any],
    *,
    explicit_expected_target_ids: Iterable[str] | None,
) -> list[str]:
    explicit = _normalized_expected_target_ids(explicit_expected_target_ids)
    if explicit:
        return explicit
    expected = _stored_expected_target_ids(window)
    if expected:
        return expected
    raise StateValidationError(
        "cannot recompute window without stored expected targets; "
        f"pass explicit expected targets: {window_key}"
    )


def _normalized_expected_target_ids(
    expected_target_ids: Iterable[str] | None,
) -> list[str]:
    if expected_target_ids is None:
        return []
    return sorted(set(expected_target_ids))


def _validate_source_metadata(
    store: WindowStateStore,
    window_key: str,
    window: dict[str, Any],
) -> None:
    store.source_window_start(window_key, window)


def _all_expected_targets_ok(window: dict[str, Any], expected: Iterable[str]) -> bool:
    targets = window.get("targets", {})
    if not isinstance(targets, dict):
        return False
    return all(
        targets.get(entity_id, {}).get("status") == "ok" for entity_id in expected
    )


def _window(store: WindowStateStore, window_key: str) -> dict[str, Any]:
    window = store.as_dict()["windows"].get(window_key)
    if not isinstance(window, dict):
        raise StateValidationError(f"unknown window: {window_key}")
    return window


def _state_windows(store: WindowStateStore) -> dict[str, Any]:
    windows = store.as_dict().get("windows")
    if not isinstance(windows, dict):
        raise StateValidationError("state windows must be an object")
    return cast(dict[str, Any], windows)


def _plan_flow_migration_changes(
    store: WindowStateStore,
) -> tuple[FlowMigrationChange, ...]:
    changes: list[FlowMigrationChange] = []
    windows = _state_windows(store)
    for window_key in sorted(windows):
        window = windows[window_key]
        if not isinstance(window, dict):
            continue
        change = _plan_flow_migration_change(window_key, window)
        if change is not None:
            changes.append(change)
    return tuple(changes)


def _plan_flow_migration_change(
    window_key: str,
    window: dict[str, Any],
) -> FlowMigrationChange | None:
    before = str(window.get("status", ""))
    if before == "dead_letter":
        return None
    expected = tuple(_derived_flow_target_ids(window))
    if not expected:
        return FlowMigrationChange(
            window_key=window_key,
            action="dropped",
            before_status=before,
            after_status=None,
            expected_target_ids=(),
        )
    return FlowMigrationChange(
        window_key=window_key,
        action="recomputed",
        before_status=before,
        after_status=_planned_flow_migration_status(window, expected),
        expected_target_ids=expected,
    )


def _apply_flow_migration_changes(
    store: WindowStateStore,
) -> tuple[FlowMigrationChange, ...]:
    changes: list[FlowMigrationChange] = []
    windows = _state_windows(store)
    for window_key in sorted(tuple(windows)):
        window = windows[window_key]
        if not isinstance(window, dict):
            continue
        before = str(window.get("status", ""))
        if before == "dead_letter":
            continue
        expected = _derived_flow_target_ids(window)
        if not expected:
            del windows[window_key]
            changes.append(
                FlowMigrationChange(
                    window_key=window_key,
                    action="dropped",
                    before_status=before,
                    after_status=None,
                    expected_target_ids=(),
                )
            )
            continue
        window["expected_target_ids"] = expected
        after = store.recompute_status(window_key, expected_target_ids=expected)
        changes.append(
            FlowMigrationChange(
                window_key=window_key,
                action="recomputed",
                before_status=before,
                after_status=after,
                expected_target_ids=tuple(expected),
            )
        )
    return tuple(changes)


def _derived_flow_target_ids(window: dict[str, Any]) -> list[str]:
    targets = window.get("targets", {})
    if not isinstance(targets, dict):
        return []
    return sorted(
        entity_id
        for entity_id, target in targets.items()
        if isinstance(entity_id, str)
        and isinstance(target, Mapping)
        and target.get("status") in _FLOW_MIGRATION_TARGET_STATUSES
    )


def _planned_flow_migration_status(
    window: dict[str, Any],
    expected_target_ids: Iterable[str],
) -> str:
    targets = window.get("targets", {})
    if not isinstance(targets, Mapping):
        return "partial"
    for entity_id in expected_target_ids:
        target = targets.get(entity_id)
        if not isinstance(target, Mapping) or target.get("status") != "ok":
            return "partial"
    return "complete"


def _backup_state_file(state_path: Path) -> Path:
    if not state_path.exists():
        raise StateValidationError(f"state file does not exist: {state_path}")
    timestamp = datetime.now(JST).strftime("%Y%m%d_%H%M%S")
    backup_path = state_path.with_name(f"{state_path.name}.{timestamp}.bak")
    counter = 1
    # The product lock makes this single-writer; the counter handles leftovers
    # from previous repairs in the same second.
    while backup_path.exists():
        backup_path = state_path.with_name(
            f"{state_path.name}.{timestamp}.{counter}.bak"
        )
        counter += 1
    shutil.copy2(state_path, backup_path)
    WindowStateStore.load(backup_path)
    return backup_path


def _verify_repair_applied(
    store: WindowStateStore,
    changes: Iterable[RepairChange],
) -> None:
    windows = store.as_dict()["windows"]
    for change in changes:
        window = windows.get(change.window_key)
        if not isinstance(window, dict):
            raise StateValidationError(f"repaired window missing: {change.window_key}")
        if window.get("status") != change.after_status:
            raise StateValidationError(
                f"repair verification failed for {change.window_key}"
            )


def _verify_flow_migration_applied(
    store: WindowStateStore,
    changes: Iterable[FlowMigrationChange],
) -> None:
    windows = _state_windows(store)
    for change in changes:
        window = windows.get(change.window_key)
        if change.action == "dropped":
            if window is not None:
                raise StateValidationError(
                    f"flow migration verification failed for {change.window_key}"
                )
            continue
        if not isinstance(window, dict):
            raise StateValidationError(
                f"flow migration verification failed for {change.window_key}"
            )
        if window.get("status") != change.after_status:
            raise StateValidationError(
                f"flow migration verification failed for {change.window_key}"
            )
        if window.get("expected_target_ids") != list(change.expected_target_ids):
            raise StateValidationError(
                f"flow migration verification failed for {change.window_key}"
            )


def _flow_migration_log_counts(
    changes: Iterable[FlowMigrationChange],
) -> dict[str, int]:
    changes_tuple = tuple(changes)
    dropped = sum(1 for change in changes_tuple if change.action == "dropped")
    recomputed = sum(1 for change in changes_tuple if change.action == "recomputed")
    return {
        "windows_planned": len(changes_tuple),
        "windows_dropped": dropped,
        "windows_recomputed": recomputed,
    }


def _interval_from_key(window_key: str) -> int:
    prefix, _, _raw_start = window_key.partition("/")
    if prefix == "per300":
        return 5
    if prefix == "per3600":
        return 60
    raise StateValidationError(f"unknown interval prefix in window: {window_key}")


def _source_window_end(
    window: dict[str, Any],
    source_start: datetime,
    interval_min: int,
) -> datetime:
    raw_end = window.get("source_window_end")
    if isinstance(raw_end, str):
        return datetime.fromisoformat(raw_end)
    return source_start + timedelta(minutes=interval_min)


def _positive_int_env(
    values: Mapping[str, str],
    key: str,
    default: int,
) -> int:
    raw = values.get(key)
    if raw is None or raw == "":
        return default
    value = int(raw)
    if value <= 0:
        raise ValueError(f"{key} must be positive")
    return value


def _summarize_target_issues(
    diagnoses: Iterable[WindowDiagnosis],
    *,
    target_ids: Callable[[WindowDiagnosis], Iterable[str]],
) -> tuple[TargetIssueSummary, ...]:
    windows_by_entity: dict[str, list[WindowDiagnosis]] = {}
    for diagnosis in diagnoses:
        for entity_id in target_ids(diagnosis):
            windows_by_entity.setdefault(entity_id, []).append(diagnosis)

    summaries = [
        TargetIssueSummary(
            entity_id=entity_id,
            count=len(windows),
            oldest_window=min(
                windows,
                key=lambda item: (item.source_window_start, item.window_key),
            ).window_key,
            newest_window=max(
                windows,
                key=lambda item: (item.source_window_start, item.window_key),
            ).window_key,
        )
        for entity_id, windows in windows_by_entity.items()
    ]
    summaries.sort(key=lambda item: (-item.count, item.entity_id))
    return tuple(summaries)


def _failed_http_status_counts(
    diagnoses: Iterable[WindowDiagnosis],
) -> tuple[tuple[int, int], ...]:
    counts: dict[int, int] = {}
    for diagnosis in diagnoses:
        for http_status in diagnosis.failed_target_http_statuses:
            if http_status is not None:
                counts[http_status] = counts.get(http_status, 0) + 1
    return tuple(sorted(counts.items()))


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


def diagnoses_to_json(diagnoses: Iterable[WindowDiagnosis]) -> str:
    """Serialize diagnoses for CLI output."""
    rows = [_diagnosis_to_row(item) for item in diagnoses]
    return json.dumps(rows, indent=2, sort_keys=True)


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
    """Render a human-readable state doctor dashboard."""
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
    lines = [_stacked_bar(status_counts, ascii_only=ascii_only)]
    lines.extend(
        f"{_status_marker(status, ascii_only=ascii_only)} {status:<11} "
        f"{count:>5} {_percentage(count, total):>6}"
        for status, count in status_counts.items()
    )
    return lines


def _stacked_bar(
    status_counts: Mapping[str, int],
    *,
    ascii_only: bool,
    width: int = 64,
) -> str:
    total = sum(status_counts.values())
    segments = _stacked_segments(status_counts, total=total, width=width)
    chars = _status_markers(ascii_only=ascii_only)
    return "[" + "".join(chars[status] * count for status, count in segments) + "]"


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


def _status_marker(status: str, *, ascii_only: bool) -> str:
    return _status_markers(ascii_only=ascii_only).get(status, "U")


def _status_markers(*, ascii_only: bool) -> dict[str, str]:
    if ascii_only:
        return {
            "complete": "C",
            "partial": "P",
            "pending": "N",
            "dead_letter": "D",
            "unknown": "U",
        }
    return {
        "complete": "█",
        "partial": "▒",
        "pending": "◆",
        "dead_letter": "×",
        "unknown": "?",
    }


def _percentage(count: int, total: int) -> str:
    if total <= 0:
        return "0.0%"
    return f"{count / total * 100:.1f}%"


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
