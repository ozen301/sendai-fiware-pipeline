"""Operator tools for inspecting and repairing window state."""

import fcntl
import logging
import os
import shutil
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Literal, cast

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
    """Read-only diagnosis for one retained open window.

    Attributes:
        target_status_category: ``"all_ok"`` when every expected target has
            status ``"ok"``, ``"all_failed"`` when every one is ``"failed"``,
            or ``"mixed"`` otherwise (including targets still ``"pending"``).
        failed_target_http_statuses: HTTP status for each entry in
            ``failed_target_ids``, at the same index (``None`` where no status
            was recorded).
    """

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
        else max_lookback_hours_from_env(product)
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
    # Sort by source_window_start (oldest first), then by window_key as a
    # tie-breaker, so the output order is fully deterministic.
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
    """Repair explicitly selected windows, dry-run by default.

    Args:
        product: Product whose state file to repair.
        window_keys: Explicit window keys to repair; must be non-empty.
        action: ``"recompute_complete"`` re-derives the window's status from
            its recorded target results; ``"dead_letter"`` force-closes it.
        reason: Note stored with the window; required for ``"dead_letter"``.
        expected_target_ids: Expected targets to use instead of the window's
            stored snapshot, for ``"recompute_complete"``.
        apply: When false, plan without writing. When true, mutate the state
            file under the product lock.
        state_path: Optional state file path override.
        lock_path: Optional lock file path override.

    Returns:
        Summary of the planned or applied repair changes.

    Raises:
        StateValidationError: If ``window_keys`` is empty, ``action`` is
            ``"dead_letter"`` without a ``reason``, a window key does not
            exist, a ``"recompute_complete"`` window has no expected targets
            (stored or explicit) or one that is not ``"ok"``, or (when
            ``apply`` is true) the state file is absent when backup starts or
            post-write verification fails.
        StateLoadError: If the state file cannot be parsed as valid window
            state, on the initial load or (when ``apply`` is true) on the
            post-write reload.
        OSError: If creating the lock or backup, saving state, or another
            filesystem operation fails.
    """
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
        StateValidationError: If (when ``apply`` is true) the state file is
            absent when backup starts, or post-write
            verification fails.
        StateLoadError: If the state file cannot be parsed as valid window
            state, on the initial load or (when ``apply`` is true) on the
            post-write reload.
        OSError: If creating the lock or backup, saving state, or another
            filesystem operation fails.
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
    product: ProductName,
    env: Mapping[str, str] | None = None,
) -> dict[int, int]:
    """Return doctor retry-horizon limits for one product from the environment."""
    values = os.environ if env is None else env
    if product == "direction":
        return {60: _positive_int_env(values, "MAX_LOOKBACK_HOURS_PER3600", 72)}
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

    # Tally per-target statuses to determine the aggregate category below.
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

    # Assign the target-status category: all_ok when every expected target
    # succeeded, all_failed when every one failed, mixed for anything between
    # (including pending targets that have not been attempted yet).
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
    """Return a positive integer environment variable, or ``default`` if unset.

    Raises:
        ValueError: If the variable is set to a non-positive integer.  Note
            that a non-integer value raises ``ValueError`` from ``int()``
            directly, which propagates to the caller without a custom message.
    """
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
    # Sort by descending count (most-affected targets first), with entity_id
    # as a tie-breaker to make the order fully deterministic.
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
