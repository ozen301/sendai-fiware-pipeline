"""Operator tools for inspecting and repairing window state."""

import fcntl
import json
import logging
import os
import shutil
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Literal

from sendai_pipeline.state import JST, StateValidationError, WindowStateStore

logger = logging.getLogger(__name__)

ProductName = Literal["flow", "direction"]
RepairAction = Literal["recompute_complete", "dead_letter"]
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
    missing_count: int
    failed_http_statuses: tuple[int, ...]
    retry_reachable: bool


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
    missing_count = 0
    failed_http_statuses: set[int] = set()
    for entity_id in expected:
        target = targets.get(entity_id)
        if not isinstance(target, dict):
            missing_count += 1
            continue
        status = target.get("status")
        if status == "ok":
            ok_count += 1
        elif status == "failed":
            failed_count += 1
            http_status = target.get("last_http_status")
            if isinstance(http_status, int):
                failed_http_statuses.add(http_status)
    if expected and ok_count == len(expected):
        category = "all_ok"
    elif expected and failed_count == len(expected):
        category = "all_failed"
    elif missing_count:
        category = "missing_targets"
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
        missing_count=missing_count,
        failed_http_statuses=tuple(sorted(failed_http_statuses)),
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


def diagnoses_to_json(diagnoses: Iterable[WindowDiagnosis]) -> str:
    """Serialize diagnoses for CLI output."""
    rows = [
        {
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
            "missing_count": item.missing_count,
            "failed_http_statuses": item.failed_http_statuses,
            "retry_reachable": item.retry_reachable,
        }
        for item in diagnoses
    ]
    return json.dumps(rows, indent=2, sort_keys=True)
