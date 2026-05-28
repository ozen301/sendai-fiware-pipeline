"""Tests for operator state doctor and repair helpers."""

import fcntl
import json
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from sendai_pipeline.state import StateValidationError, WindowStateStore
from sendai_pipeline.state_tools import (
    build_state_report,
    diagnose_state,
    diagnoses_to_json,
    load_sensor_labels,
    repair_state,
    state_report_to_pretty,
)

JST = timezone(timedelta(hours=9))
NOW = datetime(2026, 5, 25, 16, 40, 0, tzinfo=JST)
ENTITY_1 = "jp.sendai.Blesensor.per300.101"
ENTITY_2 = "jp.sendai.Blesensor.per300.102"
ENTITY_3 = "jp.sendai.Blesensor.per300.103"


def _target(status: str, http_status: int = 204) -> dict[str, object]:
    return {
        "status": status,
        "last_attempt_at": NOW.isoformat(),
        "last_http_status": http_status,
        "last_payload_sha256": "a" * 64,
    }


def _window(
    *,
    key: str,
    status: str = "partial",
    expected: list[str] | None = None,
    targets: dict[str, dict[str, object]] | None = None,
) -> dict[str, object]:
    prefix, raw_start = key.split("/")
    interval_min = 5 if prefix == "per300" else 60
    source_start = datetime.strptime(raw_start, "%Y%m%d_%H%M").replace(tzinfo=JST)
    return {
        "first_seen": NOW.isoformat(),
        "last_attempt": NOW.isoformat(),
        "attempt_count": 1,
        "interval_min": interval_min,
        "source_window_start": source_start.isoformat(),
        "source_window_end": (
            source_start + timedelta(minutes=interval_min)
        ).isoformat(),
        "expected_target_ids": expected or [],
        "targets": targets or {},
        "status": status,
    }


def _write_state(path: Path, windows: dict[str, dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"schema_version": 2, "windows": windows}, sort_keys=True),
        encoding="utf-8",
    )


def test_diagnose_state_categorizes_open_windows_deterministically(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state" / "direction.json"
    all_failed = "per300/20260525_0640"
    all_ok = "per300/20260525_0645"
    missing = "per300/20260525_0650"
    mixed = "per300/20260525_0655"
    _write_state(
        path,
        {
            mixed: _window(
                key=mixed,
                expected=[ENTITY_1, ENTITY_2],
                targets={ENTITY_1: _target("ok"), ENTITY_2: _target("failed", 502)},
            ),
            all_ok: _window(
                key=all_ok,
                expected=[ENTITY_1, ENTITY_2],
                targets={ENTITY_1: _target("ok"), ENTITY_2: _target("ok")},
            ),
            missing: _window(
                key=missing,
                expected=[ENTITY_1, ENTITY_2, ENTITY_3],
                targets={ENTITY_1: _target("ok"), ENTITY_2: _target("ok")},
            ),
            all_failed: _window(
                key=all_failed,
                expected=[ENTITY_1, ENTITY_2],
                targets={
                    ENTITY_1: _target("failed", 400),
                    ENTITY_2: _target("failed", 400),
                },
            ),
        },
    )
    store = WindowStateStore.load(path)

    diagnoses = diagnose_state(store, product="direction", now=NOW)

    assert [item.window_key for item in diagnoses] == [
        all_failed,
        all_ok,
        missing,
        mixed,
    ]
    assert [item.target_status_category for item in diagnoses] == [
        "all_failed",
        "all_ok",
        "missing_targets",
        "mixed",
    ]
    assert [item.expected_target_source for item in diagnoses] == [
        "stored",
        "stored",
        "stored",
        "stored",
    ]
    assert diagnoses[0].failed_http_statuses == (400,)
    assert diagnoses[2].missing_target_ids == (ENTITY_3,)
    assert diagnoses[3].failed_target_ids == (ENTITY_2,)
    assert diagnoses[3].failed_target_http_statuses == (502,)
    first_json_row = json.loads(diagnoses_to_json(diagnoses))[0]
    assert first_json_row["window"] == all_failed
    assert first_json_row["expected_target_source"] == "stored"


def test_diagnose_state_marks_legacy_expected_targets_as_derived(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state" / "direction.json"
    key = "per300/20260525_0640"
    legacy_window = _window(
        key=key,
        expected=[],
        targets={ENTITY_1: _target("ok"), ENTITY_2: _target("ok")},
    )
    del legacy_window["expected_target_ids"]
    _write_state(path, {key: legacy_window})
    store = WindowStateStore.load(path)

    diagnosis = diagnose_state(store, product="direction", now=NOW)[0]

    assert diagnosis.expected_target_source == "derived"
    assert diagnosis.target_status_category == "all_ok"
    assert (
        json.loads(diagnoses_to_json([diagnosis]))[0]["expected_target_source"]
        == "derived"
    )


def test_diagnose_state_retry_reachable_uses_source_window_and_config(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state" / "direction.json"
    key = "per3600/20260525_0840"
    _write_state(
        path,
        {
            key: _window(
                key=key,
                expected=[ENTITY_1],
                targets={ENTITY_1: _target("failed", 502)},
            )
        },
    )
    store = WindowStateStore.load(path)

    diagnosis = diagnose_state(
        store,
        product="direction",
        now=NOW,
        max_lookback_hours_by_interval={5: 1, 60: 9},
    )[0]

    assert diagnosis.source_window_start == datetime(2026, 5, 25, 8, 40, 0, tzinfo=JST)
    assert diagnosis.retry_reachable is True


def test_build_state_report_counts_retained_windows_and_ranks_target_issues(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state" / "direction.json"
    partial_old = "per300/20260525_0640"
    pending_new = "per300/20260525_0650"
    complete = "per300/20260525_0700"
    dead_letter = "per300/20260525_0710"
    unknown = "per300/20260525_0720"
    _write_state(
        path,
        {
            partial_old: _window(
                key=partial_old,
                expected=[ENTITY_1, ENTITY_2, ENTITY_3],
                targets={ENTITY_2: _target("failed", 502)},
            ),
            pending_new: _window(
                key=pending_new,
                status="pending",
                expected=[ENTITY_1, ENTITY_2],
                targets={ENTITY_2: _target("failed", 400)},
            ),
            complete: _window(
                key=complete,
                status="complete",
                expected=[ENTITY_1],
                targets={ENTITY_1: _target("ok")},
            ),
            dead_letter: _window(
                key=dead_letter,
                status="dead_letter",
                expected=[ENTITY_1],
                targets={ENTITY_1: _target("failed", 400)},
            ),
            unknown: _window(key=unknown, status="paused"),
        },
    )
    store = WindowStateStore.load(path)

    report = build_state_report(store, product="direction", now=NOW)

    assert report.status_counts == {
        "complete": 1,
        "partial": 1,
        "pending": 1,
        "dead_letter": 1,
        "unknown": 1,
    }
    assert [item.entity_id for item in report.missing_targets] == [ENTITY_1, ENTITY_3]
    assert report.missing_targets[0].count == 2
    assert report.missing_targets[0].oldest_window == partial_old
    assert report.missing_targets[0].newest_window == pending_new
    assert [item.entity_id for item in report.failed_targets] == [ENTITY_2]
    assert report.failed_targets[0].count == 2
    assert report.failed_http_status_counts == ((400, 1), (502, 1))


def test_pretty_report_enriches_targets_with_metadata_and_can_use_ascii_bars(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state" / "flow.json"
    metadata_path = tmp_path / "metadata" / "sensors.csv"
    key = "per300/20260525_0640"
    _write_state(
        path,
        {
            key: _window(
                key=key,
                expected=[ENTITY_1, ENTITY_2],
                targets={ENTITY_2: _target("failed", 502)},
            )
        },
    )
    metadata_path.parent.mkdir(parents=True)
    metadata_path.write_text(
        "\n".join(
            [
                "place_number,batch,expected_device_type,interval_min,"
                "entity_type,entity_id,identifcation,active",
                f"101,2026,M5Stack,5,Blesensor.per300,{ENTITY_1},101,true",
                f"102,2026,M5Stack,5,Blesensor.per300,{ENTITY_2},102,true",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    store = WindowStateStore.load(path)
    report = build_state_report(store, product="flow", now=NOW)

    output = state_report_to_pretty(
        report,
        state_path=path,
        state_size_bytes=path.stat().st_size,
        sensor_labels=load_sensor_labels(metadata_path),
        top=10,
        window_limit=10,
        window_sensor_limit=8,
        ascii_only=True,
    )

    assert "State doctor: flow" in output
    assert "C" in output
    assert "P" in output
    assert "█" not in output
    assert "C complete" in output
    assert "P partial" in output
    assert "101/2026/5m:" in output
    assert ENTITY_1 in output


def test_pretty_report_hints_when_table_rows_are_hidden(tmp_path: Path) -> None:
    path = tmp_path / "state" / "flow.json"
    first = "per300/20260525_0640"
    second = "per300/20260525_0645"
    _write_state(
        path,
        {
            first: _window(
                key=first,
                expected=[ENTITY_1, ENTITY_2],
                targets={ENTITY_2: _target("failed", 502)},
            ),
            second: _window(
                key=second,
                expected=[ENTITY_1, ENTITY_2, ENTITY_3],
                targets={ENTITY_2: _target("failed", 400)},
            ),
        },
    )
    store = WindowStateStore.load(path)
    report = build_state_report(store, product="flow", now=NOW)

    output = state_report_to_pretty(
        report,
        state_path=path,
        state_size_bytes=path.stat().st_size,
        sensor_labels={},
        top=1,
        window_limit=1,
        window_sensor_limit=8,
        ascii_only=True,
    )

    assert "... 1 more open windows hidden; rerun with --all" in output
    assert "... 1 more missing targets hidden; rerun with --all" in output
    assert f"{second}  partial" not in output


def test_pretty_report_all_rows_has_no_hidden_hint(tmp_path: Path) -> None:
    path = tmp_path / "state" / "flow.json"
    first = "per300/20260525_0640"
    second = "per300/20260525_0645"
    _write_state(
        path,
        {
            first: _window(
                key=first,
                expected=[ENTITY_1, ENTITY_2],
                targets={ENTITY_2: _target("failed", 502)},
            ),
            second: _window(
                key=second,
                expected=[ENTITY_1, ENTITY_2, ENTITY_3],
                targets={ENTITY_2: _target("failed", 400)},
            ),
        },
    )
    store = WindowStateStore.load(path)
    report = build_state_report(store, product="flow", now=NOW)

    output = state_report_to_pretty(
        report,
        state_path=path,
        state_size_bytes=path.stat().st_size,
        sensor_labels={},
        top=None,
        window_limit=None,
        window_sensor_limit=8,
        ascii_only=True,
    )

    assert "hidden; rerun with --all" not in output
    assert first in output
    assert second in output


def test_repair_state_dry_run_does_not_write(tmp_path: Path) -> None:
    path = tmp_path / "state" / "flow.json"
    key = "per3600/20260523_2200"
    _write_state(
        path,
        {
            key: _window(
                key=key,
                expected=[ENTITY_1, ENTITY_2],
                targets={ENTITY_1: _target("ok"), ENTITY_2: _target("ok")},
            )
        },
    )
    before = path.read_text(encoding="utf-8")

    result = repair_state(
        product="flow",
        window_keys=[key],
        action="recompute_complete",
        apply=False,
        state_path=path,
        lock_path=tmp_path / "state" / "flow.lock",
    )

    assert result.dry_run is True
    assert result.backup_path is None
    assert result.changes[0].after_status == "complete"
    assert path.read_text(encoding="utf-8") == before


def test_repair_state_apply_recomputes_all_ok_window_and_creates_backup(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state" / "flow.json"
    key = "per3600/20260523_2200"
    _write_state(
        path,
        {
            key: _window(
                key=key,
                expected=[ENTITY_1, ENTITY_2],
                targets={ENTITY_1: _target("ok"), ENTITY_2: _target("ok")},
            )
        },
    )

    result = repair_state(
        product="flow",
        window_keys=[key],
        action="recompute_complete",
        apply=True,
        state_path=path,
        lock_path=tmp_path / "state" / "flow.lock",
    )

    assert result.dry_run is False
    assert result.backup_path is not None
    assert result.backup_path.exists()
    assert (
        WindowStateStore.load(result.backup_path).as_dict()["windows"][key]["status"]
        == "partial"
    )
    assert WindowStateStore.load(path).as_dict()["windows"][key]["status"] == "complete"


def test_repair_state_refuses_legacy_recompute_without_expected_targets(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state" / "flow.json"
    key = "per3600/20260523_2200"
    legacy_window = _window(
        key=key,
        expected=[],
        targets={ENTITY_1: _target("ok"), ENTITY_2: _target("ok")},
    )
    del legacy_window["expected_target_ids"]
    _write_state(path, {key: legacy_window})

    with pytest.raises(StateValidationError, match="without stored expected targets"):
        repair_state(
            product="flow",
            window_keys=[key],
            action="recompute_complete",
            apply=False,
            state_path=path,
            lock_path=tmp_path / "state" / "flow.lock",
        )


def test_repair_state_can_recompute_legacy_window_with_explicit_targets(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state" / "flow.json"
    key = "per3600/20260523_2200"
    legacy_window = _window(
        key=key,
        expected=[],
        targets={ENTITY_1: _target("ok"), ENTITY_2: _target("ok")},
    )
    del legacy_window["expected_target_ids"]
    _write_state(path, {key: legacy_window})

    result = repair_state(
        product="flow",
        window_keys=[key],
        action="recompute_complete",
        expected_target_ids=[ENTITY_1, ENTITY_2],
        apply=True,
        state_path=path,
        lock_path=tmp_path / "state" / "flow.lock",
    )

    assert result.changes[0].after_status == "complete"
    window = WindowStateStore.load(path).as_dict()["windows"][key]
    assert window["expected_target_ids"] == [ENTITY_1, ENTITY_2]
    assert window["status"] == "complete"


def test_repair_state_apply_waits_for_product_lock(tmp_path: Path) -> None:
    path = tmp_path / "state" / "flow.json"
    lock_path = tmp_path / "state" / "flow.lock"
    key = "per3600/20260523_2200"
    _write_state(
        path,
        {
            key: _window(
                key=key,
                expected=[ENTITY_1],
                targets={ENTITY_1: _target("ok")},
            )
        },
    )
    lock_path.parent.mkdir(parents=True, exist_ok=True)

    with lock_path.open("a", encoding="utf-8") as lock_file:
        fcntl.flock(lock_file, fcntl.LOCK_EX)
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(
                repair_state,
                product="flow",
                window_keys=[key],
                action="recompute_complete",
                apply=True,
                state_path=path,
                lock_path=lock_path,
            )
            try:
                with pytest.raises(TimeoutError):
                    future.result(timeout=0.001)
            finally:
                fcntl.flock(lock_file, fcntl.LOCK_UN)
            result = future.result(timeout=2)

    assert result.changes[0].after_status == "complete"


def test_repair_state_dead_letter_requires_reason_and_records_it(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state" / "direction.json"
    key = "per300/20260525_0640"
    _write_state(
        path,
        {
            key: _window(
                key=key,
                expected=[ENTITY_1],
                targets={ENTITY_1: _target("failed", 400)},
            )
        },
    )

    with pytest.raises(StateValidationError, match="requires a reason"):
        repair_state(
            product="direction",
            window_keys=[key],
            action="dead_letter",
            apply=True,
            state_path=path,
            lock_path=tmp_path / "state" / "direction.lock",
        )

    repair_state(
        product="direction",
        window_keys=[key],
        action="dead_letter",
        reason="source row no longer retained",
        apply=True,
        state_path=path,
        lock_path=tmp_path / "state" / "direction.lock",
    )

    window = WindowStateStore.load(path).as_dict()["windows"][key]
    assert window["status"] == "dead_letter"
    assert window["dead_letter_reason"] == "source row no longer retained"


def test_repair_state_refuses_apply_without_explicit_windows(tmp_path: Path) -> None:
    with pytest.raises(StateValidationError, match="explicit window"):
        repair_state(
            product="flow",
            window_keys=[],
            action="recompute_complete",
            apply=True,
            state_path=tmp_path / "state" / "flow.json",
            lock_path=tmp_path / "state" / "flow.lock",
        )
