import importlib
import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pymysql
import pytest

from sendai_pipeline import auth, db, orion_client, run_direction, run_flow

ENTITY_10 = "jp.sendai.Blesensor.per3600.10"
ENTITY_11 = "jp.sendai.Blesensor.per3600.11"
ENTITY_12 = "jp.sendai.Blesensor.per3600.12"
ENTITY_13_INACTIVE = "jp.sendai.Blesensor.per3600.13"
ENTITY_99_PER300 = "jp.sendai.Blesensor.per300.99"


class FakeDbCursor:
    def __init__(self, connection: "FakeDbConnection") -> None:
        self._connection = connection

    def __enter__(self) -> "FakeDbCursor":
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, _sql: str, params: tuple[Any, ...]) -> None:
        self._connection.queries.append(params)

    def fetchall(self) -> list[dict[str, Any]]:
        return [dict(row) for row in self._connection.rows]


class FakeDbConnection:
    def __init__(
        self,
        rows: Iterable[Mapping[str, Any]] = (),
        *,
        select_errors_by_attempt: Mapping[int, BaseException] | None = None,
        close_error: BaseException | None = None,
    ) -> None:
        self.rows = [dict(row) for row in rows]
        self.queries: list[tuple[Any, ...]] = []
        self.select_errors_by_attempt = dict(select_errors_by_attempt or {})
        self.select_attempts = 0
        self.closed = False
        self.close_calls = 0
        self.close_error = close_error

    def cursor(self) -> FakeDbCursor:
        return FakeDbCursor(self)

    def select_rows(self, params: tuple[Any, ...]) -> list[dict[str, Any]]:
        self.select_attempts += 1
        self.queries.append(params)
        error = self.select_errors_by_attempt.get(self.select_attempts)
        if error is not None:
            raise error
        return [dict(row) for row in self.rows]

    def close(self) -> None:
        self.close_calls += 1
        self.closed = True
        if self.close_error is not None:
            raise self.close_error


class FakeOrionClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def update_attrs(
        self,
        entity_id: str,
        entity_type: str | None,
        attrs: Mapping[str, Any],
        *,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        self.calls.append(
            {
                "entity_id": entity_id,
                "entity_type": entity_type,
                "attrs": dict(attrs),
                "dry_run": dry_run,
            }
        )
        return {"status": 204, "ok": True}


class FakeResendLogger:
    def __init__(self) -> None:
        self.records: list[dict[str, Any]] = []

    def debug(self, message: str, *, extra: Mapping[str, Any] | None = None) -> None:
        self._append("debug", message, extra)

    def info(self, message: str, *, extra: Mapping[str, Any] | None = None) -> None:
        self._append("info", message, extra)

    def warning(self, message: str, *, extra: Mapping[str, Any] | None = None) -> None:
        self._append("warning", message, extra)

    def error(self, message: str, *, extra: Mapping[str, Any] | None = None) -> None:
        self._append("error", message, extra)

    def exception(
        self,
        message: str,
        *,
        extra: Mapping[str, Any] | None = None,
    ) -> None:
        self._append("error", message, extra)

    def _append(
        self,
        level: str,
        message: str,
        extra: Mapping[str, Any] | None,
    ) -> None:
        self.records.append({"level": level, "message": message, **dict(extra or {})})


@dataclass
class ProcessCall:
    product: str
    window_key: str
    interval_min: int
    startdate: str
    interval_entity_ids: list[str]
    expected_target_ids: list[str]
    force_resend: bool | None
    force_resend_was_passed: bool
    persist_each_target: bool | None
    persist_each_target_was_passed: bool


@pytest.fixture
def metadata_path(tmp_path: Path) -> Path:
    path = tmp_path / "metadata.csv"
    path.write_text(
        "\n".join(
            [
                "place_number,batch,expected_device_type,interval_min,"
                "entity_type,entity_id,identifcation,active",
                f"10,2026,M5Stack,60,Blesensor.per3600,{ENTITY_10},10,true",
                f"11,2026,M5Stack,60,Blesensor.per3600,{ENTITY_11},11,true",
                f"12,2026,Pixel3aUT,60,Blesensor.per3600,{ENTITY_12},12,true",
                f"13,2026,M5Stack,60,Blesensor.per3600,{ENTITY_13_INACTIVE},13,false",
                f"99,2026,M5Stack,5,Blesensor.per300,{ENTITY_99_PER300},99,true",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return path


def test_resend_requires_reason(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resend = _resend_module()
    _patch_basic_environment(monkeypatch, tmp_path, metadata_path)

    result = _call_main(
        resend,
        [
            "flow",
            "--interval-min",
            "60",
            "--from",
            "20260524_1000",
            "--to",
            "20260524_1000",
        ],
    )

    assert result != 0


def test_resend_rejects_place_and_entity_id_together(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resend = _resend_module()
    _patch_basic_environment(monkeypatch, tmp_path, metadata_path)

    result = _call_main(
        resend,
        _base_args()
        + [
            "--place",
            "10",
            "--entity-id",
            ENTITY_10,
        ],
    )

    assert result != 0


def test_resend_rejects_empty_range(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resend = _resend_module()
    _patch_basic_environment(monkeypatch, tmp_path, metadata_path)

    result = _call_main(
        resend,
        _base_args(from_window="20260524_1200", to_window="20260524_1000"),
    )

    assert result != 0


def test_resend_accepts_old_range(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resend = _resend_module()
    _patch_basic_environment(monkeypatch, tmp_path, metadata_path)

    result = _call_main(
        resend,
        _base_args(from_window="20000101_0000", to_window="20000101_0000"),
    )

    assert result == 0


def test_resend_rejects_bad_window_format(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resend = _resend_module()
    _patch_basic_environment(monkeypatch, tmp_path, metadata_path)

    result = _call_main(resend, _base_args(from_window="2026-05-24T10:00:00"))

    assert result != 0


def test_resend_rejects_unknown_place_number(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resend = _resend_module()
    _patch_basic_environment(monkeypatch, tmp_path, metadata_path)

    result = _call_main(resend, _base_args() + ["--place", "999"])

    assert result != 0


def test_resend_rejects_unknown_entity_id(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resend = _resend_module()
    _patch_basic_environment(monkeypatch, tmp_path, metadata_path)

    result = _call_main(
        resend,
        _base_args() + ["--entity-id", "jp.sendai.Blesensor.per3600.999"],
    )

    assert result != 0


def test_resend_rejects_misaligned_from_window_for_60min(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resend = _resend_module()
    _patch_basic_environment(monkeypatch, tmp_path, metadata_path)

    result = _call_main(
        resend,
        _base_args(from_window="20260524_1015", to_window="20260524_1015"),
    )

    assert result != 0


def test_resend_rejects_misaligned_to_window_for_5min(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resend = _resend_module()
    _patch_basic_environment(monkeypatch, tmp_path, metadata_path)

    result = _call_main(
        resend,
        _base_args(
            interval_min=5,
            from_window="20260524_1000",
            to_window="20260524_1003",
        ),
    )

    assert result != 0


def test_resend_enumerates_all_windows_in_range_inclusive(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    resend = _resend_module()
    _patch_basic_environment(monkeypatch, tmp_path, metadata_path)

    result = _call_main(
        resend,
        _base_args(from_window="20260524_1000", to_window="20260524_1200"),
    )

    assert result == 0
    assert _summary_window_keys(capsys.readouterr().out) == [
        "per3600/20260524_1000",
        "per3600/20260524_1100",
        "per3600/20260524_1200",
    ]


def test_resend_single_window_when_from_equals_to(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    resend = _resend_module()
    _patch_basic_environment(monkeypatch, tmp_path, metadata_path)

    result = _call_main(
        resend,
        _base_args(from_window="20260524_1000", to_window="20260524_1000"),
    )

    assert result == 0
    assert _summary_window_keys(capsys.readouterr().out) == [
        "per3600/20260524_1000",
    ]


def test_resend_dry_run_makes_no_db_query(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resend = _resend_module()
    _patch_basic_environment(monkeypatch, tmp_path, metadata_path)
    _patch_db_forbidden(monkeypatch, resend)

    result = _call_main(resend, _base_args())

    assert result == 0


def test_resend_dry_run_makes_no_orion_calls(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resend = _resend_module()
    _patch_basic_environment(monkeypatch, tmp_path, metadata_path)
    _patch_orion_forbidden(monkeypatch, resend)

    result = _call_main(resend, _base_args())

    assert result == 0


def test_resend_dry_run_does_not_require_fiware_credentials(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resend = _resend_module()
    _patch_basic_environment(monkeypatch, tmp_path, metadata_path)
    _clear_fiware_environment(monkeypatch)

    result = _call_main(resend, _base_args())

    assert result == 0


def test_resend_place_filter_narrows_interval_metadata(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _run_send(
        tmp_path,
        metadata_path,
        monkeypatch,
        _base_args(send=True) + ["--place", "10"],
    )

    assert calls[0].interval_entity_ids == [ENTITY_10]


def test_resend_entity_id_filter_narrows_interval_metadata(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _run_send(
        tmp_path,
        metadata_path,
        monkeypatch,
        _base_args(send=True) + ["--entity-id", ENTITY_11],
    )

    assert calls[0].interval_entity_ids == [ENTITY_11]


def test_resend_entity_id_without_interval_min_infers_interval(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _run_send(
        tmp_path,
        metadata_path,
        monkeypatch,
        _base_args(interval_min=None, send=True) + ["--entity-id", ENTITY_11],
    )

    assert calls[0].interval_min == 60
    assert calls[0].interval_entity_ids == [ENTITY_11]


def test_resend_mixed_interval_entity_ids_require_interval_min(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resend = _resend_module()
    _patch_basic_environment(monkeypatch, tmp_path, metadata_path)

    result = _call_main(
        resend,
        _base_args(interval_min=None)
        + ["--entity-id", ENTITY_10, "--entity-id", ENTITY_99_PER300],
    )

    assert result != 0


def test_resend_interval_min_disagreeing_with_entity_id_errors(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resend = _resend_module()
    _patch_basic_environment(monkeypatch, tmp_path, metadata_path)

    result = _call_main(
        resend,
        _base_args(interval_min=5) + ["--entity-id", ENTITY_10],
    )

    assert result != 0


def test_resend_place_without_interval_min_still_errors(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resend = _resend_module()
    _patch_basic_environment(monkeypatch, tmp_path, metadata_path)

    result = _call_main(
        resend,
        _base_args(interval_min=None) + ["--place", "10"],
    )

    assert result != 0


def test_resend_no_filter_uses_full_active_set(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _run_send(tmp_path, metadata_path, monkeypatch, _base_args(send=True))

    assert calls[0].interval_entity_ids == [ENTITY_10, ENTITY_11, ENTITY_12]


def test_resend_place_filter_filters_expected_target_ids_consistently(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _run_send(
        tmp_path,
        metadata_path,
        monkeypatch,
        _base_args(send=True) + ["--place", "10"],
    )

    assert calls[0].expected_target_ids == calls[0].interval_entity_ids == [ENTITY_10]


def test_resend_flow_place_filter_posts_requested_target_but_preserves_stored_expected(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resend = _resend_module()
    _patch_basic_environment(monkeypatch, tmp_path, metadata_path)
    db_connection, orion = _patch_real_send_dependencies(monkeypatch, resend)
    db_connection.rows = [
        {
            "startdate": "20260524_1000",
            "group_place_id": "sendai202603.10",
            "device_type": "M5Stack",
            "interval_min": 60,
            "flow_gt_m60": 6,
            "flow_gt_m80": 237,
            "flow_gt_m120": 430,
            "stay_gt_m60": 0.2,
            "stay_gt_m80": 40.9,
            "imputation_tier": 0,
        }
    ]
    state_path = tmp_path / "state" / "flow.json"
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(
        json.dumps(
            {
                "schema_version": 2,
                "windows": {
                    "per3600/20260524_1000": {
                        "first_seen": "2026-05-24T13:00:00+09:00",
                        "last_attempt": "2026-05-24T13:00:00+09:00",
                        "attempt_count": 1,
                        "interval_min": 60,
                        "source_window_start": "2026-05-24T10:00:00+09:00",
                        "source_window_end": "2026-05-24T11:00:00+09:00",
                        "expected_target_ids": [ENTITY_10, ENTITY_11],
                        "targets": {},
                        "status": "partial",
                    }
                },
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    result = _call_main(
        resend,
        _base_args(send=True) + ["--place", "10", "--force"],
    )

    assert result == 0
    assert [call["entity_id"] for call in orion.calls] == [ENTITY_10]
    window = json.loads(state_path.read_text(encoding="utf-8"))["windows"][
        "per3600/20260524_1000"
    ]
    assert window["expected_target_ids"] == [ENTITY_10, ENTITY_11]
    assert window["status"] == "partial"
    assert sorted(window["targets"]) == [ENTITY_10]


def test_resend_multiple_place_flags_narrow_to_intersection(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _run_send(
        tmp_path,
        metadata_path,
        monkeypatch,
        _base_args(send=True) + ["--place", "10", "--place", "11"],
    )

    assert calls[0].interval_entity_ids == [ENTITY_10, ENTITY_11]


def test_resend_force_passes_force_resend_true_to_process_send_window(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _run_send(
        tmp_path,
        metadata_path,
        monkeypatch,
        _base_args(send=True) + ["--force"],
    )

    assert calls[0].force_resend_was_passed is True
    assert calls[0].force_resend is True


def test_resend_no_force_passes_force_resend_false_to_process_send_window(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _run_send(tmp_path, metadata_path, monkeypatch, _base_args(send=True))

    assert calls[0].force_resend_was_passed is True
    assert calls[0].force_resend is False


def test_resend_forwards_persist_each_target_false_flow(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _run_send(tmp_path, metadata_path, monkeypatch, _base_args(send=True))

    assert calls[0].persist_each_target_was_passed is True
    assert calls[0].persist_each_target is False


def test_resend_forwards_persist_each_target_false_direction(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _run_send(
        tmp_path,
        metadata_path,
        monkeypatch,
        _base_args(product="direction", send=True),
    )

    assert calls[0].persist_each_target_was_passed is True
    assert calls[0].persist_each_target is False


def test_resend_cadence_flush_count(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for window_count, expected_saves in [(1, 1), (2, 1), (3, 2), (4, 2)]:
        case_path = tmp_path / f"case-{window_count}"
        case_path.mkdir()
        with monkeypatch.context() as case_monkeypatch:
            resend = _resend_module()
            save_count = 0
            real_save = resend.WindowStateStore.save

            def save_spy(store: Any) -> None:
                nonlocal save_count
                save_count += 1
                real_save(store)

            case_monkeypatch.setattr(resend, "_RESEND_SAVE_EVERY", 2)
            case_monkeypatch.setattr(resend.WindowStateStore, "save", save_spy)

            calls = _run_send(
                case_path,
                metadata_path,
                case_monkeypatch,
                _base_args(
                    send=True,
                    from_window="20260524_1000",
                    to_window=f"20260524_{9 + window_count:02d}00",
                ),
            )

        assert len(calls) == window_count
        assert save_count == expected_saves


def test_resend_real_path_not_per_target(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resend = _resend_module()
    _patch_basic_environment(monkeypatch, tmp_path, metadata_path)
    db_connection, orion = _patch_real_send_dependencies(monkeypatch, resend)
    db_connection.rows = [
        {
            "startdate": "20260524_1000",
            "group_place_id": "sendai202603.10",
            "device_type": "M5Stack",
            "interval_min": 60,
            "flow_gt_m60": 6,
            "flow_gt_m80": 237,
            "flow_gt_m120": 430,
            "stay_gt_m60": 0.2,
            "stay_gt_m80": 40.9,
            "imputation_tier": 0,
        },
        {
            "startdate": "20260524_1000",
            "group_place_id": "sendai202603.11",
            "device_type": "M5Stack",
            "interval_min": 60,
            "flow_gt_m60": 7,
            "flow_gt_m80": 238,
            "flow_gt_m120": 431,
            "stay_gt_m60": 0.3,
            "stay_gt_m80": 41.9,
            "imputation_tier": 0,
        },
    ]
    save_count = 0
    real_save = resend.WindowStateStore.save

    def save_spy(store: Any) -> None:
        nonlocal save_count
        save_count += 1
        real_save(store)

    monkeypatch.setattr(resend.WindowStateStore, "save", save_spy)

    # Give the inspected window a recent source_window_start so it stays
    # inside the resend GC horizon: this test asserts per-target save
    # behavior, not GC, so the window must survive to be read back below.
    _write_state(
        tmp_path / "state" / "flow.json",
        {
            "per3600/20260524_1000": _window_record(
                status="complete",
                source_start=datetime(2026, 6, 19, 10, 0, tzinfo=run_direction.JST),
                interval_min=60,
                expected_target_ids=[ENTITY_10, ENTITY_11],
                targets={
                    ENTITY_10: _target_record(),
                    ENTITY_11: _target_record(),
                },
            )
        },
    )

    result = _call_main(resend, _base_args(send=True) + ["--force"])

    assert result == 0
    assert [call["entity_id"] for call in orion.calls] == [ENTITY_10, ENTITY_11]
    assert save_count == 1
    window = json.loads((tmp_path / "state" / "flow.json").read_text(encoding="utf-8"))[
        "windows"
    ]["per3600/20260524_1000"]
    assert window["status"] == "complete"
    assert sorted(window["targets"]) == [ENTITY_10, ENTITY_11]


def test_resend_skips_window_with_no_source_rows(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resend = _resend_module()
    _patch_basic_environment(monkeypatch, tmp_path, metadata_path)
    db_connection, _orion = _patch_real_send_dependencies(monkeypatch, resend)
    db_connection.rows = []
    begin_calls: list[str] = []
    save_count = 0
    real_begin = resend.WindowStateStore.begin_window_attempt
    real_save = resend.WindowStateStore.save

    def begin_spy(store: Any, window_key: str, **kwargs: Any) -> None:
        begin_calls.append(window_key)
        real_begin(store, window_key, **kwargs)

    def save_spy(store: Any) -> None:
        nonlocal save_count
        save_count += 1
        real_save(store)

    monkeypatch.setattr(resend.WindowStateStore, "begin_window_attempt", begin_spy)
    monkeypatch.setattr(resend.WindowStateStore, "save", save_spy)

    result = _call_main(resend, _base_args(product="direction", send=True))

    assert result == 0
    assert begin_calls == []
    assert save_count == 0
    state_path = tmp_path / "state" / "direction.json"
    if state_path.exists():
        assert "per3600/20260524_1000" not in _read_windows(state_path)


def test_resend_empty_window_not_persisted_as_partial(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resend = _resend_module()
    _freeze_resend_now(monkeypatch, resend)
    _patch_basic_environment(monkeypatch, tmp_path, metadata_path)
    _patch_real_send_dependencies(monkeypatch, resend)
    # Windows sit inside the GC horizon (frozen now is 2026-06-20) so this
    # test isolates the empty-window skip from GC: the empty window must be
    # absent because it was skipped, not because it was reclaimed.
    _patch_direction_select_rows_by_startdate(
        monkeypatch,
        resend,
        {
            "20260620_1000": [],
            "20260620_1100": [_direction_row("20260620_1100")],
        },
    )

    result = _call_main(
        resend,
        _base_args(
            product="direction",
            send=True,
            from_window="20260620_1000",
            to_window="20260620_1100",
        )
        + ["--force"],
    )

    assert result == 0
    windows = _read_windows(tmp_path / "state" / "direction.json")
    assert sorted(windows) == ["per3600/20260620_1100"]


def test_resend_summary_reports_empty_window_count(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resend = _resend_module()
    logger = _patch_resend_logger(monkeypatch, resend)
    _patch_basic_environment(monkeypatch, tmp_path, metadata_path)
    _patch_real_send_dependencies(monkeypatch, resend)
    _patch_direction_select_rows_by_startdate(
        monkeypatch,
        resend,
        {
            "20260524_1000": [],
            "20260524_1100": [_direction_row("20260524_1100")],
            "20260524_1200": [],
        },
    )

    result = _call_main(
        resend,
        _base_args(
            product="direction",
            send=True,
            from_window="20260524_1000",
            to_window="20260524_1200",
        )
        + ["--force"],
    )

    assert result == 0
    assert _summary_record(logger)["windows_empty"] == 2


def test_resend_filtered_to_empty_window_still_creates_partial(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resend = _resend_module()
    logger = _patch_resend_logger(monkeypatch, resend)
    _patch_basic_environment(monkeypatch, tmp_path, metadata_path)
    db_connection, orion = _patch_real_send_dependencies(monkeypatch, resend)
    db_connection.rows = [
        {
            "startdate": "20260524_1000",
            "from_group_place_id": "quick.10",
            "to_group_place_id": "sendai202603.11",
            "from_device_type": "M5Stack",
            "to_device_type": "M5Stack",
            "interval_min": 60,
            "count": 12,
        }
    ]
    save_count = 0
    real_save = resend.WindowStateStore.save

    def save_spy(store: Any) -> None:
        nonlocal save_count
        save_count += 1
        real_save(store)

    monkeypatch.setattr(resend, "_RESEND_SAVE_EVERY", 1)
    monkeypatch.setattr(resend.WindowStateStore, "save", save_spy)

    result = _call_main(
        resend,
        _base_args(product="direction", send=True) + ["--force"],
    )

    assert result == 0
    assert orion.calls == []
    assert save_count == 1
    window = _read_windows(tmp_path / "state" / "direction.json")[
        "per3600/20260524_1000"
    ]
    assert window["status"] == "partial"
    assert window["expected_target_ids"] == [ENTITY_10, ENTITY_11, ENTITY_12]
    assert window["targets"] == {}
    assert _summary_record(logger)["windows_empty"] == 0


def test_resend_zero_post_window_advances_cadence(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resend = _resend_module()
    _patch_basic_environment(monkeypatch, tmp_path, metadata_path)
    db_connection, orion = _patch_real_send_dependencies(monkeypatch, resend)
    db_connection.rows = [
        {
            "startdate": "20260524_1000",
            "from_group_place_id": "quick.10",
            "to_group_place_id": "sendai202603.11",
            "from_device_type": "M5Stack",
            "to_device_type": "M5Stack",
            "interval_min": 60,
            "count": 12,
        }
    ]
    save_count = 0
    real_save = resend.WindowStateStore.save

    def save_spy(store: Any) -> None:
        nonlocal save_count
        save_count += 1
        real_save(store)

    monkeypatch.setattr(resend, "_RESEND_SAVE_EVERY", 1)
    monkeypatch.setattr(resend.WindowStateStore, "save", save_spy)

    result = _call_main(
        resend,
        _base_args(product="direction", send=True) + ["--force"],
    )

    assert result == 0
    assert orion.calls == []
    assert save_count == 1
    window = json.loads(
        (tmp_path / "state" / "direction.json").read_text(encoding="utf-8")
    )["windows"]["per3600/20260524_1000"]
    assert window["status"] == "partial"
    assert window["expected_target_ids"] == [ENTITY_10, ENTITY_11, ENTITY_12]
    assert window["targets"] == {}


def test_resend_gc_removes_complete_windows_older_than_horizon(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resend = _resend_module()
    logger = _patch_resend_logger(monkeypatch, resend)
    _freeze_resend_now(monkeypatch, resend)
    _patch_basic_environment(monkeypatch, tmp_path, metadata_path)
    _write_state(
        tmp_path / "state" / "flow.json",
        {
            "per3600/20260524_1000": _window_record(
                status="complete",
                source_start=datetime(2026, 5, 24, 10, 0, tzinfo=run_direction.JST),
            )
        },
    )

    _run_send(
        tmp_path,
        metadata_path,
        monkeypatch,
        _base_args(send=True, from_window="20260619_1000", to_window="20260619_1000"),
    )

    assert "per3600/20260524_1000" not in _read_windows(
        tmp_path / "state" / "flow.json"
    )
    assert _summary_record(logger)["windows_gc"] == 1
    gc_records = _gc_records(logger)
    assert len(gc_records) == 1
    assert gc_records[0]["product"] == "flow"
    assert gc_records[0]["interval_min"] == 60
    assert gc_records[0]["reason"] == "operator requested resend"


def test_resend_gc_keeps_complete_windows_inside_horizon(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resend = _resend_module()
    logger = _patch_resend_logger(monkeypatch, resend)
    _freeze_resend_now(monkeypatch, resend)
    _patch_basic_environment(monkeypatch, tmp_path, metadata_path)
    _write_state(
        tmp_path / "state" / "flow.json",
        {
            "per3600/20260619_0900": _window_record(
                status="complete",
                source_start=datetime(2026, 6, 19, 9, 0, tzinfo=run_direction.JST),
            )
        },
    )

    _run_send(
        tmp_path,
        metadata_path,
        monkeypatch,
        _base_args(send=True, from_window="20260619_1000", to_window="20260619_1000"),
    )

    assert "per3600/20260619_0900" in _read_windows(tmp_path / "state" / "flow.json")
    assert _summary_record(logger)["windows_gc"] == 0


def test_resend_gc_keeps_partial_windows(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resend = _resend_module()
    logger = _patch_resend_logger(monkeypatch, resend)
    _freeze_resend_now(monkeypatch, resend)
    _patch_basic_environment(monkeypatch, tmp_path, metadata_path)
    _write_state(
        tmp_path / "state" / "flow.json",
        {
            "per3600/20260524_1000": _window_record(
                status="partial",
                source_start=datetime(2026, 5, 24, 10, 0, tzinfo=run_direction.JST),
            )
        },
    )

    _run_send(
        tmp_path,
        metadata_path,
        monkeypatch,
        _base_args(send=True, from_window="20260619_1000", to_window="20260619_1000"),
    )

    assert "per3600/20260524_1000" in _read_windows(tmp_path / "state" / "flow.json")
    assert _summary_record(logger)["windows_gc"] == 0


def test_resend_gc_cutoff_uses_max_of_both_intervals(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resend = _resend_module()
    logger = _patch_resend_logger(monkeypatch, resend)
    _freeze_resend_now(monkeypatch, resend)
    _patch_basic_environment(monkeypatch, tmp_path, metadata_path)
    monkeypatch.setenv("MAX_LOOKBACK_HOURS_PER300", "24")
    monkeypatch.setenv("MAX_LOOKBACK_HOURS_PER3600", "72")
    _write_state(
        tmp_path / "state" / "flow.json",
        {
            "per3600/20260615_1000": _window_record(
                status="complete",
                source_start=datetime(2026, 6, 15, 10, 0, tzinfo=run_direction.JST),
            )
        },
    )

    _run_send(
        tmp_path,
        metadata_path,
        monkeypatch,
        _base_args(send=True, from_window="20260619_1000", to_window="20260619_1000"),
    )

    assert "per3600/20260615_1000" in _read_windows(tmp_path / "state" / "flow.json")
    assert _summary_record(logger)["windows_gc"] == 0


def test_resend_gc_runs_periodically_and_finally(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resend = _resend_module()
    logger = _patch_resend_logger(monkeypatch, resend)
    _freeze_resend_now(monkeypatch, resend)
    _patch_basic_environment(monkeypatch, tmp_path, metadata_path)
    monkeypatch.setattr(resend, "_RESEND_SAVE_EVERY", 2)
    _write_state(
        tmp_path / "state" / "flow.json",
        {
            "per3600/20260524_1000": _window_record(
                status="complete",
                source_start=datetime(2026, 5, 24, 10, 0, tzinfo=run_direction.JST),
            ),
            "per3600/20260524_1100": _window_record(
                status="complete",
                source_start=datetime(2026, 5, 24, 11, 0, tzinfo=run_direction.JST),
            ),
        },
    )
    gc_cutoffs: list[datetime] = []
    real_gc = resend.WindowStateStore.gc_complete_before

    def gc_spy(store: Any, cutoff: datetime) -> int:
        gc_cutoffs.append(cutoff)
        return real_gc(store, cutoff)

    monkeypatch.setattr(resend.WindowStateStore, "gc_complete_before", gc_spy)

    _run_send(
        tmp_path,
        metadata_path,
        monkeypatch,
        _base_args(send=True, from_window="20260619_1000", to_window="20260619_1200"),
    )

    assert len(gc_cutoffs) == 2
    assert gc_cutoffs[0] == gc_cutoffs[1]
    assert _summary_record(logger)["windows_gc"] == 2


def test_resend_dry_run_does_not_gc(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resend = _resend_module()
    logger = _patch_resend_logger(monkeypatch, resend)
    _freeze_resend_now(monkeypatch, resend)
    _patch_basic_environment(monkeypatch, tmp_path, metadata_path)
    _write_state(
        tmp_path / "state" / "flow.json",
        {
            "per3600/20260524_1000": _window_record(
                status="complete",
                source_start=datetime(2026, 5, 24, 10, 0, tzinfo=run_direction.JST),
            )
        },
    )

    result = _call_main(
        resend,
        _base_args(send=False, from_window="20260619_1000", to_window="20260619_1000"),
    )

    assert result == 0
    assert "per3600/20260524_1000" in _read_windows(tmp_path / "state" / "flow.json")
    assert _summary_record(logger)["windows_gc"] == 0


def test_resend_reconnects_after_dropped_connection_and_continues(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resend = _resend_module()
    logger = _patch_resend_logger(monkeypatch, resend)
    _patch_basic_environment(monkeypatch, tmp_path, metadata_path)
    sleep_calls: list[float] = []
    _patch_resend_sleep(monkeypatch, resend, sleep_calls)
    dropped = pymysql.err.OperationalError(2006, "server gone")
    dead_connection = FakeDbConnection(select_errors_by_attempt={1: dropped})
    fresh_connection = FakeDbConnection(rows=[{"startdate": "20260524_1000"}])
    connect_calls, _orion = _patch_send_dependency_sequence(
        monkeypatch,
        resend,
        [dead_connection, fresh_connection],
    )
    calls: list[ProcessCall] = []
    _patch_process_windows(monkeypatch, resend, calls)

    result = _call_main(resend, _base_args(send=True))

    assert result == 0
    assert db.is_connection_lost_error(dropped) is True
    assert connect_calls == [dead_connection, fresh_connection]
    assert dead_connection.closed is True
    assert sleep_calls == [1]
    assert fresh_connection.queries == [(60, "20260524_1000", "20260524_1000", 2)]
    assert [call.window_key for call in calls] == ["per3600/20260524_1000"]
    assert _reconnect_records(logger) == [
        {
            "level": "warning",
            "message": "db reconnect before resend select",
            "event": "resend_db_reconnect",
            "product": "flow",
            "interval_min": 60,
            "reason": "operator requested resend",
            "window": "per3600/20260524_1000",
            "attempt": 1,
            "error_class": "OperationalError",
        }
    ]


def test_resend_reconnect_retries_are_bounded_then_aborts(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resend = _resend_module()
    logger = _patch_resend_logger(monkeypatch, resend)
    _patch_basic_environment(monkeypatch, tmp_path, metadata_path)
    sleep_calls: list[float] = []
    _patch_resend_sleep(monkeypatch, resend, sleep_calls)
    attempts = resend._RESEND_DB_RECONNECT_ATTEMPTS
    lost = pymysql.err.OperationalError(2013, "lost")
    connections = [
        FakeDbConnection(select_errors_by_attempt={1: lost})
        for _attempt in range(attempts + 1)
    ]
    connect_calls, _orion = _patch_send_dependency_sequence(
        monkeypatch,
        resend,
        connections,
    )
    calls: list[ProcessCall] = []
    _patch_process_windows(monkeypatch, resend, calls)

    result = _call_main(resend, _base_args(send=True))

    assert result == 2
    assert db.is_connection_lost_error(lost) is True
    assert connect_calls == connections
    assert sleep_calls == [1] * attempts
    assert all(connection.closed for connection in connections)
    assert calls == []
    records = _reconnect_records(logger)
    assert [record["attempt"] for record in records] == list(range(1, attempts + 1))
    assert {record["window"] for record in records} == {"per3600/20260524_1000"}
    assert {record["error_class"] for record in records} == {"OperationalError"}
    assert {record["product"] for record in records} == {"flow"}
    assert {record["interval_min"] for record in records} == {60}
    assert {record["reason"] for record in records} == {"operator requested resend"}
    exhausted_records = _exhausted_reconnect_records(logger)
    assert len(exhausted_records) == 1
    assert exhausted_records[0]["level"] == "error"
    assert exhausted_records[0]["product"] == "flow"
    assert exhausted_records[0]["interval_min"] == 60
    assert exhausted_records[0]["reason"] == "operator requested resend"
    assert exhausted_records[0]["window"] == "per3600/20260524_1000"
    assert exhausted_records[0]["attempts"] == attempts


def test_resend_reconnect_exhaustion_flushes_completed_window_progress(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resend = _resend_module()
    _patch_basic_environment(monkeypatch, tmp_path, metadata_path)
    sleep_calls: list[float] = []
    _patch_resend_sleep(monkeypatch, resend, sleep_calls)
    lost = pymysql.err.OperationalError(2013, "lost")
    attempts = resend._RESEND_DB_RECONNECT_ATTEMPTS
    first_connection = FakeDbConnection(
        rows=[_direction_row("20260524_1000")],
        select_errors_by_attempt={2: lost},
    )
    exhausted_connections = [
        FakeDbConnection(select_errors_by_attempt={1: lost})
        for _attempt in range(attempts)
    ]
    _patch_send_dependency_sequence(
        monkeypatch,
        resend,
        [first_connection, *exhausted_connections],
    )

    result = _call_main(
        resend,
        _base_args(
            product="direction",
            send=True,
            from_window="20260524_1000",
            to_window="20260524_1100",
        )
        + ["--force"],
    )

    assert result == 2
    assert sleep_calls == [1] * attempts
    windows = _read_windows(tmp_path / "state" / "direction.json")
    assert "per3600/20260524_1000" in windows
    assert "per3600/20260524_1100" not in windows


def test_resend_does_not_reconnect_on_non_connection_operational_error(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resend = _resend_module()
    logger = _patch_resend_logger(monkeypatch, resend)
    _patch_basic_environment(monkeypatch, tmp_path, metadata_path)
    sleep_calls: list[float] = []
    _patch_resend_sleep(monkeypatch, resend, sleep_calls)
    bad_column = pymysql.err.OperationalError(1054, "unknown column")
    connection = FakeDbConnection(select_errors_by_attempt={1: bad_column})
    connect_calls, _orion = _patch_send_dependency_sequence(
        monkeypatch,
        resend,
        [connection],
    )
    calls: list[ProcessCall] = []
    _patch_process_windows(monkeypatch, resend, calls)

    result = _call_main(resend, _base_args(send=True))

    assert result == 2
    assert db.is_connection_lost_error(bad_column) is False
    assert sleep_calls == []
    assert connect_calls == [connection]
    assert connection.close_calls == 1
    assert calls == []
    assert _reconnect_records(logger) == []


def test_resend_does_not_reconnect_on_non_connection_db_error(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resend = _resend_module()
    logger = _patch_resend_logger(monkeypatch, resend)
    _patch_basic_environment(monkeypatch, tmp_path, metadata_path)
    sleep_calls: list[float] = []
    _patch_resend_sleep(monkeypatch, resend, sleep_calls)
    programming_error = pymysql.err.ProgrammingError("bad query shape")
    connection = FakeDbConnection(select_errors_by_attempt={1: programming_error})
    connect_calls, _orion = _patch_send_dependency_sequence(
        monkeypatch,
        resend,
        [connection],
    )
    calls: list[ProcessCall] = []
    _patch_process_windows(monkeypatch, resend, calls)

    result = _call_main(resend, _base_args(send=True))

    assert result == 2
    assert db.is_connection_lost_error(programming_error) is False
    assert sleep_calls == []
    assert connect_calls == [connection]
    assert connection.close_calls == 1
    assert calls == []
    assert _reconnect_records(logger) == []


def test_resend_reconnect_preserves_progress(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resend = _resend_module()
    logger = _patch_resend_logger(monkeypatch, resend)
    _patch_basic_environment(monkeypatch, tmp_path, metadata_path)
    sleep_calls: list[float] = []
    _patch_resend_sleep(monkeypatch, resend, sleep_calls)
    lost = pymysql.err.InterfaceError("socket closed")
    first_connection = FakeDbConnection(
        rows=[{"startdate": "20260524_1000"}],
        select_errors_by_attempt={2: lost},
    )
    fresh_connection = FakeDbConnection(rows=[{"startdate": "20260524_1100"}])
    connect_calls, _orion = _patch_send_dependency_sequence(
        monkeypatch,
        resend,
        [first_connection, fresh_connection],
    )
    calls: list[ProcessCall] = []
    _patch_process_windows(monkeypatch, resend, calls)

    result = _call_main(
        resend,
        _base_args(
            send=True,
            from_window="20260524_1000",
            to_window="20260524_1100",
        ),
    )

    assert result == 0
    assert db.is_connection_lost_error(lost) is True
    assert sleep_calls == [1]
    assert connect_calls == [first_connection, fresh_connection]
    assert [call.startdate for call in calls] == ["20260524_1000", "20260524_1100"]
    assert first_connection.queries == [
        (60, "20260524_1000", "20260524_1000", 2),
        (60, "20260524_1100", "20260524_1100", 2),
    ]
    assert fresh_connection.queries == [(60, "20260524_1100", "20260524_1100", 2)]
    assert [record["window"] for record in _reconnect_records(logger)] == [
        "per3600/20260524_1100"
    ]


def test_resend_final_close_on_dead_handle_does_not_raise(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resend = _resend_module()
    _patch_basic_environment(monkeypatch, tmp_path, metadata_path)
    close_error = pymysql.err.InterfaceError("already closed")
    connection = FakeDbConnection(
        rows=[{"startdate": "20260524_1000"}],
        close_error=close_error,
    )
    connect_calls, _orion = _patch_send_dependency_sequence(
        monkeypatch,
        resend,
        [connection],
    )
    calls: list[ProcessCall] = []
    _patch_process_windows(monkeypatch, resend, calls)

    result = _call_main(resend, _base_args(send=True))

    assert result == 0
    assert db.is_connection_lost_error(close_error) is True
    assert connect_calls == [connection]
    assert connection.close_calls == 1
    assert [call.window_key for call in calls] == ["per3600/20260524_1000"]


def test_resend_dry_run_unaffected_by_reconnect_logic(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    resend = _resend_module()
    logger = _patch_resend_logger(monkeypatch, resend)
    _patch_basic_environment(monkeypatch, tmp_path, metadata_path)
    _patch_db_forbidden(monkeypatch, resend)

    result = _call_main(resend, _base_args(send=False))

    assert result == 0
    assert resend._RESEND_DB_RECONNECT_ATTEMPTS == 2
    assert _summary_window_keys(capsys.readouterr().out) == ["per3600/20260524_1000"]
    assert _reconnect_records(logger) == []


def test_resend_gc_reclaims_just_resent_old_window(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resend = _resend_module()
    logger = _patch_resend_logger(monkeypatch, resend)
    _freeze_resend_now(monkeypatch, resend)
    _patch_basic_environment(monkeypatch, tmp_path, metadata_path)
    db_connection, _orion = _patch_real_send_dependencies(monkeypatch, resend)
    # Source rows for an old window (2026-05-24) the run delivers to
    # `complete` from a clean state. Its key-derived source_window_start is
    # older than the GC horizon, so resend must reclaim it on exit.
    db_connection.rows = [
        {
            "startdate": "20260524_1000",
            "group_place_id": "sendai202603.10",
            "device_type": "M5Stack",
            "interval_min": 60,
            "flow_gt_m60": 6,
            "flow_gt_m80": 237,
            "flow_gt_m120": 430,
            "stay_gt_m60": 0.2,
            "stay_gt_m80": 40.9,
            "imputation_tier": 0,
        },
        {
            "startdate": "20260524_1000",
            "group_place_id": "sendai202603.11",
            "device_type": "M5Stack",
            "interval_min": 60,
            "flow_gt_m60": 7,
            "flow_gt_m80": 238,
            "flow_gt_m120": 431,
            "stay_gt_m60": 0.3,
            "stay_gt_m80": 41.9,
            "imputation_tier": 0,
        },
    ]

    result = _call_main(resend, _base_args(send=True) + ["--force"])

    assert result == 0
    windows = _read_windows(tmp_path / "state" / "flow.json")
    assert "per3600/20260524_1000" not in windows
    assert _summary_record(logger)["windows_gc"] == 1


def test_resend_flow_calls_flow_process_send_window(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _run_send(tmp_path, metadata_path, monkeypatch, _base_args(send=True))

    assert [call.product for call in calls] == ["flow"]


def test_resend_flow_send_uses_default_source_max_imputation_tier(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _calls, db_connection = _run_send_with_db(
        tmp_path,
        metadata_path,
        monkeypatch,
        _base_args(send=True),
    )

    assert db_connection.queries == [(60, "20260524_1000", "20260524_1000", 2)]


def test_resend_flow_send_uses_env_source_max_imputation_tier(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SOURCE_MAX_IMPUTATION_TIER", "1")

    _calls, db_connection = _run_send_with_db(
        tmp_path,
        metadata_path,
        monkeypatch,
        _base_args(send=True),
    )

    assert db_connection.queries == [(60, "20260524_1000", "20260524_1000", 1)]


def test_resend_flow_send_max_imputation_tier_flag_overrides_env(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SOURCE_MAX_IMPUTATION_TIER", "1")

    _calls, db_connection = _run_send_with_db(
        tmp_path,
        metadata_path,
        monkeypatch,
        _base_args(send=True) + ["--max-imputation-tier", "4"],
    )

    assert db_connection.queries == [(60, "20260524_1000", "20260524_1000", 4)]


def test_resend_direction_calls_direction_process_send_window(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _run_send(
        tmp_path,
        metadata_path,
        monkeypatch,
        _base_args(product="direction", send=True),
    )

    assert [call.product for call in calls] == ["direction"]


def test_resend_flow_ignores_invalid_direction_target_batches(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resend = _resend_module()
    _patch_basic_environment(monkeypatch, tmp_path, metadata_path)
    monkeypatch.setenv("TARGET_DIRECTION_BATCHES", "2025")

    result = _call_main(resend, _base_args(product="flow"))

    assert result == 0


def test_resend_direction_ignores_invalid_flow_target_batches(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resend = _resend_module()
    _patch_basic_environment(monkeypatch, tmp_path, metadata_path)
    monkeypatch.setenv("TARGET_FLOW_BATCHES", "2025")

    result = _call_main(resend, _base_args(product="direction"))

    assert result == 0


def test_resend_selects_target_batches_for_requested_product(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    metadata_path = tmp_path / "metadata.csv"
    flow_2023_entity = "jp.sendai.Blesensor.per3600.2023"
    direction_2026_entity = "jp.sendai.Blesensor.per3600.2026"
    metadata_path.write_text(
        "\n".join(
            [
                "place_number,batch,expected_device_type,interval_min,"
                "entity_type,entity_id,identifcation,active",
                f"10,2023,Pixel3aUT,60,Blesensor.per3600,{flow_2023_entity},10,true",
                f"11,2026,M5Stack,60,Blesensor.per3600,{direction_2026_entity},11,true",
                "",
            ]
        ),
        encoding="utf-8",
    )
    resend = _resend_module()
    _patch_basic_environment(monkeypatch, tmp_path, metadata_path)
    monkeypatch.setenv("TARGET_FLOW_BATCHES", "2023")
    monkeypatch.setenv("TARGET_DIRECTION_BATCHES", "2026")
    _patch_send_dependencies(monkeypatch, resend)
    calls: list[ProcessCall] = []
    _patch_process_windows(monkeypatch, resend, calls)

    flow_result = _call_main(resend, _base_args(product="flow", send=True))
    direction_result = _call_main(resend, _base_args(product="direction", send=True))

    assert flow_result == 0
    assert direction_result == 0
    assert [call.expected_target_ids for call in calls] == [
        [flow_2023_entity],
        [direction_2026_entity],
    ]


def test_resend_direction_rejects_max_imputation_tier_flag(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resend = _resend_module()
    _patch_basic_environment(monkeypatch, tmp_path, metadata_path)

    result = _call_main(
        resend,
        _base_args(product="direction") + ["--max-imputation-tier", "1"],
    )

    assert result == 2


def test_resend_summary_reports_partial_and_complete_window_counts(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resend = _resend_module()
    logger = _patch_resend_logger(monkeypatch, resend)
    _patch_basic_environment(monkeypatch, tmp_path, metadata_path)
    _patch_real_send_dependencies(monkeypatch, resend)
    _patch_direction_select_rows_by_startdate(
        monkeypatch,
        resend,
        {
            "20260524_1000": [_direction_row("20260524_1000")],
            "20260524_1100": [
                {
                    "startdate": "20260524_1100",
                    "from_group_place_id": "quick.10",
                    "to_group_place_id": "sendai202603.11",
                    "from_device_type": "M5Stack",
                    "to_device_type": "M5Stack",
                    "interval_min": 60,
                    "count": 12,
                }
            ],
        },
    )

    result = _call_main(
        resend,
        _base_args(
            product="direction",
            send=True,
            from_window="20260524_1000",
            to_window="20260524_1100",
        )
        + ["--force"],
    )

    assert result == 0
    summary = _summary_record(logger)
    assert summary["windows_complete"] == 1
    assert summary["windows_partial"] == 1


def test_resend_partial_window_keeps_exit_code_zero(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resend = _resend_module()
    logger = _patch_resend_logger(monkeypatch, resend)
    _patch_basic_environment(monkeypatch, tmp_path, metadata_path)
    db_connection, orion = _patch_real_send_dependencies(monkeypatch, resend)
    db_connection.rows = [
        {
            "startdate": "20260524_1000",
            "from_group_place_id": "quick.10",
            "to_group_place_id": "sendai202603.11",
            "from_device_type": "M5Stack",
            "to_device_type": "M5Stack",
            "interval_min": 60,
            "count": 12,
        }
    ]

    result = _call_main(
        resend,
        _base_args(product="direction", send=True) + ["--force"],
    )

    assert result == 0
    assert orion.calls == []
    summary = _summary_record(logger)
    assert summary["posts_failed"] == 0
    assert summary["windows_partial"] == 1


def test_resend_dry_run_summary_reports_zero_partial_complete(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resend = _resend_module()
    logger = _patch_resend_logger(monkeypatch, resend)
    _patch_basic_environment(monkeypatch, tmp_path, metadata_path)

    result = _call_main(resend, _base_args(send=False))

    assert result == 0
    summary = _summary_record(logger)
    assert summary["windows_partial"] == 0
    assert summary["windows_complete"] == 0


def test_resend_aborts_before_posting_when_range_contains_dead_letter(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resend = _resend_module()
    _patch_basic_environment(monkeypatch, tmp_path, metadata_path)
    db_connection, orion = _patch_real_send_dependencies(monkeypatch, resend)
    db_connection.rows = [_direction_row("20260524_1000")]
    _write_state(
        tmp_path / "state" / "direction.json",
        {
            "per3600/20260524_1000": _window_record(
                status="dead_letter",
                source_start=datetime(2026, 5, 24, 10, 0, tzinfo=run_direction.JST),
                expected_target_ids=[ENTITY_10, ENTITY_11, ENTITY_12],
            )
        },
    )

    result = _call_main(
        resend,
        _base_args(product="direction", send=True) + ["--force"],
    )

    assert result == 2
    assert orion.calls == []
    assert db_connection.queries == []


def test_resend_preflight_lists_all_dead_letter_windows(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    resend = _resend_module()
    logger = _patch_resend_logger(monkeypatch, resend)
    _patch_basic_environment(monkeypatch, tmp_path, metadata_path)
    _patch_real_send_dependencies(monkeypatch, resend)
    _patch_direction_select_rows_by_startdate(
        monkeypatch,
        resend,
        {
            "20260524_1000": [_direction_row("20260524_1000")],
            "20260524_1100": [_direction_row("20260524_1100")],
        },
    )
    _write_state(
        tmp_path / "state" / "direction.json",
        {
            "per3600/20260524_1000": _window_record(
                status="dead_letter",
                source_start=datetime(2026, 5, 24, 10, 0, tzinfo=run_direction.JST),
            ),
            "per3600/20260524_1100": _window_record(
                status="dead_letter",
                source_start=datetime(2026, 5, 24, 11, 0, tzinfo=run_direction.JST),
            ),
        },
    )

    result = _call_main(
        resend,
        _base_args(
            product="direction",
            send=True,
            from_window="20260524_1000",
            to_window="20260524_1100",
        )
        + ["--force"],
    )

    surface = capsys.readouterr().err + json.dumps(logger.records, sort_keys=True)
    assert result == 2
    assert "per3600/20260524_1000" in surface
    assert "per3600/20260524_1100" in surface


def test_resend_preflight_aborts_on_dead_letter_with_no_source_rows(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resend = _resend_module()
    _patch_basic_environment(monkeypatch, tmp_path, metadata_path)
    db_connection, orion = _patch_real_send_dependencies(monkeypatch, resend)
    db_connection.rows = []
    _write_state(
        tmp_path / "state" / "direction.json",
        {
            "per3600/20260524_1000": _window_record(
                status="dead_letter",
                source_start=datetime(2026, 5, 24, 10, 0, tzinfo=run_direction.JST),
            )
        },
    )

    result = _call_main(
        resend,
        _base_args(product="direction", send=True) + ["--force"],
    )

    assert result == 2
    assert orion.calls == []
    assert db_connection.queries == []


def test_resend_preflight_allows_range_with_no_dead_letter(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resend = _resend_module()
    _patch_basic_environment(monkeypatch, tmp_path, metadata_path)
    db_connection, orion = _patch_real_send_dependencies(monkeypatch, resend)
    db_connection.rows = [
        {
            "startdate": "20260524_1000",
            "group_place_id": "sendai202603.10",
            "device_type": "M5Stack",
            "interval_min": 60,
            "flow_gt_m60": 6,
            "flow_gt_m80": 237,
            "flow_gt_m120": 430,
            "stay_gt_m60": 0.2,
            "stay_gt_m80": 40.9,
            "imputation_tier": 0,
        }
    ]
    order: list[str] = []

    def connect(_settings: object) -> FakeDbConnection:
        order.append("connect")
        return db_connection

    real_load = resend.WindowStateStore.load

    def load_spy(path: Path, **kwargs: Any) -> Any:
        order.append("load")
        return real_load(path, **kwargs)

    _patch_module_attr(monkeypatch, resend, "db", db, "connect", connect)
    monkeypatch.setattr(db, "connect", connect)
    monkeypatch.setattr(resend.WindowStateStore, "load", staticmethod(load_spy))

    result = _call_main(resend, _base_args(send=True) + ["--force"])

    assert result == 0
    assert [call["entity_id"] for call in orion.calls] == [ENTITY_10]
    assert order.index("load") < order.index("connect")


def _resend_module() -> Any:
    return importlib.import_module("scripts.resend")


def _base_args(
    *,
    product: str = "flow",
    interval_min: int | None = 60,
    from_window: str = "20260524_1000",
    to_window: str = "20260524_1000",
    send: bool = False,
) -> list[str]:
    args = [
        product,
        "--from",
        from_window,
        "--to",
        to_window,
        "--reason",
        "operator requested resend",
    ]
    if interval_min is not None:
        args.extend(["--interval-min", str(interval_min)])
    if send:
        args.append("--send")
    return args


def _call_main(resend: Any, argv: list[str]) -> int:
    try:
        result = resend.main(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    return int(result)


def _run_send(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    argv: list[str],
) -> list[ProcessCall]:
    calls, _db_connection = _run_send_with_db(
        tmp_path,
        metadata_path,
        monkeypatch,
        argv,
    )
    return calls


def _run_send_with_db(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    argv: list[str],
) -> tuple[list[ProcessCall], FakeDbConnection]:
    resend = _resend_module()
    _patch_basic_environment(monkeypatch, tmp_path, metadata_path)
    db_connection = _patch_send_dependencies(monkeypatch, resend)
    calls: list[ProcessCall] = []
    _patch_process_windows(monkeypatch, resend, calls)

    result = _call_main(resend, argv)

    assert result == 0
    assert calls
    return calls, db_connection


def _patch_basic_environment(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    metadata_path: Path,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("SENSOR_METADATA_PATH", str(metadata_path))
    monkeypatch.setenv("TARGET_FLOW_BATCHES", "2026")
    monkeypatch.setenv("TARGET_DIRECTION_BATCHES", "2026")


def _patch_send_dependencies(
    monkeypatch: pytest.MonkeyPatch, resend: Any
) -> FakeDbConnection:
    db_connection = FakeDbConnection(rows=[{"startdate": "20260524_1000"}])
    orion = FakeOrionClient()

    _patch_module_attr(
        monkeypatch, resend, "db", db, "connect", lambda _settings: db_connection
    )
    monkeypatch.setattr(db, "connect", lambda _settings: db_connection)
    _patch_select_metrics(monkeypatch, resend, db_connection)

    monkeypatch.setattr(auth.AuthSettings, "from_env", staticmethod(lambda: object()))
    monkeypatch.setattr(auth, "AuthClient", lambda _settings: object())
    _patch_module_attr(
        monkeypatch,
        resend,
        "auth",
        auth,
        "AuthClient",
        lambda _settings: object(),
    )

    monkeypatch.setattr(
        orion_client.OrionSettings,
        "from_env",
        staticmethod(lambda: object()),
    )
    monkeypatch.setattr(
        orion_client,
        "OrionClient",
        lambda _settings, *, auth: orion,
    )
    _patch_module_attr(
        monkeypatch,
        resend,
        "orion_client",
        orion_client,
        "OrionClient",
        lambda _settings, *, auth: orion,
    )
    return db_connection


def _patch_real_send_dependencies(
    monkeypatch: pytest.MonkeyPatch,
    resend: Any,
) -> tuple[FakeDbConnection, FakeOrionClient]:
    db_connection = FakeDbConnection()
    orion = FakeOrionClient()

    _patch_module_attr(
        monkeypatch, resend, "db", db, "connect", lambda _settings: db_connection
    )
    monkeypatch.setattr(db, "connect", lambda _settings: db_connection)
    _patch_select_metrics(monkeypatch, resend, db_connection)

    monkeypatch.setattr(auth.AuthSettings, "from_env", staticmethod(lambda: object()))
    monkeypatch.setattr(auth, "AuthClient", lambda _settings: object())
    _patch_module_attr(
        monkeypatch,
        resend,
        "auth",
        auth,
        "AuthClient",
        lambda _settings: object(),
    )

    monkeypatch.setattr(
        orion_client.OrionSettings,
        "from_env",
        staticmethod(lambda: object()),
    )
    monkeypatch.setattr(
        orion_client,
        "OrionClient",
        lambda _settings, *, auth: orion,
    )
    _patch_module_attr(
        monkeypatch,
        resend,
        "orion_client",
        orion_client,
        "OrionClient",
        lambda _settings, *, auth: orion,
    )
    return db_connection, orion


def _patch_send_dependency_sequence(
    monkeypatch: pytest.MonkeyPatch,
    resend: Any,
    connections: Iterable[FakeDbConnection],
) -> tuple[list[FakeDbConnection], FakeOrionClient]:
    connection_queue = list(connections)
    assert connection_queue
    connect_calls: list[FakeDbConnection] = []
    orion = FakeOrionClient()

    def connect(_settings: object) -> FakeDbConnection:
        if len(connect_calls) >= len(connection_queue):
            raise AssertionError("unexpected extra DB reconnect")
        connection = connection_queue[len(connect_calls)]
        connect_calls.append(connection)
        return connection

    _patch_module_attr(monkeypatch, resend, "db", db, "connect", connect)
    monkeypatch.setattr(db, "connect", connect)
    _patch_select_metrics(monkeypatch, resend, connection_queue[0])

    monkeypatch.setattr(auth.AuthSettings, "from_env", staticmethod(lambda: object()))
    monkeypatch.setattr(auth, "AuthClient", lambda _settings: object())
    _patch_module_attr(
        monkeypatch,
        resend,
        "auth",
        auth,
        "AuthClient",
        lambda _settings: object(),
    )

    monkeypatch.setattr(
        orion_client.OrionSettings,
        "from_env",
        staticmethod(lambda: object()),
    )
    monkeypatch.setattr(
        orion_client,
        "OrionClient",
        lambda _settings, *, auth: orion,
    )
    _patch_module_attr(
        monkeypatch,
        resend,
        "orion_client",
        orion_client,
        "OrionClient",
        lambda _settings, *, auth: orion,
    )
    return connect_calls, orion


def _patch_process_windows(
    monkeypatch: pytest.MonkeyPatch,
    resend: Any,
    calls: list[ProcessCall],
) -> None:
    def fake_process(product: str) -> Any:
        def capture(window_key: str, **kwargs: Any) -> None:
            calls.append(
                ProcessCall(
                    product=product,
                    window_key=window_key,
                    interval_min=kwargs["interval_min"],
                    startdate=kwargs["startdate"],
                    interval_entity_ids=_entity_ids(kwargs["interval_metadata"]),
                    expected_target_ids=list(kwargs["expected_target_ids"]),
                    force_resend=kwargs.get("force_resend"),
                    force_resend_was_passed="force_resend" in kwargs,
                    persist_each_target=kwargs.get("persist_each_target"),
                    persist_each_target_was_passed="persist_each_target" in kwargs,
                )
            )

        return capture

    flow_process = fake_process("flow")
    direction_process = fake_process("direction")
    monkeypatch.setattr(run_flow, "_process_send_window", flow_process)
    monkeypatch.setattr(run_direction, "_process_send_window", direction_process)
    monkeypatch.setattr(resend, "process_flow_window", flow_process, raising=False)
    monkeypatch.setattr(
        resend,
        "process_direction_window",
        direction_process,
        raising=False,
    )


def _patch_select_metrics(
    monkeypatch: pytest.MonkeyPatch,
    resend: Any,
    _db_connection: FakeDbConnection,
) -> None:
    def fake_select_flow(
        connection: FakeDbConnection,
        *,
        interval_min: int,
        lower_bound: str,
        upper_bound: str,
        max_imputation_tier: int,
    ) -> list[dict[str, Any]]:
        return connection.select_rows(
            (interval_min, lower_bound, upper_bound, max_imputation_tier)
        )

    def fake_select_direction(
        connection: FakeDbConnection,
        *,
        interval_min: int,
        lower_bound: str,
        upper_bound: str,
    ) -> list[dict[str, Any]]:
        return connection.select_rows((interval_min, lower_bound, upper_bound))

    monkeypatch.setattr(db, "select_flow_metrics", fake_select_flow)
    monkeypatch.setattr(db, "select_direction_metrics", fake_select_direction)
    if hasattr(resend, "db"):
        monkeypatch.setattr(resend.db, "select_flow_metrics", fake_select_flow)
        monkeypatch.setattr(
            resend.db, "select_direction_metrics", fake_select_direction
        )


def _patch_direction_select_rows_by_startdate(
    monkeypatch: pytest.MonkeyPatch,
    resend: Any,
    rows_by_startdate: Mapping[str, list[dict[str, Any]]],
) -> None:
    def fake_select_direction(
        connection: FakeDbConnection,
        *,
        interval_min: int,
        lower_bound: str,
        upper_bound: str,
    ) -> list[dict[str, Any]]:
        connection.queries.append((interval_min, lower_bound, upper_bound))
        return [dict(row) for row in rows_by_startdate[lower_bound]]

    monkeypatch.setattr(db, "select_direction_metrics", fake_select_direction)
    if hasattr(resend, "db"):
        monkeypatch.setattr(
            resend.db,
            "select_direction_metrics",
            fake_select_direction,
        )


def _patch_resend_logger(
    monkeypatch: pytest.MonkeyPatch,
    resend: Any,
) -> FakeResendLogger:
    logger = FakeResendLogger()
    monkeypatch.setattr(resend, "logger", logger)
    return logger


def _patch_resend_sleep(
    monkeypatch: pytest.MonkeyPatch,
    resend: Any,
    sleep_calls: list[float],
) -> None:
    def sleep(seconds: float) -> None:
        sleep_calls.append(seconds)

    class FakeTime:
        @staticmethod
        def sleep(seconds: float) -> None:
            sleep(seconds)

    monkeypatch.setattr(resend, "time", FakeTime, raising=False)
    monkeypatch.setattr(resend, "sleep", sleep, raising=False)


def _freeze_resend_now(
    monkeypatch: pytest.MonkeyPatch,
    resend: Any,
    now: datetime = datetime(2026, 6, 20, 12, 0, tzinfo=run_direction.JST),
) -> None:
    class FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz: Any = None) -> datetime:
            if tz is None:
                return now.replace(tzinfo=None)
            return now.astimezone(tz)

    monkeypatch.setattr(resend, "datetime", FrozenDateTime)


def _summary_record(logger: FakeResendLogger) -> dict[str, Any]:
    records = [
        record for record in logger.records if record.get("event") == "resend_summary"
    ]
    assert len(records) == 1
    return records[0]


def _reconnect_records(logger: FakeResendLogger) -> list[dict[str, Any]]:
    return [
        record
        for record in logger.records
        if record.get("event") == "resend_db_reconnect"
    ]


def _exhausted_reconnect_records(logger: FakeResendLogger) -> list[dict[str, Any]]:
    return [
        record
        for record in logger.records
        if record.get("event") == "resend_db_reconnect_exhausted"
    ]


def _gc_records(logger: FakeResendLogger) -> list[dict[str, Any]]:
    return [record for record in logger.records if record.get("event") == "resend_gc"]


def _read_windows(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))["windows"]


def _write_state(path: Path, windows: Mapping[str, Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "schema_version": 2,
                "windows": windows,
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )


def _window_record(
    *,
    status: str,
    source_start: datetime,
    interval_min: int = 60,
    expected_target_ids: Iterable[str] = (ENTITY_10,),
    targets: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    source_end = source_start + timedelta(minutes=interval_min)
    return {
        "first_seen": "2026-06-20T12:00:00+09:00",
        "last_attempt": "2026-06-20T12:00:00+09:00",
        "attempt_count": 1,
        "interval_min": interval_min,
        "source_window_start": source_start.isoformat(),
        "source_window_end": source_end.isoformat(),
        "expected_target_ids": list(expected_target_ids),
        "targets": dict(targets or {}),
        "status": status,
    }


def _target_record() -> dict[str, Any]:
    return {
        "status": "ok",
        "last_attempt_at": "2026-06-20T12:00:00+09:00",
        "last_http_status": 204,
        "last_payload_sha256": "seeded",
    }


def _direction_row(startdate: str) -> dict[str, Any]:
    return {
        "startdate": startdate,
        "from_group_place_id": "sendai202603.10",
        "to_group_place_id": "sendai202603.11",
        "from_device_type": "M5Stack",
        "to_device_type": "M5Stack",
        "interval_min": 60,
        "count": 12,
    }


def _patch_db_forbidden(monkeypatch: pytest.MonkeyPatch, resend: Any) -> None:
    def fail(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("dry-run must not touch MySQL")

    monkeypatch.setattr(db, "connect", fail)
    monkeypatch.setattr(db, "select_flow_metrics", fail)
    monkeypatch.setattr(db, "select_direction_metrics", fail)
    if hasattr(resend, "db"):
        monkeypatch.setattr(resend.db, "connect", fail)
        monkeypatch.setattr(resend.db, "select_flow_metrics", fail)
        monkeypatch.setattr(resend.db, "select_direction_metrics", fail)


def _patch_orion_forbidden(monkeypatch: pytest.MonkeyPatch, resend: Any) -> None:
    def fail(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("dry-run must not touch FIWARE")

    monkeypatch.setattr(auth.AuthSettings, "from_env", staticmethod(fail))
    monkeypatch.setattr(auth, "AuthClient", fail)
    monkeypatch.setattr(orion_client.OrionSettings, "from_env", staticmethod(fail))
    monkeypatch.setattr(orion_client, "OrionClient", fail)
    if hasattr(resend, "auth"):
        monkeypatch.setattr(resend.auth.AuthSettings, "from_env", staticmethod(fail))
        monkeypatch.setattr(resend.auth, "AuthClient", fail)
    if hasattr(resend, "orion_client"):
        monkeypatch.setattr(
            resend.orion_client.OrionSettings,
            "from_env",
            staticmethod(fail),
        )
        monkeypatch.setattr(resend.orion_client, "OrionClient", fail)


def _patch_module_attr(
    monkeypatch: pytest.MonkeyPatch,
    owner: Any,
    module_name: str,
    fallback_module: Any,
    attr_name: str,
    value: Any,
) -> None:
    module = getattr(owner, module_name, fallback_module)
    monkeypatch.setattr(module, attr_name, value)


def _clear_fiware_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in [
        "FIWARE_BASE_URL",
        "FIWARE_CONSUMER_KEY",
        "FIWARE_CONSUMER_SECRET",
        "FIWARE_TOKEN_URL",
        "FIWARE_SERVICE",
        "FIWARE_SERVICE_PATH",
    ]:
        monkeypatch.delenv(key, raising=False)


def _entity_ids(interval_metadata: Any) -> list[str]:
    values = (
        interval_metadata.values()
        if isinstance(interval_metadata, Mapping)
        else interval_metadata
    )
    return [place.entity_id for place in values]


def _summary_window_keys(output: str) -> list[str]:
    keys: list[str] = []
    for line in output.splitlines():
        for token in line.split():
            cleaned = token.strip("(),")
            if cleaned.startswith(("per3600/", "per300/")):
                keys.append(cleaned)
    return keys
