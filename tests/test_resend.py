import importlib
import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

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
    def __init__(self, rows: Iterable[Mapping[str, Any]] = ()) -> None:
        self.rows = [dict(row) for row in rows]
        self.queries: list[tuple[Any, ...]] = []
        self.closed = False

    def cursor(self) -> FakeDbCursor:
        return FakeDbCursor(self)

    def close(self) -> None:
        self.closed = True


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
            "--allow-old",
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


def test_resend_rejects_old_range_without_allow_old(
    tmp_path: Path,
    metadata_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resend = _resend_module()
    _patch_basic_environment(monkeypatch, tmp_path, metadata_path)

    result = _call_main(
        resend,
        _base_args(
            from_window="20000101_0000",
            to_window="20000101_0000",
            allow_old=False,
        ),
    )

    assert result != 0


def test_resend_accepts_old_range_with_allow_old(
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


def _resend_module() -> Any:
    return importlib.import_module("scripts.resend")


def _base_args(
    *,
    product: str = "flow",
    interval_min: int | None = 60,
    from_window: str = "20260524_1000",
    to_window: str = "20260524_1000",
    allow_old: bool = True,
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
    if allow_old:
        args.append("--allow-old")
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
    db_connection: FakeDbConnection,
) -> None:
    def fake_select_flow(
        connection: FakeDbConnection,
        *,
        interval_min: int,
        lower_bound: str,
        upper_bound: str,
        max_imputation_tier: int,
    ) -> list[dict[str, Any]]:
        connection.queries.append(
            (interval_min, lower_bound, upper_bound, max_imputation_tier)
        )
        return list(db_connection.rows)

    def fake_select_direction(
        connection: FakeDbConnection,
        *,
        interval_min: int,
        lower_bound: str,
        upper_bound: str,
    ) -> list[dict[str, Any]]:
        connection.queries.append((interval_min, lower_bound, upper_bound))
        return list(db_connection.rows)

    monkeypatch.setattr(db, "select_flow_metrics", fake_select_flow)
    monkeypatch.setattr(db, "select_direction_metrics", fake_select_direction)
    if hasattr(resend, "db"):
        monkeypatch.setattr(resend.db, "select_flow_metrics", fake_select_flow)
        monkeypatch.setattr(
            resend.db, "select_direction_metrics", fake_select_direction
        )


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
