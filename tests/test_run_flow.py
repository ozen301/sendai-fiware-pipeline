import fcntl
import json
import logging
from collections.abc import Callable, Iterable, Mapping
from copy import deepcopy
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import pytest

import sendai_pipeline.run_flow as run_flow_module
from sendai_pipeline.filter_settings import FilterConfigError, FilterSettings
from sendai_pipeline.logging_setup import _ALLOWED_EXTRA_KEYS
from sendai_pipeline.metadata import SensorPlace, active_places, index_by_place_interval
from sendai_pipeline.run_flow import (
    RunFlowConfigError,
    RunFlowResult,
    RunFlowSettings,
)
from sendai_pipeline.run_flow import (
    main as run_flow_main,
)
from sendai_pipeline.run_flow import (
    run_flow as run_product_flow,
)
from sendai_pipeline.state import WindowStateStore
from sendai_pipeline.transform_flow import transform_flow_rows

JST = timezone(timedelta(hours=9))
NOW = datetime(2026, 5, 23, 12, 17, 0, tzinfo=JST)
NOW_ON_HOUR = datetime(2026, 5, 23, 12, 0, 0, tzinfo=JST)
REVISION_NOW = datetime(2026, 6, 30, 12, 17, 43, 123456, tzinfo=JST)
NON_JST_RUN_START = datetime(2026, 5, 23, 3, 17, 59, 987654, tzinfo=UTC)
PRODUCT_A_ATTR_NAMES = [
    "dateObservedFrom",
    "dateObservedTo",
    "dateRetrieved",
    "identifcation",
    "peopleCount_immedate",
    "peopleCount_near",
    "peopleCount_far",
    "peopleOccupancy_immedate",
    "peopleOccupancy_near",
    "peopleOccupancy_far",
]


class Clock:
    def __init__(self, values: Iterable[datetime]) -> None:
        self._values = list(values)
        self._index = 0
        if not self._values:
            raise ValueError("Clock needs at least one value")

    def __call__(self) -> datetime:
        if self._index >= len(self._values):
            return self._values[-1]
        value = self._values[self._index]
        self._index += 1
        return value


class FakeDbCursor:
    def __init__(self, connection: "FakeDbConnection") -> None:
        self._connection = connection
        self._rows: list[dict[str, Any]] = []

    def __enter__(self) -> "FakeDbCursor":
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def execute(self, _sql: str, params: tuple[Any, ...]) -> None:
        normalized_sql = " ".join(_sql.split())
        if (
            "MAX(aggregated_at)" in normalized_sql
            and "GROUP BY startdate" in normalized_sql
        ):
            (
                interval_min,
                aggregated_at_lower,
                aggregated_at_upper,
                startdate_upper,
                max_imputation_tier,
            ) = params
            self._connection.discovery_queries.append(
                (
                    int(interval_min),
                    str(aggregated_at_lower),
                    str(aggregated_at_upper),
                    str(startdate_upper),
                    int(max_imputation_tier),
                )
            )
            self._rows = list(
                self._connection.discovery_rows_by_interval.get(int(interval_min), [])
            )
            return

        if "startdate IN" in normalized_sql:
            interval_min = int(params[0])
            startdates = tuple(str(value) for value in params[1:-1])
            max_imputation_tier = int(params[-1])
            self._connection.startdate_queries.append(
                (interval_min, startdates, max_imputation_tier)
            )
            startdate_set = set(startdates)
            self._rows = [
                row
                for row in self._connection.startdate_rows_by_interval.get(
                    interval_min, []
                )
                if str(row["startdate"]) in startdate_set
                and int(row.get("imputation_tier", 0)) <= max_imputation_tier
            ]
            return

        interval_min, lower_bound, upper_bound, max_imputation_tier = params
        self._connection.queries.append(
            (interval_min, lower_bound, upper_bound, max_imputation_tier)
        )
        self._rows = [
            row
            for row in self._connection.rows_by_interval.get(interval_min, [])
            if int(row.get("imputation_tier", 0)) <= max_imputation_tier
        ]

    def fetchall(self) -> list[dict[str, Any]]:
        return list(self._rows)


class FakeDbConnection:
    def __init__(
        self,
        rows_by_interval: Mapping[int, Iterable[Mapping[str, Any]]] | None = None,
        *,
        startdate_rows_by_interval: Mapping[int, Iterable[Mapping[str, Any]]]
        | None = None,
        discovery_rows_by_interval: Mapping[int, Iterable[Mapping[str, Any]]]
        | None = None,
    ) -> None:
        self.rows_by_interval = {
            interval: [dict(row) for row in rows]
            for interval, rows in (rows_by_interval or {}).items()
        }
        self.startdate_rows_by_interval = (
            {
                interval: [dict(row) for row in rows]
                for interval, rows in startdate_rows_by_interval.items()
            }
            if startdate_rows_by_interval is not None
            else self.rows_by_interval
        )
        self.discovery_rows_by_interval = {
            interval: [dict(row) for row in rows]
            for interval, rows in (discovery_rows_by_interval or {}).items()
        }
        self.queries: list[tuple[int, str, str, int]] = []
        self.startdate_queries: list[tuple[int, tuple[str, ...], int]] = []
        self.discovery_queries: list[tuple[int, str, str, str, int]] = []
        self.close_calls = 0

    def cursor(self) -> FakeDbCursor:
        return FakeDbCursor(self)

    def close(self) -> None:
        self.close_calls += 1


class FakeOrionClient:
    def __init__(
        self,
        results: Iterable[Mapping[str, Any]] | None = None,
        *,
        payload_mode: str = "failure",
    ) -> None:
        self._results = [dict(result) for result in (results or [])]
        self.payload_mode = payload_mode
        self.calls: list[dict[str, Any]] = []
        self.list_entities_calls: list[dict[str, Any]] = []

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
        result = self._results.pop(0) if self._results else {"status": 204, "ok": True}
        return {
            "status": result.get("status", 204),
            "ok": result.get("ok", True),
            "attempts": result.get("attempts", 1),
            "elapsed_ms": result.get("elapsed_ms", 3),
            "body_excerpt": result.get("body_excerpt"),
            "dry_run": dry_run,
        }

    def list_entities(
        self,
        entity_type: str,
        *,
        attrs: str | None = None,
        limit: int = 1000,
    ) -> list[dict[str, str]]:
        self.list_entities_calls.append(
            {"entity_type": entity_type, "attrs": attrs, "limit": limit}
        )
        return []


def place(
    *,
    place_number: int = 10,
    interval_min: int = 60,
    batch: str = "2026",
    expected_device_type: str = "M5Stack",
    entity_type: str | None = None,
    entity_id: str | None = None,
    identifcation: str = "",
    active: bool = True,
) -> SensorPlace:
    resolved_type = entity_type or (
        "Blesensor.per300" if interval_min == 5 else "Blesensor.per3600"
    )
    resolved_id = entity_id or f"jp.sendai.{resolved_type}.{place_number}"
    return SensorPlace(
        place_number=place_number,
        batch=batch,
        expected_device_type=expected_device_type,
        interval_min=interval_min,
        entity_type=resolved_type,
        entity_id=resolved_id,
        identifcation=identifcation or str(place_number),
        active=active,
    )


def flow_row(**overrides: Any) -> dict[str, Any]:
    values: dict[str, Any] = {
        "startdate": "20260523_0900",
        "group_place_id": "sendai202603.10",
        "device_type": "M5Stack",
        "interval_min": 60,
        "flow_gt_m60": 6,
        "flow_gt_m80": 237,
        "flow_gt_m120": 430,
        "stay_gt_m60": Decimal("0.2"),
        "stay_gt_m80": Decimal("40.9"),
        "stay_gt_m120": Decimal("0.0"),
        "imputation_tier": 0,
    }
    values.update(overrides)
    return values


def run_settings(tmp_path: Path, **overrides: Any) -> RunFlowSettings:
    values: dict[str, Any] = {
        "send_mode": "dry-run",
        "reprocess_hours_per3600": 12,
        "reprocess_hours_per300": 2,
        "max_lookback_hours_per3600": 72,
        "max_lookback_hours_per300": 72,
        "source_stability_delay_hours": 3,
        "state_path": tmp_path / "state" / "flow.json",
        "lock_path": tmp_path / "state" / "flow.lock",
    }
    values.update(overrides)
    return RunFlowSettings(**values)


def filter_settings(
    target_flow_batches: Iterable[str] = ("2026",),
    target_direction_batches: Iterable[str] = (),
    ignored_place_prefixes: tuple[str, ...] = ("quick.", "test"),
    source_max_imputation_tier: int = 2,
) -> FilterSettings:
    return FilterSettings(
        target_flow_batches=frozenset(target_flow_batches),
        target_direction_batches=frozenset(target_direction_batches),
        ignored_place_prefixes=ignored_place_prefixes,
        source_max_imputation_tier=source_max_imputation_tier,
    )


def state_store(
    tmp_path: Path, now: Callable[[], datetime] | None = None
) -> WindowStateStore:
    return WindowStateStore(tmp_path / "state" / "flow.json", now=now or Clock([NOW]))


def run_once(
    *,
    tmp_path: Path,
    db_connection: FakeDbConnection,
    orion: FakeOrionClient,
    metadata: Iterable[SensorPlace],
    store: WindowStateStore | None = None,
    settings: RunFlowSettings | None = None,
    filters: FilterSettings | None = None,
    now: datetime = NOW,
) -> RunFlowResult:
    return run_product_flow(
        db_connection=db_connection,
        orion=orion,
        metadata=list(metadata),
        state_store=store or state_store(tmp_path, Clock([now])),
        settings=settings or run_settings(tmp_path),
        filter_settings=filters or filter_settings(),
        now=Clock([now]),
    )


def records(caplog: pytest.LogCaptureFixture, event: str) -> list[Any]:
    return [
        record for record in caplog.records if getattr(record, "event", None) == event
    ]


def entity_ids(places: Iterable[SensorPlace]) -> list[str]:
    return [target.entity_id for target in places]


def payload_hash(attrs: Mapping[str, Any]) -> str:
    return run_flow_module._attrs_sha256(attrs)


def transformed_hash(row: Mapping[str, Any], places: Iterable[SensorPlace]) -> str:
    payloads = transform_flow_rows(
        [row],
        index_by_place_interval(active_places(places, target_batches=["2026"])),
        transformed_at=NOW,
    )
    assert len(payloads) == 1
    return payload_hash(payloads[0]["attrs"])


def expected_product_a_attrs(
    row: Mapping[str, Any],
    target: SensorPlace,
    *,
    transformed_at: datetime = NOW,
) -> dict[str, Any]:
    observed_from = datetime.strptime(str(row["startdate"]), "%Y%m%d_%H%M").replace(
        tzinfo=JST
    )
    observed_to = observed_from + timedelta(minutes=int(row["interval_min"]))
    retrieved = transformed_at.astimezone(JST).replace(microsecond=0)
    metadata = {
        "TimeInstant": {
            "type": "DateTime",
            "value": observed_from.isoformat(),
        }
    }

    def attr(attr_type: str, value: Any) -> dict[str, Any]:
        return {"type": attr_type, "value": value, "metadata": deepcopy(metadata)}

    return {
        "dateObservedFrom": attr("DateTime", observed_from.isoformat()),
        "dateObservedTo": attr("DateTime", observed_to.isoformat()),
        "dateRetrieved": attr("DateTime", retrieved.isoformat()),
        "identifcation": attr("Text", target.entity_id),
        "peopleCount_immedate": attr("number", row["flow_gt_m60"]),
        "peopleCount_near": attr("number", row["flow_gt_m80"]),
        "peopleCount_far": attr("number", row["flow_gt_m120"]),
        "peopleOccupancy_immedate": attr(
            "number",
            None if row["stay_gt_m60"] is None else float(row["stay_gt_m60"]),
        ),
        "peopleOccupancy_near": attr(
            "number",
            None if row["stay_gt_m80"] is None else float(row["stay_gt_m80"]),
        ),
        "peopleOccupancy_far": attr(
            "number",
            None if row["stay_gt_m120"] is None else float(row["stay_gt_m120"]),
        ),
    }


def write_state(
    path: Path,
    windows: Mapping[str, Mapping[str, Any]],
    *,
    last_aggregated_at: str | None = None,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    state: dict[str, Any] = {"windows": windows}
    if last_aggregated_at is not None:
        state["last_aggregated_at"] = last_aggregated_at
    path.write_text(
        json.dumps(state, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )


def window_record(
    *,
    first_seen: datetime,
    last_attempt: datetime,
    status: str,
    targets: Mapping[str, Mapping[str, Any]] | None = None,
    attempt_count: int = 1,
) -> dict[str, Any]:
    return {
        "first_seen": first_seen.isoformat(),
        "last_attempt": last_attempt.isoformat(),
        "attempt_count": attempt_count,
        "targets": dict(targets or {}),
        "status": status,
    }


def target_record(
    *,
    status: str,
    payload_sha256: str,
    http_status: int = 204,
    last_attempt_at: datetime = NOW,
) -> dict[str, Any]:
    return {
        "status": status,
        "last_attempt_at": last_attempt_at.isoformat(),
        "last_http_status": http_status,
        "last_payload_sha256": payload_sha256,
    }


def forbid_state_mutations(
    monkeypatch: pytest.MonkeyPatch,
    store: WindowStateStore,
) -> None:
    def fail(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("dry-run must not mutate state")

    monkeypatch.setattr(store, "begin_window_attempt", fail)
    monkeypatch.setattr(store, "record_target", fail)
    monkeypatch.setattr(store, "recompute_status", fail)
    monkeypatch.setattr(store, "gc_complete_before", fail)
    monkeypatch.setattr(store, "save", fail)


def set_required_main_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("FLOW_SEND_MODE", "dry-run")
    monkeypatch.setenv("TARGET_FLOW_BATCHES", "2026")
    monkeypatch.setenv(
        "SENSOR_METADATA_PATH", str(tmp_path / "metadata" / "sensors.csv")
    )
    monkeypatch.setenv("MYSQL_HOST", "db.example.test")
    monkeypatch.setenv("MYSQL_USER", "reader")
    monkeypatch.setenv("MYSQL_PASSWORD", "secret")
    monkeypatch.setenv("MYSQL_DATABASE", "bleData2025d")
    monkeypatch.setenv("FIWARE_BASE_URL", "https://fiware.example.test")
    monkeypatch.setenv("FIWARE_CONSUMER_KEY", "key")
    monkeypatch.setenv("FIWARE_CONSUMER_SECRET", "secret")


def test_from_env_defaults_to_dry_run() -> None:
    settings = RunFlowSettings.from_env({})

    assert settings.send_mode == "dry-run"
    assert settings.reprocess_hours_per3600 == 12
    assert settings.reprocess_hours_per300 == 2
    assert settings.max_lookback_hours_per3600 == 72
    assert settings.max_lookback_hours_per300 == 72
    assert settings.source_stability_delay_hours == 3
    assert settings.state_path == Path("state/flow.json")
    assert settings.lock_path == Path("state/flow.lock")


def test_from_env_accepts_send_mode_and_numeric_overrides() -> None:
    settings = RunFlowSettings.from_env(
        {
            "FLOW_SEND_MODE": "send",
            "REPROCESS_HOURS_PER3600": "4",
            "REPROCESS_HOURS_PER300": "1",
            "MAX_LOOKBACK_HOURS_PER3600": "12",
            "MAX_LOOKBACK_HOURS_PER300": "3",
            "SOURCE_STABILITY_DELAY_HOURS": "6",
        }
    )

    assert settings.send_mode == "send"
    assert settings.reprocess_hours_per3600 == 4
    assert settings.reprocess_hours_per300 == 1
    assert settings.max_lookback_hours_per3600 == 12
    assert settings.max_lookback_hours_per300 == 3
    assert settings.source_stability_delay_hours == 6


def test_from_env_rejects_negative_source_stability_delay() -> None:
    with pytest.raises(RunFlowConfigError):
        RunFlowSettings.from_env({"SOURCE_STABILITY_DELAY_HOURS": "-1"})


def test_from_env_rejects_invalid_send_mode() -> None:
    with pytest.raises(RunFlowConfigError):
        RunFlowSettings.from_env({"FLOW_SEND_MODE": "yes"})


def test_run_flow_settings_is_frozen(tmp_path: Path) -> None:
    settings = run_settings(tmp_path)

    with pytest.raises(FrozenInstanceError):
        settings.send_mode = "send"  # pyright: ignore[reportAttributeAccessIssue]


def test_run_flow_dry_run_default_never_live_posts(tmp_path: Path) -> None:
    rows = [
        flow_row(group_place_id="sendai202603.10"),
        flow_row(group_place_id="sendai202603.11"),
    ]
    targets = [place(place_number=10), place(place_number=11)]
    db_connection = FakeDbConnection({60: rows})
    orion = FakeOrionClient()

    result = run_once(
        tmp_path=tmp_path,
        db_connection=db_connection,
        orion=orion,
        metadata=targets,
    )

    assert result.exit_code == 0
    assert [call["dry_run"] for call in orion.calls] == [True, True]
    assert all(call["dry_run"] is True for call in orion.calls)


def test_run_flow_reuses_one_top_level_timestamp_for_five_and_sixty_minute_payloads(
    tmp_path: Path,
) -> None:
    rows_60 = [flow_row()]
    rows_5 = [
        flow_row(
            group_place_id="sendai202603.99",
            interval_min=5,
            startdate="20260523_0900",
        )
    ]
    target_60 = place()
    target_5 = place(place_number=99, interval_min=5)
    orion = FakeOrionClient()

    result = run_once(
        tmp_path=tmp_path,
        db_connection=FakeDbConnection({5: rows_5, 60: rows_60}),
        orion=orion,
        metadata=[target_5, target_60],
        now=NON_JST_RUN_START,
    )

    assert result.exit_code == 0
    assert {call["entity_id"] for call in orion.calls} == {
        target_5.entity_id,
        target_60.entity_id,
    }
    assert {call["attrs"]["dateRetrieved"]["value"] for call in orion.calls} == {
        "2026-05-23T12:17:59+09:00"
    }
    assert all(list(call["attrs"]) == PRODUCT_A_ATTR_NAMES for call in orion.calls)


def test_run_flow_dry_run_processes_rows_without_state_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "state" / "flow.json"
    write_state(
        path,
        {
            "per3600/20260523_0800": window_record(
                first_seen=NOW - timedelta(hours=1),
                last_attempt=NOW - timedelta(hours=1),
                status="partial",
                targets={},
            )
        },
    )
    store = WindowStateStore.load(path, now=Clock([NOW]))
    before_memory = deepcopy(store.as_dict())
    before_disk = path.read_text(encoding="utf-8")
    forbid_state_mutations(monkeypatch, store)
    db_connection = FakeDbConnection({60: [flow_row()]})
    orion = FakeOrionClient()

    result = run_once(
        tmp_path=tmp_path,
        db_connection=db_connection,
        orion=orion,
        metadata=[place()],
        store=store,
    )

    assert result.exit_code == 0
    assert path.read_text(encoding="utf-8") == before_disk
    assert store.as_dict() == before_memory
    assert [call["dry_run"] for call in orion.calls] == [True]
    [call] = orion.calls
    assert list(call["attrs"]) == PRODUCT_A_ATTR_NAMES
    assert call["attrs"]["dateRetrieved"]["value"] == "2026-05-23T12:17:00+09:00"
    assert call["attrs"]["identifcation"]["value"] == place().entity_id
    assert call["attrs"]["peopleOccupancy_far"]["value"] == 0.0


def test_run_flow_empty_target_batches_short_circuits_without_side_effects(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    store = state_store(tmp_path)
    forbid_state_mutations(monkeypatch, store)
    db_connection = FakeDbConnection({60: [flow_row()]})
    orion = FakeOrionClient()

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline"):
        result = run_once(
            tmp_path=tmp_path,
            db_connection=db_connection,
            orion=orion,
            metadata=[place()],
            store=store,
            filters=filter_settings(target_flow_batches=()),
        )

    assert result.exit_code == 0
    assert result.windows_seen == 0
    assert result.posts_ok == 0
    assert result.posts_failed == 0
    assert db_connection.queries == []
    assert orion.calls == []
    assert not store.path.exists()

    started = records(caplog, "run_started")
    summary = records(caplog, "run_summary")
    assert len(started) == 1
    assert started[0].target_batches == []
    assert len(summary) == 1
    assert summary[0].windows_seen == 0
    assert summary[0].windows_complete == 0
    assert summary[0].windows_partial == 0
    assert summary[0].posts_ok == 0
    assert summary[0].posts_failed == 0


def test_run_flow_target_batch_typo_raises_before_sql(tmp_path: Path) -> None:
    db_connection = FakeDbConnection({60: [flow_row()]})
    orion = FakeOrionClient()

    with pytest.raises(FilterConfigError):
        run_once(
            tmp_path=tmp_path,
            db_connection=db_connection,
            orion=orion,
            metadata=[place(batch="2026")],
            settings=run_settings(tmp_path, send_mode="send"),
            filters=filter_settings(target_flow_batches=("2025",)),
        )

    assert db_connection.queries == []
    assert orion.calls == []


def test_run_flow_send_mode_posts_all_targets_and_completes_window(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    targets = [place(place_number=10), place(place_number=11)]
    rows = [
        flow_row(group_place_id="sendai202603.10"),
        flow_row(group_place_id="sendai202603.11"),
    ]
    store = state_store(tmp_path, Clock([NOW] * 10))
    db_connection = FakeDbConnection({60: rows})
    orion = FakeOrionClient()

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline"):
        result = run_once(
            tmp_path=tmp_path,
            db_connection=db_connection,
            orion=orion,
            metadata=targets,
            store=store,
            settings=run_settings(tmp_path, send_mode="send"),
        )

    assert result.exit_code == 0
    assert result.windows_seen == 1
    assert result.windows_complete == 1
    assert result.windows_partial == 0
    assert result.posts_ok == 2
    assert result.posts_failed == 0
    assert [call["dry_run"] for call in orion.calls] == [False, False]
    window = store.as_dict()["windows"]["per3600/20260523_0900"]
    assert window["status"] == "complete"
    assert sorted(window["targets"]) == sorted(entity_ids(targets))
    assert len(records(caplog, "window_complete")) == 1


def test_process_send_window_default_saves_per_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    targets = [place(place_number=10), place(place_number=11)]
    rows = [
        flow_row(group_place_id="sendai202603.10"),
        flow_row(group_place_id="sendai202603.11"),
    ]
    store = state_store(tmp_path, Clock([NOW] * 10))
    save_count = 0
    real_save = store.save

    def save_spy() -> None:
        nonlocal save_count
        save_count += 1
        real_save()

    monkeypatch.setattr(store, "save", save_spy)

    run_flow_module._process_send_window(
        "per3600/20260523_0900",
        interval_min=60,
        startdate="20260523_0900",
        rows_for_window=cast(Any, rows),
        orion=FakeOrionClient(),
        state_store=store,
        filter_settings=filter_settings(),
        interval_metadata=index_by_place_interval(
            active_places(targets, target_batches=["2026"])
        ),
        expected_target_ids=(),
        counts=run_flow_module._RunCounts(),
        transformed_at=NOW,
    )

    assert save_count == 2


def test_process_send_window_no_persist_zero_in_loop_saves(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    targets = [place(place_number=10), place(place_number=11)]
    rows = [
        flow_row(group_place_id="sendai202603.10"),
        flow_row(group_place_id="sendai202603.11"),
    ]
    store = state_store(tmp_path, Clock([NOW] * 10))
    save_count = 0

    def save_spy() -> None:
        nonlocal save_count
        save_count += 1

    monkeypatch.setattr(store, "save", save_spy)

    run_flow_module._process_send_window(
        "per3600/20260523_0900",
        interval_min=60,
        startdate="20260523_0900",
        rows_for_window=cast(Any, rows),
        orion=FakeOrionClient(results=[{"status": 502, "ok": False}]),
        state_store=store,
        filter_settings=filter_settings(),
        interval_metadata=index_by_place_interval(
            active_places(targets, target_batches=["2026"])
        ),
        expected_target_ids=(),
        counts=run_flow_module._RunCounts(),
        transformed_at=NOW,
        persist_each_target=False,
    )

    window = store.as_dict()["windows"]["per3600/20260523_0900"]
    assert save_count == 0
    assert window["status"] == "partial"
    assert sorted(window["targets"]) == sorted(entity_ids(targets))
    assert window["targets"][targets[0].entity_id]["status"] == "failed"
    assert window["targets"][targets[1].entity_id]["status"] == "ok"


def test_process_send_window_final_state_parity(tmp_path: Path) -> None:
    targets = [place(place_number=10), place(place_number=11)]
    rows = [
        flow_row(group_place_id="sendai202603.10"),
        flow_row(group_place_id="sendai202603.11"),
    ]
    interval_metadata = index_by_place_interval(
        active_places(targets, target_batches=["2026"])
    )
    default_store = WindowStateStore(
        tmp_path / "default" / "flow.json",
        now=Clock([NOW] * 10),
    )
    deferred_store = WindowStateStore(
        tmp_path / "deferred" / "flow.json",
        now=Clock([NOW] * 10),
    )

    run_flow_module._process_send_window(
        "per3600/20260523_0900",
        interval_min=60,
        startdate="20260523_0900",
        rows_for_window=cast(Any, rows),
        orion=FakeOrionClient(results=[{"status": 502, "ok": False}]),
        state_store=default_store,
        filter_settings=filter_settings(),
        interval_metadata=interval_metadata,
        expected_target_ids=(),
        counts=run_flow_module._RunCounts(),
        transformed_at=NOW,
    )
    default_store.save()

    run_flow_module._process_send_window(
        "per3600/20260523_0900",
        interval_min=60,
        startdate="20260523_0900",
        rows_for_window=cast(Any, rows),
        orion=FakeOrionClient(results=[{"status": 502, "ok": False}]),
        state_store=deferred_store,
        filter_settings=filter_settings(),
        interval_metadata=interval_metadata,
        expected_target_ids=(),
        counts=run_flow_module._RunCounts(),
        transformed_at=NOW,
        persist_each_target=False,
    )
    deferred_store.save()

    assert json.loads(default_store.path.read_text(encoding="utf-8")) == json.loads(
        deferred_store.path.read_text(encoding="utf-8")
    )


def test_run_flow_completes_new_window_with_observed_targets_only(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    targets = [
        place(place_number=10),
        place(place_number=11),
        place(place_number=12),
        place(place_number=13),
    ]
    observed_targets = targets[:2]
    rows = [
        flow_row(group_place_id="sendai202603.10"),
        flow_row(group_place_id="sendai202603.11"),
    ]
    store = state_store(tmp_path, Clock([NOW] * 10))
    orion = FakeOrionClient()

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline"):
        result = run_once(
            tmp_path=tmp_path,
            db_connection=FakeDbConnection({60: rows}),
            orion=orion,
            metadata=targets,
            store=store,
            settings=run_settings(tmp_path, send_mode="send"),
        )

    assert result.exit_code == 0
    assert result.windows_seen == 1
    assert result.windows_complete == 1
    assert result.windows_partial == 0
    assert result.posts_ok == 2
    assert result.posts_failed == 0
    assert [call["entity_id"] for call in orion.calls] == entity_ids(observed_targets)
    window = store.as_dict()["windows"]["per3600/20260523_0900"]
    assert window["status"] == "complete"
    assert window["expected_target_ids"] == entity_ids(observed_targets)
    assert sorted(window["targets"]) == entity_ids(observed_targets)
    assert records(caplog, "window_expected_targets_changed") == []


def test_run_flow_expands_complete_window_when_target_observed_posts_only_new_target(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    ok_target = place(place_number=10)
    prior_target = place(place_number=11)
    new_target = place(place_number=12)
    ok_row = flow_row(group_place_id="sendai202603.10")
    prior_row = flow_row(group_place_id="sendai202603.11")
    new_row = flow_row(group_place_id="sendai202603.12")
    ok_hash = transformed_hash(ok_row, [ok_target])
    prior_hash = transformed_hash(prior_row, [prior_target])
    path = tmp_path / "state" / "flow.json"
    window = window_record(
        first_seen=NOW - timedelta(hours=1),
        last_attempt=NOW - timedelta(hours=1),
        status="complete",
        targets={
            ok_target.entity_id: target_record(
                status="ok",
                payload_sha256=ok_hash,
                last_attempt_at=NOW - timedelta(hours=1),
            ),
            prior_target.entity_id: target_record(
                status="ok",
                payload_sha256=prior_hash,
                last_attempt_at=NOW - timedelta(hours=1),
            ),
        },
    )
    window["expected_target_ids"] = entity_ids([ok_target, prior_target])
    write_state(path, {"per3600/20260523_0900": window})
    store = WindowStateStore.load(path, now=Clock([NOW] * 10))
    orion = FakeOrionClient()

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline"):
        result = run_once(
            tmp_path=tmp_path,
            db_connection=FakeDbConnection({60: [ok_row, prior_row, new_row]}),
            orion=orion,
            metadata=[ok_target, prior_target, new_target],
            store=store,
            settings=run_settings(tmp_path, send_mode="send"),
        )

    assert result.exit_code == 0
    assert result.windows_complete == 1
    assert result.windows_partial == 0
    assert result.posts_ok == 1
    assert result.posts_failed == 0
    assert [call["entity_id"] for call in orion.calls] == [new_target.entity_id]
    assert orion.calls[0]["attrs"]["dateRetrieved"]["value"] == (
        "2026-05-23T12:17:00+09:00"
    )
    window = store.as_dict()["windows"]["per3600/20260523_0900"]
    assert window["status"] == "complete"
    assert window["expected_target_ids"] == entity_ids(
        [ok_target, prior_target, new_target]
    )
    ok_record = store.target_record("per3600/20260523_0900", ok_target.entity_id)
    prior_record = store.target_record("per3600/20260523_0900", prior_target.entity_id)
    assert ok_record is not None
    assert prior_record is not None
    assert ok_record["last_payload_sha256"] == ok_hash
    assert prior_record["last_payload_sha256"] == prior_hash
    new_record = store.target_record("per3600/20260523_0900", new_target.entity_id)
    assert new_record is not None
    assert new_record["status"] == "ok"
    skipped = records(caplog, "post_skipped_unchanged")
    assert [record.entity_id for record in skipped] == entity_ids(
        [ok_target, prior_target]
    )


def test_run_flow_keeps_expanded_window_partial_until_new_target_retry_succeeds(
    tmp_path: Path,
) -> None:
    ok_target = place(place_number=10)
    prior_target = place(place_number=11)
    new_target = place(place_number=12)
    ok_row = flow_row(group_place_id="sendai202603.10")
    prior_row = flow_row(group_place_id="sendai202603.11")
    new_row = flow_row(group_place_id="sendai202603.12")
    path = tmp_path / "state" / "flow.json"
    window = window_record(
        first_seen=NOW - timedelta(hours=1),
        last_attempt=NOW - timedelta(hours=1),
        status="complete",
        targets={
            ok_target.entity_id: target_record(
                status="ok",
                payload_sha256=transformed_hash(ok_row, [ok_target]),
                last_attempt_at=NOW - timedelta(hours=1),
            ),
            prior_target.entity_id: target_record(
                status="ok",
                payload_sha256=transformed_hash(prior_row, [prior_target]),
                last_attempt_at=NOW - timedelta(hours=1),
            ),
        },
    )
    window["expected_target_ids"] = entity_ids([ok_target, prior_target])
    write_state(path, {"per3600/20260523_0900": window})
    store = WindowStateStore.load(path, now=Clock([NOW] * 20))
    rows = [ok_row, prior_row, new_row]
    metadata = [ok_target, prior_target, new_target]

    failed_result = run_once(
        tmp_path=tmp_path,
        db_connection=FakeDbConnection({60: rows}),
        orion=FakeOrionClient(results=[{"status": 502, "ok": False}]),
        metadata=metadata,
        store=store,
        settings=run_settings(tmp_path, send_mode="send"),
    )

    assert failed_result.exit_code == 1
    assert failed_result.windows_complete == 0
    assert failed_result.windows_partial == 1
    assert failed_result.posts_ok == 0
    assert failed_result.posts_failed == 1
    window = store.as_dict()["windows"]["per3600/20260523_0900"]
    assert window["status"] == "partial"
    assert window["expected_target_ids"] == entity_ids(metadata)
    failed_record = store.target_record("per3600/20260523_0900", new_target.entity_id)
    assert failed_record is not None
    assert failed_record["status"] == "failed"
    assert failed_record["last_http_status"] == 502

    retry_orion = FakeOrionClient()
    retried_result = run_once(
        tmp_path=tmp_path,
        db_connection=FakeDbConnection({60: rows}),
        orion=retry_orion,
        metadata=metadata,
        store=store,
        settings=run_settings(tmp_path, send_mode="send"),
    )

    assert retried_result.exit_code == 0
    assert retried_result.windows_complete == 1
    assert retried_result.windows_partial == 0
    assert retried_result.posts_ok == 1
    assert retried_result.posts_failed == 0
    assert [call["entity_id"] for call in retry_orion.calls] == [new_target.entity_id]
    assert store.as_dict()["windows"]["per3600/20260523_0900"]["status"] == "complete"
    retried_record = store.target_record("per3600/20260523_0900", new_target.entity_id)
    assert retried_record is not None
    assert retried_record["status"] == "ok"


def test_run_flow_skips_state_for_new_window_when_all_rows_drop(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    rows = [
        flow_row(group_place_id="quick.10"),
        flow_row(group_place_id="sendai202603.10", device_type="Pixel3aUT"),
    ]
    store = state_store(tmp_path, Clock([NOW] * 10))
    begin_calls: list[str] = []
    real_begin = store.begin_window_attempt

    def begin_spy(window_key: str, **kwargs: Any) -> None:
        begin_calls.append(window_key)
        real_begin(window_key, **kwargs)

    monkeypatch.setattr(store, "begin_window_attempt", begin_spy)
    orion = FakeOrionClient()

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline"):
        result = run_once(
            tmp_path=tmp_path,
            db_connection=FakeDbConnection({60: rows}),
            orion=orion,
            metadata=[place(place_number=10)],
            store=store,
            settings=run_settings(tmp_path, send_mode="send"),
        )

    assert result.exit_code == 0
    assert result.windows_seen == 1
    assert result.windows_complete == 0
    assert result.windows_partial == 0
    assert result.posts_ok == 0
    assert result.posts_failed == 0
    assert result.rows_dropped == 2
    assert orion.calls == []
    assert begin_calls == []
    assert store.as_dict()["windows"] == {}
    assert records(caplog, "window_complete") == []
    assert records(caplog, "window_partial") == []


def test_run_flow_keeps_complete_window_when_observed_subset_stored_skips_posts(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    observed_target = place(place_number=10)
    stored_only_target = place(place_number=11)
    observed_row = flow_row(group_place_id="sendai202603.10")
    stored_only_row = flow_row(group_place_id="sendai202603.11")
    observed_hash = transformed_hash(observed_row, [observed_target])
    stored_only_hash = transformed_hash(stored_only_row, [stored_only_target])
    path = tmp_path / "state" / "flow.json"
    window = window_record(
        first_seen=NOW - timedelta(hours=1),
        last_attempt=NOW - timedelta(hours=1),
        status="complete",
        targets={
            observed_target.entity_id: target_record(
                status="ok",
                payload_sha256=observed_hash,
                last_attempt_at=NOW - timedelta(hours=1),
            ),
            stored_only_target.entity_id: target_record(
                status="ok",
                payload_sha256=stored_only_hash,
                last_attempt_at=NOW - timedelta(hours=1),
            ),
        },
    )
    window["expected_target_ids"] = entity_ids([observed_target, stored_only_target])
    write_state(path, {"per3600/20260523_0900": window})
    store = WindowStateStore.load(path, now=Clock([NOW] * 10))
    orion = FakeOrionClient()

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline"):
        result = run_once(
            tmp_path=tmp_path,
            db_connection=FakeDbConnection({60: [observed_row]}),
            orion=orion,
            metadata=[observed_target, stored_only_target],
            store=store,
            settings=run_settings(tmp_path, send_mode="send"),
        )

    assert result.exit_code == 0
    assert result.windows_complete == 1
    assert result.windows_partial == 0
    assert result.posts_ok == 0
    assert result.posts_failed == 0
    assert orion.calls == []
    window = store.as_dict()["windows"]["per3600/20260523_0900"]
    assert window["status"] == "complete"
    assert window["expected_target_ids"] == entity_ids(
        [observed_target, stored_only_target]
    )
    assert records(caplog, "window_expected_targets_changed") == []
    skipped = records(caplog, "post_skipped_unchanged")
    assert len(skipped) == 1
    assert skipped[0].entity_id == observed_target.entity_id


def test_run_flow_supplemental_expands_complete_window_when_late_target_appears(
    tmp_path: Path,
) -> None:
    ok_target = place(place_number=10)
    prior_target = place(place_number=11)
    new_target = place(place_number=12)
    startdate = "20260522_0800"
    window_key = f"per3600/{startdate}"
    ok_row = flow_row(startdate=startdate, group_place_id="sendai202603.10")
    prior_row = flow_row(startdate=startdate, group_place_id="sendai202603.11")
    new_row = flow_row(startdate=startdate, group_place_id="sendai202603.12")
    ok_hash = transformed_hash(ok_row, [ok_target])
    prior_hash = transformed_hash(prior_row, [prior_target])
    path = tmp_path / "state" / "flow.json"
    window = window_record(
        first_seen=NOW - timedelta(hours=30),
        last_attempt=NOW - timedelta(hours=30),
        status="complete",
        targets={
            ok_target.entity_id: target_record(
                status="ok",
                payload_sha256=ok_hash,
                last_attempt_at=NOW - timedelta(hours=30),
            ),
            prior_target.entity_id: target_record(
                status="ok",
                payload_sha256=prior_hash,
                last_attempt_at=NOW - timedelta(hours=30),
            ),
        },
    )
    window["expected_target_ids"] = entity_ids([ok_target, prior_target])
    write_state(path, {window_key: window})
    store = WindowStateStore.load(path, now=Clock([NOW] * 20))
    db_connection = FakeDbConnection(
        {60: []},
        startdate_rows_by_interval={60: [ok_row, prior_row, new_row]},
    )
    orion = FakeOrionClient()

    result = run_once(
        tmp_path=tmp_path,
        db_connection=db_connection,
        orion=orion,
        metadata=[ok_target, prior_target, new_target],
        store=store,
        settings=run_settings(tmp_path, send_mode="send"),
    )

    assert result.exit_code == 0
    assert result.windows_seen == 1
    assert result.windows_complete == 1
    assert result.windows_partial == 0
    assert result.posts_ok == 1
    assert result.posts_failed == 0
    assert db_connection.startdate_queries == [(60, (startdate,), 2)]
    assert [call["entity_id"] for call in orion.calls] == [new_target.entity_id]
    assert orion.calls[0]["attrs"]["dateRetrieved"]["value"] == (
        "2026-05-23T12:17:00+09:00"
    )
    window = store.as_dict()["windows"][window_key]
    assert window["status"] == "complete"
    assert window["expected_target_ids"] == entity_ids(
        [ok_target, prior_target, new_target]
    )
    ok_record = store.target_record(window_key, ok_target.entity_id)
    prior_record = store.target_record(window_key, prior_target.entity_id)
    new_record = store.target_record(window_key, new_target.entity_id)
    assert ok_record is not None
    assert prior_record is not None
    assert new_record is not None
    assert ok_record["last_payload_sha256"] == ok_hash
    assert prior_record["last_payload_sha256"] == prior_hash
    assert new_record["status"] == "ok"


def test_run_flow_supplemental_noops_when_late_rows_have_no_new_target(
    tmp_path: Path,
) -> None:
    ok_target = place(place_number=10)
    prior_target = place(place_number=11)
    startdate = "20260522_0800"
    window_key = f"per3600/{startdate}"
    ok_row = flow_row(startdate=startdate, group_place_id="sendai202603.10")
    prior_row = flow_row(startdate=startdate, group_place_id="sendai202603.11")
    dropped_row = flow_row(
        startdate=startdate,
        group_place_id="sendai202603.10",
        device_type="Pixel3aUT",
    )
    ok_hash = transformed_hash(ok_row, [ok_target])
    prior_hash = transformed_hash(prior_row, [prior_target])
    path = tmp_path / "state" / "flow.json"
    window = window_record(
        first_seen=NOW - timedelta(hours=30),
        last_attempt=NOW - timedelta(hours=30),
        status="complete",
        targets={
            ok_target.entity_id: target_record(
                status="ok",
                payload_sha256=ok_hash,
                last_attempt_at=NOW - timedelta(hours=30),
            ),
            prior_target.entity_id: target_record(
                status="ok",
                payload_sha256=prior_hash,
                last_attempt_at=NOW - timedelta(hours=30),
            ),
        },
    )
    window["expected_target_ids"] = entity_ids([ok_target, prior_target])
    write_state(path, {window_key: window})
    store = WindowStateStore.load(path, now=Clock([NOW] * 20))
    before_window = deepcopy(store.as_dict()["windows"][window_key])
    db_connection = FakeDbConnection(
        {60: []},
        startdate_rows_by_interval={60: [ok_row, prior_row, dropped_row]},
    )
    orion = FakeOrionClient()

    result = run_once(
        tmp_path=tmp_path,
        db_connection=db_connection,
        orion=orion,
        metadata=[ok_target, prior_target],
        store=store,
        settings=run_settings(tmp_path, send_mode="send"),
    )

    assert result.exit_code == 0
    assert result.windows_seen == 0
    assert result.windows_complete == 0
    assert result.windows_partial == 0
    assert result.posts_ok == 0
    assert result.posts_failed == 0
    assert result.rows_dropped == 0
    assert db_connection.startdate_queries == [(60, (startdate,), 2)]
    assert orion.calls == []
    assert store.as_dict()["windows"][window_key] == before_window


def test_run_flow_supplemental_skips_query_when_no_complete_window_eligible(
    tmp_path: Path,
) -> None:
    target = place(place_number=10)
    normal_startdate = "20260523_0500"
    too_old_startdate = "20260519_0800"
    normal_row = flow_row(startdate=normal_startdate)
    too_old_row = flow_row(startdate=too_old_startdate)
    path = tmp_path / "state" / "flow.json"
    normal_window = window_record(
        first_seen=NOW - timedelta(hours=8),
        last_attempt=NOW - timedelta(hours=8),
        status="complete",
        targets={
            target.entity_id: target_record(
                status="ok",
                payload_sha256=transformed_hash(normal_row, [target]),
                last_attempt_at=NOW - timedelta(hours=8),
            )
        },
    )
    normal_window["expected_target_ids"] = [target.entity_id]
    too_old_window = window_record(
        first_seen=NOW - timedelta(hours=100),
        last_attempt=NOW - timedelta(hours=100),
        status="complete",
        targets={
            target.entity_id: target_record(
                status="ok",
                payload_sha256=transformed_hash(too_old_row, [target]),
                last_attempt_at=NOW - timedelta(hours=100),
            )
        },
    )
    too_old_window["expected_target_ids"] = [target.entity_id]
    write_state(
        path,
        {
            f"per3600/{normal_startdate}": normal_window,
            f"per3600/{too_old_startdate}": too_old_window,
        },
    )
    store = WindowStateStore.load(path, now=Clock([NOW] * 10))
    db_connection = FakeDbConnection(
        {60: []},
        startdate_rows_by_interval={60: [normal_row, too_old_row]},
    )

    orion = FakeOrionClient()
    result = run_once(
        tmp_path=tmp_path,
        db_connection=db_connection,
        orion=orion,
        metadata=[target],
        store=store,
        settings=run_settings(tmp_path, send_mode="send"),
    )

    assert result.exit_code == 0
    assert result.windows_seen == 0
    assert result.posts_ok == 0
    assert db_connection.startdate_queries == []


def test_run_flow_supplemental_skips_query_in_dry_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = place(place_number=10)
    startdate = "20260522_0800"
    row = flow_row(startdate=startdate)
    path = tmp_path / "state" / "flow.json"
    window = window_record(
        first_seen=NOW - timedelta(hours=30),
        last_attempt=NOW - timedelta(hours=30),
        status="complete",
        targets={
            target.entity_id: target_record(
                status="ok",
                payload_sha256=transformed_hash(row, [target]),
                last_attempt_at=NOW - timedelta(hours=30),
            )
        },
    )
    window["expected_target_ids"] = [target.entity_id]
    write_state(path, {f"per3600/{startdate}": window})
    store = WindowStateStore.load(path, now=Clock([NOW]))
    forbid_state_mutations(monkeypatch, store)
    db_connection = FakeDbConnection(
        {60: []},
        startdate_rows_by_interval={60: [row]},
    )

    result = run_once(
        tmp_path=tmp_path,
        db_connection=db_connection,
        orion=FakeOrionClient(),
        metadata=[target],
        store=store,
    )

    assert result.exit_code == 0
    assert result.windows_seen == 0
    assert result.posts_ok == 0
    assert db_connection.startdate_queries == []


def test_run_flow_failed_post_records_partial_and_nonzero_exit(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    store = state_store(tmp_path, Clock([NOW] * 10))
    db_connection = FakeDbConnection({60: [flow_row()]})
    orion = FakeOrionClient(results=[{"status": 502, "ok": False}])

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline"):
        result = run_once(
            tmp_path=tmp_path,
            db_connection=db_connection,
            orion=orion,
            metadata=[place()],
            store=store,
            settings=run_settings(tmp_path, send_mode="send"),
        )

    assert result.exit_code == 1
    assert result.windows_seen == 1
    assert result.windows_complete == 0
    assert result.windows_partial == 1
    assert result.posts_ok == 0
    assert result.posts_failed == 1
    target = store.target_record(
        "per3600/20260523_0900", "jp.sendai.Blesensor.per3600.10"
    )
    assert target is not None
    assert target["status"] == "failed"
    assert target["last_http_status"] == 502

    partials = records(caplog, "window_partial")
    assert len(partials) == 1
    assert partials[0].levelname == "WARNING"
    assert partials[0].window == "per3600/20260523_0900"


def test_run_flow_force_resend_reposts_unchanged_ok_target(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """force_resend=True must bypass the prior-ok hash-skip and re-POST.

    Mirrors test_run_flow_skips_unchanged_ok_target_and_keeps_window_complete
    but calls _process_send_window directly with force_resend=True and
    asserts Orion is hit. Public surface (run_once) does not expose the
    kwarg; the scripts/resend.py CLI is the only caller that flips it.
    """
    row = flow_row()
    target = place()
    hash_value = transformed_hash(row, [target])
    path = tmp_path / "state" / "flow.json"
    write_state(
        path,
        {
            "per3600/20260523_0900": window_record(
                first_seen=NOW - timedelta(hours=1),
                last_attempt=NOW - timedelta(hours=1),
                status="complete",
                targets={
                    target.entity_id: target_record(
                        status="ok",
                        payload_sha256=hash_value,
                        last_attempt_at=NOW - timedelta(hours=1),
                    )
                },
            )
        },
    )
    store = WindowStateStore.load(path, now=Clock([NOW] * 10))
    orion = FakeOrionClient()
    interval_metadata = index_by_place_interval(
        active_places([target], target_batches=("2026",))
    )
    counts = run_flow_module._RunCounts()

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline"):
        run_flow_module._process_send_window(
            "per3600/20260523_0900",
            interval_min=60,
            startdate="20260523_0900",
            rows_for_window=[row],
            orion=orion,
            state_store=store,
            filter_settings=FilterSettings(
                ignored_place_prefixes=("quick.", "test"),
                target_flow_batches=frozenset({"2026"}),
                target_direction_batches=frozenset(),
            ),
            interval_metadata=interval_metadata,
            expected_target_ids=[target.entity_id],
            counts=counts,
            transformed_at=NOW,
            force_resend=True,
        )

    assert len(orion.calls) == 1
    assert orion.calls[0]["entity_id"] == target.entity_id
    # No skip event should have fired.
    assert records(caplog, "post_skipped_unchanged") == []
    assert records(caplog, "post_skipped_drift") == []


def test_attrs_sha256_excludes_date_retrieved() -> None:
    target = place()
    attrs = expected_product_a_attrs(flow_row(), target)
    changed_retrieval = deepcopy(attrs)
    changed_retrieval["dateRetrieved"]["value"] = "2026-05-24T12:17:00+09:00"

    assert run_flow_module._attrs_sha256(attrs) == run_flow_module._attrs_sha256(
        changed_retrieval
    )


@pytest.mark.parametrize(
    "attr_name",
    [
        "dateObservedFrom",
        "dateObservedTo",
        "identifcation",
        "peopleCount_immedate",
        "peopleCount_near",
        "peopleCount_far",
        "peopleOccupancy_immedate",
        "peopleOccupancy_near",
        "peopleOccupancy_far",
    ],
)
def test_attrs_sha256_keeps_each_stable_product_a_attribute(
    attr_name: str,
) -> None:
    attrs = expected_product_a_attrs(flow_row(), place())
    changed = deepcopy(attrs)
    changed[attr_name]["value"] = f"changed-{attr_name}"

    assert run_flow_module._attrs_sha256(attrs) != run_flow_module._attrs_sha256(
        changed
    )


def test_attrs_sha256_keeps_identifcation_in_semantic_hash() -> None:
    target = place()
    attrs = expected_product_a_attrs(flow_row(), target)
    changed_identity = deepcopy(attrs)
    changed_identity["identifcation"]["value"] = f"{target.entity_id}.different"

    assert run_flow_module._attrs_sha256(attrs) != run_flow_module._attrs_sha256(
        changed_identity
    )


def test_run_flow_skips_prior_ok_when_only_date_retrieved_changes(
    tmp_path: Path,
) -> None:
    row = flow_row()
    target = place()
    prior_attrs = expected_product_a_attrs(
        row,
        target,
        transformed_at=NOW - timedelta(hours=1),
    )
    path = tmp_path / "state" / "flow.json"
    write_state(
        path,
        {
            "per3600/20260523_0900": window_record(
                first_seen=NOW - timedelta(hours=1),
                last_attempt=NOW - timedelta(hours=1),
                status="complete",
                targets={
                    target.entity_id: target_record(
                        status="ok",
                        payload_sha256=run_flow_module._attrs_sha256(prior_attrs),
                        last_attempt_at=NOW - timedelta(hours=1),
                    )
                },
            )
        },
    )
    store = WindowStateStore.load(path, now=Clock([NOW] * 10))
    orion = FakeOrionClient()

    result = run_once(
        tmp_path=tmp_path,
        db_connection=FakeDbConnection({60: [row]}),
        orion=orion,
        metadata=[target],
        store=store,
        settings=run_settings(tmp_path, send_mode="send"),
        now=NOW,
    )

    assert result.exit_code == 0
    assert orion.calls == []
    record = store.target_record("per3600/20260523_0900", target.entity_id)
    assert record is not None
    assert record["last_payload_sha256"] == run_flow_module._attrs_sha256(prior_attrs)


def test_run_flow_skips_unchanged_ok_target_and_keeps_window_complete(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    row = flow_row()
    target = place()
    hash_value = transformed_hash(row, [target])
    path = tmp_path / "state" / "flow.json"
    write_state(
        path,
        {
            "per3600/20260523_0900": window_record(
                first_seen=NOW - timedelta(hours=1),
                last_attempt=NOW - timedelta(hours=1),
                status="complete",
                targets={
                    target.entity_id: target_record(
                        status="ok",
                        payload_sha256=hash_value,
                        last_attempt_at=NOW - timedelta(hours=1),
                    )
                },
            )
        },
    )
    store = WindowStateStore.load(path, now=Clock([NOW] * 10))
    db_connection = FakeDbConnection({60: [row]})
    orion = FakeOrionClient()

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline"):
        result = run_once(
            tmp_path=tmp_path,
            db_connection=db_connection,
            orion=orion,
            metadata=[target],
            store=store,
            settings=run_settings(tmp_path, send_mode="send"),
        )

    assert result.exit_code == 0
    assert orion.calls == []
    assert store.as_dict()["windows"]["per3600/20260523_0900"]["status"] == "complete"
    skipped = records(caplog, "post_skipped_unchanged")
    assert len(skipped) == 1
    assert skipped[0].levelname == "DEBUG"
    assert skipped[0].entity_id == target.entity_id
    assert skipped[0].window == "per3600/20260523_0900"
    assert skipped[0].payload_sha256 == hash_value
    assert records(caplog, "post_skipped_drift") == []


def test_run_flow_reposts_drifted_ok_target_and_records_new_hash(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    row = flow_row(flow_gt_m60=9)
    target = place()
    old_hash = "0" * 64
    new_hash = transformed_hash(row, [target])
    path = tmp_path / "state" / "flow.json"
    write_state(
        path,
        {
            "per3600/20260523_0900": window_record(
                first_seen=NOW - timedelta(hours=1),
                last_attempt=NOW - timedelta(hours=1),
                status="complete",
                targets={
                    target.entity_id: target_record(
                        status="ok",
                        payload_sha256=old_hash,
                        last_attempt_at=NOW - timedelta(hours=1),
                    )
                },
            )
        },
    )
    store = WindowStateStore.load(path, now=Clock([NOW] * 10))
    orion = FakeOrionClient()

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline"):
        result = run_once(
            tmp_path=tmp_path,
            db_connection=FakeDbConnection({60: [row]}),
            orion=orion,
            metadata=[target],
            store=store,
            settings=run_settings(tmp_path, send_mode="send"),
        )

    assert result.exit_code == 0
    assert [call["entity_id"] for call in orion.calls] == [target.entity_id]
    record = store.target_record("per3600/20260523_0900", target.entity_id)
    assert record is not None
    assert record["status"] == "ok"
    assert record["last_payload_sha256"] == new_hash
    assert records(caplog, "post_skipped_drift") == []
    drift = records(caplog, "post_resent_drift")
    assert len(drift) == 1
    assert drift[0].entity_id == target.entity_id
    assert drift[0].window == "per3600/20260523_0900"
    assert drift[0].prior_payload_sha256 == old_hash
    assert drift[0].computed_payload_sha256 == new_hash


def test_run_flow_reposts_when_people_occupancy_far_changes_for_same_target(
    tmp_path: Path,
) -> None:
    target = place()
    prior_row = flow_row(stay_gt_m120=Decimal("1.0"))
    revised_row = flow_row(stay_gt_m120=Decimal("2.5"))
    prior_hash = run_flow_module._attrs_sha256(
        expected_product_a_attrs(prior_row, target)
    )
    expected_new_hash = run_flow_module._attrs_sha256(
        expected_product_a_attrs(revised_row, target)
    )
    path = tmp_path / "state" / "flow.json"
    write_state(
        path,
        {
            "per3600/20260523_0900": window_record(
                first_seen=NOW - timedelta(hours=1),
                last_attempt=NOW - timedelta(hours=1),
                status="complete",
                targets={
                    target.entity_id: target_record(
                        status="ok",
                        payload_sha256=prior_hash,
                        last_attempt_at=NOW - timedelta(hours=1),
                    )
                },
            )
        },
    )
    store = WindowStateStore.load(path, now=Clock([NOW] * 10))
    orion = FakeOrionClient()

    result = run_once(
        tmp_path=tmp_path,
        db_connection=FakeDbConnection({60: [revised_row]}),
        orion=orion,
        metadata=[target],
        store=store,
        settings=run_settings(tmp_path, send_mode="send"),
    )

    assert result.exit_code == 0
    [call] = orion.calls
    assert call["entity_id"] == target.entity_id
    assert call["attrs"]["peopleOccupancy_far"]["value"] == 2.5
    record = store.target_record("per3600/20260523_0900", target.entity_id)
    assert record is not None
    assert record["last_payload_sha256"] == expected_new_hash
    assert expected_new_hash != prior_hash


def test_run_flow_posts_old_shape_hash_once_and_records_new_semantic_hash(
    tmp_path: Path,
) -> None:
    target = place()
    row = flow_row()
    new_attrs = expected_product_a_attrs(row, target)
    old_attrs = {
        name: value
        for name, value in new_attrs.items()
        if name not in {"dateRetrieved", "identifcation", "peopleOccupancy_far"}
    }
    old_hash = run_flow_module._attrs_sha256(old_attrs)
    expected_new_hash = run_flow_module._attrs_sha256(new_attrs)
    path = tmp_path / "state" / "flow.json"
    write_state(
        path,
        {
            "per3600/20260523_0900": window_record(
                first_seen=NOW - timedelta(hours=1),
                last_attempt=NOW - timedelta(hours=1),
                status="complete",
                targets={
                    target.entity_id: target_record(
                        status="ok",
                        payload_sha256=old_hash,
                        last_attempt_at=NOW - timedelta(hours=1),
                    )
                },
            )
        },
    )
    store = WindowStateStore.load(path, now=Clock([NOW] * 10))
    orion = FakeOrionClient()

    result = run_once(
        tmp_path=tmp_path,
        db_connection=FakeDbConnection({60: [row]}),
        orion=orion,
        metadata=[target],
        store=store,
        settings=run_settings(tmp_path, send_mode="send"),
    )

    assert result.exit_code == 0
    assert len(orion.calls) == 1
    assert list(orion.calls[0]["attrs"]) == PRODUCT_A_ATTR_NAMES
    record = store.target_record("per3600/20260523_0900", target.entity_id)
    assert record is not None
    assert record["last_payload_sha256"] == expected_new_hash
    assert expected_new_hash != old_hash


def test_run_flow_preserves_stored_target_when_unobserved_keeps_partial_without_warning(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    current_target = place(place_number=10)
    removed_entity_id = "jp.sendai.Blesensor.per3600.11"
    hash_value = transformed_hash(flow_row(), [current_target])
    path = tmp_path / "state" / "flow.json"
    window = window_record(
        first_seen=NOW - timedelta(hours=1),
        last_attempt=NOW - timedelta(hours=1),
        status="partial",
        targets={
            current_target.entity_id: target_record(
                status="ok",
                payload_sha256=hash_value,
                last_attempt_at=NOW - timedelta(hours=1),
            )
        },
    )
    window["expected_target_ids"] = [current_target.entity_id, removed_entity_id]
    write_state(path, {"per3600/20260523_0900": window})
    store = WindowStateStore.load(path, now=Clock([NOW] * 10))
    orion = FakeOrionClient()

    with caplog.at_level(logging.WARNING, logger="sendai_pipeline"):
        result = run_once(
            tmp_path=tmp_path,
            db_connection=FakeDbConnection({60: [flow_row()]}),
            orion=orion,
            metadata=[current_target],
            store=store,
            settings=run_settings(tmp_path, send_mode="send"),
        )

    assert result.exit_code == 1
    assert orion.calls == []
    assert store.as_dict()["windows"]["per3600/20260523_0900"][
        "expected_target_ids"
    ] == [current_target.entity_id, removed_entity_id]
    assert store.as_dict()["windows"]["per3600/20260523_0900"]["status"] == "partial"
    assert records(caplog, "window_expected_targets_changed") == []
    partials = records(caplog, "window_partial")
    assert len(partials) == 1
    assert partials[0].window == "per3600/20260523_0900"


def test_run_flow_retries_failed_target_even_when_hash_matches(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    row = flow_row()
    target = place()
    hash_value = transformed_hash(row, [target])
    path = tmp_path / "state" / "flow.json"
    write_state(
        path,
        {
            "per3600/20260523_0900": window_record(
                first_seen=NOW - timedelta(hours=1),
                last_attempt=NOW - timedelta(hours=1),
                status="partial",
                targets={
                    target.entity_id: target_record(
                        status="failed",
                        http_status=502,
                        payload_sha256=hash_value,
                        last_attempt_at=NOW - timedelta(hours=1),
                    )
                },
            )
        },
    )
    store = WindowStateStore.load(path, now=Clock([NOW] * 10))
    orion = FakeOrionClient()

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline"):
        result = run_once(
            tmp_path=tmp_path,
            db_connection=FakeDbConnection({60: [row]}),
            orion=orion,
            metadata=[target],
            store=store,
            settings=run_settings(tmp_path, send_mode="send"),
        )

    assert result.exit_code == 0
    assert len(orion.calls) == 1
    record = store.target_record("per3600/20260523_0900", target.entity_id)
    assert record is not None
    assert record["status"] == "ok"
    assert records(caplog, "post_skipped_drift") == []


def test_run_flow_retries_failed_target_when_hash_differs_without_drift_log(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    row = flow_row(flow_gt_m60=9)
    target = place()
    new_hash = transformed_hash(row, [target])
    path = tmp_path / "state" / "flow.json"
    write_state(
        path,
        {
            "per3600/20260523_0900": window_record(
                first_seen=NOW - timedelta(hours=1),
                last_attempt=NOW - timedelta(hours=1),
                status="partial",
                targets={
                    target.entity_id: target_record(
                        status="failed",
                        http_status=502,
                        payload_sha256="0" * 64,
                        last_attempt_at=NOW - timedelta(hours=1),
                    )
                },
            )
        },
    )
    store = WindowStateStore.load(path, now=Clock([NOW] * 10))
    orion = FakeOrionClient()

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline"):
        result = run_once(
            tmp_path=tmp_path,
            db_connection=FakeDbConnection({60: [row]}),
            orion=orion,
            metadata=[target],
            store=store,
            settings=run_settings(tmp_path, send_mode="send"),
        )

    assert result.exit_code == 0
    assert len(orion.calls) == 1
    record = store.target_record("per3600/20260523_0900", target.entity_id)
    assert record is not None
    assert record["status"] == "ok"
    assert record["last_payload_sha256"] == new_hash
    assert records(caplog, "post_skipped_drift") == []


def test_run_flow_completes_partial_window_and_reposts_drifted_ok_target(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    ok_target = place(place_number=10)
    failed_target = place(place_number=11)
    ok_row = flow_row(group_place_id="sendai202603.10", flow_gt_m60=9)
    failed_row = flow_row(group_place_id="sendai202603.11")
    ok_old_hash = "0" * 64
    failed_hash = transformed_hash(failed_row, [failed_target])
    path = tmp_path / "state" / "flow.json"
    write_state(
        path,
        {
            "per3600/20260523_0900": window_record(
                first_seen=NOW - timedelta(hours=1),
                last_attempt=NOW - timedelta(hours=1),
                status="partial",
                targets={
                    ok_target.entity_id: target_record(
                        status="ok",
                        payload_sha256=ok_old_hash,
                        last_attempt_at=NOW - timedelta(hours=1),
                    ),
                    failed_target.entity_id: target_record(
                        status="failed",
                        http_status=502,
                        payload_sha256=failed_hash,
                        last_attempt_at=NOW - timedelta(hours=1),
                    ),
                },
            )
        },
    )
    store = WindowStateStore.load(path, now=Clock([NOW] * 10))
    orion = FakeOrionClient()

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline"):
        result = run_once(
            tmp_path=tmp_path,
            db_connection=FakeDbConnection({60: [ok_row, failed_row]}),
            orion=orion,
            metadata=[ok_target, failed_target],
            store=store,
            settings=run_settings(tmp_path, send_mode="send"),
        )

    assert result.exit_code == 0
    assert result.windows_complete == 1
    assert result.windows_partial == 0
    assert [call["entity_id"] for call in orion.calls] == [
        ok_target.entity_id,
        failed_target.entity_id,
    ]
    ok_record = store.target_record("per3600/20260523_0900", ok_target.entity_id)
    failed_record = store.target_record(
        "per3600/20260523_0900", failed_target.entity_id
    )
    assert ok_record is not None
    assert failed_record is not None
    assert ok_record["last_payload_sha256"] == transformed_hash(ok_row, [ok_target])
    assert failed_record["status"] == "ok"
    assert records(caplog, "post_skipped_drift") == []
    assert len(records(caplog, "post_resent_drift")) == 1


def test_run_flow_saves_successful_target_before_next_post(
    tmp_path: Path,
) -> None:
    targets = [place(place_number=10), place(place_number=11)]
    rows = [
        flow_row(group_place_id="sendai202603.10"),
        flow_row(group_place_id="sendai202603.11"),
    ]
    path = tmp_path / "state" / "flow.json"
    store = WindowStateStore(path, now=Clock([NOW] * 10))

    class CrashingOrionClient(FakeOrionClient):
        def update_attrs(
            self,
            entity_id: str,
            entity_type: str | None,
            attrs: Mapping[str, Any],
            *,
            dry_run: bool = False,
        ) -> dict[str, Any]:
            if self.calls:
                raise RuntimeError("boom")
            return super().update_attrs(
                entity_id,
                entity_type,
                attrs,
                dry_run=dry_run,
            )

    with pytest.raises(RuntimeError, match="boom"):
        run_once(
            tmp_path=tmp_path,
            db_connection=FakeDbConnection({60: rows}),
            orion=CrashingOrionClient(),
            metadata=targets,
            store=store,
            settings=run_settings(tmp_path, send_mode="send"),
        )

    disk_state = json.loads(path.read_text(encoding="utf-8"))
    target = disk_state["windows"]["per3600/20260523_0900"]["targets"][
        targets[0].entity_id
    ]
    assert target["status"] == "ok"


def test_run_flow_rolling_reprocess_expands_lower_bound_to_oldest_open_window(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    path = tmp_path / "state" / "flow.json"
    write_state(
        path,
        {
            "per3600/20260523_0400": window_record(
                first_seen=NOW_ON_HOUR - timedelta(hours=8),
                last_attempt=NOW_ON_HOUR - timedelta(hours=8),
                status="partial",
                targets={},
            )
        },
    )
    store = WindowStateStore.load(path, now=Clock([NOW_ON_HOUR] * 10))
    db_connection = FakeDbConnection({60: []})

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline"):
        result = run_once(
            tmp_path=tmp_path,
            db_connection=db_connection,
            orion=FakeOrionClient(),
            metadata=[place()],
            store=store,
            settings=run_settings(
                tmp_path,
                send_mode="send",
                reprocess_hours_per3600=4,
                max_lookback_hours_per3600=12,
            ),
            now=NOW_ON_HOUR,
        )

    assert result.lookback_hours_used[60] == 8.0
    assert db_connection.queries[1] == (60, "20260523_0100", "20260523_0900", 2)
    assert records(caplog, "window_giving_up_soon") == []


def test_run_flow_rolling_reprocess_uses_source_window_when_first_seen_recent(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state" / "flow.json"
    write_state(
        path,
        {
            "per3600/20260523_0400": window_record(
                first_seen=NOW_ON_HOUR,
                last_attempt=NOW_ON_HOUR,
                status="partial",
                targets={},
            )
        },
    )
    store = WindowStateStore.load(path, now=Clock([NOW_ON_HOUR] * 10))
    db_connection = FakeDbConnection({60: []})

    result = run_once(
        tmp_path=tmp_path,
        db_connection=db_connection,
        orion=FakeOrionClient(),
        metadata=[place()],
        store=store,
        settings=run_settings(
            tmp_path,
            send_mode="send",
            reprocess_hours_per3600=4,
            max_lookback_hours_per3600=12,
        ),
        now=NOW_ON_HOUR,
    )

    assert result.lookback_hours_used[60] == 8.0
    assert db_connection.queries[1] == (60, "20260523_0100", "20260523_0900", 2)


def test_run_flow_rolling_reprocess_clamps_lower_bound_and_warns_for_stuck_window(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    path = tmp_path / "state" / "flow.json"
    write_state(
        path,
        {
            "per3600/20260519_0800": window_record(
                first_seen=NOW_ON_HOUR - timedelta(hours=100),
                last_attempt=NOW_ON_HOUR - timedelta(hours=100),
                status="partial",
                targets={},
            )
        },
    )
    store = WindowStateStore.load(path, now=Clock([NOW_ON_HOUR] * 10))
    db_connection = FakeDbConnection({60: []})

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline"):
        result = run_once(
            tmp_path=tmp_path,
            db_connection=db_connection,
            orion=FakeOrionClient(),
            metadata=[place()],
            store=store,
            settings=run_settings(
                tmp_path,
                send_mode="send",
                reprocess_hours_per3600=4,
                max_lookback_hours_per3600=72,
            ),
            now=NOW_ON_HOUR,
        )

    assert result.lookback_hours_used[60] == 72.0
    assert db_connection.queries[1] == (60, "20260520_0900", "20260523_0900", 2)
    warnings = records(caplog, "window_giving_up_soon")
    assert len(warnings) == 1
    assert warnings[0].levelname == "ERROR"
    assert warnings[0].window == "per3600/20260519_0800"


def test_run_flow_selects_five_and_sixty_minute_intervals_with_distinct_bounds(
    tmp_path: Path,
) -> None:
    targets = [
        place(place_number=10, interval_min=60),
        place(place_number=10, interval_min=5),
    ]
    db_connection = FakeDbConnection(
        {
            60: [flow_row(startdate="20260523_0900", interval_min=60)],
            5: [
                flow_row(
                    startdate="20260523_0915",
                    interval_min=5,
                )
            ],
        }
    )

    result = run_once(
        tmp_path=tmp_path,
        db_connection=db_connection,
        orion=FakeOrionClient(),
        metadata=targets,
        settings=run_settings(tmp_path, send_mode="send"),
    )

    assert result.exit_code == 0
    assert db_connection.queries == [
        (5, "20260523_0715", "20260523_0915", 2),
        (60, "20260522_2100", "20260523_0900", 2),
    ]


def test_run_flow_uses_configured_source_max_imputation_tier_in_queries(
    tmp_path: Path,
) -> None:
    db_connection = FakeDbConnection({5: [], 60: []})

    run_once(
        tmp_path=tmp_path,
        db_connection=db_connection,
        orion=FakeOrionClient(),
        metadata=[place(interval_min=5), place(interval_min=60)],
        filters=filter_settings(source_max_imputation_tier=1),
    )

    assert db_connection.queries == [
        (5, "20260523_0715", "20260523_0915", 1),
        (60, "20260522_2100", "20260523_0900", 1),
    ]


def test_run_flow_excludes_rows_above_source_max_imputation_tier_before_transform(
    tmp_path: Path,
) -> None:
    rows = [
        flow_row(group_place_id="sendai202603.10", imputation_tier=0),
        flow_row(group_place_id="sendai202603.11", imputation_tier=1),
        flow_row(group_place_id="sendai202603.12", imputation_tier=2),
    ]
    orion = FakeOrionClient()

    result = run_once(
        tmp_path=tmp_path,
        db_connection=FakeDbConnection({60: rows}),
        orion=orion,
        metadata=[
            place(place_number=10),
            place(place_number=11),
            place(place_number=12),
        ],
        filters=filter_settings(source_max_imputation_tier=1),
    )

    assert [call["entity_id"] for call in orion.calls] == [
        "jp.sendai.Blesensor.per3600.10",
        "jp.sendai.Blesensor.per3600.11",
    ]
    assert result.rows_dropped == 0


def test_run_flow_source_stability_delay_override_changes_query_cutoff(
    tmp_path: Path,
) -> None:
    db_connection = FakeDbConnection({5: [], 60: []})

    result = run_once(
        tmp_path=tmp_path,
        db_connection=db_connection,
        orion=FakeOrionClient(),
        metadata=[place(interval_min=5), place(interval_min=60)],
        settings=run_settings(
            tmp_path,
            send_mode="send",
            source_stability_delay_hours=6,
        ),
    )

    assert result.exit_code == 0
    assert db_connection.queries == [
        (5, "20260523_0415", "20260523_0615", 2),
        (60, "20260522_1800", "20260523_0600", 2),
    ]


def test_run_flow_send_mode_garbage_collects_old_complete_windows_and_saves(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "state" / "flow.json"
    old_complete = "per3600/20260517_1100"
    old_partial = "per3600/20260517_1200"
    write_state(
        path,
        {
            old_complete: window_record(
                first_seen=NOW - timedelta(hours=150),
                last_attempt=NOW - timedelta(hours=150),
                status="complete",
                targets={},
            ),
            old_partial: window_record(
                first_seen=NOW - timedelta(hours=150),
                last_attempt=NOW - timedelta(hours=150),
                status="partial",
                targets={},
            ),
        },
    )
    store = WindowStateStore.load(path, now=Clock([NOW] * 10))
    save_calls = 0
    real_save = store.save

    def save_spy() -> None:
        nonlocal save_calls
        save_calls += 1
        real_save()

    monkeypatch.setattr(store, "save", save_spy)

    result = run_once(
        tmp_path=tmp_path,
        db_connection=FakeDbConnection({60: []}),
        orion=FakeOrionClient(),
        metadata=[place()],
        store=store,
        settings=run_settings(tmp_path, send_mode="send"),
    )

    assert result.exit_code == 1
    assert save_calls == 1
    windows = store.as_dict()["windows"]
    assert old_complete not in windows
    assert old_partial in windows
    assert old_complete not in json.loads(path.read_text(encoding="utf-8"))["windows"]


def test_run_flow_emits_run_started_once_with_required_extras(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    db_connection = FakeDbConnection({60: [flow_row()]})
    orion = FakeOrionClient(payload_mode="full")

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline"):
        run_once(
            tmp_path=tmp_path,
            db_connection=db_connection,
            orion=orion,
            metadata=[place()],
            settings=run_settings(tmp_path, send_mode="send"),
        )

    started = records(caplog, "run_started")
    assert len(started) == 1
    record = started[0]
    assert record.levelname == "INFO"
    assert record.product == "flow"
    assert record.send_mode == "send"
    assert record.target_batches == ["2026"]
    assert record.payload_mode == "full"
    assert record.lookback_hours_used == {5: 2.0, 60: 12.0}
    assert record.source_max_imputation_tier == 2


def test_run_flow_emits_run_summary_once_with_documented_counts(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    targets = [place(place_number=10), place(place_number=11)]
    rows = [
        flow_row(group_place_id="sendai202603.10"),
        flow_row(group_place_id="sendai202603.11"),
    ]

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline"):
        result = run_once(
            tmp_path=tmp_path,
            db_connection=FakeDbConnection({60: rows}),
            orion=FakeOrionClient(),
            metadata=targets,
            settings=run_settings(tmp_path, send_mode="send"),
        )

    summary = records(caplog, "run_summary")
    assert len(summary) == 1
    assert summary[0].levelname == "INFO"
    assert summary[0].windows_seen == result.windows_seen == 1
    assert summary[0].windows_complete == result.windows_complete == 1
    assert summary[0].windows_partial == result.windows_partial == 0
    assert summary[0].windows_dead_letter == result.windows_dead_letter == 0
    assert summary[0].posts_ok == result.posts_ok == 2
    assert summary[0].posts_failed == result.posts_failed == 0
    assert summary[0].rows_dropped == result.rows_dropped == 0
    assert summary[0].oldest_non_complete is None
    assert summary[0].lookback_hours_used == {5: 2.0, 60: 12.0}


def test_run_flow_summarizes_dropped_rows_at_info_level(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    rows = [
        flow_row(group_place_id="sendai202603.10"),
        flow_row(group_place_id="sendai202603.99"),
        flow_row(group_place_id="sendai202603.10", device_type="Pixel3aUT"),
    ]

    with caplog.at_level(logging.INFO, logger="sendai_pipeline"):
        result = run_once(
            tmp_path=tmp_path,
            db_connection=FakeDbConnection({60: rows}),
            orion=FakeOrionClient(),
            metadata=[place(place_number=10)],
        )

    summary = records(caplog, "run_summary")
    assert len(summary) == 1
    assert summary[0].rows_dropped == result.rows_dropped == 2
    assert records(caplog, "unknown_place_interval") == []
    assert records(caplog, "device_mismatch") == []


def test_run_flow_logs_window_events_for_mixed_run(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    targets = [place(place_number=10), place(place_number=11)]
    rows = [
        flow_row(startdate="20260523_0800", group_place_id="sendai202603.10"),
        flow_row(startdate="20260523_0800", group_place_id="sendai202603.11"),
        flow_row(startdate="20260523_0900", group_place_id="sendai202603.10"),
    ]
    path = tmp_path / "state" / "flow.json"
    write_state(
        path,
        {
            "per3600/20260520_0000": window_record(
                first_seen=NOW - timedelta(hours=70),
                last_attempt=NOW - timedelta(hours=70),
                status="partial",
                targets={},
            )
        },
    )
    store = WindowStateStore.load(path, now=Clock([NOW] * 20))

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline"):
        result = run_once(
            tmp_path=tmp_path,
            db_connection=FakeDbConnection({60: rows}),
            orion=FakeOrionClient(),
            metadata=targets,
            store=store,
            settings=run_settings(
                tmp_path,
                send_mode="send",
                reprocess_hours_per3600=12,
                max_lookback_hours_per3600=72,
            ),
        )

    assert result.exit_code == 1
    assert result.windows_complete == 2
    assert result.windows_partial == 0
    assert [record.window for record in records(caplog, "window_complete")] == [
        "per3600/20260523_0800",
        "per3600/20260523_0900",
    ]
    assert records(caplog, "window_partial") == []
    stuck = records(caplog, "window_giving_up_soon")
    assert len(stuck) == 1
    assert stuck[0].levelname == "ERROR"
    assert stuck[0].window == "per3600/20260520_0000"


def test_main_dry_run_smoke_configures_logging_after_lock_and_never_live_posts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    set_required_main_env(monkeypatch, tmp_path)
    events: list[str] = []
    db_connection = FakeDbConnection({60: [flow_row()]})
    orion = FakeOrionClient()

    def load_dotenv() -> None:
        events.append("dotenv")

    def configure_logging(_settings: Any, *, product: str) -> None:
        assert product == "flow"
        events.append("logging")

    def flock(_file: Any, flags: int) -> None:
        assert flags == fcntl.LOCK_EX | fcntl.LOCK_NB
        events.append("lock")

    class FakeAuthClient:
        def __init__(self, _settings: Any) -> None:
            pass

    monkeypatch.setattr(run_flow_module, "load_dotenv", load_dotenv)
    monkeypatch.setattr(run_flow_module, "configure_logging", configure_logging)
    monkeypatch.setattr(run_flow_module.fcntl, "flock", flock)
    monkeypatch.setattr(run_flow_module, "load_metadata", lambda _path: [place()])
    monkeypatch.setattr(run_flow_module.db, "connect", lambda _settings: db_connection)
    monkeypatch.setattr(run_flow_module.auth, "AuthClient", FakeAuthClient)
    monkeypatch.setattr(
        run_flow_module.orion_client,
        "OrionClient",
        lambda _settings, *, auth: orion,
    )
    monkeypatch.setattr(
        run_flow_module.entity_map,
        "validate_targets",
        lambda _places, _orion: None,
    )

    code = run_flow_main(argv=[])

    assert code == 0
    assert events.index("dotenv") < events.index("lock") < events.index("logging")
    assert events.count("logging") == 1
    assert [call["dry_run"] for call in orion.calls] == [True]
    assert all(call["dry_run"] is True for call in orion.calls)


def test_main_writes_lifecycle_records_to_configured_log_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    set_required_main_env(monkeypatch, tmp_path)
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    monkeypatch.setenv("LOG_DIR", str(tmp_path / "logs"))
    db_connection = FakeDbConnection({60: [flow_row()]})
    orion = FakeOrionClient()

    class FakeAuthClient:
        def __init__(self, _settings: Any) -> None:
            pass

    monkeypatch.setattr(run_flow_module, "load_dotenv", lambda: None)
    monkeypatch.setattr(run_flow_module, "load_metadata", lambda _path: [place()])
    monkeypatch.setattr(run_flow_module.db, "connect", lambda _settings: db_connection)
    monkeypatch.setattr(run_flow_module.auth, "AuthClient", FakeAuthClient)
    monkeypatch.setattr(
        run_flow_module.orion_client,
        "OrionClient",
        lambda _settings, *, auth: orion,
    )
    monkeypatch.setattr(
        run_flow_module.entity_map,
        "validate_targets",
        lambda _places, _orion: None,
    )

    code = run_flow_main(argv=[])

    assert code == 0
    log_records = [
        json.loads(line)
        for line in (tmp_path / "logs" / "flow.log")
        .read_text(encoding="utf-8")
        .splitlines()
    ]
    lifecycle_records = [
        record
        for record in log_records
        if record.get("event") in {"run_started", "run_summary"}
    ]
    assert [record["event"] for record in lifecycle_records] == [
        "run_started",
        "run_summary",
    ]
    assert all(record["logger"] == "sendai_pipeline" for record in lifecycle_records)


def test_main_flock_contention_returns_zero_and_warns(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    set_required_main_env(monkeypatch, tmp_path)

    def raise_contention(_file: Any, flags: int) -> None:
        assert flags == fcntl.LOCK_EX | fcntl.LOCK_NB
        raise BlockingIOError

    def configure_logging(_settings: Any, *, product: str) -> None:
        raise AssertionError("logging must not be configured when lock is busy")

    monkeypatch.setattr(run_flow_module, "load_dotenv", lambda: None)
    monkeypatch.setattr(run_flow_module, "configure_logging", configure_logging)
    monkeypatch.setattr(run_flow_module.fcntl, "flock", raise_contention)

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline"):
        code = run_flow_main(argv=[])

    assert code == 0
    assert (
        "[sendai-pipeline] flow run skipped: lock held by another process"
        in capsys.readouterr().err
    )
    assert caplog.records == []


def revision_discovery_row(startdate: str, aggregated_at: str) -> dict[str, str]:
    return {"startdate": startdate, "win_agg": aggregated_at}


def revision_flow_settings(
    tmp_path: Path,
    *,
    send_mode: str = "send",
    enabled: bool = True,
    max_windows: int = 2000,
) -> RunFlowSettings:
    return run_settings(
        tmp_path,
        send_mode=send_mode,
        revision_sweep_enabled=enabled,
        revision_sweep_max_windows=max_windows,
    )


def revision_seed_query_bound(seed: object) -> str:
    if isinstance(seed, datetime):
        seed_dt = seed
    elif isinstance(seed, str):
        assert "T" in seed and seed.endswith("+09:00")
        seed_dt = datetime.fromisoformat(seed)
    else:
        raise TypeError(f"unsupported revision seed type: {type(seed).__name__}")
    return seed_dt.replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")


def test_run_flow_sweeps_old_revised_window_missed_by_fresh_path(
    tmp_path: Path,
) -> None:
    target = place()
    revised = flow_row(startdate="20260620_0900")
    db_connection = FakeDbConnection(
        {60: []},
        startdate_rows_by_interval={60: [revised]},
        discovery_rows_by_interval={
            60: [revision_discovery_row("20260620_0900", "2026-06-30 08:15:00")]
        },
    )

    orion = FakeOrionClient()
    result = run_once(
        tmp_path=tmp_path,
        db_connection=db_connection,
        orion=orion,
        metadata=[target],
        settings=revision_flow_settings(tmp_path),
        now=REVISION_NOW,
    )

    assert result.exit_code == 0
    assert db_connection.startdate_queries == [(60, ("20260620_0900",), 2)]
    assert db_connection.queries == [
        (5, "20260630_0715", "20260630_0915", 2),
        (60, "20260629_2100", "20260630_0900", 2),
    ]
    assert orion.calls[0]["attrs"]["dateRetrieved"]["value"] == (
        "2026-06-30T12:17:43+09:00"
    )


def test_run_flow_sweeps_five_min_revision_older_than_floor_younger_than_horizon(
    tmp_path: Path,
) -> None:
    db_connection = FakeDbConnection(
        {5: []},
        startdate_rows_by_interval={
            5: [flow_row(startdate="20260630_0600", interval_min=5)]
        },
        discovery_rows_by_interval={
            5: [revision_discovery_row("20260630_0600", "2026-06-30 08:15:00")]
        },
    )

    run_once(
        tmp_path=tmp_path,
        db_connection=db_connection,
        orion=FakeOrionClient(),
        metadata=[place(interval_min=5)],
        settings=revision_flow_settings(tmp_path),
        now=REVISION_NOW,
    )

    assert db_connection.startdate_queries == [(5, ("20260630_0600",), 2)]


def test_run_flow_advances_revision_cursor_when_sweep_post_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        run_flow_module,
        "REVISION_SWEEP_DISCOVERY_SPAN",
        timedelta(hours=6),
        raising=False,
    )
    path = tmp_path / "state" / "flow.json"
    write_state(path, {}, last_aggregated_at="2026-06-30T12:00:00+09:00")
    store = WindowStateStore.load(path, now=Clock([REVISION_NOW] * 10))
    db_connection = FakeDbConnection(
        {60: []},
        startdate_rows_by_interval={60: [flow_row(startdate="20260620_0900")]},
        discovery_rows_by_interval={
            60: [revision_discovery_row("20260620_0900", "2026-06-30 12:01:00")]
        },
    )

    result = run_once(
        tmp_path=tmp_path,
        db_connection=db_connection,
        orion=FakeOrionClient(results=[{"status": 502, "ok": False}]),
        metadata=[place()],
        store=store,
        settings=revision_flow_settings(tmp_path),
        now=REVISION_NOW,
    )

    assert result.exit_code == 1
    assert store.revision_cursor() == REVISION_NOW.replace(microsecond=0)
    assert store.window_status("per3600/20260620_0900") == "partial"


def test_run_flow_retries_failed_sweep_window_from_state_after_cursor_advanced(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        run_flow_module,
        "REVISION_SWEEP_DISCOVERY_SPAN",
        timedelta(hours=6),
        raising=False,
    )
    target = place()
    path = tmp_path / "state" / "flow.json"
    write_state(
        path,
        {
            "per3600/20260620_0900": window_record(
                first_seen=REVISION_NOW - timedelta(days=1),
                last_attempt=REVISION_NOW - timedelta(days=1),
                status="partial",
                targets={
                    target.entity_id: target_record(
                        status="failed",
                        http_status=502,
                        payload_sha256="0" * 64,
                        last_attempt_at=REVISION_NOW - timedelta(days=1),
                    )
                },
            )
        },
        last_aggregated_at="2026-06-30T12:00:00+09:00",
    )
    store = WindowStateStore.load(path, now=Clock([REVISION_NOW] * 10))
    db_connection = FakeDbConnection(
        {60: []},
        startdate_rows_by_interval={60: [flow_row(startdate="20260620_0900")]},
    )

    result = run_once(
        tmp_path=tmp_path,
        db_connection=db_connection,
        orion=FakeOrionClient(),
        metadata=[target],
        store=store,
        settings=revision_flow_settings(tmp_path),
        now=REVISION_NOW,
    )

    assert result.exit_code == 0
    assert db_connection.discovery_queries
    assert db_connection.startdate_queries == [(60, ("20260620_0900",), 2)]
    assert store.window_status("per3600/20260620_0900") == "complete"


def test_run_flow_suppresses_revision_retry_during_chunked_drain(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        run_flow_module,
        "REVISION_SWEEP_DISCOVERY_SPAN",
        timedelta(hours=6),
        raising=False,
    )
    path = tmp_path / "state" / "flow.json"
    write_state(
        path,
        {
            "per3600/20260620_0900": window_record(
                first_seen=REVISION_NOW - timedelta(days=1),
                last_attempt=REVISION_NOW - timedelta(days=1),
                status="partial",
                targets={},
            )
        },
        last_aggregated_at="2026-06-23T00:00:00+09:00",
    )
    store = WindowStateStore.load(path, now=Clock([REVISION_NOW] * 10))
    db_connection = FakeDbConnection(
        {60: []},
        startdate_rows_by_interval={60: [flow_row(startdate="20260620_0900")]},
    )

    run_once(
        tmp_path=tmp_path,
        db_connection=db_connection,
        orion=FakeOrionClient(),
        metadata=[place()],
        store=store,
        settings=revision_flow_settings(tmp_path),
        now=REVISION_NOW,
    )

    assert store.window_status("per3600/20260620_0900") == "partial"
    assert db_connection.startdate_queries == []


def test_run_flow_does_not_repost_gc_succeeded_same_second_sibling(
    tmp_path: Path,
) -> None:
    # Cursor already passed the siblings' shared aggregated_at second: the
    # partial window retries from state, while the GC'd success is not rediscovered.
    failed = flow_row(startdate="20260620_0900")
    path = tmp_path / "state" / "flow.json"
    write_state(
        path,
        {
            "per3600/20260620_0900": window_record(
                first_seen=REVISION_NOW - timedelta(days=1),
                last_attempt=REVISION_NOW - timedelta(days=1),
                status="partial",
                targets={},
            )
        },
        last_aggregated_at="2026-06-30T12:00:00+09:00",
    )
    store = WindowStateStore.load(path, now=Clock([REVISION_NOW] * 10))
    db_connection = FakeDbConnection(
        {60: []},
        startdate_rows_by_interval={60: [failed]},
    )
    orion = FakeOrionClient()

    run_once(
        tmp_path=tmp_path,
        db_connection=db_connection,
        orion=orion,
        metadata=[place(), place(place_number=11)],
        store=store,
        settings=revision_flow_settings(tmp_path),
        now=REVISION_NOW,
    )

    assert [call["entity_id"] for call in orion.calls] == [place().entity_id]
    assert db_connection.startdate_queries == [(60, ("20260620_0900",), 2)]


def test_run_flow_logs_give_up_signal_for_old_revision_retry(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    path = tmp_path / "state" / "flow.json"
    write_state(
        path,
        {
            "per3600/20260620_0900": window_record(
                first_seen=REVISION_NOW - timedelta(days=5),
                last_attempt=REVISION_NOW - timedelta(days=5),
                status="partial",
                targets={},
            )
        },
        last_aggregated_at="2026-06-30T12:00:00+09:00",
    )

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline"):
        run_once(
            tmp_path=tmp_path,
            db_connection=FakeDbConnection(
                {60: []},
                startdate_rows_by_interval={60: [flow_row(startdate="20260620_0900")]},
            ),
            orion=FakeOrionClient(results=[{"status": 502, "ok": False}]),
            metadata=[place()],
            store=WindowStateStore.load(path, now=Clock([REVISION_NOW] * 10)),
            settings=revision_flow_settings(tmp_path),
            now=REVISION_NOW,
        )

    assert [record.window for record in records(caplog, "window_giving_up_soon")] == [
        "per3600/20260620_0900"
    ]


def test_run_flow_excludes_cap_deferred_revision_from_retry_this_run(
    tmp_path: Path,
) -> None:
    target = place()
    path = tmp_path / "state" / "flow.json"
    write_state(
        path,
        {
            "per3600/20260620_1000": window_record(
                first_seen=REVISION_NOW - timedelta(days=1),
                last_attempt=REVISION_NOW - timedelta(days=1),
                status="partial",
                targets={
                    target.entity_id: target_record(
                        status="failed", payload_sha256="0" * 64
                    )
                },
            )
        },
        last_aggregated_at="2026-06-30T12:00:00+09:00",
    )
    db_connection = FakeDbConnection(
        {60: []},
        startdate_rows_by_interval={
            60: [
                flow_row(startdate="20260620_0900"),
                flow_row(startdate="20260620_1000"),
            ]
        },
        discovery_rows_by_interval={
            60: [
                revision_discovery_row("20260620_0900", "2026-06-30 12:01:00"),
                revision_discovery_row("20260620_1000", "2026-06-30 12:02:00"),
            ]
        },
    )

    run_once(
        tmp_path=tmp_path,
        db_connection=db_connection,
        orion=FakeOrionClient(),
        metadata=[target],
        store=WindowStateStore.load(path, now=Clock([REVISION_NOW] * 10)),
        settings=revision_flow_settings(tmp_path, max_windows=1),
        now=REVISION_NOW,
    )

    assert db_connection.startdate_queries == [(60, ("20260620_0900",), 2)]


def test_run_flow_does_not_advance_cursor_after_unrecorded_window_error(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state" / "flow.json"
    write_state(path, {}, last_aggregated_at="2026-06-30T12:00:00+09:00")
    store = WindowStateStore.load(path, now=Clock([REVISION_NOW] * 10))

    class CrashingOrion(FakeOrionClient):
        def update_attrs(self, *_args: Any, **_kwargs: Any) -> dict[str, Any]:
            raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        run_once(
            tmp_path=tmp_path,
            db_connection=FakeDbConnection(
                {60: []},
                startdate_rows_by_interval={60: [flow_row(startdate="20260620_0900")]},
                discovery_rows_by_interval={
                    60: [revision_discovery_row("20260620_0900", "2026-06-30 12:01:00")]
                },
            ),
            orion=CrashingOrion(),
            metadata=[place()],
            store=store,
            settings=revision_flow_settings(tmp_path),
            now=REVISION_NOW,
        )

    assert store.revision_cursor() == datetime.fromisoformat(
        "2026-06-30T12:00:00+09:00"
    )


def test_run_flow_supplemental_overlap_reposts_once_from_revision_sweep(
    tmp_path: Path,
) -> None:
    target = place()
    row = flow_row(startdate="20260629_2000")
    path = tmp_path / "state" / "flow.json"
    window = window_record(
        first_seen=REVISION_NOW - timedelta(days=1),
        last_attempt=REVISION_NOW - timedelta(days=1),
        status="complete",
        targets={target.entity_id: target_record(status="ok", payload_sha256="0" * 64)},
    )
    window["expected_target_ids"] = [target.entity_id]
    write_state(
        path,
        {"per3600/20260629_2000": window},
        last_aggregated_at="2026-06-30T12:00:00+09:00",
    )

    orion = FakeOrionClient()
    run_once(
        tmp_path=tmp_path,
        db_connection=FakeDbConnection(
            {60: []},
            startdate_rows_by_interval={60: [row]},
            discovery_rows_by_interval={
                60: [revision_discovery_row("20260629_2000", "2026-06-30 12:01:00")]
            },
        ),
        orion=orion,
        metadata=[target],
        store=WindowStateStore.load(path, now=Clock([REVISION_NOW] * 10)),
        settings=revision_flow_settings(tmp_path),
        now=REVISION_NOW,
    )

    assert [call["entity_id"] for call in orion.calls] == [target.entity_id]


def test_run_flow_skips_revised_dead_letter_window(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state" / "flow.json"
    write_state(
        path,
        {
            "per3600/20260620_0900": window_record(
                first_seen=REVISION_NOW - timedelta(days=1),
                last_attempt=REVISION_NOW - timedelta(days=1),
                status="dead_letter",
                targets={},
            )
        },
        last_aggregated_at="2026-06-30T12:00:00+09:00",
    )
    orion = FakeOrionClient()

    run_once(
        tmp_path=tmp_path,
        db_connection=FakeDbConnection(
            {60: []},
            startdate_rows_by_interval={60: [flow_row(startdate="20260620_0900")]},
            discovery_rows_by_interval={
                60: [revision_discovery_row("20260620_0900", "2026-06-30 12:01:00")]
            },
        ),
        orion=orion,
        metadata=[place()],
        store=WindowStateStore.load(path, now=Clock([REVISION_NOW] * 10)),
        settings=revision_flow_settings(tmp_path),
        now=REVISION_NOW,
    )

    assert orion.calls == []


def test_run_flow_sweep_zero_payload_window_leaves_no_state(
    tmp_path: Path,
) -> None:
    store = state_store(tmp_path, Clock([REVISION_NOW] * 10))

    run_once(
        tmp_path=tmp_path,
        db_connection=FakeDbConnection(
            {60: []},
            startdate_rows_by_interval={
                60: [flow_row(startdate="20260620_0900", group_place_id="quick.10")]
            },
            discovery_rows_by_interval={
                60: [revision_discovery_row("20260620_0900", "2026-06-30 12:01:00")]
            },
        ),
        orion=FakeOrionClient(),
        metadata=[place()],
        store=store,
        settings=revision_flow_settings(tmp_path),
        now=REVISION_NOW,
    )

    assert "per3600/20260620_0900" not in store.as_dict()["windows"]


def test_run_flow_revision_sweep_disabled_does_not_scan_or_resend(
    tmp_path: Path,
) -> None:
    db_connection = FakeDbConnection(
        {60: []},
        startdate_rows_by_interval={60: [flow_row(startdate="20260620_0900")]},
        discovery_rows_by_interval={
            60: [revision_discovery_row("20260620_0900", "2026-06-30 12:01:00")]
        },
    )
    orion = FakeOrionClient()

    run_once(
        tmp_path=tmp_path,
        db_connection=db_connection,
        orion=orion,
        metadata=[place()],
        settings=revision_flow_settings(tmp_path, enabled=False),
        now=REVISION_NOW,
    )

    assert db_connection.discovery_queries == []
    assert db_connection.startdate_queries == []
    assert orion.calls == []


def test_run_flow_revision_sweep_dry_run_previews_without_state_or_retry_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "state" / "flow.json"
    write_state(path, {}, last_aggregated_at="2026-06-30T12:00:00+09:00")
    store = WindowStateStore.load(path, now=Clock([REVISION_NOW] * 10))
    before = deepcopy(store.as_dict())
    forbid_state_mutations(monkeypatch, store)
    orion = FakeOrionClient()

    run_once(
        tmp_path=tmp_path,
        db_connection=FakeDbConnection(
            {60: []},
            startdate_rows_by_interval={60: [flow_row(startdate="20260620_0900")]},
            discovery_rows_by_interval={
                60: [revision_discovery_row("20260620_0900", "2026-06-30 12:01:00")]
            },
        ),
        orion=orion,
        metadata=[place()],
        store=store,
        settings=revision_flow_settings(tmp_path, send_mode="dry-run"),
        now=REVISION_NOW,
    )

    assert store.as_dict() == before
    assert [call["dry_run"] for call in orion.calls] == [True]


def test_run_flow_revision_cursor_seeds_from_constant_when_absent(
    tmp_path: Path,
) -> None:
    store = state_store(tmp_path, Clock([REVISION_NOW] * 10))
    db_connection = FakeDbConnection({60: []})

    run_once(
        tmp_path=tmp_path,
        db_connection=db_connection,
        orion=FakeOrionClient(),
        metadata=[place()],
        store=store,
        settings=revision_flow_settings(tmp_path),
        now=REVISION_NOW,
    )

    assert db_connection.discovery_queries[0][1] == revision_seed_query_bound(
        run_flow_module.REVISION_CURSOR_SEED
    )


def test_run_flow_soft_cap_keeps_same_aggregated_at_second_together(
    tmp_path: Path,
) -> None:
    rows = [flow_row(startdate="20260620_0900"), flow_row(startdate="20260620_1000")]
    db_connection = FakeDbConnection(
        {60: []},
        startdate_rows_by_interval={60: rows},
        discovery_rows_by_interval={
            60: [
                revision_discovery_row("20260620_0900", "2026-06-30 12:01:00"),
                revision_discovery_row("20260620_1000", "2026-06-30 12:01:00"),
                revision_discovery_row("20260620_1100", "2026-06-30 12:02:00"),
            ]
        },
    )

    run_once(
        tmp_path=tmp_path,
        db_connection=db_connection,
        orion=FakeOrionClient(),
        metadata=[place(), place(place_number=11)],
        settings=revision_flow_settings(tmp_path, max_windows=1),
        now=REVISION_NOW,
    )

    assert db_connection.startdate_queries == [
        (60, ("20260620_0900", "20260620_1000"), 2)
    ]


def test_run_flow_revision_cap_bounds_discovered_and_retried_work(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state" / "flow.json"
    write_state(
        path,
        {
            "per3600/20260620_0800": window_record(
                first_seen=REVISION_NOW - timedelta(days=1),
                last_attempt=REVISION_NOW - timedelta(days=1),
                status="partial",
                targets={},
            )
        },
        last_aggregated_at="2026-06-30T12:00:00+09:00",
    )
    db_connection = FakeDbConnection(
        {60: []},
        startdate_rows_by_interval={60: [flow_row(startdate="20260620_0900")]},
        discovery_rows_by_interval={
            60: [revision_discovery_row("20260620_0900", "2026-06-30 12:01:00")]
        },
    )

    run_once(
        tmp_path=tmp_path,
        db_connection=db_connection,
        orion=FakeOrionClient(),
        metadata=[place()],
        store=WindowStateStore.load(path, now=Clock([REVISION_NOW] * 10)),
        settings=revision_flow_settings(tmp_path, max_windows=1),
        now=REVISION_NOW,
    )

    assert len(db_connection.startdate_queries) == 1


def test_run_flow_revision_discovery_chunks_by_span(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        run_flow_module,
        "REVISION_SWEEP_DISCOVERY_SPAN",
        timedelta(hours=6),
        raising=False,
    )
    path = tmp_path / "state" / "flow.json"
    write_state(path, {}, last_aggregated_at="2026-06-23T00:00:00+09:00")
    db_connection = FakeDbConnection({60: []})

    run_once(
        tmp_path=tmp_path,
        db_connection=db_connection,
        orion=FakeOrionClient(),
        metadata=[place()],
        store=WindowStateStore.load(path, now=Clock([REVISION_NOW] * 10)),
        settings=revision_flow_settings(tmp_path),
        now=REVISION_NOW,
    )

    assert db_connection.discovery_queries[0][2] == "2026-06-23 06:00:00"


def test_run_flow_initial_drain_bounds_redundant_unrevised_resends(
    tmp_path: Path,
) -> None:
    target = place()
    recent_hash = transformed_hash(flow_row(startdate="20260629_0900"), [target])
    path = tmp_path / "state" / "flow.json"
    window = window_record(
        first_seen=REVISION_NOW - timedelta(hours=12),
        last_attempt=REVISION_NOW - timedelta(hours=12),
        status="complete",
        targets={
            target.entity_id: target_record(status="ok", payload_sha256=recent_hash)
        },
    )
    window["expected_target_ids"] = [target.entity_id]
    write_state(path, {"per3600/20260629_0900": window})
    orion = FakeOrionClient()

    run_once(
        tmp_path=tmp_path,
        db_connection=FakeDbConnection(
            {60: []},
            startdate_rows_by_interval={
                60: [
                    flow_row(startdate="20260624_0900"),
                    flow_row(startdate="20260629_0900"),
                ]
            },
            discovery_rows_by_interval={
                60: [
                    revision_discovery_row("20260624_0900", "2026-06-23 01:00:00"),
                    revision_discovery_row("20260629_0900", "2026-06-23 01:01:00"),
                ]
            },
        ),
        orion=orion,
        metadata=[target],
        store=WindowStateStore.load(path, now=Clock([REVISION_NOW] * 10)),
        settings=revision_flow_settings(tmp_path),
        now=REVISION_NOW,
    )

    assert [call["attrs"]["dateObservedFrom"]["value"] for call in orion.calls] == [
        "2026-06-24T09:00:00+09:00"
    ]


def test_run_flow_logging_extras_are_allowed() -> None:
    required = {
        "lookback_hours_used",
        "oldest_non_complete",
        "windows_seen",
        "windows_complete",
        "windows_partial",
        "windows_dead_letter",
        "posts_ok",
        "posts_failed",
        "rows_dropped",
        "prior_payload_sha256",
        "computed_payload_sha256",
        "source_max_imputation_tier",
    }
    assert required <= _ALLOWED_EXTRA_KEYS
