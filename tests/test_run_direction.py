import fcntl
import hashlib
import json
import logging
from collections.abc import Callable, Iterable, Mapping
from copy import deepcopy
from dataclasses import FrozenInstanceError
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, cast

import pytest

import sendai_pipeline.run_direction as run_direction_module
from sendai_pipeline.filter_settings import FilterConfigError, FilterSettings
from sendai_pipeline.logging_setup import _ALLOWED_EXTRA_KEYS
from sendai_pipeline.metadata import SensorPlace, active_places, index_by_place_interval
from sendai_pipeline.run_direction import (
    RunDirectionConfigError,
    RunDirectionResult,
    RunDirectionSettings,
)
from sendai_pipeline.run_direction import (
    main as run_direction_main,
)
from sendai_pipeline.run_direction import (
    run_direction as run_product_direction,
)
from sendai_pipeline.state import WindowStateStore
from sendai_pipeline.transform_direction import transform_direction_rows

JST = timezone(timedelta(hours=9))
NOW = datetime(2026, 5, 23, 12, 17, 0, tzinfo=JST)
NOW_ON_HOUR = datetime(2026, 5, 23, 12, 0, 0, tzinfo=JST)
REVISION_NOW = datetime(2026, 6, 30, 12, 17, 43, 123456, tzinfo=JST)


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
            interval_min, aggregated_at_lower, aggregated_at_upper, startdate_upper = (
                params
            )
            self._connection.discovery_queries.append(
                (
                    int(interval_min),
                    str(aggregated_at_lower),
                    str(aggregated_at_upper),
                    str(startdate_upper),
                )
            )
            self._rows = list(
                self._connection.discovery_rows_by_interval.get(int(interval_min), [])
            )
            return

        if "startdate IN" in normalized_sql:
            interval_min = int(params[0])
            startdates = tuple(str(value) for value in params[1:])
            self._connection.startdate_queries.append((interval_min, startdates))
            startdate_set = set(startdates)
            self._rows = [
                row
                for row in self._connection.startdate_rows_by_interval.get(
                    interval_min, []
                )
                if str(row["startdate"]) in startdate_set
            ]
            return

        interval_min, lower_bound, upper_bound = params
        self._connection.queries.append((interval_min, lower_bound, upper_bound))
        self._rows = list(self._connection.rows_by_interval.get(interval_min, []))

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
        self.queries: list[tuple[int, str, str]] = []
        self.startdate_queries: list[tuple[int, tuple[str, ...]]] = []
        self.discovery_queries: list[tuple[int, str, str, str]] = []
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


def direction_row(**overrides: Any) -> dict[str, Any]:
    values: dict[str, Any] = {
        "startdate": "20260523_0900",
        "from_group_place_id": "sendai202603.10",
        "to_group_place_id": "sendai202603.11",
        "from_device_type": "M5Stack",
        "to_device_type": "M5Stack",
        "interval_min": 60,
        "count": 12,
    }
    values.update(overrides)
    return values


def run_settings(tmp_path: Path, **overrides: Any) -> RunDirectionSettings:
    values: dict[str, Any] = {
        "send_mode": "dry-run",
        "reprocess_hours_per3600": 12,
        "reprocess_hours_per300": 2,
        "max_lookback_hours_per3600": 72,
        "max_lookback_hours_per300": 72,
        "source_stability_delay_hours": 3,
        "state_path": tmp_path / "state" / "direction.json",
        "lock_path": tmp_path / "state" / "direction.lock",
    }
    values.update(overrides)
    return RunDirectionSettings(**values)


def filter_settings(
    target_direction_batches: Iterable[str] = ("2026",),
    target_flow_batches: Iterable[str] = (),
    ignored_place_prefixes: tuple[str, ...] = ("quick.", "test"),
) -> FilterSettings:
    return FilterSettings(
        target_flow_batches=frozenset(target_flow_batches),
        target_direction_batches=frozenset(target_direction_batches),
        ignored_place_prefixes=ignored_place_prefixes,
    )


def state_store(
    tmp_path: Path, now: Callable[[], datetime] | None = None
) -> WindowStateStore:
    return WindowStateStore(
        tmp_path / "state" / "direction.json", now=now or Clock([NOW])
    )


def run_once(
    *,
    tmp_path: Path,
    db_connection: FakeDbConnection,
    orion: FakeOrionClient,
    metadata: Iterable[SensorPlace],
    store: WindowStateStore | None = None,
    settings: RunDirectionSettings | None = None,
    filters: FilterSettings | None = None,
    now: datetime = NOW,
) -> RunDirectionResult:
    return run_product_direction(
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
    hashable = {key: value for key, value in attrs.items() if key != "dateRetrieved"}
    body = json.dumps(hashable, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(body).hexdigest()


def transformed_hash(
    row: Mapping[str, Any],
    places: Iterable[SensorPlace],
    *,
    target_entity_id: str,
) -> str:
    result = transform_direction_rows(
        [row],
        index_by_place_interval(active_places(places, target_batches=["2026"])),
    )
    payload = next(
        payload
        for payload in result.payloads
        if payload["entity_id"] == target_entity_id
    )
    return payload_hash(payload["attrs"])


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
    monkeypatch.setenv("DIRECTION_SEND_MODE", "dry-run")
    monkeypatch.setenv("TARGET_DIRECTION_BATCHES", "2026")
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
    settings = RunDirectionSettings.from_env({})

    assert settings.send_mode == "dry-run"
    assert settings.reprocess_hours_per3600 == 12
    assert settings.reprocess_hours_per300 == 2
    assert settings.max_lookback_hours_per3600 == 72
    assert settings.max_lookback_hours_per300 == 72
    assert settings.source_stability_delay_hours == 3
    assert settings.state_path == Path("state/direction.json")
    assert settings.lock_path == Path("state/direction.lock")


def test_from_env_accepts_send_mode_and_numeric_overrides() -> None:
    settings = RunDirectionSettings.from_env(
        {
            "DIRECTION_SEND_MODE": "send",
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


def test_from_env_rejects_invalid_send_mode() -> None:
    with pytest.raises(RunDirectionConfigError):
        RunDirectionSettings.from_env({"DIRECTION_SEND_MODE": "yes"})


def test_from_env_rejects_negative_source_stability_delay() -> None:
    with pytest.raises(RunDirectionConfigError):
        RunDirectionSettings.from_env({"SOURCE_STABILITY_DELAY_HOURS": "-1"})


def test_from_env_ignores_flow_send_mode() -> None:
    settings = RunDirectionSettings.from_env({"FLOW_SEND_MODE": "send"})

    assert settings.send_mode == "dry-run"


def test_run_direction_settings_is_frozen(tmp_path: Path) -> None:
    settings = run_settings(tmp_path)

    with pytest.raises(FrozenInstanceError):
        settings.send_mode = "send"  # pyright: ignore[reportAttributeAccessIssue]


def test_run_direction_dry_run_default_never_live_posts(tmp_path: Path) -> None:
    rows = [
        direction_row(
            from_group_place_id="ALL",
            to_group_place_id="sendai202603.10",
            count=7,
        ),
    ]
    db_connection = FakeDbConnection({60: rows})
    orion = FakeOrionClient()

    result = run_once(
        tmp_path=tmp_path,
        db_connection=db_connection,
        orion=orion,
        metadata=[place(place_number=10)],
    )

    assert result.exit_code == 0
    assert orion.calls
    assert all(call["dry_run"] is True for call in orion.calls)


def test_run_direction_dry_run_processes_rows_without_state_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "state" / "direction.json"
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
    db_connection = FakeDbConnection(
        {
            60: [
                direction_row(
                    from_group_place_id="ALL",
                    to_group_place_id="sendai202603.10",
                    count=7,
                )
            ]
        }
    )
    orion = FakeOrionClient()

    result = run_once(
        tmp_path=tmp_path,
        db_connection=db_connection,
        orion=orion,
        metadata=[place(place_number=10)],
        store=store,
    )

    assert result.exit_code == 0
    assert path.read_text(encoding="utf-8") == before_disk
    assert store.as_dict() == before_memory
    assert all(call["dry_run"] is True for call in orion.calls)


def test_run_direction_empty_target_batches_short_circuits_without_side_effects(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    store = state_store(tmp_path)
    forbid_state_mutations(monkeypatch, store)
    db_connection = FakeDbConnection({60: [direction_row()]})
    orion = FakeOrionClient()

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline"):
        result = run_once(
            tmp_path=tmp_path,
            db_connection=db_connection,
            orion=orion,
            metadata=[place()],
            store=store,
            filters=filter_settings(target_direction_batches=()),
        )

    assert result.exit_code == 0
    assert result.windows_seen == 0
    assert db_connection.queries == []
    assert orion.calls == []
    assert not store.path.exists()

    started = records(caplog, "run_started")
    summary = records(caplog, "run_summary")
    assert len(started) == 1
    assert started[0].product == "direction"
    assert started[0].target_batches == []
    assert len(summary) == 1
    assert summary[0].windows_seen == 0
    assert summary[0].rows_dropped == 0


def test_run_direction_target_batch_typo_raises_before_sql(tmp_path: Path) -> None:
    db_connection = FakeDbConnection({60: [direction_row()]})
    orion = FakeOrionClient()

    with pytest.raises(FilterConfigError):
        run_once(
            tmp_path=tmp_path,
            db_connection=db_connection,
            orion=orion,
            metadata=[place(batch="2026")],
            settings=run_settings(tmp_path, send_mode="send"),
            filters=filter_settings(target_direction_batches=("2025",)),
        )

    assert db_connection.queries == []
    assert orion.calls == []


def test_run_direction_send_mode_posts_one_payload_per_target_and_completes_window(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    targets = [place(place_number=10), place(place_number=11)]
    rows = [
        direction_row(
            from_group_place_id="sendai202603.10",
            to_group_place_id="sendai202603.11",
            count=12,
        ),
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
    assert sorted(call["entity_id"] for call in orion.calls) == sorted(
        entity_ids(targets)
    )
    assert all(call["dry_run"] is False for call in orion.calls)
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
        direction_row(
            from_group_place_id="sendai202603.10",
            to_group_place_id="sendai202603.11",
            count=12,
        )
    ]
    store = state_store(tmp_path, Clock([NOW] * 10))
    save_count = 0
    real_save = store.save

    def save_spy() -> None:
        nonlocal save_count
        save_count += 1
        real_save()

    monkeypatch.setattr(store, "save", save_spy)

    run_direction_module._process_send_window(
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
        expected_target_ids=entity_ids(targets),
        counts=run_direction_module._RunCounts(),
    )

    assert save_count == 2


def test_process_send_window_no_persist_zero_in_loop_saves(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    targets = [place(place_number=10), place(place_number=11)]
    rows = [
        direction_row(
            from_group_place_id="sendai202603.10",
            to_group_place_id="sendai202603.11",
            count=12,
        )
    ]
    store = state_store(tmp_path, Clock([NOW] * 10))
    save_count = 0

    def save_spy() -> None:
        nonlocal save_count
        save_count += 1

    monkeypatch.setattr(store, "save", save_spy)

    run_direction_module._process_send_window(
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
        expected_target_ids=entity_ids(targets),
        counts=run_direction_module._RunCounts(),
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
        direction_row(
            from_group_place_id="sendai202603.10",
            to_group_place_id="sendai202603.11",
            count=12,
        )
    ]
    interval_metadata = index_by_place_interval(
        active_places(targets, target_batches=["2026"])
    )
    default_store = WindowStateStore(
        tmp_path / "default" / "direction.json",
        now=Clock([NOW] * 10),
    )
    deferred_store = WindowStateStore(
        tmp_path / "deferred" / "direction.json",
        now=Clock([NOW] * 10),
    )

    run_direction_module._process_send_window(
        "per3600/20260523_0900",
        interval_min=60,
        startdate="20260523_0900",
        rows_for_window=cast(Any, rows),
        orion=FakeOrionClient(results=[{"status": 502, "ok": False}]),
        state_store=default_store,
        filter_settings=filter_settings(),
        interval_metadata=interval_metadata,
        expected_target_ids=entity_ids(targets),
        counts=run_direction_module._RunCounts(),
    )
    default_store.save()

    run_direction_module._process_send_window(
        "per3600/20260523_0900",
        interval_min=60,
        startdate="20260523_0900",
        rows_for_window=cast(Any, rows),
        orion=FakeOrionClient(results=[{"status": 502, "ok": False}]),
        state_store=deferred_store,
        filter_settings=filter_settings(),
        interval_metadata=interval_metadata,
        expected_target_ids=entity_ids(targets),
        counts=run_direction_module._RunCounts(),
        persist_each_target=False,
    )
    deferred_store.save()

    assert json.loads(default_store.path.read_text(encoding="utf-8")) == json.loads(
        deferred_store.path.read_text(encoding="utf-8")
    )


def test_run_direction_send_mode_writes_people_count_flow_attribute(
    tmp_path: Path,
) -> None:
    targets = [place(place_number=10)]
    rows = [
        direction_row(
            from_group_place_id="ALL",
            to_group_place_id="sendai202603.10",
            count=85,
        )
    ]
    db_connection = FakeDbConnection({60: rows})
    orion = FakeOrionClient()

    result = run_once(
        tmp_path=tmp_path,
        db_connection=db_connection,
        orion=orion,
        metadata=targets,
        settings=run_settings(tmp_path, send_mode="send"),
    )

    assert result.exit_code == 0
    assert len(orion.calls) == 1
    attrs = orion.calls[0]["attrs"]
    assert attrs["peopleCount_flow"]["type"] == "StructuredValue"
    assert attrs["peopleCount_flow"]["value"] == {
        "from": {"all": 85},
        "to": {"all": None},
    }
    assert "identifcation" in attrs
    assert "dateObservedFrom" in attrs
    assert "dateObservedTo" in attrs
    assert "dateRetrieved" in attrs


def test_run_direction_failed_post_records_partial_and_nonzero_exit(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    store = state_store(tmp_path, Clock([NOW] * 10))
    db_connection = FakeDbConnection(
        {
            60: [
                direction_row(
                    from_group_place_id="ALL",
                    to_group_place_id="sendai202603.10",
                    count=7,
                )
            ]
        }
    )
    orion = FakeOrionClient(results=[{"status": 502, "ok": False}])

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline"):
        result = run_once(
            tmp_path=tmp_path,
            db_connection=db_connection,
            orion=orion,
            metadata=[place(place_number=10)],
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


def test_run_direction_selects_direction_metrics_for_both_intervals(
    tmp_path: Path,
) -> None:
    targets = [
        place(place_number=10, interval_min=60),
        place(place_number=10, interval_min=5),
    ]
    db_connection = FakeDbConnection(
        {
            60: [direction_row(startdate="20260523_0900", interval_min=60)],
            5: [direction_row(startdate="20260523_0915", interval_min=5)],
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
        (5, "20260523_0715", "20260523_0915"),
        (60, "20260522_2100", "20260523_0900"),
    ]


def test_run_direction_summary_reports_rows_dropped_by_transform(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    rows = [
        direction_row(
            from_group_place_id="sendai202603.10",
            to_group_place_id="sendai202603.11",
            count=1,
        ),
        direction_row(
            from_group_place_id="quick.10",
            to_group_place_id="sendai202603.11",
            count=2,
        ),
        direction_row(
            from_group_place_id="sendai202603.999",
            to_group_place_id="sendai202603.11",
            count=3,
        ),
    ]

    with caplog.at_level(logging.INFO, logger="sendai_pipeline"):
        result = run_once(
            tmp_path=tmp_path,
            db_connection=FakeDbConnection({60: rows}),
            orion=FakeOrionClient(),
            metadata=[place(place_number=10), place(place_number=11)],
        )

    summary = records(caplog, "run_summary")
    assert len(summary) == 1
    assert summary[0].rows_dropped == result.rows_dropped == 2


def test_run_direction_rolling_reprocess_expands_lower_bound_to_oldest_open_window(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    path = tmp_path / "state" / "direction.json"
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
    assert db_connection.queries[1] == (60, "20260523_0100", "20260523_0900")
    assert records(caplog, "window_giving_up_soon") == []


def test_run_direction_rolling_reprocess_uses_source_window_when_first_seen_recent(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state" / "direction.json"
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
    assert db_connection.queries[1] == (60, "20260523_0100", "20260523_0900")


def test_run_direction_emits_run_started_once_with_product_direction(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    db_connection = FakeDbConnection(
        {
            60: [
                direction_row(
                    from_group_place_id="ALL",
                    to_group_place_id="sendai202603.10",
                    count=7,
                )
            ]
        }
    )
    orion = FakeOrionClient(payload_mode="full")

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline"):
        run_once(
            tmp_path=tmp_path,
            db_connection=db_connection,
            orion=orion,
            metadata=[place(place_number=10)],
            settings=run_settings(tmp_path, send_mode="send"),
        )

    started = records(caplog, "run_started")
    assert len(started) == 1
    record = started[0]
    assert record.levelname == "INFO"
    assert record.product == "direction"
    assert record.send_mode == "send"
    assert record.target_batches == ["2026"]
    assert record.payload_mode == "full"
    assert record.lookback_hours_used == {5: 2.0, 60: 12.0}


def test_run_direction_emits_run_summary_once_with_documented_counts(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    targets = [place(place_number=10), place(place_number=11)]
    rows = [
        direction_row(
            from_group_place_id="sendai202603.10",
            to_group_place_id="sendai202603.11",
            count=12,
        ),
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


def test_main_dry_run_smoke_configures_logging_with_product_direction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    set_required_main_env(monkeypatch, tmp_path)
    events: list[str] = []
    db_connection = FakeDbConnection(
        {
            60: [
                direction_row(
                    from_group_place_id="ALL",
                    to_group_place_id="sendai202603.10",
                    count=7,
                )
            ]
        }
    )
    orion = FakeOrionClient()

    def load_dotenv() -> None:
        events.append("dotenv")

    def configure_logging(_settings: Any, *, product: str) -> None:
        assert product == "direction"
        events.append("logging")

    def flock(_file: Any, flags: int) -> None:
        assert flags == fcntl.LOCK_EX | fcntl.LOCK_NB
        events.append("lock")

    class FakeAuthClient:
        def __init__(self, _settings: Any) -> None:
            pass

    monkeypatch.setattr(run_direction_module, "load_dotenv", load_dotenv)
    monkeypatch.setattr(run_direction_module, "configure_logging", configure_logging)
    monkeypatch.setattr(run_direction_module.fcntl, "flock", flock)
    monkeypatch.setattr(
        run_direction_module, "load_metadata", lambda _path: [place(place_number=10)]
    )
    monkeypatch.setattr(
        run_direction_module.db, "connect", lambda _settings: db_connection
    )
    monkeypatch.setattr(run_direction_module.auth, "AuthClient", FakeAuthClient)
    monkeypatch.setattr(
        run_direction_module.orion_client,
        "OrionClient",
        lambda _settings, *, auth: orion,
    )
    monkeypatch.setattr(
        run_direction_module.entity_map,
        "validate_targets",
        lambda _places, _orion: None,
    )

    code = run_direction_main(argv=[])

    assert code == 0
    assert events.index("dotenv") < events.index("lock") < events.index("logging")
    assert events.count("logging") == 1
    assert orion.calls
    assert all(call["dry_run"] is True for call in orion.calls)


def test_main_uses_direction_lock_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    set_required_main_env(monkeypatch, tmp_path)
    db_connection = FakeDbConnection({60: []})
    orion = FakeOrionClient()

    class FakeAuthClient:
        def __init__(self, _settings: Any) -> None:
            pass

    monkeypatch.setattr(run_direction_module, "load_dotenv", lambda: None)
    monkeypatch.setattr(
        run_direction_module, "configure_logging", lambda _settings, *, product: None
    )
    monkeypatch.setattr(run_direction_module, "load_metadata", lambda _path: [place()])
    monkeypatch.setattr(
        run_direction_module.db, "connect", lambda _settings: db_connection
    )
    monkeypatch.setattr(run_direction_module.auth, "AuthClient", FakeAuthClient)
    monkeypatch.setattr(
        run_direction_module.orion_client,
        "OrionClient",
        lambda _settings, *, auth: orion,
    )
    monkeypatch.setattr(
        run_direction_module.entity_map,
        "validate_targets",
        lambda _places, _orion: None,
    )

    code = run_direction_main(argv=[])

    assert code == 0
    assert (tmp_path / "state" / "direction.lock").exists()
    assert not (tmp_path / "state" / "flow.lock").exists()


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

    monkeypatch.setattr(run_direction_module, "load_dotenv", lambda: None)
    monkeypatch.setattr(run_direction_module, "configure_logging", configure_logging)
    monkeypatch.setattr(run_direction_module.fcntl, "flock", raise_contention)

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline"):
        code = run_direction_main(argv=[])

    assert code == 0
    assert (
        "[sendai-pipeline] direction run skipped: lock held by another process"
        in capsys.readouterr().err
    )
    assert caplog.records == []


def test_main_writes_lifecycle_records_to_configured_log_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    set_required_main_env(monkeypatch, tmp_path)
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    monkeypatch.setenv("LOG_DIR", str(tmp_path / "logs"))
    db_connection = FakeDbConnection(
        {
            60: [
                direction_row(
                    from_group_place_id="ALL",
                    to_group_place_id="sendai202603.10",
                    count=7,
                )
            ]
        }
    )
    orion = FakeOrionClient()

    class FakeAuthClient:
        def __init__(self, _settings: Any) -> None:
            pass

    monkeypatch.setattr(run_direction_module, "load_dotenv", lambda: None)
    monkeypatch.setattr(
        run_direction_module, "load_metadata", lambda _path: [place(place_number=10)]
    )
    monkeypatch.setattr(
        run_direction_module.db, "connect", lambda _settings: db_connection
    )
    monkeypatch.setattr(run_direction_module.auth, "AuthClient", FakeAuthClient)
    monkeypatch.setattr(
        run_direction_module.orion_client,
        "OrionClient",
        lambda _settings, *, auth: orion,
    )
    monkeypatch.setattr(
        run_direction_module.entity_map,
        "validate_targets",
        lambda _places, _orion: None,
    )

    code = run_direction_main(argv=[])

    assert code == 0
    log_records = [
        json.loads(line)
        for line in (tmp_path / "logs" / "direction.log")
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
    assert lifecycle_records[0]["product"] == "direction"


def test_run_direction_logging_extras_are_allowed() -> None:
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
    }
    assert required <= _ALLOWED_EXTRA_KEYS


def test_run_direction_hash_ignores_volatile_date_retrieved(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    target = place(place_number=10)
    row = direction_row(
        from_group_place_id="ALL",
        to_group_place_id="sendai202603.10",
        count=7,
    )
    prior_hash = transformed_hash(row, [target], target_entity_id=target.entity_id)
    path = tmp_path / "state" / "direction.json"
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
    assert orion.calls == []
    skipped = records(caplog, "post_skipped_unchanged")
    assert len(skipped) == 1
    assert skipped[0].entity_id == target.entity_id
    assert records(caplog, "post_skipped_drift") == []


def test_run_direction_force_resend_reposts_unchanged_ok_target(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """force_resend=True must bypass the prior-ok hash-skip and re-POST.

    Pins the Product B side of the resend.py --force contract. Calls
    _process_send_window directly because run_once does not expose the
    kwarg.
    """
    target = place(place_number=10)
    row = direction_row(
        from_group_place_id="ALL",
        to_group_place_id="sendai202603.10",
        count=7,
    )
    prior_hash = transformed_hash(row, [target], target_entity_id=target.entity_id)
    path = tmp_path / "state" / "direction.json"
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
    interval_metadata = index_by_place_interval(
        active_places([target], target_batches=("2026",))
    )
    counts = run_direction_module._RunCounts()

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline"):
        run_direction_module._process_send_window(
            "per3600/20260523_0900",
            interval_min=60,
            startdate="20260523_0900",
            rows_for_window=[row],
            orion=orion,
            state_store=store,
            filter_settings=FilterSettings(
                ignored_place_prefixes=("quick.", "test"),
                target_flow_batches=frozenset(),
                target_direction_batches=frozenset({"2026"}),
            ),
            interval_metadata=interval_metadata,
            expected_target_ids=[target.entity_id],
            counts=counts,
            force_resend=True,
        )

    assert len(orion.calls) == 1
    assert orion.calls[0]["entity_id"] == target.entity_id
    assert records(caplog, "post_skipped_unchanged") == []
    assert records(caplog, "post_skipped_drift") == []


def test_run_direction_send_mode_reposts_drifted_ok_target_and_records_new_hash(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    target = place(place_number=10)
    row = direction_row(
        from_group_place_id="ALL",
        to_group_place_id="sendai202603.10",
        count=9,
    )
    old_hash = "0" * 64
    new_hash = transformed_hash(row, [target], target_entity_id=target.entity_id)
    path = tmp_path / "state" / "direction.json"
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
    assert record["last_payload_sha256"] == new_hash
    assert records(caplog, "post_skipped_drift") == []
    drift = records(caplog, "post_resent_drift")
    assert len(drift) == 1
    assert drift[0].prior_payload_sha256 == old_hash
    assert drift[0].computed_payload_sha256 == new_hash


def test_run_direction_uses_stored_expected_targets_when_metadata_changed(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    current_target = place(place_number=10)
    removed_entity_id = "jp.sendai.Blesensor.per3600.11"
    row = direction_row(
        from_group_place_id="ALL",
        to_group_place_id="sendai202603.10",
        count=7,
    )
    hash_value = transformed_hash(
        row,
        [current_target],
        target_entity_id=current_target.entity_id,
    )
    path = tmp_path / "state" / "direction.json"
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
            db_connection=FakeDbConnection({60: [row]}),
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
    changed = records(caplog, "window_expected_targets_changed")
    assert len(changed) == 1
    assert changed[0].window == "per3600/20260523_0900"
    assert changed[0].count_expected == 2
    assert changed[0].count_live == 1


def test_run_direction_retries_failed_target_even_when_hash_matches(
    tmp_path: Path,
) -> None:
    target = place(place_number=10)
    row = direction_row(
        from_group_place_id="ALL",
        to_group_place_id="sendai202603.10",
        count=7,
    )
    hash_value = transformed_hash(row, [target], target_entity_id=target.entity_id)
    path = tmp_path / "state" / "direction.json"
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


def test_run_direction_send_mode_garbage_collects_old_complete_windows_and_saves(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "state" / "direction.json"
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


def test_run_direction_source_stability_delay_override_changes_query_cutoff(
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
        (5, "20260523_0415", "20260523_0615"),
        (60, "20260522_1800", "20260523_0600"),
    ]


def test_run_direction_rolling_reprocess_clamps_lower_bound_and_warns_for_stuck_window(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    path = tmp_path / "state" / "direction.json"
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
    assert db_connection.queries[1] == (60, "20260520_0900", "20260523_0900")
    warnings = records(caplog, "window_giving_up_soon")
    assert len(warnings) == 1
    assert warnings[0].levelname == "ERROR"
    assert warnings[0].window == "per3600/20260519_0800"


def revision_discovery_row(startdate: str, aggregated_at: str) -> dict[str, str]:
    return {"startdate": startdate, "win_agg": aggregated_at}


def revision_direction_settings(
    tmp_path: Path,
    *,
    send_mode: str = "send",
    enabled: bool = True,
    max_windows: int = 2000,
) -> RunDirectionSettings:
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


def test_run_direction_sweep_refetches_full_window_for_people_count_flow(
    tmp_path: Path,
) -> None:
    target = place(place_number=10)
    db_connection = FakeDbConnection(
        {60: []},
        startdate_rows_by_interval={
            60: [
                direction_row(
                    startdate="20260620_0900",
                    from_group_place_id="ALL",
                    to_group_place_id="sendai202603.10",
                    count=7,
                ),
                direction_row(
                    startdate="20260620_0900",
                    from_group_place_id="sendai202603.10",
                    to_group_place_id="ALL",
                    count=3,
                ),
            ]
        },
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
        settings=revision_direction_settings(tmp_path),
        now=REVISION_NOW,
    )

    assert result.exit_code == 0
    assert db_connection.startdate_queries == [(60, ("20260620_0900",))]
    assert orion.calls[0]["attrs"]["peopleCount_flow"]["value"] == {
        "from": {"all": 7},
        "to": {"all": 3},
    }


def test_run_direction_sweeps_five_min_revision_outside_current_lookback(
    tmp_path: Path,
) -> None:
    db_connection = FakeDbConnection(
        {5: []},
        startdate_rows_by_interval={
            5: [
                direction_row(
                    startdate="20260630_0600",
                    interval_min=5,
                    from_group_place_id="ALL",
                    to_group_place_id="sendai202603.10",
                )
            ]
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
        settings=revision_direction_settings(tmp_path),
        now=REVISION_NOW,
    )

    assert db_connection.startdate_queries == [(5, ("20260630_0600",))]


def test_run_direction_advances_cursor_on_failed_sweep_post_and_retries_from_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        run_direction_module,
        "REVISION_SWEEP_DISCOVERY_SPAN",
        timedelta(hours=6),
        raising=False,
    )
    target = place(place_number=10)
    path = tmp_path / "state" / "direction.json"
    write_state(path, {}, last_aggregated_at="2026-06-30T12:00:00+09:00")
    store = WindowStateStore.load(path, now=Clock([REVISION_NOW] * 20))
    rows = [
        direction_row(
            startdate="20260620_0900",
            from_group_place_id="ALL",
            to_group_place_id="sendai202603.10",
            count=7,
        )
    ]
    first_db = FakeDbConnection(
        {60: []},
        startdate_rows_by_interval={60: rows},
        discovery_rows_by_interval={
            60: [revision_discovery_row("20260620_0900", "2026-06-30 12:01:00")]
        },
    )

    failed = run_once(
        tmp_path=tmp_path,
        db_connection=first_db,
        orion=FakeOrionClient(results=[{"status": 502, "ok": False}]),
        metadata=[target],
        store=store,
        settings=revision_direction_settings(tmp_path),
        now=REVISION_NOW,
    )

    assert failed.exit_code == 1
    assert store.revision_cursor() == REVISION_NOW.replace(microsecond=0)
    assert store.window_status("per3600/20260620_0900") == "partial"

    retry_db = FakeDbConnection({60: []}, startdate_rows_by_interval={60: rows})
    retried = run_once(
        tmp_path=tmp_path,
        db_connection=retry_db,
        orion=FakeOrionClient(),
        metadata=[target],
        store=store,
        settings=revision_direction_settings(tmp_path),
        now=REVISION_NOW + timedelta(minutes=5),
    )

    assert retried.exit_code == 0
    assert retry_db.startdate_queries == [(60, ("20260620_0900",))]
    assert store.window_status("per3600/20260620_0900") == "complete"


def test_run_direction_suppresses_retry_during_chunked_drain(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        run_direction_module,
        "REVISION_SWEEP_DISCOVERY_SPAN",
        timedelta(hours=6),
        raising=False,
    )
    path = tmp_path / "state" / "direction.json"
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
        startdate_rows_by_interval={60: [direction_row(startdate="20260620_0900")]},
    )

    run_once(
        tmp_path=tmp_path,
        db_connection=db_connection,
        orion=FakeOrionClient(),
        metadata=[place()],
        store=store,
        settings=revision_direction_settings(tmp_path),
        now=REVISION_NOW,
    )

    assert store.window_status("per3600/20260620_0900") == "partial"
    assert db_connection.startdate_queries == []


def test_run_direction_does_not_repost_gc_succeeded_same_second_sibling(
    tmp_path: Path,
) -> None:
    # Cursor already passed the siblings' shared aggregated_at second: the
    # partial window retries from state, while the GC'd success is not rediscovered.
    target = place(place_number=10)
    path = tmp_path / "state" / "direction.json"
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
    orion = FakeOrionClient()
    db_connection = FakeDbConnection(
        {60: []},
        startdate_rows_by_interval={60: [direction_row(startdate="20260620_0900")]},
    )

    run_once(
        tmp_path=tmp_path,
        db_connection=db_connection,
        orion=orion,
        metadata=[target],
        store=WindowStateStore.load(path, now=Clock([REVISION_NOW] * 10)),
        settings=revision_direction_settings(tmp_path),
        now=REVISION_NOW,
    )

    assert len(orion.calls) == 1
    assert db_connection.startdate_queries == [(60, ("20260620_0900",))]


def test_run_direction_logs_give_up_signal_for_old_revision_retry(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    path = tmp_path / "state" / "direction.json"
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
                startdate_rows_by_interval={
                    60: [direction_row(startdate="20260620_0900")]
                },
            ),
            orion=FakeOrionClient(results=[{"status": 502, "ok": False}]),
            metadata=[place()],
            store=WindowStateStore.load(path, now=Clock([REVISION_NOW] * 10)),
            settings=revision_direction_settings(tmp_path),
            now=REVISION_NOW,
        )

    assert [record.window for record in records(caplog, "window_giving_up_soon")] == [
        "per3600/20260620_0900"
    ]


def test_run_direction_excludes_cap_deferred_revision_from_retry_this_run(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state" / "direction.json"
    write_state(
        path,
        {
            "per3600/20260620_1000": window_record(
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
        startdate_rows_by_interval={
            60: [
                direction_row(startdate="20260620_0900"),
                direction_row(startdate="20260620_1000"),
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
        metadata=[place()],
        store=WindowStateStore.load(path, now=Clock([REVISION_NOW] * 10)),
        settings=revision_direction_settings(tmp_path, max_windows=1),
        now=REVISION_NOW,
    )

    assert db_connection.startdate_queries == [(60, ("20260620_0900",))]


def test_run_direction_per_window_error_does_not_advance_cursor(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state" / "direction.json"
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
                startdate_rows_by_interval={
                    60: [direction_row(startdate="20260620_0900")]
                },
                discovery_rows_by_interval={
                    60: [revision_discovery_row("20260620_0900", "2026-06-30 12:01:00")]
                },
            ),
            orion=CrashingOrion(),
            metadata=[place()],
            store=store,
            settings=revision_direction_settings(tmp_path),
            now=REVISION_NOW,
        )

    assert store.revision_cursor() == datetime.fromisoformat(
        "2026-06-30T12:00:00+09:00"
    )


def test_run_direction_skips_revised_dead_letter_window(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state" / "direction.json"
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
            startdate_rows_by_interval={60: [direction_row(startdate="20260620_0900")]},
            discovery_rows_by_interval={
                60: [revision_discovery_row("20260620_0900", "2026-06-30 12:01:00")]
            },
        ),
        orion=orion,
        metadata=[place()],
        store=WindowStateStore.load(path, now=Clock([REVISION_NOW] * 10)),
        settings=revision_direction_settings(tmp_path),
        now=REVISION_NOW,
    )

    assert orion.calls == []


def test_run_direction_sweep_zero_payload_window_leaves_no_state(
    tmp_path: Path,
) -> None:
    store = state_store(tmp_path, Clock([REVISION_NOW] * 10))

    run_once(
        tmp_path=tmp_path,
        db_connection=FakeDbConnection(
            {60: []},
            startdate_rows_by_interval={
                60: [
                    direction_row(
                        startdate="20260620_0900",
                        from_group_place_id="quick.10",
                        to_group_place_id="quick.11",
                    )
                ]
            },
            discovery_rows_by_interval={
                60: [revision_discovery_row("20260620_0900", "2026-06-30 12:01:00")]
            },
        ),
        orion=FakeOrionClient(),
        metadata=[place()],
        store=store,
        settings=revision_direction_settings(tmp_path),
        now=REVISION_NOW,
    )

    assert "per3600/20260620_0900" not in store.as_dict()["windows"]


def test_run_direction_revision_sweep_disabled_does_not_scan_or_resend(
    tmp_path: Path,
) -> None:
    db_connection = FakeDbConnection(
        {60: []},
        startdate_rows_by_interval={60: [direction_row(startdate="20260620_0900")]},
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
        settings=revision_direction_settings(tmp_path, enabled=False),
        now=REVISION_NOW,
    )

    assert db_connection.discovery_queries == []
    assert db_connection.startdate_queries == []
    assert orion.calls == []


def test_run_direction_revision_sweep_dry_run_previews_without_mutation_or_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "state" / "direction.json"
    write_state(path, {}, last_aggregated_at="2026-06-30T12:00:00+09:00")
    store = WindowStateStore.load(path, now=Clock([REVISION_NOW] * 10))
    before = deepcopy(store.as_dict())
    forbid_state_mutations(monkeypatch, store)
    orion = FakeOrionClient()

    run_once(
        tmp_path=tmp_path,
        db_connection=FakeDbConnection(
            {60: []},
            startdate_rows_by_interval={60: [direction_row(startdate="20260620_0900")]},
            discovery_rows_by_interval={
                60: [revision_discovery_row("20260620_0900", "2026-06-30 12:01:00")]
            },
        ),
        orion=orion,
        metadata=[place()],
        store=store,
        settings=revision_direction_settings(tmp_path, send_mode="dry-run"),
        now=REVISION_NOW,
    )

    assert store.as_dict() == before
    assert [call["dry_run"] for call in orion.calls] == [True]


def test_run_direction_revision_cursor_seeds_from_constant_when_absent(
    tmp_path: Path,
) -> None:
    db_connection = FakeDbConnection({60: []})

    run_once(
        tmp_path=tmp_path,
        db_connection=db_connection,
        orion=FakeOrionClient(),
        metadata=[place()],
        settings=revision_direction_settings(tmp_path),
        now=REVISION_NOW,
    )

    assert db_connection.discovery_queries[0][1] == revision_seed_query_bound(
        run_direction_module.REVISION_CURSOR_SEED
    )


def test_run_direction_soft_cap_keeps_same_aggregated_at_second_together(
    tmp_path: Path,
) -> None:
    db_connection = FakeDbConnection(
        {60: []},
        startdate_rows_by_interval={
            60: [
                direction_row(startdate="20260620_0900"),
                direction_row(startdate="20260620_1000"),
            ]
        },
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
        metadata=[place()],
        settings=revision_direction_settings(tmp_path, max_windows=1),
        now=REVISION_NOW,
    )

    assert db_connection.startdate_queries == [(60, ("20260620_0900", "20260620_1000"))]


def test_run_direction_revision_cap_bounds_total_work(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state" / "direction.json"
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
        startdate_rows_by_interval={60: [direction_row(startdate="20260620_0900")]},
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
        settings=revision_direction_settings(tmp_path, max_windows=1),
        now=REVISION_NOW,
    )

    assert len(db_connection.startdate_queries) == 1


def test_run_direction_revision_discovery_chunks_by_span(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        run_direction_module,
        "REVISION_SWEEP_DISCOVERY_SPAN",
        timedelta(hours=6),
        raising=False,
    )
    path = tmp_path / "state" / "direction.json"
    write_state(path, {}, last_aggregated_at="2026-06-23T00:00:00+09:00")
    db_connection = FakeDbConnection({60: []})

    run_once(
        tmp_path=tmp_path,
        db_connection=db_connection,
        orion=FakeOrionClient(),
        metadata=[place()],
        store=WindowStateStore.load(path, now=Clock([REVISION_NOW] * 10)),
        settings=revision_direction_settings(tmp_path),
        now=REVISION_NOW,
    )

    assert db_connection.discovery_queries[0][2] == "2026-06-23 06:00:00"


def test_run_direction_initial_drain_bounds_redundant_unrevised_resends(
    tmp_path: Path,
) -> None:
    target = place(place_number=10)
    recent_row = direction_row(
        startdate="20260629_0900",
        from_group_place_id="ALL",
        to_group_place_id="sendai202603.10",
    )
    path = tmp_path / "state" / "direction.json"
    window = window_record(
        first_seen=REVISION_NOW - timedelta(hours=12),
        last_attempt=REVISION_NOW - timedelta(hours=12),
        status="complete",
        targets={
            target.entity_id: target_record(
                status="ok",
                payload_sha256=transformed_hash(
                    recent_row,
                    [target],
                    target_entity_id=target.entity_id,
                ),
            )
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
                    direction_row(
                        startdate="20260624_0900",
                        from_group_place_id="ALL",
                        to_group_place_id="sendai202603.10",
                    ),
                    recent_row,
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
        settings=revision_direction_settings(tmp_path),
        now=REVISION_NOW,
    )

    assert [call["attrs"]["dateObservedFrom"]["value"] for call in orion.calls] == [
        "2026-06-24T09:00:00+09:00"
    ]
