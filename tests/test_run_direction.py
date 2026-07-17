import hashlib
import json
import logging
from collections.abc import Iterable, Mapping
from copy import deepcopy
from dataclasses import fields
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

import sendai_pipeline.run_direction as run_direction_module
from sendai_pipeline.filter_settings import FilterSettings
from sendai_pipeline.logging_setup import _ALLOWED_EXTRA_KEYS
from sendai_pipeline.metadata import SensorPlace
from sendai_pipeline.run_direction import (
    RunDirectionResult,
    RunDirectionSettings,
    run_direction,
)
from sendai_pipeline.state import WindowStateStore
from sendai_pipeline.transform_direction import (
    DirectionPayloadOutcome,
    transform_direction_window,
)

JST = timezone(timedelta(hours=9))
NOW = datetime(2026, 7, 15, 12, 17, 43, 123456, tzinfo=JST)
AGGREGATE_ID = "jp.sendai.Blesensor.flow"
AGGREGATE_TYPE = "Blesensor.flow"


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

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, sql: str, params: tuple[Any, ...]) -> None:
        normalized_sql = " ".join(sql.split())
        if "MAX(aggregated_at)" in normalized_sql:
            interval_min, aggregated_at_lower, aggregated_at_upper, startdate_upper = (
                params
            )
            interval = int(interval_min)
            self._connection.discovery_queries.append(
                (
                    interval,
                    str(aggregated_at_lower),
                    str(aggregated_at_upper),
                    str(startdate_upper),
                )
            )
            self._rows = list(
                self._connection.discovery_rows_by_interval.get(interval, [])
            )
            return

        if "startdate IN" in normalized_sql:
            interval = int(params[0])
            startdates = tuple(str(value) for value in params[1:])
            self._connection.startdate_queries.append((interval, startdates))
            wanted = set(startdates)
            self._rows = [
                row
                for row in self._connection.startdate_rows_by_interval.get(interval, [])
                if str(row["startdate"]) in wanted
            ]
            return

        interval_min, lower_bound, upper_bound = params
        interval = int(interval_min)
        self._connection.fresh_queries.append(
            (interval, str(lower_bound), str(upper_bound))
        )
        self._rows = list(self._connection.rows_by_interval.get(interval, []))

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
        self.startdate_rows_by_interval = {
            interval: [dict(row) for row in rows]
            for interval, rows in (
                startdate_rows_by_interval
                if startdate_rows_by_interval is not None
                else self.rows_by_interval
            ).items()
        }
        self.discovery_rows_by_interval = {
            interval: [dict(row) for row in rows]
            for interval, rows in (discovery_rows_by_interval or {}).items()
        }
        self.fresh_queries: list[tuple[int, str, str]] = []
        self.startdate_queries: list[tuple[int, tuple[str, ...]]] = []
        self.discovery_queries: list[tuple[int, str, str, str]] = []

    def cursor(self) -> FakeDbCursor:
        return FakeDbCursor(self)


class FakeOrionClient:
    def __init__(
        self,
        results: Iterable[Mapping[str, Any]] = (),
        *,
        existing_entity_ids: Iterable[str] = (AGGREGATE_ID,),
    ) -> None:
        self._results = [dict(result) for result in results]
        self._existing_entity_ids = list(existing_entity_ids)
        self.replace_calls: list[dict[str, Any]] = []
        self.update_calls: list[dict[str, Any]] = []
        self.list_entities_calls: list[dict[str, Any]] = []

    def replace_attrs(
        self,
        entity_id: str,
        entity_type: str | None,
        attrs: Mapping[str, Any],
        *,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        self.replace_calls.append(
            {
                "entity_id": entity_id,
                "entity_type": entity_type,
                "attrs": deepcopy(dict(attrs)),
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

    def update_attrs(
        self,
        entity_id: str,
        entity_type: str | None,
        attrs: Mapping[str, Any],
        *,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        self.update_calls.append(
            {
                "entity_id": entity_id,
                "entity_type": entity_type,
                "attrs": deepcopy(dict(attrs)),
                "dry_run": dry_run,
            }
        )
        raise AssertionError("Product B must not use per-place POST/update_attrs")

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
        return [
            {"id": entity_id, "type": entity_type}
            for entity_id in self._existing_entity_ids
        ]


def place(
    place_number: int,
    *,
    interval_min: int = 60,
    batch: str = "2026",
) -> SensorPlace:
    entity_type = "Blesensor.per3600" if interval_min == 60 else "Blesensor.per300"
    return SensorPlace(
        place_number=place_number,
        batch=batch,
        expected_device_type="M5Stack",
        interval_min=interval_min,
        entity_type=entity_type,
        entity_id=f"jp.sendai.{entity_type}.{place_number}",
        identifcation=str(place_number),
        active=True,
    )


def direction_row(**overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "startdate": "20260715_0900",
        "from_group_place_id": "ALL",
        "to_group_place_id": "sendai202603.10",
        "from_device_type": "M5Stack",
        "to_device_type": "M5Stack",
        "interval_min": 60,
        "count": 8,
    }
    row.update(overrides)
    return row


def sendable_window(
    startdate: str = "20260715_0900",
    *,
    place_number: int = 10,
    from_all: int = 8,
    to_all: int = 6,
) -> list[dict[str, Any]]:
    source_id = f"sendai202603.{place_number}"
    return [
        direction_row(
            startdate=startdate,
            from_group_place_id="ALL",
            to_group_place_id=source_id,
            count=from_all,
        ),
        direction_row(
            startdate=startdate,
            from_group_place_id=source_id,
            to_group_place_id="ALL",
            count=to_all,
        ),
    ]


def degraded_window(
    startdate: str = "20260715_0900",
    *,
    boundary_count: int = 3,
) -> list[dict[str, Any]]:
    return [
        *sendable_window(startdate, place_number=10, from_all=20, to_all=21),
        direction_row(
            startdate=startdate,
            from_group_place_id="ALL",
            to_group_place_id="sendai202603.11",
            count=12,
        ),
        direction_row(
            startdate=startdate,
            from_group_place_id="sendai202603.10",
            to_group_place_id="sendai202603.11",
            count=boundary_count,
        ),
    ]


def settings(tmp_path: Path, **overrides: Any) -> RunDirectionSettings:
    values: dict[str, Any] = {
        "send_mode": "send",
        "reprocess_hours_per3600": 12,
        "reprocess_hours_per300": 2,
        "max_lookback_hours_per3600": 72,
        "max_lookback_hours_per300": 72,
        "source_stability_delay_hours": 3,
        "revision_sweep_enabled": False,
        "state_path": tmp_path / "state" / "direction.json",
        "lock_path": tmp_path / "state" / "direction.lock",
        "product_b_aggregate_entity_id": AGGREGATE_ID,
        "product_b_aggregate_entity_type": AGGREGATE_TYPE,
    }
    values.update(overrides)
    return RunDirectionSettings(**values)


def filters(target_batches: Iterable[str] = ("2026",)) -> FilterSettings:
    return FilterSettings(
        target_flow_batches=frozenset(),
        target_direction_batches=frozenset(target_batches),
        ignored_place_prefixes=("quick.", "test"),
    )


def store(tmp_path: Path, *, now: datetime = NOW) -> WindowStateStore:
    return WindowStateStore(
        tmp_path / "state" / "direction.json", now=Clock([now] * 30)
    )


def run_once(
    *,
    tmp_path: Path,
    db_connection: FakeDbConnection,
    orion: FakeOrionClient,
    metadata: Iterable[SensorPlace] = (place(10),),
    state_store: WindowStateStore | None = None,
    run_settings: RunDirectionSettings | None = None,
    filter_settings: FilterSettings | None = None,
    now: datetime = NOW,
) -> RunDirectionResult:
    return run_direction(
        db_connection=db_connection,
        orion=orion,
        metadata=list(metadata),
        state_store=state_store or store(tmp_path, now=now),
        settings=run_settings or settings(tmp_path),
        filter_settings=filter_settings or filters(),
        now=Clock([now]),
    )


def records(caplog: pytest.LogCaptureFixture, event: str) -> list[logging.LogRecord]:
    return [
        record for record in caplog.records if getattr(record, "event", None) == event
    ]


def attrs_hash(attrs: Mapping[str, Any]) -> str:
    stable_attrs = deepcopy(
        {key: value for key, value in attrs.items() if key != "dateRetrieved"}
    )
    quality = stable_attrs.get("sourceQuality")
    if isinstance(quality, dict) and isinstance(quality.get("value"), dict):
        quality["value"].pop("evaluatedAt", None)
    encoded = json.dumps(stable_attrs, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def target_record(
    *,
    status: str,
    payload_sha256: str = "0" * 64,
    http_status: int = 204,
    last_attempt_at: datetime = NOW,
) -> dict[str, Any]:
    return {
        "status": status,
        "last_attempt_at": last_attempt_at.isoformat(),
        "last_http_status": http_status,
        "last_payload_sha256": payload_sha256,
    }


def window_record(
    *,
    status: str,
    expected_target_ids: Iterable[str] = (AGGREGATE_ID,),
    targets: Mapping[str, Mapping[str, Any]] | None = None,
    when: datetime = NOW - timedelta(days=10),
) -> dict[str, Any]:
    return {
        "first_seen": when.isoformat(),
        "last_attempt": when.isoformat(),
        "attempt_count": 1,
        "expected_target_ids": list(expected_target_ids),
        "targets": {key: dict(value) for key, value in (targets or {}).items()},
        "status": status,
    }


def write_state(
    path: Path,
    windows: Mapping[str, Mapping[str, Any]],
    *,
    last_aggregated_at: str | None,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "schema_version": 3,
                "last_aggregated_at": last_aggregated_at,
                "windows": windows,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n",
        encoding="utf-8",
    )


def forbid_state_mutation(
    monkeypatch: pytest.MonkeyPatch, state_store: WindowStateStore
) -> None:
    def fail(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("dry-run and gate no-op must not mutate state")

    monkeypatch.setattr(state_store, "begin_window_attempt", fail)
    monkeypatch.setattr(state_store, "record_target", fail)
    monkeypatch.setattr(state_store, "recompute_status", fail)
    monkeypatch.setattr(state_store, "set_revision_cursor", fail)
    monkeypatch.setattr(state_store, "gc_complete_before", fail)
    monkeypatch.setattr(state_store, "save", fail)


def revision_row(startdate: str, aggregated_at: str) -> dict[str, str]:
    return {"startdate": startdate, "win_agg": aggregated_at}


def test_run_direction_result_uses_put_counters_and_no_write_window_counters() -> None:
    assert [field.name for field in fields(RunDirectionResult)] == [
        "windows_seen",
        "windows_complete",
        "windows_partial",
        "windows_dead_letter",
        "puts_ok",
        "puts_failed",
        "windows_degraded",
        "windows_no_payload",
        "windows_source_invalid",
        "rows_dropped",
        "oldest_non_complete",
        "lookback_hours_used",
        "exit_code",
    ]


def test_run_direction_empty_batch_gate_is_safe_no_op(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state_store = store(tmp_path)
    forbid_state_mutation(monkeypatch, state_store)
    db_connection = FakeDbConnection({60: sendable_window()})
    orion = FakeOrionClient()

    result = run_once(
        tmp_path=tmp_path,
        db_connection=db_connection,
        orion=orion,
        state_store=state_store,
        filter_settings=filters(()),
    )

    assert result.exit_code == 0
    assert result.windows_seen == 0
    assert result.puts_ok == 0
    assert db_connection.fresh_queries == []
    assert db_connection.discovery_queries == []
    assert orion.list_entities_calls == []
    assert orion.replace_calls == []
    assert orion.update_calls == []
    assert not state_store.path.exists()


def test_run_direction_target_batches_gate_source_metadata(
    tmp_path: Path,
) -> None:
    rows = [
        direction_row(
            from_group_place_id="ALL",
            to_group_place_id="sendai2023.10",
            count=12,
        ),
        direction_row(
            from_group_place_id="sendai2023.10",
            to_group_place_id="ALL",
            count=9,
        ),
        *sendable_window(place_number=11, from_all=8, to_all=6),
    ]
    orion = FakeOrionClient()

    result = run_once(
        tmp_path=tmp_path,
        db_connection=FakeDbConnection({60: rows}),
        orion=orion,
        metadata=[place(10, batch="2023"), place(11, batch="2026")],
        filter_settings=filters(("2026",)),
    )

    assert result.exit_code == 0
    assert result.rows_dropped == 2
    assert set(orion.replace_calls[0]["attrs"]) == {
        "dateObservedFrom",
        "dateObservedTo",
        "dateRetrieved",
        "identifcation",
        "sourceQuality",
        "peopleCount_flow_11",
    }


def test_run_direction_queries_only_60_minute_source_and_uses_only_per3600_state(
    tmp_path: Path,
) -> None:
    db_connection = FakeDbConnection(
        {
            5: sendable_window("20260715_0915"),
            60: sendable_window(),
        }
    )
    state_store = store(tmp_path)
    orion = FakeOrionClient()

    result = run_once(
        tmp_path=tmp_path,
        db_connection=db_connection,
        orion=orion,
        metadata=[place(10, interval_min=5), place(10)],
        state_store=state_store,
    )

    assert result.exit_code == 0
    assert [query[0] for query in db_connection.fresh_queries] == [60]
    assert result.lookback_hours_used == {60: 12.0}
    assert set(state_store.as_dict()["windows"]) == {"per3600/20260715_0900"}
    assert len(orion.replace_calls) == 1
    assert orion.update_calls == []


def test_run_direction_groups_each_source_window_before_transform_and_put(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    rows = [
        *sendable_window("20260715_0800", from_all=5, to_all=4),
        *sendable_window("20260715_0900", from_all=8, to_all=6),
        *sendable_window("20260715_1000", from_all=9, to_all=7),
    ]
    transformed_windows: list[tuple[str, ...]] = []
    real_transform = transform_direction_window

    def transform_spy(
        source_rows: Iterable[Mapping[str, Any]],
        metadata_index: Mapping[tuple[int, int], SensorPlace],
        **kwargs: Any,
    ) -> Any:
        materialized = list(source_rows)
        transformed_windows.append(tuple(str(row["startdate"]) for row in materialized))
        return real_transform(materialized, metadata_index, **kwargs)

    monkeypatch.setattr(
        run_direction_module, "transform_direction_window", transform_spy
    )
    orion = FakeOrionClient()

    result = run_once(
        tmp_path=tmp_path,
        db_connection=FakeDbConnection({60: rows}),
        orion=orion,
    )

    assert result.windows_seen == 3
    assert result.windows_complete == 3
    assert result.puts_ok == 3
    assert transformed_windows == [
        ("20260715_0800", "20260715_0800"),
        ("20260715_0900", "20260715_0900"),
        ("20260715_1000", "20260715_1000"),
    ]
    assert len(orion.replace_calls) == 3
    assert {call["entity_id"] for call in orion.replace_calls} == {AGGREGATE_ID}
    assert orion.update_calls == []


def test_run_direction_204_put_records_one_aggregate_target_ok(
    tmp_path: Path,
) -> None:
    state_store = store(tmp_path)
    orion = FakeOrionClient([{"status": 204, "ok": True}])

    result = run_once(
        tmp_path=tmp_path,
        db_connection=FakeDbConnection({60: sendable_window()}),
        orion=orion,
        state_store=state_store,
    )

    assert result.exit_code == 0
    assert result.windows_complete == 1
    assert result.windows_partial == 0
    assert result.puts_ok == 1
    assert result.puts_failed == 0
    window = state_store.as_dict()["windows"]["per3600/20260715_0900"]
    assert window["status"] == "complete"
    assert window["expected_target_ids"] == [AGGREGATE_ID]
    assert set(window["targets"]) == {AGGREGATE_ID}
    assert window["targets"][AGGREGATE_ID]["status"] == "ok"
    assert window["targets"][AGGREGATE_ID]["last_http_status"] == 204
    assert orion.replace_calls[0]["entity_type"] == AGGREGATE_TYPE
    assert orion.update_calls == []


def test_run_direction_uses_configured_aggregate_target_everywhere(
    tmp_path: Path,
) -> None:
    custom_id = "tenant.example.aggregate-direction"
    custom_type = "TenantAggregateDirection"
    state_store = store(tmp_path)
    orion = FakeOrionClient(existing_entity_ids=(custom_id,))

    result = run_once(
        tmp_path=tmp_path,
        db_connection=FakeDbConnection({60: sendable_window()}),
        orion=orion,
        state_store=state_store,
        run_settings=settings(
            tmp_path,
            product_b_aggregate_entity_id=custom_id,
            product_b_aggregate_entity_type=custom_type,
        ),
    )

    assert result.exit_code == 0
    assert orion.list_entities_calls[0]["entity_type"] == custom_type
    assert orion.replace_calls[0]["entity_id"] == custom_id
    assert orion.replace_calls[0]["entity_type"] == custom_type
    assert orion.replace_calls[0]["attrs"]["identifcation"]["value"] == custom_id
    window = state_store.as_dict()["windows"]["per3600/20260715_0900"]
    assert window["expected_target_ids"] == [custom_id]
    assert set(window["targets"]) == {custom_id}


def test_run_direction_replaces_stored_expected_target_without_pinning(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    path = tmp_path / "state" / "direction.json"
    write_state(
        path,
        {
            "per3600/20260715_0900": window_record(
                status="partial",
                expected_target_ids=("legacy.per-place.target",),
                targets={"legacy.per-place.target": target_record(status="failed")},
            )
        },
        last_aggregated_at=NOW.replace(microsecond=0).isoformat(),
    )
    state_store = WindowStateStore.load(path, now=Clock([NOW] * 30))

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline"):
        result = run_once(
            tmp_path=tmp_path,
            db_connection=FakeDbConnection({60: sendable_window()}),
            orion=FakeOrionClient(),
            state_store=state_store,
        )

    assert result.exit_code == 0
    window = state_store.as_dict()["windows"]["per3600/20260715_0900"]
    assert window["expected_target_ids"] == [AGGREGATE_ID]
    assert window["status"] == "complete"
    assert records(caplog, "window_expected_targets_changed") == []


def test_run_direction_hashes_full_payload_but_ignores_date_retrieved(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    state_store = store(tmp_path)
    orion = FakeOrionClient()

    first = run_once(
        tmp_path=tmp_path,
        db_connection=FakeDbConnection({60: sendable_window(from_all=8)}),
        orion=orion,
        state_store=state_store,
        now=NOW,
    )
    first_attrs = orion.replace_calls[0]["attrs"]
    saved = state_store.target_record("per3600/20260715_0900", AGGREGATE_ID)
    assert first.puts_ok == 1
    assert saved is not None
    assert saved["last_payload_sha256"] == attrs_hash(first_attrs)

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline"):
        second = run_once(
            tmp_path=tmp_path,
            db_connection=FakeDbConnection({60: sendable_window(from_all=8)}),
            orion=orion,
            state_store=state_store,
            now=NOW + timedelta(minutes=1),
        )

    assert second.puts_ok == 0
    assert len(orion.replace_calls) == 1
    assert len(records(caplog, "put_skipped_unchanged")) == 1

    third = run_once(
        tmp_path=tmp_path,
        db_connection=FakeDbConnection({60: sendable_window(from_all=9)}),
        orion=orion,
        state_store=state_store,
        now=NOW + timedelta(minutes=2),
    )

    assert third.puts_ok == 1
    assert len(orion.replace_calls) == 2
    changed = state_store.target_record("per3600/20260715_0900", AGGREGATE_ID)
    assert changed is not None
    assert changed["last_payload_sha256"] == attrs_hash(orion.replace_calls[1]["attrs"])
    assert changed["last_payload_sha256"] != saved["last_payload_sha256"]


def test_run_direction_zero_candidate_logs_debug_without_state(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    no_candidate_rows = [
        direction_row(
            from_group_place_id="quick.10",
            to_group_place_id="sendai202603.10",
        )
    ]
    state_store = store(tmp_path)

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline"):
        result = run_once(
            tmp_path=tmp_path,
            db_connection=FakeDbConnection({60: no_candidate_rows}),
            orion=FakeOrionClient(),
            state_store=state_store,
        )

    assert result.exit_code == 0
    assert result.windows_seen == 1
    assert result.windows_no_payload == 1
    assert result.windows_source_invalid == 0
    assert result.puts_ok == result.puts_failed == 0
    assert "per3600/20260715_0900" not in state_store.as_dict()["windows"]
    event = records(caplog, "direction_window_no_payload")
    assert len(event) == 1
    assert event[0].levelno == logging.DEBUG
    assert getattr(event[0], "window") == "per3600/20260715_0900"
    summary = records(caplog, "run_summary")
    assert len(summary) == 1
    assert getattr(summary[0], "windows_seen") == 1
    assert getattr(summary[0], "windows_no_payload") == 1
    assert getattr(summary[0], "windows_source_invalid") == 0
    assert getattr(summary[0], "puts_ok") == 0
    assert getattr(summary[0], "puts_failed") == 0


def test_run_direction_source_invalid_logs_missing_lists_and_exits_nonzero(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    invalid_rows = [
        direction_row(
            from_group_place_id="ALL",
            to_group_place_id="sendai202603.10",
            count=8,
        ),
        direction_row(
            from_group_place_id="sendai202603.11",
            to_group_place_id="ALL",
            count=7,
        ),
    ]
    state_store = store(tmp_path)
    orion = FakeOrionClient()

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline"):
        result = run_once(
            tmp_path=tmp_path,
            db_connection=FakeDbConnection({60: invalid_rows}),
            orion=orion,
            metadata=[place(10), place(11)],
            state_store=state_store,
        )

    assert result.exit_code == 1
    assert result.windows_seen == 1
    assert result.windows_no_payload == 0
    assert result.windows_source_invalid == 1
    assert result.puts_ok == result.puts_failed == 0
    assert "per3600/20260715_0900" not in state_store.as_dict()["windows"]
    assert orion.replace_calls == []
    event = records(caplog, "direction_window_source_invalid")
    assert len(event) == 1
    assert event[0].levelno == logging.WARNING
    assert getattr(event[0], "window") == "per3600/20260715_0900"
    assert getattr(event[0], "missing_from_all_place_numbers") == [11]
    assert getattr(event[0], "missing_to_all_place_numbers") == [10]


def test_run_direction_missing_aggregate_entity_logs_and_put_stays_authoritative(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    orion = FakeOrionClient(existing_entity_ids=("some.other.entity",))

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline"):
        result = run_once(
            tmp_path=tmp_path,
            db_connection=FakeDbConnection({60: sendable_window()}),
            orion=orion,
        )

    assert result.exit_code == 0
    assert result.puts_ok == 1
    assert len(orion.list_entities_calls) == 1
    assert orion.list_entities_calls[0]["entity_type"] == AGGREGATE_TYPE
    assert len(orion.replace_calls) == 1
    missing = records(caplog, "entity_map_missing_target")
    assert len(missing) == 1
    assert missing[0].levelno == logging.WARNING
    assert getattr(missing[0], "entity_id") == AGGREGATE_ID
    assert getattr(missing[0], "entity_type") == AGGREGATE_TYPE


def test_run_direction_missing_aggregate_entity_put_failure_controls_exit(
    tmp_path: Path,
) -> None:
    orion = FakeOrionClient([{"status": 404, "ok": False}], existing_entity_ids=())

    result = run_once(
        tmp_path=tmp_path,
        db_connection=FakeDbConnection({60: sendable_window()}),
        orion=orion,
    )

    assert result.exit_code == 1
    assert result.puts_ok == 0
    assert result.puts_failed == 1
    assert result.windows_partial == 1
    assert len(orion.replace_calls) == 1


def test_run_direction_dry_run_previews_full_replace_without_state_mutation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state_store = store(tmp_path)
    before = deepcopy(state_store.as_dict())
    forbid_state_mutation(monkeypatch, state_store)
    orion = FakeOrionClient()

    result = run_once(
        tmp_path=tmp_path,
        db_connection=FakeDbConnection({60: sendable_window()}),
        orion=orion,
        state_store=state_store,
        run_settings=settings(tmp_path, send_mode="dry-run"),
    )

    assert result.exit_code == 0
    assert state_store.as_dict() == before
    assert not state_store.path.exists()
    assert len(orion.replace_calls) == 1
    call = orion.replace_calls[0]
    assert call["dry_run"] is True
    assert call["entity_id"] == AGGREGATE_ID
    assert call["entity_type"] == AGGREGATE_TYPE
    assert set(call["attrs"]) == {
        "dateObservedFrom",
        "dateObservedTo",
        "dateRetrieved",
        "identifcation",
        "sourceQuality",
        "peopleCount_flow_10",
    }
    assert call["attrs"]["peopleCount_flow_10"]["value"] == {
        "from": {"10": 0, "all": 8},
        "to": {"10": 0, "all": 6},
    }
    assert orion.update_calls == []


def test_run_direction_initializes_missing_send_cursor_to_run_start_without_scan(
    tmp_path: Path,
) -> None:
    state_store = store(tmp_path)
    db_connection = FakeDbConnection({60: []})

    result = run_once(
        tmp_path=tmp_path,
        db_connection=db_connection,
        orion=FakeOrionClient(),
        state_store=state_store,
        run_settings=settings(tmp_path, revision_sweep_enabled=True),
    )

    assert result.exit_code == 0
    assert state_store.revision_cursor() == NOW.replace(microsecond=0)
    assert db_connection.discovery_queries == []
    saved = json.loads(state_store.path.read_text(encoding="utf-8"))
    assert saved["last_aggregated_at"] == NOW.replace(microsecond=0).isoformat()


def test_run_direction_dry_run_with_missing_cursor_does_not_initialize_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state_store = store(tmp_path)
    before = deepcopy(state_store.as_dict())
    forbid_state_mutation(monkeypatch, state_store)
    db_connection = FakeDbConnection({60: []})

    run_once(
        tmp_path=tmp_path,
        db_connection=db_connection,
        orion=FakeOrionClient(),
        state_store=state_store,
        run_settings=settings(
            tmp_path, send_mode="dry-run", revision_sweep_enabled=True
        ),
    )

    assert state_store.as_dict() == before
    assert state_store.revision_cursor() is None
    assert not state_store.path.exists()
    assert db_connection.discovery_queries == []


def test_run_direction_revision_sweep_discovers_only_60_and_refetches_full_window(
    tmp_path: Path,
) -> None:
    revised_startdate = "20260701_0900"
    complete_rows = [
        *sendable_window(revised_startdate, place_number=10, from_all=8, to_all=6),
        *sendable_window(revised_startdate, place_number=11, from_all=7, to_all=5),
        direction_row(
            startdate=revised_startdate,
            from_group_place_id="sendai202603.10",
            to_group_place_id="sendai202603.11",
            count=3,
        ),
    ]
    path = tmp_path / "state" / "direction.json"
    write_state(
        path,
        {},
        last_aggregated_at="2026-07-15T12:00:00+09:00",
    )
    state_store = WindowStateStore.load(path, now=Clock([NOW] * 30))
    db_connection = FakeDbConnection(
        {60: []},
        startdate_rows_by_interval={60: complete_rows},
        discovery_rows_by_interval={
            5: [revision_row("20260715_0905", "2026-07-15 12:01:00")],
            60: [revision_row(revised_startdate, "2026-07-15 12:01:00")],
        },
    )
    orion = FakeOrionClient()

    result = run_once(
        tmp_path=tmp_path,
        db_connection=db_connection,
        orion=orion,
        metadata=[place(10), place(11), place(10, interval_min=5)],
        state_store=state_store,
        run_settings=settings(tmp_path, revision_sweep_enabled=True),
    )

    assert result.exit_code == 0
    assert [query[0] for query in db_connection.discovery_queries] == [60]
    assert db_connection.startdate_queries == [(60, (revised_startdate,))]
    assert len(orion.replace_calls) == 1
    attrs = orion.replace_calls[0]["attrs"]
    assert attrs["peopleCount_flow_10"]["value"] == {
        "from": {"10": 0, "11": 0, "all": 8},
        "to": {"10": 0, "11": 3, "all": 6},
    }
    assert attrs["peopleCount_flow_11"]["value"] == {
        "from": {"10": 3, "11": 0, "all": 7},
        "to": {"10": 0, "11": 0, "all": 5},
    }
    assert orion.update_calls == []


def test_run_direction_revision_resends_unchanged_complete_window_with_new_retrieval(
    tmp_path: Path,
) -> None:
    revised_startdate = "20260701_0900"
    rows = sendable_window(revised_startdate)
    old_retrieval = NOW - timedelta(days=1)
    prior_outcome = transform_direction_window(
        rows,
        {(10, 60): place(10)},
        aggregate_entity_id=AGGREGATE_ID,
        aggregate_entity_type=AGGREGATE_TYPE,
        now=Clock([old_retrieval]),
    )
    assert isinstance(prior_outcome, DirectionPayloadOutcome)
    prior_attrs = prior_outcome.payload["attrs"]
    path = tmp_path / "state" / "direction.json"
    window_key = f"per3600/{revised_startdate}"
    write_state(
        path,
        {
            window_key: window_record(
                status="complete",
                targets={
                    AGGREGATE_ID: target_record(
                        status="ok", payload_sha256=attrs_hash(prior_attrs)
                    )
                },
            )
        },
        last_aggregated_at="2026-07-15T12:00:00+09:00",
    )
    state_store = WindowStateStore.load(path, now=Clock([NOW] * 30))
    db_connection = FakeDbConnection(
        {60: []},
        startdate_rows_by_interval={60: rows},
        discovery_rows_by_interval={
            60: [revision_row(revised_startdate, "2026-07-15 12:01:00")]
        },
    )
    orion = FakeOrionClient()

    result = run_once(
        tmp_path=tmp_path,
        db_connection=db_connection,
        orion=orion,
        state_store=state_store,
        run_settings=settings(tmp_path, revision_sweep_enabled=True),
    )

    assert result.exit_code == 0
    assert result.puts_ok == 1
    assert len(orion.replace_calls) == 1
    resent_attrs = orion.replace_calls[0]["attrs"]
    assert attrs_hash(resent_attrs) == attrs_hash(prior_attrs)
    assert resent_attrs["dateObservedFrom"] == prior_attrs["dateObservedFrom"]
    assert resent_attrs["dateRetrieved"] != prior_attrs["dateRetrieved"]


def test_run_direction_failed_revision_put_advances_cursor_forward(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state" / "direction.json"
    write_state(
        path,
        {},
        last_aggregated_at="2026-07-15T12:00:00+09:00",
    )
    state_store = WindowStateStore.load(path, now=Clock([NOW] * 30))
    revised_startdate = "20260701_0900"
    db_connection = FakeDbConnection(
        {60: []},
        startdate_rows_by_interval={60: sendable_window(revised_startdate)},
        discovery_rows_by_interval={
            60: [revision_row(revised_startdate, "2026-07-15 12:01:00")]
        },
    )

    result = run_once(
        tmp_path=tmp_path,
        db_connection=db_connection,
        orion=FakeOrionClient([{"status": 502, "ok": False}]),
        state_store=state_store,
        run_settings=settings(tmp_path, revision_sweep_enabled=True),
    )

    assert result.exit_code == 1
    assert result.puts_failed == 1
    advanced_cursor = state_store.revision_cursor()
    assert advanced_cursor == NOW.replace(microsecond=0)
    assert advanced_cursor is not None
    assert advanced_cursor > datetime.fromisoformat("2026-07-15T12:00:00+09:00")
    assert state_store.window_status(f"per3600/{revised_startdate}") == "partial"


def test_run_direction_revision_no_payload_advances_cursor_without_state_record(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state" / "direction.json"
    write_state(
        path,
        {},
        last_aggregated_at="2026-07-15T12:00:00+09:00",
    )
    state_store = WindowStateStore.load(path, now=Clock([NOW] * 30))
    revised_startdate = "20260701_0900"
    db_connection = FakeDbConnection(
        {60: []},
        startdate_rows_by_interval={
            60: [
                direction_row(
                    startdate=revised_startdate,
                    from_group_place_id="quick.10",
                    to_group_place_id="sendai202603.10",
                )
            ]
        },
        discovery_rows_by_interval={
            60: [revision_row(revised_startdate, "2026-07-15 12:01:00")]
        },
    )
    orion = FakeOrionClient()

    result = run_once(
        tmp_path=tmp_path,
        db_connection=db_connection,
        orion=orion,
        state_store=state_store,
        run_settings=settings(tmp_path, revision_sweep_enabled=True),
    )

    assert result.exit_code == 0
    assert result.windows_no_payload == 1
    assert state_store.revision_cursor() == NOW.replace(microsecond=0)
    assert f"per3600/{revised_startdate}" not in state_store.as_dict()["windows"]
    assert orion.replace_calls == []


def test_run_direction_revision_source_invalid_exits_nonzero_without_state_record(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state" / "direction.json"
    write_state(
        path,
        {},
        last_aggregated_at="2026-07-15T12:00:00+09:00",
    )
    state_store = WindowStateStore.load(path, now=Clock([NOW] * 30))
    revised_startdate = "20260701_0900"
    db_connection = FakeDbConnection(
        {60: []},
        startdate_rows_by_interval={60: [direction_row(startdate=revised_startdate)]},
        discovery_rows_by_interval={
            60: [revision_row(revised_startdate, "2026-07-15 12:01:00")]
        },
    )
    orion = FakeOrionClient()

    result = run_once(
        tmp_path=tmp_path,
        db_connection=db_connection,
        orion=orion,
        state_store=state_store,
        run_settings=settings(tmp_path, revision_sweep_enabled=True),
    )

    assert result.exit_code == 1
    assert result.windows_source_invalid == 1
    assert state_store.revision_cursor() == NOW.replace(microsecond=0)
    assert f"per3600/{revised_startdate}" not in state_store.as_dict()["windows"]
    assert orion.replace_calls == []


def test_run_direction_revision_sweep_disabled_does_not_scan_retry_or_move_cursor(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state" / "direction.json"
    old_cursor = "2026-07-15T12:00:00+09:00"
    old_key = "per3600/20260701_0900"
    write_state(
        path,
        {
            old_key: window_record(
                status="partial",
                targets={AGGREGATE_ID: target_record(status="failed", http_status=502)},
            )
        },
        last_aggregated_at=old_cursor,
    )
    state_store = WindowStateStore.load(path, now=Clock([NOW] * 30))
    db_connection = FakeDbConnection(
        {60: []},
        startdate_rows_by_interval={60: sendable_window("20260701_0900")},
        discovery_rows_by_interval={
            60: [revision_row("20260701_0900", "2026-07-15 12:01:00")]
        },
    )
    orion = FakeOrionClient()

    result = run_once(
        tmp_path=tmp_path,
        db_connection=db_connection,
        orion=orion,
        state_store=state_store,
        run_settings=settings(tmp_path, revision_sweep_enabled=False),
    )

    assert result.exit_code == 1
    assert db_connection.discovery_queries == []
    assert db_connection.startdate_queries == []
    assert orion.replace_calls == []
    assert state_store.revision_cursor() == datetime.fromisoformat(old_cursor)
    assert state_store.window_status(old_key) == "partial"


def test_run_direction_revision_dry_run_previews_put_without_state_mutation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "state" / "direction.json"
    old_cursor = "2026-07-15T12:00:00+09:00"
    write_state(path, {}, last_aggregated_at=old_cursor)
    state_store = WindowStateStore.load(path, now=Clock([NOW] * 30))
    before_memory = deepcopy(state_store.as_dict())
    before_disk = path.read_text(encoding="utf-8")
    forbid_state_mutation(monkeypatch, state_store)
    revised_startdate = "20260701_0900"
    db_connection = FakeDbConnection(
        {60: []},
        startdate_rows_by_interval={60: sendable_window(revised_startdate)},
        discovery_rows_by_interval={
            60: [revision_row(revised_startdate, "2026-07-15 12:01:00")]
        },
    )
    orion = FakeOrionClient()

    result = run_once(
        tmp_path=tmp_path,
        db_connection=db_connection,
        orion=orion,
        state_store=state_store,
        run_settings=settings(
            tmp_path, send_mode="dry-run", revision_sweep_enabled=True
        ),
    )

    assert result.exit_code == 0
    assert len(orion.replace_calls) == 1
    assert orion.replace_calls[0]["dry_run"] is True
    assert state_store.as_dict() == before_memory
    assert path.read_text(encoding="utf-8") == before_disk
    assert state_store.revision_cursor() == datetime.fromisoformat(old_cursor)


def test_run_direction_revision_sweep_retries_open_aggregate_window(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state" / "direction.json"
    old_startdate = "20260701_0900"
    old_key = f"per3600/{old_startdate}"
    write_state(
        path,
        {
            old_key: window_record(
                status="partial",
                targets={AGGREGATE_ID: target_record(status="failed", http_status=502)},
            )
        },
        last_aggregated_at="2026-07-15T12:00:00+09:00",
    )
    state_store = WindowStateStore.load(path, now=Clock([NOW] * 30))
    db_connection = FakeDbConnection(
        {60: []},
        startdate_rows_by_interval={60: sendable_window(old_startdate)},
        discovery_rows_by_interval={60: []},
    )
    orion = FakeOrionClient()

    result = run_once(
        tmp_path=tmp_path,
        db_connection=db_connection,
        orion=orion,
        state_store=state_store,
        run_settings=settings(tmp_path, revision_sweep_enabled=True),
    )

    assert result.exit_code == 0
    assert db_connection.startdate_queries == [(60, (old_startdate,))]
    assert len(orion.replace_calls) == 1
    assert orion.replace_calls[0]["entity_id"] == AGGREGATE_ID
    assert state_store.window_status(old_key) == "complete"
    assert state_store.expected_target_ids(old_key) == [AGGREGATE_ID]


def test_run_direction_gc_preserves_revision_window_processed_in_same_run(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state" / "direction.json"
    revised_startdate = "20260701_0900"
    revised_key = f"per3600/{revised_startdate}"
    unrelated_key = "per3600/20260630_0900"
    write_state(
        path,
        {
            unrelated_key: window_record(
                status="complete",
                targets={AGGREGATE_ID: target_record(status="ok")},
            )
        },
        last_aggregated_at="2026-07-15T12:00:00+09:00",
    )
    state_store = WindowStateStore.load(path, now=Clock([NOW] * 30))
    db_connection = FakeDbConnection(
        {60: []},
        startdate_rows_by_interval={60: sendable_window(revised_startdate)},
        discovery_rows_by_interval={
            60: [revision_row(revised_startdate, "2026-07-15 12:01:00")]
        },
    )

    result = run_once(
        tmp_path=tmp_path,
        db_connection=db_connection,
        orion=FakeOrionClient(),
        state_store=state_store,
        run_settings=settings(tmp_path, revision_sweep_enabled=True),
    )

    assert result.exit_code == 0
    windows = state_store.as_dict()["windows"]
    assert revised_key in windows
    assert windows[revised_key]["status"] == "complete"
    assert unrelated_key not in windows


def test_run_direction_degraded_put_completes_delivery_and_exits_one(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    state_store = store(tmp_path)
    orion = FakeOrionClient()

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline"):
        result = run_once(
            tmp_path=tmp_path,
            db_connection=FakeDbConnection({60: degraded_window()}),
            orion=orion,
            metadata=[place(10), place(11)],
            state_store=state_store,
        )

    assert result.exit_code == 1
    assert result.puts_ok == 1
    assert result.puts_failed == 0
    assert result.windows_complete == 1
    assert result.windows_partial == 0
    assert result.windows_degraded == 1
    assert len(orion.replace_calls) == 1
    assert state_store.window_status("per3600/20260715_0900") == "complete"
    [event] = records(caplog, "direction_window_degraded")
    assert event.levelno == logging.WARNING
    assert getattr(event, "window") == "per3600/20260715_0900"
    assert getattr(event, "excluded_place_numbers") == [11]
    assert getattr(event, "missing_from_all_place_numbers") == []
    assert getattr(event, "missing_to_all_place_numbers") == [11]
    [summary] = records(caplog, "run_summary")
    assert getattr(summary, "windows_degraded") == 1


def test_run_direction_failed_degraded_put_stays_partial_and_exits_one(
    tmp_path: Path,
) -> None:
    state_store = store(tmp_path)

    result = run_once(
        tmp_path=tmp_path,
        db_connection=FakeDbConnection({60: degraded_window()}),
        orion=FakeOrionClient([{"status": 502, "ok": False}]),
        metadata=[place(10), place(11)],
        state_store=state_store,
    )

    assert result.exit_code == 1
    assert result.puts_ok == 0
    assert result.puts_failed == 1
    assert result.windows_complete == 0
    assert result.windows_partial == 1
    assert result.windows_degraded == 1
    assert state_store.window_status("per3600/20260715_0900") == "partial"


def test_run_direction_unchanged_degraded_payload_skips_without_signal(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    state_store = store(tmp_path)
    orion = FakeOrionClient()
    first = run_once(
        tmp_path=tmp_path,
        db_connection=FakeDbConnection({60: degraded_window()}),
        orion=orion,
        metadata=[place(10), place(11)],
        state_store=state_store,
    )
    assert first.windows_degraded == 1
    caplog.clear()

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline"):
        second = run_once(
            tmp_path=tmp_path,
            db_connection=FakeDbConnection({60: degraded_window()}),
            orion=orion,
            metadata=[place(10), place(11)],
            state_store=state_store,
            now=NOW + timedelta(minutes=1),
        )

    assert second.exit_code == 0
    assert second.windows_degraded == 0
    assert second.puts_ok == second.puts_failed == 0
    assert len(orion.replace_calls) == 1
    [event] = records(caplog, "direction_window_degraded_unchanged")
    assert event.levelno == logging.DEBUG
    assert getattr(event, "window") == "per3600/20260715_0900"
    assert records(caplog, "direction_window_degraded") == []
    assert records(caplog, "put_skipped_unchanged") == []


def test_run_direction_degraded_hash_drift_puts_and_signals_again(
    tmp_path: Path,
) -> None:
    state_store = store(tmp_path)
    orion = FakeOrionClient()
    first = run_once(
        tmp_path=tmp_path,
        db_connection=FakeDbConnection({60: degraded_window(boundary_count=3)}),
        orion=orion,
        metadata=[place(10), place(11)],
        state_store=state_store,
    )
    second = run_once(
        tmp_path=tmp_path,
        db_connection=FakeDbConnection({60: degraded_window(boundary_count=4)}),
        orion=orion,
        metadata=[place(10), place(11)],
        state_store=state_store,
        now=NOW + timedelta(minutes=1),
    )

    assert first.exit_code == second.exit_code == 1
    assert first.windows_degraded == second.windows_degraded == 1
    assert first.puts_ok == second.puts_ok == 1
    assert len(orion.replace_calls) == 2


def test_run_direction_forced_revision_of_unchanged_degraded_payload_signals_again(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    state_store = store(tmp_path)
    orion = FakeOrionClient()
    first = run_once(
        tmp_path=tmp_path,
        db_connection=FakeDbConnection({60: degraded_window()}),
        orion=orion,
        metadata=[place(10), place(11)],
        state_store=state_store,
        run_settings=settings(tmp_path, revision_sweep_enabled=True),
    )
    assert first.windows_degraded == 1
    caplog.clear()
    db_connection = FakeDbConnection(
        {60: []},
        startdate_rows_by_interval={60: degraded_window()},
        discovery_rows_by_interval={
            60: [revision_row("20260715_0900", "2026-07-15 12:18:00")]
        },
    )

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline"):
        second = run_once(
            tmp_path=tmp_path,
            db_connection=db_connection,
            orion=orion,
            metadata=[place(10), place(11)],
            state_store=state_store,
            run_settings=settings(tmp_path, revision_sweep_enabled=True),
            now=NOW + timedelta(minutes=1),
        )

    assert second.exit_code == 1
    assert second.windows_degraded == 1
    assert second.puts_ok == 1
    assert len(orion.replace_calls) == 2
    assert db_connection.startdate_queries == [(60, ("20260715_0900",))]
    assert records(caplog, "direction_window_degraded")


def test_run_direction_clean_payload_has_no_degraded_counter(
    tmp_path: Path,
) -> None:
    result = run_once(
        tmp_path=tmp_path,
        db_connection=FakeDbConnection({60: sendable_window()}),
        orion=FakeOrionClient(),
    )

    assert result.exit_code == 0
    assert result.windows_degraded == 0
    assert result.windows_complete == 1


@pytest.mark.parametrize("change", ["add", "change", "remove"])
def test_run_direction_boundary_route_change_is_semantic_drift(
    tmp_path: Path, change: str
) -> None:
    state_store = store(tmp_path)
    orion = FakeOrionClient()
    with_route = degraded_window(boundary_count=3)
    without_route = [
        *sendable_window(place_number=10, from_all=20, to_all=21),
        direction_row(
            from_group_place_id="ALL",
            to_group_place_id="sendai202603.11",
            count=12,
        ),
    ]
    first_rows, second_rows = {
        "add": (without_route, with_route),
        "change": (with_route, degraded_window(boundary_count=4)),
        "remove": (with_route, without_route),
    }[change]

    run_once(
        tmp_path=tmp_path,
        db_connection=FakeDbConnection({60: first_rows}),
        orion=orion,
        metadata=[place(10), place(11)],
        state_store=state_store,
    )
    result = run_once(
        tmp_path=tmp_path,
        db_connection=FakeDbConnection({60: second_rows}),
        orion=orion,
        metadata=[place(10), place(11)],
        state_store=state_store,
        now=NOW + timedelta(minutes=1),
    )

    assert result.puts_ok == 1
    assert result.windows_degraded == 1
    assert len(orion.replace_calls) == 2


def test_run_direction_hash_ignores_quality_evaluation_time_but_keeps_quality() -> None:
    attrs = {
        "dateRetrieved": {"type": "DateTime", "value": "first"},
        "sourceQuality": {
            "type": "StructuredValue",
            "value": {
                "status": "degraded",
                "evaluatedAt": "first",
                "excludedPlaceNumbers": [11],
                "missingFromAllPlaceNumbers": [11],
                "missingToAllPlaceNumbers": [11],
            },
        },
    }
    volatile_changed = deepcopy(attrs)
    volatile_changed["dateRetrieved"]["value"] = "second"
    volatile_changed["sourceQuality"]["value"]["evaluatedAt"] = "second"
    quality_changed = deepcopy(volatile_changed)
    quality_changed["sourceQuality"]["value"]["status"] = "clean"
    quality_changed["sourceQuality"]["value"]["excludedPlaceNumbers"] = []

    assert run_direction_module._attrs_sha256(
        attrs
    ) == run_direction_module._attrs_sha256(volatile_changed)
    assert run_direction_module._attrs_sha256(
        attrs
    ) != run_direction_module._attrs_sha256(quality_changed)


def test_run_direction_dry_run_degraded_preview_is_nonmutating_and_successful(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    state_store = store(tmp_path)
    before = deepcopy(state_store.as_dict())
    forbid_state_mutation(monkeypatch, state_store)
    orion = FakeOrionClient()

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline"):
        result = run_once(
            tmp_path=tmp_path,
            db_connection=FakeDbConnection({60: degraded_window()}),
            orion=orion,
            metadata=[place(10), place(11)],
            state_store=state_store,
            run_settings=settings(tmp_path, send_mode="dry-run"),
        )

    assert result.exit_code == 0
    assert result.windows_degraded == 1
    assert result.windows_complete == 0
    assert state_store.as_dict() == before
    assert len(orion.replace_calls) == 1
    assert orion.replace_calls[0]["dry_run"] is True
    assert records(caplog, "direction_window_degraded")


def test_run_direction_revision_degraded_to_clean_restores_dense_matrix(
    tmp_path: Path,
) -> None:
    revised_startdate = "20260701_0900"
    degraded_rows = degraded_window(revised_startdate)
    clean_rows = [
        *degraded_rows,
        *sendable_window(revised_startdate, place_number=11, from_all=12, to_all=13),
    ]
    prior = transform_direction_window(
        degraded_rows,
        {(10, 60): place(10), (11, 60): place(11)},
        aggregate_entity_id=AGGREGATE_ID,
        aggregate_entity_type=AGGREGATE_TYPE,
        now=Clock([NOW - timedelta(days=1)]),
    )
    assert isinstance(prior, DirectionPayloadOutcome)
    path = tmp_path / "state" / "direction.json"
    key = f"per3600/{revised_startdate}"
    write_state(
        path,
        {
            key: window_record(
                status="complete",
                targets={
                    AGGREGATE_ID: target_record(
                        status="ok", payload_sha256=attrs_hash(prior.payload["attrs"])
                    )
                },
            )
        },
        last_aggregated_at="2026-07-15T12:00:00+09:00",
    )
    state_store = WindowStateStore.load(path, now=Clock([NOW] * 30))
    db_connection = FakeDbConnection(
        {60: []},
        startdate_rows_by_interval={60: clean_rows},
        discovery_rows_by_interval={
            60: [revision_row(revised_startdate, "2026-07-15 12:01:00")]
        },
    )
    orion = FakeOrionClient()

    result = run_once(
        tmp_path=tmp_path,
        db_connection=db_connection,
        orion=orion,
        metadata=[place(10), place(11)],
        state_store=state_store,
        run_settings=settings(tmp_path, revision_sweep_enabled=True),
    )

    assert len(orion.replace_calls) == 1
    attrs = orion.replace_calls[0]["attrs"]
    assert db_connection.startdate_queries == [(60, (revised_startdate,))]
    assert result.windows_degraded == 0
    assert result.windows_complete == 1
    assert attrs["sourceQuality"]["value"]["status"] == "clean"
    assert attrs["peopleCount_flow_10"]["value"]["to"]["11"] == 3
    assert attrs["peopleCount_flow_11"]["value"]["from"]["10"] == 3
    assert set(attrs["peopleCount_flow_11"]["value"]["to"]) == {"10", "11", "all"}
    saved = state_store.target_record(key, AGGREGATE_ID)
    assert saved is not None
    assert saved["last_payload_sha256"] == attrs_hash(attrs)


def test_run_direction_revision_clean_to_degraded_keeps_sparse_boundary(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    revised_startdate = "20260701_0900"
    degraded_rows = degraded_window(revised_startdate)
    clean_rows = [
        *degraded_rows,
        *sendable_window(revised_startdate, place_number=11, from_all=12, to_all=13),
    ]
    prior = transform_direction_window(
        clean_rows,
        {(10, 60): place(10), (11, 60): place(11)},
        aggregate_entity_id=AGGREGATE_ID,
        aggregate_entity_type=AGGREGATE_TYPE,
        now=Clock([NOW - timedelta(days=1)]),
    )
    assert isinstance(prior, DirectionPayloadOutcome)
    path = tmp_path / "state" / "direction.json"
    key = f"per3600/{revised_startdate}"
    write_state(
        path,
        {
            key: window_record(
                status="complete",
                targets={
                    AGGREGATE_ID: target_record(
                        status="ok", payload_sha256=attrs_hash(prior.payload["attrs"])
                    )
                },
            )
        },
        last_aggregated_at="2026-07-15T12:00:00+09:00",
    )
    state_store = WindowStateStore.load(path, now=Clock([NOW] * 30))
    db_connection = FakeDbConnection(
        {60: []},
        startdate_rows_by_interval={60: degraded_rows},
        discovery_rows_by_interval={
            60: [revision_row(revised_startdate, "2026-07-15 12:01:00")]
        },
    )
    orion = FakeOrionClient()

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline"):
        result = run_once(
            tmp_path=tmp_path,
            db_connection=db_connection,
            orion=orion,
            metadata=[place(10), place(11)],
            state_store=state_store,
            run_settings=settings(tmp_path, revision_sweep_enabled=True),
        )

    assert len(orion.replace_calls) == 1
    attrs = orion.replace_calls[0]["attrs"]
    assert result.exit_code == 1
    assert result.windows_degraded == 1
    assert result.windows_complete == 1
    assert "peopleCount_flow_11" not in attrs
    assert attrs["peopleCount_flow_10"]["value"]["to"]["11"] == 3
    assert state_store.window_status(key) == "complete"
    assert records(caplog, "direction_window_degraded")


def test_run_direction_logging_allowlist_carries_aggregate_contract_fields() -> None:
    assert {
        "puts_ok",
        "puts_failed",
        "windows_no_payload",
        "windows_source_invalid",
        "windows_degraded",
        "excluded_place_numbers",
        "missing_from_all_place_numbers",
        "missing_to_all_place_numbers",
    } <= _ALLOWED_EXTRA_KEYS
