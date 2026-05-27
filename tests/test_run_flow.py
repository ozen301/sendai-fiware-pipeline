import fcntl
import hashlib
import json
import logging
from collections.abc import Callable, Iterable, Mapping
from copy import deepcopy
from dataclasses import FrozenInstanceError
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

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
    ) -> None:
        self.rows_by_interval = {
            interval: [dict(row) for row in rows]
            for interval, rows in (rows_by_interval or {}).items()
        }
        self.queries: list[tuple[int, str, str, int]] = []
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
    body = json.dumps(attrs, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(body).hexdigest()


def transformed_hash(row: Mapping[str, Any], places: Iterable[SensorPlace]) -> str:
    payloads = transform_flow_rows(
        [row],
        index_by_place_interval(active_places(places, target_batches=["2026"])),
    )
    assert len(payloads) == 1
    return payload_hash(payloads[0]["attrs"])


def write_state(path: Path, windows: Mapping[str, Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"windows": windows}, sort_keys=True, separators=(",", ":")) + "\n",
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
            force_resend=True,
        )

    assert len(orion.calls) == 1
    assert orion.calls[0]["entity_id"] == target.entity_id
    # No skip event should have fired.
    assert records(caplog, "post_skipped_unchanged") == []
    assert records(caplog, "post_skipped_drift") == []


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


def test_run_flow_skips_drifted_ok_target_and_keeps_original_hash(
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
    assert orion.calls == []
    record = store.target_record("per3600/20260523_0900", target.entity_id)
    assert record is not None
    assert record["status"] == "ok"
    assert record["last_payload_sha256"] == old_hash
    drift = records(caplog, "post_skipped_drift")
    assert len(drift) == 1
    assert drift[0].entity_id == target.entity_id
    assert drift[0].window == "per3600/20260523_0900"
    assert drift[0].prior_payload_sha256 == old_hash
    assert drift[0].computed_payload_sha256 == new_hash


def test_run_flow_uses_stored_expected_targets_when_metadata_changed(
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
    changed = records(caplog, "window_expected_targets_changed")
    assert len(changed) == 1
    assert changed[0].window == "per3600/20260523_0900"
    assert changed[0].count_expected == 2
    assert changed[0].count_live == 1


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


def test_run_flow_completes_partial_window_without_reposting_drifted_ok_target(
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
    assert [call["entity_id"] for call in orion.calls] == [failed_target.entity_id]
    ok_record = store.target_record("per3600/20260523_0900", ok_target.entity_id)
    failed_record = store.target_record(
        "per3600/20260523_0900", failed_target.entity_id
    )
    assert ok_record is not None
    assert failed_record is not None
    assert ok_record["last_payload_sha256"] == ok_old_hash
    assert failed_record["status"] == "ok"
    assert len(records(caplog, "post_skipped_drift")) == 1


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
    assert [record.window for record in records(caplog, "window_complete")] == [
        "per3600/20260523_0800"
    ]
    partials = records(caplog, "window_partial")
    assert len(partials) == 1
    assert partials[0].levelname == "WARNING"
    assert partials[0].window == "per3600/20260523_0900"
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


def test_main_flock_contention_returns_zero_silently(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
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
    assert caplog.records == []


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
