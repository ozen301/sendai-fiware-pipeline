# ruff: noqa: E501
import importlib
import os
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any

import pytest
import requests

ENTITY_10 = "jp.sendai.Blesensor.per3600.10"
ENTITY_11 = "jp.sendai.Blesensor.per3600.11"
TYPE_3600 = "Blesensor.per3600"
REASON = "operator requested entity deletion"
FLOW_ATTRS = [
    "dateObservedFrom",
    "dateObservedTo",
    "peopleCount_immedate",
    "peopleCount_near",
    "peopleCount_far",
    "peopleOccupancy_immedate",
    "peopleOccupancy_near",
]
DIRECTION_ATTRS = [
    "dateObservedFrom",
    "dateObservedTo",
    "peopleCount_flow",
]


class FakeAuth:
    def __init__(self, runtime: "RuntimePatch") -> None:
        self._runtime = runtime

    def get_token(self, *, force_refresh: bool = False) -> str:
        self._runtime.token_calls.append(force_refresh)
        return "token"


class FakeOrionDelete:
    def __init__(self, outcomes: list[Any] | None = None) -> None:
        self.outcomes = list(outcomes or [])
        self.calls: list[dict[str, Any]] = []

    def __call__(
        self,
        entity_id: str,
        entity_type: str,
        *_args: Any,
        **_kwargs: Any,
    ) -> int:
        self.calls.append({"entity_id": entity_id, "entity_type": entity_type})
        return self._next_status()

    def _next_status(self) -> int:
        if not self.outcomes:
            return 204
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return int(outcome)


class FakeCometClient:
    def __init__(self, outcomes: list[Any] | None = None) -> None:
        self.outcomes = list(outcomes or [])
        self.calls: list[dict[str, Any]] = []

    def delete_attribute_history(
        self,
        entity_id: str,
        entity_type: str,
        attr: str,
    ) -> int:
        self.calls.append(
            {
                "method": "attribute",
                "entity_id": entity_id,
                "entity_type": entity_type,
                "attr": attr,
            }
        )
        return self._next_status()

    def delete_entity_history(self, entity_id: str, entity_type: str) -> int:
        self.calls.append(
            {
                "method": "entity",
                "entity_id": entity_id,
                "entity_type": entity_type,
            }
        )
        return self._next_status()

    def _next_status(self) -> int:
        if not self.outcomes:
            return 204
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return int(outcome)


@dataclass
class RuntimePatch:
    orion: FakeOrionDelete = field(default_factory=FakeOrionDelete)
    comet: FakeCometClient = field(default_factory=FakeCometClient)
    auth_inits: int = 0
    comet_inits: int = 0
    token_calls: list[bool] = field(default_factory=list)

    def build_auth(self, *_args: Any, **_kwargs: Any) -> FakeAuth:
        self.auth_inits += 1
        return FakeAuth(self)

    def build_comet(self, *_args: Any, **_kwargs: Any) -> FakeCometClient:
        self.comet_inits += 1
        return self.comet


@pytest.fixture
def runtime(monkeypatch: pytest.MonkeyPatch) -> RuntimePatch:
    monkeypatch.setenv("FIWARE_BASE_URL", "https://fiware.example.test")
    monkeypatch.setenv("FIWARE_SERVICE", "sendai")
    monkeypatch.setenv("FIWARE_SERVICE_PATH", "/city")
    return RuntimePatch()


def test_delete_entities_requires_reason(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    result = _invoke([_entity_spec(ENTITY_10)], capsys, runtime)

    assert result != 0
    assert runtime.orion.calls == []
    assert runtime.comet.calls == []


def test_delete_entities_requires_at_least_one_entity_spec(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    result = _invoke(["--reason", REASON], capsys, runtime)

    assert result != 0
    assert runtime.orion.calls == []
    assert runtime.comet.calls == []


def test_delete_entities_rejects_attrs_without_purge_history(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    result = _invoke(
        _base_args(ENTITY_10) + ["--attrs", "dateObservedFrom"],
        capsys,
        runtime,
    )

    assert result != 0
    assert runtime.orion.calls == []
    assert runtime.comet.calls == []


def test_delete_entities_rejects_flow_attrs_without_purge_history(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    result = _invoke(_base_args(ENTITY_10) + ["--flow-attrs"], capsys, runtime)

    assert result != 0
    assert runtime.orion.calls == []
    assert runtime.comet.calls == []


def test_delete_entities_rejects_direction_attrs_without_purge_history(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    result = _invoke(_base_args(ENTITY_10) + ["--direction-attrs"], capsys, runtime)

    assert result != 0
    assert runtime.orion.calls == []
    assert runtime.comet.calls == []


def test_delete_entities_rejects_attrs_and_flow_attrs_together(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    result = _invoke(
        _base_args(ENTITY_10, purge_history=True)
        + ["--attrs", "dateObservedFrom", "--flow-attrs"],
        capsys,
        runtime,
    )

    assert result != 0
    assert runtime.orion.calls == []
    assert runtime.comet.calls == []


def test_delete_entities_rejects_attrs_and_direction_attrs_together(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    result = _invoke(
        _base_args(ENTITY_10, purge_history=True)
        + ["--attrs", "dateObservedFrom", "--direction-attrs"],
        capsys,
        runtime,
    )

    assert result != 0
    assert runtime.orion.calls == []
    assert runtime.comet.calls == []


def test_delete_entities_rejects_flow_attrs_and_direction_attrs_together(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    result = _invoke(
        _base_args(ENTITY_10, purge_history=True)
        + ["--flow-attrs", "--direction-attrs"],
        capsys,
        runtime,
    )

    assert result != 0
    assert runtime.orion.calls == []
    assert runtime.comet.calls == []


def test_delete_entities_rejects_empty_entity_id(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    result = _invoke(_base_args(""), capsys, runtime)

    assert result != 0
    assert runtime.orion.calls == []
    assert runtime.comet.calls == []


def test_delete_entities_rejects_wildcard_entity_id(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    result = _invoke(_base_args("*"), capsys, runtime)

    assert result != 0
    assert runtime.orion.calls == []
    assert runtime.comet.calls == []


def test_delete_entities_rejects_non_canonical_entity_spec_without_type(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    result = _invoke(["--reason", REASON, "custom-entity"], capsys, runtime)

    assert result != 0
    assert "--type" not in capsys.readouterr().err
    assert runtime.orion.calls == []
    assert runtime.comet.calls == []


def test_delete_entities_rejects_send_in_production_service_without_override(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    runtime: RuntimePatch,
) -> None:
    monkeypatch.setenv("FIWARE_SERVICE", "")
    monkeypatch.setenv("FIWARE_SERVICE_PATH", "/city")

    result = _invoke(_base_args(ENTITY_10, send=True), capsys, runtime)

    assert result != 0
    assert runtime.orion.calls == []
    assert runtime.comet.calls == []


def test_delete_entities_rejects_send_in_root_service_path_without_override(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    runtime: RuntimePatch,
) -> None:
    monkeypatch.setenv("FIWARE_SERVICE", "sendai")
    monkeypatch.setenv("FIWARE_SERVICE_PATH", "/")

    result = _invoke(_base_args(ENTITY_10, send=True), capsys, runtime)

    assert result != 0
    assert runtime.orion.calls == []
    assert runtime.comet.calls == []


def test_delete_entities_rejects_send_when_service_path_env_blank(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    runtime: RuntimePatch,
) -> None:
    monkeypatch.setenv("FIWARE_SERVICE", "sendai")
    monkeypatch.setenv("FIWARE_SERVICE_PATH", "")

    result = _invoke(_base_args(ENTITY_10, send=True), capsys, runtime)

    assert result != 0
    assert runtime.orion.calls == []
    assert runtime.comet.calls == []


def test_delete_entities_rejects_send_when_service_env_whitespace(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    runtime: RuntimePatch,
) -> None:
    monkeypatch.setenv("FIWARE_SERVICE", "   ")
    monkeypatch.setenv("FIWARE_SERVICE_PATH", "/city")

    result = _invoke(_base_args(ENTITY_10, send=True), capsys, runtime)

    assert result != 0
    assert runtime.orion.calls == []
    assert runtime.comet.calls == []


def test_delete_entities_emits_requested_target_and_summary_log_events(
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
    runtime: RuntimePatch,
) -> None:
    runtime.orion.outcomes = [204]
    caplog.set_level("INFO", logger="scripts.delete_entities")

    result = _invoke(
        _base_args(
            ENTITY_10,
            send=True,
            production_override=True,
        ),
        capsys,
        runtime,
    )

    events = [record.__dict__.get("event") for record in caplog.records]
    assert result == 0
    assert "delete_entities_requested" in events
    assert "delete_entities_target" in events
    assert "delete_entities_summary" in events


def test_delete_entities_dry_run_emits_per_target_log_events_with_purge(
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
    runtime: RuntimePatch,
) -> None:
    caplog.set_level("INFO", logger="scripts.delete_entities")

    # Dry-run with --purge-history => one Orion target + one Comet target per entity.
    result = _invoke(
        _base_args(ENTITY_10, ENTITY_11) + ["--purge-history"],
        capsys,
        runtime,
    )

    assert result == 0
    assert runtime.orion.calls == []
    assert runtime.comet.calls == []
    target_events = [
        record.__dict__
        for record in caplog.records
        if record.__dict__.get("event") == "delete_entities_target"
    ]
    # 2 entities × (orion + comet_purge) = 4 events on dry-run.
    assert len(target_events) == 4
    phases = sorted(record["phase"] for record in target_events)
    assert phases == ["comet_purge", "comet_purge", "orion", "orion"]


def test_delete_entities_allows_send_in_production_with_override(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    runtime: RuntimePatch,
) -> None:
    monkeypatch.setenv("FIWARE_SERVICE", "")
    monkeypatch.setenv("FIWARE_SERVICE_PATH", "/")

    result = _invoke(
        _base_args(ENTITY_10, send=True, production_override=True),
        capsys,
        runtime,
    )

    assert result == 0
    assert runtime.orion.calls == [{"entity_id": ENTITY_10, "entity_type": TYPE_3600}]


def test_delete_entities_dry_run_allowed_in_production_without_override(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    runtime: RuntimePatch,
) -> None:
    monkeypatch.setenv("FIWARE_SERVICE", "")
    monkeypatch.setenv("FIWARE_SERVICE_PATH", "/")

    result = _invoke(_base_args(ENTITY_10), capsys, runtime)

    out = capsys.readouterr().out
    assert result == 0
    assert f"/orion/v2.0/entities/{ENTITY_10}?type={TYPE_3600}" in out
    assert runtime.orion.calls == []
    assert runtime.comet.calls == []


def test_delete_entities_send_with_non_production_service_does_not_require_override(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    runtime: RuntimePatch,
) -> None:
    monkeypatch.setenv("FIWARE_SERVICE", "sendai")
    monkeypatch.setenv("FIWARE_SERVICE_PATH", "/city")

    result = _invoke(_base_args(ENTITY_10, send=True), capsys, runtime)

    assert result == 0
    assert runtime.orion.calls == [{"entity_id": ENTITY_10, "entity_type": TYPE_3600}]


def test_delete_entities_dry_run_makes_no_auth_calls(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    result = _invoke(_base_args(ENTITY_10, purge_history=True), capsys, runtime)

    assert result == 0
    assert runtime.auth_inits == 0
    assert runtime.token_calls == []


def test_delete_entities_dry_run_makes_no_orion_calls(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    result = _invoke(_base_args(ENTITY_10), capsys, runtime)

    out = capsys.readouterr().out
    assert result == 0
    assert f"/orion/v2.0/entities/{ENTITY_10}?type={TYPE_3600}" in out
    assert runtime.orion.calls == []


def test_delete_entities_dry_run_makes_no_comet_calls(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    result = _invoke(_base_args(ENTITY_10, purge_history=True), capsys, runtime)

    out = capsys.readouterr().out
    assert result == 0
    assert f"contextEntities/type/{TYPE_3600}/id/{ENTITY_10}" in out
    assert runtime.comet.calls == []


def test_delete_entities_dry_run_does_not_require_fiware_credentials(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    runtime: RuntimePatch,
) -> None:
    for key in list(os.environ):
        if key.startswith("FIWARE_"):
            monkeypatch.delenv(key, raising=False)

    result = _invoke(_base_args(ENTITY_10, purge_history=True), capsys, runtime)

    assert result == 0
    assert runtime.auth_inits == 0
    assert runtime.orion.calls == []
    assert runtime.comet.calls == []


def test_delete_entities_dry_run_with_purge_history_prints_comet_plan_too(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    result = _invoke(_base_args(ENTITY_10, purge_history=True), capsys, runtime)

    out = capsys.readouterr().out
    assert result == 0
    assert f"/orion/v2.0/entities/{ENTITY_10}?type={TYPE_3600}" in out
    assert f"/comet/v1.0/contextEntities/type/{TYPE_3600}/id/{ENTITY_10}" in out
    assert runtime.orion.calls == []
    assert runtime.comet.calls == []


def test_delete_entities_calls_orion_delete_once_per_entity(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    result = _invoke(_base_args(ENTITY_10, ENTITY_11, send=True), capsys, runtime)

    assert result == 0
    assert runtime.orion.calls == [
        {"entity_id": ENTITY_10, "entity_type": TYPE_3600},
        {"entity_id": ENTITY_11, "entity_type": TYPE_3600},
    ]


def test_delete_entities_bare_canonical_id_infers_entity_type(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    result = _invoke(["--reason", REASON, "--send", ENTITY_10], capsys, runtime)

    assert result == 0
    assert runtime.orion.calls == [{"entity_id": ENTITY_10, "entity_type": TYPE_3600}]


def test_delete_entities_inline_entity_type_overrides_inferred_type(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    result = _invoke(
        ["--reason", REASON, "--send", f"{ENTITY_10}:Blesensor.per300"],
        capsys,
        runtime,
    )

    assert result == 0
    assert runtime.orion.calls == [
        {"entity_id": ENTITY_10, "entity_type": "Blesensor.per300"}
    ]


def test_delete_entities_orion_404_treated_as_noop_exit_zero(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    runtime.orion.outcomes = [404]

    result = _invoke(_base_args(ENTITY_10, send=True), capsys, runtime)

    assert result == 0
    assert runtime.orion.calls == [{"entity_id": ENTITY_10, "entity_type": TYPE_3600}]


def test_delete_entities_orion_500_counted_as_failure_exit_nonzero_continues(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    runtime.orion.outcomes = [500, 204]

    result = _invoke(_base_args(ENTITY_10, ENTITY_11, send=True), capsys, runtime)

    assert result != 0
    assert runtime.orion.calls == [
        {"entity_id": ENTITY_10, "entity_type": TYPE_3600},
        {"entity_id": ENTITY_11, "entity_type": TYPE_3600},
    ]


def test_delete_entities_does_not_call_comet_without_purge_history_flag(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    result = _invoke(_base_args(ENTITY_10, send=True), capsys, runtime)

    assert result == 0
    assert len(runtime.orion.calls) == 1
    assert runtime.comet.calls == []


def test_delete_entities_purge_history_per_entity_when_no_attrs_flag(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    result = _invoke(
        _base_args(ENTITY_10, ENTITY_11, send=True, purge_history=True),
        capsys,
        runtime,
    )

    assert result == 0
    assert runtime.comet.calls == [
        {"method": "entity", "entity_id": ENTITY_10, "entity_type": TYPE_3600},
        {"method": "entity", "entity_id": ENTITY_11, "entity_type": TYPE_3600},
    ]


def test_delete_entities_purge_history_per_attribute_when_attrs_flag(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    result = _invoke(
        _base_args(ENTITY_10, send=True, purge_history=True) + ["--attrs", "a,b,c"],
        capsys,
        runtime,
    )

    assert result == 0
    assert runtime.comet.calls == [
        {
            "method": "attribute",
            "entity_id": ENTITY_10,
            "entity_type": TYPE_3600,
            "attr": attr,
        }
        for attr in ["a", "b", "c"]
    ]


def test_delete_entities_purge_history_per_attribute_with_flow_attrs_flag(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    result = _invoke(
        _base_args(ENTITY_10, send=True, purge_history=True) + ["--flow-attrs"],
        capsys,
        runtime,
    )

    assert result == 0
    assert [call["attr"] for call in runtime.comet.calls] == FLOW_ATTRS


def test_delete_entities_purge_history_per_attribute_with_direction_attrs_flag(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    result = _invoke(
        _base_args(ENTITY_10, send=True, purge_history=True) + ["--direction-attrs"],
        capsys,
        runtime,
    )

    assert result == 0
    assert [call["attr"] for call in runtime.comet.calls] == DIRECTION_ATTRS


def test_delete_entities_purge_history_runs_after_orion_204(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    runtime.orion.outcomes = [204]

    result = _invoke(
        _base_args(ENTITY_10, send=True, purge_history=True),
        capsys,
        runtime,
    )

    assert result == 0
    assert runtime.comet.calls == [
        {"method": "entity", "entity_id": ENTITY_10, "entity_type": TYPE_3600}
    ]


def test_delete_entities_purge_history_runs_after_orion_404(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    runtime.orion.outcomes = [404]

    result = _invoke(
        _base_args(ENTITY_10, send=True, purge_history=True),
        capsys,
        runtime,
    )

    assert result == 0
    assert runtime.comet.calls == [
        {"method": "entity", "entity_id": ENTITY_10, "entity_type": TYPE_3600}
    ]


def test_delete_entities_purge_history_skipped_after_orion_500(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    runtime.orion.outcomes = [500]

    result = _invoke(
        _base_args(ENTITY_10, send=True, purge_history=True),
        capsys,
        runtime,
    )

    assert result != 0
    assert runtime.orion.calls == [{"entity_id": ENTITY_10, "entity_type": TYPE_3600}]
    assert runtime.comet.calls == []


def test_delete_entities_purge_history_comet_failure_does_not_change_orion_success_count(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    runtime.orion.outcomes = [204]
    runtime.comet.outcomes = [_http_error(500, "comet failed")]

    result = _invoke(
        _base_args(ENTITY_10, send=True, purge_history=True),
        capsys,
        runtime,
    )

    assert result == 0
    assert runtime.orion.calls == [{"entity_id": ENTITY_10, "entity_type": TYPE_3600}]
    assert runtime.comet.calls == [
        {"method": "entity", "entity_id": ENTITY_10, "entity_type": TYPE_3600}
    ]


def test_delete_entities_purge_history_orion_failure_for_one_entity_does_not_skip_next_entity_purge(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    runtime.orion.outcomes = [500, 204]

    result = _invoke(
        _base_args(ENTITY_10, ENTITY_11, send=True, purge_history=True),
        capsys,
        runtime,
    )

    assert result != 0
    assert runtime.orion.calls == [
        {"entity_id": ENTITY_10, "entity_type": TYPE_3600},
        {"entity_id": ENTITY_11, "entity_type": TYPE_3600},
    ]
    assert runtime.comet.calls == [
        {"method": "entity", "entity_id": ENTITY_11, "entity_type": TYPE_3600}
    ]


def _invoke(
    argv: list[str],
    _capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> int:
    delete_entities = _delete_entities_module()
    _patch_delete_entities_module(delete_entities, runtime)
    try:
        result = delete_entities.main(argv)
    except SystemExit as exc:
        result = exc.code
    if result is None:
        return 0
    if isinstance(result, int):
        return result
    return int(result)


def _delete_entities_module() -> Any:
    return importlib.import_module("scripts.delete_entities")


# NOTE: These tests expect scripts.delete_entities to import network-facing
# clients/settings into module-level names. Orion DELETE is patched through a
# module-level delete_one_orion_entity(entity_id, entity_type, *, ...) helper
# returning an HTTP status code, which lets the CLI orchestration tests assert
# continuation and Comet purge ordering without performing live HTTP.
def _patch_delete_entities_module(delete_entities: Any, runtime: RuntimePatch) -> None:
    delete_entities.AuthClient = runtime.build_auth
    delete_entities.AuthSettings = type(
        "FakeAuthSettings", (), {"from_env": staticmethod(lambda: object())}
    )
    delete_entities.CometSettings = type(
        "FakeCometSettings", (), {"from_env": staticmethod(_fiware_settings_from_env)}
    )
    delete_entities.CometClient = runtime.build_comet
    delete_entities.OrionSettings = type(
        "FakeOrionSettings", (), {"from_env": staticmethod(_fiware_settings_from_env)}
    )
    delete_entities.delete_one_orion_entity = runtime.orion
    delete_entities.load_dotenv = lambda *_args, **_kwargs: None
    delete_entities.find_dotenv = lambda *_args, **_kwargs: ""


def _fiware_settings_from_env() -> SimpleNamespace:
    return SimpleNamespace(
        base_url=os.environ.get("FIWARE_BASE_URL", "https://fiware.example.test"),
        service=os.environ.get("FIWARE_SERVICE", ""),
        service_path=os.environ.get("FIWARE_SERVICE_PATH", "/"),
        verify_tls=True,
        timeout=10,
    )


def _base_args(
    *entity_ids: str,
    send: bool = False,
    purge_history: bool = False,
    production_override: bool = False,
) -> list[str]:
    args = ["--reason", REASON]
    if purge_history:
        args.append("--purge-history")
    if send:
        args.append("--send")
    if production_override:
        args.append("--i-know-this-is-production")
    args.extend(_entity_spec(entity_id) for entity_id in entity_ids)
    return args


def _entity_spec(entity_id: str, entity_type: str = TYPE_3600) -> str:
    return f"{entity_id}:{entity_type}"


def _http_error(status_code: int, text: str) -> requests.HTTPError:
    response = SimpleNamespace(status_code=status_code, text=text)
    error = requests.HTTPError(text)
    error.response = response
    return error
