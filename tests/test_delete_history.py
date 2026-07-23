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
REASON = "operator requested comet history purge"
FLOW_ATTRS = [
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


class FakeAuth:
    def __init__(self, runtime: "RuntimePatch") -> None:
        self._runtime = runtime

    def get_token(self, *, force_refresh: bool = False) -> str:
        self._runtime.token_calls.append(force_refresh)
        return "token"


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


def test_delete_history_requires_reason(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    result = _invoke(["--type", TYPE_3600, ENTITY_10], capsys, runtime)

    assert result != 0
    assert runtime.comet.calls == []


def test_delete_history_rejects_attrs_and_flow_attrs_together(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    result = _invoke(
        _base_args(ENTITY_10) + ["--attrs", "dateObservedFrom", "--flow-attrs"],
        capsys,
        runtime,
    )

    assert result != 0
    assert runtime.comet.calls == []


def test_delete_history_rejects_removed_direction_attrs_flag(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    result = _invoke(_base_args(ENTITY_10) + ["--direction-attrs"], capsys, runtime)

    assert result == 2
    assert "unrecognized arguments: --direction-attrs" in capsys.readouterr().err
    assert runtime.comet.calls == []


def test_delete_history_rejects_empty_entity_id(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    result = _invoke(_base_args(""), capsys, runtime)

    assert result != 0
    assert runtime.comet.calls == []


def test_delete_history_rejects_wildcard_entity_id(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    result = _invoke(_base_args("*"), capsys, runtime)

    assert result != 0
    assert runtime.comet.calls == []


def test_delete_history_requires_at_least_one_entity_spec(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    result = _invoke(["--reason", REASON], capsys, runtime)

    assert result != 0
    assert runtime.comet.calls == []


def test_delete_history_rejects_send_in_production_service_without_override(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    runtime: RuntimePatch,
) -> None:
    monkeypatch.setenv("FIWARE_SERVICE", "")
    monkeypatch.setenv("FIWARE_SERVICE_PATH", "/")

    result = _invoke(_base_args(ENTITY_10, send=True), capsys, runtime)

    assert result != 0
    assert runtime.comet.calls == []


def test_delete_history_rejects_send_in_root_service_path_without_override(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    runtime: RuntimePatch,
) -> None:
    monkeypatch.setenv("FIWARE_SERVICE", "sendai")
    monkeypatch.setenv("FIWARE_SERVICE_PATH", "/")

    result = _invoke(_base_args(ENTITY_10, send=True), capsys, runtime)

    assert result != 0
    assert runtime.comet.calls == []


def test_delete_history_rejects_send_when_service_path_env_blank(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    runtime: RuntimePatch,
) -> None:
    monkeypatch.setenv("FIWARE_SERVICE", "sendai")
    monkeypatch.setenv("FIWARE_SERVICE_PATH", "")

    result = _invoke(_base_args(ENTITY_10, send=True), capsys, runtime)

    assert result != 0
    assert runtime.comet.calls == []


def test_delete_history_rejects_send_when_service_env_whitespace(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    runtime: RuntimePatch,
) -> None:
    monkeypatch.setenv("FIWARE_SERVICE", "   ")
    monkeypatch.setenv("FIWARE_SERVICE_PATH", "/city")

    result = _invoke(_base_args(ENTITY_10, send=True), capsys, runtime)

    assert result != 0
    assert runtime.comet.calls == []


def test_delete_history_emits_requested_target_and_summary_log_events(
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
    runtime: RuntimePatch,
) -> None:
    runtime.comet.outcomes = [204]
    caplog.set_level("INFO", logger="scripts.delete_history")

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
    assert "delete_history_requested" in events
    assert "delete_history_target" in events
    assert "delete_history_summary" in events


def test_delete_history_dry_run_emits_per_target_log_events(
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
    runtime: RuntimePatch,
) -> None:
    caplog.set_level("INFO", logger="scripts.delete_history")

    # Two entities, no --attrs => one entity-scope target each.
    result = _invoke(_base_args(ENTITY_10, ENTITY_11), capsys, runtime)

    assert result == 0
    # No live HTTP must have happened on dry-run.
    assert runtime.comet.calls == []
    events = [record.__dict__.get("event") for record in caplog.records]
    target_events = [e for e in events if e == "delete_history_target"]
    assert len(target_events) == 2
    assert "delete_history_requested" in events
    assert "delete_history_summary" in events


def test_delete_history_allows_send_in_production_with_override(
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
    assert runtime.comet.calls == [
        {"method": "entity", "entity_id": ENTITY_10, "entity_type": TYPE_3600}
    ]


def test_delete_history_dry_run_allowed_in_production_without_override(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    runtime: RuntimePatch,
) -> None:
    monkeypatch.setenv("FIWARE_SERVICE", "")
    monkeypatch.setenv("FIWARE_SERVICE_PATH", "/")

    result = _invoke(_base_args(ENTITY_10), capsys, runtime)

    out = capsys.readouterr().out
    assert result == 0
    assert "/comet/v1.0/contextEntities/type/" in out
    assert runtime.comet.calls == []


def test_delete_history_send_with_non_production_service_does_not_require_override(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    runtime: RuntimePatch,
) -> None:
    monkeypatch.setenv("FIWARE_SERVICE", "sendai")
    monkeypatch.setenv("FIWARE_SERVICE_PATH", "/city")

    result = _invoke(_base_args(ENTITY_10, send=True), capsys, runtime)

    assert result == 0
    assert runtime.comet.calls == [
        {"method": "entity", "entity_id": ENTITY_10, "entity_type": TYPE_3600}
    ]


def test_delete_history_dry_run_makes_no_auth_calls(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    result = _invoke(_base_args(ENTITY_10), capsys, runtime)

    assert result == 0
    assert runtime.auth_inits == 0
    assert runtime.token_calls == []


def test_delete_history_dry_run_makes_no_comet_calls(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    result = _invoke(_base_args(ENTITY_10), capsys, runtime)

    out = capsys.readouterr().out
    assert result == 0
    assert f"contextEntities/type/{TYPE_3600}/id/{ENTITY_10}" in out
    assert runtime.comet.calls == []


def test_delete_history_dry_run_does_not_require_fiware_credentials(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    runtime: RuntimePatch,
) -> None:
    for key in list(os.environ):
        if key.startswith("FIWARE_"):
            monkeypatch.delenv(key, raising=False)

    result = _invoke(_base_args(ENTITY_10), capsys, runtime)

    assert result == 0
    assert runtime.auth_inits == 0
    assert runtime.comet.calls == []


def test_delete_history_per_entity_calls_delete_entity_history_once_per_entity(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    result = _invoke(_base_args(ENTITY_10, ENTITY_11, send=True), capsys, runtime)

    assert result == 0
    assert runtime.comet.calls == [
        {"method": "entity", "entity_id": ENTITY_10, "entity_type": TYPE_3600},
        {"method": "entity", "entity_id": ENTITY_11, "entity_type": TYPE_3600},
    ]


def test_delete_history_per_entity_uses_inline_entity_type_from_spec(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    result = _invoke(
        _base_args(f"{ENTITY_10}:Blesensor.per300", send=True),
        capsys,
        runtime,
    )

    assert result == 0
    assert runtime.comet.calls == [
        {"method": "entity", "entity_id": ENTITY_10, "entity_type": "Blesensor.per300"}
    ]


def test_delete_history_bare_canonical_id_infers_entity_type(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    result = _invoke(["--reason", REASON, "--send", ENTITY_10], capsys, runtime)

    assert result == 0
    assert runtime.comet.calls == [
        {"method": "entity", "entity_id": ENTITY_10, "entity_type": TYPE_3600}
    ]


def test_delete_history_type_flag_overrides_inferred_entity_type(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    result = _invoke(
        ["--type", "Blesensor.per300", "--reason", REASON, "--send", ENTITY_10],
        capsys,
        runtime,
    )

    assert result == 0
    assert runtime.comet.calls == [
        {"method": "entity", "entity_id": ENTITY_10, "entity_type": "Blesensor.per300"}
    ]


def test_delete_history_non_canonical_id_without_type_errors(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    result = _invoke(["--reason", REASON, "custom-entity"], capsys, runtime)

    assert result != 0
    assert "--type" in capsys.readouterr().err
    assert runtime.comet.calls == []


def test_delete_history_per_entity_uses_default_type_when_spec_omits_it(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    result = _invoke(_base_args(ENTITY_10, send=True), capsys, runtime)

    assert result == 0
    assert runtime.comet.calls == [
        {"method": "entity", "entity_id": ENTITY_10, "entity_type": TYPE_3600}
    ]


def test_delete_history_per_attribute_calls_delete_attribute_history_once_per_attr(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    result = _invoke(
        _base_args(ENTITY_10, ENTITY_11, send=True) + ["--attrs", "a,b,c"],
        capsys,
        runtime,
    )

    assert result == 0
    assert runtime.comet.calls == [
        {
            "method": "attribute",
            "entity_id": entity_id,
            "entity_type": TYPE_3600,
            "attr": attr,
        }
        for entity_id in [ENTITY_10, ENTITY_11]
        for attr in ["a", "b", "c"]
    ]


def test_delete_history_flow_attrs_expands_to_ten_product_a_attributes(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    result = _invoke(
        _base_args(ENTITY_10, send=True) + ["--flow-attrs"],
        capsys,
        runtime,
    )

    assert result == 0
    assert [call["attr"] for call in runtime.comet.calls] == FLOW_ATTRS


def test_delete_history_204_counted_as_ok_exit_zero(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    runtime.comet.outcomes = [204]

    result = _invoke(_base_args(ENTITY_10, send=True), capsys, runtime)

    assert result == 0
    assert len(runtime.comet.calls) == 1


def test_delete_history_404_counted_as_noop_exit_zero(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    runtime.comet.outcomes = [404]

    result = _invoke(_base_args(ENTITY_10, send=True), capsys, runtime)

    assert result == 0
    assert len(runtime.comet.calls) == 1


def test_delete_history_500_continues_to_next_target_exit_nonzero(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    runtime.comet.outcomes = [_http_error(500, "comet failed"), 204]

    result = _invoke(_base_args(ENTITY_10, ENTITY_11, send=True), capsys, runtime)

    assert result != 0
    assert runtime.comet.calls == [
        {"method": "entity", "entity_id": ENTITY_10, "entity_type": TYPE_3600},
        {"method": "entity", "entity_id": ENTITY_11, "entity_type": TYPE_3600},
    ]


def _invoke(
    argv: list[str],
    _capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> int:
    delete_history = _delete_history_module()
    _patch_delete_history_module(delete_history, runtime)
    try:
        result = delete_history.main(argv)
    except SystemExit as exc:
        result = exc.code
    if result is None:
        return 0
    if isinstance(result, int):
        return result
    return int(result)


def _delete_history_module() -> Any:
    return importlib.import_module("scripts.delete_history")


# NOTE: These tests patch client classes directly on scripts.delete_history. The
# implementation should import AuthClient, CometClient, and their settings
# classes into module-level names so these fakes replace every network-facing
# client.
def _patch_delete_history_module(delete_history: Any, runtime: RuntimePatch) -> None:
    delete_history.AuthClient = runtime.build_auth
    delete_history.AuthSettings = type(
        "FakeAuthSettings", (), {"from_env": staticmethod(lambda: object())}
    )
    delete_history.CometSettings = type(
        "FakeCometSettings", (), {"from_env": staticmethod(_comet_settings_from_env)}
    )
    delete_history.CometClient = runtime.build_comet
    delete_history.load_dotenv = lambda *_args, **_kwargs: None
    delete_history.find_dotenv = lambda *_args, **_kwargs: ""


def _comet_settings_from_env() -> SimpleNamespace:
    return SimpleNamespace(
        base_url=os.environ.get("FIWARE_BASE_URL", "https://fiware.example.test"),
        service=os.environ.get("FIWARE_SERVICE", ""),
        service_path=os.environ.get("FIWARE_SERVICE_PATH", "/"),
        verify_tls=True,
        timeout=10,
    )


def _base_args(
    *entity_specs: str,
    send: bool = False,
    production_override: bool = False,
) -> list[str]:
    args = ["--type", TYPE_3600, "--reason", REASON]
    if send:
        args.append("--send")
    if production_override:
        args.append("--i-know-this-is-production")
    args.extend(_entity_spec(entity_spec) for entity_spec in entity_specs)
    return args


def _entity_spec(entity_id: str) -> str:
    return entity_id


def _http_error(status_code: int, text: str) -> requests.HTTPError:
    response = requests.Response()
    response.status_code = status_code
    response._content = text.encode("utf-8")
    error = requests.HTTPError(text)
    error.response = response
    return error
