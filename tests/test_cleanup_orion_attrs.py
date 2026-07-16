import ast
import inspect
import os
from dataclasses import dataclass, field
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

from scripts import cleanup_orion_attrs

ENTITY_ID = "jp.sendai.Blesensor.flow"
ENTITY_TYPE = "Blesensor.flow"
REASON = "exceptional Product B cutover cleanup"
ATTRS = ["peopleCount_flow_7", "legacy_direction_attr"]
SCALAR_ATTRS = [
    "dateObservedFrom",
    "dateObservedTo",
    "dateRetrieved",
    "identifcation",
]


class FakeAuth:
    pass


class FakeOrionClient:
    def __init__(self, outcomes: list[dict[str, Any]] | None = None) -> None:
        self.outcomes = list(outcomes or [])
        self.calls: list[dict[str, str]] = []

    def delete_attr(
        self,
        entity_id: str,
        entity_type: str,
        attr_name: str,
    ) -> dict[str, Any]:
        self.calls.append(
            {
                "entity_id": entity_id,
                "entity_type": entity_type,
                "attr_name": attr_name,
            }
        )
        if self.outcomes:
            return self.outcomes.pop(0)
        return _delete_result(204)


class FailIfConstructed:
    def __init__(self, *_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("cleanup must not construct an STH-Comet dependency")


@dataclass
class RuntimePatch:
    orion: FakeOrionClient = field(default_factory=FakeOrionClient)
    auth_inits: int = 0
    orion_inits: int = 0

    def build_auth(self, *_args: Any, **_kwargs: Any) -> FakeAuth:
        self.auth_inits += 1
        return FakeAuth()

    def build_orion(self, *_args: Any, **_kwargs: Any) -> FakeOrionClient:
        self.orion_inits += 1
        return self.orion


@pytest.fixture
def runtime(monkeypatch: pytest.MonkeyPatch) -> RuntimePatch:
    monkeypatch.setenv("FIWARE_BASE_URL", "https://fiware.example.test")
    monkeypatch.setenv("FIWARE_SERVICE", "sendai")
    monkeypatch.setenv("FIWARE_SERVICE_PATH", "/city")
    patched = RuntimePatch()
    _patch_cleanup_module(monkeypatch, patched)
    return patched


def test_cleanup_orion_attrs_requires_explicit_attrs_without_scanning(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    result = _invoke(
        [
            "--entity-id",
            ENTITY_ID,
            "--entity-type",
            ENTITY_TYPE,
            "--reason",
            REASON,
        ],
        capsys,
    )

    assert result != 0
    assert runtime.orion_inits == 0
    assert runtime.orion.calls == []


@pytest.mark.parametrize("attrs", ["", " , \t"])
def test_cleanup_orion_attrs_rejects_blank_explicit_attrs_without_scanning(
    attrs: str,
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    args = _base_args("placeholder")
    args[args.index("--attrs") + 1] = attrs

    result = _invoke(args, capsys)

    assert result != 0
    assert runtime.orion_inits == 0
    assert runtime.orion.calls == []


@pytest.mark.parametrize("missing_option", ["--entity-id", "--entity-type"])
def test_cleanup_orion_attrs_requires_explicit_entity_id_and_type(
    missing_option: str,
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    args = _base_args(*ATTRS)
    option_index = args.index(missing_option)
    del args[option_index : option_index + 2]

    result = _invoke(args, capsys)

    assert result != 0
    assert runtime.orion_inits == 0
    assert runtime.orion.calls == []


@pytest.mark.parametrize(
    ("option", "value"),
    [("--entity-id", "   "), ("--entity-type", "\t")],
)
def test_cleanup_orion_attrs_rejects_blank_entity_id_or_type(
    option: str,
    value: str,
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    args = _base_args(*ATTRS)
    args[args.index(option) + 1] = value

    result = _invoke(args, capsys)

    assert result != 0
    assert runtime.orion_inits == 0
    assert runtime.orion.calls == []


def test_cleanup_orion_attrs_requires_reason(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    args = _base_args(*ATTRS)
    reason_index = args.index("--reason")
    del args[reason_index : reason_index + 2]

    result = _invoke(args, capsys)

    assert result != 0
    assert runtime.orion_inits == 0
    assert runtime.orion.calls == []


def test_cleanup_orion_attrs_rejects_blank_reason(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    result = _invoke(_base_args(*ATTRS, reason="   \t"), capsys)

    assert result != 0
    assert runtime.orion_inits == 0
    assert runtime.orion.calls == []


def test_cleanup_orion_attrs_defaults_to_per_attribute_dry_run(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    result = _invoke(_base_args(*ATTRS), capsys)

    out = capsys.readouterr().out
    assert result == 0
    assert out.count("DRY-RUN") == len(ATTRS)
    assert all(attr in out for attr in ATTRS)
    assert runtime.auth_inits == 0
    assert runtime.orion_inits == 0
    assert runtime.orion.calls == []


def test_cleanup_orion_attrs_send_deletes_each_explicit_attr_and_prints_results(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    runtime.orion.outcomes = [_delete_result(204), _delete_result(404)]

    result = _invoke(_base_args(*ATTRS, send=True), capsys)

    output = _combined_output(capsys)
    assert result == 0
    assert runtime.orion.calls == _expected_calls(ATTRS)
    assert f"DELETED: {ATTRS[0]}" in output
    assert f"ALREADY ABSENT: {ATTRS[1]}" in output
    assert "deleted=1" in output
    assert "absent=1" in output
    assert "failed=0" in output


@pytest.mark.parametrize(
    ("service", "service_path"),
    [("", "/city"), ("   ", "/city"), ("sendai", "/"), ("sendai", "")],
)
def test_cleanup_orion_attrs_rejects_production_send_without_override(
    service: str,
    service_path: str,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    runtime: RuntimePatch,
) -> None:
    monkeypatch.setenv("FIWARE_SERVICE", service)
    monkeypatch.setenv("FIWARE_SERVICE_PATH", service_path)

    result = _invoke(_base_args(*ATTRS, send=True), capsys)

    assert result != 0
    assert runtime.orion_inits == 0
    assert runtime.orion.calls == []


def test_cleanup_orion_attrs_allows_production_dry_run_without_override(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    runtime: RuntimePatch,
) -> None:
    monkeypatch.setenv("FIWARE_SERVICE", "")
    monkeypatch.setenv("FIWARE_SERVICE_PATH", "/")

    result = _invoke(_base_args(*ATTRS), capsys)

    assert result == 0
    assert runtime.auth_inits == 0
    assert runtime.orion_inits == 0
    assert runtime.orion.calls == []


def test_cleanup_orion_attrs_allows_production_send_with_override(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    runtime: RuntimePatch,
) -> None:
    monkeypatch.setenv("FIWARE_SERVICE", "")
    monkeypatch.setenv("FIWARE_SERVICE_PATH", "/")

    result = _invoke(
        _base_args(*ATTRS, send=True, production_override=True),
        capsys,
    )

    assert result == 0
    assert runtime.orion.calls == _expected_calls(ATTRS)


def test_cleanup_orion_attrs_continues_after_failure_and_prints_summary(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    attrs = ["obsolete_a", "obsolete_b", "obsolete_c"]
    runtime.orion.outcomes = [
        _delete_result(204),
        _delete_result(500, ok=False, body_excerpt="server error"),
        _delete_result(404),
    ]

    result = _invoke(_base_args(*attrs, send=True), capsys)

    output = _combined_output(capsys)
    assert result != 0
    assert runtime.orion.calls == _expected_calls(attrs)
    assert f"DELETED: {attrs[0]}" in output
    assert f"FAILED: {attrs[1]}" in output
    assert f"ALREADY ABSENT: {attrs[2]}" in output
    assert "deleted=1" in output
    assert "absent=1" in output
    assert "failed=1" in output


def test_cleanup_orion_attrs_allows_explicit_contract_scalars_without_extra_guard(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    result = _invoke(_base_args(*SCALAR_ATTRS, send=True), capsys)

    assert result == 0
    assert runtime.orion.calls == _expected_calls(SCALAR_ATTRS)


def test_cleanup_orion_attrs_rejects_whole_entity_history_delete_flag(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    result = _invoke(_base_args(*ATTRS) + ["--purge-history"], capsys)

    assert result != 0
    assert runtime.orion_inits == 0
    assert runtime.orion.calls == []


def test_cleanup_orion_attrs_has_no_delete_entities_or_comet_dependency() -> None:
    imports = _direct_imports(cleanup_orion_attrs)

    assert "scripts.delete_entities" not in imports
    assert "sendai_pipeline.comet_client" not in imports


def _invoke(
    argv: list[str],
    _capsys: pytest.CaptureFixture[str],
) -> int:
    try:
        result = cleanup_orion_attrs.main(argv)
    except SystemExit as exc:
        result = exc.code
    if result is None:
        return 0
    return int(result)


def _patch_cleanup_module(
    monkeypatch: pytest.MonkeyPatch,
    runtime: RuntimePatch,
) -> None:
    monkeypatch.setattr(cleanup_orion_attrs, "AuthClient", runtime.build_auth)
    monkeypatch.setattr(
        cleanup_orion_attrs,
        "AuthSettings",
        type("FakeAuthSettings", (), {"from_env": staticmethod(lambda: object())}),
    )
    monkeypatch.setattr(
        cleanup_orion_attrs,
        "OrionSettings",
        type(
            "FakeOrionSettings",
            (),
            {"from_env": staticmethod(_fiware_settings_from_env)},
        ),
    )
    monkeypatch.setattr(cleanup_orion_attrs, "OrionClient", runtime.build_orion)
    monkeypatch.setattr(
        cleanup_orion_attrs,
        "CometClient",
        FailIfConstructed,
        raising=False,
    )
    monkeypatch.setattr(
        cleanup_orion_attrs,
        "configure_logging",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        cleanup_orion_attrs,
        "LoggingSettings",
        type("FakeLoggingSettings", (), {"from_env": staticmethod(lambda: object())}),
    )
    monkeypatch.setattr(cleanup_orion_attrs, "load_dotenv", lambda *_args: None)
    monkeypatch.setattr(cleanup_orion_attrs, "find_dotenv", lambda **_kwargs: "")


def _fiware_settings_from_env() -> SimpleNamespace:
    return SimpleNamespace(
        base_url=os.environ.get("FIWARE_BASE_URL", "https://fiware.example.test"),
        service=os.environ.get("FIWARE_SERVICE", ""),
        service_path=os.environ.get("FIWARE_SERVICE_PATH", "/"),
        verify_tls=True,
        timeout=10,
    )


def _base_args(
    *attrs: str,
    reason: str = REASON,
    send: bool = False,
    production_override: bool = False,
) -> list[str]:
    args = [
        "--entity-id",
        ENTITY_ID,
        "--entity-type",
        ENTITY_TYPE,
        "--attrs",
        ",".join(attrs),
        "--reason",
        reason,
    ]
    if send:
        args.append("--send")
    if production_override:
        args.append("--i-know-this-is-production")
    return args


def _delete_result(
    status: int,
    *,
    ok: bool = True,
    body_excerpt: str | None = None,
) -> dict[str, Any]:
    return {
        "status": status,
        "ok": ok,
        "attempts": 1,
        "elapsed_ms": 0,
        "body_excerpt": body_excerpt,
        "dry_run": False,
    }


def _expected_calls(attrs: list[str]) -> list[dict[str, str]]:
    return [
        {
            "entity_id": ENTITY_ID,
            "entity_type": ENTITY_TYPE,
            "attr_name": attr,
        }
        for attr in attrs
    ]


def _combined_output(capsys: pytest.CaptureFixture[str]) -> str:
    captured = capsys.readouterr()
    return captured.out + captured.err


def _direct_imports(module: ModuleType) -> set[str]:
    tree = ast.parse(inspect.getsource(module))
    imports: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            imports.add(node.module)
            imports.update(f"{node.module}.{alias.name}" for alias in node.names)
    return imports
