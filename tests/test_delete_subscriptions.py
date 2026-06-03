# ruff: noqa: E501
import importlib
import os
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any

import pytest
import requests

from sendai_pipeline.logging_setup import _ALLOWED_EXTRA_KEYS

SUB_A = "65e87f5c20bd0c390e057c62"
SUB_B = "65e87f5c20bd0c390e057c63"
SUB_C = "65e87f5c20bd0c390e057c64"
REASON = "retiring stale STH subscriptions per scratch note"


class FakeAuth:
    def __init__(self, runtime: "RuntimePatch") -> None:
        self._runtime = runtime

    def get_token(self, *, force_refresh: bool = False) -> str:
        self._runtime.token_calls.append(force_refresh)
        return "token"


class FakeSubscriptionGet:
    def __init__(
        self,
        outcomes: dict[str, Any] | None = None,
        default_description: str = "stale subscription",
    ) -> None:
        self.outcomes: dict[str, Any] = dict(outcomes or {})
        self.default_description = default_description
        self.calls: list[str] = []

    def __call__(
        self,
        subscription_id: str,
        *_args: Any,
        **_kwargs: Any,
    ) -> dict[str, Any] | None:
        self.calls.append(subscription_id)
        outcome = self.outcomes.get(subscription_id, None)
        if isinstance(outcome, BaseException):
            raise outcome
        if outcome is None and subscription_id not in self.outcomes:
            return {"id": subscription_id, "description": self.default_description}
        return outcome


class FakeSubscriptionDelete:
    def __init__(self, outcomes: dict[str, Any] | None = None) -> None:
        self.outcomes: dict[str, Any] = dict(outcomes or {})
        self.calls: list[str] = []

    def __call__(
        self,
        subscription_id: str,
        *_args: Any,
        **_kwargs: Any,
    ) -> int:
        self.calls.append(subscription_id)
        outcome = self.outcomes.get(subscription_id, 204)
        if isinstance(outcome, BaseException):
            raise outcome
        return int(outcome)


@dataclass
class RuntimePatch:
    get: FakeSubscriptionGet = field(default_factory=FakeSubscriptionGet)
    delete: FakeSubscriptionDelete = field(default_factory=FakeSubscriptionDelete)
    auth_inits: int = 0
    token_calls: list[bool] = field(default_factory=list)

    def build_auth(self, *_args: Any, **_kwargs: Any) -> FakeAuth:
        self.auth_inits += 1
        return FakeAuth(self)


@pytest.fixture
def runtime(monkeypatch: pytest.MonkeyPatch) -> RuntimePatch:
    monkeypatch.setenv("FIWARE_BASE_URL", "https://fiware.example.test")
    monkeypatch.setenv("FIWARE_SERVICE", "sendai")
    monkeypatch.setenv("FIWARE_SERVICE_PATH", "/city")
    return RuntimePatch()


def test_delete_subscriptions_requires_reason(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    result = _invoke([SUB_A], capsys, runtime)

    assert result != 0
    assert runtime.get.calls == []
    assert runtime.delete.calls == []


def test_delete_subscriptions_requires_at_least_one_id(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    result = _invoke(["--reason", REASON], capsys, runtime)

    assert result != 0
    assert runtime.get.calls == []
    assert runtime.delete.calls == []


def test_delete_subscriptions_rejects_empty_id(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    result = _invoke(["--reason", REASON, ""], capsys, runtime)

    assert result != 0
    assert runtime.get.calls == []
    assert runtime.delete.calls == []


def test_delete_subscriptions_rejects_wildcard_id(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    result = _invoke(["--reason", REASON, "*"], capsys, runtime)

    assert result != 0
    assert runtime.get.calls == []
    assert runtime.delete.calls == []


def test_delete_subscriptions_rejects_malformed_id(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    result = _invoke(["--reason", REASON, "not-a-real-id"], capsys, runtime)

    assert result != 0
    assert runtime.get.calls == []
    assert runtime.delete.calls == []


def test_delete_subscriptions_rejects_short_hex_id(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    result = _invoke(["--reason", REASON, "65e87f5c20bd0c390e057c6"], capsys, runtime)

    assert result != 0
    assert runtime.get.calls == []
    assert runtime.delete.calls == []


def test_delete_subscriptions_rejects_send_in_production_service_without_override(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    runtime: RuntimePatch,
) -> None:
    monkeypatch.setenv("FIWARE_SERVICE", "")
    monkeypatch.setenv("FIWARE_SERVICE_PATH", "/city")

    result = _invoke(["--reason", REASON, "--send", SUB_A], capsys, runtime)

    assert result != 0
    assert runtime.delete.calls == []


def test_delete_subscriptions_rejects_send_in_root_service_path_without_override(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    runtime: RuntimePatch,
) -> None:
    monkeypatch.setenv("FIWARE_SERVICE", "sendai")
    monkeypatch.setenv("FIWARE_SERVICE_PATH", "/")

    result = _invoke(["--reason", REASON, "--send", SUB_A], capsys, runtime)

    assert result != 0
    assert runtime.delete.calls == []


def test_delete_subscriptions_allows_send_in_production_with_override(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    runtime: RuntimePatch,
) -> None:
    monkeypatch.setenv("FIWARE_SERVICE", "")
    monkeypatch.setenv("FIWARE_SERVICE_PATH", "/")

    result = _invoke(
        ["--reason", REASON, "--send", "--i-know-this-is-production", SUB_A],
        capsys,
        runtime,
    )

    assert result == 0
    assert runtime.delete.calls == [SUB_A]


def test_delete_subscriptions_dry_run_does_not_issue_delete(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    result = _invoke(["--reason", REASON, SUB_A], capsys, runtime)

    assert result == 0
    assert runtime.get.calls == [SUB_A]
    assert runtime.delete.calls == []


def test_delete_subscriptions_dry_run_prints_description_from_prefetch(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    runtime.get.outcomes = {
        SUB_A: {
            "id": SUB_A,
            "description": "Setting for jp.sendai.Blesensor at 2024-02-23 15:18",
        }
    }

    result = _invoke(["--reason", REASON, SUB_A], capsys, runtime)

    out = capsys.readouterr().out
    assert result == 0
    assert "Setting for jp.sendai.Blesensor at 2024-02-23 15:18" in out
    assert SUB_A in out


def test_delete_subscriptions_dry_run_prints_no_description_placeholder(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    runtime.get.outcomes = {SUB_A: {"id": SUB_A}}

    result = _invoke(["--reason", REASON, SUB_A], capsys, runtime)

    out = capsys.readouterr().out
    assert result == 0
    assert "<no description>" in out


def test_delete_subscriptions_prefetch_404_skips_delete_counts_absent(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    runtime.get.outcomes = {SUB_A: None}

    result = _invoke(
        ["--reason", REASON, "--send", SUB_A],
        capsys,
        runtime,
    )

    assert result == 0
    assert runtime.get.calls == [SUB_A]
    assert runtime.delete.calls == []


def test_delete_subscriptions_send_issues_delete_per_id(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    result = _invoke(
        ["--reason", REASON, "--send", SUB_A, SUB_B, SUB_C],
        capsys,
        runtime,
    )

    assert result == 0
    assert runtime.get.calls == [SUB_A, SUB_B, SUB_C]
    assert runtime.delete.calls == [SUB_A, SUB_B, SUB_C]


def test_delete_subscriptions_delete_204_counted_as_deleted(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    runtime.delete.outcomes = {SUB_A: 204}

    result = _invoke(["--reason", REASON, "--send", SUB_A], capsys, runtime)

    assert result == 0
    assert runtime.delete.calls == [SUB_A]


def test_delete_subscriptions_delete_404_counted_as_absent_exit_zero(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    runtime.delete.outcomes = {SUB_A: 404}

    result = _invoke(["--reason", REASON, "--send", SUB_A], capsys, runtime)

    assert result == 0
    assert runtime.delete.calls == [SUB_A]


def test_delete_subscriptions_delete_500_counted_as_failure_exit_nonzero_continues(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    runtime.delete.outcomes = {SUB_A: _http_error(500, "boom"), SUB_B: 204}

    result = _invoke(
        ["--reason", REASON, "--send", SUB_A, SUB_B],
        capsys,
        runtime,
    )

    assert result != 0
    assert runtime.delete.calls == [SUB_A, SUB_B]


def test_delete_subscriptions_emits_requested_target_and_summary_log_events(
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
    runtime: RuntimePatch,
) -> None:
    caplog.set_level("INFO", logger="sendai_pipeline")

    result = _invoke(
        ["--reason", REASON, "--send", SUB_A],
        capsys,
        runtime,
    )

    events = [record.__dict__.get("event") for record in caplog.records]
    assert result == 0
    assert "delete_subscriptions_requested" in events
    assert "delete_subscriptions_target" in events
    assert "delete_subscriptions_summary" in events


def test_delete_subscriptions_target_records_carry_phase_and_reason(
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
    runtime: RuntimePatch,
) -> None:
    caplog.set_level("INFO", logger="sendai_pipeline")

    result = _invoke(
        ["--reason", REASON, "--send", SUB_A],
        capsys,
        runtime,
    )

    assert result == 0
    target_records = [
        record.__dict__
        for record in caplog.records
        if record.__dict__.get("event") == "delete_subscriptions_target"
    ]
    assert len(target_records) >= 2  # one prefetch, one delete
    phases = sorted(record["phase"] for record in target_records)
    assert phases == ["delete", "prefetch"]
    assert all(record["reason"] == REASON for record in target_records)


def test_delete_subscriptions_dry_run_target_records_carry_phase_and_reason(
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
    runtime: RuntimePatch,
) -> None:
    caplog.set_level("INFO", logger="sendai_pipeline")

    result = _invoke(["--reason", REASON, SUB_A], capsys, runtime)

    assert result == 0
    target_records = [
        record.__dict__
        for record in caplog.records
        if record.__dict__.get("event") == "delete_subscriptions_target"
    ]
    phases = sorted(record["phase"] for record in target_records)
    assert phases == ["delete", "prefetch"]
    assert all(record["reason"] == REASON for record in target_records)


def test_delete_subscriptions_summary_carries_reason(
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
    runtime: RuntimePatch,
) -> None:
    caplog.set_level("INFO", logger="sendai_pipeline")

    result = _invoke(["--reason", REASON, "--send", SUB_A], capsys, runtime)

    assert result == 0
    summary = next(
        record.__dict__
        for record in caplog.records
        if record.__dict__.get("event") == "delete_subscriptions_summary"
    )
    assert summary["reason"] == REASON


def test_delete_subscriptions_rejects_uppercase_hex_id(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    # Orion ObjectId ids are always lowercase hex; the operator-facing
    # error message must steer copy-paste typos toward the lowercase form
    # rather than silently sending something Orion would reject.
    result = _invoke(["--reason", REASON, "65E87F5C20BD0C390E057C62"], capsys, runtime)

    assert result != 0
    assert runtime.get.calls == []
    assert runtime.delete.calls == []


def test_delete_subscriptions_rejects_id_with_leading_whitespace(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    result = _invoke(["--reason", REASON, " 65e87f5c20bd0c390e057c62"], capsys, runtime)

    assert result != 0
    assert runtime.get.calls == []
    assert runtime.delete.calls == []


def test_delete_subscriptions_rejects_overlong_id(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    result = _invoke(["--reason", REASON, "65e87f5c20bd0c390e057c620"], capsys, runtime)

    assert result != 0
    assert runtime.get.calls == []
    assert runtime.delete.calls == []


def test_delete_subscriptions_rejects_id_with_trailing_newline(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    # re.match + `$` would silently accept a trailing newline; the
    # validator must fullmatch so a copy-paste with an embedded newline
    # never reaches the network.
    result = _invoke(["--reason", REASON, f"{SUB_A}\n"], capsys, runtime)

    assert result != 0
    assert runtime.get.calls == []
    assert runtime.delete.calls == []


def test_delete_subscriptions_rejects_blank_reason(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    result = _invoke(["--reason", "", SUB_A], capsys, runtime)

    assert result != 0
    assert runtime.get.calls == []
    assert runtime.delete.calls == []


def test_delete_subscriptions_rejects_whitespace_reason(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    result = _invoke(["--reason", "   ", SUB_A], capsys, runtime)

    assert result != 0
    assert runtime.get.calls == []
    assert runtime.delete.calls == []


def test_delete_subscriptions_rejects_duplicate_ids(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    result = _invoke(["--reason", REASON, SUB_A, SUB_A], capsys, runtime)

    assert result != 0
    assert runtime.get.calls == []
    assert runtime.delete.calls == []


def test_delete_subscriptions_failure_target_records_carry_http_status(
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
    runtime: RuntimePatch,
) -> None:
    caplog.set_level("INFO", logger="sendai_pipeline")
    runtime.delete.outcomes = {SUB_A: _http_error(500, "boom")}

    result = _invoke(["--reason", REASON, "--send", SUB_A], capsys, runtime)

    assert result != 0
    failure_records = [
        record.__dict__
        for record in caplog.records
        if record.__dict__.get("event") == "delete_subscriptions_target"
        and record.__dict__.get("ok") is False
    ]
    assert len(failure_records) == 1
    assert failure_records[0]["http_status"] == 500
    assert failure_records[0]["phase"] == "delete"


def test_delete_subscriptions_dry_run_requires_no_production_override(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    runtime: RuntimePatch,
) -> None:
    monkeypatch.setenv("FIWARE_SERVICE", "")
    monkeypatch.setenv("FIWARE_SERVICE_PATH", "/")

    result = _invoke(["--reason", REASON, SUB_A], capsys, runtime)

    assert result == 0
    assert runtime.delete.calls == []


def _invoke(
    argv: list[str],
    _capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> int:
    delete_subscriptions = _delete_subscriptions_module()
    _patch_delete_subscriptions_module(delete_subscriptions, runtime)
    try:
        result = delete_subscriptions.main(argv)
    except SystemExit as exc:
        result = exc.code
    if result is None:
        return 0
    if isinstance(result, int):
        return result
    return int(result)


def _delete_subscriptions_module() -> Any:
    return importlib.import_module("scripts.delete_subscriptions")


# scripts.delete_subscriptions imports auth/settings/library helpers at
# module level so the test suite can patch them without touching the
# network. get_subscription/delete_subscription are the library
# functions exposed by sendai_pipeline.sth_subscriptions and re-bound
# at import time on the script module.
def _patch_delete_subscriptions_module(module: Any, runtime: RuntimePatch) -> None:
    module.AuthClient = runtime.build_auth
    module.AuthSettings = type(
        "FakeAuthSettings", (), {"from_env": staticmethod(lambda: object())}
    )
    module.OrionSettings = type(
        "FakeOrionSettings",
        (),
        {"from_env": staticmethod(_fiware_settings_from_env)},
    )
    module.get_subscription = runtime.get
    module.delete_subscription = runtime.delete
    module.load_dotenv = lambda *_args, **_kwargs: None
    module.find_dotenv = lambda *_args, **_kwargs: ""
    # Bypass configure_logging so caplog (a root-attached handler) can
    # observe records on the sendai_pipeline logger; the real
    # configure_logging sets propagate=False, which would hide every
    # record from pytest's LogCaptureHandler.
    module.configure_logging = lambda *_args, **_kwargs: None
    module.LoggingSettings = type(
        "FakeLoggingSettings", (), {"from_env": staticmethod(lambda: object())}
    )


def _fiware_settings_from_env() -> SimpleNamespace:
    return SimpleNamespace(
        base_url=os.environ.get("FIWARE_BASE_URL", "https://fiware.example.test"),
        service=os.environ.get("FIWARE_SERVICE", ""),
        service_path=os.environ.get("FIWARE_SERVICE_PATH", "/"),
        verify_tls=True,
        timeout=10,
    )


def _http_error(status_code: int, text: str) -> requests.HTTPError:
    response = requests.Response()
    response.status_code = status_code
    response._content = text.encode("utf-8")
    error = requests.HTTPError(text)
    error.response = response
    return error


def test_delete_subscriptions_extras_are_all_allow_listed() -> None:
    # Regression for the unknown_log_field WARNINGs observed when the
    # summary record carried `deleted`/`absent` before they were added
    # to _ALLOWED_EXTRA_KEYS. The script-side test harness patches
    # configure_logging() out, so the JsonFormatter filter never runs
    # during normal tests; this asserts every extras key the script
    # emits against the allow-list directly.
    required = {
        "event",
        "subscription_id",
        "phase",
        "reason",
        "ok",
        "http_status",
        "dry_run",
        "send_mode",
        "deleted",
        "absent",
        "count_failed",
    }
    assert required <= _ALLOWED_EXTRA_KEYS
