import importlib
from typing import Any

import pytest

from sendai_pipeline.sth_subscriptions import (
    StHSubscriptionError,
    StHSubscriptionResult,
)

BASE_ENV = {
    "FIWARE_BASE_URL": "https://example.test",
    "COMET_NOTIFY_URL": "http://internal-comet.example/notify",
}
MALFORMED_PRODUCT_B_ENV = {
    "PRODUCT_B_AGGREGATE_ENTITY_ID": " malformed",
}
PRODUCT_B_ENV_KEYS = (
    "PRODUCT_B_AGGREGATE_ENTITY_ID",
    "PRODUCT_B_AGGREGATE_ENTITY_TYPE",
)


def _module() -> Any:
    return importlib.import_module("scripts.create_sth_subscriptions")


class _Recorder:
    """Counts the wiring decisions main() makes so tests can assert on them."""

    def __init__(self) -> None:
        self.a_calls = 0
        self.b_calls = 0
        self.auth_constructed = 0

    def create_a(self, **_kwargs: Any) -> StHSubscriptionResult:
        self.a_calls += 1
        return StHSubscriptionResult(would_create=1, created=0, skipped=0, failed=0)

    def create_b(self, **_kwargs: Any) -> StHSubscriptionResult:
        self.b_calls += 1
        return StHSubscriptionResult(would_create=1, created=0, skipped=0, failed=0)


def _patch(
    module: Any,
    monkeypatch: pytest.MonkeyPatch,
    env: dict[str, str],
) -> _Recorder:
    recorder = _Recorder()
    for key in (*BASE_ENV, *PRODUCT_B_ENV_KEYS):
        monkeypatch.delenv(key, raising=False)
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr(module, "load_dotenv", lambda *_a, **_k: None)
    monkeypatch.setattr(module, "find_dotenv", lambda *_a, **_k: "")
    monkeypatch.setattr(module, "configure_logging", lambda *_a, **_k: None)
    monkeypatch.setattr(
        module,
        "LoggingSettings",
        type("FakeLoggingSettings", (), {"from_env": staticmethod(lambda: object())}),
    )
    monkeypatch.setattr(
        module,
        "AuthSettings",
        type("FakeAuthSettings", (), {"from_env": staticmethod(lambda: object())}),
    )
    monkeypatch.setattr(
        module,
        "create_product_a_sth_subscription",
        lambda **kwargs: recorder.create_a(**kwargs),
    )
    monkeypatch.setattr(
        module,
        "create_product_b_sth_subscription",
        lambda **kwargs: recorder.create_b(**kwargs),
    )

    def _auth_client(*_a: Any, **_k: Any) -> Any:
        recorder.auth_constructed += 1
        return object()

    monkeypatch.setattr(module, "AuthClient", _auth_client)
    return recorder


def test_main_product_a_ignores_malformed_product_b_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # --product a passes require_product_b=False, so a malformed
    # PRODUCT_B_AGGREGATE_* value does not fail a Product A-only run.
    module = _module()
    recorder = _patch(module, monkeypatch, {**BASE_ENV, **MALFORMED_PRODUCT_B_ENV})

    exit_code = module.main(["--product", "a", "--no-show-body"])

    assert exit_code == 0
    assert recorder.a_calls == 1
    assert recorder.b_calls == 0


def test_main_product_b_validates_product_b_config_before_auth(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # --product b requires Product B config; a malformed value must be rejected
    # before any auth client is constructed or any subscription is created.
    module = _module()
    recorder = _patch(module, monkeypatch, {**BASE_ENV, **MALFORMED_PRODUCT_B_ENV})

    with pytest.raises(StHSubscriptionError, match="PRODUCT_B_AGGREGATE_ENTITY_ID"):
        module.main(["--product", "b", "--send", "--no-show-body"])

    assert recorder.auth_constructed == 0
    assert recorder.b_calls == 0


def test_main_product_all_validates_product_b_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # --product all includes b, so it validates Product B config eagerly and
    # rejects a malformed value before creating either subscription.
    module = _module()
    recorder = _patch(module, monkeypatch, {**BASE_ENV, **MALFORMED_PRODUCT_B_ENV})

    with pytest.raises(StHSubscriptionError, match="PRODUCT_B_AGGREGATE_ENTITY_ID"):
        module.main(["--product", "all", "--no-show-body"])

    assert recorder.a_calls == 0
    assert recorder.b_calls == 0
