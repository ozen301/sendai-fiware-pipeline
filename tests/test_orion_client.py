import hashlib
import json
import logging
from collections.abc import Callable
from typing import Any

import pytest
import requests

from sendai_pipeline.logging_setup import _ALLOWED_EXTRA_KEYS, payload_log_fields
from sendai_pipeline.orion_client import (
    OrionClient,
    OrionConfigError,
    OrionError,
    OrionSettings,
)


class FakeResponse:
    def __init__(
        self,
        status_code: int,
        *,
        text: str = "",
        payload: Any = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.status_code = status_code
        self.text = text
        self._payload = payload
        self.headers = headers or {}

    def json(self) -> Any:
        return self._payload

    def raise_for_status(self) -> None:
        if not 200 <= self.status_code < 300:
            raise requests.HTTPError(self.text)


class FakeSession:
    def __init__(self, outcomes: list[FakeResponse | Exception]) -> None:
        self.outcomes = list(outcomes)
        self.calls: list[dict[str, Any]] = []

    def post(self, url: str, **kwargs: Any) -> FakeResponse:
        self.calls.append({"method": "POST", "url": url, **kwargs})
        return self._next()

    def put(self, url: str, **kwargs: Any) -> FakeResponse:
        self.calls.append({"method": "PUT", "url": url, **kwargs})
        return self._next()

    def delete(self, url: str, **kwargs: Any) -> FakeResponse:
        self.calls.append({"method": "DELETE", "url": url, **kwargs})
        return self._next()

    def get(self, url: str, **kwargs: Any) -> FakeResponse:
        self.calls.append({"method": "GET", "url": url, **kwargs})
        return self._next()

    def _next(self) -> FakeResponse:
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


class FakeAuth:
    def __init__(
        self,
        *,
        tokens: list[str] | None = None,
        refresh_tokens: list[str] | None = None,
    ) -> None:
        self.tokens = tokens or ["token"]
        self.refresh_tokens = refresh_tokens or ["refreshed-token"]
        self.calls: list[bool] = []

    def get_token(self, *, force_refresh: bool = False) -> str:
        self.calls.append(force_refresh)
        if force_refresh:
            if len(self.refresh_tokens) > 1:
                return self.refresh_tokens.pop(0)
            return self.refresh_tokens[0]
        if len(self.tokens) > 1:
            return self.tokens.pop(0)
        return self.tokens[0]


class FakeSleep:
    def __init__(self) -> None:
        self.delays: list[float] = []

    def __call__(self, delay: float) -> None:
        self.delays.append(delay)


class FakeClock:
    def __init__(self, values: list[float]) -> None:
        self.values = list(values)

    def __call__(self) -> float:
        if len(self.values) > 1:
            return self.values.pop(0)
        return self.values[0]


def make_settings(**overrides: Any) -> OrionSettings:
    values = {
        "base_url": "https://fiware.example.test/",
        "service": "sendai",
        "service_path": "/",
        "verify_tls": True,
        "timeout": 3.5,
        "max_retries": 5,
    }
    values.update(overrides)
    return OrionSettings(**values)


def make_client(
    session: FakeSession,
    *,
    settings: OrionSettings | None = None,
    auth: FakeAuth | None = None,
    sleep: FakeSleep | None = None,
    now: Callable[[], float] | None = None,
    payload_mode: str = "failure",
    payload_max_bytes: int = 16384,
    response_max_bytes: int = 2048,
) -> OrionClient:
    return OrionClient(
        settings or make_settings(),
        auth=auth or FakeAuth(),
        session=session,
        sleep=sleep or FakeSleep(),
        now=now or (lambda: 1000.0),
        payload_mode=payload_mode,
        payload_max_bytes=payload_max_bytes,
        response_max_bytes=response_max_bytes,
    )


def canonical_json_bytes(value: dict[str, Any]) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def post_records(caplog: pytest.LogCaptureFixture, event: str) -> list[Any]:
    """Return log records whose ``event`` extra matches.

    Typed as ``list[Any]`` so tests can read structured extras
    (``record.entity_id`` etc.) without static type-checkers complaining —
    those attributes are attached dynamically via ``extra={...}`` and are
    not declared on :class:`logging.LogRecord`.
    """
    return [
        record for record in caplog.records if getattr(record, "event", None) == event
    ]


def sample_attrs() -> dict[str, Any]:
    return {
        "peopleCount_near": {"type": "Integer", "value": 12},
        "dateObservedFrom": {
            "type": "DateTime",
            "value": "2026-05-22T09:00:00+09:00",
        },
    }


def sample_aggregate_attrs() -> dict[str, Any]:
    return {
        "peopleCount_flow_2": {"type": "Number", "value": 3},
        "identifcation": {
            "type": "Text",
            "value": "jp.sendai.Blesensor.flow",
        },
        "dateRetrieved": {
            "type": "DateTime",
            "value": "2026-05-22T10:00:00+09:00",
        },
    }


def test_error_classes_have_config_subclass() -> None:
    assert issubclass(OrionConfigError, OrionError)


def test_settings_from_env_requires_base_url() -> None:
    with pytest.raises(OrionConfigError, match="FIWARE_BASE_URL"):
        OrionSettings.from_env({})


def test_settings_from_env_normalizes_base_url_and_defaults() -> None:
    settings = OrionSettings.from_env({"FIWARE_BASE_URL": "https://fiware.test///"})

    assert settings.base_url == "https://fiware.test"
    assert settings.service == ""
    assert settings.service_path == "/"
    assert settings.verify_tls is True
    assert settings.timeout == 10
    assert settings.max_retries == 5


def test_settings_from_env_reads_service_timeout_and_tls_flag() -> None:
    settings = OrionSettings.from_env(
        {
            "FIWARE_BASE_URL": "https://fiware.test",
            "FIWARE_SERVICE": "sendai",
            "FIWARE_SERVICE_PATH": "/city",
            "FIWARE_VERIFY_TLS": "false",
            "FIWARE_TIMEOUT_SECONDS": "2.25",
        }
    )

    assert settings.service == "sendai"
    assert settings.service_path == "/city"
    assert settings.verify_tls is False
    assert settings.timeout == 2.25


def test_update_attrs_posts_canonical_body_to_entity_attrs_endpoint() -> None:
    attrs = sample_attrs()
    session = FakeSession([FakeResponse(204)])
    auth = FakeAuth(tokens=["request-token"])
    client = make_client(session, auth=auth)

    result = client.update_attrs(
        "jp.sendai.Blesensor.per300.10",
        "Blesensor.per300",
        attrs,
    )

    assert result["ok"] is True
    assert result["status"] == 204
    assert result["attempts"] == 1
    assert len(session.calls) == 1
    call = session.calls[0]
    assert call["method"] == "POST"
    assert (
        call["url"] == "https://fiware.example.test/orion/v2.0/entities/"
        "jp.sendai.Blesensor.per300.10/attrs?type=Blesensor.per300"
    )
    assert call["headers"] == {
        "Authorization": "Bearer request-token",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Fiware-Service": "sendai",
        "Fiware-ServicePath": "/",
    }
    assert call["data"] == canonical_json_bytes(attrs)
    assert json.loads(call["data"]) == attrs
    assert "json" not in call
    assert call["timeout"] == 3.5
    assert call["verify"] is True
    assert auth.calls == [False]


def test_update_attrs_omits_type_query_and_empty_service_header() -> None:
    session = FakeSession([FakeResponse(204)])
    client = make_client(session, settings=make_settings(service=""))

    client.update_attrs("jp.sendai.Blesensor.per300.10", None, sample_attrs())

    call = session.calls[0]
    assert call["url"].endswith("/jp.sendai.Blesensor.per300.10/attrs")
    assert "?" not in call["url"]
    assert "Fiware-Service" not in call["headers"]
    assert call["headers"]["Fiware-ServicePath"] == "/"


def test_list_entities_gets_single_page_with_params_and_headers() -> None:
    response_payload = [{"id": "jp.sendai.Blesensor.per300.10"}]
    session = FakeSession([FakeResponse(200, payload=response_payload)])
    client = make_client(session)

    result = client.list_entities("Blesensor.per300", attrs="id", limit=500)

    assert result == response_payload
    assert len(session.calls) == 1
    call = session.calls[0]
    assert call["method"] == "GET"
    assert call["url"] == "https://fiware.example.test/orion/v2.0/entities"
    assert call["params"] == {
        "type": "Blesensor.per300",
        "limit": 500,
        "attrs": "id",
    }
    assert call["headers"]["Authorization"] == "Bearer token"
    assert call["headers"]["Accept"] == "application/json"
    assert "Content-Type" not in call["headers"]
    assert call["verify"] is True
    assert call["timeout"] == 3.5


def test_list_entities_defaults_to_single_page_limit_only() -> None:
    session = FakeSession([FakeResponse(200, payload=[])])
    client = make_client(session)

    assert client.list_entities("Blesensor.per3600") == []

    assert len(session.calls) == 1
    assert session.calls[0]["params"] == {
        "type": "Blesensor.per3600",
        "limit": 1000,
    }


def test_get_entity_gets_one_entity_with_optional_filters() -> None:
    response_payload = {
        "id": "jp.sendai.Blesensor.per300.10",
        "type": "Blesensor.per300",
        "peopleCount_immedate": {"type": "number", "value": 8},
    }
    session = FakeSession([FakeResponse(200, payload=response_payload)])
    client = make_client(session)

    result = client.get_entity(
        "jp.sendai.Blesensor.per300.10",
        entity_type="Blesensor.per300",
        attrs="dateObservedFrom,peopleCount_immedate",
    )

    assert result == response_payload
    assert len(session.calls) == 1
    call = session.calls[0]
    assert call["method"] == "GET"
    assert (
        call["url"] == "https://fiware.example.test/orion/v2.0/entities/"
        "jp.sendai.Blesensor.per300.10"
    )
    assert call["params"] == {
        "type": "Blesensor.per300",
        "attrs": "dateObservedFrom,peopleCount_immedate",
    }
    assert call["headers"]["Authorization"] == "Bearer token"
    assert "Content-Type" not in call["headers"]
    assert call["verify"] is True
    assert call["timeout"] == 3.5


def test_get_entity_raises_for_non_success_response() -> None:
    session = FakeSession([FakeResponse(404, text="missing")])
    client = make_client(session)

    with pytest.raises(requests.HTTPError, match="missing"):
        client.get_entity("missing")


def test_get_entity_refreshes_token_once_after_unauthorized_response() -> None:
    response_payload = {"id": "entity-1", "type": "Blesensor.per300"}
    session = FakeSession(
        [
            FakeResponse(401, text="expired"),
            FakeResponse(200, payload=response_payload),
        ]
    )
    auth = FakeAuth(tokens=["expired-token"], refresh_tokens=["fresh-token"])
    client = make_client(session, auth=auth)

    assert client.get_entity("entity-1") == response_payload

    assert auth.calls == [False, True]
    assert [c["headers"]["Authorization"] for c in session.calls] == [
        "Bearer expired-token",
        "Bearer fresh-token",
    ]


def test_unauthorized_response_force_refreshes_once_then_succeeds() -> None:
    session = FakeSession([FakeResponse(401, text="expired"), FakeResponse(204)])
    auth = FakeAuth(tokens=["expired-token"], refresh_tokens=["fresh-token"])
    sleep = FakeSleep()
    client = make_client(session, auth=auth, sleep=sleep)

    result = client.update_attrs("entity-1", "Blesensor.per300", sample_attrs())

    assert result["ok"] is True
    assert result["status"] == 204
    assert result["attempts"] == 2
    assert auth.calls == [False, True]
    assert sleep.delays == []
    assert [c["headers"]["Authorization"] for c in session.calls] == [
        "Bearer expired-token",
        "Bearer fresh-token",
    ]


def test_second_unauthorized_response_is_terminal_without_refresh_loop() -> None:
    session = FakeSession(
        [FakeResponse(401, text="expired"), FakeResponse(401, text="still expired")]
    )
    auth = FakeAuth(tokens=["expired-token"], refresh_tokens=["fresh-token"])
    sleep = FakeSleep()
    client = make_client(session, auth=auth, sleep=sleep)

    result = client.update_attrs("entity-1", "Blesensor.per300", sample_attrs())

    assert result["ok"] is False
    assert result["status"] == 401
    assert result["attempts"] == 2
    assert result["body_excerpt"] == "still expired"
    assert auth.calls == [False, True]
    assert sleep.delays == []
    assert len(session.calls) == 2


def test_server_errors_retry_with_exponential_backoff_until_exhausted() -> None:
    session = FakeSession([FakeResponse(503, text=f"boom-{i}") for i in range(6)])
    sleep = FakeSleep()
    client = make_client(session, sleep=sleep)

    result = client.update_attrs("entity-1", "Blesensor.per300", sample_attrs())

    assert result["ok"] is False
    assert result["status"] == 503
    assert result["attempts"] == 6
    assert result["body_excerpt"] == "boom-5"
    assert sleep.delays == [1, 2, 4, 8, 16]
    assert len(session.calls) == 6


def test_connection_errors_retry_with_same_budget_and_can_succeed() -> None:
    session = FakeSession(
        [requests.exceptions.ConnectionError("network down"), FakeResponse(204)]
    )
    sleep = FakeSleep()
    client = make_client(session, sleep=sleep)

    result = client.update_attrs("entity-1", "Blesensor.per300", sample_attrs())

    assert result["ok"] is True
    assert result["status"] == 204
    assert result["attempts"] == 2
    assert sleep.delays == [1]
    assert len(session.calls) == 2


def test_non_401_or_429_client_error_does_not_retry() -> None:
    session = FakeSession([FakeResponse(404, text="missing entity")])
    sleep = FakeSleep()
    client = make_client(session, sleep=sleep)

    result = client.update_attrs("missing", "Blesensor.per300", sample_attrs())

    assert result["ok"] is False
    assert result["status"] == 404
    assert result["attempts"] == 1
    assert result["body_excerpt"] == "missing entity"
    assert sleep.delays == []
    assert len(session.calls) == 1


def test_rate_limit_retry_after_header_controls_retry_sleep() -> None:
    session = FakeSession(
        [
            FakeResponse(429, text="slow down", headers={"Retry-After": "7"}),
            FakeResponse(204),
        ]
    )
    sleep = FakeSleep()
    client = make_client(session, sleep=sleep)

    result = client.update_attrs("entity-1", "Blesensor.per300", sample_attrs())

    assert result["ok"] is True
    assert result["attempts"] == 2
    assert sleep.delays == [7]


def test_rate_limit_without_retry_after_uses_standard_backoff() -> None:
    session = FakeSession([FakeResponse(429, text="slow down"), FakeResponse(204)])
    sleep = FakeSleep()
    client = make_client(session, sleep=sleep)

    result = client.update_attrs("entity-1", "Blesensor.per300", sample_attrs())

    assert result["ok"] is True
    assert result["attempts"] == 2
    assert sleep.delays == [1]


def test_success_logs_structured_post_succeeded_with_payload_hash(
    caplog: pytest.LogCaptureFixture,
) -> None:
    attrs = sample_attrs()
    session = FakeSession([FakeResponse(204)])
    clock = FakeClock([1000.0, 1000.123])
    client = make_client(session, now=clock)

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline.orion_client"):
        result = client.update_attrs("entity-1", "Blesensor.per300", attrs)

    assert result["elapsed_ms"] == 123
    records = post_records(caplog, "post_succeeded")
    assert len(records) == 1
    record = records[0]
    assert record.levelno == logging.INFO
    assert record.entity_id == "entity-1"
    assert record.http_status == 204
    assert record.ok is True
    assert record.attempts == 1
    assert record.elapsed_ms == 123
    assert record.payload_sha256 == hashlib.sha256(session.calls[0]["data"]).hexdigest()
    assert record.payload_bytes == len(session.calls[0]["data"])
    assert not hasattr(record, "payload")
    assert not hasattr(record, "response_excerpt")


def test_terminal_failure_logs_error_with_payload_and_response_excerpt(
    caplog: pytest.LogCaptureFixture,
) -> None:
    session = FakeSession([FakeResponse(400, text="bad request body")])
    client = make_client(session, response_max_bytes=8)

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline.orion_client"):
        result = client.update_attrs("entity-1", "Blesensor.per300", sample_attrs())

    records = post_records(caplog, "post_failed")
    assert len(records) == 1
    record = records[0]
    assert record.levelno == logging.ERROR
    assert record.http_status == 400
    assert record.ok is False
    assert record.payload == session.calls[0]["data"].decode("utf-8")
    assert record.response_excerpt.startswith("bad requ")
    assert "original 16 bytes" in record.response_excerpt

    expected_fields = payload_log_fields(
        session.calls[0]["data"],
        "bad request body",
        ok=False,
        mode="failure",
        payload_max_bytes=16384,
        response_max_bytes=8,
    )
    assert result["body_excerpt"] == expected_fields["response_excerpt"]


def test_retryable_failure_that_succeeds_logs_single_success(
    caplog: pytest.LogCaptureFixture,
) -> None:
    session = FakeSession([FakeResponse(502, text="temporary"), FakeResponse(204)])
    client = make_client(session, sleep=FakeSleep())

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline.orion_client"):
        result = client.update_attrs("entity-1", "Blesensor.per300", sample_attrs())

    assert result["ok"] is True
    records = post_records(caplog, "post_succeeded")
    assert len(records) == 1
    assert records[0].attempts == 2


def test_full_payload_mode_logs_payload_on_success(
    caplog: pytest.LogCaptureFixture,
) -> None:
    session = FakeSession([FakeResponse(204)])
    client = make_client(session, payload_mode="full")

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline.orion_client"):
        client.update_attrs("entity-1", "Blesensor.per300", sample_attrs())

    record = post_records(caplog, "post_succeeded")[0]
    assert record.payload == session.calls[0]["data"].decode("utf-8")
    assert record.payload_sha256 == hashlib.sha256(session.calls[0]["data"]).hexdigest()


def test_dry_run_logs_would_send_without_calling_session(
    caplog: pytest.LogCaptureFixture,
) -> None:
    session = FakeSession([FakeResponse(204)])
    client = make_client(session, payload_mode="full")

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline.orion_client"):
        result = client.update_attrs(
            "entity-1",
            "Blesensor.per300",
            sample_attrs(),
            dry_run=True,
        )

    assert result == {
        "status": 0,
        "ok": True,
        "attempts": 0,
        "elapsed_ms": 0,
        "body_excerpt": None,
        "dry_run": True,
    }
    assert session.calls == []
    records = post_records(caplog, "post_succeeded")
    assert len(records) == 1
    assert records[0].levelno == logging.INFO
    assert records[0].ok is True
    assert records[0].dry_run is True
    assert records[0].attempts == 0
    assert hasattr(records[0], "payload")
    assert "dry_run" in _ALLOWED_EXTRA_KEYS


def test_body_excerpt_is_capped_to_response_max_bytes() -> None:
    session = FakeSession([FakeResponse(500, text="x" * 50) for _ in range(6)])
    client = make_client(session, response_max_bytes=10)

    result = client.update_attrs("entity-1", "Blesensor.per300", sample_attrs())

    assert result["ok"] is False
    assert result["body_excerpt"].startswith("x" * 10)
    assert "original 50 bytes" in result["body_excerpt"]


def test_verify_tls_false_is_passed_to_update_and_list_requests() -> None:
    settings = make_settings(verify_tls=False)
    post_session = FakeSession([FakeResponse(204)])
    make_client(post_session, settings=settings).update_attrs(
        "entity-1",
        "Blesensor.per300",
        sample_attrs(),
    )
    get_session = FakeSession([FakeResponse(200, payload=[])])
    make_client(get_session, settings=settings).list_entities("Blesensor.per300")

    assert post_session.calls[0]["verify"] is False
    assert get_session.calls[0]["verify"] is False


def test_hash_payload_mode_omits_body_and_excerpt_on_failure(
    caplog: pytest.LogCaptureFixture,
) -> None:
    session = FakeSession([FakeResponse(400, text="bad request body")])
    client = make_client(session, payload_mode="hash")

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline.orion_client"):
        result = client.update_attrs("entity-1", "Blesensor.per300", sample_attrs())

    record = post_records(caplog, "post_failed")[0]
    assert record.payload_sha256 == hashlib.sha256(session.calls[0]["data"]).hexdigest()
    assert record.payload_bytes == len(session.calls[0]["data"])
    assert not hasattr(record, "payload")
    assert not hasattr(record, "response_excerpt")
    # body_excerpt mirrors logging policy: hash mode keeps logs compact and
    # therefore returns no excerpt either.
    assert result["body_excerpt"] is None


def test_read_timeout_is_retried_like_connection_error() -> None:
    session = FakeSession(
        [requests.exceptions.ReadTimeout("read timed out"), FakeResponse(204)]
    )
    sleep = FakeSleep()
    client = make_client(session, sleep=sleep)

    result = client.update_attrs("entity-1", "Blesensor.per300", sample_attrs())

    assert result["ok"] is True
    assert result["status"] == 204
    assert result["attempts"] == 2
    assert sleep.delays == [1]


def test_connection_errors_exhaust_retry_budget() -> None:
    session = FakeSession(
        [requests.exceptions.ConnectionError(f"down-{i}") for i in range(6)]
    )
    sleep = FakeSleep()
    client = make_client(session, sleep=sleep)

    result = client.update_attrs("entity-1", "Blesensor.per300", sample_attrs())

    assert result["ok"] is False
    assert result["status"] == 0
    assert result["attempts"] == 6
    assert sleep.delays == [1, 2, 4, 8, 16]
    assert "down-5" in (result["body_excerpt"] or "")


def test_non_finite_retry_after_falls_back_to_standard_backoff() -> None:
    session = FakeSession(
        [
            FakeResponse(429, text="slow down", headers={"Retry-After": "inf"}),
            FakeResponse(204),
        ]
    )
    sleep = FakeSleep()
    client = make_client(session, sleep=sleep)

    result = client.update_attrs("entity-1", "Blesensor.per300", sample_attrs())

    assert result["ok"] is True
    # `inf` is rejected by _parse_retry_after, so we fall back to the
    # first slot of the standard backoff sequence — same path as 429
    # without a Retry-After header.
    assert sleep.delays == [1]


def test_list_entities_sends_fiware_service_header() -> None:
    session = FakeSession([FakeResponse(200, payload=[])])
    client = make_client(session, settings=make_settings(service="sendai"))

    client.list_entities("Blesensor.per300")

    call = session.calls[0]
    assert call["headers"]["Fiware-Service"] == "sendai"
    assert call["headers"]["Fiware-ServicePath"] == "/"


def test_replace_attrs_puts_canonical_body_to_encoded_entity_attrs_endpoint(
    caplog: pytest.LogCaptureFixture,
) -> None:
    attrs = sample_aggregate_attrs()
    session = FakeSession([FakeResponse(204)])
    auth = FakeAuth(tokens=["request-token"])
    client = make_client(session, auth=auth)
    entity_id = "jp.sendai.Blesensor.flow/aggregate?slot=one two"
    entity_type = "Blesensor.flow/v2 & beta"

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline.orion_client"):
        result = client.replace_attrs(entity_id, entity_type, attrs)

    assert result == {
        "status": 204,
        "ok": True,
        "attempts": 1,
        "elapsed_ms": 0,
        "body_excerpt": None,
        "dry_run": False,
    }
    assert len(session.calls) == 1
    call = session.calls[0]
    assert call["method"] == "PUT"
    assert call["url"] == (
        "https://fiware.example.test/orion/v2.0/entities/"
        "jp.sendai.Blesensor.flow%2Faggregate%3Fslot%3Done%20two/attrs"
    )
    assert call["params"] == {"type": entity_type}
    prepared_url = (
        requests.Request(
            "PUT",
            call["url"],
            params=call["params"],
        )
        .prepare()
        .url
    )
    assert prepared_url == (
        "https://fiware.example.test/orion/v2.0/entities/"
        "jp.sendai.Blesensor.flow%2Faggregate%3Fslot%3Done%20two/attrs"
        "?type=Blesensor.flow%2Fv2+%26+beta"
    )
    assert entity_id not in call["url"]
    assert entity_type not in call["url"]
    assert call["headers"] == {
        "Authorization": "Bearer request-token",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Fiware-Service": "sendai",
        "Fiware-ServicePath": "/",
    }
    assert call["data"] == canonical_json_bytes(attrs)
    assert json.loads(call["data"]) == attrs
    assert "json" not in call
    assert call["timeout"] == 3.5
    assert call["verify"] is True
    assert auth.calls == [False]
    records = post_records(caplog, "put_succeeded")
    assert len(records) == 1
    assert records[0].levelno == logging.INFO
    assert records[0].entity_id == entity_id
    assert records[0].http_status == 204
    assert records[0].attempts == 1
    assert post_records(caplog, "post_succeeded") == []


def test_replace_attrs_treats_non_204_2xx_as_success(
    caplog: pytest.LogCaptureFixture,
) -> None:
    attrs = sample_aggregate_attrs()
    session = FakeSession([FakeResponse(200)])
    client = make_client(session, auth=FakeAuth(tokens=["request-token"]))

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline.orion_client"):
        result = client.replace_attrs(
            "jp.sendai.Blesensor.flow", "Blesensor.flow", attrs
        )

    assert result["ok"] is True
    assert result["status"] == 200
    assert result["attempts"] == 1
    assert len(session.calls) == 1
    records = post_records(caplog, "put_succeeded")
    assert len(records) == 1
    assert records[0].levelno == logging.INFO
    assert records[0].http_status == 200


def test_replace_attrs_dry_run_skips_network_and_logs_full_payload(
    caplog: pytest.LogCaptureFixture,
) -> None:
    attrs = sample_aggregate_attrs()
    session = FakeSession([FakeResponse(204)])
    auth = FakeAuth()
    client = make_client(session, auth=auth, payload_mode="hash")

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline.orion_client"):
        result = client.replace_attrs(
            "jp.sendai.Blesensor.flow",
            "Blesensor.flow",
            attrs,
            dry_run=True,
        )

    assert result == {
        "status": 0,
        "ok": True,
        "attempts": 0,
        "elapsed_ms": 0,
        "body_excerpt": None,
        "dry_run": True,
    }
    assert session.calls == []
    assert auth.calls == []
    records = post_records(caplog, "put_succeeded")
    assert len(records) == 1
    record = records[0]
    assert record.levelno == logging.INFO
    assert record.ok is True
    assert record.dry_run is True
    assert record.attempts == 0
    assert record.payload == canonical_json_bytes(attrs).decode("utf-8")
    assert (
        record.payload_sha256 == hashlib.sha256(canonical_json_bytes(attrs)).hexdigest()
    )
    assert post_records(caplog, "post_succeeded") == []


def test_replace_attrs_timeout_retries_and_can_succeed() -> None:
    session = FakeSession(
        [requests.exceptions.ReadTimeout("read timed out"), FakeResponse(204)]
    )
    sleep = FakeSleep()
    client = make_client(session, sleep=sleep)

    result = client.replace_attrs(
        "jp.sendai.Blesensor.flow",
        "Blesensor.flow",
        sample_aggregate_attrs(),
    )

    assert result["ok"] is True
    assert result["status"] == 204
    assert result["attempts"] == 2
    assert sleep.delays == [1]
    assert [call["method"] for call in session.calls] == ["PUT", "PUT"]


def test_replace_attrs_connection_error_retries_and_can_succeed() -> None:
    session = FakeSession(
        [requests.exceptions.ConnectionError("network down"), FakeResponse(204)]
    )
    sleep = FakeSleep()
    client = make_client(session, sleep=sleep)

    result = client.replace_attrs(
        "jp.sendai.Blesensor.flow",
        "Blesensor.flow",
        sample_aggregate_attrs(),
    )

    assert result["ok"] is True
    assert result["status"] == 204
    assert result["attempts"] == 2
    assert sleep.delays == [1]
    assert len(session.calls) == 2


def test_replace_attrs_transient_errors_retry_with_exponential_backoff() -> None:
    session = FakeSession(
        [
            FakeResponse(503, text="unavailable"),
            FakeResponse(502, text="bad gateway"),
            FakeResponse(204),
        ]
    )
    sleep = FakeSleep()
    client = make_client(
        session,
        settings=make_settings(max_retries=2),
        sleep=sleep,
    )

    result = client.replace_attrs(
        "jp.sendai.Blesensor.flow",
        "Blesensor.flow",
        sample_aggregate_attrs(),
    )

    assert result["ok"] is True
    assert result["status"] == 204
    assert result["attempts"] == 3
    assert sleep.delays == [1, 2]
    assert len(session.calls) == 3


def test_replace_attrs_rate_limit_honors_retry_after() -> None:
    session = FakeSession(
        [
            FakeResponse(429, text="slow down", headers={"Retry-After": "7"}),
            FakeResponse(204),
        ]
    )
    sleep = FakeSleep()
    client = make_client(session, sleep=sleep)

    result = client.replace_attrs(
        "jp.sendai.Blesensor.flow",
        "Blesensor.flow",
        sample_aggregate_attrs(),
    )

    assert result["ok"] is True
    assert result["attempts"] == 2
    assert sleep.delays == [7]


def test_replace_attrs_unauthorized_refreshes_once_then_succeeds() -> None:
    session = FakeSession([FakeResponse(401, text="expired"), FakeResponse(204)])
    auth = FakeAuth(tokens=["expired-token"], refresh_tokens=["fresh-token"])
    sleep = FakeSleep()
    client = make_client(session, auth=auth, sleep=sleep)

    result = client.replace_attrs(
        "jp.sendai.Blesensor.flow",
        "Blesensor.flow",
        sample_aggregate_attrs(),
    )

    assert result["ok"] is True
    assert result["status"] == 204
    assert result["attempts"] == 2
    assert auth.calls == [False, True]
    assert sleep.delays == []
    assert [call["headers"]["Authorization"] for call in session.calls] == [
        "Bearer expired-token",
        "Bearer fresh-token",
    ]


def test_replace_attrs_second_unauthorized_is_terminal() -> None:
    session = FakeSession(
        [FakeResponse(401, text="expired"), FakeResponse(401, text="still expired")]
    )
    auth = FakeAuth(tokens=["expired-token"], refresh_tokens=["fresh-token"])
    sleep = FakeSleep()
    client = make_client(session, auth=auth, sleep=sleep)

    result = client.replace_attrs(
        "jp.sendai.Blesensor.flow",
        "Blesensor.flow",
        sample_aggregate_attrs(),
    )

    assert result["ok"] is False
    assert result["status"] == 401
    assert result["attempts"] == 2
    assert result["body_excerpt"] == "still expired"
    assert auth.calls == [False, True]
    assert sleep.delays == []
    assert len(session.calls) == 2


def test_replace_attrs_terminal_client_error_does_not_retry() -> None:
    session = FakeSession([FakeResponse(400, text="invalid aggregate")])
    sleep = FakeSleep()
    client = make_client(session, sleep=sleep)

    result = client.replace_attrs(
        "jp.sendai.Blesensor.flow",
        "Blesensor.flow",
        sample_aggregate_attrs(),
    )

    assert result["ok"] is False
    assert result["status"] == 400
    assert result["attempts"] == 1
    assert result["body_excerpt"] == "invalid aggregate"
    assert sleep.delays == []
    assert len(session.calls) == 1


def test_replace_attrs_exhausts_retry_budget_and_logs_failure(
    caplog: pytest.LogCaptureFixture,
) -> None:
    session = FakeSession(
        [FakeResponse(503, text=f"boom-{index}") for index in range(3)]
    )
    sleep = FakeSleep()
    client = make_client(
        session,
        settings=make_settings(max_retries=2),
        sleep=sleep,
    )

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline.orion_client"):
        result = client.replace_attrs(
            "jp.sendai.Blesensor.flow",
            "Blesensor.flow",
            sample_aggregate_attrs(),
        )

    assert result == {
        "status": 503,
        "ok": False,
        "attempts": 3,
        "elapsed_ms": 0,
        "body_excerpt": "boom-2",
        "dry_run": False,
    }
    assert sleep.delays == [1, 2]
    assert len(session.calls) == 3
    records = post_records(caplog, "put_failed")
    assert len(records) == 1
    assert records[0].levelno == logging.ERROR
    assert records[0].http_status == 503
    assert records[0].attempts == 3
    assert records[0].payload == canonical_json_bytes(sample_aggregate_attrs()).decode(
        "utf-8"
    )
    assert records[0].response_excerpt == "boom-2"
    assert post_records(caplog, "post_failed") == []


def test_delete_attr_deletes_encoded_path_segments_with_type_and_no_body(
    caplog: pytest.LogCaptureFixture,
) -> None:
    session = FakeSession([FakeResponse(204)])
    auth = FakeAuth(tokens=["request-token"])
    client = make_client(session, auth=auth)
    entity_id = "jp.sendai.Blesensor.flow/aggregate?slot=one two"
    entity_type = "Blesensor.flow/v2 & beta"
    attr_name = "peopleCount_flow/7?source=north gate"

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline.orion_client"):
        result = client.delete_attr(entity_id, entity_type, attr_name)

    assert result == {
        "status": 204,
        "ok": True,
        "attempts": 1,
        "elapsed_ms": 0,
        "body_excerpt": None,
        "dry_run": False,
    }
    assert len(session.calls) == 1
    call = session.calls[0]
    assert call["method"] == "DELETE"
    assert call["url"] == (
        "https://fiware.example.test/orion/v2.0/entities/"
        "jp.sendai.Blesensor.flow%2Faggregate%3Fslot%3Done%20two/attrs/"
        "peopleCount_flow%2F7%3Fsource%3Dnorth%20gate"
    )
    assert call["params"] == {"type": entity_type}
    prepared_url = (
        requests.Request(
            "DELETE",
            call["url"],
            params=call["params"],
        )
        .prepare()
        .url
    )
    assert prepared_url == (
        "https://fiware.example.test/orion/v2.0/entities/"
        "jp.sendai.Blesensor.flow%2Faggregate%3Fslot%3Done%20two/attrs/"
        "peopleCount_flow%2F7%3Fsource%3Dnorth%20gate"
        "?type=Blesensor.flow%2Fv2+%26+beta"
    )
    assert "data" not in call
    assert "json" not in call
    assert call["headers"]["Authorization"] == "Bearer request-token"
    assert "Content-Type" not in call["headers"]
    assert auth.calls == [False]
    records = post_records(caplog, "delete_succeeded")
    assert len(records) == 1
    assert records[0].levelno == logging.INFO
    assert records[0].entity_id == entity_id
    assert records[0].http_status == 204
    assert records[0].attempts == 1


def test_delete_attr_unauthorized_refreshes_once_then_succeeds() -> None:
    session = FakeSession([FakeResponse(401, text="expired"), FakeResponse(204)])
    auth = FakeAuth(tokens=["expired-token"], refresh_tokens=["fresh-token"])
    sleep = FakeSleep()
    client = make_client(session, auth=auth, sleep=sleep)

    result = client.delete_attr(
        "jp.sendai.Blesensor.flow",
        "Blesensor.flow",
        "peopleCount_flow_7",
    )

    assert result["ok"] is True
    assert result["status"] == 204
    assert result["attempts"] == 2
    assert auth.calls == [False, True]
    assert sleep.delays == []
    assert [call["headers"]["Authorization"] for call in session.calls] == [
        "Bearer expired-token",
        "Bearer fresh-token",
    ]


def test_delete_attr_second_unauthorized_is_terminal() -> None:
    session = FakeSession(
        [FakeResponse(401, text="expired"), FakeResponse(401, text="still expired")]
    )
    auth = FakeAuth(tokens=["expired-token"], refresh_tokens=["fresh-token"])
    sleep = FakeSleep()
    client = make_client(session, auth=auth, sleep=sleep)

    result = client.delete_attr(
        "jp.sendai.Blesensor.flow",
        "Blesensor.flow",
        "peopleCount_flow_7",
    )

    assert result["ok"] is False
    assert result["status"] == 401
    assert result["attempts"] == 2
    assert result["body_excerpt"] == "still expired"
    assert auth.calls == [False, True]
    assert sleep.delays == []
    assert len(session.calls) == 2


def test_delete_attr_already_absent_is_success(
    caplog: pytest.LogCaptureFixture,
) -> None:
    session = FakeSession([FakeResponse(404, text="not found")])
    client = make_client(session)

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline.orion_client"):
        result = client.delete_attr(
            "jp.sendai.Blesensor.flow",
            "Blesensor.flow",
            "peopleCount_flow_7",
        )

    assert result == {
        "status": 404,
        "ok": True,
        "attempts": 1,
        "elapsed_ms": 0,
        "body_excerpt": None,
        "dry_run": False,
    }
    records = post_records(caplog, "delete_succeeded")
    assert len(records) == 1
    assert records[0].http_status == 404


@pytest.mark.parametrize("status", [200, 400, 500])
def test_delete_attr_unexpected_status_is_terminal_failure(
    status: int,
    caplog: pytest.LogCaptureFixture,
) -> None:
    session = FakeSession([FakeResponse(status, text="unexpected delete response")])
    sleep = FakeSleep()
    client = make_client(session, sleep=sleep)

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline.orion_client"):
        result = client.delete_attr(
            "jp.sendai.Blesensor.flow",
            "Blesensor.flow",
            "peopleCount_flow_7",
        )

    assert result == {
        "status": status,
        "ok": False,
        "attempts": 1,
        "elapsed_ms": 0,
        "body_excerpt": "unexpected delete response",
        "dry_run": False,
    }
    assert sleep.delays == []
    assert len(session.calls) == 1
    records = post_records(caplog, "delete_failed")
    assert len(records) == 1
    assert records[0].levelno == logging.ERROR
    assert records[0].http_status == status


@pytest.mark.parametrize(
    "exc",
    [
        requests.exceptions.ConnectionError("network down"),
        requests.exceptions.ReadTimeout("read timed out"),
    ],
)
def test_delete_attr_connection_error_is_caught_as_single_shot_failure(
    exc: Exception,
    caplog: pytest.LogCaptureFixture,
) -> None:
    session = FakeSession([exc])
    sleep = FakeSleep()
    client = make_client(session, sleep=sleep)

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline.orion_client"):
        result = client.delete_attr(
            "jp.sendai.Blesensor.flow",
            "Blesensor.flow",
            "peopleCount_flow_7",
        )

    assert result["ok"] is False
    assert result["status"] == 0
    assert result["attempts"] == 1
    assert result["dry_run"] is False
    assert sleep.delays == []
    assert len(session.calls) == 1
    assert str(exc) in (result["body_excerpt"] or "")
    records = post_records(caplog, "delete_failed")
    assert len(records) == 1
    assert records[0].levelno == logging.ERROR


def test_update_attrs_product_a_remains_post() -> None:
    session = FakeSession([FakeResponse(204)])
    client = make_client(session)

    client.update_attrs(
        "jp.sendai.Blesensor.per3600.10",
        "Blesensor.per3600",
        sample_attrs(),
    )

    assert len(session.calls) == 1
    assert session.calls[0]["method"] == "POST"
    assert session.calls[0]["url"] == (
        "https://fiware.example.test/orion/v2.0/entities/"
        "jp.sendai.Blesensor.per3600.10/attrs?type=Blesensor.per3600"
    )
    assert "params" not in session.calls[0]
