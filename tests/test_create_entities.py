"""Tests for sendai_pipeline.create_entities."""

import json
import logging
from typing import Any

import pytest
import requests

from sendai_pipeline.create_entities import (
    CreateEntitiesError,
    CreateEntitiesSettings,
    EntitySpec,
    create_entities,
    parse_entity_specs,
)


class FakeResponse:
    def __init__(self, status_code: int, text: str = "") -> None:
        self.status_code = status_code
        self.text = text

    def json(self) -> Any:
        return json.loads(self.text) if self.text else {}


class FakeSession:
    def __init__(self, responses: list[FakeResponse] | None = None) -> None:
        self._responses = list(responses or [])
        self.calls: list[dict[str, Any]] = []

    def post(self, url: str, **kwargs: Any) -> FakeResponse:
        self.calls.append({"method": "POST", "url": url, **kwargs})
        if not self._responses:
            raise AssertionError("FakeSession.post called more times than expected")
        return self._responses.pop(0)


class FakeAuth:
    def get_token(self, *, force_refresh: bool = False) -> str:
        return "test-token"


class FakeFailingSession:
    def __init__(self, exc: Exception) -> None:
        self.exc = exc
        self.calls: list[dict[str, Any]] = []

    def post(self, url: str, **kwargs: Any) -> FakeResponse:
        self.calls.append({"method": "POST", "url": url, **kwargs})
        raise self.exc


def _spec(n: int, entity_type: str = "Blesensor.per3600") -> EntitySpec:
    return EntitySpec(
        entity_id=f"jp.sendai.{entity_type}.{n}",
        entity_type=entity_type,
    )


def _settings(**overrides: Any) -> CreateEntitiesSettings:
    defaults: dict[str, Any] = {
        "base_url": "https://fiware.example.test",
        "service": "",
        "service_path": "/",
        "verify_tls": True,
        "timeout": 5.0,
        "entities": (_spec(101),),
        "dry_run": False,
    }
    defaults.update(overrides)
    return CreateEntitiesSettings(**defaults)


def test_settings_from_env_defaults() -> None:
    env: dict[str, str] = {
        "FIWARE_BASE_URL": "https://host.example",
    }
    s = CreateEntitiesSettings.from_env(env)
    assert s.base_url == "https://host.example"
    assert s.dry_run is True
    assert s.service_path == "/"
    assert s.timeout == 10.0
    assert s.entities == ()


def test_settings_from_env_explicit_values() -> None:
    env: dict[str, str] = {
        "FIWARE_BASE_URL": "https://host.example",
        "FIWARE_SERVICE": "svc",
        "FIWARE_SERVICE_PATH": "/sensors",
        "FIWARE_VERIFY_TLS": "false",
        "FIWARE_TIMEOUT_SECONDS": "20",
    }
    s = CreateEntitiesSettings.from_env(env)
    assert s.service == "svc"
    assert s.service_path == "/sensors"
    assert s.verify_tls is False
    assert s.timeout == 20.0
    assert s.entities == ()
    assert s.dry_run is True


def test_settings_from_env_missing_base_url_raises() -> None:
    with pytest.raises(CreateEntitiesError, match="FIWARE_BASE_URL"):
        CreateEntitiesSettings.from_env({})


def test_settings_from_env_bad_timeout_raises() -> None:
    with pytest.raises(CreateEntitiesError, match="FIWARE_TIMEOUT_SECONDS"):
        CreateEntitiesSettings.from_env(
            {
                "FIWARE_BASE_URL": "https://host.example",
                "FIWARE_TIMEOUT_SECONDS": "not-a-number",
            }
        )


def test_parse_entity_specs_accepts_multiple_arguments() -> None:
    specs = parse_entity_specs(
        [
            "jp.sendai.Blesensor.per3600.101:Blesensor.per3600",
            "jp.sendai.Blesensor.per300.101:Blesensor.per300",
        ]
    )

    assert specs == (
        EntitySpec("jp.sendai.Blesensor.per3600.101", "Blesensor.per3600"),
        EntitySpec("jp.sendai.Blesensor.per300.101", "Blesensor.per300"),
    )


def test_parse_entity_specs_accepts_comma_separated_argument() -> None:
    specs = parse_entity_specs(
        [
            "jp.sendai.Blesensor.per3600.101:Blesensor.per3600,"
            "jp.sendai.Blesensor.per300.101:Blesensor.per300"
        ]
    )

    assert specs == (
        EntitySpec("jp.sendai.Blesensor.per3600.101", "Blesensor.per3600"),
        EntitySpec("jp.sendai.Blesensor.per300.101", "Blesensor.per300"),
    )


def test_parse_entity_specs_strips_whitespace() -> None:
    specs = parse_entity_specs([" jp.sendai.X.1 : TypeA , jp.sendai.X.2 : TypeB "])

    assert specs == (
        EntitySpec("jp.sendai.X.1", "TypeA"),
        EntitySpec("jp.sendai.X.2", "TypeB"),
    )


def test_parse_entity_specs_malformed_token_raises() -> None:
    with pytest.raises(CreateEntitiesError, match="id:type"):
        parse_entity_specs(["no-colon-here"])


def test_parse_entity_specs_empty_input_raises() -> None:
    with pytest.raises(CreateEntitiesError, match="at least one"):
        parse_entity_specs(["", ","])


def test_dry_run_no_network_calls() -> None:
    session = FakeSession()
    settings = _settings(dry_run=True, entities=(_spec(101), _spec(102)))

    result = create_entities(
        settings.entities, settings=settings, auth=None, session=session
    )

    assert session.calls == []
    assert result.would_create == 2
    assert result.created == 0
    assert result.skipped == 0
    assert result.failed == 0


def test_dry_run_exit_code_zero() -> None:
    settings = _settings(dry_run=True)
    result = create_entities(
        settings.entities, settings=settings, auth=None, session=FakeSession()
    )
    assert result.exit_code == 0


def test_dry_run_logs_summary_with_safe_extra_keys(
    caplog: pytest.LogCaptureFixture,
) -> None:
    settings = _settings(dry_run=True)

    with caplog.at_level(logging.INFO, logger="sendai_pipeline.create_entities"):
        result = create_entities(
            settings.entities,
            settings=settings,
            auth=None,
            session=FakeSession(),
        )

    summary_records = [
        record
        for record in caplog.records
        if getattr(record, "event", None) == "create_entities_summary"
    ]
    assert result.would_create == 1
    assert len(summary_records) == 1
    assert getattr(summary_records[0], "count_would_create", None) == 1
    assert getattr(summary_records[0], "count_created", None) == 0


def test_live_mode_missing_auth_raises_create_entities_error() -> None:
    settings = _settings(dry_run=False)

    with pytest.raises(CreateEntitiesError, match="auth is required"):
        create_entities(settings.entities, settings=settings, auth=None)


def test_post_correct_url_and_headers() -> None:
    session = FakeSession([FakeResponse(201)])
    settings = _settings(
        base_url="https://fiware.example.test",
        service="mysvc",
        service_path="/root",
    )

    create_entities(
        settings.entities, settings=settings, auth=FakeAuth(), session=session
    )

    assert len(session.calls) == 1
    call = session.calls[0]
    assert call["url"] == "https://fiware.example.test/orion/v2.0/entities"
    headers = call["headers"]
    assert headers["Authorization"] == "Bearer test-token"
    assert headers["Content-Type"] == "application/json"
    assert headers["Fiware-Service"] == "mysvc"
    assert headers["Fiware-ServicePath"] == "/root"


def test_post_no_fiware_service_header_when_empty() -> None:
    session = FakeSession([FakeResponse(201)])
    settings = _settings(service="")

    create_entities(
        settings.entities, settings=settings, auth=FakeAuth(), session=session
    )

    assert "Fiware-Service" not in session.calls[0]["headers"]


def test_entity_body_has_id_type_and_all_attributes() -> None:
    session = FakeSession([FakeResponse(201)])
    settings = _settings(
        entities=(EntitySpec("jp.sendai.Blesensor.per3600.101", "Blesensor.per3600"),)
    )

    create_entities(
        settings.entities, settings=settings, auth=FakeAuth(), session=session
    )

    body = json.loads(session.calls[0]["data"])
    assert body["id"] == "jp.sendai.Blesensor.per3600.101"
    assert body["type"] == "Blesensor.per3600"
    expected_attrs = {
        "dateObservedFrom",
        "dateObservedTo",
        "peopleCount_immedate",
        "peopleCount_near",
        "peopleCount_far",
        "peopleOccupancy_immedate",
        "peopleOccupancy_near",
    }
    assert expected_attrs.issubset(body.keys())


def test_entity_attribute_types_correct() -> None:
    session = FakeSession([FakeResponse(201)])
    settings = _settings()

    create_entities(
        settings.entities,
        settings=settings,
        auth=FakeAuth(),
        session=session,
    )

    body = json.loads(session.calls[0]["data"])
    assert body["dateObservedFrom"]["type"] == "DateTime"
    assert body["dateObservedTo"]["type"] == "DateTime"
    assert body["peopleCount_immedate"]["type"] == "number"
    assert body["peopleCount_near"]["type"] == "number"
    assert body["peopleCount_far"]["type"] == "number"
    assert body["peopleOccupancy_immedate"]["type"] == "number"
    assert body["peopleOccupancy_near"]["type"] == "number"


def test_entity_attribute_values_are_null() -> None:
    session = FakeSession([FakeResponse(201)])

    create_entities(
        _settings().entities, settings=_settings(), auth=FakeAuth(), session=session
    )

    body = json.loads(session.calls[0]["data"])
    for attr in (
        "dateObservedFrom",
        "dateObservedTo",
        "peopleCount_immedate",
        "peopleCount_near",
        "peopleCount_far",
        "peopleOccupancy_immedate",
        "peopleOccupancy_near",
    ):
        assert body[attr]["value"] is None, f"{attr} value should be null"


def test_201_counted_as_created() -> None:
    session = FakeSession([FakeResponse(201), FakeResponse(201)])
    settings = _settings(entities=(_spec(101), _spec(102)))

    result = create_entities(
        settings.entities, settings=settings, auth=FakeAuth(), session=session
    )

    assert result.created == 2
    assert result.skipped == 0
    assert result.failed == 0


def test_422_counted_as_skipped() -> None:
    session = FakeSession([FakeResponse(422, '{"description": "Already Exists"}')])

    result = create_entities(
        _settings().entities, settings=_settings(), auth=FakeAuth(), session=session
    )

    assert result.created == 0
    assert result.skipped == 1
    assert result.failed == 0


def test_409_counted_as_skipped() -> None:
    session = FakeSession([FakeResponse(409, '{"description": "Already Exists"}')])

    result = create_entities(
        _settings().entities, settings=_settings(), auth=FakeAuth(), session=session
    )

    assert result.skipped == 1
    assert result.failed == 0


def test_403_counted_as_failed() -> None:
    session = FakeSession([FakeResponse(403, "Forbidden")])

    result = create_entities(
        _settings().entities, settings=_settings(), auth=FakeAuth(), session=session
    )

    assert result.failed == 1
    assert result.created == 0


def test_500_counted_as_failed() -> None:
    session = FakeSession([FakeResponse(500, "Internal Server Error")])

    result = create_entities(
        _settings().entities, settings=_settings(), auth=FakeAuth(), session=session
    )

    assert result.failed == 1


def test_connection_error_logs_exception_and_counts_failed(
    caplog: pytest.LogCaptureFixture,
) -> None:
    settings = _settings()
    session = FakeFailingSession(requests.exceptions.Timeout("timed out"))

    with caplog.at_level(logging.ERROR, logger="sendai_pipeline.create_entities"):
        result = create_entities(
            settings.entities,
            settings=settings,
            auth=FakeAuth(),
            session=session,
        )

    failure_records = [
        record
        for record in caplog.records
        if getattr(record, "event", None) == "create_entity_post_failed"
    ]
    assert result.failed == 1
    assert len(session.calls) == 1
    assert len(failure_records) == 1
    assert failure_records[0].exc_info is not None
    assert getattr(failure_records[0], "error_type", None) == "Timeout"


def test_mixed_outcomes_counted_correctly() -> None:
    settings = _settings(entities=(_spec(101), _spec(102), _spec(103)))
    session = FakeSession(
        [
            FakeResponse(201),
            FakeResponse(422, "Already Exists"),
            FakeResponse(500, "err"),
        ]
    )

    result = create_entities(
        settings.entities, settings=settings, auth=FakeAuth(), session=session
    )

    assert result.created == 1
    assert result.skipped == 1
    assert result.failed == 1


def test_exit_code_zero_when_no_failures() -> None:
    session = FakeSession([FakeResponse(201)])

    result = create_entities(
        _settings().entities, settings=_settings(), auth=FakeAuth(), session=session
    )

    assert result.exit_code == 0


def test_exit_code_one_when_any_failure() -> None:
    session = FakeSession([FakeResponse(500, "err")])

    result = create_entities(
        _settings().entities, settings=_settings(), auth=FakeAuth(), session=session
    )

    assert result.exit_code == 1
