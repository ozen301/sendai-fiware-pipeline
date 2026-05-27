import logging
from typing import Any

import pytest

from sendai_pipeline.entity_map import EntityMapResult, validate_targets
from sendai_pipeline.logging_setup import _ALLOWED_EXTRA_KEYS
from sendai_pipeline.metadata import SensorPlace
from sendai_pipeline.orion_client import OrionClient, OrionSettings


class FakeResponse:
    def __init__(self, payload: Any) -> None:
        self.status_code = 200
        self.headers: dict[str, str] = {}
        self.text = ""
        self._payload = payload

    def json(self) -> Any:
        return self._payload


class FakeSession:
    def __init__(self, payloads_by_type: dict[str, Any]) -> None:
        self.payloads_by_type = dict(payloads_by_type)
        self.calls: list[dict[str, Any]] = []

    def get(self, url: str, **kwargs: Any) -> FakeResponse:
        self.calls.append({"method": "GET", "url": url, **kwargs})
        entity_type = kwargs["params"]["type"]
        if entity_type not in self.payloads_by_type:
            raise AssertionError(f"unexpected GET for type {entity_type!r}")
        return FakeResponse(self.payloads_by_type[entity_type])


class FakeAuth:
    def get_token(self, *, force_refresh: bool = False) -> str:
        return "token"


def make_settings(**overrides: Any) -> OrionSettings:
    values: dict[str, Any] = {
        "base_url": "https://fiware.example.test",
        "service": "",
        "service_path": "/",
        "verify_tls": True,
        "timeout": 3.5,
        "max_retries": 5,
    }
    values.update(overrides)
    return OrionSettings(**values)


def make_client(session: FakeSession) -> OrionClient:
    return OrionClient(
        make_settings(),
        auth=FakeAuth(),
        session=session,
        sleep=lambda _delay: None,
        now=lambda: 1000.0,
    )


def place(
    *,
    place_number: int,
    interval_min: int,
    entity_type: str,
    entity_id: str,
    batch: str = "2026",
    expected_device_type: str = "M5Stack",
    identifcation: str = "",
    active: bool = True,
) -> SensorPlace:
    return SensorPlace(
        place_number=place_number,
        batch=batch,
        expected_device_type=expected_device_type,
        interval_min=interval_min,
        entity_type=entity_type,
        entity_id=entity_id or f"jp.sendai.{entity_type}.{place_number}",
        identifcation=identifcation or str(place_number),
        active=active,
    )


def records(caplog: pytest.LogCaptureFixture, event: str) -> list[Any]:
    return [
        record for record in caplog.records if getattr(record, "event", None) == event
    ]


def test_validate_targets_finds_no_mismatch_when_metadata_matches_live() -> None:
    places = [
        place(
            place_number=10,
            interval_min=60,
            entity_type="Blesensor.per3600",
            entity_id="jp.sendai.Blesensor.per3600.10",
        ),
        place(
            place_number=105,
            interval_min=60,
            entity_type="Blesensor.per3600",
            entity_id="jp.sendai.Blesensor.per3600.105",
        ),
        place(
            place_number=10,
            interval_min=5,
            entity_type="Blesensor.per300",
            entity_id="jp.sendai.Blesensor.per300.10",
        ),
    ]
    session = FakeSession(
        {
            "Blesensor.per3600": [
                {"id": "jp.sendai.Blesensor.per3600.10"},
                {"id": "jp.sendai.Blesensor.per3600.105"},
            ],
            "Blesensor.per300": [{"id": "jp.sendai.Blesensor.per300.10"}],
        }
    )

    result = validate_targets(places, make_client(session))

    assert isinstance(result, EntityMapResult)
    assert result.expected_by_type == {
        "Blesensor.per3600": frozenset(
            {
                "jp.sendai.Blesensor.per3600.10",
                "jp.sendai.Blesensor.per3600.105",
            }
        ),
        "Blesensor.per300": frozenset({"jp.sendai.Blesensor.per300.10"}),
    }
    assert result.live_by_type == result.expected_by_type
    assert result.missing_by_type == {
        "Blesensor.per3600": frozenset(),
        "Blesensor.per300": frozenset(),
    }
    assert result.extra_by_type == {
        "Blesensor.per3600": frozenset(),
        "Blesensor.per300": frozenset(),
    }
    assert result.truncated_types == frozenset()
    assert result.has_missing is False


def test_validate_targets_issues_one_get_per_entity_type_with_attrs_id() -> None:
    places = [
        place(
            place_number=10,
            interval_min=60,
            entity_type="Blesensor.per3600",
            entity_id="jp.sendai.Blesensor.per3600.10",
        ),
        place(
            place_number=10,
            interval_min=5,
            entity_type="Blesensor.per300",
            entity_id="jp.sendai.Blesensor.per300.10",
        ),
    ]
    session = FakeSession(
        {
            "Blesensor.per3600": [{"id": "jp.sendai.Blesensor.per3600.10"}],
            "Blesensor.per300": [{"id": "jp.sendai.Blesensor.per300.10"}],
        }
    )

    validate_targets(places, make_client(session))

    types_called = [call["params"]["type"] for call in session.calls]
    assert sorted(types_called) == ["Blesensor.per300", "Blesensor.per3600"]
    for call in session.calls:
        assert call["params"]["attrs"] == "id"
        assert call["params"]["limit"] == 1000


def test_validate_targets_reports_missing_targets_and_warns_per_id(
    caplog: pytest.LogCaptureFixture,
) -> None:
    places = [
        place(
            place_number=10,
            interval_min=60,
            entity_type="Blesensor.per3600",
            entity_id="jp.sendai.Blesensor.per3600.10",
        ),
        place(
            place_number=105,
            interval_min=60,
            entity_type="Blesensor.per3600",
            entity_id="jp.sendai.Blesensor.per3600.105",
        ),
        place(
            place_number=200,
            interval_min=60,
            entity_type="Blesensor.per3600",
            entity_id="jp.sendai.Blesensor.per3600.200",
        ),
    ]
    session = FakeSession(
        {
            "Blesensor.per3600": [{"id": "jp.sendai.Blesensor.per3600.10"}],
        }
    )

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline"):
        result = validate_targets(places, make_client(session))

    assert result.missing_by_type == {
        "Blesensor.per3600": frozenset(
            {
                "jp.sendai.Blesensor.per3600.105",
                "jp.sendai.Blesensor.per3600.200",
            }
        ),
    }
    assert result.has_missing is True

    missing_events = records(caplog, "entity_map_missing_target")
    missing_ids = sorted(record.entity_id for record in missing_events)
    assert missing_ids == [
        "jp.sendai.Blesensor.per3600.105",
        "jp.sendai.Blesensor.per3600.200",
    ]
    for record in missing_events:
        assert record.levelname == "WARNING"
        assert record.entity_type == "Blesensor.per3600"


def test_validate_targets_records_extras_without_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    places = [
        place(
            place_number=10,
            interval_min=60,
            entity_type="Blesensor.per3600",
            entity_id="jp.sendai.Blesensor.per3600.10",
        ),
    ]
    session = FakeSession(
        {
            "Blesensor.per3600": [
                {"id": "jp.sendai.Blesensor.per3600.10"},
                {"id": "jp.sendai.Blesensor.per3600.999"},
                {"id": "jp.sendai.Blesensor.per3600.other"},
            ],
        }
    )

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline"):
        result = validate_targets(places, make_client(session))

    assert result.extra_by_type == {
        "Blesensor.per3600": frozenset(
            {
                "jp.sendai.Blesensor.per3600.999",
                "jp.sendai.Blesensor.per3600.other",
            }
        ),
    }
    assert result.missing_by_type == {"Blesensor.per3600": frozenset()}
    assert records(caplog, "entity_map_missing_target") == []


def test_validate_targets_warns_when_response_count_equals_limit(
    caplog: pytest.LogCaptureFixture,
) -> None:
    places = [
        place(
            place_number=i,
            interval_min=60,
            entity_type="Blesensor.per3600",
            entity_id=f"jp.sendai.Blesensor.per3600.{i}",
        )
        for i in range(1, 4)
    ]
    live = [{"id": f"jp.sendai.Blesensor.per3600.{i}"} for i in range(1, 4)]
    session = FakeSession({"Blesensor.per3600": live})

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline"):
        result = validate_targets(places, make_client(session), list_limit=3)

    assert result.truncated_types == frozenset({"Blesensor.per3600"})
    truncated = records(caplog, "entity_map_truncated")
    assert len(truncated) == 1
    assert truncated[0].entity_type == "Blesensor.per3600"
    assert truncated[0].levelname == "WARNING"


def test_validate_targets_does_not_warn_truncation_when_under_limit(
    caplog: pytest.LogCaptureFixture,
) -> None:
    places = [
        place(
            place_number=10,
            interval_min=60,
            entity_type="Blesensor.per3600",
            entity_id="jp.sendai.Blesensor.per3600.10",
        ),
    ]
    session = FakeSession(
        {"Blesensor.per3600": [{"id": "jp.sendai.Blesensor.per3600.10"}]}
    )

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline"):
        result = validate_targets(places, make_client(session), list_limit=10)

    assert result.truncated_types == frozenset()
    assert records(caplog, "entity_map_truncated") == []


def test_validate_targets_emits_refreshed_event_per_type(
    caplog: pytest.LogCaptureFixture,
) -> None:
    places = [
        place(
            place_number=10,
            interval_min=60,
            entity_type="Blesensor.per3600",
            entity_id="jp.sendai.Blesensor.per3600.10",
        ),
        place(
            place_number=200,
            interval_min=60,
            entity_type="Blesensor.per3600",
            entity_id="jp.sendai.Blesensor.per3600.200",
        ),
        place(
            place_number=10,
            interval_min=5,
            entity_type="Blesensor.per300",
            entity_id="jp.sendai.Blesensor.per300.10",
        ),
    ]
    session = FakeSession(
        {
            "Blesensor.per3600": [
                {"id": "jp.sendai.Blesensor.per3600.10"},
                {"id": "jp.sendai.Blesensor.per3600.extra"},
            ],
            "Blesensor.per300": [{"id": "jp.sendai.Blesensor.per300.10"}],
        }
    )

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline"):
        validate_targets(places, make_client(session))

    refreshed = {r.entity_type: r for r in records(caplog, "entity_map_refreshed")}
    assert set(refreshed) == {"Blesensor.per3600", "Blesensor.per300"}
    per3600 = refreshed["Blesensor.per3600"]
    assert per3600.count_expected == 2
    assert per3600.count_live == 2
    assert per3600.count_missing == 1
    assert per3600.count_extra == 1
    assert per3600.levelname == "INFO"
    per300 = refreshed["Blesensor.per300"]
    assert per300.count_expected == 1
    assert per300.count_live == 1
    assert per300.count_missing == 0
    assert per300.count_extra == 0


def test_validate_targets_returns_empty_result_when_no_active_places() -> None:
    session = FakeSession({})

    result = validate_targets([], make_client(session))

    assert result.expected_by_type == {}
    assert result.live_by_type == {}
    assert result.missing_by_type == {}
    assert result.extra_by_type == {}
    assert result.truncated_types == frozenset()
    assert session.calls == []
    assert result.has_missing is False


def test_validate_targets_groups_places_with_duplicate_entity_id_in_metadata() -> None:
    place_60 = place(
        place_number=10,
        interval_min=60,
        entity_type="Blesensor.per3600",
        entity_id="jp.sendai.Blesensor.per3600.10",
    )
    place_5 = place(
        place_number=10,
        interval_min=5,
        entity_type="Blesensor.per3600",
        entity_id="jp.sendai.Blesensor.per3600.10",
    )
    session = FakeSession(
        {"Blesensor.per3600": [{"id": "jp.sendai.Blesensor.per3600.10"}]}
    )

    result = validate_targets([place_60, place_5], make_client(session))

    assert result.expected_by_type == {
        "Blesensor.per3600": frozenset({"jp.sendai.Blesensor.per3600.10"})
    }
    assert result.missing_by_type == {"Blesensor.per3600": frozenset()}
    assert len(session.calls) == 1


def test_validate_targets_does_not_raise_on_missing_targets() -> None:
    places = [
        place(
            place_number=10,
            interval_min=60,
            entity_type="Blesensor.per3600",
            entity_id="jp.sendai.Blesensor.per3600.10",
        ),
    ]
    session = FakeSession({"Blesensor.per3600": []})

    result = validate_targets(places, make_client(session))

    assert result.missing_by_type == {
        "Blesensor.per3600": frozenset({"jp.sendai.Blesensor.per3600.10"})
    }
    assert result.has_missing is True


def test_entity_map_logging_extras_are_allowed() -> None:
    required = {
        "entity_type",
        "count_expected",
        "count_live",
        "count_missing",
        "count_extra",
        "limit",
    }
    assert required <= _ALLOWED_EXTRA_KEYS
