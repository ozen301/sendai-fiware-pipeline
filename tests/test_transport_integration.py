import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

from sendai_pipeline.filter_settings import FilterSettings
from sendai_pipeline.metadata import SensorPlace
from sendai_pipeline.orion_client import OrionClient, OrionSettings
from sendai_pipeline.run_direction import (
    DirectionWindowPublishResult,
    RunDirectionSettings,
    publish_direction_window,
)
from sendai_pipeline.run_flow import FlowWindowPublishResult, publish_flow_window
from sendai_pipeline.state import WindowStateStore

JST = timezone(timedelta(hours=9))
TRANSFORMED_AT = datetime(2026, 7, 15, 12, 17, 43, 123456, tzinfo=JST)
BASE_URL = "https://fiware.example.test"
SERVICE = "sendai"
SERVICE_PATH = "/city"
TOKEN = "request-token"


class RecordingResponse:
    def __init__(self) -> None:
        self.status_code = 204
        self.text = ""
        self.headers: dict[str, str] = {}


class RecordingSession:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def post(self, url: str, **kwargs: Any) -> RecordingResponse:
        self.calls.append({"method": "POST", "url": url, **kwargs})
        return RecordingResponse()

    def put(self, url: str, **kwargs: Any) -> RecordingResponse:
        self.calls.append({"method": "PUT", "url": url, **kwargs})
        return RecordingResponse()


class StaticTokenProvider:
    def __init__(self) -> None:
        self.calls: list[bool] = []

    def get_token(self, *, force_refresh: bool = False) -> str:
        self.calls.append(force_refresh)
        return TOKEN


def _orion_client(
    session: RecordingSession,
    token_provider: StaticTokenProvider,
) -> OrionClient:
    return OrionClient(
        OrionSettings(
            base_url=BASE_URL,
            service=SERVICE,
            service_path=SERVICE_PATH,
            verify_tls=False,
            timeout=2.5,
            max_retries=0,
        ),
        auth=token_provider,
        session=session,
        now=lambda: 1.0,
    )


def _place() -> SensorPlace:
    return SensorPlace(
        place_number=10,
        batch="2026",
        expected_device_type="M5Stack",
        interval_min=60,
        entity_type="Blesensor.per3600",
        entity_id="jp.sendai.Blesensor.per3600.10",
        identifcation="10",
        active=True,
    )


def _filters() -> FilterSettings:
    return FilterSettings(
        target_flow_batches=frozenset({"2026"}),
        target_direction_batches=frozenset({"2026"}),
        ignored_place_prefixes=("quick.", "test"),
    )


def _attribute(
    attribute_type: str,
    value: Any,
    *,
    timeinstant: str,
) -> dict[str, Any]:
    return {
        "type": attribute_type,
        "value": value,
        "metadata": {
            "TimeInstant": {
                "type": "DateTime",
                "value": timeinstant,
            }
        },
    }


def _canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _expected_headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {TOKEN}",
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Fiware-Service": SERVICE,
        "Fiware-ServicePath": SERVICE_PATH,
    }


def test_product_a_payload_reaches_real_orion_client_as_canonical_post(
    tmp_path: Path,
) -> None:
    place = _place()
    state_store = WindowStateStore(
        tmp_path / "flow.json",
        now=lambda: TRANSFORMED_AT,
    )
    session = RecordingSession()
    token_provider = StaticTokenProvider()
    orion = _orion_client(session, token_provider)

    result = publish_flow_window(
        interval_min=60,
        startdate="20260715_0900",
        rows_for_window=[
            {
                "startdate": "20260715_0900",
                "group_place_id": "sendai202603.10",
                "device_type": "M5Stack",
                "interval_min": 60,
                "flow_gt_m60": 8,
                "flow_gt_m80": 6,
                "flow_gt_m120": 4,
                "stay_gt_m60": 3.5,
                "stay_gt_m80": 2.5,
                "stay_gt_m120": 1.5,
            }
        ],
        orion=orion,
        state_store=state_store,
        filter_settings=_filters(),
        interval_metadata={(10, 60): place},
        transformed_at=TRANSFORMED_AT,
    )

    observed_from = "2026-07-15T09:00:00+09:00"
    expected_attrs = {
        "dateObservedFrom": _attribute(
            "DateTime",
            observed_from,
            timeinstant=observed_from,
        ),
        "dateObservedTo": _attribute(
            "DateTime",
            "2026-07-15T10:00:00+09:00",
            timeinstant=observed_from,
        ),
        "dateRetrieved": _attribute(
            "DateTime",
            "2026-07-15T12:17:43+09:00",
            timeinstant=observed_from,
        ),
        "identifcation": _attribute(
            "Text",
            place.entity_id,
            timeinstant=observed_from,
        ),
        "peopleCount_immedate": _attribute(
            "number",
            8,
            timeinstant=observed_from,
        ),
        "peopleCount_near": _attribute(
            "number",
            6,
            timeinstant=observed_from,
        ),
        "peopleCount_far": _attribute(
            "number",
            4,
            timeinstant=observed_from,
        ),
        "peopleOccupancy_immedate": _attribute(
            "number",
            3.5,
            timeinstant=observed_from,
        ),
        "peopleOccupancy_near": _attribute(
            "number",
            2.5,
            timeinstant=observed_from,
        ),
        "peopleOccupancy_far": _attribute(
            "number",
            1.5,
            timeinstant=observed_from,
        ),
    }

    assert result == FlowWindowPublishResult(
        windows_complete=1,
        windows_partial=0,
        windows_dead_letter=0,
        posts_ok=1,
        posts_failed=0,
        rows_dropped=0,
    )
    assert len(session.calls) == 1
    call = session.calls[0]
    assert call["method"] == "POST"
    assert call["url"] == (
        f"{BASE_URL}/orion/v2.0/entities/{place.entity_id}/attrs"
        f"?type={place.entity_type}"
    )
    assert call["headers"] == _expected_headers()
    assert call["data"] == _canonical_json_bytes(expected_attrs)
    assert json.loads(call["data"]) == expected_attrs
    assert "json" not in call
    assert call["timeout"] == 2.5
    assert call["verify"] is False
    assert token_provider.calls == [False]
    window_key = "per3600/20260715_0900"
    assert state_store.window_status(window_key) == "complete"
    target = state_store.target_record(window_key, place.entity_id)
    assert target is not None
    assert target["status"] == "ok"
    assert state_store.path.exists()


def test_product_b_payload_reaches_real_orion_client_as_canonical_put(
    tmp_path: Path,
) -> None:
    place = _place()
    run_settings = RunDirectionSettings()
    state_store = WindowStateStore(
        tmp_path / "direction.json",
        now=lambda: TRANSFORMED_AT,
    )
    session = RecordingSession()
    token_provider = StaticTokenProvider()
    orion = _orion_client(session, token_provider)

    result = publish_direction_window(
        interval_min=60,
        startdate="20260715_0900",
        rows_for_window=[
            {
                "startdate": "20260715_0900",
                "from_group_place_id": "ALL",
                "to_group_place_id": "sendai202603.10",
                "from_device_type": "M5Stack",
                "to_device_type": "M5Stack",
                "interval_min": 60,
                "count": 8,
            },
            {
                "startdate": "20260715_0900",
                "from_group_place_id": "sendai202603.10",
                "to_group_place_id": "ALL",
                "from_device_type": "M5Stack",
                "to_device_type": "M5Stack",
                "interval_min": 60,
                "count": 6,
            },
        ],
        orion=orion,
        state_store=state_store,
        settings=run_settings,
        filter_settings=_filters(),
        interval_metadata={(10, 60): place},
        transformed_at=TRANSFORMED_AT,
    )

    observed_from = "2026-07-15T09:00:00+09:00"
    expected_attrs = {
        "dateObservedFrom": _attribute(
            "DateTime",
            observed_from,
            timeinstant=observed_from,
        ),
        "dateObservedTo": _attribute(
            "DateTime",
            "2026-07-15T10:00:00+09:00",
            timeinstant=observed_from,
        ),
        "dateRetrieved": _attribute(
            "DateTime",
            "2026-07-15T12:17:43+09:00",
            timeinstant=observed_from,
        ),
        "identifcation": _attribute(
            "Text",
            run_settings.product_b_aggregate_entity_id,
            timeinstant=observed_from,
        ),
        "sourceQuality": _attribute(
            "StructuredValue",
            {
                "status": "clean",
                "evaluatedAt": "2026-07-15T12:17:43+09:00",
                "excludedPlaceNumbers": [],
                "missingFromAllPlaceNumbers": [],
                "missingToAllPlaceNumbers": [],
            },
            timeinstant=observed_from,
        ),
        "peopleCount_flow_10": _attribute(
            "StructuredValue",
            {
                "from": {"10": 0, "all": 8},
                "to": {"10": 0, "all": 6},
            },
            timeinstant=observed_from,
        ),
    }

    assert result == DirectionWindowPublishResult(
        windows_complete=1,
        windows_partial=0,
        windows_dead_letter=0,
        puts_ok=1,
        puts_failed=0,
        windows_degraded=0,
        windows_no_payload=0,
        windows_source_invalid=0,
        rows_dropped=0,
    )
    assert len(session.calls) == 1
    call = session.calls[0]
    encoded_entity_id = quote(
        run_settings.product_b_aggregate_entity_id,
        safe="",
    )
    assert call["method"] == "PUT"
    assert call["url"] == (f"{BASE_URL}/orion/v2.0/entities/{encoded_entity_id}/attrs")
    assert "?" not in call["url"]
    assert call["params"] == {"type": run_settings.product_b_aggregate_entity_type}
    assert call["headers"] == _expected_headers()
    assert call["data"] == _canonical_json_bytes(expected_attrs)
    assert json.loads(call["data"]) == expected_attrs
    assert "json" not in call
    assert call["timeout"] == 2.5
    assert call["verify"] is False
    assert token_provider.calls == [False]
    window_key = "per3600/20260715_0900"
    assert state_store.window_status(window_key) == "complete"
    target = state_store.target_record(
        window_key,
        run_settings.product_b_aggregate_entity_id,
    )
    assert target is not None
    assert target["status"] == "ok"
    assert state_store.path.exists()
