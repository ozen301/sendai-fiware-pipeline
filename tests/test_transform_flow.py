import json
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from sendai_pipeline.logging_setup import _ALLOWED_EXTRA_KEYS
from sendai_pipeline.metadata import SensorPlace, index_by_place_interval, load_metadata
from sendai_pipeline.transform_flow import transform_flow_rows

FIXTURES_DIR = Path(__file__).parent / "fixtures"
TRANSFORMED_AT = datetime(
    2026,
    5,
    23,
    1,
    2,
    3,
    987654,
    tzinfo=timezone(timedelta(hours=-4)),
)
DATE_RETRIEVED = "2026-05-23T14:02:03+09:00"
PRODUCT_A_ATTRS = [
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

TRANSFORM_EVENTS = frozenset(
    {
        "ignored_place_prefix",
        "unknown_place_interval",
        "device_mismatch",
    }
)


def place(
    *,
    place_number: int = 105,
    interval_min: int = 60,
    entity_type: str = "Blesensor.per3600",
    entity_id: str = "jp.sendai.Blesensor.per3600.105",
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
        entity_id=entity_id,
        identifcation=identifcation or str(place_number),
        active=active,
    )


def flow_row(**overrides: Any) -> dict[str, Any]:
    values: dict[str, Any] = {
        "startdate": "20260523_0900",
        "group_place_id": "sendai202603.105",
        "device_type": "M5Stack",
        "interval_min": 60,
        "flow_gt_m60": 6,
        "flow_gt_m80": 237,
        "flow_gt_m120": 430,
        "stay_gt_m60": Decimal("0.2"),
        "stay_gt_m80": Decimal("40.9"),
        "stay_gt_m120": Decimal("0.0"),
    }
    values.update(overrides)
    return values


def timeinstant(value: str = "2026-05-23T09:00:00+09:00") -> dict[str, Any]:
    return {"TimeInstant": {"type": "DateTime", "value": value}}


def records(caplog: pytest.LogCaptureFixture, event: str) -> list[Any]:
    return [
        record for record in caplog.records if getattr(record, "event", None) == event
    ]


def transform(
    rows: list[dict[str, Any]],
    metadata_index: dict[tuple[int, int], SensorPlace],
    *,
    transformed_at: datetime = TRANSFORMED_AT,
    ignored_place_prefixes: tuple[str, ...] = ("quick.", "test"),
) -> list[dict[str, Any]]:
    return transform_flow_rows(
        rows,
        metadata_index,
        transformed_at=transformed_at,
        ignored_place_prefixes=ignored_place_prefixes,
    )


def test_transform_flow_builds_per3600_payload_with_full_attribute_set() -> None:
    metadata_index = {
        (105, 60): place(
            entity_id="jp.sendai.Blesensor.per3600.105",
            entity_type="Blesensor.per3600",
        )
    }

    result = transform([flow_row()], metadata_index)

    assert result == [
        {
            "entity_id": "jp.sendai.Blesensor.per3600.105",
            "entity_type": "Blesensor.per3600",
            "attrs": {
                "dateObservedFrom": {
                    "type": "DateTime",
                    "value": "2026-05-23T09:00:00+09:00",
                    "metadata": timeinstant(),
                },
                "dateObservedTo": {
                    "type": "DateTime",
                    "value": "2026-05-23T10:00:00+09:00",
                    "metadata": timeinstant(),
                },
                "dateRetrieved": {
                    "type": "DateTime",
                    "value": DATE_RETRIEVED,
                    "metadata": timeinstant(),
                },
                "identifcation": {
                    "type": "Text",
                    "value": "jp.sendai.Blesensor.per3600.105",
                    "metadata": timeinstant(),
                },
                "peopleCount_immedate": {
                    "type": "number",
                    "value": 6,
                    "metadata": timeinstant(),
                },
                "peopleCount_near": {
                    "type": "number",
                    "value": 237,
                    "metadata": timeinstant(),
                },
                "peopleCount_far": {
                    "type": "number",
                    "value": 430,
                    "metadata": timeinstant(),
                },
                "peopleOccupancy_immedate": {
                    "type": "number",
                    "value": 0.2,
                    "metadata": timeinstant(),
                },
                "peopleOccupancy_near": {
                    "type": "number",
                    "value": 40.9,
                    "metadata": timeinstant(),
                },
                "peopleOccupancy_far": {
                    "type": "number",
                    "value": 0.0,
                    "metadata": timeinstant(),
                },
            },
        }
    ]


def test_transform_flow_builds_per300_payload_with_five_minute_window() -> None:
    metadata_index = {
        (10, 5): place(
            place_number=10,
            interval_min=5,
            entity_id="jp.sendai.Blesensor.per300.10",
            entity_type="Blesensor.per300",
            batch="2023",
            expected_device_type="Pixel3aUT",
        )
    }

    result = transform(
        [
            flow_row(
                startdate="20260523_2355",
                group_place_id="sendai2023.10",
                device_type="Pixel3aUT",
                interval_min=5,
            )
        ],
        metadata_index,
    )

    assert len(result) == 1
    assert result[0]["entity_id"] == "jp.sendai.Blesensor.per300.10"
    assert result[0]["entity_type"] == "Blesensor.per300"
    assert result[0]["attrs"]["dateObservedFrom"] == {
        "type": "DateTime",
        "value": "2026-05-23T23:55:00+09:00",
        "metadata": timeinstant("2026-05-23T23:55:00+09:00"),
    }
    assert result[0]["attrs"]["dateObservedTo"] == {
        "type": "DateTime",
        "value": "2026-05-24T00:00:00+09:00",
        "metadata": timeinstant("2026-05-23T23:55:00+09:00"),
    }


def test_transform_flow_drops_unsupported_interval_without_log(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline"):
        result = transform(
            [flow_row(interval_min=1)],
            {},
        )

    assert result == []
    assert caplog.records == []


def test_transform_flow_drops_default_noise_prefix_before_metadata_lookup(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline"):
        result = transform(
            [flow_row(group_place_id="quick.105")],
            {},
        )

    assert result == []
    ignored = records(caplog, "ignored_place_prefix")
    assert len(ignored) == 1
    assert ignored[0].levelname == "DEBUG"
    assert ignored[0].group_place_id == "quick.105"
    assert ignored[0].matched_prefix == "quick."
    assert records(caplog, "unknown_place_interval") == []


def test_transform_flow_uses_custom_noise_prefixes_instead_of_defaults(
    caplog: pytest.LogCaptureFixture,
) -> None:
    metadata_index = {
        (10, 60): place(
            place_number=10,
            entity_id="jp.sendai.Blesensor.per3600.10",
        )
    }

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline"):
        result = transform(
            [
                flow_row(group_place_id="foo.10"),
                flow_row(group_place_id="quick.10"),
            ],
            metadata_index,
            ignored_place_prefixes=("foo.",),
        )

    assert [payload["entity_id"] for payload in result] == [
        "jp.sendai.Blesensor.per3600.10"
    ]
    ignored = records(caplog, "ignored_place_prefix")
    assert len(ignored) == 1
    assert ignored[0].group_place_id == "foo.10"
    assert ignored[0].matched_prefix == "foo."
    assert records(caplog, "unknown_place_interval") == []


def test_transform_flow_logs_and_drops_unknown_place_interval(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline"):
        result = transform(
            [flow_row(group_place_id="sendai2023.99")],
            {},
        )

    assert result == []
    unknown = records(caplog, "unknown_place_interval")
    assert len(unknown) == 1
    assert unknown[0].levelname == "DEBUG"
    assert unknown[0].group_place_id == "sendai2023.99"
    assert unknown[0].place_number == 99
    assert unknown[0].interval_min == 60
    assert records(caplog, "device_mismatch") == []


def test_transform_flow_logs_and_drops_device_mismatch(
    caplog: pytest.LogCaptureFixture,
) -> None:
    metadata_index = {
        (105, 60): place(
            place_number=105,
            interval_min=60,
            expected_device_type="M5Stack",
        )
    }

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline"):
        result = transform(
            [flow_row(device_type="Pixel3aUT")],
            metadata_index,
        )

    assert result == []
    mismatches = records(caplog, "device_mismatch")
    assert len(mismatches) == 1
    assert mismatches[0].levelname == "DEBUG"
    assert mismatches[0].place_number == 105
    assert mismatches[0].interval_min == 60
    assert mismatches[0].device_type == "Pixel3aUT"
    assert mismatches[0].expected_device_type == "M5Stack"


def test_transform_flow_reads_entity_id_and_type_verbatim_from_metadata() -> None:
    metadata_index = {
        (10, 60): place(
            place_number=10,
            interval_min=60,
            entity_id="jp.sendai.Blesensor.per3600.10.custom-suffix",
            entity_type="Custom.Blesensor.Type",
            batch="2023",
            expected_device_type="Pixel3aUT",
        )
    }

    result = transform(
        [
            flow_row(
                group_place_id="sendai2023.10",
                device_type="Pixel3aUT",
            )
        ],
        metadata_index,
    )

    assert len(result) == 1
    assert result[0]["entity_id"] == "jp.sendai.Blesensor.per3600.10.custom-suffix"
    assert result[0]["entity_type"] == "Custom.Blesensor.Type"
    assert result[0]["attrs"]["identifcation"] == {
        "type": "Text",
        "value": "jp.sendai.Blesensor.per3600.10.custom-suffix",
        "metadata": timeinstant(),
    }


def test_transform_flow_uses_entity_id_instead_of_metadata_identifcation() -> None:
    entity_id = "jp.sendai.Blesensor.per3600.105.full"
    metadata_index = {
        (105, 60): place(entity_id=entity_id, identifcation="legacy-bare-105")
    }

    [payload] = transform([flow_row()], metadata_index)

    assert payload["entity_id"] == entity_id
    assert payload["attrs"]["identifcation"]["type"] == "Text"
    assert payload["attrs"]["identifcation"]["value"] == entity_id
    assert payload["attrs"]["identifcation"]["value"] != "legacy-bare-105"


def test_transform_flow_constructs_exact_ten_attributes_in_stable_order() -> None:
    [payload] = transform([flow_row()], {(105, 60): place()})

    assert list(payload["attrs"]) == PRODUCT_A_ATTRS


def test_transform_flow_normalizes_retrieval_timestamp_to_whole_second_jst() -> None:
    [payload] = transform([flow_row()], {(105, 60): place()})

    date_retrieved = payload["attrs"]["dateRetrieved"]
    assert date_retrieved["type"] == "DateTime"
    assert date_retrieved["value"] == DATE_RETRIEVED
    parsed = datetime.fromisoformat(date_retrieved["value"])
    assert parsed.utcoffset() == timedelta(hours=9)
    assert parsed.microsecond == 0


def test_transform_flow_converts_decimal_values_to_json_numbers() -> None:
    metadata_index = {(105, 60): place()}

    [payload] = transform(
        [
            flow_row(
                stay_gt_m60=Decimal("0.2"),
                stay_gt_m80=Decimal("40.9"),
                stay_gt_m120=Decimal("120.1"),
            )
        ],
        metadata_index,
    )

    value = payload["attrs"]["peopleOccupancy_immedate"]["value"]
    assert type(value) is float
    assert value == 0.2

    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    assert '"peopleOccupancy_immedate":{' in serialized
    assert '"value":0.2' in serialized

    for name, expected in (
        ("peopleOccupancy_immedate", 0.2),
        ("peopleOccupancy_near", 40.9),
        ("peopleOccupancy_far", 120.1),
    ):
        value = payload["attrs"][name]["value"]
        assert type(value) is float
        assert value == expected
    assert "Decimal" not in serialized


def test_transform_flow_preserves_null_occupancy_values() -> None:
    metadata_index = {(105, 60): place()}

    [payload] = transform(
        [
            flow_row(
                stay_gt_m60=None,
                stay_gt_m80=None,
                stay_gt_m120=None,
            )
        ],
        metadata_index,
    )

    attrs = payload["attrs"]
    assert attrs["peopleOccupancy_immedate"]["value"] is None
    assert attrs["peopleOccupancy_near"]["value"] is None
    assert attrs["peopleOccupancy_far"]["value"] is None

    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    assert '"peopleOccupancy_immedate":{' in serialized
    assert '"value":null' in serialized


def test_transform_flow_preserves_observed_zero_occupancy_distinct_from_null() -> None:
    metadata_index = {(105, 60): place()}

    [payload] = transform(
        [
            flow_row(
                stay_gt_m60=Decimal("0.0"),
                stay_gt_m80=0,
                stay_gt_m120=Decimal("0.0"),
            )
        ],
        metadata_index,
    )

    attrs = payload["attrs"]
    assert attrs["peopleOccupancy_immedate"]["value"] == 0.0
    assert attrs["peopleOccupancy_near"]["value"] == 0.0
    assert attrs["peopleOccupancy_far"]["value"] == 0.0
    assert all(
        attrs[name]["value"] is not None
        for name in (
            "peopleOccupancy_immedate",
            "peopleOccupancy_near",
            "peopleOccupancy_far",
        )
    )


def test_transform_flow_preserves_integer_value_types_for_count_attributes() -> None:
    metadata_index = {(105, 60): place()}

    [payload] = transform([flow_row()], metadata_index)
    attrs = payload["attrs"]

    assert type(attrs["peopleCount_immedate"]["value"]) is int
    assert type(attrs["peopleCount_near"]["value"]) is int
    assert type(attrs["peopleCount_far"]["value"]) is int
    assert type(attrs["peopleOccupancy_immedate"]["value"]) is float
    assert type(attrs["peopleOccupancy_near"]["value"]) is float
    assert type(attrs["peopleOccupancy_far"]["value"]) is float


def test_transform_flow_sets_timeinstant_metadata_to_observed_from() -> None:
    metadata_index = {(105, 60): place()}

    [payload] = transform([flow_row()], metadata_index)

    attrs = payload["attrs"]
    for attr_name in PRODUCT_A_ATTRS:
        assert attrs[attr_name]["metadata"] == timeinstant()


def test_transform_flow_builds_payload_against_csv_loaded_metadata_index() -> None:
    metadata = load_metadata(FIXTURES_DIR / "sensors_minimal.csv")
    index = index_by_place_interval(metadata)

    result = transform(
        [
            flow_row(
                startdate="20260523_0900",
                group_place_id="sendai202603.105",
                device_type="M5Stack",
                interval_min=60,
            )
        ],
        index,
    )

    assert len(result) == 1
    payload = result[0]
    assert payload["entity_id"] == "jp.sendai.Blesensor.per3600.105"
    assert payload["entity_type"] == "Blesensor.per3600"
    assert payload["attrs"]["dateObservedFrom"]["value"] == (
        "2026-05-23T09:00:00+09:00"
    )


def test_transform_flow_preserves_batch_order_for_multiple_rows() -> None:
    metadata_index = {
        (105, 60): place(
            place_number=105,
            interval_min=60,
            entity_id="jp.sendai.Blesensor.per3600.105",
        ),
        (106, 60): place(
            place_number=106,
            interval_min=60,
            entity_id="jp.sendai.Blesensor.per3600.106",
        ),
    }

    result = transform(
        [
            flow_row(group_place_id="sendai202603.106"),
            flow_row(group_place_id="sendai202603.105"),
        ],
        metadata_index,
    )

    assert [payload["entity_id"] for payload in result] == [
        "jp.sendai.Blesensor.per3600.106",
        "jp.sendai.Blesensor.per3600.105",
    ]


def test_transform_flow_returns_empty_list_for_empty_input_without_log(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline"):
        result = transform([], {})

    assert result == []
    assert caplog.records == []


def test_transform_flow_emits_only_known_structured_events(
    caplog: pytest.LogCaptureFixture,
) -> None:
    metadata_index = {
        (10, 60): place(
            place_number=10,
            interval_min=60,
            expected_device_type="M5Stack",
        )
    }

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline"):
        result = transform(
            [
                flow_row(group_place_id="quick.10"),
                flow_row(group_place_id="sendai2023.99"),
                flow_row(group_place_id="sendai2023.10", device_type="Pixel3aUT"),
            ],
            metadata_index,
        )

    assert result == []
    events = [getattr(record, "event", None) for record in caplog.records]
    assert events == [
        "ignored_place_prefix",
        "unknown_place_interval",
        "device_mismatch",
    ]
    assert set(events) <= TRANSFORM_EVENTS


@pytest.mark.parametrize(
    ("interval_min", "entity_type", "entity_id", "device_type"),
    [
        (5, "Blesensor.per300", "jp.sendai.Blesensor.per300.105", "M5Stack"),
        (60, "Blesensor.per3600", "jp.sendai.Blesensor.per3600.105", "M5Stack"),
    ],
)
def test_transform_flow_keeps_people_count_far_mapped_from_flow_gt_m120(
    interval_min: int,
    entity_type: str,
    entity_id: str,
    device_type: str,
) -> None:
    metadata_index = {
        (105, interval_min): place(
            interval_min=interval_min,
            entity_type=entity_type,
            entity_id=entity_id,
            expected_device_type=device_type,
        )
    }

    [payload] = transform(
        [
            flow_row(
                interval_min=interval_min,
                device_type=device_type,
                flow_gt_m120=987,
                stay_gt_m120=Decimal("12.3"),
            )
        ],
        metadata_index,
    )

    assert payload["attrs"]["peopleCount_far"]["value"] == 987
    assert payload["attrs"]["peopleOccupancy_far"]["value"] == 12.3


def test_transform_flow_logging_extras_are_allowed() -> None:
    required = {
        "group_place_id",
        "place_number",
        "interval_min",
        "device_type",
        "expected_device_type",
        "matched_prefix",
    }
    assert required <= _ALLOWED_EXTRA_KEYS
