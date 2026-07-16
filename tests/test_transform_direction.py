import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from sendai_pipeline.metadata import SensorPlace
from sendai_pipeline.transform_direction import (
    DirectionNoPayloadOutcome,
    DirectionPayloadOutcome,
    DirectionSourceInvalidOutcome,
    DirectionTransformOutcome,
    transform_direction_window,
)

JST = timezone(timedelta(hours=9))
AGGREGATE_ENTITY_ID = "test.aggregate.direction"
AGGREGATE_ENTITY_TYPE = "TestAggregateDirection"
FIXED_NOW = datetime(2026, 5, 24, 13, 25, 43, 288677, tzinfo=JST)
FIXED_NOW_ISO = "2026-05-24T13:25:43+09:00"
OBSERVED_FROM = "2026-05-23T09:00:00+09:00"
OBSERVED_TO = "2026-05-23T10:00:00+09:00"


def fixed_clock() -> datetime:
    return FIXED_NOW


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


def direction_row(**overrides: Any) -> dict[str, Any]:
    values: dict[str, Any] = {
        "startdate": "20260523_0900",
        "from_group_place_id": "sendai202603.105",
        "to_group_place_id": "sendai202603.106",
        "from_device_type": "M5Stack",
        "to_device_type": "M5Stack",
        "interval_min": 60,
        "count": 12,
    }
    values.update(overrides)
    return values


def timeinstant(value: str = OBSERVED_FROM) -> dict[str, Any]:
    return {"TimeInstant": {"type": "DateTime", "value": value}}


def records(caplog: pytest.LogCaptureFixture, event: str) -> list[Any]:
    return [
        record for record in caplog.records if getattr(record, "event", None) == event
    ]


def transform(
    rows: list[dict[str, Any]],
    metadata_index: dict[tuple[int, int], SensorPlace],
) -> DirectionTransformOutcome:
    return transform_direction_window(
        rows,
        metadata_index,
        aggregate_entity_id=AGGREGATE_ENTITY_ID,
        aggregate_entity_type=AGGREGATE_ENTITY_TYPE,
        now=fixed_clock,
    )


def payload_from(outcome: DirectionTransformOutcome) -> dict[str, Any]:
    assert isinstance(outcome, DirectionPayloadOutcome)
    return outcome.payload


def complete_totals(
    source_id: str,
    *,
    from_all: int,
    to_all: int,
    device_type: str = "M5Stack",
    interval_min: int = 60,
    startdate: str = "20260523_0900",
) -> list[dict[str, Any]]:
    return [
        direction_row(
            startdate=startdate,
            from_group_place_id="ALL",
            to_group_place_id=source_id,
            from_device_type=device_type,
            to_device_type=device_type,
            interval_min=interval_min,
            count=from_all,
        ),
        direction_row(
            startdate=startdate,
            from_group_place_id=source_id,
            to_group_place_id="ALL",
            from_device_type=device_type,
            to_device_type=device_type,
            interval_min=interval_min,
            count=to_all,
        ),
    ]


def test_transform_direction_builds_aggregate_with_exact_ngsi_contract() -> None:
    metadata_index = {
        (105, 60): place(place_number=105),
        (106, 60): place(
            place_number=106,
            entity_id="jp.sendai.Blesensor.per3600.106",
        ),
        (107, 60): place(
            place_number=107,
            entity_id="jp.sendai.Blesensor.per3600.107",
        ),
    }
    rows = [
        direction_row(count=12),
        direction_row(
            from_group_place_id="sendai202603.105",
            to_group_place_id="sendai202603.105",
            count=7,
        ),
        *complete_totals("sendai202603.105", from_all=85, to_all=82),
        *complete_totals("sendai202603.106", from_all=67, to_all=71),
    ]

    outcome = transform(rows, metadata_index)

    assert isinstance(outcome, DirectionPayloadOutcome)
    assert outcome.rows_dropped == 0
    assert outcome.payload == {
        "entity_id": AGGREGATE_ENTITY_ID,
        "entity_type": AGGREGATE_ENTITY_TYPE,
        "attrs": {
            "dateObservedFrom": {
                "type": "DateTime",
                "value": OBSERVED_FROM,
                "metadata": timeinstant(),
            },
            "dateObservedTo": {
                "type": "DateTime",
                "value": OBSERVED_TO,
                "metadata": timeinstant(),
            },
            "dateRetrieved": {
                "type": "DateTime",
                "value": FIXED_NOW_ISO,
                "metadata": timeinstant(),
            },
            "identifcation": {
                "type": "Text",
                "value": AGGREGATE_ENTITY_ID,
                "metadata": timeinstant(),
            },
            "peopleCount_flow_105": {
                "type": "StructuredValue",
                "value": {
                    "from": {"105": 7, "106": 0, "all": 85},
                    "to": {"105": 7, "106": 12, "all": 82},
                },
                "metadata": timeinstant(),
            },
            "peopleCount_flow_106": {
                "type": "StructuredValue",
                "value": {
                    "from": {"105": 12, "106": 0, "all": 67},
                    "to": {"105": 0, "106": 0, "all": 71},
                },
                "metadata": timeinstant(),
            },
        },
    }


def test_transform_direction_ignores_five_minute_rows_in_sendable_window() -> None:
    metadata_index = {
        (5, 5): place(
            place_number=5,
            interval_min=5,
            entity_type="Blesensor.per300",
            entity_id="jp.sendai.Blesensor.per300.5",
        ),
        (105, 60): place(place_number=105),
    }
    rows = [
        *complete_totals("sendai202603.105", from_all=8, to_all=6),
        *complete_totals(
            "sendai202603.5",
            from_all=4,
            to_all=3,
            interval_min=5,
            startdate="20260523_0955",
        ),
    ]

    outcome = transform(rows, metadata_index)
    payload = payload_from(outcome)

    assert outcome.rows_dropped == 2
    assert set(payload["attrs"]) == {
        "dateObservedFrom",
        "dateObservedTo",
        "dateRetrieved",
        "identifcation",
        "peopleCount_flow_105",
    }
    assert payload["attrs"]["dateObservedFrom"]["value"] == OBSERVED_FROM
    assert payload["attrs"]["dateObservedTo"]["value"] == OBSERVED_TO


def test_transform_direction_rejects_rows_from_multiple_sixty_minute_windows() -> None:
    metadata_index = {(105, 60): place(place_number=105)}
    rows = [
        *complete_totals("sendai202603.105", from_all=8, to_all=6),
        *complete_totals(
            "sendai202603.105",
            from_all=7,
            to_all=5,
            startdate="20260523_1000",
        ),
    ]

    with pytest.raises(ValueError, match="single 60-minute source window"):
        transform(rows, metadata_index)


def test_transform_direction_keeps_cross_batch_rows_for_oldest_device_type() -> None:
    metadata_index = {
        (9, 60): place(
            place_number=9,
            entity_id="jp.sendai.Blesensor.per3600.9",
            batch="2023",
            expected_device_type="M5Stack",
            active=False,
        ),
        (10, 60): place(
            place_number=10,
            entity_id="jp.sendai.Blesensor.per3600.10",
            batch="2023",
            expected_device_type="Pixel3aUT",
        ),
        (105, 60): place(place_number=105),
    }
    pixel_rows = [
        direction_row(
            from_group_place_id="sendai2023.10",
            to_group_place_id="sendai202603.105",
            from_device_type="Pixel3aUT",
            to_device_type="Pixel3aUT",
            count=14,
        ),
        *complete_totals(
            "sendai2023.10",
            from_all=31,
            to_all=29,
            device_type="Pixel3aUT",
        ),
        *complete_totals(
            "sendai202603.105",
            from_all=41,
            to_all=43,
            device_type="Pixel3aUT",
        ),
    ]
    newer_device_rows = [
        {
            **row,
            "from_device_type": "M5Stack",
            "to_device_type": "M5Stack",
            "count": 999,
        }
        for row in pixel_rows
    ]

    outcome = transform([*pixel_rows, *newer_device_rows], metadata_index)
    payload = payload_from(outcome)

    assert outcome.rows_dropped == len(newer_device_rows)
    assert payload["attrs"]["peopleCount_flow_10"]["value"] == {
        "from": {"10": 0, "105": 0, "all": 31},
        "to": {"10": 0, "105": 14, "all": 29},
    }
    assert payload["attrs"]["peopleCount_flow_105"]["value"] == {
        "from": {"10": 14, "105": 0, "all": 41},
        "to": {"10": 0, "105": 0, "all": 43},
    }


def test_transform_direction_emits_candidate_with_totals_and_no_pairwise_rows() -> None:
    metadata_index = {
        (105, 60): place(place_number=105),
        (106, 60): place(
            place_number=106,
            entity_id="jp.sendai.Blesensor.per3600.106",
        ),
    }

    outcome = transform(
        complete_totals("sendai202603.105", from_all=5, to_all=4),
        metadata_index,
    )
    payload = payload_from(outcome)

    assert set(payload["attrs"]) == {
        "dateObservedFrom",
        "dateObservedTo",
        "dateRetrieved",
        "identifcation",
        "peopleCount_flow_105",
    }
    assert payload["attrs"]["peopleCount_flow_105"]["value"] == {
        "from": {"105": 0, "all": 5},
        "to": {"105": 0, "all": 4},
    }
    assert "peopleCount_flow_106" not in payload["attrs"]
    assert all(
        attribute["value"] is not None for attribute in payload["attrs"].values()
    )


def test_transform_direction_returns_typed_no_payload_for_zero_candidates(
    caplog: pytest.LogCaptureFixture,
) -> None:
    metadata_index = {
        (10, 60): place(
            place_number=10,
            entity_id="jp.sendai.Blesensor.per3600.10",
            batch="2023",
            expected_device_type="Pixel3aUT",
        ),
        (105, 60): place(place_number=105),
        (108, 60): place(
            place_number=108,
            entity_id="jp.sendai.Blesensor.per3600.108",
            active=False,
        ),
        (109, 5): place(
            place_number=109,
            interval_min=5,
            entity_type="Blesensor.per300",
            entity_id="jp.sendai.Blesensor.per300.109",
        ),
    }
    rows = [
        direction_row(interval_min=5),
        direction_row(interval_min=15),
        direction_row(from_group_place_id="quick.10"),
        direction_row(from_group_place_id="sendai2023.999"),
        direction_row(
            from_group_place_id="sendai2023.105",
            to_group_place_id="ALL",
        ),
        direction_row(
            from_group_place_id="sendai202603.108",
            to_group_place_id="ALL",
        ),
        direction_row(
            from_group_place_id="sendai202603.109",
            to_group_place_id="ALL",
        ),
        direction_row(
            from_group_place_id="sendai2023.10",
            to_group_place_id="sendai202603.105",
            from_device_type="M5Stack",
            to_device_type="M5Stack",
        ),
    ]

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline"):
        outcome = transform(rows, metadata_index)

    assert isinstance(outcome, DirectionNoPayloadOutcome)
    assert not isinstance(outcome, DirectionSourceInvalidOutcome)
    assert outcome.rows_dropped == len(rows)
    assert records(caplog, "ignored_place_prefix")
    assert records(caplog, "unknown_place_interval")
    assert records(caplog, "device_mismatch")
    assert all(record.levelno == logging.DEBUG for record in caplog.records)


def test_transform_direction_drops_unsupported_interval_before_logging(
    caplog: pytest.LogCaptureFixture,
) -> None:
    row = direction_row(
        from_group_place_id="unknown.from",
        to_group_place_id="unknown.to",
        interval_min=15,
    )

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline"):
        outcome = transform([row], {})

    assert isinstance(outcome, DirectionNoPayloadOutcome)
    assert outcome.rows_dropped == 1
    assert caplog.records == []


@pytest.mark.parametrize(
    ("source_id", "ignored_prefixes", "expected_prefix"),
    [
        ("quick.999", ("quick.", "test"), "quick."),
        ("noise.999", ("noise.",), "noise."),
    ],
    ids=("default-prefix", "custom-prefix"),
)
def test_transform_direction_drops_ignored_prefix_before_metadata_lookup(
    source_id: str,
    ignored_prefixes: tuple[str, ...],
    expected_prefix: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline"):
        outcome = transform_direction_window(
            [
                direction_row(
                    from_group_place_id=source_id,
                    to_group_place_id="sendai202603.999",
                )
            ],
            {},
            aggregate_entity_id=AGGREGATE_ENTITY_ID,
            aggregate_entity_type=AGGREGATE_ENTITY_TYPE,
            ignored_place_prefixes=ignored_prefixes,
            now=fixed_clock,
        )

    assert isinstance(outcome, DirectionNoPayloadOutcome)
    assert outcome.rows_dropped == 1
    [ignored_record] = records(caplog, "ignored_place_prefix")
    assert ignored_record.matched_prefix == expected_prefix
    assert records(caplog, "unknown_place_interval") == []


def test_transform_direction_rejects_pairwise_candidate_without_totals() -> None:
    metadata_index = {
        (105, 60): place(place_number=105),
        (106, 60): place(
            place_number=106,
            entity_id="jp.sendai.Blesensor.per3600.106",
        ),
    }
    rows = [
        direction_row(count=12),
        *complete_totals("sendai202603.105", from_all=85, to_all=82),
    ]

    outcome = transform(rows, metadata_index)

    assert isinstance(outcome, DirectionSourceInvalidOutcome)
    assert not isinstance(outcome, DirectionNoPayloadOutcome)
    assert outcome.missing_from_all_place_numbers == (106,)
    assert outcome.missing_to_all_place_numbers == (106,)
    assert outcome.rows_dropped == 0
    assert not hasattr(outcome, "payload")


def test_transform_direction_sorts_multiple_missing_total_places_ascending() -> None:
    metadata_index = {
        (105, 60): place(place_number=105),
        (210, 60): place(
            place_number=210,
            entity_id="jp.sendai.Blesensor.per3600.210",
        ),
    }
    rows = [
        direction_row(
            from_group_place_id="sendai202603.210",
            to_group_place_id="sendai202603.105",
        )
    ]

    outcome = transform(rows, metadata_index)

    assert isinstance(outcome, DirectionSourceInvalidOutcome)
    assert outcome.missing_from_all_place_numbers == (105, 210)
    assert outcome.missing_to_all_place_numbers == (105, 210)
    assert outcome.rows_dropped == 0
    assert not hasattr(outcome, "payload")


@pytest.mark.parametrize(
    ("row", "missing_from", "missing_to"),
    [
        (
            direction_row(
                from_group_place_id="ALL",
                to_group_place_id="sendai202603.106",
                count=8,
            ),
            (),
            (106,),
        ),
        (
            direction_row(
                from_group_place_id="sendai202603.106",
                to_group_place_id="ALL",
                count=6,
            ),
            (106,),
            (),
        ),
    ],
    ids=("only-all-to-place", "only-place-to-all"),
)
def test_transform_direction_rejects_candidate_whose_only_row_is_one_total(
    row: dict[str, Any],
    missing_from: tuple[int, ...],
    missing_to: tuple[int, ...],
) -> None:
    metadata_index = {
        (105, 60): place(place_number=105),
        (106, 60): place(
            place_number=106,
            entity_id="jp.sendai.Blesensor.per3600.106",
        ),
    }
    outcome = transform(
        [
            *complete_totals("sendai202603.105", from_all=5, to_all=4),
            row,
        ],
        metadata_index,
    )

    assert isinstance(outcome, DirectionSourceInvalidOutcome)
    assert outcome.missing_from_all_place_numbers == missing_from
    assert outcome.missing_to_all_place_numbers == missing_to
    assert outcome.rows_dropped == 0
    assert not hasattr(outcome, "payload")
