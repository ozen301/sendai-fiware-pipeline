import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from sendai_pipeline.logging_setup import _ALLOWED_EXTRA_KEYS
from sendai_pipeline.metadata import SensorPlace, index_by_place_interval, load_metadata
from sendai_pipeline.transform_direction import (
    TransformDirectionResult,
    transform_direction_rows,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures"
JST = timezone(timedelta(hours=9))

TRANSFORM_EVENTS = frozenset(
    {
        "ignored_place_prefix",
        "unknown_place_interval",
        "device_mismatch",
        "cross_batch_pair",
    }
)

FIXED_NOW = datetime(2026, 5, 24, 13, 25, 43, tzinfo=JST)
FIXED_NOW_ISO = "2026-05-24T13:25:43+09:00"


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


def timeinstant(value: str = "2026-05-23T09:00:00+09:00") -> dict[str, Any]:
    return {"TimeInstant": {"type": "DateTime", "value": value}}


def records(caplog: pytest.LogCaptureFixture, event: str) -> list[Any]:
    return [
        record for record in caplog.records if getattr(record, "event", None) == event
    ]


def payloads_by_entity(
    result: TransformDirectionResult,
) -> dict[str, dict[str, Any]]:
    return {payload["entity_id"]: payload for payload in result.payloads}


def test_transform_direction_emits_per3600_payload_with_pairwise_and_all_keys() -> None:
    metadata_index = {
        (105, 60): place(place_number=105, identifcation="105"),
        (106, 60): place(
            place_number=106,
            entity_id="jp.sendai.Blesensor.per3600.106",
            identifcation="106",
        ),
    }

    rows = [
        direction_row(
            from_group_place_id="sendai202603.105",
            to_group_place_id="sendai202603.106",
            count=12,
        ),
        direction_row(
            from_group_place_id="sendai202603.106",
            to_group_place_id="sendai202603.105",
            count=9,
        ),
        direction_row(
            from_group_place_id="ALL",
            to_group_place_id="sendai202603.105",
            count=85,
        ),
        direction_row(
            from_group_place_id="sendai202603.105",
            to_group_place_id="ALL",
            count=82,
        ),
        direction_row(
            from_group_place_id="ALL",
            to_group_place_id="sendai202603.106",
            count=67,
        ),
        direction_row(
            from_group_place_id="sendai202603.106",
            to_group_place_id="ALL",
            count=71,
        ),
    ]

    result = transform_direction_rows(rows, metadata_index, now=fixed_clock)
    by_entity = payloads_by_entity(result)

    assert set(by_entity) == {
        "jp.sendai.Blesensor.per3600.105",
        "jp.sendai.Blesensor.per3600.106",
    }
    payload_105 = by_entity["jp.sendai.Blesensor.per3600.105"]
    assert payload_105["entity_type"] == "Blesensor.per3600"
    assert payload_105["attrs"]["identifcation"] == {
        "type": "Text",
        "value": "105",
        "metadata": timeinstant(),
    }
    assert payload_105["attrs"]["dateObservedFrom"] == {
        "type": "DateTime",
        "value": "2026-05-23T09:00:00+09:00",
        "metadata": timeinstant(),
    }
    assert payload_105["attrs"]["dateObservedTo"] == {
        "type": "DateTime",
        "value": "2026-05-23T10:00:00+09:00",
        "metadata": timeinstant(),
    }
    assert payload_105["attrs"]["dateRetrieved"] == {
        "type": "DateTime",
        "value": FIXED_NOW_ISO,
        "metadata": timeinstant(),
    }
    assert payload_105["attrs"]["peopleCount_flow"] == {
        "type": "StructuredValue",
        "value": {
            "from": {"all": 85, "106": 9},
            "to": {"all": 82, "106": 12},
        },
        "metadata": timeinstant(),
    }
    payload_106 = by_entity["jp.sendai.Blesensor.per3600.106"]
    assert payload_106["attrs"]["identifcation"]["value"] == "106"
    assert payload_106["attrs"]["peopleCount_flow"]["value"] == {
        "from": {"all": 67, "105": 12},
        "to": {"all": 71, "105": 9},
    }


def test_transform_direction_builds_per300_payload_for_five_minute_window() -> None:
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

    result = transform_direction_rows(
        [
            direction_row(
                startdate="20260523_2355",
                from_group_place_id="ALL",
                to_group_place_id="sendai2023.10",
                from_device_type="Pixel3aUT",
                to_device_type="Pixel3aUT",
                interval_min=5,
                count=4,
            )
        ],
        metadata_index,
        now=fixed_clock,
    )

    assert len(result.payloads) == 1
    payload = result.payloads[0]
    assert payload["entity_id"] == "jp.sendai.Blesensor.per300.10"
    assert payload["entity_type"] == "Blesensor.per300"
    assert payload["attrs"]["dateObservedFrom"]["value"] == (
        "2026-05-23T23:55:00+09:00"
    )
    assert payload["attrs"]["dateObservedTo"]["value"] == ("2026-05-24T00:00:00+09:00")
    assert payload["attrs"]["dateRetrieved"]["value"] == FIXED_NOW_ISO
    assert payload["attrs"]["peopleCount_flow"]["value"] == {
        "from": {"all": 4},
        "to": {"all": None},
    }


def test_transform_direction_returns_empty_list_for_empty_input(
    caplog: pytest.LogCaptureFixture,
) -> None:
    metadata_index = {(105, 60): place(place_number=105)}

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline"):
        result = transform_direction_rows([], metadata_index, now=fixed_clock)

    assert result == TransformDirectionResult(payloads=[], rows_dropped=0)
    assert caplog.records == []


def test_transform_direction_emits_sentinel_for_target_with_no_rows_in_window() -> None:
    metadata_index = {
        (105, 60): place(place_number=105, identifcation="105"),
        (106, 60): place(
            place_number=106,
            entity_id="jp.sendai.Blesensor.per3600.106",
            identifcation="106",
        ),
    }

    result = transform_direction_rows(
        [
            direction_row(
                from_group_place_id="ALL",
                to_group_place_id="sendai202603.105",
                count=85,
            ),
        ],
        metadata_index,
        now=fixed_clock,
    )
    by_entity = payloads_by_entity(result)

    assert set(by_entity) == {
        "jp.sendai.Blesensor.per3600.105",
        "jp.sendai.Blesensor.per3600.106",
    }
    assert by_entity["jp.sendai.Blesensor.per3600.105"]["attrs"]["peopleCount_flow"][
        "value"
    ] == {
        "from": {"all": 85},
        "to": {"all": None},
    }
    assert by_entity["jp.sendai.Blesensor.per3600.106"]["attrs"]["peopleCount_flow"][
        "value"
    ] == {
        "from": {"all": None},
        "to": {"all": None},
    }


def test_transform_direction_skips_inactive_metadata_entries() -> None:
    metadata_index = {
        (105, 60): place(place_number=105, identifcation="105"),
        (999, 60): place(
            place_number=999,
            entity_id="jp.sendai.Blesensor.per3600.999",
            identifcation="999",
            active=False,
        ),
    }

    result = transform_direction_rows(
        [
            direction_row(
                from_group_place_id="ALL",
                to_group_place_id="sendai202603.105",
                count=85,
            )
        ],
        metadata_index,
        now=fixed_clock,
    )
    by_entity = payloads_by_entity(result)

    assert set(by_entity) == {"jp.sendai.Blesensor.per3600.105"}


def test_transform_direction_preserves_observed_zero_distinct_from_null() -> None:
    metadata_index = {
        (105, 60): place(place_number=105),
        (106, 60): place(
            place_number=106,
            entity_id="jp.sendai.Blesensor.per3600.106",
        ),
    }

    result = transform_direction_rows(
        [
            direction_row(
                from_group_place_id="sendai202603.106",
                to_group_place_id="sendai202603.105",
                count=0,
            ),
            direction_row(
                from_group_place_id="ALL",
                to_group_place_id="sendai202603.105",
                count=0,
            ),
        ],
        metadata_index,
        now=fixed_clock,
    )
    by_entity = payloads_by_entity(result)
    payload_105 = by_entity["jp.sendai.Blesensor.per3600.105"]

    assert payload_105["attrs"]["peopleCount_flow"]["value"] == {
        "from": {"all": 0, "106": 0},
        "to": {"all": None},
    }


def test_transform_direction_drops_unsupported_interval_without_log(
    caplog: pytest.LogCaptureFixture,
) -> None:
    metadata_index = {(105, 60): place(place_number=105)}

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline"):
        result = transform_direction_rows(
            [direction_row(interval_min=1)],
            metadata_index,
            now=fixed_clock,
        )

    by_entity = payloads_by_entity(result)
    assert "jp.sendai.Blesensor.per3600.105" not in by_entity
    assert caplog.records == []


def test_transform_direction_drops_default_noise_prefix_before_metadata_lookup(
    caplog: pytest.LogCaptureFixture,
) -> None:
    metadata_index = {(105, 60): place(place_number=105)}

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline"):
        transform_direction_rows(
            [
                direction_row(
                    from_group_place_id="quick.106",
                    to_group_place_id="sendai202603.105",
                ),
                direction_row(
                    from_group_place_id="sendai202603.105",
                    to_group_place_id="test.106",
                ),
            ],
            metadata_index,
            now=fixed_clock,
        )

    ignored = records(caplog, "ignored_place_prefix")
    assert {record.matched_prefix for record in ignored} == {"quick.", "test"}
    assert all(record.levelname == "DEBUG" for record in ignored)
    assert records(caplog, "unknown_place_interval") == []


def test_transform_direction_does_not_treat_literal_all_as_noise_prefix(
    caplog: pytest.LogCaptureFixture,
) -> None:
    metadata_index = {(105, 60): place(place_number=105)}

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline"):
        result = transform_direction_rows(
            [
                direction_row(
                    from_group_place_id="ALL",
                    to_group_place_id="sendai202603.105",
                    count=85,
                ),
            ],
            metadata_index,
            now=fixed_clock,
        )

    by_entity = payloads_by_entity(result)
    assert (
        by_entity["jp.sendai.Blesensor.per3600.105"]["attrs"]["peopleCount_flow"][
            "value"
        ]["from"]["all"]
        == 85
    )
    assert records(caplog, "ignored_place_prefix") == []


def test_transform_direction_uses_custom_noise_prefixes_instead_of_defaults(
    caplog: pytest.LogCaptureFixture,
) -> None:
    metadata_index = {
        (105, 60): place(place_number=105),
        (106, 60): place(
            place_number=106,
            entity_id="jp.sendai.Blesensor.per3600.106",
        ),
    }

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline"):
        result = transform_direction_rows(
            [
                direction_row(
                    from_group_place_id="foo.106",
                    to_group_place_id="sendai202603.105",
                ),
                direction_row(
                    from_group_place_id="quick.106",
                    to_group_place_id="sendai202603.105",
                    count=7,
                ),
            ],
            metadata_index,
            ignored_place_prefixes=("foo.",),
            now=fixed_clock,
        )

    by_entity = payloads_by_entity(result)
    assert by_entity["jp.sendai.Blesensor.per3600.105"]["attrs"]["peopleCount_flow"][
        "value"
    ]["from"] == {"all": None}
    ignored = records(caplog, "ignored_place_prefix")
    assert [record.matched_prefix for record in ignored] == ["foo."]


def test_transform_direction_logs_unknown_pairwise_place_at_debug(
    caplog: pytest.LogCaptureFixture,
) -> None:
    metadata_index = {(105, 60): place(place_number=105)}

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline"):
        result = transform_direction_rows(
            [
                direction_row(
                    from_group_place_id="sendai2023.1001",
                    to_group_place_id="sendai202603.105",
                    count=3,
                ),
            ],
            metadata_index,
            now=fixed_clock,
        )

    by_entity = payloads_by_entity(result)
    assert by_entity["jp.sendai.Blesensor.per3600.105"]["attrs"]["peopleCount_flow"][
        "value"
    ] == {
        "from": {"all": None},
        "to": {"all": None},
    }
    unknown = records(caplog, "unknown_place_interval")
    assert len(unknown) == 1
    assert unknown[0].levelname == "DEBUG"
    assert unknown[0].place_number == 1001
    assert unknown[0].interval_min == 60


def test_transform_direction_logs_device_mismatch_at_debug(
    caplog: pytest.LogCaptureFixture,
) -> None:
    metadata_index = {(105, 60): place(place_number=105)}

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline"):
        result = transform_direction_rows(
            [
                direction_row(
                    from_group_place_id="ALL",
                    to_group_place_id="sendai202603.105",
                    from_device_type="Pixel3aUT",
                    to_device_type="Pixel3aUT",
                    count=99,
                ),
            ],
            metadata_index,
            now=fixed_clock,
        )

    by_entity = payloads_by_entity(result)
    assert by_entity["jp.sendai.Blesensor.per3600.105"]["attrs"]["peopleCount_flow"][
        "value"
    ] == {
        "from": {"all": None},
        "to": {"all": None},
    }
    mismatches = records(caplog, "device_mismatch")
    assert len(mismatches) == 1
    assert mismatches[0].levelname == "DEBUG"
    assert mismatches[0].expected_device_type == "M5Stack"


def test_transform_direction_drops_mixed_device_type_rows() -> None:
    metadata_index = {(105, 60): place(place_number=105)}

    result = transform_direction_rows(
        [
            direction_row(
                from_group_place_id="ALL",
                to_group_place_id="sendai202603.105",
                from_device_type="Pixel3aUT",
                to_device_type="M5Stack",
                count=99,
            ),
        ],
        metadata_index,
        now=fixed_clock,
    )

    by_entity = payloads_by_entity(result)
    assert by_entity["jp.sendai.Blesensor.per3600.105"]["attrs"]["peopleCount_flow"][
        "value"
    ] == {
        "from": {"all": None},
        "to": {"all": None},
    }


def test_transform_direction_warns_and_drops_same_device_cross_batch_pair(
    caplog: pytest.LogCaptureFixture,
) -> None:
    metadata_index = {
        (10, 60): place(
            place_number=10,
            entity_id="jp.sendai.Blesensor.per3600.10",
            batch="2023",
            expected_device_type="Pixel3aUT",
        ),
        (105, 60): place(
            place_number=105,
            entity_id="jp.sendai.Blesensor.per3600.105",
            batch="2026",
            expected_device_type="M5Stack",
        ),
    }

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline"):
        result = transform_direction_rows(
            [
                direction_row(
                    from_group_place_id="sendai2023.10",
                    to_group_place_id="sendai202603.105",
                    from_device_type="M5Stack",
                    to_device_type="M5Stack",
                    count=3,
                ),
            ],
            metadata_index,
            now=fixed_clock,
        )

    by_entity = payloads_by_entity(result)
    for entity_id in (
        "jp.sendai.Blesensor.per3600.10",
        "jp.sendai.Blesensor.per3600.105",
    ):
        assert by_entity[entity_id]["attrs"]["peopleCount_flow"]["value"] == {
            "from": {"all": None},
            "to": {"all": None},
        }
    warns = records(caplog, "cross_batch_pair")
    assert len(warns) == 1
    assert warns[0].levelname == "WARNING"


def test_transform_direction_does_not_sum_pairwise_to_approximate_all() -> None:
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

    result = transform_direction_rows(
        [
            direction_row(
                from_group_place_id="sendai202603.106",
                to_group_place_id="sendai202603.105",
                count=10,
            ),
            direction_row(
                from_group_place_id="sendai202603.107",
                to_group_place_id="sendai202603.105",
                count=20,
            ),
        ],
        metadata_index,
        now=fixed_clock,
    )
    by_entity = payloads_by_entity(result)

    assert by_entity["jp.sendai.Blesensor.per3600.105"]["attrs"]["peopleCount_flow"][
        "value"
    ]["from"] == {"all": None, "106": 10, "107": 20}


def test_transform_direction_emits_one_payload_per_window_interval_and_target() -> None:
    metadata_index = {
        (105, 60): place(place_number=105),
        (106, 60): place(
            place_number=106,
            entity_id="jp.sendai.Blesensor.per3600.106",
        ),
        (105, 5): place(
            place_number=105,
            interval_min=5,
            entity_id="jp.sendai.Blesensor.per300.105",
            entity_type="Blesensor.per300",
        ),
        (106, 5): place(
            place_number=106,
            interval_min=5,
            entity_id="jp.sendai.Blesensor.per300.106",
            entity_type="Blesensor.per300",
        ),
    }

    result = transform_direction_rows(
        [
            direction_row(
                startdate="20260523_0900",
                from_group_place_id="ALL",
                to_group_place_id="sendai202603.105",
                interval_min=60,
                count=85,
            ),
            direction_row(
                startdate="20260523_0900",
                from_group_place_id="ALL",
                to_group_place_id="sendai202603.105",
                interval_min=5,
                count=7,
            ),
            direction_row(
                startdate="20260523_0905",
                from_group_place_id="ALL",
                to_group_place_id="sendai202603.106",
                interval_min=5,
                count=4,
            ),
        ],
        metadata_index,
        now=fixed_clock,
    )

    keyed = {
        (
            payload["entity_id"],
            payload["attrs"]["dateObservedFrom"]["value"],
        ): payload
        for payload in result.payloads
    }

    assert set(keyed) == {
        ("jp.sendai.Blesensor.per3600.105", "2026-05-23T09:00:00+09:00"),
        ("jp.sendai.Blesensor.per3600.106", "2026-05-23T09:00:00+09:00"),
        ("jp.sendai.Blesensor.per300.105", "2026-05-23T09:00:00+09:00"),
        ("jp.sendai.Blesensor.per300.106", "2026-05-23T09:00:00+09:00"),
        ("jp.sendai.Blesensor.per300.105", "2026-05-23T09:05:00+09:00"),
        ("jp.sendai.Blesensor.per300.106", "2026-05-23T09:05:00+09:00"),
    }
    assert (
        keyed[("jp.sendai.Blesensor.per3600.105", "2026-05-23T09:00:00+09:00")][
            "attrs"
        ]["peopleCount_flow"]["value"]["from"]["all"]
        == 85
    )
    assert keyed[("jp.sendai.Blesensor.per3600.106", "2026-05-23T09:00:00+09:00")][
        "attrs"
    ]["peopleCount_flow"]["value"] == {
        "from": {"all": None},
        "to": {"all": None},
    }
    assert (
        keyed[("jp.sendai.Blesensor.per300.105", "2026-05-23T09:00:00+09:00")]["attrs"][
            "peopleCount_flow"
        ]["value"]["from"]["all"]
        == 7
    )
    assert (
        keyed[("jp.sendai.Blesensor.per300.106", "2026-05-23T09:05:00+09:00")]["attrs"][
            "peopleCount_flow"
        ]["value"]["from"]["all"]
        == 4
    )


def test_transform_direction_filters_self_loops() -> None:
    metadata_index = {(105, 60): place(place_number=105)}

    result = transform_direction_rows(
        [
            direction_row(
                from_group_place_id="sendai202603.105",
                to_group_place_id="sendai202603.105",
                count=5,
            ),
        ],
        metadata_index,
        now=fixed_clock,
    )
    by_entity = payloads_by_entity(result)

    assert by_entity["jp.sendai.Blesensor.per3600.105"]["attrs"]["peopleCount_flow"][
        "value"
    ] == {
        "from": {"all": None},
        "to": {"all": None},
    }


def test_transform_direction_payload_is_json_serializable() -> None:
    metadata_index = {(105, 60): place(place_number=105)}

    result = transform_direction_rows(
        [
            direction_row(
                from_group_place_id="ALL",
                to_group_place_id="sendai202603.105",
                count=85,
            )
        ],
        metadata_index,
        now=fixed_clock,
    )

    [payload] = result.payloads
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    assert '"peopleCount_flow":{' in serialized
    assert '"StructuredValue"' in serialized
    assert '"all":85' in serialized
    assert '"identifcation":{' in serialized
    assert '"dateRetrieved":{' in serialized


def test_transform_direction_reads_entity_and_identifcation_from_metadata() -> None:
    metadata_index = {
        (10, 60): place(
            place_number=10,
            interval_min=60,
            entity_id="jp.sendai.Blesensor.per3600.10.custom-suffix",
            entity_type="Custom.Blesensor.Type",
            batch="2023",
            expected_device_type="Pixel3aUT",
            identifcation="custom-identifcation",
        )
    }

    result = transform_direction_rows(
        [
            direction_row(
                from_group_place_id="ALL",
                to_group_place_id="sendai2023.10",
                from_device_type="Pixel3aUT",
                to_device_type="Pixel3aUT",
                count=3,
            )
        ],
        metadata_index,
        now=fixed_clock,
    )

    assert len(result.payloads) == 1
    payload = result.payloads[0]
    assert payload["entity_id"] == "jp.sendai.Blesensor.per3600.10.custom-suffix"
    assert payload["entity_type"] == "Custom.Blesensor.Type"
    assert payload["attrs"]["identifcation"]["value"] == "custom-identifcation"


def test_transform_direction_builds_payload_against_csv_loaded_metadata_index() -> None:
    metadata = load_metadata(FIXTURES_DIR / "sensors_minimal.csv")
    index = index_by_place_interval(metadata)

    result = transform_direction_rows(
        [
            direction_row(
                from_group_place_id="ALL",
                to_group_place_id="sendai202603.105",
                count=85,
            ),
            direction_row(
                from_group_place_id="ALL",
                to_group_place_id="sendai2023.10",
                from_device_type="Pixel3aUT",
                to_device_type="Pixel3aUT",
                count=3,
            ),
        ],
        index,
        now=fixed_clock,
    )
    by_entity = payloads_by_entity(result)

    assert (
        by_entity["jp.sendai.Blesensor.per3600.105"]["attrs"]["peopleCount_flow"][
            "value"
        ]["from"]["all"]
        == 85
    )
    assert (
        by_entity["jp.sendai.Blesensor.per3600.10"]["attrs"]["peopleCount_flow"][
            "value"
        ]["from"]["all"]
        == 3
    )


def test_transform_direction_emits_only_known_structured_events(
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
    }

    with caplog.at_level(logging.DEBUG, logger="sendai_pipeline"):
        transform_direction_rows(
            [
                direction_row(
                    from_group_place_id="quick.106",
                    to_group_place_id="sendai202603.105",
                ),
                direction_row(
                    from_group_place_id="sendai2023.99",
                    to_group_place_id="sendai202603.105",
                ),
                direction_row(
                    from_group_place_id="ALL",
                    to_group_place_id="sendai202603.105",
                    from_device_type="Pixel3aUT",
                    to_device_type="Pixel3aUT",
                ),
                direction_row(
                    from_group_place_id="sendai2023.10",
                    to_group_place_id="sendai202603.105",
                    from_device_type="M5Stack",
                    to_device_type="M5Stack",
                ),
            ],
            metadata_index,
            now=fixed_clock,
        )

    events = [getattr(record, "event", None) for record in caplog.records]
    assert set(events) <= TRANSFORM_EVENTS


def test_transform_direction_counts_rows_dropped_for_each_filter_path() -> None:
    metadata_index = {
        (105, 60): place(place_number=105, identifcation="105"),
        (106, 60): place(
            place_number=106,
            entity_id="jp.sendai.Blesensor.per3600.106",
            identifcation="106",
        ),
    }
    rows = [
        direction_row(interval_min=15, count=1),
        direction_row(
            from_group_place_id="quick.105",
            to_group_place_id="sendai202603.106",
            count=2,
        ),
        direction_row(
            from_group_place_id="sendai202603.999",
            to_group_place_id="sendai202603.106",
            count=3,
        ),
        direction_row(
            from_group_place_id="sendai202603.105",
            to_group_place_id="sendai202603.105",
            count=4,
        ),
        direction_row(
            from_group_place_id="sendai202603.105",
            to_group_place_id="sendai202603.106",
            from_device_type="Pixel3aUT",
            to_device_type="M5Stack",
            count=5,
        ),
        direction_row(
            from_group_place_id="sendai202603.105",
            to_group_place_id="sendai202603.106",
            count=6,
        ),
    ]

    result = transform_direction_rows(rows, metadata_index, now=fixed_clock)

    assert result.rows_dropped == 5


def test_transform_direction_returns_zero_rows_dropped_when_all_rows_survive() -> None:
    metadata_index = {(105, 60): place(place_number=105, identifcation="105")}

    result = transform_direction_rows(
        [
            direction_row(
                from_group_place_id="ALL",
                to_group_place_id="sendai202603.105",
                count=85,
            ),
        ],
        metadata_index,
        now=fixed_clock,
    )

    assert result.rows_dropped == 0


def test_transform_direction_truncates_date_retrieved_to_whole_seconds() -> None:
    metadata_index = {(105, 60): place(place_number=105, identifcation="105")}
    micros_clock = datetime(2026, 5, 25, 11, 40, 3, 288677, tzinfo=JST)

    result = transform_direction_rows(
        [
            direction_row(
                from_group_place_id="ALL",
                to_group_place_id="sendai202603.105",
                count=1,
            ),
        ],
        metadata_index,
        now=lambda: micros_clock,
    )

    [payload] = result.payloads
    assert payload["attrs"]["dateRetrieved"]["value"] == "2026-05-25T11:40:03+09:00"


def test_transform_direction_logging_extras_are_allowed() -> None:
    required = {
        "from_group_place_id",
        "to_group_place_id",
        "from_device_type",
        "to_device_type",
        "place_number",
        "interval_min",
        "matched_prefix",
        "expected_device_type",
    }
    assert required <= _ALLOWED_EXTRA_KEYS
