"""Transform flow metric rows into NGSI v2 attribute payloads."""

import logging
from collections.abc import Iterable, Mapping
from datetime import datetime, timedelta, timezone
from typing import Any

from sendai_pipeline.metadata import SensorPlace

logger = logging.getLogger(__name__)

JST = timezone(timedelta(hours=9))
_ALLOWED_INTERVALS = frozenset({5, 60})


def transform_flow_rows(
    rows: Iterable[Mapping[str, Any]],
    metadata_index: Mapping[tuple[int, int], SensorPlace],
    *,
    ignored_place_prefixes: tuple[str, ...] = ("quick.", "test"),
) -> list[dict[str, Any]]:
    """Return Orion-ready attribute payloads for flow metric rows.

    Args:
        rows: Source rows returned from a dict-style database cursor.
        metadata_index: Active sensor metadata keyed by place number and
            aggregation interval.
        ignored_place_prefixes: ``group_place_id`` prefixes to filter before
            metadata lookup.

    Returns:
        Payload dictionaries containing ``entity_id``, ``entity_type``, and
        ``attrs`` keys. Rows with unsupported intervals, configured noise
        prefixes, unknown metadata keys, or device-type mismatches are omitted.
    """
    payloads: list[dict[str, Any]] = []

    for row in rows:
        interval_min = row["interval_min"]
        if interval_min not in _ALLOWED_INTERVALS:
            continue

        group_place_id = row["group_place_id"]
        matched_prefix = _matched_prefix(group_place_id, ignored_place_prefixes)
        if matched_prefix is not None:
            logger.debug(
                "ignored flow metric row by place prefix",
                extra={
                    "event": "ignored_place_prefix",
                    "group_place_id": group_place_id,
                    "matched_prefix": matched_prefix,
                },
            )
            continue

        place_number = int(group_place_id.rsplit(".", 1)[-1])
        place = metadata_index.get((place_number, interval_min))
        if place is None:
            logger.debug(
                "flow metric row has no metadata target",
                extra={
                    "event": "unknown_place_interval",
                    "group_place_id": group_place_id,
                    "place_number": place_number,
                    "interval_min": interval_min,
                },
            )
            continue

        device_type = row["device_type"]
        if device_type != place.expected_device_type:
            logger.debug(
                "flow metric row device type does not match metadata",
                extra={
                    "event": "device_mismatch",
                    "place_number": place_number,
                    "interval_min": interval_min,
                    "device_type": device_type,
                    "expected_device_type": place.expected_device_type,
                },
            )
            continue

        payloads.append(
            {
                "entity_id": place.entity_id,
                "entity_type": place.entity_type,
                "attrs": _attrs(row, interval_min),
            }
        )

    return payloads


def _matched_prefix(value: str, prefixes: Iterable[str]) -> str | None:
    for prefix in prefixes:
        if value.startswith(prefix):
            return prefix
    return None


def _attrs(row: Mapping[str, Any], interval_min: int) -> dict[str, Any]:
    observed_from = datetime.strptime(row["startdate"], "%Y%m%d_%H%M").replace(
        tzinfo=JST
    )
    observed_to = observed_from + timedelta(minutes=interval_min)
    timeinstant_value = observed_from.isoformat()

    return {
        "dateObservedFrom": {
            "type": "DateTime",
            "value": observed_from.isoformat(),
            "metadata": _timeinstant_metadata(timeinstant_value),
        },
        "dateObservedTo": {
            "type": "DateTime",
            "value": observed_to.isoformat(),
            "metadata": _timeinstant_metadata(timeinstant_value),
        },
        "peopleCount_immedate": {
            "type": "number",
            "value": row["flow_gt_m60"],
            "metadata": _timeinstant_metadata(timeinstant_value),
        },
        "peopleCount_near": {
            "type": "number",
            "value": row["flow_gt_m80"],
            "metadata": _timeinstant_metadata(timeinstant_value),
        },
        "peopleCount_far": {
            "type": "number",
            "value": row["flow_gt_m120"],
            "metadata": _timeinstant_metadata(timeinstant_value),
        },
        "peopleOccupancy_immedate": {
            "type": "number",
            "value": _float_or_none(row["stay_gt_m60"]),
            "metadata": _timeinstant_metadata(timeinstant_value),
        },
        "peopleOccupancy_near": {
            "type": "number",
            "value": _float_or_none(row["stay_gt_m80"]),
            "metadata": _timeinstant_metadata(timeinstant_value),
        },
    }


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def _timeinstant_metadata(value: str) -> dict[str, dict[str, str]]:
    """Return TimeInstant metadata for STH-Comet source-time storage."""
    return {"TimeInstant": {"type": "DateTime", "value": value}}
