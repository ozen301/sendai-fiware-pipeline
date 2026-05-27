"""Transform direction metric rows into NGSI v2 attribute payloads."""

import logging
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from sendai_pipeline.metadata import SensorPlace

logger = logging.getLogger(__name__)

JST = timezone(timedelta(hours=9))
_ALL = "ALL"
_ALLOWED_INTERVALS = frozenset({5, 60})
_SOURCE_BATCH_PREFIXES = {
    "sendai2023.": "2023",
    "sendai202603.": "2026",
}

type _FlowSide = dict[str, int | None]
type _FlowValue = dict[str, _FlowSide]


@dataclass(frozen=True)
class TransformDirectionResult:
    """Outcome of transforming a batch of direction metric rows.

    Attributes:
        payloads: Orion-ready attribute payloads, one per active target per
            window — including sentinel payloads for targets with no
            observations.
        rows_dropped: Source rows the transform filtered out before they
            could contribute to any payload (unsupported interval, noise
            prefix, metadata miss, batch mismatch, device-type mismatch, or
            self-loop).
    """

    payloads: list[dict[str, Any]]
    rows_dropped: int


@dataclass
class _Window:
    observed_from: datetime
    interval_min: int
    flows_by_place: dict[int, _FlowValue] = field(default_factory=dict)


def transform_direction_rows(
    rows: Iterable[Mapping[str, Any]],
    metadata_index: Mapping[tuple[int, int], SensorPlace],
    *,
    ignored_place_prefixes: tuple[str, ...] = ("quick.", "test"),
    now: Callable[[], datetime] | None = None,
) -> TransformDirectionResult:
    """Transform direction metric rows into Orion-ready attribute payloads.

    Args:
        rows: Source rows returned from a dict-style database cursor.
        metadata_index: Sensor metadata keyed by place number and aggregation
            interval.
        ignored_place_prefixes: Source place-key prefixes to filter before
            metadata lookup.
        now: Optional clock used to populate ``dateRetrieved``. When omitted,
            the current JST wall-clock time is used.

    Returns:
        A ``TransformDirectionResult`` whose ``payloads`` carry ``entity_id``,
        ``entity_type``, and ``attrs`` keys. Each supported source window emits
        one payload for every active metadata target in the same interval.
        Targets with no valid observations receive a ``peopleCount_flow``
        sentinel with ``null`` all values. ``rows_dropped`` counts source rows
        the transform filtered out before they could contribute to a payload.
    """
    active_index = {key: place for key, place in metadata_index.items() if place.active}
    active_places_by_interval = _active_places_by_interval(active_index.values())
    windows: dict[tuple[str, int], _Window] = {}
    rows_dropped = 0

    for row in rows:
        interval_min = int(row["interval_min"])
        if interval_min not in _ALLOWED_INTERVALS:
            rows_dropped += 1
            continue

        from_group_place_id = str(row["from_group_place_id"])
        to_group_place_id = str(row["to_group_place_id"])
        matched_prefix = _matched_row_prefix(
            from_group_place_id,
            to_group_place_id,
            ignored_place_prefixes,
        )
        if matched_prefix is not None:
            logger.debug(
                "ignored direction metric row by place prefix",
                extra={
                    "event": "ignored_place_prefix",
                    "from_group_place_id": from_group_place_id,
                    "to_group_place_id": to_group_place_id,
                    "matched_prefix": matched_prefix,
                },
            )
            rows_dropped += 1
            continue

        startdate = str(row["startdate"])
        window_key = (startdate, interval_min)
        window = windows.get(window_key)
        if window is None:
            window = _Window(
                observed_from=_parse_startdate(startdate),
                interval_min=interval_min,
            )
            windows[window_key] = window

        from_place = _resolve_place(
            from_group_place_id,
            interval_min,
            active_index,
            from_group_place_id=from_group_place_id,
            to_group_place_id=to_group_place_id,
        )
        to_place = _resolve_place(
            to_group_place_id,
            interval_min,
            active_index,
            from_group_place_id=from_group_place_id,
            to_group_place_id=to_group_place_id,
        )
        if _resolution_failed(from_group_place_id, from_place) or _resolution_failed(
            to_group_place_id, to_place
        ):
            rows_dropped += 1
            continue

        from_device_type = str(row["from_device_type"])
        to_device_type = str(row["to_device_type"])

        if from_place is not None and to_place is not None:
            if from_place.place_number == to_place.place_number:
                rows_dropped += 1
                continue
            if from_place.batch != to_place.batch:
                if from_device_type == to_device_type:
                    logger.warning(
                        "direction metric row crosses metadata batches",
                        extra={
                            "event": "cross_batch_pair",
                            "from_group_place_id": from_group_place_id,
                            "to_group_place_id": to_group_place_id,
                            "from_device_type": from_device_type,
                            "to_device_type": to_device_type,
                            "interval_min": interval_min,
                        },
                    )
                rows_dropped += 1
                continue

        expected_place = from_place if to_place is None else to_place
        if expected_place is None:
            rows_dropped += 1
            continue

        if not _device_types_match(
            expected_place,
            from_device_type=from_device_type,
            to_device_type=to_device_type,
            from_group_place_id=from_group_place_id,
            to_group_place_id=to_group_place_id,
            interval_min=interval_min,
        ):
            rows_dropped += 1
            continue

        count = int(row["count"])
        if from_group_place_id == _ALL and to_place is not None:
            _flow_for(window, to_place.place_number)["from"]["all"] = count
        elif to_group_place_id == _ALL and from_place is not None:
            _flow_for(window, from_place.place_number)["to"]["all"] = count
        elif from_place is not None and to_place is not None:
            _flow_for(window, from_place.place_number)["to"][
                str(to_place.place_number)
            ] = count
            _flow_for(window, to_place.place_number)["from"][
                str(from_place.place_number)
            ] = count

    if not windows:
        return TransformDirectionResult(payloads=[], rows_dropped=rows_dropped)

    retrieved_at = _retrieved_at(now)
    payloads: list[dict[str, Any]] = []
    for window in windows.values():
        for place in active_places_by_interval.get(window.interval_min, ()):
            payloads.append(
                {
                    "entity_id": place.entity_id,
                    "entity_type": place.entity_type,
                    "attrs": _attrs(
                        place=place,
                        window=window,
                        retrieved_at=retrieved_at,
                    ),
                }
            )

    return TransformDirectionResult(payloads=payloads, rows_dropped=rows_dropped)


def _active_places_by_interval(
    places: Iterable[SensorPlace],
) -> dict[int, list[SensorPlace]]:
    """Group active metadata places by aggregation interval.

    Used to emit one payload per active target per window so that targets with
    no surviving rows still receive a sentinel ``peopleCount_flow``.
    """
    places_by_interval: dict[int, list[SensorPlace]] = {}
    for place in places:
        if place.interval_min in _ALLOWED_INTERVALS:
            places_by_interval.setdefault(place.interval_min, []).append(place)
    return places_by_interval


def _matched_row_prefix(
    from_group_place_id: str,
    to_group_place_id: str,
    prefixes: Iterable[str],
) -> str | None:
    """Return the noise prefix matched on either side, or None.

    The literal ``ALL`` aggregate marker is exempt from prefix matching — only
    real source place keys are inspected.
    """
    for group_place_id in (from_group_place_id, to_group_place_id):
        if group_place_id == _ALL:
            continue
        matched_prefix = _matched_prefix(group_place_id, prefixes)
        if matched_prefix is not None:
            return matched_prefix
    return None


def _matched_prefix(value: str, prefixes: Iterable[str]) -> str | None:
    for prefix in prefixes:
        if value.startswith(prefix):
            return prefix
    return None


def _resolve_place(
    group_place_id: str,
    interval_min: int,
    metadata_index: Mapping[tuple[int, int], SensorPlace],
    *,
    from_group_place_id: str,
    to_group_place_id: str,
) -> SensorPlace | None:
    """Look up the metadata target for a source place key.

    Returns ``None`` for the ``ALL`` aggregate marker, for unparseable keys,
    when no metadata exists for the (place_number, interval) pair, or when the
    source-side batch derived from the key prefix disagrees with the metadata
    batch (guards against place-number collisions between the 2023 and 2026
    install batches).
    """
    if group_place_id == _ALL:
        return None

    parsed = _parse_source_place(group_place_id)
    if parsed is None:
        return None

    place_number, source_batch = parsed
    place = metadata_index.get((place_number, interval_min))
    if place is None or source_batch != place.batch:
        logger.debug(
            "direction metric row has no metadata target",
            extra={
                "event": "unknown_place_interval",
                "from_group_place_id": from_group_place_id,
                "to_group_place_id": to_group_place_id,
                "place_number": place_number,
                "interval_min": interval_min,
            },
        )
        return None

    return place


def _parse_source_place(group_place_id: str) -> tuple[int, str | None] | None:
    """Split a source place key into (place_number, batch).

    Batch is the install-batch tag derived from the key prefix (``"2023"`` or
    ``"2026"``), or ``None`` when the key has no recognized batch prefix; the
    caller then treats the row as a batch-mismatch and skips it.
    """
    try:
        place_number = int(group_place_id.rsplit(".", 1)[-1])
    except ValueError:
        return None

    for prefix, batch in _SOURCE_BATCH_PREFIXES.items():
        if group_place_id.startswith(prefix):
            return place_number, batch

    return place_number, None


def _resolution_failed(group_place_id: str, place: SensorPlace | None) -> bool:
    """True when a non-ALL side resolved to no metadata target (drop the row)."""
    return group_place_id != _ALL and place is None


def _device_types_match(
    place: SensorPlace,
    *,
    from_device_type: str,
    to_device_type: str,
    from_group_place_id: str,
    to_group_place_id: str,
    interval_min: int,
) -> bool:
    """True when both row sides report the metadata target's expected device type.

    The source table emits parallel rows under both ``(Pixel3aUT, Pixel3aUT)``
    and ``(M5Stack, M5Stack)`` for every per-place target, so this filter is a
    required disambiguator — without it every count would be double-counted.
    """
    if (
        from_device_type == place.expected_device_type
        and to_device_type == place.expected_device_type
    ):
        return True

    logger.debug(
        "direction metric row device type does not match metadata",
        extra={
            "event": "device_mismatch",
            "from_group_place_id": from_group_place_id,
            "to_group_place_id": to_group_place_id,
            "from_device_type": from_device_type,
            "to_device_type": to_device_type,
            "place_number": place.place_number,
            "interval_min": interval_min,
            "expected_device_type": place.expected_device_type,
        },
    )
    return False


def _flow_for(window: _Window, place_number: int) -> _FlowValue:
    """Return the mutable flow bucket for a place, seeding the sentinel on first use."""
    return window.flows_by_place.setdefault(place_number, _sentinel_flow())


def _sentinel_flow() -> _FlowValue:
    """Default ``peopleCount_flow`` value meaning 'no observation this window'.

    Distinct from an observed zero: ``null`` here signals the sensor reported
    nothing, while ``0`` would mean it reported zero people.
    """
    return {"from": {"all": None}, "to": {"all": None}}


def _attrs(
    *,
    place: SensorPlace,
    window: _Window,
    retrieved_at: datetime,
) -> dict[str, Any]:
    observed_to = window.observed_from + timedelta(minutes=window.interval_min)
    timeinstant_value = window.observed_from.isoformat()

    return {
        "identifcation": {
            "type": "Text",
            "value": place.identifcation,
            "metadata": _timeinstant_metadata(timeinstant_value),
        },
        "dateObservedFrom": {
            "type": "DateTime",
            "value": window.observed_from.isoformat(),
            "metadata": _timeinstant_metadata(timeinstant_value),
        },
        "dateObservedTo": {
            "type": "DateTime",
            "value": observed_to.isoformat(),
            "metadata": _timeinstant_metadata(timeinstant_value),
        },
        "dateRetrieved": {
            "type": "DateTime",
            "value": retrieved_at.isoformat(),
            "metadata": _timeinstant_metadata(timeinstant_value),
        },
        "peopleCount_flow": {
            "type": "StructuredValue",
            "value": _flow_value(window, place.place_number),
            "metadata": _timeinstant_metadata(timeinstant_value),
        },
    }


def _flow_value(window: _Window, place_number: int) -> _FlowValue:
    """Snapshot a place's flow bucket for payload emission.

    Returns a shallow copy so later mutations to the window's internal state
    can't leak into already-emitted payloads.
    """
    observed = window.flows_by_place.get(place_number)
    if observed is None:
        return _sentinel_flow()
    return {
        "from": dict(observed["from"]),
        "to": dict(observed["to"]),
    }


def _parse_startdate(value: str) -> datetime:
    """Parse a ``YYYYMMDD_HHMM`` source startdate string as JST."""
    return datetime.strptime(value, "%Y%m%d_%H%M").replace(tzinfo=JST)


def _retrieved_at(now: Callable[[], datetime] | None) -> datetime:
    """Resolve the ``dateRetrieved`` timestamp, normalizing to JST.

    Naive values from the injected clock are assumed JST; aware values are
    converted. Falls back to the real wall clock when no clock is injected.
    The result is truncated to whole seconds because Orion v2 rejects
    DateTime values whose fractional-second precision exceeds milliseconds.
    """
    if now is None:
        value = datetime.now(JST)
    else:
        value = now()
        if value.tzinfo is None:
            value = value.replace(tzinfo=JST)
        else:
            value = value.astimezone(JST)
    return value.replace(microsecond=0)


def _timeinstant_metadata(value: str) -> dict[str, dict[str, str]]:
    """NGSI metadata block telling STH-Comet which timestamp to index history by."""
    return {"TimeInstant": {"type": "DateTime", "value": value}}
