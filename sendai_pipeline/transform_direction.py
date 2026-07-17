"""Transform one direction source window into an aggregate NGSI v2 payload."""

import logging
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from sendai_pipeline.metadata import SensorPlace

logger = logging.getLogger(__name__)

JST = timezone(timedelta(hours=9))
_ALL = "ALL"
_DIRECTION_INTERVAL_MIN = 60
_SOURCE_BATCH_PREFIXES = {
    "sendai2023.": "2023",
    "sendai202603.": "2026",
}


@dataclass(frozen=True)
class DirectionPayloadOutcome:
    """A clean or degraded aggregate direction payload.

    Attributes:
        payload: The Orion write for the aggregate entity, shaped as
            ``{"entity_id": str, "entity_type": str, "attrs": {...}}``. The
            top-level values are two strings plus the nested ``attrs`` mapping,
            and within ``attrs`` each attribute's own ``value`` also varies
            (a timestamp string, the entity id, or a place's dense
            ``from``/``to`` matrix), so ``payload`` is typed ``dict[str, Any]``.
            ``attrs`` always carries the four scalars ``dateObservedFrom``,
            ``dateObservedTo``, ``dateRetrieved``, and ``identifcation``, then
            ``sourceQuality`` and one ``peopleCount_flow_<N>`` per emitted
            place; each attribute is a ``{"type", "value", "metadata"}`` dict.
            A degraded payload omits candidates that lack either required
            total while retaining their observed routes on emitted places.
            For example::

                {
                    "entity_id": "jp.sendai.Blesensor.flow",
                    "entity_type": "Blesensor.flow",
                    "attrs": {
                        "dateObservedFrom": {
                            "type": "DateTime", "value": "...", "metadata": {...}
                        },
                        # dateObservedTo, dateRetrieved, identifcation likewise
                        "peopleCount_flow_105": {
                            "type": "StructuredValue",
                            "value": {
                                "from": {"105": 7, "all": 85},
                                "to": {"105": 7, "all": 82},
                            },
                            "metadata": {...},
                        },
                    },
                }

        excluded_place_numbers: Sorted candidate places omitted because one or
            both required totals are missing.
        missing_from_all_place_numbers: Sorted candidates without ``ALL -> N``.
        missing_to_all_place_numbers: Sorted candidates without ``N -> ALL``.
        rows_dropped: Count of source rows rejected by the transform filters.
    """

    payload: dict[str, Any]
    excluded_place_numbers: tuple[int, ...]
    missing_from_all_place_numbers: tuple[int, ...]
    missing_to_all_place_numbers: tuple[int, ...]
    rows_dropped: int


@dataclass(frozen=True)
class DirectionNoPayloadOutcome:
    """A direction window with no surviving candidate places.

    Attributes:
        rows_dropped: Count of source rows rejected by the transform filters.
    """

    rows_dropped: int


@dataclass(frozen=True)
class DirectionSourceInvalidOutcome:
    """A direction window whose every candidate lacks required totals.

    Attributes:
        missing_from_all_place_numbers: Candidate place numbers without a
            surviving ``ALL -> N`` source row.
        missing_to_all_place_numbers: Candidate place numbers without a
            surviving ``N -> ALL`` source row.
        rows_dropped: Count of source rows rejected by the transform filters.
    """

    missing_from_all_place_numbers: tuple[int, ...]
    missing_to_all_place_numbers: tuple[int, ...]
    rows_dropped: int


type DirectionTransformOutcome = (
    DirectionPayloadOutcome | DirectionNoPayloadOutcome | DirectionSourceInvalidOutcome
)


@dataclass
class _CandidateFlow:
    """One candidate place's surviving totals and pairwise routes.

    A candidate place is created the first time a surviving row references it;
    each field stays ``None``/empty until a row supplies its count. A self-loop
    ``N -> N`` row sets both ``incoming[N]`` and ``outgoing[N]``.

    Attributes:
        from_all: Count moving into this place from the ``ALL`` aggregate
            (source ``ALL -> N`` row). ``None`` until that row is seen.
        to_all: Count moving out of this place to the ``ALL`` aggregate
            (source ``N -> ALL`` row). ``None`` until that row is seen.
        incoming: Counts moving into this place, keyed by the other place
            number (from each surviving ``other -> N`` pairwise row).
        outgoing: Counts moving out of this place, keyed by the other place
            number (from each surviving ``N -> other`` pairwise row).
    """

    from_all: int | None = None
    to_all: int | None = None
    incoming: dict[int, int] = field(default_factory=dict)
    outgoing: dict[int, int] = field(default_factory=dict)


def transform_direction_window(
    rows: Iterable[Mapping[str, Any]],
    metadata_index: Mapping[tuple[int, int], SensorPlace],
    *,
    aggregate_entity_id: str,
    aggregate_entity_type: str,
    ignored_place_prefixes: tuple[str, ...] = ("quick.", "test"),
    now: Callable[[], datetime] | None = None,
) -> DirectionTransformOutcome:
    """Build one aggregate Product B payload from one source window.

    The transform accepts only 60-minute rows. It filters ignored source keys,
    unresolved or inactive metadata places, and rows whose device types do not
    match the oldest active source batch. Every place on a surviving row is a
    candidate. A candidate is emitted only when it has both source ``ALL``
    totals. When complete and incomplete candidates coexist, the incomplete
    candidates are excluded and the aggregate is degraded. The whole window
    is source-invalid only when every candidate is excluded.

    Args:
        rows: Direction source rows from a dict-style database cursor.
        metadata_index: Sensor metadata keyed by place number and interval.
        aggregate_entity_id: Entity id for the single aggregate payload.
        aggregate_entity_type: Entity type for the single aggregate payload.
        ignored_place_prefixes: Source-key prefixes filtered before metadata
            lookup.
        now: Optional clock for ``dateRetrieved``. Naive values are treated as
            JST; aware values are converted to JST.

    Returns:
        A clean or degraded payload outcome when at least one candidate is
        complete, a no-payload outcome when no candidate survives, or a
        source-invalid outcome when every candidate lacks a required total.

    Raises:
        ValueError: If accepted 60-minute source rows span more than one
            source window.
    """
    active_index = {
        key: place
        for key, place in metadata_index.items()
        if place.active and place.interval_min == _DIRECTION_INTERVAL_MIN
    }
    expected_device_type = _selected_device_type(active_index.values())
    # Candidate places discovered in this window, keyed by place number.
    candidates: dict[int, _CandidateFlow] = {}
    source_startdate: str | None = None
    rows_dropped = 0

    for row in rows:
        # Keep only 60-minute rows; Product B does not publish 5-minute flow.
        interval_min = int(row["interval_min"])
        if interval_min != _DIRECTION_INTERVAL_MIN:
            rows_dropped += 1
            continue

        # Drop noise rows whose source key matches an ignored prefix (ALL exempt).
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

        # Require every 60-minute, non-ignored row to share one source startdate.
        row_startdate = str(row["startdate"])
        if source_startdate is None:
            source_startdate = row_startdate
        elif row_startdate != source_startdate:
            raise ValueError(
                "direction transform requires a single 60-minute source window"
            )

        # Resolve each non-ALL side through active metadata; drop unknown places.
        from_place = _resolve_place(
            from_group_place_id,
            active_index,
            from_group_place_id=from_group_place_id,
            to_group_place_id=to_group_place_id,
        )
        to_place = _resolve_place(
            to_group_place_id,
            active_index,
            from_group_place_id=from_group_place_id,
            to_group_place_id=to_group_place_id,
        )
        if _resolution_failed(from_group_place_id, from_place) or _resolution_failed(
            to_group_place_id, to_place
        ):
            rows_dropped += 1
            continue

        # Drop rows unless both device types match the oldest active batch's.
        if expected_device_type is None:
            rows_dropped += 1
            continue

        from_device_type = str(row["from_device_type"])
        to_device_type = str(row["to_device_type"])
        log_place = from_place if from_place is not None else to_place
        if log_place is None:
            rows_dropped += 1
            continue
        if not _device_types_match(
            log_place,
            expected_device_type=expected_device_type,
            from_device_type=from_device_type,
            to_device_type=to_device_type,
            from_group_place_id=from_group_place_id,
            to_group_place_id=to_group_place_id,
        ):
            rows_dropped += 1
            continue

        # Accumulate the surviving count into its per-place candidate buckets.
        count = int(row["count"])
        if from_group_place_id == _ALL and to_place is not None:
            _candidate_for(candidates, to_place.place_number).from_all = count
        elif to_group_place_id == _ALL and from_place is not None:
            _candidate_for(candidates, from_place.place_number).to_all = count
        elif from_place is not None and to_place is not None:
            from_number = from_place.place_number
            to_number = to_place.place_number
            _candidate_for(candidates, from_number).outgoing[to_number] = count
            _candidate_for(candidates, to_number).incoming[from_number] = count

    if not candidates:
        return DirectionNoPayloadOutcome(rows_dropped=rows_dropped)

    place_numbers = sorted(candidates)
    missing_from_all = tuple(
        place_number
        for place_number in place_numbers
        if candidates[place_number].from_all is None
    )
    missing_to_all = tuple(
        place_number
        for place_number in place_numbers
        if candidates[place_number].to_all is None
    )
    excluded_place_numbers = tuple(sorted(set(missing_from_all) | set(missing_to_all)))
    emitted_place_numbers = [
        place_number
        for place_number in place_numbers
        if place_number not in excluded_place_numbers
    ]
    if not emitted_place_numbers:
        return DirectionSourceInvalidOutcome(
            missing_from_all_place_numbers=missing_from_all,
            missing_to_all_place_numbers=missing_to_all,
            rows_dropped=rows_dropped,
        )

    # Type guard only: non-empty candidates always have a source startdate.
    if source_startdate is None:
        return DirectionNoPayloadOutcome(rows_dropped=rows_dropped)

    observed_from = _parse_startdate(source_startdate)
    retrieved_at = _retrieved_at(now)
    return DirectionPayloadOutcome(
        payload={
            "entity_id": aggregate_entity_id,
            "entity_type": aggregate_entity_type,
            "attrs": _aggregate_attrs(
                aggregate_entity_id=aggregate_entity_id,
                observed_from=observed_from,
                retrieved_at=retrieved_at,
                candidates=candidates,
                emitted_place_numbers=emitted_place_numbers,
                excluded_place_numbers=excluded_place_numbers,
                missing_from_all_place_numbers=missing_from_all,
                missing_to_all_place_numbers=missing_to_all,
            ),
        },
        excluded_place_numbers=excluded_place_numbers,
        missing_from_all_place_numbers=missing_from_all,
        missing_to_all_place_numbers=missing_to_all,
        rows_dropped=rows_dropped,
    )


def _selected_device_type(places: Iterable[SensorPlace]) -> str | None:
    """Return the expected device type from the oldest active batch."""
    selected: tuple[tuple[int, str], str] | None = None
    for place in places:
        candidate = (_batch_sort_key(place.batch), place.expected_device_type)
        if selected is None or candidate[0] < selected[0]:
            selected = candidate
    return None if selected is None else selected[1]


def _batch_sort_key(batch: str) -> tuple[int, str]:
    """Return a stable chronological key for a metadata batch label."""
    try:
        return (int(batch), batch)
    except ValueError:
        return (10**9, batch)


def _matched_row_prefix(
    from_group_place_id: str,
    to_group_place_id: str,
    prefixes: Iterable[str],
) -> str | None:
    """Return the ignored prefix matched on either non-``ALL`` side."""
    for group_place_id in (from_group_place_id, to_group_place_id):
        if group_place_id == _ALL:
            continue
        for prefix in prefixes:
            if group_place_id.startswith(prefix):
                return prefix
    return None


def _resolve_place(
    group_place_id: str,
    metadata_index: Mapping[tuple[int, int], SensorPlace],
    *,
    from_group_place_id: str,
    to_group_place_id: str,
) -> SensorPlace | None:
    """Resolve a non-``ALL`` source key through active 60-minute metadata."""
    if group_place_id == _ALL:
        return None

    parsed = _parse_source_place(group_place_id)
    place_number = parsed[0] if parsed is not None else None
    source_batch = parsed[1] if parsed is not None else None
    place = (
        metadata_index.get((place_number, _DIRECTION_INTERVAL_MIN))
        if place_number is not None
        else None
    )
    if place is None or source_batch != place.batch:
        logger.debug(
            "direction metric row has no metadata target",
            extra={
                "event": "unknown_place_interval",
                "from_group_place_id": from_group_place_id,
                "to_group_place_id": to_group_place_id,
                "place_number": place_number,
                "interval_min": _DIRECTION_INTERVAL_MIN,
            },
        )
        return None
    return place


def _parse_source_place(group_place_id: str) -> tuple[int, str | None] | None:
    """Split a source key into its numeric place and metadata batch."""
    try:
        place_number = int(group_place_id.rsplit(".", 1)[-1])
    except ValueError:
        return None

    for prefix, batch in _SOURCE_BATCH_PREFIXES.items():
        if group_place_id.startswith(prefix):
            return place_number, batch
    return place_number, None


def _resolution_failed(group_place_id: str, place: SensorPlace | None) -> bool:
    """Return whether a non-``ALL`` source side failed metadata resolution."""
    return group_place_id != _ALL and place is None


def _device_types_match(
    place: SensorPlace,
    *,
    expected_device_type: str,
    from_device_type: str,
    to_device_type: str,
    from_group_place_id: str,
    to_group_place_id: str,
) -> bool:
    """Return whether both row sides use the selected source device type."""
    if (
        from_device_type == expected_device_type
        and to_device_type == expected_device_type
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
            "interval_min": _DIRECTION_INTERVAL_MIN,
            "expected_device_type": expected_device_type,
        },
    )
    return False


def _candidate_for(
    candidates: dict[int, _CandidateFlow], place_number: int
) -> _CandidateFlow:
    """Return a mutable candidate bucket, creating it when necessary."""
    return candidates.setdefault(place_number, _CandidateFlow())


def _aggregate_attrs(
    *,
    aggregate_entity_id: str,
    observed_from: datetime,
    retrieved_at: datetime,
    candidates: Mapping[int, _CandidateFlow],
    emitted_place_numbers: list[int],
    excluded_place_numbers: tuple[int, ...],
    missing_from_all_place_numbers: tuple[int, ...],
    missing_to_all_place_numbers: tuple[int, ...],
) -> dict[str, Any]:
    """Build the aggregate attributes for the complete candidate places."""
    timeinstant_value = observed_from.isoformat()
    observed_to = observed_from + timedelta(minutes=_DIRECTION_INTERVAL_MIN)
    attrs: dict[str, Any] = {
        "dateObservedFrom": _attribute(
            "DateTime", observed_from.isoformat(), timeinstant_value
        ),
        "dateObservedTo": _attribute(
            "DateTime", observed_to.isoformat(), timeinstant_value
        ),
        "dateRetrieved": _attribute(
            "DateTime", retrieved_at.isoformat(), timeinstant_value
        ),
        # The spelling matches the platform contract.
        "identifcation": _attribute("Text", aggregate_entity_id, timeinstant_value),
        "sourceQuality": _attribute(
            "StructuredValue",
            {
                "status": "degraded" if excluded_place_numbers else "clean",
                "evaluatedAt": retrieved_at.isoformat(),
                "excludedPlaceNumbers": list(excluded_place_numbers),
                "missingFromAllPlaceNumbers": list(missing_from_all_place_numbers),
                "missingToAllPlaceNumbers": list(missing_to_all_place_numbers),
            },
            timeinstant_value,
        ),
    }
    excluded_place_set = set(excluded_place_numbers)
    for place_number in emitted_place_numbers:
        flow = candidates[place_number]
        incoming = {
            str(other_number): flow.incoming.get(other_number, 0)
            for other_number in emitted_place_numbers
        }
        outgoing = {
            str(other_number): flow.outgoing.get(other_number, 0)
            for other_number in emitted_place_numbers
        }
        incoming.update(
            {
                str(other_number): count
                for other_number, count in flow.incoming.items()
                if other_number in excluded_place_set
            }
        )
        outgoing.update(
            {
                str(other_number): count
                for other_number, count in flow.outgoing.items()
                if other_number in excluded_place_set
            }
        )
        attrs[f"peopleCount_flow_{place_number}"] = _attribute(
            "StructuredValue",
            {
                "from": {
                    **incoming,
                    "all": flow.from_all,
                },
                "to": {
                    **outgoing,
                    "all": flow.to_all,
                },
            },
            timeinstant_value,
        )
    return attrs


def _attribute(attribute_type: str, value: Any, timeinstant: str) -> dict[str, Any]:
    """Build one NGSI attribute with the Product B time metadata."""
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


def _parse_startdate(value: str) -> datetime:
    """Parse a ``YYYYMMDD_HHMM`` source startdate as JST."""
    return datetime.strptime(value, "%Y%m%d_%H%M").replace(tzinfo=JST)


def _retrieved_at(now: Callable[[], datetime] | None) -> datetime:
    """Resolve ``dateRetrieved`` in JST and truncate it to whole seconds."""
    if now is None:
        value = datetime.now(JST)
    else:
        value = now()
        if value.tzinfo is None:
            value = value.replace(tzinfo=JST)
        else:
            value = value.astimezone(JST)
    return value.replace(microsecond=0)
