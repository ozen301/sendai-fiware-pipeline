"""Sensor metadata loading for the Sendai FIWARE pipeline."""

import csv
import logging
import os
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import NoReturn

logger = logging.getLogger(__name__)

_REQUIRED_COLUMNS: tuple[str, ...] = (
    "place_number",
    "batch",
    "expected_device_type",
    "interval_min",
    "entity_type",
    "entity_id",
    "identifcation",
    "active",
)
_VALID_BATCHES: frozenset[str] = frozenset({"2023", "2026"})
_VALID_DEVICE_TYPES: frozenset[str] = frozenset({"Pixel3aUT", "M5Stack"})
_VALID_INTERVALS: frozenset[int] = frozenset({5, 60})
_ENTITY_ID_PREFIX = "jp.sendai."
_DEFAULT_METADATA_PATH = Path("metadata/sensors.csv")
INTERVAL_BY_TYPE_SUFFIX: dict[str, int] = {"per300": 5, "per3600": 60}


class MetadataLoadError(RuntimeError):
    """Raised when sensor metadata cannot be loaded or validated."""


def metadata_path_from_env(env: Mapping[str, str] | None = None) -> Path:
    """Return the runtime sensor metadata path.

    Args:
        env: Optional mapping used in place of ``os.environ`` for tests.

    Returns:
        ``SENSOR_METADATA_PATH`` when non-empty, otherwise
        ``metadata/sensors.csv``.
    """
    source = os.environ if env is None else env
    value = source.get("SENSOR_METADATA_PATH")
    if value is None or value == "":
        return _DEFAULT_METADATA_PATH
    return Path(value)


@dataclass(frozen=True)
class SensorPlace:
    """A validated sensor target row from runtime metadata.

    Attributes:
        place_number: Numeric identifier for this sensor place, read from metadata.
        batch: Sensor installation batch identifier.
        expected_device_type: Device type expected for rows from this place.
        interval_min: Aggregation interval in minutes.
        entity_type: FIWARE entity type read from metadata.
        entity_id: FIWARE entity id read from metadata.
        identifcation: Attribute value for the platform's misspelled field
            (``identifcation`` is intentionally misspelled — it matches the
            live platform's attribute name; do not "fix" this spelling).
        active: Whether this metadata row should be used for publishing.
    """

    place_number: int
    batch: str
    expected_device_type: str
    interval_min: int
    entity_type: str
    entity_id: str
    identifcation: str
    active: bool


@dataclass(frozen=True)
class ParsedEntityId:
    """Entity identity parsed from a canonical Sendai FIWARE entity id.

    Attributes:
        entity_id: Original entity id string.
        entity_type: NGSI entity type embedded in the id, e.g.
            ``"Blesensor.per3600"``.
        place_number: Numeric place suffix from the id.
        interval_min: Aggregation interval inferred from the entity type suffix.
            This is ``None`` when the id has the canonical shape but the type
            suffix is not one of the known interval suffixes.
    """

    entity_id: str
    entity_type: str
    place_number: int
    interval_min: int | None


def parse_entity_id(entity_id: str) -> ParsedEntityId | None:
    """Parse a canonical Sendai target entity id.

    The canonical shape is ``jp.sendai.<entity_type>.<place_number>``. Shape
    failures return ``None`` so callers can keep explicit type or interval
    escape hatches. If the shape parses but the entity type's last segment is
    not a known interval suffix, the returned :class:`ParsedEntityId` has
    ``interval_min=None``.

    Args:
        entity_id: Entity id to parse.

    Returns:
        Parsed canonical components, or ``None`` when the id does not have the
        canonical prefix, a non-empty entity type, and an all-digits place
        suffix.
    """
    # Strip the fixed "jp.sendai." prefix, then split the trailing ".<place>"
    # off the remainder; what is left is the entity type.
    if not entity_id.startswith(_ENTITY_ID_PREFIX):
        return None
    remainder = entity_id.removeprefix(_ENTITY_ID_PREFIX)
    entity_type, separator, place_number_text = remainder.rpartition(".")
    if not separator or entity_type == "" or not place_number_text.isdigit():
        return None
    # The interval lives in the type's last segment ("per300" / "per3600");
    # an unknown segment leaves interval_min as None (shape still valid).
    type_suffix = entity_type.rsplit(".", maxsplit=1)[-1]
    return ParsedEntityId(
        entity_id=entity_id,
        entity_type=entity_type,
        place_number=int(place_number_text),
        interval_min=INTERVAL_BY_TYPE_SUFFIX.get(type_suffix),
    )


def load_metadata(path: Path) -> list[SensorPlace]:
    """Load and validate sensor metadata from a CSV file.

    Args:
        path: CSV path to read.

    Returns:
        Validated :class:`SensorPlace` rows in file order.  All rows, including
        both active and inactive, are returned; callers filter by
        :func:`active_places` when they only want publishable targets.

    Raises:
        MetadataLoadError: If the file is missing, empty, or contains invalid
            metadata.
    """
    try:
        with path.open(newline="", encoding="utf-8-sig") as csv_file:
            reader = csv.DictReader(csv_file)
            fieldnames = reader.fieldnames
            if fieldnames is None:
                raise MetadataLoadError(f"metadata CSV is empty: {path}")

            missing_columns = [
                column for column in _REQUIRED_COLUMNS if column not in fieldnames
            ]
            if missing_columns:
                columns = ", ".join(missing_columns)
                raise MetadataLoadError(
                    f"metadata CSV header row is missing required column(s): {columns}"
                )

            places = [_parse_row(row, reader.line_num) for row in reader]
    except FileNotFoundError as exc:
        raise MetadataLoadError(f"metadata CSV not found: {path}") from exc
    except OSError as exc:
        raise MetadataLoadError(f"metadata CSV could not be read: {path}") from exc

    if not places:
        raise MetadataLoadError(f"metadata CSV contains no data rows: {path}")

    logger.debug(
        "loaded sensor metadata",
        extra={"event": "metadata_loaded", "path": str(path), "rows": len(places)},
    )
    return places


def active_places(
    places: Iterable[SensorPlace],
    *,
    target_batches: Iterable[str],
) -> list[SensorPlace]:
    """Return active places whose batch is in the requested batch set.

    Preserves the original iteration order of *places*; no additional sorting
    is applied.  Callers that need a stable order should sort the result
    themselves, or rely on the iteration order of the source CSV.
    """
    batches = set(target_batches)
    return [place for place in places if place.active and place.batch in batches]


def index_by_place_interval(
    places: Iterable[SensorPlace],
) -> dict[tuple[int, int], SensorPlace]:
    """Index places by ``(place_number, interval_min)``.

    Each ``(place_number, interval_min)`` pair must be unique across *places*;
    the uniqueness guarantee is what lets transform helpers use a direct
    ``dict.get`` lookup without resolving ambiguity.

    Args:
        places: Metadata rows to index.

    Returns:
        Mapping from ``(place_number, interval_min)`` to the matching row,
        e.g. ``{(10, 5): <SensorPlace>, (10, 60): <SensorPlace>}``.

    Raises:
        MetadataLoadError: If more than one row has the same
            ``(place_number, interval_min)`` key.
    """
    index: dict[tuple[int, int], SensorPlace] = {}
    for place in places:
        key = (place.place_number, place.interval_min)
        if key in index:
            raise MetadataLoadError(
                f"duplicate metadata row for place/interval key {key}"
            )
        index[key] = place
    return index


def _parse_row(row: dict[str, str | None], row_number: int) -> SensorPlace:
    """Validate and convert a CSV row into a sensor place."""
    values = {
        column: _required_value(row, column, row_number) for column in _REQUIRED_COLUMNS
    }
    identifcation = values["identifcation"].strip()

    return SensorPlace(
        place_number=_parse_place_number(values["place_number"], row_number),
        batch=_parse_enum(values["batch"], "batch", _VALID_BATCHES, row_number),
        expected_device_type=_parse_enum(
            values["expected_device_type"],
            "expected_device_type",
            _VALID_DEVICE_TYPES,
            row_number,
        ),
        interval_min=_parse_interval_min(values["interval_min"], row_number),
        entity_type=values["entity_type"],
        entity_id=values["entity_id"],
        identifcation=identifcation,
        active=_parse_active(values["active"], row_number),
    )


def _required_value(row: dict[str, str | None], column: str, row_number: int) -> str:
    """Return a required CSV value exactly as written.

    Whitespace-only values are rejected the same way empty strings are, but
    non-empty values are returned unchanged: entity ids and types must match
    the platform's records verbatim, so this function never reformats them.
    """
    raw = row.get(column)
    stripped = raw.strip() if raw is not None else ""
    if stripped == "":
        _raise_column_error(column, row_number, "must not be empty")
    return raw if raw is not None else ""


def _parse_place_number(value: str, row_number: int) -> int:
    """Parse a positive integer place number."""
    try:
        place_number = int(value)
    except ValueError:
        _raise_column_error("place_number", row_number, "must be a positive integer")

    if place_number <= 0:
        _raise_column_error("place_number", row_number, "must be a positive integer")
    return place_number


def _parse_interval_min(value: str, row_number: int) -> int:
    """Parse and validate an aggregation interval."""
    try:
        interval_min = int(value)
    except ValueError:
        _raise_column_error("interval_min", row_number, "must be one of 5, 60")

    if interval_min not in _VALID_INTERVALS:
        _raise_column_error("interval_min", row_number, "must be one of 5, 60")
    return interval_min


def _parse_enum(
    value: str,
    column: str,
    allowed_values: frozenset[str],
    row_number: int,
) -> str:
    """Validate a string enum exactly as written in metadata."""
    if value not in allowed_values:
        allowed = ", ".join(sorted(allowed_values))
        _raise_column_error(column, row_number, f"must be one of {allowed}")
    return value


def _parse_active(value: str, row_number: int) -> bool:
    """Parse the active flag from case-insensitive true or false text."""
    lowered = value.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    _raise_column_error("active", row_number, "must be true or false")


def _raise_column_error(column: str, row_number: int, detail: str) -> NoReturn:
    """Raise a consistent metadata validation error for a CSV column."""
    raise MetadataLoadError(f"metadata column {column} at row {row_number} {detail}")
