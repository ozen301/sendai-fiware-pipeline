"""Sensor metadata loading for the Sendai FIWARE pipeline."""

import csv
import logging
from collections.abc import Iterable
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


class MetadataLoadError(RuntimeError):
    """Raised when sensor metadata cannot be loaded or validated."""


@dataclass(frozen=True)
class SensorPlace:
    """A validated sensor target row from runtime metadata.

    Attributes:
        place_number: Numeric place identifier derived by the metadata producer.
        batch: Sensor installation batch identifier.
        expected_device_type: Device type expected for rows from this place.
        interval_min: Aggregation interval in minutes.
        entity_type: FIWARE entity type read from metadata.
        entity_id: FIWARE entity id read from metadata.
        identifcation: Attribute value for the platform's misspelled field.
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


def load_metadata(path: Path) -> list[SensorPlace]:
    """Load and validate sensor metadata from a CSV file.

    Args:
        path: CSV path to read.

    Returns:
        Validated metadata rows in file order.

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
    """Return active places whose batch is in the requested batch set."""
    batches = set(target_batches)
    return [place for place in places if place.active and place.batch in batches]


def index_by_place_interval(
    places: Iterable[SensorPlace],
) -> dict[tuple[int, int], SensorPlace]:
    """Index places by ``(place_number, interval_min)``.

    Args:
        places: Metadata rows to index.

    Returns:
        Mapping from place and interval to the matching metadata row.

    Raises:
        MetadataLoadError: If more than one row has the same key.
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

    Whitespace-only values are rejected the same way empty strings are —
    but non-empty values are left intact because metadata strings such as
    entity ids and types are authoritative inputs, not reconstructed targets.
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
