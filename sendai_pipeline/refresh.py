"""Refresh runtime sensor metadata from a stable seed and a staged refreshable file.

The refresh job unions an operator-maintained stable seed CSV with an
operator-staged refreshable CSV into a single canonical sensor metadata file
that the pipeline reads at runtime. It runs out-of-band from the pipeline
(separate cron / systemd timer) so the hot path stays decoupled from
spreadsheet availability.

The staged refreshable file uses the canonical schema with one allowed
deviation: it carries an ``ID`` column where the canonical schema uses
``identifcation``. ``ID`` values are whitespace-trimmed and the column is
renamed during normalization. All other canonical columns must be present
and well-formed in both inputs.

The combined file is validated in full before the runtime output is
replaced, and the replacement itself is atomic (write to a sibling
``.new`` temp file and ``os.replace`` into place) so a pipeline run that
starts mid-refresh always reads a consistent file.
"""

import csv
import logging
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Self

from sendai_pipeline.logging_setup import LoggingSettings, configure_logging
from sendai_pipeline.metadata import (
    MetadataLoadError,
    SensorPlace,
    index_by_place_interval,
    load_metadata,
)

logger = logging.getLogger(__name__)

CANONICAL_COLUMNS: tuple[str, ...] = (
    "place_number",
    "batch",
    "expected_device_type",
    "interval_min",
    "entity_type",
    "entity_id",
    "identifcation",
    "active",
)

_STAGED_RENAMED_COLUMN = "ID"
_STAGED_REQUIRED_COLUMNS: tuple[str, ...] = tuple(
    _STAGED_RENAMED_COLUMN if column == "identifcation" else column
    for column in CANONICAL_COLUMNS
)

_DEFAULT_OUTPUT_PATH = Path("metadata/sensors.csv")
_DEFAULT_STABLE_PATH = Path("metadata/sensors_stable.csv")
_DEFAULT_STAGED_PATH = Path("metadata/sensors_refreshable.csv.staged")

EXIT_OK = 0
EXIT_VALIDATION_ERROR = 1
EXIT_IO_ERROR = 2

_LOG_PRODUCT = "metadata"


class RefreshMetadataError(RuntimeError):
    """Raised when the staged refreshable file cannot be read or normalized."""


@dataclass(frozen=True)
class RefreshMetadataSettings:
    """Filesystem paths consumed by the refresh job."""

    output_path: Path
    stable_path: Path
    staged_path: Path

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> Self:
        """Load settings from environment variables.

        Args:
            env: Optional mapping used in place of ``os.environ`` for tests.

        Returns:
            Settings with default paths when env vars are unset or empty.
        """
        source = env if env is not None else os.environ
        return cls(
            output_path=_path_from_env(
                source, "SENSOR_METADATA_PATH", _DEFAULT_OUTPUT_PATH
            ),
            stable_path=_path_from_env(
                source, "SENSOR_METADATA_STABLE_PATH", _DEFAULT_STABLE_PATH
            ),
            staged_path=_path_from_env(
                source, "SENSOR_METADATA_STAGED_PATH", _DEFAULT_STAGED_PATH
            ),
        )


@dataclass(frozen=True)
class RefreshDiff:
    """Per-row differences between the previous output and the new combined file."""

    added: list[SensorPlace]
    removed: list[SensorPlace]
    changed: list[tuple[SensorPlace, SensorPlace]]


@dataclass(frozen=True)
class RefreshResult:
    """Outcome of a successful refresh."""

    places: list[SensorPlace]
    diff: RefreshDiff


def refresh_metadata(
    stable_path: Path,
    staged_path: Path,
    output_path: Path,
) -> RefreshResult:
    """Union stable and staged metadata, validate, and atomically replace the output.

    Args:
        stable_path: Path to the canonical-schema stable seed CSV.
        staged_path: Path to the operator-staged refreshable CSV. Uses the
            canonical schema with ``ID`` in place of ``identifcation``.
        output_path: Runtime metadata file to write atomically.

    Returns:
        The validated places and a diff against the previous output.

    Raises:
        MetadataLoadError: If the stable seed or the combined file fails
            canonical schema validation, including duplicate
            ``(place_number, interval_min)`` keys.
        RefreshMetadataError: If the staged file is missing or has an
            invalid header.
    """
    stable_keys = _read_stable_keys(stable_path)
    stable_rows = _read_canonical_rows(stable_path)
    staged_rows = _read_staged_rows(staged_path)

    combined_rows = stable_rows + staged_rows

    tmp_path = output_path.with_name(output_path.name + ".new")
    try:
        _write_canonical_csv(tmp_path, combined_rows)
        places = load_metadata(tmp_path)
        index_by_place_interval(places)
        _write_places_csv(tmp_path, places)
    except (MetadataLoadError, OSError):
        tmp_path.unlink(missing_ok=True)
        raise

    previous_places = _try_load_previous(output_path)
    diff = _compute_diff(previous_places, places)

    os.replace(tmp_path, output_path)

    _log_summary(output_path, places, diff, stable_keys)

    return RefreshResult(places=places, diff=diff)


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry point. Returns one of a fixed set of exit codes.

    Exit codes:
        0 on success.
        1 on validation failure (bad schema, duplicate key, header problem).
        2 on filesystem / I/O failure.
    """
    _ = argv  # The job takes no positional arguments; settings come from env.

    try:
        logging_settings = LoggingSettings.from_env()
        configure_logging(logging_settings, product=_LOG_PRODUCT)
    except ValueError:
        # Logging misconfiguration shouldn't sink the refresh; fall back to a
        # minimal stderr-friendly setup so the eventual failure still surfaces.
        logging.basicConfig(level=logging.INFO, format="%(message)s")

    try:
        settings = RefreshMetadataSettings.from_env()
        refresh_metadata(
            settings.stable_path, settings.staged_path, settings.output_path
        )
    except (MetadataLoadError, RefreshMetadataError) as exc:
        if _is_io_error(exc):
            logger.exception(
                "metadata refresh i/o error: %s",
                exc,
                extra={
                    "event": "metadata_refresh_failed",
                    "error_type": type(exc.__cause__).__name__,
                },
            )
            return EXIT_IO_ERROR
        logger.error(
            "metadata refresh failed: %s",
            exc,
            extra={
                "event": "metadata_refresh_failed",
                "error_type": type(exc).__name__,
            },
        )
        return EXIT_VALIDATION_ERROR
    except OSError as exc:
        logger.exception(
            "metadata refresh i/o error: %s",
            exc,
            extra={"event": "metadata_refresh_failed", "error_type": "OSError"},
        )
        return EXIT_IO_ERROR

    return EXIT_OK


def _path_from_env(source: Mapping[str, str], key: str, default: Path) -> Path:
    """Return the env-provided path or the default if unset/empty."""
    raw = source.get(key, "")
    return Path(raw) if raw else default


def _is_io_error(exc: BaseException) -> bool:
    """Return whether a validation wrapper was caused by filesystem I/O."""
    return isinstance(exc.__cause__, OSError)


def _read_canonical_rows(path: Path) -> list[dict[str, str]]:
    """Read a canonical-schema CSV as a list of row dicts."""
    try:
        with path.open(newline="", encoding="utf-8-sig") as csv_file:
            reader = csv.DictReader(csv_file)
            fieldnames = reader.fieldnames
            if fieldnames is None:
                raise MetadataLoadError(f"metadata CSV is empty: {path}")
            missing = [c for c in CANONICAL_COLUMNS if c not in fieldnames]
            if missing:
                columns = ", ".join(missing)
                raise MetadataLoadError(
                    f"metadata CSV header row is missing required column(s): {columns}"
                )
            return [
                {column: (row.get(column) or "") for column in CANONICAL_COLUMNS}
                for row in reader
            ]
    except FileNotFoundError as exc:
        raise MetadataLoadError(f"metadata CSV not found: {path}") from exc
    except OSError as exc:
        raise MetadataLoadError(f"metadata CSV could not be read: {path}") from exc


def _read_stable_keys(path: Path) -> set[tuple[int, int]]:
    """Return the set of ``(place_number, interval_min)`` keys in the stable seed."""
    places = load_metadata(path)
    return {(p.place_number, p.interval_min) for p in places}


def _read_staged_rows(path: Path) -> list[dict[str, str]]:
    """Read the staged refreshable CSV and rename ``ID`` to ``identifcation``.

    The staged header must carry every canonical column, with
    ``identifcation`` replaced by ``ID``; missing columns are rejected
    even when the file has zero data rows so an operator typo is caught
    before stable rows alone are written as the runtime metadata. ``ID``
    values are whitespace-trimmed so the combined output already carries
    the canonical form.
    """
    try:
        with path.open(newline="", encoding="utf-8-sig") as csv_file:
            reader = csv.DictReader(csv_file)
            fieldnames = reader.fieldnames
            if fieldnames is None:
                raise RefreshMetadataError(f"staged refreshable CSV is empty: {path}")
            if "identifcation" in fieldnames:
                raise RefreshMetadataError(
                    "staged refreshable CSV must not contain both ID and "
                    f"identifcation columns: {path}"
                )
            missing = [c for c in _STAGED_REQUIRED_COLUMNS if c not in fieldnames]
            if missing:
                columns = ", ".join(missing)
                raise RefreshMetadataError(
                    "staged refreshable CSV header row is missing required "
                    f"column(s): {columns} (path={path})"
                )
            canonical_rows: list[dict[str, str]] = []
            for row in reader:
                normalized: dict[str, str] = {}
                for column in CANONICAL_COLUMNS:
                    if column == "identifcation":
                        raw = row.get(_STAGED_RENAMED_COLUMN) or ""
                        normalized[column] = raw.strip()
                    else:
                        normalized[column] = row.get(column) or ""
                canonical_rows.append(normalized)
            return canonical_rows
    except FileNotFoundError as exc:
        raise RefreshMetadataError(f"staged refreshable CSV not found: {path}") from exc
    except OSError as exc:
        raise RefreshMetadataError(
            f"staged refreshable CSV could not be read: {path}"
        ) from exc


def _write_canonical_csv(path: Path, rows: list[dict[str, str]]) -> None:
    """Write ``rows`` to ``path`` using the canonical header order."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(
            csv_file, fieldnames=list(CANONICAL_COLUMNS), extrasaction="ignore"
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {column: row.get(column, "") for column in CANONICAL_COLUMNS}
            )


def _write_places_csv(path: Path, places: list[SensorPlace]) -> None:
    """Write validated places using the canonical runtime representation."""
    rows = [
        {
            "place_number": str(place.place_number),
            "batch": place.batch,
            "expected_device_type": place.expected_device_type,
            "interval_min": str(place.interval_min),
            "entity_type": place.entity_type,
            "entity_id": place.entity_id,
            "identifcation": place.identifcation,
            "active": "true" if place.active else "false",
        }
        for place in places
    ]
    _write_canonical_csv(path, rows)


def _try_load_previous(path: Path) -> list[SensorPlace] | None:
    """Return the previously-written places, or ``None`` if absent/unreadable."""
    if not path.exists():
        return None
    try:
        return load_metadata(path)
    except MetadataLoadError:
        # An unreadable previous output should not block a fresh refresh from
        # replacing it — treat as "no prior state" for diff purposes.
        return None


def _compute_diff(
    previous: list[SensorPlace] | None, current: list[SensorPlace]
) -> RefreshDiff:
    """Compute per-row diff keyed by ``(place_number, interval_min)``."""
    previous_by_key = {(p.place_number, p.interval_min): p for p in (previous or [])}
    current_by_key = {(p.place_number, p.interval_min): p for p in current}

    added = [
        p for p in current if (p.place_number, p.interval_min) not in previous_by_key
    ]
    removed = [
        p
        for p in (previous or [])
        if (p.place_number, p.interval_min) not in current_by_key
    ]
    changed: list[tuple[SensorPlace, SensorPlace]] = []
    for key, after in current_by_key.items():
        before = previous_by_key.get(key)
        if before is not None and before != after:
            changed.append((before, after))
    return RefreshDiff(added=added, removed=removed, changed=changed)


def _log_summary(
    output_path: Path,
    places: list[SensorPlace],
    diff: RefreshDiff,
    stable_keys: set[tuple[int, int]],
) -> None:
    """Emit the refresh summary and a warning for stable-seed touches.

    Added / removed / changed rows are always logged individually. Any
    added or changed row whose ``(place_number, interval_min)`` key is in the
    current stable seed is additionally treated as unexpected: the stable seed
    is supposed to carry forward unchanged, so churn there should be reviewed
    by an operator.
    """
    for before, after in diff.changed:
        logger.info(
            "metadata row changed: %s -> %s",
            before,
            after,
            extra={
                "event": "metadata_row_changed",
                "place_number": after.place_number,
                "interval_min": after.interval_min,
                "before": before,
                "after": after,
            },
        )
        if (after.place_number, after.interval_min) in stable_keys:
            logger.warning(
                "stable seed row changed unexpectedly: %s -> %s",
                before,
                after,
                extra={"event": "stable_seed_changed"},
            )
    for place in diff.added:
        logger.info(
            "metadata row added: %s",
            place,
            extra={
                "event": "metadata_row_added",
                "place_number": place.place_number,
                "interval_min": place.interval_min,
            },
        )
        if (place.place_number, place.interval_min) in stable_keys:
            logger.warning(
                "stable seed gained a row unexpectedly: %s",
                place,
                extra={"event": "stable_seed_changed"},
            )
    for place in diff.removed:
        logger.info(
            "metadata row removed from output: %s",
            place,
            extra={
                "event": "metadata_row_removed",
                "place_number": place.place_number,
                "interval_min": place.interval_min,
            },
        )

    logger.info(
        "wrote %s with %d rows (added=%d removed=%d changed=%d)",
        output_path,
        len(places),
        len(diff.added),
        len(diff.removed),
        len(diff.changed),
        extra={"event": "metadata_refreshed"},
    )
