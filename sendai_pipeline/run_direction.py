"""Run the direction-metric publishing pipeline."""

import fcntl
import hashlib
import json
import logging
import os
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Protocol, cast

from dotenv import load_dotenv

from sendai_pipeline import auth, db, entity_map, orion_client
from sendai_pipeline.filter_settings import FilterConfigError, FilterSettings
from sendai_pipeline.logging_setup import LoggingSettings, configure_logging
from sendai_pipeline.metadata import (
    SensorPlace,
    active_places,
    index_by_place_interval,
    load_metadata,
)
from sendai_pipeline.state import WindowStateStore
from sendai_pipeline.transform_direction import (
    TransformDirectionResult,
    transform_direction_rows,
)

logger = logging.getLogger(__name__)
_lifecycle_logger = logging.getLogger("sendai_pipeline")

JST = timezone(timedelta(hours=9))

_INTERVALS: tuple[int, ...] = (5, 60)
_INTERVAL_PREFIXES: dict[int, str] = {5: "per300", 60: "per3600"}
_VALID_SEND_MODES: frozenset[str] = frozenset({"dry-run", "send"})
_DEFAULT_METADATA_PATH = Path("metadata/sensors.csv")
_DEFAULT_SOURCE_STABILITY_DELAY_HOURS = 3

_DirectionRow = Mapping[str, Any]
_PostResult = Mapping[str, Any]

__all__ = [
    "FilterConfigError",
    "JST",
    "RunDirectionConfigError",
    "RunDirectionResult",
    "RunDirectionSettings",
    "main",
    "run_direction",
]


class RunDirectionConfigError(RuntimeError):
    """Raised when direction-run configuration is invalid."""


class _DbConnection(Protocol):
    def cursor(self) -> Any:
        """Return a DB-API cursor context manager."""
        ...


class _OrionForDirection(Protocol):
    def update_attrs(
        self,
        entity_id: str,
        entity_type: str | None,
        attrs: Mapping[str, Any],
        *,
        dry_run: bool = False,
    ) -> _PostResult:
        """Update one Orion entity's attributes."""
        ...

    def list_entities(
        self,
        entity_type: str,
        *,
        attrs: str | None = None,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        """Return Orion entities for one entity type."""
        ...


@dataclass(frozen=True)
class RunDirectionSettings:
    """Configuration for one direction publishing run.

    Attributes:
        send_mode: ``"dry-run"`` to avoid live POSTs, or ``"send"`` to write
            attributes to Orion.
        reprocess_hours_per3600: Minimum 60-minute lookback.
        reprocess_hours_per300: Minimum 5-minute lookback.
        max_lookback_hours_per3600: Maximum 60-minute lookback.
        max_lookback_hours_per300: Maximum 5-minute lookback.
        source_stability_delay_hours: Hours to wait after source windows end
            before they become eligible for publication.
        state_path: JSON state file path.
        lock_path: Process lock file path.
    """

    send_mode: str = "dry-run"
    reprocess_hours_per3600: int = 12
    reprocess_hours_per300: int = 2
    max_lookback_hours_per3600: int = 72
    max_lookback_hours_per300: int = 72
    source_stability_delay_hours: int = _DEFAULT_SOURCE_STABILITY_DELAY_HOURS
    state_path: Path = Path("state/direction.json")
    lock_path: Path = Path("state/direction.lock")

    def __post_init__(self) -> None:
        if self.send_mode not in _VALID_SEND_MODES:
            raise RunDirectionConfigError(
                f"invalid DIRECTION_SEND_MODE {self.send_mode!r}; "
                "expected dry-run or send"
            )
        _validate_non_negative_hours(
            {
                "REPROCESS_HOURS_PER3600": self.reprocess_hours_per3600,
                "REPROCESS_HOURS_PER300": self.reprocess_hours_per300,
                "MAX_LOOKBACK_HOURS_PER3600": self.max_lookback_hours_per3600,
                "MAX_LOOKBACK_HOURS_PER300": self.max_lookback_hours_per300,
                "SOURCE_STABILITY_DELAY_HOURS": self.source_stability_delay_hours,
            }
        )
        _validate_lookback_ceiling(
            "PER3600",
            self.reprocess_hours_per3600,
            self.max_lookback_hours_per3600,
        )
        _validate_lookback_ceiling(
            "PER300",
            self.reprocess_hours_per300,
            self.max_lookback_hours_per300,
        )

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "RunDirectionSettings":
        """Build settings from environment variables.

        Args:
            env: Optional mapping used in place of ``os.environ`` for tests.

        Returns:
            Parsed direction-run settings.

        Raises:
            RunDirectionConfigError: If a setting is malformed or unsupported.
        """
        source = os.environ if env is None else env
        return cls(
            send_mode=_optional_env(source, "DIRECTION_SEND_MODE", "dry-run")
            .strip()
            .lower(),
            reprocess_hours_per3600=_int_env(source, "REPROCESS_HOURS_PER3600", 12),
            reprocess_hours_per300=_int_env(source, "REPROCESS_HOURS_PER300", 2),
            max_lookback_hours_per3600=_int_env(
                source, "MAX_LOOKBACK_HOURS_PER3600", 72
            ),
            max_lookback_hours_per300=_int_env(source, "MAX_LOOKBACK_HOURS_PER300", 72),
            source_stability_delay_hours=_int_env(
                source,
                "SOURCE_STABILITY_DELAY_HOURS",
                _DEFAULT_SOURCE_STABILITY_DELAY_HOURS,
            ),
            state_path=Path("state/direction.json"),
            lock_path=Path("state/direction.lock"),
        )


@dataclass
class RunDirectionResult:
    """Outcome summary for one direction publishing run.

    Attributes:
        windows_seen: Source windows processed during this run.
        windows_complete: Processed windows that ended complete.
        windows_partial: Processed windows that ended partial.
        windows_dead_letter: Processed windows that ended dead-lettered.
        posts_ok: Attribute update attempts that succeeded.
        posts_failed: Attribute update attempts that failed.
        rows_dropped: Source rows omitted during transformation.
        oldest_non_complete: Oldest retained pending or partial window.
        lookback_hours_used: Effective lookback hours by interval.
        exit_code: Process exit code for the entry point.

    Note:
        In dry-run mode, ``windows_complete``, ``windows_partial``, and
        ``windows_dead_letter`` are always zero. The state store is not
        touched during dry-run, so window status is never computed.
    """

    windows_seen: int
    windows_complete: int
    windows_partial: int
    windows_dead_letter: int
    posts_ok: int
    posts_failed: int
    rows_dropped: int
    oldest_non_complete: datetime | None
    lookback_hours_used: dict[int, float]
    exit_code: int


def run_direction(
    *,
    db_connection: _DbConnection,
    orion: _OrionForDirection,
    metadata: list[SensorPlace],
    state_store: WindowStateStore,
    settings: RunDirectionSettings,
    filter_settings: FilterSettings,
    now: Callable[[], datetime],
) -> RunDirectionResult:
    """Publish direction metrics for the eligible reprocessing windows.

    Args:
        db_connection: Database connection used by ``sendai_pipeline.db``.
        orion: Orion client or test double exposing ``update_attrs``.
        metadata: Runtime sensor metadata.
        state_store: Per-window delivery state store.
        settings: Direction-run settings.
        filter_settings: Batch and source-row filter settings.
        now: Clock returning the run timestamp. Called once.

    Returns:
        Run summary and exit code.
    """
    run_started_at = _coerce_jst_datetime(now())
    target_batches = sorted(filter_settings.target_direction_batches)

    if target_batches:
        filter_settings.validate_target_direction_batches(
            {place.batch for place in metadata}
        )

    lookback_hours_used = _lookback_hours_by_interval(
        state_store,
        settings=settings,
        run_started_at=run_started_at,
    )
    _log_run_started(
        settings=settings,
        orion=orion,
        target_batches=target_batches,
        lookback_hours_used=lookback_hours_used,
    )

    if not target_batches:
        result = RunDirectionResult(
            windows_seen=0,
            windows_complete=0,
            windows_partial=0,
            windows_dead_letter=0,
            posts_ok=0,
            posts_failed=0,
            rows_dropped=0,
            oldest_non_complete=None,
            lookback_hours_used=lookback_hours_used,
            exit_code=0,
        )
        _log_run_summary(result)
        return result

    active_targets = active_places(metadata, target_batches=target_batches)
    metadata_index = index_by_place_interval(active_targets)
    _validate_orion_targets(active_targets, orion)

    counts = _RunCounts()
    for interval_min in _INTERVALS:
        _process_interval(
            interval_min,
            db_connection=db_connection,
            orion=orion,
            state_store=state_store,
            settings=settings,
            filter_settings=filter_settings,
            metadata_index=metadata_index,
            lookback_hours=lookback_hours_used[interval_min],
            run_started_at=run_started_at,
            counts=counts,
        )

    if settings.send_mode == "send":
        cutoff_gc = run_started_at - timedelta(
            hours=2
            * max(
                settings.max_lookback_hours_per3600,
                settings.max_lookback_hours_per300,
            )
        )
        state_store.gc_complete_before(cutoff_gc)
        state_store.save()

    oldest_non_complete = _oldest_non_complete(state_store)
    has_open_windows = oldest_non_complete is not None
    exit_code = (
        1
        if settings.send_mode == "send"
        and (counts.windows_partial > 0 or counts.posts_failed > 0 or has_open_windows)
        else 0
    )
    result = RunDirectionResult(
        windows_seen=counts.windows_seen,
        windows_complete=counts.windows_complete,
        windows_partial=counts.windows_partial,
        windows_dead_letter=counts.windows_dead_letter,
        posts_ok=counts.posts_ok,
        posts_failed=counts.posts_failed,
        rows_dropped=counts.rows_dropped,
        oldest_non_complete=oldest_non_complete,
        lookback_hours_used=lookback_hours_used,
        exit_code=exit_code,
    )
    _log_run_summary(result)
    return result


def main(argv: list[str] | None = None) -> int:
    """Run the direction entry point and return a process exit code."""
    del argv
    load_dotenv()
    settings = RunDirectionSettings.from_env()

    settings.lock_path.parent.mkdir(parents=True, exist_ok=True)
    with settings.lock_path.open("a", encoding="utf-8") as lock_file:
        try:
            fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return 0

        logging_settings = LoggingSettings.from_env()
        configure_logging(logging_settings, product="direction")

        filter_settings = FilterSettings.from_env()
        auth_settings = auth.AuthSettings.from_env()
        auth_client = auth.AuthClient(auth_settings)
        orion = orion_client.OrionClient(
            orion_client.OrionSettings.from_env(),
            auth=auth_client,
        )
        _copy_payload_logging_settings(orion, logging_settings)

        db_connection = db.connect(db.DbSettings.from_env())
        try:
            result = run_direction(
                db_connection=db_connection,
                orion=orion,
                metadata=load_metadata(_metadata_path_from_env()),
                state_store=WindowStateStore.load(
                    settings.state_path,
                    now=lambda: datetime.now(JST),
                ),
                settings=settings,
                filter_settings=filter_settings,
                now=lambda: datetime.now(JST),
            )
        finally:
            close = getattr(db_connection, "close", None)
            if close is not None:
                close()

    return result.exit_code


@dataclass
class _RunCounts:
    windows_seen: int = 0
    windows_complete: int = 0
    windows_partial: int = 0
    windows_dead_letter: int = 0
    posts_ok: int = 0
    posts_failed: int = 0
    rows_dropped: int = 0


def _validate_orion_targets(
    active_targets: Iterable[SensorPlace],
    orion: _OrionForDirection,
) -> None:
    """Compare configured metadata targets with the live Orion entity set."""
    entity_map.validate_targets(
        active_targets,
        cast(orion_client.OrionClient, orion),
    )


def _process_interval(
    interval_min: int,
    *,
    db_connection: _DbConnection,
    orion: _OrionForDirection,
    state_store: WindowStateStore,
    settings: RunDirectionSettings,
    filter_settings: FilterSettings,
    metadata_index: Mapping[tuple[int, int], SensorPlace],
    lookback_hours: float,
    run_started_at: datetime,
    counts: _RunCounts,
) -> None:
    """Fetch and publish all source windows for one aggregation interval."""
    cutoff = _eligible_source_cutoff(
        run_started_at,
        interval_min,
        source_stability_delay_hours=settings.source_stability_delay_hours,
    )
    rows = db.select_direction_metrics(
        db_connection,
        interval_min=interval_min,
        lower_bound=_format_sql_window_bound(cutoff - timedelta(hours=lookback_hours)),
        upper_bound=_format_sql_window_bound(cutoff),
    )
    interval_metadata = _metadata_index_for_interval(metadata_index, interval_min)
    expected_target_ids = [place.entity_id for place in interval_metadata.values()]

    for startdate, rows_for_window in _group_rows_by_startdate(rows):
        window_key = _window_key(interval_min, startdate)
        counts.windows_seen += 1

        if settings.send_mode == "send":
            _process_send_window(
                window_key,
                interval_min=interval_min,
                startdate=startdate,
                rows_for_window=rows_for_window,
                orion=orion,
                state_store=state_store,
                filter_settings=filter_settings,
                interval_metadata=interval_metadata,
                expected_target_ids=expected_target_ids,
                counts=counts,
            )
        else:
            _process_dry_run_window(
                rows_for_window=rows_for_window,
                orion=orion,
                filter_settings=filter_settings,
                interval_metadata=interval_metadata,
                counts=counts,
            )

    if settings.send_mode == "send":
        _log_windows_near_retry_horizon(
            state_store,
            interval_min=interval_min,
            run_started_at=run_started_at,
            reprocess_hours=_reprocess_hours(settings, interval_min),
            max_lookback_hours=_max_lookback_hours(settings, interval_min),
        )


def _process_send_window(
    window_key: str,
    *,
    interval_min: int,
    startdate: str,
    rows_for_window: list[_DirectionRow],
    orion: _OrionForDirection,
    state_store: WindowStateStore,
    filter_settings: FilterSettings,
    interval_metadata: Mapping[tuple[int, int], SensorPlace],
    expected_target_ids: Iterable[str],
    counts: _RunCounts,
    force_resend: bool = False,
) -> None:
    """Post one source window and update persistent per-target state."""
    source_start = _parse_source_window_start(startdate)
    effective_expected_target_ids = _effective_expected_target_ids(
        state_store,
        window_key,
        expected_target_ids,
    )
    effective_metadata = _metadata_for_expected_targets(
        interval_metadata,
        effective_expected_target_ids,
    )
    state_store.begin_window_attempt(
        window_key,
        interval_min=interval_min,
        source_window_start=source_start,
        source_window_end=source_start + timedelta(minutes=interval_min),
        expected_target_ids=effective_expected_target_ids,
    )
    transformed = _transform(rows_for_window, filter_settings, effective_metadata)
    counts.rows_dropped += transformed.rows_dropped

    for payload in transformed.payloads:
        payload_sha256 = _attrs_sha256(payload["attrs"])
        entity_id = payload["entity_id"]
        prior = state_store.target_record(window_key, entity_id)
        if not force_resend and prior is not None and prior.get("status") == "ok":
            prior_payload_sha256 = prior.get("last_payload_sha256")
            if prior_payload_sha256 == payload_sha256:
                logger.debug(
                    "target payload unchanged",
                    extra={
                        "event": "post_skipped_unchanged",
                        "entity_id": entity_id,
                        "window": window_key,
                        "payload_sha256": payload_sha256,
                    },
                )
            else:
                logger.debug(
                    "target payload drift skipped",
                    extra={
                        "event": "post_skipped_drift",
                        "entity_id": entity_id,
                        "window": window_key,
                        "prior_payload_sha256": prior_payload_sha256,
                        "computed_payload_sha256": payload_sha256,
                    },
                )
            continue

        result = orion.update_attrs(
            entity_id,
            payload["entity_type"],
            payload["attrs"],
        )
        ok = bool(result.get("ok"))
        if ok:
            counts.posts_ok += 1
        else:
            counts.posts_failed += 1
        state_store.record_target(
            window_key,
            entity_id,
            status="ok" if ok else "failed",
            http_status=int(result.get("status", 0)),
            payload_sha256=payload_sha256,
        )
        state_store.save()

    status = state_store.recompute_status(window_key, effective_expected_target_ids)
    if status == "complete":
        counts.windows_complete += 1
        logger.info(
            "window complete",
            extra={"event": "window_complete", "window": window_key},
        )
    elif status == "partial":
        counts.windows_partial += 1
        logger.warning(
            "window partial",
            extra={"event": "window_partial", "window": window_key},
        )
    elif status == "dead_letter":
        counts.windows_dead_letter += 1


def _effective_expected_target_ids(
    state_store: WindowStateStore,
    window_key: str,
    configured_expected_target_ids: Iterable[str],
) -> list[str]:
    """Return the target set this attempt must honor for one window."""
    configured = sorted(set(configured_expected_target_ids))
    stored = state_store.expected_target_ids(window_key)
    if not stored:
        return configured
    if stored != configured:
        _lifecycle_logger.warning(
            "window expected targets differ from active metadata",
            extra={
                "event": "window_expected_targets_changed",
                "window": window_key,
                "count_expected": len(stored),
                "count_live": len(configured),
            },
        )
    return stored


def _metadata_for_expected_targets(
    interval_metadata: Mapping[tuple[int, int], SensorPlace],
    expected_target_ids: Iterable[str],
) -> dict[tuple[int, int], SensorPlace]:
    expected = set(expected_target_ids)
    return {
        key: place
        for key, place in interval_metadata.items()
        if place.entity_id in expected
    }


def _process_dry_run_window(
    *,
    rows_for_window: list[_DirectionRow],
    orion: _OrionForDirection,
    filter_settings: FilterSettings,
    interval_metadata: Mapping[tuple[int, int], SensorPlace],
    counts: _RunCounts,
) -> None:
    """Build and log payloads for one source window without sending live POSTs."""
    transformed = _transform(rows_for_window, filter_settings, interval_metadata)
    counts.rows_dropped += transformed.rows_dropped
    for payload in transformed.payloads:
        result = orion.update_attrs(
            payload["entity_id"],
            payload["entity_type"],
            payload["attrs"],
            dry_run=True,
        )
        if bool(result.get("ok")):
            counts.posts_ok += 1
        else:
            counts.posts_failed += 1


def _transform(
    rows_for_window: list[_DirectionRow],
    filter_settings: FilterSettings,
    interval_metadata: Mapping[tuple[int, int], SensorPlace],
) -> TransformDirectionResult:
    return transform_direction_rows(
        rows_for_window,
        interval_metadata,
        ignored_place_prefixes=filter_settings.ignored_place_prefixes,
    )


def _lookback_hours_by_interval(
    state_store: WindowStateStore,
    *,
    settings: RunDirectionSettings,
    run_started_at: datetime,
) -> dict[int, float]:
    """Return the lookback hours used by each interval this run."""
    return {
        interval_min: _lookback_hours(
            state_store,
            interval_min=interval_min,
            reprocess_hours=_reprocess_hours(settings, interval_min),
            max_lookback_hours=_max_lookback_hours(settings, interval_min),
            run_started_at=run_started_at,
        )
        for interval_min in _INTERVALS
    }


def _lookback_hours(
    state_store: WindowStateStore,
    *,
    interval_min: int,
    reprocess_hours: int,
    max_lookback_hours: int,
    run_started_at: datetime,
) -> float:
    """Pick the lookback for one interval.

    Starts at the reprocess-hours floor, expands to cover the oldest open
    window for this interval if it sits further back, then caps at the
    max-lookback ceiling. A run with no open windows always uses the floor.
    """
    prefix = f"{_INTERVAL_PREFIXES[interval_min]}/"
    oldest_retry_anchor: datetime | None = None
    for key, window in state_store.iter_open_windows():
        if not key.startswith(prefix):
            continue
        retry_anchor = state_store.retry_anchor(key, window)
        if oldest_retry_anchor is None or retry_anchor < oldest_retry_anchor:
            oldest_retry_anchor = retry_anchor

    oldest_age_hours = 0.0
    if oldest_retry_anchor is not None:
        oldest_age_hours = max(
            0.0,
            (run_started_at - oldest_retry_anchor).total_seconds() / 3600,
        )

    raw_lookback = max(float(reprocess_hours), oldest_age_hours)
    return max(float(reprocess_hours), min(raw_lookback, float(max_lookback_hours)))


def _log_run_started(
    *,
    settings: RunDirectionSettings,
    orion: _OrionForDirection,
    target_batches: list[str],
    lookback_hours_used: dict[int, float],
) -> None:
    """Emit the structured lifecycle record at the start of a run."""
    _lifecycle_logger.info(
        "direction run started",
        extra={
            "event": "run_started",
            "product": "direction",
            "send_mode": settings.send_mode,
            "target_batches": target_batches,
            "payload_mode": getattr(orion, "payload_mode", "failure"),
            "lookback_hours_used": lookback_hours_used,
        },
    )


def _log_run_summary(result: RunDirectionResult) -> None:
    """Emit the structured lifecycle record at the end of a run."""
    _lifecycle_logger.info(
        "direction run summary",
        extra={
            "event": "run_summary",
            "windows_seen": result.windows_seen,
            "windows_complete": result.windows_complete,
            "windows_partial": result.windows_partial,
            "windows_dead_letter": result.windows_dead_letter,
            "posts_ok": result.posts_ok,
            "posts_failed": result.posts_failed,
            "rows_dropped": result.rows_dropped,
            "oldest_non_complete": result.oldest_non_complete,
            "lookback_hours_used": result.lookback_hours_used,
        },
    )


def _log_windows_near_retry_horizon(
    state_store: WindowStateStore,
    *,
    interval_min: int,
    run_started_at: datetime,
    reprocess_hours: int,
    max_lookback_hours: int,
) -> None:
    """Log open windows that are about to fall outside the retry horizon."""
    prefix = f"{_INTERVAL_PREFIXES[interval_min]}/"
    threshold = run_started_at - timedelta(hours=max_lookback_hours - reprocess_hours)
    for key, window in state_store.iter_open_windows():
        if not key.startswith(prefix):
            continue
        retry_anchor = state_store.retry_anchor(key, window)
        if retry_anchor < threshold:
            logger.error(
                "window is near the retry horizon",
                extra={"event": "window_giving_up_soon", "window": key},
            )


def _oldest_non_complete(state_store: WindowStateStore) -> datetime | None:
    """Return the oldest retained pending or partial window timestamp."""
    oldest: datetime | None = None
    for key, window in state_store.iter_open_windows():
        retry_anchor = state_store.retry_anchor(key, window)
        if oldest is None or retry_anchor < oldest:
            oldest = retry_anchor
    return oldest


def _group_rows_by_startdate(
    rows: Iterable[_DirectionRow],
) -> Iterable[tuple[str, list[_DirectionRow]]]:
    """Group DB rows by source window while preserving first-seen order."""
    grouped: dict[str, list[_DirectionRow]] = {}
    order: list[str] = []
    for row in rows:
        startdate = str(row["startdate"])
        if startdate not in grouped:
            grouped[startdate] = []
            order.append(startdate)
        grouped[startdate].append(row)

    for startdate in order:
        yield startdate, grouped[startdate]


def _metadata_index_for_interval(
    metadata_index: Mapping[tuple[int, int], SensorPlace],
    interval_min: int,
) -> dict[tuple[int, int], SensorPlace]:
    """Restrict the active metadata index to one aggregation interval."""
    return {
        key: place for key, place in metadata_index.items() if key[1] == interval_min
    }


def _attrs_sha256(attrs: Mapping[str, Any]) -> str:
    """Return the canonical payload hash used for unchanged-target skips.

    ``dateRetrieved`` is excluded because it changes every run; including it
    would mark every prior-OK target as drifted and defeat the skip path.
    """
    hashable = {key: value for key, value in attrs.items() if key != "dateRetrieved"}
    body = json.dumps(hashable, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(body).hexdigest()


def _window_key(interval_min: int, startdate: str) -> str:
    """Return the state key for one source aggregation window."""
    return f"{_INTERVAL_PREFIXES[interval_min]}/{startdate}"


def _parse_source_window_start(startdate: str) -> datetime:
    """Parse the MySQL source-window key into a JST datetime."""
    return datetime.strptime(startdate, "%Y%m%d_%H%M").replace(tzinfo=JST)


def _eligible_source_cutoff(
    run_started_at: datetime,
    interval_min: int,
    *,
    source_stability_delay_hours: int,
) -> datetime:
    """Return the newest source window eligible for publication."""
    return _floor_datetime(
        run_started_at - timedelta(hours=source_stability_delay_hours),
        interval_min,
    )


def _floor_datetime(value: datetime, interval_min: int) -> datetime:
    """Round a datetime down to the nearest interval boundary."""
    return value - timedelta(
        minutes=value.minute % interval_min,
        seconds=value.second,
        microseconds=value.microsecond,
    )


def _format_sql_window_bound(value: datetime) -> str:
    """Format a source-window timestamp for the MySQL ``startdate`` column."""
    return value.strftime("%Y%m%d_%H%M")


def _reprocess_hours(settings: RunDirectionSettings, interval_min: int) -> int:
    """Return the configured minimum reprocess span for one interval."""
    if interval_min == 5:
        return settings.reprocess_hours_per300
    return settings.reprocess_hours_per3600


def _max_lookback_hours(settings: RunDirectionSettings, interval_min: int) -> int:
    """Return the configured maximum reprocess span for one interval."""
    if interval_min == 5:
        return settings.max_lookback_hours_per300
    return settings.max_lookback_hours_per3600


def _parse_state_datetime(value: str) -> datetime:
    """Parse an ISO timestamp from state and normalize it to JST."""
    return _coerce_jst_datetime(datetime.fromisoformat(value))


def _coerce_jst_datetime(value: datetime) -> datetime:
    """Return ``value`` as a JST-aware datetime."""
    if value.tzinfo is None:
        return value.replace(tzinfo=JST)
    return value.astimezone(JST)


def _metadata_path_from_env(env: Mapping[str, str] | None = None) -> Path:
    """Return the runtime metadata path from the environment."""
    source = os.environ if env is None else env
    value = source.get("SENSOR_METADATA_PATH")
    if value is None or value == "":
        return _DEFAULT_METADATA_PATH
    return Path(value)


def _copy_payload_logging_settings(
    orion: _OrionForDirection,
    settings: LoggingSettings,
) -> None:
    """Copy the payload-logging fields from ``LoggingSettings`` onto ``orion``."""
    for name in ("payload_mode", "payload_max_bytes", "response_max_bytes"):
        if hasattr(orion, name):
            setattr(orion, name, getattr(settings, name))


def _optional_env(env: Mapping[str, str], key: str, default: str) -> str:
    """Return an environment value or a default when unset or empty."""
    value = env.get(key)
    if value is None or value == "":
        return default
    return value


def _int_env(env: Mapping[str, str], key: str, default: int) -> int:
    """Parse an optional integer environment variable."""
    value = _optional_env(env, key, "")
    if value == "":
        return default
    try:
        return int(value)
    except ValueError as exc:
        raise RunDirectionConfigError(
            f"environment variable must be an integer: {key}"
        ) from exc


def _validate_non_negative_hours(values: Mapping[str, int]) -> None:
    """Reject negative hour settings."""
    for key, value in values.items():
        if value < 0:
            raise RunDirectionConfigError(
                f"environment variable must be non-negative: {key}"
            )


def _validate_lookback_ceiling(name: str, reprocess_hours: int, max_hours: int) -> None:
    """Reject a max lookback that is lower than its reprocess floor."""
    if max_hours < reprocess_hours:
        raise RunDirectionConfigError(
            f"MAX_LOOKBACK_HOURS_{name} must be >= REPROCESS_HOURS_{name}"
        )


if __name__ == "__main__":
    raise SystemExit(main())
