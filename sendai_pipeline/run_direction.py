"""Run the direction-metric publishing pipeline."""

import fcntl
import hashlib
import json
import logging
import os
import sys
from collections.abc import Callable, Iterable, Mapping
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Protocol

from dotenv import load_dotenv

from sendai_pipeline import auth, db, orion_client
from sendai_pipeline.filter_settings import FilterConfigError, FilterSettings
from sendai_pipeline.logging_setup import LoggingSettings, configure_logging
from sendai_pipeline.metadata import (
    SensorPlace,
    active_places,
    index_by_place_interval,
    load_metadata,
    metadata_path_from_env,
)
from sendai_pipeline.revision_sweep import (
    RevisionWorkItem,
    revision_retry_items,
    split_discovered_revisions,
)
from sendai_pipeline.settings_validation import (
    optional_env,
    parse_exact_env_value,
    parse_int_env,
    validate_lookback_ceiling,
    validate_non_negative_settings,
)
from sendai_pipeline.state import WindowStateStore
from sendai_pipeline.transform_direction import (
    DirectionNoPayloadOutcome,
    DirectionPayloadOutcome,
    DirectionSourceInvalidOutcome,
    DirectionTransformOutcome,
    transform_direction_window,
)
from sendai_pipeline.windowing import (
    JST,
    coerce_jst_datetime,
    eligible_source_cutoff,
    format_mysql_timestamp,
    format_sql_window_bound,
    parse_revision_aggregated_at,
    parse_source_window_start,
)
from sendai_pipeline.windowing import (
    window_key as make_window_key,
)

logger = logging.getLogger(__name__)
_lifecycle_logger = logging.getLogger("sendai_pipeline")

_DIRECTION_INTERVAL_MIN = 60
_INTERVALS: tuple[int, ...] = (_DIRECTION_INTERVAL_MIN,)
_INTERVAL_PREFIXES: dict[int, str] = {_DIRECTION_INTERVAL_MIN: "per3600"}
_VALID_SEND_MODES: frozenset[str] = frozenset({"dry-run", "send"})
_DEFAULT_SOURCE_STABILITY_DELAY_HOURS = 3
_DEFAULT_REVISION_SWEEP_MAX_WINDOWS = 2000

# Keep each discovery scan small enough for the MySQL read timeout; the
# max-window setting controls PUT volume separately from this time span.
REVISION_SWEEP_DISCOVERY_SPAN = timedelta(hours=6)

_DirectionRow = Mapping[str, Any]
_PutResult = Mapping[str, Any]

__all__ = [
    "DirectionWindowPublishResult",
    "FilterConfigError",
    "JST",
    "RunDirectionConfigError",
    "RunDirectionResult",
    "RunDirectionSettings",
    "main",
    "publish_direction_window",
    "replay_direction_window",
    "run_direction",
]


class RunDirectionConfigError(RuntimeError):
    """Raised when direction-run configuration is invalid."""


class _DbConnection(Protocol):
    def cursor(self) -> Any:
        """Return a DB-API cursor context manager."""
        ...


class _OrionForDirection(Protocol):
    def replace_attrs(
        self,
        entity_id: str,
        entity_type: str,
        attrs: Mapping[str, Any],
        *,
        dry_run: bool = False,
    ) -> _PutResult:
        """Fully replace one Orion entity's attributes."""
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
        send_mode: ``"dry-run"`` to avoid live PUTs, or ``"send"`` to write
            attributes to Orion.
        reprocess_hours_per3600: Minimum 60-minute lookback.
        max_lookback_hours_per3600: Maximum 60-minute lookback.
        source_stability_delay_hours: Minimum age of a source window's start
            time before it becomes eligible for publication.
        revision_sweep_enabled: Whether to scan old windows by revision time.
        revision_sweep_max_windows: Maximum revision-sweep windows per run.
        state_path: JSON state file path.
        lock_path: Process lock file path.
        product_b_aggregate_entity_id: Orion entity id for aggregate Product B
            writes.
        product_b_aggregate_entity_type: Orion entity type for aggregate Product B
            writes.
    """

    send_mode: str = "dry-run"
    reprocess_hours_per3600: int = 12
    max_lookback_hours_per3600: int = 72
    source_stability_delay_hours: int = _DEFAULT_SOURCE_STABILITY_DELAY_HOURS
    revision_sweep_enabled: bool = True
    revision_sweep_max_windows: int = _DEFAULT_REVISION_SWEEP_MAX_WINDOWS
    state_path: Path = Path("state/direction.json")
    lock_path: Path = Path("state/direction.lock")
    product_b_aggregate_entity_id: str = "jp.sendai.Blesensor.flow"
    product_b_aggregate_entity_type: str = "Blesensor.flow"

    def __post_init__(self) -> None:
        if self.send_mode not in _VALID_SEND_MODES:
            raise RunDirectionConfigError(
                f"invalid DIRECTION_SEND_MODE {self.send_mode!r}; "
                "expected dry-run or send"
            )
        validate_non_negative_settings(
            {
                "REPROCESS_HOURS_PER3600": self.reprocess_hours_per3600,
                "MAX_LOOKBACK_HOURS_PER3600": self.max_lookback_hours_per3600,
                "SOURCE_STABILITY_DELAY_HOURS": self.source_stability_delay_hours,
            },
            RunDirectionConfigError,
        )
        validate_lookback_ceiling(
            "PER3600",
            self.reprocess_hours_per3600,
            self.max_lookback_hours_per3600,
            RunDirectionConfigError,
        )
        if self.revision_sweep_max_windows < 1:
            raise RunDirectionConfigError("REVISION_SWEEP_MAX_WINDOWS must be positive")

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
            send_mode=optional_env(source, "DIRECTION_SEND_MODE", "dry-run")
            .strip()
            .lower(),
            reprocess_hours_per3600=parse_int_env(
                source,
                "REPROCESS_HOURS_PER3600",
                12,
                RunDirectionConfigError,
            ),
            max_lookback_hours_per3600=parse_int_env(
                source,
                "MAX_LOOKBACK_HOURS_PER3600",
                72,
                RunDirectionConfigError,
            ),
            source_stability_delay_hours=parse_int_env(
                source,
                "SOURCE_STABILITY_DELAY_HOURS",
                _DEFAULT_SOURCE_STABILITY_DELAY_HOURS,
                RunDirectionConfigError,
            ),
            revision_sweep_enabled=auth._parse_bool(
                optional_env(source, "REVISION_SWEEP_ENABLED", "true")
            ),
            revision_sweep_max_windows=parse_int_env(
                source,
                "REVISION_SWEEP_MAX_WINDOWS",
                _DEFAULT_REVISION_SWEEP_MAX_WINDOWS,
                RunDirectionConfigError,
            ),
            product_b_aggregate_entity_id=parse_exact_env_value(
                source,
                "PRODUCT_B_AGGREGATE_ENTITY_ID",
                "jp.sendai.Blesensor.flow",
                RunDirectionConfigError,
            ),
            product_b_aggregate_entity_type=parse_exact_env_value(
                source,
                "PRODUCT_B_AGGREGATE_ENTITY_TYPE",
                "Blesensor.flow",
                RunDirectionConfigError,
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
        puts_ok: Full-replace attempts that succeeded.
        puts_failed: Full-replace attempts that failed.
        windows_degraded: Degraded payloads attempted in send mode or
            previewed in dry-run. Unchanged degraded payloads are not counted.
        windows_no_payload: Windows with no surviving candidate places.
        windows_source_invalid: Windows whose every candidate is missing a
            required source total.
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
    puts_ok: int
    puts_failed: int
    windows_degraded: int
    windows_no_payload: int
    windows_source_invalid: int
    rows_dropped: int
    oldest_non_complete: datetime | None
    lookback_hours_used: dict[int, float]
    exit_code: int


@dataclass(frozen=True)
class DirectionWindowPublishResult:
    """Publishing counters produced for one Product B source window.

    Run-level window selection is intentionally excluded; orchestration owns
    ``windows_seen``.

    Attributes:
        windows_complete: ``1`` if the window ended complete, otherwise ``0``.
        windows_partial: ``1`` if the window ended partial, otherwise ``0``.
        windows_dead_letter: ``1`` if the window ended dead-lettered,
            otherwise ``0``.
        puts_ok: Orion aggregate replacements that succeeded.
        puts_failed: Orion aggregate replacements that failed.
        windows_degraded: ``1`` if a degraded payload was written,
            otherwise ``0``.
        windows_no_payload: ``1`` if filtering left no payload, otherwise ``0``.
        windows_source_invalid: ``1`` if required source totals were missing,
            otherwise ``0``.
        rows_dropped: Source rows omitted during transformation.
    """

    windows_complete: int
    windows_partial: int
    windows_dead_letter: int
    puts_ok: int
    puts_failed: int
    windows_degraded: int
    windows_no_payload: int
    windows_source_invalid: int
    rows_dropped: int


def publish_direction_window(
    *,
    interval_min: int,
    startdate: str,
    rows_for_window: list[_DirectionRow],
    orion: _OrionForDirection,
    state_store: WindowStateStore,
    settings: RunDirectionSettings,
    filter_settings: FilterSettings,
    interval_metadata: Mapping[tuple[int, int], SensorPlace],
    transformed_at: datetime,
) -> DirectionWindowPublishResult:
    """Publish one Product B window under normal runner policy.

    Args:
        interval_min: Source aggregation interval; Product B requires 60.
        startdate: Source window start in ``YYYYMMDD_HHMM`` format.
        rows_for_window: Complete Product B source rows for the window.
        orion: Orion writer for the aggregate attribute replacement.
        state_store: Delivery state store whose attempted target is persisted.
        settings: Product B aggregate target settings.
        filter_settings: Source-row filters used by the transform.
        interval_metadata: Active Product B metadata for this interval.
        transformed_at: Timestamp used for retrieval and quality metadata.

    Returns:
        Immutable counters for this window, excluding ``windows_seen``.

    Raises:
        ValueError: If ``interval_min`` is not 60.
    """
    return _publish_direction_window(
        interval_min=interval_min,
        startdate=startdate,
        rows_for_window=rows_for_window,
        orion=orion,
        state_store=state_store,
        settings=settings,
        filter_settings=filter_settings,
        interval_metadata=interval_metadata,
        transformed_at=transformed_at,
        force_resend=False,
        persist_after_write=True,
    )


def replay_direction_window(
    *,
    interval_min: int,
    startdate: str,
    rows_for_window: list[_DirectionRow],
    orion: _OrionForDirection,
    state_store: WindowStateStore,
    settings: RunDirectionSettings,
    filter_settings: FilterSettings,
    interval_metadata: Mapping[tuple[int, int], SensorPlace],
    transformed_at: datetime,
    force: bool,
) -> DirectionWindowPublishResult:
    """Replay one Product B window under operator-requested policy.

    Args:
        interval_min: Source aggregation interval; Product B requires 60.
        startdate: Source window start in ``YYYYMMDD_HHMM`` format.
        rows_for_window: Complete Product B source rows for the window.
        orion: Orion writer for the aggregate attribute replacement.
        state_store: Delivery state store to update in memory. The caller owns
            persistence cadence.
        settings: Product B aggregate target settings.
        filter_settings: Source-row filters used by the transform.
        interval_metadata: Active Product B metadata for this interval.
        transformed_at: Timestamp used for retrieval and quality metadata.
        force: Whether to rewrite an unchanged prior-success aggregate.

    Returns:
        Immutable counters for this window, excluding ``windows_seen``.

    Raises:
        ValueError: If ``interval_min`` is not 60.
    """
    return _publish_direction_window(
        interval_min=interval_min,
        startdate=startdate,
        rows_for_window=rows_for_window,
        orion=orion,
        state_store=state_store,
        settings=settings,
        filter_settings=filter_settings,
        interval_metadata=interval_metadata,
        transformed_at=transformed_at,
        force_resend=force,
        persist_after_write=False,
    )


def _publish_direction_window(
    *,
    interval_min: int,
    startdate: str,
    rows_for_window: list[_DirectionRow],
    orion: _OrionForDirection,
    state_store: WindowStateStore,
    settings: RunDirectionSettings,
    filter_settings: FilterSettings,
    interval_metadata: Mapping[tuple[int, int], SensorPlace],
    transformed_at: datetime,
    force_resend: bool,
    persist_after_write: bool,
) -> DirectionWindowPublishResult:
    if interval_min != _DIRECTION_INTERVAL_MIN:
        raise ValueError(
            f"Product B supports only 60-minute windows, got {interval_min}"
        )

    counts = _RunCounts()
    _process_send_window(
        make_window_key(interval_min, startdate),
        interval_min=interval_min,
        startdate=startdate,
        rows_for_window=rows_for_window,
        orion=orion,
        state_store=state_store,
        settings=settings,
        filter_settings=filter_settings,
        interval_metadata=interval_metadata,
        counts=counts,
        transformed_at=transformed_at,
        force_resend=force_resend,
        persist_after_write=persist_after_write,
    )
    return DirectionWindowPublishResult(
        windows_complete=counts.windows_complete,
        windows_partial=counts.windows_partial,
        windows_dead_letter=counts.windows_dead_letter,
        puts_ok=counts.puts_ok,
        puts_failed=counts.puts_failed,
        windows_degraded=counts.windows_degraded,
        windows_no_payload=counts.windows_no_payload,
        windows_source_invalid=counts.windows_source_invalid,
        rows_dropped=counts.rows_dropped,
    )


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
    """Publish aggregate direction metrics for eligible 60-minute windows.

    A *reprocessing window* is a source aggregation window whose
    ``startdate`` falls within the lookback range that ends at the
    stability cutoff.  A window is *eligible* when its source data is
    old enough to have settled (controlled by
    ``source_stability_delay_hours``) and young enough to still be
    within the maximum lookback horizon.

    Args:
        db_connection: Database connection used by ``sendai_pipeline.db``.
        orion: Orion client or test double exposing ``replace_attrs``.
        metadata: Runtime sensor metadata.
        state_store: Per-window delivery state store.
        settings: Direction-run settings.
        filter_settings: Batch and source-row filter settings.
        now: Clock returning the run timestamp. Called once.

    Returns:
        Run summary including ``exit_code``: ``1`` in send mode when any
        degraded payload is attempted, or any source-invalid, partial,
        failed-PUT, or open window remains after the run; ``0`` otherwise
        (including dry-run).
    """
    run_started_at = coerce_jst_datetime(now())
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
            puts_ok=0,
            puts_failed=0,
            windows_degraded=0,
            windows_no_payload=0,
            windows_source_invalid=0,
            rows_dropped=0,
            oldest_non_complete=None,
            lookback_hours_used=lookback_hours_used,
            exit_code=0,
        )
        _log_run_summary(result)
        return result

    active_source_places = active_places(metadata, target_batches=target_batches)
    metadata_index = index_by_place_interval(active_source_places)
    _validate_orion_target(
        settings.product_b_aggregate_entity_id,
        settings.product_b_aggregate_entity_type,
        orion,
    )

    counts = _RunCounts()
    revision_sweep_windows: set[str] = set()
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

    if settings.revision_sweep_enabled:
        revision_sweep_windows = _process_revision_sweep(
            db_connection=db_connection,
            orion=orion,
            state_store=state_store,
            settings=settings,
            filter_settings=filter_settings,
            metadata_index=metadata_index,
            lookback_hours_used=lookback_hours_used,
            run_started_at=run_started_at,
            counts=counts,
        )

    if settings.send_mode == "send":
        # Use 2× the maximum lookback as the GC horizon — same reasoning as
        # run_flow: keeps a safety margin for windows near the retry boundary.
        cutoff_gc = run_started_at - timedelta(
            hours=2 * settings.max_lookback_hours_per3600
        )
        state_store.gc_complete_before(
            cutoff_gc,
            preserve_window_keys=revision_sweep_windows,
        )
        state_store.save()

    oldest_non_complete = _oldest_non_complete(state_store)
    has_open_windows = oldest_non_complete is not None
    exit_code = (
        1
        if settings.send_mode == "send"
        and (
            counts.windows_source_invalid > 0
            or counts.windows_degraded > 0
            or counts.windows_partial > 0
            or counts.puts_failed > 0
            or has_open_windows
        )
        else 0
    )
    result = RunDirectionResult(
        windows_seen=counts.windows_seen,
        windows_complete=counts.windows_complete,
        windows_partial=counts.windows_partial,
        windows_dead_letter=counts.windows_dead_letter,
        puts_ok=counts.puts_ok,
        puts_failed=counts.puts_failed,
        windows_degraded=counts.windows_degraded,
        windows_no_payload=counts.windows_no_payload,
        windows_source_invalid=counts.windows_source_invalid,
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
            print(
                "[sendai-pipeline] direction run skipped: lock held by another process",
                file=sys.stderr,
            )
            return 0

        logging_settings = LoggingSettings.from_env()
        configure_logging(logging_settings, product="direction")

        filter_settings = FilterSettings.from_env()
        auth_settings = auth.AuthSettings.from_env()
        auth_client = auth.AuthClient(auth_settings)
        orion = orion_client.OrionClient(
            orion_client.OrionSettings.from_env(),
            auth=auth_client,
            payload_mode=logging_settings.payload_mode,
            payload_max_bytes=logging_settings.payload_max_bytes,
            response_max_bytes=logging_settings.response_max_bytes,
        )

        db_connection = db.connect(db.DbSettings.from_env())
        try:
            result = run_direction(
                db_connection=db_connection,
                orion=orion,
                metadata=load_metadata(metadata_path_from_env()),
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
    puts_ok: int = 0
    puts_failed: int = 0
    windows_degraded: int = 0
    windows_no_payload: int = 0
    windows_source_invalid: int = 0
    rows_dropped: int = 0


def _validate_orion_target(
    entity_id: str,
    entity_type: str,
    orion: _OrionForDirection,
) -> None:
    """Check whether Orion currently contains the aggregate Product B target.

    Returns ``None`` — this function only logs; it never raises and never
    blocks the run. A missing target does not stop publication because the PUT
    result is the authoritative delivery outcome.
    """
    limit = 1000
    entities = orion.list_entities(entity_type, attrs="id", limit=limit)
    live_ids = {str(entity["id"]) for entity in entities if "id" in entity}
    if len(entities) >= limit:
        logger.warning(
            "orion list_entities response may be truncated",
            extra={
                "event": "entity_map_truncated",
                "entity_type": entity_type,
                "count_live": len(live_ids),
                "limit": limit,
            },
        )
    if entity_id not in live_ids:
        logger.warning(
            "aggregate target missing from orion",
            extra={
                "event": "entity_map_missing_target",
                "entity_type": entity_type,
                "entity_id": entity_id,
            },
        )
    logger.info(
        "validated aggregate target against orion",
        extra={
            "event": "entity_map_refreshed",
            "entity_type": entity_type,
            "count_expected": 1,
            "count_live": len(live_ids),
            "count_missing": int(entity_id not in live_ids),
            "count_extra": len(live_ids - {entity_id}),
        },
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
    cutoff = eligible_source_cutoff(
        run_started_at,
        interval_min,
        source_stability_delay_hours=settings.source_stability_delay_hours,
    )
    rows = db.select_direction_metrics(
        db_connection,
        interval_min=interval_min,
        lower_bound=format_sql_window_bound(cutoff - timedelta(hours=lookback_hours)),
        upper_bound=format_sql_window_bound(cutoff),
    )
    interval_metadata = _metadata_index_for_interval(metadata_index, interval_min)

    for startdate, rows_for_window in _group_rows_by_startdate(rows):
        counts.windows_seen += 1

        if settings.send_mode == "send":
            result = publish_direction_window(
                interval_min=interval_min,
                startdate=startdate,
                rows_for_window=rows_for_window,
                orion=orion,
                state_store=state_store,
                settings=settings,
                filter_settings=filter_settings,
                interval_metadata=interval_metadata,
                transformed_at=run_started_at,
            )
            counts.windows_complete += result.windows_complete
            counts.windows_partial += result.windows_partial
            counts.windows_dead_letter += result.windows_dead_letter
            counts.puts_ok += result.puts_ok
            counts.puts_failed += result.puts_failed
            counts.windows_degraded += result.windows_degraded
            counts.windows_no_payload += result.windows_no_payload
            counts.windows_source_invalid += result.windows_source_invalid
            counts.rows_dropped += result.rows_dropped
        else:
            _process_dry_run_window(
                rows_for_window=rows_for_window,
                orion=orion,
                settings=settings,
                filter_settings=filter_settings,
                interval_metadata=interval_metadata,
                counts=counts,
                window_key=make_window_key(interval_min, startdate),
                transformed_at=run_started_at,
            )

    if settings.send_mode == "send":
        _log_windows_near_retry_horizon(
            state_store,
            interval_min=interval_min,
            run_started_at=run_started_at,
            reprocess_hours=settings.reprocess_hours_per3600,
            max_lookback_hours=settings.max_lookback_hours_per3600,
        )


def _process_send_window(
    window_key: str,
    *,
    interval_min: int,
    startdate: str,
    rows_for_window: list[_DirectionRow],
    orion: _OrionForDirection,
    state_store: WindowStateStore,
    settings: RunDirectionSettings,
    filter_settings: FilterSettings,
    interval_metadata: Mapping[tuple[int, int], SensorPlace],
    counts: _RunCounts,
    transformed_at: datetime,
    persist_after_write: bool,
    force_resend: bool = False,
) -> bool:
    """PUT one clean or degraded aggregate and update target state.

    The transform runs before state creation. A degraded payload includes only
    the complete candidate places and logs a warning when a PUT is attempted.
    An unchanged prior-OK degraded payload is a DEBUG no-op. An outcome with
    no payload, or with every candidate excluded (source-invalid), creates no
    window record.

    Args:
        window_key: State key for this source window.
        interval_min: Aggregation interval in minutes.
        startdate: Source ``startdate`` string for the window.
        rows_for_window: Direction metric rows fetched for this window.
        orion: Orion client facade used for the full-replace write.
        state_store: Persistent window-state store.
        settings: Runner settings containing the aggregate target.
        filter_settings: Runtime filters applied before payload creation.
        interval_metadata: Active Product B metadata for this interval.
        counts: Mutable per-run counters updated while sending this window.
        transformed_at: Timestamp used for the payload's retrieval time.
        persist_after_write: Whether to save the target attempt immediately.
        force_resend: Whether to rewrite an unchanged prior-``ok`` target.

    Returns:
        ``True`` for a payload outcome; ``False`` for a no-write outcome.

    """
    transformed = _transform(
        rows_for_window,
        filter_settings,
        interval_metadata,
        settings=settings,
        transformed_at=transformed_at,
    )
    counts.rows_dropped += transformed.rows_dropped
    if _record_no_write_outcome(window_key, transformed, counts):
        return False
    if not isinstance(transformed, DirectionPayloadOutcome):
        raise TypeError(f"unsupported direction transform outcome: {type(transformed)}")

    source_start = parse_source_window_start(startdate)
    aggregate_target_ids = [settings.product_b_aggregate_entity_id]
    state_store.begin_window_attempt(
        window_key,
        interval_min=interval_min,
        source_window_start=source_start,
        source_window_end=source_start + timedelta(minutes=interval_min),
        expected_target_ids=aggregate_target_ids,
    )

    payload = transformed.payload
    attrs = payload["attrs"]
    payload_sha256 = _attrs_sha256(attrs)
    entity_id = str(payload["entity_id"])
    prior = state_store.target_record(window_key, entity_id)
    degraded = bool(transformed.excluded_place_numbers)
    # Prior-ok unchanged payloads are true no-ops. A prior-ok hash drift means
    # the source was revised after delivery, so replace the aggregate again.
    if not force_resend and prior is not None and prior.get("status") == "ok":
        prior_payload_sha256 = prior.get("last_payload_sha256")
        if prior_payload_sha256 == payload_sha256:
            if degraded:
                _log_degraded_window(
                    window_key,
                    transformed,
                    event="direction_window_degraded_unchanged",
                    attempted=False,
                )
            else:
                logger.debug(
                    "aggregate payload unchanged",
                    extra={
                        "event": "put_skipped_unchanged",
                        "entity_id": entity_id,
                        "window": window_key,
                        "payload_sha256": payload_sha256,
                    },
                )
        else:
            logger.debug(
                "aggregate payload drift resent",
                extra={
                    "event": "put_resent_drift",
                    "entity_id": entity_id,
                    "window": window_key,
                    "prior_payload_sha256": prior_payload_sha256,
                    "computed_payload_sha256": payload_sha256,
                },
            )

    should_send = (
        force_resend
        or prior is None
        or prior.get("status") != "ok"
        or (prior.get("last_payload_sha256") != payload_sha256)
    )
    if should_send:
        if degraded:
            counts.windows_degraded += 1
            _log_degraded_window(
                window_key,
                transformed,
                event="direction_window_degraded",
                attempted=True,
            )
        result = orion.replace_attrs(
            entity_id,
            payload["entity_type"],
            attrs,
        )
        ok = bool(result.get("ok"))
        if ok:
            counts.puts_ok += 1
        else:
            counts.puts_failed += 1
        state_store.record_target(
            window_key,
            entity_id,
            status="ok" if ok else "failed",
            http_status=int(result.get("status", 0)),
            payload_sha256=payload_sha256,
        )
        if persist_after_write:
            state_store.save()

    # The unchanged branch still recomputes the existing complete status after
    # refreshing the expected-target snapshot above.
    status = state_store.recompute_status(window_key, aggregate_target_ids)
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

    return True


def _process_revision_sweep(
    *,
    db_connection: _DbConnection,
    orion: _OrionForDirection,
    state_store: WindowStateStore,
    settings: RunDirectionSettings,
    filter_settings: FilterSettings,
    metadata_index: Mapping[tuple[int, int], SensorPlace],
    lookback_hours_used: dict[int, float],
    run_started_at: datetime,
    counts: _RunCounts,
) -> set[str]:
    """Discover old revised windows and publish their current payloads.

    The sweep is ordered by source ``aggregated_at`` rather than by recent
    ``startdate``.  It resolves the forward-only revision cursor, scans the
    half-open chunk ``[cursor, upper)``, refetches full source windows, then
    sends or dry-runs each current payload.  In send mode the cursor advances
    after the work list finishes.  Failed PUTs stay as ``partial`` window
    state and are retried from state; they do not hold the cursor back.

    Args:
        db_connection: Open MySQL connection for discovery and refetch queries.
        orion: Orion client facade used to PUT (or dry-run) payloads.
        state_store: Persistent window-state store; holds the cursor and the
            open windows the retry pass re-sends.
        settings: Runner settings, including send mode and the per-run cap.
        filter_settings: Runtime filters applied by the Product B transform,
            such as the ignored place-prefix filter.
        metadata_index: Active ``(place_number, interval_min)`` -> metadata map.
        lookback_hours_used: Per-interval lookback the fresh path applied this
            run.  The fresh path's lower bound equals the sweep's
            ``startdate`` upper bound, so the two paths never process the
            same window.
        run_started_at: Run start, floored to seconds; it bounds the
            discovery chunk's upper edge.
        counts: Mutable per-run counters updated by this sweep.

    Returns:
        State window keys processed by this sweep run.  The caller preserves
        these keys during same-run GC so a completed old window is not removed
        before the final state save.
    """
    # A missing stored cursor means this state file has not swept revisions
    # yet.  Seed it at the current run start and skip discovery this run:
    # unlike flow, direction keeps no fixed historical seed, so there is
    # nothing queued to drain — discovery starts covering revisions from this
    # run forward.
    run_upper = run_started_at.replace(microsecond=0)
    stored_cursor = state_store.revision_cursor()
    if stored_cursor is None:
        if settings.send_mode == "send":
            state_store.set_revision_cursor(run_upper)
            logger.debug(
                "initialized direction revision cursor",
                extra={
                    "event": "revision_cursor_advanced",
                    "old_cursor": None,
                    "new_cursor": run_upper,
                },
            )
        return set()

    # Resolve the half-open discovery chunk.  The upper bound is the earlier
    # of the floored run start and one discovery span past the cursor, so a
    # cursor that has fallen behind (e.g. after missed runs) still drains in
    # bounded MySQL scans.
    cursor = coerce_jst_datetime(stored_cursor).replace(microsecond=0)
    span_upper = cursor + REVISION_SWEEP_DISCOVERY_SPAN
    aggregated_at_upper = max(cursor, min(run_upper, span_upper))
    chunk_binds = span_upper < run_upper
    aggregated_at_lower_sql = format_mysql_timestamp(cursor)
    aggregated_at_upper_sql = format_mysql_timestamp(aggregated_at_upper)
    interval_metadata_by_interval = {
        interval_min: _metadata_index_for_interval(metadata_index, interval_min)
        for interval_min in _INTERVALS
    }
    startdate_upper_by_interval: dict[int, datetime] = {}
    discovered: list[RevisionWorkItem] = []

    _lifecycle_logger.info(
        "direction revision sweep started",
        extra={
            "event": "revision_sweep_started",
            "product": "direction",
            "old_cursor": cursor,
            "new_cursor": aggregated_at_upper,
        },
    )

    # Discover candidate windows per interval.  ``startdate_upper`` is the
    # fresh path's lower bound; the sweep handles only older windows that the
    # normal rolling lookback will not publish in this run.
    for interval_min in _INTERVALS:
        cutoff = eligible_source_cutoff(
            run_started_at,
            interval_min,
            source_stability_delay_hours=settings.source_stability_delay_hours,
        )
        startdate_upper = cutoff - timedelta(hours=lookback_hours_used[interval_min])
        startdate_upper_by_interval[interval_min] = startdate_upper
        rows = db.discover_direction_revised_windows(
            db_connection,
            interval_min=interval_min,
            aggregated_at_lower=aggregated_at_lower_sql,
            aggregated_at_upper=aggregated_at_upper_sql,
            startdate_upper=format_sql_window_bound(startdate_upper),
        )
        for row in rows:
            startdate = str(row["startdate"])
            discovered.append(
                RevisionWorkItem(
                    interval_min=interval_min,
                    startdate=startdate,
                    window_key=make_window_key(interval_min, startdate),
                    aggregated_at=parse_revision_aggregated_at(row["win_agg"]),
                )
            )

    # Process revisions in cursor order, then apply the soft cap.  The cap
    # keeps a whole ``aggregated_at`` second together so the next cursor can
    # safely point at the first deferred second.
    discovered.sort(
        key=lambda item: (
            item.aggregated_at or cursor,
            item.startdate,
            item.interval_min,
        )
    )
    discovered_to_process, discovered_deferred = split_discovered_revisions(
        discovered,
        settings.revision_sweep_max_windows,
    )
    discovered_keys = {item.window_key for item in discovered}
    retry_items: list[RevisionWorkItem] = []
    # The retry pass re-sends old open (pending/partial) windows the fresh
    # path's lookback can no longer reach (typically one whose earlier send
    # failed).  It runs only at steady state, never during the initial drain,
    # and skips every key discovery surfaced this run (``discovered_keys``,
    # cap-deferred ones included).
    #
    # The steady-state restriction matters because each window has two
    # independent clocks.  GC and the lookback use its ``startdate`` (how old
    # the window is); discovery and this cursor use its ``aggregated_at`` (how
    # recently it was revised).  So an old window can be GC-eligible the moment
    # it completes while its newest revision still sits ahead of the cursor.
    #
    # During the drain the cursor lags far behind now, so such a window exists:
    # old by ``startdate`` yet re-revised at an ``aggregated_at`` the cursor has
    # not reached (say a window dated 06-10, corrected again at 06-29 while the
    # cursor is still back at 06-27).  It is not discovered this run, so the
    # exclusion below cannot skip it.  Were retry to complete it, GC would soon
    # drop it (old ``startdate``), and a later run, on reaching that revision,
    # would re-discover it with no state and send it again: a duplicate.
    # Suppression instead leaves the window's open state in place, so the run
    # that reaches its revision processes it through discovery with state
    # intact, which avoids the complete-then-GC that would let a later chunk
    # re-send it.
    #
    # At steady state the cursor has caught up: ``upper`` is the run start, so
    # discovery covers every revision since the last run.  A freshly revised
    # window is therefore always discovered (and excluded), leaving retry only
    # windows whose revision is already behind the cursor, which discovery can
    # never re-surface, so completing them here is safe.
    if settings.send_mode == "send" and not chunk_binds:
        remaining_capacity = max(
            0,
            settings.revision_sweep_max_windows - len(discovered_to_process),
        )
        retry_items = revision_retry_items(
            state_store,
            startdate_upper_by_interval=startdate_upper_by_interval,
            excluded_window_keys=discovered_keys,
            limit=remaining_capacity,
        )

    # Assemble the work list, dropping any window an operator dead-lettered
    # (that status is the operator's decision to stop sending it).  Then refetch
    # each window's complete current rows: discovery returns only the changed
    # (interval, startdate) key, not the rows, so rebuild the window's payloads
    # from a fresh full read.
    work_items = [
        item
        for item in [*discovered_to_process, *retry_items]
        if state_store.window_status(item.window_key) != "dead_letter"
    ]
    rows_by_key = _select_revision_direction_rows(db_connection, work_items)

    # Do not catch unexpected per-window errors here.  If transformation or
    # writing raises before normal state records are written, abort before the
    # cursor is saved so the same discovery range is retried next run.
    for item in work_items:
        if item.retry:
            logger.debug(
                "retrying old revision-sweep window",
                extra={"event": "revision_retry_attempted", "window": item.window_key},
            )
        rows_for_window = rows_by_key.get((item.interval_min, item.startdate), [])
        counts.windows_seen += 1
        if settings.send_mode == "send":
            _process_send_window(
                item.window_key,
                interval_min=item.interval_min,
                startdate=item.startdate,
                rows_for_window=rows_for_window,
                orion=orion,
                state_store=state_store,
                settings=settings,
                filter_settings=filter_settings,
                interval_metadata=interval_metadata_by_interval[item.interval_min],
                counts=counts,
                transformed_at=run_started_at,
                force_resend=not item.retry,
                persist_after_write=True,
            )
        else:
            _process_dry_run_window(
                rows_for_window=rows_for_window,
                orion=orion,
                settings=settings,
                filter_settings=filter_settings,
                interval_metadata=interval_metadata_by_interval[item.interval_min],
                counts=counts,
                window_key=item.window_key,
                transformed_at=run_started_at,
            )

    if settings.send_mode == "send":
        # If the cap deferred work, advance to the first deferred second rather
        # than the scan upper bound.  The soft cap guarantees processed and
        # deferred windows are in disjoint ``aggregated_at`` seconds, so the
        # deferred second is rediscovered cleanly on the next run.
        computed_cursor = (
            discovered_deferred[0].aggregated_at
            if discovered_deferred
            else aggregated_at_upper
        )
        if computed_cursor is None:
            computed_cursor = aggregated_at_upper
        new_cursor = max(cursor, computed_cursor)
        state_store.set_revision_cursor(new_cursor)
        logger.debug(
            "advanced direction revision cursor",
            extra={
                "event": "revision_cursor_advanced",
                "old_cursor": cursor,
                "new_cursor": new_cursor,
            },
        )

    _lifecycle_logger.info(
        "direction revision sweep summary",
        extra={
            "event": "revision_sweep_summary",
            "product": "direction",
            "windows_discovered": len(discovered),
            "windows_retried": len(retry_items),
            "windows_deferred": len(discovered_deferred),
        },
    )
    return {item.window_key for item in work_items}


def _select_revision_direction_rows(
    db_connection: _DbConnection,
    work_items: Iterable[RevisionWorkItem],
) -> dict[tuple[int, str], list[_DirectionRow]]:
    """Re-fetch the complete row set for each work item's source window.

    For every work item this queries all current rows of its source window by
    exact ``startdate`` and groups them by ``(interval_min, startdate)``.  This
    is necessary because discovery returns only the changed
    ``(interval_min, startdate)`` key instead of which rows within it changed,
    while a target's ``peopleCount_flow_<N>`` can draw on several of the
    window's rows.  The sweep thus re-fetches the whole window and rebuilds
    each target's payload from the complete row set.

    Returns:
        Mapping from ``(interval_min, startdate)`` to that window's complete
        direction rows, for example ``(60, "20260629_1200")``.
    """
    startdates_by_interval: dict[int, list[str]] = {}
    for item in work_items:
        startdates_by_interval.setdefault(item.interval_min, []).append(item.startdate)

    rows_by_key: dict[tuple[int, str], list[_DirectionRow]] = {}
    for interval_min, startdates in startdates_by_interval.items():
        rows = db.select_direction_metrics_for_startdates(
            db_connection,
            interval_min=interval_min,
            startdates=startdates,
        )
        grouped = dict(_group_rows_by_startdate(rows))
        for startdate in startdates:
            rows_by_key[(interval_min, startdate)] = grouped.get(startdate, [])
    return rows_by_key


def _process_dry_run_window(
    *,
    rows_for_window: list[_DirectionRow],
    orion: _OrionForDirection,
    settings: RunDirectionSettings,
    filter_settings: FilterSettings,
    interval_metadata: Mapping[tuple[int, int], SensorPlace],
    counts: _RunCounts,
    window_key: str,
    transformed_at: datetime,
) -> None:
    """Preview one clean or degraded payload without mutating delivery state.

    A degraded preview increments ``windows_degraded`` and emits its warning,
    but dry-run remains successful and does not compute a delivery status.
    """
    transformed = _transform(
        rows_for_window,
        filter_settings,
        interval_metadata,
        settings=settings,
        transformed_at=transformed_at,
    )
    counts.rows_dropped += transformed.rows_dropped
    if _record_no_write_outcome(window_key, transformed, counts):
        return
    if not isinstance(transformed, DirectionPayloadOutcome):
        raise TypeError(f"unsupported direction transform outcome: {type(transformed)}")

    if transformed.excluded_place_numbers:
        counts.windows_degraded += 1
        _log_degraded_window(
            window_key,
            transformed,
            event="direction_window_degraded",
            attempted=True,
        )
    payload = transformed.payload
    result = orion.replace_attrs(
        str(payload["entity_id"]),
        payload["entity_type"],
        payload["attrs"],
        dry_run=True,
    )
    if bool(result.get("ok")):
        counts.puts_ok += 1
    else:
        counts.puts_failed += 1


def _log_degraded_window(
    window_key: str,
    transformed: DirectionPayloadOutcome,
    *,
    event: str,
    attempted: bool,
) -> None:
    """Log one degraded payload decision with its sorted quality lists."""
    extra = {
        "event": event,
        "window": window_key,
        "excluded_place_numbers": list(transformed.excluded_place_numbers),
        "missing_from_all_place_numbers": list(
            transformed.missing_from_all_place_numbers
        ),
        "missing_to_all_place_numbers": list(transformed.missing_to_all_place_numbers),
    }
    if attempted:
        logger.warning("direction window payload is degraded", extra=extra)
    else:
        logger.debug("unchanged degraded direction payload skipped", extra=extra)


def _record_no_write_outcome(
    window_key: str,
    transformed: DirectionTransformOutcome,
    counts: _RunCounts,
) -> bool:
    """Count and log a typed transform outcome that must not reach Orion."""
    if isinstance(transformed, DirectionNoPayloadOutcome):
        counts.windows_no_payload += 1
        logger.debug(
            "direction window has no payload",
            extra={"event": "direction_window_no_payload", "window": window_key},
        )
        return True
    if isinstance(transformed, DirectionSourceInvalidOutcome):
        counts.windows_source_invalid += 1
        logger.warning(
            "direction window is missing required source totals",
            extra={
                "event": "direction_window_source_invalid",
                "window": window_key,
                "missing_from_all_place_numbers": list(
                    transformed.missing_from_all_place_numbers
                ),
                "missing_to_all_place_numbers": list(
                    transformed.missing_to_all_place_numbers
                ),
            },
        )
        return True
    return False


def _transform(
    rows_for_window: list[_DirectionRow],
    filter_settings: FilterSettings,
    interval_metadata: Mapping[tuple[int, int], SensorPlace],
    *,
    settings: RunDirectionSettings,
    transformed_at: datetime,
) -> DirectionTransformOutcome:
    return transform_direction_window(
        rows_for_window,
        interval_metadata,
        aggregate_entity_id=settings.product_b_aggregate_entity_id,
        aggregate_entity_type=settings.product_b_aggregate_entity_type,
        ignored_place_prefixes=filter_settings.ignored_place_prefixes,
        now=lambda: transformed_at,
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
            reprocess_hours=settings.reprocess_hours_per3600,
            max_lookback_hours=settings.max_lookback_hours_per3600,
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
            "puts_ok": result.puts_ok,
            "puts_failed": result.puts_failed,
            "windows_degraded": result.windows_degraded,
            "windows_no_payload": result.windows_no_payload,
            "windows_source_invalid": result.windows_source_invalid,
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

    ``dateRetrieved`` and ``sourceQuality.value.evaluatedAt`` are excluded
    because they change every run. Quality status and exclusion lists remain
    semantic and therefore stay in the hash.
    """
    hashable = deepcopy(
        {key: value for key, value in attrs.items() if key != "dateRetrieved"}
    )
    source_quality = hashable.get("sourceQuality")
    if isinstance(source_quality, dict):
        value = source_quality.get("value")
        if isinstance(value, dict):
            value.pop("evaluatedAt", None)
    body = json.dumps(hashable, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(body).hexdigest()


if __name__ == "__main__":
    raise SystemExit(main())
