"""Run the flow-metric publishing pipeline."""

import fcntl
import hashlib
import json
import logging
import os
import sys
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
from sendai_pipeline.transform_flow import transform_flow_rows

logger = logging.getLogger(__name__)
# Under ``python -m sendai_pipeline.run_flow``, ``__name__`` is ``"__main__"``.
# Lifecycle records use the configured package logger so they reliably reach
# the rotating file handler and keep a stable JSON ``logger`` field.
_lifecycle_logger = logging.getLogger("sendai_pipeline")

JST = timezone(timedelta(hours=9))

_INTERVALS: tuple[int, ...] = (5, 60)
_INTERVAL_PREFIXES: dict[int, str] = {5: "per300", 60: "per3600"}
_VALID_SEND_MODES: frozenset[str] = frozenset({"dry-run", "send"})
_DEFAULT_METADATA_PATH = Path("metadata/sensors.csv")
_DEFAULT_SOURCE_STABILITY_DELAY_HOURS = 3
_DEFAULT_REVISION_SWEEP_MAX_WINDOWS = 2000

# Revision cursor state is stored as a JST ISO datetime.  Discovery crosses the
# MySQL boundary as second-resolution wall-clock strings in the JST session.
REVISION_CURSOR_SEED = datetime(2026, 6, 23, 0, 0, 0, tzinfo=JST)
# Keep each discovery scan small enough for the MySQL read timeout; the
# max-window setting controls POST volume separately from this time span.
REVISION_SWEEP_DISCOVERY_SPAN = timedelta(hours=6)

_FlowRow = Mapping[str, Any]
_PostResult = Mapping[str, Any]

__all__ = [
    "FilterConfigError",
    "JST",
    "RunFlowConfigError",
    "RunFlowResult",
    "RunFlowSettings",
    "main",
    "run_flow",
]


class RunFlowConfigError(RuntimeError):
    """Raised when flow-run configuration is invalid."""


class _DbConnection(Protocol):
    def cursor(self) -> Any:
        """Return a DB-API cursor context manager."""
        ...


class _OrionForFlow(Protocol):
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
class RunFlowSettings:
    """Configuration for one flow publishing run.

    Attributes:
        send_mode: ``"dry-run"`` to avoid live POSTs, or ``"send"`` to write
            attributes to Orion.
        reprocess_hours_per3600: Minimum 60-minute lookback.
        reprocess_hours_per300: Minimum 5-minute lookback.
        max_lookback_hours_per3600: Maximum 60-minute lookback.
        max_lookback_hours_per300: Maximum 5-minute lookback.
        source_stability_delay_hours: Hours to wait after source windows end
            before they become eligible for publication.
        revision_sweep_enabled: Whether to scan old windows by revision time.
        revision_sweep_max_windows: Maximum revision-sweep windows per run.
        state_path: JSON state file path.
        lock_path: Process lock file path.
    """

    send_mode: str = "dry-run"
    reprocess_hours_per3600: int = 12
    reprocess_hours_per300: int = 2
    max_lookback_hours_per3600: int = 72
    max_lookback_hours_per300: int = 72
    source_stability_delay_hours: int = _DEFAULT_SOURCE_STABILITY_DELAY_HOURS
    revision_sweep_enabled: bool = True
    revision_sweep_max_windows: int = _DEFAULT_REVISION_SWEEP_MAX_WINDOWS
    state_path: Path = Path("state/flow.json")
    lock_path: Path = Path("state/flow.lock")

    def __post_init__(self) -> None:
        if self.send_mode not in _VALID_SEND_MODES:
            raise RunFlowConfigError(
                f"invalid FLOW_SEND_MODE {self.send_mode!r}; expected dry-run or send"
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
        if self.revision_sweep_max_windows < 1:
            raise RunFlowConfigError("REVISION_SWEEP_MAX_WINDOWS must be positive")

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "RunFlowSettings":
        """Build settings from environment variables.

        Args:
            env: Optional mapping used in place of ``os.environ`` for tests.

        Returns:
            Parsed flow-run settings.

        Raises:
            RunFlowConfigError: If a setting is malformed or unsupported.
        """
        source = os.environ if env is None else env
        return cls(
            send_mode=_optional_env(source, "FLOW_SEND_MODE", "dry-run")
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
            revision_sweep_enabled=auth._parse_bool(
                _optional_env(source, "REVISION_SWEEP_ENABLED", "true")
            ),
            revision_sweep_max_windows=_int_env(
                source,
                "REVISION_SWEEP_MAX_WINDOWS",
                _DEFAULT_REVISION_SWEEP_MAX_WINDOWS,
            ),
            state_path=Path("state/flow.json"),
            lock_path=Path("state/flow.lock"),
        )


@dataclass
class RunFlowResult:
    """Outcome summary for one flow publishing run.

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


def run_flow(
    *,
    db_connection: _DbConnection,
    orion: _OrionForFlow,
    metadata: list[SensorPlace],
    state_store: WindowStateStore,
    settings: RunFlowSettings,
    filter_settings: FilterSettings,
    now: Callable[[], datetime],
) -> RunFlowResult:
    """Publish flow metrics for the eligible reprocessing windows.

    A *reprocessing window* is a source aggregation window whose
    ``startdate`` falls within the lookback range that ends at the
    stability cutoff.  A window is *eligible* when its source data is
    old enough to have settled (controlled by
    ``source_stability_delay_hours``) and young enough to still be
    within the maximum lookback horizon.

    Args:
        db_connection: Database connection used by ``sendai_pipeline.db``.
        orion: Orion client or test double exposing ``update_attrs``.
        metadata: Runtime sensor metadata.
        state_store: Per-window delivery state store.
        settings: Flow-run settings.
        filter_settings: Batch and source-row filter settings.
        now: Clock returning the run timestamp. Called once.

    Returns:
        Run summary including ``exit_code``: ``1`` in send mode when any
        partial windows, failed POSTs, or open windows remain after the
        run; ``0`` otherwise (including dry-run).
    """
    run_started_at = _coerce_jst_datetime(now())
    target_batches = sorted(filter_settings.target_flow_batches)

    if target_batches:
        filter_settings.validate_target_flow_batches(
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
        source_max_imputation_tier=filter_settings.source_max_imputation_tier,
    )

    if not target_batches:
        result = RunFlowResult(
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
        # Use 2× the maximum lookback as the GC horizon so that windows
        # processed in a previous run at full lookback depth still have
        # time to appear in the supplemental-complete pass before being
        # removed.  A 1× cutoff would evict windows the moment they age
        # past the retry range, leaving no safety margin.
        cutoff_gc = run_started_at - timedelta(
            hours=2
            * max(
                settings.max_lookback_hours_per3600,
                settings.max_lookback_hours_per300,
            )
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
        and (counts.windows_partial > 0 or counts.posts_failed > 0 or has_open_windows)
        else 0
    )
    result = RunFlowResult(
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
    """Run the flow entry point and return a process exit code."""
    del argv
    load_dotenv()
    settings = RunFlowSettings.from_env()

    settings.lock_path.parent.mkdir(parents=True, exist_ok=True)
    with settings.lock_path.open("a", encoding="utf-8") as lock_file:
        try:
            fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print(
                "[sendai-pipeline] flow run skipped: lock held by another process",
                file=sys.stderr,
            )
            return 0

        logging_settings = LoggingSettings.from_env()
        configure_logging(logging_settings, product="flow")

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
            result = run_flow(
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


@dataclass(frozen=True)
class _RevisionWorkItem:
    """One revision-sweep unit of work.

    Attributes:
        interval_min: Aggregation interval for the source window.
        startdate: Source ``startdate`` string, for example
            ``"20260629_1200"``.
        window_key: State key for the source window, for example
            ``"per300/20260629_1200"``.
        aggregated_at: Cursor key for discovered windows.  Retry items come
            from state, not discovery rows, so this is ``None`` for retries.
        retry: ``False`` for windows discovered by ``aggregated_at`` in this
            run; ``True`` for old open windows retried from state.
    """

    interval_min: int
    startdate: str
    window_key: str
    aggregated_at: datetime | None = None
    retry: bool = False


def _validate_orion_targets(
    active_targets: Iterable[SensorPlace],
    orion: _OrionForFlow,
) -> None:
    """Compare configured metadata targets with the live Orion entity set.

    Returns ``None`` — this function only logs; it never raises and never
    blocks the run.  The entity-map module logs missing, extra, and
    potentially truncated target sets.  Missing targets do not stop the
    run because the POST result for each entity is the authoritative
    delivery outcome.
    """
    entity_map.validate_targets(
        active_targets,
        cast(orion_client.OrionClient, orion),
    )


def _process_interval(
    interval_min: int,
    *,
    db_connection: _DbConnection,
    orion: _OrionForFlow,
    state_store: WindowStateStore,
    settings: RunFlowSettings,
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
    normal_lower_bound = cutoff - timedelta(hours=lookback_hours)
    rows = db.select_flow_metrics(
        db_connection,
        interval_min=interval_min,
        lower_bound=_format_sql_window_bound(normal_lower_bound),
        upper_bound=_format_sql_window_bound(cutoff),
        max_imputation_tier=filter_settings.source_max_imputation_tier,
    )
    interval_metadata = _metadata_index_for_interval(metadata_index, interval_min)

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
                expected_target_ids=(),
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
        _process_supplemental_complete_windows(
            interval_min,
            db_connection=db_connection,
            orion=orion,
            state_store=state_store,
            settings=settings,
            filter_settings=filter_settings,
            interval_metadata=interval_metadata,
            cutoff=cutoff,
            normal_lower_bound=normal_lower_bound,
            counts=counts,
        )
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
    rows_for_window: list[_FlowRow],
    orion: _OrionForFlow,
    state_store: WindowStateStore,
    filter_settings: FilterSettings,
    interval_metadata: Mapping[tuple[int, int], SensorPlace],
    expected_target_ids: Iterable[str],
    counts: _RunCounts,
    force_resend: bool = False,
    skip_if_no_new_target: bool = False,
    persist_each_target: bool = True,
) -> bool:
    """Post one source window and update persistent per-target state.

    Flow windows derive their expected target set from targets observed in
    transformed source rows unioned with any stored snapshot for the same
    window. The ``expected_target_ids`` argument is accepted to keep this
    helper's call surface aligned with Product B, but Product A does not use it
    to shrink or expand state; callers filter ``interval_metadata`` to control
    which payloads are built.
    """
    source_start = _parse_source_window_start(startdate)
    payloads = transform_flow_rows(
        rows_for_window,
        interval_metadata,
        ignored_place_prefixes=filter_settings.ignored_place_prefixes,
    )
    observed_target_ids = _payload_target_ids(payloads)
    if skip_if_no_new_target:
        stored_target_ids = set(state_store.expected_target_ids(window_key) or ())
        if set(observed_target_ids) <= stored_target_ids:
            return False

    counts.rows_dropped += len(rows_for_window) - len(payloads)
    effective_expected_target_ids = _effective_expected_target_ids(
        state_store,
        window_key,
        observed_target_ids=observed_target_ids,
    )
    if not effective_expected_target_ids:
        return False

    state_store.begin_window_attempt(
        window_key,
        interval_min=interval_min,
        source_window_start=source_start,
        source_window_end=source_start + timedelta(minutes=interval_min),
        expected_target_ids=effective_expected_target_ids,
    )

    for payload in payloads:
        payload_sha256 = _attrs_sha256(payload["attrs"])
        entity_id = payload["entity_id"]
        prior = state_store.target_record(window_key, entity_id)
        # Prior-ok unchanged payloads are true no-ops.  A prior-ok hash drift
        # means the source was revised after delivery, so repost it to publish
        # the revised value to Orion/STH-Comet.  This accepts duplicate
        # STH-Comet history rows as described in pipeline_spec.md section 2.9.
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
                continue
            logger.debug(
                "target payload drift resent",
                extra={
                    "event": "post_resent_drift",
                    "entity_id": entity_id,
                    "window": window_key,
                    "prior_payload_sha256": prior_payload_sha256,
                    "computed_payload_sha256": payload_sha256,
                },
            )

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
        if persist_each_target:
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

    return True


def _process_supplemental_complete_windows(
    interval_min: int,
    *,
    db_connection: _DbConnection,
    orion: _OrionForFlow,
    state_store: WindowStateStore,
    settings: RunFlowSettings,
    filter_settings: FilterSettings,
    interval_metadata: Mapping[tuple[int, int], SensorPlace],
    cutoff: datetime,
    normal_lower_bound: datetime,
    counts: _RunCounts,
) -> None:
    """Re-query retained complete windows older than the normal source range."""
    prefix = f"{_INTERVAL_PREFIXES[interval_min]}/"
    horizon = cutoff - timedelta(hours=_max_lookback_hours(settings, interval_min))
    eligible_windows: list[tuple[str, str]] = []

    for window_key, window in state_store.iter_complete_windows():
        if not window_key.startswith(prefix):
            continue
        source_window_start = state_store.source_window_start(window_key, window)
        if horizon <= source_window_start < normal_lower_bound:
            eligible_windows.append((window_key, window_key[len(prefix) :]))

    if not eligible_windows:
        return

    startdates = [startdate for _, startdate in eligible_windows]
    rows = db.select_flow_metrics_for_startdates(
        db_connection,
        interval_min=interval_min,
        startdates=startdates,
        max_imputation_tier=filter_settings.source_max_imputation_tier,
    )
    rows_by_startdate = dict(_group_rows_by_startdate(rows))

    for window_key, startdate in eligible_windows:
        attempted = _process_send_window(
            window_key,
            interval_min=interval_min,
            startdate=startdate,
            rows_for_window=rows_by_startdate.get(startdate, []),
            orion=orion,
            state_store=state_store,
            filter_settings=filter_settings,
            interval_metadata=interval_metadata,
            expected_target_ids=(),
            counts=counts,
            skip_if_no_new_target=True,
        )
        if attempted:
            counts.windows_seen += 1


def _process_revision_sweep(
    *,
    db_connection: _DbConnection,
    orion: _OrionForFlow,
    state_store: WindowStateStore,
    settings: RunFlowSettings,
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
    after the work list finishes.  Failed POSTs stay as ``partial`` window
    state and are retried from state; they do not hold the cursor back.

    Args:
        db_connection: Open MySQL connection for discovery and refetch queries.
        orion: Orion client facade used to post (or dry-run) payloads.
        state_store: Persistent window-state store; holds the cursor and the
            open windows the retry pass re-sends.
        settings: Runner settings, including send mode and the per-run cap.
        filter_settings: Runtime filters, including the imputation-tier gate.
        metadata_index: Active ``(place_number, interval_min)`` -> metadata map.
        lookback_hours_used: Per-interval lookback the fresh path applied this
            run.  Its lower bound is the sweep's ``startdate`` upper bound, so
            the two paths never process the same window.
        run_started_at: Run start; floored to seconds it bounds the discovery
            chunk's upper edge.
        counts: Mutable per-run counters updated by this sweep.

    Returns:
        State window keys processed by this sweep run.  The caller preserves
        these keys during same-run GC so a completed old window is not removed
        before the final state save.
    """
    # Resolve the JST cursor and the half-open discovery chunk.  The upper
    # bound is the earlier of the floored run start and one discovery span past
    # the cursor, so an initial backlog drains in bounded MySQL scans.
    cursor = (state_store.revision_cursor() or REVISION_CURSOR_SEED).replace(
        microsecond=0
    )
    cursor = _coerce_jst_datetime(cursor)
    run_upper = run_started_at.replace(microsecond=0)
    span_upper = cursor + REVISION_SWEEP_DISCOVERY_SPAN
    aggregated_at_upper = min(run_upper, span_upper)
    chunk_binds = span_upper < run_upper
    aggregated_at_lower_sql = _format_mysql_timestamp(cursor)
    aggregated_at_upper_sql = _format_mysql_timestamp(aggregated_at_upper)
    interval_metadata_by_interval = {
        interval_min: _metadata_index_for_interval(metadata_index, interval_min)
        for interval_min in _INTERVALS
    }
    startdate_upper_by_interval: dict[int, datetime] = {}
    discovered: list[_RevisionWorkItem] = []

    _lifecycle_logger.info(
        "flow revision sweep started",
        extra={
            "event": "revision_sweep_started",
            "product": "flow",
            "old_cursor": cursor,
            "new_cursor": aggregated_at_upper,
        },
    )

    # Discover candidate windows per interval.  ``startdate_upper`` is the
    # fresh path's lower bound; the sweep handles only older windows that the
    # normal rolling lookback will not publish in this run.
    for interval_min in _INTERVALS:
        cutoff = _eligible_source_cutoff(
            run_started_at,
            interval_min,
            source_stability_delay_hours=settings.source_stability_delay_hours,
        )
        startdate_upper = cutoff - timedelta(hours=lookback_hours_used[interval_min])
        startdate_upper_by_interval[interval_min] = startdate_upper
        rows = db.discover_flow_revised_windows(
            db_connection,
            interval_min=interval_min,
            aggregated_at_lower=aggregated_at_lower_sql,
            aggregated_at_upper=aggregated_at_upper_sql,
            startdate_upper=_format_sql_window_bound(startdate_upper),
            max_imputation_tier=filter_settings.source_max_imputation_tier,
        )
        for row in rows:
            startdate = str(row["startdate"])
            discovered.append(
                _RevisionWorkItem(
                    interval_min=interval_min,
                    startdate=startdate,
                    window_key=_window_key(interval_min, startdate),
                    aggregated_at=_parse_revision_aggregated_at(row["win_agg"]),
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
    discovered_to_process, discovered_deferred = _split_discovered_revisions(
        discovered,
        settings.revision_sweep_max_windows,
    )
    discovered_keys = {item.window_key for item in discovered}
    retry_items: list[_RevisionWorkItem] = []
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
        retry_items = _revision_retry_items(
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
    rows_by_key = _select_revision_flow_rows(
        db_connection,
        work_items,
        max_imputation_tier=filter_settings.source_max_imputation_tier,
    )

    # Do not catch unexpected per-window errors here.  If transformation or
    # posting raises before normal state records are written, abort before the
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
                filter_settings=filter_settings,
                interval_metadata=interval_metadata_by_interval[item.interval_min],
                expected_target_ids=(),
                counts=counts,
            )
        else:
            _process_dry_run_window(
                rows_for_window=rows_for_window,
                orion=orion,
                filter_settings=filter_settings,
                interval_metadata=interval_metadata_by_interval[item.interval_min],
                counts=counts,
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
            "advanced flow revision cursor",
            extra={
                "event": "revision_cursor_advanced",
                "old_cursor": cursor,
                "new_cursor": new_cursor,
            },
        )

    _lifecycle_logger.info(
        "flow revision sweep summary",
        extra={
            "event": "revision_sweep_summary",
            "product": "flow",
            "windows_discovered": len(discovered),
            "windows_retried": len(retry_items),
            "windows_deferred": len(discovered_deferred),
        },
    )
    return {item.window_key for item in work_items}


def _split_discovered_revisions(
    discovered: list[_RevisionWorkItem],
    max_windows: int,
) -> tuple[list[_RevisionWorkItem], list[_RevisionWorkItem]]:
    """Split cursor-ordered discoveries into a capped batch and a deferred rest.

    Args:
        discovered: Discovered work items, sorted by the cursor key
            (``aggregated_at`` then ``startdate``).
        max_windows: Soft per-run cap on how many windows to process.

    Returns:
        A ``(to_process, deferred)`` pair.  ``to_process`` is the first
        ``max_windows`` items, extended forward to include every item that
        shares the last one's ``aggregated_at`` second; ``deferred`` is the
        remainder, left for a later run.  Keeping a whole second together lets
        the next cursor point at the first deferred second and rediscover those
        windows cleanly, instead of splitting one second across two runs.
    """
    if len(discovered) <= max_windows:
        return discovered, []

    boundary = discovered[max_windows - 1].aggregated_at
    split_at = max_windows
    while split_at < len(discovered) and discovered[split_at].aggregated_at == boundary:
        split_at += 1
    return discovered[:split_at], discovered[split_at:]


def _revision_retry_items(
    state_store: WindowStateStore,
    *,
    startdate_upper_by_interval: Mapping[int, datetime],
    excluded_window_keys: set[str],
    limit: int,
) -> list[_RevisionWorkItem]:
    """Return old open windows for the sweep's retry pass to re-send.

    These are ``pending`` or ``partial`` windows whose source start is older
    than the fresh path's lower bound: too old for the rolling lookback to
    reach, yet still owed a retry.  Windows that discovery surfaced this run
    (processed or cap-deferred) are skipped, so discovery and retry never act
    on the same window in one run.

    Args:
        state_store: Persistent window-state store to scan for open windows.
        startdate_upper_by_interval: Per-interval fresh-path lower bound; only
            windows whose source start is strictly older are eligible.
        excluded_window_keys: Window keys discovered this run (processed or
            cap-deferred), which this pass skips.
        limit: Maximum number of windows to return (the sweep's remaining
            per-run capacity).

    Returns:
        Retry work items, each flagged ``retry=True``.
    """
    if limit <= 0:
        return []

    items: list[_RevisionWorkItem] = []
    for window_key, window in state_store.iter_open_windows():
        if window_key in excluded_window_keys:
            continue
        interval_min = _interval_from_window_key(window_key)
        if interval_min is None:
            continue
        if (
            state_store.source_window_start(window_key, window)
            >= (startdate_upper_by_interval[interval_min])
        ):
            continue
        items.append(
            _RevisionWorkItem(
                interval_min=interval_min,
                startdate=_startdate_from_window_key(window_key),
                window_key=window_key,
                retry=True,
            )
        )
        if len(items) >= limit:
            break
    return items


def _select_revision_flow_rows(
    db_connection: _DbConnection,
    work_items: Iterable[_RevisionWorkItem],
    *,
    max_imputation_tier: int,
) -> dict[tuple[int, str], list[_FlowRow]]:
    """Re-fetch the complete row set for each work item's source window.

    For every work item this queries all current rows of its source window by
    exact ``startdate`` and groups them by ``(interval_min, startdate)``.  This
    is necessary because discovery returns only the changed
    ``(interval_min, startdate)`` key instead of which rows within it changed,
    while a flow window holds one row per place, each producing that place's
    payload.  The sweep thus re-fetches the whole window and rebuilds every
    per-place payload; idempotency then skips any place whose payload is
    unchanged.

    Returns:
        Mapping from ``(interval_min, startdate)`` to that window's complete
        flow rows, for example ``(5, "20260629_1200")``.
    """
    startdates_by_interval: dict[int, list[str]] = {}
    for item in work_items:
        startdates_by_interval.setdefault(item.interval_min, []).append(item.startdate)

    rows_by_key: dict[tuple[int, str], list[_FlowRow]] = {}
    for interval_min, startdates in startdates_by_interval.items():
        rows = db.select_flow_metrics_for_startdates(
            db_connection,
            interval_min=interval_min,
            startdates=startdates,
            max_imputation_tier=max_imputation_tier,
        )
        grouped = dict(_group_rows_by_startdate(rows))
        for startdate in startdates:
            rows_by_key[(interval_min, startdate)] = grouped.get(startdate, [])
    return rows_by_key


def _effective_expected_target_ids(
    state_store: WindowStateStore,
    window_key: str,
    *,
    observed_target_ids: Iterable[str],
) -> list[str]:
    """Return the target set this attempt must respect for one window.

    The result is the union of any previously stored expected targets and the
    targets observed in the current transformation pass.  Stored targets take
    precedence in the sense that they are never removed — once a target is
    expected for a window it remains expected for all future retries.  The
    union ensures that a target that was present in an earlier attempt but
    absent from the current source rows (e.g. due to a partial DB result) is
    still tracked as expected, rather than silently dropped.

    Returns:
        Sorted list of unique entity ID strings, e.g.
        ``["jp.sendai.Blesensor.per300.10", "jp.sendai.Blesensor.per300.11"]``.
        An empty list signals that no targets are known for this window; callers
        should treat this as a no-op and skip the window.
    """
    stored = state_store.expected_target_ids(window_key)
    return sorted(set(stored or ()).union(observed_target_ids))


def _payload_target_ids(payloads: Iterable[Mapping[str, Any]]) -> list[str]:
    """Return unique target entity IDs observed in transformed payloads.

    Returns:
        Sorted list of unique ``entity_id`` strings extracted from the
        payload dicts, e.g.
        ``["jp.sendai.Blesensor.per300.10", "jp.sendai.Blesensor.per300.11"]``.
    """
    return sorted({str(payload["entity_id"]) for payload in payloads})


def _process_dry_run_window(
    *,
    rows_for_window: list[_FlowRow],
    orion: _OrionForFlow,
    filter_settings: FilterSettings,
    interval_metadata: Mapping[tuple[int, int], SensorPlace],
    counts: _RunCounts,
) -> None:
    """Build and log payloads for one source window without sending live POSTs.

    Calls ``orion.update_attrs(..., dry_run=True)`` so the request body is
    logged but never reaches the network, and never touches the state store.
    """
    payloads = transform_flow_rows(
        rows_for_window,
        interval_metadata,
        ignored_place_prefixes=filter_settings.ignored_place_prefixes,
    )
    counts.rows_dropped += len(rows_for_window) - len(payloads)
    for payload in payloads:
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


def _lookback_hours_by_interval(
    state_store: WindowStateStore,
    *,
    settings: RunFlowSettings,
    run_started_at: datetime,
) -> dict[int, float]:
    """Return the lookback hours used by each interval this run.

    The value is what the SQL ``lower_bound`` will subtract from each
    interval's cutoff.
    """
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
    settings: RunFlowSettings,
    orion: _OrionForFlow,
    target_batches: list[str],
    lookback_hours_used: dict[int, float],
    source_max_imputation_tier: int,
) -> None:
    """Emit the structured lifecycle record at the start of a run."""
    _lifecycle_logger.info(
        "flow run started",
        extra={
            "event": "run_started",
            "product": "flow",
            "send_mode": settings.send_mode,
            "target_batches": target_batches,
            "payload_mode": getattr(orion, "payload_mode", "failure"),
            "lookback_hours_used": lookback_hours_used,
            "source_max_imputation_tier": source_max_imputation_tier,
        },
    )


def _log_run_summary(result: RunFlowResult) -> None:
    """Emit the structured lifecycle record at the end of a run."""
    _lifecycle_logger.info(
        "flow run summary",
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
    rows: Iterable[_FlowRow],
) -> Iterable[tuple[str, list[_FlowRow]]]:
    """Group DB rows by source window while preserving first-seen order.

    Yields:
        ``(startdate, rows)`` tuples in the order each ``startdate`` was
        first encountered in *rows*, e.g.
        ``("20240601_1000", [row1, row2])``.
    """
    grouped: dict[str, list[_FlowRow]] = {}
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
    """Return the canonical SHA-256 hash of a serialized NGSI attribute dict.

    Uses ``sort_keys=True`` + ``separators=(",", ":")`` so two equivalent
    attribute dicts always produce the same hash regardless of key insertion
    order.  Flow attributes do not contain a per-run wall-clock timestamp, so
    the entire ``attrs`` dict is hashed directly — unlike Product B, no keys
    need to be excluded.  If envelope fields that change every run (e.g. a
    ``dateRetrieved`` timestamp) are ever added to flow payloads, they must be
    excluded from ``attrs`` before hashing, or the skip-on-unchanged
    optimization would never fire.  See ``run_direction._attrs_sha256`` for the
    matching pattern in Product B, which does exclude ``dateRetrieved``.
    """
    body = json.dumps(attrs, sort_keys=True, separators=(",", ":")).encode("utf-8")
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
    """Return the newest source window eligible for publication.

    Recent aggregate windows can still receive late writes. The stability delay
    keeps those windows out of the publishing range until their source data has
    had time to settle.
    """
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


def _format_mysql_timestamp(value: datetime) -> str:
    """Format a cursor datetime for MySQL ``aggregated_at`` comparisons.

    State stores cursor values as JST ISO datetimes.  Discovery SQL compares
    against MySQL wall-clock strings in the JST session, floored to whole
    seconds: ``"YYYY-MM-DD HH:MM:SS"``.
    """
    return (
        _coerce_jst_datetime(value).replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
    )


def _parse_revision_aggregated_at(value: Any) -> datetime:
    """Parse a MySQL ``aggregated_at`` value into the cursor representation.

    The DB driver may return a ``datetime`` or a string.  The runner normalizes
    either form to a JST datetime floored to whole seconds so it can be stored
    back into the ISO cursor field without mixing precisions.
    """
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value)
        if "T" in text:
            parsed = datetime.fromisoformat(text)
        else:
            parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
    return _coerce_jst_datetime(parsed).replace(microsecond=0)


def _interval_from_window_key(window_key: str) -> int | None:
    """Return the aggregation interval encoded in a state window key."""
    prefix, separator, _startdate = window_key.partition("/")
    if not separator:
        return None
    for interval_min, expected_prefix in _INTERVAL_PREFIXES.items():
        if prefix == expected_prefix:
            return interval_min
    return None


def _startdate_from_window_key(window_key: str) -> str:
    """Return the source startdate encoded in a state window key."""
    return window_key.partition("/")[2]


def _reprocess_hours(settings: RunFlowSettings, interval_min: int) -> int:
    """Return the configured minimum reprocess span for one interval."""
    if interval_min == 5:
        return settings.reprocess_hours_per300
    return settings.reprocess_hours_per3600


def _max_lookback_hours(settings: RunFlowSettings, interval_min: int) -> int:
    """Return the configured maximum reprocess span for one interval."""
    if interval_min == 5:
        return settings.max_lookback_hours_per300
    return settings.max_lookback_hours_per3600


def _parse_state_datetime(value: str) -> datetime:
    """Parse an ISO timestamp from state and normalize it to JST."""
    return _coerce_jst_datetime(datetime.fromisoformat(value))


def _coerce_jst_datetime(value: datetime) -> datetime:
    """Return ``value`` as a JST-aware datetime.

    Attaches JST when ``value`` is naive; converts when ``value`` is
    already timezone-aware.
    """
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
    orion: _OrionForFlow,
    settings: LoggingSettings,
) -> None:
    """Copy the payload-logging fields from ``LoggingSettings`` onto ``orion``.

    Keeps ``payload_mode`` / ``payload_max_bytes`` / ``response_max_bytes``
    in sync between the entry-point logging config and the Orion client that
    formats per-POST log records.
    """
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
        raise RunFlowConfigError(
            f"environment variable must be an integer: {key}"
        ) from exc


def _validate_non_negative_hours(values: Mapping[str, int]) -> None:
    """Reject negative hour settings."""
    for key, value in values.items():
        if value < 0:
            raise RunFlowConfigError(
                f"environment variable must be non-negative: {key}"
            )


def _validate_lookback_ceiling(name: str, reprocess_hours: int, max_hours: int) -> None:
    """Reject a max lookback that is lower than its reprocess floor."""
    if max_hours < reprocess_hours:
        raise RunFlowConfigError(
            f"MAX_LOOKBACK_HOURS_{name} must be >= REPROCESS_HOURS_{name}"
        )


if __name__ == "__main__":
    raise SystemExit(main())
