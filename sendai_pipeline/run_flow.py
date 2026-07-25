"""Run the flow-metric publishing pipeline."""

import fcntl
import hashlib
import json
import logging
import os
import sys
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
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
    metadata_path_from_env,
)
from sendai_pipeline.revision_sweep import (
    RevisionWorkItem,
    revision_retry_items,
    split_discovered_revisions,
)
from sendai_pipeline.settings_validation import (
    optional_env,
    parse_int_env,
    validate_lookback_ceiling,
    validate_non_negative_settings,
)
from sendai_pipeline.state import WindowStateStore
from sendai_pipeline.transform_flow import transform_flow_rows
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
# Under ``python -m sendai_pipeline.run_flow``, ``__name__`` is ``"__main__"``.
# Lifecycle records use the configured package logger so they reliably reach
# the rotating file handler and keep a stable JSON ``logger`` field.
_lifecycle_logger = logging.getLogger("sendai_pipeline")

_INTERVALS: tuple[int, ...] = (5, 60)
_INTERVAL_PREFIXES: dict[int, str] = {5: "per300", 60: "per3600"}
_VALID_SEND_MODES: frozenset[str] = frozenset({"dry-run", "send"})
_DEFAULT_SOURCE_STABILITY_DELAY_HOURS = 3
_DEFAULT_REVISION_SWEEP_MAX_WINDOWS = 2000

# The cursor persists as a JST ISO datetime.  When a discovery query reaches
# MySQL, that same instant is passed as a second-resolution wall-clock string.
REVISION_CURSOR_SEED = datetime(2026, 6, 23, 0, 0, 0, tzinfo=JST)
# Keep each discovery scan small enough for the MySQL read timeout; the
# max-window setting controls POST volume separately from this time span.
REVISION_SWEEP_DISCOVERY_SPAN = timedelta(hours=6)

_FlowRow = Mapping[str, Any]
_PostResult = Mapping[str, Any]

__all__ = [
    "FilterConfigError",
    "FlowWindowPublishResult",
    "JST",
    "RunFlowConfigError",
    "RunFlowResult",
    "RunFlowSettings",
    "main",
    "publish_flow_window",
    "replay_flow_window",
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
        source_stability_delay_hours: Minimum age of a source window's start
            time before it becomes eligible for publication.
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
        validate_non_negative_settings(
            {
                "REPROCESS_HOURS_PER3600": self.reprocess_hours_per3600,
                "REPROCESS_HOURS_PER300": self.reprocess_hours_per300,
                "MAX_LOOKBACK_HOURS_PER3600": self.max_lookback_hours_per3600,
                "MAX_LOOKBACK_HOURS_PER300": self.max_lookback_hours_per300,
                "SOURCE_STABILITY_DELAY_HOURS": self.source_stability_delay_hours,
            },
            RunFlowConfigError,
        )
        validate_lookback_ceiling(
            "PER3600",
            self.reprocess_hours_per3600,
            self.max_lookback_hours_per3600,
            RunFlowConfigError,
        )
        validate_lookback_ceiling(
            "PER300",
            self.reprocess_hours_per300,
            self.max_lookback_hours_per300,
            RunFlowConfigError,
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
            send_mode=optional_env(source, "FLOW_SEND_MODE", "dry-run").strip().lower(),
            reprocess_hours_per3600=parse_int_env(
                source,
                "REPROCESS_HOURS_PER3600",
                12,
                RunFlowConfigError,
            ),
            reprocess_hours_per300=parse_int_env(
                source,
                "REPROCESS_HOURS_PER300",
                2,
                RunFlowConfigError,
            ),
            max_lookback_hours_per3600=parse_int_env(
                source,
                "MAX_LOOKBACK_HOURS_PER3600",
                72,
                RunFlowConfigError,
            ),
            max_lookback_hours_per300=parse_int_env(
                source,
                "MAX_LOOKBACK_HOURS_PER300",
                72,
                RunFlowConfigError,
            ),
            source_stability_delay_hours=parse_int_env(
                source,
                "SOURCE_STABILITY_DELAY_HOURS",
                _DEFAULT_SOURCE_STABILITY_DELAY_HOURS,
                RunFlowConfigError,
            ),
            revision_sweep_enabled=auth._parse_bool(
                optional_env(source, "REVISION_SWEEP_ENABLED", "true")
            ),
            revision_sweep_max_windows=parse_int_env(
                source,
                "REVISION_SWEEP_MAX_WINDOWS",
                _DEFAULT_REVISION_SWEEP_MAX_WINDOWS,
                RunFlowConfigError,
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


@dataclass(frozen=True)
class FlowWindowPublishResult:
    """Publishing counters produced for one Product A source window.

    Run-level window selection is intentionally excluded; orchestration owns
    ``windows_seen``.

    Attributes:
        windows_complete: ``1`` if the window ended complete, otherwise ``0``.
        windows_partial: ``1`` if the window ended partial, otherwise ``0``.
        windows_dead_letter: ``1`` if the window ended dead-lettered,
            otherwise ``0``.
        posts_ok: Orion attribute updates that succeeded.
        posts_failed: Orion attribute updates that failed.
        rows_dropped: Source rows omitted during transformation.
    """

    windows_complete: int
    windows_partial: int
    windows_dead_letter: int
    posts_ok: int
    posts_failed: int
    rows_dropped: int


def publish_flow_window(
    *,
    interval_min: int,
    startdate: str,
    rows_for_window: list[_FlowRow],
    orion: _OrionForFlow,
    state_store: WindowStateStore,
    filter_settings: FilterSettings,
    interval_metadata: Mapping[tuple[int, int], SensorPlace],
    transformed_at: datetime,
) -> FlowWindowPublishResult:
    """Publish one Product A window under normal runner policy.

    State is saved after each attempted target and unchanged prior-success
    payloads are skipped.

    Args:
        interval_min: Source aggregation interval in minutes.
        startdate: Source window start in ``YYYYMMDD_HHMM`` format.
        rows_for_window: Complete Product A source rows for the window.
        orion: Orion writer for per-target attribute updates.
        state_store: Delivery state store to update and persist.
        filter_settings: Source-row filters used by the transform.
        interval_metadata: Active target metadata for this interval.
        transformed_at: Timestamp used for retrieval metadata.

    Returns:
        Immutable counters for this window, excluding ``windows_seen``.
    """
    return _publish_flow_window(
        interval_min=interval_min,
        startdate=startdate,
        rows_for_window=rows_for_window,
        orion=orion,
        state_store=state_store,
        filter_settings=filter_settings,
        interval_metadata=interval_metadata,
        transformed_at=transformed_at,
        force_resend=False,
        persist_each_target=True,
    )


def replay_flow_window(
    *,
    interval_min: int,
    startdate: str,
    rows_for_window: list[_FlowRow],
    orion: _OrionForFlow,
    state_store: WindowStateStore,
    filter_settings: FilterSettings,
    interval_metadata: Mapping[tuple[int, int], SensorPlace],
    transformed_at: datetime,
    force: bool,
) -> FlowWindowPublishResult:
    """Replay one Product A window under operator-requested policy.

    The caller owns persistence cadence. ``force`` bypasses the unchanged
    prior-success hash skip.

    Args:
        interval_min: Source aggregation interval in minutes.
        startdate: Source window start in ``YYYYMMDD_HHMM`` format.
        rows_for_window: Complete Product A source rows for the window.
        orion: Orion writer for per-target attribute updates.
        state_store: Delivery state store to update without saving in-loop.
        filter_settings: Source-row filters used by the transform.
        interval_metadata: Active target metadata selected for this replay.
        transformed_at: Timestamp used for retrieval metadata.
        force: Whether to rewrite unchanged prior-success payloads.

    Returns:
        Immutable counters for this window, excluding ``windows_seen``.
    """
    return _publish_flow_window(
        interval_min=interval_min,
        startdate=startdate,
        rows_for_window=rows_for_window,
        orion=orion,
        state_store=state_store,
        filter_settings=filter_settings,
        interval_metadata=interval_metadata,
        transformed_at=transformed_at,
        force_resend=force,
        persist_each_target=False,
    )


def _publish_flow_window(
    *,
    interval_min: int,
    startdate: str,
    rows_for_window: list[_FlowRow],
    orion: _OrionForFlow,
    state_store: WindowStateStore,
    filter_settings: FilterSettings,
    interval_metadata: Mapping[tuple[int, int], SensorPlace],
    transformed_at: datetime,
    force_resend: bool,
    persist_each_target: bool,
) -> FlowWindowPublishResult:
    counts = _RunCounts()
    _process_send_window(
        make_window_key(interval_min, startdate),
        interval_min=interval_min,
        startdate=startdate,
        rows_for_window=rows_for_window,
        orion=orion,
        state_store=state_store,
        filter_settings=filter_settings,
        interval_metadata=interval_metadata,
        counts=counts,
        transformed_at=transformed_at,
        force_resend=force_resend,
        persist_each_target=persist_each_target,
    )
    return FlowWindowPublishResult(
        windows_complete=counts.windows_complete,
        windows_partial=counts.windows_partial,
        windows_dead_letter=counts.windows_dead_letter,
        posts_ok=counts.posts_ok,
        posts_failed=counts.posts_failed,
        rows_dropped=counts.rows_dropped,
    )


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
    run_started_at = coerce_jst_datetime(now())
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
            payload_mode=logging_settings.payload_mode,
            payload_max_bytes=logging_settings.payload_max_bytes,
            response_max_bytes=logging_settings.response_max_bytes,
        )

        db_connection = db.connect(db.DbSettings.from_env())
        try:
            result = run_flow(
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
    posts_ok: int = 0
    posts_failed: int = 0
    rows_dropped: int = 0


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
    cutoff = eligible_source_cutoff(
        run_started_at,
        interval_min,
        source_stability_delay_hours=settings.source_stability_delay_hours,
    )
    normal_lower_bound = cutoff - timedelta(hours=lookback_hours)
    rows = db.select_flow_metrics(
        db_connection,
        interval_min=interval_min,
        lower_bound=format_sql_window_bound(normal_lower_bound),
        upper_bound=format_sql_window_bound(cutoff),
        max_imputation_tier=filter_settings.source_max_imputation_tier,
    )
    interval_metadata = _metadata_index_for_interval(metadata_index, interval_min)

    for startdate, rows_for_window in _group_rows_by_startdate(rows):
        counts.windows_seen += 1

        if settings.send_mode == "send":
            result = publish_flow_window(
                interval_min=interval_min,
                startdate=startdate,
                rows_for_window=rows_for_window,
                orion=orion,
                state_store=state_store,
                filter_settings=filter_settings,
                interval_metadata=interval_metadata,
                transformed_at=run_started_at,
            )
            counts.windows_complete += result.windows_complete
            counts.windows_partial += result.windows_partial
            counts.windows_dead_letter += result.windows_dead_letter
            counts.posts_ok += result.posts_ok
            counts.posts_failed += result.posts_failed
            counts.rows_dropped += result.rows_dropped
        else:
            _process_dry_run_window(
                rows_for_window=rows_for_window,
                orion=orion,
                filter_settings=filter_settings,
                interval_metadata=interval_metadata,
                counts=counts,
                transformed_at=run_started_at,
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
            transformed_at=run_started_at,
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
    counts: _RunCounts,
    transformed_at: datetime,
    force_resend: bool = False,
    skip_if_no_new_target: bool = False,
    persist_each_target: bool = True,
) -> bool:
    """Post one source window and update persistent per-target state.

    Flow windows derive their expected target set from targets observed in
    transformed source rows unioned with any stored snapshot for the same
    window. Callers filter ``interval_metadata`` to control which payloads are
    built.
    """
    source_start = parse_source_window_start(startdate)
    payloads = transform_flow_rows(
        rows_for_window,
        interval_metadata,
        transformed_at=transformed_at,
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
    transformed_at: datetime,
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
            counts=counts,
            transformed_at=transformed_at,
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
    # Resolve the JST cursor and the half-open discovery chunk.  The upper
    # bound is the earlier of the floored run start and one discovery span past
    # the cursor, so an initial backlog drains in bounded MySQL scans.
    cursor = (state_store.revision_cursor() or REVISION_CURSOR_SEED).replace(
        microsecond=0
    )
    cursor = coerce_jst_datetime(cursor)
    run_upper = run_started_at.replace(microsecond=0)
    span_upper = cursor + REVISION_SWEEP_DISCOVERY_SPAN
    aggregated_at_upper = min(run_upper, span_upper)
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
        cutoff = eligible_source_cutoff(
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
            startdate_upper=format_sql_window_bound(startdate_upper),
            max_imputation_tier=filter_settings.source_max_imputation_tier,
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
                counts=counts,
                transformed_at=run_started_at,
            )
        else:
            _process_dry_run_window(
                rows_for_window=rows_for_window,
                orion=orion,
                filter_settings=filter_settings,
                interval_metadata=interval_metadata_by_interval[item.interval_min],
                counts=counts,
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


def _select_revision_flow_rows(
    db_connection: _DbConnection,
    work_items: Iterable[RevisionWorkItem],
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
    transformed_at: datetime,
) -> None:
    """Build and log payloads for one source window without sending live POSTs.

    Calls ``orion.update_attrs(..., dry_run=True)`` so the request body is
    logged but never reaches the network, and never touches the state store.
    """
    payloads = transform_flow_rows(
        rows_for_window,
        interval_metadata,
        transformed_at=transformed_at,
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

    Each interval's SQL ``lower_bound`` equals its cutoff minus this many
    hours.
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
    """Return the Product A semantic SHA-256 hash.

    The canonical serialization excludes only the top-level
    ``dateRetrieved`` attribute because it is the per-run wall-clock value.
    All other Product A attributes remain part of the hash.  A shallow copy
    keeps the outgoing payload unchanged.
    """
    semantic_attrs = dict(attrs)
    semantic_attrs.pop("dateRetrieved", None)
    body = json.dumps(
        semantic_attrs,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(body).hexdigest()


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


if __name__ == "__main__":
    raise SystemExit(main())
