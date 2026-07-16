"""Replay explicit source windows through the product send path."""

import argparse
import fcntl
import logging
import os
import sys
import time
from collections.abc import Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, cast

from dotenv import find_dotenv, load_dotenv

from sendai_pipeline import auth, db, orion_client
from sendai_pipeline.filter_settings import FilterSettings
from sendai_pipeline.logging_setup import LoggingSettings, configure_logging
from sendai_pipeline.metadata import (
    SensorPlace,
    active_places,
    index_by_place_interval,
    load_metadata,
    parse_entity_id,
)
from sendai_pipeline.run_direction import (
    JST,
    RunDirectionSettings,
)
from sendai_pipeline.run_direction import (
    _format_sql_window_bound as direction_sql_bound,
)
from sendai_pipeline.run_direction import (
    _process_send_window as process_direction_window,
)
from sendai_pipeline.run_direction import (
    _RunCounts as DirectionCounts,
)
from sendai_pipeline.run_direction import (
    _window_key as direction_window_key,
)
from sendai_pipeline.run_flow import (
    _format_sql_window_bound as flow_sql_bound,
)
from sendai_pipeline.run_flow import (
    _metadata_path_from_env as metadata_path_from_env,
)
from sendai_pipeline.run_flow import (
    _process_send_window as process_flow_window,
)
from sendai_pipeline.run_flow import (
    _RunCounts as FlowCounts,
)
from sendai_pipeline.run_flow import (
    _window_key as flow_window_key,
)
from sendai_pipeline.state import WindowStateStore
from sendai_pipeline.state_tools import PRODUCT_LOCK_PATHS, PRODUCT_STATE_PATHS

logger = logging.getLogger("sendai_pipeline")

_MAX_LOOKBACK_ENV: dict[int, str] = {
    5: "MAX_LOOKBACK_HOURS_PER300",
    60: "MAX_LOOKBACK_HOURS_PER3600",
}
_RESEND_SAVE_EVERY = 100
_RESEND_DB_RECONNECT_ATTEMPTS = 2
_RESEND_DB_RECONNECT_BACKOFF_SECONDS = 1
_WINDOW_FORMAT = "%Y%m%d_%H%M"


class ResendConfigError(RuntimeError):
    """Raised when resend arguments or environment are invalid."""


@dataclass(frozen=True)
class ResendPlan:
    """Validated resend request."""

    product: str
    interval_min: int
    from_window: str
    to_window: str
    reason: str
    force: bool
    send: bool
    windows: tuple[str, ...]
    interval_metadata: dict[tuple[int, int], SensorPlace]
    source_max_imputation_tier: int | None = None
    direction_settings: RunDirectionSettings | None = None

    @property
    def expected_target_ids(self) -> list[str]:
        """Return the entity ids expected for each requested source window."""
        return [place.entity_id for place in self.interval_metadata.values()]

    @property
    def target_count(self) -> int:
        """Return the number of Orion targets written per source window."""
        return 1 if self.product == "direction" else len(self.expected_target_ids)


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Replay source windows through the product send path.",
    )
    parser.add_argument("product", choices=("flow", "direction"))
    parser.add_argument("--interval-min", type=int, choices=(5, 60))
    parser.add_argument("--from", dest="from_window", required=True)
    parser.add_argument("--to", dest="to_window", required=True)
    parser.add_argument("--place", type=int, action="append", default=[])
    parser.add_argument("--entity-id", action="append", default=[])
    parser.add_argument("--reason", required=True)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--send", action="store_true")
    parser.add_argument("--max-imputation-tier", type=_non_negative_int)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run the resend entry point and return a process exit code."""
    load_dotenv(find_dotenv(usecwd=True))
    try:
        args = _parse_args(argv)
        logging_settings = LoggingSettings.from_env()
        configure_logging(logging_settings, product="resend")
        plan = _build_plan(args)
        with _product_lock(plan.product):
            return _send_resend(plan) if plan.send else _dry_run(plan)
    except ResendConfigError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


def _build_plan(args: argparse.Namespace) -> ResendPlan:
    if args.place and args.entity_id:
        raise ResendConfigError("--place and --entity-id are mutually exclusive")
    if args.product == "direction":
        if args.place:
            raise ResendConfigError(
                "--place is not valid for aggregate direction resend"
            )
        if args.entity_id:
            raise ResendConfigError(
                "--entity-id is not valid for aggregate direction resend"
            )
        if args.interval_min == 5:
            raise ResendConfigError(
                "aggregate direction resend only supports --interval-min 60"
            )
    interval_min = _resolve_interval_min(args)
    if args.product == "direction" and args.max_imputation_tier is not None:
        raise ResendConfigError("--max-imputation-tier is only valid for flow")
    reason = args.reason.strip()
    if not reason:
        raise ResendConfigError("--reason must not be empty")

    from_dt = _parse_window_arg(args.from_window, flag="--from")
    to_dt = _parse_window_arg(args.to_window, flag="--to")
    _validate_window_alignment(from_dt, interval_min, flag="--from")
    _validate_window_alignment(to_dt, interval_min, flag="--to")
    if to_dt < from_dt:
        raise ResendConfigError("--to must be greater than or equal to --from")

    filter_settings = FilterSettings.from_env()
    source_max_imputation_tier = (
        args.max_imputation_tier
        if args.max_imputation_tier is not None
        else filter_settings.source_max_imputation_tier
    )
    metadata = load_metadata(metadata_path_from_env())
    if args.product == "flow":
        target_batches = filter_settings.target_flow_batches
        validate_target_batches = filter_settings.validate_target_flow_batches
    else:
        target_batches = filter_settings.target_direction_batches
        validate_target_batches = filter_settings.validate_target_direction_batches

    if target_batches:
        validate_target_batches({place.batch for place in metadata})
    active_targets = active_places(
        metadata,
        target_batches=target_batches,
    )
    metadata_index = index_by_place_interval(active_targets)
    interval_metadata = _metadata_for_request(
        metadata_index,
        interval_min=interval_min,
        place_numbers=args.place,
        entity_ids=args.entity_id,
    )
    if not interval_metadata:
        raise ResendConfigError("no active metadata targets matched the request")

    return ResendPlan(
        product=args.product,
        interval_min=interval_min,
        from_window=args.from_window,
        to_window=args.to_window,
        reason=reason,
        force=bool(args.force),
        send=bool(args.send),
        windows=tuple(_enumerate_windows(from_dt, to_dt, interval_min)),
        interval_metadata=interval_metadata,
        source_max_imputation_tier=(
            source_max_imputation_tier if args.product == "flow" else None
        ),
        direction_settings=(
            RunDirectionSettings.from_env() if args.product == "direction" else None
        ),
    )


def _resolve_interval_min(args: argparse.Namespace) -> int:
    """Resolve the source aggregation interval from flags and entity ids.

    ``--interval-min`` is optional: when omitted, the interval is inferred
    from the canonical ``--entity-id`` values (``per300`` -> 5, ``per3600``
    -> 60; see :func:`sendai_pipeline.metadata.parse_entity_id`). Precedence:

    1. ``--interval-min`` given: it wins, but any ``--entity-id`` whose own
       interval contradicts it is rejected so a typo cannot resend the wrong
       series.
    2. No ``--interval-min`` and no ``--entity-id``: nothing to infer from,
       so the flag is required (this also covers the ``--place`` path, where
       a place exists at both intervals and the id carries no interval hint).
    3. No ``--interval-min`` with ``--entity-id``: every id must yield one
       interval, and all ids must agree on a single value.

    Returns:
        The resolved interval in minutes (5 or 60).

    Raises:
        ResendConfigError: If the flag and ids disagree, if the interval
            cannot be inferred, or if the ids span multiple intervals.
    """
    explicit_interval = args.interval_min
    if args.product == "direction" and explicit_interval is None:
        return 60
    entity_ids = tuple(args.entity_id)
    inferred_intervals = _intervals_from_entity_ids(entity_ids)

    # Case 1: explicit flag wins, but reject ids that contradict it.
    if explicit_interval is not None:
        disagreeing = [
            entity_id
            for entity_id, inferred_interval in inferred_intervals.items()
            if inferred_interval is not None and inferred_interval != explicit_interval
        ]
        if disagreeing:
            raise ResendConfigError(
                "--interval-min disagrees with --entity-id interval(s): "
                f"{_csv(disagreeing)}"
            )
        return int(explicit_interval)

    # Case 2: no flag and no ids to infer from (includes the --place path).
    if not entity_ids:
        raise ResendConfigError(
            "--interval-min is required unless --entity-id is given"
        )

    # Case 3: infer from ids. Every id must parse to an interval...
    missing = [
        entity_id
        for entity_id, inferred_interval in inferred_intervals.items()
        if inferred_interval is None
    ]
    if missing:
        raise ResendConfigError(
            "--interval-min is required when an --entity-id interval cannot be "
            f"inferred: {_csv(missing)}"
        )

    # ...and they must all agree, since one resend run targets one interval.
    concrete_intervals = {
        inferred_interval
        for inferred_interval in inferred_intervals.values()
        if inferred_interval is not None
    }
    if len(concrete_intervals) != 1:
        raise ResendConfigError(
            "--entity-id values span multiple intervals; pass --interval-min"
        )
    return concrete_intervals.pop()


def _intervals_from_entity_ids(entity_ids: Iterable[str]) -> dict[str, int | None]:
    """Map each entity id to the interval inferred from its canonical shape.

    Args:
        entity_ids: Entity ids to inspect.

    Returns:
        A mapping whose keys are exactly the given *entity_ids*. Each value is
        that id's inferred interval in minutes, or ``None`` when the id is
        non-canonical or carries an unknown type suffix. Keeping every input
        id as a key lets the caller report exactly which ids failed to infer.

        For example, given the two ids ``"jp.sendai.Blesensor.per300.10"``
        (canonical) and ``"bad-id"`` (non-canonical), the result is::

            {"jp.sendai.Blesensor.per300.10": 5, "bad-id": None}
    """
    intervals: dict[str, int | None] = {}
    for entity_id in entity_ids:
        parsed = parse_entity_id(entity_id)
        intervals[entity_id] = parsed.interval_min if parsed is not None else None
    return intervals


def _dry_run(plan: ResendPlan) -> int:
    _log_requested(plan)
    target_count = plan.target_count
    for startdate in plan.windows:
        window_key = _window_key(plan.product, plan.interval_min, startdate)
        write_plan = (
            "would_put=1"
            if plan.product == "direction"
            else f"would_post={target_count}"
        )
        print(
            f"DRY-RUN: would resend {plan.product} {window_key} "
            f"(target_count={target_count}, skipped_by_hash=0, {write_plan})"
        )
        _log_window_processed(
            plan,
            window_key=window_key,
            rows=0,
            writes_ok=0,
            writes_failed=0,
            rows_dropped=0,
            count_skipped=0,
            count_would_create=target_count,
            dry_run=True,
        )
    _log_summary(
        plan,
        windows_seen=len(plan.windows),
        writes_ok=0,
        writes_failed=0,
        windows_partial=0,
        windows_complete=0,
    )
    return 0


def _send_resend(plan: ResendPlan) -> int:
    run_started_at = datetime.now(JST)
    _log_requested(plan)
    writes_ok = 0
    writes_failed = 0
    windows_empty = 0
    windows_no_payload = 0
    windows_source_invalid = 0
    windows_gc = 0
    run_windows_partial = 0
    run_windows_complete = 0

    state_path = PRODUCT_STATE_PATHS[cast("Any", plan.product)]
    store = WindowStateStore.load(state_path)
    _abort_on_dead_letter_windows(plan, store)

    db_connection = None
    try:
        filter_settings = FilterSettings.from_env()
        auth_client = auth.AuthClient(auth.AuthSettings.from_env())
        orion = orion_client.OrionClient(
            orion_client.OrionSettings.from_env(),
            auth=auth_client,
        )
        db_connection = _connect_db()
        gc_cutoff = _resend_gc_cutoff(run_started_at)

        counter = 0
        try:
            for startdate in plan.windows:
                counts = _counts_for_product(plan.product)
                window_key = _window_key(plan.product, plan.interval_min, startdate)
                rows, db_connection = _select_rows_with_reconnect(
                    plan,
                    db_connection,
                    window_key=window_key,
                    startdate=startdate,
                )
                if not rows:
                    windows_empty += 1
                    _log_window_empty(plan, window_key=window_key)
                    continue

                _process_window(
                    plan,
                    window_key=window_key,
                    startdate=startdate,
                    rows=rows,
                    orion=orion,
                    state_store=store,
                    filter_settings=filter_settings,
                    counts=counts,
                    transformed_at=run_started_at,
                )
                if plan.product == "flow":
                    writes_ok += counts.posts_ok
                    writes_failed += counts.posts_failed
                else:
                    writes_ok += counts.puts_ok
                    writes_failed += counts.puts_failed
                    windows_no_payload += counts.windows_no_payload
                    windows_source_invalid += counts.windows_source_invalid
                run_windows_partial += counts.windows_partial
                run_windows_complete += counts.windows_complete
                _log_window_processed(
                    plan,
                    window_key=window_key,
                    rows=len(rows),
                    writes_ok=(
                        counts.posts_ok if plan.product == "flow" else counts.puts_ok
                    ),
                    writes_failed=(
                        counts.posts_failed
                        if plan.product == "flow"
                        else counts.puts_failed
                    ),
                    rows_dropped=counts.rows_dropped,
                    count_skipped=0,
                    count_would_create=plan.target_count,
                    dry_run=False,
                )
                counter += 1
                if counter >= _RESEND_SAVE_EVERY:
                    removed = _run_resend_gc(plan, store, gc_cutoff)
                    windows_gc += removed
                    store.save()
                    counter = 0
        except Exception:
            _best_effort_save(store)
            raise
        removed = _run_resend_gc(plan, store, gc_cutoff)
        windows_gc += removed
        if counter > 0 or removed > 0:
            store.save()
    finally:
        if db_connection is not None:
            _close_db_connection(db_connection)

    _log_summary(
        plan,
        windows_seen=len(plan.windows),
        writes_ok=writes_ok,
        writes_failed=writes_failed,
        windows_empty=windows_empty,
        windows_no_payload=windows_no_payload,
        windows_source_invalid=windows_source_invalid,
        windows_gc=windows_gc,
        windows_partial=run_windows_partial,
        windows_complete=run_windows_complete,
    )
    return 0 if writes_failed == 0 and windows_source_invalid == 0 else 1


def _abort_on_dead_letter_windows(
    plan: ResendPlan,
    store: WindowStateStore,
) -> None:
    """Abort live resend when requested windows include dead-letter state."""
    dead_letter_keys: list[str] = []
    for startdate in plan.windows:
        window_key = _window_key(plan.product, plan.interval_min, startdate)
        if store.window_status(window_key) == "dead_letter":
            dead_letter_keys.append(window_key)
    if dead_letter_keys:
        raise ResendConfigError(
            f"resend range contains dead-letter window(s): {_csv(dead_letter_keys)}"
        )


def _resend_gc_cutoff(run_started_at: datetime) -> datetime:
    """Return the stable complete-window GC cutoff for a resend run."""
    max_hours = max(_max_lookback_hours(5), _max_lookback_hours(60))
    horizon_hours = max(0, 2 * max_hours)
    return run_started_at - timedelta(hours=horizon_hours)


def _run_resend_gc(
    plan: ResendPlan,
    store: WindowStateStore,
    cutoff: datetime,
) -> int:
    """Reclaim old complete windows and log the per-call result."""
    removed = store.gc_complete_before(cutoff)
    logger.info(
        "resend gc completed",
        extra={
            "event": "resend_gc",
            "product": plan.product,
            "interval_min": plan.interval_min,
            "reason": plan.reason,
            "cutoff": cutoff.isoformat(),
            "windows_gc": removed,
        },
    )
    return removed


def _best_effort_save(store: WindowStateStore) -> None:
    """Try to flush resend progress without replacing the original failure."""
    try:
        store.save()
    except Exception:
        return


def _metadata_for_request(
    metadata_index: Mapping[tuple[int, int], SensorPlace],
    *,
    interval_min: int,
    place_numbers: Iterable[int],
    entity_ids: Iterable[str],
) -> dict[tuple[int, int], SensorPlace]:
    interval_metadata = {
        key: place for key, place in metadata_index.items() if key[1] == interval_min
    }
    places = tuple(place_numbers)
    ids = tuple(entity_ids)
    if places:
        missing = [
            place for place in places if (place, interval_min) not in metadata_index
        ]
        if missing:
            raise ResendConfigError(
                f"unknown active place number(s): {_csv(str(item) for item in missing)}"
            )
        allowed = set(places)
        return {
            key: place
            for key, place in interval_metadata.items()
            if place.place_number in allowed
        }
    if ids:
        active_ids = {place.entity_id for place in interval_metadata.values()}
        missing = [entity_id for entity_id in ids if entity_id not in active_ids]
        if missing:
            raise ResendConfigError(f"unknown active entity id(s): {_csv(missing)}")
        allowed_ids = set(ids)
        return {
            key: place
            for key, place in interval_metadata.items()
            if place.entity_id in allowed_ids
        }
    return interval_metadata


@contextmanager
def _product_lock(product: str) -> Iterator[None]:
    lock_path = PRODUCT_LOCK_PATHS[cast("Any", product)]
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a", encoding="utf-8") as lock_file:
        fcntl.flock(lock_file, fcntl.LOCK_EX)
        yield


def _process_window(
    plan: ResendPlan,
    *,
    window_key: str,
    startdate: str,
    rows: list[Mapping[str, Any]],
    orion: Any,
    state_store: WindowStateStore,
    filter_settings: FilterSettings,
    counts: Any,
    transformed_at: datetime,
) -> None:
    if plan.product == "flow":
        process_flow_window(
            window_key,
            interval_min=plan.interval_min,
            startdate=startdate,
            rows_for_window=rows,
            orion=orion,
            state_store=state_store,
            filter_settings=filter_settings,
            interval_metadata=plan.interval_metadata,
            expected_target_ids=plan.expected_target_ids,
            counts=counts,
            force_resend=plan.force,
            persist_each_target=False,
        )
        return

    if plan.direction_settings is None:
        raise ResendConfigError("direction resend requires aggregate settings")
    process_direction_window(
        window_key,
        interval_min=plan.interval_min,
        startdate=startdate,
        rows_for_window=rows,
        orion=orion,
        state_store=state_store,
        settings=plan.direction_settings,
        filter_settings=filter_settings,
        interval_metadata=plan.interval_metadata,
        counts=counts,
        transformed_at=transformed_at,
        force_resend=plan.force,
    )


def _select_rows(
    product: str,
    db_connection: Any,
    *,
    interval_min: int,
    startdate: str,
    max_imputation_tier: int | None,
) -> list[Mapping[str, Any]]:
    source_start = _parse_window_arg(startdate, flag="window")
    sql_bound = flow_sql_bound if product == "flow" else direction_sql_bound
    if product == "flow":
        if max_imputation_tier is None:
            raise ResendConfigError("flow resend requires an imputation-tier ceiling")
        return list(
            db.select_flow_metrics(
                db_connection,
                interval_min=interval_min,
                lower_bound=sql_bound(source_start),
                upper_bound=sql_bound(source_start),
                max_imputation_tier=max_imputation_tier,
            )
        )
    return list(
        db.select_direction_metrics(
            db_connection,
            interval_min=interval_min,
            lower_bound=sql_bound(source_start),
            upper_bound=sql_bound(source_start),
        )
    )


def _select_rows_with_reconnect(
    plan: ResendPlan,
    db_connection: Any,
    *,
    window_key: str,
    startdate: str,
) -> tuple[list[Mapping[str, Any]], Any]:
    """Select one resend window, reopening MySQL after dropped connections."""
    connection = db_connection
    for failed_attempts in range(_RESEND_DB_RECONNECT_ATTEMPTS + 1):
        try:
            rows = _select_rows(
                plan.product,
                connection,
                interval_min=plan.interval_min,
                startdate=startdate,
                max_imputation_tier=plan.source_max_imputation_tier,
            )
        except Exception as exc:
            if not db.is_connection_lost_error(exc):
                raise
            if failed_attempts >= _RESEND_DB_RECONNECT_ATTEMPTS:
                logger.exception(
                    "db reconnect exhausted before resend select",
                    extra={
                        "event": "resend_db_reconnect_exhausted",
                        "product": plan.product,
                        "interval_min": plan.interval_min,
                        "reason": plan.reason,
                        "window": window_key,
                        "attempts": _RESEND_DB_RECONNECT_ATTEMPTS,
                    },
                )
                _close_db_connection(connection)
                raise

            attempt = failed_attempts + 1
            logger.warning(
                "db reconnect before resend select",
                extra={
                    "event": "resend_db_reconnect",
                    "product": plan.product,
                    "interval_min": plan.interval_min,
                    "reason": plan.reason,
                    "window": window_key,
                    "attempt": attempt,
                    "error_class": exc.__class__.__name__,
                },
            )
            _close_db_connection(connection)
            time.sleep(_RESEND_DB_RECONNECT_BACKOFF_SECONDS)
            connection = _connect_db()
        else:
            return rows, connection

    raise AssertionError("unreachable resend reconnect state")


def _close_db_connection(db_connection: Any) -> None:
    """Close a MySQL handle without letting dead-socket close errors escape."""
    close = getattr(db_connection, "close", None)
    if close is None:
        return
    try:
        close()
    except Exception:
        return


def _connect_db() -> Any:
    try:
        settings = db.DbSettings.from_env()
    except db.DbConfigError as exc:
        try:
            return db.connect(cast("Any", None))
        except Exception:
            raise exc
    return db.connect(settings)


def _non_negative_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be an integer") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be non-negative")
    return parsed


def _counts_for_product(product: str) -> Any:
    return FlowCounts() if product == "flow" else DirectionCounts()


def _window_key(product: str, interval_min: int, startdate: str) -> str:
    if product == "flow":
        return flow_window_key(interval_min, startdate)
    return direction_window_key(interval_min, startdate)


def _parse_window_arg(value: str, *, flag: str) -> datetime:
    try:
        return datetime.strptime(value, _WINDOW_FORMAT).replace(tzinfo=JST)
    except ValueError as exc:
        raise ResendConfigError(
            f"{flag} must use YYYYMMDD_HHMM source-window format"
        ) from exc


def _validate_window_alignment(
    value: datetime,
    interval_min: int,
    *,
    flag: str,
) -> None:
    if value.minute % interval_min != 0:
        raise ResendConfigError(f"{flag} is not aligned to {interval_min} minutes")


def _max_lookback_hours(interval_min: int) -> int:
    key = _MAX_LOOKBACK_ENV[interval_min]
    value = os.environ.get(key)
    if value is None or value == "":
        return 72
    try:
        parsed = int(value)
    except ValueError as exc:
        raise ResendConfigError(
            f"environment variable must be an integer: {key}"
        ) from exc
    if parsed < 0:
        raise ResendConfigError(f"environment variable must be non-negative: {key}")
    return parsed


def _enumerate_windows(
    from_dt: datetime,
    to_dt: datetime,
    interval_min: int,
) -> Iterable[str]:
    current = from_dt
    while current <= to_dt:
        yield current.strftime(_WINDOW_FORMAT)
        current += timedelta(minutes=interval_min)


def _log_requested(plan: ResendPlan) -> None:
    logger.info(
        "resend requested",
        extra={
            "event": "resend_requested",
            "product": plan.product,
            "interval_min": plan.interval_min,
            "source_window_start": plan.from_window,
            "source_window_end": plan.to_window,
            "reason": plan.reason,
            "dry_run": not plan.send,
            "count_expected": plan.target_count,
        },
    )


def _log_window_processed(
    plan: ResendPlan,
    *,
    window_key: str,
    rows: int,
    writes_ok: int,
    writes_failed: int,
    rows_dropped: int,
    count_skipped: int,
    count_would_create: int,
    dry_run: bool,
) -> None:
    write_counts = (
        {"puts_ok": writes_ok, "puts_failed": writes_failed}
        if plan.product == "direction"
        else {"posts_ok": writes_ok, "posts_failed": writes_failed}
    )
    logger.info(
        "resend window processed",
        extra={
            "event": "resend_window_processed",
            "product": plan.product,
            "interval_min": plan.interval_min,
            "window": window_key,
            "reason": plan.reason,
            "rows": rows,
            **write_counts,
            "rows_dropped": rows_dropped,
            "count_skipped": count_skipped,
            "count_would_create": count_would_create,
            "dry_run": dry_run,
        },
    )


def _log_window_empty(plan: ResendPlan, *, window_key: str) -> None:
    logger.debug(
        "resend window empty",
        extra={
            "event": "resend_window_empty",
            "product": plan.product,
            "interval_min": plan.interval_min,
            "window": window_key,
            "reason": plan.reason,
        },
    )


def _log_summary(
    plan: ResendPlan,
    *,
    windows_seen: int,
    writes_ok: int,
    writes_failed: int,
    windows_partial: int,
    windows_complete: int,
    windows_empty: int = 0,
    windows_gc: int = 0,
    windows_no_payload: int = 0,
    windows_source_invalid: int = 0,
) -> None:
    write_counts = (
        {"puts_ok": writes_ok, "puts_failed": writes_failed}
        if plan.product == "direction"
        else {"posts_ok": writes_ok, "posts_failed": writes_failed}
    )
    direction_outcomes = (
        {
            "windows_no_payload": windows_no_payload,
            "windows_source_invalid": windows_source_invalid,
        }
        if plan.product == "direction"
        else {}
    )
    logger.info(
        "resend summary",
        extra={
            "event": "resend_summary",
            "product": plan.product,
            "interval_min": plan.interval_min,
            "windows_seen": windows_seen,
            **write_counts,
            **direction_outcomes,
            "windows_empty": windows_empty,
            "windows_gc": windows_gc,
            "windows_partial": windows_partial,
            "windows_complete": windows_complete,
            "reason": plan.reason,
            "dry_run": not plan.send,
        },
    )


def _csv(values: Iterable[str]) -> str:
    return ", ".join(values)


if __name__ == "__main__":
    raise SystemExit(main())
