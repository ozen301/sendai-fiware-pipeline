"""Replay explicit source windows through the product send path."""

import argparse
import fcntl
import logging
import os
import sys
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
)
from sendai_pipeline.run_direction import (
    JST,
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

    @property
    def expected_target_ids(self) -> list[str]:
        """Return the entity ids expected for each requested source window."""
        return [place.entity_id for place in self.interval_metadata.values()]


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Replay source windows through the product send path.",
    )
    parser.add_argument("product", choices=("flow", "direction"))
    parser.add_argument("--interval-min", type=int, choices=(5, 60), required=True)
    parser.add_argument("--from", dest="from_window", required=True)
    parser.add_argument("--to", dest="to_window", required=True)
    parser.add_argument("--place", type=int, action="append", default=[])
    parser.add_argument("--entity-id", action="append", default=[])
    parser.add_argument("--reason", required=True)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--allow-old", action="store_true")
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
    if args.product == "direction" and args.max_imputation_tier is not None:
        raise ResendConfigError("--max-imputation-tier is only valid for flow")
    reason = args.reason.strip()
    if not reason:
        raise ResendConfigError("--reason must not be empty")

    from_dt = _parse_window_arg(args.from_window, flag="--from")
    to_dt = _parse_window_arg(args.to_window, flag="--to")
    _validate_window_alignment(from_dt, args.interval_min, flag="--from")
    _validate_window_alignment(to_dt, args.interval_min, flag="--to")
    if to_dt < from_dt:
        raise ResendConfigError("--to must be greater than or equal to --from")
    if not args.allow_old:
        _validate_lookback(from_dt, args.interval_min)

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
        interval_min=args.interval_min,
        place_numbers=args.place,
        entity_ids=args.entity_id,
    )
    if not interval_metadata:
        raise ResendConfigError("no active metadata targets matched the request")

    return ResendPlan(
        product=args.product,
        interval_min=args.interval_min,
        from_window=args.from_window,
        to_window=args.to_window,
        reason=reason,
        force=bool(args.force),
        send=bool(args.send),
        windows=tuple(_enumerate_windows(from_dt, to_dt, args.interval_min)),
        interval_metadata=interval_metadata,
        source_max_imputation_tier=(
            source_max_imputation_tier if args.product == "flow" else None
        ),
    )


def _dry_run(plan: ResendPlan) -> int:
    _log_requested(plan)
    target_count = len(plan.expected_target_ids)
    for startdate in plan.windows:
        window_key = _window_key(plan.product, plan.interval_min, startdate)
        print(
            f"DRY-RUN: would resend {plan.product} {window_key} "
            f"(target_count={target_count}, skipped_by_hash=0, "
            f"would_post={target_count})"
        )
        _log_window_processed(
            plan,
            window_key=window_key,
            rows=0,
            posts_ok=0,
            posts_failed=0,
            rows_dropped=0,
            count_skipped=0,
            count_would_create=target_count,
            dry_run=True,
        )
    _log_summary(plan, windows_seen=len(plan.windows), posts_ok=0, posts_failed=0)
    return 0


def _send_resend(plan: ResendPlan) -> int:
    _log_requested(plan)
    posts_ok = 0
    posts_failed = 0

    db_connection = _connect_db()
    try:
        filter_settings = FilterSettings.from_env()
        auth_client = auth.AuthClient(auth.AuthSettings.from_env())
        orion = orion_client.OrionClient(
            orion_client.OrionSettings.from_env(),
            auth=auth_client,
        )
        store = WindowStateStore.load(PRODUCT_STATE_PATHS[cast("Any", plan.product)])

        for startdate in plan.windows:
            counts = _counts_for_product(plan.product)
            rows = _select_rows(
                plan.product,
                db_connection,
                interval_min=plan.interval_min,
                startdate=startdate,
                max_imputation_tier=plan.source_max_imputation_tier,
            )
            _process_window(
                plan,
                window_key=_window_key(plan.product, plan.interval_min, startdate),
                startdate=startdate,
                rows=rows,
                orion=orion,
                state_store=store,
                filter_settings=filter_settings,
                counts=counts,
            )
            posts_ok += counts.posts_ok
            posts_failed += counts.posts_failed
            _log_window_processed(
                plan,
                window_key=_window_key(plan.product, plan.interval_min, startdate),
                rows=len(rows),
                posts_ok=counts.posts_ok,
                posts_failed=counts.posts_failed,
                rows_dropped=counts.rows_dropped,
                count_skipped=0,
                count_would_create=len(plan.expected_target_ids),
                dry_run=False,
            )
        store.save()
    finally:
        close = getattr(db_connection, "close", None)
        if close is not None:
            close()

    _log_summary(
        plan,
        windows_seen=len(plan.windows),
        posts_ok=posts_ok,
        posts_failed=posts_failed,
    )
    return 0 if posts_failed == 0 else 1


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
) -> None:
    process = (
        process_flow_window if plan.product == "flow" else process_direction_window
    )
    process(
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


def _validate_lookback(from_dt: datetime, interval_min: int) -> None:
    now = datetime.now(JST)
    max_hours = _max_lookback_hours(interval_min)
    if from_dt < now - timedelta(hours=max_hours):
        raise ResendConfigError(
            f"--from is older than MAX_LOOKBACK_HOURS for interval {interval_min}; "
            "pass --allow-old to continue"
        )


def _max_lookback_hours(interval_min: int) -> int:
    key = _MAX_LOOKBACK_ENV[interval_min]
    value = os.environ.get(key)
    if value is None or value == "":
        return 72
    try:
        return int(value)
    except ValueError as exc:
        raise ResendConfigError(
            f"environment variable must be an integer: {key}"
        ) from exc


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
            "count_expected": len(plan.expected_target_ids),
        },
    )


def _log_window_processed(
    plan: ResendPlan,
    *,
    window_key: str,
    rows: int,
    posts_ok: int,
    posts_failed: int,
    rows_dropped: int,
    count_skipped: int,
    count_would_create: int,
    dry_run: bool,
) -> None:
    logger.info(
        "resend window processed",
        extra={
            "event": "resend_window_processed",
            "product": plan.product,
            "interval_min": plan.interval_min,
            "window": window_key,
            "reason": plan.reason,
            "rows": rows,
            "posts_ok": posts_ok,
            "posts_failed": posts_failed,
            "rows_dropped": rows_dropped,
            "count_skipped": count_skipped,
            "count_would_create": count_would_create,
            "dry_run": dry_run,
        },
    )


def _log_summary(
    plan: ResendPlan,
    *,
    windows_seen: int,
    posts_ok: int,
    posts_failed: int,
) -> None:
    logger.info(
        "resend summary",
        extra={
            "event": "resend_summary",
            "product": plan.product,
            "interval_min": plan.interval_min,
            "windows_seen": windows_seen,
            "posts_ok": posts_ok,
            "posts_failed": posts_failed,
            "reason": plan.reason,
            "dry_run": not plan.send,
        },
    )


def _csv(values: Iterable[str]) -> str:
    return ", ".join(values)


if __name__ == "__main__":
    raise SystemExit(main())
