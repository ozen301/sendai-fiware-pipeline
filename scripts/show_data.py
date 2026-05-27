"""Read Orion current values or STH-Comet history for operator inspection."""

import argparse
import json
import logging
import os
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests
from dotenv import find_dotenv, load_dotenv

from sendai_pipeline.auth import AuthClient, AuthSettings
from sendai_pipeline.comet_client import CometClient, CometSettings, HistoryQuery
from sendai_pipeline.logging_setup import LoggingSettings, configure_logging
from sendai_pipeline.metadata import index_by_place_interval, load_metadata
from sendai_pipeline.orion_client import OrionClient, OrionSettings
from sendai_pipeline.sth_subscriptions import (
    PRODUCT_A_HISTORY_ATTRS,
    PRODUCT_B_HISTORY_ATTRS,
)

logger = logging.getLogger(__name__)

DEFAULT_LAST_N = 10
DEFAULT_METADATA_PATH = Path("metadata/sensors.csv")
_WINDOW_FORMAT = "%Y%m%d_%H%M"
_JST = timezone(timedelta(hours=9), name="JST")


@dataclass(frozen=True)
class EntityTarget:
    """One entity selected for display."""

    entity_id: str
    entity_type: str | None


class ShowDataConfigError(RuntimeError):
    """Raised when show-data arguments are invalid."""


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    """Parse command-line arguments for the data inspection helper."""
    parser = argparse.ArgumentParser(
        description="Read Orion current values or STH-Comet history.",
    )
    parser.add_argument(
        "entity_specs",
        nargs="*",
        metavar="ENTITY_ID[:ENTITY_TYPE]",
        help="Entity spec to read. Repeat for multiple entities.",
    )
    parser.add_argument("--source", choices=("orion", "comet"), required=True)
    parser.add_argument(
        "--type",
        dest="entity_type",
        help="Default NGSI entity type for specs that omit one.",
    )
    attrs = parser.add_mutually_exclusive_group()
    attrs.add_argument("--attrs", help="Comma-separated attribute selector.")
    attrs.add_argument(
        "--flow-attrs",
        action="store_true",
        help="Use the Product A attribute set.",
    )
    attrs.add_argument(
        "--direction-attrs",
        action="store_true",
        help="Use the Product B attribute set.",
    )
    parser.add_argument("--place", type=int, action="append", default=[])
    parser.add_argument(
        "--entity-id",
        action="append",
        default=[],
        metavar="ENTITY_ID[:ENTITY_TYPE]",
        help="Entity spec to read. Repeat for multiple entities.",
    )
    parser.add_argument("--from", dest="date_from", help="Comet lower time bound.")
    parser.add_argument("--to", dest="date_to", help="Comet upper time bound.")
    parser.add_argument("--last-n", type=int, help="Comet latest-record count.")
    parser.add_argument("--h-limit", type=int, help="STH-Comet page size.")
    parser.add_argument("--h-offset", type=int, help="STH-Comet page offset.")
    parser.add_argument("--aggr-method", help="STH-Comet aggregation method.")
    parser.add_argument("--aggr-period", help="STH-Comet aggregation period.")
    parser.add_argument("--interval-min", type=int, choices=(5, 60))
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the read-only data inspection entry point."""
    load_dotenv(find_dotenv(usecwd=True))
    try:
        args = _parse_args(argv)
        configure_logging(LoggingSettings.from_env(), product="show_data")
        targets = _resolve_targets(args)
        attrs = _attrs(args)
        _validate_source_args(args, attrs=attrs, targets=targets)
        comet_attrs = _split_attrs(attrs) if args.source == "comet" else []
        history_query = _history_query(args) if args.source == "comet" else None
    except ShowDataConfigError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    auth = AuthClient(AuthSettings.from_env())
    failures = 0

    if args.source == "orion":
        orion = OrionClient(OrionSettings.from_env(), auth=auth)
        records, failures = _read_orion(orion, targets, attrs=attrs)
    else:
        comet = CometClient(CometSettings.from_env(), auth=auth)
        assert history_query is not None
        records, failures = _read_comet(
            comet,
            targets,
            attrs=comet_attrs,
            query=history_query,
        )

    _emit(records, pretty=bool(args.pretty))
    return 1 if failures else 0


def _validate_source_args(
    args: argparse.Namespace,
    *,
    attrs: str | None,
    targets: Sequence[EntityTarget],
) -> None:
    """Validate source-specific argument combinations."""
    if args.source == "orion":
        rejected = {
            "--from": args.date_from,
            "--to": args.date_to,
            "--last-n": args.last_n,
            "--h-limit": args.h_limit,
            "--h-offset": args.h_offset,
            "--aggr-method": args.aggr_method,
            "--aggr-period": args.aggr_period,
        }
        used = [flag for flag, value in rejected.items() if value is not None]
        if used:
            raise ShowDataConfigError(
                f"{', '.join(used)} can only be used with --source comet"
            )
    if args.source == "comet":
        if attrs is None:
            raise ShowDataConfigError("--source comet requires an attribute selection")
        for target in targets:
            if target.entity_type is None:
                raise ShowDataConfigError(
                    f"missing entity type for target: {target.entity_id}"
                )


def _resolve_targets(args: argparse.Namespace) -> list[EntityTarget]:
    """Resolve explicit entity specs or place numbers to entity targets."""
    explicit_specs = [*args.entity_id, *args.entity_specs]
    if args.place and explicit_specs:
        raise ShowDataConfigError("--place and --entity-id are mutually exclusive")
    if args.place:
        if args.interval_min is None:
            raise ShowDataConfigError("--place requires --interval-min")
        return _targets_from_places(args.place, interval_min=args.interval_min)
    if explicit_specs:
        return _parse_entity_specs(explicit_specs, default_type=args.entity_type)
    raise ShowDataConfigError(
        "at least one --place, --entity-id, or ENTITY_SPEC is required"
    )


def _targets_from_places(
    place_numbers: Sequence[int],
    *,
    interval_min: int,
) -> list[EntityTarget]:
    """Resolve place numbers through runtime metadata."""
    places = [
        place for place in load_metadata(_metadata_path_from_env()) if place.active
    ]
    index = index_by_place_interval(places)

    targets: list[EntityTarget] = []
    for place_number in place_numbers:
        place = index.get((place_number, interval_min))
        if place is None:
            raise ShowDataConfigError(
                f"no active metadata row for place {place_number} "
                f"at {interval_min} minutes"
            )
        targets.append(EntityTarget(place.entity_id, place.entity_type))
    return targets


def _parse_entity_specs(
    specs: Sequence[str],
    *,
    default_type: str | None,
) -> list[EntityTarget]:
    """Parse ``ENTITY_ID[:ENTITY_TYPE]`` command-line specs."""
    targets: list[EntityTarget] = []
    for spec in specs:
        entity_id, separator, entity_type = spec.rpartition(":")
        if not separator:
            if spec == "":
                raise ShowDataConfigError("entity id must not be empty")
            targets.append(EntityTarget(spec, default_type))
            continue
        if not entity_id or not entity_type:
            raise ShowDataConfigError(f"invalid entity spec: {spec!r}")
        targets.append(EntityTarget(entity_id, entity_type))
    return targets


def _attrs(args: argparse.Namespace) -> str | None:
    """Return the selected comma-separated attribute list."""
    if args.flow_attrs:
        return ",".join(PRODUCT_A_HISTORY_ATTRS)
    if args.direction_attrs:
        return ",".join(PRODUCT_B_HISTORY_ATTRS)
    return args.attrs


def _split_attrs(attrs: str | None) -> list[str]:
    """Split and validate a comma-separated attribute selector."""
    if attrs is None:
        return []
    names = [part.strip() for part in attrs.split(",") if part.strip()]
    if not names:
        raise ShowDataConfigError("--attrs must name at least one attribute")
    return names


def _history_query(args: argparse.Namespace) -> HistoryQuery:
    """Build an STH-Comet query from command-line arguments."""
    last_n = args.last_n
    if last_n is None and args.date_from is None and args.date_to is None:
        last_n = DEFAULT_LAST_N
    return HistoryQuery(
        last_n=last_n,
        date_from=_normalize_date_arg(args.date_from, flag="--from"),
        date_to=_normalize_date_arg(args.date_to, flag="--to"),
        h_limit=args.h_limit,
        h_offset=args.h_offset,
        aggr_method=args.aggr_method,
        aggr_period=args.aggr_period,
    )


def _normalize_date_arg(value: str | None, *, flag: str) -> str | None:
    """Accept either ``YYYYMMDD_HHMM`` (JST) or ISO-8601; emit ISO for Comet.

    Operators commonly think in source-window keys (``20260524_1000``) to
    match ``resend.py``; Comet expects ISO-8601. Convert the window-key
    form to JST ISO transparently so a window-key works in show_data.py
    too. ISO-8601 strings are passed through unchanged.
    """
    if value is None:
        return None
    try:
        return datetime.strptime(value, _WINDOW_FORMAT).replace(tzinfo=_JST).isoformat()
    except ValueError:
        pass
    # Best-effort ISO-8601 validation: if it doesn't parse as a window key
    # AND doesn't parse as ISO either, surface a clear error early.
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ShowDataConfigError(
            f"{flag} must be ISO-8601 or YYYYMMDD_HHMM, got {value!r}"
        ) from exc
    return value


def _read_orion(
    orion: OrionClient,
    targets: Sequence[EntityTarget],
    *,
    attrs: str | None,
) -> tuple[list[dict[str, Any]], int]:
    """Read selected entities from Orion."""
    records: list[dict[str, Any]] = []
    failures = 0
    for target in targets:
        try:
            records.append(
                orion.get_entity(
                    target.entity_id,
                    entity_type=target.entity_type,
                    attrs=attrs,
                )
            )
        except requests.HTTPError as exc:
            if _status_code(exc) == 404:
                records.append({"entity_id": target.entity_id, "error": "not_found"})
                continue
            failures += 1
            print(
                f"ERROR: {target.entity_id}: Orion request failed: {exc}",
                file=sys.stderr,
            )
    return records, failures


def _read_comet(
    comet: CometClient,
    targets: Sequence[EntityTarget],
    *,
    attrs: Sequence[str],
    query: HistoryQuery,
) -> tuple[list[dict[str, Any]], int]:
    """Read selected entity attributes from STH-Comet."""
    records: list[dict[str, Any]] = []
    failures = 0
    for target in targets:
        assert target.entity_type is not None
        for attr in attrs:
            try:
                records.append(
                    comet.get_history(
                        target.entity_id,
                        target.entity_type,
                        attr,
                        query=query,
                    )
                )
            except requests.HTTPError as exc:
                if _status_code(exc) == 404:
                    records.append(
                        {
                            "entity_id": target.entity_id,
                            "attr": attr,
                            "error": "not_found",
                        }
                    )
                    continue
                failures += 1
                print(
                    f"ERROR: {target.entity_id}:{attr}: "
                    f"STH-Comet request failed: {exc}",
                    file=sys.stderr,
                )
    return records, failures


def _emit(records: Sequence[dict[str, Any]], *, pretty: bool) -> None:
    """Print records in JSON or table form."""
    if pretty:
        _print_table(_pretty_rows(records))
        return
    for record in records:
        print(json.dumps(record, ensure_ascii=False, indent=2, sort_keys=True))


def _pretty_rows(records: Sequence[dict[str, Any]]) -> list[tuple[str, str, str, str]]:
    """Convert raw response records to table rows.

    For Comet rows, group by ``(entity, recvTime)`` because rows from one
    Orion -> Comet notification share a recvTime; within each group, use
    the co-arriving ``dateObservedFrom`` value (when present) as the
    group's window key so groups order by their measurement window
    rather than the wall-clock arrival time. Duplicate notifications for
    the same window appear as separate adjacent groups under the same
    window.
    """
    grouped: dict[str, list[tuple[str, str, str, str]]] = {}
    order: list[str] = []
    for record in records:
        if record.get("error") == "not_found":
            new_rows = [
                (
                    str(record.get("entity_id", "")),
                    str(record.get("attr", "")),
                    "(not found)",
                    "",
                )
            ]
        elif "contextResponses" in record:
            new_rows = _comet_rows(record)
        else:
            new_rows = _orion_rows(record)
        for row in new_rows:
            entity_id = row[0]
            if entity_id not in grouped:
                grouped[entity_id] = []
                order.append(entity_id)
            grouped[entity_id].append(row)

    rows: list[tuple[str, str, str, str]] = []
    for entity_id in order:
        rows.extend(_sort_entity_rows(grouped[entity_id]))
    return rows


def _sort_entity_rows(
    rows: Sequence[tuple[str, str, str, str]],
) -> list[tuple[str, str, str, str]]:
    """Order one entity's rows by window-then-arrival, attr alphabetical."""
    by_recv_time: dict[str, list[tuple[str, str, str, str]]] = {}
    for row in rows:
        by_recv_time.setdefault(row[3], []).append(row)

    window_keys: dict[str, str] = {}
    for recv_time, group_rows in by_recv_time.items():
        window_keys[recv_time] = _window_key_for_group(group_rows) or recv_time

    sorted_recv_times = sorted(
        by_recv_time, key=lambda recv_time: (window_keys[recv_time], recv_time)
    )
    ordered: list[tuple[str, str, str, str]] = []
    for recv_time in sorted_recv_times:
        ordered.extend(sorted(by_recv_time[recv_time], key=lambda row: row[1]))
    return ordered


def _window_key_for_group(rows: Sequence[tuple[str, str, str, str]]) -> str | None:
    """Return the dateObservedFrom value among ``rows`` if present."""
    for row in rows:
        if row[1] == "dateObservedFrom":
            return row[2]
    return None


def _orion_rows(record: dict[str, Any]) -> list[tuple[str, str, str, str]]:
    """Convert one Orion entity JSON object to table rows."""
    entity_id = str(record.get("id", record.get("entity_id", "")))
    rows: list[tuple[str, str, str, str]] = []
    for attr, payload in record.items():
        if attr in {"id", "type"}:
            continue
        if not isinstance(payload, dict) or "value" not in payload:
            continue
        rows.append(
            (
                entity_id,
                attr,
                _format_value(payload.get("value")),
                _orion_time(payload),
            )
        )
    return rows


def _orion_time(payload: dict[str, Any]) -> str:
    """Return the Orion TimeInstant metadata value when present."""
    metadata = payload.get("metadata")
    if not isinstance(metadata, dict):
        return ""
    time_instant = metadata.get("TimeInstant")
    if not isinstance(time_instant, dict):
        return ""
    value = time_instant.get("value")
    return "" if value is None else str(value)


def _comet_rows(record: dict[str, Any]) -> list[tuple[str, str, str, str]]:
    """Convert one STH-Comet history JSON object to table rows."""
    rows: list[tuple[str, str, str, str]] = []
    for response in record.get("contextResponses", []):
        if not isinstance(response, dict):
            continue
        element = response.get("contextElement")
        if not isinstance(element, dict):
            continue
        entity_id = str(element.get("id", ""))
        for attr in element.get("attributes", []):
            if not isinstance(attr, dict):
                continue
            name = str(attr.get("name", ""))
            values = attr.get("values", [])
            if not isinstance(values, list):
                continue
            for item in values:
                if not isinstance(item, dict):
                    continue
                rows.append(
                    (
                        entity_id,
                        name,
                        _format_value(item.get("attrValue")),
                        "" if item.get("recvTime") is None else str(item["recvTime"]),
                    )
                )
    return rows


def _print_table(rows: Sequence[tuple[str, str, str, str]]) -> None:
    """Print a compact four-column table."""
    headers = ("entity", "attr", "value", "time")
    widths = [len(header) for header in headers]
    for row in rows:
        for index, value in enumerate(row):
            widths[index] = max(widths[index], len(value))
    print(
        "  ".join(header.ljust(widths[index]) for index, header in enumerate(headers))
    )
    for row in rows:
        print("  ".join(value.ljust(widths[index]) for index, value in enumerate(row)))


def _format_value(value: Any) -> str:
    """Return a readable table cell value."""
    if value is None:
        return "null"
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _status_code(exc: requests.HTTPError) -> int | None:
    """Return an HTTP status code from a ``requests.HTTPError`` if available."""
    response = getattr(exc, "response", None)
    return getattr(response, "status_code", None)


def _metadata_path_from_env() -> Path:
    """Return the runtime metadata path from the environment."""
    value = os.environ.get("SENSOR_METADATA_PATH")
    if value is None or value == "":
        return DEFAULT_METADATA_PATH
    return Path(value)


if __name__ == "__main__":
    sys.exit(main())
