"""Read Orion current values or STH-Comet history for operator inspection."""

import argparse
import csv
import json
import logging
import os
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests
from dotenv import find_dotenv, load_dotenv

from sendai_pipeline.auth import AuthClient, AuthSettings
from sendai_pipeline.comet_client import CometClient, CometSettings, HistoryQuery
from sendai_pipeline.logging_setup import LoggingSettings, configure_logging
from sendai_pipeline.metadata import (
    index_by_place_interval,
    load_metadata,
    parse_entity_id,
)
from sendai_pipeline.orion_client import OrionClient, OrionSettings
from sendai_pipeline.settings_validation import validate_exact_value
from sendai_pipeline.sth_subscriptions import (
    PRODUCT_A_HISTORY_ATTRS,
    PRODUCT_B_STABLE_WRITE_ATTRS,
)

logger = logging.getLogger(__name__)

DEFAULT_LAST_N = 10
DEFAULT_METADATA_PATH = Path("metadata/sensors.csv")
_DEFAULT_PRODUCT_B_AGGREGATE_ENTITY_ID = "jp.sendai.Blesensor.flow"
_DEFAULT_PRODUCT_B_AGGREGATE_ENTITY_TYPE = "Blesensor.flow"
_WINDOW_FORMAT = "%Y%m%d_%H%M"
_JST = timezone(timedelta(hours=9), name="JST")


@dataclass(frozen=True)
class EntityTarget:
    """One entity selected for display."""

    entity_id: str
    entity_type: str | None


class ShowDataConfigError(RuntimeError):
    """Raised when show-data arguments are invalid."""


@dataclass(frozen=True)
class ShowDataSettings:
    """Configuration used to identify the Product B aggregate entity.

    The Product B aggregate identity is validated lazily via
    :meth:`product_b_aggregate_target`, not in :meth:`from_env`. A target is
    classified as the aggregate by comparing its id against the *unvalidated*
    configured id (:meth:`product_b_aggregate_entity_id_raw`). The
    ``PRODUCT_B_AGGREGATE_*`` values are validated only when the aggregate type
    must be resolved from configuration — a matched bare id with no ``--type``;
    an explicit ``--type`` supplies the type and bypasses that validation. A
    Product A read (whose id differs from the configured aggregate id) never
    validates ``PRODUCT_B_AGGREGATE_*``, so a malformed value cannot fail it.
    """

    metadata_path: Path = DEFAULT_METADATA_PATH
    # Raw, unvalidated env values for the aggregate identity (``None`` when
    # unset). Validated on demand by ``product_b_aggregate_target``.
    _product_b_entity_id_env: str | None = None
    _product_b_entity_type_env: str | None = None

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "ShowDataSettings":
        """Build aggregate entity settings from environment variables."""
        source = os.environ if env is None else env
        metadata_path = source.get("SENSOR_METADATA_PATH", "").strip()
        return cls(
            metadata_path=Path(metadata_path)
            if metadata_path
            else DEFAULT_METADATA_PATH,
            _product_b_entity_id_env=source.get("PRODUCT_B_AGGREGATE_ENTITY_ID"),
            _product_b_entity_type_env=source.get("PRODUCT_B_AGGREGATE_ENTITY_TYPE"),
        )

    def product_b_aggregate_entity_id_raw(self) -> str:
        """Return the configured aggregate id, default applied, *unvalidated*.

        Used to test whether a target is the aggregate entity before deciding
        whether to validate the full identity. Comparing against this raw value
        (rather than a canonical-shape heuristic) keeps a canonical-shaped
        aggregate override working, while a Product A target that does not match
        it never triggers ``PRODUCT_B_AGGREGATE_*`` validation.
        """
        if self._product_b_entity_id_env is None:
            return _DEFAULT_PRODUCT_B_AGGREGATE_ENTITY_ID
        return self._product_b_entity_id_env

    def product_b_aggregate_target(self) -> tuple[str, str]:
        """Return the validated Product B aggregate ``(entity_id, entity_type)``.

        Call this only for a confirmed aggregate target — one whose id equals
        :meth:`product_b_aggregate_entity_id_raw`. A Product A read never calls
        it and so never touches ``PRODUCT_B_AGGREGATE_*``.

        Raises:
            ShowDataConfigError: If either ``PRODUCT_B_AGGREGATE_*`` value is
                empty, has surrounding whitespace, or contains a control
                character.
        """
        return (
            validate_exact_value(
                self._product_b_entity_id_env,
                "PRODUCT_B_AGGREGATE_ENTITY_ID",
                _DEFAULT_PRODUCT_B_AGGREGATE_ENTITY_ID,
                ShowDataConfigError,
            ),
            validate_exact_value(
                self._product_b_entity_type_env,
                "PRODUCT_B_AGGREGATE_ENTITY_TYPE",
                _DEFAULT_PRODUCT_B_AGGREGATE_ENTITY_TYPE,
                ShowDataConfigError,
            ),
        )


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
        settings = ShowDataSettings.from_env()
        targets = _resolve_targets(args, settings=settings)
        attrs = _attrs(args, targets=targets, settings=settings)
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
                    f"cannot infer entity type for {target.entity_id!r}; "
                    "pass an explicit :TYPE or --type"
                )


def _resolve_targets(
    args: argparse.Namespace,
    *,
    settings: ShowDataSettings,
) -> list[EntityTarget]:
    """Resolve explicit entity specs or place numbers to entity targets."""
    explicit_specs = [*args.entity_id, *args.entity_specs]
    if args.place and explicit_specs:
        raise ShowDataConfigError("--place and --entity-id are mutually exclusive")
    if args.place:
        return _targets_from_places(
            args.place,
            interval_min=args.interval_min,
            metadata_path=settings.metadata_path,
        )
    if explicit_specs:
        return _parse_entity_specs(
            explicit_specs,
            default_type=args.entity_type,
            settings=settings,
        )
    raise ShowDataConfigError(
        "at least one --place, --entity-id, or ENTITY_SPEC is required"
    )


def _targets_from_places(
    place_numbers: Sequence[int],
    *,
    interval_min: int | None,
    metadata_path: Path,
) -> list[EntityTarget]:
    """Resolve place numbers to entity targets through runtime metadata.

    Args:
        place_numbers: Place numbers to resolve.
        interval_min: Aggregation interval to select. When ``None``
            (``--interval-min`` omitted), every active interval for the place
            is returned — usually both the 5-minute and 60-minute entities.
        metadata_path: Sensor metadata CSV to read.

    Returns:
        Resolved :class:`EntityTarget` rows. With ``interval_min=None`` a
        single place can expand to multiple targets.

    Raises:
        ShowDataConfigError: If a place has no active metadata row (at the
            requested interval, when one is given).
    """
    places = [place for place in load_metadata(metadata_path) if place.active]
    # Index is keyed by (place_number, interval_min); see metadata module.
    index = index_by_place_interval(places)

    targets: list[EntityTarget] = []
    for place_number in place_numbers:
        # No interval given: take every active interval for this place.
        if interval_min is None:
            matched_places = [
                place for key, place in index.items() if key[0] == place_number
            ]
            if not matched_places:
                raise ShowDataConfigError(
                    f"no active metadata row for place {place_number}"
                )
            targets.extend(
                EntityTarget(place.entity_id, place.entity_type)
                for place in matched_places
            )
            continue
        # Interval given: resolve the single matching (place, interval) row.
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
    settings: ShowDataSettings,
) -> list[EntityTarget]:
    """Parse ``ENTITY_ID[:ENTITY_TYPE]`` command-line specs into targets.

    A spec equal to the configured aggregate id is treated as a bare aggregate
    id before any ``:TYPE`` split, so a colon-bearing configured id (e.g. a URN)
    is not mis-split. Otherwise the entity type is optional, with precedence:
    explicit ``:ENTITY_TYPE``, else the ``--type`` flag (*default_type*), else
    the type inferred from a canonical id (see
    :func:`sendai_pipeline.metadata.parse_entity_id`). An inline ``:TYPE``
    applies only to the spec it is attached to, unlike ``--type``, which is
    the fallback for every spec in the batch that has no inline type.

    The inline split uses ``str.rpartition(":")``, the *last* colon in the
    spec, which has two consequences for a colon-bearing (e.g. URN-style)
    non-aggregate id:

    - It cannot be passed bare: a bare ``urn:ngsi-ld:Foo:Bar`` is mis-split
      into id ``urn:ngsi-ld:Foo`` and type ``Bar``. Because the split
      happens before the ``--type`` fallback is consulted, that ``Bar`` wins
      and ``--type`` does not rescue it. Give it a colon-free inline type
      instead, e.g. ``urn:ngsi-ld:Foo:Bar:SomeType``: the last colon splits
      off ``SomeType`` as the type and keeps the full id intact.
    - Its type cannot itself contain a colon inline (that colon would be the
      split point). Pass such a type with ``--type`` — but that only works
      when the id is colon-free or is the configured aggregate id.

    The configured aggregate id is exempt from the bare-id mis-split above:
    the exact-match check resolves it as a whole before any split, so it works
    bare whether or not it contains colons. (An inline colon-bearing type on it
    still re-enters the split, so pass such a type with ``--type``.)

    Args:
        specs: Raw ``ENTITY_ID`` or ``ENTITY_ID:ENTITY_TYPE`` strings.
        default_type: Fallback entity type from ``--type``, used for bare ids
            when no inline type is given.
        settings: Configured Product B aggregate identity.

    Returns:
        One :class:`EntityTarget` per spec, in input order.

    Raises:
        ShowDataConfigError: For an empty id, an empty inline id/type, or a
            non-canonical bare id with no ``--type`` to fall back on.
    """
    targets: list[EntityTarget] = []
    for spec in specs:
        # An exact match to the configured aggregate id is a bare aggregate id,
        # resolved before any ":TYPE" split: the configured id may itself
        # contain colons (e.g. a URN), which rpartition would otherwise
        # mis-split as an inline type. An explicit ``ID:TYPE`` (spec differs
        # from the configured id) still falls through to the inline path below.
        if spec == settings.product_b_aggregate_entity_id_raw():
            entity_type = _entity_type_for_spec(
                spec,
                explicit_type=default_type,
                settings=settings,
                error_hint="pass an explicit :TYPE or --type",
            )
            targets.append(EntityTarget(spec, entity_type))
            continue
        # rpartition on ":" splits off an inline type; no ":" means bare id.
        entity_id, separator, entity_type = spec.rpartition(":")
        if not separator:
            # Bare id: resolve the type via --type or canonical inference.
            if spec == "":
                raise ShowDataConfigError("entity id must not be empty")
            entity_id = spec
            entity_type = _entity_type_for_spec(
                entity_id,
                explicit_type=default_type,
                settings=settings,
                error_hint="pass an explicit :TYPE or --type",
            )
            targets.append(EntityTarget(entity_id, entity_type))
            continue
        # Inline form: both sides must be non-empty; the inline type wins.
        if not entity_id or not entity_type:
            raise ShowDataConfigError(f"invalid entity spec: {spec!r}")
        _log_if_type_override(entity_id, entity_type)
        targets.append(EntityTarget(entity_id, entity_type))
    return targets


def _entity_type_for_spec(
    entity_id: str,
    *,
    explicit_type: str | None,
    settings: ShowDataSettings,
    error_hint: str,
) -> str:
    """Resolve the entity type for a bare id from ``--type`` or the id itself.

    Precedence: an explicit *explicit_type* (the ``--type`` flag) wins;
    otherwise, if *entity_id* equals the configured aggregate id, the validated
    Product B type; otherwise a canonical id's inferred type. The aggregate id
    is matched against the *unvalidated* configured id, so any configured
    aggregate id — including a canonical-shaped one — gets the configured type;
    a target that does not match never validates ``PRODUCT_B_AGGREGATE_*``, so a
    malformed value cannot fail a Product A read.

    Args:
        entity_id: Bare entity id (no inline ``:TYPE``).
        explicit_type: The ``--type`` flag value, or ``None`` if unset.
        settings: Configured Product B aggregate identity.
        error_hint: Trailing guidance appended to the error message naming
            the flags this tool accepts.

    Returns:
        The resolved entity type.

    Raises:
        ShowDataConfigError: If no ``--type`` is given and *entity_id* is
            neither the configured aggregate id nor canonical, so no type can
            be inferred; or if it is the aggregate id but the configured
            ``PRODUCT_B_AGGREGATE_*`` values are malformed.
    """
    if explicit_type is not None:
        _log_if_type_override(entity_id, explicit_type)
        return explicit_type
    if entity_id == settings.product_b_aggregate_entity_id_raw():
        _, aggregate_type = settings.product_b_aggregate_target()
        return aggregate_type
    parsed = parse_entity_id(entity_id)
    if parsed is not None:
        return parsed.entity_type
    raise ShowDataConfigError(
        f"cannot infer entity type for {entity_id!r}; {error_hint}"
    )


def _log_if_type_override(entity_id: str, explicit_type: str) -> None:
    """Log at DEBUG when an explicit type differs from the id's inferred type."""
    parsed = parse_entity_id(entity_id)
    if parsed is not None and parsed.entity_type != explicit_type:
        logger.debug(
            "explicit entity type overrides inferred type",
            extra={
                "event": "entity_type_override",
                "entity_id": entity_id,
                "inferred_entity_type": parsed.entity_type,
                "explicit_entity_type": explicit_type,
            },
        )


def _attrs(
    args: argparse.Namespace,
    *,
    targets: Sequence[EntityTarget],
    settings: ShowDataSettings,
) -> str | None:
    """Return the selected comma-separated attribute list."""
    if args.flow_attrs:
        return ",".join(PRODUCT_A_HISTORY_ATTRS)
    if args.attrs is not None:
        return args.attrs
    if args.source == "comet" and targets and _targets_are_aggregate(targets, settings):
        return ",".join(_aggregate_history_attrs(settings.metadata_path))
    return args.attrs


def _targets_are_aggregate(
    targets: Sequence[EntityTarget],
    settings: ShowDataSettings,
) -> bool:
    """Whether every target is the configured Product B aggregate entity.

    Compares each target id against the *unvalidated* configured aggregate id,
    so a Product A read (whose ids differ from it) never validates the Product B
    config — a malformed ``PRODUCT_B_AGGREGATE_*`` cannot fail it. This
    comparison does not itself validate the aggregate config; that happens
    earlier if the aggregate type is resolved from configuration (a bare
    aggregate id with no ``--type``).
    """
    aggregate_id = settings.product_b_aggregate_entity_id_raw()
    return all(target.entity_id == aggregate_id for target in targets)


def _aggregate_history_attrs(metadata_path: Path) -> tuple[str, ...]:
    """Return scalar and active 60-minute dynamic aggregate attributes.

    Enumerates one ``peopleCount_flow_<place_number>`` per active 60-minute
    metadata row, appended to the stable scalar attrs.

    This reads the CSV directly rather than through ``metadata.load_metadata``
    on purpose: enumeration must be *batch-independent* so an operator can
    inspect the history of any active place, including one whose ``batch`` is
    not among the values ``load_metadata`` recognises. ``load_metadata``
    validates ``batch`` against its own hard-coded set of known batch values
    and raises on any other value, which would hide that row's attribute from
    inspection. (That check is separate from the configured publish-target
    batches, which the pipeline filters on elsewhere.)
    """
    active_place_numbers: set[int] = set()
    try:
        with metadata_path.open(newline="", encoding="utf-8-sig") as handle:
            for row_number, row in enumerate(csv.DictReader(handle), start=2):
                active = (row.get("active") or "").strip().lower()
                if active not in {"true", "false"}:
                    raise ShowDataConfigError(
                        "metadata column active at row "
                        f"{row_number} must be true or false"
                    )
                if active == "false":
                    continue
                try:
                    interval_min = int((row.get("interval_min") or "").strip())
                    place_number = int((row.get("place_number") or "").strip())
                except ValueError as exc:
                    raise ShowDataConfigError(
                        f"invalid interval or place number at metadata row {row_number}"
                    ) from exc
                if interval_min == 60:
                    active_place_numbers.add(place_number)
    except OSError as exc:
        raise ShowDataConfigError(f"failed to read metadata: {metadata_path}") from exc

    dynamic_attrs = tuple(
        f"peopleCount_flow_{place_number}"
        for place_number in sorted(active_place_numbers)
    )
    return (*PRODUCT_B_STABLE_WRITE_ATTRS, *dynamic_attrs)


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


if __name__ == "__main__":
    sys.exit(main())
