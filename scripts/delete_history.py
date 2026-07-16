"""Delete STH-Comet history for operator-selected entities or attributes."""

import argparse
import logging
import os
import sys
from collections.abc import Sequence
from dataclasses import dataclass

import requests
from dotenv import find_dotenv, load_dotenv

from sendai_pipeline.auth import AuthClient, AuthSettings
from sendai_pipeline.comet_client import CometClient, CometSettings
from sendai_pipeline.logging_setup import LoggingSettings, configure_logging
from sendai_pipeline.metadata import parse_entity_id
from sendai_pipeline.sth_subscriptions import (
    PRODUCT_A_HISTORY_ATTRS,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class HistoryTarget:
    """One entity whose Comet history may be deleted."""

    entity_id: str
    entity_type: str


class DeleteHistoryConfigError(RuntimeError):
    """Raised when delete-history arguments are invalid."""


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    """Parse command-line arguments for the history delete helper."""
    parser = argparse.ArgumentParser(
        description="Delete STH-Comet history for selected entities.",
    )
    parser.add_argument("entity_specs", nargs="+", metavar="ENTITY_ID[:ENTITY_TYPE]")
    parser.add_argument("--type", dest="entity_type")
    attrs = parser.add_mutually_exclusive_group()
    attrs.add_argument("--attrs", help="Comma-separated attributes to delete.")
    attrs.add_argument("--flow-attrs", action="store_true")
    parser.add_argument("--reason", required=True)
    parser.add_argument("--send", action="store_true")
    parser.add_argument("--i-know-this-is-production", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the STH-Comet history delete entry point."""
    load_dotenv(find_dotenv(usecwd=True))
    try:
        args = _parse_args(argv)
        configure_logging(LoggingSettings.from_env(), product="delete_history")
        targets = _parse_targets(args.entity_specs, default_type=args.entity_type)
        attrs = _attrs(args)
        if args.send and _requires_production_override(args):
            raise DeleteHistoryConfigError(
                "--send against a catch-all FIWARE service/path requires "
                "--i-know-this-is-production"
            )
    except DeleteHistoryConfigError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    logger.info(
        "delete-history requested",
        extra={
            "event": "delete_history_requested",
            "send": bool(args.send),
            "reason": args.reason,
            "target_count": len(targets),
            "attr_scope": list(attrs) if attrs is not None else None,
        },
    )

    if not args.send:
        for plan in _planned_targets(targets, attrs=attrs):
            print(f"DRY-RUN: would DELETE {plan['url']}")
            logger.info(
                "delete-history dry-run planned",
                extra={
                    "event": "delete_history_target",
                    "entity_id": plan["entity_id"],
                    "entity_type": plan["entity_type"],
                    "scope": plan["scope"],
                    "attr": plan["attr"] or None,
                    "send": False,
                },
            )
        logger.info(
            "delete-history dry-run summary",
            extra={
                "event": "delete_history_summary",
                "send": False,
                "target_count": len(targets),
                "deleted": 0,
                "absent": 0,
                "failed": 0,
            },
        )
        return 0

    auth = AuthClient(AuthSettings.from_env())
    comet = CometClient(CometSettings.from_env(), auth=auth)

    deleted = 0
    absent = 0
    failed = 0
    for target in targets:
        if attrs is None:
            outcome = _delete_one_entity(comet, target)
        else:
            for attr in attrs:
                outcome = _delete_one_attr(comet, target, attr)
                if outcome == 204:
                    deleted += 1
                elif outcome == 404:
                    absent += 1
                else:
                    failed += 1
            continue
        if outcome == 204:
            deleted += 1
        elif outcome == 404:
            absent += 1
        else:
            failed += 1

    logger.info(
        "delete-history summary",
        extra={
            "event": "delete_history_summary",
            "send": True,
            "target_count": len(targets),
            "deleted": deleted,
            "absent": absent,
            "failed": failed,
        },
    )
    return 1 if failed else 0


def _parse_targets(
    specs: Sequence[str],
    *,
    default_type: str | None,
) -> list[HistoryTarget]:
    """Parse ``ENTITY_ID[:ENTITY_TYPE]`` specs into history targets.

    The entity type is optional. Precedence is: an explicit ``:ENTITY_TYPE`` on
    the spec, else the ``--type`` flag (*default_type*), else the type
    inferred from a canonical entity id (see
    :func:`sendai_pipeline.metadata.parse_entity_id`).

    Args:
        specs: Raw ``ENTITY_ID`` or ``ENTITY_ID:ENTITY_TYPE`` strings.
        default_type: Fallback entity type from ``--type``, used for bare ids
            when no inline type is given.

    Returns:
        One :class:`HistoryTarget` per spec, in input order.

    Raises:
        DeleteHistoryConfigError: For an empty/wildcard id, an empty inline
            type, or a non-canonical bare id with no ``--type`` to fall back on.
    """
    targets: list[HistoryTarget] = []
    for spec in specs:
        # rpartition on ":" splits off an inline type; no ":" means bare id.
        entity_id, separator, entity_type = spec.rpartition(":")
        if not separator:
            # Bare id: guard against catch-all, then resolve via --type or id.
            entity_id = spec
            if entity_id in {"", "*"}:
                raise DeleteHistoryConfigError(f"invalid entity spec: {spec!r}")
            entity_type = _entity_type_for_spec(
                entity_id,
                explicit_type=default_type,
                error_hint="pass an explicit :TYPE or --type",
            )
        elif entity_id in {"", "*"} or entity_type == "":
            # Inline form present but a side is empty/wildcard: reject.
            raise DeleteHistoryConfigError(f"invalid entity spec: {spec!r}")
        else:
            # Explicit inline type wins; note it if it shadows inference.
            _log_if_type_override(entity_id, entity_type)
        targets.append(HistoryTarget(entity_id, entity_type))
    return targets


def _entity_type_for_spec(
    entity_id: str,
    *,
    explicit_type: str | None,
    error_hint: str,
) -> str:
    """Resolve the entity type for a bare id from ``--type`` or the id itself.

    An explicit *explicit_type* (the ``--type`` flag) wins and shadows any
    inferred type; otherwise the type is inferred from the canonical id.

    Args:
        entity_id: Bare entity id (no inline ``:TYPE``).
        explicit_type: The ``--type`` flag value, or ``None`` if unset.
        error_hint: Trailing guidance appended to the error message naming
            the flags this tool accepts.

    Returns:
        The resolved entity type.

    Raises:
        DeleteHistoryConfigError: If no ``--type`` is given and *entity_id*
            is not canonical, so no type can be inferred.
    """
    parsed = parse_entity_id(entity_id)
    if explicit_type is not None:
        _log_if_type_override(entity_id, explicit_type)
        return explicit_type
    if parsed is not None:
        return parsed.entity_type
    raise DeleteHistoryConfigError(
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


def _attrs(args: argparse.Namespace) -> tuple[str, ...] | None:
    """Return selected attributes, or ``None`` for per-entity deletion."""
    if args.flow_attrs:
        return PRODUCT_A_HISTORY_ATTRS
    if args.attrs is None:
        return None
    attrs = tuple(part.strip() for part in args.attrs.split(",") if part.strip())
    if not attrs:
        raise DeleteHistoryConfigError("--attrs must name at least one attribute")
    return attrs


def _planned_targets(
    targets: Sequence[HistoryTarget],
    *,
    attrs: Sequence[str] | None,
) -> list[dict[str, str]]:
    """Return the Comet DELETE plan with per-target identity and URL."""
    base_url = os.environ.get("FIWARE_BASE_URL", "").rstrip("/")
    plans: list[dict[str, str]] = []
    for target in targets:
        entity_url = (
            f"{base_url}/comet/v1.0/contextEntities/type/"
            f"{target.entity_type}/id/{target.entity_id}"
        )
        if attrs is None:
            plans.append(
                {
                    "entity_id": target.entity_id,
                    "entity_type": target.entity_type,
                    "scope": "entity",
                    "attr": "",
                    "url": entity_url,
                }
            )
            continue
        for attr in attrs:
            plans.append(
                {
                    "entity_id": target.entity_id,
                    "entity_type": target.entity_type,
                    "scope": "attribute",
                    "attr": attr,
                    "url": f"{entity_url}/attributes/{attr}",
                }
            )
    return plans


def _delete_one_entity(comet: CometClient, target: HistoryTarget) -> int:
    """Delete all history for one entity; return HTTP status (0 on error)."""
    try:
        status = comet.delete_entity_history(target.entity_id, target.entity_type)
    except requests.HTTPError as exc:
        logger.exception(
            "delete-history target failed",
            extra={
                "event": "delete_history_target",
                "entity_id": target.entity_id,
                "entity_type": target.entity_type,
                "scope": "entity",
                "ok": False,
            },
        )
        print(
            f"ERROR: {target.entity_id}: STH-Comet delete failed: {exc}",
            file=sys.stderr,
        )
        return 0
    logger.info(
        "delete-history target processed",
        extra={
            "event": "delete_history_target",
            "entity_id": target.entity_id,
            "entity_type": target.entity_type,
            "scope": "entity",
            "ok": True,
            "http_status": status,
        },
    )
    return status


def _delete_one_attr(comet: CometClient, target: HistoryTarget, attr: str) -> int:
    """Delete one attribute history series; return HTTP status (0 on error)."""
    try:
        status = comet.delete_attribute_history(
            target.entity_id, target.entity_type, attr
        )
    except requests.HTTPError as exc:
        logger.exception(
            "delete-history target failed",
            extra={
                "event": "delete_history_target",
                "entity_id": target.entity_id,
                "entity_type": target.entity_type,
                "scope": "attribute",
                "attr": attr,
                "ok": False,
            },
        )
        print(
            f"ERROR: {target.entity_id}:{attr}: STH-Comet delete failed: {exc}",
            file=sys.stderr,
        )
        return 0
    logger.info(
        "delete-history target processed",
        extra={
            "event": "delete_history_target",
            "entity_id": target.entity_id,
            "entity_type": target.entity_type,
            "scope": "attribute",
            "attr": attr,
            "ok": True,
            "http_status": status,
        },
    )
    return status


def _requires_production_override(args: argparse.Namespace) -> bool:
    """Return whether live deletion needs the explicit production override.

    Matches the blank-as-default normalization performed by
    ``CometSettings.from_env`` so a ``FIWARE_SERVICE_PATH=`` env var
    (set but empty) is treated like the root catch-all path, not an
    arbitrary non-catch-all literal "".
    """
    if args.i_know_this_is_production:
        return False
    service = os.environ.get("FIWARE_SERVICE", "").strip()
    service_path = os.environ.get("FIWARE_SERVICE_PATH", "/").strip() or "/"
    return service == "" or service_path == "/"


if __name__ == "__main__":
    sys.exit(main())
