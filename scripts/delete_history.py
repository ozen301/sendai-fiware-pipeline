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
from sendai_pipeline.sth_subscriptions import (
    PRODUCT_A_HISTORY_ATTRS,
    PRODUCT_B_HISTORY_ATTRS,
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
    attrs.add_argument("--direction-attrs", action="store_true")
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
    """Parse and validate ``ENTITY_ID[:ENTITY_TYPE]`` specs."""
    targets: list[HistoryTarget] = []
    for spec in specs:
        entity_id, separator, entity_type = spec.rpartition(":")
        if not separator:
            entity_id = spec
            entity_type = default_type or ""
        if entity_id in {"", "*"} or entity_type == "":
            raise DeleteHistoryConfigError(f"invalid entity spec: {spec!r}")
        targets.append(HistoryTarget(entity_id, entity_type))
    return targets


def _attrs(args: argparse.Namespace) -> tuple[str, ...] | None:
    """Return selected attributes, or ``None`` for per-entity deletion."""
    if args.flow_attrs:
        return PRODUCT_A_HISTORY_ATTRS
    if args.direction_attrs:
        return PRODUCT_B_HISTORY_ATTRS
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
