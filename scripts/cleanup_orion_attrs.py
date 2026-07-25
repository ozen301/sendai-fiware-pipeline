"""Delete explicit Orion attributes without touching STH-Comet history.

This standalone operator command is dry-run by default and performs one Orion
DELETE request per explicitly named attribute when ``--send`` is supplied. It
never scans for stale attributes and has no STH-Comet dependency. Deleting a
stable scalar attribute is supported for exceptional recovery, but operators
should normally preserve ``dateObservedFrom``, ``dateObservedTo``,
``dateRetrieved``, and ``identifcation``.
"""

import argparse
import logging
import os
import sys
from collections.abc import Sequence

from dotenv import find_dotenv, load_dotenv

from sendai_pipeline.auth import AuthClient, AuthSettings
from sendai_pipeline.logging_setup import LoggingSettings, configure_logging
from sendai_pipeline.orion_client import OrionClient, OrionSettings

logger = logging.getLogger(__name__)


class CleanupOrionAttrsConfigError(RuntimeError):
    """Raised when cleanup arguments do not satisfy the safety contract."""


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    """Parse command-line arguments for attribute cleanup."""
    parser = argparse.ArgumentParser(
        description=(
            "Delete explicit Orion attributes without deleting STH-Comet history."
        ),
    )
    parser.add_argument("--entity-id", required=True)
    parser.add_argument("--entity-type", required=True)
    parser.add_argument("--attrs", required=True)
    parser.add_argument("--reason", required=True)
    parser.add_argument("--send", action="store_true")
    parser.add_argument("--i-know-this-is-production", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the history-safe Orion attribute cleanup entry point."""
    load_dotenv(find_dotenv(usecwd=True))
    try:
        args = _parse_args(argv)
        configure_logging(
            LoggingSettings.from_env(),
            product="cleanup_orion_attrs",
        )
        entity_id = _require_nonblank(args.entity_id, "--entity-id")
        entity_type = _require_nonblank(args.entity_type, "--entity-type")
        attrs = _parse_attrs(args.attrs)
        reason = _require_nonblank(args.reason, "--reason")
        if args.send and _requires_production_override(args):
            raise CleanupOrionAttrsConfigError(
                "--send against a catch-all FIWARE service/path requires "
                "--i-know-this-is-production"
            )
    except CleanupOrionAttrsConfigError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    logger.info(
        "Orion attribute cleanup requested",
        extra={
            "event": "cleanup_orion_attrs_requested",
            "entity_id": entity_id,
            "entity_type": entity_type,
            "send_mode": "live" if args.send else "dry-run",
            "reason": reason,
            "count_expected": len(attrs),
            "dry_run": not args.send,
        },
    )

    if not args.send:
        _print_dry_run(entity_id, entity_type, attrs, reason=reason)
        _log_summary(
            entity_id,
            entity_type,
            send=False,
            deleted=0,
            absent=0,
            failed=0,
        )
        return 0

    auth = AuthClient(AuthSettings.from_env())
    orion = OrionClient(OrionSettings.from_env(), auth=auth)

    deleted = 0
    absent = 0
    failed = 0
    for attr_name in attrs:
        result = orion.delete_attr(entity_id, entity_type, attr_name)
        status = int(result["status"])
        ok = bool(result["ok"])

        if ok and status == 204:
            deleted += 1
            print(f"DELETED: {attr_name}")
        elif ok and status == 404:
            absent += 1
            print(f"ALREADY ABSENT: {attr_name}")
        else:
            failed += 1
            detail = result.get("body_excerpt")
            suffix = f": {detail}" if detail else ""
            print(f"FAILED: {attr_name} (status={status}){suffix}", file=sys.stderr)

        log_target = logger.info if ok else logger.error
        log_target(
            "Orion attribute cleanup target processed",
            extra={
                "event": "cleanup_orion_attr_target",
                "entity_id": entity_id,
                "entity_type": entity_type,
                "path": attr_name,
                "phase": "orion",
                "http_status": status,
                "ok": ok,
            },
        )

    print(f"SUMMARY: deleted={deleted} absent={absent} failed={failed}")
    _log_summary(
        entity_id,
        entity_type,
        send=True,
        deleted=deleted,
        absent=absent,
        failed=failed,
    )
    return 1 if failed else 0


def _require_nonblank(value: str, option: str) -> str:
    """Return a stripped option value or raise for blank input."""
    normalized = value.strip()
    if not normalized:
        raise CleanupOrionAttrsConfigError(f"{option} must not be blank")
    return normalized


def _parse_attrs(value: str) -> tuple[str, ...]:
    """Return explicit attribute names in command-line order."""
    attrs = tuple(part.strip() for part in value.split(",") if part.strip())
    if not attrs:
        raise CleanupOrionAttrsConfigError(
            "--attrs must name at least one explicit attribute"
        )
    return attrs


def _print_dry_run(
    entity_id: str,
    entity_type: str,
    attrs: Sequence[str],
    *,
    reason: str,
) -> None:
    """Print and log one planned Orion-only deletion per attribute."""
    for attr_name in attrs:
        print(
            "DRY-RUN: would DELETE Orion attribute "
            f"{attr_name} from {entity_id} (type={entity_type})"
        )
        logger.info(
            "Orion attribute cleanup target planned",
            extra={
                "event": "cleanup_orion_attr_target",
                "entity_id": entity_id,
                "entity_type": entity_type,
                "path": attr_name,
                "phase": "orion",
                "send_mode": "dry-run",
                "reason": reason,
                "dry_run": True,
            },
        )


def _log_summary(
    entity_id: str,
    entity_type: str,
    *,
    send: bool,
    deleted: int,
    absent: int,
    failed: int,
) -> None:
    """Log aggregate cleanup counts."""
    logger.info(
        "Orion attribute cleanup summary",
        extra={
            "event": "cleanup_orion_attrs_summary",
            "entity_id": entity_id,
            "entity_type": entity_type,
            "send_mode": "live" if send else "dry-run",
            "deleted": deleted,
            "absent": absent,
            "count_failed": failed,
            "dry_run": not send,
        },
    )


def _requires_production_override(args: argparse.Namespace) -> bool:
    """Return whether live deletion needs the production override flag."""
    if args.i_know_this_is_production:
        return False
    service = os.environ.get("FIWARE_SERVICE", "").strip()
    service_path = os.environ.get("FIWARE_SERVICE_PATH", "/").strip() or "/"
    return service == "" or service_path == "/"


if __name__ == "__main__":
    sys.exit(main())
