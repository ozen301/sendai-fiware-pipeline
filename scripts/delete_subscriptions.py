"""Delete Orion subscriptions by id."""

import argparse
import logging
import os
import re
import sys
from collections.abc import Sequence

import requests
from dotenv import find_dotenv, load_dotenv

from sendai_pipeline.auth import AuthClient, AuthSettings
from sendai_pipeline.logging_setup import LoggingSettings, configure_logging
from sendai_pipeline.orion_client import OrionSettings
from sendai_pipeline.sth_subscriptions import (
    delete_subscription,
    get_subscription,
)

logger = logging.getLogger("sendai_pipeline")

_SUBSCRIPTION_ID_PATTERN = re.compile(r"[0-9a-f]{24}")


class DeleteSubscriptionsConfigError(RuntimeError):
    """Raised when delete-subscriptions arguments are invalid."""


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    """Parse command-line arguments for the subscription delete helper."""
    parser = argparse.ArgumentParser(
        description="Delete Orion subscriptions by id.",
    )
    parser.add_argument("subscription_ids", nargs="+", metavar="SUBSCRIPTION_ID")
    parser.add_argument("--reason", required=True)
    parser.add_argument("--send", action="store_true")
    parser.add_argument("--i-know-this-is-production", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the Orion subscription delete entry point."""
    load_dotenv(find_dotenv(usecwd=True))
    try:
        args = _parse_args(argv)
        configure_logging(LoggingSettings.from_env(), product="delete_subscriptions")
        args.reason = _validate_reason(args.reason)
        subscription_ids = _validate_subscription_ids(args.subscription_ids)
        if args.send and _requires_production_override(args):
            raise DeleteSubscriptionsConfigError(
                "--send against a catch-all FIWARE service/path requires "
                "--i-know-this-is-production"
            )
    except DeleteSubscriptionsConfigError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    logger.info(
        "delete-subscriptions requested",
        extra={
            "event": "delete_subscriptions_requested",
            "send_mode": "live" if args.send else "dry-run",
            "reason": args.reason,
            "dry_run": not args.send,
        },
    )

    settings = OrionSettings.from_env()
    auth = AuthClient(AuthSettings.from_env())

    deleted = 0
    absent = 0
    failed = 0
    for subscription_id in subscription_ids:
        try:
            existing = get_subscription(subscription_id, settings=settings, auth=auth)
        except requests.HTTPError as exc:
            logger.exception(
                "delete-subscriptions prefetch failed",
                extra=_failure_extras(subscription_id, "prefetch", args.reason, exc),
            )
            print(
                f"ERROR: {subscription_id}: prefetch failed: {exc}",
                file=sys.stderr,
            )
            failed += 1
            continue

        if existing is None:
            print(f"{subscription_id}: not found (already absent)")
            logger.info(
                "delete-subscriptions prefetch found no subscription",
                extra={
                    "event": "delete_subscriptions_target",
                    "subscription_id": subscription_id,
                    "phase": "prefetch",
                    "reason": args.reason,
                    "ok": True,
                    "http_status": 404,
                },
            )
            absent += 1
            continue

        description = existing.get("description")
        if not isinstance(description, str) or description == "":
            description = "<no description>"
        logger.info(
            "delete-subscriptions prefetch found subscription",
            extra={
                "event": "delete_subscriptions_target",
                "subscription_id": subscription_id,
                "phase": "prefetch",
                "reason": args.reason,
                "ok": True,
                "http_status": 200,
            },
        )
        action = "DELETE" if args.send else "DRY-RUN: would DELETE"
        print(f"{action} {subscription_id}: {description}")

        if not args.send:
            logger.info(
                "delete-subscriptions dry-run planned delete",
                extra={
                    "event": "delete_subscriptions_target",
                    "subscription_id": subscription_id,
                    "phase": "delete",
                    "reason": args.reason,
                    "ok": True,
                    "dry_run": True,
                },
            )
            continue

        try:
            status = delete_subscription(subscription_id, settings=settings, auth=auth)
        except requests.HTTPError as exc:
            logger.exception(
                "delete-subscriptions delete failed",
                extra=_failure_extras(subscription_id, "delete", args.reason, exc),
            )
            print(
                f"ERROR: {subscription_id}: delete failed: {exc}",
                file=sys.stderr,
            )
            failed += 1
            continue

        logger.info(
            "delete-subscriptions delete processed",
            extra={
                "event": "delete_subscriptions_target",
                "subscription_id": subscription_id,
                "phase": "delete",
                "reason": args.reason,
                "ok": True,
                "http_status": status,
            },
        )
        if status == 204:
            deleted += 1
        elif status == 404:
            absent += 1

    logger.info(
        "delete-subscriptions summary",
        extra={
            "event": "delete_subscriptions_summary",
            "send_mode": "live" if args.send else "dry-run",
            "reason": args.reason,
            "deleted": deleted,
            "absent": absent,
            "count_failed": failed,
        },
    )
    return 1 if failed else 0


def _validate_subscription_ids(specs: Sequence[str]) -> list[str]:
    """Reject empty, wildcard, malformed, or duplicate subscription ids."""
    ids: list[str] = []
    seen: set[str] = set()
    for spec in specs:
        if spec in {"", "*"} or not _SUBSCRIPTION_ID_PATTERN.fullmatch(spec):
            raise DeleteSubscriptionsConfigError(
                f"invalid subscription id: {spec!r} (expected 24-char lowercase hex)"
            )
        if spec in seen:
            raise DeleteSubscriptionsConfigError(f"duplicate subscription id: {spec!r}")
        seen.add(spec)
        ids.append(spec)
    return ids


def _failure_extras(
    subscription_id: str,
    phase: str,
    reason: str,
    exc: requests.HTTPError,
) -> dict[str, object]:
    """Build a target-record extras dict for a failed prefetch or delete."""
    extras: dict[str, object] = {
        "event": "delete_subscriptions_target",
        "subscription_id": subscription_id,
        "phase": phase,
        "reason": reason,
        "ok": False,
    }
    response = getattr(exc, "response", None)
    status = getattr(response, "status_code", None)
    if isinstance(status, int):
        extras["http_status"] = status
    return extras


def _validate_reason(reason: str) -> str:
    """Reject empty/whitespace ``--reason`` values that defeat the audit trail."""
    stripped = reason.strip()
    if stripped == "":
        raise DeleteSubscriptionsConfigError("--reason must not be blank")
    return stripped


def _requires_production_override(args: argparse.Namespace) -> bool:
    """Return whether live deletion needs the explicit production override."""
    if args.i_know_this_is_production:
        return False
    service = os.environ.get("FIWARE_SERVICE", "").strip()
    service_path = os.environ.get("FIWARE_SERVICE_PATH", "/").strip() or "/"
    return service == "" or service_path == "/"


if __name__ == "__main__":
    sys.exit(main())
