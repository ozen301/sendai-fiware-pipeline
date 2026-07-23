"""Show Orion subscriptions for operator inspection."""

import argparse
import json
import logging
import re
import sys
from collections.abc import Mapping, Sequence
from typing import Any

import requests
from dotenv import find_dotenv, load_dotenv

from sendai_pipeline.auth import AuthClient, AuthSettings
from sendai_pipeline.logging_setup import LoggingSettings, configure_logging
from sendai_pipeline.orion_client import OrionSettings
from sendai_pipeline.sth_subscriptions import (
    SubscriptionInventoryError,
    fetch_subscription_inventory,
    get_subscription,
)

logger = logging.getLogger(__name__)

_SUBSCRIPTION_ID_PATTERN = re.compile(r"[0-9a-f]{24}")
_TRANSPORT_NAMES = (
    "http",
    "httpCustom",
    "mqtt",
    "mqttCustom",
    "kafka",
    "kafkaCustom",
)
_CUSTOM_BODY_FIELDS = ("payload", "json", "ngsi")


class ShowSubscriptionsConfigError(RuntimeError):
    """Raised when show-subscriptions arguments are invalid."""


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    """Parse command-line arguments for subscription inspection."""
    parser = argparse.ArgumentParser(
        description="Show Orion subscriptions.",
    )
    parser.add_argument(
        "subscription_ids",
        nargs="*",
        metavar="SUBSCRIPTION_ID",
    )
    parser.add_argument("--json", action="store_true", dest="json_output")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the Orion subscription inspection entry point."""
    load_dotenv(find_dotenv(usecwd=True))
    try:
        args = _parse_args(argv)
        configure_logging(
            LoggingSettings.from_env(),
            product="show_subscriptions",
        )
        _bind_operator_logging()
        subscription_ids = _validate_subscription_ids(args.subscription_ids)
        settings = OrionSettings.from_env()
        auth = AuthClient(AuthSettings.from_env())
    except ShowSubscriptionsConfigError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:  # noqa: BLE001 - configuration boundary
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    mode = "id" if subscription_ids else "list"
    logger.info(
        "show-subscriptions requested",
        extra={
            "event": "show_subscriptions_requested",
            "phase": mode,
            "count_expected": len(subscription_ids),
        },
    )

    try:
        auth.get_token()
    except Exception as exc:  # noqa: BLE001 - runtime auth is operational
        _report_read_failure(exc)
        _log_summary(mode=mode, count=0, ok=False)
        return 1

    if subscription_ids:
        subscriptions, failed = _fetch_named_subscriptions(
            subscription_ids,
            settings=settings,
            auth=auth,
        )
    else:
        try:
            subscriptions = fetch_subscription_inventory(
                settings=settings,
                auth=auth,
            )
        except Exception as exc:  # noqa: BLE001 - inventory is fail-closed
            _report_read_failure(exc)
            _log_summary(mode=mode, count=0, ok=False)
            return 1
        failed = False

    _write_result(subscriptions, json_output=args.json_output)
    _log_summary(mode=mode, count=len(subscriptions), ok=not failed)
    return 1 if failed else 0


def _bind_operator_logging() -> None:
    """Use package logging and force console records onto stderr."""
    package_logger = logging.getLogger("sendai_pipeline")
    # ``scripts`` is outside the package logger's name hierarchy, so connect
    # this module logger to the handlers configured for the package.
    logger.parent = package_logger
    logger.setLevel(logging.NOTSET)
    for handler in package_logger.handlers:
        if isinstance(handler, logging.StreamHandler) and not isinstance(
            handler, logging.FileHandler
        ):
            handler.setStream(sys.stderr)


def _validate_subscription_ids(specs: Sequence[str]) -> list[str]:
    """Reject empty, wildcard, malformed, or duplicate subscription ids."""
    subscription_ids: list[str] = []
    seen: set[str] = set()
    for spec in specs:
        if spec in {"", "*"} or not _SUBSCRIPTION_ID_PATTERN.fullmatch(spec):
            raise ShowSubscriptionsConfigError(
                f"invalid subscription id: {spec!r} (expected 24-char lowercase hex)"
            )
        if spec in seen:
            raise ShowSubscriptionsConfigError(f"duplicate subscription id: {spec!r}")
        seen.add(spec)
        subscription_ids.append(spec)
    return subscription_ids


def _fetch_named_subscriptions(
    subscription_ids: Sequence[str],
    *,
    settings: Any,
    auth: Any,
) -> tuple[list[dict[str, Any]], bool]:
    """Fetch named subscriptions, continuing after per-id failures."""
    subscriptions: list[dict[str, Any]] = []
    failed = False
    for subscription_id in subscription_ids:
        try:
            subscription = get_subscription(
                subscription_id,
                settings=settings,
                auth=auth,
            )
        except Exception as exc:  # noqa: BLE001 - continue the requested ids
            status = _http_status(exc)
            print(
                f"ERROR: {subscription_id}: read failed: {exc}",
                file=sys.stderr,
            )
            logger.error(
                "show-subscriptions id read failed",
                extra={
                    "event": "show_subscriptions_target",
                    "subscription_id": subscription_id,
                    "http_status": status,
                    "ok": False,
                },
            )
            failed = True
            continue

        if subscription is None:
            print(f"{subscription_id}: not found", file=sys.stderr)
            logger.warning(
                "show-subscriptions id not found",
                extra={
                    "event": "show_subscriptions_target",
                    "subscription_id": subscription_id,
                    "http_status": 404,
                    "ok": False,
                },
            )
            failed = True
            continue

        subscriptions.append(subscription)
        logger.info(
            "show-subscriptions id read",
            extra={
                "event": "show_subscriptions_target",
                "subscription_id": subscription_id,
                "http_status": 200,
                "ok": True,
            },
        )
    return subscriptions, failed


def _report_read_failure(exc: BaseException) -> None:
    """Print full operator detail while logging only a safe status code."""
    print(f"ERROR: subscription read failed: {exc}", file=sys.stderr)
    logger.error(
        "show-subscriptions read failed",
        extra={
            "event": "show_subscriptions_failed",
            "http_status": _http_status(exc),
        },
    )


def _http_status(exc: BaseException) -> int:
    """Return the numeric HTTP status carried by an operational failure."""
    if isinstance(exc, SubscriptionInventoryError):
        return exc.http_status
    if isinstance(exc, requests.RequestException):
        response = getattr(exc, "response", None)
        status = getattr(response, "status_code", None)
        if isinstance(status, int):
            return status
    return 0


def _write_result(
    subscriptions: Sequence[Mapping[str, Any]],
    *,
    json_output: bool,
) -> None:
    """Write the raw JSON array or deterministic human summary."""
    if json_output:
        print(json.dumps(subscriptions, ensure_ascii=False))
        print(f"{len(subscriptions)} subscription(s)", file=sys.stderr)
        return

    for subscription in subscriptions:
        print(_render_subscription(subscription), end="")
    print(f"{len(subscriptions)} subscription(s)")


def _render_subscription(subscription: Mapping[str, Any]) -> str:
    """Render one deterministic human-readable subscription block."""
    subject = _mapping(subscription.get("subject"))
    condition = _mapping(subject.get("condition"))
    notification = _mapping(subscription.get("notification"))
    description = subscription.get("description")
    if not isinstance(description, str) or description == "":
        description = "<no description>"

    lines = [
        _format_value(subscription.get("id")),
        f"  status:        {_format_value(subscription.get('status'))}",
        f"  expires:       {_format_value(subscription.get('expires'))}",
        f"  description:   {description}",
        f"  entities:      {_format_entities(subject.get('entities'))}",
        f"  trigger attrs: {_format_trigger_attrs(condition.get('attrs'))}",
        f"  expression:    {_format_value(condition.get('expression'))}",
        (
            "  notifyOnMetadataChange: "
            f"{_format_value(condition.get('notifyOnMetadataChange'))}"
        ),
        f"  throttling:    {_format_value(subscription.get('throttling'))}",
        f"  notification:  {_format_notification(notification)}",
        (
            "  delivery:      "
            f"timesSent={_format_value(notification.get('timesSent'))} "
            f"failsCounter={_format_value(notification.get('failsCounter'))} "
            "lastNotification="
            f"{_format_value(notification.get('lastNotification'))}"
        ),
        (
            "                 "
            f"lastSuccess={_format_value(notification.get('lastSuccess'))}"
            f"({_format_value(notification.get('lastSuccessCode'))}) "
            f"lastFailure={_format_value(notification.get('lastFailure'))}"
            f"({_format_value(notification.get('lastFailureCode'))}) "
            "lastFailureReason="
            f"{_format_value(notification.get('lastFailureReason'))}"
        ),
    ]
    return "\n".join(lines) + "\n"


def _format_entities(value: Any) -> str:
    """Render Orion entity selectors in broker order."""
    if not isinstance(value, list) or not value:
        return "<none>"
    selectors: list[str] = []
    for item in value:
        if not isinstance(item, Mapping):
            selectors.append(_format_value(item))
            continue
        parts: list[str] = []
        for key in ("type", "id", "idPattern"):
            if key in item:
                parts.append(f"{key}={_format_value(item[key])}")
        selectors.append(" ".join(parts) if parts else "<none>")
    return " ; ".join(selectors)


def _format_notification(notification: Mapping[str, Any]) -> str:
    """Render the selected Orion notification transport and common fields."""
    transport = next(
        (name for name in _TRANSPORT_NAMES if name in notification),
        None,
    )
    transport_body = (
        _mapping(notification.get(transport)) if transport is not None else {}
    )
    parts = [f"transport={transport or '<none>'}"]

    if transport in {"http", "httpCustom"}:
        parts.append(f"url={_format_value(transport_body.get('url'))}")
    elif transport in {"mqtt", "mqttCustom"}:
        parts.extend(
            (
                f"url={_format_value(transport_body.get('url'))}",
                f"topic={_format_value(transport_body.get('topic'))}",
            )
        )
        for key in ("qos", "retain"):
            if key in transport_body:
                parts.append(f"{key}={_format_value(transport_body[key])}")
    elif transport in {"kafka", "kafkaCustom"}:
        parts.extend(
            (
                f"url={_format_value(transport_body.get('url'))}",
                f"topic={_format_value(transport_body.get('topic'))}",
            )
        )

    parts.extend(
        (
            f"format={_format_value(notification.get('attrsFormat'))}",
            f"metadata={_format_list(notification.get('metadata'))}",
            f"attrs={_format_all_attrs(notification.get('attrs'))}",
        )
    )

    if transport == "httpCustom":
        for key in ("method", "headers", "qs", *_CUSTOM_BODY_FIELDS):
            if key in transport_body:
                parts.append(f"{key}={_format_value(transport_body[key])}")
    elif transport in {"mqtt", "mqttCustom"}:
        for key in ("user", "passwd"):
            if key in transport_body:
                parts.append(f"{key}={_format_value(transport_body[key])}")
        if transport == "mqttCustom":
            for key in _CUSTOM_BODY_FIELDS:
                if key in transport_body:
                    parts.append(f"{key}={_format_value(transport_body[key])}")
    elif transport == "kafkaCustom":
        for key in _CUSTOM_BODY_FIELDS:
            if key in transport_body:
                parts.append(f"{key}={_format_value(transport_body[key])}")

    return " ".join(parts)


def _format_all_attrs(value: Any) -> str:
    """Render omitted or empty attributes as Orion's all-attribute form."""
    if value is None or value == []:
        return "<all>"
    return _format_list(value)


def _format_trigger_attrs(value: Any) -> str:
    """Render trigger attributes as a comma-separated summary."""
    if value is None or value == []:
        return "<all>"
    if not isinstance(value, list):
        return _format_value(value)
    return ", ".join(_format_value(item) for item in value)


def _format_list(value: Any) -> str:
    """Render a simple list without JSON string quoting."""
    if value is None:
        return "<none>"
    if not isinstance(value, list):
        return _format_value(value)
    return f"[{','.join(_format_value(item) for item in value)}]"


def _format_value(value: Any) -> str:
    """Render a scalar or structured value deterministically."""
    if value is None:
        return "<none>"
    if isinstance(value, (Mapping, list)):
        return json.dumps(value, sort_keys=True, separators=(",", ":"))
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _mapping(value: Any) -> Mapping[str, Any]:
    """Return a mapping value or an empty mapping."""
    return value if isinstance(value, Mapping) else {}


def _log_summary(*, mode: str, count: int, ok: bool) -> None:
    """Log the safe outcome summary."""
    logger.info(
        "show-subscriptions summary",
        extra={
            "event": "show_subscriptions_summary",
            "phase": mode,
            "count_live": count,
            "ok": ok,
        },
    )


if __name__ == "__main__":
    sys.exit(main())
