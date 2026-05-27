r"""Create Orion subscriptions for STH-Comet history (Products A and B).

The default is dry-run: it prints the redacted subscription bodies for the
selected products and does not contact FIWARE. Pass ``--send`` only after
relevant cron jobs are stopped and ``COMET_NOTIFY_URL`` is set in private
runtime configuration.

Usage:
    uv run python scripts/create_sth_subscriptions.py
    uv run python scripts/create_sth_subscriptions.py --product b
    uv run python scripts/create_sth_subscriptions.py --send

The default ``--product all`` is safe to re-run: the creator skips any
subscription it already finds.
"""

import argparse
import sys
from collections.abc import Sequence
from dataclasses import replace

from dotenv import find_dotenv, load_dotenv

from sendai_pipeline.auth import AuthClient, AuthSettings
from sendai_pipeline.logging_setup import LoggingSettings, configure_logging
from sendai_pipeline.sth_subscriptions import (
    StHSubscriptionResult,
    StHSubscriptionSettings,
    create_product_a_sth_subscription,
    create_product_b_sth_subscription,
    redacted_product_a_subscription_json,
    redacted_product_b_subscription_json,
)

_PRODUCT_CHOICES = ("a", "b", "all")


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Create Product A and/or Product B STH-Comet Orion subscriptions.",
    )
    parser.add_argument(
        "--product",
        choices=_PRODUCT_CHOICES,
        default="all",
        help="Which product subscription to create (default: all).",
    )
    parser.add_argument(
        "--send",
        action="store_true",
        help="Create subscriptions live. Omit for dry-run.",
    )
    parser.add_argument(
        "--show-body",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Print the redacted subscription body in dry-run mode.",
    )
    return parser.parse_args(argv)


def _selected_products(product: str) -> tuple[str, ...]:
    return ("a", "b") if product == "all" else (product,)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the subscription creation entry point."""
    load_dotenv(find_dotenv(usecwd=True))

    logging_settings = LoggingSettings.from_env()
    configure_logging(logging_settings, product="create_sth_subscriptions")

    args = _parse_args(argv)
    settings = replace(StHSubscriptionSettings.from_env(), dry_run=not args.send)
    auth = AuthClient(AuthSettings.from_env()) if not settings.dry_run else None

    products = _selected_products(args.product)
    results: list[StHSubscriptionResult] = []
    for product in products:
        if product == "a":
            if settings.dry_run and args.show_body:
                sys.stdout.write(redacted_product_a_subscription_json(settings) + "\n")
            results.append(
                create_product_a_sth_subscription(settings=settings, auth=auth)
            )
        else:
            if settings.dry_run and args.show_body:
                sys.stdout.write(redacted_product_b_subscription_json(settings) + "\n")
            results.append(
                create_product_b_sth_subscription(settings=settings, auth=auth)
            )

    return max((result.exit_code for result in results), default=0)


if __name__ == "__main__":
    sys.exit(main())
