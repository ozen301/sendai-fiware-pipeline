r"""One-shot entry point for creating Orion entities before first publication.

Operator supplies entity ids and types directly as command-line
``entity_id:entity_type`` specs; no metadata CSV is read. The default is
dry-run. Pass ``--send`` to create entities live.

Usage (dry-run to inspect):
    uv run python scripts/create_entities.py \
      jp.sendai.Blesensor.per3600.101:Blesensor.per3600

Usage (live):
    uv run python scripts/create_entities.py --send \
      jp.sendai.Blesensor.per3600.101:Blesensor.per3600
"""

import argparse
import sys
from collections.abc import Sequence
from dataclasses import replace

from dotenv import find_dotenv, load_dotenv

from sendai_pipeline.auth import AuthClient, AuthSettings
from sendai_pipeline.create_entities import (
    CreateEntitiesSettings,
    create_entities,
    parse_entity_specs,
)
from sendai_pipeline.logging_setup import LoggingSettings, configure_logging


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    """Parse command-line arguments for the one-shot create helper."""
    parser = argparse.ArgumentParser(
        description="Create Orion entities from explicit entity_id:entity_type specs.",
    )
    parser.add_argument(
        "entity_specs",
        nargs="+",
        metavar="ENTITY_ID:ENTITY_TYPE",
        help="Entity spec to create. Repeat or separate multiple specs with commas.",
    )
    parser.add_argument(
        "--send",
        action="store_true",
        help="Create entities live. Omit for dry-run.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the create-entities entry point and return a process exit code."""
    load_dotenv(find_dotenv(usecwd=True))

    logging_settings = LoggingSettings.from_env()
    configure_logging(logging_settings, product="create_entities")

    args = _parse_args(argv)
    entities = parse_entity_specs(args.entity_specs)
    settings = replace(
        CreateEntitiesSettings.from_env(),
        entities=entities,
        dry_run=not args.send,
    )

    # Auth credentials are not needed for dry-run; defer construction so a
    # safe inspection run does not require FIWARE_CONSUMER_KEY/SECRET.
    auth = AuthClient(AuthSettings.from_env()) if not settings.dry_run else None

    result = create_entities(settings.entities, settings=settings, auth=auth)
    return result.exit_code


if __name__ == "__main__":
    sys.exit(main())
