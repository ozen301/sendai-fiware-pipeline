"""Migrate retained flow state to recorded target expectations."""

import argparse
import json
import sys
from collections.abc import Sequence

from dotenv import find_dotenv, load_dotenv

from sendai_pipeline.state_tools import migrate_flow_state


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Dry-run or apply the retained flow state migration.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually mutate flow state. Omit for dry-run.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the flow state migration helper."""
    load_dotenv(find_dotenv(usecwd=True))
    args = _parse_args(argv)
    try:
        result = migrate_flow_state(apply=args.apply)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    payload = {
        "product": result.product,
        "dry_run": result.dry_run,
        "backup_path": str(result.backup_path) if result.backup_path else None,
        "changes": [
            {
                "window": change.window_key,
                "action": change.action,
                "before_status": change.before_status,
                "after_status": change.after_status,
            }
            for change in result.changes
        ],
    }
    sys.stdout.write(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
