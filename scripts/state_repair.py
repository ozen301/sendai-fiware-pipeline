"""Repair explicitly selected pipeline state windows."""

import argparse
import json
import sys
from collections.abc import Sequence

from dotenv import find_dotenv, load_dotenv

from sendai_pipeline.state_tools import repair_state


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Dry-run or apply repairs to retained pipeline state windows.",
    )
    parser.add_argument("product", choices=("flow", "direction"))
    parser.add_argument(
        "--window",
        action="append",
        required=True,
        help="Window key to repair. Repeat for multiple windows.",
    )
    parser.add_argument(
        "--action",
        choices=("recompute_complete", "dead_letter"),
        required=True,
    )
    parser.add_argument(
        "--expected-target-id",
        action="append",
        help="Explicit expected entity id for legacy recompute repair.",
    )
    parser.add_argument("--reason", help="Required for dead_letter.")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually mutate state. Omit for dry-run.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the state repair helper."""
    load_dotenv(find_dotenv(usecwd=True))
    args = _parse_args(argv)
    try:
        result = repair_state(
            product=args.product,
            window_keys=args.window,
            action=args.action,
            reason=args.reason,
            expected_target_ids=args.expected_target_id,
            apply=args.apply,
        )
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
                "reason": change.reason,
            }
            for change in result.changes
        ],
    }
    sys.stdout.write(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
