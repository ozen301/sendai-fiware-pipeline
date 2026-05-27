"""Read-only diagnostic for retained pipeline window state."""

import argparse
import sys
from collections.abc import Sequence
from pathlib import Path

from dotenv import find_dotenv, load_dotenv

from sendai_pipeline.state_tools import (
    PRODUCT_STATE_PATHS,
    diagnose_state,
    diagnoses_to_json,
    load_product_state,
)


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Report pending and partial pipeline state windows.",
    )
    parser.add_argument("product", choices=("flow", "direction"))
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the read-only state doctor."""
    load_dotenv(find_dotenv(usecwd=True))
    args = _parse_args(argv)
    product = args.product
    try:
        state_path = PRODUCT_STATE_PATHS[product]
        mtime_before = _mtime_ns(state_path)
        store = load_product_state(product)
        diagnoses = diagnose_state(store, product=product)
        mtime_after = _mtime_ns(state_path)
        if mtime_before != mtime_after:
            print("WARNING: state file changed during doctor read", file=sys.stderr)
        sys.stdout.write(diagnoses_to_json(diagnoses) + "\n")
        return 0
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


def _mtime_ns(path: Path) -> int | None:
    try:
        return path.stat().st_mtime_ns
    except FileNotFoundError:
        return None


if __name__ == "__main__":
    sys.exit(main())
