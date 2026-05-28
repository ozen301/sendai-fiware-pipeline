"""Read-only diagnostic for retained pipeline window state."""

import argparse
import os
import sys
from collections.abc import Sequence
from pathlib import Path

from dotenv import find_dotenv, load_dotenv

from sendai_pipeline.state_tools import (
    PRODUCT_STATE_PATHS,
    build_state_report,
    load_product_state,
    state_report_to_json,
    state_report_to_pretty,
    try_load_sensor_labels,
)

_DEFAULT_METADATA_PATH = Path("metadata/sensors.csv")
_DEFAULT_WINDOW_LIMIT = 30


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Report pending and partial pipeline state windows.",
    )
    parser.add_argument("product", choices=("flow", "direction"))
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="render a human-readable dashboard instead of JSON",
    )
    parser.add_argument(
        "--top",
        type=_positive_int,
        default=20,
        help="ranked target rows to show in pretty output",
    )
    parser.add_argument(
        "--window-limit",
        type=_positive_int,
        default=_DEFAULT_WINDOW_LIMIT,
        help="open-window rows to show in pretty output",
    )
    parser.add_argument(
        "--window-sensor-limit",
        type=_positive_int,
        default=8,
        help="target labels to show per open window in pretty output",
    )
    parser.add_argument(
        "--ascii",
        action="store_true",
        help="use plain ASCII bars in pretty output",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="show all pretty dashboard table rows",
    )
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
        report = build_state_report(store, product=product)
        mtime_after = _mtime_ns(state_path)
        if mtime_before != mtime_after:
            print("WARNING: state file changed during doctor read", file=sys.stderr)
        if args.pretty:
            sys.stdout.write(
                state_report_to_pretty(
                    report,
                    state_path=state_path,
                    state_size_bytes=_size_bytes(state_path),
                    sensor_labels=try_load_sensor_labels(_metadata_path_from_env()),
                    top=None if args.all else args.top,
                    window_limit=None if args.all else args.window_limit,
                    window_sensor_limit=args.window_sensor_limit,
                    ascii_only=bool(args.ascii),
                )
                + "\n"
            )
        else:
            sys.stdout.write(state_report_to_json(report) + "\n")
        return 0
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a positive integer") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def _metadata_path_from_env() -> Path:
    value = os.environ.get("SENSOR_METADATA_PATH")
    if value is None or value == "":
        return _DEFAULT_METADATA_PATH
    return Path(value)


def _mtime_ns(path: Path) -> int | None:
    try:
        return path.stat().st_mtime_ns
    except FileNotFoundError:
        return None


def _size_bytes(path: Path) -> int | None:
    try:
        return path.stat().st_size
    except FileNotFoundError:
        return None


if __name__ == "__main__":
    sys.exit(main())
