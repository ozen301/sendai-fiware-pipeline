"""Dev probe: inspect ``'ALL'``-keyed rows in ``direction_metrics2_per_place2_agg``.

Run before implementing the Product B transform. Confirms three properties
the transform will depend on:

1. ``'ALL'`` is the literal aggregate key, with no batch prefix
   (no ``sendai2023.ALL``, no ``sendai202603.ALL``, no other variants).
2. ``'ALL'`` rows carry the same batch-specific device-type fields as
   pairwise rows: ``Pixel3aUT`` when the other side resolves to a 2023
   place (``sendai2023.*``), ``M5Stack`` when the other side resolves to
   a 2026 place (``sendai202603.*``).
3. The shape is identical across the 5-min and 60-min intervals.

Read-only: issues only ``SELECT`` queries. Operator-run, not part of CI.

Usage from project root:

    uv run python scripts/dev/inspect_direction_all_rows.py
"""

from typing import Any

from dotenv import load_dotenv

from sendai_pipeline.db import (
    DIRECTION_METRICS_TABLE,
    DbSettings,
    connect,
)

INTERVALS: tuple[int, ...] = (5, 60)
BATCHES: tuple[tuple[str, str, str], ...] = (
    ("2023", "sendai2023.", "Pixel3aUT"),
    ("2026", "sendai202603.", "M5Stack"),
)
SAMPLE_LIMIT = 5


def _rows(
    connection: Any, sql: str, params: tuple[Any, ...] | None = None
) -> list[dict[str, Any]]:
    with connection.cursor() as cursor:
        if params is None:
            cursor.execute(sql)
        else:
            cursor.execute(sql, params)
        return list(cursor.fetchall())


def _scalar(connection: Any, sql: str, params: tuple[Any, ...] | None = None) -> Any:
    rows = _rows(connection, sql, params)
    if not rows:
        return None
    return next(iter(rows[0].values()))


def _print_all_token_variants(connection: Any) -> None:
    """List every distinct value containing 'ALL' (case-insensitive) on either side."""
    print("\n=== distinct values containing 'ALL' (either side) ===")
    for column in ("from_group_place_id", "to_group_place_id"):
        variants = _rows(
            connection,
            (
                f"SELECT DISTINCT {column} AS value "
                f"FROM {DIRECTION_METRICS_TABLE} "
                f"WHERE UPPER({column}) LIKE '%ALL%' "
                f"ORDER BY {column}"
            ),
        )
        values = [row["value"] for row in variants]
        print(f"  {column}: {values}")


def _print_all_row_device_types(
    connection: Any,
    *,
    interval_min: int,
    batch_label: str,
    other_side_prefix: str,
) -> None:
    """For each `'ALL'` direction, count device-type pairings against a batch."""
    print(
        f"\n--- {batch_label} batch, interval_min={interval_min}, "
        f"other side prefix={other_side_prefix!r} ---"
    )

    queries: tuple[tuple[str, str, str], ...] = (
        (
            "from='ALL', to resolves to place N (powers peopleCount_flow.from.all)",
            "from_group_place_id",
            "to_group_place_id",
        ),
        (
            "to='ALL', from resolves to place N (powers peopleCount_flow.to.all)",
            "to_group_place_id",
            "from_group_place_id",
        ),
    )

    for label, all_side, other_side in queries:
        rows = _rows(
            connection,
            (
                "SELECT from_device_type, to_device_type, COUNT(*) AS n "
                f"FROM {DIRECTION_METRICS_TABLE} "
                f"WHERE {all_side} = 'ALL' "
                f"  AND {other_side} LIKE %s "
                "  AND interval_min = %s "
                "GROUP BY from_device_type, to_device_type "
                "ORDER BY n DESC"
            ),
            (f"{other_side_prefix}%", interval_min),
        )
        print(f"  {label}:")
        if not rows:
            print("    (no rows)")
            continue
        for row in rows:
            print(
                f"    from_device_type={row['from_device_type']!r:>12s}, "
                f"to_device_type={row['to_device_type']!r:>12s}, "
                f"n={row['n']:,}"
            )


def _print_all_row_samples(
    connection: Any,
    *,
    interval_min: int,
    batch_label: str,
    other_side_prefix: str,
) -> None:
    """Print a few raw `'ALL'`-keyed rows for visual shape inspection."""
    print(
        f"\n--- sample rows: {batch_label}, interval_min={interval_min}, "
        f"other side {other_side_prefix!r} ---"
    )
    rows = _rows(
        connection,
        (
            "SELECT startdate, from_group_place_id, to_group_place_id, "
            "from_device_type, to_device_type, count "
            f"FROM {DIRECTION_METRICS_TABLE} "
            "WHERE (from_group_place_id = 'ALL' OR to_group_place_id = 'ALL') "
            "  AND (from_group_place_id LIKE %s OR to_group_place_id LIKE %s) "
            "  AND interval_min = %s "
            f"ORDER BY startdate DESC LIMIT {SAMPLE_LIMIT}"
        ),
        (f"{other_side_prefix}%", f"{other_side_prefix}%", interval_min),
    )
    if not rows:
        print("  (no rows)")
        return
    for row in rows:
        print(f"  {row}")


def _print_shape_parity(connection: Any) -> None:
    """Compare counts and device-type makeup across the 5-min and 60-min intervals."""
    print("\n=== shape parity: 5-min vs 60-min ===")
    counts: dict[int, int] = {}
    for interval_min in INTERVALS:
        n = _scalar(
            connection,
            (
                f"SELECT COUNT(*) FROM {DIRECTION_METRICS_TABLE} "
                "WHERE (from_group_place_id = 'ALL' OR to_group_place_id = 'ALL') "
                "  AND interval_min = %s"
            ),
            (interval_min,),
        )
        counts[interval_min] = int(n or 0)
        print(
            f"  interval_min={interval_min}: {counts[interval_min]:,} 'ALL'-keyed rows"
        )

    print("  per-interval device-type pair distribution on 'ALL' rows:")
    for interval_min in INTERVALS:
        rows = _rows(
            connection,
            (
                "SELECT from_device_type, to_device_type, COUNT(*) AS n "
                f"FROM {DIRECTION_METRICS_TABLE} "
                "WHERE (from_group_place_id = 'ALL' OR to_group_place_id = 'ALL') "
                "  AND interval_min = %s "
                "GROUP BY from_device_type, to_device_type "
                "ORDER BY n DESC"
            ),
            (interval_min,),
        )
        print(f"    interval_min={interval_min}:")
        for row in rows:
            print(
                f"      ({row['from_device_type']!r}, {row['to_device_type']!r}) "
                f"-> {row['n']:,}"
            )


def main() -> None:
    """Run the probe and print a human-readable summary."""
    load_dotenv()
    settings = DbSettings.from_env()
    print(
        f"Connecting to {settings.host}:{settings.port}/{settings.database} "
        f"as {settings.user}..."
    )
    connection = connect(settings)
    try:
        _print_all_token_variants(connection)

        for interval_min in INTERVALS:
            for batch_label, prefix, _expected_dev_type in BATCHES:
                _print_all_row_device_types(
                    connection,
                    interval_min=interval_min,
                    batch_label=batch_label,
                    other_side_prefix=prefix,
                )

        for interval_min in INTERVALS:
            for batch_label, prefix, _expected_dev_type in BATCHES:
                _print_all_row_samples(
                    connection,
                    interval_min=interval_min,
                    batch_label=batch_label,
                    other_side_prefix=prefix,
                )

        _print_shape_parity(connection)
    finally:
        connection.close()


if __name__ == "__main__":
    main()
