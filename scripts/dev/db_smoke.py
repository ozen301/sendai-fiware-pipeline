"""Dev probe: one-shot sanity check of the source MySQL tables.

Prints row counts, distinct ``interval_min`` values, distinct
``group_place_id`` counts (with a small sample), ``startdate`` min/max
boundaries, and a count of the ``'ALL'``-keyed rows in the direction table.
This script exists to verify the assumptions in the spec against real data
once at setup time — it is operator-run, not part of CI or any cron job.

Usage from project root:

    uv run python scripts/dev/db_smoke.py

Requires ``MYSQL_*`` environment variables in ``.env``. Read-only: issues
only ``SELECT`` queries.
"""

from typing import Any

from dotenv import load_dotenv

from sendai_pipeline.db import (
    DIRECTION_METRICS_TABLE,
    FLOW_METRICS_TABLE,
    DbSettings,
    connect,
)

SAMPLE_LIMIT = 5


def _scalar(connection: Any, sql: str) -> Any:
    with connection.cursor() as cursor:
        cursor.execute(sql)
        row = cursor.fetchone()
    if row is None:
        return None
    return next(iter(row.values()))


def _rows(connection: Any, sql: str) -> list[dict[str, Any]]:
    with connection.cursor() as cursor:
        cursor.execute(sql)
        return list(cursor.fetchall())


def _print_table_summary(
    connection: Any,
    table: str,
    place_columns: tuple[str, ...],
) -> None:
    print(f"\n=== {table} ===")

    total = _scalar(connection, f"SELECT COUNT(*) AS n FROM {table}")
    print(f"  row_count            : {total:,}")

    intervals = _rows(
        connection,
        f"SELECT DISTINCT interval_min FROM {table} ORDER BY interval_min",
    )
    print(f"  distinct interval_min: {[row['interval_min'] for row in intervals]}")

    bounds = _rows(
        connection,
        f"SELECT MIN(startdate) AS min_sd, MAX(startdate) AS max_sd FROM {table}",
    )
    if bounds:
        print(f"  startdate range      : {bounds[0]['min_sd']} → {bounds[0]['max_sd']}")

    for column in place_columns:
        distinct_count = _scalar(
            connection,
            f"SELECT COUNT(DISTINCT {column}) AS n FROM {table}",
        )
        sample = _rows(
            connection,
            (
                f"SELECT DISTINCT {column} AS gpid FROM {table} "
                f"ORDER BY {column} LIMIT {SAMPLE_LIMIT}"
            ),
        )
        sample_values = [row["gpid"] for row in sample]
        print(f"  distinct {column:<20s}: {distinct_count:,} (sample: {sample_values})")


def _print_direction_all_summary(connection: Any) -> None:
    all_count = _scalar(
        connection,
        (
            f"SELECT COUNT(*) AS n FROM {DIRECTION_METRICS_TABLE} "
            "WHERE from_group_place_id = 'ALL' OR to_group_place_id = 'ALL'"
        ),
    )
    print(f"  'ALL'-keyed row count: {all_count:,}")

    sample = _rows(
        connection,
        (
            "SELECT startdate, from_group_place_id, to_group_place_id, "
            "from_device_type, to_device_type, count "
            f"FROM {DIRECTION_METRICS_TABLE} "
            "WHERE from_group_place_id = 'ALL' OR to_group_place_id = 'ALL' "
            f"ORDER BY startdate DESC LIMIT {SAMPLE_LIMIT}"
        ),
    )
    if sample:
        print("  'ALL' sample rows    :")
        for row in sample:
            print(f"    {row}")


def main() -> None:
    """Run the smoke probe and print a human-readable summary."""
    load_dotenv()
    settings = DbSettings.from_env()
    print(
        f"Connecting to {settings.host}:{settings.port}/{settings.database} "
        f"as {settings.user}..."
    )
    connection = connect(settings)
    try:
        _print_table_summary(
            connection,
            FLOW_METRICS_TABLE,
            place_columns=("group_place_id",),
        )
        _print_table_summary(
            connection,
            DIRECTION_METRICS_TABLE,
            place_columns=("from_group_place_id", "to_group_place_id"),
        )
        _print_direction_all_summary(connection)
    finally:
        connection.close()


if __name__ == "__main__":
    main()
