"""MySQL access helpers for the Sendai FIWARE pipeline."""

import logging
import os
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any, Self

import pymysql
import pymysql.connections
import pymysql.cursors

logger = logging.getLogger(__name__)

FLOW_METRICS_TABLE: str = "flow_metrics2_per_place2_agg_imputed"
DIRECTION_METRICS_TABLE: str = "direction_metrics2_per_place2_agg"

_FLOW_METRICS_SQL = f"""
SELECT startdate, group_place_id, device_type, interval_min,
       flow_gt_m60, flow_gt_m80, flow_gt_m120,
       stay_gt_m60, stay_gt_m80
FROM {FLOW_METRICS_TABLE}
WHERE interval_min = %s
  AND startdate >= %s
  AND startdate <= %s
  AND imputation_tier <= %s
ORDER BY startdate, group_place_id
"""

_FLOW_METRICS_FOR_STARTDATES_SQL_TEMPLATE = f"""
SELECT startdate, group_place_id, device_type, interval_min,
       flow_gt_m60, flow_gt_m80, flow_gt_m120,
       stay_gt_m60, stay_gt_m80
FROM {FLOW_METRICS_TABLE}
WHERE interval_min = %s
  AND startdate IN ({{placeholders}})
  AND imputation_tier <= %s
ORDER BY startdate, group_place_id
"""

_DIRECTION_METRICS_SQL = f"""
SELECT startdate, from_group_place_id, to_group_place_id,
       from_device_type, to_device_type, interval_min, count
FROM {DIRECTION_METRICS_TABLE}
WHERE interval_min = %s
  AND startdate >= %s
  AND startdate <= %s
ORDER BY startdate
"""


class DbError(RuntimeError):
    """Base class for database-related failures."""


class DbConfigError(DbError):
    """Raised when required database configuration is missing or invalid."""


def is_connection_lost_error(exc: BaseException) -> bool:
    """Return whether a PyMySQL exception means the connection was lost.

    Args:
        exc: Exception raised by a database operation.

    Returns:
        ``True`` for PyMySQL closed-socket errors, server-gone errors, and
        server-lost errors; ``False`` for all other exceptions.
    """
    if isinstance(exc, pymysql.err.InterfaceError):
        return True
    if not isinstance(exc, pymysql.err.OperationalError):
        return False
    code = exc.args[0] if exc.args else None
    return code in {2006, 2013}


@dataclass
class DbSettings:
    """Configuration for MySQL database connections."""

    host: str
    user: str
    password: str
    database: str
    port: int = 3306
    connect_timeout: int = 10
    read_timeout: int = 30
    charset: str = "utf8mb4"

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> Self:
        """Build database settings from environment variables."""
        values = os.environ if env is None else env
        return cls(
            host=_required_env(values, "MYSQL_HOST"),
            user=_required_env(values, "MYSQL_USER"),
            password=_required_env(values, "MYSQL_PASSWORD"),
            database=_required_env(values, "MYSQL_DATABASE"),
            port=_optional_int_env(values, "MYSQL_PORT", 3306),
            connect_timeout=_optional_int_env(
                values,
                "MYSQL_CONNECT_TIMEOUT",
                10,
            ),
            read_timeout=_optional_int_env(values, "MYSQL_READ_TIMEOUT", 30),
            charset=_optional_env(values, "MYSQL_CHARSET", "utf8mb4"),
        )


def connect(settings: DbSettings) -> pymysql.connections.Connection:
    """Open a MySQL connection using the resolved database settings."""
    return pymysql.connect(
        host=settings.host,
        user=settings.user,
        password=settings.password,
        database=settings.database,
        port=settings.port,
        connect_timeout=settings.connect_timeout,
        read_timeout=settings.read_timeout,
        charset=settings.charset,
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )


def select_flow_metrics(
    connection: Any,
    *,
    interval_min: int,
    lower_bound: str,
    upper_bound: str,
    max_imputation_tier: int,
) -> list[dict[str, Any]]:
    """Return flow metric rows for an inclusive time window."""
    with connection.cursor() as cursor:
        cursor.execute(
            _FLOW_METRICS_SQL,
            (interval_min, lower_bound, upper_bound, max_imputation_tier),
        )
        return cursor.fetchall()


def select_flow_metrics_for_startdates(
    connection: Any,
    *,
    interval_min: int,
    startdates: Iterable[str],
    max_imputation_tier: int,
) -> list[dict[str, Any]]:
    """Return flow metric rows for exact source-window start dates."""
    startdate_values = tuple(startdates)
    if not startdate_values:
        return []

    placeholders = ", ".join("%s" for _ in startdate_values)
    sql = _FLOW_METRICS_FOR_STARTDATES_SQL_TEMPLATE.format(placeholders=placeholders)
    with connection.cursor() as cursor:
        cursor.execute(
            sql,
            (interval_min, *startdate_values, max_imputation_tier),
        )
        return cursor.fetchall()


def select_direction_metrics(
    connection: Any,
    *,
    interval_min: int,
    lower_bound: str,
    upper_bound: str,
) -> list[dict[str, Any]]:
    """Return direction metric rows for an inclusive time window."""
    with connection.cursor() as cursor:
        cursor.execute(
            _DIRECTION_METRICS_SQL,
            (interval_min, lower_bound, upper_bound),
        )
        return cursor.fetchall()


def _required_env(env: Mapping[str, str], key: str) -> str:
    """Return a required environment value or raise a config error."""
    value = env.get(key)
    if value is None or value == "":
        raise DbConfigError(f"missing required environment variable: {key}")
    return value


def _optional_env(env: Mapping[str, str], key: str, default: str) -> str:
    """Return the env value if set and non-empty, otherwise the default."""
    value = env.get(key)
    if value is None or value == "":
        return default
    return value


def _optional_int_env(env: Mapping[str, str], key: str, default: int) -> int:
    """Return an optional integer environment value."""
    value = _optional_env(env, key, "")
    if value == "":
        return default

    try:
        return int(value)
    except ValueError as exc:
        raise DbConfigError(f"environment variable must be an integer: {key}") from exc
