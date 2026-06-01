from typing import Any

import pymysql
import pymysql.cursors
import pytest

from sendai_pipeline.db import (
    DIRECTION_METRICS_TABLE,
    FLOW_METRICS_TABLE,
    DbConfigError,
    DbSettings,
    connect,
    select_direction_metrics,
    select_flow_metrics,
    select_flow_metrics_for_startdates,
)

EXPECTED_FLOW_SQL = " ".join(
    """
    SELECT startdate, group_place_id, device_type, interval_min,
           flow_gt_m60, flow_gt_m80, flow_gt_m120,
           stay_gt_m60, stay_gt_m80
    FROM flow_metrics2_per_place2_agg_imputed
    WHERE interval_min = %s
      AND startdate >= %s
      AND startdate <= %s
      AND imputation_tier <= %s
    ORDER BY startdate, group_place_id
    """.split()
)

EXPECTED_FLOW_STARTDATES_SQL = " ".join(
    """
    SELECT startdate, group_place_id, device_type, interval_min,
           flow_gt_m60, flow_gt_m80, flow_gt_m120,
           stay_gt_m60, stay_gt_m80
    FROM flow_metrics2_per_place2_agg_imputed
    WHERE interval_min = %s
      AND startdate IN (%s, %s, %s)
      AND imputation_tier <= %s
    ORDER BY startdate, group_place_id
    """.split()
)

EXPECTED_DIRECTION_SQL = " ".join(
    """
    SELECT startdate, from_group_place_id, to_group_place_id,
           from_device_type, to_device_type, interval_min, count
    FROM direction_metrics2_per_place2_agg
    WHERE interval_min = %s
      AND startdate >= %s
      AND startdate <= %s
    ORDER BY startdate
    """.split()
)


def _env() -> dict[str, str]:
    return {
        "MYSQL_HOST": "db.example.test",
        "MYSQL_USER": "reader",
        "MYSQL_PASSWORD": "secret",
        "MYSQL_DATABASE": "bleData2025d",
    }


def _normalize_sql(sql: str) -> str:
    return " ".join(sql.split()).rstrip(";")


def test_table_constants_match_source_schema() -> None:
    assert FLOW_METRICS_TABLE == "flow_metrics2_per_place2_agg_imputed"
    assert DIRECTION_METRICS_TABLE == "direction_metrics2_per_place2_agg"


def test_from_env_uses_required_values() -> None:
    settings = DbSettings.from_env(_env())
    assert settings.host == "db.example.test"
    assert settings.user == "reader"
    assert settings.password == "secret"
    assert settings.database == "bleData2025d"


def test_from_env_defaults_for_optional_fields() -> None:
    settings = DbSettings.from_env(_env())
    assert settings.port == 3306
    assert settings.connect_timeout == 10
    assert settings.read_timeout == 30
    assert settings.charset == "utf8mb4"


def test_from_env_overrides_optional_fields() -> None:
    env = _env() | {
        "MYSQL_PORT": "3307",
        "MYSQL_CONNECT_TIMEOUT": "5",
        "MYSQL_READ_TIMEOUT": "45",
        "MYSQL_CHARSET": "utf8",
    }
    settings = DbSettings.from_env(env)
    assert settings.port == 3307
    assert settings.connect_timeout == 5
    assert settings.read_timeout == 45
    assert settings.charset == "utf8"


@pytest.mark.parametrize(
    "key,default_value",
    [
        ("MYSQL_PORT", 3306),
        ("MYSQL_CONNECT_TIMEOUT", 10),
        ("MYSQL_READ_TIMEOUT", 30),
    ],
)
def test_from_env_treats_empty_optional_int_as_default(
    key: str, default_value: int
) -> None:
    env = _env() | {key: ""}
    settings = DbSettings.from_env(env)
    assert getattr(settings, key.removeprefix("MYSQL_").lower()) == default_value


def test_from_env_treats_empty_optional_charset_as_default() -> None:
    env = _env() | {"MYSQL_CHARSET": ""}
    settings = DbSettings.from_env(env)
    assert settings.charset == "utf8mb4"


def test_from_env_with_no_argument_reads_os_environ(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for key, value in _env().items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("MYSQL_PORT", "3399")

    settings = DbSettings.from_env()

    assert settings.host == "db.example.test"
    assert settings.port == 3399


@pytest.mark.parametrize(
    "missing",
    ["MYSQL_HOST", "MYSQL_USER", "MYSQL_PASSWORD", "MYSQL_DATABASE"],
)
def test_from_env_raises_when_required_missing(missing: str) -> None:
    env = _env()
    env.pop(missing)
    with pytest.raises(DbConfigError) as excinfo:
        DbSettings.from_env(env)
    assert missing in str(excinfo.value)


@pytest.mark.parametrize(
    "empty",
    ["MYSQL_HOST", "MYSQL_USER", "MYSQL_PASSWORD", "MYSQL_DATABASE"],
)
def test_from_env_raises_when_required_empty(empty: str) -> None:
    env = _env()
    env[empty] = ""
    with pytest.raises(DbConfigError) as excinfo:
        DbSettings.from_env(env)
    assert empty in str(excinfo.value)


@pytest.mark.parametrize(
    "key",
    ["MYSQL_PORT", "MYSQL_CONNECT_TIMEOUT", "MYSQL_READ_TIMEOUT"],
)
def test_from_env_raises_when_optional_int_is_not_numeric(key: str) -> None:
    env = _env() | {key: "abc"}
    with pytest.raises(DbConfigError) as excinfo:
        DbSettings.from_env(env)
    assert key in str(excinfo.value)


class _ConnectorCall:
    def __init__(self) -> None:
        self.kwargs: dict[str, Any] = {}
        self.sentinel: object = object()

    def __call__(self, **kwargs: Any) -> object:
        self.kwargs = kwargs
        return self.sentinel


def test_connect_passes_settings_to_pymysql(monkeypatch: pytest.MonkeyPatch) -> None:
    call = _ConnectorCall()
    monkeypatch.setattr("sendai_pipeline.db.pymysql.connect", call)

    settings = DbSettings(
        host="db.example.test",
        user="reader",
        password="secret",
        database="bleData2025d",
        port=3307,
        connect_timeout=5,
        read_timeout=45,
        charset="utf8",
    )
    result = connect(settings)

    assert result is call.sentinel
    assert call.kwargs["host"] == "db.example.test"
    assert call.kwargs["user"] == "reader"
    assert call.kwargs["password"] == "secret"
    assert call.kwargs["database"] == "bleData2025d"
    assert call.kwargs["port"] == 3307
    assert call.kwargs["connect_timeout"] == 5
    assert call.kwargs["read_timeout"] == 45
    assert call.kwargs["charset"] == "utf8"


def test_connect_uses_dict_cursor_for_named_columns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    call = _ConnectorCall()
    monkeypatch.setattr("sendai_pipeline.db.pymysql.connect", call)
    settings = DbSettings(host="h", user="u", password="p", database="d")

    connect(settings)

    assert call.kwargs["cursorclass"] is pymysql.cursors.DictCursor


def test_connect_enables_autocommit(monkeypatch: pytest.MonkeyPatch) -> None:
    call = _ConnectorCall()
    monkeypatch.setattr("sendai_pipeline.db.pymysql.connect", call)
    settings = DbSettings(host="h", user="u", password="p", database="d")

    connect(settings)

    assert call.kwargs["autocommit"] is True


class FakeCursor:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows
        self.executed: list[tuple[str, tuple[Any, ...]]] = []
        self.entered = False
        self.exited = False
        self.fetchall_calls = 0
        self.execute_error: Exception | None = None
        self.fetchall_error: Exception | None = None

    def __enter__(self) -> "FakeCursor":
        self.entered = True
        return self

    def __exit__(self, *_: Any) -> None:
        self.exited = True
        return None

    def execute(self, sql: str, params: tuple[Any, ...]) -> None:
        if self.execute_error is not None:
            raise self.execute_error
        self.executed.append((sql, params))

    def fetchall(self) -> list[dict[str, Any]]:
        self.fetchall_calls += 1
        if self.fetchall_error is not None:
            raise self.fetchall_error
        return self.rows


class FakeConnection:
    def __init__(self, rows: list[dict[str, Any]] | None = None) -> None:
        self._cursor = FakeCursor(rows or [])
        self.close_calls = 0
        self.cursor_calls = 0

    def cursor(self) -> FakeCursor:
        self.cursor_calls += 1
        return self._cursor

    def close(self) -> None:
        self.close_calls += 1


_FLOW_BOUNDS = {
    "interval_min": 60,
    "lower_bound": "20260513_0500",
    "upper_bound": "20260513_0700",
    "max_imputation_tier": 2,
}
_DIRECTION_BOUNDS = {
    "interval_min": 5,
    "lower_bound": "20260513_0655",
    "upper_bound": "20260513_0700",
}


def test_select_flow_metrics_emits_exact_sql_and_returns_rows() -> None:
    rows = [
        {
            "startdate": "20260513_0700",
            "group_place_id": "sendai2023.10",
            "device_type": "Pixel3aUT",
            "interval_min": 60,
            "flow_gt_m60": 1,
            "flow_gt_m80": 2,
            "flow_gt_m120": 3,
            "stay_gt_m60": 4,
            "stay_gt_m80": 5,
        }
    ]
    connection = FakeConnection(rows)

    result = select_flow_metrics(connection, **_FLOW_BOUNDS)  # type: ignore[arg-type]

    assert result == rows
    assert len(connection._cursor.executed) == 1
    sql, params = connection._cursor.executed[0]
    assert _normalize_sql(sql) == EXPECTED_FLOW_SQL
    assert params == (60, "20260513_0500", "20260513_0700", 2)


def test_select_flow_metrics_for_startdates_emits_in_clause_and_returns_rows() -> None:
    rows = [
        {
            "startdate": "20260513_0700",
            "group_place_id": "sendai2023.10",
            "device_type": "Pixel3aUT",
            "interval_min": 60,
            "flow_gt_m60": 1,
            "flow_gt_m80": 2,
            "flow_gt_m120": 3,
            "stay_gt_m60": 4,
            "stay_gt_m80": 5,
        }
    ]
    connection = FakeConnection(rows)

    result = select_flow_metrics_for_startdates(  # type: ignore[arg-type]
        connection,
        interval_min=60,
        startdates=("20260513_0500", "20260513_0600", "20260513_0700"),
        max_imputation_tier=2,
    )

    assert result == rows
    assert len(connection._cursor.executed) == 1
    sql, params = connection._cursor.executed[0]
    assert _normalize_sql(sql) == EXPECTED_FLOW_STARTDATES_SQL
    assert params == (
        60,
        "20260513_0500",
        "20260513_0600",
        "20260513_0700",
        2,
    )


def test_select_flow_metrics_for_startdates_empty_startdates_skips_cursor() -> None:
    connection = FakeConnection([{"startdate": "20260513_0700"}])

    result = select_flow_metrics_for_startdates(  # type: ignore[arg-type]
        connection,
        interval_min=60,
        startdates=[],
        max_imputation_tier=2,
    )

    assert result == []
    assert connection.cursor_calls == 0
    assert connection._cursor.executed == []
    assert connection._cursor.entered is False


def test_select_direction_metrics_emits_exact_sql_and_returns_rows() -> None:
    rows = [
        {
            "startdate": "20260513_0700",
            "from_group_place_id": "sendai202603.105",
            "to_group_place_id": "ALL",
            "from_device_type": "M5Stack",
            "to_device_type": "M5Stack",
            "count": 12,
        }
    ]
    connection = FakeConnection(rows)

    result = select_direction_metrics(connection, **_DIRECTION_BOUNDS)  # type: ignore[arg-type]

    assert result == rows
    assert len(connection._cursor.executed) == 1
    sql, params = connection._cursor.executed[0]
    assert _normalize_sql(sql) == EXPECTED_DIRECTION_SQL
    assert params == (5, "20260513_0655", "20260513_0700")


@pytest.fixture(
    params=[
        (select_flow_metrics, _FLOW_BOUNDS),
        (select_direction_metrics, _DIRECTION_BOUNDS),
    ]
)
def selector_with_bounds(request: pytest.FixtureRequest) -> tuple[Any, dict[str, Any]]:
    return request.param


def test_selector_returns_empty_list_when_no_rows(
    selector_with_bounds: tuple[Any, dict[str, Any]],
) -> None:
    selector, bounds = selector_with_bounds
    connection = FakeConnection([])
    result = selector(connection, **bounds)
    assert result == []


def test_selector_uses_cursor_context_and_calls_fetchall_once(
    selector_with_bounds: tuple[Any, dict[str, Any]],
) -> None:
    selector, bounds = selector_with_bounds
    connection = FakeConnection([{"startdate": "20260513_0700"}])

    selector(connection, **bounds)

    assert connection._cursor.entered is True
    assert connection._cursor.exited is True
    assert connection._cursor.fetchall_calls == 1


def test_selector_closes_cursor_when_execute_raises(
    selector_with_bounds: tuple[Any, dict[str, Any]],
) -> None:
    selector, bounds = selector_with_bounds
    connection = FakeConnection([])
    error = pymysql.err.OperationalError(2006, "server has gone away")
    connection._cursor.execute_error = error

    with pytest.raises(pymysql.err.OperationalError) as excinfo:
        selector(connection, **bounds)

    assert excinfo.value is error
    assert connection._cursor.entered is True
    assert connection._cursor.exited is True


def test_selector_closes_cursor_when_fetchall_raises(
    selector_with_bounds: tuple[Any, dict[str, Any]],
) -> None:
    selector, bounds = selector_with_bounds
    connection = FakeConnection([])
    error = pymysql.err.OperationalError(2013, "lost connection during query")
    connection._cursor.fetchall_error = error

    with pytest.raises(pymysql.err.OperationalError) as excinfo:
        selector(connection, **bounds)

    assert excinfo.value is error
    assert connection._cursor.exited is True


def test_selector_does_not_close_connection(
    selector_with_bounds: tuple[Any, dict[str, Any]],
) -> None:
    selector, bounds = selector_with_bounds
    connection = FakeConnection([])

    selector(connection, **bounds)

    assert connection.close_calls == 0
