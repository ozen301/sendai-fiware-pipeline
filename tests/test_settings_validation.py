import pytest

from sendai_pipeline.settings_validation import (
    optional_env,
    parse_int_env,
    validate_lookback_ceiling,
    validate_non_negative_settings,
)


class ConfigError(RuntimeError):
    pass


@pytest.mark.parametrize("env", [{}, {"SETTING": ""}])
def test_optional_env_missing_or_empty_returns_default(env: dict[str, str]) -> None:
    assert optional_env(env, "SETTING", "fallback") == "fallback"


@pytest.mark.parametrize("value", ["configured", "  "])
def test_optional_env_non_empty_preserves_value(value: str) -> None:
    assert optional_env({"SETTING": value}, "SETTING", "fallback") == value


def test_parse_int_env_returns_default_or_configured_integer() -> None:
    assert parse_int_env({}, "COUNT", 3, ConfigError) == 3
    assert parse_int_env({"COUNT": "7"}, "COUNT", 3, ConfigError) == 7


def test_parse_int_env_raises_supplied_error_type() -> None:
    with pytest.raises(ConfigError, match="must be an integer: COUNT"):
        parse_int_env({"COUNT": "seven"}, "COUNT", 3, ConfigError)


def test_validate_non_negative_settings_names_invalid_setting() -> None:
    with pytest.raises(ConfigError, match="must be non-negative: HOURS"):
        validate_non_negative_settings({"HOURS": -1}, ConfigError)


def test_validate_lookback_ceiling_names_invalid_interval() -> None:
    with pytest.raises(
        ConfigError,
        match="MAX_LOOKBACK_HOURS_PER3600 must be >= REPROCESS_HOURS_PER3600",
    ):
        validate_lookback_ceiling("PER3600", 12, 10, ConfigError)
