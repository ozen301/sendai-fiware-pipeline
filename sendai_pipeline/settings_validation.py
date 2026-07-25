"""Validation and default handling for environment-backed settings."""

import unicodedata
from collections.abc import Mapping


def optional_env(
    env: Mapping[str, str],
    key: str,
    default: str,
) -> str:
    """Return the environment value, treating only missing or empty as unset.

    Empty ``KEY=`` placeholders in ``.env`` files use the documented default.
    Non-empty values are returned unchanged, including surrounding whitespace;
    callers may normalize them after reading.
    """
    value = env.get(key)
    if value is None or value == "":
        return default
    return value


def parse_int_env(
    env: Mapping[str, str],
    key: str,
    default: int,
    error_type: type[Exception],
) -> int:
    """Parse an optional integer, raising the supplied configuration error."""
    value = optional_env(env, key, "")
    if value == "":
        return default
    try:
        return int(value)
    except ValueError as exc:
        raise error_type(f"environment variable must be an integer: {key}") from exc


def validate_non_negative_settings(
    values: Mapping[str, int],
    error_type: type[Exception],
) -> None:
    """Reject negative integer settings with their environment names."""
    for key, value in values.items():
        if value < 0:
            raise error_type(f"environment variable must be non-negative: {key}")


def validate_lookback_ceiling(
    name: str,
    reprocess_hours: int,
    max_hours: int,
    error_type: type[Exception],
) -> None:
    """Reject a maximum lookback below its matching reprocess floor."""
    if max_hours < reprocess_hours:
        raise error_type(f"MAX_LOOKBACK_HOURS_{name} must be >= REPROCESS_HOURS_{name}")


def parse_exact_env_value(
    env: Mapping[str, str],
    key: str,
    default: str,
    error_type: type[Exception],
) -> str:
    """Return an exact nonempty environment value or its default.

    Args:
        env: Environment-style mapping to read.
        key: Variable name used for lookup and error reporting.
        default: Value returned when the variable is absent.
        error_type: Configuration exception type raised for invalid values.

    Returns:
        The configured value unchanged, or ``default`` when unset.

    Raises:
        Exception: The supplied configuration exception when the value is empty,
            has surrounding whitespace, or contains a control character.
    """
    return validate_exact_value(env.get(key), key, default, error_type)


def validate_exact_value(
    value: str | None,
    key: str,
    default: str,
    error_type: type[Exception],
) -> str:
    """Validate an already-fetched environment value; return it or the default.

    Same rules as :func:`parse_exact_env_value`, but takes the raw value
    directly instead of a mapping and key. This lets a caller defer validation
    to the point of use — for example, validating a Product-B-only setting only
    when a Product B operation actually needs it, so a malformed value never
    fails an unrelated Product A operation.

    Args:
        value: The already-fetched value, or ``None`` when the variable is
            unset.
        key: Variable name used only for error reporting.
        default: Value returned when *value* is ``None``.
        error_type: Configuration exception type raised for invalid values.

    Returns:
        *value* unchanged, or *default* when *value* is ``None``.

    Raises:
        Exception: The supplied configuration exception when the value is empty,
            has surrounding whitespace, or contains a control character.
    """
    if value is None:
        return default
    if value == "":
        raise error_type(f"environment variable must be nonempty: {key}")
    if value != value.strip():
        raise error_type(
            f"environment variable must not have surrounding whitespace: {key}"
        )
    if any(unicodedata.category(character) == "Cc" for character in value):
        raise error_type(
            f"environment variable must not contain control characters: {key}"
        )
    return value
