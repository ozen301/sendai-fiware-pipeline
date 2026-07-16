"""Shared validation for environment-backed settings."""

import unicodedata
from collections.abc import Mapping


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
