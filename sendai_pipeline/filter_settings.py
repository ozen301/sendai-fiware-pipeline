"""Filter configuration for pipeline row selection."""

import os
from collections.abc import Iterable, Mapping
from dataclasses import dataclass

_DEFAULT_IGNORED_PLACE_PREFIXES: tuple[str, ...] = ("quick.", "test")
_DEFAULT_SOURCE_MAX_IMPUTATION_TIER = 2


class FilterConfigError(RuntimeError):
    """Raised when filter configuration is invalid."""


@dataclass(frozen=True)
class FilterSettings:
    """Configuration controlling which metadata and places are processed.

    Attributes:
        target_flow_batches: Metadata batch identifiers to include for Product
            A flow publishing. An empty set means no batches are eligible — the
            caller short-circuits to a no-op rather than publishing everything.
        target_direction_batches: Metadata batch identifiers to include for
            Product B direction publishing. An empty set means no batches are
            eligible — the caller short-circuits to a no-op rather than
            publishing everything.
        ignored_place_prefixes: Place ID prefixes the transform drops
            silently before metadata lookup.
        source_max_imputation_tier: Highest Product A imputation tier allowed
            when reading the source table.
    """

    target_flow_batches: frozenset[str]
    target_direction_batches: frozenset[str]
    ignored_place_prefixes: tuple[str, ...]
    source_max_imputation_tier: int = _DEFAULT_SOURCE_MAX_IMPUTATION_TIER

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "FilterSettings":
        """Build filter settings from environment variables."""
        values = os.environ if env is None else env
        return cls(
            target_flow_batches=_parse_batch_set(values.get("TARGET_FLOW_BATCHES")),
            target_direction_batches=_parse_batch_set(
                values.get("TARGET_DIRECTION_BATCHES")
            ),
            ignored_place_prefixes=_parse_ignored_place_prefixes(
                values.get("IGNORED_PLACE_PREFIXES")
            ),
            source_max_imputation_tier=_parse_source_max_imputation_tier(
                values.get("SOURCE_MAX_IMPUTATION_TIER")
            ),
        )

    def validate_target_flow_batches(self, metadata_batches: Iterable[str]) -> None:
        """Raise if configured flow target batches are absent from metadata."""
        _validate_batch_set(
            self.target_flow_batches,
            metadata_batches,
            variable_name="TARGET_FLOW_BATCHES",
        )

    def validate_target_direction_batches(
        self, metadata_batches: Iterable[str]
    ) -> None:
        """Raise if configured direction target batches are absent from metadata."""
        _validate_batch_set(
            self.target_direction_batches,
            metadata_batches,
            variable_name="TARGET_DIRECTION_BATCHES",
        )


def _validate_batch_set(
    target_batches: frozenset[str],
    metadata_batches: Iterable[str],
    *,
    variable_name: str,
) -> None:
    """Raise if configured target batches are absent from metadata."""
    unknown = sorted(target_batches - set(metadata_batches))
    if unknown:
        raise FilterConfigError(
            f"Unknown {variable_name} entries: {', '.join(unknown)}"
        )


def _parse_ignored_place_prefixes(value: str | None) -> tuple[str, ...]:
    """Parse the ignored place prefix environment value."""
    if value is None or value == "":
        return _DEFAULT_IGNORED_PLACE_PREFIXES
    if value == ",":
        return ()

    return tuple(
        entry for entry in (part.strip() for part in value.split(",")) if entry
    )


def _parse_batch_set(value: str | None) -> frozenset[str]:
    """Parse a target batch environment value."""
    if value is None or value == "":
        return frozenset()

    return frozenset(
        entry for entry in (part.strip() for part in value.split(",")) if entry
    )


def _parse_source_max_imputation_tier(value: str | None) -> int:
    """Parse the Product A source imputation-tier ceiling."""
    if value is None or value == "":
        return _DEFAULT_SOURCE_MAX_IMPUTATION_TIER

    try:
        parsed = int(value)
    except ValueError as exc:
        raise FilterConfigError(
            "environment variable must be an integer: SOURCE_MAX_IMPUTATION_TIER"
        ) from exc

    if parsed < 0:
        raise FilterConfigError(
            "environment variable must be non-negative: SOURCE_MAX_IMPUTATION_TIER"
        )
    return parsed
