"""Validate metadata-derived target entity ids against the live Orion broker.

The pipeline reads its target set from runtime metadata (see
:mod:`sendai_pipeline.metadata`) — not from Orion. This module checks the
metadata-derived set against the entities Orion actually has, so missing
targets surface in logs without failing the run. POSTs still go out for
missing targets: the platform-side 4xx is the authoritative signal that a
specific entity is wrong, and silently dropping would hide that.
"""

import logging
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from sendai_pipeline.metadata import SensorPlace
from sendai_pipeline.orion_client import OrionClient

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class EntityMapResult:
    """Outcome of comparing metadata targets against live Orion entities.

    Attributes:
        expected_by_type: Metadata-derived target ids, grouped by NGSI
            entity type.
        live_by_type: Ids returned by Orion for each queried type, in
            the same key set as ``expected_by_type``.
        missing_by_type: Ids present in metadata but absent from Orion,
            per type. POSTs still go out for these; the surfaced 4xx is
            the authoritative error.
        extra_by_type: Ids present in Orion but not in metadata. Reported
            for diagnostics only — the platform may legitimately host
            entities owned by other pipelines.
        truncated_types: Types whose live response returned exactly the
            requested ``list_limit``, meaning the live set may be
            incomplete and the comparison cannot be trusted.
    """

    expected_by_type: dict[str, frozenset[str]]
    live_by_type: dict[str, frozenset[str]]
    missing_by_type: dict[str, frozenset[str]]
    extra_by_type: dict[str, frozenset[str]]
    truncated_types: frozenset[str]

    @property
    def has_missing(self) -> bool:
        """Whether any metadata target is missing from Orion."""
        return any(missing for missing in self.missing_by_type.values())


def validate_targets(
    places: Iterable[SensorPlace],
    orion: OrionClient,
    *,
    list_limit: int = 1000,
) -> EntityMapResult:
    """Compare metadata-derived target ids against live Orion entities.

    Issues one ``GET /entities?type=<entity_type>&attrs=id`` per distinct
    ``entity_type`` in ``places`` and diffs the result against the
    metadata-derived expected set. Missing targets are logged at
    ``WARNING`` per id and reported in the returned result; the function
    never raises on missing targets so the caller can still POST and let
    the platform-side error surface.

    Args:
        places: Active metadata rows to validate. Callers typically pass
            the output of :func:`sendai_pipeline.metadata.active_places`.
        orion: Orion client used for the list calls. Does not need
            to be authenticated ahead of time — the client refreshes
            tokens internally.
        list_limit: Maximum results requested per ``list_entities`` call.
            A response containing exactly this many entries is treated
            as potentially truncated and logged as such.

    Returns:
        :class:`EntityMapResult` summarising expected, live, missing,
        extra, and truncated-type sets per NGSI type.
    """
    expected_by_type: dict[str, set[str]] = {}
    for place in places:
        expected_by_type.setdefault(place.entity_type, set()).add(place.entity_id)

    live_by_type: dict[str, frozenset[str]] = {}
    missing_by_type: dict[str, frozenset[str]] = {}
    extra_by_type: dict[str, frozenset[str]] = {}
    truncated_types: set[str] = set()

    for entity_type in sorted(expected_by_type):
        expected = expected_by_type[entity_type]
        entities = orion.list_entities(entity_type, attrs="id", limit=list_limit)
        live = _extract_ids(entities)

        truncated = len(entities) >= list_limit
        if truncated:
            truncated_types.add(entity_type)
            logger.warning(
                "orion list_entities response may be truncated",
                extra={
                    "event": "entity_map_truncated",
                    "entity_type": entity_type,
                    "count_live": len(live),
                    "limit": list_limit,
                },
            )

        missing = expected - live
        extra = live - expected

        for entity_id in sorted(missing):
            logger.warning(
                "metadata target missing from orion",
                extra={
                    "event": "entity_map_missing_target",
                    "entity_type": entity_type,
                    "entity_id": entity_id,
                },
            )

        logger.info(
            "validated metadata targets against orion",
            extra={
                "event": "entity_map_refreshed",
                "entity_type": entity_type,
                "count_expected": len(expected),
                "count_live": len(live),
                "count_missing": len(missing),
                "count_extra": len(extra),
            },
        )

        live_by_type[entity_type] = frozenset(live)
        missing_by_type[entity_type] = frozenset(missing)
        extra_by_type[entity_type] = frozenset(extra)

    return EntityMapResult(
        expected_by_type={
            entity_type: frozenset(ids) for entity_type, ids in expected_by_type.items()
        },
        live_by_type=live_by_type,
        missing_by_type=missing_by_type,
        extra_by_type=extra_by_type,
        truncated_types=frozenset(truncated_types),
    )


def _extract_ids(entities: Iterable[dict[str, Any]]) -> set[str]:
    """Return the set of ``id`` values from an Orion list response.

    Entries without an ``id`` field are skipped silently. Orion always
    returns ``id`` on entity objects, so a missing key would mean an
    unexpected response shape; this function ignores that case rather
    than raising.
    """
    return {entity["id"] for entity in entities if "id" in entity}
