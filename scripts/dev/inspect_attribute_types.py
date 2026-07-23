"""Dev probe: confirm the NGSI attribute types we write match what Orion has.

The pipeline does not discover types from Orion at startup; it ships with a
fixed type map and the probe is how we verify the map before locking it into
the Product A transform tests. The probe lists existing entities for every
NGSI type used in the runtime sensor metadata, inspects each entity body,
and reports observed ``type`` values per attribute name next to the
expected values:

- ``dateObservedFrom``, ``dateObservedTo``, ``dateRetrieved`` → ``DateTime``.
- ``identifcation`` → ``Text``.
- ``peopleCount_immedate``, ``peopleCount_near``, ``peopleCount_far``
  → ``"number"`` (lowercase string — that's what the live Sendai broker
  already carries; writing the NGSI v2 canonical ``Integer`` would change
  the recorded type and create mixed-type history in STH-Comet).
- ``peopleOccupancy_immedate``, ``peopleOccupancy_near``,
  ``peopleOccupancy_far`` → ``"number"``
  (same reasoning).

Operator-run, not part of CI or any cron job. Usage from project root:

    uv run python scripts/dev/inspect_attribute_types.py

Requires the standard ``FIWARE_*`` and ``SENSOR_METADATA_PATH`` env vars.
Read-only: issues only ``GET`` requests.

Exit codes:

- ``0`` — every expected Product A attribute name was observed on at
  least one sampled entity and every observed NGSI ``type`` matches the
  expected value.
- ``1`` — at least one observed NGSI ``type`` disagrees with the
  expected value (mismatch).
- ``2`` — inconclusive: an entity_type returned no live entities, or
  none of the sampled entities carried an expected Product A attribute.
  The probe deliberately does not pass in this case — a silent "OK"
  on an empty broker would be worse than a loud "could not verify".
"""

import os
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from sendai_pipeline.auth import AuthClient, AuthSettings
from sendai_pipeline.metadata import SensorPlace, load_metadata
from sendai_pipeline.orion_client import OrionClient, OrionSettings

SAMPLE_LIMIT = 25

EXIT_OK = 0
EXIT_MISMATCH = 1
EXIT_INCONCLUSIVE = 2

EXPECTED_TYPES: dict[str, str] = {
    "dateObservedFrom": "DateTime",
    "dateObservedTo": "DateTime",
    "dateRetrieved": "DateTime",
    "identifcation": "Text",
    "peopleCount_immedate": "number",
    "peopleCount_near": "number",
    "peopleCount_far": "number",
    "peopleOccupancy_immedate": "number",
    "peopleOccupancy_near": "number",
    "peopleOccupancy_far": "number",
}

REQUIRED_PRODUCT_A_ATTRS: frozenset[str] = frozenset(
    {
        "dateObservedFrom",
        "dateObservedTo",
        "dateRetrieved",
        "identifcation",
        "peopleCount_immedate",
        "peopleCount_near",
        "peopleCount_far",
        "peopleOccupancy_immedate",
        "peopleOccupancy_near",
        "peopleOccupancy_far",
    }
)


def _collect_observed_types(
    entities: list[dict[str, Any]],
) -> dict[str, set[str]]:
    """Group observed NGSI ``type`` values by attribute name.

    Skips Orion's reserved ``id`` and ``type`` keys on each entity body.
    """
    observed: dict[str, set[str]] = defaultdict(set)
    for entity in entities:
        for attr_name, attr_value in entity.items():
            if attr_name in {"id", "type"}:
                continue
            if isinstance(attr_value, dict) and "type" in attr_value:
                observed[attr_name].add(str(attr_value["type"]))
    return observed


def _distinct_entity_types(places: list[SensorPlace]) -> list[str]:
    """Return distinct ``entity_type`` values from active metadata rows."""
    return sorted({place.entity_type for place in places if place.active})


def _report_entity_type(
    entity_type: str,
    entities: list[dict[str, Any]],
) -> tuple[int, int]:
    """Print observed-vs-expected types for one entity_type.

    Returns a ``(mismatches, inconclusive)`` count pair: ``mismatches``
    grows when an observed NGSI ``type`` disagrees with the expected
    value; ``inconclusive`` grows when the type returned no entities
    at all or none of the sampled entities carried any of the required
    Product A attributes.
    """
    print(f"\n=== {entity_type} ===")
    print(f"  sampled entities     : {len(entities)}")
    if not entities:
        print("  (no live entities for this type — verification inconclusive)")
        return 0, 1

    observed = _collect_observed_types(entities)
    if not observed:
        print("  (sampled entities carry no NGSI-shaped attribute bodies —")
        print("   verification inconclusive)")
        return 0, 1

    mismatches = 0
    for attr_name in sorted(observed):
        observed_types = sorted(observed[attr_name])
        expected = EXPECTED_TYPES.get(attr_name)
        marker = "  "
        if expected is None:
            note = "(no documented expectation)"
        elif observed_types == [expected]:
            note = f"OK (expected {expected})"
        else:
            note = f"MISMATCH — expected {expected}"
            marker = "! "
            mismatches += 1
        joined = ", ".join(observed_types)
        print(f"  {marker}{attr_name:<28s} observed={joined!s:<24s} {note}")

    missing_required = REQUIRED_PRODUCT_A_ATTRS - observed.keys()
    inconclusive = 0
    if missing_required:
        inconclusive = 1
        names = ", ".join(sorted(missing_required))
        print(
            "  (no sampled entity carried these expected Product A attributes — "
            "verification inconclusive)"
        )
        print(f"     missing: {names}")

    return mismatches, inconclusive


def main() -> int:
    """Run the attribute-type probe and report the result."""
    load_dotenv()
    metadata_path = Path(os.environ.get("SENSOR_METADATA_PATH", "metadata/sensors.csv"))
    print(f"Loading metadata from {metadata_path}...")
    places = load_metadata(metadata_path)
    entity_types = _distinct_entity_types(places)
    print(f"Distinct active entity_types: {entity_types}")

    if not entity_types:
        print("\nNo active entity_types in metadata — verification inconclusive.")
        return EXIT_INCONCLUSIVE

    orion_settings = OrionSettings.from_env()
    auth = AuthClient(AuthSettings.from_env())
    orion = OrionClient(orion_settings, auth=auth)

    total_mismatches = 0
    total_inconclusive = 0
    for entity_type in entity_types:
        entities = orion.list_entities(entity_type, limit=SAMPLE_LIMIT)
        mismatches, inconclusive = _report_entity_type(entity_type, entities)
        total_mismatches += mismatches
        total_inconclusive += inconclusive

    print()
    if total_mismatches:
        print(
            f"FAIL: {total_mismatches} attribute type mismatch(es). "
            "Reconcile the expected map with what Orion actually has before "
            "shipping Product A; update the planning docs and the "
            "transform_flow tests so the locked map stays accurate."
        )
        return EXIT_MISMATCH

    if total_inconclusive:
        print(
            f"INCONCLUSIVE: {total_inconclusive} entity_type(s) could not be "
            "verified (no live entities, or no sampled entity carried the "
            "required Product A attributes). Re-run after entities exist or "
            "widen the sample before locking the type map."
        )
        return EXIT_INCONCLUSIVE

    print("OK: every observed attribute type matches the documented expectation.")
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
