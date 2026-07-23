from importlib import import_module

PRODUCT_A_TYPES = {
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


def test_inspect_attribute_types_uses_exact_product_a_type_contract() -> None:
    probe = import_module("scripts.dev.inspect_attribute_types")

    assert probe.EXPECTED_TYPES == PRODUCT_A_TYPES
    assert probe.REQUIRED_PRODUCT_A_ATTRS == frozenset(PRODUCT_A_TYPES)
    assert "peopleCount_flow" not in probe.EXPECTED_TYPES
