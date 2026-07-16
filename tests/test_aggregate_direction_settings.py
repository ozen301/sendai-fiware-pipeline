from collections.abc import Callable, Mapping
from dataclasses import fields

import pytest

from sendai_pipeline.run_direction import (
    RunDirectionConfigError,
    RunDirectionSettings,
)
from sendai_pipeline.sth_subscriptions import (
    StHSubscriptionError,
    StHSubscriptionSettings,
)

Settings = RunDirectionSettings | StHSubscriptionSettings
SettingsFactory = Callable[[Mapping[str, str]], Settings]


def direction_settings(env: Mapping[str, str]) -> RunDirectionSettings:
    return RunDirectionSettings.from_env(env)


def subscription_settings(env: Mapping[str, str]) -> StHSubscriptionSettings:
    return StHSubscriptionSettings.from_env(
        {
            "FIWARE_BASE_URL": "https://example.test",
            "COMET_NOTIFY_URL": "http://internal-comet.example/notify",
            **env,
        }
    )


@pytest.fixture(
    params=(direction_settings, subscription_settings),
    ids=("runner", "subscription"),
)
def settings_factory(request: pytest.FixtureRequest) -> SettingsFactory:
    return request.param


def test_settings_from_env_uses_aggregate_target_defaults(
    settings_factory: SettingsFactory,
) -> None:
    settings = settings_factory({})

    assert settings.product_b_aggregate_entity_id == "jp.sendai.Blesensor.flow"
    assert settings.product_b_aggregate_entity_type == "Blesensor.flow"


def test_settings_from_env_accepts_aggregate_target_overrides(
    settings_factory: SettingsFactory,
) -> None:
    settings = settings_factory(
        {
            "PRODUCT_B_AGGREGATE_ENTITY_ID": "custom.aggregate.entity",
            "PRODUCT_B_AGGREGATE_ENTITY_TYPE": "Custom.aggregate.type",
        }
    )

    assert settings.product_b_aggregate_entity_id == "custom.aggregate.entity"
    assert settings.product_b_aggregate_entity_type == "Custom.aggregate.type"


def test_settings_from_env_accepts_url_sensitive_aggregate_target_characters(
    settings_factory: SettingsFactory,
) -> None:
    settings = settings_factory(
        {
            "PRODUCT_B_AGGREGATE_ENTITY_ID": "urn:ngsi-ld:Blesensor.flow:Sendai",
            "PRODUCT_B_AGGREGATE_ENTITY_TYPE": "Blesensor.flow:v2",
        }
    )

    assert settings.product_b_aggregate_entity_id == "urn:ngsi-ld:Blesensor.flow:Sendai"
    assert settings.product_b_aggregate_entity_type == "Blesensor.flow:v2"


@pytest.mark.parametrize(
    ("env_name", "malformed_value"),
    (
        ("PRODUCT_B_AGGREGATE_ENTITY_ID", ""),
        ("PRODUCT_B_AGGREGATE_ENTITY_ID", " aggregate.entity"),
        ("PRODUCT_B_AGGREGATE_ENTITY_ID", "aggregate.entity "),
        ("PRODUCT_B_AGGREGATE_ENTITY_ID", "aggregate\nentity"),
        ("PRODUCT_B_AGGREGATE_ENTITY_TYPE", ""),
        ("PRODUCT_B_AGGREGATE_ENTITY_TYPE", " aggregate.type"),
        ("PRODUCT_B_AGGREGATE_ENTITY_TYPE", "aggregate.type "),
        ("PRODUCT_B_AGGREGATE_ENTITY_TYPE", "aggregate\x00type"),
    ),
)
def test_settings_from_env_rejects_malformed_aggregate_target_values(
    settings_factory: SettingsFactory,
    env_name: str,
    malformed_value: str,
) -> None:
    expected_error = (
        RunDirectionConfigError
        if settings_factory is direction_settings
        else StHSubscriptionError
    )

    with pytest.raises(expected_error, match=env_name):
        settings_factory({env_name: malformed_value})


def test_settings_expose_aggregate_target_without_identifcation_setting(
    settings_factory: SettingsFactory,
) -> None:
    settings = settings_factory({})

    assert settings.product_b_aggregate_entity_id == "jp.sendai.Blesensor.flow"
    assert settings.product_b_aggregate_entity_type == "Blesensor.flow"
    assert not hasattr(settings, "identifcation")
    assert not hasattr(settings, "product_b_aggregate_identifcation")


def test_settings_from_env_ignores_direction_revision_cursor_seed(
    settings_factory: SettingsFactory,
) -> None:
    settings_without_seed = settings_factory({})
    settings_with_seed = settings_factory(
        {"DIRECTION_REVISION_CURSOR_SEED": "not-a-timestamp\x00"}
    )

    assert settings_with_seed == settings_without_seed
    field_names = {field.name for field in fields(settings_with_seed)}
    assert not any("revision" in name and "seed" in name for name in field_names)
