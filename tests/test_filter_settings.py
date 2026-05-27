from collections.abc import Iterable, Mapping
from dataclasses import FrozenInstanceError

import pytest

from sendai_pipeline.filter_settings import FilterConfigError, FilterSettings

TARGET_BATCH_FIELDS = {
    "TARGET_FLOW_BATCHES": "target_flow_batches",
    "TARGET_DIRECTION_BATCHES": "target_direction_batches",
}


def _settings_from_env(env: Mapping[str, str]) -> FilterSettings:
    return FilterSettings.from_env(env)


def _filter_settings(
    *,
    target_flow_batches: Iterable[str] = (),
    target_direction_batches: Iterable[str] = (),
    ignored_place_prefixes: tuple[str, ...] = ("quick.", "test"),
    source_max_imputation_tier: int = 2,
) -> FilterSettings:
    return FilterSettings(
        target_flow_batches=frozenset(target_flow_batches),
        target_direction_batches=frozenset(target_direction_batches),
        ignored_place_prefixes=ignored_place_prefixes,
        source_max_imputation_tier=source_max_imputation_tier,
    )


def test_from_env_uses_defaults_when_values_are_unset() -> None:
    settings = _settings_from_env({})

    assert settings.ignored_place_prefixes == ("quick.", "test")
    assert settings.target_flow_batches == frozenset()
    assert settings.target_direction_batches == frozenset()
    assert settings.source_max_imputation_tier == 2


def test_from_env_uses_defaults_when_values_are_empty() -> None:
    settings = _settings_from_env(
        {
            "IGNORED_PLACE_PREFIXES": "",
            "TARGET_FLOW_BATCHES": "",
            "TARGET_DIRECTION_BATCHES": "",
            "SOURCE_MAX_IMPUTATION_TIER": "",
        }
    )

    assert settings.ignored_place_prefixes == ("quick.", "test")
    assert settings.target_flow_batches == frozenset()
    assert settings.target_direction_batches == frozenset()
    assert settings.source_max_imputation_tier == 2


def test_from_env_disables_ignored_prefixes_for_single_comma_sentinel() -> None:
    settings = _settings_from_env({"IGNORED_PLACE_PREFIXES": ","})

    assert settings.ignored_place_prefixes == ()


def test_from_env_parses_single_ignored_prefix() -> None:
    settings = _settings_from_env({"IGNORED_PLACE_PREFIXES": "foo."})

    assert settings.ignored_place_prefixes == ("foo.",)


def test_from_env_parses_multiple_ignored_prefixes() -> None:
    settings = _settings_from_env({"IGNORED_PLACE_PREFIXES": "foo.,bar"})

    assert settings.ignored_place_prefixes == ("foo.", "bar")


def test_from_env_strips_whitespace_from_ignored_prefixes() -> None:
    settings = _settings_from_env({"IGNORED_PLACE_PREFIXES": "  foo.  ,  bar  "})

    assert settings.ignored_place_prefixes == ("foo.", "bar")


def test_from_env_drops_empty_ignored_prefix_entries() -> None:
    settings = _settings_from_env({"IGNORED_PLACE_PREFIXES": "foo.,,bar"})

    assert settings.ignored_place_prefixes == ("foo.", "bar")


@pytest.mark.parametrize(
    ("env_var", "value", "expected_batches"),
    [
        ("TARGET_FLOW_BATCHES", "2026", frozenset({"2026"})),
        ("TARGET_FLOW_BATCHES", "2023,2026", frozenset({"2023", "2026"})),
        ("TARGET_FLOW_BATCHES", "  2023  ,  2026  ", frozenset({"2023", "2026"})),
        ("TARGET_FLOW_BATCHES", "2023,,2026", frozenset({"2023", "2026"})),
        ("TARGET_FLOW_BATCHES", "2026,2023,2026", frozenset({"2023", "2026"})),
        ("TARGET_DIRECTION_BATCHES", "2026", frozenset({"2026"})),
        ("TARGET_DIRECTION_BATCHES", "2023,2026", frozenset({"2023", "2026"})),
        (
            "TARGET_DIRECTION_BATCHES",
            "  2023  ,  2026  ",
            frozenset({"2023", "2026"}),
        ),
        ("TARGET_DIRECTION_BATCHES", "2023,,2026", frozenset({"2023", "2026"})),
        (
            "TARGET_DIRECTION_BATCHES",
            "2026,2023,2026",
            frozenset({"2023", "2026"}),
        ),
    ],
)
def test_from_env_parses_target_batch_sets(
    env_var: str,
    value: str,
    expected_batches: frozenset[str],
) -> None:
    settings = _settings_from_env({env_var: value})

    assert getattr(settings, TARGET_BATCH_FIELDS[env_var]) == expected_batches


def test_from_env_with_no_argument_reads_os_environ(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("IGNORED_PLACE_PREFIXES", raising=False)
    monkeypatch.delenv("TARGET_FLOW_BATCHES", raising=False)
    monkeypatch.delenv("TARGET_DIRECTION_BATCHES", raising=False)
    monkeypatch.delenv("SOURCE_MAX_IMPUTATION_TIER", raising=False)
    monkeypatch.setenv("IGNORED_PLACE_PREFIXES", "live.,sandbox")
    monkeypatch.setenv("TARGET_FLOW_BATCHES", "2023,2026")
    monkeypatch.setenv("TARGET_DIRECTION_BATCHES", "2026")
    monkeypatch.setenv("SOURCE_MAX_IMPUTATION_TIER", "1")

    settings = FilterSettings.from_env()

    assert settings.ignored_place_prefixes == ("live.", "sandbox")
    assert settings.target_flow_batches == frozenset({"2023", "2026"})
    assert settings.target_direction_batches == frozenset({"2026"})
    assert settings.source_max_imputation_tier == 1


def test_from_env_parses_source_max_imputation_tier() -> None:
    settings = _settings_from_env({"SOURCE_MAX_IMPUTATION_TIER": "5"})

    assert settings.source_max_imputation_tier == 5


def test_from_env_rejects_non_integer_source_max_imputation_tier() -> None:
    with pytest.raises(FilterConfigError) as excinfo:
        _settings_from_env({"SOURCE_MAX_IMPUTATION_TIER": "abc"})

    assert "SOURCE_MAX_IMPUTATION_TIER" in str(excinfo.value)


def test_from_env_rejects_negative_source_max_imputation_tier() -> None:
    with pytest.raises(FilterConfigError) as excinfo:
        _settings_from_env({"SOURCE_MAX_IMPUTATION_TIER": "-1"})

    assert "SOURCE_MAX_IMPUTATION_TIER" in str(excinfo.value)


@pytest.mark.parametrize(
    "metadata_batches",
    [
        ["2023", "2026", "2027"],
        {"2023", "2026", "2027"},
        frozenset({"2023", "2026", "2027"}),
    ],
)
def test_validate_target_flow_batches_accepts_subset_of_metadata(
    metadata_batches: Iterable[str],
) -> None:
    settings = _filter_settings(target_flow_batches=("2023", "2026"))

    settings.validate_target_flow_batches(metadata_batches)


@pytest.mark.parametrize(
    "metadata_batches",
    [
        ["2023", "2026", "2027"],
        {"2023", "2026", "2027"},
        frozenset({"2023", "2026", "2027"}),
    ],
)
def test_validate_target_direction_batches_accepts_subset_of_metadata(
    metadata_batches: Iterable[str],
) -> None:
    settings = _filter_settings(target_direction_batches=("2023", "2026"))

    settings.validate_target_direction_batches(metadata_batches)


def test_validate_target_batch_sets_accept_empty_targets() -> None:
    settings = _filter_settings()

    settings.validate_target_flow_batches([])
    settings.validate_target_direction_batches([])


def test_validate_target_flow_batches_raises_when_targets_are_unknown() -> None:
    settings = _filter_settings(target_flow_batches=("2023", "2024", "2025"))

    with pytest.raises(FilterConfigError) as excinfo:
        settings.validate_target_flow_batches(["2023", "2026"])

    assert str(excinfo.value) == "Unknown TARGET_FLOW_BATCHES entries: 2024, 2025"


def test_validate_target_direction_batches_raises_when_targets_are_unknown() -> None:
    settings = _filter_settings(target_direction_batches=("2023", "2024", "2025"))

    with pytest.raises(FilterConfigError) as excinfo:
        settings.validate_target_direction_batches(["2023", "2026"])

    assert str(excinfo.value) == "Unknown TARGET_DIRECTION_BATCHES entries: 2024, 2025"


def test_validate_target_batch_sets_are_independent() -> None:
    settings = _filter_settings(
        target_flow_batches=("2023", "2026"),
        target_direction_batches=("2023", "2025"),
    )

    settings.validate_target_flow_batches(["2023", "2026"])

    with pytest.raises(FilterConfigError) as excinfo:
        settings.validate_target_direction_batches(["2023", "2026"])

    assert str(excinfo.value) == "Unknown TARGET_DIRECTION_BATCHES entries: 2025"


def test_filter_settings_is_frozen() -> None:
    settings = _filter_settings(target_flow_batches=("2026",))

    with pytest.raises(FrozenInstanceError):
        settings.target_flow_batches = frozenset()  # pyright: ignore[reportAttributeAccessIssue]


def test_filter_settings_equality_and_hashability_behave_as_frozen_dataclass() -> None:
    first = _filter_settings(
        target_flow_batches=("2023", "2026"),
        target_direction_batches=("2026",),
    )
    second = _filter_settings(
        target_flow_batches=("2026", "2023"),
        target_direction_batches=("2026",),
    )

    assert first == second
    assert hash(first) == hash(second)
    assert {first: "matched"}[second] == "matched"
