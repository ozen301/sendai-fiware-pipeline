from collections.abc import Iterable
from pathlib import Path
from textwrap import dedent

import pytest

from sendai_pipeline.metadata import (
    MetadataLoadError,
    ParsedEntityId,
    SensorPlace,
    active_places,
    index_by_place_interval,
    load_metadata,
    parse_entity_id,
)

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "sensors_minimal.csv"


REQUIRED_COLUMNS = (
    "place_number",
    "batch",
    "expected_device_type",
    "interval_min",
    "entity_type",
    "entity_id",
    "identifcation",
    "active",
)


def _write_csv(tmp_path: Path, rows: Iterable[str]) -> Path:
    path = tmp_path / "sensors.csv"
    path.write_text("\n".join(rows) + "\n", encoding="utf-8")
    return path


def _full_row(**overrides: str) -> str:
    values = {
        "place_number": "10",
        "batch": "2023",
        "expected_device_type": "Pixel3aUT",
        "interval_min": "60",
        "entity_type": "Blesensor.per3600",
        "entity_id": "jp.sendai.Blesensor.per3600.10",
        "identifcation": "10",
        "active": "true",
    }
    values.update(overrides)
    return ",".join(values[col] for col in REQUIRED_COLUMNS)


def _header() -> str:
    return ",".join(REQUIRED_COLUMNS)


def test_load_metadata_returns_all_rows_from_minimal_fixture() -> None:
    places = load_metadata(FIXTURE_PATH)
    assert len(places) == 5


@pytest.mark.parametrize(
    "entity_id,entity_type,place_number,interval_min",
    [
        (
            "jp.sendai.Blesensor.per3600.10",
            "Blesensor.per3600",
            10,
            60,
        ),
        (
            "jp.sendai.Blesensor.per300.105",
            "Blesensor.per300",
            105,
            5,
        ),
    ],
)
def test_parse_entity_id_parses_canonical_ids(
    entity_id: str,
    entity_type: str,
    place_number: int,
    interval_min: int,
) -> None:
    parsed = parse_entity_id(entity_id)

    assert parsed == ParsedEntityId(
        entity_id=entity_id,
        entity_type=entity_type,
        place_number=place_number,
        interval_min=interval_min,
    )


@pytest.mark.parametrize(
    "entity_id",
    [
        "Blesensor.per3600.10",
        "jp.sendai.Blesensor.per3600.place10",
        "jp.sendai..10",
    ],
)
def test_parse_entity_id_returns_none_for_non_canonical_shape(
    entity_id: str,
) -> None:
    assert parse_entity_id(entity_id) is None


def test_parse_entity_id_parses_unknown_interval_suffix_without_interval() -> None:
    parsed = parse_entity_id("jp.sendai.Custom.weird.99")

    assert parsed == ParsedEntityId(
        entity_id="jp.sendai.Custom.weird.99",
        entity_type="Custom.weird",
        place_number=99,
        interval_min=None,
    )


def test_load_metadata_returns_sensor_place_with_typed_fields() -> None:
    places = load_metadata(FIXTURE_PATH)
    first = next(p for p in places if p.place_number == 10 and p.interval_min == 60)
    assert isinstance(first, SensorPlace)
    assert first.place_number == 10
    assert first.batch == "2023"
    assert first.expected_device_type == "Pixel3aUT"
    assert first.interval_min == 60
    assert first.entity_type == "Blesensor.per3600"
    assert first.entity_id == "jp.sendai.Blesensor.per3600.10"
    assert first.identifcation == "10"
    assert first.active is True


def test_load_metadata_trims_whitespace_on_identifcation() -> None:
    places = load_metadata(FIXTURE_PATH)
    trimmed = next(p for p in places if p.place_number == 105 and p.interval_min == 60)
    assert trimmed.identifcation == "105"


def test_load_metadata_reads_entity_id_and_type_verbatim(tmp_path: Path) -> None:
    path = _write_csv(
        tmp_path,
        [
            _header(),
            _full_row(
                entity_id="jp.sendai.Custom.weird-name.99",
                entity_type="Custom.weird-name",
            ),
        ],
    )
    [place] = load_metadata(path)
    assert place.entity_id == "jp.sendai.Custom.weird-name.99"
    assert place.entity_type == "Custom.weird-name"


def test_load_metadata_loads_inactive_rows_without_dropping_them() -> None:
    places = load_metadata(FIXTURE_PATH)
    inactive = [p for p in places if p.active is False]
    assert len(inactive) == 1
    assert inactive[0].place_number == 999


def test_sensor_place_is_frozen() -> None:
    places = load_metadata(FIXTURE_PATH)
    with pytest.raises((AttributeError, TypeError)):
        places[0].place_number = 999  # type: ignore[misc]


def test_sensor_place_is_hashable() -> None:
    places = load_metadata(FIXTURE_PATH)
    seen = {places[0], places[1]}
    assert len(seen) == 2


@pytest.mark.parametrize("missing", REQUIRED_COLUMNS)
def test_load_metadata_raises_when_required_column_missing(
    tmp_path: Path, missing: str
) -> None:
    header_cols = [col for col in REQUIRED_COLUMNS if col != missing]
    row_values = {
        "place_number": "10",
        "batch": "2023",
        "expected_device_type": "Pixel3aUT",
        "interval_min": "60",
        "entity_type": "Blesensor.per3600",
        "entity_id": "jp.sendai.Blesensor.per3600.10",
        "identifcation": "10",
        "active": "true",
    }
    path = _write_csv(
        tmp_path,
        [
            ",".join(header_cols),
            ",".join(row_values[col] for col in header_cols),
        ],
    )
    with pytest.raises(MetadataLoadError) as excinfo:
        load_metadata(path)
    assert missing in str(excinfo.value)


@pytest.mark.parametrize("empty_column", REQUIRED_COLUMNS)
def test_load_metadata_raises_when_required_value_empty(
    tmp_path: Path, empty_column: str
) -> None:
    path = _write_csv(
        tmp_path,
        [_header(), _full_row(**{empty_column: ""})],
    )
    with pytest.raises(MetadataLoadError) as excinfo:
        load_metadata(path)
    assert empty_column in str(excinfo.value)


@pytest.mark.parametrize("whitespace_column", REQUIRED_COLUMNS)
def test_load_metadata_raises_when_required_value_whitespace_only(
    tmp_path: Path, whitespace_column: str
) -> None:
    path = _write_csv(
        tmp_path,
        [_header(), _full_row(**{whitespace_column: "   "})],
    )
    with pytest.raises(MetadataLoadError) as excinfo:
        load_metadata(path)
    assert whitespace_column in str(excinfo.value)


def test_load_metadata_preserves_whitespace_on_entity_id_and_type(
    tmp_path: Path,
) -> None:
    path = _write_csv(
        tmp_path,
        [
            _header(),
            _full_row(
                entity_id="  jp.sendai.Blesensor.per3600.10  ",
                entity_type="  Blesensor.per3600  ",
            ),
        ],
    )
    [place] = load_metadata(path)
    assert place.entity_id == "  jp.sendai.Blesensor.per3600.10  "
    assert place.entity_type == "  Blesensor.per3600  "


@pytest.mark.parametrize("bad_value", ["2024", "2025", "", "twenty-three"])
def test_load_metadata_raises_when_batch_invalid(
    tmp_path: Path, bad_value: str
) -> None:
    path = _write_csv(
        tmp_path,
        [_header(), _full_row(batch=bad_value)],
    )
    with pytest.raises(MetadataLoadError) as excinfo:
        load_metadata(path)
    assert "batch" in str(excinfo.value)


@pytest.mark.parametrize("bad_value", ["pixel3aut", "iphone", "M5stack"])
def test_load_metadata_raises_when_device_type_invalid(
    tmp_path: Path, bad_value: str
) -> None:
    path = _write_csv(
        tmp_path,
        [_header(), _full_row(expected_device_type=bad_value)],
    )
    with pytest.raises(MetadataLoadError) as excinfo:
        load_metadata(path)
    assert "expected_device_type" in str(excinfo.value)


@pytest.mark.parametrize("bad_value", ["1", "10", "30", "120", "abc"])
def test_load_metadata_raises_when_interval_min_invalid(
    tmp_path: Path, bad_value: str
) -> None:
    path = _write_csv(
        tmp_path,
        [_header(), _full_row(interval_min=bad_value)],
    )
    with pytest.raises(MetadataLoadError) as excinfo:
        load_metadata(path)
    assert "interval_min" in str(excinfo.value)


@pytest.mark.parametrize("bad_value", ["abc", "10a", "1.5", "-5"])
def test_load_metadata_raises_when_place_number_not_integer(
    tmp_path: Path, bad_value: str
) -> None:
    path = _write_csv(
        tmp_path,
        [_header(), _full_row(place_number=bad_value)],
    )
    with pytest.raises(MetadataLoadError) as excinfo:
        load_metadata(path)
    assert "place_number" in str(excinfo.value)


@pytest.mark.parametrize(
    "value,expected",
    [
        ("true", True),
        ("True", True),
        ("TRUE", True),
        ("false", False),
        ("False", False),
        ("FALSE", False),
    ],
)
def test_load_metadata_parses_bool_active(
    tmp_path: Path, value: str, expected: bool
) -> None:
    path = _write_csv(
        tmp_path,
        [_header(), _full_row(active=value)],
    )
    [place] = load_metadata(path)
    assert place.active is expected


@pytest.mark.parametrize(
    "bad_value",
    ["1", "0", "yes", "no", "y", "n", "T", "F", "maybe", "2"],
)
def test_load_metadata_raises_when_active_unparseable(
    tmp_path: Path, bad_value: str
) -> None:
    path = _write_csv(
        tmp_path,
        [_header(), _full_row(active=bad_value)],
    )
    with pytest.raises(MetadataLoadError) as excinfo:
        load_metadata(path)
    assert "active" in str(excinfo.value)


def test_load_metadata_raises_when_file_missing(tmp_path: Path) -> None:
    missing = tmp_path / "does_not_exist.csv"
    with pytest.raises(MetadataLoadError) as excinfo:
        load_metadata(missing)
    assert str(missing) in str(excinfo.value)


def test_load_metadata_raises_when_csv_is_empty(tmp_path: Path) -> None:
    path = tmp_path / "empty.csv"
    path.write_text("", encoding="utf-8")
    with pytest.raises(MetadataLoadError):
        load_metadata(path)


def test_load_metadata_raises_when_header_only_no_rows(tmp_path: Path) -> None:
    path = _write_csv(tmp_path, [_header()])
    with pytest.raises(MetadataLoadError):
        load_metadata(path)


def test_load_metadata_error_includes_row_number(tmp_path: Path) -> None:
    path = _write_csv(
        tmp_path,
        [
            _header(),
            _full_row(),
            _full_row(batch="2099"),
        ],
    )
    with pytest.raises(MetadataLoadError) as excinfo:
        load_metadata(path)
    message = str(excinfo.value)
    lower = message.lower()
    assert "row" in lower or "line" in lower
    assert "3" in message or "2" in message


def test_load_metadata_reads_columns_by_header_name_not_position(
    tmp_path: Path,
) -> None:
    reordered = (
        "active,interval_min,entity_id,entity_type,"
        "identifcation,expected_device_type,batch,place_number"
    )
    row = "true,5,jp.sendai.Blesensor.per300.42,Blesensor.per300,42,M5Stack,2026,42"
    path = _write_csv(tmp_path, [reordered, row])
    [place] = load_metadata(path)
    assert place.place_number == 42
    assert place.batch == "2026"
    assert place.expected_device_type == "M5Stack"
    assert place.interval_min == 5
    assert place.entity_type == "Blesensor.per300"
    assert place.entity_id == "jp.sendai.Blesensor.per300.42"
    assert place.identifcation == "42"
    assert place.active is True


def test_active_places_filters_out_inactive() -> None:
    places = load_metadata(FIXTURE_PATH)
    selected = active_places(places, target_batches=("2023", "2026"))
    assert all(p.active for p in selected)
    assert len(selected) == 4


def test_active_places_filters_by_target_batches() -> None:
    places = load_metadata(FIXTURE_PATH)
    selected = active_places(places, target_batches=("2026",))
    assert {p.batch for p in selected} == {"2026"}
    assert all(p.place_number == 105 for p in selected)


def test_active_places_with_empty_target_batches_returns_empty() -> None:
    # `active_places` is a pure filter: it does not validate that
    # `target_batches` values exist in the metadata. Detecting a typo is the
    # config layer's responsibility and must surface before this function is
    # called. An empty filter input therefore legitimately returns no places.
    places = load_metadata(FIXTURE_PATH)
    selected = active_places(places, target_batches=())
    assert selected == []


def test_active_places_preserves_input_order() -> None:
    places = load_metadata(FIXTURE_PATH)
    selected = active_places(places, target_batches=("2023", "2026"))
    expected_order = [(p.place_number, p.interval_min) for p in places if p.active]
    actual_order = [(p.place_number, p.interval_min) for p in selected]
    assert actual_order == expected_order


def test_index_by_place_interval_keys_each_row_by_tuple() -> None:
    places = load_metadata(FIXTURE_PATH)
    index = index_by_place_interval(places)
    assert (10, 60) in index
    assert (10, 5) in index
    assert (105, 60) in index
    assert (105, 5) in index
    assert (999, 60) in index
    assert index[(10, 60)].entity_id == "jp.sendai.Blesensor.per3600.10"
    assert index[(105, 5)].entity_id == "jp.sendai.Blesensor.per300.105"


def test_index_by_place_interval_raises_on_duplicate_key(tmp_path: Path) -> None:
    path = _write_csv(
        tmp_path,
        [
            _header(),
            _full_row(),
            _full_row(),
        ],
    )
    places = load_metadata(path)
    with pytest.raises(MetadataLoadError) as excinfo:
        index_by_place_interval(places)
    message = str(excinfo.value)
    assert "duplicate" in message.lower() or "(10, 60)" in message


def test_load_metadata_accepts_utf8_bom(tmp_path: Path) -> None:
    path = tmp_path / "sensors_bom.csv"
    contents = dedent(
        f"""\
        {_header()}
        {_full_row()}
        """
    )
    path.write_bytes(b"\xef\xbb\xbf" + contents.encode("utf-8"))
    [place] = load_metadata(path)
    assert place.place_number == 10
