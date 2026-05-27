import csv
import logging
import os
import subprocess
import sys
from collections.abc import Iterable
from pathlib import Path

import pytest

from sendai_pipeline.metadata import MetadataLoadError, SensorPlace
from sendai_pipeline.refresh import (
    EXIT_IO_ERROR,
    EXIT_OK,
    EXIT_VALIDATION_ERROR,
    RefreshMetadataError,
    RefreshMetadataSettings,
    RefreshResult,
    main,
    refresh_metadata,
)

STABLE_HEADER = (
    "place_number,batch,expected_device_type,interval_min,"
    "entity_type,entity_id,identifcation,active"
)

STAGED_HEADER = (
    "place_number,batch,expected_device_type,interval_min,"
    "entity_type,entity_id,ID,active"
)


def _stable_row(**overrides: str) -> str:
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
    cols = [
        "place_number",
        "batch",
        "expected_device_type",
        "interval_min",
        "entity_type",
        "entity_id",
        "identifcation",
        "active",
    ]
    return ",".join(values[col] for col in cols)


def _staged_row(**overrides: str) -> str:
    values = {
        "place_number": "105",
        "batch": "2026",
        "expected_device_type": "M5Stack",
        "interval_min": "60",
        "entity_type": "Blesensor.per3600",
        "entity_id": "jp.sendai.Blesensor.per3600.105",
        "ID": "105",
        "active": "true",
    }
    values.update(overrides)
    cols = [
        "place_number",
        "batch",
        "expected_device_type",
        "interval_min",
        "entity_type",
        "entity_id",
        "ID",
        "active",
    ]
    return ",".join(values[col] for col in cols)


def _write(path: Path, lines: Iterable[str]) -> Path:
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def _read_output_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as csv_file:
        return list(csv.DictReader(csv_file))


def _make_inputs(
    tmp_path: Path,
    *,
    stable_rows: Iterable[str] = (),
    staged_rows: Iterable[str] = (),
) -> tuple[Path, Path, Path]:
    stable_path = _write(tmp_path / "sensors_stable.csv", [STABLE_HEADER, *stable_rows])
    staged_path = _write(
        tmp_path / "sensors_refreshable.csv.staged", [STAGED_HEADER, *staged_rows]
    )
    output_path = tmp_path / "sensors.csv"
    return stable_path, staged_path, output_path


def _by_place_interval(
    places: Iterable[SensorPlace],
) -> dict[tuple[int, int], SensorPlace]:
    return {(p.place_number, p.interval_min): p for p in places}


def test_refresh_metadata_unions_stable_and_staged_rows(tmp_path: Path) -> None:
    stable_path, staged_path, output_path = _make_inputs(
        tmp_path,
        stable_rows=[_stable_row()],
        staged_rows=[_staged_row()],
    )
    result = refresh_metadata(stable_path, staged_path, output_path)
    keys = {(p.place_number, p.interval_min) for p in result.places}
    assert keys == {(10, 60), (105, 60)}


def test_refresh_metadata_writes_output_with_canonical_header(tmp_path: Path) -> None:
    stable_path, staged_path, output_path = _make_inputs(
        tmp_path,
        stable_rows=[_stable_row()],
        staged_rows=[_staged_row()],
    )
    refresh_metadata(stable_path, staged_path, output_path)
    header = output_path.read_text(encoding="utf-8").splitlines()[0]
    assert header == STABLE_HEADER


def test_refresh_metadata_renames_id_to_identifcation_in_output(tmp_path: Path) -> None:
    stable_path, staged_path, output_path = _make_inputs(
        tmp_path,
        stable_rows=[_stable_row()],
        staged_rows=[_staged_row(ID="105")],
    )
    refresh_metadata(stable_path, staged_path, output_path)
    contents = output_path.read_text(encoding="utf-8")
    assert "ID" not in contents.splitlines()[0]
    assert "identifcation" in contents.splitlines()[0]


def test_refresh_metadata_trims_whitespace_on_staged_id(tmp_path: Path) -> None:
    stable_path, staged_path, output_path = _make_inputs(
        tmp_path,
        stable_rows=[_stable_row()],
        staged_rows=[_staged_row(ID="  105  ")],
    )
    result = refresh_metadata(stable_path, staged_path, output_path)
    [staged_place] = [p for p in result.places if p.place_number == 105]
    assert staged_place.identifcation == "105"


def test_refresh_metadata_writes_trimmed_stable_identifcation(
    tmp_path: Path,
) -> None:
    stable_path, staged_path, output_path = _make_inputs(
        tmp_path,
        stable_rows=[_stable_row(identifcation="  10  ")],
        staged_rows=[],
    )
    result = refresh_metadata(stable_path, staged_path, output_path)
    [stable_place] = result.places
    assert stable_place.identifcation == "10"
    [stable_row] = _read_output_rows(output_path)
    assert stable_row["identifcation"] == "10"


def test_refresh_metadata_preserves_entity_id_and_type_whitespace(
    tmp_path: Path,
) -> None:
    stable_path, staged_path, output_path = _make_inputs(
        tmp_path,
        stable_rows=[
            _stable_row(
                entity_id="  jp.sendai.Blesensor.per3600.10  ",
                entity_type="  Blesensor.per3600  ",
            )
        ],
        staged_rows=[],
    )
    result = refresh_metadata(stable_path, staged_path, output_path)
    [stable_place] = result.places
    assert stable_place.entity_id == "  jp.sendai.Blesensor.per3600.10  "
    assert stable_place.entity_type == "  Blesensor.per3600  "
    [stable_row] = _read_output_rows(output_path)
    assert stable_row["entity_id"] == "  jp.sendai.Blesensor.per3600.10  "
    assert stable_row["entity_type"] == "  Blesensor.per3600  "


def test_refresh_metadata_raises_when_staged_lacks_id_column(tmp_path: Path) -> None:
    stable_path = _write(
        tmp_path / "sensors_stable.csv", [STABLE_HEADER, _stable_row()]
    )
    # Same set of columns as the staged header but without ID — operator
    # typo would otherwise silently no-op the refreshable side.
    no_id_header = (
        "place_number,batch,expected_device_type,interval_min,"
        "entity_type,entity_id,active"
    )
    no_id_row = (
        "105,2026,M5Stack,60,Blesensor.per3600,jp.sendai.Blesensor.per3600.105,true"
    )
    staged_path = _write(
        tmp_path / "sensors_refreshable.csv.staged",
        [no_id_header, no_id_row],
    )
    output_path = tmp_path / "sensors.csv"
    with pytest.raises(RefreshMetadataError) as excinfo:
        refresh_metadata(stable_path, staged_path, output_path)
    assert "ID" in str(excinfo.value)
    assert not output_path.exists()


def test_refresh_metadata_raises_when_staged_lacks_id_column_and_is_empty(
    tmp_path: Path,
) -> None:
    stable_path = _write(
        tmp_path / "sensors_stable.csv", [STABLE_HEADER, _stable_row()]
    )
    no_id_header = (
        "place_number,batch,expected_device_type,interval_min,"
        "entity_type,entity_id,active"
    )
    staged_path = _write(tmp_path / "sensors_refreshable.csv.staged", [no_id_header])
    output_path = tmp_path / "sensors.csv"
    with pytest.raises(RefreshMetadataError) as excinfo:
        refresh_metadata(stable_path, staged_path, output_path)
    assert "ID" in str(excinfo.value)
    assert not output_path.exists()


@pytest.mark.parametrize(
    "missing_column",
    [
        "place_number",
        "batch",
        "expected_device_type",
        "interval_min",
        "entity_type",
        "entity_id",
        "active",
    ],
)
def test_refresh_metadata_raises_when_staged_missing_other_canonical_column(
    tmp_path: Path, missing_column: str
) -> None:
    stable_path = _write(
        tmp_path / "sensors_stable.csv", [STABLE_HEADER, _stable_row()]
    )
    columns = [
        "place_number",
        "batch",
        "expected_device_type",
        "interval_min",
        "entity_type",
        "entity_id",
        "ID",
        "active",
    ]
    kept = [c for c in columns if c != missing_column]
    staged_path = _write(tmp_path / "sensors_refreshable.csv.staged", [",".join(kept)])
    output_path = tmp_path / "sensors.csv"
    with pytest.raises(RefreshMetadataError) as excinfo:
        refresh_metadata(stable_path, staged_path, output_path)
    assert missing_column in str(excinfo.value)
    assert not output_path.exists()


def test_refresh_metadata_raises_when_staged_has_both_id_and_identifcation(
    tmp_path: Path,
) -> None:
    stable_path = _write(
        tmp_path / "sensors_stable.csv", [STABLE_HEADER, _stable_row()]
    )
    conflicting_header = STAGED_HEADER + ",identifcation"
    conflicting_row = _staged_row() + ",105"
    staged_path = _write(
        tmp_path / "sensors_refreshable.csv.staged",
        [conflicting_header, conflicting_row],
    )
    output_path = tmp_path / "sensors.csv"
    with pytest.raises(RefreshMetadataError) as excinfo:
        refresh_metadata(stable_path, staged_path, output_path)
    message = str(excinfo.value)
    assert "ID" in message and "identifcation" in message


def test_refresh_metadata_validates_required_fields_in_staged(tmp_path: Path) -> None:
    stable_path, staged_path, output_path = _make_inputs(
        tmp_path,
        stable_rows=[_stable_row()],
        staged_rows=[_staged_row(batch="")],
    )
    with pytest.raises(MetadataLoadError) as excinfo:
        refresh_metadata(stable_path, staged_path, output_path)
    assert "batch" in str(excinfo.value)
    assert not output_path.exists()


def test_refresh_metadata_validates_required_fields_in_stable(tmp_path: Path) -> None:
    stable_path, staged_path, output_path = _make_inputs(
        tmp_path,
        stable_rows=[_stable_row(expected_device_type="iPhone")],
        staged_rows=[_staged_row()],
    )
    with pytest.raises(MetadataLoadError) as excinfo:
        refresh_metadata(stable_path, staged_path, output_path)
    assert "expected_device_type" in str(excinfo.value)
    assert not output_path.exists()


def test_refresh_metadata_raises_on_duplicate_place_interval_across_files(
    tmp_path: Path,
) -> None:
    stable_path, staged_path, output_path = _make_inputs(
        tmp_path,
        stable_rows=[_stable_row(place_number="42", interval_min="60")],
        staged_rows=[_staged_row(place_number="42", interval_min="60")],
    )
    with pytest.raises(MetadataLoadError) as excinfo:
        refresh_metadata(stable_path, staged_path, output_path)
    message = str(excinfo.value)
    assert "duplicate" in message.lower() or "(42, 60)" in message
    assert not output_path.exists()


def test_refresh_metadata_allows_same_place_with_different_intervals(
    tmp_path: Path,
) -> None:
    stable_path, staged_path, output_path = _make_inputs(
        tmp_path,
        stable_rows=[
            _stable_row(interval_min="60"),
            _stable_row(
                interval_min="5",
                entity_type="Blesensor.per300",
                entity_id="jp.sendai.Blesensor.per300.10",
            ),
        ],
        staged_rows=[_staged_row()],
    )
    result = refresh_metadata(stable_path, staged_path, output_path)
    keys = {(p.place_number, p.interval_min) for p in result.places}
    assert keys == {(10, 60), (10, 5), (105, 60)}


def test_refresh_metadata_replaces_existing_output(tmp_path: Path) -> None:
    stable_path, staged_path, output_path = _make_inputs(
        tmp_path,
        stable_rows=[_stable_row()],
        staged_rows=[_staged_row()],
    )
    output_path.write_text(
        STABLE_HEADER + "\n" + _stable_row(place_number="999") + "\n",
        encoding="utf-8",
    )
    refresh_metadata(stable_path, staged_path, output_path)
    contents = output_path.read_text(encoding="utf-8")
    assert "999" not in contents
    assert "105" in contents


def test_refresh_metadata_uses_atomic_replace_from_sibling_temp_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stable_path, staged_path, output_path = _make_inputs(
        tmp_path,
        stable_rows=[_stable_row()],
        staged_rows=[_staged_row()],
    )
    calls: list[tuple[str, str]] = []
    real_replace = os.replace

    def spy_replace(src, dst):  # type: ignore[no-untyped-def]
        calls.append((str(src), str(dst)))
        return real_replace(src, dst)

    monkeypatch.setattr("sendai_pipeline.refresh.os.replace", spy_replace)
    refresh_metadata(stable_path, staged_path, output_path)
    assert len(calls) == 1
    src, dst = calls[0]
    assert dst == str(output_path)
    assert src == str(output_path.with_name(output_path.name + ".new"))
    assert Path(src).parent == Path(dst).parent


def test_refresh_metadata_leaves_existing_output_unchanged_on_failure(
    tmp_path: Path,
) -> None:
    stable_path, staged_path, output_path = _make_inputs(
        tmp_path,
        stable_rows=[_stable_row()],
        staged_rows=[_staged_row(batch="")],
    )
    previous = STABLE_HEADER + "\n" + _stable_row(place_number="999") + "\n"
    output_path.write_text(previous, encoding="utf-8")
    with pytest.raises(MetadataLoadError):
        refresh_metadata(stable_path, staged_path, output_path)
    assert output_path.read_text(encoding="utf-8") == previous


def test_refresh_metadata_cleans_up_temp_file_on_failure(tmp_path: Path) -> None:
    stable_path, staged_path, output_path = _make_inputs(
        tmp_path,
        stable_rows=[_stable_row()],
        staged_rows=[_staged_row(batch="")],
    )
    with pytest.raises(MetadataLoadError):
        refresh_metadata(stable_path, staged_path, output_path)
    tmp_path_new = output_path.parent / (output_path.name + ".new")
    assert not tmp_path_new.exists()


def test_refresh_metadata_returns_all_added_when_no_previous_output(
    tmp_path: Path,
) -> None:
    stable_path, staged_path, output_path = _make_inputs(
        tmp_path,
        stable_rows=[_stable_row()],
        staged_rows=[_staged_row()],
    )
    result = refresh_metadata(stable_path, staged_path, output_path)
    added_keys = {(p.place_number, p.interval_min) for p in result.diff.added}
    assert added_keys == {(10, 60), (105, 60)}
    assert result.diff.removed == []
    assert result.diff.changed == []


def test_refresh_metadata_diff_detects_removed_rows(tmp_path: Path) -> None:
    stable_path, staged_path, output_path = _make_inputs(
        tmp_path,
        stable_rows=[_stable_row()],
        staged_rows=[],
    )
    # Previous output contained an extra row that's no longer present.
    output_path.write_text(
        STABLE_HEADER
        + "\n"
        + _stable_row()
        + "\n"
        + _stable_row(
            place_number="105",
            batch="2026",
            expected_device_type="M5Stack",
            entity_id="jp.sendai.Blesensor.per3600.105",
            identifcation="105",
        )
        + "\n",
        encoding="utf-8",
    )
    result = refresh_metadata(stable_path, staged_path, output_path)
    removed_keys = {(p.place_number, p.interval_min) for p in result.diff.removed}
    assert removed_keys == {(105, 60)}
    assert result.diff.added == []
    assert result.diff.changed == []


def test_refresh_metadata_diff_detects_changed_rows(tmp_path: Path) -> None:
    stable_path, staged_path, output_path = _make_inputs(
        tmp_path,
        stable_rows=[_stable_row(identifcation="10")],
        staged_rows=[_staged_row(ID="105")],
    )
    output_path.write_text(
        STABLE_HEADER
        + "\n"
        + _stable_row(identifcation="10-old")
        + "\n"
        + _stable_row(
            place_number="105",
            batch="2026",
            expected_device_type="M5Stack",
            entity_id="jp.sendai.Blesensor.per3600.105",
            identifcation="105",
        )
        + "\n",
        encoding="utf-8",
    )
    result = refresh_metadata(stable_path, staged_path, output_path)
    assert len(result.diff.changed) == 1
    before, after = result.diff.changed[0]
    assert before.place_number == 10
    assert before.identifcation == "10-old"
    assert after.identifcation == "10"
    assert result.diff.added == []
    assert result.diff.removed == []


def test_refresh_metadata_logs_summary(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    stable_path, staged_path, output_path = _make_inputs(
        tmp_path,
        stable_rows=[_stable_row()],
        staged_rows=[_staged_row()],
    )
    with caplog.at_level(logging.INFO, logger="sendai_pipeline.refresh"):
        refresh_metadata(stable_path, staged_path, output_path)
    events = [
        record.event  # type: ignore[attr-defined]
        for record in caplog.records
        if hasattr(record, "event")
    ]
    assert "metadata_refreshed" in events
    assert "metadata_row_added" in events


def test_refresh_metadata_logs_changed_row(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    stable_path, staged_path, output_path = _make_inputs(
        tmp_path,
        stable_rows=[_stable_row()],
        staged_rows=[_staged_row(ID="105-new")],
    )
    output_path.write_text(
        STABLE_HEADER
        + "\n"
        + _stable_row()
        + "\n"
        + _stable_row(
            place_number="105",
            batch="2026",
            expected_device_type="M5Stack",
            entity_id="jp.sendai.Blesensor.per3600.105",
            identifcation="105-old",
        )
        + "\n",
        encoding="utf-8",
    )
    with caplog.at_level(logging.INFO, logger="sendai_pipeline.refresh"):
        refresh_metadata(stable_path, staged_path, output_path)
    changed_records = [
        record
        for record in caplog.records
        if getattr(record, "event", None) == "metadata_row_changed"
    ]
    assert len(changed_records) == 1
    assert getattr(changed_records[0], "place_number") == 105
    assert getattr(changed_records[0], "interval_min") == 60


def test_refresh_metadata_logs_warning_when_stable_row_changed(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    stable_path, staged_path, output_path = _make_inputs(
        tmp_path,
        stable_rows=[_stable_row(identifcation="10")],
        staged_rows=[_staged_row()],
    )
    output_path.write_text(
        STABLE_HEADER
        + "\n"
        + _stable_row(identifcation="10-old")
        + "\n"
        + _stable_row(
            place_number="105",
            batch="2026",
            expected_device_type="M5Stack",
            entity_id="jp.sendai.Blesensor.per3600.105",
            identifcation="105",
        )
        + "\n",
        encoding="utf-8",
    )
    with caplog.at_level(logging.WARNING, logger="sendai_pipeline.refresh"):
        refresh_metadata(stable_path, staged_path, output_path)
    events = [
        record.event  # type: ignore[attr-defined]
        for record in caplog.records
        if hasattr(record, "event")
    ]
    assert "stable_seed_changed" in events


def test_refresh_metadata_logs_warning_when_stable_row_added(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    # Previous output did not contain place 11; it now appears in the
    # stable seed. The stable seed is supposed to carry forward unchanged,
    # so growing it is treated as unexpected.
    stable_path, staged_path, output_path = _make_inputs(
        tmp_path,
        stable_rows=[
            _stable_row(),
            _stable_row(place_number="11", entity_id="jp.sendai.Blesensor.per3600.11"),
        ],
        staged_rows=[_staged_row()],
    )
    output_path.write_text(
        STABLE_HEADER + "\n" + _stable_row() + "\n",
        encoding="utf-8",
    )
    with caplog.at_level(logging.WARNING, logger="sendai_pipeline.refresh"):
        refresh_metadata(stable_path, staged_path, output_path)
    events = [
        record.event  # type: ignore[attr-defined]
        for record in caplog.records
        if hasattr(record, "event")
    ]
    assert "stable_seed_changed" in events


def test_refresh_metadata_logs_removed_row(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    stable_path, staged_path, output_path = _make_inputs(
        tmp_path,
        stable_rows=[_stable_row()],
        staged_rows=[],
    )
    output_path.write_text(
        STABLE_HEADER
        + "\n"
        + _stable_row()
        + "\n"
        + _stable_row(
            place_number="105",
            batch="2026",
            expected_device_type="M5Stack",
            entity_id="jp.sendai.Blesensor.per3600.105",
            identifcation="105",
        )
        + "\n",
        encoding="utf-8",
    )
    with caplog.at_level(logging.INFO, logger="sendai_pipeline.refresh"):
        refresh_metadata(stable_path, staged_path, output_path)
    events = [
        record.event  # type: ignore[attr-defined]
        for record in caplog.records
        if hasattr(record, "event")
    ]
    assert "metadata_row_removed" in events
    assert "stable_seed_changed" not in events


def test_refresh_metadata_raises_when_stable_missing(tmp_path: Path) -> None:
    stable_path = tmp_path / "missing_stable.csv"
    staged_path = _write(
        tmp_path / "sensors_refreshable.csv.staged", [STAGED_HEADER, _staged_row()]
    )
    output_path = tmp_path / "sensors.csv"
    with pytest.raises(MetadataLoadError) as excinfo:
        refresh_metadata(stable_path, staged_path, output_path)
    assert str(stable_path) in str(excinfo.value)


def test_refresh_metadata_raises_when_staged_missing(tmp_path: Path) -> None:
    stable_path = _write(
        tmp_path / "sensors_stable.csv", [STABLE_HEADER, _stable_row()]
    )
    staged_path = tmp_path / "missing_staged.csv"
    output_path = tmp_path / "sensors.csv"
    with pytest.raises(RefreshMetadataError) as excinfo:
        refresh_metadata(stable_path, staged_path, output_path)
    assert str(staged_path) in str(excinfo.value)


def test_refresh_metadata_accepts_empty_staged_with_only_header(
    tmp_path: Path,
) -> None:
    stable_path, staged_path, output_path = _make_inputs(
        tmp_path,
        stable_rows=[_stable_row()],
        staged_rows=[],
    )
    result = refresh_metadata(stable_path, staged_path, output_path)
    assert {(p.place_number, p.interval_min) for p in result.places} == {(10, 60)}


def test_refresh_metadata_result_is_dataclass_like(tmp_path: Path) -> None:
    stable_path, staged_path, output_path = _make_inputs(
        tmp_path,
        stable_rows=[_stable_row()],
        staged_rows=[_staged_row()],
    )
    result = refresh_metadata(stable_path, staged_path, output_path)
    assert isinstance(result, RefreshResult)
    assert isinstance(result.places, list)


def test_refresh_metadata_settings_from_env_uses_defaults() -> None:
    settings = RefreshMetadataSettings.from_env({})
    assert settings.output_path == Path("metadata/sensors.csv")
    assert settings.stable_path == Path("metadata/sensors_stable.csv")
    assert settings.staged_path == Path("metadata/sensors_refreshable.csv.staged")


def test_refresh_metadata_settings_from_env_uses_overrides() -> None:
    settings = RefreshMetadataSettings.from_env(
        {
            "SENSOR_METADATA_PATH": "/var/data/sensors.csv",
            "SENSOR_METADATA_STABLE_PATH": "/var/data/stable.csv",
            "SENSOR_METADATA_STAGED_PATH": "/var/data/staged.csv",
        }
    )
    assert settings.output_path == Path("/var/data/sensors.csv")
    assert settings.stable_path == Path("/var/data/stable.csv")
    assert settings.staged_path == Path("/var/data/staged.csv")


def test_refresh_metadata_settings_from_env_treats_empty_as_default() -> None:
    settings = RefreshMetadataSettings.from_env(
        {"SENSOR_METADATA_PATH": "", "SENSOR_METADATA_STABLE_PATH": ""}
    )
    assert settings.output_path == Path("metadata/sensors.csv")
    assert settings.stable_path == Path("metadata/sensors_stable.csv")


def test_main_returns_zero_on_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stable_path, staged_path, output_path = _make_inputs(
        tmp_path,
        stable_rows=[_stable_row()],
        staged_rows=[_staged_row()],
    )
    monkeypatch.setenv("SENSOR_METADATA_PATH", str(output_path))
    monkeypatch.setenv("SENSOR_METADATA_STABLE_PATH", str(stable_path))
    monkeypatch.setenv("SENSOR_METADATA_STAGED_PATH", str(staged_path))
    monkeypatch.setenv("LOG_DIR", str(tmp_path / "logs"))
    assert main([]) == EXIT_OK
    assert output_path.exists()


def test_script_entrypoint_loads_dotenv(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    stable_path, staged_path, output_path = _make_inputs(
        tmp_path,
        stable_rows=[_stable_row()],
        staged_rows=[_staged_row()],
    )
    (tmp_path / ".env").write_text(
        "\n".join(
            [
                f"SENSOR_METADATA_PATH={output_path}",
                f"SENSOR_METADATA_STABLE_PATH={stable_path}",
                f"SENSOR_METADATA_STAGED_PATH={staged_path}",
                f"LOG_DIR={tmp_path / 'logs'}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    env = os.environ.copy()
    for key in (
        "SENSOR_METADATA_PATH",
        "SENSOR_METADATA_STABLE_PATH",
        "SENSOR_METADATA_STAGED_PATH",
        "LOG_DIR",
    ):
        env.pop(key, None)
    env["PYTHONPATH"] = str(repo_root)

    completed = subprocess.run(
        [sys.executable, str(repo_root / "scripts" / "refresh_metadata.py")],
        cwd=tmp_path,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == EXIT_OK, completed.stderr
    assert output_path.exists()


def test_main_returns_validation_exit_code_on_bad_schema(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stable_path, staged_path, output_path = _make_inputs(
        tmp_path,
        stable_rows=[_stable_row()],
        staged_rows=[_staged_row(batch="")],
    )
    monkeypatch.setenv("SENSOR_METADATA_PATH", str(output_path))
    monkeypatch.setenv("SENSOR_METADATA_STABLE_PATH", str(stable_path))
    monkeypatch.setenv("SENSOR_METADATA_STAGED_PATH", str(staged_path))
    monkeypatch.setenv("LOG_DIR", str(tmp_path / "logs"))
    assert main([]) == EXIT_VALIDATION_ERROR
    assert not output_path.exists()


def test_main_returns_io_exit_code_on_missing_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SENSOR_METADATA_PATH", str(tmp_path / "out.csv"))
    monkeypatch.setenv(
        "SENSOR_METADATA_STABLE_PATH", str(tmp_path / "does_not_exist_stable.csv")
    )
    monkeypatch.setenv(
        "SENSOR_METADATA_STAGED_PATH", str(tmp_path / "does_not_exist_staged.csv")
    )
    monkeypatch.setenv("LOG_DIR", str(tmp_path / "logs"))
    assert main([]) == EXIT_IO_ERROR


def test_main_returns_io_exit_code_on_filesystem_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stable_path, staged_path, output_path = _make_inputs(
        tmp_path,
        stable_rows=[_stable_row()],
        staged_rows=[_staged_row()],
    )
    monkeypatch.setenv("SENSOR_METADATA_PATH", str(output_path))
    monkeypatch.setenv("SENSOR_METADATA_STABLE_PATH", str(stable_path))
    monkeypatch.setenv("SENSOR_METADATA_STAGED_PATH", str(staged_path))
    monkeypatch.setenv("LOG_DIR", str(tmp_path / "logs"))

    def boom(*args, **kwargs):  # type: ignore[no-untyped-def]
        raise OSError("disk full")

    monkeypatch.setattr("sendai_pipeline.refresh.os.replace", boom)
    assert main([]) == EXIT_IO_ERROR


def test_refresh_metadata_diff_added_preserves_combined_order(tmp_path: Path) -> None:
    stable_path, staged_path, output_path = _make_inputs(
        tmp_path,
        stable_rows=[
            _stable_row(place_number="10"),
            _stable_row(place_number="11", entity_id="jp.sendai.Blesensor.per3600.11"),
        ],
        staged_rows=[_staged_row(place_number="105")],
    )
    result = refresh_metadata(stable_path, staged_path, output_path)
    place_order = [p.place_number for p in result.diff.added]
    assert place_order == [10, 11, 105]


def test_refresh_metadata_places_contains_all_sensor_place_fields(
    tmp_path: Path,
) -> None:
    stable_path, staged_path, output_path = _make_inputs(
        tmp_path,
        stable_rows=[_stable_row()],
        staged_rows=[_staged_row(ID="  105  ")],
    )
    result = refresh_metadata(stable_path, staged_path, output_path)
    indexed = _by_place_interval(result.places)
    staged_place = indexed[(105, 60)]
    assert staged_place.batch == "2026"
    assert staged_place.expected_device_type == "M5Stack"
    assert staged_place.entity_type == "Blesensor.per3600"
    assert staged_place.entity_id == "jp.sendai.Blesensor.per3600.105"
    assert staged_place.identifcation == "105"
    assert staged_place.active is True
