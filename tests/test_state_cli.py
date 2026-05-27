import json
import os
from pathlib import Path

import pytest

import scripts.state_doctor as state_doctor
import scripts.state_repair as state_repair
from sendai_pipeline.state import WindowStateStore


def test_state_doctor_cli_reports_empty_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    result = state_doctor.main(["flow"])

    assert result == 0
    assert json.loads(capsys.readouterr().out) == []


def test_state_doctor_cli_warns_when_state_changes_during_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    path = tmp_path / "state" / "flow.json"
    store = WindowStateStore(path)
    store.save()

    def mutate_during_diagnose(*_args: object, **_kwargs: object) -> list[object]:
        path.write_text(
            json.dumps({"schema_version": 2, "windows": {"changed": {}}}),
            encoding="utf-8",
        )
        shifted = path.stat().st_mtime_ns + 1_000_000_000
        os.utime(path, ns=(shifted, shifted))
        return []

    monkeypatch.setattr(state_doctor, "diagnose_state", mutate_during_diagnose)

    result = state_doctor.main(["flow"])

    assert result == 0
    captured = capsys.readouterr()
    assert json.loads(captured.out) == []
    assert "WARNING: state file changed during doctor read" in captured.err


def test_state_repair_cli_reports_clean_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    result = state_repair.main(
        [
            "flow",
            "--window",
            "per3600/20260523_2200",
            "--action",
            "recompute_complete",
        ]
    )

    assert result == 2
    assert "ERROR:" in capsys.readouterr().err


def test_state_doctor_cli_reports_clean_config_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("MAX_LOOKBACK_HOURS_PER300", "0")

    result = state_doctor.main(["flow"])

    assert result == 2
    assert (
        "ERROR: MAX_LOOKBACK_HOURS_PER300 must be positive" in capsys.readouterr().err
    )
