import json
import os
from pathlib import Path

import pytest

import scripts.state_doctor as state_doctor
import scripts.state_repair as state_repair
from sendai_pipeline.state import WindowStateStore
from sendai_pipeline.state_tools import StateDoctorReport


def test_state_doctor_cli_reports_empty_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    result = state_doctor.main(["flow"])

    assert result == 0
    output = json.loads(capsys.readouterr().out)
    assert output["product"] == "flow"
    assert output["total_windows"] == 0
    assert output["open_windows"] == []


def test_state_doctor_cli_warns_when_state_changes_during_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    path = tmp_path / "state" / "flow.json"
    store = WindowStateStore(path)
    store.save()

    def mutate_during_report(*_args: object, **_kwargs: object) -> StateDoctorReport:
        path.write_text(
            json.dumps({"schema_version": 2, "windows": {"changed": {}}}),
            encoding="utf-8",
        )
        shifted = path.stat().st_mtime_ns + 1_000_000_000
        os.utime(path, ns=(shifted, shifted))
        return StateDoctorReport(
            product="flow",
            status_counts={
                "complete": 0,
                "partial": 0,
                "pending": 0,
                "dead_letter": 0,
                "unknown": 0,
            },
            open_windows=(),
            missing_targets=(),
            failed_targets=(),
            failed_http_status_counts=(),
        )

    monkeypatch.setattr(state_doctor, "build_state_report", mutate_during_report)

    result = state_doctor.main(["flow"])

    assert result == 0
    captured = capsys.readouterr()
    assert json.loads(captured.out)["open_windows"] == []
    assert "WARNING: state file changed during doctor read" in captured.err


def test_state_doctor_cli_pretty_reports_dashboard_with_unicode_bars(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    path = tmp_path / "state" / "flow.json"
    store = WindowStateStore(path)
    store.begin_window_attempt(
        "per300/20260525_0640",
        expected_target_ids=[
            "jp.sendai.Blesensor.per300.101",
            "jp.sendai.Blesensor.per300.102",
        ],
    )
    store.record_target(
        "per300/20260525_0640",
        "jp.sendai.Blesensor.per300.102",
        status="failed",
        http_status=502,
        payload_sha256="a" * 64,
    )
    store.recompute_status(
        "per300/20260525_0640",
        [
            "jp.sendai.Blesensor.per300.101",
            "jp.sendai.Blesensor.per300.102",
        ],
    )
    store.save()

    result = state_doctor.main(["flow", "--pretty"])

    assert result == 0
    output = capsys.readouterr().out
    assert "State doctor: flow" in output
    assert "Status overview" in output
    assert "Open windows" in output
    assert "Top missing targets" in output
    assert "▒ partial" in output


def test_state_doctor_cli_pretty_can_use_ascii_bars(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    path = tmp_path / "state" / "flow.json"
    store = WindowStateStore(path)
    store.begin_window_attempt(
        "per300/20260525_0640",
        expected_target_ids=["jp.sendai.Blesensor.per300.101"],
    )
    store.save()

    result = state_doctor.main(["flow", "--pretty", "--ascii"])

    assert result == 0
    output = capsys.readouterr().out
    assert "N" in output
    assert "N pending" in output
    assert "█" not in output


def test_state_doctor_cli_pretty_limits_windows_and_all_shows_them(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    path = tmp_path / "state" / "flow.json"
    store = WindowStateStore(path)
    for window_key in ("per300/20260525_0640", "per300/20260525_0645"):
        store.begin_window_attempt(
            window_key,
            expected_target_ids=["jp.sendai.Blesensor.per300.101"],
        )
        store.recompute_status(
            window_key,
            ["jp.sendai.Blesensor.per300.101"],
        )
    store.save()

    limited = state_doctor.main(["flow", "--pretty", "--window-limit", "1"])
    limited_output = capsys.readouterr().out
    all_rows = state_doctor.main(["flow", "--pretty", "--window-limit", "1", "--all"])
    all_output = capsys.readouterr().out

    assert limited == 0
    assert all_rows == 0
    assert "... 1 more open windows hidden; rerun with --all" in limited_output
    assert "per300/20260525_0645  partial" not in limited_output
    assert "hidden; rerun with --all" not in all_output
    assert "per300/20260525_0645  partial" in all_output


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
