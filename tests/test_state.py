"""Contract tests for the per-window JSON status store.

The store records the latest per-target POST result for each opaque window key.
Callers own payload construction and hashing, including removing volatile
time-of-send fields before passing the payload hash into the store.
"""

import json
import os
from collections.abc import Iterable
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import TracebackType
from typing import Any

import pytest

from sendai_pipeline.logging_setup import _ALLOWED_EXTRA_KEYS
from sendai_pipeline.state import (
    StateLoadError,
    StateValidationError,
    WindowStateStore,
)

JST = timezone(timedelta(hours=9))

T0 = datetime(2026, 5, 13, 7, 10, 0, tzinfo=JST)
T1 = datetime(2026, 5, 13, 7, 10, 5, tzinfo=JST)
T2 = datetime(2026, 5, 13, 8, 10, 0, tzinfo=JST)
T3 = datetime(2026, 5, 13, 8, 10, 5, tzinfo=JST)
T4 = datetime(2026, 5, 13, 9, 10, 0, tzinfo=JST)
T5 = datetime(2026, 5, 13, 9, 10, 5, tzinfo=JST)

WINDOW = "per3600/20260513_0700"
WINDOW_LATER = "per3600/20260513_0800"
WINDOW_EARLY = "per3600/20260513_0600"

ENTITY_10 = "jp.sendai.Blesensor.per3600.10"
ENTITY_11 = "jp.sendai.Blesensor.per3600.11"
ENTITY_12 = "jp.sendai.Blesensor.per3600.12"

HASH_A = "a" * 64
HASH_B = "b" * 64
HASH_C = "c" * 64


class Clock:
    def __init__(self, values: Iterable[datetime]) -> None:
        self._values = list(values)
        self._index = 0
        if not self._values:
            raise ValueError("Clock needs at least one value")

    def __call__(self) -> datetime:
        if self._index >= len(self._values):
            return self._values[-1]
        value = self._values[self._index]
        self._index += 1
        return value


def _state_path(tmp_path: Path) -> Path:
    return tmp_path / "state" / "flow.json"


def _target(
    *,
    status: str,
    attempted_at: datetime = T0,
    http_status: int = 204,
    payload_sha256: str = HASH_A,
) -> dict[str, object]:
    return {
        "status": status,
        "last_attempt_at": attempted_at.isoformat(),
        "last_http_status": http_status,
        "last_payload_sha256": payload_sha256,
    }


def _window(
    *,
    first_seen: datetime,
    last_attempt: datetime,
    status: str,
    attempt_count: int = 1,
    targets: dict[str, dict[str, object]] | None = None,
) -> dict[str, object]:
    return {
        "first_seen": first_seen.isoformat(),
        "last_attempt": last_attempt.isoformat(),
        "attempt_count": attempt_count,
        "targets": targets or {},
        "status": status,
    }


def _window_metadata(
    *,
    interval_min: int = 60,
    source_window_start: datetime = datetime(2026, 5, 13, 7, 0, 0, tzinfo=JST),
) -> dict[str, object]:
    return {
        "interval_min": interval_min,
        "source_window_start": source_window_start.isoformat(),
        "source_window_end": (
            source_window_start + timedelta(minutes=interval_min)
        ).isoformat(),
    }


def _write_state(path: Path, windows: dict[str, dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"windows": windows}, sort_keys=True),
        encoding="utf-8",
    )


def test_load_missing_file_returns_empty_store(tmp_path: Path) -> None:
    path = _state_path(tmp_path)

    store = WindowStateStore.load(path, now=Clock([T0]))

    assert store.as_dict() == {"schema_version": 2, "windows": {}}
    assert store.summary_counts() == {
        "pending": 0,
        "partial": 0,
        "complete": 0,
        "dead_letter": 0,
        "posts_ok": 0,
        "posts_failed": 0,
    }


@pytest.mark.parametrize("contents", ["", "{not valid json"])
def test_load_malformed_file_raises_state_load_error(
    tmp_path: Path, contents: str
) -> None:
    path = _state_path(tmp_path)
    path.parent.mkdir(parents=True)
    path.write_text(contents, encoding="utf-8")

    with pytest.raises(StateLoadError) as excinfo:
        WindowStateStore.load(path, now=Clock([T0]))

    assert str(path) in str(excinfo.value)


def test_save_round_trips_window_and_target_shape(tmp_path: Path) -> None:
    path = _state_path(tmp_path)
    store = WindowStateStore(path, now=Clock([T0, T1]))

    store.begin_window_attempt(WINDOW)
    store.record_target(
        WINDOW,
        ENTITY_10,
        status="ok",
        http_status=204,
        payload_sha256=HASH_A,
    )
    assert store.recompute_status(WINDOW, expected_target_ids=[ENTITY_10]) == "complete"
    store.save()

    expected = {
        "schema_version": 2,
        "windows": {
            WINDOW: {
                "first_seen": T0.isoformat(),
                "last_attempt": T0.isoformat(),
                "attempt_count": 1,
                **_window_metadata(),
                "expected_target_ids": [ENTITY_10],
                "targets": {
                    ENTITY_10: {
                        "status": "ok",
                        "last_attempt_at": T1.isoformat(),
                        "last_http_status": 204,
                        "last_payload_sha256": HASH_A,
                    }
                },
                "status": "complete",
            }
        },
    }
    assert json.loads(path.read_text(encoding="utf-8")) == expected

    reloaded = WindowStateStore.load(path, now=Clock([T2]))
    assert reloaded.as_dict() == expected


def test_save_uses_atomic_replace_from_sibling_temp_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = _state_path(tmp_path)
    store = WindowStateStore(path, now=Clock([T0]))
    store.begin_window_attempt(WINDOW)
    calls: list[tuple[Path, Path]] = []
    real_replace = os.replace

    def spy_replace(
        src: str | os.PathLike[str],
        dst: str | os.PathLike[str],
    ) -> None:
        calls.append((Path(src), Path(dst)))
        real_replace(src, dst)

    monkeypatch.setattr("sendai_pipeline.state.os.replace", spy_replace)

    store.save()

    assert len(calls) == 1
    src, dst = calls[0]
    assert dst == path
    assert src != path
    assert src.parent == path.parent
    assert not src.exists()


def test_save_cleans_temp_file_and_preserves_target_when_write_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = _state_path(tmp_path)
    path.parent.mkdir(parents=True)
    previous = '{"windows":{"already":"present"}}\n'
    path.write_text(previous, encoding="utf-8")
    store = WindowStateStore(path, now=Clock([T0]))
    store.begin_window_attempt(WINDOW)
    real_open = Path.open

    class FailingWriter:
        def __init__(
            self,
            temp_path: Path,
            mode: str,
            args: tuple[Any, ...],
            kwargs: dict[str, Any],
        ) -> None:
            self._handle = real_open(temp_path, mode, *args, **kwargs)

        def __enter__(self) -> "FailingWriter":
            return self

        def __exit__(
            self,
            exc_type: type[BaseException] | None,
            exc: BaseException | None,
            tb: TracebackType | None,
        ) -> bool:
            self._handle.close()
            return False

        def write(self, data: Any) -> int:
            self._handle.write(data[:8])
            self._handle.flush()
            raise OSError("disk full")

    def failing_open(
        self: Path,
        mode: str = "r",
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        if "w" in mode and self.parent == path.parent and self != path:
            return FailingWriter(self, mode, args, kwargs)
        return real_open(self, mode, *args, **kwargs)

    monkeypatch.setattr(Path, "open", failing_open)

    with pytest.raises(OSError, match="disk full"):
        store.save()

    assert path.read_text(encoding="utf-8") == previous
    assert sorted(child.name for child in path.parent.iterdir()) == ["flow.json"]


def test_record_target_rejects_unknown_status(tmp_path: Path) -> None:
    store = WindowStateStore(_state_path(tmp_path), now=Clock([T0]))
    store.begin_window_attempt(WINDOW)

    with pytest.raises(StateValidationError) as excinfo:
        store.record_target(
            WINDOW,
            ENTITY_10,
            status="skipped",
            http_status=204,
            payload_sha256=HASH_A,
        )

    assert "skipped" in str(excinfo.value)


def test_begin_window_attempt_initializes_and_increments_attempt(
    tmp_path: Path,
) -> None:
    store = WindowStateStore(_state_path(tmp_path), now=Clock([T0, T1, T2]))

    store.begin_window_attempt(WINDOW)

    data: dict[str, Any] = store.as_dict()
    window = data["windows"][WINDOW]
    assert window == {
        "first_seen": T0.isoformat(),
        "last_attempt": T0.isoformat(),
        "attempt_count": 1,
        **_window_metadata(),
        "expected_target_ids": [],
        "targets": {},
        "status": "pending",
    }

    store.record_target(
        WINDOW,
        ENTITY_10,
        status="failed",
        http_status=502,
        payload_sha256=HASH_B,
    )
    store.begin_window_attempt(WINDOW)

    data = store.as_dict()
    window = data["windows"][WINDOW]
    assert window["first_seen"] == T0.isoformat()
    assert window["last_attempt"] == T2.isoformat()
    assert window["attempt_count"] == 2
    assert window["status"] == "pending"
    assert window["targets"][ENTITY_10]["status"] == "failed"


def test_record_target_overwrites_previous_result(tmp_path: Path) -> None:
    store = WindowStateStore(
        _state_path(tmp_path),
        now=Clock([T0, T1, T2, T3, T4, T5]),
    )

    store.begin_window_attempt(WINDOW)
    store.record_target(
        WINDOW,
        ENTITY_10,
        status="failed",
        http_status=502,
        payload_sha256=HASH_B,
    )
    assert store.recompute_status(WINDOW, expected_target_ids=[ENTITY_10]) == "partial"

    store.begin_window_attempt(WINDOW)
    store.record_target(
        WINDOW,
        ENTITY_10,
        status="ok",
        http_status=204,
        payload_sha256=HASH_A,
    )
    assert store.recompute_status(WINDOW, expected_target_ids=[ENTITY_10]) == "complete"
    assert store.target_record(WINDOW, ENTITY_10) == {
        "status": "ok",
        "last_attempt_at": T3.isoformat(),
        "last_http_status": 204,
        "last_payload_sha256": HASH_A,
    }
    assert store.summary_counts()["posts_ok"] == 1
    assert store.summary_counts()["posts_failed"] == 0

    store.begin_window_attempt(WINDOW)
    store.record_target(
        WINDOW,
        ENTITY_10,
        status="failed",
        http_status=503,
        payload_sha256=HASH_C,
    )
    assert store.recompute_status(WINDOW, expected_target_ids=[ENTITY_10]) == "partial"
    assert store.target_record(WINDOW, ENTITY_10) == {
        "status": "failed",
        "last_attempt_at": T5.isoformat(),
        "last_http_status": 503,
        "last_payload_sha256": HASH_C,
    }
    assert store.summary_counts()["posts_ok"] == 0
    assert store.summary_counts()["posts_failed"] == 1


def test_recompute_status_marks_complete_only_when_all_expected_targets_ok(
    tmp_path: Path,
) -> None:
    store = WindowStateStore(_state_path(tmp_path), now=Clock([T0, T1, T2]))
    store.begin_window_attempt(WINDOW)
    store.record_target(
        WINDOW,
        ENTITY_10,
        status="ok",
        http_status=204,
        payload_sha256=HASH_A,
    )
    store.record_target(
        WINDOW,
        ENTITY_11,
        status="ok",
        http_status=204,
        payload_sha256=HASH_B,
    )

    status = store.recompute_status(
        WINDOW,
        expected_target_ids=[ENTITY_10, ENTITY_11],
    )

    assert status == "complete"
    assert store.as_dict()["windows"][WINDOW]["status"] == "complete"


def test_recompute_status_marks_missing_expected_target_partial(
    tmp_path: Path,
) -> None:
    store = WindowStateStore(_state_path(tmp_path), now=Clock([T0, T1]))
    store.begin_window_attempt(WINDOW)
    store.record_target(
        WINDOW,
        ENTITY_10,
        status="ok",
        http_status=204,
        payload_sha256=HASH_A,
    )

    status = store.recompute_status(
        WINDOW,
        expected_target_ids=[ENTITY_10, ENTITY_11],
    )

    assert status == "partial"
    assert store.summary_counts()["posts_ok"] == 1
    assert store.summary_counts()["posts_failed"] == 0


def test_gc_complete_before_removes_only_old_complete_windows(tmp_path: Path) -> None:
    path = _state_path(tmp_path)
    _write_state(
        path,
        {
            "old_complete": _window(
                first_seen=T0,
                last_attempt=T0,
                status="complete",
            ),
            "recent_complete": _window(
                first_seen=T0,
                last_attempt=T4,
                status="complete",
            ),
            "old_dead_letter": _window(
                first_seen=T0,
                last_attempt=T0,
                status="dead_letter",
            ),
            "old_partial": _window(
                first_seen=T0,
                last_attempt=T0,
                status="partial",
            ),
            "old_pending": _window(
                first_seen=T0,
                last_attempt=T0,
                status="pending",
            ),
        },
    )
    store = WindowStateStore.load(path, now=Clock([T5]))

    removed = store.gc_complete_before(T2)

    assert removed == 1
    assert set(store.as_dict()["windows"]) == {
        "recent_complete",
        "old_dead_letter",
        "old_partial",
        "old_pending",
    }


def test_iter_open_windows_returns_pending_and_partial_sorted_by_first_seen(
    tmp_path: Path,
) -> None:
    path = _state_path(tmp_path)
    _write_state(
        path,
        {
            WINDOW_LATER: _window(
                first_seen=T2,
                last_attempt=T3,
                status="partial",
            ),
            WINDOW: _window(
                first_seen=T1,
                last_attempt=T1,
                status="pending",
            ),
            WINDOW_EARLY: _window(
                first_seen=T0,
                last_attempt=T0,
                status="complete",
            ),
            "per3600/20260513_0500": _window(
                first_seen=T0,
                last_attempt=T0,
                status="dead_letter",
            ),
        },
    )
    store = WindowStateStore.load(path, now=Clock([T5]))

    keys = [key for key, _window_data in store.iter_open_windows()]

    assert keys == [WINDOW, WINDOW_LATER]


def test_summary_counts_returns_window_and_target_status_totals(
    tmp_path: Path,
) -> None:
    path = _state_path(tmp_path)
    _write_state(
        path,
        {
            "pending_window": _window(
                first_seen=T0,
                last_attempt=T0,
                status="pending",
            ),
            "partial_window": _window(
                first_seen=T0,
                last_attempt=T1,
                status="partial",
                targets={
                    ENTITY_10: _target(status="ok", attempted_at=T1),
                    ENTITY_11: _target(
                        status="failed",
                        attempted_at=T1,
                        http_status=502,
                        payload_sha256=HASH_B,
                    ),
                },
            ),
            "complete_window": _window(
                first_seen=T0,
                last_attempt=T2,
                status="complete",
                targets={
                    ENTITY_10: _target(status="ok", attempted_at=T2),
                    ENTITY_12: _target(
                        status="ok",
                        attempted_at=T2,
                        payload_sha256=HASH_C,
                    ),
                },
            ),
            "dead_letter_window": _window(
                first_seen=T0,
                last_attempt=T3,
                status="dead_letter",
                targets={
                    ENTITY_11: _target(
                        status="failed",
                        attempted_at=T3,
                        http_status=404,
                    )
                },
            ),
        },
    )
    store = WindowStateStore.load(path, now=Clock([T5]))

    assert store.summary_counts() == {
        "pending": 1,
        "partial": 1,
        "complete": 1,
        "dead_letter": 1,
        "posts_ok": 3,
        "posts_failed": 2,
    }


def test_logging_allowlist_contains_fields_state_callers_use() -> None:
    required = {
        "event",
        "window",
        "entity_id",
        "http_status",
        "payload_sha256",
        "attempts",
        "path",
    }

    assert required <= _ALLOWED_EXTRA_KEYS


def test_pending_target_status_does_not_satisfy_completion(tmp_path: Path) -> None:
    store = WindowStateStore(_state_path(tmp_path), now=Clock([T0, T1]))
    store.begin_window_attempt(WINDOW)
    store.record_target(
        WINDOW,
        ENTITY_10,
        status="pending",
        http_status=0,
        payload_sha256=HASH_A,
    )

    status = store.recompute_status(WINDOW, expected_target_ids=[ENTITY_10])

    assert status == "partial"
    counts = store.summary_counts()
    assert counts["posts_ok"] == 0
    assert counts["posts_failed"] == 0
    assert counts["partial"] == 1


def test_begin_window_attempt_records_explicit_source_metadata_and_expected_targets(
    tmp_path: Path,
) -> None:
    source_start = datetime(2026, 5, 13, 7, 0, 0, 123456, tzinfo=JST)
    store = WindowStateStore(_state_path(tmp_path), now=Clock([T0]))

    store.begin_window_attempt(
        WINDOW,
        interval_min=60,
        source_window_start=source_start,
        source_window_end=source_start + timedelta(hours=1),
        expected_target_ids=[ENTITY_11, ENTITY_10, ENTITY_10],
    )

    window = store.as_dict()["windows"][WINDOW]
    assert window["interval_min"] == 60
    assert window["source_window_start"] == "2026-05-13T07:00:00+09:00"
    assert window["source_window_end"] == "2026-05-13T08:00:00+09:00"
    assert window["expected_target_ids"] == [ENTITY_10, ENTITY_11]


def test_load_legacy_state_derives_source_window_start_from_key(tmp_path: Path) -> None:
    path = _state_path(tmp_path)
    _write_state(
        path,
        {
            WINDOW: _window(
                first_seen=T2,
                last_attempt=T2,
                status="partial",
            )
        },
    )
    store = WindowStateStore.load(path, now=Clock([T5]))

    assert store.as_dict()["schema_version"] == 1
    assert store.source_window_start(WINDOW) == datetime(
        2026, 5, 13, 7, 0, 0, tzinfo=JST
    )
    assert store.retry_anchor(WINDOW, store.as_dict()["windows"][WINDOW]) == datetime(
        2026, 5, 13, 7, 0, 0, tzinfo=JST
    )


def test_recompute_status_rejects_changed_expected_target_snapshot(
    tmp_path: Path,
) -> None:
    store = WindowStateStore(_state_path(tmp_path), now=Clock([T0, T1]))
    store.begin_window_attempt(WINDOW, expected_target_ids=[ENTITY_10])
    store.record_target(
        WINDOW,
        ENTITY_10,
        status="ok",
        http_status=204,
        payload_sha256=HASH_A,
    )

    with pytest.raises(StateValidationError, match="expected targets changed"):
        store.recompute_status(WINDOW, expected_target_ids=[ENTITY_10, ENTITY_11])


def test_recompute_status_rejects_empty_expected_target_set(tmp_path: Path) -> None:
    store = WindowStateStore(_state_path(tmp_path), now=Clock([T0]))
    store.begin_window_attempt(WINDOW)

    with pytest.raises(StateValidationError, match="no expected targets"):
        store.recompute_status(WINDOW)


def test_begin_window_attempt_rejects_changed_expected_targets_on_retry(
    tmp_path: Path,
) -> None:
    store = WindowStateStore(_state_path(tmp_path), now=Clock([T0, T1]))
    store.begin_window_attempt(WINDOW, expected_target_ids=[ENTITY_10])

    with pytest.raises(StateValidationError, match="expected targets changed"):
        store.begin_window_attempt(WINDOW, expected_target_ids=[ENTITY_11])


def test_begin_window_attempt_rejects_dead_letter_retry(tmp_path: Path) -> None:
    path = _state_path(tmp_path)
    _write_state(
        path,
        {
            WINDOW: _window(
                first_seen=T0,
                last_attempt=T0,
                status="dead_letter",
            )
        },
    )
    store = WindowStateStore.load(path, now=Clock([T1]))

    with pytest.raises(StateValidationError, match="dead-letter"):
        store.begin_window_attempt(WINDOW)
