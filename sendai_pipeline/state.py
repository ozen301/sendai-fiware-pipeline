"""Per-window JSON state storage for pipeline delivery attempts."""

import json
import logging
import os
from collections.abc import Callable, Iterable, Iterator, Mapping
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

logger = logging.getLogger(__name__)

JST = timezone(timedelta(hours=9))
SCHEMA_VERSION = 2

_VALID_TARGET_STATUSES: frozenset[str] = frozenset({"ok", "failed", "pending"})
_WINDOW_STATUSES: tuple[str, ...] = ("pending", "partial", "complete", "dead_letter")
_WINDOW_PREFIX_INTERVALS: dict[str, int] = {"per300": 5, "per3600": 60}


class StateLoadError(RuntimeError):
    """Raised when window state cannot be loaded from disk."""


class StateValidationError(RuntimeError):
    """Raised when an invalid state update is requested."""


class WindowStateStore:
    """JSON-backed state store for per-window target POST results.

    The store keeps one mutable in-memory dictionary and writes it to disk
    atomically on :meth:`save`.
    """

    def __init__(
        self,
        path: Path,
        *,
        now: Callable[[], datetime] = lambda: datetime.now(JST),
    ) -> None:
        """Create an empty state store at ``path``."""
        self.path = Path(path)
        self._now = now
        self._state: dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "windows": {},
        }

    @classmethod
    def load(
        cls,
        path: Path,
        *,
        now: Callable[[], datetime] = lambda: datetime.now(JST),
    ) -> "WindowStateStore":
        """Load a state store from disk, or return an empty store if missing.

        Args:
            path: JSON state file path.
            now: Clock used for future state updates.

        Returns:
            Loaded or empty store.

        Raises:
            StateLoadError: If the file exists but cannot be read or parsed.
        """
        store = cls(path, now=now)
        if not store.path.exists():
            return store

        try:
            contents = store.path.read_text(encoding="utf-8")
            if contents == "":
                raise StateLoadError(f"state file is empty: {store.path}")
            data = json.loads(contents)
        except StateLoadError:
            raise
        except (OSError, json.JSONDecodeError) as exc:
            raise StateLoadError(
                f"could not load state file {store.path}: {exc}"
            ) from exc

        if not isinstance(data, Mapping):
            raise StateLoadError(f"state file must contain a JSON object: {store.path}")

        windows = data.get("windows")
        if not isinstance(windows, Mapping):
            raise StateLoadError(f"state file is missing windows object: {store.path}")

        schema_version = data.get("schema_version", 1)
        if not isinstance(schema_version, int):
            raise StateLoadError(
                f"state file schema_version must be an integer: {store.path}"
            )

        store._state = {
            "schema_version": schema_version,
            "windows": dict(windows),
        }
        logger.debug(
            "loaded window state",
            extra={"event": "state_loaded", "path": str(store.path)},
        )
        return store

    def as_dict(self) -> dict[str, Any]:
        """Return the live JSON-shaped state dictionary."""
        return self._state

    def save(self) -> None:
        """Atomically write the current state to disk."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._state["schema_version"] = SCHEMA_VERSION
        temp_path = self.path.with_name(f".{self.path.name}.{uuid4().hex}.tmp")

        try:
            with temp_path.open("w", encoding="utf-8") as temp_file:
                json.dump(
                    self._state,
                    temp_file,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                temp_file.write("\n")
            os.replace(temp_path, self.path)
        except Exception:
            temp_path.unlink(missing_ok=True)
            raise

        logger.debug(
            "saved window state",
            extra={"event": "state_saved", "path": str(self.path)},
        )

    def begin_window_attempt(
        self,
        window_key: str,
        *,
        interval_min: int | None = None,
        source_window_start: datetime | None = None,
        source_window_end: datetime | None = None,
        expected_target_ids: Iterable[str] | None = None,
    ) -> None:
        """Start or retry a window attempt.

        For new windows, this creates the full window record and snapshots the
        expected target set.  For retries, it increments ``attempt_count`` and
        resets ``status`` to ``"pending"``.

        Args:
            window_key: Stable key for the source aggregation window.
            interval_min: Source aggregation interval. Derived from the key
                for legacy callers when omitted.
            source_window_start: Inclusive source window start. Derived from
                the key when omitted.
            source_window_end: Exclusive source window end. Derived from start
                plus interval when omitted.
            expected_target_ids: Entity IDs expected for this source window.
                ``None`` or an empty iterable are both treated as "not
                provided": new windows store an empty list; retries preserve
                the stored snapshot.  A non-empty iterable replaces the stored
                snapshot on a retry, so callers that want to expand the
                expected set should pass the full new set explicitly.
        """
        timestamp = self._now().isoformat()
        windows = self._windows()
        window = windows.get(window_key)
        metadata = self._window_metadata(
            window_key,
            interval_min=interval_min,
            source_window_start=source_window_start,
            source_window_end=source_window_end,
        )
        expected = _normalized_expected_target_ids(expected_target_ids)

        if window is None:
            windows[window_key] = {
                "first_seen": timestamp,
                "last_attempt": timestamp,
                "attempt_count": 1,
                **metadata,
                "expected_target_ids": expected,
                "targets": {},
                "status": "pending",
            }
            return

        if window.get("status") == "dead_letter":
            raise StateValidationError(f"cannot retry dead-letter window: {window_key}")

        self._ensure_window_metadata(window_key, window, metadata)
        if expected:
            window["expected_target_ids"] = expected
        else:
            window.setdefault("expected_target_ids", [])

        window["last_attempt"] = timestamp
        window["attempt_count"] = int(window.get("attempt_count", 0)) + 1
        window["status"] = "pending"
        window.setdefault("targets", {})

    def record_target(
        self,
        window_key: str,
        entity_id: str,
        *,
        status: str,
        http_status: int,
        payload_sha256: str,
    ) -> None:
        """Record the latest POST result for one target entity."""
        if status not in _VALID_TARGET_STATUSES:
            raise StateValidationError(f"invalid target status: {status}")

        window = self._window(window_key)
        targets = window.setdefault("targets", {})
        targets[entity_id] = {
            "status": status,
            "last_attempt_at": self._now().isoformat(),
            "last_http_status": http_status,
            "last_payload_sha256": payload_sha256,
        }

    def target_record(self, window_key: str, entity_id: str) -> dict[str, Any] | None:
        """Return the latest target record, or ``None`` if absent."""
        window = self._windows().get(window_key)
        if window is None:
            return None
        target = window.get("targets", {}).get(entity_id)
        return target if isinstance(target, dict) else None

    def expected_target_ids(self, window_key: str) -> list[str] | None:
        """Return a window's stored expected target snapshot, if present."""
        window = self._windows().get(window_key)
        if window is None:
            return None
        expected = window.get("expected_target_ids")
        if not isinstance(expected, list):
            return None
        if not all(isinstance(entity_id, str) for entity_id in expected):
            return None
        return list(expected)

    def recompute_status(
        self,
        window_key: str,
        expected_target_ids: Iterable[str] | None = None,
    ) -> str:
        """Recompute, store, and return a window's aggregate status.

        The *aggregate status* summarises all target deliveries for the window:

        - ``"complete"`` — every expected target has ``status == "ok"``.
        - ``"partial"`` — at least one expected target does not have
          ``status == "ok"`` (either ``"failed"`` or still ``"pending"``).

        Dead-letter promotion is done separately via the repair tool; this
        method never sets ``"dead_letter"``.

        Args:
            window_key: Stable key for the source aggregation window.
            expected_target_ids: Optional expected entity IDs. A non-empty
                value replaces the stored snapshot; ``None`` or an empty
                iterable uses the stored snapshot.

        Returns:
            The updated aggregate window status (``"complete"`` or
            ``"partial"``).

        Raises:
            StateValidationError: If the window has not been started or the
                effective expected target set is empty.
        """
        window = self._window(window_key)
        targets = window.setdefault("targets", {})
        if expected_target_ids is None:
            stored_expected = window.get("expected_target_ids")
            expected = list(stored_expected) if stored_expected is not None else []
        else:
            expected = _normalized_expected_target_ids(expected_target_ids)
        if expected:
            window["expected_target_ids"] = expected

        if not expected:
            raise StateValidationError(
                f"cannot recompute window with no expected targets: {window_key}"
            )

        all_expected_ok = all(
            targets.get(entity_id, {}).get("status") == "ok" for entity_id in expected
        )

        status = "complete" if all_expected_ok else "partial"
        window["status"] = status
        return status

    def gc_complete_before(self, cutoff: datetime) -> int:
        """Remove complete windows whose source start is strictly before cutoff.

        Args:
            cutoff: Source-window start cutoff. Complete windows older than
                this timestamp are removed.  Windows whose source start equals
                the cutoff are kept (strict ``<`` comparison), so the cutoff
                boundary window is never prematurely dropped.

        Returns:
            Number of complete windows removed.
        """
        windows = self._windows()
        remove_keys = [
            key
            for key, window in windows.items()
            if window.get("status") == "complete"
            and self.source_window_start(key, window) < cutoff
        ]

        for key in remove_keys:
            del windows[key]

        if remove_keys:
            logger.debug(
                "removed old complete windows",
                extra={"event": "state_gc_swept", "attempts": len(remove_keys)},
            )
        return len(remove_keys)

    def iter_open_windows(self) -> Iterator[tuple[str, dict[str, Any]]]:
        """Yield pending and partial windows sorted by retry anchor time.

        Windows are sorted oldest-first by ``retry_anchor``, so callers that
        expand the lookback horizon to cover the oldest open window see the
        most urgent windows first.

        The yielded window dictionaries are live state. Callers should treat
        them as read-only.
        """
        open_windows = [
            (key, window)
            for key, window in self._windows().items()
            if window.get("status") in {"pending", "partial"}
        ]
        open_windows.sort(key=lambda item: self.retry_anchor(item[0], item[1]))
        return iter(open_windows)

    def iter_complete_windows(self) -> Iterator[tuple[str, dict[str, Any]]]:
        """Yield complete windows sorted by source start time, then key.

        Windows are sorted oldest-first by source start time; the key is used
        as a tie-breaker so the order is fully deterministic.  This ordering
        lets the supplemental-complete pass in ``run_flow`` process earlier
        windows first.

        The yielded window dictionaries are live state. Callers should treat
        them as read-only.

        Returns:
            Iterator of ``(window_key, window)`` pairs.
        """
        complete_windows = [
            (key, window)
            for key, window in self._windows().items()
            if window.get("status") == "complete"
        ]
        complete_windows.sort(
            key=lambda item: (self.source_window_start(*item), item[0])
        )
        return iter(complete_windows)

    def source_window_start(
        self,
        window_key: str,
        window: Mapping[str, Any] | None = None,
    ) -> datetime:
        """Return a window's stored or legacy-derived source start time.

        Preferred path: reads ``source_window_start`` from the window dict (an
        ISO-formatted string written by ``begin_window_attempt``).  Fallback
        path (legacy windows that pre-date the field): derives the start time
        directly from the ``YYYYMMDD_HHMM`` portion of the window key via
        ``_derive_source_window_metadata``.
        """
        data = self._windows().get(window_key) if window is None else window
        if isinstance(data, Mapping):
            raw_start = data.get("source_window_start")
            if isinstance(raw_start, str):
                return datetime.fromisoformat(raw_start)
        return _derive_source_window_metadata(window_key)["source_window_start"]

    def retry_anchor(self, window_key: str, window: Mapping[str, Any]) -> datetime:
        """Return the timestamp used to size retry lookback for one window.

        The anchor is ``min(first_seen, source_window_start)``.  Taking the
        minimum ensures that windows first seen very recently but whose source
        data is old are still reached by a sufficiently wide lookback, and that
        windows with a recent source start but an old ``first_seen`` (e.g.
        discovered late) expand the lookback to cover their first attempt time.
        """
        first_seen = datetime.fromisoformat(str(window["first_seen"]))
        source_start = self.source_window_start(window_key, window)
        return min(first_seen, source_start)

    def summary_counts(self) -> dict[str, int]:
        """Return window status totals and retained target result totals."""
        counts = {
            "pending": 0,
            "partial": 0,
            "complete": 0,
            "dead_letter": 0,
            "posts_ok": 0,
            "posts_failed": 0,
        }

        for window in self._windows().values():
            status = window.get("status")
            if status in _WINDOW_STATUSES:
                counts[status] += 1

            for target in window.get("targets", {}).values():
                target_status = target.get("status")
                if target_status == "ok":
                    counts["posts_ok"] += 1
                elif target_status == "failed":
                    counts["posts_failed"] += 1

        return counts

    def _windows(self) -> dict[str, dict[str, Any]]:
        """Return the live ``windows`` mapping from internal state.

        Each value is a per-window dict with the following keys:

        - ``first_seen`` (str, ISO): timestamp when the window was first
          observed by ``begin_window_attempt``.
        - ``last_attempt`` (str, ISO): timestamp of the most recent attempt.
        - ``attempt_count`` (int): number of times ``begin_window_attempt``
          has been called for this window.
        - ``expected_target_ids`` (list[str]): entity IDs expected to be
          delivered for this window, e.g.
          ``["jp.sendai.Blesensor.per300.10", "jp.sendai.Blesensor.per300.11"]``.
        - ``targets`` (dict): per-entity delivery records; each value has
          ``status``, ``last_attempt_at``, ``last_http_status``, and
          ``last_payload_sha256``.
        - ``status`` (str): aggregate window status — one of ``"pending"``,
          ``"partial"``, ``"complete"``, or ``"dead_letter"``.
        - ``interval_min`` (int): aggregation interval in minutes.
        - ``source_window_start`` (str, ISO): inclusive start of the source
          aggregation window.
        - ``source_window_end`` (str, ISO): exclusive end of the source
          aggregation window.

        The last three keys (``interval_min``, ``source_window_start``,
        ``source_window_end``) are window metadata. Windows written by an
        older code version may lack them until a later run backfills them
        via ``_ensure_window_metadata``; readers that need them should go
        through ``source_window_start`` / ``_window_metadata``, which derive
        a fallback when the stored value is missing.

        Example::

            {
                "per300/20240601_1000": {
                    "first_seen": "2024-06-01T10:05:00+09:00",
                    "last_attempt": "2024-06-01T10:10:00+09:00",
                    "attempt_count": 2,
                    "expected_target_ids": ["jp.sendai.Blesensor.per300.10"],
                    "targets": {
                        "jp.sendai.Blesensor.per300.10": {
                            "status": "ok",
                            "last_attempt_at": "2024-06-01T10:10:00+09:00",
                            "last_http_status": 204,
                            "last_payload_sha256": "abc123...",
                        }
                    },
                    "status": "complete",
                    "interval_min": 5,
                    "source_window_start": "2024-06-01T10:00:00+09:00",
                    "source_window_end": "2024-06-01T10:05:00+09:00",
                }
            }
        """
        return self._state["windows"]

    def _window(self, window_key: str) -> dict[str, Any]:
        windows = self._windows()
        if window_key not in windows:
            raise StateValidationError(f"window has not been started: {window_key}")
        return windows[window_key]

    def _window_metadata(
        self,
        window_key: str,
        *,
        interval_min: int | None,
        source_window_start: datetime | None,
        source_window_end: datetime | None,
    ) -> dict[str, Any]:
        """Build the metadata fields stored on a window record.

        When ``interval_min`` or ``source_window_start`` is ``None``, falls
        back to deriving both from the window key (legacy callers that do not
        supply these explicitly).  ``source_window_end`` is computed from
        start plus interval when not provided.

        Returns:
            Dict with ``interval_min``, ``source_window_start`` (ISO str),
            and ``source_window_end`` (ISO str).
        """
        if source_window_start is None or interval_min is None:
            derived = _derive_source_window_metadata(window_key)
            if source_window_start is None:
                source_window_start = derived["source_window_start"]
            if interval_min is None:
                interval_min = int(derived["interval_min"])
        if source_window_start is None or interval_min is None:
            raise StateValidationError(f"cannot derive source metadata: {window_key}")
        if source_window_end is None:
            source_window_end = source_window_start + timedelta(minutes=interval_min)
        return {
            "interval_min": interval_min,
            "source_window_start": _without_microseconds(
                source_window_start
            ).isoformat(),
            "source_window_end": _without_microseconds(source_window_end).isoformat(),
        }

    def _ensure_window_metadata(
        self,
        window_key: str,
        window: dict[str, Any],
        metadata: Mapping[str, Any],
    ) -> None:
        """Back-fill missing metadata fields on a legacy window record.

        Writes ``interval_min``, ``source_window_start``, and
        ``source_window_end`` into *window* only when those keys are absent,
        so existing values are never overwritten.  The caller-supplied
        *metadata* dict is applied first; any keys it does not cover are
        derived from the window key.
        """
        derived = _derive_source_window_metadata(window_key)
        for key, value in metadata.items():
            if key not in window:
                window[key] = value
        if "interval_min" not in window:
            window["interval_min"] = derived["interval_min"]
        if "source_window_start" not in window:
            window["source_window_start"] = derived["source_window_start"].isoformat()
        if "source_window_end" not in window:
            window["source_window_end"] = derived["source_window_end"].isoformat()


def _normalized_expected_target_ids(
    expected_target_ids: Iterable[str] | None,
) -> list[str]:
    if expected_target_ids is None:
        return []
    return sorted(set(expected_target_ids))


def _without_microseconds(value: datetime) -> datetime:
    return value.replace(microsecond=0)


def _derive_source_window_metadata(window_key: str) -> dict[str, Any]:
    prefix, separator, raw_start = window_key.partition("/")
    if not separator or prefix not in _WINDOW_PREFIX_INTERVALS:
        raise StateValidationError(
            f"cannot derive source window metadata: {window_key}"
        )
    interval_min = _WINDOW_PREFIX_INTERVALS[prefix]
    try:
        source_start = datetime.strptime(raw_start, "%Y%m%d_%H%M").replace(tzinfo=JST)
    except ValueError as exc:
        raise StateValidationError(
            f"cannot derive source window metadata: {window_key}"
        ) from exc
    source_end = source_start + timedelta(minutes=interval_min)
    return {
        "interval_min": interval_min,
        "source_window_start": source_start,
        "source_window_end": source_end,
    }
