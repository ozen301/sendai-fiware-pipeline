"""Logging configuration for the sendai-pipeline package.

Library modules just emit records via ``logging.getLogger(__name__)``; entry
points call :func:`configure_logging` once at startup (after acquiring the
per-product run lock that the rotating file handler will share). The
configuration installs two handlers on the ``sendai_pipeline`` package
logger:

- A ``RotatingFileHandler`` writing one JSON object per line to
  ``{log_dir}/{product}.log``. Full ``DEBUG``/``INFO`` detail.
- A ``StreamHandler`` on stdout at ``WARNING`` and above. Keeps cron mail
  and the cron-redirected stdout file bounded by warning/error volume.

The :class:`JsonFormatter` builds output from an allowlist of ``LogRecord``
attributes plus explicitly accepted ``extra`` keys (see
``_ALLOWED_EXTRA_KEYS``). Unknown ``extra`` keys are dropped with a one-time
warning to catch typos without flooding logs. Output is sanitized by
:class:`SecretsFilter` before either handler sees it.

Timestamps are emitted in JST (``+09:00``) and are always offset-aware.
"""

import hashlib
import json
import logging
import os
import re
import sys
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Self

PACKAGE_LOGGER_NAME = "sendai_pipeline"

JST = timezone(timedelta(hours=9))

_VALID_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
_VALID_FORMATS = {"json", "text"}
_VALID_PAYLOAD_MODES = {"hash", "failure", "full"}

# Allowlisted ``extra`` keys for structured JSON output. Anything outside this
# set is dropped from the JSON payload (with a one-time warning).
_ALLOWED_EXTRA_KEYS: frozenset[str] = frozenset(
    {
        "event",
        "product",
        "run_id",
        "send_mode",
        "target_batches",
        "interval",
        "interval_min",
        "place_number",
        "group_place_id",
        "from_group_place_id",
        "to_group_place_id",
        "device_type",
        "from_device_type",
        "to_device_type",
        "expected_device_type",
        "matched_prefix",
        "window",
        "entity_id",
        "http_status",
        "ok",
        "attempts",
        "elapsed_ms",
        "payload_sha256",
        "prior_payload_sha256",
        "computed_payload_sha256",
        "payload_bytes",
        "payload",
        "response_excerpt",
        "error_type",
        "payload_mode",
        "all_rows_present",
        "before",
        "after",
        "dry_run",
        "entity_type",
        "subscription_id",
        "count_expected",
        "count_live",
        "count_missing",
        "count_extra",
        "limit",
        "path",
        "backup_path",
        "reason",
        "rows",
        "source_window_start",
        "source_window_end",
        "source_max_imputation_tier",
        "target_status_category",
        "retry_reachable",
        "lookback_hours_used",
        "oldest_non_complete",
        "windows_seen",
        "windows_complete",
        "windows_partial",
        "windows_dead_letter",
        "posts_ok",
        "posts_failed",
        "windows_empty",
        "windows_gc",
        "cutoff",
        "rows_dropped",
        "count_would_create",
        "count_created",
        "count_skipped",
        "count_failed",
        "peer_product",
        "peer_trigger_attrs",
        "phase",
        "deleted",
        "absent",
    }
)

# Built-in ``LogRecord`` attribute names. Anything in ``record.__dict__`` that
# is *not* in this set is treated as a user-supplied ``extra`` key.
_RESERVED_LOGRECORD_ATTRS: frozenset[str] = frozenset(
    {
        "name",
        "msg",
        "args",
        "levelname",
        "levelno",
        "pathname",
        "filename",
        "module",
        "exc_info",
        "exc_text",
        "stack_info",
        "lineno",
        "funcName",
        "created",
        "msecs",
        "relativeCreated",
        "thread",
        "threadName",
        "processName",
        "process",
        "message",
        "asctime",
        "taskName",
    }
)

_REDACT_KEYS: frozenset[str] = frozenset(
    {
        "authorization",
        "token",
        "access_token",
        "consumer_key",
        "consumer_secret",
        "password",
        "secret",
    }
)

_BEARER_RE = re.compile(r"Bearer\s+\S+", re.IGNORECASE)
_REDACTED = "***REDACTED***"
_BEARER_REPLACEMENT = "Bearer ***"


@dataclass
class LoggingSettings:
    """Settings for configuring the pipeline logging system.

    Attributes:
        level: Minimum level for the package logger and the rotating file
            handler. One of ``"DEBUG"``, ``"INFO"``, ``"WARNING"``,
            ``"ERROR"``, ``"CRITICAL"`` (case-insensitive; normalized to
            upper case). ``DEBUG`` includes cache-hit and refresh-lock
            wait events; ``INFO`` (default) keeps lifecycle and per-POST
            success records; ``WARNING`` drops successes and keeps only
            retryable/terminal failures and data-quality flags. The stdout
            handler is always pinned to ``WARNING`` regardless of this
            setting, so cron output stays bounded.
        format: ``"json"`` (default, production) or ``"text"`` (local dev).
        payload_mode: ``"hash"`` | ``"failure"`` | ``"full"``. ``hash``
            logs only ``payload_sha256`` and ``payload_bytes``;
            ``failure`` (default) adds the full body and a response
            excerpt on non-2xx responses; ``full`` always logs the body.
            See :func:`payload_log_fields` for the per-mode matrix.
        payload_max_bytes: Maximum UTF-8 bytes retained for logged request
            bodies. Excess is truncated with a visible marker.
        response_max_bytes: Maximum UTF-8 bytes retained for logged
            response excerpts.
        log_dir: Directory holding the rotating log file. Created on
            demand by :func:`configure_logging`.
    """

    level: str = "INFO"
    format: str = "json"
    payload_mode: str = "failure"
    payload_max_bytes: int = 16384
    response_max_bytes: int = 2048
    log_dir: Path = field(default_factory=lambda: Path("logs"))

    def __post_init__(self) -> None:
        self.level = self.level.upper()
        self.format = self.format.lower()
        self.payload_mode = self.payload_mode.lower()
        self.log_dir = Path(self.log_dir)

        if self.level not in _VALID_LEVELS:
            raise ValueError(
                f"invalid LOG_LEVEL {self.level!r}; "
                f"expected one of {sorted(_VALID_LEVELS)}"
            )
        if self.format not in _VALID_FORMATS:
            raise ValueError(
                f"invalid LOG_FORMAT {self.format!r}; "
                f"expected one of {sorted(_VALID_FORMATS)}"
            )
        if self.payload_mode not in _VALID_PAYLOAD_MODES:
            raise ValueError(
                f"invalid LOG_PAYLOAD_MODE {self.payload_mode!r}; "
                f"expected one of {sorted(_VALID_PAYLOAD_MODES)}"
            )
        if self.payload_max_bytes < 0 or self.response_max_bytes < 0:
            raise ValueError("LOG_*_MAX_BYTES must be non-negative")

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> Self:
        """Build logging settings from environment variables."""
        values = os.environ if env is None else env
        return cls(
            level=_optional_env(values, "LOG_LEVEL", "INFO"),
            format=_optional_env(values, "LOG_FORMAT", "json"),
            payload_mode=_optional_env(values, "LOG_PAYLOAD_MODE", "failure"),
            payload_max_bytes=int(
                _optional_env(values, "LOG_PAYLOAD_MAX_BYTES", "16384")
            ),
            response_max_bytes=int(
                _optional_env(values, "LOG_RESPONSE_MAX_BYTES", "2048")
            ),
            log_dir=Path(_optional_env(values, "LOG_DIR", "logs")),
        )


class SecretsFilter(logging.Filter):
    """Strip secrets from log records before any handler sees them.

    Walks ``record.args`` and any user-supplied ``extra`` fields, redacting
    values whose key matches the secret deny-list (case-insensitive) and
    rewriting ``Bearer <token>`` substrings inside string values.
    """

    def filter(self, record: logging.LogRecord) -> bool:  # noqa: D401
        """Redact secrets in place; always return ``True``."""
        if isinstance(record.args, dict):
            record.args = _redact(record.args)  # type: ignore[assignment]
        elif isinstance(record.args, tuple) and record.args:
            record.args = tuple(_redact(a) for a in record.args)

        for key in list(record.__dict__.keys()):
            if key in _RESERVED_LOGRECORD_ATTRS:
                continue
            value = record.__dict__[key]
            if key.lower() in _REDACT_KEYS:
                record.__dict__[key] = _REDACTED
            else:
                record.__dict__[key] = _redact(value)
        return True


def _redact(value: Any) -> Any:
    """Return a redacted copy of *value* (mutating dicts/lists structurally)."""
    if isinstance(value, str):
        return _BEARER_RE.sub(_BEARER_REPLACEMENT, value)
    if isinstance(value, Mapping):
        redacted: dict[Any, Any] = {}
        for k, v in value.items():
            if isinstance(k, str) and k.lower() in _REDACT_KEYS:
                redacted[k] = _REDACTED
            else:
                redacted[k] = _redact(v)
        return redacted
    if isinstance(value, list):
        return [_redact(v) for v in value]
    if isinstance(value, tuple):
        return tuple(_redact(v) for v in value)
    return value


class JsonFormatter(logging.Formatter):
    """Emit one JSON object per log record built from an allowlist.

    Always-present fields come from ``LogRecord`` attributes; additional
    fields are accepted only when their key is in ``_ALLOWED_EXTRA_KEYS``.
    Unknown keys are dropped and a one-time warning is emitted per process.
    """

    _warned_keys: set[str] = set()

    def format(self, record: logging.LogRecord) -> str:
        """Serialize a log record to a JSON string."""
        obj: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=JST).isoformat(
                timespec="milliseconds"
            ),
            "level": record.levelname,
            "logger": record.name,
            "module": record.module,
            "line": record.lineno,
            "message": record.getMessage(),
        }
        if record.exc_info:
            obj["exception"] = self.formatException(record.exc_info)

        for key, value in record.__dict__.items():
            if key in _RESERVED_LOGRECORD_ATTRS:
                continue
            if key in _ALLOWED_EXTRA_KEYS:
                obj[key] = _normalize(value)
                continue
            if key not in self._warned_keys:
                self._warned_keys.add(key)
                # The unknown-field event has no other extras, so this
                # warning does not recurse through the allowlist check.
                logging.getLogger(PACKAGE_LOGGER_NAME).warning(
                    "unknown log field %s",
                    key,
                    extra={"event": "unknown_log_field"},
                )

        obj = _redact_dict(obj)
        return json.dumps(obj, ensure_ascii=False)


def _normalize(value: Any) -> Any:
    """Convert non-JSON-native values into JSON-safe ones, recursively."""
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=JST)
        return value.isoformat()
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, BaseException):
        return repr(value)
    if isinstance(value, Mapping):
        return {str(k): _normalize(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_normalize(v) for v in value]
    # NaN/Inf are not valid JSON; map to None so logs stay parseable.
    if isinstance(value, float) and (value != value or value in (_INF, _NEG_INF)):
        return None
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return repr(value)


_INF = float("inf")
_NEG_INF = float("-inf")


def _redact_dict(d: dict[str, Any]) -> dict[str, Any]:
    """Defensive final pass over the JSON object before serialization."""
    redacted: dict[str, Any] = {}
    for k, v in d.items():
        if k.lower() in _REDACT_KEYS:
            redacted[k] = _REDACTED
        else:
            redacted[k] = _redact(v)
    return redacted


class TextFormatter(logging.Formatter):
    """Human-readable formatter for local development (``LOG_FORMAT=text``)."""

    _BASE_FMT = "%(asctime)s %(levelname)s %(name)s — %(message)s"

    def __init__(self) -> None:
        super().__init__(self._BASE_FMT)

    def formatTime(  # noqa: N802 — matches the stdlib API
        self, record: logging.LogRecord, datefmt: str | None = None
    ) -> str:
        """Return an ISO-8601 timestamp in JST."""
        return datetime.fromtimestamp(record.created, tz=JST).isoformat(
            timespec="milliseconds"
        )

    def format(self, record: logging.LogRecord) -> str:
        """Format a log record as a human-readable line with structured extras."""
        base = super().format(record)
        suffix_parts: list[str] = []
        for key, value in record.__dict__.items():
            if key in _RESERVED_LOGRECORD_ATTRS or key not in _ALLOWED_EXTRA_KEYS:
                continue
            suffix_parts.append(f"{key}={value!r}")
        if suffix_parts:
            base = f"{base} | {' '.join(suffix_parts)}"
        return base


def configure_logging(settings: LoggingSettings, *, product: str) -> None:
    """Configure logging for an entry point.

    Idempotent: safe to call multiple times in one process. Replaces any
    handlers previously installed by this function on the
    ``sendai_pipeline`` package logger, while leaving the package's
    ``NullHandler`` fallback in place.

    Args:
        settings: Resolved settings — typically from
            :meth:`LoggingSettings.from_env`.
        product: Short product identifier used in the log file name
            (e.g. ``"flow"`` → ``logs/flow.log``).
    """
    settings.log_dir.mkdir(parents=True, exist_ok=True)
    log_path = settings.log_dir / f"{product}.log"

    pkg_logger = logging.getLogger(PACKAGE_LOGGER_NAME)
    for handler in list(pkg_logger.handlers):
        if isinstance(handler, logging.NullHandler):
            continue
        pkg_logger.removeHandler(handler)
        try:
            handler.close()
        except Exception:  # noqa: BLE001 — best-effort cleanup
            pass

    formatter: logging.Formatter = (
        JsonFormatter() if settings.format == "json" else TextFormatter()
    )
    secrets_filter = SecretsFilter()

    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=10 * 1024 * 1024,
        backupCount=10,
        encoding="utf-8",
    )
    file_handler.setLevel(settings.level)
    file_handler.setFormatter(formatter)
    file_handler.addFilter(secrets_filter)

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.WARNING)
    stdout_handler.setFormatter(formatter)
    stdout_handler.addFilter(secrets_filter)

    pkg_logger.setLevel(settings.level)
    pkg_logger.addHandler(file_handler)
    pkg_logger.addHandler(stdout_handler)
    # We own the entry-point's logging; do not also propagate to root.
    pkg_logger.propagate = False


def payload_sha256(body_bytes: bytes) -> str:
    """Return the hex sha256 of the literal request body bytes."""
    return hashlib.sha256(body_bytes).hexdigest()


def payload_log_fields(
    body_bytes: bytes | None,
    response_text: str | None,
    *,
    ok: bool,
    mode: str,
    payload_max_bytes: int,
    response_max_bytes: int,
) -> dict[str, Any]:
    """Build the ``extra`` fields describing a POST payload for logging.

    Per-mode keys returned:

    - ``hash`` — always: ``payload_mode``, ``ok``, ``payload_sha256``,
      ``payload_bytes`` (the last two are omitted when ``body_bytes`` is
      ``None``).
    - ``failure`` — on 2xx: same as ``hash``; on non-2xx: additionally
      ``payload`` (truncated body text) and ``response_excerpt`` (when
      ``response_text`` is not ``None``).
    - ``full`` — always: same as ``hash`` plus ``payload``; additionally
      ``response_excerpt`` on non-2xx (same condition as ``failure``).

    Oversize ``payload`` / ``response_excerpt`` values are truncated to
    ``payload_max_bytes`` / ``response_max_bytes`` with a visible marker
    that includes the original byte length.

    Args:
        body_bytes: Serialized request body, or ``None`` for dry-run.
        response_text: Response body text (UTF-8 decoded), or ``None``.
        ok: Whether the response was 2xx (and no exception was raised).
        mode: ``"hash"``, ``"failure"``, or ``"full"``.
        payload_max_bytes: Maximum bytes retained for ``payload``.
        response_max_bytes: Maximum bytes retained for ``response_excerpt``.

    Returns:
        Dict suitable for ``logger.info(..., extra=payload_log_fields(...))``.
    """
    if mode not in _VALID_PAYLOAD_MODES:
        raise ValueError(f"invalid payload mode {mode!r}")

    fields: dict[str, Any] = {"payload_mode": mode, "ok": ok}

    if body_bytes is not None:
        fields["payload_sha256"] = payload_sha256(body_bytes)
        fields["payload_bytes"] = len(body_bytes)

    include_payload = mode == "full" or (mode == "failure" and not ok)
    if include_payload and body_bytes is not None:
        fields["payload"] = _truncate(
            body_bytes.decode("utf-8", errors="replace"),
            payload_max_bytes,
        )

    if not ok and response_text is not None and mode in {"failure", "full"}:
        fields["response_excerpt"] = _truncate(response_text, response_max_bytes)

    return fields


def _optional_env(env: Mapping[str, str], key: str, default: str) -> str:
    """Return the env value if set and non-empty, otherwise *default*.

    Matches the helper of the same name in :mod:`sendai_pipeline.auth`:
    ``.env`` placeholders such as ``LOG_LEVEL=`` should fall back to
    the documented default rather than producing an empty string that
    later fails validation.
    """
    value = env.get(key)
    if value is None or value == "":
        return default
    return value


def _truncate(text: str, limit: int) -> str:
    """Truncate *text* to *limit* UTF-8 bytes with a visible marker.

    The marker carries the original UTF-8 byte length so operators reading
    ``response_excerpt`` (which has no separate ``*_bytes`` field) can still
    see how much was cut.
    """
    encoded = text.encode("utf-8")
    if len(encoded) <= limit:
        return text
    head = encoded[:limit].decode("utf-8", errors="replace")
    return f"{head}…[truncated; original {len(encoded)} bytes]"
