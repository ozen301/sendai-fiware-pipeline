import hashlib
import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest

from sendai_pipeline.logging_setup import (
    JST,
    PACKAGE_LOGGER_NAME,
    JsonFormatter,
    LoggingSettings,
    SecretsFilter,
    TextFormatter,
    configure_logging,
    payload_log_fields,
    payload_sha256,
)


def make_record(
    *,
    name: str = "sendai_pipeline.test",
    level: int = logging.INFO,
    msg: str = "hello",
    args: tuple[Any, ...] | dict[str, Any] = (),
    exc_info: Any = None,
    extras: dict[str, Any] | None = None,
) -> logging.LogRecord:
    record = logging.LogRecord(
        name=name,
        level=level,
        pathname="/repo/sendai_pipeline/auth.py",
        lineno=42,
        msg=msg,
        args=args,
        exc_info=exc_info,
    )
    if extras:
        for k, v in extras.items():
            record.__dict__[k] = v
    return record


def test_logging_settings_from_env_uses_documented_defaults() -> None:
    settings = LoggingSettings.from_env({})

    assert settings.level == "INFO"
    assert settings.format == "json"
    assert settings.payload_mode == "failure"
    assert settings.payload_max_bytes == 16384
    assert settings.response_max_bytes == 2048
    assert settings.log_dir == Path("logs")


def test_logging_settings_from_env_treats_empty_optional_values_as_unset() -> None:
    settings = LoggingSettings.from_env(
        {
            "LOG_LEVEL": "",
            "LOG_FORMAT": "",
            "LOG_PAYLOAD_MODE": "",
            "LOG_PAYLOAD_MAX_BYTES": "",
            "LOG_RESPONSE_MAX_BYTES": "",
            "LOG_DIR": "",
        }
    )

    assert settings.level == "INFO"
    assert settings.format == "json"
    assert settings.payload_mode == "failure"
    assert settings.payload_max_bytes == 16384
    assert settings.response_max_bytes == 2048
    assert settings.log_dir == Path("logs")


def test_logging_settings_from_env_normalizes_case() -> None:
    settings = LoggingSettings.from_env(
        {"LOG_LEVEL": "debug", "LOG_FORMAT": "JSON", "LOG_PAYLOAD_MODE": "Hash"}
    )

    assert settings.level == "DEBUG"
    assert settings.format == "json"
    assert settings.payload_mode == "hash"


def test_logging_settings_rejects_invalid_payload_mode() -> None:
    with pytest.raises(ValueError, match="LOG_PAYLOAD_MODE"):
        LoggingSettings.from_env({"LOG_PAYLOAD_MODE": "verbose"})


def test_logging_settings_rejects_invalid_level() -> None:
    with pytest.raises(ValueError, match="LOG_LEVEL"):
        LoggingSettings.from_env({"LOG_LEVEL": "loud"})


def test_logging_settings_rejects_invalid_format() -> None:
    with pytest.raises(ValueError, match="LOG_FORMAT"):
        LoggingSettings.from_env({"LOG_FORMAT": "yaml"})


def test_logging_settings_rejects_negative_max_bytes() -> None:
    with pytest.raises(ValueError, match="MAX_BYTES"):
        LoggingSettings(payload_max_bytes=-1)


def test_json_formatter_emits_required_fields() -> None:
    record = make_record()
    payload = json.loads(JsonFormatter().format(record))

    assert payload["level"] == "INFO"
    assert payload["logger"] == "sendai_pipeline.test"
    assert payload["module"] == "auth"
    assert payload["line"] == 42
    assert payload["message"] == "hello"
    assert "exception" not in payload


def test_json_formatter_ts_is_offset_aware_jst() -> None:
    record = make_record()
    payload = json.loads(JsonFormatter().format(record))

    assert payload["ts"].endswith("+09:00")
    # Round-trip to confirm it parses as an aware JST instant.
    parsed = datetime.fromisoformat(payload["ts"])
    assert parsed.utcoffset() == JST.utcoffset(None)


def test_json_formatter_includes_allowed_extras() -> None:
    record = make_record(
        extras={
            "event": "post_succeeded",
            "entity_id": "urn:ngsi-ld:Sensor:42",
            "http_status": 204,
            "ok": True,
        }
    )
    payload = json.loads(JsonFormatter().format(record))

    assert payload["event"] == "post_succeeded"
    assert payload["entity_id"] == "urn:ngsi-ld:Sensor:42"
    assert payload["http_status"] == 204
    assert payload["ok"] is True


def test_json_formatter_includes_exception_traceback() -> None:
    try:
        raise RuntimeError("boom")
    except RuntimeError:
        import sys

        exc_info = sys.exc_info()
    record = make_record(level=logging.ERROR, exc_info=exc_info)
    payload = json.loads(JsonFormatter().format(record))

    assert "exception" in payload
    assert "RuntimeError: boom" in payload["exception"]


def test_json_formatter_normalizes_path_and_datetime_and_bytes() -> None:
    when = datetime(2026, 5, 21, 9, 0, 0, tzinfo=JST)
    record = make_record(
        extras={
            "event": "run_started",
            "payload": b"hello \xff world",
            "window": str(Path("state/per3600/window-1")),
            "run_id": when,
        }
    )
    payload = json.loads(JsonFormatter().format(record))

    assert payload["window"] == "state/per3600/window-1"
    assert payload["payload"].startswith("hello ")
    assert payload["run_id"].endswith("+09:00")


def test_json_formatter_maps_nan_and_inf_to_null() -> None:
    record = make_record(
        extras={
            "event": "post_succeeded",
            "elapsed_ms": float("nan"),
            "payload_bytes": float("inf"),
        }
    )
    payload = json.loads(JsonFormatter().format(record))

    assert payload["elapsed_ms"] is None
    assert payload["payload_bytes"] is None


def test_json_formatter_drops_unknown_extras_and_warns_once(
    caplog: pytest.LogCaptureFixture,
) -> None:
    formatter = JsonFormatter()
    caplog.set_level(logging.WARNING, logger=PACKAGE_LOGGER_NAME)

    record_one = make_record(extras={"bogus_key": "ignored"})
    payload_one = json.loads(formatter.format(record_one))
    assert "bogus_key" not in payload_one

    warnings_for_key = [r for r in caplog.records if "bogus_key" in r.getMessage()]
    assert len(warnings_for_key) == 1

    caplog.clear()
    record_two = make_record(extras={"bogus_key": "still ignored"})
    formatter.format(record_two)
    assert not any("bogus_key" in r.getMessage() for r in caplog.records)


def test_secrets_filter_redacts_known_keys_case_insensitive() -> None:
    record = make_record(
        extras={
            "Authorization": "Bearer abc123",
            "consumer_secret": "shh",
            "entity_id": "urn:ngsi-ld:Sensor:42",
        }
    )

    SecretsFilter().filter(record)

    assert record.__dict__["Authorization"] == "***REDACTED***"
    assert record.__dict__["consumer_secret"] == "***REDACTED***"
    assert record.__dict__["entity_id"] == "urn:ngsi-ld:Sensor:42"


def test_secrets_filter_redacts_bearer_token_in_string_values() -> None:
    record = make_record(
        extras={
            "response_excerpt": "GET /foo HTTP/1.1\nAuthorization: Bearer abc123def",
        }
    )

    SecretsFilter().filter(record)

    assert "abc123def" not in record.__dict__["response_excerpt"]
    assert "Bearer ***" in record.__dict__["response_excerpt"]


def test_secrets_filter_walks_nested_dicts_and_lists() -> None:
    record = make_record(
        extras={
            "payload": {
                "headers": {"Authorization": "Bearer xyz"},
                "items": [{"password": "p"}, "Bearer something"],
                "ok": True,
            }
        }
    )

    SecretsFilter().filter(record)

    nested = record.__dict__["payload"]
    assert nested["headers"]["Authorization"] == "***REDACTED***"
    assert nested["items"][0]["password"] == "***REDACTED***"
    assert nested["items"][1] == "Bearer ***"
    assert nested["ok"] is True


def test_secrets_filter_leaves_non_secret_payload_untouched() -> None:
    record = make_record(
        extras={
            "payload": {"value": 42, "metadata": {"unit": "m"}},
        }
    )

    SecretsFilter().filter(record)

    assert record.__dict__["payload"] == {"value": 42, "metadata": {"unit": "m"}}


PAYLOAD = b'{"x":1}'


def test_payload_log_fields_hash_mode_success_includes_only_fingerprint() -> None:
    fields = payload_log_fields(
        PAYLOAD,
        None,
        ok=True,
        mode="hash",
        payload_max_bytes=16384,
        response_max_bytes=2048,
    )

    assert fields["payload_mode"] == "hash"
    assert fields["ok"] is True
    assert fields["payload_sha256"] == hashlib.sha256(PAYLOAD).hexdigest()
    assert fields["payload_bytes"] == len(PAYLOAD)
    assert "payload" not in fields
    assert "response_excerpt" not in fields


def test_payload_log_fields_hash_mode_failure_still_only_hashes() -> None:
    fields = payload_log_fields(
        PAYLOAD,
        "internal error",
        ok=False,
        mode="hash",
        payload_max_bytes=16384,
        response_max_bytes=2048,
    )

    assert "payload" not in fields
    assert "response_excerpt" not in fields


def test_payload_log_fields_failure_mode_success_only_hashes() -> None:
    fields = payload_log_fields(
        PAYLOAD,
        None,
        ok=True,
        mode="failure",
        payload_max_bytes=16384,
        response_max_bytes=2048,
    )

    assert "payload" not in fields
    assert "response_excerpt" not in fields
    assert fields["payload_sha256"] == hashlib.sha256(PAYLOAD).hexdigest()


def test_payload_log_fields_failure_mode_failure_includes_body() -> None:
    fields = payload_log_fields(
        PAYLOAD,
        "server boom",
        ok=False,
        mode="failure",
        payload_max_bytes=16384,
        response_max_bytes=2048,
    )

    assert fields["payload"] == '{"x":1}'
    assert fields["response_excerpt"] == "server boom"


def test_payload_log_fields_full_mode_always_includes_body() -> None:
    fields = payload_log_fields(
        PAYLOAD,
        None,
        ok=True,
        mode="full",
        payload_max_bytes=16384,
        response_max_bytes=2048,
    )

    assert fields["payload"] == '{"x":1}'


def test_payload_log_fields_truncates_oversize_payload_with_marker() -> None:
    big = b"a" * 100
    fields = payload_log_fields(
        big,
        None,
        ok=False,
        mode="failure",
        payload_max_bytes=10,
        response_max_bytes=10,
    )

    assert re.search(r"\[truncated; original \d+ bytes\]", fields["payload"])
    assert "original 100 bytes" in fields["payload"]
    assert fields["payload"].startswith("aaaaaaaaaa")


def test_payload_log_fields_invalid_mode_raises() -> None:
    with pytest.raises(ValueError, match="invalid payload mode"):
        payload_log_fields(
            PAYLOAD,
            None,
            ok=True,
            mode="bogus",
            payload_max_bytes=16384,
            response_max_bytes=2048,
        )


def test_payload_sha256_matches_hashlib() -> None:
    assert payload_sha256(PAYLOAD) == hashlib.sha256(PAYLOAD).hexdigest()


def test_configure_logging_is_idempotent(tmp_path: Path) -> None:
    settings = LoggingSettings(log_dir=tmp_path)

    configure_logging(settings, product="flow")
    handlers_first = [
        h
        for h in logging.getLogger(PACKAGE_LOGGER_NAME).handlers
        if not isinstance(h, logging.NullHandler)
    ]
    configure_logging(settings, product="flow")
    handlers_second = [
        h
        for h in logging.getLogger(PACKAGE_LOGGER_NAME).handlers
        if not isinstance(h, logging.NullHandler)
    ]

    assert len(handlers_first) == 2
    assert len(handlers_second) == 2


def test_configure_logging_writes_json_lines_to_file(tmp_path: Path) -> None:
    settings = LoggingSettings(log_dir=tmp_path)
    configure_logging(settings, product="flow")

    pkg_logger = logging.getLogger(PACKAGE_LOGGER_NAME)
    pkg_logger.info(
        "post succeeded",
        extra={"event": "post_succeeded", "entity_id": "urn:ngsi-ld:Sensor:1"},
    )
    for h in pkg_logger.handlers:
        h.flush()

    log_file = tmp_path / "flow.log"
    text = log_file.read_text(encoding="utf-8").strip()
    line = json.loads(text.splitlines()[-1])
    assert line["event"] == "post_succeeded"
    assert line["entity_id"] == "urn:ngsi-ld:Sensor:1"
    assert line["ts"].endswith("+09:00")


def test_configure_logging_preserves_null_handler(tmp_path: Path) -> None:
    pkg_logger = logging.getLogger(PACKAGE_LOGGER_NAME)
    null_handlers_before = [
        h for h in pkg_logger.handlers if isinstance(h, logging.NullHandler)
    ]
    configure_logging(LoggingSettings(log_dir=tmp_path), product="flow")
    null_handlers_after = [
        h for h in pkg_logger.handlers if isinstance(h, logging.NullHandler)
    ]
    assert null_handlers_before == null_handlers_after


def test_text_formatter_renders_event_suffix_when_extras_present() -> None:
    record = make_record(extras={"event": "post_succeeded", "entity_id": "x"})
    out = TextFormatter().format(record)

    assert "post_succeeded" in out
    assert "entity_id" in out
    assert "INFO" in out
    assert "+09:00" in out
