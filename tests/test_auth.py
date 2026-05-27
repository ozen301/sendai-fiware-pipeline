import base64
import json
import logging
import stat
import tempfile
from pathlib import Path
from typing import Any

import pytest
import requests

from sendai_pipeline.auth import AuthClient, AuthSettings, get_token


class FakeResponse:
    def __init__(self, payload: dict[str, Any], status_error: Exception | None = None):
        self._payload = payload
        self._status_error = status_error

    def json(self) -> dict[str, Any]:
        return self._payload

    def raise_for_status(self) -> None:
        if self._status_error is not None:
            raise self._status_error


class FakeSession:
    def __init__(self, response: FakeResponse):
        self.response = response
        self.posts: list[dict[str, Any]] = []

    def post(self, url: str, **kwargs: Any) -> FakeResponse:
        self.posts.append({"url": url, **kwargs})
        return self.response


def make_settings(tmp_path: Path, **overrides: Any) -> AuthSettings:
    values = {
        "base_url": "https://fiware.example.test",
        "consumer_key": "consumer-key",
        "consumer_secret": "consumer-secret",
        "token_url": "https://fiware.example.test/oauth2/token",
        "token_cache_path": tmp_path / "token.json",
        "refresh_margin_seconds": 60,
    }
    values.update(overrides)
    return AuthSettings(**values)


def jwt_with_exp(exp: int) -> str:
    header = base64.urlsafe_b64encode(b'{"alg":"none"}').rstrip(b"=")
    payload = base64.urlsafe_b64encode(json.dumps({"exp": exp}).encode()).rstrip(b"=")
    return b".".join([header, payload, b"signature"]).decode()


def test_from_env_derives_token_url_from_base_url() -> None:
    settings = AuthSettings.from_env(
        {
            "FIWARE_BASE_URL": "https://fiware.example.test/",
            "FIWARE_CONSUMER_KEY": "key",
            "FIWARE_CONSUMER_SECRET": "secret",
        }
    )

    assert settings.base_url == "https://fiware.example.test"
    assert settings.token_url == "https://fiware.example.test/oauth2/token"
    assert settings.token_scope == "default"
    assert settings.consumer_key == "key"
    assert settings.consumer_secret == "secret"


def test_from_env_allows_token_scope_override() -> None:
    settings = AuthSettings.from_env(
        {
            "FIWARE_BASE_URL": "https://fiware.example.test",
            "FIWARE_CONSUMER_KEY": "key",
            "FIWARE_CONSUMER_SECRET": "secret",
            "FIWARE_TOKEN_SCOPE": "custom",
        }
    )

    assert settings.token_scope == "custom"


def test_from_env_allows_token_url_override() -> None:
    settings = AuthSettings.from_env(
        {
            "FIWARE_BASE_URL": "https://fiware.example.test",
            "FIWARE_CONSUMER_KEY": "key",
            "FIWARE_CONSUMER_SECRET": "secret",
            "FIWARE_TOKEN_URL": "https://auth.example.test/oauth2/token",
        }
    )

    assert settings.token_url == "https://auth.example.test/oauth2/token"


def test_from_env_treats_empty_optional_values_as_unset() -> None:
    settings = AuthSettings.from_env(
        {
            "FIWARE_BASE_URL": "https://fiware.example.test",
            "FIWARE_CONSUMER_KEY": "key",
            "FIWARE_CONSUMER_SECRET": "secret",
            "FIWARE_TOKEN_URL": "",
            "FIWARE_TOKEN_SCOPE": "",
            "FIWARE_TOKEN_CACHE_PATH": "",
            "FIWARE_TOKEN_REFRESH_MARGIN_SECONDS": "",
            "FIWARE_TOKEN_TIMEOUT_SECONDS": "",
            "FIWARE_VERIFY_TLS": "",
        }
    )

    assert settings.token_url == "https://fiware.example.test/oauth2/token"
    assert settings.token_scope == "default"
    assert settings.token_cache_path == Path("state/token.json")
    assert settings.refresh_margin_seconds == 60
    assert settings.timeout == 10
    assert settings.verify_tls is True


def test_from_env_parses_tls_verify_false() -> None:
    settings = AuthSettings.from_env(
        {
            "FIWARE_BASE_URL": "https://fiware.example.test",
            "FIWARE_CONSUMER_KEY": "key",
            "FIWARE_CONSUMER_SECRET": "secret",
            "FIWARE_VERIFY_TLS": "false",
        }
    )

    assert settings.verify_tls is False


def test_get_token_fetches_client_credentials_token_and_caches_it() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        session = FakeSession(
            FakeResponse({"access_token": "token-1", "expires_in": 3600})
        )
        client = AuthClient(
            make_settings(tmp_path), session=session, now=lambda: 1000.0
        )

        token = client.get_token()

        assert token == "token-1"
        assert len(session.posts) == 1
        post = session.posts[0]
        assert post["url"] == "https://fiware.example.test/oauth2/token"
        assert post["data"] == {
            "scope": "default",
            "grant_type": "client_credentials",
            "client_id": "consumer-key",
            "client_secret": "consumer-secret",
        }
        assert post["timeout"] == 10
        assert post["headers"]["Accept"] == "application/json"
        assert "Authorization" not in post["headers"]
        assert json.loads((tmp_path / "token.json").read_text()) == {
            "access_token": "token-1",
            "expires_at": 4600.0,
        }


def test_get_token_writes_cache_file_readable_only_by_owner() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        session = FakeSession(
            FakeResponse({"access_token": "token-1", "expires_in": 3600})
        )
        client = AuthClient(
            make_settings(tmp_path), session=session, now=lambda: 1000.0
        )

        client.get_token()

        mode = stat.S_IMODE((tmp_path / "token.json").stat().st_mode)
        assert mode == 0o600


def test_get_token_returns_valid_cached_token_without_http() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        (tmp_path / "token.json").write_text(
            json.dumps({"access_token": "cached-token", "expires_at": 2000.0})
        )
        session = FakeSession(
            FakeResponse({"access_token": "new-token", "expires_in": 3600})
        )
        client = AuthClient(
            make_settings(tmp_path), session=session, now=lambda: 1000.0
        )

        token = client.get_token()

        assert token == "cached-token"
        assert session.posts == []


def test_get_token_refreshes_cached_token_inside_refresh_margin() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        (tmp_path / "token.json").write_text(
            json.dumps({"access_token": "old-token", "expires_at": 1059.0})
        )
        session = FakeSession(
            FakeResponse({"access_token": "new-token", "expires_in": 3600})
        )
        client = AuthClient(
            make_settings(tmp_path), session=session, now=lambda: 1000.0
        )

        token = client.get_token()

        assert token == "new-token"
        assert len(session.posts) == 1


def test_get_token_force_refresh_bypasses_valid_cache() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        (tmp_path / "token.json").write_text(
            json.dumps({"access_token": "cached-token", "expires_at": 2000.0})
        )
        session = FakeSession(
            FakeResponse({"access_token": "forced-token", "expires_in": 3600})
        )
        client = AuthClient(
            make_settings(tmp_path), session=session, now=lambda: 1000.0
        )

        token = client.get_token(force_refresh=True)

        assert token == "forced-token"
        assert len(session.posts) == 1


def test_get_token_uses_jwt_exp_when_expires_in_is_missing() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        access_token = jwt_with_exp(2222)
        session = FakeSession(FakeResponse({"access_token": access_token}))
        client = AuthClient(
            make_settings(tmp_path), session=session, now=lambda: 1000.0
        )

        token = client.get_token()

        assert token == access_token
        assert json.loads((tmp_path / "token.json").read_text())["expires_at"] == 2222.0


def test_get_token_logs_cache_hit_when_cache_is_valid(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG, logger="sendai_pipeline")
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        (tmp_path / "token.json").write_text(
            json.dumps({"access_token": "cached-token", "expires_at": 2000.0})
        )
        session = FakeSession(
            FakeResponse({"access_token": "new-token", "expires_in": 3600})
        )
        client = AuthClient(
            make_settings(tmp_path), session=session, now=lambda: 1000.0
        )

        client.get_token()

    events = [getattr(record, "event", None) for record in caplog.records]
    assert "token_cache_hit" in events


def test_get_token_logs_refresh_succeeded_on_first_fetch(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG, logger="sendai_pipeline")
    secret_token = "secret-token-abc-xyz"
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        session = FakeSession(
            FakeResponse({"access_token": secret_token, "expires_in": 3600})
        )
        client = AuthClient(
            make_settings(tmp_path), session=session, now=lambda: 1000.0
        )

        client.get_token()

    events = [getattr(record, "event", None) for record in caplog.records]
    assert "token_refresh_started" in events
    assert "token_refresh_succeeded" in events
    for record in caplog.records:
        assert secret_token not in record.getMessage()
        for value in record.__dict__.values():
            if isinstance(value, str):
                assert secret_token not in value


def test_get_token_logs_refresh_failed_on_http_error(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG, logger="sendai_pipeline")
    http_error = requests.HTTPError("500 server error")
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        session = FakeSession(FakeResponse({}, status_error=http_error))
        client = AuthClient(
            make_settings(tmp_path), session=session, now=lambda: 1000.0
        )

        with pytest.raises(requests.HTTPError):
            client.get_token()

    failure_records = [
        r for r in caplog.records if getattr(r, "event", None) == "token_refresh_failed"
    ]
    assert len(failure_records) == 1
    assert getattr(failure_records[0], "error_type", None) == "HTTPError"


def test_get_token_wrapper_uses_auth_client() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        session = FakeSession(
            FakeResponse({"access_token": "wrapped-token", "expires_in": 3600})
        )

        token = get_token(
            settings=make_settings(tmp_path),
            session=session,
            now=lambda: 1000.0,
        )

        assert token == "wrapped-token"
        assert len(session.posts) == 1
