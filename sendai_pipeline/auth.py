"""OAuth2 client-credentials token handling for Sendai FIWARE."""

import base64
import fcntl
import json
import logging
import os
import time
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Self

import requests

from sendai_pipeline.settings_validation import optional_env

logger = logging.getLogger(__name__)


class AuthError(RuntimeError):
    """Base class for auth-related failures."""


class AuthConfigError(AuthError):
    """Raised when required auth configuration is missing or invalid."""


class TokenResponseError(AuthError):
    """Raised when the token endpoint response cannot be used."""


@dataclass
class AuthSettings:
    """Configuration for OAuth2 client-credentials token requests.

    Attributes:
        base_url: Canonical Sendai platform base URL, used to derive the
            default token URL when ``FIWARE_TOKEN_URL`` is unset. Some
            developer tooling also reads this field directly off the
            settings object instead of reading the environment again.
    """

    base_url: str
    consumer_key: str
    consumer_secret: str
    token_url: str
    token_scope: str = "default"
    token_cache_path: Path = Path("state/token.json")
    refresh_margin_seconds: int = 60
    timeout: float = 10
    verify_tls: bool = True

    def __post_init__(self) -> None:
        self.base_url = self.base_url.rstrip("/")
        self.token_cache_path = Path(self.token_cache_path)

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> Self:
        """Build auth settings from environment variables."""
        values = os.environ if env is None else env
        base_url = _required_env(values, "FIWARE_BASE_URL").rstrip("/")
        token_url = (
            optional_env(values, "FIWARE_TOKEN_URL", "") or f"{base_url}/oauth2/token"
        )

        return cls(
            base_url=base_url,
            consumer_key=_required_env(values, "FIWARE_CONSUMER_KEY"),
            consumer_secret=_required_env(values, "FIWARE_CONSUMER_SECRET"),
            token_url=token_url,
            token_scope=optional_env(values, "FIWARE_TOKEN_SCOPE", "default"),
            token_cache_path=Path(
                optional_env(values, "FIWARE_TOKEN_CACHE_PATH", "state/token.json")
            ),
            refresh_margin_seconds=int(
                optional_env(values, "FIWARE_TOKEN_REFRESH_MARGIN_SECONDS", "60")
            ),
            timeout=float(optional_env(values, "FIWARE_TOKEN_TIMEOUT_SECONDS", "10")),
            verify_tls=_parse_bool(optional_env(values, "FIWARE_VERIFY_TLS", "true")),
        )


class AuthClient:
    """Fetch and cache OAuth2 access tokens for Sendai FIWARE."""

    def __init__(
        self,
        settings: AuthSettings,
        *,
        session: Any = None,
        now: Callable[[], float] = time.time,
    ) -> None:
        self.settings = settings
        self.session: requests.Session = session or requests.Session()
        self.now = now

    def get_token(self, *, force_refresh: bool = False) -> str:
        """Return a cached access token, refreshing it when needed."""
        if not force_refresh:
            cached = self._read_cache()
            if cached is not None and self._is_cache_usable(cached):
                logger.debug("token cache hit", extra={"event": "token_cache_hit"})
                return cached["access_token"]

        with self._refresh_lock():
            if not force_refresh:
                cached = self._read_cache()
                if cached is not None and self._is_cache_usable(cached):
                    # A concurrent process refreshed the cache while we were
                    # waiting for the lock — return the fresh token without
                    # re-fetching.
                    return cached["access_token"]

            logger.info(
                "token refresh starting",
                extra={"event": "token_refresh_started"},
            )
            try:
                token_record = self._fetch_token()
                self._write_cache(token_record)
            except Exception as exc:
                logger.exception(
                    "token refresh failed",
                    extra={
                        "event": "token_refresh_failed",
                        "error_type": type(exc).__name__,
                    },
                )
                raise
            logger.info(
                "token refreshed",
                extra={"event": "token_refresh_succeeded"},
            )
            return token_record["access_token"]

    def _is_cache_usable(self, record: dict[str, Any]) -> bool:
        """Return whether a cached token still has more than the refresh margin left."""
        return (
            self.now()
            < float(record["expires_at"]) - self.settings.refresh_margin_seconds
        )

    def _read_cache(self) -> dict[str, Any] | None:
        """Read and structurally validate a token cache record.

        Returns ``None`` for a missing file, invalid JSON, or a malformed
        record (not a dict, a non-string ``access_token``, or an
        ``expires_at`` that cannot convert to a float). Does not check
        whether the token is still fresh — see :meth:`_is_cache_usable`.
        """
        path = self.settings.token_cache_path
        if not path.exists():
            return None

        try:
            record = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None

        if not isinstance(record, dict):
            return None
        if not isinstance(record.get("access_token"), str):
            return None
        try:
            record["expires_at"] = float(record["expires_at"])
        except (KeyError, TypeError, ValueError):
            return None
        return record

    def _fetch_token(self) -> dict[str, Any]:
        """Fetch a fresh token using Sendai's client-credentials form request."""
        response = self.session.post(
            self.settings.token_url,
            data={
                "scope": self.settings.token_scope,
                "grant_type": "client_credentials",
                "client_id": self.settings.consumer_key,
                "client_secret": self.settings.consumer_secret,
            },
            headers={
                "Accept": "application/json",
            },
            timeout=self.settings.timeout,
            verify=self.settings.verify_tls,
        )
        response.raise_for_status()
        payload = response.json()

        access_token = payload.get("access_token")
        if not isinstance(access_token, str) or not access_token:
            raise TokenResponseError("token response did not include access_token")

        return {
            "access_token": access_token,
            "expires_at": self._expires_at(payload, access_token),
        }

    def _expires_at(self, payload: Mapping[str, Any], access_token: str) -> float:
        """Calculate absolute token expiry from response metadata or JWT claims."""
        expires_in = payload.get("expires_in")
        if expires_in is not None:
            try:
                return self.now() + float(expires_in)
            except (TypeError, ValueError) as exc:
                raise TokenResponseError(
                    "token response expires_in is not numeric"
                ) from exc

        return _jwt_exp(access_token)

    def _write_cache(self, record: Mapping[str, Any]) -> None:
        """Atomically write a token cache record."""
        path = self.settings.token_cache_path
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
        try:
            tmp_path.write_text(
                json.dumps(record, sort_keys=True),
                encoding="utf-8",
            )
            tmp_path.chmod(0o600)
            os.replace(tmp_path, path)
            path.chmod(0o600)
        finally:
            try:
                tmp_path.unlink()
            except FileNotFoundError:
                pass

    @contextmanager
    def _refresh_lock(self) -> Iterator[None]:
        """Serialize token refreshes across concurrent pipeline processes."""
        lock_path = self.settings.token_cache_path.with_suffix(".lock")
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        with lock_path.open("a", encoding="utf-8") as lock_file:
            logger.debug(
                "waiting for token refresh lock",
                extra={"event": "token_refresh_waiting_lock"},
            )
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def get_token(
    *,
    force_refresh: bool = False,
    settings: AuthSettings | None = None,
    session: Any = None,
    now: Callable[[], float] = time.time,
) -> str:
    """Return an OAuth2 access token using explicit or environment settings."""
    client = AuthClient(settings or AuthSettings.from_env(), session=session, now=now)
    return client.get_token(force_refresh=force_refresh)


def _required_env(env: Mapping[str, str], key: str) -> str:
    """Return a required environment value or raise a config error."""
    value = env.get(key)
    if value is None or value == "":
        raise AuthConfigError(f"missing required environment variable: {key}")
    return value


def _parse_bool(value: str) -> bool:
    """Parse an environment-style boolean that defaults to true.

    Only ``"0"``, ``"false"``, ``"no"``, or ``"off"`` (case-insensitive)
    parse as ``False``; every other value, including malformed input, is
    ``True``.
    """
    return value.strip().lower() not in {"0", "false", "no", "off"}


def _jwt_exp(access_token: str) -> float:
    """Extract the expiry timestamp from a JWT access token payload."""
    try:
        _header, payload, *_rest = access_token.split(".")
        payload_json = _urlsafe_b64decode(payload)
        exp = json.loads(payload_json)["exp"]
        return float(exp)
    except (ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
        raise TokenResponseError(
            "token response omitted expires_in and JWT exp is unavailable"
        ) from exc


def _urlsafe_b64decode(value: str) -> bytes:
    """Decode URL-safe base64 strings that omit padding."""
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(value + padding)
