"""Orion NGSI v2 client for Sendai FIWARE attribute updates."""

import json
import logging
import math
import os
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol, Self

import requests

from sendai_pipeline.logging_setup import payload_log_fields

logger = logging.getLogger(__name__)


class OrionError(RuntimeError):
    """Base class for Orion client errors."""


class OrionConfigError(OrionError):
    """Raised when required Orion configuration is missing or invalid."""


class TokenProvider(Protocol):
    """Anything that can hand out a Sendai FIWARE access token.

    Production code passes ``sendai_pipeline.auth.AuthClient``; tests can
    pass any object whose ``get_token`` shape matches.
    """

    def get_token(self, *, force_refresh: bool = False) -> str:
        """Return a valid bearer token, refreshing if asked."""
        ...


@dataclass
class OrionSettings:
    """Configuration for Sendai FIWARE Orion API requests."""

    base_url: str
    service: str = ""
    service_path: str = "/"
    verify_tls: bool = True
    timeout: float = 10
    max_retries: int = 5

    def __post_init__(self) -> None:
        self.base_url = self.base_url.rstrip("/")

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> Self:
        """Build Orion settings from environment variables."""
        values = os.environ if env is None else env
        return cls(
            base_url=_required_env(values, "FIWARE_BASE_URL").rstrip("/"),
            service=_optional_env(values, "FIWARE_SERVICE", ""),
            service_path=_optional_env(values, "FIWARE_SERVICE_PATH", "/"),
            verify_tls=_parse_bool(_optional_env(values, "FIWARE_VERIFY_TLS", "true")),
            timeout=float(_optional_env(values, "FIWARE_TIMEOUT_SECONDS", "10")),
        )


class OrionClient:
    """NGSI v2 client with Sendai FIWARE auth, retry, and structured logging.

    Each :meth:`update_attrs` call serialises the body to canonical JSON,
    fetches a token from the injected ``auth`` provider, and posts the
    attributes to ``/orion/v2.0/entities/<id>/attrs``. Transient failures
    (5xx, ``ConnectionError``, ``Timeout``, 429) are retried with
    exponential backoff (delays ``1, 2, 4, 8, 16`` seconds). A single 401
    triggers a forced token refresh and one out-of-band retry; the second
    consecutive 401 is terminal. Other 4xx responses are terminal without
    retry. Each call emits exactly one structured log record
    (``post_succeeded`` or ``post_failed``).

    :meth:`list_entities` is a thin GET helper used by ``entity_map`` to
    validate targets; it does not retry.

    The class does not catch unexpected exceptions — connection-class
    failures are caught explicitly; anything else propagates to the caller.
    """

    def __init__(
        self,
        settings: OrionSettings,
        *,
        auth: TokenProvider,
        session: Any = None,
        sleep: Callable[[float], None] = time.sleep,
        now: Callable[[], float] = time.time,
        payload_mode: str = "failure",
        payload_max_bytes: int = 16384,
        response_max_bytes: int = 2048,
    ) -> None:
        """Build an Orion client.

        Args:
            settings: Resolved :class:`OrionSettings`, typically from
                :meth:`OrionSettings.from_env`.
            auth: Token provider — see :class:`TokenProvider`.
                In production this is a ``sendai_pipeline.auth.AuthClient``;
                tests inject a fake.
            session: HTTP session used for requests. Defaults to a fresh
                :class:`requests.Session`. Anything with ``.post`` and
                ``.get`` methods returning a response-shaped object works,
                which is how tests inject fakes.
            sleep: Callable used to pause between retries. Injected so
                tests can record delays without real waits.
            now: Callable returning a monotonic-enough wall time in
                seconds, used to compute ``elapsed_ms``. Injected for
                deterministic tests.
            payload_mode: One of ``"hash"`` / ``"failure"`` / ``"full"``,
                controlling how POST bodies are logged. See
                :func:`sendai_pipeline.logging_setup.payload_log_fields`
                for the per-mode matrix.
            payload_max_bytes: Cap (UTF-8 bytes) on the ``payload`` log
                field; oversize bodies are truncated with a marker.
            response_max_bytes: Cap (UTF-8 bytes) on the
                ``response_excerpt`` log field and on the ``body_excerpt``
                returned by :meth:`update_attrs`.
        """
        self.settings = settings
        self.auth = auth
        self.session = session or requests.Session()
        self.sleep = sleep
        self.now = now
        self.payload_mode = payload_mode
        self.payload_max_bytes = payload_max_bytes
        self.response_max_bytes = response_max_bytes

    def update_attrs(
        self,
        entity_id: str,
        entity_type: str | None,
        attrs: Mapping[str, Any],
        *,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Post NGSI v2 attributes to one Orion entity.

        The body is serialised as canonical JSON
        (``sort_keys=True, separators=(",", ":")``) and sent as raw bytes
        so the wire payload is deterministic — callers may hash it for
        change detection.

        Args:
            entity_id: Target entity id (e.g.
                ``"jp.sendai.Blesensor.per3600.10"``).
            entity_type: Optional NGSI entity type. When provided it is
                appended as ``?type=<entity_type>`` to disambiguate
                entities sharing an id; pass ``None`` to omit.
            attrs: Mapping of attribute name → NGSI attribute object
                (each typically ``{"type": ..., "value": ...}``). Sent
                verbatim as the request body.
            dry_run: When true, build the URL and body, log a
                ``post_succeeded`` record with ``dry_run=True``, and
                return without contacting the network.

        Returns:
            A dict with keys ``status`` (int, ``0`` on dry-run or
            connection failure), ``ok`` (bool), ``attempts`` (int),
            ``elapsed_ms`` (int), ``body_excerpt`` (truncated response
            text on failure, ``None`` on success or dry-run),
            and ``dry_run`` (bool).

        Raises:
            Any exception other than ``requests.exceptions.ConnectionError``
            / ``requests.exceptions.Timeout`` propagates to the caller —
            those two are caught and treated as retryable.
        """
        body_bytes = json.dumps(
            attrs,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")

        if dry_run:
            self._log_post_result(
                entity_id=entity_id,
                status=0,
                ok=True,
                attempts=0,
                elapsed_ms=0,
                body_bytes=body_bytes,
                response_text=None,
                dry_run=True,
                payload_mode="full",
            )
            return {
                "status": 0,
                "ok": True,
                "attempts": 0,
                "elapsed_ms": 0,
                "body_excerpt": None,
                "dry_run": True,
            }

        started = self.now()
        attempts = 0
        backoff_index = 0
        refreshed_after_401 = False
        force_refresh = False
        status = 0
        response_text: str | None = None
        ok = False
        max_attempts = self.settings.max_retries + 1

        while attempts < max_attempts:
            attempts += 1
            try:
                response = self.session.post(
                    self._attrs_url(entity_id, entity_type),
                    data=body_bytes,
                    headers=self._headers(
                        include_content_type=True,
                        force_refresh=force_refresh,
                    ),
                    timeout=self.settings.timeout,
                    verify=self.settings.verify_tls,
                )
            except (
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
            ) as exc:
                force_refresh = False
                status = 0
                response_text = str(exc)
                ok = False
                if attempts >= max_attempts:
                    break
                self.sleep(self._backoff_delay(backoff_index))
                backoff_index += 1
                continue

            force_refresh = False
            status = response.status_code
            response_text = response.text
            ok = 200 <= status < 300

            if ok:
                break

            if status == 401 and not refreshed_after_401:
                refreshed_after_401 = True
                force_refresh = True
                if attempts < max_attempts:
                    continue
                break

            retry_delay, advance_backoff = self._retry_delay(response, backoff_index)
            if retry_delay is None or attempts >= max_attempts:
                break

            self.sleep(retry_delay)
            if advance_backoff:
                backoff_index += 1

        elapsed_ms = int(round((self.now() - started) * 1000))
        body_excerpt = self._log_post_result(
            entity_id=entity_id,
            status=status,
            ok=ok,
            attempts=attempts,
            elapsed_ms=elapsed_ms,
            body_bytes=body_bytes,
            response_text=response_text,
            dry_run=False,
            payload_mode=self.payload_mode,
        )

        return {
            "status": status,
            "ok": ok,
            "attempts": attempts,
            "elapsed_ms": elapsed_ms,
            "body_excerpt": body_excerpt,
            "dry_run": False,
        }

    def list_entities(
        self,
        entity_type: str,
        *,
        attrs: str | None = None,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        """Return one page of Orion entities for a given NGSI type.

        Single page only — pagination is intentionally out of scope
        because the Sendai deployment has far fewer entities per type
        than the 1000-item default ``limit``.

        Args:
            entity_type: NGSI entity type (e.g. ``"Blesensor.per3600"``).
            attrs: Optional comma-separated attribute selector. Pass
                ``"id"`` to fetch only ids when validating existence.
            limit: Maximum results per page; Orion's own default is
                applied if higher than the platform's hard cap.

        Returns:
            The JSON body returned by Orion — a list of entity-shaped
            dicts.
        """
        params: dict[str, Any] = {
            "type": entity_type,
            "limit": limit,
        }
        if attrs is not None:
            params["attrs"] = attrs

        response = self.session.get(
            f"{self.settings.base_url}/orion/v2.0/entities",
            params=params,
            headers=self._headers(include_content_type=False),
            timeout=self.settings.timeout,
            verify=self.settings.verify_tls,
        )
        return response.json()

    def get_entity(
        self,
        entity_id: str,
        *,
        entity_type: str | None = None,
        attrs: str | None = None,
    ) -> dict[str, Any]:
        """Return one Orion entity by id.

        Args:
            entity_id: NGSI entity id to read.
            entity_type: Optional NGSI entity type. Pass this when the
                broker may contain the same id under more than one type.
            attrs: Optional comma-separated attribute selector.

        Returns:
            The JSON body returned by Orion.

        Raises:
            requests.HTTPError: If Orion returns a non-2xx response.
        """
        params: dict[str, Any] = {}
        if entity_type is not None:
            params["type"] = entity_type
        if attrs is not None:
            params["attrs"] = attrs

        response = self.session.get(
            f"{self.settings.base_url}/orion/v2.0/entities/{entity_id}",
            params=params,
            headers=self._headers(include_content_type=False),
            timeout=self.settings.timeout,
            verify=self.settings.verify_tls,
        )
        if response.status_code == 401:
            response = self.session.get(
                f"{self.settings.base_url}/orion/v2.0/entities/{entity_id}",
                params=params,
                headers=self._headers(
                    include_content_type=False,
                    force_refresh=True,
                ),
                timeout=self.settings.timeout,
                verify=self.settings.verify_tls,
            )
        response.raise_for_status()
        return response.json()

    def _attrs_url(self, entity_id: str, entity_type: str | None) -> str:
        """Return the entity attributes endpoint URL."""
        url = f"{self.settings.base_url}/orion/v2.0/entities/{entity_id}/attrs"
        if entity_type is not None:
            url = f"{url}?type={entity_type}"
        return url

    def _headers(
        self,
        *,
        include_content_type: bool,
        force_refresh: bool = False,
    ) -> dict[str, str]:
        """Build headers for authenticated Orion requests."""
        token = self.auth.get_token(force_refresh=force_refresh)
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Fiware-ServicePath": self.settings.service_path,
        }
        if include_content_type:
            headers["Content-Type"] = "application/json"
        if self.settings.service:
            headers["Fiware-Service"] = self.settings.service
        return headers

    def _retry_delay(
        self,
        response: Any,
        backoff_index: int,
    ) -> tuple[float | None, bool]:
        """Return retry delay and whether the standard backoff was consumed."""
        status = response.status_code
        if status == 429:
            retry_after = _parse_retry_after(response.headers.get("Retry-After"))
            if retry_after is not None:
                return retry_after, False
            return self._backoff_delay(backoff_index), True

        if 500 <= status < 600:
            return self._backoff_delay(backoff_index), True

        return None, False

    def _backoff_delay(self, index: int) -> float:
        """Return the exponential retry delay for a retry index."""
        return float(2**index)

    def _log_post_result(
        self,
        *,
        entity_id: str,
        status: int,
        ok: bool,
        attempts: int,
        elapsed_ms: int,
        body_bytes: bytes,
        response_text: str | None,
        dry_run: bool,
        payload_mode: str,
    ) -> str | None:
        """Emit the terminal POST log record and return any response excerpt."""
        fields = payload_log_fields(
            body_bytes,
            response_text,
            ok=ok,
            mode=payload_mode,
            payload_max_bytes=self.payload_max_bytes,
            response_max_bytes=self.response_max_bytes,
        )
        extra = {
            "event": "post_succeeded" if ok else "post_failed",
            "entity_id": entity_id,
            "http_status": status,
            "ok": ok,
            "attempts": attempts,
            "elapsed_ms": elapsed_ms,
            "dry_run": dry_run,
            **fields,
        }

        if ok:
            logger.info("orion post succeeded", extra=extra)
            return None

        logger.error("orion post failed", extra=extra)
        return fields.get("response_excerpt")


def _required_env(env: Mapping[str, str], key: str) -> str:
    """Return a required environment value or raise a config error."""
    value = env.get(key)
    if value is None or value == "":
        raise OrionConfigError(f"missing required environment variable: {key}")
    return value


def _optional_env(env: Mapping[str, str], key: str, default: str) -> str:
    """Return the env value if set and non-empty, otherwise *default*."""
    value = env.get(key)
    if value is None or value == "":
        return default
    return value


def _parse_bool(value: str) -> bool:
    """Parse an environment boolean."""
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise OrionConfigError(f"invalid boolean value: {value!r}")


def _parse_retry_after(value: str | None) -> float | None:
    """Parse a Retry-After seconds header.

    Returns ``None`` for missing, non-numeric, negative, or non-finite
    values so the caller falls back to the standard backoff sequence
    instead of e.g. sleeping for ``inf`` seconds.
    """
    if value is None or value == "":
        return None
    try:
        delay = float(value)
    except ValueError:
        return None
    if not math.isfinite(delay) or delay < 0:
        return None
    return delay
