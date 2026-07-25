"""STH-Comet client for Sendai FIWARE history reads and deletes."""

import os
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol, Self

import requests


class CometError(RuntimeError):
    """Base class for STH-Comet client errors."""


class CometConfigError(CometError):
    """Raised when required STH-Comet configuration is missing or invalid."""


class TokenProvider(Protocol):
    """Anything that can provide a Sendai FIWARE access token."""

    def get_token(self, *, force_refresh: bool = False) -> str:
        """Return a valid bearer token, refreshing if asked."""
        ...


@dataclass
class CometSettings:
    """Configuration for Sendai FIWARE STH-Comet API requests."""

    base_url: str
    service: str = ""
    service_path: str = "/"
    verify_tls: bool = True
    timeout: float = 10

    def __post_init__(self) -> None:
        self.base_url = self.base_url.rstrip("/")

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> Self:
        """Build STH-Comet settings from environment variables."""
        values = os.environ if env is None else env
        return cls(
            base_url=_required_env(values, "FIWARE_BASE_URL").rstrip("/"),
            service=_optional_env(values, "FIWARE_SERVICE", ""),
            service_path=_optional_env(values, "FIWARE_SERVICE_PATH", "/"),
            verify_tls=_parse_bool(_optional_env(values, "FIWARE_VERIFY_TLS", "true")),
            timeout=float(_optional_env(values, "FIWARE_TIMEOUT_SECONDS", "10")),
        )


@dataclass(frozen=True)
class HistoryQuery:
    """Query parameters for one STH-Comet historical attribute read."""

    last_n: int | None = None
    date_from: str | None = None
    date_to: str | None = None
    h_limit: int | None = None
    h_offset: int | None = None
    aggr_method: str | None = None
    aggr_period: str | None = None


class CometClient:
    """Read and delete historical NGSI attribute values via STH-Comet."""

    def __init__(
        self,
        settings: CometSettings,
        *,
        auth: TokenProvider,
        session: Any = None,
    ) -> None:
        """Build an STH-Comet client."""
        self.settings = settings
        self.auth = auth
        self.session = session or requests.Session()

    def get_history(
        self,
        entity_id: str,
        entity_type: str,
        attr: str,
        *,
        query: HistoryQuery | None = None,
    ) -> dict[str, Any]:
        """Return historical values for one entity attribute.

        Raises:
            requests.HTTPError: If STH-Comet returns a non-2xx response.
        """
        response = self.session.get(
            self._history_url(entity_type, entity_id, attr),
            params=_query_params(query or HistoryQuery()),
            headers=self._headers(include_content_type=False),
            timeout=self.settings.timeout,
            verify=self.settings.verify_tls,
        )
        if response.status_code == 401:
            response = self.session.get(
                self._history_url(entity_type, entity_id, attr),
                params=_query_params(query or HistoryQuery()),
                headers=self._headers(
                    force_refresh=True,
                    include_content_type=False,
                ),
                timeout=self.settings.timeout,
                verify=self.settings.verify_tls,
            )
        response.raise_for_status()
        return response.json()

    def delete_attribute_history(
        self, entity_id: str, entity_type: str, attr: str
    ) -> int:
        """Delete historical values for one entity attribute.

        Returns:
            The STH-Comet response status code for successful deletion or
            already-absent history.

        Raises:
            requests.HTTPError: If STH-Comet returns a response other than
                success or already-absent history.
        """
        response = self.session.delete(
            self._history_url(entity_type, entity_id, attr),
            headers=self._headers(include_content_type=False),
            timeout=self.settings.timeout,
            verify=self.settings.verify_tls,
        )
        if response.status_code == 401:
            response = self.session.delete(
                self._history_url(entity_type, entity_id, attr),
                headers=self._headers(
                    force_refresh=True,
                    include_content_type=False,
                ),
                timeout=self.settings.timeout,
                verify=self.settings.verify_tls,
            )
        if response.status_code not in {204, 404}:
            # raise_for_status() only fires on non-2xx; an unexpected 2xx
            # (the swagger documents only 204) must still be treated as
            # a contract violation, not a silent success.
            response.raise_for_status()
            raise requests.HTTPError(
                f"unexpected STH-Comet DELETE status {response.status_code}",
                response=response,
            )
        return response.status_code

    def delete_entity_history(self, entity_id: str, entity_type: str) -> int:
        """Delete all historical values for one entity.

        Returns:
            The STH-Comet response status code for successful deletion or
            already-absent history.

        Raises:
            requests.HTTPError: If STH-Comet returns a response other than
                success or already-absent history.
        """
        response = self.session.delete(
            self._entity_history_url(entity_type, entity_id),
            headers=self._headers(include_content_type=False),
            timeout=self.settings.timeout,
            verify=self.settings.verify_tls,
        )
        if response.status_code == 401:
            response = self.session.delete(
                self._entity_history_url(entity_type, entity_id),
                headers=self._headers(
                    force_refresh=True,
                    include_content_type=False,
                ),
                timeout=self.settings.timeout,
                verify=self.settings.verify_tls,
            )
        if response.status_code not in {204, 404}:
            # raise_for_status() only fires on non-2xx; an unexpected 2xx
            # (the swagger documents only 204) must still be treated as
            # a contract violation, not a silent success.
            response.raise_for_status()
            raise requests.HTTPError(
                f"unexpected STH-Comet DELETE status {response.status_code}",
                response=response,
            )
        return response.status_code

    def _history_url(self, entity_type: str, entity_id: str, attr: str) -> str:
        """Return the STH-Comet historical attribute endpoint URL."""
        return (
            f"{self.settings.base_url}/comet/v1.0/contextEntities/type/"
            f"{entity_type}/id/{entity_id}/attributes/{attr}"
        )

    def _entity_history_url(self, entity_type: str, entity_id: str) -> str:
        """Return the STH-Comet historical entity endpoint URL."""
        return (
            f"{self.settings.base_url}/comet/v1.0/contextEntities/type/"
            f"{entity_type}/id/{entity_id}"
        )

    def _headers(
        self,
        *,
        force_refresh: bool = False,
        include_content_type: bool = True,
    ) -> dict[str, str]:
        """Build headers for authenticated STH-Comet requests."""
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


def _query_params(query: HistoryQuery) -> dict[str, int | str]:
    """Return STH-Comet query parameters, omitting unset fields."""
    params: dict[str, int | str] = {}
    if query.last_n is not None:
        params["lastN"] = query.last_n
    if query.date_from is not None:
        params["dateFrom"] = query.date_from
    if query.date_to is not None:
        params["dateTo"] = query.date_to
    if query.h_limit is not None:
        params["hLimit"] = query.h_limit
    if query.h_offset is not None:
        params["hOffset"] = query.h_offset
    if query.aggr_method is not None:
        params["aggrMethod"] = query.aggr_method
    if query.aggr_period is not None:
        params["aggrPeriod"] = query.aggr_period
    return params


def _required_env(env: Mapping[str, str], name: str) -> str:
    """Return a required environment value or raise a config error."""
    value = env.get(name)
    if value is None or value.strip() == "":
        raise CometConfigError(f"missing required environment variable: {name}")
    return value


def _optional_env(env: Mapping[str, str], name: str, default: str) -> str:
    """Return an optional value, defaulting missing or whitespace-only input.

    Nonblank values are returned unchanged, including surrounding whitespace.
    """
    value = env.get(name)
    if value is None or value.strip() == "":
        return default
    return value


def _parse_bool(value: str) -> bool:
    """Parse environment-style booleans."""
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise CometConfigError(f"invalid boolean value: {value!r}")
