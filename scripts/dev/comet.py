"""Dev probe: ad-hoc STH-Comet historical-data helper for exploration.

Not used by the production pipeline. The production pipeline does not (yet)
read history; this module exists so you can sanity-check what Comet has
stored for an entity from a REPL or notebook.

Tokens come from :func:`sendai_pipeline.auth.get_token` so refreshes happen
transparently and the shared cache at ``state/token.json`` is reused.
"""

import os

import requests
from dotenv import load_dotenv

from sendai_pipeline.auth import AuthClient, AuthSettings

load_dotenv()

_settings = AuthSettings.from_env()
_auth = AuthClient(_settings)

BASE_URL = _settings.base_url
FIWARE_SERVICE = os.environ.get("FIWARE_SERVICE", "")
FIWARE_SERVICE_PATH = os.environ.get("FIWARE_SERVICE_PATH", "/")
VERIFY_TLS: bool | str = _settings.verify_tls

COMET = f"{BASE_URL}/comet/v1.0"


def _headers() -> dict[str, str]:
    """Build authenticated request headers; fetches a fresh token per call."""
    h = {
        "Authorization": f"Bearer {_auth.get_token()}",
        "Accept": "application/json",
    }
    if FIWARE_SERVICE:
        h["Fiware-Service"] = FIWARE_SERVICE
    h["Fiware-ServicePath"] = FIWARE_SERVICE_PATH
    return h


def get_history(
    entity_id: str,
    entity_type: str,
    attr: str,
    last_n: int | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    h_limit: int | None = None,
    h_offset: int | None = None,
    aggr_method: str | None = None,
    aggr_period: str | None = None,
) -> requests.Response:
    """GET /comet/v1.0/contextEntities/type/<type>/id/<id>/attributes/<attr>

    Fetch historical (time-series) data for a single attribute.
    One of the following param combinations is required by STH-Comet:
      - last_n: latest N records
      - h_limit + h_offset: pagination
      - aggr_method + aggr_period: aggregated stats such as max/min/sum/occur
        plus month/day/hour/minute/second

    date_from / date_to: ISO8601 strings, e.g. '2024-01-01T00:00:00Z'
    """
    params: dict[str, int | str] = {}
    if last_n is not None:
        params["lastN"] = last_n
    if date_from is not None:
        params["dateFrom"] = date_from
    if date_to is not None:
        params["dateTo"] = date_to
    if h_limit is not None:
        params["hLimit"] = h_limit
    if h_offset is not None:
        params["hOffset"] = h_offset
    if aggr_method is not None:
        params["aggrMethod"] = aggr_method
    if aggr_period is not None:
        params["aggrPeriod"] = aggr_period

    url = f"{COMET}/contextEntities/type/{entity_type}/id/{entity_id}/attributes/{attr}"
    return requests.get(url, params=params, headers=_headers(), verify=VERIFY_TLS)
