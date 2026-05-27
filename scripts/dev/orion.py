"""Dev probe: ad-hoc Orion NGSI v2 helper for interactive exploration.

Not used by the production pipeline. The pipeline uses
``sendai_pipeline.orion_client.OrionClient`` instead, which adds retry,
backoff, structured logging, and a 401-refresh-once policy. This module
keeps the broader surface (subscriptions, batch update, geo queries) that
the production client does not need but is useful for poking at the
platform from a REPL or notebook.

Usage from a project-root REPL:

    uv run python -i scripts/dev/orion.py

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

ORION = f"{BASE_URL}/orion/v2.0"


def _headers(content_type: str | None = None) -> dict[str, str]:
    """Build authenticated request headers; fetches a fresh token per call."""
    h = {
        "Authorization": f"Bearer {_auth.get_token()}",
        "Accept": "application/json",
    }
    if content_type:
        h["Content-Type"] = content_type
    if FIWARE_SERVICE:
        h["Fiware-Service"] = FIWARE_SERVICE
    h["Fiware-ServicePath"] = FIWARE_SERVICE_PATH
    return h


def create_entity(entity: dict) -> requests.Response:
    """POST /entities — create a new entity."""
    return requests.post(
        f"{ORION}/entities",
        json=entity,
        headers=_headers("application/json"),
        verify=VERIFY_TLS,
    )


def get_entity(entity_id: str, entity_type: str | None = None) -> requests.Response:
    """GET /entities/<id> — fetch a single entity."""
    params = {"type": entity_type} if entity_type else {}
    return requests.get(
        f"{ORION}/entities/{entity_id}",
        params=params,
        headers=_headers(),
        verify=VERIFY_TLS,
    )


def list_entities(**params) -> requests.Response:
    """GET /entities — list entities; pass type=, q=, limit=, etc. as kwargs."""
    return requests.get(
        f"{ORION}/entities",
        params=params,
        headers=_headers(),
        verify=VERIFY_TLS,
    )


def list_entities_near(
    lat: float,
    lon: float,
    max_distance: int,
    min_distance: int | None = None,
    **params,
) -> requests.Response:
    """GET /entities — filter by proximity to a point (WGS-84).

    max_distance: radius in metres.
    min_distance: optional inner radius in metres (for ring queries).
    Additional kwargs (type=, attrs=, limit=, etc.) are passed as query params.
    """
    georel = f"near;maxDistance:{max_distance}"
    if min_distance is not None:
        georel += f";minDistance:{min_distance}"
    return requests.get(
        f"{ORION}/entities",
        params={
            "georel": georel,
            "geometry": "point",
            "coords": f"{lat},{lon}",
            **params,
        },
        headers=_headers(),
        verify=VERIFY_TLS,
    )


def update_attrs(
    entity_id: str, attrs: dict, entity_type: str | None = None
) -> requests.Response:
    """POST /entities/<id>/attrs — append or update attributes."""
    params = {"type": entity_type} if entity_type else {}
    return requests.post(
        f"{ORION}/entities/{entity_id}/attrs",
        params=params,
        json=attrs,
        headers=_headers("application/json"),
        verify=VERIFY_TLS,
    )


def delete_entity(entity_id: str, entity_type: str | None = None) -> requests.Response:
    """DELETE /entities/<id> — remove an entity."""
    params = {"type": entity_type} if entity_type else {}
    return requests.delete(
        f"{ORION}/entities/{entity_id}",
        params=params,
        headers=_headers(),
        verify=VERIFY_TLS,
    )


def batch_update(action: str, entities: list) -> requests.Response:
    """POST /op/update — bulk create/update/delete.

    action: 'append' | 'update' | 'delete'
    """
    return requests.post(
        f"{ORION}/op/update",
        json={"actionType": action, "entities": entities},
        headers=_headers("application/json"),
        verify=VERIFY_TLS,
    )


def create_subscription(subscription: dict) -> requests.Response:
    """POST /subscriptions — register a new subscription."""
    return requests.post(
        f"{ORION}/subscriptions",
        json=subscription,
        headers=_headers("application/json"),
        verify=VERIFY_TLS,
    )


def list_subscriptions() -> requests.Response:
    """GET /subscriptions — list all subscriptions."""
    return requests.get(
        f"{ORION}/subscriptions",
        headers=_headers(),
        verify=VERIFY_TLS,
    )


def update_subscription(sub_id: str, patch: dict) -> requests.Response:
    """PATCH /subscriptions/<id> — update a subscription (e.g. extend expiry)."""
    return requests.patch(
        f"{ORION}/subscriptions/{sub_id}",
        json=patch,
        headers=_headers("application/json"),
        verify=VERIFY_TLS,
    )


def delete_subscription(sub_id: str) -> requests.Response:
    """DELETE /subscriptions/<id> — remove a subscription."""
    return requests.delete(
        f"{ORION}/subscriptions/{sub_id}",
        headers=_headers(),
        verify=VERIFY_TLS,
    )
