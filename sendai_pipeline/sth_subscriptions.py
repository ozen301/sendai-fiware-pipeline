"""Create Orion subscriptions for STH-Comet history (Products A and B)."""

import json
import logging
import os
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Protocol, Self

import requests

from sendai_pipeline.settings_validation import parse_exact_env_value

logger = logging.getLogger(__name__)

JST = timezone(timedelta(hours=9))

PRODUCT_A_ENTITY_TYPES: tuple[str, ...] = ("Blesensor.per300", "Blesensor.per3600")
PRODUCT_A_HISTORY_ATTRS: tuple[str, ...] = (
    "dateObservedFrom",
    "dateObservedTo",
    "peopleCount_immedate",
    "peopleCount_near",
    "peopleCount_far",
    "peopleOccupancy_immedate",
    "peopleOccupancy_near",
)
PRODUCT_A_TRIGGER_ATTRS: tuple[str, ...] = ("peopleCount_immedate",)

_PRODUCT_A_DESCRIPTION_PREFIX = "Product A STH-Comet history"
_PRODUCT_B_DESCRIPTION_PREFIX = "Product B aggregate STH-Comet history"
PRODUCT_B_STABLE_WRITE_ATTRS: tuple[str, ...] = (
    "dateObservedFrom",
    "dateObservedTo",
    "dateRetrieved",
    "identifcation",
)


@dataclass(frozen=True)
class _ProductSpec:
    """Per-product values that vary between Product A and Product B."""

    label: str
    description_prefix: str
    entity_types: tuple[str, ...]
    history_attrs: tuple[str, ...]
    trigger_attrs: tuple[str, ...]


_PRODUCT_A_SPEC = _ProductSpec(
    label="Product A",
    description_prefix=_PRODUCT_A_DESCRIPTION_PREFIX,
    entity_types=PRODUCT_A_ENTITY_TYPES,
    history_attrs=PRODUCT_A_HISTORY_ATTRS,
    trigger_attrs=PRODUCT_A_TRIGGER_ATTRS,
)


class StHSubscriptionError(RuntimeError):
    """Raised when STH subscription configuration or creation fails."""


class TokenProvider(Protocol):
    """Anything that can provide a Sendai FIWARE access token."""

    def get_token(self, *, force_refresh: bool = False) -> str:
        """Return a valid bearer token, refreshing if asked."""
        ...


@dataclass(frozen=True)
class StHSubscriptionSettings:
    """Settings for Product A and Product B STH-Comet subscriptions."""

    base_url: str
    comet_notify_url: str
    service: str = ""
    service_path: str = "/"
    verify_tls: bool = True
    timeout: float = 10
    dry_run: bool = True
    expires: str = ""
    skip_initial_notification: bool = True
    product_b_aggregate_entity_id: str = "jp.sendai.Blesensor.flow"
    product_b_aggregate_entity_type: str = "Blesensor.flow"

    def __post_init__(self) -> None:
        object.__setattr__(self, "base_url", self.base_url.rstrip("/"))

    @classmethod
    def from_env(
        cls,
        env: Mapping[str, str] | None = None,
        *,
        require_product_b: bool = True,
    ) -> Self:
        """Build settings from environment variables.

        Args:
            env: Environment mapping to read; defaults to ``os.environ``.
            require_product_b: When ``True`` (the default), read and validate
                the ``PRODUCT_B_AGGREGATE_*`` variables, raising on a malformed
                value. When ``False``, skip them entirely and leave the field
                defaults in place. A Product-A-only run passes ``False`` so a
                malformed Product B value never fails it; only a run that
                creates the Product B subscription needs those variables.
        """
        values = os.environ if env is None else env
        # Omit the Product B kwargs when not required so the dataclass field
        # defaults apply and no PRODUCT_B_AGGREGATE_* value is read or validated.
        product_b_kwargs: dict[str, str] = {}
        if require_product_b:
            product_b_kwargs = {
                "product_b_aggregate_entity_id": parse_exact_env_value(
                    values,
                    "PRODUCT_B_AGGREGATE_ENTITY_ID",
                    "jp.sendai.Blesensor.flow",
                    StHSubscriptionError,
                ),
                "product_b_aggregate_entity_type": parse_exact_env_value(
                    values,
                    "PRODUCT_B_AGGREGATE_ENTITY_TYPE",
                    "Blesensor.flow",
                    StHSubscriptionError,
                ),
            }
        return cls(
            base_url=_required_env(values, "FIWARE_BASE_URL"),
            comet_notify_url=_required_env(values, "COMET_NOTIFY_URL"),
            **product_b_kwargs,
            service=_optional_env(values, "FIWARE_SERVICE", ""),
            service_path=_optional_env(values, "FIWARE_SERVICE_PATH", "/"),
            verify_tls=_parse_bool(_optional_env(values, "FIWARE_VERIFY_TLS", "true")),
            timeout=_parse_float(values, "FIWARE_TIMEOUT_SECONDS", 10.0),
            dry_run=True,
            expires=_optional_env(values, "STH_SUBSCRIPTION_EXPIRES", ""),
            skip_initial_notification=_parse_bool(
                _optional_env(values, "STH_SUBSCRIPTION_SKIP_INITIAL", "true")
            ),
        )


@dataclass(frozen=True)
class StHSubscriptionResult:
    """Outcome summary for one subscription creation run."""

    would_create: int
    created: int
    skipped: int
    failed: int
    subscription_ids: tuple[str, ...] = ()

    @property
    def exit_code(self) -> int:
        """Return 1 when any subscription failed, otherwise 0."""
        return 1 if self.failed > 0 else 0


def _build_product_a_subscription_body(
    spec: _ProductSpec,
    settings: StHSubscriptionSettings,
    *,
    redact_url: bool = False,
    now: datetime | None = None,
) -> dict[str, Any]:
    description_time = now or datetime.now(tz=JST)
    body: dict[str, Any] = {
        "description": (
            f"{spec.description_prefix} set at "
            f"{description_time.isoformat(timespec='seconds')}"
        ),
        "subject": {
            "entities": [
                {"idPattern": ".*", "type": entity_type}
                for entity_type in spec.entity_types
            ],
            "condition": {
                "attrs": list(spec.trigger_attrs),
                "notifyOnMetadataChange": True,
            },
        },
        "notification": {
            "http": {
                "url": "<COMET_NOTIFY_URL>" if redact_url else settings.comet_notify_url
            },
            "attrsFormat": "legacy",
            "attrs": list(spec.history_attrs),
            "metadata": ["TimeInstant"],
        },
    }
    if settings.expires:
        body["expires"] = settings.expires
    return body


def build_product_a_subscription_body(
    settings: StHSubscriptionSettings,
    *,
    redact_url: bool = False,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build the Orion subscription body for Product A STH-Comet history."""
    return _build_product_a_subscription_body(
        _PRODUCT_A_SPEC, settings, redact_url=redact_url, now=now
    )


def build_product_b_subscription_body(
    settings: StHSubscriptionSettings,
    *,
    redact_url: bool = False,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build the Orion subscription body for Product B STH-Comet history."""
    description_time = now or datetime.now(tz=JST)
    body: dict[str, Any] = {
        "description": (
            f"{_PRODUCT_B_DESCRIPTION_PREFIX} set at "
            f"{description_time.isoformat(timespec='seconds')}"
        ),
        "subject": {
            "entities": [
                {
                    "id": settings.product_b_aggregate_entity_id,
                    "type": settings.product_b_aggregate_entity_type,
                }
            ],
            "condition": {
                "attrs": ["dateRetrieved"],
                "notifyOnMetadataChange": True,
            },
        },
        "notification": {
            "http": {
                "url": "<COMET_NOTIFY_URL>" if redact_url else settings.comet_notify_url
            },
            "attrsFormat": "legacy",
            "metadata": ["TimeInstant"],
        },
    }
    if settings.expires:
        body["expires"] = settings.expires
    return body


def _create_sth_subscription(
    *,
    label: str,
    own_history_attrs: tuple[str, ...],
    peer_label: str,
    peer_description_prefixes: tuple[str, ...],
    body_builder: Callable[[StHSubscriptionSettings], dict[str, Any]],
    subscription_matcher: Callable[[StHSubscriptionSettings, Any], tuple[str, ...]],
    settings: StHSubscriptionSettings,
    auth: TokenProvider | None,
    session: Any = None,
) -> StHSubscriptionResult:
    """Create one product's STH-Comet subscription, idempotently and safely.

    Runs the shared create flow used by both products:

    1. In dry-run mode, log the intent and return without any HTTP call.
    2. GET the existing subscriptions (the preflight read); return ``failed``
       when the response is not 200 after the authentication retry.
    3. Abort if a *peer* product's subscription would cross-fire on this
       product's writes (see ``_find_stale_peer_subscription``). Creating this
       subscription alongside such a peer would append duplicate history rows,
       so the run fails instead of creating it.
    4. Skip if this product's subscription already exists, per
       ``subscription_matcher`` (the idempotent no-op on re-runs).
    5. Otherwise POST the built body. Return ``created`` and report its id for
       a 201 response; return ``failed`` for any other response after the
       authentication retry.

    Args:
        label: Product name used in log messages, e.g. ``"Product A"``.
        own_history_attrs: The product's stable written attribute names to
            compare with peer triggers. The stale-peer check aborts when a peer
            subscription triggers on any of these (or triggers on everything),
            since that peer would then fire on this product's own updates.
        peer_label: The other product's name, used in log messages.
        peer_description_prefixes: Description prefixes that identify the peer
            product's subscriptions during the stale-peer check.
        body_builder: Builds the subscription body to POST for this product.
        subscription_matcher: Returns the ids of existing subscriptions that
            already match this product's contract; drives the step-4 skip.
        settings: Subscription settings (base URL, TLS, dry-run, entity ids).
        auth: Token provider; required unless ``settings.dry_run`` is set.
        session: Optional injected HTTP session for tests.

    Returns:
        A result counting exactly one of ``would_create`` / ``created`` /
        ``skipped`` / ``failed``, carrying the matched or created subscription
        id when there is one.
    """
    if settings.dry_run:
        logger.info(
            f"dry-run: would create {label} STH subscription",
            extra={
                "event": "sth_subscription_would_create",
                "dry_run": True,
                "count_would_create": 1,
            },
        )
        return StHSubscriptionResult(would_create=1, created=0, skipped=0, failed=0)

    if auth is None:
        raise StHSubscriptionError("auth is required when dry_run is false")

    http = session or requests.Session()
    existing_response = http.get(
        f"{settings.base_url}/orion/v2.0/subscriptions",
        headers=_headers(auth.get_token(), settings, include_content_type=False),
        timeout=settings.timeout,
        verify=settings.verify_tls,
    )
    if existing_response.status_code == 401:
        existing_response = http.get(
            f"{settings.base_url}/orion/v2.0/subscriptions",
            headers=_headers(
                auth.get_token(force_refresh=True),
                settings,
                include_content_type=False,
            ),
            timeout=settings.timeout,
            verify=settings.verify_tls,
        )
    if existing_response.status_code != 200:
        logger.error(
            f"{label} STH subscription preflight failed",
            extra={
                "event": "sth_subscription_failed",
                "dry_run": False,
                "http_status": existing_response.status_code,
                "response_excerpt": existing_response.text[:512],
                "count_failed": 1,
            },
        )
        return StHSubscriptionResult(would_create=0, created=0, skipped=0, failed=1)

    existing_subscriptions = existing_response.json()

    stale_peer = _find_stale_peer_subscription(
        existing_subscriptions,
        own_history_attrs=own_history_attrs,
        peer_label=peer_label,
        peer_description_prefixes=peer_description_prefixes,
    )
    if stale_peer is not None:
        peer_label, peer_id, peer_trigger = stale_peer
        logger.error(
            (
                f"{label} STH subscription aborted: peer "
                f"{peer_label} subscription has a non-exclusive trigger that "
                "would be fired by this product's updates"
            ),
            extra={
                "event": "sth_subscription_failed",
                "dry_run": False,
                "subscription_id": peer_id,
                "peer_product": peer_label,
                "peer_trigger_attrs": peer_trigger,
                "count_failed": 1,
            },
        )
        return StHSubscriptionResult(would_create=0, created=0, skipped=0, failed=1)

    existing_ids = subscription_matcher(settings, existing_subscriptions)
    if existing_ids:
        logger.info(
            f"{label} STH subscription already exists",
            extra={
                "event": "sth_subscription_exists",
                "dry_run": False,
                "subscription_id": existing_ids[0],
                "count_skipped": 1,
            },
        )
        return StHSubscriptionResult(
            would_create=0,
            created=0,
            skipped=1,
            failed=0,
            subscription_ids=existing_ids,
        )

    body = body_builder(settings)
    body_bytes = json.dumps(body, sort_keys=True, separators=(",", ":")).encode("utf-8")
    response = http.post(
        f"{settings.base_url}/orion/v2.0/subscriptions",
        params=_create_params(settings),
        data=body_bytes,
        headers=_headers(auth.get_token(), settings),
        timeout=settings.timeout,
        verify=settings.verify_tls,
    )
    if response.status_code == 401:
        response = http.post(
            f"{settings.base_url}/orion/v2.0/subscriptions",
            params=_create_params(settings),
            data=body_bytes,
            headers=_headers(auth.get_token(force_refresh=True), settings),
            timeout=settings.timeout,
            verify=settings.verify_tls,
        )

    if response.status_code == 201:
        subscription_id = response.headers.get("Location", "").rsplit("/", 1)[-1]
        logger.info(
            f"{label} STH subscription created",
            extra={
                "event": "sth_subscription_created",
                "dry_run": False,
                "http_status": response.status_code,
                "subscription_id": subscription_id,
                "count_created": 1,
            },
        )
        return StHSubscriptionResult(
            would_create=0,
            created=1,
            failed=0,
            skipped=0,
            subscription_ids=(subscription_id,) if subscription_id else (),
        )

    logger.error(
        f"{label} STH subscription creation failed",
        extra={
            "event": "sth_subscription_failed",
            "dry_run": False,
            "http_status": response.status_code,
            "response_excerpt": response.text[:512],
            "count_failed": 1,
        },
    )
    return StHSubscriptionResult(would_create=0, created=0, skipped=0, failed=1)


def create_product_a_sth_subscription(
    *,
    settings: StHSubscriptionSettings,
    auth: TokenProvider | None,
    session: Any = None,
) -> StHSubscriptionResult:
    """Create the Product A Orion subscription, or report it in dry-run mode."""
    return _create_sth_subscription(
        label="Product A",
        own_history_attrs=PRODUCT_A_HISTORY_ATTRS,
        peer_label="Product B",
        peer_description_prefixes=(_PRODUCT_B_DESCRIPTION_PREFIX,),
        body_builder=build_product_a_subscription_body,
        subscription_matcher=_matching_product_a_subscription_ids,
        settings=settings,
        auth=auth,
        session=session,
    )


def create_product_b_sth_subscription(
    *,
    settings: StHSubscriptionSettings,
    auth: TokenProvider | None,
    session: Any = None,
) -> StHSubscriptionResult:
    """Create the Product B Orion subscription, or report it in dry-run mode."""
    return _create_sth_subscription(
        label="Product B",
        # Product B always writes these stable scalar attrs. A Product A
        # subscription triggering on any of them would fire on Product B's
        # writes. Dynamic peopleCount_flow_<N> attrs are absent because their
        # names are not enumerable; the current Product A trigger names only
        # peopleCount_immedate, so it cannot overlap them.
        own_history_attrs=PRODUCT_B_STABLE_WRITE_ATTRS,
        peer_label="Product A",
        peer_description_prefixes=(_PRODUCT_A_DESCRIPTION_PREFIX,),
        body_builder=build_product_b_subscription_body,
        subscription_matcher=_matching_product_b_subscription_ids,
        settings=settings,
        auth=auth,
        session=session,
    )


def get_subscription(
    subscription_id: str,
    *,
    settings: Any,
    auth: TokenProvider,
    session: Any = None,
) -> dict[str, Any] | None:
    """Return one Orion subscription by id, or ``None`` if it is absent.

    Settings is structurally typed: any object exposing the fields used
    by ``_headers`` (``base_url``, ``service``, ``service_path``,
    ``verify_tls``, ``timeout``) works. Both ``StHSubscriptionSettings``
    and ``sendai_pipeline.orion_client.OrionSettings`` qualify; the
    delete tool uses ``OrionSettings`` since it has no need for
    ``COMET_NOTIFY_URL``.

    401 triggers one forced-refresh retry. Non-2xx other than 404 raises
    ``requests.HTTPError`` so callers can decide how to surface it.
    """
    http = session or requests.Session()
    url = f"{settings.base_url}/orion/v2.0/subscriptions/{subscription_id}"
    response = http.get(
        url,
        headers=_headers(auth.get_token(), settings, include_content_type=False),
        timeout=settings.timeout,
        verify=settings.verify_tls,
    )
    if response.status_code == 401:
        response = http.get(
            url,
            headers=_headers(
                auth.get_token(force_refresh=True),
                settings,
                include_content_type=False,
            ),
            timeout=settings.timeout,
            verify=settings.verify_tls,
        )
    if response.status_code == 404:
        return None
    if response.status_code != 200:
        raise requests.HTTPError(
            f"unexpected Orion GET subscription status {response.status_code}",
            response=response,
        )
    body = response.json()
    if not isinstance(body, dict):
        raise requests.HTTPError(
            "Orion GET subscription returned a non-object body",
            response=response,
        )
    return body


def delete_subscription(
    subscription_id: str,
    *,
    settings: Any,
    auth: TokenProvider,
    session: Any = None,
) -> int:
    """Delete one Orion subscription by id and return the HTTP status.

    Settings is structurally typed; see :func:`get_subscription`.

    Returns 204 on success and 204/404 are the only success-shaped
    outcomes (404 means another operator already removed it). 401
    triggers one forced-refresh retry. Any other status raises
    ``requests.HTTPError``.
    """
    http = session or requests.Session()
    url = f"{settings.base_url}/orion/v2.0/subscriptions/{subscription_id}"
    response = http.delete(
        url,
        headers=_headers(auth.get_token(), settings, include_content_type=False),
        timeout=settings.timeout,
        verify=settings.verify_tls,
    )
    if response.status_code == 401:
        response = http.delete(
            url,
            headers=_headers(
                auth.get_token(force_refresh=True),
                settings,
                include_content_type=False,
            ),
            timeout=settings.timeout,
            verify=settings.verify_tls,
        )
    if response.status_code not in {204, 404}:
        raise requests.HTTPError(
            f"unexpected Orion DELETE subscription status {response.status_code}",
            response=response,
        )
    return int(response.status_code)


def redacted_subscription_json(settings: StHSubscriptionSettings) -> str:
    """Return pretty JSON for operator review without the private notify URL.

    Kept for backward compatibility — prefer
    ``redacted_product_a_subscription_json`` /
    ``redacted_product_b_subscription_json`` in new code.
    """
    return redacted_product_a_subscription_json(settings)


def redacted_product_a_subscription_json(settings: StHSubscriptionSettings) -> str:
    """Return pretty Product A JSON for operator review with the URL redacted."""
    return _render_subscription_json(
        build_product_a_subscription_body(settings, redact_url=True)
    )


def redacted_product_b_subscription_json(settings: StHSubscriptionSettings) -> str:
    """Return pretty Product B JSON for operator review with the URL redacted."""
    return _render_subscription_json(
        build_product_b_subscription_body(settings, redact_url=True)
    )


def _render_subscription_json(body: Mapping[str, Any]) -> str:
    return json.dumps(
        body,
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
    )


def _create_params(settings: StHSubscriptionSettings) -> dict[str, str]:
    """Return Orion subscription creation query params."""
    if settings.skip_initial_notification:
        return {"options": "skipInitialNotification"}
    return {}


def _headers(
    token: str,
    settings: Any,
    *,
    include_content_type: bool = True,
) -> dict[str, str]:
    """Build headers for Orion subscription requests."""
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Fiware-ServicePath": settings.service_path,
    }
    if include_content_type:
        headers["Content-Type"] = "application/json"
    if settings.service:
        headers["Fiware-Service"] = settings.service
    return headers


def _find_stale_peer_subscription(
    subscriptions: Any,
    *,
    own_history_attrs: tuple[str, ...],
    peer_label: str,
    peer_description_prefixes: tuple[str, ...],
) -> tuple[str, str, list[str]] | None:
    """Return a peer subscription whose trigger could fire on this product's writes.

    Returns ``(peer_label, peer_id, peer_trigger_attrs)`` when a subscription
    with one of the supplied current peer prefixes has a trigger this product's
    normal updates could fire — either because it overlaps the supplied written
    attrs, or because its trigger is absent, empty, or unparseable and is
    treated as broad (an Orion subscription with no effective ``condition.attrs``
    fires for every attribute update on its subject entities). Returns ``None``
    when no recognized current peer subscription has such a trigger.

    The comparison is on trigger attributes only; it deliberately ignores which
    entities the peer targets. Hence "could" rather than "would": a disjoint
    entity set means no real cross-fire, but the check still returns the peer.
    This is a conservative fail-safe — it can abort creation on a
    trigger-attribute overlap alone, rather than risk missing a real cross-fire.
    """
    if not isinstance(subscriptions, list):
        return None
    own_history = set(own_history_attrs)
    for subscription in subscriptions:
        if not isinstance(subscription, dict):
            continue
        subscription_id = subscription.get("id")
        description = subscription.get("description")
        if not isinstance(subscription_id, str) or not isinstance(description, str):
            continue
        if not description.startswith(peer_description_prefixes):
            continue
        subject = subscription.get("subject")
        condition_attrs: list[str] = []
        if isinstance(subject, dict):
            condition = subject.get("condition")
            if isinstance(condition, dict):
                raw = condition.get("attrs")
                if isinstance(raw, list):
                    condition_attrs = [item for item in raw if isinstance(item, str)]
        if not condition_attrs or set(condition_attrs) & own_history:
            return peer_label, subscription_id, condition_attrs
    return None


def _matching_product_a_subscription_ids(
    _settings: StHSubscriptionSettings, subscriptions: Any
) -> tuple[str, ...]:
    """Return ids for subscriptions matching Product A's existing shape."""
    return _matching_subscription_ids(_PRODUCT_A_SPEC, subscriptions)


def _matching_product_b_subscription_ids(
    settings: StHSubscriptionSettings, subscriptions: Any
) -> tuple[str, ...]:
    """Return ids for subscriptions matching the aggregate Product B shape."""
    if not isinstance(subscriptions, list):
        return ()

    matches: list[str] = []
    for subscription in subscriptions:
        if not isinstance(subscription, dict):
            continue
        subscription_id = subscription.get("id")
        if (
            isinstance(subscription_id, str)
            and subscription_id
            and _has_product_b_aggregate_shape(settings, subscription)
        ):
            matches.append(subscription_id)
    return tuple(matches)


def _has_product_b_aggregate_shape(
    settings: StHSubscriptionSettings, subscription: Mapping[str, Any]
) -> bool:
    """Return whether a live subscription is Product B's aggregate subscription.

    Matches only the exact redesigned shape: the Product B aggregate
    description prefix, the configured aggregate entity id/type, a
    ``dateRetrieved`` metadata-change trigger, legacy format, ``TimeInstant``
    metadata, and the configured notify URL. Two checks are subtle:

    - ``notification.attrs``: the builder omits it so Orion notifies the full
      replaced entity. Orion echoes an omitted ``attrs`` as ``[]`` on read, so
      an omitted key and an empty list both match here; any non-empty ``attrs``
      is a different subscription and does not match. This keeps a re-run from
      creating a duplicate of the subscription it just wrote.
    - The notify URL must equal the configured URL exactly; a different URL is a
      different subscription, not this one.
    """
    description = subscription.get("description")
    subject = subscription.get("subject")
    notification = subscription.get("notification")
    if (
        not isinstance(description, str)
        or not description.startswith(_PRODUCT_B_DESCRIPTION_PREFIX)
        or not isinstance(subject, dict)
        or not isinstance(notification, dict)
    ):
        return False

    condition = subject.get("condition")
    http = notification.get("http")
    if not isinstance(condition, dict) or not isinstance(http, dict):
        return False

    notification_attrs_match = (
        "attrs" not in notification or notification.get("attrs") == []
    )
    return (
        subject.get("entities")
        == [
            {
                "id": settings.product_b_aggregate_entity_id,
                "type": settings.product_b_aggregate_entity_type,
            }
        ]
        and condition.get("attrs") == ["dateRetrieved"]
        and condition.get("notifyOnMetadataChange") is True
        and http.get("url") == settings.comet_notify_url
        and notification.get("attrsFormat") == "legacy"
        and notification.get("metadata") == ["TimeInstant"]
        and notification_attrs_match
    )


def _matching_subscription_ids(
    spec: _ProductSpec, subscriptions: Any
) -> tuple[str, ...]:
    """Return ids for subscriptions matching this product's history shape."""
    if not isinstance(subscriptions, list):
        return ()

    matches: list[str] = []
    for subscription in subscriptions:
        if not isinstance(subscription, dict):
            continue
        subscription_id = subscription.get("id")
        if not isinstance(subscription_id, str) or not subscription_id:
            continue
        description = subscription.get("description")
        if isinstance(description, str) and description.startswith(
            spec.description_prefix
        ):
            matches.append(subscription_id)
            continue
        if _has_product_shape(spec, subscription):
            matches.append(subscription_id)
    return tuple(matches)


def _has_product_shape(spec: _ProductSpec, subscription: Mapping[str, Any]) -> bool:
    """Return whether a subscription already covers this product's STH history."""
    subject = subscription.get("subject")
    notification = subscription.get("notification")
    if not isinstance(subject, dict) or not isinstance(notification, dict):
        return False

    condition = subject.get("condition")
    if not isinstance(condition, dict):
        return False

    entities = subject.get("entities")
    if not isinstance(entities, list):
        return False
    entity_types = {
        entity.get("type")
        for entity in entities
        if isinstance(entity, dict) and isinstance(entity.get("type"), str)
    }
    if not set(spec.entity_types) <= entity_types:
        return False

    condition_attrs = condition.get("attrs")
    notification_attrs = notification.get("attrs")
    if not isinstance(condition_attrs, list) or not isinstance(
        notification_attrs, list
    ):
        return False

    return (
        notification.get("attrsFormat") == "legacy"
        and notification.get("metadata") == ["TimeInstant"]
        and condition.get("notifyOnMetadataChange") is True
        and set(spec.trigger_attrs) <= set(condition_attrs)
        and set(spec.history_attrs) <= set(notification_attrs)
    )


def _required_env(env: Mapping[str, str], key: str) -> str:
    """Return a required environment value or raise a config error."""
    value = env.get(key)
    if value is None or value.strip() == "":
        raise StHSubscriptionError(f"missing required environment variable: {key}")
    return value.strip()


def _optional_env(env: Mapping[str, str], key: str, default: str) -> str:
    """Return the env value if set and non-empty, otherwise *default*."""
    value = env.get(key)
    if value is None or value.strip() == "":
        return default
    return value.strip()


def _parse_bool(value: str) -> bool:
    """Parse an environment-style boolean."""
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise StHSubscriptionError(f"invalid boolean value: {value!r}")


def _parse_float(env: Mapping[str, str], key: str, default: float) -> float:
    """Parse an optional float environment variable."""
    raw = _optional_env(env, key, "")
    if raw == "":
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise StHSubscriptionError(
            f"environment variable must be a number: {key}"
        ) from exc
