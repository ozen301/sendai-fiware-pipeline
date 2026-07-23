"""Create Orion subscriptions for STH-Comet history (Products A and B)."""

import json
import logging
import os
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, timezone
from typing import Any, Protocol, Self

import requests

from sendai_pipeline.settings_validation import parse_exact_env_value

logger = logging.getLogger(__name__)

JST = timezone(timedelta(hours=9))

PRODUCT_A_ENTITY_TYPES: tuple[str, ...] = ("Blesensor.per300", "Blesensor.per3600")
PRODUCT_A_HISTORY_ATTRS: tuple[str, ...] = (
    "dateObservedFrom",
    "dateObservedTo",
    "dateRetrieved",
    "identifcation",
    "peopleCount_immedate",
    "peopleCount_near",
    "peopleCount_far",
    "peopleOccupancy_immedate",
    "peopleOccupancy_near",
    "peopleOccupancy_far",
)
PRODUCT_A_TRIGGER_ATTRS: tuple[str, ...] = PRODUCT_A_HISTORY_ATTRS

_PRODUCT_A_DESCRIPTION_PREFIX = "Product A STH-Comet history"
_PRODUCT_B_DESCRIPTION_PREFIX = "Product B aggregate STH-Comet history"
_PRODUCT_B_LEGACY_DESCRIPTION_PREFIX = "Product B STH-Comet history"
_SUBSCRIPTION_INVENTORY_PAGE_SIZE = 100
_NOTIFICATION_RUNTIME_FIELDS: frozenset[str] = frozenset(
    {
        "failsCounter",
        "lastFailure",
        "lastFailureCode",
        "lastFailureReason",
        "lastNotification",
        "lastSuccess",
        "lastSuccessCode",
        "timesSent",
    }
)
_NOTIFICATION_FALSE_DEFAULT_FIELDS: frozenset[str] = frozenset(
    {
        "covered",
        "onlyChangedAttrs",
    }
)
PRODUCT_B_STABLE_WRITE_ATTRS: tuple[str, ...] = (
    "dateObservedFrom",
    "dateObservedTo",
    "dateRetrieved",
    "identifcation",
    "sourceQuality",
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


class SubscriptionInventoryError(RuntimeError):
    """Failure while reading Orion's complete subscription inventory."""

    def __init__(self, message: str, *, http_status: int = 0) -> None:
        super().__init__(message)
        self.http_status = http_status
        self.response_excerpt = message[:512]


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


def fetch_subscription_inventory(
    *,
    settings: Any,
    auth: TokenProvider,
    session: Any = None,
) -> list[dict[str, Any]]:
    """Read and validate Orion's complete counted subscription inventory."""
    http = requests.Session() if session is None else session
    url = f"{settings.base_url}/orion/v2.0/subscriptions"
    subscriptions: list[dict[str, Any]] = []
    subscription_ids: set[str] = set()
    expected_total: int | None = None
    offset = 0

    while expected_total is None or offset < expected_total:
        params = {
            "limit": _SUBSCRIPTION_INVENTORY_PAGE_SIZE,
            "offset": offset,
            "options": "count",
        }
        try:
            response = http.get(
                url,
                params=params,
                headers=_headers(
                    auth.get_token(), settings, include_content_type=False
                ),
                timeout=settings.timeout,
                verify=settings.verify_tls,
            )
            if response.status_code == 401:
                response = http.get(
                    url,
                    params=params,
                    headers=_headers(
                        auth.get_token(force_refresh=True),
                        settings,
                        include_content_type=False,
                    ),
                    timeout=settings.timeout,
                    verify=settings.verify_tls,
                )
        except requests.RequestException as exc:
            raise SubscriptionInventoryError(str(exc)) from exc
        if response.status_code != 200:
            raise SubscriptionInventoryError(
                response.text,
                http_status=response.status_code,
            )

        total_text = _response_header(response.headers, "Fiware-Total-Count")
        if total_text is None or not total_text.isdigit():
            raise SubscriptionInventoryError(
                "Orion subscription inventory returned a missing or invalid "
                "Fiware-Total-Count",
                http_status=response.status_code,
            )
        page_total = int(total_text)
        if expected_total is None:
            expected_total = page_total
        elif page_total != expected_total:
            raise SubscriptionInventoryError(
                "Orion subscription inventory total changed between pages",
                http_status=response.status_code,
            )

        try:
            page = response.json()
        except (TypeError, ValueError) as exc:
            raise SubscriptionInventoryError(
                "Orion subscription inventory returned invalid JSON",
                http_status=response.status_code,
            ) from exc
        if not isinstance(page, list):
            raise SubscriptionInventoryError(
                "Orion subscription inventory returned a non-list page",
                http_status=response.status_code,
            )

        expected_page_size = min(
            _SUBSCRIPTION_INVENTORY_PAGE_SIZE,
            expected_total - offset,
        )
        if len(page) != expected_page_size:
            raise SubscriptionInventoryError(
                "Orion subscription inventory was incomplete",
                http_status=response.status_code,
            )
        for subscription in page:
            if not isinstance(subscription, dict):
                raise SubscriptionInventoryError(
                    "Orion subscription inventory returned a malformed entry",
                    http_status=response.status_code,
                )
            subscription_id = subscription.get("id")
            if not isinstance(subscription_id, str) or not subscription_id:
                raise SubscriptionInventoryError(
                    "Orion subscription inventory returned a malformed entry",
                    http_status=response.status_code,
                )
            if subscription_id in subscription_ids:
                raise SubscriptionInventoryError(
                    "Orion subscription inventory repeated a subscription id",
                    http_status=response.status_code,
                )
            subscription_ids.add(subscription_id)
        subscriptions.extend(page)
        offset += _SUBSCRIPTION_INVENTORY_PAGE_SIZE

    if expected_total is None or len(subscriptions) != expected_total:
        raise SubscriptionInventoryError(
            "Orion subscription inventory was incomplete",
            http_status=200,
        )
    return subscriptions


def _response_header(headers: Mapping[str, Any], name: str) -> str | None:
    """Return one response header using a case-insensitive name match."""
    for key, value in headers.items():
        if key.lower() == name.lower() and isinstance(value, str):
            return value
    return None


def _create_sth_subscription(
    *,
    label: str,
    own_written_attrs: tuple[str, ...],
    own_entity_selectors: Callable[
        [StHSubscriptionSettings], tuple[Mapping[str, str], ...]
    ],
    peer_label: str,
    peer_description_prefixes: tuple[str, ...],
    body_builder: Callable[[StHSubscriptionSettings], dict[str, Any]],
    subscription_matcher: Callable[[StHSubscriptionSettings, Any], tuple[str, ...]],
    stale_subscription_finder: (
        Callable[[StHSubscriptionSettings, Any], tuple[str, ...]] | None
    ),
    settings: StHSubscriptionSettings,
    auth: TokenProvider | None,
    session: Any = None,
) -> StHSubscriptionResult:
    """Create one product's STH-Comet subscription, idempotently and safely.

    Runs the shared create flow used by both products:

    1. In dry-run mode, log the intent and return without any HTTP call.
    2. Read the complete counted subscription inventory; return ``failed`` when
       any page is unavailable, malformed, or inconsistent.
    3. Abort if a *peer* product's subscription would cross-fire on this
       product's writes (see ``_find_unsafe_peer_subscription``). Creating this
       subscription alongside such a peer would append duplicate history rows,
       so the run fails instead of creating it.
    4. Abort if a recognized same-product subscription has stale behavior that
       requires exact-id operator removal.
    5. Skip if this product's subscription already exists, per
       ``subscription_matcher`` (the idempotent no-op on re-runs).
    6. Otherwise POST the built body. Return ``created`` and report its id for
       a 201 response; return ``failed`` for any other response after the
       authentication retry.

    Args:
        label: Product name used in log messages, e.g. ``"Product A"``.
        own_written_attrs: The product's complete enumerable written attribute
            names to compare with peer triggers. The peer check aborts when a
            subscription triggers on any of these (or triggers on everything),
            since that peer would then fire on this product's own updates.
        own_entity_selectors: Builds the entity selectors for this product's
            writes. The peer guard uses them to prove canonical peer selectors
            disjoint before allowing a shared attribute name.
        peer_label: The other product's name, used in log messages.
        peer_description_prefixes: Description prefixes that identify the peer
            product's subscriptions during the stale-peer check.
        body_builder: Builds the subscription body to POST for this product.
        subscription_matcher: Returns the ids of existing subscriptions that
            already match this product's contract; drives the step-4 skip.
        stale_subscription_finder: Returns ids of recognized same-product
            subscriptions whose behavior does not match the current contract.
            Such subscriptions require operator removal before creation.
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
    try:
        existing_subscriptions = fetch_subscription_inventory(
            settings=settings,
            auth=auth,
            session=http,
        )
    except SubscriptionInventoryError as exc:
        logger.error(
            f"{label} STH subscription preflight failed",
            extra={
                "event": "sth_subscription_failed",
                "dry_run": False,
                "http_status": exc.http_status,
                "response_excerpt": exc.response_excerpt,
                "count_failed": 1,
            },
        )
        return StHSubscriptionResult(would_create=0, created=0, skipped=0, failed=1)

    unsafe_peer = _find_unsafe_peer_subscription(
        existing_subscriptions,
        settings=settings,
        own_written_attrs=own_written_attrs,
        own_entity_selectors=own_entity_selectors(settings),
        peer_label=peer_label,
        peer_description_prefixes=peer_description_prefixes,
    )
    if unsafe_peer is not None:
        peer_label, peer_id, peer_trigger = unsafe_peer
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

    if stale_subscription_finder is not None:
        stale_ids = stale_subscription_finder(settings, existing_subscriptions)
        if stale_ids:
            logger.error(
                (
                    f"{label} STH subscription creation aborted: an existing "
                    "same-product subscription requires operator removal"
                ),
                extra={
                    "event": "sth_subscription_failed",
                    "dry_run": False,
                    "subscription_id": stale_ids[0],
                    "count_failed": 1,
                },
            )
            return StHSubscriptionResult(
                would_create=0,
                created=0,
                skipped=0,
                failed=1,
                subscription_ids=stale_ids,
            )

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
        own_written_attrs=PRODUCT_A_HISTORY_ATTRS,
        own_entity_selectors=_product_a_entity_selectors,
        peer_label="Product B",
        peer_description_prefixes=(_PRODUCT_B_DESCRIPTION_PREFIX,),
        body_builder=build_product_a_subscription_body,
        subscription_matcher=_matching_product_a_subscription_ids,
        stale_subscription_finder=_stale_product_a_subscription_ids,
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
        # Product B always writes these scalar attrs. Dynamic
        # peopleCount_flow_<N> attrs are absent because their names are not
        # enumerable; Product A's fixed trigger cannot overlap them.
        own_written_attrs=PRODUCT_B_STABLE_WRITE_ATTRS,
        own_entity_selectors=_product_b_entity_selectors,
        peer_label="Product A",
        peer_description_prefixes=(_PRODUCT_A_DESCRIPTION_PREFIX,),
        body_builder=build_product_b_subscription_body,
        subscription_matcher=_matching_product_b_subscription_ids,
        stale_subscription_finder=None,
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


def _product_a_entity_selectors(
    _settings: StHSubscriptionSettings | None = None,
) -> tuple[Mapping[str, str], ...]:
    """Return Product A's exact per-place subscription selectors."""
    return tuple(
        {"idPattern": ".*", "type": entity_type}
        for entity_type in PRODUCT_A_ENTITY_TYPES
    )


def _product_b_entity_selectors(
    settings: StHSubscriptionSettings,
) -> tuple[Mapping[str, str], ...]:
    """Return Product B's exact configured aggregate selector."""
    return (
        {
            "id": settings.product_b_aggregate_entity_id,
            "type": settings.product_b_aggregate_entity_type,
        },
    )


def _entity_selectors_equal(
    raw_selectors: Any,
    expected_selectors: tuple[Mapping[str, str], ...],
) -> bool:
    """Return whether selector lists contain exactly the same mappings."""
    if not isinstance(raw_selectors, list) or len(raw_selectors) != len(
        expected_selectors
    ):
        return False
    if not all(isinstance(selector, dict) for selector in raw_selectors):
        return False
    # Compare only string-valued selectors. A selector whose value is a list or
    # dict is unhashable (and never equal to the string-valued expected
    # selectors); rejecting it here keeps the set build below from raising and
    # lets the caller treat the entry as "not this product's exact shape".
    if not all(
        isinstance(value, str)
        for selector in raw_selectors
        for value in selector.values()
    ):
        return False
    return {tuple(sorted(selector.items())) for selector in raw_selectors} == {
        tuple(sorted(selector.items())) for selector in expected_selectors
    }


def _entity_selectors_may_overlap(
    left: tuple[Mapping[str, str], ...],
    right: tuple[Mapping[str, str], ...],
) -> bool:
    """Return whether two canonical selector sets can select one same entity."""
    return any(
        _entity_selector_pair_may_overlap(left_selector, right_selector)
        for left_selector in left
        for right_selector in right
    )


def _entity_selector_pair_may_overlap(
    left: Mapping[str, str],
    right: Mapping[str, str],
) -> bool:
    """Return whether two selector mappings may select an entity in common.

    A missing exact type is broad in Orion, and a type pattern is outside the
    canonical Product A/B shapes, so either case remains conservatively
    overlapping. Only two present, unequal exact types prove disjointness.
    """
    left_type = left.get("type")
    right_type = right.get("type")
    if (
        not isinstance(left_type, str)
        or not left_type
        or not isinstance(right_type, str)
        or not right_type
    ):
        return True
    if left_type != right_type:
        return False
    left_id = left.get("id")
    right_id = right.get("id")
    if left_id is not None and right_id is not None:
        return left_id == right_id
    if left.get("idPattern") == ".*" or right.get("idPattern") == ".*":
        return True
    return True


def _find_unsafe_peer_subscription(
    subscriptions: Any,
    *,
    settings: StHSubscriptionSettings,
    own_written_attrs: tuple[str, ...],
    own_entity_selectors: tuple[Mapping[str, str], ...],
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

    An overlapping trigger is safe only when the peer uses its exact current
    selector and that selector is provably disjoint from this product's exact
    selector. Broad or malformed triggers and non-canonical selectors fail
    closed.
    """
    if not isinstance(subscriptions, list):
        return None
    own_written = set(own_written_attrs)
    for subscription in subscriptions:
        if not isinstance(subscription, dict):
            continue
        subscription_id = subscription.get("id")
        description = subscription.get("description")
        if (
            not isinstance(subscription_id, str)
            or not subscription_id
            or not isinstance(description, str)
        ):
            continue
        if not description.startswith(peer_description_prefixes):
            continue

        subject = subscription.get("subject")
        if not isinstance(subject, dict):
            return peer_label, subscription_id, []
        condition = subject.get("condition")
        if not isinstance(condition, dict):
            return peer_label, subscription_id, []
        raw_condition_attrs = condition.get("attrs")
        if (
            not isinstance(raw_condition_attrs, list)
            or not raw_condition_attrs
            or not all(
                isinstance(item, str) and bool(item) for item in raw_condition_attrs
            )
        ):
            return peer_label, subscription_id, []

        condition_attrs = list(raw_condition_attrs)
        if not set(condition_attrs) & own_written:
            continue

        peer_selectors = _canonical_peer_entity_selectors(
            settings,
            peer_label,
            subject.get("entities"),
        )
        if peer_selectors is not None and not _entity_selectors_may_overlap(
            peer_selectors,
            own_entity_selectors,
        ):
            continue

        if set(condition_attrs) & own_written:
            return peer_label, subscription_id, condition_attrs
    return None


def _matching_product_a_subscription_ids(
    settings: StHSubscriptionSettings, subscriptions: Any
) -> tuple[str, ...]:
    """Return ids for active subscriptions matching Product A's exact contract."""
    if not isinstance(subscriptions, list):
        return ()
    return tuple(
        subscription["id"]
        for subscription in subscriptions
        if isinstance(subscription, dict)
        and isinstance(subscription.get("id"), str)
        and bool(subscription["id"])
        and _has_product_a_shape(settings, subscription)
    )


def _stale_product_a_subscription_ids(
    settings: StHSubscriptionSettings, subscriptions: Any
) -> tuple[str, ...]:
    """Return ids for recognized Product A subscriptions with stale behavior."""
    if not isinstance(subscriptions, list):
        return ()
    return tuple(
        subscription["id"]
        for subscription in subscriptions
        if isinstance(subscription, dict)
        and isinstance(subscription.get("id"), str)
        and bool(subscription["id"])
        and _is_product_a_subscription_candidate(subscription)
        and not _has_product_a_shape(settings, subscription)
    )


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


def _notification_behavior_fields(
    notification: Mapping[str, Any],
) -> dict[str, Any] | None:
    """Return client-controlled notification fields in canonical GET form.

    Orion GET responses can add delivery telemetry and can materialize the
    omitted ``onlyChangedAttrs`` and ``covered`` defaults as ``false``. Those
    representations are equivalent to the body builders' omissions. A true or
    non-Boolean default field changes or invalidates notification behavior and
    therefore does not have a canonical equivalent.
    """
    behavior: dict[str, Any] = {}
    for key, value in notification.items():
        if key in _NOTIFICATION_RUNTIME_FIELDS:
            continue
        if key in _NOTIFICATION_FALSE_DEFAULT_FIELDS:
            if value is not False:
                return None
            continue
        behavior[key] = value
    return behavior


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
    - ``onlyChangedAttrs`` and ``covered``: Orion can echo either omitted
      false-default field as ``false``. Omission and literal ``false`` match;
      ``true`` or a non-Boolean value is different behavior.
    - The notify URL must equal the configured URL exactly; a different URL is a
      different subscription, not this one.
    """
    expected_top_level = {
        "id",
        "status",
        "description",
        "subject",
        "notification",
    }
    if settings.expires:
        expected_top_level.add("expires")
    if set(subscription) != expected_top_level:
        return False

    description = subscription.get("description")
    if (
        subscription.get("status") != "active"
        or not isinstance(description, str)
        or not description.startswith(_PRODUCT_B_DESCRIPTION_PREFIX)
    ):
        return False

    subject = subscription.get("subject")
    notification = subscription.get("notification")
    if not isinstance(subject, dict) or not isinstance(notification, dict):
        return False
    if set(subject) != {"entities", "condition"}:
        return False

    entities = subject.get("entities")
    condition = subject.get("condition")
    if (
        not _entity_selectors_equal(
            entities,
            _product_b_entity_selectors(settings),
        )
        or not isinstance(condition, dict)
        or set(condition) != {"attrs", "notifyOnMetadataChange"}
        or condition.get("attrs") != ["dateRetrieved"]
        or condition.get("notifyOnMetadataChange") is not True
    ):
        return False

    behavior_notification = _notification_behavior_fields(notification)
    if behavior_notification is None or set(behavior_notification) not in (
        {"http", "attrsFormat", "metadata"},
        {"http", "attrsFormat", "metadata", "attrs"},
    ):
        return False

    http = behavior_notification.get("http")
    notification_matches = (
        isinstance(http, dict)
        and set(http) == {"url"}
        and http.get("url") == settings.comet_notify_url
        and behavior_notification.get("attrsFormat") == "legacy"
        and behavior_notification.get("metadata") == ["TimeInstant"]
        and (
            "attrs" not in behavior_notification
            or behavior_notification.get("attrs") == []
        )
    )
    if not notification_matches:
        return False
    return _subscription_expiration_matches(settings.expires, subscription)


def _is_product_a_subscription_candidate(
    subscription: Mapping[str, Any],
) -> bool:
    """Return whether a subscription is recognizable as Product A."""
    description = subscription.get("description")
    if isinstance(description, str):
        if description.startswith(_PRODUCT_A_DESCRIPTION_PREFIX):
            return True
        if description.startswith(
            (_PRODUCT_B_DESCRIPTION_PREFIX, _PRODUCT_B_LEGACY_DESCRIPTION_PREFIX)
        ):
            return False
    subject = subscription.get("subject")
    return isinstance(subject, dict) and _entity_selectors_equal(
        subject.get("entities"),
        _product_a_entity_selectors(),
    )


def _canonical_peer_entity_selectors(
    settings: StHSubscriptionSettings,
    peer_label: str,
    raw_selectors: Any,
) -> tuple[Mapping[str, str], ...] | None:
    """Return a peer's canonical selectors when the raw list is an exact match.

    Peer notification behavior is intentionally irrelevant here: exact,
    disjoint entity selectors alone prove that this product's writes cannot
    trigger the peer.
    """
    if peer_label == "Product A":
        expected = _product_a_entity_selectors(settings)
    elif peer_label == "Product B":
        expected = _product_b_entity_selectors(settings)
    else:
        return None
    return expected if _entity_selectors_equal(raw_selectors, expected) else None


def _has_product_a_shape(
    settings: StHSubscriptionSettings,
    subscription: Mapping[str, Any],
) -> bool:
    """Return whether an Orion GET body matches Product A's live contract."""
    expected_top_level = {
        "id",
        "status",
        "description",
        "subject",
        "notification",
    }
    if settings.expires:
        expected_top_level.add("expires")
    if set(subscription) != expected_top_level:
        return False

    description = subscription.get("description")
    if (
        subscription.get("status") != "active"
        or not isinstance(description, str)
        or not description.startswith(_PRODUCT_A_DESCRIPTION_PREFIX)
    ):
        return False

    subject = subscription.get("subject")
    notification = subscription.get("notification")
    if not isinstance(subject, dict) or set(subject) != {"entities", "condition"}:
        return False
    if not isinstance(notification, dict):
        return False

    condition = subject.get("condition")
    if not isinstance(condition, dict) or set(condition) != {
        "attrs",
        "notifyOnMetadataChange",
    }:
        return False

    if not _entity_selectors_equal(
        subject.get("entities"),
        _product_a_entity_selectors(settings),
    ):
        return False

    if (
        not _exact_string_list(condition.get("attrs"), PRODUCT_A_TRIGGER_ATTRS)
        or condition.get("notifyOnMetadataChange") is not True
    ):
        return False

    behavior_notification = _notification_behavior_fields(notification)
    if behavior_notification is None or set(behavior_notification) != {
        "http",
        "attrsFormat",
        "attrs",
        "metadata",
    }:
        return False
    http = behavior_notification.get("http")
    if (
        not isinstance(http, dict)
        or set(http) != {"url"}
        or http.get("url") != settings.comet_notify_url
        or behavior_notification.get("attrsFormat") != "legacy"
        or not _exact_string_list(
            behavior_notification.get("attrs"),
            PRODUCT_A_HISTORY_ATTRS,
        )
        or behavior_notification.get("metadata") != ["TimeInstant"]
    ):
        return False

    return _subscription_expiration_matches(settings.expires, subscription)


def _exact_string_list(value: Any, expected: tuple[str, ...]) -> bool:
    """Return whether a list contains each expected string exactly once."""
    return (
        isinstance(value, list)
        and len(value) == len(expected)
        and all(isinstance(item, str) for item in value)
        and set(value) == set(expected)
    )


def _parse_subscription_expiration(value: Any) -> datetime | None:
    """Parse an aware ISO 8601 subscription expiration for instant comparison."""
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(UTC)


def _subscription_expiration_matches(
    configured_expires: str,
    subscription: Mapping[str, Any],
) -> bool:
    """Return whether a GET body has the configured expiration instant."""
    if not configured_expires:
        return "expires" not in subscription
    configured = _parse_subscription_expiration(configured_expires)
    live = _parse_subscription_expiration(subscription.get("expires"))
    return configured is not None and live is not None and configured == live


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
