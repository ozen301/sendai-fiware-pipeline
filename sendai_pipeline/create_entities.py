"""Create Orion entities before first publication.

Creates one entity per entry in the operator-supplied list, using the entity
ids and types provided directly rather than reading a metadata CSV. The POST
uses ``/orion/v2.0/entities`` (no ``?options=upsert``) so a 422/409 response
unambiguously means the entity already exists and is treated as a skip —
making the operation safe to re-run.

Intended for one-shot pre-flight use before enabling a new sensor batch.
"""

import json
import logging
import os
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any, Self

import requests

from sendai_pipeline.orion_client import TokenProvider
from sendai_pipeline.settings_validation import optional_env

logger = logging.getLogger(__name__)

_ALREADY_EXISTS_STATUSES: frozenset[int] = frozenset({409, 422})


class CreateEntitiesError(RuntimeError):
    """Raised when create-entities configuration or input is invalid."""


@dataclass(frozen=True)
class EntitySpec:
    """A single entity to create.

    Attributes:
        entity_id: NGSI entity id.
        entity_type: NGSI entity type.
    """

    entity_id: str
    entity_type: str


@dataclass(frozen=True)
class CreateEntitiesSettings:
    """Configuration for one entity creation run.

    Attributes:
        base_url: Sendai platform base URL.
        service: Fiware-Service header value; empty string omits the header.
        service_path: Fiware-ServicePath header value.
        verify_tls: Whether to verify TLS certificates.
        timeout: Per-request timeout in seconds.
        entities: Entities to create. Each entry carries the id and type that
            will be written verbatim to Orion.
        dry_run: When true, build entity bodies and report counts without
            contacting the network.
    """

    base_url: str
    service: str
    service_path: str
    verify_tls: bool
    timeout: float
    entities: tuple[EntitySpec, ...]
    dry_run: bool

    def __post_init__(self) -> None:
        object.__setattr__(self, "base_url", self.base_url.rstrip("/"))

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> Self:
        """Build reusable FIWARE settings from environment variables.

        Args:
            env: Optional mapping used in place of ``os.environ`` for tests.

        Returns:
            Parsed create-entities settings with no requested entities and
            dry-run mode enabled. One-shot inputs should be supplied by the
            entry point when the command is invoked.

        Raises:
            CreateEntitiesError: If a required variable is missing or a value is
                malformed.
        """
        values = os.environ if env is None else env
        return cls(
            base_url=_required_env(values, "FIWARE_BASE_URL").rstrip("/"),
            service=optional_env(values, "FIWARE_SERVICE", ""),
            service_path=optional_env(values, "FIWARE_SERVICE_PATH", "/"),
            verify_tls=_parse_bool(optional_env(values, "FIWARE_VERIFY_TLS", "true")),
            timeout=_parse_float(values, "FIWARE_TIMEOUT_SECONDS", 10.0),
            entities=(),
            dry_run=True,
        )


@dataclass
class CreateEntitiesResult:
    """Outcome summary for one entity creation run.

    Attributes:
        would_create: Number of entities that would be created (dry-run only).
        created: Entities successfully created (live mode only).
        skipped: Entities that already existed and were left untouched.
        failed: Entities whose creation failed with an unexpected error.
        exit_code: Suggested process exit code; 0 if no failures, else 1.
    """

    would_create: int
    created: int
    skipped: int
    failed: int

    @property
    def exit_code(self) -> int:
        """Return 1 when any entity failed to seed, otherwise 0."""
        return 1 if self.failed > 0 else 0


def create_entities(
    entities: Iterable[EntitySpec],
    *,
    settings: CreateEntitiesSettings,
    auth: TokenProvider | None,
    session: Any = None,
) -> CreateEntitiesResult:
    """Create Orion entities for each entry in the supplied list.

    Posts one entity per :class:`EntitySpec`. Responses of 201 are counted as
    created; 409/422 mean the entity already exists and are counted as skipped;
    anything else is counted as failed.

    In dry-run mode no network calls are made and only ``would_create`` is
    populated.

    Args:
        entities: Entities to create. Each entry carries the id and type to
            write to Orion verbatim.
        settings: Resolved :class:`CreateEntitiesSettings`.
        auth: Token provider whose ``get_token`` returns a bearer token.
            Pass ``None`` for dry-run (credentials are not needed).
        session: HTTP session with a ``.post`` method. Defaults to a fresh
            :class:`requests.Session`.

    Returns:
        :class:`CreateEntitiesResult` with counts and a suggested exit code.
    """
    targets = list(entities)

    if settings.dry_run:
        for spec in targets:
            logger.info(
                "dry-run: would create entity",
                extra={
                    "event": "create_entity_would_create",
                    "entity_id": spec.entity_id,
                    "entity_type": spec.entity_type,
                    "dry_run": True,
                },
            )
        logger.info(
            "create-entities dry-run complete",
            extra={
                "event": "create_entities_summary",
                "dry_run": True,
                "count_would_create": len(targets),
                "count_created": 0,
                "count_skipped": 0,
                "count_failed": 0,
            },
        )
        return CreateEntitiesResult(
            would_create=len(targets), created=0, skipped=0, failed=0
        )

    if auth is None:
        raise CreateEntitiesError("auth is required when dry_run is false")

    http = session or requests.Session()
    created = 0
    skipped = 0
    failed = 0
    url = f"{settings.base_url}/orion/v2.0/entities"

    for spec in targets:
        body = _entity_body(spec)
        body_bytes = json.dumps(body, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
        token = auth.get_token()
        headers = _headers(token, settings)

        try:
            response = http.post(
                url,
                data=body_bytes,
                headers=headers,
                timeout=settings.timeout,
                verify=settings.verify_tls,
            )
        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
        ) as exc:
            logger.exception(
                "create entity request failed",
                extra={
                    "event": "create_entity_post_failed",
                    "entity_id": spec.entity_id,
                    "entity_type": spec.entity_type,
                    "http_status": 0,
                    "error_type": type(exc).__name__,
                },
            )
            failed += 1
            continue

        status = response.status_code
        if status == 201:
            created += 1
            logger.info(
                "create entity succeeded",
                extra={
                    "event": "create_entity_post_created",
                    "entity_id": spec.entity_id,
                    "entity_type": spec.entity_type,
                    "http_status": status,
                },
            )
        elif status in _ALREADY_EXISTS_STATUSES:
            skipped += 1
            logger.info(
                "create entity already exists",
                extra={
                    "event": "create_entity_post_skipped",
                    "entity_id": spec.entity_id,
                    "entity_type": spec.entity_type,
                    "http_status": status,
                },
            )
        else:
            failed += 1
            logger.error(
                "create entity failed",
                extra={
                    "event": "create_entity_post_failed",
                    "entity_id": spec.entity_id,
                    "entity_type": spec.entity_type,
                    "http_status": status,
                    "response_excerpt": response.text[:512],
                },
            )

    logger.info(
        "create-entities run complete",
        extra={
            "event": "create_entities_summary",
            "dry_run": False,
            "count_would_create": 0,
            "count_created": created,
            "count_skipped": skipped,
            "count_failed": failed,
        },
    )
    return CreateEntitiesResult(
        would_create=0, created=created, skipped=skipped, failed=failed
    )


def _entity_body(spec: EntitySpec) -> dict[str, Any]:
    """Build the NGSI v2 entity creation body for one entity spec."""
    return {"id": spec.entity_id, "type": spec.entity_type}


def _headers(token: str, settings: CreateEntitiesSettings) -> dict[str, str]:
    """Build headers for an authenticated Orion entity creation request."""
    h = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Fiware-ServicePath": settings.service_path,
    }
    if settings.service:
        h["Fiware-Service"] = settings.service
    return h


def parse_entity_specs(values: Iterable[str]) -> tuple[EntitySpec, ...]:
    """Parse one or more ``id:type`` entity specs.

    Each value may contain one or more comma-separated specs. This lets the CLI
    accept either repeated positional arguments or a pasted comma-separated
    list.

    Args:
        values: Raw entity specs in ``entity_id:entity_type`` form.

    Returns:
        Parsed entity specs.

    Raises:
        CreateEntitiesError: If no specs are supplied, any spec is missing the
            colon separator, or either side is empty after stripping.
    """
    specs: list[EntitySpec] = []
    for value in values:
        for token in value.split(","):
            token = token.strip()
            if not token:
                continue
            parts = token.split(":", 1)
            if len(parts) != 2 or not parts[0].strip() or not parts[1].strip():
                raise CreateEntitiesError(
                    f"entity spec {token!r} must be in id:type format"
                )
            specs.append(
                EntitySpec(entity_id=parts[0].strip(), entity_type=parts[1].strip())
            )
    if not specs:
        raise CreateEntitiesError("at least one entity spec is required")
    return tuple(specs)


def _parse_float(env: Mapping[str, str], key: str, default: float) -> float:
    """Parse an optional float environment variable or return *default*."""
    raw = optional_env(env, key, "")
    if raw == "":
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise CreateEntitiesError(
            f"environment variable must be a number: {key}"
        ) from exc


def _required_env(env: Mapping[str, str], key: str) -> str:
    """Return a required environment value or raise a config error."""
    value = env.get(key)
    if value is None or value == "":
        raise CreateEntitiesError(f"missing required environment variable: {key}")
    return value


def _parse_bool(value: str) -> bool:
    """Parse a boolean environment variable."""
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise CreateEntitiesError(f"invalid boolean value: {value!r}")
