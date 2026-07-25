"""Delete Orion entities, optionally purging their STH-Comet history."""

import argparse
import logging
import os
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

import requests
from dotenv import find_dotenv, load_dotenv

from sendai_pipeline.auth import AuthClient, AuthSettings
from sendai_pipeline.comet_client import CometClient, CometSettings
from sendai_pipeline.logging_setup import LoggingSettings, configure_logging
from sendai_pipeline.metadata import parse_entity_id
from sendai_pipeline.orion_client import OrionSettings
from sendai_pipeline.sth_subscriptions import (
    PRODUCT_A_HISTORY_ATTRS,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class EntityTarget:
    """One Orion entity selected for deletion."""

    entity_id: str
    entity_type: str


class DeleteEntitiesConfigError(RuntimeError):
    """Raised when delete-entities arguments are invalid."""


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    """Parse command-line arguments for the entity delete helper."""
    parser = argparse.ArgumentParser(
        description="Delete Orion entities and optionally purge Comet history.",
    )
    parser.add_argument("entity_specs", nargs="+", metavar="ENTITY_ID[:ENTITY_TYPE]")
    parser.add_argument("--purge-history", action="store_true")
    attrs = parser.add_mutually_exclusive_group()
    attrs.add_argument("--attrs", help="Comma-separated Comet attributes to purge.")
    attrs.add_argument("--flow-attrs", action="store_true")
    parser.add_argument("--reason", required=True)
    parser.add_argument("--send", action="store_true")
    parser.add_argument("--i-know-this-is-production", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the Orion entity delete entry point."""
    load_dotenv(find_dotenv(usecwd=True))
    try:
        args = _parse_args(argv)
        configure_logging(LoggingSettings.from_env(), product="delete_entities")
        targets = _parse_targets(args.entity_specs)
        attrs = _attrs(args)
        if attrs is not None and not args.purge_history:
            raise DeleteEntitiesConfigError("attribute flags require --purge-history")
        if args.send and _requires_production_override(args):
            raise DeleteEntitiesConfigError(
                "--send against a catch-all FIWARE service/path requires "
                "--i-know-this-is-production"
            )
    except DeleteEntitiesConfigError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    logger.info(
        "delete-entities requested",
        extra={
            "event": "delete_entities_requested",
            "send": bool(args.send),
            "reason": args.reason,
            "target_count": len(targets),
            "purge_history": bool(args.purge_history),
            "attr_scope": list(attrs) if attrs is not None else None,
        },
    )

    if not args.send:
        _print_dry_run(targets, purge_history=bool(args.purge_history), attrs=attrs)
        logger.info(
            "delete-entities dry-run summary",
            extra={
                "event": "delete_entities_summary",
                "send": False,
                "target_count": len(targets),
                "deleted": 0,
                "absent": 0,
                "failed": 0,
                "purge_failed": 0,
            },
        )
        return 0

    auth = AuthClient(AuthSettings.from_env())
    orion_settings = OrionSettings.from_env()
    comet = (
        CometClient(CometSettings.from_env(), auth=auth) if args.purge_history else None
    )

    deleted = 0
    absent = 0
    failed = 0
    purge_failed = 0
    for target in targets:
        try:
            status = delete_one_orion_entity(
                target.entity_id,
                target.entity_type,
                settings=orion_settings,
                auth=auth,
            )
        except requests.HTTPError as exc:
            logger.exception(
                "delete-entities Orion delete failed",
                extra={
                    "event": "delete_entities_target",
                    "entity_id": target.entity_id,
                    "entity_type": target.entity_type,
                    "phase": "orion",
                    "ok": False,
                },
            )
            print(
                f"ERROR: {target.entity_id}: Orion delete failed: {exc}",
                file=sys.stderr,
            )
            failed += 1
            continue

        logger.info(
            "delete-entities Orion delete processed",
            extra={
                "event": "delete_entities_target",
                "entity_id": target.entity_id,
                "entity_type": target.entity_type,
                "phase": "orion",
                "ok": True,
                "http_status": status,
            },
        )
        if status == 204:
            deleted += 1
        elif status == 404:
            absent += 1
        else:  # defensive; delete_one_orion_entity should have raised
            failed += 1
            continue

        if comet is not None:
            if not _purge_comet_history(comet, target, attrs=attrs):
                purge_failed += 1

    logger.info(
        "delete-entities summary",
        extra={
            "event": "delete_entities_summary",
            "send": True,
            "target_count": len(targets),
            "deleted": deleted,
            "absent": absent,
            "failed": failed,
            "purge_failed": purge_failed,
        },
    )
    return 1 if failed else 0


def delete_one_orion_entity(
    entity_id: str,
    entity_type: str,
    *,
    settings: Any,
    auth: Any,
) -> int:
    """Delete one Orion entity and return the HTTP status code."""
    response = requests.delete(
        f"{settings.base_url}/orion/v2.0/entities/{entity_id}",
        params={"type": entity_type},
        headers=_orion_headers(settings, auth),
        timeout=settings.timeout,
        verify=settings.verify_tls,
    )
    if response.status_code == 401:
        response = requests.delete(
            f"{settings.base_url}/orion/v2.0/entities/{entity_id}",
            params={"type": entity_type},
            headers=_orion_headers(settings, auth, force_refresh=True),
            timeout=settings.timeout,
            verify=settings.verify_tls,
        )
    if response.status_code not in {204, 404}:
        # raise_for_status() only fires on non-2xx; trap unexpected 2xx
        # responses (NGSI v2 documents only 204 for entity delete) so
        # they are not silently treated as success.
        response.raise_for_status()
        raise requests.HTTPError(
            f"unexpected Orion DELETE status {response.status_code}",
            response=response,
        )
    return int(response.status_code)


def _parse_targets(specs: Sequence[str]) -> list[EntityTarget]:
    """Parse ``ENTITY_ID[:ENTITY_TYPE]`` specs into delete targets.

    The entity type is optional: an explicit ``:ENTITY_TYPE`` is authoritative,
    otherwise it is inferred from a canonical entity id (see
    :func:`sendai_pipeline.metadata.parse_entity_id`).

    Args:
        specs: Raw ``ENTITY_ID`` or ``ENTITY_ID:ENTITY_TYPE`` strings.

    Returns:
        One :class:`EntityTarget` per spec, in input order.

    Raises:
        DeleteEntitiesConfigError: For an empty/wildcard id, an empty inline
            type, or a non-canonical id whose type cannot be inferred.
    """
    targets: list[EntityTarget] = []
    for spec in specs:
        # rpartition on ":" splits off an inline type; no ":" means bare id.
        entity_id, separator, entity_type = spec.rpartition(":")
        if not separator:
            # Bare id: guard against catch-all, then infer the type from it.
            entity_id = spec
            if entity_id in {"", "*"}:
                raise DeleteEntitiesConfigError(f"invalid entity spec: {spec!r}")
            entity_type = _entity_type_for_spec(entity_id)
        elif entity_id in {"", "*"} or entity_type == "":
            # Inline form present but a side is empty/wildcard: reject.
            raise DeleteEntitiesConfigError(f"invalid entity spec: {spec!r}")
        else:
            # Explicit inline type wins; note it if it shadows inference.
            _log_if_type_override(entity_id, entity_type)
        targets.append(EntityTarget(entity_id, entity_type))
    return targets


def _entity_type_for_spec(entity_id: str) -> str:
    """Infer the entity type from a canonical id, or require an inline type.

    Returns:
        The entity type parsed from *entity_id*.

    Raises:
        DeleteEntitiesConfigError: If *entity_id* is not canonical, so the
            operator must supply an inline ``:TYPE`` (this tool has no
            ``--type`` flag).
    """
    parsed = parse_entity_id(entity_id)
    if parsed is not None:
        return parsed.entity_type
    raise DeleteEntitiesConfigError(
        f"cannot infer entity type for {entity_id!r}; pass an explicit :TYPE"
    )


def _log_if_type_override(entity_id: str, explicit_type: str) -> None:
    """Log at DEBUG when an inline type differs from the id's inferred type."""
    parsed = parse_entity_id(entity_id)
    if parsed is not None and parsed.entity_type != explicit_type:
        logger.debug(
            "explicit entity type overrides inferred type",
            extra={
                "event": "entity_type_override",
                "entity_id": entity_id,
                "inferred_entity_type": parsed.entity_type,
                "explicit_entity_type": explicit_type,
            },
        )


def _attrs(args: argparse.Namespace) -> tuple[str, ...] | None:
    """Return selected purge attributes, or ``None`` for per-entity purge."""
    if args.flow_attrs:
        return PRODUCT_A_HISTORY_ATTRS
    if args.attrs is None:
        return None
    attrs = tuple(part.strip() for part in args.attrs.split(",") if part.strip())
    if not attrs:
        raise DeleteEntitiesConfigError("--attrs must name at least one attribute")
    return attrs


def _print_dry_run(
    targets: Sequence[EntityTarget],
    *,
    purge_history: bool,
    attrs: Sequence[str] | None,
) -> None:
    """Print the delete plan without making auth or network calls.

    Emits a `delete_entities_target` log record per planned Orion delete
    and per planned Comet purge URL so the audit trail mirrors the live
    path. No HTTP is performed.
    """
    for target in targets:
        print(f"DRY-RUN: would DELETE {_orion_url(target)}")
        logger.info(
            "delete-entities dry-run planned Orion delete",
            extra={
                "event": "delete_entities_target",
                "entity_id": target.entity_id,
                "entity_type": target.entity_type,
                "phase": "orion",
                "send": False,
            },
        )
        if purge_history:
            for url in _comet_urls(target, attrs=attrs):
                print(f"DRY-RUN: would DELETE {url}")
                logger.info(
                    "delete-entities dry-run planned Comet purge",
                    extra={
                        "event": "delete_entities_target",
                        "entity_id": target.entity_id,
                        "entity_type": target.entity_type,
                        "phase": "comet_purge",
                        "send": False,
                    },
                )


def _purge_comet_history(
    comet: CometClient,
    target: EntityTarget,
    *,
    attrs: Sequence[str] | None,
) -> bool:
    """Best-effort purge of Comet history for one entity.

    Returns:
        True if the purge completed without errors, False on any HTTP
        failure. Callers do not change the exit code based on this return
        value: Comet purge is best-effort, so a failure is surfaced in
        the summary but does not fail the run when the Orion delete
        already succeeded.
    """
    try:
        if attrs is None:
            comet.delete_entity_history(target.entity_id, target.entity_type)
        else:
            for attr in attrs:
                comet.delete_attribute_history(
                    target.entity_id, target.entity_type, attr
                )
    except requests.HTTPError as exc:
        logger.warning(
            "delete-entities Comet purge failed",
            extra={
                "event": "delete_entities_target",
                "entity_id": target.entity_id,
                "entity_type": target.entity_type,
                "phase": "comet_purge",
                "ok": False,
                "response_excerpt": str(exc),
            },
        )
        return False
    logger.info(
        "delete-entities Comet purge processed",
        extra={
            "event": "delete_entities_target",
            "entity_id": target.entity_id,
            "entity_type": target.entity_type,
            "phase": "comet_purge",
            "ok": True,
        },
    )
    return True


def _orion_url(target: EntityTarget) -> str:
    """Return the Orion DELETE URL used in dry-run output."""
    base_url = os.environ.get("FIWARE_BASE_URL", "").rstrip("/")
    return (
        f"{base_url}/orion/v2.0/entities/{target.entity_id}?type={target.entity_type}"
    )


def _comet_urls(
    target: EntityTarget,
    *,
    attrs: Sequence[str] | None,
) -> list[str]:
    """Return the Comet DELETE URLs used in dry-run output."""
    base_url = os.environ.get("FIWARE_BASE_URL", "").rstrip("/")
    entity_url = (
        f"{base_url}/comet/v1.0/contextEntities/type/"
        f"{target.entity_type}/id/{target.entity_id}"
    )
    if attrs is None:
        return [entity_url]
    return [f"{entity_url}/attributes/{attr}" for attr in attrs]


def _orion_headers(
    settings: Any,
    auth: Any,
    *,
    force_refresh: bool = False,
) -> dict[str, str]:
    """Build Orion DELETE headers."""
    headers = {
        "Authorization": f"Bearer {auth.get_token(force_refresh=force_refresh)}",
        "Accept": "application/json",
        "Fiware-ServicePath": settings.service_path,
    }
    if settings.service:
        headers["Fiware-Service"] = settings.service
    return headers


def _requires_production_override(args: argparse.Namespace) -> bool:
    """Return whether live deletion needs the explicit production override.

    Matches the blank-as-default normalization performed by
    ``OrionSettings.from_env`` / ``CometSettings.from_env`` so a
    ``FIWARE_SERVICE_PATH=`` env var (set but empty) is treated like the
    root catch-all path, not an arbitrary non-catch-all literal "".
    """
    if args.i_know_this_is_production:
        return False
    service = os.environ.get("FIWARE_SERVICE", "").strip()
    service_path = os.environ.get("FIWARE_SERVICE_PATH", "/").strip() or "/"
    return service == "" or service_path == "/"


if __name__ == "__main__":
    sys.exit(main())
