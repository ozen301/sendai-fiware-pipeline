"""Probe Sendai Orion-to-STH-Comet subscription behavior.

This is a guarded live probe for operator use before creating production
STH-Comet subscriptions. By default it prints the request plan only. Pass
``--execute`` to create a temporary probe entity, create an Orion subscription
to Comet, update one probe attribute with ``TimeInstant`` metadata, read the
resulting STH history, optionally test attribute-level STH deletion, and clean
up the temporary Orion resources.

Example:
    uv run python scripts/dev/probe_sth_subscription.py --execute
"""

import argparse
import json
import os
import sys
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import quote

import requests
from dotenv import find_dotenv, load_dotenv

from sendai_pipeline.auth import AuthClient, AuthSettings

DEFAULT_ENTITY_TYPE = "SendaiPipelineProbe"
DEFAULT_ATTR = "probeValue"
DEFAULT_WAIT_SECONDS = 3.0
DEFAULT_HISTORY_LAST_N = 10
DEFAULT_TIMEINSTANT_OFFSET_HOURS = -24.0


@dataclass(frozen=True)
class ProbeConfig:
    """Resolved probe configuration."""

    entity_id: str
    entity_type: str
    attr: str
    trigger_attr: str
    notification_url: str
    use_skip_initial_notification: bool
    probe_delete: bool
    allow_non_probe_delete: bool
    wait_seconds: float
    history_last_n: int
    timeinstant_offset_hours: float
    keep_resources: bool
    execute: bool


@dataclass
class ProbeClient:
    """Small authenticated HTTP client for this dev-only probe."""

    base_url: str
    service: str
    service_path: str
    verify_tls: bool
    timeout: float
    auth: AuthClient
    session: requests.Session

    def request(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, str | int] | None = None,
        body: dict[str, Any] | None = None,
    ) -> requests.Response:
        """Send one authenticated request to the Sendai FIWARE gateway."""
        headers = self._headers(content_type="application/json" if body else None)
        return self.session.request(
            method,
            f"{self.base_url}{path}",
            params=params,
            json=body,
            headers=headers,
            timeout=self.timeout,
            verify=self.verify_tls,
        )

    def _headers(self, *, content_type: str | None = None) -> dict[str, str]:
        """Build FIWARE headers without exposing secrets to logs."""
        headers = {
            "Authorization": f"Bearer {self.auth.get_token()}",
            "Accept": "application/json",
            "Fiware-ServicePath": self.service_path,
        }
        if content_type is not None:
            headers["Content-Type"] = content_type
        if self.service:
            headers["Fiware-Service"] = self.service
        return headers


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Probe Orion subscription behavior against STH-Comet.",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Run live create/update/read/delete requests. Omit for dry-run.",
    )
    parser.add_argument(
        "--entity-id",
        default=_default_entity_id(),
        help="Temporary probe entity id. Must contain 'probe' for delete probing.",
    )
    parser.add_argument(
        "--entity-type",
        default=DEFAULT_ENTITY_TYPE,
        help=f"Temporary probe entity type. Defaults to {DEFAULT_ENTITY_TYPE}.",
    )
    parser.add_argument(
        "--attr",
        default=DEFAULT_ATTR,
        help=f"Temporary probe attribute. Defaults to {DEFAULT_ATTR}.",
    )
    parser.add_argument(
        "--trigger-attr",
        help=(
            "Attribute that triggers the subscription. Defaults to --attr. "
            "Use this to test storing one attr when a separate timestamp attr changes."
        ),
    )
    parser.add_argument(
        "--notification-url",
        help=(
            "Full Comet notification URL. Overrides COMET_NOTIFY_URL and "
            "--notification-path."
        ),
    )
    parser.add_argument(
        "--notification-path",
        default="/comet/notify",
        help="Comet notification path appended to FIWARE_BASE_URL.",
    )
    parser.add_argument(
        "--no-skip-initial-notification",
        action="store_true",
        help="Do not pass options=skipInitialNotification on subscription create.",
    )
    parser.add_argument(
        "--no-probe-delete",
        action="store_true",
        help="Skip the STH attribute DELETE exposure probe.",
    )
    parser.add_argument(
        "--allow-non-probe-delete",
        action="store_true",
        help="Allow STH DELETE probing even if the entity id lacks 'probe'.",
    )
    parser.add_argument(
        "--keep-resources",
        action="store_true",
        help="Do not delete the temporary Orion subscription/entity at the end.",
    )
    parser.add_argument(
        "--wait-seconds",
        type=float,
        default=DEFAULT_WAIT_SECONDS,
        help=(
            "Seconds to wait after update before reading history. "
            f"Default {DEFAULT_WAIT_SECONDS}."
        ),
    )
    parser.add_argument(
        "--history-last-n",
        type=int,
        default=DEFAULT_HISTORY_LAST_N,
        help=f"STH lastN value for history reads. Default {DEFAULT_HISTORY_LAST_N}.",
    )
    parser.add_argument(
        "--timeinstant-offset-hours",
        type=float,
        default=DEFAULT_TIMEINSTANT_OFFSET_HOURS,
        help=(
            "Offset applied to the probe TimeInstant relative to update time. "
            f"Default {DEFAULT_TIMEINSTANT_OFFSET_HOURS}."
        ),
    )
    return parser.parse_args(argv)


def _config(args: argparse.Namespace, base_url: str) -> ProbeConfig:
    """Resolve probe config from parsed args and environment."""
    notification_url = (
        args.notification_url
        or _optional_env("COMET_NOTIFY_URL", "")
        or f"{base_url}{args.notification_path}"
    )
    return ProbeConfig(
        entity_id=args.entity_id,
        entity_type=args.entity_type,
        attr=args.attr,
        trigger_attr=args.trigger_attr or args.attr,
        notification_url=notification_url,
        use_skip_initial_notification=not args.no_skip_initial_notification,
        probe_delete=not args.no_probe_delete,
        allow_non_probe_delete=args.allow_non_probe_delete,
        wait_seconds=args.wait_seconds,
        history_last_n=args.history_last_n,
        timeinstant_offset_hours=args.timeinstant_offset_hours,
        keep_resources=args.keep_resources,
        execute=args.execute,
    )


def _default_entity_id() -> str:
    """Return a unique temporary probe entity id."""
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    return f"sendai.pipeline.probe.{stamp}"


def _subscription_body(config: ProbeConfig) -> dict[str, Any]:
    """Build the Orion subscription body under test."""
    return {
        "description": f"sendai-pipeline STH probe for {config.entity_id}",
        "subject": {
            "entities": [
                {
                    "id": config.entity_id,
                    "type": config.entity_type,
                }
            ],
            "condition": {
                "attrs": [config.trigger_attr],
                "notifyOnMetadataChange": True,
            },
        },
        "notification": {
            "http": {"url": config.notification_url},
            "attrs": [config.attr],
            "attrsFormat": "legacy",
            "metadata": ["TimeInstant"],
            "onlyChangedAttrs": False,
        },
        "expires": (datetime.now(UTC) + timedelta(hours=1)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        ),
    }


def _entity_body(
    config: ProbeConfig,
    *,
    value: int,
    observed_at: str,
) -> dict[str, Any]:
    """Build a temporary Orion entity body."""
    return {
        "id": config.entity_id,
        "type": config.entity_type,
        **_attrs(config, value=value, observed_at=observed_at),
    }


def _attrs_body(config: ProbeConfig, *, value: int, observed_at: str) -> dict[str, Any]:
    """Build an Orion attrs update body."""
    return _attrs(config, value=value, observed_at=observed_at)


def _attrs(config: ProbeConfig, *, value: int, observed_at: str) -> dict[str, Any]:
    """Build probe attributes, including a separate trigger when requested."""
    attrs = {config.attr: _attr(value=value, observed_at=observed_at)}
    if config.trigger_attr != config.attr:
        attrs[config.trigger_attr] = {
            "type": "DateTime",
            "value": observed_at,
        }
    return attrs


def _attr(*, value: int, observed_at: str) -> dict[str, Any]:
    """Build the probe attribute with TimeInstant metadata."""
    return {
        "type": "Number",
        "value": value,
        "metadata": {
            "TimeInstant": {
                "type": "DateTime",
                "value": observed_at,
            }
        },
    }


def _subscription_params(config: ProbeConfig) -> dict[str, str] | None:
    """Return Orion subscription query params for the requested initial policy."""
    if config.use_skip_initial_notification:
        return {"options": "skipInitialNotification"}
    return None


def _history_params(config: ProbeConfig) -> dict[str, int]:
    """Return STH history read query parameters."""
    return {"lastN": config.history_last_n}


def _orion_entity_path(entity_id: str) -> str:
    """Return the Orion entity resource path."""
    return f"/orion/v2.0/entities/{quote(entity_id, safe='')}"


def _orion_attrs_path(entity_id: str) -> str:
    """Return the Orion attrs resource path."""
    return f"{_orion_entity_path(entity_id)}/attrs"


def _orion_subscription_path(subscription_id: str) -> str:
    """Return the Orion subscription resource path."""
    return f"/orion/v2.0/subscriptions/{quote(subscription_id, safe='')}"


def _history_path(config: ProbeConfig) -> str:
    """Return the STH-Comet v1.0 history path for the probe's attribute."""
    return (
        "/comet/v1.0/contextEntities/type/"
        f"{quote(config.entity_type, safe='')}/id/{quote(config.entity_id, safe='')}"
        f"/attributes/{quote(config.attr, safe='')}"
    )


def _delete_probe_paths(config: ProbeConfig) -> list[str]:
    """Return candidate STH attribute deletion paths to probe."""
    entity_type = quote(config.entity_type, safe="")
    entity_id = quote(config.entity_id, safe="")
    attr = quote(config.attr, safe="")
    return [
        f"/comet/v1.0/contextEntities/type/{entity_type}/id/{entity_id}/attributes/{attr}",
        f"/STH/v1/contextEntities/type/{entity_type}/id/{entity_id}/attributes/{attr}",
    ]


def _subscription_id(response: requests.Response) -> str | None:
    """Extract an Orion subscription id from a create response."""
    location = response.headers.get("Location", "")
    if location:
        return location.rstrip("/").rsplit("/", 1)[-1]
    try:
        body = response.json()
    except ValueError:
        return None
    value = body.get("id") if isinstance(body, dict) else None
    return value if isinstance(value, str) else None


def _history_values(history: Any) -> list[dict[str, Any]]:
    """Extract raw value rows from either STH v1 or v2 response shapes."""
    if isinstance(history, dict):
        values = history.get("values")
        if isinstance(values, list):
            return [value for value in values if isinstance(value, dict)]

        responses = history.get("contextResponses")
        if isinstance(responses, list):
            extracted: list[dict[str, Any]] = []
            for response in responses:
                if not isinstance(response, dict):
                    continue
                context = response.get("contextElement", {})
                if not isinstance(context, dict):
                    continue
                attributes = context.get("attributes", [])
                if not isinstance(attributes, list):
                    continue
                for attribute in attributes:
                    if not isinstance(attribute, dict):
                        continue
                    attr_values = attribute.get("values", [])
                    if isinstance(attr_values, list):
                        extracted.extend(
                            value for value in attr_values if isinstance(value, dict)
                        )
            return extracted
    return []


def _response_summary(response: requests.Response) -> dict[str, Any]:
    """Return a compact non-secret response summary."""
    text = response.text
    return {
        "status": response.status_code,
        "ok": response.ok,
        "body_excerpt": text[:500] if text else "",
    }


def _raise_for_step(step: str, response: requests.Response) -> None:
    """Raise an HTTPError with step context if a required request failed."""
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        raise requests.HTTPError(f"{step} failed: {exc}") from exc


def _print_json(data: dict[str, Any]) -> None:
    """Print JSON in a stable operator-readable form."""
    print(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True))


def _dry_run(config: ProbeConfig) -> int:
    """Print the planned live requests without contacting FIWARE."""
    _print_json(
        {
            "execute": False,
            "entity": _entity_body(
                config,
                value=0,
                observed_at="DRY-RUN-T0",
            ),
            "subscription_params": _subscription_params(config),
            "subscription": _subscription_body(config),
            "update_attrs": _attrs_body(
                config,
                value=1,
                observed_at="DRY-RUN-T1",
            ),
            "history_path": _history_path(config),
            "delete_probe_paths": (
                _delete_probe_paths(config) if config.probe_delete else []
            ),
        }
    )
    return 0


def _safe_to_probe_delete(config: ProbeConfig) -> bool:
    """Return whether DELETE probing is allowed for the configured entity id."""
    return config.allow_non_probe_delete or "probe" in config.entity_id.lower()


def _run_live(config: ProbeConfig, client: ProbeClient) -> int:
    """Run the live probe and print a JSON report."""
    if config.probe_delete and not _safe_to_probe_delete(config):
        print(
            "ERROR: refusing STH DELETE probe for entity id without 'probe'; "
            "use --allow-non-probe-delete if this is intentional",
            file=sys.stderr,
        )
        return 2

    observed_initial = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    observed_update = (
        datetime.now(UTC) + timedelta(hours=config.timeinstant_offset_hours)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")
    subscription_id: str | None = None
    report: dict[str, Any] = {
        "entity_id": config.entity_id,
        "entity_type": config.entity_type,
        "attr": config.attr,
        "trigger_attr": config.trigger_attr,
        "notification_url": config.notification_url,
        "used_skip_initial_notification": config.use_skip_initial_notification,
    }
    exit_code = 0

    try:
        create_entity = client.request(
            "POST",
            "/orion/v2.0/entities",
            body=_entity_body(config, value=0, observed_at=observed_initial),
        )
        report["create_entity"] = _response_summary(create_entity)
        _raise_for_step("create entity", create_entity)

        create_subscription = client.request(
            "POST",
            "/orion/v2.0/subscriptions",
            params=_subscription_params(config),
            body=_subscription_body(config),
        )
        report["create_subscription"] = _response_summary(create_subscription)
        _raise_for_step("create subscription", create_subscription)
        subscription_id = _subscription_id(create_subscription)
        report["subscription_id"] = subscription_id

        before_history = client.request(
            "GET",
            _history_path(config),
            params=_history_params(config),
        )
        report["history_before_update"] = _response_summary(before_history)
        before_values: list[dict[str, Any]] = []
        if before_history.ok:
            before_body = before_history.json()
            before_values = _history_values(before_body)
            report["history_before_update"]["value_count"] = len(before_values)

        update = client.request(
            "POST",
            _orion_attrs_path(config.entity_id),
            params={"type": config.entity_type},
            body=_attrs_body(config, value=1, observed_at=observed_update),
        )
        report["update_attrs"] = _response_summary(update)
        _raise_for_step("update attrs", update)

        time.sleep(config.wait_seconds)

        after_history = client.request(
            "GET",
            _history_path(config),
            params=_history_params(config),
        )
        report["history_after_update"] = _response_summary(after_history)
        after_values: list[dict[str, Any]] = []
        if after_history.ok:
            after_body = after_history.json()
            after_values = _history_values(after_body)
            report["history_after_update"]["value_count"] = len(after_values)
            report["history_after_update"]["values"] = after_values
            report["timeinstant_probe"] = _timeinstant_summary(
                after_values,
                observed_update,
            )

        if subscription_id:
            subscription = client.request(
                "GET",
                _orion_subscription_path(subscription_id),
            )
            report["subscription_after_update"] = _response_summary(subscription)
            if subscription.ok:
                report["subscription_after_update"]["body"] = subscription.json()

        if config.probe_delete:
            report["delete_history_probe"] = _probe_history_delete(
                client,
                config,
                history_before=len(after_values),
            )

    except Exception as exc:
        report["error"] = {
            "type": type(exc).__name__,
            "message": str(exc),
        }
        exit_code = 1

    finally:
        cleanup: dict[str, Any] = {}
        if not config.keep_resources:
            if subscription_id:
                cleanup["delete_subscription"] = _response_summary(
                    client.request(
                        "DELETE",
                        _orion_subscription_path(subscription_id),
                    )
                )
            cleanup["delete_entity"] = _response_summary(
                client.request(
                    "DELETE",
                    _orion_entity_path(config.entity_id),
                    params={"type": config.entity_type},
                )
            )
        else:
            cleanup["kept_resources"] = True
        report["cleanup"] = cleanup

    _print_json(report)
    return exit_code


def _timeinstant_summary(
    values: list[dict[str, Any]],
    expected_timeinstant: str,
) -> dict[str, Any]:
    """Summarize whether STH appears to use the probe TimeInstant."""
    recv_times = [
        str(value.get("recvTime"))
        for value in values
        if value.get("recvTime") is not None
    ]
    return {
        "expected_timeinstant": expected_timeinstant,
        "recv_times": recv_times,
        "matched_expected_timeinstant": any(
            _same_instant(expected_timeinstant, recv_time) for recv_time in recv_times
        ),
    }


def _same_instant(left: str, right: str) -> bool:
    """Return whether two ISO timestamps represent the same UTC instant."""
    left_dt = _parse_utc_datetime(left)
    right_dt = _parse_utc_datetime(right)
    if left_dt is None or right_dt is None:
        return left == right
    return left_dt == right_dt


def _parse_utc_datetime(value: str) -> datetime | None:
    """Parse a UTC ISO timestamp from Orion/STH output."""
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return None


def _probe_history_delete(
    client: ProbeClient,
    config: ProbeConfig,
    *,
    history_before: int,
) -> dict[str, Any]:
    """Try safe STH DELETE variants, then verify their effect with a GET."""
    attempts: list[dict[str, Any]] = []
    final_response: requests.Response | None = None
    for path in _delete_probe_paths(config):
        response = client.request("DELETE", path)
        summary = _response_summary(response)
        summary["path"] = path
        attempts.append(summary)
        final_response = response
        if response.ok:
            break

    if final_response is None:
        raise RuntimeError("no STH DELETE probe path is configured")

    after_response = client.request(
        "GET",
        _history_path(config),
        params=_history_params(config),
    )
    verification = _history_delete_verification(
        after_response,
        history_before=history_before,
    )
    return {
        "attempts": attempts,
        "interpretation": _delete_status_interpretation(
            final_response.status_code,
            final_response.text,
        ),
        "verification": verification,
    }


def _delete_status_interpretation(status: int, body: str) -> str:
    """Classify the gateway or backend outcome of an STH DELETE attempt."""
    if status in {200, 204}:
        return "delete_succeeded"
    if status == 404 and "am:fault" in body:
        return "gateway_route_missing"
    if status == 404:
        return "backend_not_found"
    if status == 405:
        return "gateway_method_not_allowed"
    if status == 403:
        return "backend_forbidden"
    return "unexpected_status"


def _history_delete_verification(
    response: requests.Response,
    *,
    history_before: int,
) -> dict[str, Any]:
    """Summarize whether a post-DELETE history read confirms removal."""
    summary = _response_summary(response)
    summary["values_before"] = history_before

    if response.status_code == 404:
        values_after = 0
    elif response.ok:
        try:
            values_after = len(_history_values(response.json()))
        except ValueError:
            summary["effect"] = "inconclusive_invalid_history_json"
            return summary
    else:
        summary["effect"] = "inconclusive_history_read_failed"
        return summary

    summary["values_after"] = values_after
    if history_before <= 0:
        summary["effect"] = "inconclusive_no_prior_history"
    elif values_after == 0:
        summary["effect"] = "verified_removed"
    elif values_after < history_before:
        summary["effect"] = "verified_reduced"
    else:
        summary["effect"] = "verified_not_removed"
    return summary


def _client(auth_settings: AuthSettings) -> ProbeClient:
    """Build the HTTP client used by the probe."""
    service = _optional_env("FIWARE_SERVICE", "")
    service_path = _optional_env("FIWARE_SERVICE_PATH", "/")
    timeout = float(_optional_env("FIWARE_TIMEOUT_SECONDS", "10"))
    return ProbeClient(
        base_url=auth_settings.base_url,
        service=service,
        service_path=service_path,
        verify_tls=auth_settings.verify_tls,
        timeout=timeout,
        auth=AuthClient(auth_settings),
        session=requests.Session(),
    )


def _optional_env(name: str, default: str) -> str:
    """Return an optional environment value with blank treated as default."""
    value = os.environ.get(name)
    if value is None or value.strip() == "":
        return default
    return value


def main(argv: Sequence[str] | None = None) -> int:
    """Run the STH subscription probe."""
    args = _parse_args(argv)
    if not args.execute:
        base_url = _optional_env("FIWARE_BASE_URL", "https://<FIWARE_BASE_URL>")
        config = _config(args, base_url.rstrip("/"))
        return _dry_run(config)

    load_dotenv(find_dotenv(usecwd=True))
    auth_settings = AuthSettings.from_env()
    config = _config(args, auth_settings.base_url)
    return _run_live(config, _client(auth_settings))


if __name__ == "__main__":
    raise SystemExit(main())
