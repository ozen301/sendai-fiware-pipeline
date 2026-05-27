"""Dev probe: end-to-end test of whether STH-Comet DELETE is reachable.

Lifecycle (cleanup always runs, even on error):
  1. Create a test-only Orion entity (type prefixed with "probe.").
  2. Create a test-only STH subscription that targets only that type.
  3. Push several attribute updates so STH-Comet records history.
  4. Verify history exists via GET /comet/v1.0/contextEntities/.../attributes/<attr>.
  5. Try DELETE on the same path and report the response.
  6. Cleanup: delete the subscription and the entity from Orion.

The test type "probe.sth_delete" is unique to this probe and is NOT covered
by the production STH subscriptions, so production history is untouched.
"""

import os
import sys
import time
import traceback
from datetime import UTC, datetime

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
COMET_NOTIFY_URL = os.environ.get("COMET_NOTIFY_URL", "")

ORION = f"{BASE_URL}/orion/v2.0"
COMET = f"{BASE_URL}/comet/v1.0"

TEST_TYPE = "probe.sth_delete"
TEST_ENTITY_ID = "jp.sendai.probe.sth_delete.001"
TEST_ATTR = "probeValue"
TEST_TRIGGER_ATTR = "probeTrigger"
UPDATE_COUNT = 3
UPDATE_INTERVAL_SECONDS = 2
STH_SETTLE_SECONDS = 5

COMET_ATTR_URL = (
    f"{COMET}/contextEntities/type/{TEST_TYPE}"
    f"/id/{TEST_ENTITY_ID}/attributes/{TEST_ATTR}"
)


def _headers(content_type: str | None = None) -> dict[str, str]:
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


def _now_iso() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat()


def create_entity() -> None:
    print(f"\n[1] Creating test entity {TEST_ENTITY_ID} ({TEST_TYPE}) ...")
    body = {
        "id": TEST_ENTITY_ID,
        "type": TEST_TYPE,
        TEST_ATTR: {"type": "Number", "value": 0},
        TEST_TRIGGER_ATTR: {"type": "DateTime", "value": _now_iso()},
    }
    r = requests.post(
        f"{ORION}/entities",
        json=body,
        headers=_headers("application/json"),
        verify=VERIFY_TLS,
    )
    print(f"    POST /entities -> {r.status_code}")
    if r.status_code not in (201, 204):
        print(f"    body: {r.text[:300]}")
        raise RuntimeError(f"create_entity failed: {r.status_code}")


def create_subscription() -> str:
    print(f"\n[2] Creating test STH subscription for type={TEST_TYPE} ...")
    if not COMET_NOTIFY_URL:
        raise RuntimeError("COMET_NOTIFY_URL is not set; cannot create subscription")

    body = {
        "description": f"probe sth_delete test at {_now_iso()}",
        "subject": {
            "entities": [{"idPattern": ".*", "type": TEST_TYPE}],
            "condition": {
                "attrs": [TEST_TRIGGER_ATTR],
                "notifyOnMetadataChange": True,
            },
        },
        "notification": {
            "http": {"url": COMET_NOTIFY_URL},
            "attrsFormat": "legacy",
            "attrs": [TEST_ATTR, TEST_TRIGGER_ATTR],
            "metadata": ["TimeInstant"],
        },
    }
    r = requests.post(
        f"{ORION}/subscriptions",
        params={"options": "skipInitialNotification"},
        json=body,
        headers=_headers("application/json"),
        verify=VERIFY_TLS,
    )
    print(f"    POST /subscriptions -> {r.status_code}")
    if r.status_code != 201:
        print(f"    body: {r.text[:300]}")
        raise RuntimeError(f"create_subscription failed: {r.status_code}")
    sub_id = r.headers.get("Location", "").rsplit("/", 1)[-1]
    print(f"    subscription id: {sub_id}")
    return sub_id


def send_updates() -> None:
    print(f"\n[3] Sending {UPDATE_COUNT} attribute updates ...")
    for i in range(1, UPDATE_COUNT + 1):
        body = {
            TEST_ATTR: {"type": "Number", "value": i * 10},
            TEST_TRIGGER_ATTR: {"type": "DateTime", "value": _now_iso()},
        }
        r = requests.post(
            f"{ORION}/entities/{TEST_ENTITY_ID}/attrs",
            params={"type": TEST_TYPE},
            json=body,
            headers=_headers("application/json"),
            verify=VERIFY_TLS,
        )
        print(f"    update {i}/{UPDATE_COUNT} -> {r.status_code}")
        if r.status_code not in (200, 204):
            print(f"    body: {r.text[:200]}")
        if i < UPDATE_COUNT:
            time.sleep(UPDATE_INTERVAL_SECONDS)

    print(f"    waiting {STH_SETTLE_SECONDS}s for STH-Comet to persist ...")
    time.sleep(STH_SETTLE_SECONDS)


def verify_history() -> int:
    print("\n[4] Verifying STH-Comet has history via GET ...")
    r = requests.get(
        COMET_ATTR_URL,
        params={"lastN": 10},
        headers=_headers(),
        verify=VERIFY_TLS,
    )
    print(f"    GET -> {r.status_code}")
    print(f"    body: {r.text[:600]}")
    if r.status_code != 200:
        return 0
    try:
        data = r.json()
        values = (
            data.get("contextResponses", [{}])[0]
            .get("contextElement", {})
            .get("attributes", [{}])[0]
            .get("values", [])
        )
        return len(values) if isinstance(values, list) else 0
    except Exception:
        return 0


def try_delete() -> tuple[int, str]:
    print("\n[5] Attempting DELETE on STH-Comet ...")
    r = requests.delete(COMET_ATTR_URL, headers=_headers(), verify=VERIFY_TLS)
    print(f"    DELETE -> {r.status_code}")
    print(f"    body: {r.text[:600]}")
    return r.status_code, r.text


def verify_after_delete(history_before: int) -> None:
    print("\n[5b] Re-checking history after DELETE ...")
    r = requests.get(
        COMET_ATTR_URL, params={"lastN": 10}, headers=_headers(), verify=VERIFY_TLS
    )
    print(f"    GET -> {r.status_code}")
    try:
        values = (
            r.json()
            .get("contextResponses", [{}])[0]
            .get("contextElement", {})
            .get("attributes", [{}])[0]
            .get("values", [])
        )
        after = len(values) if isinstance(values, list) else 0
        print(f"    values before DELETE: {history_before}, after DELETE: {after}")
    except Exception:
        print(f"    body: {r.text[:300]}")


def cleanup(sub_id: str | None) -> None:
    print("\n[6] Cleanup ...")
    if sub_id:
        r = requests.delete(
            f"{ORION}/subscriptions/{sub_id}",
            headers=_headers(),
            verify=VERIFY_TLS,
        )
        print(f"    DELETE subscription {sub_id} -> {r.status_code}")
    r = requests.delete(
        f"{ORION}/entities/{TEST_ENTITY_ID}",
        params={"type": TEST_TYPE},
        headers=_headers(),
        verify=VERIFY_TLS,
    )
    print(f"    DELETE entity {TEST_ENTITY_ID} -> {r.status_code}")


def main() -> int:
    sub_id: str | None = None
    try:
        create_entity()
        sub_id = create_subscription()
        send_updates()
        n_before = verify_history()
        if n_before == 0:
            print(
                "\n=> WARNING: STH-Comet returned 0 history rows. "
                "DELETE result will not be conclusive."
            )
        status, body = try_delete()
        verify_after_delete(n_before)

        print("\n" + "=" * 60)
        if status in (200, 204):
            print("=> DELETE returned success. STH DELETE appears USABLE.")
        elif status == 404 and "am:fault" in body:
            print("=> DELETE returned 404 from WSO2 gateway (route NOT registered).")
        elif status == 404:
            print(
                "=> DELETE returned 404 from backend "
                "(route exists; entity missing in STH)."
            )
        elif status == 405:
            print("=> DELETE blocked at gateway (Method Not Allowed).")
        elif status == 403:
            print("=> DELETE reached backend but FORBIDDEN.")
        else:
            print(f"=> DELETE unexpected status {status}.")
        print("=" * 60)
        return 0
    except Exception:
        traceback.print_exc()
        return 1
    finally:
        try:
            cleanup(sub_id)
        except Exception:
            traceback.print_exc()


if __name__ == "__main__":
    sys.exit(main())
