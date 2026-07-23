import json
from dataclasses import fields
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
import requests

from sendai_pipeline.logging_setup import _ALLOWED_EXTRA_KEYS
from sendai_pipeline.sth_subscriptions import (
    PRODUCT_A_HISTORY_ATTRS,
    PRODUCT_A_TRIGGER_ATTRS,
    PRODUCT_B_STABLE_WRITE_ATTRS,
    StHSubscriptionError,
    StHSubscriptionResult,
    StHSubscriptionSettings,
    _entity_selector_pair_may_overlap,
    build_product_a_subscription_body,
    build_product_b_subscription_body,
    create_product_a_sth_subscription,
    create_product_b_sth_subscription,
    delete_subscription,
    get_subscription,
    redacted_product_a_subscription_json,
    redacted_product_b_subscription_json,
    redacted_subscription_json,
)

JST = timezone(timedelta(hours=9))
AGGREGATE_ENTITY_ID = "custom.aggregate.entity"
AGGREGATE_ENTITY_TYPE = "Custom.aggregate.type"
COMET_NOTIFY_URL = "http://internal-comet.example/notify"
PRODUCT_A_ATTRS = (
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
OLD_PRODUCT_A_ATTRS = (
    "dateObservedFrom",
    "dateObservedTo",
    "peopleCount_immedate",
    "peopleCount_near",
    "peopleCount_far",
    "peopleOccupancy_immedate",
    "peopleOccupancy_near",
)


class FakeAuth:
    def __init__(self) -> None:
        self.force_refreshes: list[bool] = []

    def get_token(self, *, force_refresh: bool = False) -> str:
        self.force_refreshes.append(force_refresh)
        return "token-refreshed" if force_refresh else "token"


class FakeResponse:
    def __init__(
        self,
        status_code: int,
        *,
        text: str = "",
        headers: dict[str, str] | None = None,
        json_body: Any = None,
        infer_total_count: bool = True,
    ) -> None:
        self.status_code = status_code
        self.text = text
        self.json_body = [] if json_body is None else json_body
        self.headers = dict(headers or {})
        if (
            infer_total_count
            and status_code == 200
            and isinstance(self.json_body, list)
            and "Fiware-Total-Count" not in self.headers
        ):
            self.headers["Fiware-Total-Count"] = str(len(self.json_body))

    def json(self) -> Any:
        return self.json_body


class FakeSession:
    def __init__(
        self,
        post_responses: list[FakeResponse] | None = None,
        get_responses: list[Any] | None = None,
        delete_responses: list[FakeResponse] | None = None,
    ) -> None:
        self.post_responses = post_responses or []
        self.get_responses = get_responses or []
        self.delete_responses = delete_responses or []
        self.posts: list[dict[str, Any]] = []
        self.gets: list[dict[str, Any]] = []
        self.deletes: list[dict[str, Any]] = []

    def get(self, url: str, **kwargs: Any) -> FakeResponse:
        self.gets.append({"url": url, **kwargs})
        outcome = self.get_responses.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    def post(self, url: str, **kwargs: Any) -> FakeResponse:
        self.posts.append({"url": url, **kwargs})
        return self.post_responses.pop(0)

    def delete(self, url: str, **kwargs: Any) -> FakeResponse:
        self.deletes.append({"url": url, **kwargs})
        return self.delete_responses.pop(0)


class PaginatedInventorySession(FakeSession):
    def __init__(
        self,
        later_page: list[dict[str, Any]],
        *,
        second_total_delta: int = 0,
    ) -> None:
        super().__init__(
            post_responses=[
                FakeResponse(201, headers={"Location": "/subscriptions/unexpected"})
            ]
        )
        self.later_page = later_page
        self.second_total_delta = second_total_delta

    def get(self, url: str, **kwargs: Any) -> FakeResponse:
        self.gets.append({"url": url, **kwargs})
        params = kwargs.get("params")
        if not isinstance(params, dict):
            return FakeResponse(200, json_body=[])

        limit = params.get("limit")
        offset = params.get("offset")
        if not isinstance(limit, int) or limit <= 0:
            return FakeResponse(
                200,
                headers={"Fiware-Total-Count": "1"},
                json_body=[],
            )
        total = limit + 1
        if offset == 0:
            return FakeResponse(
                200,
                headers={"Fiware-Total-Count": str(total)},
                json_body=[unrelated_subscription(index) for index in range(limit)],
            )
        return FakeResponse(
            200,
            headers={"Fiware-Total-Count": str(total + self.second_total_delta)},
            json_body=self.later_page,
        )


def settings(**overrides: Any) -> StHSubscriptionSettings:
    values: dict[str, Any] = {
        "base_url": "https://example.test",
        "comet_notify_url": COMET_NOTIFY_URL,
    }
    values.update(overrides)
    return StHSubscriptionSettings(**values)


def product_b_subscription(
    *,
    subscription_id: str = "aggregate-sub",
    description: str = "Product B aggregate STH-Comet history set at earlier",
    entity_id: str = AGGREGATE_ENTITY_ID,
    entity_type: str = AGGREGATE_ENTITY_TYPE,
    notify_url: str = COMET_NOTIFY_URL,
) -> dict[str, Any]:
    return {
        "id": subscription_id,
        "status": "active",
        "description": description,
        "subject": {
            "entities": [{"id": entity_id, "type": entity_type}],
            "condition": {
                "attrs": ["dateRetrieved"],
                "notifyOnMetadataChange": True,
            },
        },
        "notification": {
            "http": {"url": notify_url},
            "attrsFormat": "legacy",
            "attrs": [],
            "metadata": ["TimeInstant"],
            "onlyChangedAttrs": False,
            "covered": False,
            "timesSent": 9,
            "lastNotification": "2026-07-24T00:00:01.000Z",
            "lastSuccess": "2026-07-24T00:00:01.000Z",
            "lastSuccessCode": 200,
        },
    }


def product_a_get_subscription(
    *,
    subscription_id: str = "product-a-current",
    status: Any = "active",
    attrs: tuple[str, ...] = PRODUCT_A_ATTRS,
    trigger_attrs: tuple[str, ...] = PRODUCT_A_ATTRS,
    expires: Any = "2030-01-01T00:00:00.000Z",
) -> dict[str, Any]:
    subscription: dict[str, Any] = {
        "id": subscription_id,
        "status": status,
        "description": "Product A STH-Comet history set at earlier",
        "subject": {
            "entities": [
                {"idPattern": ".*", "type": "Blesensor.per300"},
                {"idPattern": ".*", "type": "Blesensor.per3600"},
            ],
            "condition": {
                "attrs": list(trigger_attrs),
                "notifyOnMetadataChange": True,
            },
        },
        "notification": {
            "http": {"url": COMET_NOTIFY_URL},
            "attrsFormat": "legacy",
            "attrs": list(attrs),
            "metadata": ["TimeInstant"],
            "lastNotification": "2026-07-23T00:00:01.000Z",
            "timesSent": 17,
            "lastSuccess": "2026-07-23T00:00:01.000Z",
            "lastSuccessCode": 200,
            "onlyChangedAttrs": False,
            "covered": False,
        },
    }
    if expires is not None:
        subscription["expires"] = expires
    return subscription


def unrelated_subscription(index: int) -> dict[str, Any]:
    return {
        "id": f"unrelated-{index}",
        "status": "active",
        "description": f"Unrelated subscription {index}",
        "subject": {
            "entities": [{"id": f"other-{index}", "type": "Other.Type"}],
            "condition": {"attrs": ["otherAttr"]},
        },
        "notification": {
            "http": {"url": COMET_NOTIFY_URL},
            "attrsFormat": "legacy",
            "metadata": ["TimeInstant"],
        },
    }


def remove_nested_field(body: dict[str, Any], path: tuple[str | int, ...]) -> None:
    current: Any = body
    for key in path[:-1]:
        current = current[key]
    del current[path[-1]]


def create_product_b_with_existing(
    existing: dict[str, Any],
) -> tuple[StHSubscriptionResult, FakeSession]:
    fake_session = FakeSession(
        post_responses=[
            FakeResponse(201, headers={"Location": "/subscriptions/correct-sub"})
        ],
        get_responses=[FakeResponse(200, json_body=[existing])],
    )
    result = create_product_b_sth_subscription(
        settings=settings(
            dry_run=False,
            product_b_aggregate_entity_id=AGGREGATE_ENTITY_ID,
            product_b_aggregate_entity_type=AGGREGATE_ENTITY_TYPE,
        ),
        auth=FakeAuth(),
        session=fake_session,
    )
    return result, fake_session


def test_build_product_a_subscription_preserves_shape_without_throttling() -> None:
    body = build_product_a_subscription_body(
        settings(),
        now=datetime(2026, 5, 24, 12, 30, tzinfo=JST),
    )

    assert body == {
        "description": "Product A STH-Comet history set at 2026-05-24T12:30:00+09:00",
        "subject": {
            "entities": [
                {"idPattern": ".*", "type": "Blesensor.per300"},
                {"idPattern": ".*", "type": "Blesensor.per3600"},
            ],
            "condition": {
                "attrs": list(PRODUCT_A_ATTRS),
                "notifyOnMetadataChange": True,
            },
        },
        "notification": {
            "http": {"url": COMET_NOTIFY_URL},
            "attrsFormat": "legacy",
            "attrs": list(PRODUCT_A_ATTRS),
            "metadata": ["TimeInstant"],
        },
    }


def test_product_a_shared_constants_equal_exact_ten_attribute_contract() -> None:
    assert PRODUCT_A_HISTORY_ATTRS == PRODUCT_A_ATTRS
    assert PRODUCT_A_TRIGGER_ATTRS == PRODUCT_A_ATTRS


def test_product_a_trigger_covers_people_occupancy_near_only_correction_gap() -> None:
    body = build_product_a_subscription_body(settings())
    before = {"peopleCount_immedate": 6, "peopleOccupancy_near": 40.9}
    after = {"peopleCount_immedate": 6, "peopleOccupancy_near": 41.0}
    changed_attrs = {name for name in before if before[name] != after[name]}

    assert changed_attrs == {"peopleOccupancy_near"}
    assert changed_attrs <= set(body["subject"]["condition"]["attrs"])
    assert before["peopleCount_immedate"] == after["peopleCount_immedate"]


def test_product_a_subscription_omits_unconditional_entity_update_trigger() -> None:
    body = build_product_a_subscription_body(settings())

    assert "alterationTypes" not in body["subject"]["condition"]


def test_redacted_subscription_json_omits_private_notify_url() -> None:
    rendered = redacted_subscription_json(settings())

    assert "internal-comet.example" not in rendered
    assert "<COMET_NOTIFY_URL>" in rendered


def test_settings_from_env_uses_private_notify_url_and_defaults() -> None:
    parsed = StHSubscriptionSettings.from_env(
        {
            "FIWARE_BASE_URL": "https://example.test/",
            "COMET_NOTIFY_URL": "http://internal-comet.example/notify",
        }
    )

    assert parsed.base_url == "https://example.test"
    assert parsed.comet_notify_url == "http://internal-comet.example/notify"
    assert parsed.service == ""
    assert parsed.service_path == "/"
    assert parsed.dry_run is True
    assert parsed.skip_initial_notification is True
    assert "throttling_seconds" not in {field.name for field in fields(parsed)}


def test_settings_from_env_accepts_subscription_overrides() -> None:
    parsed = StHSubscriptionSettings.from_env(
        {
            "FIWARE_BASE_URL": "https://example.test",
            "COMET_NOTIFY_URL": "http://internal-comet.example/notify",
            "FIWARE_SERVICE": "service",
            "FIWARE_SERVICE_PATH": "/path",
            "STH_SUBSCRIPTION_EXPIRES": "2030-01-01T00:00:00Z",
            "STH_SUBSCRIPTION_SKIP_INITIAL": "false",
        }
    )

    assert parsed.service == "service"
    assert parsed.service_path == "/path"
    assert parsed.expires == "2030-01-01T00:00:00Z"
    assert parsed.skip_initial_notification is False


def test_build_product_a_subscription_ignores_removed_legacy_throttling_override() -> (
    None
):
    parsed = StHSubscriptionSettings.from_env(
        {
            "FIWARE_BASE_URL": "https://example.test",
            "COMET_NOTIFY_URL": COMET_NOTIFY_URL,
            "STH_SUBSCRIPTION_THROTTLING_SECONDS": "30",
        }
    )

    body = build_product_a_subscription_body(
        parsed,
        now=datetime(2026, 5, 24, 12, 30, tzinfo=JST),
    )

    # The removed shared knob deliberately changes configured Product A bodies only
    # by omitting throttling; its selector, trigger, projection, and metadata stay.
    assert "throttling" not in body
    assert body["subject"]["entities"] == [
        {"idPattern": ".*", "type": "Blesensor.per300"},
        {"idPattern": ".*", "type": "Blesensor.per3600"},
    ]
    assert body["subject"]["condition"] == {
        "attrs": list(PRODUCT_A_TRIGGER_ATTRS),
        "notifyOnMetadataChange": True,
    }
    assert body["notification"]["attrs"] == list(PRODUCT_A_HISTORY_ATTRS)
    assert body["notification"]["metadata"] == ["TimeInstant"]


def test_settings_from_env_requires_comet_notify_url() -> None:
    with pytest.raises(StHSubscriptionError, match="COMET_NOTIFY_URL"):
        StHSubscriptionSettings.from_env({"FIWARE_BASE_URL": "https://example.test"})


def test_settings_from_env_skips_product_b_validation_when_not_required() -> None:
    # A Product-A-only run passes require_product_b=False, so a malformed
    # PRODUCT_B_AGGREGATE_* value is neither read nor validated; the aggregate
    # fields keep their defaults instead of failing an unrelated Product A run.
    parsed = StHSubscriptionSettings.from_env(
        {
            "FIWARE_BASE_URL": "https://example.test",
            "COMET_NOTIFY_URL": COMET_NOTIFY_URL,
            "PRODUCT_B_AGGREGATE_ENTITY_ID": " malformed",
            "PRODUCT_B_AGGREGATE_ENTITY_TYPE": "",
        },
        require_product_b=False,
    )

    assert parsed.product_b_aggregate_entity_id == "jp.sendai.Blesensor.flow"
    assert parsed.product_b_aggregate_entity_type == "Blesensor.flow"


def test_settings_from_env_validates_product_b_when_required() -> None:
    # The default (require_product_b=True) still rejects a malformed value, so
    # a run that creates the Product B subscription cannot use bad config.
    with pytest.raises(StHSubscriptionError, match="PRODUCT_B_AGGREGATE_ENTITY_ID"):
        StHSubscriptionSettings.from_env(
            {
                "FIWARE_BASE_URL": "https://example.test",
                "COMET_NOTIFY_URL": COMET_NOTIFY_URL,
                "PRODUCT_B_AGGREGATE_ENTITY_ID": " malformed",
            }
        )


def test_create_product_a_subscription_dry_run_does_not_post(
    caplog: pytest.LogCaptureFixture,
) -> None:
    fake_session = FakeSession()

    with caplog.at_level("INFO", logger="sendai_pipeline"):
        result = create_product_a_sth_subscription(
            settings=settings(dry_run=True),
            auth=None,
            session=fake_session,
        )

    assert result.would_create == 1
    assert result.created == 0
    assert result.skipped == 0
    assert result.failed == 0
    assert fake_session.gets == []
    assert fake_session.posts == []
    assert [getattr(record, "event", None) for record in caplog.records] == [
        "sth_subscription_would_create"
    ]


def test_create_product_a_subscription_posts_body_and_skip_initial_param() -> None:
    fake_auth = FakeAuth()
    fake_session = FakeSession(
        post_responses=[
            FakeResponse(201, headers={"Location": "/orion/v2.0/subscriptions/sub-1"})
        ],
        get_responses=[FakeResponse(200, json_body=[])],
    )

    result = create_product_a_sth_subscription(
        settings=settings(
            dry_run=False,
            service="svc",
            service_path="/",
            expires="2030-01-01T00:00:00Z",
        ),
        auth=fake_auth,
        session=fake_session,
    )

    assert result.created == 1
    assert result.skipped == 0
    assert result.failed == 0
    assert result.subscription_ids == ("sub-1",)
    assert fake_auth.force_refreshes == [False, False]
    assert (
        fake_session.gets[0]["url"] == "https://example.test/orion/v2.0/subscriptions"
    )
    assert "Content-Type" not in fake_session.gets[0]["headers"]
    [post] = fake_session.posts
    assert post["url"] == "https://example.test/orion/v2.0/subscriptions"
    assert post["params"] == {"options": "skipInitialNotification"}
    assert post["headers"]["Authorization"] == "Bearer token"
    assert post["headers"]["Content-Type"] == "application/json"
    assert post["headers"]["Fiware-Service"] == "svc"
    body = json.loads(post["data"])
    assert body["notification"]["http"]["url"] == "http://internal-comet.example/notify"
    assert body["notification"]["metadata"] == ["TimeInstant"]
    assert body["expires"] == "2030-01-01T00:00:00Z"
    assert "throttling" not in body


def test_create_product_a_rejects_description_only_stale_shape() -> None:
    fake_session = FakeSession(
        get_responses=[
            FakeResponse(
                200,
                json_body=[
                    {
                        "id": "existing-sub",
                        "description": "Product A STH-Comet history set at earlier",
                    }
                ],
            )
        ]
    )

    result = create_product_a_sth_subscription(
        settings=settings(dry_run=False),
        auth=FakeAuth(),
        session=fake_session,
    )

    assert result.created == 0
    assert result.skipped == 0
    assert result.failed == 1
    assert result.subscription_ids == ("existing-sub",)
    assert fake_session.posts == []


def test_create_product_a_subscription_skips_representative_orion_get_shape() -> None:
    existing = product_a_get_subscription(subscription_id="shape-sub")
    fake_session = FakeSession(get_responses=[FakeResponse(200, json_body=[existing])])

    result = create_product_a_sth_subscription(
        settings=settings(
            dry_run=False,
            expires="2030-01-01T09:00:00+09:00",
        ),
        auth=FakeAuth(),
        session=fake_session,
    )

    assert existing["id"]
    assert existing["status"] == "active"
    assert existing["expires"] == "2030-01-01T00:00:00.000Z"
    assert {
        "lastNotification",
        "timesSent",
        "lastSuccess",
        "lastSuccessCode",
        "onlyChangedAttrs",
        "covered",
    } <= existing["notification"].keys()
    assert result.skipped == 1
    assert result.failed == 0
    assert result.subscription_ids == ("shape-sub",)
    assert fake_session.posts == []


def test_create_product_a_subscription_skips_exact_shape_with_fails_counter() -> None:
    # Orion adds notification.failsCounter after consecutive delivery failures.
    # It is server-managed telemetry like timesSent, so an otherwise-exact
    # subscription carrying it must still match and be skipped, not flagged
    # stale (which would happen exactly when Comet is unhealthy).
    existing = product_a_get_subscription(subscription_id="shape-sub")
    existing["notification"]["failsCounter"] = 4
    fake_session = FakeSession(get_responses=[FakeResponse(200, json_body=[existing])])

    result = create_product_a_sth_subscription(
        settings=settings(dry_run=False, expires="2030-01-01T09:00:00+09:00"),
        auth=FakeAuth(),
        session=fake_session,
    )

    assert result.skipped == 1
    assert result.failed == 0
    assert result.subscription_ids == ("shape-sub",)
    assert fake_session.posts == []


@pytest.mark.parametrize("field", ["onlyChangedAttrs", "covered"])
def test_create_product_a_subscription_skips_when_false_default_is_omitted(
    field: str,
) -> None:
    existing = product_a_get_subscription(subscription_id=f"omitted-{field}")
    existing["notification"].pop(field)
    fake_session = FakeSession(get_responses=[FakeResponse(200, json_body=[existing])])

    result = create_product_a_sth_subscription(
        settings=settings(dry_run=False, expires="2030-01-01T09:00:00+09:00"),
        auth=FakeAuth(),
        session=fake_session,
    )

    assert result.skipped == 1
    assert result.failed == 0
    assert result.subscription_ids == (f"omitted-{field}",)
    assert fake_session.posts == []


@pytest.mark.parametrize("field", ["onlyChangedAttrs", "covered"])
def test_create_product_a_subscription_rejects_enabled_notification_default(
    field: str,
) -> None:
    stale = product_a_get_subscription(subscription_id=f"enabled-{field}")
    stale["notification"][field] = True
    fake_session = FakeSession(
        get_responses=[FakeResponse(200, json_body=[stale])],
    )

    result = create_product_a_sth_subscription(
        settings=settings(
            dry_run=False,
            expires="2030-01-01T00:00:00Z",
        ),
        auth=FakeAuth(),
        session=fake_session,
    )

    assert result.failed == 1
    assert result.skipped == 0
    assert result.subscription_ids == (f"enabled-{field}",)
    assert fake_session.posts == []


def test_create_product_a_reports_stale_for_unhashable_selector_without_crashing() -> (
    None
):
    # A recognized Product A subscription whose entity selector carries a
    # non-string (unhashable) value must yield a controlled stale result, not a
    # raw TypeError escaping the preflight's failed=1 contract.
    malformed = product_a_get_subscription(subscription_id="malformed-selector")
    malformed["subject"]["entities"][0]["type"] = ["Blesensor.per300"]
    fake_session = FakeSession(get_responses=[FakeResponse(200, json_body=[malformed])])

    # settings.expires matches the fixture's expires so the top-level-key check
    # passes and the shape check reaches the selector comparison — the code path
    # this guard covers. Without matching expires the subscription would be
    # rejected earlier and the test would pass regardless of the guard.
    result = create_product_a_sth_subscription(
        settings=settings(dry_run=False, expires="2030-01-01T09:00:00+09:00"),
        auth=FakeAuth(),
        session=fake_session,
    )

    assert result.failed == 1
    assert result.created == 0
    assert result.subscription_ids == ("malformed-selector",)
    assert fake_session.posts == []


def test_create_product_a_subscription_reports_stale_shape_for_operator_removal() -> (
    None
):
    stale = product_a_get_subscription(subscription_id="legacy-sub")
    stale["notification"].pop("metadata")
    fake_session = FakeSession(
        get_responses=[FakeResponse(200, json_body=[stale])],
    )

    result = create_product_a_sth_subscription(
        settings=settings(
            dry_run=False,
            expires="2030-01-01T00:00:00Z",
        ),
        auth=FakeAuth(),
        session=fake_session,
    )

    assert result.created == 0
    assert result.skipped == 0
    assert result.failed == 1
    assert result.subscription_ids == ("legacy-sub",)
    assert fake_session.posts == []


def test_create_product_a_subscription_rejects_old_seven_attribute_shape() -> None:
    old = product_a_get_subscription(
        subscription_id="old-seven",
        attrs=OLD_PRODUCT_A_ATTRS,
        trigger_attrs=("peopleCount_immedate",),
    )
    fake_session = FakeSession(
        get_responses=[FakeResponse(200, json_body=[old])],
    )

    result = create_product_a_sth_subscription(
        settings=settings(
            dry_run=False,
            expires="2030-01-01T00:00:00Z",
        ),
        auth=FakeAuth(),
        session=fake_session,
    )

    assert result.failed == 1
    assert result.skipped == 0
    assert result.subscription_ids == ("old-seven",)
    assert fake_session.posts == []


@pytest.mark.parametrize(
    "live_status",
    [
        pytest.param("inactive", id="inactive"),
        pytest.param("expired", id="expired"),
        pytest.param("oneshot", id="oneshot"),
        pytest.param(None, id="absent"),
        pytest.param({"unexpected": "shape"}, id="unparseable"),
    ],
)
def test_create_product_a_subscription_treats_non_active_status_as_stale(
    live_status: Any,
) -> None:
    stale = product_a_get_subscription(
        subscription_id="status-stale",
        status=live_status,
    )
    if live_status is None:
        stale.pop("status")
    fake_session = FakeSession(
        get_responses=[FakeResponse(200, json_body=[stale])],
    )

    result = create_product_a_sth_subscription(
        settings=settings(
            dry_run=False,
            expires="2030-01-01T00:00:00Z",
        ),
        auth=FakeAuth(),
        session=fake_session,
    )

    assert result.failed == 1
    assert result.skipped == 0
    assert result.subscription_ids == ("status-stale",)
    assert fake_session.posts == []


def test_create_product_a_subscription_matches_normalized_expiration_instant() -> None:
    existing = product_a_get_subscription(
        expires="2030-01-01T00:00:00.000Z",
    )
    fake_session = FakeSession(
        get_responses=[FakeResponse(200, json_body=[existing])],
    )

    result = create_product_a_sth_subscription(
        settings=settings(
            dry_run=False,
            expires="2030-01-01T09:00:00+09:00",
        ),
        auth=FakeAuth(),
        session=fake_session,
    )

    assert result.skipped == 1
    assert result.failed == 0
    assert fake_session.posts == []


def test_create_product_a_subscription_rejects_different_expiration_instant() -> None:
    stale = product_a_get_subscription(
        subscription_id="wrong-expiry",
        expires="2030-01-02T00:00:00Z",
    )
    fake_session = FakeSession(
        get_responses=[FakeResponse(200, json_body=[stale])],
    )

    result = create_product_a_sth_subscription(
        settings=settings(
            dry_run=False,
            expires="2030-01-01T09:00:00+09:00",
        ),
        auth=FakeAuth(),
        session=fake_session,
    )

    assert result.failed == 1
    assert result.subscription_ids == ("wrong-expiry",)
    assert fake_session.posts == []


def test_create_product_a_permanent_subscription_matches_without_live_expires() -> None:
    existing = product_a_get_subscription(expires=None)
    fake_session = FakeSession(
        get_responses=[FakeResponse(200, json_body=[existing])],
    )

    result = create_product_a_sth_subscription(
        settings=settings(dry_run=False, expires=""),
        auth=FakeAuth(),
        session=fake_session,
    )

    assert "expires" not in existing
    assert result.skipped == 1
    assert result.failed == 0
    assert fake_session.posts == []


def test_create_product_a_permanent_config_rejects_live_expiring_subscription() -> None:
    stale = product_a_get_subscription(
        subscription_id="unexpected-expiry",
        expires="2030-01-01T00:00:00Z",
    )
    fake_session = FakeSession(
        get_responses=[FakeResponse(200, json_body=[stale])],
    )

    result = create_product_a_sth_subscription(
        settings=settings(dry_run=False, expires=""),
        auth=FakeAuth(),
        session=fake_session,
    )

    assert result.failed == 1
    assert result.subscription_ids == ("unexpected-expiry",)
    assert fake_session.posts == []


def test_create_product_a_subscription_finds_exact_match_on_later_page() -> None:
    later_match = product_a_get_subscription(
        subscription_id="later-page-match",
        expires=None,
    )
    fake_session = PaginatedInventorySession([later_match])

    result = create_product_a_sth_subscription(
        settings=settings(dry_run=False, expires=""),
        auth=FakeAuth(),
        session=fake_session,
    )

    assert result.skipped == 1
    assert result.subscription_ids == ("later-page-match",)
    assert fake_session.posts == []
    assert len(fake_session.gets) == 2
    first_params = fake_session.gets[0]["params"]
    second_params = fake_session.gets[1]["params"]
    assert first_params["limit"] > 0
    assert first_params["offset"] == 0
    assert first_params["options"] == "count"
    assert second_params == {
        "limit": first_params["limit"],
        "offset": first_params["limit"],
        "options": "count",
    }


def test_create_product_a_subscription_detects_unsafe_peer_on_later_page() -> None:
    unsafe_peer = product_b_subscription(
        subscription_id="later-unsafe-peer",
        entity_id="jp.sendai.Blesensor.per3600.10",
        entity_type="Blesensor.per3600",
    )
    unsafe_peer["subject"]["condition"]["attrs"] = ["peopleOccupancy_near"]
    fake_session = PaginatedInventorySession([unsafe_peer])

    result = create_product_a_sth_subscription(
        settings=settings(dry_run=False),
        auth=FakeAuth(),
        session=fake_session,
    )

    assert result.failed == 1
    assert fake_session.posts == []
    assert len(fake_session.gets) == 2


@pytest.mark.parametrize(
    ("second_total_delta", "second_page"),
    [
        pytest.param(0, [], id="incomplete"),
        pytest.param(1, [unrelated_subscription(0)], id="inconsistent-total"),
    ],
)
def test_create_product_a_subscription_fails_closed_for_incomplete_inventory(
    second_total_delta: int,
    second_page: list[dict[str, Any]],
) -> None:
    fake_session = PaginatedInventorySession(
        second_page,
        second_total_delta=second_total_delta,
    )

    result = create_product_a_sth_subscription(
        settings=settings(dry_run=False),
        auth=FakeAuth(),
        session=fake_session,
    )

    assert result.failed == 1
    assert result.created == 0
    assert result.skipped == 0
    assert fake_session.posts == []


def test_create_product_a_subscription_fails_closed_without_total_count() -> None:
    fake_session = FakeSession(
        post_responses=[
            FakeResponse(201, headers={"Location": "/subscriptions/unexpected"}),
        ],
        get_responses=[
            FakeResponse(
                200,
                json_body=[],
                infer_total_count=False,
            )
        ],
    )

    result = create_product_a_sth_subscription(
        settings=settings(dry_run=False),
        auth=FakeAuth(),
        session=fake_session,
    )

    assert result.failed == 1
    assert result.created == 0
    assert fake_session.posts == []


def test_create_product_a_subscription_fails_closed_for_unparseable_total_count() -> (
    None
):
    fake_session = FakeSession(
        post_responses=[
            FakeResponse(201, headers={"Location": "/subscriptions/unexpected"}),
        ],
        get_responses=[
            FakeResponse(
                200,
                headers={"Fiware-Total-Count": "not-an-integer"},
                json_body=[],
            )
        ],
    )

    result = create_product_a_sth_subscription(
        settings=settings(dry_run=False),
        auth=FakeAuth(),
        session=fake_session,
    )

    assert result.failed == 1
    assert result.created == 0
    assert fake_session.posts == []


def test_create_product_a_subscription_retries_post_once_on_unauthorized() -> None:
    fake_auth = FakeAuth()
    fake_session = FakeSession(
        post_responses=[
            FakeResponse(401, text="expired"),
            FakeResponse(201, headers={"Location": "/subscriptions/sub-2"}),
        ],
        get_responses=[FakeResponse(200, json_body=[])],
    )

    result = create_product_a_sth_subscription(
        settings=settings(dry_run=False),
        auth=fake_auth,
        session=fake_session,
    )

    assert result.created == 1
    assert len(fake_session.posts) == 2
    assert fake_auth.force_refreshes == [False, False, True]
    assert fake_session.posts[1]["headers"]["Authorization"] == "Bearer token-refreshed"


def test_create_product_a_subscription_retries_preflight_once_on_unauthorized() -> None:
    fake_auth = FakeAuth()
    fake_session = FakeSession(
        post_responses=[
            FakeResponse(201, headers={"Location": "/subscriptions/sub-3"}),
        ],
        get_responses=[
            FakeResponse(401, text="expired"),
            FakeResponse(200, json_body=[]),
        ],
    )

    result = create_product_a_sth_subscription(
        settings=settings(dry_run=False),
        auth=fake_auth,
        session=fake_session,
    )

    assert result.created == 1
    assert len(fake_session.gets) == 2
    assert fake_auth.force_refreshes == [False, True, False]


def test_create_product_a_subscription_reports_preflight_failure() -> None:
    fake_session = FakeSession(get_responses=[FakeResponse(500, text="gateway down")])

    result = create_product_a_sth_subscription(
        settings=settings(dry_run=False),
        auth=FakeAuth(),
        session=fake_session,
    )

    assert result.failed == 1
    assert fake_session.posts == []


def test_create_product_a_subscription_reports_failure_without_secret_url(
    caplog: pytest.LogCaptureFixture,
) -> None:
    fake_session = FakeSession(
        post_responses=[FakeResponse(400, text="bad subscription body")],
        get_responses=[FakeResponse(200, json_body=[])],
    )

    result = create_product_a_sth_subscription(
        settings=settings(dry_run=False),
        auth=FakeAuth(),
        session=fake_session,
    )

    assert result.failed == 1
    messages = [record.getMessage() for record in caplog.records]
    assert messages == ["Product A STH subscription creation failed"]
    assert "internal-comet" not in messages[0]


def test_build_product_b_subscription_uses_exact_aggregate_shape() -> None:
    body = build_product_b_subscription_body(
        settings(
            product_b_aggregate_entity_id=AGGREGATE_ENTITY_ID,
            product_b_aggregate_entity_type=AGGREGATE_ENTITY_TYPE,
        ),
        now=datetime(2026, 5, 25, 12, 30, tzinfo=JST),
    )

    assert body == {
        "description": (
            "Product B aggregate STH-Comet history set at 2026-05-25T12:30:00+09:00"
        ),
        "subject": {
            "entities": [{"id": AGGREGATE_ENTITY_ID, "type": AGGREGATE_ENTITY_TYPE}],
            "condition": {
                "attrs": ["dateRetrieved"],
                "notifyOnMetadataChange": True,
            },
        },
        "notification": {
            "http": {"url": COMET_NOTIFY_URL},
            "attrsFormat": "legacy",
            "metadata": ["TimeInstant"],
        },
    }


def test_redacted_product_b_subscription_json_omits_private_notify_url() -> None:
    rendered = redacted_product_b_subscription_json(
        settings(
            product_b_aggregate_entity_id=AGGREGATE_ENTITY_ID,
            product_b_aggregate_entity_type=AGGREGATE_ENTITY_TYPE,
        )
    )

    assert "internal-comet.example" not in rendered
    assert "<COMET_NOTIFY_URL>" in rendered
    assert '"Product B aggregate STH-Comet history' in rendered
    assert f'"id": "{AGGREGATE_ENTITY_ID}"' in rendered
    assert '"idPattern"' not in rendered
    assert "attrs" not in json.loads(rendered)["notification"]
    assert '"throttling"' not in rendered


def test_redacted_subscription_json_remains_product_a_for_back_compat() -> None:
    legacy = json.loads(redacted_subscription_json(settings()))
    direct = json.loads(redacted_product_a_subscription_json(settings()))
    # description carries datetime.now(); strip it before comparing.
    legacy.pop("description")
    direct.pop("description")
    assert legacy == direct


def test_create_product_b_subscription_dry_run_does_not_post(
    caplog: pytest.LogCaptureFixture,
) -> None:
    fake_session = FakeSession()

    with caplog.at_level("INFO", logger="sendai_pipeline"):
        result = create_product_b_sth_subscription(
            settings=settings(dry_run=True),
            auth=None,
            session=fake_session,
        )

    assert result.would_create == 1
    assert result.created == 0
    assert result.skipped == 0
    assert result.failed == 0
    assert fake_session.gets == []
    assert fake_session.posts == []
    messages = [record.getMessage() for record in caplog.records]
    assert messages == ["dry-run: would create Product B STH subscription"]


def test_create_product_b_subscription_posts_body_and_skip_initial_param() -> None:
    fake_session = FakeSession(
        post_responses=[
            FakeResponse(201, headers={"Location": "/orion/v2.0/subscriptions/sub-b"})
        ],
        get_responses=[FakeResponse(200, json_body=[])],
    )

    result = create_product_b_sth_subscription(
        settings=settings(
            dry_run=False,
            product_b_aggregate_entity_id=AGGREGATE_ENTITY_ID,
            product_b_aggregate_entity_type=AGGREGATE_ENTITY_TYPE,
        ),
        auth=FakeAuth(),
        session=fake_session,
    )

    assert result.created == 1
    assert result.subscription_ids == ("sub-b",)
    [post] = fake_session.posts
    body = json.loads(post["data"])
    assert body["subject"] == {
        "entities": [{"id": AGGREGATE_ENTITY_ID, "type": AGGREGATE_ENTITY_TYPE}],
        "condition": {
            "attrs": ["dateRetrieved"],
            "notifyOnMetadataChange": True,
        },
    }
    assert body["notification"] == {
        "http": {"url": COMET_NOTIFY_URL},
        "attrsFormat": "legacy",
        "metadata": ["TimeInstant"],
    }
    assert body["description"].startswith("Product B aggregate STH-Comet history")
    assert "throttling" not in body


def test_create_product_b_creation_unaffected_by_valid_product_a_subscription() -> None:
    existing_product_a = product_a_get_subscription(
        subscription_id="existing-product-a",
        expires=None,
    )
    fake_session = FakeSession(
        post_responses=[
            FakeResponse(201, headers={"Location": "/orion/v2.0/subscriptions/sub-b"}),
        ],
        get_responses=[FakeResponse(200, json_body=[existing_product_a])],
    )

    result = create_product_b_sth_subscription(
        settings=settings(dry_run=False),
        auth=FakeAuth(),
        session=fake_session,
    )

    assert result.created == 1
    assert result.subscription_ids == ("sub-b",)


def test_create_product_b_unaffected_by_product_a_peer_with_fails_counter() -> None:
    # The peer guard must recognize an exact Product A peer even when it carries
    # notification.failsCounter, so it can prove the selectors are disjoint and
    # not falsely abort Product B creation on the shared dateRetrieved trigger.
    existing_product_a = product_a_get_subscription(
        subscription_id="existing-product-a",
        expires=None,
    )
    existing_product_a["notification"]["failsCounter"] = 9
    fake_session = FakeSession(
        post_responses=[
            FakeResponse(201, headers={"Location": "/orion/v2.0/subscriptions/sub-b"}),
        ],
        get_responses=[FakeResponse(200, json_body=[existing_product_a])],
    )

    result = create_product_b_sth_subscription(
        settings=settings(dry_run=False),
        auth=FakeAuth(),
        session=fake_session,
    )

    assert result.created == 1
    assert result.subscription_ids == ("sub-b",)


def test_create_product_b_ignores_disjoint_product_a_with_notification_drift() -> None:
    existing_product_a = product_a_get_subscription(
        subscription_id="existing-product-a",
        expires=None,
    )
    existing_product_a["notification"]["covered"] = True
    fake_session = FakeSession(
        post_responses=[
            FakeResponse(201, headers={"Location": "/orion/v2.0/subscriptions/sub-b"}),
        ],
        get_responses=[FakeResponse(200, json_body=[existing_product_a])],
    )

    result = create_product_b_sth_subscription(
        settings=settings(dry_run=False),
        auth=FakeAuth(),
        session=fake_session,
    )

    assert result.created == 1
    assert result.subscription_ids == ("sub-b",)


def test_create_product_a_ignores_legacy_product_b_subscription() -> None:
    fake_session = FakeSession(
        post_responses=[
            FakeResponse(201, headers={"Location": "/orion/v2.0/subscriptions/sub-a"}),
        ],
        get_responses=[
            FakeResponse(
                200,
                json_body=[
                    {
                        "id": "existing-product-b",
                        "description": "Product B STH-Comet history set at ...",
                        "subject": {
                            "entities": [
                                {"idPattern": ".*", "type": "Blesensor.per300"},
                                {"idPattern": ".*", "type": "Blesensor.per3600"},
                            ],
                            "condition": {
                                "attrs": ["peopleCount_flow"],
                                "notifyOnMetadataChange": True,
                            },
                        },
                        "notification": {
                            "attrsFormat": "legacy",
                            "attrs": [
                                "dateObservedFrom",
                                "dateObservedTo",
                                "peopleCount_flow",
                            ],
                            "metadata": ["TimeInstant"],
                        },
                    }
                ],
            )
        ],
    )

    result = create_product_a_sth_subscription(
        settings=settings(dry_run=False),
        auth=FakeAuth(),
        session=fake_session,
    )

    assert result.created == 1
    assert result.subscription_ids == ("sub-a",)


def test_create_product_a_ignores_exact_product_b_despite_shared_date_retrieved() -> (
    None
):
    existing_product_b = product_b_subscription()
    fake_session = FakeSession(
        post_responses=[
            FakeResponse(201, headers={"Location": "/orion/v2.0/subscriptions/sub-a"}),
        ],
        get_responses=[FakeResponse(200, json_body=[existing_product_b])],
    )

    result = create_product_a_sth_subscription(
        settings=settings(
            dry_run=False,
            product_b_aggregate_entity_id=AGGREGATE_ENTITY_ID,
            product_b_aggregate_entity_type=AGGREGATE_ENTITY_TYPE,
        ),
        auth=FakeAuth(),
        session=fake_session,
    )

    assert result.created == 1
    assert result.subscription_ids == ("sub-a",)
    assert existing_product_b["subject"]["condition"]["attrs"] == ["dateRetrieved"]
    assert "dateRetrieved" in PRODUCT_A_ATTRS


def test_create_product_a_ignores_disjoint_product_b_with_notification_drift() -> None:
    existing_product_b = product_b_subscription()
    existing_product_b["notification"]["onlyChangedAttrs"] = True
    fake_session = FakeSession(
        post_responses=[
            FakeResponse(201, headers={"Location": "/orion/v2.0/subscriptions/sub-a"})
        ],
        get_responses=[FakeResponse(200, json_body=[existing_product_b])],
    )

    result = create_product_a_sth_subscription(
        settings=settings(
            dry_run=False,
            product_b_aggregate_entity_id=AGGREGATE_ENTITY_ID,
            product_b_aggregate_entity_type=AGGREGATE_ENTITY_TYPE,
        ),
        auth=FakeAuth(),
        session=fake_session,
    )

    assert result.created == 1
    assert result.subscription_ids == ("sub-a",)


def test_create_product_b_subscription_skips_complete_shape_with_exact_url() -> None:
    result, fake_session = create_product_b_with_existing(product_b_subscription())

    assert result.skipped == 1
    assert result.subscription_ids == ("aggregate-sub",)
    assert fake_session.posts == []


def test_create_product_b_subscription_skips_when_notification_attrs_omitted() -> None:
    existing = product_b_subscription()
    existing["notification"].pop("attrs")

    result, fake_session = create_product_b_with_existing(existing)

    assert result.skipped == 1
    assert result.subscription_ids == ("aggregate-sub",)
    assert fake_session.posts == []


def test_create_product_b_subscription_skips_when_false_defaults_are_omitted() -> None:
    existing = product_b_subscription()
    existing["notification"].pop("onlyChangedAttrs")
    existing["notification"].pop("covered")

    result, fake_session = create_product_b_with_existing(existing)

    assert result.skipped == 1
    assert result.subscription_ids == ("aggregate-sub",)
    assert fake_session.posts == []


@pytest.mark.parametrize("field", ["onlyChangedAttrs", "covered"])
def test_create_product_b_subscription_creates_when_notification_default_enabled(
    field: str,
) -> None:
    existing = product_b_subscription()
    existing["notification"][field] = True

    result, fake_session = create_product_b_with_existing(existing)

    assert result.created == 1
    assert result.skipped == 0
    assert result.subscription_ids == ("correct-sub",)
    assert len(fake_session.posts) == 1


@pytest.mark.parametrize(
    "missing_path",
    [
        pytest.param(("description",), id="description"),
        pytest.param(("subject",), id="subject"),
        pytest.param(("subject", "entities"), id="entities"),
        pytest.param(("subject", "entities", 0, "id"), id="entity-id"),
        pytest.param(("subject", "entities", 0, "type"), id="entity-type"),
        pytest.param(("subject", "condition"), id="condition"),
        pytest.param(("subject", "condition", "attrs"), id="condition-attrs"),
        pytest.param(
            ("subject", "condition", "notifyOnMetadataChange"),
            id="metadata-change",
        ),
        pytest.param(("notification",), id="notification"),
        pytest.param(("notification", "http"), id="http"),
        pytest.param(("notification", "http", "url"), id="notification-url"),
        pytest.param(("notification", "attrsFormat"), id="format"),
        pytest.param(("notification", "metadata"), id="metadata"),
    ],
)
def test_create_product_b_subscription_creates_when_required_field_missing(
    missing_path: tuple[str | int, ...],
) -> None:
    existing = product_b_subscription()
    remove_nested_field(existing, missing_path)
    result, _fake_session = create_product_b_with_existing(existing)

    assert result.created == 1
    assert result.skipped == 0
    assert result.subscription_ids == ("correct-sub",)


@pytest.mark.parametrize(
    "extra_attrs",
    [
        pytest.param(["dateRetrieved", "peopleCount_flow_1"], id="trigger-attrs"),
        pytest.param(["dateRetrieved", "dateObservedFrom"], id="trigger-scalar"),
    ],
)
def test_create_product_b_subscription_creates_with_extra_trigger_attrs(
    extra_attrs: list[str],
) -> None:
    existing = product_b_subscription()
    existing["subject"]["condition"]["attrs"] = extra_attrs
    result, _fake_session = create_product_b_with_existing(existing)

    assert result.created == 1
    assert result.skipped == 0


@pytest.mark.parametrize(
    "entities",
    [
        pytest.param(
            [{"id": "wrong.aggregate.entity", "type": AGGREGATE_ENTITY_TYPE}],
            id="wrong-id",
        ),
        pytest.param(
            [{"id": AGGREGATE_ENTITY_ID, "type": "Wrong.aggregate.type"}],
            id="wrong-type",
        ),
        pytest.param(
            [
                {
                    "id": AGGREGATE_ENTITY_ID,
                    "idPattern": ".*",
                    "type": AGGREGATE_ENTITY_TYPE,
                }
            ],
            id="id-pattern",
        ),
        pytest.param(
            [
                {"id": AGGREGATE_ENTITY_ID, "type": AGGREGATE_ENTITY_TYPE},
                {"id": "extra.aggregate.entity", "type": AGGREGATE_ENTITY_TYPE},
            ],
            id="extra-entity",
        ),
    ],
)
def test_create_product_b_subscription_creates_with_non_exact_entity_selector(
    entities: list[dict[str, str]],
) -> None:
    existing = product_b_subscription()
    existing["subject"]["entities"] = entities
    result, _fake_session = create_product_b_with_existing(existing)

    assert result.created == 1
    assert result.skipped == 0


def test_create_product_b_subscription_creates_when_notification_attrs_present() -> (
    None
):
    existing = product_b_subscription()
    existing["notification"]["attrs"] = ["dateRetrieved"]
    result, _fake_session = create_product_b_with_existing(existing)

    assert result.created == 1
    assert result.skipped == 0


def test_create_product_b_subscription_creates_with_wrong_attrs_format() -> None:
    existing = product_b_subscription()
    existing["notification"]["attrsFormat"] = "normalized"

    result, _fake_session = create_product_b_with_existing(existing)

    assert result.created == 1
    assert result.skipped == 0


def test_create_product_b_subscription_creates_with_metadata_change_disabled() -> None:
    existing = product_b_subscription()
    existing["subject"]["condition"]["notifyOnMetadataChange"] = False

    result, _fake_session = create_product_b_with_existing(existing)

    assert result.created == 1
    assert result.skipped == 0


def test_create_product_b_subscription_creates_with_extra_notification_metadata() -> (
    None
):
    existing = product_b_subscription()
    existing["notification"]["metadata"] = ["TimeInstant", "unitCode"]
    result, _fake_session = create_product_b_with_existing(existing)

    assert result.created == 1
    assert result.skipped == 0


def test_create_product_b_subscription_creates_with_wrong_notification_url() -> None:
    existing = product_b_subscription(notify_url="http://stale-comet.example/notify")
    result, _fake_session = create_product_b_with_existing(existing)

    assert result.created == 1
    assert result.skipped == 0


def test_create_product_b_subscription_creates_for_old_prefix_and_shape() -> None:
    old_subscription = {
        "id": "old-product-b",
        "description": "Product B STH-Comet history set at earlier",
        "subject": {
            "entities": [
                {"idPattern": ".*", "type": "Blesensor.per300"},
                {"idPattern": ".*", "type": "Blesensor.per3600"},
            ],
            "condition": {
                "attrs": ["peopleCount_flow"],
                "notifyOnMetadataChange": True,
            },
        },
        "notification": {
            "http": {"url": COMET_NOTIFY_URL},
            "attrsFormat": "legacy",
            "attrs": [
                "dateObservedFrom",
                "dateObservedTo",
                "peopleCount_flow",
            ],
            "metadata": ["TimeInstant"],
        },
    }
    result, _fake_session = create_product_b_with_existing(old_subscription)

    assert result.created == 1
    assert result.skipped == 0
    assert result.subscription_ids == ("correct-sub",)


def test_create_product_b_subscription_creates_with_old_prefix_on_new_shape() -> None:
    existing = product_b_subscription(
        description="Product B STH-Comet history set at earlier"
    )

    result, _fake_session = create_product_b_with_existing(existing)

    assert result.created == 1
    assert result.skipped == 0
    assert result.subscription_ids == ("correct-sub",)


def test_create_product_b_aborts_when_product_a_uses_stale_shared_trigger(
    caplog: pytest.LogCaptureFixture,
) -> None:
    fake_session = FakeSession(
        get_responses=[
            FakeResponse(
                200,
                json_body=[
                    {
                        "id": "stale-product-a",
                        "description": "Product A STH-Comet history set at ...",
                        "subject": {
                            "entities": [
                                {
                                    "idPattern": ".*",
                                    "type": AGGREGATE_ENTITY_TYPE,
                                }
                            ],
                            "condition": {"attrs": ["identifcation"]},
                        },
                        "notification": {
                            "attrsFormat": "legacy",
                            "attrs": list(PRODUCT_A_HISTORY_ATTRS),
                            "metadata": ["TimeInstant"],
                            "notifyOnMetadataChange": True,
                        },
                    }
                ],
            )
        ],
    )

    with caplog.at_level("ERROR", logger="sendai_pipeline"):
        result = create_product_b_sth_subscription(
            settings=settings(
                dry_run=False,
                product_b_aggregate_entity_id=AGGREGATE_ENTITY_ID,
                product_b_aggregate_entity_type=AGGREGATE_ENTITY_TYPE,
            ),
            auth=FakeAuth(),
            session=fake_session,
        )

    assert result.failed == 1
    assert result.created == 0
    assert fake_session.posts == []
    [record] = caplog.records
    assert getattr(record, "event", None) == "sth_subscription_failed"
    assert getattr(record, "peer_product", None) == "Product A"
    assert getattr(record, "subscription_id", None) == "stale-product-a"


def test_create_aborts_on_stale_peer_even_when_own_subscription_exists() -> None:
    fake_session = FakeSession(
        get_responses=[
            FakeResponse(
                200,
                json_body=[
                    product_b_subscription(subscription_id="current-product-b"),
                    {
                        "id": "stale-product-a",
                        "description": "Product A STH-Comet history set at ...",
                        "subject": {
                            "entities": [
                                {
                                    "idPattern": ".*",
                                    "type": AGGREGATE_ENTITY_TYPE,
                                }
                            ],
                            "condition": {"attrs": ["dateObservedFrom"]},
                        },
                        "notification": {
                            "attrsFormat": "legacy",
                            "attrs": list(PRODUCT_A_HISTORY_ATTRS),
                            "metadata": ["TimeInstant"],
                            "notifyOnMetadataChange": True,
                        },
                    },
                ],
            )
        ],
    )

    result = create_product_b_sth_subscription(
        settings=settings(
            dry_run=False,
            product_b_aggregate_entity_id=AGGREGATE_ENTITY_ID,
            product_b_aggregate_entity_type=AGGREGATE_ENTITY_TYPE,
        ),
        auth=FakeAuth(),
        session=fake_session,
    )

    assert result.failed == 1
    assert result.skipped == 0
    assert fake_session.posts == []


def test_create_product_a_aborts_when_current_product_b_uses_shared_trigger() -> None:
    fake_session = FakeSession(
        get_responses=[
            FakeResponse(
                200,
                json_body=[
                    {
                        "id": "stale-product-b",
                        "description": (
                            "Product B aggregate STH-Comet history set at ..."
                        ),
                        "subject": {
                            "entities": [
                                {
                                    "id": AGGREGATE_ENTITY_ID,
                                    "type": AGGREGATE_ENTITY_TYPE,
                                }
                            ],
                            "condition": {"attrs": ["dateObservedFrom"]},
                        },
                        "notification": {
                            "attrsFormat": "legacy",
                            "metadata": ["TimeInstant"],
                            "notifyOnMetadataChange": True,
                        },
                    }
                ],
            )
        ],
    )

    result = create_product_a_sth_subscription(
        settings=settings(dry_run=False),
        auth=FakeAuth(),
        session=fake_session,
    )

    assert result.failed == 1
    assert result.created == 0
    assert fake_session.posts == []


def test_create_aborts_when_peer_subscription_has_no_trigger_attrs() -> None:
    fake_session = FakeSession(
        get_responses=[
            FakeResponse(
                200,
                json_body=[
                    {
                        "id": "broad-product-a",
                        "description": "Product A STH-Comet history set at ...",
                        "subject": {
                            "entities": [
                                {
                                    "idPattern": ".*",
                                    "type": AGGREGATE_ENTITY_TYPE,
                                }
                            ],
                            "condition": {},
                        },
                        "notification": {
                            "attrsFormat": "legacy",
                            "attrs": list(PRODUCT_A_HISTORY_ATTRS),
                            "metadata": ["TimeInstant"],
                        },
                    }
                ],
            )
        ],
    )

    result = create_product_b_sth_subscription(
        settings=settings(
            dry_run=False,
            product_b_aggregate_entity_id=AGGREGATE_ENTITY_ID,
            product_b_aggregate_entity_type=AGGREGATE_ENTITY_TYPE,
        ),
        auth=FakeAuth(),
        session=fake_session,
    )

    assert result.failed == 1
    assert result.created == 0
    assert fake_session.posts == []


def test_product_b_trigger_shares_date_retrieved_with_product_a_by_design() -> None:
    body = build_product_b_subscription_body(settings())

    assert PRODUCT_B_STABLE_WRITE_ATTRS.count("sourceQuality") == 1
    assert PRODUCT_B_STABLE_WRITE_ATTRS.index(
        "sourceQuality"
    ) > PRODUCT_B_STABLE_WRITE_ATTRS.index("identifcation")
    trigger_attrs = body["subject"]["condition"]["attrs"]
    assert trigger_attrs == ["dateRetrieved"]
    assert "attrs" not in body["notification"]

    # Product A triggers on and projects dateRetrieved, so the two products
    # intentionally overlap on that attribute. Cross-product safety comes from
    # disjoint entity selectors, not attribute disjointness — see
    # test_create_product_a_ignores_exact_product_b_despite_shared_date_retrieved
    # and test_create_product_b_creation_unaffected_by_valid_product_a_subscription.
    assert "dateRetrieved" in PRODUCT_A_TRIGGER_ATTRS
    assert "dateRetrieved" in PRODUCT_A_HISTORY_ATTRS
    assert set(trigger_attrs) <= set(PRODUCT_A_HISTORY_ATTRS)


def test_entity_selector_pair_treats_typeless_selector_as_may_overlap() -> None:
    assert _entity_selector_pair_may_overlap(
        {"id": AGGREGATE_ENTITY_ID},
        {"idPattern": ".*", "type": "Blesensor.per300"},
    )


def test_sth_subscription_logging_extras_are_allowed() -> None:
    required = {
        "subscription_id",
        "count_would_create",
        "count_created",
        "count_skipped",
        "count_failed",
    }
    assert required <= _ALLOWED_EXTRA_KEYS


SUB_ID = "65e87f5c20bd0c390e057c62"


def test_get_subscription_returns_parsed_body_on_200() -> None:
    fake_session = FakeSession(
        get_responses=[
            FakeResponse(
                200,
                json_body={
                    "id": SUB_ID,
                    "description": "Setting for jp.sendai.Blesensor",
                },
            )
        ]
    )

    result = get_subscription(
        SUB_ID, settings=settings(), auth=FakeAuth(), session=fake_session
    )

    assert result == {"id": SUB_ID, "description": "Setting for jp.sendai.Blesensor"}
    assert fake_session.gets[0]["url"].endswith(f"/orion/v2.0/subscriptions/{SUB_ID}")


def test_get_subscription_returns_none_on_404() -> None:
    fake_session = FakeSession(get_responses=[FakeResponse(404, text="not found")])

    result = get_subscription(
        SUB_ID, settings=settings(), auth=FakeAuth(), session=fake_session
    )

    assert result is None


def test_get_subscription_retries_once_on_unauthorized() -> None:
    fake_auth = FakeAuth()
    fake_session = FakeSession(
        get_responses=[
            FakeResponse(401, text="expired"),
            FakeResponse(200, json_body={"id": SUB_ID, "description": "x"}),
        ]
    )

    result = get_subscription(
        SUB_ID, settings=settings(), auth=fake_auth, session=fake_session
    )

    assert result is not None
    assert fake_auth.force_refreshes == [False, True]


def test_get_subscription_raises_on_unexpected_status() -> None:
    fake_session = FakeSession(get_responses=[FakeResponse(500, text="boom")])

    with pytest.raises(requests.HTTPError):
        get_subscription(
            SUB_ID, settings=settings(), auth=FakeAuth(), session=fake_session
        )


def test_delete_subscription_returns_204_on_success() -> None:
    fake_session = FakeSession(delete_responses=[FakeResponse(204)])

    status = delete_subscription(
        SUB_ID, settings=settings(), auth=FakeAuth(), session=fake_session
    )

    assert status == 204
    assert fake_session.deletes[0]["url"].endswith(
        f"/orion/v2.0/subscriptions/{SUB_ID}"
    )


def test_delete_subscription_returns_404_when_already_absent() -> None:
    fake_session = FakeSession(delete_responses=[FakeResponse(404, text="gone")])

    status = delete_subscription(
        SUB_ID, settings=settings(), auth=FakeAuth(), session=fake_session
    )

    assert status == 404


def test_delete_subscription_retries_once_on_unauthorized() -> None:
    fake_auth = FakeAuth()
    fake_session = FakeSession(
        delete_responses=[FakeResponse(401, text="expired"), FakeResponse(204)]
    )

    status = delete_subscription(
        SUB_ID, settings=settings(), auth=fake_auth, session=fake_session
    )

    assert status == 204
    assert fake_auth.force_refreshes == [False, True]


def test_delete_subscription_raises_on_unexpected_status() -> None:
    fake_session = FakeSession(delete_responses=[FakeResponse(500, text="boom")])

    with pytest.raises(requests.HTTPError):
        delete_subscription(
            SUB_ID, settings=settings(), auth=FakeAuth(), session=fake_session
        )


@pytest.mark.parametrize(
    "malformed_entry",
    [
        pytest.param("not-an-object", id="non-dict"),
        pytest.param({}, id="missing-id"),
        pytest.param({"id": ""}, id="empty-id"),
    ],
)
def test_fetch_subscription_inventory_rejects_malformed_page_entry(
    malformed_entry: Any,
) -> None:
    fetch_subscription_inventory, inventory_error = _public_inventory_api()
    fake_session = FakeSession(
        get_responses=[FakeResponse(200, json_body=[malformed_entry])]
    )

    with pytest.raises(inventory_error):
        fetch_subscription_inventory(
            settings=settings(),
            auth=FakeAuth(),
            session=fake_session,
        )


def test_fetch_subscription_inventory_rejects_repeated_id_across_pages() -> None:
    fetch_subscription_inventory, inventory_error = _public_inventory_api()
    fake_session = PaginatedInventorySession([unrelated_subscription(0)])

    with pytest.raises(inventory_error):
        fetch_subscription_inventory(
            settings=settings(),
            auth=FakeAuth(),
            session=fake_session,
        )


def test_fetch_subscription_inventory_rejects_invalid_json_page() -> None:
    fetch_subscription_inventory, inventory_error = _public_inventory_api()

    class InvalidJsonResponse(FakeResponse):
        def json(self) -> Any:
            raise ValueError("invalid JSON")

    fake_session = FakeSession(
        get_responses=[
            InvalidJsonResponse(
                200,
                headers={"Fiware-Total-Count": "0"},
                infer_total_count=False,
            )
        ]
    )

    with pytest.raises(inventory_error):
        fetch_subscription_inventory(
            settings=settings(),
            auth=FakeAuth(),
            session=fake_session,
        )


def test_fetch_subscription_inventory_rejects_non_list_page() -> None:
    fetch_subscription_inventory, inventory_error = _public_inventory_api()
    fake_session = FakeSession(
        get_responses=[
            FakeResponse(
                200,
                headers={"Fiware-Total-Count": "1"},
                json_body={"id": SUB_ID},
            )
        ]
    )

    with pytest.raises(inventory_error):
        fetch_subscription_inventory(
            settings=settings(),
            auth=FakeAuth(),
            session=fake_session,
        )


@pytest.mark.parametrize(
    "total_header",
    [
        pytest.param(None, id="missing"),
        pytest.param("not-an-integer", id="unparseable"),
    ],
)
def test_fetch_subscription_inventory_rejects_missing_or_unparseable_total_count(
    total_header: str | None,
) -> None:
    fetch_subscription_inventory, inventory_error = _public_inventory_api()
    headers = {} if total_header is None else {"Fiware-Total-Count": total_header}
    fake_session = FakeSession(
        get_responses=[
            FakeResponse(
                200,
                headers=headers,
                json_body=[],
                infer_total_count=False,
            )
        ]
    )

    with pytest.raises(inventory_error):
        fetch_subscription_inventory(
            settings=settings(),
            auth=FakeAuth(),
            session=fake_session,
        )


def test_fetch_subscription_inventory_rejects_total_change_between_pages() -> None:
    fetch_subscription_inventory, inventory_error = _public_inventory_api()
    fake_session = PaginatedInventorySession(
        [unrelated_subscription(100)],
        second_total_delta=1,
    )

    with pytest.raises(inventory_error):
        fetch_subscription_inventory(
            settings=settings(),
            auth=FakeAuth(),
            session=fake_session,
        )


def test_fetch_subscription_inventory_rejects_incomplete_page() -> None:
    fetch_subscription_inventory, inventory_error = _public_inventory_api()
    fake_session = FakeSession(
        get_responses=[
            FakeResponse(
                200,
                headers={"Fiware-Total-Count": "2"},
                json_body=[unrelated_subscription(0)],
            )
        ]
    )

    with pytest.raises(inventory_error):
        fetch_subscription_inventory(
            settings=settings(),
            auth=FakeAuth(),
            session=fake_session,
        )


def test_fetch_subscription_inventory_refreshes_unauthorized_token_per_page() -> None:
    fetch_subscription_inventory, _inventory_error = _public_inventory_api()
    first_page = [unrelated_subscription(index) for index in range(100)]
    second_page = [unrelated_subscription(100)]
    fake_auth = FakeAuth()
    fake_session = FakeSession(
        get_responses=[
            FakeResponse(401, text="expired", infer_total_count=False),
            FakeResponse(
                200,
                headers={"Fiware-Total-Count": "101"},
                json_body=first_page,
            ),
            FakeResponse(401, text="expired", infer_total_count=False),
            FakeResponse(
                200,
                headers={"Fiware-Total-Count": "101"},
                json_body=second_page,
            ),
        ]
    )

    result = fetch_subscription_inventory(
        settings=settings(),
        auth=fake_auth,
        session=fake_session,
    )

    assert result == [*first_page, *second_page]
    assert fake_auth.force_refreshes == [False, True, False, True]
    assert [call["params"]["offset"] for call in fake_session.gets] == [
        0,
        0,
        100,
        100,
    ]


@pytest.mark.parametrize(
    "transport_error",
    [
        pytest.param(requests.ConnectionError("connection failed"), id="connection"),
        pytest.param(requests.Timeout("request timed out"), id="timeout"),
    ],
)
def test_fetch_subscription_inventory_wraps_transport_error(
    transport_error: requests.RequestException,
) -> None:
    fetch_subscription_inventory, inventory_error = _public_inventory_api()
    fake_session = FakeSession(get_responses=[transport_error])

    with pytest.raises(inventory_error) as exc_info:
        fetch_subscription_inventory(
            settings=settings(),
            auth=FakeAuth(),
            session=fake_session,
        )

    assert exc_info.value.http_status == 0
    assert isinstance(exc_info.value.response_excerpt, str)


def test_fetch_subscription_inventory_preserves_http_failure_status_and_excerpt() -> (
    None
):
    fetch_subscription_inventory, inventory_error = _public_inventory_api()
    response_text = "gateway returned a diagnostic body"
    fake_session = FakeSession(
        get_responses=[
            FakeResponse(
                503,
                text=response_text,
                infer_total_count=False,
            )
        ]
    )

    with pytest.raises(inventory_error) as exc_info:
        fetch_subscription_inventory(
            settings=settings(),
            auth=FakeAuth(),
            session=fake_session,
        )

    assert exc_info.value.http_status == 503
    assert exc_info.value.response_excerpt == response_text


def test_fetch_subscription_inventory_builds_session_when_omitted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fetch_subscription_inventory, _inventory_error = _public_inventory_api()
    import sendai_pipeline.sth_subscriptions as subscriptions_module

    fake_session = FakeSession(get_responses=[FakeResponse(200, json_body=[])])
    session_builds = 0

    def build_session() -> FakeSession:
        nonlocal session_builds
        session_builds += 1
        return fake_session

    monkeypatch.setattr(subscriptions_module.requests, "Session", build_session)

    result = fetch_subscription_inventory(
        settings=settings(),
        auth=FakeAuth(),
    )

    assert result == []
    assert session_builds == 1
    assert len(fake_session.gets) == 1


def test_fetch_subscription_inventory_returns_dicts_from_multiple_pages() -> None:
    fetch_subscription_inventory, _inventory_error = _public_inventory_api()
    first_page = [unrelated_subscription(index) for index in range(100)]
    second_page = [unrelated_subscription(100)]
    fake_session = FakeSession(
        get_responses=[
            FakeResponse(
                200,
                headers={"Fiware-Total-Count": "101"},
                json_body=first_page,
            ),
            FakeResponse(
                200,
                headers={"Fiware-Total-Count": "101"},
                json_body=second_page,
            ),
        ]
    )

    result = fetch_subscription_inventory(
        settings=settings(),
        auth=FakeAuth(),
        session=fake_session,
    )

    assert result == [*first_page, *second_page]
    assert all(isinstance(subscription, dict) for subscription in result)


def _public_inventory_api() -> tuple[Any, Any]:
    # Deliberately imported only when a new contract test runs. During the
    # tests-first stage these names do not exist yet, but the 65 established
    # tests in this module must still collect and execute.
    from sendai_pipeline.sth_subscriptions import (
        SubscriptionInventoryError,
        fetch_subscription_inventory,
    )

    return fetch_subscription_inventory, SubscriptionInventoryError
