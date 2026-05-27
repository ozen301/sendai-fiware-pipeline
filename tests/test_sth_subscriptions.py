import json
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
import requests

from sendai_pipeline.logging_setup import _ALLOWED_EXTRA_KEYS
from sendai_pipeline.sth_subscriptions import (
    PRODUCT_A_HISTORY_ATTRS,
    PRODUCT_A_TRIGGER_ATTRS,
    PRODUCT_B_HISTORY_ATTRS,
    PRODUCT_B_TRIGGER_ATTRS,
    StHSubscriptionError,
    StHSubscriptionSettings,
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
    ) -> None:
        self.status_code = status_code
        self.text = text
        self.headers = headers or {}
        self.json_body = [] if json_body is None else json_body

    def json(self) -> Any:
        return self.json_body


class FakeSession:
    def __init__(
        self,
        post_responses: list[FakeResponse] | None = None,
        get_responses: list[FakeResponse] | None = None,
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
        return self.get_responses.pop(0)

    def post(self, url: str, **kwargs: Any) -> FakeResponse:
        self.posts.append({"url": url, **kwargs})
        return self.post_responses.pop(0)

    def delete(self, url: str, **kwargs: Any) -> FakeResponse:
        self.deletes.append({"url": url, **kwargs})
        return self.delete_responses.pop(0)


def settings(**overrides: Any) -> StHSubscriptionSettings:
    values: dict[str, Any] = {
        "base_url": "https://example.test",
        "comet_notify_url": "http://internal-comet.example/notify",
    }
    values.update(overrides)
    return StHSubscriptionSettings(**values)


def test_build_product_a_subscription_triggers_on_exclusive_attribute() -> None:
    body = build_product_a_subscription_body(
        settings(),
        now=datetime(2026, 5, 24, 12, 30, tzinfo=JST),
    )

    assert body["subject"]["entities"] == [
        {"idPattern": ".*", "type": "Blesensor.per300"},
        {"idPattern": ".*", "type": "Blesensor.per3600"},
    ]
    assert body["subject"]["condition"]["attrs"] == list(PRODUCT_A_TRIGGER_ATTRS)
    assert body["subject"]["condition"]["attrs"] == ["peopleCount_immedate"]
    assert body["notification"]["attrsFormat"] == "legacy"
    assert body["notification"]["attrs"] == list(PRODUCT_A_HISTORY_ATTRS)
    assert body["notification"]["metadata"] == ["TimeInstant"]
    assert body["subject"]["condition"]["notifyOnMetadataChange"] is True
    assert "notifyOnMetadataChange" not in body["notification"]


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
    assert parsed.throttling_seconds == 0


def test_settings_from_env_accepts_subscription_overrides() -> None:
    parsed = StHSubscriptionSettings.from_env(
        {
            "FIWARE_BASE_URL": "https://example.test",
            "COMET_NOTIFY_URL": "http://internal-comet.example/notify",
            "FIWARE_SERVICE": "service",
            "FIWARE_SERVICE_PATH": "/path",
            "STH_SUBSCRIPTION_EXPIRES": "2030-01-01T00:00:00Z",
            "STH_SUBSCRIPTION_THROTTLING_SECONDS": "30",
            "STH_SUBSCRIPTION_SKIP_INITIAL": "false",
        }
    )

    assert parsed.service == "service"
    assert parsed.service_path == "/path"
    assert parsed.expires == "2030-01-01T00:00:00Z"
    assert parsed.throttling_seconds == 30
    assert parsed.skip_initial_notification is False


def test_settings_from_env_requires_comet_notify_url() -> None:
    with pytest.raises(StHSubscriptionError, match="COMET_NOTIFY_URL"):
        StHSubscriptionSettings.from_env({"FIWARE_BASE_URL": "https://example.test"})


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
            throttling_seconds=30,
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
    assert body["throttling"] == 30


def test_create_product_a_subscription_skips_when_subscription_already_exists() -> None:
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
    assert result.skipped == 1
    assert result.failed == 0
    assert result.subscription_ids == ("existing-sub",)
    assert fake_session.posts == []


def test_create_product_a_subscription_detects_existing_matching_shape() -> None:
    fake_session = FakeSession(
        get_responses=[
            FakeResponse(
                200,
                json_body=[
                    {
                        "id": "shape-sub",
                        "subject": {
                            "entities": [
                                {"idPattern": ".*", "type": "Blesensor.per300"},
                                {"idPattern": ".*", "type": "Blesensor.per3600"},
                            ],
                            "condition": {
                                "attrs": list(PRODUCT_A_TRIGGER_ATTRS),
                                "notifyOnMetadataChange": True,
                            },
                        },
                        "notification": {
                            "attrsFormat": "legacy",
                            "attrs": list(PRODUCT_A_HISTORY_ATTRS),
                            "metadata": ["TimeInstant"],
                        },
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

    assert result.skipped == 1
    assert result.subscription_ids == ("shape-sub",)
    assert fake_session.posts == []


def test_create_product_a_subscription_ignores_shape_without_timeinstant_metadata() -> (
    None
):
    fake_session = FakeSession(
        post_responses=[
            FakeResponse(201, headers={"Location": "/subscriptions/correct-sub"}),
        ],
        get_responses=[
            FakeResponse(
                200,
                json_body=[
                    {
                        "id": "legacy-sub",
                        "subject": {
                            "entities": [
                                {"idPattern": ".*", "type": "Blesensor.per300"},
                                {"idPattern": ".*", "type": "Blesensor.per3600"},
                            ],
                            "condition": {"attrs": list(PRODUCT_A_TRIGGER_ATTRS)},
                        },
                        "notification": {
                            "attrsFormat": "legacy",
                            "attrs": list(PRODUCT_A_HISTORY_ATTRS),
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
    assert result.skipped == 0
    assert result.subscription_ids == ("correct-sub",)
    assert len(fake_session.posts) == 1


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


def test_build_product_b_subscription_uses_people_count_flow_attrs() -> None:
    body = build_product_b_subscription_body(
        settings(),
        now=datetime(2026, 5, 25, 12, 30, tzinfo=JST),
    )

    assert body["subject"]["entities"] == [
        {"idPattern": ".*", "type": "Blesensor.per300"},
        {"idPattern": ".*", "type": "Blesensor.per3600"},
    ]
    assert body["subject"]["condition"]["attrs"] == list(PRODUCT_B_TRIGGER_ATTRS)
    assert body["subject"]["condition"]["attrs"] == ["peopleCount_flow"]
    assert body["notification"]["attrsFormat"] == "legacy"
    assert body["notification"]["attrs"] == list(PRODUCT_B_HISTORY_ATTRS)
    assert "peopleCount_flow" in body["notification"]["attrs"]
    assert body["notification"]["metadata"] == ["TimeInstant"]
    assert body["subject"]["condition"]["notifyOnMetadataChange"] is True
    assert "notifyOnMetadataChange" not in body["notification"]
    assert body["description"].startswith("Product B STH-Comet history")


def test_redacted_product_b_subscription_json_omits_private_notify_url() -> None:
    rendered = redacted_product_b_subscription_json(settings())

    assert "internal-comet.example" not in rendered
    assert "<COMET_NOTIFY_URL>" in rendered
    assert '"Product B STH-Comet history' in rendered


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
        settings=settings(dry_run=False),
        auth=FakeAuth(),
        session=fake_session,
    )

    assert result.created == 1
    assert result.subscription_ids == ("sub-b",)
    [post] = fake_session.posts
    body = json.loads(post["data"])
    assert body["notification"]["attrs"] == list(PRODUCT_B_HISTORY_ATTRS)
    assert body["description"].startswith("Product B STH-Comet history")


def test_create_product_b_subscription_does_not_skip_when_only_product_a_exists() -> (
    None
):
    fake_session = FakeSession(
        post_responses=[
            FakeResponse(201, headers={"Location": "/orion/v2.0/subscriptions/sub-b"}),
        ],
        get_responses=[
            FakeResponse(
                200,
                json_body=[
                    {
                        "id": "existing-product-a",
                        "description": "Product A STH-Comet history set at ...",
                        "subject": {
                            "entities": [
                                {"idPattern": ".*", "type": "Blesensor.per300"},
                                {"idPattern": ".*", "type": "Blesensor.per3600"},
                            ],
                            "condition": {
                                "attrs": list(PRODUCT_A_TRIGGER_ATTRS),
                                "notifyOnMetadataChange": True,
                            },
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
        settings=settings(dry_run=False),
        auth=FakeAuth(),
        session=fake_session,
    )

    assert result.created == 1
    assert result.subscription_ids == ("sub-b",)


def test_create_product_a_subscription_does_not_skip_when_only_product_b_exists() -> (
    None
):
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
                                "attrs": list(PRODUCT_B_TRIGGER_ATTRS),
                                "notifyOnMetadataChange": True,
                            },
                        },
                        "notification": {
                            "attrsFormat": "legacy",
                            "attrs": list(PRODUCT_B_HISTORY_ATTRS),
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


def test_create_product_b_subscription_skips_when_already_exists() -> None:
    fake_session = FakeSession(
        get_responses=[
            FakeResponse(
                200,
                json_body=[
                    {
                        "id": "existing-product-b",
                        "description": "Product B STH-Comet history set at ...",
                    }
                ],
            )
        ],
    )

    result = create_product_b_sth_subscription(
        settings=settings(dry_run=False),
        auth=FakeAuth(),
        session=fake_session,
    )

    assert result.skipped == 1
    assert result.subscription_ids == ("existing-product-b",)
    assert fake_session.posts == []


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
                                {"idPattern": ".*", "type": "Blesensor.per300"},
                                {"idPattern": ".*", "type": "Blesensor.per3600"},
                            ],
                            "condition": {"attrs": ["dateObservedFrom"]},
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
            settings=settings(dry_run=False),
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
                    {
                        "id": "current-product-b",
                        "description": "Product B STH-Comet history set at ...",
                        "subject": {
                            "entities": [
                                {"idPattern": ".*", "type": "Blesensor.per300"},
                                {"idPattern": ".*", "type": "Blesensor.per3600"},
                            ],
                            "condition": {
                                "attrs": list(PRODUCT_B_TRIGGER_ATTRS),
                                "notifyOnMetadataChange": True,
                            },
                        },
                        "notification": {
                            "attrsFormat": "legacy",
                            "attrs": list(PRODUCT_B_HISTORY_ATTRS),
                            "metadata": ["TimeInstant"],
                        },
                    },
                    {
                        "id": "stale-product-a",
                        "description": "Product A STH-Comet history set at ...",
                        "subject": {
                            "entities": [
                                {"idPattern": ".*", "type": "Blesensor.per300"},
                                {"idPattern": ".*", "type": "Blesensor.per3600"},
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
        settings=settings(dry_run=False),
        auth=FakeAuth(),
        session=fake_session,
    )

    assert result.failed == 1
    assert result.skipped == 0
    assert fake_session.posts == []


def test_create_product_a_aborts_when_product_b_uses_stale_shared_trigger() -> None:
    fake_session = FakeSession(
        get_responses=[
            FakeResponse(
                200,
                json_body=[
                    {
                        "id": "stale-product-b",
                        "description": "Product B STH-Comet history set at ...",
                        "subject": {
                            "entities": [
                                {"idPattern": ".*", "type": "Blesensor.per300"},
                                {"idPattern": ".*", "type": "Blesensor.per3600"},
                            ],
                            "condition": {"attrs": ["dateObservedFrom"]},
                        },
                        "notification": {
                            "attrsFormat": "legacy",
                            "attrs": list(PRODUCT_B_HISTORY_ATTRS),
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
                                {"idPattern": ".*", "type": "Blesensor.per300"},
                                {"idPattern": ".*", "type": "Blesensor.per3600"},
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
        settings=settings(dry_run=False),
        auth=FakeAuth(),
        session=fake_session,
    )

    assert result.failed == 1
    assert result.created == 0
    assert fake_session.posts == []


def test_trigger_attrs_are_exclusive_between_products() -> None:
    assert set(PRODUCT_A_TRIGGER_ATTRS).isdisjoint(PRODUCT_B_HISTORY_ATTRS)
    assert set(PRODUCT_B_TRIGGER_ATTRS).isdisjoint(PRODUCT_A_HISTORY_ATTRS)


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
