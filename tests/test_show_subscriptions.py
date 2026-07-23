import importlib
import json
import logging
import os
import sys
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any

import pytest
import requests

import sendai_pipeline.sth_subscriptions as sth_subscriptions

SUB_A = "65e87f5c20bd0c390e057c62"
SUB_B = "65e87f5c20bd0c390e057c63"
SUB_C = "65e87f5c20bd0c390e057c64"
COMET_NOTIFY_URL = "http://internal-comet.example/notify"
HEADER_CREDENTIAL = "header-credential-731"
QUERY_CREDENTIAL = "query-credential-842"
MQTT_USER = "mqtt-user-953"
MQTT_PASSWORD = "mqtt-password-164"
BODY_CREDENTIAL = "body-credential-275"
ERROR_CREDENTIAL = "error-credential-386"
AUTH_CREDENTIAL = "auth-credential-497"

_BASE_LOG_RECORD_FIELDS = frozenset(
    logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="test",
        args=(),
        exc_info=None,
    ).__dict__
)


class FakeAuth:
    def __init__(self) -> None:
        self.force_refreshes: list[bool] = []
        self.error: BaseException | None = None

    def get_token(self, *, force_refresh: bool = False) -> str:
        self.force_refreshes.append(force_refresh)
        if self.error is not None:
            raise self.error
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
        self.headers = dict(headers or {})
        self.json_body = [] if json_body is None else json_body
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
    def __init__(self, get_responses: list[Any] | None = None) -> None:
        self.get_responses = list(get_responses or [])
        self.gets: list[dict[str, Any]] = []

    def get(self, url: str, **kwargs: Any) -> FakeResponse:
        self.gets.append({"url": url, **kwargs})
        outcome = self.get_responses.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


@dataclass
class RuntimePatch:
    session: FakeSession = field(default_factory=FakeSession)
    auth: FakeAuth = field(default_factory=FakeAuth)
    settings_error: BaseException | None = None
    auth_settings_error: BaseException | None = None
    inventory_calls: list[dict[str, Any]] = field(default_factory=list)
    id_calls: list[str] = field(default_factory=list)
    console_logging: bool = False

    def build_auth(self, *_args: Any, **_kwargs: Any) -> FakeAuth:
        return self.auth


@pytest.fixture
def runtime(monkeypatch: pytest.MonkeyPatch) -> RuntimePatch:
    monkeypatch.setenv("FIWARE_BASE_URL", "https://fiware.example.test")
    monkeypatch.setenv("FIWARE_SERVICE", "sendai")
    monkeypatch.setenv("FIWARE_SERVICE_PATH", "/city")
    monkeypatch.delenv("COMET_NOTIFY_URL", raising=False)
    return RuntimePatch()


@pytest.fixture(autouse=True)
def restore_package_logger() -> Any:
    logger = logging.getLogger("sendai_pipeline")
    old_handlers = list(logger.handlers)
    old_level = logger.level
    old_propagate = logger.propagate
    yield
    for handler in list(logger.handlers):
        if handler not in old_handlers:
            logger.removeHandler(handler)
            handler.close()
    logger.handlers = old_handlers
    logger.setLevel(old_level)
    logger.propagate = old_propagate


def test_show_subscriptions_lists_single_page_with_deterministic_human_block(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    subscription = _full_subscription()
    runtime.session.get_responses = [
        FakeResponse(200, json_body=[subscription]),
    ]

    result = _invoke([], capsys, runtime)

    captured = capsys.readouterr()
    assert result == 0
    assert captured.err == ""
    assert captured.out == (
        f"{SUB_A}\n"
        "  status:        active\n"
        "  expires:       2030-01-01T00:00:00.000Z\n"
        "  description:   Product A complete subscription\n"
        "  entities:      type=Blesensor.per300 idPattern=.* ; "
        "type=Blesensor.per3600 idPattern=.*\n"
        "  trigger attrs: dateObservedFrom, dateRetrieved\n"
        '  expression:    {"georel":"near","q":"temp>40"}\n'
        "  notifyOnMetadataChange: true\n"
        "  throttling:    5\n"
        f"  notification:  transport=http url={COMET_NOTIFY_URL} "
        "format=legacy metadata=[TimeInstant] "
        "attrs=[dateObservedFrom,dateRetrieved]\n"
        "  delivery:      timesSent=7 failsCounter=2 "
        "lastNotification=2026-07-23T00:00:01.000Z\n"
        "                 lastSuccess=2026-07-23T00:00:01.000Z(200) "
        "lastFailure=2026-07-22T23:59:00.000Z(500) "
        "lastFailureReason=upstream unavailable\n"
        "1 subscription(s)\n"
    )
    assert len(runtime.session.gets) == 1
    assert runtime.session.gets[0]["params"] == {
        "limit": 100,
        "offset": 0,
        "options": "count",
    }


def test_show_subscriptions_lists_multiple_pages_in_broker_order(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    subscriptions = [_minimal_subscription(f"{index:024x}") for index in range(101)]
    runtime.session.get_responses = [
        FakeResponse(
            200,
            headers={"Fiware-Total-Count": "101"},
            json_body=subscriptions[:100],
        ),
        FakeResponse(
            200,
            headers={"Fiware-Total-Count": "101"},
            json_body=subscriptions[100:],
        ),
    ]

    result = _invoke([], capsys, runtime)

    captured = capsys.readouterr()
    assert result == 0
    assert captured.err == ""
    assert captured.out.endswith("101 subscription(s)\n")
    assert captured.out.index(subscriptions[0]["id"]) < captured.out.index(
        subscriptions[-1]["id"]
    )
    assert [call["params"]["offset"] for call in runtime.session.gets] == [0, 100]


def test_show_subscriptions_json_returns_complete_raw_array(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    subscriptions = [_full_subscription(), _minimal_subscription(SUB_B)]
    runtime.session.get_responses = [FakeResponse(200, json_body=subscriptions)]

    result = _invoke(["--json"], capsys, runtime)

    captured = capsys.readouterr()
    assert result == 0
    assert json.loads(captured.out) == subscriptions
    assert captured.err == "2 subscription(s)\n"


def test_show_subscriptions_shows_secrets_in_human_and_json_but_never_logs_them(
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
    runtime: RuntimePatch,
) -> None:
    caplog.set_level(logging.INFO, logger="sendai_pipeline")
    subscriptions = _secret_subscriptions()
    runtime.session.get_responses = [
        FakeResponse(200, json_body=subscriptions),
        FakeResponse(200, json_body=subscriptions),
    ]

    human_result = _invoke([], capsys, runtime)
    human = capsys.readouterr()
    json_result = _invoke(["--json"], capsys, runtime)
    machine = capsys.readouterr()

    secrets = (
        HEADER_CREDENTIAL,
        QUERY_CREDENTIAL,
        MQTT_USER,
        MQTT_PASSWORD,
        BODY_CREDENTIAL,
    )
    assert human_result == 0
    assert json_result == 0
    assert json.loads(machine.out) == subscriptions
    events = {record.__dict__.get("event") for record in caplog.records}
    assert "show_subscriptions_requested" in events
    assert "show_subscriptions_summary" in events
    for secret in secrets:
        assert secret in human.out
        assert secret in machine.out
        assert secret not in _all_log_record_fields(caplog.records)


def test_show_subscriptions_inventory_error_surfaces_secret_only_in_diagnostic(
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
    runtime: RuntimePatch,
) -> None:
    caplog.set_level(logging.INFO, logger="sendai_pipeline")
    response_text = f"gateway echoed credential={ERROR_CREDENTIAL}"
    runtime.session.get_responses = [
        FakeResponse(503, text=response_text, infer_total_count=False)
    ]

    result = _invoke(["--json"], capsys, runtime)

    captured = capsys.readouterr()
    assert result == 1
    assert captured.out == ""
    assert ERROR_CREDENTIAL in captured.err
    assert ERROR_CREDENTIAL not in _all_log_record_fields(caplog.records)
    failure = _failure_record_with_status(caplog.records, 503)
    assert failure.getMessage() == failure.msg
    assert failure.args == ()
    assert failure.exc_info is None
    assert "response_excerpt" not in failure.__dict__
    assert _extra_record_fields(failure) == {"event", "http_status"}


def test_show_subscriptions_runtime_auth_error_surfaces_secret_only_in_diagnostic(
    capsys: pytest.CaptureFixture[str],
    caplog: pytest.LogCaptureFixture,
    runtime: RuntimePatch,
) -> None:
    caplog.set_level(logging.INFO, logger="sendai_pipeline")
    runtime.auth.error = RuntimeError(
        f"token endpoint rejected credential={AUTH_CREDENTIAL}"
    )

    result = _invoke([SUB_A, "--json"], capsys, runtime)

    captured = capsys.readouterr()
    assert result == 1
    assert captured.out == ""
    assert AUTH_CREDENTIAL in captured.err
    assert runtime.session.gets == []
    assert AUTH_CREDENTIAL not in _all_log_record_fields(caplog.records)
    failure = _failure_record_with_status(caplog.records, 0)
    assert failure.getMessage() == failure.msg
    assert failure.args == ()
    assert failure.exc_info is None
    assert "response_excerpt" not in failure.__dict__
    assert _extra_record_fields(failure) == {"event", "http_status"}


@pytest.mark.parametrize(
    ("transport", "transport_body", "expected"),
    [
        pytest.param(
            "http",
            {"url": "http://notify.example.test/basic"},
            "transport=http url=http://notify.example.test/basic format=legacy "
            "metadata=[TimeInstant] attrs=<all>",
            id="http",
        ),
        pytest.param(
            "httpCustom",
            {
                "url": "http://notify.example.test/custom",
                "method": "POST",
                "headers": {"X-Z": "z", "Authorization": "custom-header"},
                "qs": {"z": "9", "a": "1"},
                "payload": {"z": 1, "token": "custom-body"},
                "json": {"content": "custom-json", "z": 2},
                "ngsi": {"data": "custom-ngsi", "version": 2},
            },
            "transport=httpCustom url=http://notify.example.test/custom "
            "format=legacy metadata=[TimeInstant] attrs=<all> method=POST "
            'headers={"Authorization":"custom-header","X-Z":"z"} '
            'qs={"a":"1","z":"9"} payload={"token":"custom-body","z":1} '
            'json={"content":"custom-json","z":2} '
            'ngsi={"data":"custom-ngsi","version":2}',
            id="http-custom",
        ),
        pytest.param(
            "mqtt",
            {
                "url": "mqtt://broker.example.test",
                "topic": "sendai/basic",
                "qos": 1,
                "retain": True,
                "user": "mqtt-user",
                "passwd": "mqtt-pass",
            },
            "transport=mqtt url=mqtt://broker.example.test topic=sendai/basic "
            "qos=1 retain=true format=legacy metadata=[TimeInstant] attrs=<all> "
            "user=mqtt-user passwd=mqtt-pass",
            id="mqtt",
        ),
        pytest.param(
            "mqttCustom",
            {
                "url": "mqtt://broker.example.test",
                "topic": "sendai/custom",
                "qos": 2,
                "retain": False,
                "user": "custom-user",
                "passwd": "custom-pass",
                "ngsi": {"data": "custom-ngsi", "version": 2},
            },
            "transport=mqttCustom url=mqtt://broker.example.test "
            "topic=sendai/custom qos=2 retain=false format=legacy "
            "metadata=[TimeInstant] attrs=<all> user=custom-user "
            'passwd=custom-pass ngsi={"data":"custom-ngsi","version":2}',
            id="mqtt-custom",
        ),
        pytest.param(
            "kafka",
            {
                "url": "kafka://broker.example.test",
                "topic": "sendai-basic",
            },
            "transport=kafka url=kafka://broker.example.test topic=sendai-basic "
            "format=legacy metadata=[TimeInstant] attrs=<all>",
            id="kafka",
        ),
        pytest.param(
            "kafkaCustom",
            {
                "url": "kafka://broker.example.test",
                "topic": "sendai-custom",
                "payload": {"token": "kafka-body", "z": 1},
            },
            "transport=kafkaCustom url=kafka://broker.example.test "
            "topic=sendai-custom format=legacy metadata=[TimeInstant] "
            'attrs=<all> payload={"token":"kafka-body","z":1}',
            id="kafka-custom",
        ),
    ],
)
def test_show_subscriptions_renders_each_transport_shape(
    transport: str,
    transport_body: dict[str, Any],
    expected: str,
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    subscription = _minimal_subscription(SUB_A)
    subscription["notification"] = {
        transport: transport_body,
        "attrsFormat": "legacy",
        "metadata": ["TimeInstant"],
    }
    runtime.session.get_responses = [FakeResponse(200, json_body=[subscription])]

    result = _invoke([], capsys, runtime)

    captured = capsys.readouterr()
    assert result == 0
    assert f"  notification:  {expected}\n" in captured.out


def test_show_subscriptions_prints_none_fillers_and_sorted_expression_json(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    absent = _minimal_subscription(SUB_A)
    present = _minimal_subscription(SUB_B)
    present["subject"] = {
        "entities": [{"type": "Other.Type", "id": "entity-2"}],
        "condition": {
            "expression": {"z": 1, "a": {"d": 4, "b": 2}},
        },
    }
    runtime.session.get_responses = [FakeResponse(200, json_body=[absent, present])]

    result = _invoke([], capsys, runtime)

    captured = capsys.readouterr()
    assert result == 0
    absent_block = captured.out.split(SUB_B, maxsplit=1)[0]
    assert "  status:        <none>\n" in absent_block
    assert "  expires:       <none>\n" in absent_block
    assert "  description:   <no description>\n" in absent_block
    assert "  entities:      <none>\n" in absent_block
    assert "  trigger attrs: <all>\n" in absent_block
    assert "  expression:    <none>\n" in absent_block
    assert "  notifyOnMetadataChange: <none>\n" in absent_block
    assert "  throttling:    <none>\n" in absent_block
    assert "metadata=<none>" in absent_block
    assert (
        "  delivery:      timesSent=<none> failsCounter=<none> "
        "lastNotification=<none>\n"
    ) in absent_block
    assert (
        "                 lastSuccess=<none>(<none>) "
        "lastFailure=<none>(<none>) lastFailureReason=<none>\n"
    ) in absent_block
    assert "  entities:      type=Other.Type id=entity-2\n" in captured.out
    assert '  expression:    {"a":{"b":2,"d":4},"z":1}\n' in captured.out


def test_show_subscriptions_json_keeps_result_on_stdout_and_everything_else_on_stderr(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    runtime.console_logging = True
    runtime.session.get_responses = [FakeResponse(404, text="not found")]

    result = _invoke([SUB_A, "--json"], capsys, runtime)

    captured = capsys.readouterr()
    assert result == 1
    assert json.loads(captured.out) == []
    assert captured.out.strip() == "[]"
    assert f"{SUB_A}: not found" in captured.err
    assert "INFO:" in captured.err


@pytest.mark.parametrize(
    "error_target",
    [
        pytest.param("settings", id="settings"),
        pytest.param("auth-settings", id="auth-settings"),
    ],
)
def test_show_subscriptions_json_settings_or_auth_parse_error_emits_no_array(
    error_target: str,
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    if error_target == "settings":
        runtime.settings_error = ValueError("invalid FIWARE_BASE_URL")
    else:
        runtime.auth_settings_error = ValueError("invalid OS_AUTH_URL")

    result = _invoke(["--json"], capsys, runtime)

    captured = capsys.readouterr()
    assert result == 2
    assert captured.out == ""
    assert "invalid" in captured.err


def test_show_subscriptions_json_all_named_ids_absent_emits_empty_array(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    runtime.session.get_responses = [
        FakeResponse(404, text="not found"),
        FakeResponse(404, text="not found"),
    ]

    result = _invoke([SUB_A, SUB_B, "--json"], capsys, runtime)

    captured = capsys.readouterr()
    assert result == 1
    assert json.loads(captured.out) == []
    assert f"{SUB_A}: not found" in captured.err
    assert f"{SUB_B}: not found" in captured.err


def test_show_subscriptions_json_all_named_ids_erroring_emits_empty_array(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    runtime.session.get_responses = [
        FakeResponse(502, text="gateway one"),
        FakeResponse(503, text="gateway two"),
    ]

    result = _invoke([SUB_A, SUB_B, "--json"], capsys, runtime)

    captured = capsys.readouterr()
    assert result == 1
    assert json.loads(captured.out) == []
    assert SUB_A in captured.err
    assert SUB_B in captured.err
    assert len(runtime.session.gets) == 2


def test_show_subscriptions_id_mode_emits_found_and_reports_absent_id(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    found = _minimal_subscription(SUB_A)
    runtime.session.get_responses = [
        FakeResponse(200, json_body=found),
        FakeResponse(404, text="not found"),
    ]

    result = _invoke([SUB_A, SUB_B, "--json"], capsys, runtime)

    captured = capsys.readouterr()
    assert result == 1
    assert json.loads(captured.out) == [found]
    assert f"{SUB_B}: not found" in captured.err
    assert runtime.id_calls == [SUB_A, SUB_B]


def test_show_subscriptions_id_mode_continues_after_transport_error(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    found = _minimal_subscription(SUB_B)
    runtime.session.get_responses = [
        requests.ConnectionError("broker connection failed"),
        FakeResponse(200, json_body=found),
    ]

    result = _invoke([SUB_A, SUB_B, "--json"], capsys, runtime)

    captured = capsys.readouterr()
    assert result == 1
    assert json.loads(captured.out) == [found]
    assert SUB_A in captured.err
    assert "broker connection failed" in captured.err
    assert runtime.id_calls == [SUB_A, SUB_B]


@pytest.mark.parametrize(
    "argv",
    [
        pytest.param(["not-a-real-id"], id="invalid"),
        pytest.param([SUB_A, SUB_A], id="duplicate"),
    ],
)
def test_show_subscriptions_rejects_invalid_or_duplicate_id(
    argv: list[str],
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    result = _invoke(argv, capsys, runtime)

    captured = capsys.readouterr()
    assert result == 2
    assert runtime.session.gets == []
    assert captured.out == ""


def test_show_subscriptions_runtime_auth_failure_is_operational_error(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    runtime.auth.error = RuntimeError("token endpoint unreachable")

    result = _invoke(["--json"], capsys, runtime)

    captured = capsys.readouterr()
    assert result == 1
    assert captured.out == ""
    assert "token endpoint unreachable" in captured.err
    assert runtime.session.gets == []


def test_show_subscriptions_succeeds_without_comet_or_product_b_config(
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    runtime: RuntimePatch,
) -> None:
    monkeypatch.delenv("COMET_NOTIFY_URL", raising=False)
    monkeypatch.delenv("PRODUCT_B_AGGREGATE_ENTITY_ID", raising=False)
    monkeypatch.delenv("PRODUCT_B_AGGREGATE_ENTITY_TYPE", raising=False)
    runtime.session.get_responses = [FakeResponse(200, json_body=[])]

    result = _invoke([], capsys, runtime)

    captured = capsys.readouterr()
    assert result == 0
    assert captured.out == "0 subscription(s)\n"


@pytest.mark.parametrize(
    "attrs",
    [pytest.param(None, id="omitted"), pytest.param([], id="empty")],
)
def test_show_subscriptions_displays_zero_telemetry_and_all_attribute_notification(
    attrs: list[str] | None,
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    subscription = _minimal_subscription(SUB_A)
    subscription["notification"]["timesSent"] = 0
    subscription["notification"]["failsCounter"] = 0
    if attrs is not None:
        subscription["notification"]["attrs"] = attrs
    runtime.session.get_responses = [FakeResponse(200, json_body=[subscription])]

    result = _invoke([], capsys, runtime)

    captured = capsys.readouterr()
    assert result == 0
    assert "timesSent=0 failsCounter=0" in captured.out
    assert "attrs=<all>" in captured.out
    assert "timesSent=<none>" not in captured.out
    assert "failsCounter=<none>" not in captured.out


def test_show_subscriptions_empty_broker_is_success(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    runtime.session.get_responses = [FakeResponse(200, json_body=[])]

    result = _invoke([], capsys, runtime)

    captured = capsys.readouterr()
    assert result == 0
    assert captured.out == "0 subscription(s)\n"
    assert captured.err == ""


def _invoke(
    argv: list[str],
    _capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> int:
    show_subscriptions = importlib.import_module("scripts.show_subscriptions")
    _patch_show_subscriptions_module(show_subscriptions, runtime)
    try:
        result = show_subscriptions.main(argv)
    except SystemExit as exc:
        result = exc.code
    if result is None:
        return 0
    if isinstance(result, int):
        return result
    return int(result)


def _patch_show_subscriptions_module(module: Any, runtime: RuntimePatch) -> None:
    def settings_from_env() -> SimpleNamespace:
        if runtime.settings_error is not None:
            raise runtime.settings_error
        return SimpleNamespace(
            base_url=os.environ["FIWARE_BASE_URL"],
            service=os.environ.get("FIWARE_SERVICE", ""),
            service_path=os.environ.get("FIWARE_SERVICE_PATH", "/"),
            verify_tls=True,
            timeout=10,
        )

    def auth_settings_from_env() -> object:
        if runtime.auth_settings_error is not None:
            raise runtime.auth_settings_error
        return object()

    def inventory_reader(**kwargs: Any) -> list[dict[str, Any]]:
        runtime.inventory_calls.append(dict(kwargs))
        return sth_subscriptions.fetch_subscription_inventory(
            **kwargs,
            session=runtime.session,
        )

    def subscription_get(subscription_id: str, **kwargs: Any) -> dict[str, Any] | None:
        runtime.id_calls.append(subscription_id)
        return sth_subscriptions.get_subscription(
            subscription_id,
            **kwargs,
            session=runtime.session,
        )

    module.AuthClient = runtime.build_auth
    module.AuthSettings = type(
        "FakeAuthSettings",
        (),
        {"from_env": staticmethod(auth_settings_from_env)},
    )
    module.OrionSettings = type(
        "FakeOrionSettings",
        (),
        {"from_env": staticmethod(settings_from_env)},
    )
    module.fetch_subscription_inventory = inventory_reader
    module.get_subscription = subscription_get
    module.load_dotenv = lambda *_args, **_kwargs: None
    module.find_dotenv = lambda *_args, **_kwargs: ""
    module.LoggingSettings = type(
        "FakeLoggingSettings",
        (),
        {"from_env": staticmethod(lambda: object())},
    )

    logger = logging.getLogger("sendai_pipeline")
    logger.setLevel(logging.INFO)
    if runtime.console_logging:
        module.configure_logging = _install_stdout_console_handler
    else:
        module.configure_logging = lambda *_args, **_kwargs: None
        logger.propagate = True


def _install_stdout_console_handler(*_args: Any, **_kwargs: Any) -> None:
    logger = logging.getLogger("sendai_pipeline")
    for handler in list(logger.handlers):
        if not isinstance(handler, logging.NullHandler):
            logger.removeHandler(handler)
            handler.close()
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter("%(levelname)s:%(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False


def _full_subscription() -> dict[str, Any]:
    return {
        "id": SUB_A,
        "status": "active",
        "expires": "2030-01-01T00:00:00.000Z",
        "description": "Product A complete subscription",
        "subject": {
            "entities": [
                {"idPattern": ".*", "type": "Blesensor.per300"},
                {"idPattern": ".*", "type": "Blesensor.per3600"},
            ],
            "condition": {
                "attrs": ["dateObservedFrom", "dateRetrieved"],
                "expression": {"q": "temp>40", "georel": "near"},
                "notifyOnMetadataChange": True,
            },
        },
        "throttling": 5,
        "notification": {
            "http": {"url": COMET_NOTIFY_URL},
            "attrsFormat": "legacy",
            "attrs": ["dateObservedFrom", "dateRetrieved"],
            "metadata": ["TimeInstant"],
            "timesSent": 7,
            "failsCounter": 2,
            "lastNotification": "2026-07-23T00:00:01.000Z",
            "lastSuccess": "2026-07-23T00:00:01.000Z",
            "lastSuccessCode": 200,
            "lastFailure": "2026-07-22T23:59:00.000Z",
            "lastFailureCode": 500,
            "lastFailureReason": "upstream unavailable",
        },
    }


def _minimal_subscription(subscription_id: str) -> dict[str, Any]:
    return {
        "id": subscription_id,
        "notification": {
            "http": {"url": "http://notify.example.test"},
        },
    }


def _secret_subscriptions() -> list[dict[str, Any]]:
    http_custom = _minimal_subscription(SUB_A)
    http_custom["notification"] = {
        "httpCustom": {
            "url": "http://notify.example.test/custom",
            "headers": {"X-Internal-Credential": HEADER_CREDENTIAL},
            "qs": {"access": QUERY_CREDENTIAL},
            "payload": {"credential": BODY_CREDENTIAL},
        }
    }
    mqtt = _minimal_subscription(SUB_B)
    mqtt["notification"] = {
        "mqtt": {
            "url": "mqtt://broker.example.test",
            "topic": "sendai/secrets",
            "user": MQTT_USER,
            "passwd": MQTT_PASSWORD,
        }
    }
    return [http_custom, mqtt]


def _all_log_record_fields(records: list[logging.LogRecord]) -> str:
    return "\n".join(
        repr(value) for record in records for value in record.__dict__.values()
    )


def _failure_record_with_status(
    records: list[logging.LogRecord],
    http_status: int,
) -> logging.LogRecord:
    matches = [
        record
        for record in records
        if record.levelno >= logging.ERROR
        and record.__dict__.get("http_status") == http_status
    ]
    assert len(matches) == 1
    return matches[0]


def _extra_record_fields(record: logging.LogRecord) -> set[str]:
    return set(record.__dict__) - _BASE_LOG_RECORD_FIELDS - {"message", "asctime"}
