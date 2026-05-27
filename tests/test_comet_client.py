from typing import Any

import pytest
import requests

from sendai_pipeline.comet_client import (
    CometClient,
    CometConfigError,
    CometError,
    CometSettings,
    HistoryQuery,
)


class FakeResponse:
    def __init__(
        self,
        status_code: int,
        *,
        text: str = "",
        payload: Any = None,
    ) -> None:
        self.status_code = status_code
        self.text = text
        self._payload = payload

    def json(self) -> Any:
        return self._payload

    def raise_for_status(self) -> None:
        if not 200 <= self.status_code < 300:
            raise requests.HTTPError(self.text)


class FakeSession:
    def __init__(self, outcomes: list[FakeResponse]) -> None:
        self.outcomes = list(outcomes)
        self.calls: list[dict[str, Any]] = []

    def get(self, url: str, **kwargs: Any) -> FakeResponse:
        self.calls.append({"method": "GET", "url": url, **kwargs})
        return self.outcomes.pop(0)

    def delete(self, url: str, **kwargs: Any) -> FakeResponse:
        self.calls.append({"method": "DELETE", "url": url, **kwargs})
        return self.outcomes.pop(0)


class FakeAuth:
    def __init__(
        self,
        *,
        tokens: list[str] | None = None,
        refresh_tokens: list[str] | None = None,
    ) -> None:
        self.tokens = tokens or ["token"]
        self.refresh_tokens = refresh_tokens or ["refreshed-token"]
        self.calls: list[bool] = []

    def get_token(self, *, force_refresh: bool = False) -> str:
        self.calls.append(force_refresh)
        if force_refresh:
            return self.refresh_tokens[0]
        return self.tokens[0]


def make_settings(**overrides: Any) -> CometSettings:
    values = {
        "base_url": "https://fiware.example.test/",
        "service": "sendai",
        "service_path": "/",
        "verify_tls": True,
        "timeout": 3.5,
    }
    values.update(overrides)
    return CometSettings(**values)


def make_client(
    session: FakeSession,
    *,
    settings: CometSettings | None = None,
    auth: FakeAuth | None = None,
) -> CometClient:
    return CometClient(
        settings or make_settings(),
        auth=auth or FakeAuth(),
        session=session,
    )


def test_error_classes_have_config_subclass() -> None:
    assert issubclass(CometConfigError, CometError)


def test_settings_from_env_requires_base_url() -> None:
    with pytest.raises(CometConfigError, match="FIWARE_BASE_URL"):
        CometSettings.from_env({})


def test_settings_from_env_normalizes_base_url_and_defaults() -> None:
    settings = CometSettings.from_env({"FIWARE_BASE_URL": "https://fiware.test///"})

    assert settings.base_url == "https://fiware.test"
    assert settings.service == ""
    assert settings.service_path == "/"
    assert settings.verify_tls is True
    assert settings.timeout == 10


def test_settings_from_env_reads_service_timeout_and_tls_flag() -> None:
    settings = CometSettings.from_env(
        {
            "FIWARE_BASE_URL": "https://fiware.test",
            "FIWARE_SERVICE": "sendai",
            "FIWARE_SERVICE_PATH": "/city",
            "FIWARE_VERIFY_TLS": "false",
            "FIWARE_TIMEOUT_SECONDS": "2.25",
        }
    )

    assert settings.service == "sendai"
    assert settings.service_path == "/city"
    assert settings.verify_tls is False
    assert settings.timeout == 2.25


def test_get_history_gets_attribute_history_with_query_params_and_headers() -> None:
    response_payload = {"contextResponses": []}
    session = FakeSession([FakeResponse(200, payload=response_payload)])
    client = make_client(session)

    result = client.get_history(
        "jp.sendai.Blesensor.per300.10",
        "Blesensor.per300",
        "peopleCount_immedate",
        query=HistoryQuery(
            last_n=20,
            date_from="2026-05-24T00:00:00+09:00",
            date_to="2026-05-24T01:00:00+09:00",
            h_limit=10,
            h_offset=5,
            aggr_method="sum",
            aggr_period="minute",
        ),
    )

    assert result == response_payload
    assert len(session.calls) == 1
    call = session.calls[0]
    assert call["method"] == "GET"
    assert (
        call["url"] == "https://fiware.example.test/comet/v1.0/"
        "contextEntities/type/Blesensor.per300/id/"
        "jp.sendai.Blesensor.per300.10/attributes/peopleCount_immedate"
    )
    assert call["params"] == {
        "lastN": 20,
        "dateFrom": "2026-05-24T00:00:00+09:00",
        "dateTo": "2026-05-24T01:00:00+09:00",
        "hLimit": 10,
        "hOffset": 5,
        "aggrMethod": "sum",
        "aggrPeriod": "minute",
    }
    assert call["headers"] == {
        "Authorization": "Bearer token",
        "Accept": "application/json",
        "Fiware-Service": "sendai",
        "Fiware-ServicePath": "/",
    }
    assert call["verify"] is True
    assert call["timeout"] == 3.5


def test_get_history_omits_empty_service_header_and_unset_params() -> None:
    session = FakeSession([FakeResponse(200, payload={})])
    client = make_client(session, settings=make_settings(service=""))

    assert client.get_history("entity-1", "Type", "attr") == {}

    call = session.calls[0]
    assert call["params"] == {}
    assert "Fiware-Service" not in call["headers"]
    assert call["headers"]["Fiware-ServicePath"] == "/"


def test_get_history_refreshes_token_once_after_unauthorized_response() -> None:
    response_payload = {"contextResponses": []}
    session = FakeSession(
        [
            FakeResponse(401, text="expired"),
            FakeResponse(200, payload=response_payload),
        ]
    )
    auth = FakeAuth(tokens=["expired-token"], refresh_tokens=["fresh-token"])
    client = make_client(session, auth=auth)

    assert client.get_history("entity-1", "Type", "attr") == response_payload

    assert auth.calls == [False, True]
    assert [c["headers"]["Authorization"] for c in session.calls] == [
        "Bearer expired-token",
        "Bearer fresh-token",
    ]


def test_get_history_raises_for_non_success_response() -> None:
    session = FakeSession([FakeResponse(404, text="missing history")])
    client = make_client(session)

    with pytest.raises(requests.HTTPError, match="missing history"):
        client.get_history("missing", "Type", "attr")


def test_delete_attribute_history_returns_success_status() -> None:
    session = FakeSession([FakeResponse(204)])
    client = make_client(session)

    assert client.delete_attribute_history("entity-1", "Type", "attr") == 204


def test_delete_attribute_history_returns_absent_status() -> None:
    session = FakeSession([FakeResponse(404, text="missing history")])
    client = make_client(session)

    assert client.delete_attribute_history("entity-1", "Type", "attr") == 404


def test_delete_attribute_history_raises_server_error() -> None:
    session = FakeSession([FakeResponse(500, text="comet failed")])
    client = make_client(session)

    with pytest.raises(requests.HTTPError, match="comet failed"):
        client.delete_attribute_history("entity-1", "Type", "attr")


def test_delete_attribute_history_raises_on_unexpected_success_code() -> None:
    # Swagger lists 204 as the only DELETE success; anything else 2xx is
    # unexpected and must NOT be silently treated as success.
    session = FakeSession([FakeResponse(200, text="unexpected success")])
    client = make_client(session)

    with pytest.raises(
        requests.HTTPError, match="unexpected STH-Comet DELETE status 200"
    ):
        client.delete_attribute_history("entity-1", "Type", "attr")


def test_delete_attribute_history_retries_unauthorized_once_then_succeeds() -> None:
    retry_session = FakeSession([FakeResponse(401), FakeResponse(204)])
    retry_auth = FakeAuth(tokens=["expired-token"], refresh_tokens=["fresh-token"])
    retry_client = make_client(retry_session, auth=retry_auth)

    assert retry_client.delete_attribute_history("entity-1", "Type", "attr") == 204
    assert retry_auth.calls == [False, True]
    assert [c["headers"]["Authorization"] for c in retry_session.calls] == [
        "Bearer expired-token",
        "Bearer fresh-token",
    ]


def test_delete_attribute_history_propagates_second_unauthorized_response() -> None:
    failed_session = FakeSession(
        [FakeResponse(401, text="expired"), FakeResponse(401, text="still expired")]
    )
    failed_auth = FakeAuth(tokens=["expired-token"], refresh_tokens=["fresh-token"])
    failed_client = make_client(failed_session, auth=failed_auth)

    with pytest.raises(requests.HTTPError, match="still expired"):
        failed_client.delete_attribute_history("entity-1", "Type", "attr")
    assert failed_auth.calls == [False, True]


def test_delete_attribute_history_sends_attribute_path_url() -> None:
    session = FakeSession([FakeResponse(204)])
    client = make_client(session)

    client.delete_attribute_history(
        "jp.sendai.Blesensor.per300.10",
        "Blesensor.per300",
        "peopleCount_immedate",
    )

    assert session.calls[0]["method"] == "DELETE"
    assert (
        session.calls[0]["url"] == "https://fiware.example.test/comet/v1.0/"
        "contextEntities/type/Blesensor.per300/id/"
        "jp.sendai.Blesensor.per300.10/attributes/peopleCount_immedate"
    )


def test_delete_attribute_history_sends_headers() -> None:
    session = FakeSession([FakeResponse(204)])
    client = make_client(session, settings=make_settings(service_path="/city"))

    client.delete_attribute_history("entity-1", "Type", "attr")

    assert session.calls[0]["headers"] == {
        "Authorization": "Bearer token",
        "Accept": "application/json",
        "Fiware-Service": "sendai",
        "Fiware-ServicePath": "/city",
    }


def test_delete_attribute_history_sends_no_query_params() -> None:
    session = FakeSession([FakeResponse(204)])
    client = make_client(session)

    client.delete_attribute_history("entity-1", "Type", "attr")

    assert session.calls[0].get("params") in (None, {})


def test_delete_entity_history_returns_success_status() -> None:
    session = FakeSession([FakeResponse(204)])
    client = make_client(session)

    assert client.delete_entity_history("entity-1", "Type") == 204


def test_delete_entity_history_returns_absent_status() -> None:
    session = FakeSession([FakeResponse(404, text="missing history")])
    client = make_client(session)

    assert client.delete_entity_history("entity-1", "Type") == 404


def test_delete_entity_history_raises_server_error() -> None:
    session = FakeSession([FakeResponse(500, text="comet failed")])
    client = make_client(session)

    with pytest.raises(requests.HTTPError, match="comet failed"):
        client.delete_entity_history("entity-1", "Type")


def test_delete_entity_history_raises_on_unexpected_success_code() -> None:
    session = FakeSession([FakeResponse(202, text="accepted but unexpected")])
    client = make_client(session)

    with pytest.raises(
        requests.HTTPError, match="unexpected STH-Comet DELETE status 202"
    ):
        client.delete_entity_history("entity-1", "Type")


def test_delete_entity_history_retries_unauthorized_once_then_succeeds() -> None:
    retry_session = FakeSession([FakeResponse(401), FakeResponse(204)])
    retry_auth = FakeAuth(tokens=["expired-token"], refresh_tokens=["fresh-token"])
    retry_client = make_client(retry_session, auth=retry_auth)

    assert retry_client.delete_entity_history("entity-1", "Type") == 204
    assert retry_auth.calls == [False, True]
    assert [c["headers"]["Authorization"] for c in retry_session.calls] == [
        "Bearer expired-token",
        "Bearer fresh-token",
    ]


def test_delete_entity_history_propagates_second_unauthorized_response() -> None:
    failed_session = FakeSession(
        [FakeResponse(401, text="expired"), FakeResponse(401, text="still expired")]
    )
    failed_auth = FakeAuth(tokens=["expired-token"], refresh_tokens=["fresh-token"])
    failed_client = make_client(failed_session, auth=failed_auth)

    with pytest.raises(requests.HTTPError, match="still expired"):
        failed_client.delete_entity_history("entity-1", "Type")
    assert failed_auth.calls == [False, True]


def test_delete_entity_history_sends_entity_path_url() -> None:
    session = FakeSession([FakeResponse(204)])
    client = make_client(session)

    client.delete_entity_history(
        "jp.sendai.Blesensor.per300.10",
        "Blesensor.per300",
    )

    assert session.calls[0]["method"] == "DELETE"
    assert (
        session.calls[0]["url"] == "https://fiware.example.test/comet/v1.0/"
        "contextEntities/type/Blesensor.per300/id/jp.sendai.Blesensor.per300.10"
    )


def test_delete_entity_history_sends_headers() -> None:
    session = FakeSession([FakeResponse(204)])
    client = make_client(session, settings=make_settings(service_path="/city"))

    client.delete_entity_history("entity-1", "Type")

    assert session.calls[0]["headers"] == {
        "Authorization": "Bearer token",
        "Accept": "application/json",
        "Fiware-Service": "sendai",
        "Fiware-ServicePath": "/city",
    }


def test_delete_entity_history_sends_no_query_params() -> None:
    session = FakeSession([FakeResponse(204)])
    client = make_client(session)

    client.delete_entity_history("entity-1", "Type")

    assert session.calls[0].get("params") in (None, {})
