from importlib import import_module
from types import SimpleNamespace
from typing import Any

import pytest


def _module() -> Any:
    return import_module("scripts.dev.probe_sth_subscription")


class FakeResponse:
    def __init__(
        self,
        status_code: int,
        *,
        body: Any = None,
        text: str = "",
    ) -> None:
        self.status_code = status_code
        self.ok = 200 <= status_code < 300
        self._body = body
        self.text = text

    def json(self) -> Any:
        if isinstance(self._body, BaseException):
            raise self._body
        return self._body


class FakeClient:
    def __init__(self, responses: list[FakeResponse]) -> None:
        self.responses = list(responses)
        self.calls: list[tuple[str, str, Any]] = []

    def request(
        self,
        method: str,
        path: str,
        *,
        params: Any = None,
        body: Any = None,
    ) -> FakeResponse:
        del body
        self.calls.append((method, path, params))
        return self.responses.pop(0)


def _config() -> Any:
    return SimpleNamespace(
        entity_id="sendai.pipeline.probe.test",
        entity_type="SendaiPipelineProbe",
        attr="probeValue",
        history_last_n=10,
    )


@pytest.mark.parametrize(
    ("status", "body", "expected"),
    [
        (204, "", "delete_succeeded"),
        (404, "<am:fault>missing route</am:fault>", "gateway_route_missing"),
        (404, "not found", "backend_not_found"),
        (405, "", "gateway_method_not_allowed"),
        (403, "", "backend_forbidden"),
        (500, "", "unexpected_status"),
    ],
)
def test_delete_status_interpretation_reports_endpoint_outcome(
    status: int,
    body: str,
    expected: str,
) -> None:
    module = _module()

    assert module._delete_status_interpretation(status, body) == expected


def test_probe_history_delete_rechecks_history_and_verifies_removal() -> None:
    module = _module()
    client = FakeClient(
        [
            FakeResponse(204),
            FakeResponse(200, body={"values": []}),
        ]
    )

    result = module._probe_history_delete(
        client,
        _config(),
        history_before=2,
    )

    assert result["attempts"][0]["status"] == 204
    assert result["interpretation"] == "delete_succeeded"
    assert result["verification"] == {
        "status": 200,
        "ok": True,
        "body_excerpt": "",
        "values_before": 2,
        "values_after": 0,
        "effect": "verified_removed",
    }
    assert [call[0] for call in client.calls] == ["DELETE", "GET"]


def test_probe_history_delete_reports_inconclusive_failed_history_read() -> None:
    module = _module()
    client = FakeClient(
        [
            FakeResponse(405),
            FakeResponse(500, text="unexpected DELETE response"),
            FakeResponse(500, text="backend unavailable"),
        ]
    )

    result = module._probe_history_delete(
        client,
        _config(),
        history_before=2,
    )

    assert len(result["attempts"]) == 2
    assert result["interpretation"] == "unexpected_status"
    assert result["verification"]["effect"] == "inconclusive_history_read_failed"
    assert [call[0] for call in client.calls] == ["DELETE", "DELETE", "GET"]


def test_probe_history_delete_reports_inconclusive_without_prior_history() -> None:
    module = _module()
    client = FakeClient(
        [
            FakeResponse(204),
            FakeResponse(404, body={}),
        ]
    )

    result = module._probe_history_delete(
        client,
        _config(),
        history_before=0,
    )

    assert result["verification"]["values_after"] == 0
    assert result["verification"]["effect"] == "inconclusive_no_prior_history"
