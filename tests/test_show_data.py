import importlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
import requests

ENTITY_10 = "jp.sendai.Blesensor.per3600.10"
ENTITY_10_PER300 = "jp.sendai.Blesensor.per300.10"
ENTITY_11 = "jp.sendai.Blesensor.per3600.11"
ENTITY_14_OUTSIDE_DIRECTION_BATCH = "jp.sendai.Blesensor.per3600.14"
ENTITY_99 = "jp.sendai.Blesensor.per300.99"
TYPE_3600 = "Blesensor.per3600"
TYPE_300 = "Blesensor.per300"
AGGREGATE_ENTITY_ID = "custom.aggregate.entity"
AGGREGATE_ENTITY_TYPE = "Custom.aggregate.type"
AGGREGATE_ATTRS = [
    "dateObservedFrom",
    "dateObservedTo",
    "dateRetrieved",
    "identifcation",
    "peopleCount_flow_10",
    "peopleCount_flow_11",
    "peopleCount_flow_14",
]


class FakeAuth:
    def __init__(self, *_args: Any, **_kwargs: Any) -> None:
        self.calls: list[bool] = []

    def get_token(self, *, force_refresh: bool = False) -> str:
        self.calls.append(force_refresh)
        return "token"


class FakeOrionClient:
    def __init__(self, outcomes: list[Any]) -> None:
        self.outcomes = list(outcomes)
        self.calls: list[dict[str, Any]] = []

    def get_entity(
        self,
        entity_id: str,
        *,
        entity_type: str | None = None,
        attrs: str | None = None,
    ) -> dict[str, Any]:
        self.calls.append(
            {"entity_id": entity_id, "entity_type": entity_type, "attrs": attrs}
        )
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


class FakeCometClient:
    def __init__(self, outcomes: list[Any]) -> None:
        self.outcomes = list(outcomes)
        self.calls: list[dict[str, Any]] = []

    def get_history(
        self,
        entity_id: str,
        entity_type: str,
        attr: str,
        *,
        query: Any = None,
    ) -> dict[str, Any]:
        self.calls.append(
            {
                "entity_id": entity_id,
                "entity_type": entity_type,
                "attr": attr,
                "query": query,
            }
        )
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


@dataclass
class RuntimePatch:
    module: Any | None
    orion: FakeOrionClient
    comet: FakeCometClient


_ACTIVE_RUNTIME: RuntimePatch | None = None


@pytest.fixture
def metadata_path(tmp_path: Path) -> Path:
    path = tmp_path / "sensors.csv"
    path.write_text(
        "\n".join(
            [
                "place_number,batch,expected_device_type,interval_min,"
                "entity_type,entity_id,identifcation,active",
                f"10,2026,M5Stack,60,{TYPE_3600},{ENTITY_10},10,true",
                f"10,2026,M5Stack,5,{TYPE_300},{ENTITY_10_PER300},10,true",
                f"11,2026,M5Stack,60,{TYPE_3600},{ENTITY_11},11,true",
                f"99,2026,M5Stack,5,{TYPE_300},{ENTITY_99},99,true",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return path


@pytest.fixture
def aggregate_metadata_path(tmp_path: Path, metadata_path: Path) -> Path:
    path = tmp_path / "aggregate-sensors.csv"
    base_rows = metadata_path.read_text(encoding="utf-8").splitlines()
    path.write_text(
        "\n".join(
            [
                *base_rows,
                "14,2025,M5Stack,60,Blesensor.per3600,"
                f"{ENTITY_14_OUTSIDE_DIRECTION_BATCH},14,true",
                "12,2026,M5Stack,60,Blesensor.per3600,"
                "jp.sendai.Blesensor.per3600.12,12,false",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return path


@pytest.fixture
def runtime(
    monkeypatch: pytest.MonkeyPatch,
    metadata_path: Path,
) -> RuntimePatch:
    global _ACTIVE_RUNTIME

    orion = FakeOrionClient([])
    comet = FakeCometClient([])
    patch = RuntimePatch(module=None, orion=orion, comet=comet)

    monkeypatch.setenv("FIWARE_BASE_URL", "https://fiware.example.test")
    monkeypatch.setenv("FIWARE_SERVICE", "sendai")
    monkeypatch.setenv("FIWARE_SERVICE_PATH", "/")
    monkeypatch.setenv("SENSOR_METADATA_PATH", str(metadata_path))
    monkeypatch.setenv("TARGET_DIRECTION_BATCHES", "2026")
    monkeypatch.setenv("PRODUCT_B_AGGREGATE_ENTITY_ID", AGGREGATE_ENTITY_ID)
    monkeypatch.setenv("PRODUCT_B_AGGREGATE_ENTITY_TYPE", AGGREGATE_ENTITY_TYPE)
    _ACTIVE_RUNTIME = patch

    return patch


@pytest.fixture
def aggregate_runtime(
    monkeypatch: pytest.MonkeyPatch,
    aggregate_metadata_path: Path,
    runtime: RuntimePatch,
) -> RuntimePatch:
    monkeypatch.setenv("SENSOR_METADATA_PATH", str(aggregate_metadata_path))
    return runtime


def test_show_data_rejects_attrs_and_flow_attrs_together(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    result = _invoke(
        [
            "--source",
            "orion",
            "--type",
            TYPE_3600,
            "--entity-id",
            ENTITY_10,
            "--attrs",
            "dateObservedFrom",
            "--flow-attrs",
        ],
        capsys,
    )

    assert result != 0
    assert runtime.orion.calls == []


def test_show_data_rejects_place_and_entity_id_together(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    result = _invoke(
        [
            "--source",
            "orion",
            "--type",
            TYPE_3600,
            "--place",
            "10",
            "--entity-id",
            ENTITY_10,
        ],
        capsys,
    )

    assert result != 0
    assert runtime.orion.calls == []


def test_show_data_place_without_interval_min_resolves_both_intervals(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    runtime.orion.outcomes = [
        _orion_entity(ENTITY_10),
        _orion_entity(ENTITY_10_PER300, entity_type=TYPE_300),
    ]

    result = _invoke(
        ["--source", "orion", "--place", "10"],
        capsys,
    )

    assert result == 0
    assert [
        (call["entity_id"], call["entity_type"]) for call in runtime.orion.calls
    ] == [
        (ENTITY_10, TYPE_3600),
        (ENTITY_10_PER300, TYPE_300),
    ]


def _assert_orion_source_rejects_flag(
    flag: str,
    value: str,
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    result = _invoke(
        [
            "--source",
            "orion",
            "--type",
            TYPE_3600,
            "--entity-id",
            ENTITY_10,
            flag,
            value,
        ],
        capsys,
    )

    assert result != 0
    assert runtime.orion.calls == []


def test_show_data_orion_source_rejects_from(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    _assert_orion_source_rejects_flag(
        "--from", "2026-05-24T10:00:00+09:00", capsys, runtime
    )


def test_show_data_orion_source_rejects_to(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    _assert_orion_source_rejects_flag(
        "--to", "2026-05-24T11:00:00+09:00", capsys, runtime
    )


def test_show_data_orion_source_rejects_last_n(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    _assert_orion_source_rejects_flag("--last-n", "20", capsys, runtime)


def test_show_data_orion_source_rejects_h_limit(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    _assert_orion_source_rejects_flag("--h-limit", "10", capsys, runtime)


def test_show_data_orion_source_rejects_h_offset(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    _assert_orion_source_rejects_flag("--h-offset", "5", capsys, runtime)


def test_show_data_orion_source_rejects_aggr_method(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    _assert_orion_source_rejects_flag("--aggr-method", "sum", capsys, runtime)


def test_show_data_orion_source_rejects_aggr_period(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    _assert_orion_source_rejects_flag("--aggr-period", "minute", capsys, runtime)


def test_show_data_orion_emits_one_json_per_entity(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    runtime.orion.outcomes = [_orion_entity(ENTITY_10), _orion_entity(ENTITY_11)]

    result = _invoke(_orion_args(ENTITY_10, ENTITY_11), capsys)

    assert result == 0
    assert [call["entity_id"] for call in runtime.orion.calls] == [ENTITY_10, ENTITY_11]
    assert _json_objects(capsys.readouterr().out) == [
        _orion_entity(ENTITY_10),
        _orion_entity(ENTITY_11),
    ]


def test_show_data_orion_404_emits_not_found_record_exit_zero(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    runtime.orion.outcomes = [_http_error(404, "missing")]

    result = _invoke(_orion_args("missing-entity"), capsys)

    assert result == 0
    assert _json_objects(capsys.readouterr().out) == [
        {"entity_id": "missing-entity", "error": "not_found"}
    ]


def test_show_data_orion_500_exits_nonzero(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    runtime.orion.outcomes = [_http_error(500, "server failed")]

    result = _invoke(_orion_args(ENTITY_10), capsys)

    assert result != 0


def test_show_data_orion_uses_indent_2_sort_keys_true_in_default_mode(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    calls: list[dict[str, Any]] = []
    real_dumps = json.dumps
    runtime.orion.outcomes = [_orion_entity(ENTITY_10)]
    show_data = _show_data_module()
    _patch_show_data_module(show_data, runtime)

    def spy_dumps(obj: Any, **kwargs: Any) -> str:
        calls.append({"obj": obj, "kwargs": kwargs})
        return real_dumps(obj, **kwargs)

    monkeypatch.setattr(show_data.json, "dumps", spy_dumps)

    result = _invoke(_orion_args(ENTITY_10), capsys)

    assert result == 0
    assert calls == [
        {
            "obj": _orion_entity(ENTITY_10),
            "kwargs": {"ensure_ascii": False, "indent": 2, "sort_keys": True},
        }
    ]


def test_show_data_comet_emits_one_json_per_entity_attr_pair(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    runtime.comet.outcomes = [
        _history(entity_id, attr)
        for entity_id in (ENTITY_10, ENTITY_11)
        for attr in ("a", "b", "c")
    ]

    result = _invoke(
        _comet_args(ENTITY_10, ENTITY_11) + ["--attrs", "a,b,c"],
        capsys,
    )

    assert result == 0
    assert [(c["entity_id"], c["attr"]) for c in runtime.comet.calls] == [
        (entity_id, attr)
        for entity_id in (ENTITY_10, ENTITY_11)
        for attr in ("a", "b", "c")
    ]
    assert _json_objects(capsys.readouterr().out) == [
        _history(entity_id, attr)
        for entity_id in (ENTITY_10, ENTITY_11)
        for attr in ("a", "b", "c")
    ]


def test_show_data_comet_infers_type_from_bare_canonical_id(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    runtime.comet.outcomes = [_history(ENTITY_10, "a")]

    result = _invoke(
        ["--source", "comet", "--entity-id", ENTITY_10, "--attrs", "a"],
        capsys,
    )

    assert result == 0
    assert runtime.comet.calls[0]["entity_type"] == TYPE_3600


def test_show_data_comet_passes_through_last_n(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    runtime.comet.outcomes = [_history(ENTITY_10, "a")]

    result = _invoke(
        _comet_args(ENTITY_10) + ["--attrs", "a", "--last-n", "25"], capsys
    )

    assert result == 0
    assert _query_value(runtime.comet.calls[0]["query"], "last_n") == 25


def test_show_data_comet_passes_through_date_from_and_date_to(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    runtime.comet.outcomes = [_history(ENTITY_10, "a")]

    result = _invoke(
        _comet_args(ENTITY_10)
        + [
            "--attrs",
            "a",
            "--from",
            "2026-05-24T10:00:00+09:00",
            "--to",
            "2026-05-24T11:00:00+09:00",
        ],
        capsys,
    )

    assert result == 0
    query = runtime.comet.calls[0]["query"]
    assert _query_value(query, "date_from") == "2026-05-24T10:00:00+09:00"
    assert _query_value(query, "date_to") == "2026-05-24T11:00:00+09:00"


def test_show_data_comet_converts_window_key_to_jst_iso(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    runtime.comet.outcomes = [_history(ENTITY_10, "a")]

    result = _invoke(
        _comet_args(ENTITY_10)
        + [
            "--attrs",
            "a",
            "--from",
            "20260524_1000",
            "--to",
            "20260524_1100",
        ],
        capsys,
    )

    assert result == 0
    query = runtime.comet.calls[0]["query"]
    assert _query_value(query, "date_from") == "2026-05-24T10:00:00+09:00"
    assert _query_value(query, "date_to") == "2026-05-24T11:00:00+09:00"


def test_show_data_comet_rejects_unparseable_from_value(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    result = _invoke(
        _comet_args(ENTITY_10) + ["--attrs", "a", "--from", "not-a-date"],
        capsys,
    )

    assert result == 2
    err = capsys.readouterr().err
    assert "--from" in err and "not-a-date" in err


def test_show_data_comet_passes_through_h_limit_and_h_offset(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    runtime.comet.outcomes = [_history(ENTITY_10, "a")]

    result = _invoke(
        _comet_args(ENTITY_10) + ["--attrs", "a", "--h-limit", "10", "--h-offset", "5"],
        capsys,
    )

    assert result == 0
    query = runtime.comet.calls[0]["query"]
    assert _query_value(query, "h_limit") == 10
    assert _query_value(query, "h_offset") == 5


def test_show_data_comet_passes_through_aggr_method_and_aggr_period(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    runtime.comet.outcomes = [_history(ENTITY_10, "a")]

    result = _invoke(
        _comet_args(ENTITY_10)
        + ["--attrs", "a", "--aggr-method", "sum", "--aggr-period", "minute"],
        capsys,
    )

    assert result == 0
    query = runtime.comet.calls[0]["query"]
    assert _query_value(query, "aggr_method") == "sum"
    assert _query_value(query, "aggr_period") == "minute"


def test_show_data_comet_404_emits_not_found_record_exit_zero(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    runtime.comet.outcomes = [_http_error(404, "missing")]

    result = _invoke(
        _comet_args(ENTITY_10) + ["--attrs", "peopleCount_immedate"], capsys
    )

    assert result == 0
    assert _json_objects(capsys.readouterr().out) == [
        {
            "attr": "peopleCount_immedate",
            "entity_id": ENTITY_10,
            "error": "not_found",
        }
    ]


def test_show_data_comet_aggregate_enumerates_contract_and_active_interval_60_attrs(
    capsys: pytest.CaptureFixture[str],
    aggregate_runtime: RuntimePatch,
) -> None:
    aggregate_runtime.comet.outcomes = [
        _history(AGGREGATE_ENTITY_ID, attr) for attr in AGGREGATE_ATTRS
    ]

    result = _invoke(_aggregate_comet_args(), capsys)

    assert result == 0
    assert [call["attr"] for call in aggregate_runtime.comet.calls] == AGGREGATE_ATTRS


def test_show_data_comet_aggregate_reports_missing_attr_and_continues(
    capsys: pytest.CaptureFixture[str],
    aggregate_runtime: RuntimePatch,
) -> None:
    aggregate_runtime.comet.outcomes = [
        _history(AGGREGATE_ENTITY_ID, "dateObservedFrom"),
        _http_error(404, "missing"),
        *[_history(AGGREGATE_ENTITY_ID, attr) for attr in AGGREGATE_ATTRS[2:]],
    ]

    result = _invoke(_aggregate_comet_args(), capsys)

    assert result == 0
    assert [call["attr"] for call in aggregate_runtime.comet.calls] == AGGREGATE_ATTRS
    assert _json_objects(capsys.readouterr().out)[1] == {
        "attr": "dateObservedTo",
        "entity_id": AGGREGATE_ENTITY_ID,
        "error": "not_found",
    }


def test_show_data_comet_aggregate_id_auto_resolves_type_from_env(
    capsys: pytest.CaptureFixture[str],
    aggregate_runtime: RuntimePatch,
) -> None:
    aggregate_runtime.comet.outcomes = [
        _history(AGGREGATE_ENTITY_ID, attr) for attr in AGGREGATE_ATTRS
    ]

    result = _invoke(
        ["--source", "comet", "--entity-id", AGGREGATE_ENTITY_ID],
        capsys,
    )

    assert result == 0
    assert [call["attr"] for call in aggregate_runtime.comet.calls] == AGGREGATE_ATTRS
    assert [call["entity_type"] for call in aggregate_runtime.comet.calls] == [
        AGGREGATE_ENTITY_TYPE
    ] * len(AGGREGATE_ATTRS)


def test_show_data_comet_aggregate_enumeration_requires_explicit_id(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    result = _invoke(
        ["--source", "comet", "--type", AGGREGATE_ENTITY_TYPE],
        capsys,
    )

    assert result == 2
    assert runtime.comet.calls == []


def test_show_data_comet_non_aggregate_requires_attrs_selection(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    result = _invoke(_comet_args(ENTITY_10), capsys)

    assert result != 0
    assert runtime.comet.calls == []


def test_show_data_comet_explicit_attrs_reads_inactive_historical_place(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    attr = "peopleCount_flow_99"
    runtime.comet.outcomes = [_history(AGGREGATE_ENTITY_ID, attr)]

    result = _invoke(_aggregate_comet_args() + ["--attrs", attr], capsys)

    assert result == 0
    assert [call["attr"] for call in runtime.comet.calls] == [attr]


def test_show_data_product_a_read_ignores_malformed_product_b_config(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    # A malformed Product B aggregate config must not fail a Product A read.
    # Reading a canonical Product A id (no --type) infers the type from the id
    # and never resolves PRODUCT_B_AGGREGATE_*, so the bad values are ignored.
    monkeypatch.setenv("PRODUCT_B_AGGREGATE_ENTITY_ID", " malformed")
    monkeypatch.setenv("PRODUCT_B_AGGREGATE_ENTITY_TYPE", "")
    runtime.comet.outcomes = [_history(ENTITY_10, "peopleCount_immedate")]

    result = _invoke(
        [
            "--source",
            "comet",
            "--entity-id",
            ENTITY_10,
            "--attrs",
            "peopleCount_immedate",
        ],
        capsys,
    )

    assert result == 0
    assert runtime.comet.calls[0]["entity_id"] == ENTITY_10
    assert runtime.comet.calls[0]["entity_type"] == TYPE_3600


def test_show_data_aggregate_read_still_rejects_malformed_product_b_config(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    # Reading the aggregate entity (non-canonical) still resolves and validates
    # the Product B config, so a malformed type is a config error (exit 2), not
    # a silent pass.
    monkeypatch.setenv("PRODUCT_B_AGGREGATE_ENTITY_TYPE", " malformed")

    result = _invoke(["--source", "comet", "--entity-id", AGGREGATE_ENTITY_ID], capsys)

    assert result == 2
    assert runtime.comet.calls == []


def test_show_data_comet_canonical_shaped_aggregate_override_uses_configured_type(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    aggregate_runtime: RuntimePatch,
) -> None:
    # The aggregate id may be overridden to a canonical-shaped value. Such an
    # id must still get the CONFIGURED Product B type and aggregate attribute
    # enumeration, not the type embedded in the id (here "Blesensor.aggregate").
    canonical_shaped_id = "jp.sendai.Blesensor.aggregate.999"
    monkeypatch.setenv("PRODUCT_B_AGGREGATE_ENTITY_ID", canonical_shaped_id)
    monkeypatch.setenv("PRODUCT_B_AGGREGATE_ENTITY_TYPE", "Blesensor.flow")
    aggregate_runtime.comet.outcomes = [
        _history(canonical_shaped_id, attr) for attr in AGGREGATE_ATTRS
    ]

    result = _invoke(["--source", "comet", "--entity-id", canonical_shaped_id], capsys)

    assert result == 0
    assert [call["attr"] for call in aggregate_runtime.comet.calls] == AGGREGATE_ATTRS
    assert [call["entity_type"] for call in aggregate_runtime.comet.calls] == [
        "Blesensor.flow"
    ] * len(AGGREGATE_ATTRS)


def test_show_data_comet_urn_shaped_aggregate_override_is_not_colon_split(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    aggregate_runtime: RuntimePatch,
) -> None:
    # A URN aggregate id contains colons. Passing it as a bare id must match the
    # configured id (and enumerate aggregate attrs with the configured type),
    # not be mis-split into id "urn:ngsi-ld:Blesensor.flow" + inline type
    # "Sendai".
    urn_id = "urn:ngsi-ld:Blesensor.flow:Sendai"
    monkeypatch.setenv("PRODUCT_B_AGGREGATE_ENTITY_ID", urn_id)
    monkeypatch.setenv("PRODUCT_B_AGGREGATE_ENTITY_TYPE", "Blesensor.flow")
    aggregate_runtime.comet.outcomes = [
        _history(urn_id, attr) for attr in AGGREGATE_ATTRS
    ]

    result = _invoke(["--source", "comet", "--entity-id", urn_id], capsys)

    assert result == 0
    assert [call["entity_id"] for call in aggregate_runtime.comet.calls] == [
        urn_id
    ] * len(AGGREGATE_ATTRS)
    assert [call["attr"] for call in aggregate_runtime.comet.calls] == AGGREGATE_ATTRS
    assert [call["entity_type"] for call in aggregate_runtime.comet.calls] == [
        "Blesensor.flow"
    ] * len(AGGREGATE_ATTRS)


def test_show_data_orion_product_a_read_ignores_malformed_product_b_config(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    # The Orion path must also tolerate malformed Product B config for a
    # Product A read: a canonical id resolves its type without consulting it.
    monkeypatch.setenv("PRODUCT_B_AGGREGATE_ENTITY_ID", " malformed")
    monkeypatch.setenv("PRODUCT_B_AGGREGATE_ENTITY_TYPE", "")
    runtime.orion.outcomes = [_orion_entity(ENTITY_10)]

    result = _invoke(["--source", "orion", "--entity-id", ENTITY_10], capsys)

    assert result == 0
    assert runtime.orion.calls[0]["entity_id"] == ENTITY_10
    assert runtime.orion.calls[0]["entity_type"] == TYPE_3600


def test_show_data_place_resolves_to_entity_id_via_metadata(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    runtime.orion.outcomes = [_orion_entity(ENTITY_10)]

    result = _invoke(
        ["--source", "orion", "--place", "10", "--interval-min", "60"],
        capsys,
    )

    assert result == 0
    assert runtime.orion.calls[0]["entity_id"] == ENTITY_10
    assert runtime.orion.calls[0]["entity_type"] == TYPE_3600


def test_show_data_place_without_interval_min_errors_only_when_no_active_row(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    result = _invoke(["--source", "orion", "--place", "999"], capsys)

    assert result != 0
    assert runtime.orion.calls == []


def test_show_data_multiple_places_resolve_to_multiple_entities(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    runtime.orion.outcomes = [_orion_entity(ENTITY_10), _orion_entity(ENTITY_11)]

    result = _invoke(
        [
            "--source",
            "orion",
            "--place",
            "10",
            "--place",
            "11",
            "--interval-min",
            "60",
        ],
        capsys,
    )

    assert result == 0
    assert [call["entity_id"] for call in runtime.orion.calls] == [ENTITY_10, ENTITY_11]


def test_show_data_entity_id_with_inline_type_overrides_default_type(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    runtime.orion.outcomes = [_orion_entity(ENTITY_99, entity_type=TYPE_300)]

    result = _invoke(
        [
            "--source",
            "orion",
            "--type",
            TYPE_3600,
            "--entity-id",
            f"{ENTITY_99}:{TYPE_300}",
        ],
        capsys,
    )

    assert result == 0
    assert runtime.orion.calls[0]["entity_id"] == ENTITY_99
    assert runtime.orion.calls[0]["entity_type"] == TYPE_300


def test_show_data_entity_id_without_inline_type_uses_default_type(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    runtime.orion.outcomes = [_orion_entity(ENTITY_10)]

    result = _invoke(_orion_args(ENTITY_10), capsys)

    assert result == 0
    assert runtime.orion.calls[0]["entity_type"] == TYPE_3600


def test_show_data_default_type_overrides_inferred_entity_type(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    runtime.orion.outcomes = [_orion_entity(ENTITY_10, entity_type=TYPE_300)]

    result = _invoke(
        ["--source", "orion", "--type", TYPE_300, "--entity-id", ENTITY_10],
        capsys,
    )

    assert result == 0
    assert runtime.orion.calls[0]["entity_type"] == TYPE_300


def test_show_data_flow_attrs_expands_to_seven_product_a_attributes(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    runtime.orion.outcomes = [_orion_entity(ENTITY_10)]

    result = _invoke(_orion_args(ENTITY_10) + ["--flow-attrs"], capsys)

    assert result == 0
    assert runtime.orion.calls[0]["attrs"] == ",".join(
        [
            "dateObservedFrom",
            "dateObservedTo",
            "peopleCount_immedate",
            "peopleCount_near",
            "peopleCount_far",
            "peopleOccupancy_immedate",
            "peopleOccupancy_near",
        ]
    )


def test_show_data_rejects_removed_direction_attrs_flag(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    runtime.comet.outcomes = [
        _history(ENTITY_10, attr)
        for attr in ("dateObservedFrom", "dateObservedTo", "peopleCount_flow")
    ]

    result = _invoke(_comet_args(ENTITY_10) + ["--direction-attrs"], capsys)

    assert result == 2
    assert "unrecognized arguments: --direction-attrs" in capsys.readouterr().err
    assert runtime.comet.calls == []


def test_show_data_pretty_renders_orion_table_with_columns(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    runtime.orion.outcomes = [_orion_entity(ENTITY_10)]

    result = _invoke(
        _orion_args(ENTITY_10) + ["--attrs", "peopleCount_immedate", "--pretty"],
        capsys,
    )

    out = capsys.readouterr().out
    assert result == 0
    assert "entity" in out
    assert "attr" in out
    assert "value" in out
    assert "time" in out
    assert ENTITY_10 in out
    assert "peopleCount_immedate" in out
    assert "42" in out
    assert "2026-05-24T10:00:00+09:00" in out


def test_show_data_pretty_renders_comet_table_with_history_rows(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    runtime.comet.outcomes = [_history(ENTITY_10, "peopleCount_immedate")]

    result = _invoke(
        _comet_args(ENTITY_10) + ["--attrs", "peopleCount_immedate", "--pretty"],
        capsys,
    )

    out = capsys.readouterr().out
    assert result == 0
    assert ENTITY_10 in out
    assert "peopleCount_immedate" in out
    assert "41" in out
    assert "42" in out
    assert "2026-05-24T09:55:00+09:00" in out
    assert "2026-05-24T10:00:00+09:00" in out


def test_show_data_pretty_renders_null_value_as_text_null(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    runtime.orion.outcomes = [_orion_entity(ENTITY_10, value=None)]

    result = _invoke(
        _orion_args(ENTITY_10) + ["--attrs", "peopleCount_immedate", "--pretty"],
        capsys,
    )

    assert result == 0
    assert "null" in capsys.readouterr().out


def test_show_data_pretty_renders_not_found_row_for_404(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    runtime.orion.outcomes = [_http_error(404, "missing")]

    result = _invoke(_orion_args("missing-entity") + ["--pretty"], capsys)

    out = capsys.readouterr().out
    assert result == 0
    assert "missing-entity" in out
    assert "(not found)" in out


def test_show_data_pretty_orders_comet_groups_by_window_when_envelope_present(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    runtime.comet.outcomes = [
        {
            "contextResponses": [
                {
                    "contextElement": {
                        "id": ENTITY_10,
                        "type": TYPE_3600,
                        "attributes": [
                            {
                                "name": "dateObservedFrom",
                                "values": [
                                    {
                                        "recvTime": "2026-05-24T11:00:00+09:00",
                                        "attrValue": "2026-05-24T09:00:00+09:00",
                                    },
                                    {
                                        "recvTime": "2026-05-24T10:00:00+09:00",
                                        "attrValue": "2026-05-24T10:00:00+09:00",
                                    },
                                ],
                            }
                        ],
                    },
                    "statusCode": {"code": "200", "reasonPhrase": "OK"},
                }
            ]
        },
        {
            "contextResponses": [
                {
                    "contextElement": {
                        "id": ENTITY_10,
                        "type": TYPE_3600,
                        "attributes": [
                            {
                                "name": "peopleCount_immedate",
                                "values": [
                                    {
                                        "recvTime": "2026-05-24T11:00:00+09:00",
                                        "attrValue": 9,
                                    },
                                    {
                                        "recvTime": "2026-05-24T10:00:00+09:00",
                                        "attrValue": 5,
                                    },
                                ],
                            }
                        ],
                    },
                    "statusCode": {"code": "200", "reasonPhrase": "OK"},
                }
            ]
        },
    ]

    result = _invoke(
        _comet_args(ENTITY_10)
        + ["--attrs", "dateObservedFrom,peopleCount_immedate", "--pretty"],
        capsys,
    )

    out = capsys.readouterr().out
    assert result == 0
    body_lines = [line for line in out.splitlines() if ENTITY_10 in line]
    attrs_and_values = [
        (line.split()[1].strip(), line.split()[2].strip()) for line in body_lines
    ]
    # The recvTime=11:00 group carries the earlier window (09:00) and must
    # render first even though its recvTime is later.
    assert attrs_and_values == [
        ("dateObservedFrom", "2026-05-24T09:00:00+09:00"),
        ("peopleCount_immedate", "9"),
        ("dateObservedFrom", "2026-05-24T10:00:00+09:00"),
        ("peopleCount_immedate", "5"),
    ]


def test_show_data_pretty_sorts_comet_rows_by_time_then_attr_within_entity(
    capsys: pytest.CaptureFixture[str],
    runtime: RuntimePatch,
) -> None:
    runtime.comet.outcomes = [
        _history(ENTITY_10, "peopleCount_near"),
        _history(ENTITY_10, "peopleCount_immedate"),
    ]

    result = _invoke(
        _comet_args(ENTITY_10)
        + ["--attrs", "peopleCount_near,peopleCount_immedate", "--pretty"],
        capsys,
    )

    out = capsys.readouterr().out
    assert result == 0
    body_lines = [line for line in out.splitlines() if ENTITY_10 in line]
    times_and_attrs = [
        (line.split()[3], line.split()[1].strip()) for line in body_lines
    ]
    assert times_and_attrs == [
        ("2026-05-24T09:55:00+09:00", "peopleCount_immedate"),
        ("2026-05-24T09:55:00+09:00", "peopleCount_near"),
        ("2026-05-24T10:00:00+09:00", "peopleCount_immedate"),
        ("2026-05-24T10:00:00+09:00", "peopleCount_near"),
    ]


def _invoke(
    argv: list[str],
    capsys: pytest.CaptureFixture[str],
) -> int:
    show_data = _show_data_module()
    if _ACTIVE_RUNTIME is not None:
        _patch_show_data_module(show_data, _ACTIVE_RUNTIME)
    try:
        result = show_data.main(argv)
    except SystemExit as exc:
        result = exc.code
    if result is None:
        return 0
    if isinstance(result, int):
        return result
    return int(result)


def _show_data_module() -> Any:
    return importlib.import_module("scripts.show_data")


# NOTE: These tests patch client classes directly on scripts.show_data. The
# implementation should import AuthClient, OrionClient, CometClient, and their
# settings classes into module-level names instead of dereferencing package
# modules at call sites, so these fakes replace every network-facing client.
def _patch_show_data_module(show_data: Any, runtime: RuntimePatch) -> None:
    runtime.module = show_data
    show_data.AuthClient = FakeAuth
    show_data.AuthSettings = type(
        "FakeAuthSettings", (), {"from_env": staticmethod(lambda: object())}
    )
    show_data.OrionSettings = type(
        "FakeOrionSettings", (), {"from_env": staticmethod(lambda: object())}
    )
    show_data.CometSettings = type(
        "FakeCometSettings", (), {"from_env": staticmethod(lambda: object())}
    )
    show_data.OrionClient = lambda *_args, **_kwargs: runtime.orion
    show_data.CometClient = lambda *_args, **_kwargs: runtime.comet
    show_data.load_dotenv = lambda *_args, **_kwargs: None
    show_data.find_dotenv = lambda *_args, **_kwargs: ""


def _orion_args(*entity_ids: str) -> list[str]:
    args = ["--source", "orion", "--type", TYPE_3600]
    for entity_id in entity_ids:
        args.extend(["--entity-id", entity_id])
    return args


def _comet_args(*entity_ids: str) -> list[str]:
    args = ["--source", "comet", "--type", TYPE_3600]
    for entity_id in entity_ids:
        args.extend(["--entity-id", entity_id])
    return args


def _aggregate_comet_args() -> list[str]:
    return [
        "--source",
        "comet",
        "--type",
        AGGREGATE_ENTITY_TYPE,
        "--entity-id",
        AGGREGATE_ENTITY_ID,
    ]


def _orion_entity(
    entity_id: str,
    *,
    entity_type: str = TYPE_3600,
    value: int | None = 42,
) -> dict[str, Any]:
    return {
        "id": entity_id,
        "type": entity_type,
        "peopleCount_immedate": {
            "type": "Number",
            "value": value,
            "metadata": {
                "TimeInstant": {
                    "type": "DateTime",
                    "value": "2026-05-24T10:00:00+09:00",
                }
            },
        },
    }


def _history(entity_id: str, attr: str) -> dict[str, Any]:
    return {
        "contextResponses": [
            {
                "contextElement": {
                    "id": entity_id,
                    "type": TYPE_3600,
                    "attributes": [
                        {
                            "name": attr,
                            "values": [
                                {
                                    "recvTime": "2026-05-24T09:55:00+09:00",
                                    "attrValue": 41,
                                },
                                {
                                    "recvTime": "2026-05-24T10:00:00+09:00",
                                    "attrValue": 42,
                                },
                            ],
                        }
                    ],
                },
                "statusCode": {"code": "200", "reasonPhrase": "OK"},
            }
        ]
    }


def _http_error(status_code: int, text: str) -> requests.HTTPError:
    response = requests.Response()
    response.status_code = status_code
    response._content = text.encode("utf-8")
    exc = requests.HTTPError(text)
    exc.response = response
    return exc


def _json_objects(output: str) -> list[Any]:
    decoder = json.JSONDecoder()
    index = 0
    objects = []
    while index < len(output):
        while index < len(output) and output[index].isspace():
            index += 1
        if index >= len(output):
            break
        obj, index = decoder.raw_decode(output, index)
        objects.append(obj)
    return objects


def _query_value(query: Any, name: str) -> Any:
    if isinstance(query, dict):
        return query[name]
    return getattr(query, name)
