"""Offline coverage for fail-closed discovery and ETL-required sales columns."""
import json
import re
from unittest.mock import patch

import pytest
import requests
from hotglue_singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from hotglue_etl_exceptions import InvalidCredentialsError
from jsonschema import validate

from tap_netsuite_rest.streams import InvoicesStream, ItemStream, SalesOrdersStream, SalesOrderLinesStream
from tap_netsuite_rest.tap import TapNetSuite

CONFIG = {
    "ns_account": "example",
    "ns_consumer_key": "test",
    "ns_consumer_secret": "test",
    "ns_token_key": "test",
    "ns_token_secret": "test",
    "remove_unauthorized_streams": False,
}
MINIMUM = {
    SalesOrdersStream: {"id", "foreigntotal", "createddate", "closedate", "tranid"},
    SalesOrderLinesStream: {"transaction", "uniquekey", "item", "quantity", "netamount"},
}
RECORDS = {
    SalesOrdersStream: {
        "id": "123", "foreigntotal": "-11.50", "createddate": "2026-10-01T12:00:00Z",
        "closedate": None, "tranid": "SO123",
    },
    SalesOrderLinesStream: {
        "transaction": "123", "uniquekey": "9007199254740993", "item": "42",
        "quantity": 0.25, "netamount": "-11.50",
    },
}


def response(body, status=200):
    result = requests.Response()
    result.status_code = status
    result._content = json.dumps(body).encode()
    return result


def catalog_for(stream_type, missing=()):
    properties = {field: {"type": ["string", "null"]}
                  for field in MINIMUM[stream_type] - set(missing)}
    if "quantity" in properties:
        properties["quantity"] = {"type": ["number", "null"]}
    return {"streams": [{
        "tap_stream_id": stream_type.name,
        "stream": stream_type.name,
        "schema": {"type": "object", "properties": properties},
        "key_properties": [],
        "metadata": [{"breadcrumb": [], "metadata": {"selected": True}}],
    }]}


@pytest.fixture(autouse=True)
def offline():
    with patch("requests.sessions.Session.send", side_effect=AssertionError("Unexpected HTTP")), \
            patch("backoff._sync.time.sleep"):
        yield


@pytest.mark.parametrize("stream_type", MINIMUM)
@pytest.mark.parametrize("sample", [[], [{"externalid": "example"}]])
def test_empty_or_sparse_samples_keep_etl_columns(stream_type, sample):
    with patch("requests.sessions.Session.send", side_effect=[response({}, 404), response({"items": sample})]):
        stream = stream_type(TapNetSuite(config=CONFIG))
    schema = stream.schema
    assert MINIMUM[stream_type] <= schema["properties"].keys()
    validate(RECORDS[stream_type], schema)


@pytest.mark.parametrize("stream_type", MINIMUM)
def test_sparse_metadata_keeps_etl_columns(stream_type):
    with patch("requests.sessions.Session.send", return_value=response({"properties": {"externalid": {"type": "string"}}})):
        stream = stream_type(TapNetSuite(config=CONFIG))
    assert MINIMUM[stream_type] <= stream.schema["properties"].keys()
    validate(RECORDS[stream_type], stream.schema)


@pytest.mark.parametrize("status,error", [(400, FatalAPIError), (403, FatalAPIError), (401, InvalidCredentialsError)])
def test_sales_inference_error_does_not_publish_catalog(status, error, capsys):
    config = {**CONFIG, "remove_unauthorized_streams": True}
    with patch("tap_netsuite_rest.tap.include_streams", ["SalesOrdersStream"]), \
            patch("requests.sessions.Session.send", side_effect=[response({}, 404), response({}, status)]):
        with pytest.raises(error):
            TapNetSuite(config=config).run_discovery()
    assert capsys.readouterr().out == ""


@pytest.mark.parametrize("body", [{}, {"items": None}, {"items": {}}, {"items": [None]}])
def test_malformed_samples_do_not_publish_catalog(body, capsys):
    with patch("tap_netsuite_rest.tap.include_streams", ["SalesOrdersStream"]), \
            patch("requests.sessions.Session.send", side_effect=[response({}, 404), response(body)]):
        with pytest.raises((ValueError, AttributeError)):
            TapNetSuite(config=CONFIG).run_discovery()
    assert capsys.readouterr().out == ""


@pytest.mark.parametrize("status,budget", [(429, 12), (503, 8)])
@pytest.mark.parametrize("endpoint", ["metadata", "sample"])
def test_exhausted_retries_fail_without_outer_retry_multiplier(status, budget, endpoint):
    attempts = []

    def send(request, **kwargs):
        if endpoint == "sample" and request.method == "GET":
            return response({}, 404)
        attempts.append(request)
        return response({}, status)

    with patch("requests.sessions.Session.send", side_effect=send):
        with pytest.raises(RetriableAPIError):
            SalesOrdersStream(TapNetSuite(config=CONFIG))
    assert len(attempts) == budget


@pytest.mark.parametrize("status", [429, 503])
def test_retry_recovers_with_fresh_oauth_nonce(status):
    attempts = []

    def send(request, **kwargs):
        if request.method == "GET":
            return response({}, 404)
        attempts.append(request)
        return response({}, status) if len(attempts) == 1 else response({"items": [RECORDS[SalesOrdersStream]]})

    with patch("requests.sessions.Session.send", side_effect=send):
        stream = SalesOrdersStream(TapNetSuite(config=CONFIG))
    nonces = [re.search(r'oauth_nonce="([^"]+)"', request.headers["Authorization"].decode()).group(1)
              for request in attempts]
    assert len(nonces) == len(set(nonces)) == 2
    validate(RECORDS[SalesOrdersStream], stream.schema)


def test_metadata_authentication_failure_does_not_fall_back():
    with patch("requests.sessions.Session.send", side_effect=[response({}, 401)]):
        with pytest.raises(InvalidCredentialsError):
            SalesOrdersStream(TapNetSuite(config=CONFIG))


def test_malformed_metadata_can_use_valid_suiteql_sample():
    with patch("requests.sessions.Session.send", side_effect=[response({"unexpected": True}), response({"items": [{"id": "123", "custbody_example": "value"}]})]):
        stream = SalesOrdersStream(TapNetSuite(config=CONFIG))
    assert "custbody_example" in stream.schema["properties"]
    assert MINIMUM[SalesOrdersStream] <= stream.schema["properties"].keys()


@pytest.mark.parametrize("stream_type,missing", [
    (SalesOrdersStream, field) for field in MINIMUM[SalesOrdersStream] - {"createddate"}
] + [
    (SalesOrderLinesStream, field) for field in MINIMUM[SalesOrderLinesStream]
])
def test_incomplete_supplied_catalog_requires_rediscovery(stream_type, missing, capsys):
    with pytest.raises(FatalAPIError, match=f"Incomplete catalog for {stream_type.name}: missing {missing}.*rediscovery"):
        TapNetSuite(config=CONFIG, catalog=catalog_for(stream_type, [missing])).run_discovery()
    assert capsys.readouterr().out == ""


def test_supplied_sales_catalog_does_not_discover_unselected_tables():
    tap = TapNetSuite(config=CONFIG, catalog=catalog_for(SalesOrdersStream))
    assert set(tap.streams) == {"sales_orders"}
    validate(RECORDS[SalesOrdersStream], tap.streams["sales_orders"].schema)


def test_inaccessible_optional_table_is_omitted_from_catalog(capsys):
    config = {**CONFIG, "remove_unauthorized_streams": True}
    with patch("tap_netsuite_rest.tap.include_streams", ["ContactsStream"]), \
            patch("requests.sessions.Session.send", side_effect=[response({}, 404), response({}, 403), response({}, 403)]):
        catalog = json.loads(TapNetSuite(config=config).run_discovery())
    assert catalog["streams"] == []
    capsys.readouterr()


def test_accessible_table_inference_failure_is_not_permission_filtered(capsys):
    config = {**CONFIG, "remove_unauthorized_streams": True}
    with patch("tap_netsuite_rest.tap.include_streams", ["ContactsStream"]), \
            patch("requests.sessions.Session.send", side_effect=[response({}, 404), response({}, 403), response({"items": [{"id": "1"}]})]):
        with pytest.raises(FatalAPIError):
            TapNetSuite(config=config).run_discovery()
    assert capsys.readouterr().out == ""


@pytest.mark.parametrize("status,error,budget", [(401, InvalidCredentialsError, 1), (503, RetriableAPIError, 5)])
def test_permission_probe_errors_cannot_hide_a_table(status, error, budget, capsys):
    config = {**CONFIG, "remove_unauthorized_streams": True}
    with patch("tap_netsuite_rest.tap.include_streams", ["ContactsStream"]), \
            patch("requests.sessions.Session.send", side_effect=[response({}, 404), response({}, 403)] + [response({}, status)] * budget):
        with pytest.raises(error):
            TapNetSuite(config=config).run_discovery()
    assert capsys.readouterr().out == ""


def test_nullable_closedate_survives_sales_record_processing():
    with patch("requests.sessions.Session.send", side_effect=[response({}, 404), response({"items": []})]):
        stream = SalesOrdersStream(TapNetSuite(config=CONFIG))
    record = stream.post_process({"id": "123", "closedate": None}, None)
    assert record["closedate"] is None
    validate(record, stream.schema)


def test_invoice_fallback_preserves_custom_field_types():
    with patch("requests.sessions.Session.send", side_effect=[
        response({}, 404),
        response({
            "items": [{"scriptid": "custbody_example", "fieldvaluetype": "Integer Number"}],
            "offset": 0, "count": 1, "totalResults": 1, "hasMore": False,
        }),
        response({"items": [{"id": "123"}]}),
    ]):
        stream = InvoicesStream(TapNetSuite(config=CONFIG))
    record = stream.post_process({"id": "123", "custbody_example": "456"}, None)
    assert record["custbody_example"] == 456
    validate(record, stream.schema)


@pytest.mark.parametrize("statuses", [[429, 503] * 4, [503, 429] * 6])
def test_mixed_transient_failures_share_request_attempt_budget(statuses):
    attempts = []

    def send(request, **kwargs):
        if request.method == "GET":
            return response({}, 404)
        attempts.append(request)
        if len(attempts) <= len(statuses):
            return response({}, statuses[len(attempts) - 1])
        return response({"items": []})

    with patch("requests.sessions.Session.send", side_effect=send):
        with pytest.raises(RetriableAPIError):
            SalesOrdersStream(TapNetSuite(config=CONFIG))
    assert len(attempts) == len(statuses)


@pytest.mark.parametrize("source", ["catalog", "metadata", "sample"])
@pytest.mark.parametrize("include_field", [True, False])
def test_item_stock_update_is_nullable_text(source, include_field, capsys):
    field = "custitem_magento_stock_update"
    config = {**CONFIG, "start_date": "2026-08-01T00:00:00Z"}
    properties = {
        "id": {"type": ["string", "null"]},
        "lastmodifieddate": {"type": ["string", "null"], "format": "date-time"},
    }
    sample = {"id": "42", "lastmodifieddate": "2026-08-02 12:00:00"}
    if include_field:
        properties[field] = {"type": ["string", "null"], "format": "date-time"}
        sample[field] = "2026-08-02 11:30:00"
    catalog = {"streams": [{
        "tap_stream_id": "item",
        "stream": "item",
        "schema": {"type": "object", "properties": properties},
        "key_properties": ["id", "lastmodifieddate"],
        "replication_key": "lastmodifieddate",
        "replication_method": "INCREMENTAL",
        "metadata": [{"breadcrumb": [], "metadata": {"selected": True}}],
    }]}
    original = json.dumps(catalog, sort_keys=True)
    replies = []
    if source == "metadata":
        replies = [response({"properties": {
            name: {**schema, "type": "string"}
            for name, schema in properties.items()
        }})]
    elif source == "sample":
        replies = [response({}, 404), response({"items": [sample]})]

    with patch("tap_netsuite_rest.tap.include_streams", ["ItemStream"]), \
            patch.object(ItemStream, "date_fields", []), \
            patch("requests.sessions.Session.send", side_effect=replies):
        tap = TapNetSuite(config=config, catalog=catalog if source == "catalog" else None)
        discovered = json.loads(tap.run_discovery())
        schema = discovered["streams"][0]["schema"]
        assert json.dumps(catalog, sort_keys=True) == original
        assert schema["properties"]["lastmodifieddate"]["format"] == "date-time"
        if include_field:
            assert schema["properties"][field] == {"type": ["string", "null"]}
            validate({"id": "42", field: "not a date"}, schema)
            validate({"id": "42", field: None}, schema)
        else:
            assert field not in schema["properties"]

        selected_catalog = catalog if source == "catalog" else discovered
        selected_catalog["streams"][0]["metadata"] = [
            {"breadcrumb": [], "metadata": {"selected": True}},
            *[{"breadcrumb": ["properties", name], "metadata": {"selected": True}}
              for name in schema["properties"]],
        ]
        stream = TapNetSuite(config=config, catalog=selected_catalog).streams["item"]
        capsys.readouterr()
        stream._write_schema_message()
        emitted_schema = json.loads(capsys.readouterr().out)["schema"]
        if include_field:
            assert emitted_schema["properties"][field] == {"type": ["string", "null"]}
        else:
            assert field not in emitted_schema["properties"]
        query = stream.prepare_request_payload(None, None)["q"]
        assert stream.format_date_query("lastmodifieddate") in query
        if include_field:
            assert f"item.{field} AS {field}" in query
            assert f"TO_CHAR (item.{field}," not in query
            for value in ("2026-08-02 11:30:00", None):
                assert stream.post_process({"id": "42", field: value}, None)[field] == value
        else:
            assert field not in query
    capsys.readouterr()
