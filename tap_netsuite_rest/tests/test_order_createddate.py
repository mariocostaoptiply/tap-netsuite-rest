"""Offline regressions for locale-dependent order createddate catalogs."""
from copy import deepcopy
from unittest.mock import Mock, patch

import pytest
import requests

from tap_netsuite_rest.client import NetsuiteDynamicStream
from tap_netsuite_rest.streams import (
    ItemReceiptsStream,
    PurchaseOrdersStream,
    SalesOrdersStream,
)
from tap_netsuite_rest.tap import TapNetSuite

CONFIG = {
    "ns_account": "example",
    "ns_consumer_key": "test",
    "ns_consumer_secret": "test",
    "ns_token_key": "test",
    "ns_token_secret": "test",
    "start_date": "2026-08-01T00:00:00Z",
    "remove_unauthorized_streams": False,
}
FORMATTED_CREATED = (
    "TO_CHAR (transaction.createddate, 'YYYY-MM-DD HH24:MI:SS') AS createddate"
)


@pytest.fixture(autouse=True)
def no_network():
    with patch("requests.sessions.Session.send", side_effect=AssertionError("Unexpected HTTP")):
        yield


def catalog_for(stream_type, createddate):
    properties = {
        "id": {"type": ["string", "null"]},
        "lastmodifieddate": {"type": ["string", "null"], "format": "date-time"},
    }
    if createddate is not None:
        properties["createddate"] = createddate
    return {"streams": [{
        "tap_stream_id": stream_type.name,
        "stream": stream_type.name,
        "schema": {"type": "object", "properties": properties},
        "key_properties": ["id"],
        "metadata": [{"breadcrumb": [], "metadata": {"selected": True}}],
    }]}


def check_query_and_record(stream):
    query = stream.prepare_request_payload(None, None)["q"]
    assert FORMATTED_CREATED in query
    assert stream.custom_filter in query
    assert "transaction.lastmodifieddate>TO_TIMESTAMP(" in query
    assert "ORDER BY transaction.lastmodifieddate" in query
    schema = stream.schema["properties"]["createddate"]
    assert schema["format"] == "date-time"
    assert schema["type"] == ["string", "null"]
    # Values returned by the explicit SQL expression, not ambiguous locale dates.
    for value in ("2026-08-04 15:50:58", "2026-08-13 15:38:02"):
        record = stream.post_process({"id": "123", "createddate": value}, None)
        assert record["createddate"].isoformat() == value.replace(" ", "T") + "+00:00"
        assert record["id"] == "123"


@pytest.mark.parametrize("stream_type", [SalesOrdersStream, PurchaseOrdersStream])
@pytest.mark.parametrize("createddate", [
    {"type": ["string", "null"], "description": "Original description"},
    {"type": ["string", "null"], "format": "date-time"},
    None,
])
def test_saved_catalog_is_normalized_without_mutation(stream_type, createddate):
    catalog = catalog_for(stream_type, createddate)
    original = deepcopy(catalog)
    tap = TapNetSuite(config=CONFIG, catalog=catalog)
    saved = deepcopy(tap.input_catalog.get(stream_type.name).schema.to_dict())
    stream = stream_type(tap)
    check_query_and_record(stream)
    assert catalog == original
    assert tap.input_catalog.get(stream_type.name).schema.to_dict() == saved
    if createddate and "description" in createddate:
        assert stream.schema["properties"]["createddate"]["description"] == createddate["description"]
    for field, schema in saved["properties"].items():
        if field != "createddate":
            assert stream.schema["properties"][field] == schema


@pytest.mark.parametrize("stream_type", [SalesOrdersStream, PurchaseOrdersStream])
@pytest.mark.parametrize("sample", ["04/08/2026", "13/08/2026", "16/09/2026"])
def test_sample_discovery_cannot_downgrade_createddate(stream_type, sample):
    metadata = Mock()
    metadata.raise_for_status.side_effect = requests.HTTPError("Metadata unavailable")
    records = Mock()
    records.json.return_value = {"items": [{
        "id": "123", "createddate": sample, "lastmodifieddate": "2026-09-16 10:00:00",
    }]}
    # Discovery is mocked at HTTP; execute the real inference and schema builders.
    with patch("requests.sessions.Session.send", side_effect=[metadata, records]) as send, patch.object(NetsuiteDynamicStream, "date_fields", []):
        stream = stream_type(TapNetSuite(config=CONFIG))
        check_query_and_record(stream)
        assert send.call_count == 2
        assert "SELECT TOP 1000 * FROM transaction" in send.call_args.args[0].body.decode()


@pytest.mark.parametrize("stream_type", [SalesOrdersStream, PurchaseOrdersStream])
def test_metadata_string_is_normalized(stream_type):
    response = Mock()
    response.json.return_value = {"properties": {
        "id": {"type": "string"},
        "createddate": {"type": "string"},
        "lastmodifieddate": {"type": "string", "format": "date-time"},
    }}
    with patch("requests.sessions.Session.send", return_value=response) as send:
        stream = stream_type(TapNetSuite(config=CONFIG))
        check_query_and_record(stream)
        assert send.call_count == 1


def test_other_bulk_parent_streams_are_unchanged():
    catalog = catalog_for(ItemReceiptsStream, {"type": ["string", "null"]})
    tap = TapNetSuite(config=CONFIG, catalog=catalog)
    stream = ItemReceiptsStream(tap)
    schema = stream.schema
    assert schema is not None
    assert schema == tap.input_catalog.get(stream.name).schema.to_dict()
    assert "format" not in schema["properties"]["createddate"]
