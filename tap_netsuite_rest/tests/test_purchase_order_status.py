"""Offline regression for language-independent purchase-order statuses."""
from copy import deepcopy
from unittest.mock import patch

import pytest

from tap_netsuite_rest.streams import PurchaseOrdersStream
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


@pytest.fixture(autouse=True)
def no_network():
    with patch(
        "requests.sessions.Session.send",
        side_effect=AssertionError("Unexpected HTTP"),
    ):
        yield


def test_purchase_orders_include_raw_status_code_with_saved_catalog():
    catalog = {
        "streams": [{
            "tap_stream_id": PurchaseOrdersStream.name,
            "stream": PurchaseOrdersStream.name,
            "schema": {
                "type": "object",
                "properties": {
                    "id": {"type": ["string", "null"]},
                    "status": {"type": ["string", "null"]},
                    "lastmodifieddate": {
                        "type": ["string", "null"],
                        "format": "date-time",
                    },
                },
            },
            "key_properties": ["id"],
            "metadata": [{"breadcrumb": [], "metadata": {"selected": True}}],
        }],
    }
    original = deepcopy(catalog)
    tap = TapNetSuite(config=CONFIG, catalog=catalog)
    stream = PurchaseOrdersStream(tap)

    query = stream.prepare_request_payload(None, None)["q"]
    assert "transaction.status AS status_code" in query
    assert "BUILTIN.DF(transaction.status) AS status" in query
    assert stream.schema["properties"]["status_code"] == {
        "type": ["string", "null"]
    }

    record = stream.post_process(
        {
            "id": "16079",
            "status_code": "H",
            "status": "Inkooporder : Gesloten",
        },
        None,
    )
    assert record["status_code"] == "H"
    assert record["status"] == "Inkooporder : Gesloten"
    assert catalog == original
