"""Offline regressions for NetSuite item receipt lines."""
from copy import deepcopy
from unittest.mock import patch

import pytest

from tap_netsuite_rest.streams import ItemReceiptLinesStream
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


def test_item_receipt_lines_restore_required_fields_with_saved_catalog():
    catalog = {
        "streams": [{
            "tap_stream_id": ItemReceiptLinesStream.name,
            "stream": ItemReceiptLinesStream.name,
            "schema": {
                "type": "object",
                "properties": {
                    "uniquekey": {"type": ["string", "null"]},
                    "transaction": {"type": ["string", "null"]},
                },
            },
            "key_properties": [],
            "metadata": [{"breadcrumb": [], "metadata": {"selected": True}}],
        }],
    }
    original = deepcopy(catalog)
    tap = TapNetSuite(config=CONFIG, catalog=catalog)
    stream = ItemReceiptLinesStream(tap)

    query = stream.prepare_request_payload(
        {"ids": ["9102316"]}, None
    )["q"]
    assert (
        "mainline = 'F' AND isinventoryaffecting = 'T' AND iscogs = 'F'"
        in query
    )
    assert "tl.transaction IN ('9102316')" in query

    properties = stream.schema["properties"]
    assert properties["item"] == {"type": ["string", "null"]}
    assert properties["quantity"] == {"type": ["number", "null"]}
    assert properties["rate"] == {"type": ["number", "null"]}
    assert properties["createdfrom"] == {"type": ["string", "null"]}
    assert properties["itemtype"] == {"type": ["string", "null"]}

    record = stream.post_process(
        {
            "uniquekey": "24940613",
            "transaction": "9102316",
            "createdfrom": "8905792",
            "item": "42042",
            "quantity": "1",
            "itemtype": "InvtPart",
        },
        {"ids": ["9102316"]},
    )
    assert record["createdfrom"] == "8905792"
    assert record["item"] == "42042"
    assert record["quantity"] == 1
    assert catalog == original
