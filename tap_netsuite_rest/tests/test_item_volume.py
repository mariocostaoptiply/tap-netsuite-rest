"""Offline regressions for the item volume custom field."""
from unittest.mock import patch

from tap_netsuite_rest.streams import ItemStream
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


def test_item_schema_includes_numeric_volume_when_metadata_omits_it():
    with patch.object(
        ItemStream,
        "schema_response",
        {"properties": {"id": {"type": "string"}}},
    ), patch(
        "requests.sessions.Session.send", side_effect=AssertionError("Unexpected HTTP")
    ):
        stream = ItemStream(TapNetSuite(config=CONFIG))

        volume_schema = stream.schema["properties"]["custitem_volume_cm3"]

    assert "number" in volume_schema["type"]


def test_item_volume_is_coerced_to_number():
    with patch.object(
        ItemStream,
        "schema_response",
        {"properties": {"id": {"type": "string"}}},
    ), patch(
        "requests.sessions.Session.send", side_effect=AssertionError("Unexpected HTTP")
    ):
        stream = ItemStream(TapNetSuite(config=CONFIG))

        record = stream.post_process({"custitem_volume_cm3": "1250.5"}, None)

    assert record["custitem_volume_cm3"] == 1250.5
