"""Offline regressions for current assembly BOM composition extraction."""
from unittest.mock import patch

import pytest

from tap_netsuite_rest.streams import AssemblyBomComponentsStream
from tap_netsuite_rest.tap import TapNetSuite, streams_to_sync


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
        "requests.sessions.Session.send", side_effect=AssertionError("Unexpected HTTP")
    ):
        yield


def test_stream_is_discovered_as_full_table():
    tap = TapNetSuite(config=CONFIG)

    discovered = streams_to_sync(tap, ["AssemblyBomComponentsStream"], [])

    assert [stream.name for stream in discovered] == ["assembly_bom_components"]
    stream = discovered[0]
    assert stream.primary_keys == ["id"]
    assert stream.replication_key is None
    assert stream.replication_method == "FULL_TABLE"


def test_query_groups_current_active_components_at_composition_grain():
    stream = AssemblyBomComponentsStream(TapNetSuite(config=CONFIG))

    query = " ".join(stream.prepare_request_payload(None, None)["q"].split())

    assert "CONCAT(CONCAT(a.assembly, '_'), c.item) AS id" in query
    assert "SUM(c.quantity) AS quantity" in query
    assert "SUM(c.bomquantity) AS bomquantity" in query
    assert "COUNT(*) AS sourcelines" in query
    assert "INNER JOIN bomrevision r ON r.id = a.currentrevision" in query
    assert "LEFT JOIN item component ON component.id = c.item" in query
    assert "component.custitem_item_uom AS itemuomid" in query
    assert (
        "WHERE a.inactive = 'No' AND b.isinactive = 'F' AND r.isinactive = 'F'"
        in query
    )
    assert query.endswith("ORDER BY a.assembly, c.item")
    assert "r.lastmodifieddate>" not in query


def test_stream_coerces_quantities_counts_and_revision_timestamp():
    stream = AssemblyBomComponentsStream(TapNetSuite(config=CONFIG))

    record = stream.post_process(
        {
            "id": "100_200",
            "parentitem": "100",
            "item": "200",
            "quantity": "8.25",
            "bomquantity": "8.25",
            "revisionlastmodifieddate": "2026-10-09 15:00:00",
            "sourcelines": "2",
        },
        None,
    )

    assert record["quantity"] == 8.25
    assert record["bomquantity"] == 8.25
    assert record["sourcelines"] == 2
    assert record["revisionlastmodifieddate"].isoformat() == "2026-10-09T15:00:00+00:00"
