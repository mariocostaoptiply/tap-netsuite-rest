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


def test_query_returns_distinct_active_current_bom_parents():
    stream = AssemblyBomComponentsStream(TapNetSuite(config=CONFIG))

    query = " ".join(stream.prepare_request_payload(None, None)["q"].split())

    assert (
        "SELECT DISTINCT a.assembly AS id, a.assembly AS parentitem "
        "FROM assemblyitembom a"
    ) in query
    assert "INNER JOIN bom b ON b.id = a.billofmaterials" in query
    assert "INNER JOIN bomrevision r ON r.id = a.currentrevision" in query
    assert (
        "INNER JOIN bomrevisioncomponentmember c ON c.bomrevision = r.id"
        in query
    )
    assert (
        "WHERE a.inactive = 'No' AND b.isinactive = 'F' AND r.isinactive = 'F'"
        in query
    )
    assert query.endswith("ORDER BY a.assembly")
    assert set(stream.schema["properties"]) == {"id", "parentitem"}
