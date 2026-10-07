"""Offline inventory backfills with root-state overrides and retained bookmarks."""
from copy import deepcopy
import json
import re
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

import pytest
import requests

from tap_netsuite_rest.streams import InventoryItemLocationsStream
from tap_netsuite_rest.tap import TapNetSuite

CONFIG = {
    "ns_account": "example",
    "ns_consumer_key": "test",
    "ns_consumer_secret": "test",
    "ns_token_key": "test",
    "ns_token_secret": "test",
    "start_date": "2024-01-01T00:00:00Z",
    "remove_unauthorized_streams": False,
}
BOOKMARKS = {
    "inventory_item_locations": {
        "replication_key": "lastquantityavailablechange",
        "replication_key_value": "2026-10-01 00:00:00",
    },
    "item": {"replication_key": "lastmodifieddate", "replication_key_value": "2026-09-01"},
}
SOURCE = [
    {"item": "1", "location": "113", "quantityavailable": "3", "reorderpoint": "1",
     "lastquantityavailablechange": "2026-10-05 00:00:00"},
    {"item": "2", "location": "113", "quantityavailable": "7", "reorderpoint": "0",
     "lastquantityavailablechange": "2001-01-01 00:00:00"},
    {"item": "3", "location": "113", "quantityavailable": "0", "reorderpoint": "0",
     "lastquantityavailablechange": None},
    {"item": "42366", "location": "113", "quantityavailable": "0", "reorderpoint": "0"},
    {"item": "42366", "location": "114", "quantityavailable": "9", "reorderpoint": "2",
     "lastquantityavailablechange": "2002-01-01 00:00:00"},
    {"item": "99999", "location": "113", "quantityavailable": "4", "reorderpoint": "0",
     "lastquantityavailablechange": "2003-01-01 00:00:00"},
    {"item": "100000", "location": "113", "quantityavailable": "0", "reorderpoint": "0"},
    {"item": "105000", "location": "113", "quantityavailable": "-2", "reorderpoint": "3",
     "lastquantityavailablechange": "2026-10-06 00:00:00"},
]


@pytest.fixture(autouse=True)
def no_network():
    with patch("requests.sessions.Session.send", side_effect=AssertionError("Unexpected HTTP")):
        yield


def make_tap(state, config=None):
    catalog = {"streams": [{
        "tap_stream_id": "inventory_item_locations",
        "stream": "inventory_item_locations",
        "schema": deepcopy(InventoryItemLocationsStream.schema),
        "key_properties": [],
        "replication_key": "lastquantityavailablechange",
        "replication_method": "INCREMENTAL",
        "metadata": [{"breadcrumb": [], "metadata": {"selected": True}}],
    }]}
    with patch("tap_netsuite_rest.tap.include_streams", ["InventoryItemLocationsStream"]):
        tap = TapNetSuite(config=config or CONFIG, catalog=catalog, state=state)
        stream = tap.streams["inventory_item_locations"]
    stream.page_size = 2
    return tap, stream


def source_response(request, **kwargs):
    query = json.loads(request.body)["q"]
    rows = deepcopy(SOURCE)
    item_range = re.search(r"item >= (\d+)(?: AND item < (\d+))?", query)
    if item_range:
        lower, upper = item_range.groups()
        rows = [row for row in rows if int(row["item"]) >= int(lower)
                and (upper is None or int(row["item"]) < int(upper))]
    cutoff = re.search(r"lastquantityavailablechange>TO_TIMESTAMP\('([^']+)'", query)
    if cutoff:
        rows = [row for row in rows if row.get("lastquantityavailablechange")
                and row["lastquantityavailablechange"] > cutoff.group(1)]
        rows.sort(key=lambda row: row["lastquantityavailablechange"])
    else:
        rows.sort(key=lambda row: (int(row["item"]), int(row["location"])))
    params = parse_qs(urlsplit(request.url).query)
    offset, limit = int(params["offset"][0]), int(params["limit"][0])
    page = rows[offset:offset + limit]
    response = requests.Response()
    response.status_code = 200
    response._content = json.dumps({
        "items": page, "count": len(page), "offset": offset,
        "hasMore": offset + limit < len(rows), "totalResults": len(rows),
    }).encode()
    return response


def sync_messages(tap, capsys):
    with patch("requests.sessions.Session.send", side_effect=source_response):
        tap.sync_all()
    messages = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    return messages


def sync_records(tap, capsys):
    messages = sync_messages(tap, capsys)
    assert all("force_sync_inventory" not in message["value"]
               for message in messages if message["type"] == "STATE")
    return [message["record"] for message in messages if message["type"] == "RECORD"]


def inventory_values(rows):
    return [(row["item"], row["location"], row["quantityavailable"], row["reorderpoint"])
            for row in rows]


@pytest.mark.parametrize("flag", [True, "true", "TRUE"])
@pytest.mark.parametrize("has_start_date", [True, False])
@pytest.mark.parametrize("has_inventory_bookmark", [True, False])
def test_root_flag_backfills_old_and_null_rows(
    flag, has_start_date, has_inventory_bookmark, capsys,
):
    bookmarks = deepcopy(BOOKMARKS)
    if not has_inventory_bookmark:
        bookmarks.pop("inventory_item_locations")
    state = {"force_sync_inventory": flag, "bookmarks": bookmarks}
    original_state = deepcopy(state)
    config = deepcopy(CONFIG)
    if not has_start_date:
        config.pop("start_date")
    tap, stream = make_tap(state, config)
    records = sync_records(tap, capsys)
    assert inventory_values(records) == inventory_values(SOURCE)
    expected_bookmarks = deepcopy(bookmarks)
    expected_bookmarks.setdefault("inventory_item_locations", {})
    assert tap.state["bookmarks"] == expected_bookmarks
    assert "force_sync_inventory" not in tap.state
    assert state == original_state
    assert stream.replication_method == "FULL_TABLE"


def test_missing_inventory_bookmark_ignores_start_date(capsys):
    bookmarks = deepcopy(BOOKMARKS)
    bookmarks.pop("inventory_item_locations")
    tap, stream = make_tap({"bookmarks": bookmarks})

    records = sync_records(tap, capsys)

    assert inventory_values(records) == inventory_values(SOURCE)
    assert stream.replication_method == "INCREMENTAL"
    assert tap.state["bookmarks"]["inventory_item_locations"] == {
        "replication_key": "lastquantityavailablechange",
        "replication_key_value": "2026-10-06 00:00:00",
    }


@pytest.mark.parametrize("flag", [False, "false", "FALSE", None])
def test_false_root_flag_retains_incremental_coverage(flag, capsys):
    tap, stream = make_tap({"force_sync_inventory": flag, "bookmarks": deepcopy(BOOKMARKS)})
    records = sync_records(tap, capsys)
    expected = [SOURCE[0], SOURCE[-1]]
    assert inventory_values(records) == inventory_values(expected)
    assert tap.state["bookmarks"]["item"] == BOOKMARKS["item"]
    assert stream.replication_method == "INCREMENTAL"


def test_config_fallback_matches_supplier_force_flag(capsys):
    config = {**CONFIG, "force_sync_inventory": "true"}
    tap, _ = make_tap({"bookmarks": deepcopy(BOOKMARKS)}, config)
    assert inventory_values(sync_records(tap, capsys)) == inventory_values(SOURCE)


def test_force_override_does_not_repeat_from_emitted_state(capsys):
    tap, stream = make_tap({
        "force_sync_inventory": True, "bookmarks": deepcopy(BOOKMARKS),
    })
    stream.STATE_MSG_FREQUENCY = 1
    messages = sync_messages(tap, capsys)
    records = [message["record"] for message in messages if message["type"] == "RECORD"]
    assert inventory_values(records) == inventory_values(SOURCE)
    states = [message["value"] for message in messages if message["type"] == "STATE"]
    assert all("force_sync_inventory" not in state for state in states)
    assert states[-1]["bookmarks"] == BOOKMARKS

    next_tap, next_stream = make_tap(states[-1])
    records = sync_records(next_tap, capsys)
    assert inventory_values(records) == inventory_values([SOURCE[0], SOURCE[-1]])
    assert next_stream.replication_method == "INCREMENTAL"


def test_forced_batch_rejects_results_above_suiteql_cap():
    tap, stream = make_tap({"force_sync_inventory": True, "bookmarks": deepcopy(BOOKMARKS)})
    response = requests.Response()
    response.status_code = 200
    response._content = json.dumps({
        "hasMore": True, "offset": 0, "totalResults": stream.cap_total_results + 1,
    }).encode()
    with pytest.raises(Exception, match="totalResults is greater than"):
        stream.get_next_page_token(response, None)
