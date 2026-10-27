"""Offline regressions for supplier-specific transaction contacts."""
from copy import deepcopy
import sqlite3
from unittest.mock import patch

import pytest

from tap_netsuite_rest.streams import VendorStream
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
SCHEMA = {
    "type": "object",
    "properties": {
        "id": {"type": ["string", "null"]},
        "defaultbillingaddress": {"type": ["string", "null"]},
        "lastmodifieddate": {
            "type": ["string", "null"], "format": "date-time"
        },
    },
}


@pytest.fixture(autouse=True)
def no_network():
    with patch(
        "requests.sessions.Session.send",
        side_effect=AssertionError("Unexpected HTTP"),
    ), patch.object(VendorStream, "schema_response", SCHEMA):
        yield


def make_stream(enabled=False, catalog=None, state=None):
    return VendorStream(TapNetSuite(
        config={**CONFIG, "vendor_transaction_contact": enabled},
        catalog=catalog,
        state=state,
    ))


def test_vendor_contact_uses_latest_po_then_unique_active_contact():
    stream = make_stream(enabled=True)
    query = stream.prepare_request_payload(None, None)["q"]
    with sqlite3.connect(":memory:") as db:
        db.row_factory = sqlite3.Row
        db.create_function("TO_CHAR", 2, lambda value, format_string: value)
        db.executescript("""
            CREATE TABLE vendor (id INTEGER, defaultbillingaddress TEXT, lastmodifieddate TEXT);
            CREATE TABLE vendorsubsidiaryrelationship (entity INTEGER, subsidiary INTEGER);
            CREATE TABLE "transaction" (
                id INTEGER, entity INTEGER, type TEXT, createddate TEXT,
                custbody_transaction_contact INTEGER
            );
            CREATE TABLE CompanyContactRelationship (company INTEGER, contact INTEGER);
            CREATE TABLE contact (id INTEGER, isinactive TEXT);
            INSERT INTO vendor VALUES
                (101, NULL, '2026-01-01'), (102, NULL, '2026-01-01'),
                (103, NULL, '2026-01-01'), (104, NULL, '2026-01-01'),
                (105, NULL, '2026-01-01'), (106, NULL, '2026-01-01'),
                (107, NULL, '2026-01-01'), (108, NULL, '2026-01-01'),
                (109, NULL, '2026-01-01');
            INSERT INTO vendorsubsidiaryrelationship VALUES
                (101, 1), (102, 1), (103, 1), (104, 1), (105, 1),
                (106, 1), (107, 1), (108, 1), (109, 1);
            INSERT INTO contact VALUES
                (11, 'F'), (12, 'F'), (13, 'T'), (21, 'F'), (22, 'F'),
                (43, 'T'), (51, 'F'), (61, 'F'), (62, 'T'), (71, 'F'),
                (72, 'F'), (91, 'F'), (92, 'T');
            INSERT INTO CompanyContactRelationship VALUES
                (101, 11), (101, 12), (101, 13), (102, 21), (102, 22),
                (102, 22), (104, 43), (105, 51), (106, 61), (106, 61),
                (106, 62), (107, 71), (107, 72), (109, 91), (109, 92);
            INSERT INTO "transaction" VALUES
                (1001, 101, 'PurchOrd', '2026-10-01', 11),
                (1002, 101, 'PurchOrd', '2026-10-02', 12),
                (9000, 101, 'PurchOrd', '2026-09-30', 11),
                (1004, 101, 'PurchOrd', '2026-10-05', NULL),
                (1005, 101, 'PurchOrd', '2026-10-06', 13),
                (1006, 101, 'PurchOrd', '2026-10-07', 21),
                (2001, 102, 'PurchOrd', '2026-10-03', 21),
                (2002, 102, 'PurchOrd', '2026-10-03', 22),
                (4001, 104, 'PurchOrd', '2026-10-03', 43),
                (5001, 105, 'SalesOrd', '2026-10-03', 51),
                (8001, 108, 'PurchOrd', '2026-10-03', 61),
                (9001, 109, 'PurchOrd', '2026-10-03', 92);
        """)
        # SQLite reserves TRANSACTION; only quote that table name for the offline engine.
        rows = db.execute(query.replace("FROM transaction po", 'FROM "transaction" po')).fetchall()

    assert [(row["id"], row["transaction_contact_id"]) for row in rows] == [
        (101, 12), (102, 22), (103, None), (104, None), (105, 51),
        (106, 61), (107, None), (108, None), (109, 91)
    ]
    processed = stream.post_process(dict(rows[0]), None)
    assert processed["transaction_contact_id"] == "12"
    assert stream.post_process({"id": "103"}, None)["transaction_contact_id"] is None


def test_saved_incremental_catalog_cannot_hide_contact_only_changes():
    catalog = {"streams": [{
        "tap_stream_id": "vendor",
        "stream": "vendor",
        "schema": deepcopy(SCHEMA),
        "key_properties": ["id"],
        "replication_key": "lastmodifieddate",
        "replication_method": "INCREMENTAL",
        "metadata": [{"breadcrumb": [], "metadata": {"selected": True}}],
    }]}
    original = deepcopy(catalog)
    state = {"bookmarks": {"vendor": {
        "replication_key": "lastmodifieddate",
        "replication_key_value": "2026-10-27T12:00:00Z",
    }}}
    stream = make_stream(enabled=True, catalog=catalog, state=state)
    query = stream.prepare_request_payload(None, None)["q"]

    assert stream.replication_method == "FULL_TABLE"
    assert stream.replication_key is None
    assert "TO_TIMESTAMP" not in query
    assert query.endswith("ORDER BY v.id, vsr.subsidiary")
    assert stream.schema["properties"]["transaction_contact_id"] == {
        "type": ["string", "null"]
    }
    stream._increment_stream_state(
        {"id": "101", "transaction_contact_id": "12"}, context=None
    )
    assert stream.stream_state["replication_key_value"] == "2026-10-27T12:00:00Z"
    assert catalog == original


def test_disabled_contact_enrichment_preserves_vendor_incremental_query():
    stream = make_stream()
    query = stream.prepare_request_payload(None, None)["q"]

    assert stream.replication_method == "INCREMENTAL"
    assert stream.replication_key == "lastmodifieddate"
    assert "TO_TIMESTAMP" in query
    assert "custbody_transaction_contact" not in query
    assert "transaction_contact_id" not in stream.schema["properties"]
    assert stream.post_process({"id": "101"}, None) == {"id": "101"}
