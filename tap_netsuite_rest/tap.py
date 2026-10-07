"""NetSuite tap class."""

from typing import List, Optional, Union
from pathlib import PurePath

from hotglue_singer_sdk import Stream, Tap
from hotglue_singer_sdk import typing as th  # JSON schema typing helpers
from hotglue_singer_sdk.helpers.capabilities import AlertingLevel

import inspect 
import requests

from tap_netsuite_rest import streams
from tap_netsuite_rest.client_soap import NetsuiteSOAPClient
import os
import logging

# This branch is scoped to the streams consumed by the Mitari ETL. INCLUDE_STREAMS
# may narrow this set for diagnostics, but it cannot enable other streams.
MITARI_STREAMS = frozenset({
    "DeletedRecordsStream",
    "InventoryItemLocationsStream",
    "ItemStream",
    "ItemPriceStream",
    "ItemReceiptLinesStream",
    "ItemReceiptsStream",
    "ItemVendorStream",
    "kitItemMemberStream",
    "PurchaseOrderLinesStream",
    "PurchaseOrdersStream",
    "SalesOrderLinesStream",
    "SalesOrdersStream",
    "VendorStream",
})

include_streams = os.environ.get("INCLUDE_STREAMS", "").split(",") if os.environ.get("INCLUDE_STREAMS") else []
logging.info("INCLUDE_STREAMS: " + os.environ.get("INCLUDE_STREAMS", ""))

ignore_streams = os.environ.get("IGNORE_STREAMS", "").split(",") if os.environ.get("IGNORE_STREAMS") else []
logging.info("IGNORE_STREAMS: " + os.environ.get("IGNORE_STREAMS", ""))


def streams_to_sync(self, include_streams, ignore_streams):
    """Return the Mitari stream types permitted for this run."""
    selected_streams = MITARI_STREAMS.intersection(include_streams) if include_streams else MITARI_STREAMS
    stream_types = []
    catalog_types = None
    if self.config.get("use_input_catalog", True) and self.input_catalog:
        catalog_types = {
            cls for _, cls in inspect.getmembers(streams, inspect.isclass)
            if cls.__module__ == 'tap_netsuite_rest.streams' and self.input_catalog.get(cls.name)
        }
        pending = list(catalog_types)
        for cls in pending:
            parent = getattr(cls, "parent_stream_type", None)
            if parent is not None and parent not in catalog_types:
                catalog_types.add(parent)
                pending.append(parent)

    for name, cls in inspect.getmembers(streams, inspect.isclass):
        if cls.__module__ == 'tap_netsuite_rest.streams':
            if cls.name == 'bill_attachments':
                continue
            if name not in selected_streams or name in ignore_streams:
                continue
            if catalog_types is not None and cls not in catalog_types:
                continue
            stream_types.append(cls(self))
    return stream_types

class TapNetSuite(Tap):
    """NetSuite tap class."""

    name = "tap-netsuite-rest"
    custom_fields = None
    alerting_level = AlertingLevel.ERROR
    exception_alerting_level_map = {
        requests.exceptions.ConnectionError: AlertingLevel.NONE,
    }

    config_jsonschema = th.PropertiesList(
        th.Property("ns_account", th.StringType, required=True),
        th.Property("ns_consumer_key", th.StringType, required=True),
        th.Property("ns_consumer_secret", th.StringType, required=True),
        th.Property("ns_token_key", th.StringType, required=True),
        th.Property("ns_token_secret", th.StringType, required=True),
        th.Property("window_days", th.IntegerType, default=10),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync",
        ),
        th.Property("bill_attachments_restlet_url", th.StringType, description="Base URL for bill attachments Restlet"),
        th.Property("bill_attachments_suitelet_url", th.StringType, description="Base URL for bill attachments Suitelet (file download)"),
        th.Property(
            "remove_unauthorized_streams",
            th.BooleanType,
            default=True,
            description="When true, omit streams from catalog discover if a SuiteQL probe against the stream table fails.",
        ),
    ).to_dict()

    def __init__(
        self,
        config: Optional[Union[dict, PurePath, str, List[Union[PurePath, str]]]] = None,
        catalog: Union[PurePath, str, dict, None] = None,
        state: Union[PurePath, str, dict, None] = None,
        parse_env_config: bool = False,
        validate_config: bool = True,
    ) -> None:
        self.force_sync_inventory = False
        self._table_access_cache = {}
        super().__init__(config, catalog, state, parse_env_config, validate_config)
        self.soap_client = NetsuiteSOAPClient(self.config, self.logger)
    

    def load_state(self, state):
        super().load_state(state)
        self.force_sync_inventory = state.get("force_sync_inventory", False)
        self.state.pop("force_sync_inventory", None)

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        streams = streams_to_sync(self, include_streams, ignore_streams)
        # flag add for test back compatibility also not run probe table during get, only during discover
        if not self.config.get("remove_unauthorized_streams") or self.input_catalog:
            return streams

        accessible = []
        table_access_cache = self._table_access_cache

        for stream in streams:
            probe_table_name = getattr(stream, "_probe_table_name", None)
            if probe_table_name is None:
                accessible.append(stream)
                continue

            table = probe_table_name()
            if table is None:
                accessible.append(stream)
                continue

            if table not in table_access_cache:
                self.logger.info("Probing access for table '%s'", table)
                table_access_cache[table] = stream.probe_table_access(table)

            if table_access_cache[table]:
                accessible.append(stream)
            else:
                self.logger.info(
                    "Excluding stream '%s' from catalog: no access to table '%s'",
                    stream.name,
                    table,
                )

        return accessible

if __name__ == "__main__":
    TapNetSuite.cli()
