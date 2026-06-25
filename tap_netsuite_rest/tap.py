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

# When a new stream is added or changes in the tap, it would break all existing test suites due to dynamic discover.
# By allowing caller to include only streams we need we are able to ensure existing tests continue to pass.
# 1. Get the environment variable INCLUDE_STREAMS and split by commas
include_streams = os.environ.get('INCLUDE_STREAMS', "").split(',') if os.environ.get('INCLUDE_STREAMS', "") else []
logging.info("INCLUDE_STREAMS: "+ os.environ.get('INCLUDE_STREAMS', ''))

# 2. Get the environment variable IGNORE_STREAMS and split by commas
ignore_streams = os.environ.get('IGNORE_STREAMS', "").split(',') if os.environ.get('IGNORE_STREAMS', "") else []
logging.info("IGNORE_STREAMS: "+ os.environ.get('IGNORE_STREAMS', ''))


def get_bill_attachments_stream(config):
    if 'bill_attachments_restlet_url' in config \
        and 'bill_attachments_suitelet_url' in config:
        return streams.BillAttachmentsRestletStream
    
    return streams.BillAttachmentsSOAPStream


# Function to filter streams to be tested
def streams_to_sync(self, include_streams, ignore_streams):
    stream_types = []

    if not ((include_streams and 'BillAttachmentsStream' not in include_streams) or 'BillAttachmentsStream' in ignore_streams):
        stream_types.append(get_bill_attachments_stream(self.config)(self))

    for name, cls in inspect.getmembers(streams, inspect.isclass):
        if cls.__module__ == 'tap_netsuite_rest.streams':
            if cls.name == 'bill_attachments':
                continue
            if (include_streams and name not in include_streams) or name in ignore_streams:
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
        super().__init__(config, catalog, state, parse_env_config, validate_config)
        self.soap_client = NetsuiteSOAPClient(self.config, self.logger)
    

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        streams = streams_to_sync(self, include_streams, ignore_streams)
        # flag add for test back compatibility also not run probe table during get, only during discover
        if not self.config.get("remove_unauthorized_streams") or self.input_catalog:
            return streams

        accessible = []
        table_access_cache: dict[str, bool] = {}

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
