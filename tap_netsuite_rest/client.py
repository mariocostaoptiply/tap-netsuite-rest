"""REST client handling, including NetSuiteStream base class."""

import logging
import backoff
import requests
import pendulum
import copy
import re

from pathlib import Path
from datetime import datetime, timedelta, date
from typing import Any, Callable, Dict, Optional, cast, Iterable, List

from memoization import cached
from oauthlib import oauth1
from requests_oauthlib import OAuth1Session
from hotglue_singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from hotglue_singer_sdk.helpers.jsonpath import extract_jsonpath
from hotglue_singer_sdk.streams import RESTStream, Stream
from hotglue_singer_sdk import typing as th
from pendulum import parse
from requests.exceptions import HTTPError
import json
from http.client import RemoteDisconnected
from dateutil.relativedelta import relativedelta
import pytz
from copy import deepcopy
from hotglue_singer_sdk.helpers._state import (
    finalize_state_progress_markers,
    log_sort_error,
)
from hotglue_singer_sdk.exceptions import InvalidStreamSortException
import singer
from singer import StateMessage
from hotglue_etl_exceptions import InvalidCredentialsError


SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")
logging.getLogger("backoff").setLevel(logging.CRITICAL)

class RetryRequest(Exception):
    pass


# REST metadata fields that are not safe to include in SuiteQL SELECT clauses.
SUITEQL_EXCLUDED_FIELDS = frozenset(
    {"links", "refname", "classtranslation", "currencyname"}
)


class NetSuiteStream(RESTStream):
    """NetSuite stream class."""

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        url_account = self.config["ns_account"].replace("_", "-").replace("SB", "sb")
        return f"https://{url_account}.suitetalk.api.netsuite.com/services/rest/query/v1/suiteql"

    records_jsonpath = "$.items[*]"
    type_filter = None
    page_size = 1000
    cap_total_results = 100_000
    path = None
    rest_method = "POST"
    query_date = None
    select = None
    join = None
    custom_filter = None
    replication_key_prefix = None
    select_prefix = None
    order_by = None
    append_select = None
    time_jump = relativedelta(months=1)
    always_add_default_fields = False
    query_table = None
    timeout = 500

    def get_replication_key_conditions(self, context):
        """Return a list of replication-key filter strings, or None to use default (get_starting_time / query_date)."""
        return None

    def __init__(
        self,
        tap,
        name = None,
        schema = None,
        path = None,
    ) -> None:
        """Initialize the REST stream.

        Args:
            tap: Singer Tap this stream belongs to.
            schema: JSON schema for records in this stream.
            name: Name of this stream.
            path: URL path for this entity stream.
        """
        super().__init__(name=name, schema=schema, tap=tap, path=path)
        self.record_ids = set()
        self.invalid_fields = []

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        headers["Prefer"] = "transient"

        return headers

    def get_session(self) -> requests.Session:
        """Get requests session.

        Returns:
            The `requests.Session`_ object for HTTP requests.

        .. _requests.Session:
            https://docs.python-requests.org/en/latest/api/#request-sessions
        """
        ns_account = self.config["ns_account"].replace("-", "_").upper()

        return OAuth1Session(
            client_key=self.config["ns_consumer_key"],
            client_secret=self.config["ns_consumer_secret"],
            resource_owner_key=self.config["ns_token_key"],
            resource_owner_secret=self.config["ns_token_secret"],
            realm=ns_account,
            signature_method=oauth1.SIGNATURE_HMAC_SHA256,
        )

    def _probe_table_name(self) -> Optional[str]:
        """Return base SuiteQL table name to probe, or None if probing should be skipped."""
        if getattr(self, "name", None) == "bill_attachments":
            return None

        table = getattr(self, "table", None)
        return table

    @backoff.on_exception(
        backoff.expo,
        (
            (
                requests.exceptions.Timeout,
                requests.exceptions.ConnectionError,
                RemoteDisconnected,
                RetriableAPIError,
            )
        ),
        max_tries=5,
        factor=2,
    )
    def probe_table_access(self, table: str) -> bool:
        """Return True if a minimal SuiteQL query against table succeeds."""
        session = self.get_session()
        prepared_req = session.prepare_request(
            requests.Request(
                method="POST",
                url=f"{self.url_base}?limit=1",
                headers=self.http_headers,
                json={"q": f"SELECT * FROM {table}"},
            )
        )
        try:
            response = session.send(prepared_req, timeout=self.timeout)
            self.validate_response(response)
            return response.status_code == 200
        except Exception as e:
            self.logger.error(f"Error probing table {table}: {e}")
            return False

    def prepare_request(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> requests.PreparedRequest:
        """Prepare a request object."""
        http_method = self.rest_method
        url: str = self.get_url(context)
        params: dict = self.get_url_params(context, next_page_token)
        request_data = self.prepare_request_payload(context, next_page_token)
        headers = self.http_headers

        # Generate a new OAuth1 session
        client = self.get_session()

        request = cast(
            requests.PreparedRequest,
            client.prepare_request(
                requests.Request(
                    method=http_method,
                    url=url,
                    params=params,
                    headers=headers,
                    json=request_data,
                ),
            ),
        )

        return request

    def get_next_page_token( # noqa: C901
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        has_next = next(extract_jsonpath("$.hasMore", response.json()))
        offset = next(extract_jsonpath("$.offset", response.json()))
        offset += self.page_size

        totalResults = next(extract_jsonpath("$.totalResults", response.json()))
        self.logger.info(f"[{self.name}] Total results = {totalResults}. Offset = {offset}")

        if not self.stream_state.get("replication_key") and self.name == "inventory_item_locations" and totalResults > self.cap_total_results:
            # NOTE: this is to avoid a case where we miss data, better to report an error than to miss data
            raise Exception(f"totalResults is greater than {self.cap_total_results} records. This should not happen.")

        if has_next:
            if offset >= self.cap_total_results and (
                (self.name == "transaction_lines" or self.name == "transactions") 
                and not self.config.get("transaction_lines_monthly")
            ):
                if self.replication_key:
                    json_path = f"$.items[-1].{self.replication_key}"
                    last_dt = next(extract_jsonpath(json_path, response.json()))
                    try:
                        self.query_date = pendulum.parse(last_dt).subtract(seconds=1)
                    except Exception:
                        self.query_date = datetime.strptime(last_dt, "%d/%m/%Y") - timedelta(seconds=1)
                    self.logger.warning(
                        f"[{self.name}] Offset regressed ({offset} -> 0); "
                        f"advancing replication boundary to {self.query_date} and resetting offset."
                    )
                    return 0

                raise RuntimeError(
                    f"[{self.name}] Offset regressed ({offset} -> 0) "
                    "without replication key; aborting to avoid infinite loop."
                )

            if (
                (isinstance(self, TransactionRootStream) or isinstance(self, BulkParentStream))
                and self.config.get("transaction_lines_monthly")
                and self.replication_key
                and totalResults > 10000
            ):
                self.logger.info(
                    f"totalResults = {totalResults}, time_jump = {self.time_jump}"
                )
                if self.time_jump == relativedelta(months=1):
                    self.logger.info("Dropping time_jump to 1 week")
                    self.time_jump = relativedelta(weeks=1)
                    # need to reset the offset
                    return 0
                elif self.time_jump == relativedelta(weeks=1):
                    self.logger.info("Dropping time_jump to 3 days")
                    self.time_jump = relativedelta(days=3)
                    # need to reset the offset
                    return 0
                elif self.time_jump == relativedelta(days=3):
                    self.logger.info("Dropping time_jump to 1 day")
                    self.time_jump = relativedelta(days=1)
                    # need to reset the offset
                    return 0
                elif self.time_jump == relativedelta(days=1):
                    self.logger.info("Dropping time_jump to 12 hours")
                    self.time_jump = relativedelta(hours=12)
                    # need to reset the offset
                    return 0
                elif self.time_jump == relativedelta(hours=12):
                    self.logger.info("Dropping time_jump to 6 hours")
                    self.time_jump = relativedelta(hours=6)
                    # need to reset the offset
                    return 0
                elif self.time_jump == relativedelta(hours=6):
                    self.logger.info("Dropping time_jump to 1 hours")
                    self.time_jump = relativedelta(hours=1)
                    # need to reset the offset
                    return 0
                elif self.time_jump == relativedelta(hours=1):
                    self.logger.info("Dropping time_jump to 30 min")
                    self.time_jump = relativedelta(minutes=30)
                    # need to reset the offset
                    return 0
                elif self.time_jump == relativedelta(minutes=30):
                    self.logger.info("Dropping time_jump to 5 min")
                    self.time_jump = relativedelta(minutes=5)
                    # need to reset the offset
                    return 0
                elif self.time_jump == relativedelta(minutes=5):
                    self.logger.info("Dropping time_jump to 1 min")
                    self.time_jump = relativedelta(minutes=1)
                    # need to reset the offset
                    return 0
                else:
                    self.logger.error(
                        f"Even with minimum delta we are getting more than {self.cap_total_results} records! We will likely infinite loop."
                    )

            return offset

        if not self.stream_state.get("replication_key") and self.name == "inventory_item_locations" and not has_next:
            max_item_value = self.cap_total_results # TODO: this probably should be more dynamic
            interval_increment = 2500 # TODO: maybe we should lower this even further or make it dynamic
            # in the case we need to keep iterating, we should increment
            if self.custom_filter == f"item >= {max_item_value}":
                return None

            # extract the current range from the custom filter
            current_range = self.custom_filter.split("AND")
            min_value = int(current_range[0].split(">=")[1])
            max_value = int(current_range[1].split("<")[1])

            if min_value == max_item_value:
                self.custom_filter = f"item >= {max_item_value}"
            else:
                self.custom_filter = f"item >= {min_value + interval_increment} AND item < {max_value + interval_increment}"

            return 0

        if (
            (isinstance(self, TransactionRootStream) or isinstance(self, BulkParentStream)) 
            and self.config.get("transaction_lines_monthly") 
            and self.replication_key 
            and not has_next
        ):
            today = datetime.now()
            today = today.replace(tzinfo=pytz.UTC)
            if self.end_date >= today:
                self.logger.info("Reached the end of the line! Stopping")
                return None
            else:
                if self.time_jump in [relativedelta(minutes=30), relativedelta(minutes=5), relativedelta(minutes=1)]:
                    # reset the time_jump if we're going into a new hour
                    reset_time_jump = self.start_date.hour != (self.start_date + self.time_jump).hour
                else:
                    # reset the time_jump if we're going into a new month
                    reset_time_jump = self.start_date.month != (self.start_date + self.time_jump).month
                # we should move to the next date range now
                self.start_date = self.start_date + self.time_jump
                if reset_time_jump:
                    self.logger.info("Resetting time_jump to 1 month for next iteration...")
                    self.time_jump = relativedelta(months=1)
                self.logger.info(f"Reached end of data for current period. Moving start date to {self.start_date}")
                return 0

        if not has_next and offset < totalResults:
            if self.replication_key:
                json_path = f"$.items[-1].{self.replication_key}"
                last_dt = next(extract_jsonpath(json_path, response.json()))
                try:
                    self.query_date = pendulum.parse(last_dt)
                except Exception:
                    self.query_date = datetime.strptime(last_dt, "%d/%m/%Y")
                return offset
        return None

    def get_starting_timestamp(self, context):
        value = self.get_starting_replication_key_value(context)

        if value is None:
            return None

        if not self.is_timestamp_replication_key:
            raise ValueError(
                f"The replication key {self.replication_key} is not of timestamp type"
            )
        try:
            return cast(datetime, pendulum.parse(value))
        except pendulum.exceptions.ParserError:
            formats = [
                'MM/DD/YYYY',
            ]
            for fmt in formats:
                try:
                    parsed_date = pendulum.from_format(value, fmt)
                    return parsed_date
                except ValueError:
                    continue
            else:
                raise ValueError(f"Could not parse date: {value}")

    @cached
    def get_starting_time(self, context):
        start_date = parse(self.config.get("start_date"))
        rep_key = self.get_starting_timestamp(context)
        return rep_key or start_date

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        params["offset"] = (next_page_token or 0) % self.cap_total_results
        params["limit"] = self.page_size
        return params

    def get_date_boundaries(self):
        rep_key = self.stream_state
        window = self.config.get("window_days")
        report_periods = self.config.get("report_periods", 3)
        if self.query_date:
            start_date = self.query_date
            self.start_date_f = start_date.strftime("%Y-%m-%d")
        elif (self.name == "general_ledger_report" and self.config.get("gl_full_sync")) or ("replication_key" not in rep_key):
            start_date = parse(self.config["start_date"])
            self.start_date_f = start_date.strftime("%Y-%m-01")
        elif self.name == "general_ledger_report":
            today = date.today()
            beginning_of_month = today.replace(day=1)
            start_date = (beginning_of_month - relativedelta(months=report_periods - 1))
            self.start_date_f = start_date.strftime("%Y-%m-%d")
            self.logger.info(f"Not initial sync, fetching GL entries for last {report_periods} months, starting from {self.start_date_f}")
        else:
            start_date = self.get_starting_time({})
            self.start_date_f = start_date.strftime("%Y-%m-01")
        self.end_date = (start_date + timedelta(window)).strftime("%Y-%m-%d")

    def format_date_query(self, field_name):
        prefix = self.select_prefix or self.table
        return f"TO_CHAR ({prefix}.{field_name}, 'YYYY-MM-DD HH24:MI:SS') AS {field_name}"

    def _field_name_to_select_expr(self, field_name: str) -> str:
        """Build a single-field SuiteQL SELECT expression for the given schema field."""
        field_type = self.schema["properties"].get(field_name) or {}
        if field_type.get("format") == "date-time":
            return self.format_date_query(field_name)
        prefix = self.select_prefix or self.table
        return f"{prefix}.{field_name} AS {field_name}"

    def _suiteql_probe_response(
        self, select_exprs: List[str], where: Optional[str] = None
    ) -> Optional[requests.Response]:
        """Run a minimal SuiteQL probe query and return the response, if any."""
        if not select_exprs:
            return None
        session = self.get_session()
        table = self.query_table or self.table
        join = self.join if self.join else ""
        query = f"SELECT {', '.join(select_exprs)} FROM {table} {join}"
        if where:
            query += f" WHERE {where}"
        prepared_req = session.prepare_request(
            requests.Request(
                method="POST",
                url=f"{self.url_base}?limit=1",
                headers=self.http_headers,
                json={"q": query},
            )
        )
        try:
            return session.send(prepared_req, timeout=self.timeout)
        except Exception as exc:
            self.logger.debug(
                "SuiteQL field probe failed for stream %s: %s; error: %s",
                self.name,
                query,
                exc,
            )
            return None

    def _probe_suiteql_select(
        self, select_exprs: List[str], where: Optional[str] = None
    ) -> bool:
        """Return True when a minimal SuiteQL query with the given SELECT succeeds."""
        response = self._suiteql_probe_response(select_exprs, where)
        if response is not None and response.status_code == 200:
            return True
        if response is not None:
            self.logger.debug(
                "SuiteQL field probe returned %s for stream %s; response: %s",
                response.status_code,
                self.name,
                (response.text or "")[:500],
            )
        return False

    def _probe_suiteql_field_is_invalid(
        self, field_name: str, where: Optional[str] = None
    ) -> Optional[bool]:
        """Return True if the field is invalid, False if valid, None if inconclusive."""
        response = self._suiteql_probe_response(
            [self._field_name_to_select_expr(field_name)],
            where=where,
        )
        if response is None:
            return None
        if response.status_code == 200:
            return False
        if response.status_code == 400:
            invalid_names = self._extract_invalid_suiteql_fields_from_400(response)
            if field_name.lower() in invalid_names:
                return True
        if response.status_code == 500 and "UNEXPECTED_ERROR" in response.text:
            return True
        self.logger.debug(
            "SuiteQL field probe inconclusive for %s on stream %s: status=%s; response: %s",
            field_name,
            self.name,
            response.status_code,
            (response.text or "")[:500],
        )
        return None

    def _selected_field_names(self, select_all_by_default: bool = False) -> List[str]:
        """Return catalog field names that would be included in a dynamic SuiteQL SELECT."""
        return [
            key[1]
            for key, value in self.metadata.items()
            if isinstance(key, tuple)
            and len(key) == 2
            and (value.selected if not select_all_by_default else True)
            and key[1] not in self.invalid_fields
            and key[1] not in SUITEQL_EXCLUDED_FIELDS
        ]

    def _identify_and_skip_invalid_suiteql_field(self) -> bool:
        """Probe selected fields individually and skip the first one that breaks SuiteQL."""
        prefix = self.select_prefix or self.table

        # sanity check to make sure the stream is accessible
        sanity_select = [f"{prefix}.id AS id"]
        sanity_where = self.custom_filter or None
        probe_where = sanity_where
        if not self._probe_suiteql_select(sanity_select, where=sanity_where):
            if sanity_where:
                if not self._probe_suiteql_select(sanity_select):
                    return False
                probe_where = None
            else:
                return False

        field_names = self._selected_field_names()
        if not field_names:
            return False

        for field_name in field_names:
            field_invalid = self._probe_suiteql_field_is_invalid(
                field_name,
                where=probe_where,
            )
            if field_invalid is None:
                continue
            if field_invalid:
                self.invalid_fields.append(field_name)
                self.logger.info(
                    "Field %s causes SuiteQL errors on stream %s, skipping it from the query",
                    field_name,
                    self.name,
                )
                return True
        return False

    def _extract_invalid_suiteql_fields_from_400(
        self, response: requests.Response
    ) -> List[str]:
        """Return field names NetSuite flagged as invalid in a 400 SuiteQL error."""
        if (
            "Search error occurred: Field" in response.text
            or "Invalid search query" in response.text
        ):
            error_details = response.json()["o:errorDetails"][0]["detail"]
            field_names = [
                match.group(1).lower()
                for match in re.finditer(r"(?i)field '(\w+)'", error_details)
            ]
            if field_names:
                return field_names
            unknown_in_detail = re.search(
                r"Unknown identifier '([^']+)'",
                error_details,
            )
            if unknown_in_detail:
                return [unknown_in_detail.group(1).lower()]

        unknown_identifier = re.search(
            r"Unknown identifier '([^']+)'",
            response.text,
        )
        if unknown_identifier:
            return [unknown_identifier.group(1).lower()]
        return []

    def _skip_suiteql_fields_and_retry(
        self,
        field_names: List[str],
        response_text: str,
    ) -> None:
        """Drop fields from the next SuiteQL SELECT and retry the request."""
        newly_skipped = []
        for field_name in field_names:
            if field_name not in self.invalid_fields:
                self.invalid_fields.append(field_name)
                newly_skipped.append(field_name)
                self.logger.info(
                    "Field %s is not valid for SuiteQL on stream %s, skipping it from the query",
                    field_name,
                    self.name,
                )

        if newly_skipped:
            self.logger.info(
                "Skipping invalid SuiteQL fields on stream %s: %s",
                self.name,
                self.invalid_fields,
            )
            raise RetryRequest(response_text)

    def get_selected_properties(self, select_all_by_default=False):
        selected_properties = []
        for key, value in self.metadata.items():
            if isinstance(key, tuple) and len(key) == 2 and (value.selected if not select_all_by_default else True) and key[1] not in self.invalid_fields and key[1] not in SUITEQL_EXCLUDED_FIELDS:
                field_name = key[-1]
                prefix = self.select_prefix or self.table
                field_type = self.schema["properties"].get(field_name) or dict()
                if field_type.get("format") == "date-time":
                    field_name = self.format_date_query(field_name)
                else:
                    field_name = f"{prefix}.{field_name} AS {field_name}"
                selected_properties.append(field_name)
        return selected_properties

    def prepare_request_payload( # noqa: C901
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:

        filters = []
        order_by = ""
        time_format = "TO_TIMESTAMP('%Y-%m-%d %H:%M:%S', 'YYYY-MM-DD HH24:MI:SS')"

        if self.replication_key and "_report" not in self.name:
            prefix = self.replication_key_prefix or self.table
            order_by = f"ORDER BY {prefix}.{self.replication_key}"

            conditions = self.get_replication_key_conditions(context)
            if conditions is not None:
                filters.extend(conditions)
            else:
                start_date = self.get_starting_time(context)
                if self.query_date:
                    start_date_str = self.query_date.strftime(time_format)
                    filters.append(f"{prefix}.{self.replication_key}>{start_date_str}")
                elif start_date:
                    start_date_str = start_date.strftime(time_format)
                    filters.append(f"{prefix}.{self.replication_key}>{start_date_str}")

        if self.replication_key_prefix is None and self.order_by is not None:
            order_by = self.order_by

        if "_report" in self.name and self.custom_filter:
            self.get_date_boundaries()
            custom_filter = self.custom_filter.format(
                start_date=self.start_date_f , end_date=self.end_date
            )
            filters.append(custom_filter)
        else:
            if self.type_filter:
                filters.append(f"(Type='{self.type_filter}')")
            if self.custom_filter:
                filters.append(self.custom_filter)

        if filters:
            filters = "WHERE " + " AND ".join(filters)
        else:
            filters = ""

        selected_properties = self.get_selected_properties()

        if self.select:
            select = self.select
        else:
            select = ", ".join(selected_properties)

        if self.append_select:
            select = self.append_select + select

        if not select:
            select = "*"

        join = self.join if self.join else ""
        table = self.query_table or self.table

        payload = dict(
            q=f"SELECT {select} FROM {table} {join} {filters} {order_by}"
        )
        self.logger.info(f"Making query ({payload['q']})")
        return payload

    def validate_response(self, response: requests.Response) -> None: # noqa: C901
        """Validate HTTP response."""
        if response.status_code == 400:
            if hasattr(self,"entities_fallback") and self.entities_fallback:
                for entity in self.entities_fallback:
                    if "Record \'{}\' was not found.".lower().format(entity['name']) in response.text.lower():
                        self.logger.info(f"Missing {entity['name']} permission. Retrying with updated query...")
                        if "select_replace" in entity:
                            replacement = entity.get("select_replace_with", "")
                            self.select = self.select.replace(entity['select_replace'], replacement)
                        if "join_replace" in entity:  
                            self.join = self.join.replace(entity['join_replace'], "")
                        if entity['name'] == "accountingbook":
                            self.gl_use_only_primary_accounting_book = lambda: False
                        raise RetryRequest(response.text)

            # looks for invalid fields in the response to skip them and retry the request
            invalid_field_names = self._extract_invalid_suiteql_fields_from_400(
                response
            )
            if invalid_field_names:
                self._skip_suiteql_fields_and_retry(
                    invalid_field_names,
                    response.text,
                )

        if response.status_code == 401:
            raise InvalidCredentialsError(f"Authentication failed with response code {response.status_code}: {response.text}")

        if 500 <= response.status_code < 600 or response.status_code in [429]:
            # 500 UNEXPECTED_ERROR sometimes happens when a field is invalid
            if (
                response.status_code == 500
                and "UNEXPECTED_ERROR" in response.text
                and self._identify_and_skip_invalid_suiteql_field()
            ):
                raise RetryRequest(response.text)

            msg = (
                f"{response.status_code} Server Error: "
                f"{response.reason} for path: {self.path}"
                f"Response: {response.text}"
            )
            raise RetriableAPIError(msg)
        elif 400 <= response.status_code < 500:
            msg = (
                f"{response.status_code} Client Error: "
                f"{response.reason} for path: {self.path}"
                f"Response: {response.text}"
            )
            raise FatalAPIError(msg)

    def request_decorator(self, func: Callable) -> Callable:
        """Instantiate a decorator for handling request failures."""
        decorator: Callable = backoff.on_exception(
            backoff.expo,
            (
                HTTPError,
                RetriableAPIError,
                requests.exceptions.Timeout,
                requests.exceptions.ConnectionError,
                RemoteDisconnected,
                RetryRequest,
                InvalidCredentialsError,
            ),
            max_tries=10,
            factor=3,
        )(func)
        return decorator

    def last_day_of_month(self, any_day):
        # The day 28 exists in every month. 4 days later, it's always next month
        next_month = any_day.replace(day=28) + timedelta(days=4)
        # subtracting the number of the current day brings us back one month
        return next_month - timedelta(days=next_month.day)

    def make_request(self, context, next_page_token):
        # Retry the request with updated query
        # NOTE: We have to call prepare_request again to properly build the OAuth1 headers or we get 401
        prepared_request = self.prepare_request(
            context, next_page_token=next_page_token
        )
        resp = self._request(prepared_request, context)
        return resp

    def request_records(self, context: Optional[dict]) -> Iterable[dict]:
        # override the request_records method to handle updated query
        next_page_token: Any = None
        finished = False
        decorated_request = self.request_decorator(self.make_request)

        while not finished:
            resp = decorated_request(context, next_page_token)

            # store primary keys to avoid duplicated records if primary keys is available
            for row in self.parse_response(resp):
                # need to use final_row otherwise the pk may be missing
                final_row = self.post_process(row, context)
                if self.primary_keys:
                    if len(self.primary_keys) == 1:
                        pk = final_row[self.primary_keys[0]]
                    else:
                        pk = "-".join([str(final_row[key]) for key in self.primary_keys])
                    if pk not in self.record_ids:
                        self.record_ids.add(pk)
                        yield row
                else:
                    yield row
            previous_token = copy.deepcopy(next_page_token)
            next_page_token = self.get_next_page_token(
                response=resp, previous_token=previous_token
            )
            if next_page_token and next_page_token == previous_token:
                raise RuntimeError(
                    f"Loop detected in pagination. "
                    f"Pagination token {next_page_token} is identical to prior token."
                )
            # Cycle until get_next_page_token() no longer returns a value
            finished = next_page_token is None

    def _write_state_message(self) -> None:
        """Write out a STATE message with the latest state."""
        tap_state = self.tap_state

        if tap_state and tap_state.get("bookmarks"):
            for stream_name in tap_state.get("bookmarks").keys():
                if tap_state["bookmarks"][stream_name].get("partitions"):
                    tap_state["bookmarks"][stream_name]["partitions"] = []

        singer.write_message(StateMessage(value=tap_state))

    def process_number(self, field, value):
        return_value = value
        # Attempt to cast to float only if the value is a string with decimals
        if isinstance(value, str) and "." in value:
            try:
                return_value = float(value)
            except ValueError:
                self.logger.error(
                    f"Could not cast {field} : `{value}` to number"
                )
                raise Exception(ValueError)
        # only parse if it's a string
        elif isinstance(value, str):
            # Attempt to cast to int if there are no decimals
            try:
                return_value = int(value)
            except ValueError:
                self.logger.error(f"Could not cast {field} : `{value}` to integer")
                raise Exception(ValueError)
        return return_value

    def _join_filters(self, filters):
        return f"({' '.join(filters)})"

    def _escape_quotes(self, value):
        if isinstance(value, str):
            escaped_value = value.replace("'", "''")
            return f"'{escaped_value}'"
        return value

    def _parse_filters(self, filters):
        parsed_filters = []
        for key, value in filters.items():
            if key.startswith("group_"):
                group_filters = self._parse_filters(value)
                if group_filters and len(group_filters) > 0:
                    parsed_filters.append(self._join_filters(group_filters))
            elif key.startswith("clause_"):
                if value['operator'] == "EQ":
                    parsed_filters.append(f"{value['field']} = {self._escape_quotes(value['value'])}")
                elif value['operator'] == "IN":
                    if isinstance(value['value'], list):
                        if len(value['value']) == 0:
                            continue
                        filter_value = ", ".join(f"{self._escape_quotes(v)}" for v in value['value'])
                    else:
                        filter_value = f"{self._escape_quotes(value['value'])}"
                    parsed_filters.append(f"{value['field']} {value['operator']} ({filter_value})")
                else:
                    raise ValueError(f"Unsupported operator: {value['operator']}")
            elif key.startswith("operator_"):
                parsed_filters.append(value)

        return parsed_filters

    def setup_selected_filters(self):
        if self._selected_filters:
            self.logger.info(f"Parsing '{self.name}' filters: {self._selected_filters}")
            parsed_filters = self._parse_filters(self._selected_filters)
            if parsed_filters and len(parsed_filters) > 0:
                parsed_filters = self._join_filters(parsed_filters)
                if self.custom_filter:
                    self.custom_filter = f"{self.custom_filter} AND {parsed_filters}"
                else:
                    self.custom_filter = parsed_filters


class NetsuiteDynamicSchema(NetSuiteStream):
    schema_response = None
    fields = None
    date_fields = []
    bool_fields = []
    use_dynamic_fields = False
    filter_fields = False
    default_fields = []


    def __init__(self, *args, **kwargs):
        self.float_fields = []
        self.integer_fields = []
        return super().__init__(*args, **kwargs)

    @backoff.on_exception(backoff.expo, (
        HTTPError,
        RetriableAPIError,
        requests.exceptions.Timeout,
        requests.exceptions.ConnectionError,
        RemoteDisconnected,
    ), max_tries=5, factor=2)
    def get_schema(self): # noqa: C901
        s = self.get_session()
        self.logger.debug(
            "get_schema(%s) start table=%s use_dynamic_fields=%s",
            self.name,
            self.table,
            self.use_dynamic_fields,
        )

        try:
            if self.use_dynamic_fields:
                # TODO: refactor this to not force the except like this lol
                raise Exception("Switching to dynamic fields...")

            self.logger.info(f"Getting schema for {self.table} - stream: {self.name}")

            account = self.config["ns_account"].replace("_", "-").replace("SB", "sb")
            url = f"https://{account}.suitetalk.api.netsuite.com/services/rest/record/v1/metadata-catalog/{self.table}"
            prepared_req = s.prepare_request(
                requests.Request(
                    method="GET",
                    url=url,
                    headers=self.http_headers,
                )
            )
            prepared_req.headers.update({"Accept": "application/schema+json"})
            self.logger.debug("get_schema(%s): metadata-catalog GET send", self.name)
            response = s.send(prepared_req, timeout=self.timeout)
            self.logger.debug(
                "get_schema(%s): metadata-catalog GET done status=%s",
                self.name,
                response.status_code,
            )
            response.raise_for_status()
            self.schema_response = response.json()
        except:
            pass
        
        # if any stream doesn't have access to metadata endpoint, fetch first 1k records and custom fields to build the schema

        # fetch custom fields
        add_custom_fields_streams = ["invoices", "bills", "invoice_lines", "bill_lines", "bill_expenses"]
        if not self.schema_response  and self._tap.custom_fields is None and self.name in add_custom_fields_streams:
            # request custom fields types
            offset = 0
            custom_fields = {}

            self.logger.info("Fetching custom fields data")
            while offset is not None:
                self.logger.debug(
                    "get_schema(%s): customfield suiteql send offset=%s",
                    self.name,
                    offset,
                )
                prepared_req = s.prepare_request(
                    requests.Request(
                        method="POST",
                        url=f"{self.url_base}?offset={offset}&limit=1000",
                        headers=self.http_headers,
                        json={
                            "q": "SELECT * FROM customfield"
                        }
                    )
                )
                response = s.send(prepared_req, timeout=self.timeout)
                self.logger.debug(
                    "get_schema(%s): customfield suiteql done offset=%s status=%s",
                    self.name,
                    offset,
                    response.status_code,
                )
                if response.status_code not in [200]:
                    self.logger.error(f"Failed to fetch custom fields for {self.table} - stream: {self.name}, Error: {response.text}, not able to add custom fields to the schema")
                    break
                offset = self.get_next_page_token(response, offset)
                custom_fields.update({cf.get("scriptid").lower(): cf.get("fieldvaluetype") for cf in response.json().get("items", [])})
            
            self._tap.custom_fields = custom_fields


        # fetch top 1000 records to infer fields and types
        if not self.schema_response or self.filter_fields:
            self.fields = set()

            self.logger.info(f"Getting schema for {self.table} - stream: {self.name}")
            url = f"{self.url_base}?offset=0&limit=1000"

            prepared_req = s.prepare_request(
                requests.Request(
                    method="POST",
                    url=url,
                    headers=self.http_headers,
                    json={
                        "q": f"SELECT TOP 1000 * FROM {self.table} ORDER BY {self.replication_key} DESC" if self.replication_key else f"SELECT TOP 1000 * FROM {self.table}"
                    }
                )
            )

            self.logger.debug(
                "get_schema(%s): suiteql schema inference POST send url=%s",
                self.name,
                url,
            )

            response = s.send(prepared_req, timeout=self.timeout)
            self.logger.debug(
                "get_schema(%s): suiteql schema inference POST done status=%s",
                self.name,
                response.status_code,
            )
            try:
                response.raise_for_status()
                self.logger.debug(
                    "get_schema(%s): suiteql schema inference parsing response JSON",
                    self.name,
                )
                # NOTE: this will only get fields in the first 1k records, we could still miss things
                for item in response.json().get("items"):
                    self.fields.update(set(item.keys()))

                # decide which ones are date fields
                pot_date_fields = [f for f in self.fields if 'date' in f and 'custbody' not in f and 'custrecord' not in f]
                for f in pot_date_fields:
                    match = [i for i in response.json().get("items") if i.get(f)]
                    if len(match) > 0:
                        try:
                            try:
                                parse(match[0][f])
                            except:
                                pendulum.from_format(match[0][f], "MM/DD/YYYY")
                            self.date_fields.append(f)
                        except:
                            pass

                # decide who ones are boolean fields
                def all_bool(f):
                    match = [i for i in response.json().get("items") if i.get(f) in ["T", "F", None]]
                    return len(match) == len(response.json().get("items"))

                self.bool_fields = [f for f in self.fields if all_bool(f)]

                self.fields -= SUITEQL_EXCLUDED_FIELDS

                # for bills and invoices add/update custom fields and its types
                if self._tap.custom_fields:
                    cf_prefix = None
                    if self.name in ["invoices", "bills"]:
                        cf_prefix = "custbody"
                    elif self.name in ["invoice_lines", "bill_lines", "bill_expenses"]:
                        cf_prefix = "custcol"
                    
                    # add fields and types to build schema
                    if cf_prefix:
                        table_cf = {k:v for k,v in self._tap.custom_fields.items() if k.startswith(cf_prefix)}
                        self.fields.update(table_cf.keys())              
                        for cf, cf_type in table_cf.items():
                            if cf_type in ["Decimal Number", "Percent"]:
                                self.float_fields.append(cf)
                            elif cf_type in ["Integer Number"]:
                                self.integer_fields.append(cf)
                            elif cf_type in ["Date/Time"]:
                                self.date_fields.append(cf)
                            elif cf_type in ["Check Box"]:
                                self.bool_fields .append(cf)
                self.logger.debug(
                    "get_schema(%s): suiteql schema inference finished",
                    self.name,
                )
            except:
                self.logger.warning(f"Failed to get schema for {self.table} - stream: {self.name}")
                pass


    @property
    def schema(self): # noqa: C901
        if self.config.get("use_input_catalog", True) and self._tap.input_catalog and self._tap.input_catalog.get(self.name):
            return self._tap.input_catalog.get(self.name).schema.to_dict()

        # Get netsuite schema for table
        if self.fields is None and self.schema_response is None:
            self.logger.debug("schema(%s): calling get_schema()", self.name)
            self.get_schema()
            self.logger.debug("schema(%s): get_schema() returned", self.name)

        if self.fields is not None and not self.schema_response:
            fields = self.fields
            properties_list = deepcopy(self.default_fields)
            # Remove any fields that are already in default_fields to avoid overriding the type
            fields = {f for f in fields if not any(df.name == f.lower() for df in self.default_fields)}
            for field in fields:
                if field in SUITEQL_EXCLUDED_FIELDS:
                    continue
                if field == self.replication_key or field in self.date_fields:
                    properties_list.append(th.Property(field.lower(), th.DateTimeType))
                elif field in self.bool_fields:
                    properties_list.append(th.Property(field.lower(), th.BooleanType))
                elif field in self.float_fields:
                    properties_list.append(th.Property(field.lower(), th.NumberType))
                elif field in self.integer_fields:
                    properties_list.append(th.Property(field.lower(), th.IntegerType))
                else:
                    properties_list.append(th.Property(field.lower(), th.StringType))

            return th.PropertiesList(*properties_list).to_dict()

        if self.schema_response:
            response = self.schema_response 
            properties_list = deepcopy(self.default_fields) if self.always_add_default_fields else []
            if response is not None and response.get("properties"):
                for field, value in response.get("properties").items():
                    field_lower = field.lower()
                    if field_lower in SUITEQL_EXCLUDED_FIELDS:
                        continue
                    if self.fields and self.filter_fields and field_lower not in self.fields:
                        continue

                    if value.get("format") == 'date-time':
                        properties_list.append(th.Property(field.lower(), th.DateTimeType))
                    elif value.get("format") == "date":
                        properties_list.append(th.Property(field.lower(), th.DateType))
                    elif value["type"] == "string":
                        properties_list.append(th.Property(field.lower(), th.StringType))
                    elif value["type"] == "boolean":
                        properties_list.append(th.Property(field.lower(), th.BooleanType))
                    elif value["type"] in ["number", "integer"]:
                        properties_list.append(th.Property(field.lower(), th.NumberType))
                    else:
                        #Object and array as custom types
                        properties_list.append(th.Property(field.lower(), th.CustomType({"type": [value["type"],"string"]})))
            return th.PropertiesList(*properties_list).to_dict()

class NetsuiteDynamicStream(NetsuiteDynamicSchema):
    schema_response = None
    fields = None
    date_fields = []
    bool_fields = []
    use_dynamic_fields = False
    default_fields = []

    @property
    def select(self):
        if not self.selected and self.has_selected_descendents:
            selected_properties = self.get_selected_properties(select_all_by_default=True)
            return ",".join(selected_properties)
        elif self.selected and hasattr(self, "_select"):
            selected_fields = self._select.split(",")
            if any(f for f in selected_fields if f.endswith("*")):
                # For .* queries, keep the wildcard but explicitly format datetime fields
                datetime_fields = []
                for field_name, field_info in self.schema["properties"].items():
                    field_type = field_info.get("type", ["null"])[0]
                    field_format = field_info.get("format")
                    if field_type == "string" and field_format in ["date-time", "date"] and field_name not in self.invalid_fields:
                        datetime_fields.append(self.format_date_query(field_name))

                # Replace any .* with explicit datetime fields + .*
                modified_fields = []
                for field in selected_fields:
                    if datetime_fields:
                        modified_fields.extend(datetime_fields)
                    if field.endswith("*"):
                        modified_fields.insert(0, field)
                    else:
                        modified_fields.append(field)
                return ",".join(modified_fields)
            else:
                return self._select
        else:
            return None
    
    def process_types(self, row, schema=None): # noqa: C901
        if schema is None:
            schema = self.schema["properties"]
        for field, value in row.items():
            if field not in schema:
                # Skip fields not found in the schema
                continue

            field_info = schema[field]
            field_type = field_info.get("type", ["null"])[0]
            # Process nested properties
            if "properties" in field_info:
                row[field] = self.process_types(value, field_info["properties"])
            # Process nested properties
            if "items" in field_info:
                if isinstance(value, list):
                    row[field] = [
                        self.process_types(v, field_info["items"].get("properties"))
                        for v in value
                    ]
            field_format = field_info.get("format", None)
            if field_type == "string" and field_format == "date-time":
                # if it's already correctly a datetime, don't need to do anything
                if isinstance(value, datetime):
                    row[field] = value
                    continue

                try:
                    # Attempt to parse string as date-time
                    # If successful, no need to cast
                    _ = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ")
                except ValueError:
                    # If parsing fails, consider it as a type mismatch and attempt to cast
                    try:
                        row[field] = parse(value)
                    except:
                        row[field] = pendulum.from_format(value, "MM/DD/YYYY")
            elif field_type == "boolean":
                if not isinstance(value, bool):
                    # Attempt to cast to boolean
                    if value.lower() in ["true", "t"]:
                        row[field] = True
                    elif value.lower() in ["false", "f"]:
                        row[field] = False
                    else:
                        # No need to raise an error, just continue with the loop
                        continue

            elif field_type == "number" or field_type == "integer":
                if isinstance(value, str):
                    row[field] = self.process_number(field, value)

            elif field_type == "string":
                if not isinstance(value, str):
                    # Attempt to cast to string
                    row[field] = str(value)
            elif field_type == "array":
                array_types = field_info.get("type", ["null"])
                if isinstance(value, list):
                    continue
                else:
                    for array_type in array_types:
                        if array_type == "string":
                            try:
                                # Attempt to cast to JSON
                                parsed_value = json.loads(value)
                                if isinstance(parsed_value, list):
                                    row[field] = parsed_value
                                else:
                                    # We only want valid lists
                                    raise ValueError
                            except (ValueError, json.JSONDecodeError, TypeError):
                                if not isinstance(value, str):
                                    # Attempt to cast to string
                                    row[field] = str(value)
                        if array_type == "number" or array_type == "integer":
                            row[field] = self.process_number(field, value)

            else:
                # Unsupported type
                # No need to raise an error, just continue with the loop
                continue
        return row
    
    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        row = self.process_types(row)
        return row


class BulkParentStream(NetsuiteDynamicStream):

    child_context_keys = ["ids"]
    start_date = None
    end_date = None

    @property
    def child_context_size(self):
        return self.config.get("child_context_size", 250)

    def get_replication_key_conditions(self, context):
        if not self.config.get("transaction_lines_monthly") or not self.replication_key:
            return None
        start = self.start_date or super().get_starting_time(context)
        if not start:
            return None
        self.start_date = start
        self.end_date = self.start_date + self.time_jump
        time_fmt = "TO_TIMESTAMP('%Y-%m-%d %H:%M:%S', 'YYYY-MM-DD HH24:MI:SS')"
        prefix = self.replication_key_prefix or self.table
        start_str = self.start_date.strftime(time_fmt)
        end_str = self.end_date.strftime(time_fmt)
        # Use >= so boundary records are included in the next window (avoids dropping between windows).
        return [
            f"{prefix}.{self.replication_key}>{start_str}",
            f"{prefix}.{self.replication_key}<={end_str}",
        ]

    def _sync_records(  # noqa C901  # too complex
        self, context: Optional[dict] = None
    ) -> None:
        record_count = 0
        current_context: Optional[dict]
        context_list: Optional[List[dict]]
        context_list = [context] if context is not None else self.partitions
        selected = self.selected

        for current_context in context_list or [{}]:
            partition_record_count = 0
            current_context = current_context or None
            state = self.get_context_state(current_context)
            state_partition_context = self._get_state_partition_context(current_context)
            self._write_starting_replication_value(current_context)
            child_context: Optional[dict] = (
                None if current_context is None else copy.copy(current_context)
            )
            child_context_bulk = {key: [] for key in self.child_context_keys}
            for record_result in self.get_records(current_context):
                if isinstance(record_result, tuple):
                    # Tuple items should be the record and the child context
                    record, child_context = record_result
                else:
                    record = record_result
                child_context = copy.copy(
                    self.get_child_context(record=record, context=child_context)
                )
                for key, val in (state_partition_context or {}).items():
                    # Add state context to records if not already present
                    if key not in record:
                        record[key] = val

                # Sync children, except when primary mapper filters out the record
                if self.stream_maps[0].get_filter_result(record):
                    # add id to child_context_bulk ids
                    if child_context:
                        for key, value in child_context.items():
                            child_context_bulk[key].extend(child_context[key]) if value else None
                
                if any(len(v) >= self.child_context_size for v in child_context_bulk.values()):
                    self._sync_children(child_context_bulk)
                    child_context_bulk = {key: [] for key in self.child_context_keys}

                self._check_max_record_limit(record_count)
                if selected:
                    if (record_count - 1) % self.STATE_MSG_FREQUENCY == 0:
                        self._write_state_message()
                    self._write_record_message(record)
                    try:
                        self._increment_stream_state(record, context=current_context)
                    except InvalidStreamSortException as ex:
                        log_sort_error(
                            log_fn=self.logger.error,
                            ex=ex,
                            record_count=record_count + 1,
                            partition_record_count=partition_record_count + 1,
                            current_context=current_context,
                            state_partition_context=state_partition_context,
                            stream_name=self.name,
                        )
                        raise ex

                record_count += 1
                partition_record_count += 1
            # process remaining child context if len < 1000
            if any(v != [] for v in child_context_bulk.values()):
                self._sync_children(child_context_bulk)
            #----
            if current_context == state_partition_context:
                # Finalize per-partition state only if 1:1 with context
                finalize_state_progress_markers(state)
        if not context:
            # Finalize total stream only if we have the full full context.
            # Otherwise will be finalized by tap at end of sync.
            finalize_state_progress_markers(self.stream_state)
        self._write_record_count_log(record_count=record_count, context=context)
        # Reset interim bookmarks before emitting final STATE message:
        self._write_state_message()

class TransactionRootStream(NetsuiteDynamicStream):
    select = None
    start_date = None
    end_date = None
    fields = None
    default_fields = []
    date_fields = []

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:
        # Avoid using my new logic if the flag is off
        if not self.config.get("transaction_lines_monthly"):
            return super().prepare_request_payload(context, next_page_token)

        filters = []
        # get order query
        prefix = self.replication_key_prefix or self.table
        order_by = f"ORDER BY {prefix}.{self.replication_key}"

        # get filter query
        start_date = self.start_date or self.get_starting_time(context)
        time_format = "TO_TIMESTAMP('%Y-%m-%d %H:%M:%S', 'YYYY-MM-DD HH24:MI:SS')"

        if start_date:
            start_date_str = start_date.strftime(time_format)

            self.start_date = start_date
            self.end_date = start_date + self.time_jump
            end_date_str = self.end_date.strftime(time_format)
            timeframe = f"{start_date_str} to {end_date_str}"

            filters.append(f"{prefix}.{self.replication_key}>={start_date_str} AND {prefix}.{self.replication_key}<{end_date_str}")

            filters = "WHERE " + " AND ".join(filters)

        selected_properties = self.get_selected_properties()
        if self.select:
            select = self.select.strip()
        else:
            select = ", ".join(selected_properties)

        join = self.join if self.join else ""

        payload = dict(
            q=f"SELECT {select} FROM {self.table} {join} {filters} {order_by}"
        )
        self.logger.info(f"Making query ({timeframe})")
        return payload


    # Remove double spaces that might result from empty address fields
    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        # Collapse duplicate spaces in address fields
        row = super().post_process(row, context)
        if row.get("shippingaddress"):
            row["shippingaddress"] = re.sub(r'(, )+', ', ', row["shippingaddress"]).strip(', ')
            if row["shippingaddress"] == "":
                row.pop("shippingaddress")
        if row.get("billingaddress"):
            row["billingaddress"] = re.sub(r'(, )+', ', ', row["billingaddress"]).strip(', ')
            if row["billingaddress"] == "":
                row.pop("billingaddress")
        

        return row


class NetsuiteSOAPStream(Stream):
    """NetSuite SOAP stream class."""
    page_size = 100


    def prepare_request_payload(self, context):
        return {}


    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        payload = self.prepare_request_payload(context)

        for record in self._tap.soap_client.search(payload, self.extract_json_path, self.page_size):
            transformed_record = self.post_process(record, context)
            if transformed_record is None:
                # Record filtered out during post_process()
                continue
            yield transformed_record
