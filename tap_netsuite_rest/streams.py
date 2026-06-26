"""Stream type classes for tap-netsuite-rest."""

from typing import Any, Dict, Optional, Iterable, Tuple
import uuid
import requests
import base64

from hotglue_singer_sdk import typing as th

from tap_netsuite_rest.client import (
    NetSuiteStream,
    NetsuiteDynamicStream,
    TransactionRootStream,
    BulkParentStream,
    NetsuiteSOAPStream,
)
from hotglue_singer_sdk.helpers.jsonpath import extract_jsonpath
from datetime import datetime, timedelta
from pendulum import parse
from hotglue_singer_sdk.exceptions import FatalAPIError

import os
job_id = os.environ.get("JOB_ID")
if job_id:
    sync_output_folder = f"/home/hotglue/{job_id}/sync-output"
else:
    sync_output_folder = "."

class VendorCreditStream(BulkParentStream):
    name = "vendor_credits"
    table = "transaction"
    custom_filter = "type = 'VendCred'"
    replication_key = "lastmodifieddate"
    _select = "*, BUILTIN.DF(status) status"

    default_fields = [
        th.Property("taxtotal", th.NumberType),
        th.Property("externalid", th.StringType),
        th.Property("lastmodifieddate", th.DateTimeType)
    ]

    def get_child_context(self, record, context) -> dict:
        return {"ids": [record["id"]]}

class VendorCreditLinesStream(NetsuiteDynamicStream):
    name = "vendor_credit_lines"
    table = "transactionline"
    parent_stream_type = VendorCreditStream
    _custom_filter = "mainline = 'F' AND (hascostline = 'T' OR accountinglinetype = 'EXPENSE')"

    default_fields = [
        th.Property("item", th.StringType),
        th.Property("quantity", th.NumberType),
        th.Property("rate", th.NumberType),
        th.Property("taxamount", th.NumberType),
    ]

    def prepare_request_payload(self, context, next_page_token):
        # fetch invoice lines filtering by transaction id
        ids = ", ".join(f"'{id}'" for id in context["ids"])
        self.custom_filter = f"{self._custom_filter}"
        self.custom_filter = f"{self.custom_filter} and transaction IN ({ids})"
        return super().prepare_request_payload(context, next_page_token)

class VendorCreditExpensesStream(NetsuiteDynamicStream):
    name = "vendor_credit_expenses"
    table = "transactionline"
    parent_stream_type = VendorCreditStream
    _select = "t.recordtype, tl.*"
    select_prefix = "tl"
    query_table = "transaction t"
    join = "INNER JOIN transactionline tl on tl.transaction = t.id"
    _custom_filter = "mainline = 'F' and accountinglinetype is NULL"

    default_fields = [
        th.Property("taxamount", th.NumberType),
    ]

    def prepare_request_payload(self, context, next_page_token):
        # fetch bill expenses filtering by transaction id from bills parent stream
        ids = ", ".join(f"'{id}'" for id in context["ids"])
        self.custom_filter = f"{self._custom_filter}"
        self.custom_filter = f"{self.custom_filter} and tl.transaction IN ({ids})"
        return super().prepare_request_payload(context, next_page_token)
    
class VendorCreditTaxLinesStream(NetsuiteDynamicStream):
    name = "vendor_credit_tax_lines"
    table = "transactionline"
    parent_stream_type = VendorCreditStream
    _select = "t.recordtype, tl.*"
    select_prefix = "tl"
    query_table = "transaction t"
    join = "INNER JOIN transactionline tl on tl.transaction = t.id"
    _custom_filter = "mainline = 'F' and taxline = 'T'"

    def prepare_request_payload(self, context, next_page_token):
        # fetch bill expenses filtering by transaction id from bills parent stream
        ids = ", ".join(f"'{id}'" for id in context["ids"])
        self.custom_filter = f"{self._custom_filter}"
        self.custom_filter = f"{self.custom_filter} and tl.transaction IN ({ids})"
        return super().prepare_request_payload(context, next_page_token)

class SalesTransactionsStream(TransactionRootStream):
    name = "sales_transactions"
    primary_keys = ["id", "lastmodifieddate"]
    table = "transaction"
    replication_key = "lastmodifieddate"
    custom_filter = "transaction.recordtype = 'salesorder'"


    join = """
        LEFT JOIN TransactionShippingAddress tsa ON transaction.shippingaddress = tsa.nkey
        LEFT JOIN TransactionBillingAddress tba ON transaction.billingaddress = tba.nkey
    """

    def get_selected_properties(self):
        transaction_properties = super().get_selected_properties()
        transaction_properties.extend([
            "COALESCE(tsa.addr1, '') || ', ' || COALESCE(tsa.addr2, '') || ', ' || COALESCE(tsa.addr3, '') || ', ' || COALESCE(tsa.city, '') || ', ' || COALESCE(tsa.state, '') || ', ' || COALESCE(tsa.zip, '') || ', ' || COALESCE(tsa.country, '') as shippingaddress",
            "COALESCE(tba.addr1, '') || ', ' || COALESCE(tba.addr2, '') || ', ' || COALESCE(tba.addr3, '') || ', ' || COALESCE(tba.city, '') || ', ' || COALESCE(tba.state, '') || ', ' || COALESCE(tba.zip, '') || ', ' || COALESCE(tba.country, '') as billingaddress"
        ])
        return transaction_properties

    schema = th.PropertiesList(
        th.Property("abbrevtype", th.StringType),
        th.Property("actualshipdate", th.DateTimeType),
        th.Property("billingaddress", th.StringType),
        th.Property("billingstatus", th.StringType),
        th.Property("closedate", th.DateTimeType),
        th.Property("createdby", th.StringType),
        th.Property("createddate", th.DateTimeType),
        th.Property("currency", th.StringType),
        th.Property("daysopen", th.StringType),
        th.Property("email", th.StringType),
        th.Property("employee", th.StringType),
        th.Property("entity", th.StringType),
        th.Property("exchangerate", th.StringType),
        th.Property("externalid", th.StringType),
        th.Property("foreigntotal", th.StringType),
        th.Property("id", th.StringType),
        th.Property("isfinchrg", th.StringType),
        th.Property("isreversal", th.StringType),
        th.Property("lastmodifiedby", th.StringType),
        th.Property("lastmodifieddate", th.DateTimeType),
        th.Property("linkedtrackingnumberlist", th.StringType),
        th.Property("nexus", th.StringType),
        th.Property("number", th.StringType),
        th.Property("ordpicked", th.StringType),
        th.Property("paymenthold", th.StringType),
        th.Property("paymentoption", th.StringType),
        th.Property("posting", th.StringType),
        th.Property("postingperiod", th.StringType),
        th.Property("printedpickingticket", th.StringType),
        th.Property("recordtype", th.StringType),
        th.Property("shipcomplete", th.StringType),
        th.Property("shipdate", th.DateTimeType),
        th.Property("shippingaddress", th.StringType),
        th.Property("source", th.StringType),
        th.Property("status", th.StringType),
        th.Property("trandate", th.DateTimeType),
        th.Property("trandisplayname", th.StringType),
        th.Property("tranid", th.StringType),
        th.Property("transactionnumber", th.StringType),
        th.Property("type", th.StringType),
        th.Property("typebaseddocumentnumber", th.StringType),
        th.Property("userevenuearrangement", th.StringType),
        th.Property("visibletocustomer", th.StringType),
        th.Property("void", th.StringType),
        th.Property("voided", th.StringType),
    ).to_dict()


class VendorBillsStream(TransactionRootStream):
    name = "vendor_bill_transactions"
    primary_keys = ["id"]
    table = "transaction"
    replication_key = "lastmodifieddate"
    custom_filter = "recordtype = 'vendorbill'"


    join = """
        LEFT JOIN TransactionShippingAddress tsa ON transaction.shippingaddress = tsa.nkey
        LEFT JOIN TransactionBillingAddress tba ON transaction.billingaddress = tba.nkey
    """

    schema = th.PropertiesList(
        th.Property("abbrevtype", th.StringType),
        th.Property("approvalstatus", th.StringType),
        th.Property("balsegstatus", th.StringType),
        th.Property("billingstatus", th.StringType),
        th.Property("billingaddress", th.StringType),
        th.Property("shippingaddress", th.StringType),
        th.Property("closedate", th.DateTimeType),
        th.Property("createdby", th.StringType),
        th.Property("createddate", th.DateTimeType),
        th.Property("currency", th.StringType),
        th.Property("customtype", th.StringType),
        th.Property("daysopen", th.StringType),
        th.Property("daysoverduesearch", th.StringType),
        th.Property("duedate", th.DateTimeType),
        th.Property("entity", th.StringType),
        th.Property("exchangerate", th.StringType),
        th.Property("foreignamountpaid", th.StringType),
        th.Property("foreignamountunpaid", th.StringType),
        th.Property("foreigntotal", th.StringType),
        th.Property("id", th.StringType),
        th.Property("isfinchrg", th.StringType),
        th.Property("isreversal", th.StringType),
        th.Property("lastmodifiedby", th.StringType),
        th.Property("lastmodifieddate", th.DateTimeType),
        th.Property("nexus", th.StringType),
        th.Property("number", th.StringType),
        th.Property("ordpicked", th.StringType),
        th.Property("paymenthold", th.StringType),
        th.Property("posting", th.StringType),
        th.Property("postingperiod", th.StringType),
        th.Property("printedpickingticket", th.StringType),
        th.Property("recordtype", th.StringType),
        th.Property("status", th.StringType),
        th.Property("trandate", th.DateTimeType),
        th.Property("trandisplayname", th.StringType),
        th.Property("tranid", th.StringType),
        th.Property("transactionnumber", th.StringType),
        th.Property("type", th.StringType),
        th.Property("userevenuearrangement", th.StringType),
        th.Property("visibletocustomer", th.StringType),
        th.Property("void", th.StringType),
        th.Property("voided", th.StringType),
    ).to_dict()

    def get_selected_properties(self):
        transaction_properties = super().get_selected_properties()
        transaction_properties.extend([
            "COALESCE(tsa.addr1, '') || ', ' || COALESCE(tsa.addr2, '') || ', ' || COALESCE(tsa.addr3, '') || ', ' || COALESCE(tsa.city, '') || ', ' || COALESCE(tsa.state, '') || ', ' || COALESCE(tsa.zip, '') || ', ' || COALESCE(tsa.country, '') as shippingaddress",
            "COALESCE(tba.addr1, '') || ', ' || COALESCE(tba.addr2, '') || ', ' || COALESCE(tba.addr3, '') || ', ' || COALESCE(tba.city, '') || ', ' || COALESCE(tba.state, '') || ', ' || COALESCE(tba.zip, '') || ', ' || COALESCE(tba.country, '') as billingaddress"
        ])
        return transaction_properties
    

class SalesTransactionLinesStream(TransactionRootStream):
    name = "sales_transactions_lines"
    primary_keys = ["id", "transaction"]
    table = "transaction t"
    replication_key = "linelastmodifieddate"
    join = "INNER JOIN transactionLine tl ON tl.transaction = t.id"
    custom_filter = "t.recordtype = 'salesorder'"
    replication_key_prefix = "tl"
    select_prefix = "tl"

    schema = th.PropertiesList(
        th.Property("blandedcost", th.StringType),
        th.Property("class", th.StringType),
        th.Property("cleared", th.StringType),
        th.Property("commitinventory", th.StringType),
        th.Property("commitmentfirm", th.StringType),
        th.Property("createdfrom", th.StringType),
        th.Property("debitforeignamount", th.StringType),
        th.Property("department", th.StringType),
        th.Property("donotdisplayline", th.StringType),
        th.Property("entity", th.StringType),
        th.Property("expenseaccount", th.StringType),
        th.Property("foreignamount", th.StringType),
        th.Property("fulfillable", th.StringType),
        th.Property("hasfulfillableitems", th.StringType),
        th.Property("id", th.StringType),
        th.Property("isbillable", th.StringType),
        th.Property("isclosed", th.StringType),
        th.Property("iscogs", th.StringType),
        th.Property("isfullyshipped", th.StringType),
        th.Property("isfxvariance", th.StringType),
        th.Property("isinventoryaffecting", th.StringType),
        th.Property("item", th.StringType),
        th.Property("itemtype", th.StringType),
        th.Property("kitcomponent", th.StringType),
        th.Property("landedcostperline", th.StringType),
        th.Property("linelastmodifieddate", th.DateTimeType),
        th.Property("linesequencenumber", th.StringType),
        th.Property("location", th.StringType),
        th.Property("mainline", th.StringType),
        th.Property("matchbilltoreceipt", th.StringType),
        th.Property("netamount", th.StringType),
        th.Property("oldcommitmentfirm", th.StringType),
        th.Property("paymentmethod", th.StringType),
        th.Property("processedbyrevcommit", th.StringType),
        th.Property("quantity", th.StringType),
        th.Property("quantitybilled", th.StringType),
        th.Property("quantitypacked", th.StringType),
        th.Property("quantitypicked", th.StringType),
        th.Property("quantityrejected", th.StringType),
        th.Property("quantityshiprecv", th.StringType),
        th.Property("shipmethod", th.StringType),
        th.Property("subsidiary", th.StringType),
        th.Property("taxline", th.StringType),
        th.Property("transaction", th.StringType),
        th.Property("transactiondiscount", th.StringType),
        th.Property("uniquekey", th.StringType),
        th.Property("units", th.StringType),
    ).to_dict()


# class InventoryItemLocationStream(NetSuiteStream):
#     name = "inventory_item_location"
#     primary_keys = ["ns_item_id", "lastmodifieddate"]
#     select = """
#         i.id AS ns_item_id,
#         i.itemid AS sku,
#         iil.quantity
#         """
#     table = "item i"
#     join = """
#         INNER JOIN (SELECT item, SUM(quantityavailable)
#         AS quantity FROM inventoryitemlocations GROUP BY item) iil
#         ON i.id = iil.item
#         """
#     custom_filter = "i.isinactive='F' AND i.itemtype='InvtPart'"

#     schema = th.PropertiesList(
#         th.Property("ns_item_id", th.StringType),
#         th.Property("quantity", th.StringType),
#         th.Property("sku", th.StringType),
#     ).to_dict()


class PricingStream(NetSuiteStream):
    name = "pricing"
    primary_keys = ["internalid"]
    table = "pricing"

    schema = th.PropertiesList(
        th.Property("internalid", th.StringType),
        th.Property("item", th.StringType),
        th.Property("pricelevel", th.StringType),
        th.Property("quantity", th.StringType),
        th.Property("saleunit", th.StringType),
        th.Property("unitprice", th.StringType),
    ).to_dict()


class InventoryPricingStream(NetSuiteStream):
    name = "inventory_pricing"
    primary_keys = ["ns_item_id"]
    select = (
        "p.item AS ns_item_id, p.pricelevel AS price_level_id, p.unitprice AS price"
    )
    table = "pricing p"
    join = "INNER JOIN item i ON p.item = i.id"
    custom_filter = "i.itemtype='InvtPart'"

    schema = th.PropertiesList(
        th.Property("ns_item_id", th.StringType),
        th.Property("price", th.StringType),
        th.Property("price_level_id", th.StringType),
    ).to_dict()


class VendorStream(BulkParentStream):
    name = "vendor"
    primary_keys = ["id"]
    table = "vendor"
    query_table = "vendor v"
    _select = "v.*, vsr.subsidiary, vsr.entity"
    join = "JOIN vendorsubsidiaryrelationship vsr ON vsr.entity = v.id"
    replication_key = "lastmodifieddate"
    replication_key_prefix = "v"
    always_add_default_fields = True
    select_prefix = "v"

    default_fields = [
        th.Property("defaultbillingaddress", th.StringType)
    ]

    def get_child_context(self, record, context) -> dict:
        address_keys = ["defaultbillingaddress", "defaultshippingaddress"]
        # Collect valid address IDs
        address_ids = {
            record.get(key) for key in address_keys 
            if record.get(key)
        }
        return {"ids": list(address_ids)}

    def get_available_filters_metadata(self) -> Dict[str, Any]:
        return {
            "supported_operators": ["OR", "AND"],
            "supports_nesting_clauses": True,
            "filters": {
                "id": {
                    "label": "Vendor ID",
                    "supported_operators": ["IN", "EQ"],
                    "target_field": "v.id",
                    "options": "reference_data.vendor.id",
                },
                "name": {
                    "label": "Vendor Name",
                    "supported_operators": ["IN", "EQ"],
                    "target_field": "v.altname",
                    "options": "reference_data.vendor.altname",
                },
            },
        }


# The following streams were removed because they are not documented by Netsuite nor well behaved with keys:
# Instead, shipping + billing address is joined on transaction streams
# class ShippingAddressStream(NetsuiteDynamicStream):
#     name = "shipping_address"
#     primary_keys = ["nkey"]
#     table = "TransactionShippingAddress"
#     replication_key = "lastmodifieddate"


# class BillingAddressStream(NetsuiteDynamicStream):
#     name = "billing_address"
#     primary_keys = ["nkey"]
#     table = "TransactionBillingAddress"
#     replication_key = "lastmodifieddate"


class TermStream(NetsuiteDynamicStream):
    name = "term"
    primary_keys = ["id"]
    table = "term"
    replication_key = "lastmodifieddate"


class TrialBalanceReportStream(NetSuiteStream):
    name = "trial_balance_report"
    start_date_f = None
    end_date = None
    primary_keys = ["id"]

    schema = th.PropertiesList(
        th.Property("account_type", th.StringType),
        th.Property("account_name", th.StringType),
        th.Property("account_number", th.StringType),
        th.Property("currency", th.StringType),
        th.Property("company_name", th.StringType),
        th.Property("period_name", th.StringType),
        th.Property("period_start_date", th.StringType),
        th.Property("period_end_date", th.StringType),
        th.Property("posting_period", th.StringType),
        th.Property("accumulated_amount", th.StringType),
        th.Property("credit_amount", th.StringType),
        th.Property("debit_amount", th.StringType),
    ).to_dict()

    def prepare_request_payload(self, context, next_page_token):
        return {
            "q": """
            SELECT
                Account.AcctType account_type,
                Account.displaynamewithhierarchy as account_name,
                Account.acctnumber as account_number,
                Transaction.currency as currency,
                Entity.altname as company_name,
                AccountingPeriod.PeriodName as period_name,
                AccountingPeriod.StartDate as period_start_date,
                AccountingPeriod.EndDate as period_end_date,
                Transaction.postingperiod as posting_period,
                SUM(COALESCE(TransactionAccountingLine.amount, 0)) AS accumulated_amount,
                SUM(CASE WHEN TransactionAccountingLine.amount > 0 THEN TransactionAccountingLine.amount ELSE 0 END) AS credit_amount,
                SUM(CASE WHEN TransactionAccountingLine.amount < 0 THEN TransactionAccountingLine.amount ELSE 0 END) AS debit_amount
            From
                Account
                INNER JOIN TransactionAccountingLine ON (Account.ID = TransactionAccountingLine.Account)
                INNER JOIN Transaction ON (Transaction.ID = TransactionAccountingLine.Transaction)
                INNER JOIN AccountingPeriod ON (AccountingPeriod.ID = Transaction.PostingPeriod)
                LEFT JOIN Entity ON (Transaction.entity = Entity.id)
            WHERE TransactionAccountingLine.amount != 0 AND (Transaction.Posting = 'T')
                AND (
                    Account.AcctType IN (
                        'Income',
                        'COGS',
                        'Expense',
                        'OthIncome',
                        'OthExpense'
                    )
                )
            GROUP BY
                Account.AcctType,
                Account.displaynamewithhierarchy,
                Account.acctnumber,
                Transaction.currency,
                Entity.altname,
                Transaction.postingperiod,
                AccountingPeriod.PeriodName,
                AccountingPeriod.StartDate,
                AccountingPeriod.EndDate
            ORDER BY
                AccountingPeriod.StartDate ASC
        """
        }


class PriceLevelStream(NetsuiteDynamicStream):
    name = "price_level"
    primary_keys = ["id", "lastmodifieddate"]
    table = "pricelevel"
    replication_key = "lastmodifieddate"


class LocationsStream(BulkParentStream):
    name = "locations"
    primary_keys = ["id", "lastmodifieddate"]
    table = "location"
    join = """
        LEFT JOIN locationMainAddress ma ON(location.mainaddress = ma.nkey)
        """
    # Merge group and order by
    order_by = """
    ORDER BY location.lastmodifieddate ASC
    """
    child_context_keys = ["return_address_ids", "main_address_ids"]

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("addressee", th.StringType),
        th.Property("addrtext", th.StringType),
        th.Property("country", th.StringType),
        th.Property("fullname", th.StringType),
        th.Property("includechildren", th.StringType),
        th.Property("isinactive", th.StringType),
        th.Property("lastmodifieddate", th.DateTimeType),
        th.Property("mainaddress", th.StringType),
        th.Property("name", th.StringType),
        th.Property("nkey", th.StringType),
        th.Property("override", th.StringType),
        th.Property("recordowner", th.StringType),
        th.Property("subsidiary", th.StringType),
        th.Property("externalid", th.StringType),
        th.Property("parent", th.StringType),
        th.Property("locationtype", th.StringType),
        th.Property("returnaddress", th.StringType),
    ).to_dict()

    def get_child_context(self, record, context) -> dict:
        return {
            "return_address_ids": [record["returnaddress"]]
            if record.get("returnaddress") is not None
            else [],
            "main_address_ids": [record["mainaddress"]]
            if record.get("mainaddress") is not None
            else [],
        }

    def request_records(self, context: Optional[dict]) -> Iterable[dict]:
        try:
            yield from super().request_records(context)
        except Exception as e:
            err = str(e)
            if "Record 'location' was not found" in err:
                self.logger.warning(
                    "Could not query locations: the location record type is not available in SuiteQL "
                    "for this account (Locations feature disabled, permissions, or account type). "
                    "Skipping the locations stream."
                )
                return []
            raise


class LocationReturnAddressStream(NetsuiteDynamicStream):
    name = "location_return_address"
    table = "locationreturnaddress"
    parent_stream_type = LocationsStream

    def prepare_request_payload(self, context, next_page_token):
        # fetch addresses filtering by addres id from vendor parent stream
        ids = ", ".join(f"'{id}'" for id in context["return_address_ids"]) or "NULL"
        self.custom_filter = f"nkey IN ({ids})"
        return super().prepare_request_payload(context, next_page_token)


class LocationMainAddressStream(NetsuiteDynamicStream):
    name = "location_main_address"
    table = "locationmainaddress"
    parent_stream_type = LocationsStream

    def prepare_request_payload(self, context, next_page_token):
        # fetch addresses filtering by addres id from vendor parent stream
        ids = ", ".join(f"'{id}'" for id in context["main_address_ids"]) or "NULL"
        self.custom_filter = f"nkey IN ({ids})"
        return super().prepare_request_payload(context, next_page_token)


class CostStream(NetSuiteStream):
    name = "cost"
    primary_keys = ["id", "lastmodifieddate"]
    table = "item"
    custom_filter = "itemtype='InvtPart'"
    replication_key = "lastmodifieddate"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("averagecost", th.StringType),
        th.Property("lastmodifieddate", th.DateTimeType),
    ).to_dict()


class ItemStream(BulkParentStream):
    name = "item"
    primary_keys = ["id", "lastmodifieddate"]
    table = "item"
    type_filter = False
    replication_key = "lastmodifieddate"

    default_fields = [
        th.Property("id", th.StringType),
        th.Property("lastmodifieddate", th.DateTimeType),
        th.Property("fullname", th.StringType),
        th.Property("itemid", th.StringType),
        th.Property("displayname", th.StringType),
        th.Property("itemtype", th.StringType),
        th.Property("subtype", th.StringType),
        th.Property("totalquantityonhand", th.StringType),
        th.Property("itemid", th.StringType),
        th.Property("displayname", th.StringType),
        th.Property("itemtype", th.StringType),
        th.Property("subtype", th.StringType),
        th.Property("totalquantityonhand", th.StringType),
        th.Property("subsidiary", th.StringType),
        th.Property("assetaccount", th.StringType),
        th.Property("incomeaccount", th.StringType),
        th.Property("expenseaccount", th.StringType),
        th.Property("location", th.StringType),
        th.Property("class", th.StringType),
        th.Property("department", th.StringType),
        th.Property("isinactive", th.BooleanType),
        th.Property("createddate", th.DateTimeType),
        th.Property("externalid", th.StringType),
    ]

    def get_child_context(self, record, context) -> dict:
        return {"ids": [record["id"]]}


class ClassificationStream(NetSuiteStream):
    name = "classification"
    primary_keys = ["id", "lastmodifieddate"]
    table = "classification"
    type_filter = False

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("fullname", th.StringType),
        th.Property("includechildren", th.StringType),
        th.Property("isinactive", th.StringType),
        th.Property("lastmodifieddate", th.DateTimeType),
        th.Property("name", th.StringType),
        th.Property("subsidiary", th.StringType),
        th.Property("parent", th.StringType),
        th.Property("externalid", th.StringType),
    ).to_dict()

    def request_records(self, context: Optional[dict]) -> Iterable[dict]:
        try:
            yield from super().request_records(context)
        except Exception as e:
            if "Record 'classification' was not found" in str(e):
                self.logger.warning(
                    "Could not query classification: the classification record type is not available "
                    "in SuiteQL for this account (Classes feature disabled or permissions). "
                    "Skipping the classification stream."
                )
                return
            raise


class InventoryItemLocationsStream(NetSuiteStream):
    name = "inventory_item_locations"
    primary_keys = []
    table = "inventoryitemlocations"
    replication_key = "lastquantityavailablechange"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if not self.stream_state.get("replication_key"):
            self._custom_filter = "item >= 0 AND item < 2500"
        else:
            self._custom_filter = ""

    @property
    def custom_filter(self):
        return self._custom_filter

    @custom_filter.setter
    def custom_filter(self, value):
        self._custom_filter = value


    schema = th.PropertiesList(
        th.Property("averagecostmli", th.StringType),
        th.Property("costaccountingstatus", th.StringType),
        th.Property("item", th.StringType),
        th.Property("lastpurchasepricemli", th.StringType),
        th.Property("lastquantityavailablechange", th.DateTimeType),
        th.Property("location", th.StringType),
        th.Property("onhandvaluemli", th.StringType),
        th.Property("quantityavailable", th.StringType),
        th.Property("quantitybackordered", th.StringType),
        th.Property("quantitycommitted", th.StringType),
        th.Property("quantityonhand", th.StringType),
        th.Property("lastquantityavailablechange", th.DateTimeType),
        th.Property("fixedlotmultiple", th.StringType),
        th.Property("leadtime", th.StringType),
        th.Property("minimumorderquantity", th.StringType),
        th.Property("supplylotsizingmethod", th.StringType),
    ).to_dict()


class ItemLocationConfigurationStream(NetSuiteStream):
    name = "item_location_configurations"
    primary_keys = ["id"]
    table = "itemlocationconfiguration"
    replication_key = "lastmodifieddate"

    def get_replication_key_conditions(self, context):
        start_date = self.get_starting_time(context)
        if not start_date:
            return None

        time_format = "TO_TIMESTAMP('%Y-%m-%d %H:%M:%S', 'YYYY-MM-DD HH24:MI:SS')"
        start_date_str = start_date.strftime(time_format)

        # lastmodifieddate is date-granular for item location configurations, so use >=
        # to avoid missing records changed later on the same day as the saved state.
        return [f"{self.table}.{self.replication_key}>={start_date_str}"]

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("item", th.StringType),
        th.Property("location", th.StringType),
        th.Property("lastmodifieddate", th.DateTimeType),
        th.Property("custrecord_optiply_moq", th.StringType),
        th.Property("custrecord_optiply_fixed_lot_multiple", th.StringType),
    ).to_dict()


class ProfitLossReportStream(NetSuiteStream):
    name = "profit_loss_report"
    start_date_f = None
    end_date = None
    primary_keys = ["id"]
    select = """
        Entity.altname as name, Entity.firstname, Entity.lastname, Subsidiary.fullname as subsidiary, Transaction.tranid, Transaction.externalid, Transaction.abbrevtype as TransactionType, Transaction.postingperiod, Transaction.memo, Transaction.journaltype, Account.accountsearchdisplayname as split, Account.displaynamewithhierarchy as Categories, AccountingPeriod.PeriodName, TO_CHAR (AccountingPeriod.StartDate, 'YYYY-MM-DD HH24:MI:SS') as StartDate, Account.AcctType, TO_CHAR (Transaction.TranDate, 'YYYY-MM-DD HH24:MI:SS') as Date, Account.acctnumber as Num, TransactionLine.amount, Department.name as department, CONCAT(CONCAT(Transaction.id, '_'), TransactionLine.id) as id
        """
    table = "Transaction"
    join = """
        INNER JOIN TransactionLine ON ( TransactionLine.Transaction = Transaction.ID ) LEFT JOIN department ON ( TransactionLine.department = department.ID ) INNER JOIN Account ON ( Account.ID = TransactionLine.Account ) INNER JOIN AccountingPeriod ON ( AccountingPeriod.ID = Transaction.PostingPeriod ) LEFT JOIN Entity ON ( Transaction.entity = Entity.id ) LEFT JOIN subsidiary On ( Transactionline.subsidiary = Subsidiary.id )
        """
    custom_filter = "( Transaction.TranDate BETWEEN TO_DATE( '{start_date}', 'YYYY-MM-DD' ) AND TO_DATE( '{end_date}', 'YYYY-MM-DD' ) ) AND ( Transaction.Posting = 'T' ) AND ( Account.AcctType IN ( 'Income', 'COGS', 'Expense', 'OthIncome','OthExpense' ) ) AND TransactionLine.amount !=0"
    # Merge group and order by
    order_by = """
    ORDER BY CASE WHEN Account.AcctType = 'Income' THEN 1 WHEN Account.AcctType = 'OthIncome' THEN 2 WHEN Account.AcctType = 'COGS' THEN 3  WHEN Account.AcctType = 'Expense' THEN 4 ELSE 9 END ASC, AccountingPeriod.StartDate ASC
    """
    replication_key = "date"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("accttype", th.StringType),
        th.Property("amount", th.StringType),
        th.Property("categories", th.StringType),
        th.Property("subsidiary", th.StringType),
        th.Property("date", th.DateTimeType),
        th.Property("externalid", th.StringType),
        th.Property("firstname", th.StringType),
        th.Property("lastname", th.StringType),
        th.Property("name", th.StringType),
        th.Property("num", th.StringType),
        th.Property("periodname", th.StringType),
        th.Property("postingperiod", th.StringType),
        th.Property("split", th.StringType),
        th.Property("startdate", th.DateTimeType),
        th.Property("tranid", th.StringType),
        th.Property("transactiontype", th.StringType),
        th.Property("memo", th.StringType),
        th.Property("class", th.StringType),
        th.Property("department", th.StringType),
    ).to_dict()

    def get_next_page_token(self, response, previous_token):
        """Return a token for identifying next page or None if no more pages."""
        has_next = next(extract_jsonpath("$.hasMore", response.json()))
        offset = next(extract_jsonpath("$.offset", response.json()))
        offset += self.page_size

        if has_next:
            return offset

        totalResults = next(extract_jsonpath("$.totalResults", response.json()))

        if offset >= totalResults:
            self.query_date = (parse(self.end_date) + timedelta(1)).replace(tzinfo=None)
            report_end_date = parse(self.config.get("report_end_date")).replace(tzinfo=None) if self.config.get("report_end_date") else None
            end_date = report_end_date or datetime.utcnow()
            if self.query_date < end_date:
                return self.query_date
        return None

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        if self.query_date == next_page_token:
            next_page_token = 0
        params["offset"] = int(next_page_token or 0)
        params["limit"] = self.page_size
        return params


class GeneralLedgerReportStream(ProfitLossReportStream):
    start_date_f = None
    end_date = None
    primary_keys = ["id"]
    custom_segment_field_scriptids = None
    name = "general_ledger_report"
    select = "Account.accountsearchdisplayname as split, Account.displaynamewithhierarchy as categories, Account.accttype, Account.acctnumber as num, Account.id as accountid, COALESCE(HeaderEntity.altname, LineEntity.altname) as name, COALESCE(HeaderEntity.firstname, LineEntity.firstname) as firstname, COALESCE(HeaderEntity.lastname, LineEntity.lastname) as lastname, COALESCE(HeaderEntity.id, LineEntity.id) as entityid, COALESCE(HeaderEntity.Type, LineEntity.Type) as entitytype, (Transaction.id || '_' || TransactionLine.id) AS id, Transaction.tranid, Transaction.externalid, Transaction.abbrevtype as transactiontype, TO_CHAR(Transaction.TranDate, 'YYYY-MM-DD HH24:MI:SS') as date, Transaction.transactionnumber, Transaction.trandisplayname, Transaction.memo as memo, Transaction.journaltype, TransactionLine.memo as linememo, AccountingBook.id as accountingbook, CASE WHEN TransactionAccountingLine.credit IS NOT NULL THEN 'Credit' ELSE 'Debit' END entrytype, TransactionAccountingLine.amount, TransactionAccountingLine.credit creditamount, TransactionAccountingLine.debit debitamount, department.id as departmentid, department.fullname as department, TransactionLine.location as locationid, Location.name as locationname, Subsidiary.currency currencyid, Subsidiary.fullname as subsidiary, Currency.name as currency, Currency.symbol as currencysymbol, Transaction.currency as transactioncurrencyid, TransactionAccountingLine.exchangeRate as exchangerate, TransactionLine.subsidiary as subsidiaryid, Classification.id as classid, Classification.name as class, CASE WHEN Transaction.TranDate BETWEEN AccountingPeriod.StartDate AND AccountingPeriod.EndDate THEN TO_CHAR(Transaction.TranDate, 'YYYY-MM-DD HH24:MI:SS') ELSE TO_CHAR(AccountingPeriod.StartDate, 'YYYY-MM-DD HH24:MI:SS') END AS postingDate, Transaction.postingperiod, AccountingPeriod.periodname, TO_CHAR(AccountingPeriod.StartDate, 'YYYY-MM-DD HH24:MI:SS') as startdate, TO_CHAR(AccountingPeriod.EndDate, 'YYYY-MM-DD HH24:MI:SS') as enddate, Transaction.employee as employeeid, Employee.entityid as employee"
    table = "Transaction"
    join = "INNER JOIN TransactionLine ON (TransactionLine.transaction = Transaction.id) INNER JOIN TransactionAccountingLine ON (TransactionAccountingLine.Transaction = Transaction.id AND TransactionAccountingLine.TransactionLine = TransactionLine.id) LEFT JOIN AccountingBook ON AccountingBook.id = TransactionAccountingLine.accountingBook LEFT JOIN department ON (TransactionLine.department = department.id) INNER JOIN Account ON (Account.id = TransactionAccountingLine.account) INNER JOIN AccountingPeriod ON (AccountingPeriod.id = Transaction.postingperiod) LEFT JOIN Entity AS HeaderEntity ON (Transaction.entity = HeaderEntity.id) LEFT JOIN Entity AS LineEntity ON (TransactionLine.entity = LineEntity.id) LEFT JOIN subsidiary ON (Transactionline.subsidiary = Subsidiary.id) INNER JOIN Currency ON (Currency.ID = Subsidiary.Currency) LEFT JOIN Classification ON (Transactionline.class = Classification.id) LEFT JOIN Location ON (Transactionline.location = Location.id) LEFT JOIN Employee ON (Transaction.employee = Employee.id)"
    order_by = "ORDER BY Transaction.id ASC, TransactionLine.id ASC, TransactionAccountingLine.accountingBook ASC"
    replication_key = "postingdate"


    entities_fallback = [
        {
            "name": "subsidiary",
            "select_replace": "Subsidiary.currency currencyid, Subsidiary.fullname as subsidiary, Currency.name as currency, Currency.symbol as currencysymbol,",
            "join_replace": "LEFT JOIN subsidiary ON (Transactionline.subsidiary = Subsidiary.id) INNER JOIN Currency ON (Currency.ID = Subsidiary.Currency)",
        },
        {
            "name": "department",
            "select_replace": "department.id as departmentid, department.fullname as department,",
            "join_replace": "LEFT JOIN department ON (TransactionLine.department = department.id)",
        },
        {
            "name": "classification",
            "select_replace": "Classification.id as classid, Classification.name as class,",
            "join_replace": "LEFT JOIN Classification ON (Transactionline.class = Classification.id)",
        },
        {
            "name": "location",
            "select_replace": ", Location.name as locationname",
            "join_replace": "LEFT JOIN Location ON (Transactionline.location = Location.id)",
        },
        {
            "name": "currency",
            "select_replace": ", Currency.name as currency, Currency.symbol as currencysymbol",
            "join_replace": "INNER JOIN Currency ON (Currency.ID = Subsidiary.Currency)",
        },
        {
            "name": "accountingbook",
            "select_replace": ", AccountingBook.id as accountingbook",
            "select_replace_with": ", TransactionAccountingLine.accountingBook as accountingbook",
            "join_replace": " LEFT JOIN AccountingBook ON AccountingBook.id = TransactionAccountingLine.accountingBook",
        },
        {
            "name": "employee",
            "select_replace": ", Employee.entityid as employee",
            "join_replace": "LEFT JOIN Employee ON (Transaction.employee = Employee.id)",
        }
    ]

    def gl_use_only_primary_accounting_book(self):
        return self.config.get("gl_use_only_primary_accounting_book", False)

    @property
    def custom_filter(self):
        _filter = (
            "( CASE WHEN Transaction.TranDate BETWEEN AccountingPeriod.StartDate "
            "AND AccountingPeriod.EndDate THEN Transaction.TranDate "
            "ELSE AccountingPeriod.StartDate END "
            "BETWEEN TO_DATE( '{start_date}', 'YYYY-MM-DD' ) "
            "AND TO_DATE( '{end_date}', 'YYYY-MM-DD' ) ) "
            "AND ( Transaction.Posting = 'T' ) AND TransactionAccountingLine.amount != 0"
        )


        if self.gl_use_only_primary_accounting_book():
            _filter += " AND AccountingBook.isprimary = 'T'"

        return _filter

    def get_next_page_token(self, response, previous_token):
        """Return the next page token.

        While the current date window has more pages, returns an ID cursor tuple (txn_id, line_id, book_id) to fetch the next page. 
        We use this instead of offset pagination to be able to fetch more than 100,000 records per window. 
        When the window is exhausted, advances query_date by one day and returns it as the token to trigger a new window.
        Returns None when all windows are done.
        """
        data = response.json()
        has_next = next(extract_jsonpath("$.hasMore", data))

        if has_next:
            return self._id_cursor_from_last_item(data.get("items", []))

        self.query_date = (parse(self.end_date) + timedelta(1)).replace(tzinfo=None)
        report_end_date = (
            parse(self.config.get("report_end_date")).replace(tzinfo=None)
            if self.config.get("report_end_date") else None
        )
        end_date = report_end_date or datetime.utcnow()
        if self.query_date < end_date:
            return self.query_date
        return None

    def _id_cursor_from_last_item(self, items):
        """Build an ID cursor tuple (txn_id, line_id, book_id) from the last item in a page.

        The three-part cursor matches the ORDER BY (Transaction.id, TransactionLine.id,
TransactionAccountingLine.accountingBook) and uniquely identifies a row, since.
        """
        if not items:
            self.logger.warning(f"[{self.name}] hasMore=True but response has no items; stopping pagination.")
            return None
        last = items[-1]
        txn_id, line_id = last["id"].split("_", 1)
        book_id = last.get("accountingbook")
        return (
            int(txn_id),
            int(line_id),
            int(book_id) if book_id is not None else None,
        )

    def get_url_params(self, context, next_page_token):
        """Always fetch from offset 0; pagination position is encoded in the WHERE clause."""
        return {"offset": 0, "limit": self.page_size}

    def prepare_request_payload(self, context, next_page_token):
        """Inject the ID cursor into the query WHERE clause when paginating within a window."""
        payload = super().prepare_request_payload(context, next_page_token)
        if isinstance(next_page_token, tuple):
            payload["q"] = self._inject_id_cursor(payload["q"], next_page_token)
        return payload

    def _inject_id_cursor(self, query, cursor):
        """Add a keyset pagination filter to the query using the given cursor.

            Appends a WHERE condition that skips all rows up to and including the (cursor) tuple (txn_id, line_id, book_id),
            so the next page continues exactly where the previous one ended. Matches the ORDER BY clause.
        """
        txn_id, line_id, book_id = cursor
        self.logger.info(f"[{self.name}] Paginating with ID cursor: txn={txn_id} line={line_id} book={book_id}")
        if book_id is not None:
            filter_sql = (
                f"(Transaction.id > {txn_id} "
                f"OR (Transaction.id = {txn_id} AND TransactionLine.id > {line_id}) "
                f"OR (Transaction.id = {txn_id} AND TransactionLine.id = {line_id} "
                f"AND TransactionAccountingLine.accountingBook > {book_id}))"
            )
        else:
            filter_sql = (
                f"(Transaction.id > {txn_id} "
                f"OR (Transaction.id = {txn_id} AND TransactionLine.id > {line_id}))"
            )
        order_idx = query.upper().rfind(" ORDER BY ")
        if order_idx >= 0:
            return query[:order_idx] + f" AND {filter_sql}" + query[order_idx:]
        return query + f" AND {filter_sql}"

    def _transaction_line_custom_segment_usable(
        self, session: requests.Session, scriptid: str
    ) -> bool:
        """True if SuiteQL can read this segment on TransactionLine (same shapes as the GL SELECT).

        The `customsegment` list can succeed while line-level fields still fail (role, GL impact, etc.).
        """
        q = (
            f"SELECT TOP 1 TransactionLine.{scriptid}, "
            f"BUILTIN.DF(TransactionLine.{scriptid}) "
            f"FROM Transaction "
            f"INNER JOIN TransactionLine ON (TransactionLine.transaction = Transaction.id)"
        )
        prepared_req = session.prepare_request(
            requests.Request(
                method="POST",
                url=f"{self.url_base}?limit=1",
                headers=self.http_headers,
                json={"q": q},
            )
        )
        prepared_req.headers.update({"Content-Type": "application/json"})
        probe = session.send(prepared_req, timeout=self.timeout)
        if probe.status_code == 200:
            return True
        self.logger.debug(
            f"SuiteQL probe failed for TransactionLine.{scriptid} "
            f"(status={probe.status_code}): {probe.text[:800]}"
        )
        return False

    def get_custom_segment_fields_scriptids(self):
        if self.custom_segment_field_scriptids is None:
            custom_segment_fields = []
            try:
                self.logger.info(f"Getting custom segments for stream: {self.name}")

                s = self.get_session()
                prepared_req = s.prepare_request(
                    requests.Request(
                        method="POST",
                        url=f"{self.url_base}?limit=1000",
                        headers=self.http_headers,
                        json={
                            "q": "SELECT name, scriptid FROM customsegment"
                        }
                    )
                )
                prepared_req.headers.update({"Content-Type": "application/json"})
                response = s.send(prepared_req, timeout=self.timeout)
                if response.status_code == 400:
                    body = response.text or ""
                    if "record 'customsegment' was not found" in body.lower():
                        self.logger.warning(
                            "Custom Segments are not queryable in SuiteQL for this account "
                            "(feature disabled, role permissions, or sandbox limits). "
                            "general_ledger_report will sync without custom segment columns."
                        )
                        self.custom_segment_field_scriptids = []
                        return self.custom_segment_field_scriptids
                response.raise_for_status()
                raw_fields = response.json().get("items", [])
                for cs_field in raw_fields:
                    # make it lowercase because we'll use it as db field name
                    # and the db will return it lowercase, if it's not lowercase
                    # well have problems because it won't be in the selected properties list
                    cs_field["scriptid"] = cs_field["scriptid"].lower()

                for cs_field in raw_fields:
                    scriptid = cs_field["scriptid"]
                    if self._transaction_line_custom_segment_usable(s, scriptid):
                        custom_segment_fields.append(cs_field)
                    else:
                        self.logger.warning(
                            f"Omitting custom segment {cs_field.get('name', scriptid)!r} ({scriptid}): "
                            "TransactionLine field / BUILTIN.DF not available in SuiteQL for this role."
                        )

                if custom_segment_fields:
                    self.select = self.select + ", " + ", ".join(f"'{cs_field['name']}' as custom_segment_{cs_field['scriptid']}, TransactionLine.{cs_field['scriptid']} as {cs_field['scriptid']}_value_id, BUILTIN.DF( TransactionLine.{cs_field['scriptid']} ) as {cs_field['scriptid']}_value_name" for cs_field in custom_segment_fields)
            except Exception as e:
                self.logger.error(f"Error getting custom segments for stream: {self.name}, Error: {e}")
                custom_segment_fields = []
            
            self.custom_segment_field_scriptids = [cs_field["scriptid"] for cs_field in custom_segment_fields]
        return self.custom_segment_field_scriptids

    @property
    def schema(self):
        properties_list = th.PropertiesList(
            th.Property("id", th.StringType),
            th.Property("accttype", th.StringType),
            th.Property("amount", th.NumberType),
            th.Property("categories", th.StringType),
            th.Property("subsidiary", th.StringType),
            th.Property("subsidiaryid", th.StringType),
            th.Property("date", th.DateTimeType),
            th.Property("externalid", th.StringType),
            th.Property("firstname", th.StringType),
            th.Property("lastname", th.StringType),
            th.Property("name", th.StringType),
            th.Property("num", th.StringType),
            th.Property("postingdate", th.DateTimeType),
            th.Property("periodname", th.StringType),
            th.Property("postingperiod", th.StringType),
            th.Property("split", th.StringType),
            th.Property("startdate", th.DateTimeType),
            th.Property("enddate", th.DateTimeType),
            th.Property("tranid", th.StringType),
            th.Property("transactiontype", th.StringType),
            th.Property("memo", th.StringType),
            th.Property("class", th.StringType),
            th.Property("classid", th.StringType),
            th.Property("department", th.StringType),
            th.Property("departmentid", th.StringType),
            th.Property("locationid", th.StringType),
            th.Property("locationname", th.StringType),
            th.Property("currency", th.StringType),
            th.Property("currencyid", th.StringType),
            th.Property("currencysymbol", th.StringType),
            th.Property("accountid", th.StringType),
            th.Property("transactionnumber", th.StringType),
            th.Property("trandisplayname", th.StringType),
            th.Property("entityid", th.StringType),
            th.Property("entitytype", th.StringType),
            th.Property("journaltype", th.StringType),
            th.Property("linememo", th.StringType),
            th.Property("entrytype", th.StringType),
            th.Property("creditamount", th.NumberType),
            th.Property("debitamount", th.NumberType),
            th.Property("exchangerate", th.StringType),
            th.Property("transactioncurrencyid", th.StringType),
            th.Property("accountingbook", th.StringType),
            th.Property("employee", th.StringType),
            th.Property("employeeid", th.StringType),
        )

        custom_segment_fields_scriptids = self.get_custom_segment_fields_scriptids()
        for custom_segment_field_scriptid in custom_segment_fields_scriptids:
            properties_list.append(th.Property(f"custom_segment_{custom_segment_field_scriptid}", th.StringType))
            properties_list.append(th.Property(f"{custom_segment_field_scriptid}_value_id", th.StringType))
            properties_list.append(th.Property(f"{custom_segment_field_scriptid}_value_name", th.StringType))
        
        return properties_list.to_dict()

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        if self.custom_segment_field_scriptids:
            for cs_field_scriptid in self.custom_segment_field_scriptids:
                # if the value id is not present, remove the custom segment name from the row
                if not row.get(f"{cs_field_scriptid}_value_id") and row.get(f"custom_segment_{cs_field_scriptid}"):
                    row.pop(f"custom_segment_{cs_field_scriptid}", None)

        amount_fields = ["amount", "creditamount", "debitamount"]
        for amount_field in amount_fields:
            if row.get(amount_field):
                row[amount_field] = self.process_number(amount_field, row[amount_field])
        return row


class TransactionsStream(TransactionRootStream):
    name = "transactions"
    primary_keys = ["id", "lastmodifieddate"]
    table = "transaction"
    replication_key = "lastmodifieddate"


    join = """
        LEFT JOIN TransactionShippingAddress tsa ON transaction.shippingaddress = tsa.nkey
        LEFT JOIN TransactionBillingAddress tba ON transaction.billingaddress = tba.nkey
    """
    
    default_fields = [
        th.Property("id", th.StringType),
        th.Property("type", th.StringType),
        th.Property("entity", th.StringType),
        th.Property("shippingaddress", th.StringType),
        th.Property("billingaddress", th.StringType),
        th.Property("otherrefnum", th.StringType),
        th.Property("closedate", th.DateType),
        th.Property("duedate", th.DateType),
        th.Property("createddate", th.DateTimeType),
        th.Property("foreigntotal", th.NumberType),
        th.Property("foreignamountpaid", th.NumberType),
        th.Property("foreignamountunpaid", th.NumberType),
        th.Property("currency", th.StringType),
        th.Property("exchangerate", th.NumberType),
        th.Property("status", th.StringType),
        th.Property("status_description", th.StringType),
        th.Property("approvalstatus", th.StringType),
        th.Property("approvalstatus_description", th.StringType),
        th.Property("trandate", th.DateType),
        th.Property("trandisplayname", th.StringType),
        th.Property("memo", th.StringType),
        th.Property("lastmodifieddate", th.DateTimeType),
        th.Property("tranid", th.StringType),
        th.Property("transactionnumber", th.StringType),
        th.Property("externalid", th.StringType)
    ]

    def get_selected_properties(self):
        selected_properties = super().get_selected_properties()
        ignore_queries = [
            "transaction.status_description AS status_description",
            "transaction.approvalstatus_description AS approvalstatus_description",
        ]

        for q in ignore_queries:
            if q in selected_properties:
                selected_properties.remove(q)


        selected_properties.append('BUILTIN.DF( Transaction.Status ) AS status_description')
        selected_properties.append('BUILTIN.DF( Transaction.ApprovalStatus ) AS approvalstatus_description')
        
        # Build Formatted Addresses
        selected_properties.append("COALESCE(tsa.addr1, '') || ', ' || COALESCE(tsa.addr2, '') || ', ' || COALESCE(tsa.addr3, '') || ', ' || COALESCE(tsa.city, '') || ', ' || COALESCE(tsa.state, '') || ', ' || COALESCE(tsa.zip, '') || ', ' || COALESCE(tsa.country, '') as shippingaddress")
        selected_properties.append("COALESCE(tba.addr1, '') || ', ' || COALESCE(tba.addr2, '') || ', ' || COALESCE(tba.addr3, '') || ', ' || COALESCE(tba.city, '') || ', ' || COALESCE(tba.state, '') || ', ' || COALESCE(tba.zip, '') || ', ' || COALESCE(tba.country, '') as billingaddress")
        

        return selected_properties


class TransactionLinesStream(TransactionRootStream):
    name = "transaction_lines"
    primary_keys = ["id", "transaction"]
    replication_key = "linelastmodifieddate"
    table = "transactionline"
    start_date = None
    end_date = None

    append_select = "Transaction.type as recordtype, "
    join = """INNER JOIN Transaction ON ( Transaction.ID = TransactionLine.Transaction )"""

    default_fields = [
        th.Property("id", th.StringType),
        th.Property("recordtype", th.StringType),
        th.Property("linelastmodifieddate", th.DateTimeType),
        th.Property("linesequencenumber", th.IntegerType),
        th.Property("transaction", th.StringType),
        th.Property("createdfrom", th.StringType),
        th.Property("entity", th.StringType),
        th.Property("accountinglinetype", th.StringType),
        th.Property("foreignamount", th.NumberType),
        th.Property("foreignamountpaid", th.NumberType),
        th.Property("foreignamountunpaid", th.NumberType),
        th.Property("revenueelement", th.StringType),
        th.Property("revrecstartdate", th.DateType),
        th.Property("revrecenddate", th.DateType),
        th.Property("revrecterminmonths", th.NumberType),
        th.Property("subscription", th.StringType),
        th.Property("subscriptionline", th.StringType),
        th.Property("memo", th.StringType),
        th.Property("quantity", th.NumberType),
        th.Property("quantitybilled", th.NumberType),
        th.Property("cleared", th.BooleanType),
        th.Property("closedate", th.DateTimeType),
        th.Property("commitmentfirm", th.BooleanType),
        th.Property("costestimatetype", th.StringType),
        th.Property("debitforeignamount", th.NumberType),
        th.Property("department", th.StringType),
        th.Property("donotdisplayline", th.BooleanType),
        th.Property("eliminate", th.BooleanType),
        th.Property("expenseaccount", th.StringType),
        th.Property("fxamountlinked", th.NumberType),
        th.Property("hasfulfillableitems", th.BooleanType),
        th.Property("invsoebundle", th.BooleanType),
        th.Property("isbillable", th.BooleanType),
        th.Property("isclosed", th.BooleanType),
        th.Property("iscogs", th.BooleanType),
        th.Property("iscustomglline", th.BooleanType),
        th.Property("isfullyshipped", th.BooleanType),
        th.Property("isfxvariance", th.BooleanType),
        th.Property("isinventoryaffecting", th.BooleanType),
        th.Property("isrevrectransaction", th.BooleanType),
        th.Property("deferrevrec", th.BooleanType),
        th.Property("revrecschedule", th.StringType),
        th.Property("revcommittingtransaction", th.StringType),
        th.Property("kitcomponent", th.BooleanType),
        th.Property("location", th.StringType),
        th.Property("mainline", th.BooleanType),
        th.Property("matchbilltoreceipt", th.BooleanType),
        th.Property("needsrevenueelement", th.BooleanType),
        th.Property("netamount", th.NumberType),
        th.Property("oldcommitmentfirm", th.BooleanType),
        th.Property("processedbyrevcommit", th.BooleanType),
        th.Property("quantityrejected", th.NumberType),
        th.Property("quantityshiprecv", th.NumberType),
        th.Property("subsidiary", th.StringType),
        th.Property("taxline", th.BooleanType),
        th.Property("transactiondiscount", th.BooleanType),
        th.Property("uniquekey", th.IntegerType),
        th.Property("item", th.StringType),
        th.Property("itemtype", th.StringType),
        th.Property("isallocation", th.BooleanType),
        th.Property("price", th.StringType),
        th.Property("transactionlinetype", th.StringType),
        th.Property("acknowledgefulfillinstruction", th.BooleanType),
        th.Property("actualshipdate", th.DateTimeType),
        th.Property("quantityallocated", th.NumberType),
        th.Property("quantitydemandallocated", th.NumberType),
        th.Property("allocationalert", th.StringType),
        th.Property("vsoeprice", th.NumberType),
        th.Property("vsoesopgroup", th.StringType),
        th.Property("amortizationenddate", th.DateTimeType),
        th.Property("amortizationsched", th.StringType),
        th.Property("amortizstartdate", th.DateTimeType),
        th.Property("assembly", th.StringType),
        th.Property("assemblycomponent", th.BooleanType),
        th.Property("assemblyunits", th.StringType),
        th.Property("quantitybackordered", th.NumberType),
        th.Property("isbillable", th.BooleanType),
        th.Property("billingschedule", th.StringType),
    ]

    def get_selected_properties(self):
        selected_properties = super().get_selected_properties()

        if "transactionline.recordtype AS recordtype" in selected_properties:
            selected_properties.remove("transactionline.recordtype AS recordtype")

        return selected_properties

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:
        # Avoid using my new logic if the flag is off
        if not self.config.get("transaction_lines_monthly"):
            return super().prepare_request_payload(context, next_page_token)

        filters = [
            "( Transaction.type IN ( 'RevArrng', 'CustCred', 'CustPymt', 'CustDep', 'CustRfnd', 'CustInvc', 'SalesOrd' ) )"
        ]
        # get order query
        prefix = self.table
        order_by = (
            f"ORDER BY {prefix}.{self.replication_key}, transactionline.uniquekey"
        )

        # get filter query
        start_date = self.start_date or self.get_starting_time(context)
        time_format = "TO_TIMESTAMP('%Y-%m-%d %H:%M:%S', 'YYYY-MM-DD HH24:MI:SS')"

        if start_date:
            start_date_str = start_date.strftime(time_format)

            self.start_date = start_date
            self.end_date = start_date + self.time_jump
            end_date_str = self.end_date.strftime(time_format)
            timeframe = f"{start_date_str} to {end_date_str}"

            filters.append(
                f"{prefix}.{self.replication_key}>={start_date_str} AND {prefix}.{self.replication_key}<{end_date_str}"
            )

            filters = "WHERE " + " AND ".join(filters)

        selected_properties = self.get_selected_properties()

        select = "Transaction.type as recordtype, " + ", ".join(selected_properties)

        join = self.join if self.join else ""

        payload = dict(
            q=f"SELECT {select} FROM {self.table} {join} {filters} {order_by}"
        )
        self.logger.info(f"Making query ({timeframe})")
        return payload


class TransactionAccountingLinesStream(NetSuiteStream):
    table = "TransactionAccountingLine"
    primary_keys = ["accountingbook", "transaction", "transactionline"]
    name = "transaction_accounting_lines"
    select = "*"
    replication_key = None

    schema = th.PropertiesList(
        th.Property("account", th.StringType),
        th.Property("accountingbook", th.StringType),
        th.Property("amount", th.StringType),
        th.Property("credit", th.StringType),
        th.Property("debit", th.StringType),
        th.Property("netamount", th.StringType),
        th.Property("amountlinked", th.StringType),
        th.Property("amountpaid", th.StringType),
        th.Property("amountunpaid", th.StringType),
        th.Property("overheadParentItem", th.StringType),
        th.Property("paymentamountunused", th.StringType),
        th.Property("paymentamountused", th.StringType),
        th.Property("processedbyrevcommit", th.StringType),
        th.Property("exchangerate", th.StringType),
        th.Property("posting", th.StringType),
        th.Property("transaction", th.StringType),
        th.Property("transactionline", th.StringType),
        th.Property("lastmodifieddate", th.DateTimeType),
    ).to_dict()


class CurrenciesStream(NetsuiteDynamicStream):
    name = "currencies"
    primary_keys = ["id"]
    table = "currency"
    select = None
    filter_fields = True

    def request_records(self, context: Optional[dict]) -> Iterable[dict]:
        try:
            yield from super().request_records(context)
        except Exception as e:
            if "Record \'currency\' was not found" in str(e):
                self.logger.warning("""
                Could not fetch Currencies. This feature is disabled for the current Netsuite Instance
                or the current user doesn't have permissions to fetch currencies.
                To enable this feature, please go to Setup > Company > Enable Features,
                check the "Multiple Currencies" box.
                Then give the current user permissions to fetch currencies.
                Go to Setup > Setup Manager > Users/Roles > Manage Roles > Edit the desired role.
                On the Permissions tab, go to Lists and add the "Currencies" permission.
                """)
                return []
            raise


class DepartmentsStream(NetsuiteDynamicStream):
    name = "departments"
    primary_keys = ["id"]
    table = "department"

    def request_records(self, context: Optional[dict]) -> Iterable[dict]:
        try:
            yield from super().request_records(context)
        except Exception as e:
            if "Record 'department' was not found" in str(e):
                self.logger.warning(
                    "Could not query departments: the department record type is not available "
                    "in SuiteQL for this account (Departments feature disabled or permissions). "
                    "Skipping the departments stream."
                )
                return
            raise


class SubsidiariesStream(BulkParentStream):
    name = "subsidiaries"
    primary_keys = ["id"]
    table = "subsidiary"
    filter_fields = True
    always_add_default_fields = True
    child_context_keys = [
        "return_address_ids",
        "main_address_ids",
        "shipping_address_ids",
    ]

    default_fields = [
        th.Property("id", th.StringType),
        th.Property("externalid", th.StringType),
        th.Property("name", th.StringType),
        th.Property("fullname", th.StringType),
        th.Property("returnaddress", th.StringType),
        th.Property("email", th.StringType),
        th.Property("url", th.StringType),
        th.Property("currency", th.StringType),
        th.Property("currencyname", th.StringType),
        th.Property("isinactive", th.BooleanType),
    ]

    def get_child_context(self, record, context) -> dict:
        return {
            "return_address_ids": [record["returnaddress"]]
            if record.get("returnaddress") is not None
            else [],
            "main_address_ids": [record["mainaddress"]]
            if record.get("mainaddress") is not None
            else [],
            "shipping_address_ids": [record["shippingaddress"]]
            if record.get("shippingaddress") is not None
            else [],
        }

    def _suiteql_first_row(self, q: str) -> Optional[Dict[str, Any]]:
        """Run SuiteQL and return the first row, or None on failure / empty."""
        session = self.get_session()
        prepared_req = session.prepare_request(
            requests.Request(
                method="POST",
                url=f"{self.url_base}?limit=1",
                headers=self.http_headers,
                json={"q": q},
            )
        )
        prepared_req.headers.update({"Content-Type": "application/json"})
        response = session.send(prepared_req, timeout=self.timeout)
        if response.status_code != 200:
            self.logger.debug(
                "SuiteQL probe returned %s: %s",
                response.status_code,
                (response.text or "")[:500],
            )
            return None
        items = response.json().get("items") or []
        if not items:
            return None
        return {k.lower(): v for k, v in items[0].items()}

    def _non_oneworld_subsidiary_placeholder_row(self) -> Dict[str, Any]:
        """Subsidiary list is unavailable; infer id/name from lines and currency from transactions."""
        row: Dict[str, Any] = {
            "returnaddress": None,
            "mainaddress": None,
            "shippingaddress": None,
            "isinactive": False,
        }
        q = "SELECT subsidiary AS id, BUILTIN.DF(subsidiary) AS name FROM TransactionLine WHERE subsidiary IS NOT NULL"
        line_row = self._suiteql_first_row(q)
        if line_row:
            row["id"] = str(line_row["id"])
            row["name"] = line_row["name"]
            row["fullname"] = line_row["name"]
        else:
            row["id"] = "1"
            row["name"] = "Parent Subsidiary"
            row["fullname"] = "Parent Subsidiary"

        q = "SELECT currency, BUILTIN.DF(currency) AS currencyname FROM Transaction WHERE currency IS NOT NULL"
        txn_row = self._suiteql_first_row(q)
        if txn_row:
            row["currency"] = str(txn_row["currency"])
            row["currencyname"] = txn_row["currencyname"]
        else:
            row["currency"] = "1"
            row["currencyname"] = "US Dollar"

        return row

    def request_records(self, context: Optional[dict]) -> Iterable[dict]:
        try:
            yield from super().request_records(context)
        except Exception as e:
            err = str(e)
            if "Record 'subsidiary' was not found" in err:
                self.logger.warning(
                    "Could not query subsidiaries: OneWorld is not enabled for this account, "
                    "so the subsidiary record type is not available in SuiteQL. "
                    "Emitting one inferred row from TransactionLine / Transaction."
                )
                yield self._non_oneworld_subsidiary_placeholder_row()
                return
            raise


class SubsidiaryReturnAddressStream(NetsuiteDynamicStream):
    name = "subsidiary_return_address"
    table = "subsidiaryreturnaddress"
    parent_stream_type = SubsidiariesStream

    def prepare_request_payload(self, context, next_page_token):
        # fetch addresses filtering by addres id from vendor parent stream
        ids = ", ".join(f"'{id}'" for id in context["return_address_ids"]) or "NULL"
        self.custom_filter = f"nkey IN ({ids})"
        return super().prepare_request_payload(context, next_page_token)


class SubsidiaryMainAddressStream(NetsuiteDynamicStream):
    name = "subsidiary_main_address"
    table = "subsidiarymainaddress"
    parent_stream_type = SubsidiariesStream

    def prepare_request_payload(self, context, next_page_token):
        # fetch addresses filtering by addres id from vendor parent stream
        ids = ", ".join(f"'{id}'" for id in context["main_address_ids"]) or "NULL"
        self.custom_filter = f"nkey IN ({ids})"
        return super().prepare_request_payload(context, next_page_token)


class SubsidiaryShippingAddressStream(NetsuiteDynamicStream):
    name = "subsidiary_shipping_address"
    table = "subsidiaryshippingaddress"
    parent_stream_type = SubsidiariesStream

    def prepare_request_payload(self, context, next_page_token):
        # fetch addresses filtering by addres id from vendor parent stream
        ids = ", ".join(f"'{id}'" for id in context["shipping_address_ids"]) or "NULL"
        self.custom_filter = f"nkey IN ({ids})"
        return super().prepare_request_payload(context, next_page_token)


class AccountsStream(NetsuiteDynamicStream):
    name = "accounts"
    primary_keys = ["id"]
    table = "account"
    select = None
    use_dynamic_fields = True

    default_fields = [
        th.Property("id", th.StringType),
        th.Property("parent", th.StringType),
        th.Property("accttype", th.StringType),
        th.Property("acctnumber", th.StringType),
        th.Property("class", th.StringType),
        th.Property("department", th.StringType),
        th.Property("currency", th.StringType),
        th.Property("generalrate", th.StringType),
        th.Property("fullname", th.StringType),
    ]

    def get_selected_properties(self):
        selected_properties = super().get_selected_properties()
        # add accountsearchdisplayname as fullname as default field
        selected_properties = [
            "account.accountsearchdisplayname AS fullname"
            if prop == "account.fullname AS fullname"
            else prop
            for prop in selected_properties
        ]
        return selected_properties


class ConsolidatedExchangeRates(NetsuiteDynamicStream):
    name = "consolidated_exchange_rates"
    primary_keys = ["id"]
    table = "consolidatedexchangerate"


class AccountingPeriodsStream(NetsuiteDynamicStream):
    name = "accounting_periods"
    primary_keys = ["id"]
    table = "accountingperiod"
    select = None
    filter_fields = True


class CustomersStream(BulkParentStream):
    name = "customers"
    primary_keys = ["id"]
    table = "customer"
    query_table = "customer c"
    always_add_default_fields = True
    _select = "c.*, csr.subsidiary, csr.entity"
    select_prefix = "c"
    join = "JOIN customersubsidiaryrelationship csr ON csr.entity = c.id"
    replication_key = "lastmodifieddate"
    replication_key_prefix = "c"

    default_fields = [
        th.Property("defaultbillingaddress", th.StringType),
        th.Property("parent", th.StringType),
        th.Property("subsidiary", th.StringType),
    ]

    def get_child_context(self, record, context) -> dict:
        address_keys = ["defaultbillingaddress", "defaultshippingaddress"]
        # Collect valid address IDs
        address_ids = {record.get(key) for key in address_keys if record.get(key)}
        return {"ids": list(address_ids)}


class DeletedRecordsStream(NetSuiteStream):
    name = "deleted_records"
    table = "deletedrecord"
    replication_key = "deleteddate"
    primary_keys = ["recordid", "recordtypeid", "scriptid"]

    schema = th.PropertiesList(
        th.Property("name", th.StringType),
        th.Property("recordid", th.StringType),
        th.Property("recordtypeid", th.StringType),
        th.Property("scriptid", th.StringType),
        th.Property("context", th.StringType),
        th.Property("deletedby", th.StringType),
        th.Property("deleteddate", th.DateTimeType),
        th.Property("iscustomlist", th.StringType),
        th.Property("iscustomrecord", th.StringType),
        th.Property("iscustomtransaction", th.StringType),
        th.Property("type", th.StringType),
    ).to_dict()


class RevenueElementStream(NetsuiteDynamicStream):
    name = "revenueelement"
    primary_keys = ["id"]
    table = "revenueelement"

    default_fields = [
        th.Property("id", th.StringType),
        th.Property("referenceid", th.StringType),
        th.Property("revrecstartdate", th.DateType),
        th.Property("revrecenddate", th.DateType),
        th.Property("item", th.StringType),
        th.Property("quantity", th.NumberType),
        th.Property("salesamount", th.NumberType),
        th.Property("currency", th.StringType),
        th.Property("exchangerate", th.NumberType),
    ]


class RelatedTransactionLinesStream(TransactionRootStream):
    name = "related_transaction_lines"
    table = "NextTransactionLineLink"
    start_date_f = None
    end_date = None
    primary_keys = ["compositeid"]
    replication_key = "lastmodifieddate"
    select = """
        DISTINCT
            NextTransactionLineLink.PreviousLine as lineno,
            NextTransactionLineLink.PreviousDoc AS transactionid,
            NextTransactionLineLink.NextDoc AS relatedtransactionid,
            NextTransactionLineLink.NextLine as relatedlineno,
            NextTransactionLineLink.ForeignAmount,
            NextTransactionLineLink.LastModifiedDate,
            NextTransactionLineLink.LinkType,
            NextTransactionLineLink.NextType as relatedtransactiontype,
            NextTransactionLineLink.PreviousType as transactiontype
    """

    schema = th.PropertiesList(
        th.Property("compositeid", th.StringType),
        th.Property("lineno", th.StringType),
        th.Property("transactionid", th.StringType),
        th.Property("relatedtransactionid", th.StringType),
        th.Property("relatedlineno", th.StringType),
        th.Property("foreignamount", th.StringType),
        th.Property("lastmodifieddate", th.DateTimeType),
        th.Property("linktype", th.StringType),
        th.Property("relatedtransactiontype", th.StringType),
        th.Property("transactiontype", th.StringType),
    ).to_dict()

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        row[
            "compositeid"
        ] = f"{row['transactionid']}-{row['lineno']}-{row['relatedtransactionid']}"
        return row


class SubscriptionsStream(NetsuiteDynamicStream):
    name = "subscriptions"
    primary_keys = ["id"]
    table = "subscription"

    default_fields = [
        th.Property("id", th.StringType),
        th.Property("name", th.StringType),
        th.Property("subscriptionrevision", th.IntegerType),
        th.Property("customer", th.StringType),
        th.Property("startdate", th.DateType),
        th.Property("enddate", th.DateType),
        th.Property("billingsubscriptionstatus", th.StringType),
        th.Property("frequency", th.StringType),
    ]


class SubscriptionLinesStream(NetsuiteDynamicStream):
    name = "subscription_lines"
    primary_keys = ["id"]
    table = "subscriptionline"


class SubscriptionPlansStream(NetsuiteDynamicStream):
    name = "subscription_plans"
    primary_keys = ["id"]
    table = "subscriptionplan"


class SubscriptionTermsStream(NetsuiteDynamicStream):
    name = "subscription_terms"
    primary_keys = ["id"]
    table = "subscriptionterm"


class SalesInvoicedStream(NetSuiteStream):
    name = "sales_invoiced"
    primary_keys = ["id"]
    table = "salesinvoiced"

    schema = th.PropertiesList(
        th.Property("account", th.StringType),
        th.Property("amount", th.StringType),
        th.Property("amountnet", th.StringType),
        th.Property("class", th.StringType),
        th.Property("entity", th.StringType),
        th.Property("trandate", th.StringType),
        th.Property("department", th.StringType),
        th.Property("costestimate", th.StringType),
        th.Property("estgrossprofit", th.StringType),
        th.Property("estgrossprofitpercent", th.StringType),
        th.Property("item", th.StringType),
        th.Property("location", th.StringType),
        th.Property("memo", th.StringType),
        th.Property("partner", th.StringType),
        th.Property("postingperiod", th.StringType),
        th.Property("promotioncombinations", th.StringType),
        th.Property("itemcount", th.StringType),
        th.Property("employee", th.StringType),
        th.Property("subsidiary", th.StringType),
        th.Property("transaction", th.StringType),
        th.Property("tranline", th.StringType),
        th.Property("type", th.StringType),
        th.Property("uniquekey", th.StringType),
    ).to_dict()


class SalesOrderedStream(NetSuiteStream):
    name = "sales_ordered"
    primary_keys = ["id"]
    table = "salesordered"

    schema = th.PropertiesList(
        th.Property("account", th.StringType),
        th.Property("amount", th.StringType),
        th.Property("amountnet", th.StringType),
        th.Property("class", th.StringType),
        th.Property("entity", th.StringType),
        th.Property("trandate", th.StringType),
        th.Property("department", th.StringType),
        th.Property("costestimate", th.StringType),
        th.Property("estgrossprofit", th.StringType),
        th.Property("estgrossprofitpercent", th.StringType),
        th.Property("item", th.StringType),
        th.Property("location", th.StringType),
        th.Property("memo", th.StringType),
        th.Property("partner", th.StringType),
        th.Property("postingperiod", th.StringType),
        th.Property("promotioncombinations", th.StringType),
        th.Property("itemcount", th.StringType),
        th.Property("employee", th.StringType),
        th.Property("subsidiary", th.StringType),
        th.Property("transaction", th.StringType),
        th.Property("tranline", th.StringType),
        th.Property("type", th.StringType),
        th.Property("uniquekey", th.StringType),
    ).to_dict()


class InvoiceGroupStream(NetsuiteDynamicStream):
    name = "invoice_group"
    primary_keys = ["id"]
    table = "invoicegroup"
    replication_key = "lastmodifieddate"

    schema = th.PropertiesList(
        th.Property("amountdue", th.StringType),
        th.Property("amountpaid", th.StringType),
        th.Property("billaddresslist", th.StringType),
        th.Property("currency", th.StringType),
        th.Property("customer", th.StringType),
        th.Property("customername", th.StringType),
        th.Property("trandate", th.StringType),
        th.Property("discounttotal", th.StringType),
        th.Property("duedate", th.StringType),
        th.Property("externalid", th.StringType),
        th.Property("groupedbypo", th.StringType),
        th.Property("handlingcost", th.StringType),
        th.Property("id", th.StringType),
        th.Property("invoicegroupnumber", th.StringType),
        th.Property("lastmodifieddate", th.DateTimeType),
        th.Property("memo", th.StringType),
        th.Property("lastmodifiedby", th.StringType),
        th.Property("datedriven", th.StringType),
        th.Property("dayofmonthnetdue", th.StringType),
        th.Property("daysuntilnetdue", th.StringType),
        th.Property("duenextmonthifwithindays", th.StringType),
        th.Property("ponumber", th.StringType),
        th.Property("shippingcost", th.StringType),
        th.Property("status", th.StringType),
        th.Property("subsidiary", th.StringType),
        th.Property("taxtotal", th.StringType),
        th.Property("terms", th.StringType),
        th.Property("total", th.StringType),
    ).to_dict()


class BillingSchedulesStream(NetSuiteStream):
    name = "billing_schedules"
    primary_keys = ["id"]
    table = "billingschedule"

    schema = th.PropertiesList(
        th.Property("applytosubtotal", th.StringType),
        th.Property("recurrence", th.StringType),
        th.Property("externalid", th.StringType),
        th.Property("isinactive", th.StringType),
        th.Property("inarrears", th.StringType),
        th.Property("initialamount", th.StringType),
        th.Property("initialterms", th.StringType),
        th.Property("id", th.StringType),
        th.Property("billforactuals", th.StringType),
        th.Property("milestone", th.StringType),
        th.Property("name", th.StringType),
        th.Property("job", th.StringType),
        th.Property("ispublic", th.StringType),
        th.Property("numberremaining", th.StringType),
        th.Property("frequency", th.DateTimeType),
        th.Property("recurrencepattern", th.StringType),
        th.Property("recurrenceterms", th.StringType),
        th.Property("repeatevery", th.StringType),
        th.Property("transaction", th.StringType),
        th.Property("scheduletype", th.StringType),
    ).to_dict()


class PriceBookStream(NetsuiteDynamicStream):
    name = "pricebooks"
    primary_keys = ["id"]
    table = "pricebook"


class PriceBookLineIntervalStream(NetSuiteStream):
    name = "pricebook_line_intervals"
    primary_keys = ["id"]
    table = "pricebooklineinterval"
    select = "*"

    schema = th.PropertiesList(
        th.Property("frequency", th.StringType),
        th.Property("multiplierline", th.StringType),
        th.Property("id", th.StringType),
        th.Property("startoffsetunit", th.StringType),
        th.Property("item", th.StringType),
        th.Property("linenumber", th.StringType),
        th.Property("chargetype", th.StringType),
        th.Property("pricebook", th.StringType),
        th.Property("overagefrequency", th.StringType),
        th.Property("overagepriceplan", th.StringType),
        th.Property("overagerepeatevery", th.StringType),
        th.Property("priceplan", th.StringType),
        th.Property("prorateby", th.StringType),
        th.Property("repeatevery", th.StringType),
        th.Property("isrequired", th.BooleanType),
        th.Property("startoffsetvalue", th.StringType),
    ).to_dict()


class PriceModelTypeStream(NetSuiteStream):
    name = "price_model_type"
    primary_keys = ["key"]
    table = "pricemodeltype"
    select = "*"

    schema = th.PropertiesList(
        th.Property("isinactive", th.BooleanType),
        th.Property("key", th.StringType),
        th.Property("name", th.StringType),
    ).to_dict()


class PricePlanStream(NetsuiteDynamicStream):
    name = "price_plan"
    primary_keys = ["id"]
    table = "priceplan"


class PriceTiersStream(NetSuiteStream):
    name = "price_tiers"
    primary_keys = ["id"]
    table = "pricetiers"
    select = "*"

    schema = th.PropertiesList(
        th.Property("externalid", th.StringType),
        th.Property("fromval", th.StringType),
        th.Property("id", th.StringType),
        th.Property("lineid", th.StringType),
        th.Property("maxamount", th.StringType),
        th.Property("minamount", th.StringType),
        th.Property("priceplan", th.StringType),
        th.Property("pricingoption", th.StringType),
    ).to_dict()


class SubscriptionChangeOrderStream(NetsuiteDynamicStream):
    name = "subscription_change_order"
    primary_keys = ["id"]
    table = "subscriptionchangeorder"


class ChangeOrderLineStream(NetsuiteDynamicStream):
    name = "change_order_line"
    primary_keys = ["subscriptionchangeorder", "subscriptionline"]
    table = "changeorderline"
    select = "*"

    schema = th.PropertiesList(
        th.Property("discount", th.StringType),
        th.Property("item", th.StringType),
        th.Property("newdiscount", th.StringType),
        th.Property("newpriceplan", th.StringType),
        th.Property("newquantity", th.NumberType),
        th.Property("newstatus", th.StringType),
        th.Property("subscriptionchangeorder", th.StringType),
        th.Property("priceplan", th.StringType),
        th.Property("quantity", th.NumberType),
        th.Property("status", th.StringType),
        th.Property("subscriptionline", th.StringType),
        th.Property("sequence", th.IntegerType),
    ).to_dict()


# This stream doesn't seem to have a PK nor have enough info to provide any real value. Commenting out for now
# Until I can learn more about it
# class SubscriptionChangeOrderNewLineStream(NetSuiteStream):
#     name = "subscription_change_order_new_line"
#     primary_keys = ["uuid"]
#     table = "subscriptionchangeordernewline"
#     select = "*"
#
#     schema = th.PropertiesList(
#         th.Property("uuid", th.StringType),
#         th.Property("discount", th.StringType),
#         th.Property("include", th.StringType),
#         th.Property("multiplierline", th.IntegerType),
#         th.Property("itemdisplay", th.StringType),
#         th.Property("sequence", th.StringType),
#         th.Property("subscriptionlinetype", th.StringType),
#         th.Property("subscriptionchangeorder", th.StringType),
#         th.Property("priceplan", th.StringType),
#         th.Property("quantity", th.NumberType),
#         th.Property("required", th.StringType),
#         th.Property("status", th.StringType),
#     ).to_dict()
#
#     def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
#         # NOTE: temporarily forcing a pk
#         row["uuid"] = str(uuid4())
#         return row


class SubscriptionChangeOrderRenewalStepsStream(NetSuiteStream):
    name = "subscription_change_order_renewal_steps"
    primary_keys = ["subscription", "subscriptionchangeorder"]
    table = "subscriptionchangeorderrenewalsteps"
    select = "*"

    schema = th.PropertiesList(
        th.Property("error", th.StringType),
        th.Property("subscriptionchangeorder", th.StringType),
        th.Property("step", th.StringType),
        th.Property("status", th.StringType),
        th.Property("subscription", th.StringType),
        th.Property("transaction", th.StringType),
    ).to_dict()


class SubscriptionChangeOrderStatusStream(NetSuiteStream):
    name = "subscription_change_order_status"
    primary_keys = ["key"]
    table = "subscriptionchangeorderstatus"
    select = "*"

    schema = th.PropertiesList(
        th.Property("key", th.StringType),
        th.Property("name", th.StringType),
    ).to_dict()


class SubscriptionLineRevisionStream(NetsuiteDynamicStream):
    name = "subscription_line_revision"
    primary_keys = ["subscription", "subscriptionline", "subscriptionrevision"]
    table = "subscriptionlinerevision"
    use_dynamic_fields = True

    schema = th.PropertiesList(
        th.Property("appliedtochangeorder", th.BooleanType),
        th.Property("changeordereffectivedate", th.DateType),
        th.Property("createdby", th.StringType),
        th.Property("createdfromvoid", th.BooleanType),
        th.Property("datecreated", th.DateType),
        th.Property("deltaamount", th.NumberType),
        th.Property("deltaquantity", th.NumberType),
        th.Property("externalid", th.StringType),
        th.Property("id", th.StringType),
        th.Property("overagepriceplan", th.StringType),
        th.Property("priceplan", th.StringType),
        th.Property("quantity", th.NumberType),
        th.Property("recurringamount", th.NumberType),
        th.Property("revenueelement", th.StringType),
        th.Property("subscription", th.StringType),
        th.Property("changeorder", th.StringType),
        th.Property("subscriptionline", th.StringType),
        th.Property("subscriptionrevision", th.StringType),
        th.Property("subsidiary", th.StringType),
        th.Property("totalcontractvalue", th.NumberType),
        th.Property("item_id", th.StringType),
        th.Property("item_name", th.StringType),
        th.Property("revrecstartdate", th.DateType),
        th.Property("revrecenddate", th.DateTimeType),
        th.Property("action", th.StringType),
        th.Property("subscriptionchangeorderstatus", th.StringType),
        th.Property("frequency", th.StringType),
        th.Property("repeatevery", th.IntegerType),
    ).to_dict()


class SubscriptionLineStatusStream(NetSuiteStream):
    name = "subscription_line_status"
    primary_keys = ["key"]
    table = "subscriptionlinestatus"
    select = "*"

    schema = th.PropertiesList(
        th.Property("key", th.StringType),
        th.Property("name", th.StringType),
    ).to_dict()


class SubscriptionLineTypeStream(NetSuiteStream):
    name = "subscription_line_type"
    primary_keys = ["key"]
    table = "subscriptionlinetype"
    select = "*"

    schema = th.PropertiesList(
        th.Property("key", th.StringType),
        th.Property("name", th.StringType),
    ).to_dict()


class SubscriptionPriceIntervalStream(NetSuiteStream):
    name = "subscription_price_interval"
    primary_keys = ["id"]
    table = "subscriptionpriceinterval"
    select = "*"

    schema = th.PropertiesList(
        th.Property("catalogtype", th.StringType),
        th.Property("frequency", th.StringType),
        th.Property("includedquantity", th.NumberType),
        th.Property("multiplierline", th.StringType),
        th.Property("id", th.StringType),
        th.Property("status", th.StringType),
        th.Property("item", th.StringType),
        th.Property("linenumber", th.StringType),
        th.Property("chargetype", th.StringType),
        th.Property("subscription", th.StringType),
        th.Property("overagefrequency", th.StringType),
        th.Property("overagepriceplan", th.StringType),
        th.Property("overagerepeatevery", th.StringType),
        th.Property("priceplan", th.StringType),
        th.Property("prorateby", th.StringType),
        th.Property("quantity", th.NumberType),
        th.Property("recurringamount", th.NumberType),
        th.Property("repeatevery", th.StringType),
        th.Property("startdate", th.DateType),
        th.Property("startoffsetvalue", th.StringType),
    ).to_dict()


class SalesTaxItemStream(NetsuiteDynamicStream):
    name = "sales_tax_item"
    primary_keys = []
    table = "salestaxitem"


class TaxItemGroupStream(NetSuiteStream):
    name = "tax_item_group"
    primary_keys = ["id"]
    table = "taxitemtaxgroup"
    select = "*"

    schema = th.PropertiesList(
        th.Property("description", th.StringType),
        th.Property("isinactive", th.BooleanType),
        th.Property("id", th.StringType),
        th.Property("taxtype", th.StringType),
    ).to_dict()


class TaxTypeStream(NetsuiteDynamicStream):
    name = "tax_type"
    primary_keys = ["id"]
    table = "taxtype"


class VendorCategoryStream(NetsuiteDynamicStream):
    name = "vendor_category"
    primary_keys = ["id"]
    table = "vendorcategory"


class VendorEntityAddressesStream(NetsuiteDynamicStream):
    name = "vendor_addresses"
    primary_keys = ["nkey"]
    table = "vendoraddressbookentityaddress"
    parent_stream_type = VendorStream
    custom_filter = ""

    def prepare_request_payload(self, context, next_page_token):
        # fetch addresses filtering by addres id from vendor parent stream
        ids = ", ".join(f"'{id}'" for id in context["ids"])
        self.custom_filter = f"nkey IN ({ids})"
        return super().prepare_request_payload(context, next_page_token)


class CustomerCategoryStream(NetsuiteDynamicStream):
    name = "customer_category"
    primary_keys = ["id"]
    table = "customercategory"


class CustomerEntityAddressesStream(NetsuiteDynamicStream):
    name = "customer_addresses"
    primary_keys = ["nkey"]
    table = "customeraddressbookentityaddress"
    parent_stream_type = CustomersStream
    custom_filter = ""

    def prepare_request_payload(self, context, next_page_token):
        # fetch addresses filtering by addres id from vendor parent stream
        ids = ", ".join(f"'{id}'" for id in context["ids"])
        self.custom_filter = f"nkey IN ({ids})"
        return super().prepare_request_payload(context, next_page_token)


class SalesRepStream(NetsuiteDynamicStream):
    name = "sales_rep"
    primary_keys = ["id"]
    table = "employee"
    custom_filter = "issalesrep = 'T'"


class ItemVendorStream(NetsuiteDynamicStream):
    name = "item_vendors"
    table = "itemvendor"
    parent_stream_type = ItemStream

    def prepare_request_payload(self, context, next_page_token):
        # fetch addresses filtering by addres id from vendor parent stream
        ids = ", ".join(f"'{id}'" for id in context["ids"])
        self.custom_filter = f"item IN ({ids})"
        return super().prepare_request_payload(context, next_page_token)


class ItemVendorsFullSyncStream(NetsuiteDynamicStream):
    """Daily full itemvendor extract used for supplierProduct reconciliation."""

    name = "item_vendors_full_sync"
    table = "itemvendor"

    def should_run_full_sync(self):
        force_sync_supplier_products = (self.tap_state or {}).get("force_sync_supplier_products", False)
        if isinstance(force_sync_supplier_products, str):
            force_sync_supplier_products = force_sync_supplier_products.lower() == "true"
        if force_sync_supplier_products:
            self.logger.info(
                "Running item_vendors_full_sync: force_sync_supplier_products is true in state"
            )
            return True

        hg_last_modified = (self.tap_state or {}).get("hg_last_modified")
        if not hg_last_modified:
            self.logger.info("Skipping item_vendors_full_sync: hg_last_modified missing from state")
            return False

        try:
            hg_last_modified_date = datetime.strptime(str(hg_last_modified)[:10], "%Y-%m-%d").date()
        except ValueError:
            self.logger.warning(
                "Skipping item_vendors_full_sync: could not parse hg_last_modified=%s",
                hg_last_modified,
            )
            return False

        today = datetime.utcnow().date()
        should_run = hg_last_modified_date < today
        if should_run:
            self.logger.info(
                "Running item_vendors_full_sync: hg_last_modified date %s is before today %s",
                hg_last_modified_date,
                today,
            )
        else:
            self.logger.info(
                "Skipping item_vendors_full_sync: hg_last_modified date %s is not before today %s",
                hg_last_modified_date,
                today,
            )
        return should_run

    def request_records(self, context: Optional[dict]) -> Iterable[dict]:
        if not self.should_run_full_sync():
            return
        yield from super().request_records(context)


class ItemPriceStream(NetsuiteDynamicStream):
    name = "item_prices"
    table = "itemprice"
    parent_stream_type = ItemStream

    def prepare_request_payload(self, context, next_page_token):
        # fetch addresses filtering by addres id from vendor parent stream
        ids = ", ".join(f"'{id}'" for id in context["ids"])
        self.custom_filter = f"item IN ({ids})"
        return super().prepare_request_payload(context, next_page_token)


class BillsStream(BulkParentStream):
    name = "bills"
    table = "transaction"
    custom_filter = "transaction.type = 'VendBill'"
    replication_key = "lastmodifieddate"
    join = "LEFT JOIN Entity ON (transaction.entity = Entity.id)"
    _select = "transaction.*, BUILTIN.DF(transaction.status) status"

    default_fields = [
        th.Property("externalid", th.StringType),
        th.Property("lastmodifieddate", th.DateTimeType)
    ]

    def get_child_context(self, record, context) -> dict:
        return {"ids": [record["id"]]}


    def prepare_request_payload(self, context, next_page_token):
        """Add Entity join and vendor name filter when filter_vendor_names is in config."""
        vendor_names = self.config.get("filter_vendor_names") or []
        if vendor_names:
            saved_custom_filter = self.custom_filter
            try:
                # Qualify type to avoid ambiguity with Entity
                quoted = ", ".join(f"'{name.replace(chr(39), chr(39) + chr(39))}'" for name in vendor_names)
                self.custom_filter = f"transaction.type = 'VendBill' AND Entity.altname IN ({quoted})"
                return super().prepare_request_payload(context, next_page_token)
            finally:
                self.custom_filter = saved_custom_filter
        return super().prepare_request_payload(context, next_page_token)

    def get_available_filters_metadata(self) -> Dict[str, Any]:
        return {
            "supported_operators": ["AND", "OR"],
            "supports_nesting_clauses": True,
            "filters": {
                "vendor_id": {
                    "label": "Bill Vendor ID",
                    "supported_operators": ["IN", "EQ"],
                    "target_field": "transaction.entity",
                    "options": "reference_data.vendor.id",
                },
                "vendor_name": {
                    "label": "Bill Vendor Name",
                    "supported_operators": ["IN", "EQ"],
                    "target_field": "Entity.altname",
                    "options": "reference_data.vendor.altname",
                },
                "status": {
                    "label": "Bill Status",
                    "supported_operators": ["IN", "EQ"],
                    "target_field": "BUILTIN.DF(transaction.status)",
                    "options": [
                        "Bill : Open",
                        "Bill : Pending Approval",
                        "Bill : Approved",
                        "Bill : Rejected",
                        "Bill : Paid In Full",
                    ],
                },
                "memo": {
                    "label": "Bill Memo",
                    "supported_operators": ["EQ"],
                    "target_field": "transaction.memo",
                },
            },
        }


class BillLinesStream(NetsuiteDynamicStream):
    name = "bill_lines"
    table = "transactionline"
    parent_stream_type = BillsStream
    _select = "t.recordtype, tl.*"
    select_prefix = "tl"
    query_table = "transaction t"
    join = "INNER JOIN transactionline tl on tl.transaction = t.id"
    _custom_filter = "mainline = 'F' AND (hascostline = 'T' OR accountinglinetype = 'EXPENSE')"

    default_fields = [
        th.Property("item", th.StringType),
        th.Property("quantity", th.NumberType),
        th.Property("rate", th.NumberType),
        th.Property("taxamount", th.NumberType),
    ]

    def prepare_request_payload(self, context, next_page_token):
        # fetch bill lines filtering by transaction id from bills parent stream
        ids = ", ".join(f"'{id}'" for id in context["ids"])
        self.custom_filter = f"{self._custom_filter}"
        self.custom_filter = f"{self.custom_filter} and tl.transaction IN ({ids})"
        return super().prepare_request_payload(context, next_page_token)


class BillExpensesStream(NetsuiteDynamicStream):
    name = "bill_expenses"
    table = "transactionline"
    parent_stream_type = BillsStream
    _select = "t.recordtype, tl.*"
    select_prefix = "tl"
    query_table = "transaction t"
    join = "INNER JOIN transactionline tl on tl.transaction = t.id"
    _custom_filter = "mainline = 'F' and accountinglinetype is null"

    default_fields = [
        th.Property("taxamount", th.NumberType),
    ]

    def prepare_request_payload(self, context, next_page_token):
        # fetch bill expenses filtering by transaction id from bills parent stream
        ids = ", ".join(f"'{id}'" for id in context["ids"])
        self.custom_filter = f"{self._custom_filter}"
        self.custom_filter = f"{self.custom_filter} and tl.transaction IN ({ids})"
        return super().prepare_request_payload(context, next_page_token)


class BillPaymentsStream(NetsuiteDynamicStream):
    name = "bill_payments"
    table = "transactionline"
    parent_stream_type = BillsStream
    select = "DISTINCT NTLL.previousdoc transaction, NT.ID id, NT.tranid, NT.transactionnumber, NT.externalId, NT.account account, NT.trandate, NT.type, BUILTIN.DF(NT.status) status, NT.foreigntotal amount, currency, exchangerate, NT.entity, NTL.subsidiary, NTL.location, NTL.class, NTL.department"
    query_table = "NextTransactionLineLink AS NTLL"
    join = "INNER JOIN Transaction AS NT ON (NT.id = NTLL.nextdoc) INNER JOIN TransactionLine AS NTL ON (NTL.transaction = NT.ID)"
    _custom_filter = "NT.recordtype = 'vendorpayment'"
    order_by = "ORDER BY NT.id"

    schema = th.PropertiesList(
        th.Property("account", th.StringType),
        th.Property("amount", th.StringType),
        th.Property("currency", th.StringType),
        th.Property("exchangerate", th.StringType),
        th.Property("id", th.StringType),
        th.Property("status", th.StringType),
        th.Property("trandate", th.StringType),
        th.Property("transaction", th.StringType),
        th.Property("transactionnumber", th.StringType),
        th.Property("externalid", th.StringType),
        th.Property("entity", th.StringType),
        th.Property("subsidiary", th.StringType),
        th.Property("type", th.StringType),
        th.Property("tranid", th.StringType),
        th.Property("location", th.StringType),
        th.Property("class", th.StringType),
        th.Property("department", th.StringType),
    ).to_dict()

    def prepare_request_payload(self, context, next_page_token):
        # fetch bill payments filtering by transaction id from bill parent stream
        ids = ", ".join(f"'{id}'" for id in context["ids"])
        self.custom_filter = f"{self._custom_filter}"
        self.custom_filter = f"{self.custom_filter} and NTLL.previousdoc in ({ids})"
        return super().prepare_request_payload(context, next_page_token)
    

# this schema is used for both BillAttachmentsRestletStream and BillAttachmentsSOAPStream
# they should have the same output, they only fetch data in a different way
BillAttachmentsSchema = th.PropertiesList(
        th.Property("tranid", th.StringType),
        th.Property("transaction", th.StringType),
        th.Property("file_id", th.StringType),
        th.Property("file_name", th.StringType),
        th.Property("file_type", th.StringType),
        th.Property("file_url", th.StringType),
        th.Property("downloaded_file", th.StringType)
    ).to_dict()

class BillAttachmentsRestletStream(NetsuiteDynamicStream):
    name = "bill_attachments"
    table = "suitescript_bill_attachments"
    parent_stream_type = BillsStream
    
    schema = BillAttachmentsSchema
    
    def get_url(self, context):
        restlet_url = self.config.get("bill_attachments_restlet_url") or ""
        if not restlet_url.strip():
            raise FatalAPIError(
                "bill_attachments stream is selected but 'bill_attachments_restlet_url' is missing or empty in config. "
                "Add the Restlet base URL to config to sync bill attachments."
            )
        return restlet_url
    
    def prepare_request_payload(self, context, next_page_token):
        self._current_request_id = str(uuid.uuid4())
        return {
            "requestId": self._current_request_id,
            "vendorBillIds": context["ids"],
        }

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        data = response.json()
        self._current_download_token = data.get("download_token")
        for item in data.get("items", []):
            yield item

    def get_next_page_token(self, response, previous_token):
        return None

    def _download_attachment(
        self, file_id: str, file_name: str, transaction: str
    ) -> Tuple[Optional[str], Optional[str]]:
        """Download a single attachment to disk. Returns (relative_path, None) on success, (None, error_message) on failure."""
        import requests

        suitelet_base = (self.config.get("bill_attachments_suitelet_url") or "").strip()
        if not suitelet_base:
            raise FatalAPIError(
                "bill_attachments stream is selected but 'bill_attachments_suitelet_url' is missing or empty in config. "
                "Add the Suitelet base URL to config to download bill attachment files."
            )
        dir_path = os.path.join(sync_output_folder, "bill_attachments", transaction)
        os.makedirs(dir_path, exist_ok=True)
        request_id = getattr(self, "_current_request_id", None)
        download_token = getattr(self, "_current_download_token", None)
        url = f"{suitelet_base}&fileId={file_id}&requestId={request_id}&download_token={download_token}"
        try:
            response = requests.get(url)
            response.raise_for_status()
            with open(os.path.join(dir_path, file_name), "wb") as f:
                f.write(response.content)
            return (f"bill_attachments/{transaction}/{file_name}", None)
        except Exception as e:
            return (None, str(e))

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        file_id = row.get("file_id")
        if file_id:
            rel_path, error = self._download_attachment(
                file_id, row.get("file_name"), row.get("transaction")
            )
            if error:
                file_name = row.get("file_name")
                if file_name:
                    file_desc = f"file {file_name} (file_id={file_id})"
                else:
                    file_desc = f"file with id {file_id}"
                raise FatalAPIError(
                    f"Bill attachment download failed for {file_desc}: {error}"
                )
            else:
                row["downloaded_file"] = rel_path
        return row


class BillAttachmentsSOAPStream(NetsuiteSOAPStream):
    name = "bill_attachments"
    table = "suitescript_bill_attachments"
    extract_json_path = "$['soapenv:Envelope']['soapenv:Body']['searchResponse']['platformCore:searchResult']"
    parent_stream_type = BillsStream
    
    schema = BillAttachmentsSchema
    
    
    def prepare_request_payload(self, context):
        bill_ids = context["ids"]
        return {
            "platformMsgs:search": {
                "platformMsgs:searchRecord": {
                    "@xsi:type": "tranSales:TransactionSearchAdvanced",
                    "tranSales:criteria": {
                        "tranSales:basic": {
                            "platformCommon:type": {
                                "@operator": "anyOf",
                                "platformCore:searchValue": "_vendorBill"
                            },
                            "platformCommon:mainLine": {
                                "searchValue": "true"
                            },
                            "platformCommon:internalId": {
                                "@operator": "anyOf",
                                "platformCore:searchValue": [
                                        { "@internalId": bill_id } for bill_id in bill_ids
                                ]
                            }
                        }
                    },
                    "tranSales:columns": {
                        "tranSales:basic": {
                            "platformCommon:internalId": {},
                            "platformCommon:tranId": {},
                            "platformCommon:tranDate": {}
                        },
                        "tranSales:fileJoin": {
                            "platformCommon:internalId": {},
                            "platformCommon:name": {},
                            "platformCommon:fileType": {},
                            "platformCommon:documentSize": {},
                            "platformCommon:url": {}
                        }
                    }
                }
            }
        }


    def download_attachment_file(self, transaction: str, file_id: str, file_name: str):
        response = self._tap.soap_client.get("file", file_id)

        dir_path = os.path.join(sync_output_folder, "bill_attachments", transaction)
        os.makedirs(dir_path, exist_ok=True)
        try:
            with open(os.path.join(dir_path, file_name), "wb") as f:
                if response["docFileCab:content"]:
                    file_content = base64.b64decode(response["docFileCab:content"])
                else:
                    file_content = b""
                f.write(file_content)
            return f"bill_attachments/{transaction}/{file_name}"
        except Exception as e:
            raise FatalAPIError(
                f"Bill attachment download failed for file {file_name} (file_id={file_id}): {str(e)}"
            )


    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        if not row.get("tranSales:fileJoin"):
            return None
        
        file_type = row["tranSales:fileJoin"]["platformCommon:fileType"]["platformCore:searchValue"]
        if file_type.startswith("_"):
            file_type = file_type[1:]

        record = {
            "tranid": row.get("tranSales:basic", {}).get("platformCommon:tranId", {}).get("platformCore:searchValue", ""),
            "transaction": row["tranSales:basic"]["platformCommon:internalId"]["platformCore:searchValue"]["@internalId"],
            "file_id": row["tranSales:fileJoin"]["platformCommon:internalId"]["platformCore:searchValue"]["@internalId"],
            "file_name": row["tranSales:fileJoin"]["platformCommon:name"]["platformCore:searchValue"].replace("/", "_"),
            "file_type": file_type,
            "file_url": row["tranSales:fileJoin"]["platformCommon:url"]["platformCore:searchValue"],
        }

        

        file_path = self.download_attachment_file(record["transaction"], record["file_id"], record["file_name"])
        record["downloaded_file"] = file_path

        return record


class BillTaxLinesStream(NetsuiteDynamicStream):
    name = "bill_tax_lines"
    table = "transactionline"
    parent_stream_type = BillsStream
    _select = "t.recordtype, tl.*"
    select_prefix = "tl"
    query_table = "transaction t"
    join = "INNER JOIN transactionline tl on tl.transaction = t.id"
    _custom_filter = "mainline = 'F' and taxline = 'T'"

    def prepare_request_payload(self, context, next_page_token):
        # fetch bill expenses filtering by transaction id from bills parent stream
        ids = ", ".join(f"'{id}'" for id in context["ids"])
        self.custom_filter = f"{self._custom_filter}"
        self.custom_filter = f"{self.custom_filter} and tl.transaction IN ({ids})"
        return super().prepare_request_payload(context, next_page_token)


    
class InvoicesStream(BulkParentStream):
    name = "invoices"
    table = "transaction"
    custom_filter = "type = 'CustInvc'"
    child_context_keys = ["ids", "addresses"]
    replication_key = "lastmodifieddate"
    _select = "*, BUILTIN.DF(status) status"
    address_ids = set()

    default_fields = [
        th.Property("shipdate", th.DateTimeType),
        th.Property("taxtotal", th.NumberType),
        th.Property("externalid", th.StringType),
        th.Property("lastmodifieddate", th.DateTimeType)
    ]

    def get_child_context(self, record, context) -> dict:
        # get addresses ids
        address_keys = ["billingaddress", "shippingaddress"]
        # Collect valid address IDs
        address_ids = {record.get(key) for key in address_keys if record.get(key) and record.get(key) not in self.address_ids}
        self.address_ids.update(address_ids)
        return {"ids": [record["id"]], "addresses": list(address_ids)}
    
    def _sync_children(self, child_context: dict):
        if child_context is not None and "addresses" in child_context and len(child_context["addresses"]) > 0:
            super()._sync_children(child_context)

class InvoiceLinesStream(NetsuiteDynamicStream):
    name = "invoice_lines"
    table = "transactionline"
    parent_stream_type = InvoicesStream
    _custom_filter = "mainline = 'F' and accountinglinetype = 'INCOME'"

    default_fields = [
        th.Property("item", th.StringType),
        th.Property("quantity", th.NumberType),
        th.Property("rate", th.NumberType),
        th.Property("externalid", th.StringType),
        th.Property("taxamount", th.NumberType),
    ]

    def prepare_request_payload(self, context, next_page_token):
        # fetch invoice lines filtering by transaction id
        ids = ", ".join(f"'{id}'" for id in context["ids"])
        self.custom_filter = f"{self._custom_filter}"
        self.custom_filter = f"{self.custom_filter} and transaction IN ({ids})"
        return super().prepare_request_payload(context, next_page_token)


class InvoicePaymentsStream(NetsuiteDynamicStream):
    name = "invoice_payments"
    table = "transactionline"
    parent_stream_type = InvoicesStream
    select = "DISTINCT NTLL.PreviousDoc transaction, NT.ID id, NT.transactionNumber, NT.externalId, NT.account account, NT.TranDate, NT.Type, NT.TranID, BUILTIN.DF(NT.Status) status, NT.ForeignTotal amount, currency, exchangeRate, NT.entity, NTL.subsidiary, NTL.location, NTL.class, NTL.department"
    query_table = "NextTransactionLineLink AS NTLL"
    join = "INNER JOIN Transaction AS NT ON (NT.ID = NTLL.NextDoc) INNER JOIN TransactionLine AS NTL ON (NTL.transaction = NT.ID)"
    _custom_filter = "NT.recordtype = 'customerpayment'"
    order_by = "ORDER BY NT.id"

    schema = th.PropertiesList(
        th.Property("account", th.StringType),
        th.Property("amount", th.StringType),
        th.Property("currency", th.StringType),
        th.Property("exchangerate", th.StringType),
        th.Property("id", th.StringType),
        th.Property("status", th.StringType),
        th.Property("trandate", th.StringType),
        th.Property("transaction", th.StringType),
        th.Property("transactionnumber", th.StringType),
        th.Property("externalid", th.StringType),
        th.Property("type", th.StringType),
        th.Property("entity", th.StringType),
        th.Property("subsidiary", th.StringType),
        th.Property("tranid", th.StringType),
        th.Property("location", th.StringType),
        th.Property("class", th.StringType),
        th.Property("department", th.StringType),
    ).to_dict()

    def prepare_request_payload(self, context, next_page_token):
        # fetch invoice payments filtering by transaction id from parent stream
        ids = ", ".join(f"'{id}'" for id in context["ids"])
        self.custom_filter = f"{self._custom_filter}"
        self.custom_filter = f"{self.custom_filter} and NTLL.previousdoc in ({ids})"
        return super().prepare_request_payload(context, next_page_token)

class InvoiceTaxLinesStream(NetsuiteDynamicStream):
    name = "invoice_tax_lines"
    table = "transactionline"
    parent_stream_type = InvoicesStream
    _select = "t.recordtype, tl.*"
    select_prefix = "tl"
    query_table = "transaction t"
    join = "INNER JOIN transactionline tl on tl.transaction = t.id"
    _custom_filter = "mainline = 'F' and taxline = 'T'"

    def prepare_request_payload(self, context, next_page_token):
        # fetch bill expenses filtering by transaction id from bills parent stream
        ids = ", ".join(f"'{id}'" for id in context["ids"])
        self.custom_filter = f"{self._custom_filter}"
        self.custom_filter = f"{self.custom_filter} and tl.transaction IN ({ids})"
        return super().prepare_request_payload(context, next_page_token)


class InvoiceAddressesStream(NetsuiteDynamicStream):
    name = "invoice_addresses"
    table = "transactionaddressmappingaddress"
    parent_stream_type = InvoicesStream

    def prepare_request_payload(self, context, next_page_token):
        # fetch invoice addresses filtering by addres id from invoice parent stream
        ids = ", ".join(f"'{id}'" for id in context["addresses"])
        self.custom_filter = f"nkey IN ({ids})"
        return super().prepare_request_payload(context, next_page_token)


class ItemReceiptsStream(BulkParentStream):
    name = "item_receipts"
    table = "transaction"
    custom_filter = "type = 'ItemRcpt'"
    replication_key = "lastmodifieddate"
    
    default_fields = [
        th.Property("externalid", th.StringType),
        th.Property("lastmodifieddate", th.DateTimeType)
    ]

    def get_child_context(self, record, context) -> dict:
        return {"ids": [record["id"]]}


class ItemReceiptLinesStream(NetsuiteDynamicStream):
    name = "item_receipt_lines"
    table = "transactionline"
    parent_stream_type = ItemReceiptsStream
    _select = "t.recordtype, tl.*"
    select_prefix = "tl"
    query_table = "transaction t"
    join = "INNER JOIN transactionline tl on tl.transaction = t.id"
    _custom_filter = "mainline = 'F'"

    def prepare_request_payload(self, context, next_page_token):
        # fetch bill lines filtering by transaction id from bills parent stream
        ids = ", ".join(f"'{id}'" for id in context["ids"])
        self.custom_filter = f"{self._custom_filter}"
        self.custom_filter = f"{self.custom_filter} and tl.transaction IN ({ids})"
        return super().prepare_request_payload(context, next_page_token)


class SourceDetailsStream(NetSuiteStream):
    name = "source_details"
    table = "sourceDetails"

    schema = th.PropertiesList(
        th.Property("links", th.ArrayType(th.StringType)),
        th.Property("revenueelement", th.StringType),
        th.Property("sourceid", th.StringType),
        th.Property("sourcetype", th.StringType),
    ).to_dict()


class PurchaseOrdersStream(BulkParentStream):
    name = "purchase_orders"
    table = "transaction"
    custom_filter = "type = 'PurchOrd'"
    replication_key = "lastmodifieddate"
    _select = "*, BUILTIN.DF(status) status"

    default_fields = [
        th.Property("externalid", th.StringType),
        th.Property("lastmodifieddate", th.DateTimeType)
    ]

    def get_child_context(self, record, context) -> dict:
        return {"ids": [record["id"]]}


class PurchaseOrderLinesStream(NetsuiteDynamicStream):
    name = "purchase_order_lines"
    table = "transactionline"
    parent_stream_type = PurchaseOrdersStream
    _select = "t.recordtype, tl.*"
    select_prefix = "tl"
    query_table = "transaction t"
    join = "INNER JOIN transactionline tl on tl.transaction = t.id"
    _custom_filter = "mainline = 'F'" # this filter returns the same amount of lines as the purchase order in the UI

    default_fields = [
        th.Property("item", th.StringType),
        th.Property("quantity", th.NumberType),
        th.Property("rate", th.NumberType),
    ]

    def prepare_request_payload(self, context, next_page_token):
        # fetch bill lines filtering by transaction id from bills parent stream
        ids = ", ".join(f"'{id}'" for id in context["ids"])
        self.custom_filter = f"{self._custom_filter}"
        self.custom_filter = f"{self.custom_filter} and tl.transaction IN ({ids})"
        return super().prepare_request_payload(context, next_page_token)


class SalesOrdersStream(BulkParentStream):
    name = "sales_orders"
    table = "transaction"
    custom_filter = "type = 'SalesOrd'"
    replication_key = "lastmodifieddate"
    _select = "*, BUILTIN.DF(status) status"

    default_fields = [
        th.Property("externalid", th.StringType),
        th.Property("lastmodifieddate", th.DateTimeType)
    ]

    def get_child_context(self, record, context) -> dict:
        return {"ids": [record["id"]]}


class SalesOrderLinesStream(NetsuiteDynamicStream):
    name = "sales_order_lines"
    table = "transactionline"
    parent_stream_type = SalesOrdersStream
    _select = "t.recordtype, tl.*"
    select_prefix = "tl"
    query_table = "transaction t"
    join = "INNER JOIN transactionline tl on tl.transaction = t.id"
    _custom_filter = "mainline = 'F'" # this filter returns the same amount of lines as the sales order in the UI + discount items if exists

    default_fields = [
        th.Property("item", th.StringType),
        th.Property("quantity", th.NumberType),
        th.Property("rate", th.NumberType),
    ]

    def prepare_request_payload(self, context, next_page_token):
        # fetch bill lines filtering by transaction id from bills parent stream
        ids = ", ".join(f"'{id}'" for id in context["ids"])
        self.custom_filter = f"{self._custom_filter}"
        self.custom_filter = f"{self.custom_filter} and tl.transaction IN ({ids})"
        return super().prepare_request_payload(context, next_page_token)

class kitItemMemberStream(NetsuiteDynamicStream):
    name = "kit_item_members"
    table = "kititemmember"
    parent_stream_type = ItemStream
    select = "kititemmember.*, parentitem.id as parentitemid, parentitem.itemid as parentitemname, memberitem.id as memberitemid, memberitem.itemid as itemname"
    select_prefix = "kititemmember"
    query_table = "item as parentitem"
    join = "INNER JOIN kititemmember ON ( kititemmember.parentitem = parentitem.id ) INNER JOIN item AS memberitem ON ( memberitem.ID = kititemmember.item )"
    order_by = "ORDER BY kititemmember.linenumber"

    default_fields = [
        th.Property("parentitemname", th.StringType),
        th.Property("itemname", th.StringType),
    ]
    
    def prepare_request_payload(self, context, next_page_token):
        # fetch kit item members filtering by item id from item parent stream
        ids = ", ".join(f"'{id}'" for id in context["ids"])
        self.custom_filter = f"parentitem.id IN ({ids})"
        return super().prepare_request_payload(context, next_page_token)

class EmployeesStream(NetsuiteDynamicStream):
    name = "employees"
    primary_keys = ["id"]
    table = "employee"

class ProjectsStream(NetsuiteDynamicStream):
    name = "projects"
    primary_keys = ["id"]
    table = "job"


class AccountingBooksStream(NetsuiteDynamicStream):
    name = "accounting_books"
    primary_keys = ["id"]
    table = "accountingbook"

    default_fields = [
        th.Property("id", th.StringType),
        th.Property("name", th.StringType),
        th.Property("isadjustmentonly", th.BooleanType),
        th.Property("basebook", th.StringType),
        th.Property("effectiveperiod", th.StringType),
        th.Property("isconsolidated", th.BooleanType),
        th.Property("contingentrevenuehandling", th.BooleanType),
        th.Property("twosteprevenueallocation", th.BooleanType),
        th.Property("externalid", th.StringType),
        th.Property("isprimary", th.BooleanType),
        th.Property("unbilledreceivablegrouping", th.StringType),
    ]

class CustomFieldsStream(NetsuiteDynamicStream):
    name = "custom_fields"
    primary_keys = ["id"]
    table = "customfield"

    default_fields = [
        th.Property("id", th.StringType),
        th.Property("name", th.StringType),
        th.Property("visibleontransactions", th.StringType),
        th.Property("lastmodifieddate", th.StringType),
        th.Property("scriptid", th.StringType),
        th.Property("ismandatory", th.BooleanType),
        th.Property("description", th.StringType),
        th.Property("fieldvaluetyperecord", th.StringType),
        th.Property("isshowinlist", th.BooleanType),
        th.Property("fieldvaluetype", th.StringType),
        th.Property("internalid", th.StringType),
        th.Property("fieldtype", th.StringType),
        th.Property("isstored", th.BooleanType),
        th.Property("owner", th.StringType),
        th.Property("recordtype", th.StringType),
    ]


class CustomSegmentsStream(BulkParentStream):
    child_context_size = 1
    name = "custom_segments"
    primary_keys = ["internalid"]
    table = "customsegment"
    child_context_keys = ["scriptid"]

    schema = th.PropertiesList(
        th.Property("internalid", th.StringType),
        th.Property("balancing", th.BooleanType),
        th.Property("displayorder", th.IntegerType),
        th.Property("glimpact", th.BooleanType),
        th.Property("internal", th.BooleanType),
        th.Property("isinactive", th.BooleanType),
        th.Property("name", th.StringType),
        th.Property("recordtype", th.StringType),
        th.Property("scriptid", th.StringType),
    ).to_dict()

    def request_records(self, context: Optional[dict]) -> Iterable[dict]:
        try:
            yield from super().request_records(context)
        except Exception as e:
            if "Search error occurred: Record 'customsegment' was not found" in str(e):
                self.logger.warning(
                    """
                    Could not fetch Custom Segments. This feature is disabled for the current Netsuite Instance
                    or the current user doesn't have permissions to fetch custom segments.
                    To enable this feature, please go to Setup > Company > Enable Features, on the SuiteCloud tab enable
                    the "Custom Segments" feature.
                    Then give the current user permissions to fetch custom segments.
                    Go to Setup > Setup Manager > Users/Roles > Manage Roles > Edit the desired role.
                    On the Permissions tab, go to Setup and add the "Custom Segments" permission.
                    Then go to Custom Record tab and add permissions for the desired custom segments.
                    """
                )
                return []
            else:
                raise e

    def get_child_context(self, record, context) -> dict:
        return {
            "scriptid": [record["scriptid"]]
            if record.get("scriptid") is not None
            else []
        }


class CustomSegmentValuesStream(NetsuiteDynamicStream):
    name = "custom_segment_values"
    table = "CUSTOMRECORD_{scriptid}"
    select = "*"
    parent_stream_type = CustomSegmentsStream

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("isinactive", th.BooleanType),
        th.Property("lastmodified", th.DateType),
        th.Property("lastmodifiedby", th.StringType),
        th.Property("name", th.StringType),
        th.Property("owner", th.StringType),
        th.Property("parent", th.StringType),
        th.Property("parent_scriptid", th.StringType),
        th.Property("scriptid", th.StringType),
        th.Property("recordid", th.StringType),
        th.Property("subsidiary", th.StringType),

    ).to_dict()

    def prepare_request_payload(self, context, next_page_token):
        scriptid = context["scriptid"][0]
        self.table = f"CUSTOMRECORD_{scriptid}"
        return super().prepare_request_payload(context, next_page_token)

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        scriptid = context["scriptid"][0]
        row["parent_scriptid"] = scriptid
        row["subsidiary"] = row.get(f"{scriptid.lower()}_filterby_subsidiary", "")
        row = super().post_process(row, context)
        return row

    def request_records(self, context: Optional[dict]) -> Iterable[dict]:
        try:
            yield from super().request_records(context)
        except Exception as e:
            scriptid = context["scriptid"][0].upper()
            if f"Record \'CUSTOMRECORD_{scriptid}\' was not found" in str(e):
                self.logger.warning(f"The current user doesn't have permissions to fetch custom segment values for {scriptid}: {e}")
                return []
            raise


class EntityStatusStream(NetsuiteDynamicStream):
    name = "entity_statuses"
    primary_keys = ["key"]
    table = "entitystatus"


class ContactsStream(NetsuiteDynamicStream):
    name = "contacts"
    primary_keys = ["id"]
    table = "contact"
    replication_key = "lastmodifieddate"


class EntityGroupsStream(NetsuiteDynamicStream):
    name = "entity_groups"
    primary_keys = ["id"]
    table = "entitygroup"
    replication_key = "lastmodifieddate"

    default_fields = [
        th.Property("lastmodifieddate", th.DateTimeType)
    ]


class PartnersStream(NetsuiteDynamicStream):
    name = "partners"
    primary_keys = ["id"]
    table = "partner"
    replication_key = "lastmodifieddate"


class JobTypesStream(NetsuiteDynamicStream):
    name = "job_types"
    primary_keys = ["id"]
    table = "jobtype"
