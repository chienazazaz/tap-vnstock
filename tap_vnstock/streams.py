"""Stream type classes for tap-vnstock."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Tuple, ClassVar

from pathlib import Path

from requests import Response
from datetime import datetime
import re
from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_vnstock.client import vnstockStream

# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")
# TODO: - Override `UsersStream` and `GroupsStream` with your own stream definition.
#       - Copy-paste as many times as needed to create multiple stream types.


class InstrumentsStream(vnstockStream):
    records_jsonpath = "$[*]"
    path = "/instruments"
    name = "instruments"
    primary_keys: ClassVar[list[str]] = ["symbol"]
    schema_filepath = SCHEMAS_DIR / "instruments.json"
    replication_method = "FULL_TABLE"

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict | None:
        """Return a context dictionary for child streams."""
        if record.get("type") == "stock":
            return {"symbol": record["symbol"]}
        else:
            return


class QuotesStream(vnstockStream):
    """Quotes stream"""

    parent_stream_type = InstrumentsStream

    records_jsonpath = "$[*]"

    name = "quotes"

    path = "symbols/{symbol}/historical-quotes"

    primary_keys: ClassVar[list[str]] = ["symbol", "date"]

    replication_key = "date"

    replication_method = "FULL_TABLE"

    schema_filepath = SCHEMAS_DIR / "quotes.json"

    def get_url_params(self, context: Optional[dict], *args) -> Dict[str, Any]:
        params = super().get_url_params(context)
        params["limit"] = 100

        return params


class EventsStream(vnstockStream):
    """Events stream"""

    parent_stream_type = InstrumentsStream

    records_jsonpath = "$[*]"

    name = "events"

    path = "/symbols/{symbol}/timescale-marks"

    primary_keys: ClassVar[list[str]] = ["symbol", "date"]

    replication_key = "date"

    replication_method = "FULL_TABLE"

    schema_filepath = SCHEMAS_DIR / "events.json"

    def get_url_params(self, context: Optional[dict], *args) -> Dict[str, Any]:
        params = super().get_url_params(context)
        params["limit"] = 100

        return params

    def parse_response(self, response: Response) -> Iterable[dict]:
        resp_json = response.json()
        symbol_search = re.search('symbols/(\w{3})/timescale-marks', response.url, re.IGNORECASE)
        assert symbol_search is not None
        symbol = symbol_search.group(1)
        for row in resp_json:
            row.update({"symbol":symbol})
            
            yield row


# class FundamentalStream(vnstockStream):
#     """Fundamental stream"""

#     parent_stream_type = InstrumentsStream

#     records_jsonpath = "$"

#     name = "fundamental"

#     path = "/symbols/{symbol}/fundamental"

#     primary_keys: ClassVar[list[str]] = ["symbol"]

#     replication_method = "FULL_TABLE"

#     schema_filepath = SCHEMAS_DIR / "fundamental.json"

#     def get_url_params(self, context: Optional[dict], *args) -> Dict[str, Any]:
#         params = super().get_url_params(context)
#         return params

#     def parse_response(self, response: Response) -> Iterable[dict]:
#         resp_json = response.json()
       
#         print(resp_json,1234)
#         return resp_json


class IndirectCashflowStream(vnstockStream):
    """Indirect Cashflow stream"""

    parent_stream_type = InstrumentsStream

    records_jsonpath = "$[*]"

    name = "indirect_cashflow"

    path = "/symbols/{symbol}/full-financial-reports"

    primary_keys: ClassVar[list[str]] = ["symbol"]

    replication_method = "FULL_TABLE"

    schema_filepath = SCHEMAS_DIR / "financial_reports.json"

    def get_url_params(self, context: Optional[dict], *args) -> Dict[str, Any]:
        params = super().get_url_params(context)
        params["limit"] = 1
        params["year"] = datetime.today().year
        params["quarter"] = datetime.today().month // 3 + 1
        params["type"] = 4

        return params

    def parse_response(self, response: Response) -> Iterable[dict]:
        resp_json = response.json()
        symbol_search = re.search('symbols/(\w{3})/full-financial-reports', response.url, re.IGNORECASE)
        assert symbol_search is not None
        symbol = symbol_search.group(1)
        if resp_json:
            for row in resp_json:
                row.update({"symbol":symbol})
                
                yield row


class DirectCashflowStream(vnstockStream):
    """Direct Cashflow stream"""

    parent_stream_type = InstrumentsStream

    records_jsonpath = "$[*]"

    name = "direct_cashflow"

    path = "/symbols/{symbol}/full-financial-reports"

    primary_keys: ClassVar[list[str]] = ["symbol"]

    replication_method = "FULL_TABLE"

    schema_filepath = SCHEMAS_DIR / "financial_reports.json"

    def get_url_params(self, context: Optional[dict], *args) -> Dict[str, Any]:
        params = super().get_url_params(context)
        params["limit"] = 1
        params["year"] = datetime.today().year
        params["quarter"] = datetime.today().month // 3 + 1
        params["type"] = 3

        return params

    def parse_response(self, response: Response) -> Iterable[dict]:
        resp_json = response.json()
        symbol_search = re.search('symbols/(\w{3})/full-financial-reports', response.url, re.IGNORECASE)
        assert symbol_search is not None
        symbol = symbol_search.group(1)
        if resp_json:
            for row in resp_json:
                row.update({"symbol":symbol})
                
                yield row


class BalanceStream(vnstockStream):
    """Balance stream"""

    parent_stream_type = InstrumentsStream

    records_jsonpath = "$[*]"

    name = "balance"

    path = "/symbols/{symbol}/full-financial-reports"

    primary_keys: ClassVar[list[str]] = ["symbol"]

    replication_method = "FULL_TABLE"

    schema_filepath = SCHEMAS_DIR / "financial_reports.json"

    def get_url_params(self, context: Optional[dict], *args) -> Dict[str, Any]:
        params = super().get_url_params(context)
        params["limit"] = 1
        params["year"] = datetime.today().year
        params["quarter"] = datetime.today().month // 3 + 1
        params["type"] = 1

        return params

    def parse_response(self, response: Response) -> Iterable[dict]:
        resp_json = response.json()
        symbol_search = re.search('symbols/(\w{3})/full-financial-reports', response.url, re.IGNORECASE)
        assert symbol_search is not None
        symbol = symbol_search.group(1)
        if resp_json:
            for row in resp_json:
                row.update({"symbol":symbol})
                
                yield row


class IncomeStatementStream(vnstockStream):
    """Income Statement stream"""

    parent_stream_type = InstrumentsStream

    records_jsonpath = "$[*]"

    name = "income_statement"

    path = "/symbols/{symbol}/full-financial-reports"

    primary_keys: ClassVar[list[str]] = ["symbol"]

    replication_method = "FULL_TABLE"

    schema_filepath = SCHEMAS_DIR / "financial_reports.json"

    def get_url_params(self, context: Optional[dict], *args) -> Dict[str, Any]:
        params = super().get_url_params(context)
        params["limit"] = 1
        params["year"] = datetime.today().year
        params["quarter"] = datetime.today().month // 3 + 1
        params["type"] = 2

        return params

    def parse_response(self, response: Response) -> Iterable[dict]:
        resp_json = response.json()
        symbol_search = re.search('symbols/(\w{3})/full-financial-reports', response.url, re.IGNORECASE)
        assert symbol_search is not None
        symbol = symbol_search.group(1)
        if resp_json:
            for row in resp_json:
                row.update({"symbol":symbol})
                
                yield row


class IndicatorsStream(vnstockStream):
    """Indicators stream"""

    parent_stream_type = InstrumentsStream

    records_jsonpath = "$[*]"

    name = "indicators"

    path = "/symbols/{symbol}/financial-indicators"

    primary_keys: ClassVar[list[str]] = ["symbol"]

    replication_method = "FULL_TABLE"

    schema_filepath = SCHEMAS_DIR / "indicators.json"

    def get_url_params(self, context: Optional[dict], *args) -> Dict[str, Any]:
        params = super().get_url_params(context)
        return params

    def parse_response(self, response: Response) -> Iterable[dict]:
        resp_json = response.json()
        symbol_search = re.search("symbols/(\w{3})/financial-indicators", response.url, re.IGNORECASE)
        assert symbol_search is not None
        symbol = symbol_search.group(1)
        for row in resp_json:
            row.update({"symbol":symbol})
            
            yield row
