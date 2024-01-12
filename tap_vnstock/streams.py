"""Stream type classes for tap-vnstock."""

from __future__ import annotations
from itertools import count

from typing import Any, Dict, Iterable, List, Optional, Tuple, ClassVar

from pathlib import Path

from requests import Response
from datetime import date, datetime, timedelta
import re
from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_vnstock.client import vnstockStream
from singer_sdk.pagination import BaseHATEOASPaginator

from urllib.parse import urlparse, parse_qs, parse_qsl, urlencode

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class FireAntPaginator(BaseHATEOASPaginator):
    def get_next_url(self, response):
        url = urlparse(response.request.url)
        params = parse_qs(url.query, encoding="utf-8")  # type: ignore
        # startDate = params.get('startDate')[0]
        endDate = datetime.strptime(params.get("endDate")[0], "%Y-%m-%d %H:%M:%S.%f")  # type: ignore

        if endDate < datetime.today():
            startDate = (endDate + timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S.%f")
            endDate = (endDate + timedelta(days=90)).strftime("%Y-%m-%d %H:%M:%S.%f")
            return urlencode({"startDate": startDate, "endDate": endDate})


class InstrumentsStream(vnstockStream):
    records_jsonpath = "$[*]"
    path = "/instruments"
    name = "instruments"
    primary_keys: ClassVar[list[str]] = ["symbol"]
    schema_filepath = SCHEMAS_DIR / "instruments.json"  # type: ignore
    replication_method = "FULL_TABLE"  # type: ignore

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict | None:
        """Return a context dictionary for child streams."""
        if (record.get("type") == "stock") & (len(record.get("symbol")) == 3):  # type: ignore
            return {"symbol": record["symbol"]}
        # else:
        #     return {"symbol": None}


class QuotesStream(vnstockStream):
    """Quotes stream"""

    parent_stream_type = InstrumentsStream

    records_jsonpath = "$[*]"

    name = "quotes"

    path = "symbols/{symbol}/historical-quotes"

    primary_keys: ClassVar[list[str]] = ["symbol", "date"]

    replication_key = "date"  # type: ignore

    replication_method = "INCREMENTAL"  # type: ignore

    schema_filepath = SCHEMAS_DIR / "quotes.json"  # type: ignore

    def get_new_paginator(self):
        """Create a new pagination helper instance.

        If the source API can make use of the `next_page_token_jsonpath`
        attribute, or it contains a `X-Next-Page` header in the response
        then you can remove this method.

        If you need custom pagination that uses page numbers, "next" links, or
        other approaches, please read the guide: https://sdk.meltano.com/en/v0.25.0/guides/pagination-classes.html.

        Returns:
            A pagination helper instance.
        """

        return FireAntPaginator()
        # return

    def get_url_params(
        self, context: Optional[dict], next_page_token: dict
    ) -> Dict[str, Any]:
        params = super().get_url_params(context)
        params["limit"] = 100

        if next_page_token:
            params.update(parse_qsl(next_page_token.path))  # type: ignore

        return params


class EventsStream(vnstockStream):
    """Events stream"""

    parent_stream_type = InstrumentsStream

    records_jsonpath = "$[*]"

    name = "events"

    path = "/symbols/{symbol}/timescale-marks"

    primary_keys: ClassVar[list[str]] = ["symbol", "date"]

    replication_key = "date"  # type: ignore

    replication_method = "INCREMENTAL"  # type: ignore

    schema_filepath = SCHEMAS_DIR / "events.json"  # type: ignore

    def get_url_params(self, context: Optional[dict], next_page_token: dict) -> Dict[str, Any]:
        params = super().get_url_params(context)
        params["limit"] = 100

        if next_page_token:
            params.update(parse_qsl(next_page_token.path))  # t

        return params

    def get_new_paginator(self):
        """Create a new pagination helper instance.

        If the source API can make use of the `next_page_token_jsonpath`
        attribute, or it contains a `X-Next-Page` header in the response
        then you can remove this method.

        If you need custom pagination that uses page numbers, "next" links, or
        other approaches, please read the guide: https://sdk.meltano.com/en/v0.25.0/guides/pagination-classes.html.

        Returns:
            A pagination helper instance.
        """

        return FireAntPaginator()

    def parse_response(self, response: Response) -> Iterable[dict]:
        resp_json = response.json()
        if resp_json:
            symbol_search = re.search(
                "symbols\/(\w+)\/timescale-marks", response.url, re.IGNORECASE
            )
            assert symbol_search is not None
            symbol = symbol_search.group(1)
            for row in resp_json:
                row.update({"symbol": symbol})
                yield row


class IndirectCashflowStream(vnstockStream):
    """Indirect Cashflow stream"""

    parent_stream_type = InstrumentsStream

    records_jsonpath = "$[*]"

    name = "indirect_cashflow"

    path = "/symbols/{symbol}/full-financial-reports"

    primary_keys: ClassVar[list[str]] = ["symbol",'id']

    replication_method = "FULL_TABLE"  # type: ignore

    schema_filepath = SCHEMAS_DIR / "financial_reports.json"  # type: ignore

    def get_url_params(self, context: Optional[dict], *args) -> Dict[str, Any]:
        params = super().get_url_params(context)
        params["limit"] = 1
        params["year"] = datetime.today().year
        params["quarter"] = datetime.today().month // 3 + 1
        params["type"] = 4

        return params

    def parse_response(self, response: Response) -> Iterable[dict]:
        resp_json = response.json()
        if resp_json:
            symbol_search = re.search(
                "symbols\/(\w+)\/full-financial-reports", response.url, re.IGNORECASE
            )
            assert symbol_search is not None
            symbol = symbol_search.group(1)
            for row in resp_json:
                row.update({"symbol": symbol})

                yield row


class DirectCashflowStream(vnstockStream):
    """Direct Cashflow stream"""

    parent_stream_type = InstrumentsStream

    records_jsonpath = "$[*]"

    name = "direct_cashflow"

    path = "/symbols/{symbol}/full-financial-reports"

    primary_keys: ClassVar[list[str]] = ["symbol",'id']

    replication_method = "FULL_TABLE"  # type: ignore

    schema_filepath = SCHEMAS_DIR / "financial_reports.json"  # type: ignore

    def get_url_params(self, context: Optional[dict], *args) -> Dict[str, Any]:
        params = super().get_url_params(context)
        params["limit"] = 4
        params["year"] = datetime.today().year
        params["quarter"] = (datetime.today().month // 3) + 1
        params["type"] = 3

        return params

    def parse_response(self, response: Response) -> Iterable[dict]:
        resp_json = response.json()
        if resp_json:
            symbol_search = re.search(
                "symbols\/(\w+)\/full-financial-reports", response.url, re.IGNORECASE
            )
            assert symbol_search is not None
            symbol = symbol_search.group(1)
            for row in resp_json:
                row.update({"symbol": symbol})

                yield row


class BalanceStream(vnstockStream):
    """Balance stream"""

    parent_stream_type = InstrumentsStream

    records_jsonpath = "$[*]"

    name = "balance"

    path = "/symbols/{symbol}/full-financial-reports"

    primary_keys: ClassVar[list[str]] = ["symbol",'id']

    replication_method = "FULL_TABLE"  # type: ignore

    schema_filepath = SCHEMAS_DIR / "financial_reports.json"  # type: ignore

    def get_url_params(self, context: Optional[dict], *args) -> Dict[str, Any]:
        params = super().get_url_params(context)
        params["limit"] = 1
        params["year"] = datetime.today().year
        params["quarter"] = datetime.today().month // 3 + 1
        params["type"] = 1

        return params

    def parse_response(self, response: Response) -> Iterable[dict]:
        resp_json = response.json()
        if resp_json:
            symbol_search = re.search(
                "symbols\/(\w+)\/full-financial-reports", response.url, re.IGNORECASE
            )
            assert symbol_search is not None
            symbol = symbol_search.group(1)
            for row in resp_json:
                row.update({"symbol": symbol})

                yield row


class IncomeStatementStream(vnstockStream):
    """Income Statement stream"""

    parent_stream_type = InstrumentsStream

    records_jsonpath = "$[*]"

    name = "income_statement"

    path = "/symbols/{symbol}/full-financial-reports"

    primary_keys: ClassVar[list[str]] = ["symbol",'id']

    replication_method = "FULL_TABLE"  # type: ignore

    schema_filepath = SCHEMAS_DIR / "financial_reports.json"  # type: ignore

    def get_url_params(self, context: Optional[dict], *args) -> Dict[str, Any]:
        params = super().get_url_params(context)
        params["limit"] = 1
        params["year"] = datetime.today().year
        params["quarter"] = datetime.today().month // 3 + 1
        params["type"] = 2

        return params

    def parse_response(self, response: Response) -> Iterable[dict]:
        resp_json = response.json()
        if resp_json:
            symbol_search = re.search(
                "symbols\/(\w+)\/full-financial-reports", response.url, re.IGNORECASE
            )
            assert symbol_search is not None
            symbol = symbol_search.group(1)
            for row in resp_json:
                row.update({"symbol": symbol})

                yield row


class IndicatorsStream(vnstockStream):
    """Indicators stream"""

    parent_stream_type = InstrumentsStream

    records_jsonpath = "$[*]"

    name = "indicators"

    path = "symbols/{symbol}/financial-indicators"

    primary_keys: ClassVar[list[str]] = ["symbol", "shortName"]

    replication_method = "FULL_TABLE"  # type: ignore

    schema_filepath = SCHEMAS_DIR / "indicators.json"  # type: ignore

    def get_url_params(self, context: Optional[dict], *args) -> Dict[str, Any]:
        params = super().get_url_params(context)
        return params

    def parse_response(self, response: Response) -> Iterable[dict]:
        resp_json = response.json()
        symbol_search = re.search(
            "symbols\/(\w+)\/financial-indicators", response.url, re.IGNORECASE
        )
        if resp_json:
            assert symbol_search is not None
            symbol = symbol_search.group(1)
            for row in resp_json:
                row.update({"symbol": symbol})
                yield row
