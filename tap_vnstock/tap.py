"""vnstock tap class."""

from __future__ import annotations

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers

from typing import List

from tap_vnstock.streams import (
    BalanceStream,
    DirectCashflowStream,
    EventsStream,
    # FundamentalStream,
    IncomeStatementStream,
    IndicatorsStream,
    IndirectCashflowStream,
    InstrumentsStream,
    QuotesStream,
)

STREAM_TYPES = [
    InstrumentsStream,
    QuotesStream,
    # FundamentalStream,
    IncomeStatementStream,
    IndirectCashflowStream,
    DirectCashflowStream,
    BalanceStream,
    EventsStream,
    IndicatorsStream,
]


class Tapvnstock(Tap):
    """vnstock tap class."""

    name = "tap-vnstock"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "access_token",
            th.StringType,
            required=True,
        ),

    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""

        return [stream_class(tap=self) for stream_class in STREAM_TYPES]


if __name__ == "__main__":
    Tapvnstock.cli()
