"""Canonical market data contracts for the Pine stack."""

from marketdata_provider.contracts.bar import Bar
from marketdata_provider.contracts.errors import (
    CoverageValidationError,
    InvalidBarError,
    InvalidBarQueryError,
    InvalidInstrumentError,
    InvalidTimeframeError,
    MarketDataContractError,
)
from marketdata_provider.contracts.events import LiveKlineEvent
from marketdata_provider.contracts.footprint import (
    AggTrade,
    FootprintBar,
    FootprintLevel,
    FootprintQuery,
    FootprintSeries,
)
from marketdata_provider.contracts.instrument import InstrumentKey
from marketdata_provider.contracts.protocols import (
    CandleStore,
    FootprintProvider,
    LiveKlineClient,
    LiveKlineClientFactory,
    MarketDataProvider,
)
from marketdata_provider.contracts.query import BarQuery
from marketdata_provider.contracts.series import BarSeries, CoverageReport, StoreResult
from marketdata_provider.contracts.timeframe import Timeframe, parse_timeframe

__all__ = [
    "Bar",
    "BarQuery",
    "BarSeries",
    "CandleStore",
    "CoverageReport",
    "CoverageValidationError",
    "AggTrade",
    "FootprintBar",
    "FootprintLevel",
    "FootprintProvider",
    "FootprintQuery",
    "FootprintSeries",
    "InstrumentKey",
    "InvalidBarError",
    "InvalidBarQueryError",
    "InvalidInstrumentError",
    "InvalidTimeframeError",
    "LiveKlineClient",
    "LiveKlineClientFactory",
    "LiveKlineEvent",
    "MarketDataContractError",
    "MarketDataProvider",
    "StoreResult",
    "Timeframe",
    "parse_timeframe",
]
