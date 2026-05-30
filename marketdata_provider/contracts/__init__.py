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
from marketdata_provider.contracts.instrument import InstrumentKey
from marketdata_provider.contracts.protocols import CandleStore, LiveKlineClient, LiveKlineClientFactory, MarketDataProvider
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
    "InstrumentKey",
    "InvalidBarError",
    "InvalidBarQueryError",
    "InvalidInstrumentError",
    "InvalidTimeframeError",
    "LiveKlineClient",
    "LiveKlineClientFactory",
    "MarketDataContractError",
    "MarketDataProvider",
    "StoreResult",
    "Timeframe",
    "parse_timeframe",
]
