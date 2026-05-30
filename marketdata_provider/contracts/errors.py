from __future__ import annotations


class MarketDataContractError(ValueError):
    """Base error for invalid market data contracts."""


class InvalidInstrumentError(MarketDataContractError):
    """Raised when an instrument key is malformed."""


class InvalidTimeframeError(MarketDataContractError):
    """Raised when a timeframe cannot be represented canonically."""


class InvalidBarError(MarketDataContractError):
    """Raised when an OHLCV bar violates contract invariants."""


class InvalidBarQueryError(MarketDataContractError):
    """Raised when a query violates window or policy invariants."""


class CoverageValidationError(MarketDataContractError):
    """Raised when delivered coverage is invalid for its query."""
