from __future__ import annotations

class MarketDataError(Exception):
    code = "MD_ERROR"
    def __init__(self, message: str = "", *, details: dict | None = None):
        super().__init__(message or self.code)
        self.message = message or self.code
        self.details = details or {}

class MDSymbolAmbiguous(MarketDataError): code = "MD_SYMBOL_AMBIGUOUS"
class MDSymbolUnsupported(MarketDataError): code = "MD_SYMBOL_UNSUPPORTED"
class MDTimeframeUnsupported(MarketDataError): code = "MD_TIMEFRAME_UNSUPPORTED"
class MDInvalidExchangeResponse(MarketDataError): code = "MD_INVALID_EXCHANGE_RESPONSE"
class MDPaginationStalled(MarketDataError): code = "MD_PAGINATION_STALLED"
class MDValidationError(MarketDataError): code = "MD_VALIDATION_ERROR"
class MDIntrabarDataUnavailable(MarketDataError): code = "MD_INTRABAR_DATA_UNAVAILABLE"
class MDRuntimeIntrabarUnsupported(MarketDataError): code = "MD_RUNTIME_INTRABAR_UNSUPPORTED"
class MDNetworkUnavailable(MarketDataError): code = "MD_NETWORK_UNAVAILABLE"
class MDUnsupportedFeature(MarketDataError): code = "MD_UNSUPPORTED_FEATURE"
class MDCacheConflict(MarketDataError): code = "MD_CACHE_CONFLICT"
class MDWsRestCandleMismatch(MarketDataError): code = "MD_WS_REST_CANDLE_MISMATCH"
class MDSymbolNotTradableForStream(MarketDataError): code = "MD_SYMBOL_NOT_TRADABLE_FOR_STREAM"

# Backwards-compatible aliases for the discarded sketch's naming style.
MD_SYMBOL_NOT_FOUND = MDSymbolUnsupported
MD_PAGINATION_STALLED = MDPaginationStalled
MD_INVALID_EXCHANGE_RESPONSE = MDInvalidExchangeResponse
