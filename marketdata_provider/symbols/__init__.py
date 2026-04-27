from __future__ import annotations
from dataclasses import dataclass
from marketdata_provider.errors import MDSymbolAmbiguous, MDSymbolUnsupported

@dataclass(frozen=True, slots=True)
class NormalizedSymbol:
    exchange: str
    market: str
    base_symbol: str
    tv_symbol: str
    exchange_symbol: str
    is_perpetual: bool

_EXCHANGES = {"BINANCE", "BYBIT"}

def normalize_symbol(symbol: str, *, exchange: str | None = None, market: str | None = None) -> NormalizedSymbol:
    raw = symbol.strip().upper()
    if not raw: raise MDSymbolUnsupported("Empty symbol")
    parsed_exchange = None
    if ":" in raw:
        parsed_exchange, raw = raw.split(":", 1)
        if parsed_exchange not in _EXCHANGES:
            raise MDSymbolUnsupported(f"Unsupported exchange: {parsed_exchange}")
    ex = (exchange or parsed_exchange or "").upper()
    if not ex:
        raise MDSymbolAmbiguous("Symbol without exchange is ambiguous; pass exchange='BINANCE' or use BINANCE:BTCUSDT")
    if ex not in _EXCHANGES: raise MDSymbolUnsupported(f"Unsupported exchange: {ex}")
    is_perp = raw.endswith(".P")
    base = raw[:-2] if is_perp else raw
    if not base.endswith("USDT"):
        raise MDSymbolUnsupported(f"Stage A supports USDT symbols only, got {symbol}")
    requested_market = (market or "").lower()
    if is_perp:
        mkt = requested_market or ("usdm" if ex == "BINANCE" else "linear")
    else:
        mkt = requested_market or "spot"
    allowed = {"BINANCE": {"spot", "usdm"}, "BYBIT": {"spot", "linear"}}[ex]
    if mkt not in allowed: raise MDSymbolUnsupported(f"Unsupported {ex} market: {mkt}")
    tv = f"{ex}:{base}{'.P' if mkt in {'usdm','linear'} else ''}"
    return NormalizedSymbol(ex.lower(), mkt, base, tv, base, mkt in {"usdm", "linear"})
