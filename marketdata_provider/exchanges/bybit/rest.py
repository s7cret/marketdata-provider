from __future__ import annotations
from typing import Any, Sequence, cast
from marketdata_provider.config import BybitConfig
from marketdata_provider.core.bar import MarketBar
from marketdata_provider.errors import MDInvalidExchangeResponse
from marketdata_provider.timeframes import close_time_ms, to_bybit_interval
from marketdata_provider.validation import exclude_open_candle, validate_bars

BYBIT_ENDPOINT = "/v5/market/kline"

def _extract_rows(payload: Any) -> Sequence[Sequence[Any]]:
    if isinstance(payload, dict):
        try: return payload["result"]["list"]
        except Exception as e: raise MDInvalidExchangeResponse("Bybit payload missing result.list") from e
    return payload

def normalize_bybit_klines(payload: Any, *, symbol: str, market: str, timeframe: str, server_time_ms: int | None = None, include_open_candle: bool = False) -> list[MarketBar]:
    rows = _extract_rows(payload)
    bars: list[MarketBar] = []
    for r in rows:
        if len(r) < 6: raise MDInvalidExchangeResponse("Bybit kline row too short")
        open_time = int(r[0])
        bars.append(MarketBar(
            time=open_time, open=float(r[1]), high=float(r[2]), low=float(r[3]), close=float(r[4]), volume=float(r[5]), time_close=close_time_ms(open_time, timeframe),
            exchange="bybit", market=market, symbol=symbol.upper(), timeframe=timeframe, source="fixture", is_closed=True,
            quote_volume=float(r[6]) if len(r) > 6 and r[6] not in (None, "") else None,
        ))
    bars = sorted(bars, key=lambda b: b.time)  # Bybit V5 often returns newest first.
    if not include_open_candle: bars = cast(list[MarketBar], exclude_open_candle(bars, server_time_ms=server_time_ms))
    validate_bars(bars)
    return bars

class OfflineBybitRestAdapter:
    def __init__(self, payload: Any, *, config: BybitConfig | None = None, server_time_ms: int | None = None):
        self.payload = payload; self.config = config or BybitConfig(); self.server_time_ms = server_time_ms
    def get_klines(self, *, symbol: str, market: str, interval: str, start: int | None, end: int | None, limit: int | None = None, include_open_candle: bool = False) -> list[MarketBar]:
        tf = interval
        _ = to_bybit_interval(interval)
        bars = normalize_bybit_klines(self.payload, symbol=symbol, market=market, timeframe=tf, server_time_ms=self.server_time_ms, include_open_candle=include_open_candle)
        if limit: bars = bars[:limit]
        return [b for b in bars if (start is None or b.time >= start) and (end is None or b.time < end)]
