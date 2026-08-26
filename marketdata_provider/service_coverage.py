from __future__ import annotations

from typing import Protocol

from marketdata_provider.contracts.query import BarQuery
from marketdata_provider.core.bar import MarketBar


class CurrentCandleStore(Protocol):
    def get_current_market_candle(
        self, *, exchange: str, market: str, symbol: str, timeframe: str
    ) -> MarketBar | None: ...


def include_current_bar(
    bars: list[MarketBar],
    query: BarQuery,
    store: CurrentCandleStore,
    *,
    enabled: bool,
) -> list[MarketBar]:
    if not enabled:
        return bars
    current = store.get_current_market_candle(
        exchange=query.instrument.exchange,
        market=query.instrument.market,
        symbol=query.instrument.symbol,
        timeframe=query.timeframe.canonical,
    )
    if current is not None and query.start_ms <= current.time < query.end_ms:
        return [*bars, current]
    return bars


def coverage_complete(bars: list[MarketBar], query: BarQuery) -> bool:
    duration = query.timeframe.duration_ms
    if duration is None:
        return bool(bars)
    present = {bar.time for bar in bars}
    return all(ts in present for ts in range(query.start_ms, query.end_ms, duration))
