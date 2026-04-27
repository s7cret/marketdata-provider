from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from marketdata_provider.core.bar import MarketBar
from marketdata_provider.providers import OfflineDataProvider
from marketdata_provider.store.candle_store import CandleStore
from marketdata_provider.store.segment_store import market_bar_checksum
from marketdata_provider.timeframes import canonical_timeframe, close_time_ms


@dataclass(frozen=True, slots=True)
class AuditIssue:
    code: str
    time: int
    message: str


@dataclass(frozen=True, slots=True)
class AuditReport:
    ok: bool
    checked: int
    issues: list[AuditIssue]


def market_bar_from_bar(bar, *, exchange: str, market: str, symbol: str, timeframe: str, source_transport: str = "rest", source_kind: str = "trade_kline") -> MarketBar:
    return MarketBar(time=bar.time, open=bar.open, high=bar.high, low=bar.low, close=bar.close, volume=bar.volume, time_close=bar.time_close or close_time_ms(bar.time, timeframe), exchange=exchange.lower(), market=market.lower(), symbol=symbol.upper(), timeframe=canonical_timeframe(timeframe), source_transport=source_transport, source_kind=source_kind, is_closed=True)


def load_repair_source(path: str | Path, *, exchange: str, market: str, symbol: str, timeframe: str, source_transport: str = "rest", source_kind: str = "trade_kline") -> list[MarketBar]:
    bars = OfflineDataProvider(path, timeframe=timeframe).get_bars(symbol, timeframe, None, None)
    return [market_bar_from_bar(b, exchange=exchange, market=market, symbol=symbol, timeframe=timeframe, source_transport=source_transport, source_kind=source_kind) for b in bars]


def _same_candle_values(a: MarketBar, b: MarketBar) -> bool:
    return (a.time, a.time_close, a.open, a.high, a.low, a.close, a.volume, a.quote_volume, a.turnover, a.trades_count) == (b.time, b.time_close, b.open, b.high, b.low, b.close, b.volume, b.quote_volume, b.turnover, b.trades_count)


def audit_against_source(store: CandleStore, source_bars: list[MarketBar], *, exchange: str, market: str, symbol: str, timeframe: str, source_kind: str = "trade_kline") -> AuditReport:
    existing = {b.time: b for b in store.get_market_bars(exchange=exchange, market=market, symbol=symbol, timeframe=timeframe, source_kind=source_kind)}
    issues: list[AuditIssue] = []
    for src in source_bars:
        cur = existing.get(src.time)
        if cur is None:
            issues.append(AuditIssue("MD_AUDIT_MISSING_BAR", src.time, "bar missing from finalized store"))
        elif not _same_candle_values(cur, src):
            issues.append(AuditIssue("MD_WS_REST_CANDLE_MISMATCH", src.time, "stored candle differs from source candle"))
    return AuditReport(ok=not issues, checked=len(source_bars), issues=issues)


def repair_from_source(store: CandleStore, source_bars: list[MarketBar], *, exchange: str, market: str, symbol: str, timeframe: str, source_kind: str = "trade_kline") -> int:
    existing = {b.time: b for b in store.get_market_bars(exchange=exchange, market=market, symbol=symbol, timeframe=timeframe, source_kind=source_kind)}
    changed = 0
    for src in source_bars:
        cur = existing.get(src.time)
        if cur is None or not _same_candle_values(cur, src):
            existing[src.time] = src
            changed += 1
    if changed:
        store.segments.replace_all(list(existing.values()), exchange=exchange, market=market, symbol=symbol, timeframe=timeframe, source_kind=source_kind)
    return changed
