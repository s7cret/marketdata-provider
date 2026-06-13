from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from marketdata_provider.core.bar import MarketBar
from marketdata_provider.timeframes import (
    canonical_timeframe,
    close_time_ms,
    to_bybit_interval,
)


@dataclass(frozen=True, slots=True)
class KlineUpdate:
    exchange: str
    market: str
    symbol: str
    timeframe: str
    event_time: int
    open_time: int
    close_time: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    source_kind: Literal["trade_kline", "mark_kline", "index_kline"] = "trade_kline"
    quote_volume: float | None = None
    turnover: float | None = None
    trades_count: int | None = None
    taker_buy_base_volume: float | None = None
    taker_buy_quote_volume: float | None = None
    is_closed: bool = False
    raw_source: Literal["rest", "ws"] = "ws"
    raw_event_id: str | None = None
    received_at: int | None = None

    def to_market_bar(self, *, downloaded_at: int | None = None) -> MarketBar:
        return MarketBar(
            time=self.open_time,
            open=self.open,
            high=self.high,
            low=self.low,
            close=self.close,
            volume=self.volume,
            time_close=self.close_time,
            exchange=self.exchange.lower(),
            market=self.market.lower(),
            symbol=self.symbol.upper(),
            timeframe=canonical_timeframe(self.timeframe),
            quote_volume=self.quote_volume,
            turnover=self.turnover,
            trades_count=self.trades_count,
            taker_buy_base_volume=self.taker_buy_base_volume,
            taker_buy_quote_volume=self.taker_buy_quote_volume,
            source_transport=self.raw_source,
            source_kind=self.source_kind,
            is_closed=self.is_closed,
            downloaded_at=(
                downloaded_at if downloaded_at is not None else self.received_at
            ),
        )


def normalize_binance_kline(
    payload: dict[str, Any],
    *,
    market: str,
    timeframe: str | None = None,
    source_kind: Literal["trade_kline", "mark_kline", "index_kline"] = "trade_kline",
    received_at: int | None = None,
) -> KlineUpdate:
    k = payload["k"]
    tf = timeframe or k.get("i") or "1m"
    return KlineUpdate(
        exchange="binance",
        market=market,
        symbol=str(k.get("s") or payload.get("s")).upper(),
        timeframe=tf,
        event_time=int(payload.get("E") or 0),
        open_time=int(k["t"]),
        close_time=int(k["T"]),
        open=float(k["o"]),
        high=float(k["h"]),
        low=float(k["l"]),
        close=float(k["c"]),
        volume=float(k["v"]),
        quote_volume=float(k["q"]) if k.get("q") is not None else None,
        trades_count=int(k["n"]) if k.get("n") is not None else None,
        taker_buy_base_volume=float(k["V"]) if k.get("V") is not None else None,
        taker_buy_quote_volume=float(k["Q"]) if k.get("Q") is not None else None,
        is_closed=bool(k.get("x")),
        source_kind=source_kind,
        raw_source="ws",
        raw_event_id=str(payload.get("e", "kline")) + ":" + str(payload.get("E", "")),
        received_at=received_at,
    )


def normalize_bybit_kline(
    payload: dict[str, Any],
    *,
    market: str,
    source_kind: Literal["trade_kline", "mark_kline", "index_kline"] = "trade_kline",
    received_at: int | None = None,
) -> list[KlineUpdate]:
    topic = str(payload.get("topic", ""))
    parts = topic.split(".")
    interval = parts[1] if len(parts) >= 3 else "1"
    symbol = parts[2] if len(parts) >= 3 else str(payload.get("symbol", ""))
    out: list[KlineUpdate] = []
    for item in payload.get("data", []):
        start = int(item["start"])
        end = int(item.get("end") or close_time_ms(start, interval))
        out.append(
            KlineUpdate(
                exchange="bybit",
                market=market,
                symbol=symbol.upper(),
                timeframe=interval,
                event_time=int(item.get("timestamp") or payload.get("ts") or 0),
                open_time=start,
                close_time=end,
                open=float(item["open"]),
                high=float(item["high"]),
                low=float(item["low"]),
                close=float(item["close"]),
                volume=float(item["volume"]),
                turnover=(
                    float(item["turnover"])
                    if item.get("turnover") is not None
                    else None
                ),
                is_closed=bool(item.get("confirm")),
                source_kind=source_kind,
                raw_source="ws",
                raw_event_id=topic + ":" + str(payload.get("ts", "")),
                received_at=received_at,
            )
        )
    return out


def bybit_topic(timeframe: str, symbol: str) -> str:
    return f"kline.{to_bybit_interval(timeframe)}.{symbol.upper()}"
