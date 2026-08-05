from __future__ import annotations

from collections.abc import Sequence
from typing import Any, cast

from marketdata_provider.config import BinanceConfig
from marketdata_provider.core.bar import MarketBar
from marketdata_provider.errors import MDInvalidExchangeResponse
from marketdata_provider.timeframes import close_time_ms, to_binance_interval
from marketdata_provider.validation import exclude_open_candle, validate_bars

BINANCE_ENDPOINTS = {
    "spot": "/api/v3/klines",
    "usdm": "/fapi/v1/klines",
    "coinm": "/dapi/v1/klines",
}


def normalize_binance_klines(
    rows: Sequence[Sequence[Any]],
    *,
    symbol: str,
    market: str,
    timeframe: str,
    server_time_ms: int | None = None,
    include_open_candle: bool = False,
) -> list[MarketBar]:
    bars: list[MarketBar] = []
    for r in rows:
        if len(r) < 6:
            raise MDInvalidExchangeResponse("Binance kline row too short")
        open_time = int(r[0])
        close_time = (
            int(r[6])
            if len(r) > 6 and r[6] is not None
            else close_time_ms(open_time, timeframe)
        )
        if close_time <= open_time:
            close_time = close_time_ms(open_time, timeframe)
        bars.append(
            MarketBar(
                time=open_time,
                open=float(r[1]),
                high=float(r[2]),
                low=float(r[3]),
                close=float(r[4]),
                volume=float(r[5]),
                time_close=close_time,
                exchange="binance",
                market=market,
                symbol=symbol.upper(),
                timeframe=timeframe,
                source="fixture",
                is_closed=True,
                quote_volume=(
                    float(r[7]) if len(r) > 7 and r[7] not in (None, "") else None
                ),
                trades_count=(
                    int(r[8]) if len(r) > 8 and r[8] not in (None, "") else None
                ),
                taker_buy_base_volume=(
                    float(r[9]) if len(r) > 9 and r[9] not in (None, "") else None
                ),
                taker_buy_quote_volume=(
                    float(r[10]) if len(r) > 10 and r[10] not in (None, "") else None
                ),
            )
        )
    bars = sorted(bars, key=lambda b: b.time)
    if not include_open_candle:
        bars = cast(
            list[MarketBar], exclude_open_candle(bars, server_time_ms=server_time_ms)
        )
    validate_bars(bars)
    return bars


class OfflineBinanceRestAdapter:
    def __init__(
        self,
        rows: Sequence[Sequence[Any]],
        *,
        config: BinanceConfig | None = None,
        server_time_ms: int | None = None,
    ):
        self.rows = rows
        self.config = config or BinanceConfig()
        self.server_time_ms = server_time_ms

    def get_klines(
        self,
        *,
        symbol: str,
        market: str,
        interval: str,
        start: int | None,
        end: int | None,
        limit: int | None = None,
        include_open_candle: bool = False,
    ) -> list[MarketBar]:
        tf = interval
        _ = to_binance_interval(interval)
        rows = self.rows[: limit or len(self.rows)]
        bars = normalize_binance_klines(
            rows,
            symbol=symbol,
            market=market,
            timeframe=tf,
            server_time_ms=self.server_time_ms,
            include_open_candle=include_open_candle,
        )
        return [
            b
            for b in bars
            if (start is None or b.time >= start) and (end is None or b.time < end)
        ]
