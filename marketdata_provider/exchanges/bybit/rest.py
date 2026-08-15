from __future__ import annotations

from collections.abc import Sequence
from typing import Any, cast

from marketdata_provider.canonical.bar import bar_finality
from marketdata_provider.config import BybitConfig
from marketdata_provider.core.bar import MarketBar
from marketdata_provider.errors import MDInvalidExchangeResponse
from marketdata_provider.timeframes import close_time_ms, to_bybit_interval
from marketdata_provider.validation import exclude_open_candle, validate_bars
from openpine_contracts import Finality

BYBIT_ENDPOINT = "/v5/market/kline"


def _extract_rows(payload: Any) -> Sequence[Sequence[Any]]:
    if isinstance(payload, dict):
        try:
            return payload["result"]["list"]
        except Exception as exc:
            raise MDInvalidExchangeResponse(
                "Bybit payload missing result.list"
            ) from exc
    return payload


def _normalize_row(
    row: Sequence[Any],
    *,
    symbol: str,
    market: str,
    timeframe: str,
    server_time_ms: int | None,
) -> MarketBar:
    try:
        open_time = int(row[0])
        close_time = close_time_ms(open_time, timeframe)
        return MarketBar(
            time=open_time,
            open=float(row[1]),
            high=float(row[2]),
            low=float(row[3]),
            close=float(row[4]),
            volume=float(row[5]),
            time_close=close_time,
            exchange="bybit",
            market=market,
            symbol=symbol.upper(),
            timeframe=timeframe,
            source="fixture",
            is_closed=bar_finality(
                close_time_ms=close_time, server_time_ms=server_time_ms
            )
            is Finality.FINAL,
            quote_volume=(
                float(row[6]) if len(row) > 6 and row[6] not in (None, "") else None
            ),
        )
    except (IndexError, TypeError, ValueError) as exc:
        raise MDInvalidExchangeResponse(
            "Bybit kline row is invalid", details={"row": row}
        ) from exc


def normalize_bybit_klines(
    payload: Any,
    *,
    symbol: str,
    market: str,
    timeframe: str,
    server_time_ms: int | None = None,
    include_open_candle: bool = False,
) -> list[MarketBar]:
    rows = _extract_rows(payload)
    bars = [
        _normalize_row(
            row,
            symbol=symbol,
            market=market,
            timeframe=timeframe,
            server_time_ms=server_time_ms,
        )
        for row in rows
    ]
    bars.sort(key=lambda bar: bar.time)  # Bybit V5 often returns newest first.
    if not include_open_candle:
        bars = cast(
            list[MarketBar], exclude_open_candle(bars, server_time_ms=server_time_ms)
        )
    validate_bars(bars)
    return bars


class OfflineBybitRestAdapter:
    def __init__(
        self,
        payload: Any,
        *,
        config: BybitConfig | None = None,
        server_time_ms: int | None = None,
    ):
        self.payload = payload
        self.config = config or BybitConfig()
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
        _ = to_bybit_interval(interval)
        bars = normalize_bybit_klines(
            self.payload,
            symbol=symbol,
            market=market,
            timeframe=interval,
            server_time_ms=self.server_time_ms,
            include_open_candle=include_open_candle,
        )
        filtered = [
            bar
            for bar in bars
            if (start is None or bar.time >= start) and (end is None or bar.time < end)
        ]
        return filtered if limit is None else filtered[:limit]
