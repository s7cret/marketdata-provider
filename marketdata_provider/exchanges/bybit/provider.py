from __future__ import annotations

import asyncio
import time
from typing import Any, cast

import httpx

from marketdata_provider.config import BybitConfig
from marketdata_provider.core.bar import Bar
from marketdata_provider.errors import (
    MDInvalidExchangeResponse,
    MDNetworkUnavailable,
    MDPaginationStalled,
    MDSymbolUnsupported,
)
from marketdata_provider.exchanges.bybit.rest import (
    BYBIT_ENDPOINT,
    normalize_bybit_klines,
)
from marketdata_provider.symbols import normalize_symbol
from marketdata_provider.timeframes import next_open_time_ms, to_bybit_interval
from marketdata_provider.validation import validate_bars

_RATE_LIMIT_CODES = {10006}
_RATE_LIMIT_STATUSES = {429}


def _category(market: str) -> str:
    if market in {"spot", "linear", "inverse"}:
        return market
    raise MDSymbolUnsupported(f"Unsupported Bybit market: {market}")


def _get_json(
    client: httpx.Client, url: str, params: dict[str, Any], *, max_retries: int
) -> Any:
    last: Exception | None = None
    for attempt in range(max_retries + 1):
        try:
            r = client.get(url, params=params)
            if r.status_code in _RATE_LIMIT_STATUSES:
                if attempt >= max_retries:
                    raise MDNetworkUnavailable(
                        "Bybit rate limit exceeded",
                        details={"status": r.status_code, "params": params},
                    )
                time.sleep(min(2.0, 0.25 * (2**attempt)))
                continue
            if 500 <= r.status_code < 600 and attempt < max_retries:
                time.sleep(min(2.0, 0.25 * (2**attempt)))
                continue
            r.raise_for_status()
            payload = r.json()
            if (
                isinstance(payload, dict)
                and int(payload.get("retCode", 0)) in _RATE_LIMIT_CODES
            ):
                if attempt >= max_retries:
                    raise MDNetworkUnavailable(
                        "Bybit rate limit exceeded",
                        details={
                            "retCode": payload.get("retCode"),
                            "retMsg": payload.get("retMsg"),
                            "params": params,
                        },
                    )
                time.sleep(min(2.0, 0.25 * (2**attempt)))
                continue
            return payload
        except httpx.HTTPError as e:
            last = e
            if attempt >= max_retries:
                raise MDNetworkUnavailable(
                    "Bybit HTTP request failed",
                    details={"error": str(e), "params": params},
                ) from e
            time.sleep(min(2.0, 0.25 * (2**attempt)))
    raise MDNetworkUnavailable(
        "Bybit HTTP request failed", details={"error": str(last) if last else "unknown"}
    )


def _server_time_ms(client: httpx.Client, base_url: str, *, max_retries: int) -> int:
    payload = _get_json(
        client, base_url + "/v5/market/time", {}, max_retries=max_retries
    )
    try:
        result = payload.get("result", {}) if isinstance(payload, dict) else {}
        time_nano = result.get("timeNano")
        if time_nano not in (None, "", 0):
            return int(cast(str | int, time_nano)) // 1_000_000
        time_second = result.get("timeSecond")
        if time_second not in (None, ""):
            return int(cast(str | int, time_second)) * 1000
        raise KeyError("timeNano/timeSecond")
    except Exception as e:
        raise MDInvalidExchangeResponse(
            "Bybit server time payload missing result.timeNano/timeSecond"
        ) from e


def bybit_get_bars_sync(
    symbol: str,
    timeframe: str,
    start: int | None,
    end: int | None,
    cfg: BybitConfig,
    market: str | None = None,
    timeout: float = 15.0,
    max_retries: int = 3,
    max_bars: int | None = None,
    include_open_candle: bool = False,
) -> list[Bar]:
    ns = normalize_symbol(symbol, exchange="BYBIT", market=market)
    category = _category(ns.market)
    interval = to_bybit_interval(timeframe)
    per_page = min(cfg.max_limit, max_bars or cfg.max_limit)
    out: list[Bar] = []
    cursor = start
    with httpx.Client(
        timeout=timeout, headers={"User-Agent": cfg.user_agent}
    ) as client:
        server_time = _server_time_ms(client, cfg.base_url, max_retries=max_retries)
        while True:
            remaining = None if max_bars is None else max_bars - len(out)
            if remaining is not None and remaining <= 0:
                break
            limit = min(per_page, remaining) if remaining is not None else per_page
            params: dict[str, Any] = {
                "category": category,
                "symbol": ns.exchange_symbol,
                "interval": interval,
                "limit": limit,
            }
            if cursor is not None:
                params["start"] = cursor
            if end is not None:
                params["end"] = end - 1
            payload = _get_json(
                client, cfg.base_url + BYBIT_ENDPOINT, params, max_retries=max_retries
            )
            if not isinstance(payload, dict) or int(payload.get("retCode", 0)) != 0:
                raise MDInvalidExchangeResponse(
                    "Bybit kline request returned non-zero retCode",
                    details={"payload": payload if isinstance(payload, dict) else None},
                )
            page = normalize_bybit_klines(
                payload,
                symbol=ns.exchange_symbol,
                market=ns.market,
                timeframe=timeframe,
                server_time_ms=server_time,
                include_open_candle=include_open_candle,
            )
            page = [
                b
                for b in page
                if (start is None or b.time >= start) and (end is None or b.time < end)
            ]
            if not page:
                break
            old_cursor = cursor
            out.extend(page)
            if len(payload.get("result", {}).get("list", [])) < limit:
                break
            cursor = next_open_time_ms(page[-1].time, timeframe)
            if old_cursor is not None and cursor <= old_cursor:
                raise MDPaginationStalled(
                    "Bybit pagination cursor did not advance",
                    details={"cursor": cursor},
                )
            if end is not None and cursor >= end:
                break
    by_time = {b.time: b for b in out}
    final = [
        Bar(b.time, b.open, b.high, b.low, b.close, b.volume, b.time_close)
        for b in (by_time[t] for t in sorted(by_time))
    ]
    if max_bars is not None:
        final = final[:max_bars]
    validate_bars(final)
    return final


async def bybit_get_bars(
    symbol: str,
    timeframe: str,
    start: int | None,
    end: int | None,
    cfg: BybitConfig,
    market: str = "linear",
    timeout: float = 15.0,
    max_retries: int = 3,
    max_bars: int | None = None,
) -> list[Bar]:
    return await asyncio.to_thread(
        bybit_get_bars_sync,
        symbol,
        timeframe,
        start,
        end,
        cfg,
        market,
        timeout,
        max_retries,
        max_bars,
    )


async def bybit_get_intrabar_bars(
    symbol: str,
    chart_bar: Bar,
    lower_timeframe: str | None,
    cfg: BybitConfig,
    market: str = "linear",
    timeout: float = 15.0,
    max_retries: int = 3,
) -> list[Bar]:
    tf = lower_timeframe or "1m"
    end = chart_bar.time_close + 1 if chart_bar.time_close is not None else None
    return await bybit_get_bars(
        symbol,
        tf,
        chart_bar.time,
        end,
        cfg,
        market=market,
        timeout=timeout,
        max_retries=max_retries,
    )
