from __future__ import annotations

import asyncio
import time
from typing import Any

import httpx

from marketdata_provider.config import BinanceConfig
from marketdata_provider.core.bar import Bar
from marketdata_provider.errors import MDInvalidExchangeResponse, MDNetworkUnavailable, MDPaginationStalled, MDSymbolUnsupported
from marketdata_provider.exchanges.binance.rest import BINANCE_ENDPOINTS, normalize_binance_klines
from marketdata_provider.symbols import normalize_symbol
from marketdata_provider.timeframes import next_open_time_ms, to_binance_interval
from marketdata_provider.validation import validate_bars

_RATE_LIMIT_STATUSES = {418, 429}


def _base_url(cfg: BinanceConfig, market: str) -> str:
    if market == "spot":
        return cfg.spot_base_url
    if market == "usdm":
        return cfg.usdm_base_url
    raise MDSymbolUnsupported(f"Unsupported Binance market: {market}")


def _limit(cfg: BinanceConfig, market: str) -> int:
    return cfg.max_limit_spot if market == "spot" else cfg.max_limit_usdm


def _get_json(client: httpx.Client, url: str, params: dict[str, Any], *, max_retries: int) -> Any:
    last: Exception | None = None
    for attempt in range(max_retries + 1):
        try:
            r = client.get(url, params=params)
            if r.status_code in _RATE_LIMIT_STATUSES:
                retry_after = r.headers.get("Retry-After")
                if attempt >= max_retries:
                    raise MDNetworkUnavailable("Binance rate limit exceeded", details={"status": r.status_code, "retry_after": retry_after, "params": params})
                time.sleep(float(retry_after) if retry_after else min(2.0, 0.25 * (2 ** attempt)))
                continue
            if 500 <= r.status_code < 600 and attempt < max_retries:
                time.sleep(min(2.0, 0.25 * (2 ** attempt)))
                continue
            r.raise_for_status()
            return r.json()
        except httpx.HTTPError as e:
            last = e
            if attempt >= max_retries:
                raise MDNetworkUnavailable("Binance HTTP request failed", details={"error": str(e), "params": params}) from e
            time.sleep(min(2.0, 0.25 * (2 ** attempt)))
    raise MDNetworkUnavailable("Binance HTTP request failed", details={"error": str(last) if last else "unknown"})


def _server_time_ms(client: httpx.Client, base_url: str, market: str, *, max_retries: int) -> int:
    endpoint = "/api/v3/time" if market == "spot" else "/fapi/v1/time"
    payload = _get_json(client, base_url + endpoint, {}, max_retries=max_retries)
    try:
        return int(payload["serverTime"])
    except Exception as e:
        raise MDInvalidExchangeResponse("Binance server time payload missing serverTime") from e


def binance_get_bars_sync(symbol: str, timeframe: str, start: int | None, end: int | None, cfg: BinanceConfig, market: str | None = None, timeout: float = 15.0, max_retries: int = 3, max_bars: int | None = None, include_open_candle: bool = False) -> list[Bar]:
    ns = normalize_symbol(symbol, exchange="BINANCE", market=market)
    interval = to_binance_interval(timeframe)
    base = _base_url(cfg, ns.market)
    endpoint = BINANCE_ENDPOINTS[ns.market]
    per_page = min(_limit(cfg, ns.market), max_bars or _limit(cfg, ns.market))
    out: list[Bar] = []
    cursor = start
    with httpx.Client(timeout=timeout, headers={"User-Agent": cfg.user_agent}) as client:
        server_time = None if include_open_candle else _server_time_ms(client, base, ns.market, max_retries=max_retries)
        while True:
            remaining = None if max_bars is None else max_bars - len(out)
            if remaining is not None and remaining <= 0:
                break
            limit = min(per_page, remaining) if remaining is not None else per_page
            params: dict[str, Any] = {"symbol": ns.exchange_symbol, "interval": interval, "limit": limit}
            if cursor is not None:
                params["startTime"] = cursor
            if end is not None:
                params["endTime"] = end - 1
            payload = _get_json(client, base + endpoint, params, max_retries=max_retries)
            if not isinstance(payload, list):
                raise MDInvalidExchangeResponse("Binance kline payload must be a list")
            page = normalize_binance_klines(payload, symbol=ns.exchange_symbol, market=ns.market, timeframe=timeframe, server_time_ms=server_time, include_open_candle=include_open_candle)
            page = [b for b in page if (start is None or b.time >= start) and (end is None or b.time < end)]
            if not page:
                break
            old_cursor = cursor
            out.extend(page)
            if len(payload) < limit:
                break
            cursor = next_open_time_ms(page[-1].time, timeframe)
            if old_cursor is not None and cursor <= old_cursor:
                raise MDPaginationStalled("Binance pagination cursor did not advance", details={"cursor": cursor})
            if end is not None and cursor >= end:
                break
    # De-duplicate page overlaps deterministically.
    by_time = {b.time: b for b in out}
    final = [Bar(b.time, b.open, b.high, b.low, b.close, b.volume, b.time_close) for b in (by_time[t] for t in sorted(by_time))]
    if max_bars is not None:
        final = final[:max_bars]
    validate_bars(final)
    return final


async def binance_get_bars(symbol: str, timeframe: str, start: int | None, end: int | None, cfg: BinanceConfig, market: str = "usdm", timeout: float = 15.0, max_retries: int = 3, max_bars: int | None = None) -> list[Bar]:
    return await asyncio.to_thread(binance_get_bars_sync, symbol, timeframe, start, end, cfg, market, timeout, max_retries, max_bars)


async def binance_get_intrabar_bars(symbol: str, chart_bar: Bar, lower_timeframe: str | None, cfg: BinanceConfig, market: str = "usdm", timeout: float = 15.0, max_retries: int = 3) -> list[Bar]:
    tf = lower_timeframe or "1m"
    end = chart_bar.time_close + 1 if chart_bar.time_close is not None else None
    return await binance_get_bars(symbol, tf, chart_bar.time, end, cfg, market=market, timeout=timeout, max_retries=max_retries)
