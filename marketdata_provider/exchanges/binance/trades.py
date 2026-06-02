from __future__ import annotations

import time
from typing import Any

import httpx

from marketdata_provider.config import BinanceConfig
from marketdata_provider.contracts.footprint import AggTrade
from marketdata_provider.errors import MDInvalidExchangeResponse, MDNetworkUnavailable, MDPaginationStalled
from marketdata_provider.exchanges.binance.provider import _base_url
from marketdata_provider.symbols import normalize_symbol

BINANCE_AGG_TRADES_ENDPOINTS = {"spot": "/api/v3/aggTrades", "usdm": "/fapi/v1/aggTrades"}
_RATE_LIMIT_STATUSES = {418, 429}


def normalize_binance_agg_trades(payload: Any) -> list[AggTrade]:
    if not isinstance(payload, list):
        raise MDInvalidExchangeResponse("Binance aggTrades payload must be a list")
    trades: list[AggTrade] = []
    for row in payload:
        try:
            trades.append(
                AggTrade(
                    trade_id=int(row["a"]),
                    time=int(row["T"]),
                    price=float(row["p"]),
                    quantity=float(row["q"]),
                    buyer_maker=bool(row["m"]),
                )
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise MDInvalidExchangeResponse("Binance aggTrade row is invalid", details={"row": row}) from exc
    return sorted(trades, key=lambda trade: (trade.time, trade.trade_id))


def _get_json(client: httpx.Client, url: str, params: dict[str, Any], *, max_retries: int) -> Any:
    for attempt in range(max_retries + 1):
        try:
            response = client.get(url, params=params)
            if response.status_code in _RATE_LIMIT_STATUSES:
                if attempt >= max_retries:
                    raise MDNetworkUnavailable("Binance aggTrades rate limit exceeded", details={"status": response.status_code, "params": params})
                time.sleep(min(2.0, 0.25 * (2**attempt)))
                continue
            if 500 <= response.status_code < 600 and attempt < max_retries:
                time.sleep(min(2.0, 0.25 * (2**attempt)))
                continue
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError as exc:
            if attempt >= max_retries:
                raise MDNetworkUnavailable("Binance aggTrades request failed", details={"error": str(exc), "params": params}) from exc
            time.sleep(min(2.0, 0.25 * (2**attempt)))
    raise MDNetworkUnavailable("Binance aggTrades request failed")


def binance_get_agg_trades_sync(
    symbol: str,
    start: int,
    end: int,
    cfg: BinanceConfig,
    *,
    market: str | None = None,
    timeout: float = 15.0,
    max_retries: int = 3,
    max_trades: int | None = None,
) -> list[AggTrade]:
    ns = normalize_symbol(symbol, exchange="BINANCE", market=market)
    if ns.market not in BINANCE_AGG_TRADES_ENDPOINTS:
        return []
    base = _base_url(cfg, ns.market)
    endpoint = BINANCE_AGG_TRADES_ENDPOINTS[ns.market]
    per_page = min(1000, max_trades or 1000)
    cursor = start
    out: list[AggTrade] = []
    with httpx.Client(timeout=timeout, headers={"User-Agent": cfg.user_agent}) as client:
        while cursor < end:
            remaining = None if max_trades is None else max_trades - len(out)
            if remaining is not None and remaining <= 0:
                break
            limit = min(per_page, remaining) if remaining is not None else per_page
            params = {"symbol": ns.exchange_symbol, "startTime": cursor, "endTime": end - 1, "limit": limit}
            page = normalize_binance_agg_trades(_get_json(client, base + endpoint, params, max_retries=max_retries))
            page = [trade for trade in page if start <= trade.time < end]
            if not page:
                break
            out.extend(page)
            next_cursor = page[-1].time + 1
            if next_cursor <= cursor:
                raise MDPaginationStalled("Binance aggTrades pagination cursor did not advance", details={"cursor": cursor})
            cursor = next_cursor
            if len(page) < limit:
                break
    by_id = {trade.trade_id: trade for trade in out}
    return [by_id[trade_id] for trade_id in sorted(by_id)]

