from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, TypeAlias

import httpx

from marketdata_provider.core.bar import Bar
from marketdata_provider.errors import (
    MDInvalidExchangeResponse,
    MDNetworkUnavailable,
    MDSymbolUnsupported,
)
from marketdata_provider.exchanges.public_intervals import (
    _bitget_granularity,
    _bitget_mix_granularity,
    _coinbase_granularity,
    _gate_interval,
    _gate_settlement,
    _htx_period,
    _kraken_futures_interval,
    _kucoin_type,
    _mexc_contract_interval,
    _mexc_interval,
    _minutes,
    _okx_bar,
    _requested_limit,
)
from marketdata_provider.timeframes import close_time_ms
from marketdata_provider.validation import validate_bars

SUPPORTED_PUBLIC_SPOT_EXCHANGES = {
    "okx",
    "coinbase",
    "kraken",
    "kucoin",
    "bitget",
    "gateio",
    "htx",
    "mexc",
}
SUPPORTED_PUBLIC_MARKET_EXCHANGES = SUPPORTED_PUBLIC_SPOT_EXCHANGES
_PUBLIC_MARKET_ALIASES = {
    "spot": "spot",
    "cash": "spot",
    "margin": "margin",
    "linear": "linear",
    "usdm": "linear",
    "futures": "linear",
    "usdt_futures": "linear",
    "usdt-futures": "linear",
    "usdc_futures": "linear",
    "usdc-futures": "linear",
    "swap": "linear",
    "inverse": "inverse",
    "coinm": "inverse",
    "coin_futures": "inverse",
    "coin-futures": "inverse",
    "delivery": "inverse",
    "delivery_futures": "delivery_futures",
    "delivery-futures": "delivery_futures",
    "dated_futures": "delivery_futures",
    "dated-futures": "delivery_futures",
}
_RATE_LIMIT_STATUSES = {418, 429}
_QueryParam: TypeAlias = str | int | float | bool | None
_QueryParams: TypeAlias = dict[str, _QueryParam]


def public_spot_get_bars_sync(
    *,
    exchange: str,
    symbol: str,
    timeframe: str,
    start: int | None,
    end: int | None,
    user_agent: str,
    include_open_candle: bool = False,
    timeout: float = 15.0,
    max_retries: int = 3,
) -> list[Bar]:
    del include_open_candle  # Public spot adapters return exchange-finalized rows.
    ex = exchange.strip().lower()
    if ex not in SUPPORTED_PUBLIC_SPOT_EXCHANGES:
        raise MDSymbolUnsupported(f"Unsupported public spot exchange: {exchange}")
    url, params = _spot_request(ex, symbol, timeframe, start, end)
    payload = _http_get_json(
        url,
        params=params,
        timeout=timeout,
        user_agent=user_agent,
        max_retries=max_retries,
    )
    bars = _normalize_spot_klines(ex, payload, symbol=symbol, timeframe=timeframe)
    filtered = [
        bar
        for bar in bars
        if (start is None or bar.time >= start) and (end is None or bar.time < end)
    ]
    validate_bars(filtered)
    return filtered


def public_market_get_bars_sync(
    *,
    exchange: str,
    market: str,
    symbol: str,
    timeframe: str,
    start: int | None,
    end: int | None,
    user_agent: str,
    include_open_candle: bool = False,
    timeout: float = 15.0,
    max_retries: int = 3,
) -> list[Bar]:
    del include_open_candle  # Public adapters return exchange-finalized rows.
    ex = exchange.strip().lower()
    provider_market = _public_provider_market(market)
    if ex not in SUPPORTED_PUBLIC_MARKET_EXCHANGES:
        raise MDSymbolUnsupported(f"Unsupported public market exchange: {exchange}")
    if provider_market in {"spot", "margin"}:
        return public_spot_get_bars_sync(
            exchange=ex,
            symbol=symbol,
            timeframe=timeframe,
            start=start,
            end=end,
            user_agent=user_agent,
            timeout=timeout,
            max_retries=max_retries,
        )
    if ex == "coinbase":
        raise MDSymbolUnsupported(f"Unsupported Coinbase market: {market}")
    url, params = _market_request(ex, provider_market, symbol, timeframe, start, end)
    payload = _http_get_json(
        url,
        params=params,
        timeout=timeout,
        user_agent=user_agent,
        max_retries=max_retries,
    )
    bars = _normalize_market_klines(
        ex, provider_market, payload, symbol=symbol, timeframe=timeframe
    )
    filtered = [
        bar
        for bar in bars
        if (start is None or bar.time >= start) and (end is None or bar.time < end)
    ]
    validate_bars(filtered)
    return filtered


def _public_provider_market(market: str) -> str:
    key = market.strip().lower().replace(" ", "_")
    try:
        return _PUBLIC_MARKET_ALIASES[key]
    except KeyError as exc:
        raise MDSymbolUnsupported(f"Unsupported public market type: {market}") from exc


def _http_get_json(
    url: str,
    *,
    params: _QueryParams,
    timeout: float,
    user_agent: str,
    max_retries: int,
) -> Any:
    last: Exception | None = None
    with httpx.Client(
        timeout=timeout, headers={"User-Agent": user_agent}, trust_env=False
    ) as client:
        for attempt in range(max_retries + 1):
            try:
                response = client.get(url, params=params)
                if response.status_code in _RATE_LIMIT_STATUSES:
                    if attempt >= max_retries:
                        raise MDNetworkUnavailable(
                            "Public spot rate limit exceeded",
                            details={
                                "status": response.status_code,
                                "url": url,
                                "params": params,
                            },
                        )
                    time.sleep(min(2.0, 0.25 * (2**attempt)))
                    continue
                if 500 <= response.status_code < 600 and attempt < max_retries:
                    time.sleep(min(2.0, 0.25 * (2**attempt)))
                    continue
                response.raise_for_status()
                return response.json()
            except httpx.HTTPError as exc:
                last = exc
                if attempt >= max_retries:
                    raise MDNetworkUnavailable(
                        "Public spot HTTP request failed",
                        details={"url": url, "params": params, "error": str(exc)},
                    ) from exc
                time.sleep(min(2.0, 0.25 * (2**attempt)))
    raise MDNetworkUnavailable(
        "Public spot HTTP request failed",
        details={
            "url": url,
            "params": params,
            "error": str(last) if last else "unknown",
        },
    )


def _spot_request(
    exchange: str, symbol: str, timeframe: str, start: int | None, end: int | None
) -> tuple[str, _QueryParams]:
    if exchange == "okx":
        params: _QueryParams = {
            "instId": symbol.upper(),
            "bar": _okx_bar(timeframe),
            "limit": 300,
        }
        if end is not None:
            params["after"] = end
        return "https://www.okx.com/api/v5/market/candles", params
    if exchange == "coinbase":
        params = {"granularity": _coinbase_granularity(timeframe)}
        if start is not None:
            params["start"] = _iso_ms(start)
        if end is not None:
            params["end"] = _iso_ms(end)
        return (
            f"https://api.exchange.coinbase.com/products/{symbol.upper()}/candles",
            params,
        )
    if exchange == "kraken":
        params = {"pair": symbol.upper(), "interval": _minutes(timeframe)}
        if start is not None:
            params["since"] = start // 1000
        return "https://api.kraken.com/0/public/OHLC", params
    if exchange == "kucoin":
        params = {"symbol": symbol.upper(), "type": _kucoin_type(timeframe)}
        if start is not None:
            params["startAt"] = start // 1000
        if end is not None:
            params["endAt"] = end // 1000
        return "https://api.kucoin.com/api/v1/market/candles", params
    if exchange == "bitget":
        params = {
            "symbol": symbol.upper(),
            "granularity": _bitget_granularity(timeframe),
            "limit": 1000,
        }
        if start is not None:
            params["startTime"] = start
        if end is not None:
            params["endTime"] = end
        return "https://api.bitget.com/api/v2/spot/market/candles", params
    if exchange == "gateio":
        params = {
            "currency_pair": symbol.upper(),
            "interval": _gate_interval(timeframe),
            "limit": 1000,
        }
        if start is not None:
            params["from"] = start // 1000
        if end is not None:
            params["to"] = end // 1000
        return "https://api.gateio.ws/api/v4/spot/candlesticks", params
    if exchange == "htx":
        return (
            "https://api.huobi.pro/market/history/kline",
            {
                "symbol": symbol.lower(),
                "period": _htx_period(timeframe),
                "size": _requested_limit(timeframe, start, end, max_limit=2000),
            },
        )
    if exchange == "mexc":
        params = {
            "symbol": symbol.upper(),
            "interval": _mexc_interval(timeframe),
            "limit": 1000,
        }
        if start is not None:
            params["startTime"] = start
        if end is not None:
            params["endTime"] = end - 1
        return "https://api.mexc.com/api/v3/klines", params
    raise MDSymbolUnsupported(f"Unsupported public spot exchange: {exchange}")


def _market_request(
    exchange: str,
    market: str,
    symbol: str,
    timeframe: str,
    start: int | None,
    end: int | None,
) -> tuple[str, _QueryParams]:
    if exchange == "okx":
        return _spot_request(exchange, symbol, timeframe, start, end)
    if exchange == "kraken":
        params: _QueryParams = {}
        if start is not None:
            params["from"] = start // 1000
        if end is not None:
            params["to"] = end // 1000
        return (
            f"https://futures.kraken.com/api/charts/v1/trade/{symbol.upper()}/{_kraken_futures_interval(timeframe)}",
            params,
        )
    if exchange == "kucoin":
        params = {"symbol": symbol.upper(), "granularity": _minutes(timeframe)}
        if start is not None:
            params["from"] = start
        if end is not None:
            params["to"] = end
        return "https://api-futures.kucoin.com/api/v1/kline/query", params
    if exchange == "bitget":
        params = {
            "symbol": symbol.upper(),
            "productType": "USDT-FUTURES" if market == "linear" else "COIN-FUTURES",
            "granularity": _bitget_mix_granularity(timeframe),
            "limit": 1000,
        }
        if start is not None:
            params["startTime"] = start
        if end is not None:
            params["endTime"] = end
        return "https://api.bitget.com/api/v2/mix/market/candles", params
    if exchange == "gateio":
        settlement = _gate_settlement(symbol, market)
        namespace = "delivery" if market == "delivery_futures" else "futures"
        params = {
            "contract": symbol.upper(),
            "interval": _gate_interval(timeframe),
            "limit": 1000,
        }
        if start is not None:
            params["from"] = start // 1000
        if end is not None:
            params["to"] = end // 1000
        return (
            f"https://api.gateio.ws/api/v4/{namespace}/{settlement}/candlesticks",
            params,
        )
    if exchange == "htx":
        if market == "linear" or market == "delivery_futures":
            url = "https://api.hbdm.com/linear-swap-ex/market/history/kline"
        else:
            url = "https://api.hbdm.com/swap-ex/market/history/kline"
        return url, {
            "contract_code": symbol.upper(),
            "period": _htx_period(timeframe),
            "size": _requested_limit(timeframe, start, end, max_limit=2000),
        }
    if exchange == "mexc":
        params = {"interval": _mexc_contract_interval(timeframe)}
        if start is not None:
            params["start"] = start // 1000
        if end is not None:
            params["end"] = end // 1000
        return (
            f"https://contract.mexc.com/api/v1/contract/kline/{symbol.upper()}",
            params,
        )
    raise MDSymbolUnsupported(f"Unsupported public market exchange: {exchange}")


def _normalize_spot_klines(
    exchange: str, payload: Any, *, symbol: str, timeframe: str
) -> list[Bar]:
    del symbol
    rows = _extract_rows(exchange, payload)
    bars: list[Bar] = []
    for row in rows:
        bar = _row_to_bar(exchange, row, timeframe=timeframe)
        if bar is not None:
            bars.append(bar)
    bars.sort(key=lambda bar: bar.time)
    return [
        Bar(
            bar.time, bar.open, bar.high, bar.low, bar.close, bar.volume, bar.time_close
        )
        for bar in bars
    ]


def _normalize_market_klines(
    exchange: str, market: str, payload: Any, *, symbol: str, timeframe: str
) -> list[Bar]:
    del market, symbol
    rows = _extract_market_rows(exchange, payload)
    bars: list[Bar] = []
    for row in rows:
        bar = _row_to_market_bar(exchange, row, timeframe=timeframe)
        if bar is not None:
            bars.append(bar)
    bars.sort(key=lambda bar: bar.time)
    return [
        Bar(
            bar.time, bar.open, bar.high, bar.low, bar.close, bar.volume, bar.time_close
        )
        for bar in bars
    ]


def _extract_market_rows(exchange: str, payload: Any) -> list[Any]:
    if exchange == "okx":
        rows = payload.get("data") if isinstance(payload, dict) else None
    elif exchange == "kraken":
        rows = payload.get("candles") if isinstance(payload, dict) else None
    elif exchange == "kucoin" or exchange == "bitget":
        rows = payload.get("data") if isinstance(payload, dict) else None
    elif exchange in {"gateio", "htx"}:
        rows = payload.get("data") if isinstance(payload, dict) else payload
    elif exchange == "mexc":
        rows = _mexc_contract_rows(payload)
    else:
        rows = None
    if not isinstance(rows, list):
        raise MDInvalidExchangeResponse(f"{exchange} kline payload missing rows")
    return rows


def _mexc_contract_rows(payload: Any) -> list[Any] | None:
    data = payload.get("data") if isinstance(payload, dict) else None
    if isinstance(data, list):
        return data
    if not isinstance(data, dict):
        return None
    times = data.get("time")
    if not isinstance(times, list):
        return None
    keys = ("time", "open", "high", "low", "close", "vol")
    rows: list[dict[str, object]] = []
    for index in range(len(times)):
        try:
            rows.append({key: data[key][index] for key in keys})
        except (KeyError, IndexError, TypeError):
            raise MDInvalidExchangeResponse("mexc kline row is invalid") from None
    return rows


def _extract_rows(exchange: str, payload: Any) -> list[Any]:
    if exchange == "okx":
        rows = payload.get("data") if isinstance(payload, dict) else None
    elif exchange == "kraken":
        result = payload.get("result", {}) if isinstance(payload, dict) else {}
        rows = (
            next((value for key, value in result.items() if key != "last"), [])
            if isinstance(result, dict)
            else []
        )
    elif exchange == "kucoin" or exchange == "bitget" or exchange == "htx":
        rows = payload.get("data") if isinstance(payload, dict) else None
    elif exchange == "mexc":
        rows = payload if isinstance(payload, list) else None
    else:
        rows = payload if isinstance(payload, list) else None
    if not isinstance(rows, list):
        raise MDInvalidExchangeResponse(f"{exchange} kline payload missing rows")
    return rows


def _row_to_bar(exchange: str, row: Any, *, timeframe: str) -> Bar | None:
    try:
        if exchange == "okx":
            return _bar_ms(row[0], row[1], row[2], row[3], row[4], row[5], timeframe)
        if exchange == "coinbase":
            return _bar_seconds(
                row[0], row[3], row[2], row[1], row[4], row[5], timeframe
            )
        if exchange == "kraken":
            return _bar_seconds(
                row[0], row[1], row[2], row[3], row[4], row[6], timeframe
            )
        if exchange == "kucoin":
            return _bar_seconds(
                row[0], row[1], row[3], row[4], row[2], row[5], timeframe
            )
        if exchange == "bitget":
            return _bar_ms(row[0], row[1], row[2], row[3], row[4], row[5], timeframe)
        if exchange == "gateio":
            return _bar_seconds(
                row[0], row[5], row[3], row[4], row[2], row[1], timeframe
            )
        if exchange == "htx":
            return _bar_seconds(
                row["id"],
                row["open"],
                row["high"],
                row["low"],
                row["close"],
                row["amount"],
                timeframe,
            )
        if exchange == "mexc":
            return _bar_ms(row[0], row[1], row[2], row[3], row[4], row[5], timeframe)
    except (KeyError, IndexError, TypeError, ValueError) as exc:
        raise MDInvalidExchangeResponse(
            f"{exchange} kline row is invalid", details={"row": row}
        ) from exc
    return None


def _row_to_market_bar(exchange: str, row: Any, *, timeframe: str) -> Bar | None:
    try:
        if exchange == "okx":
            return _bar_ms(row[0], row[1], row[2], row[3], row[4], row[5], timeframe)
        if exchange == "kraken":
            return _bar_epoch(
                row["time"],
                row["open"],
                row["high"],
                row["low"],
                row["close"],
                row["volume"],
                timeframe,
            )
        if exchange == "kucoin":
            return _bar_epoch(row[0], row[1], row[2], row[3], row[4], row[5], timeframe)
        if exchange == "bitget":
            return _bar_ms(row[0], row[1], row[2], row[3], row[4], row[5], timeframe)
        if exchange == "gateio":
            if isinstance(row, dict):
                return _bar_seconds(
                    row["t"],
                    row["o"],
                    row["h"],
                    row["l"],
                    row["c"],
                    row["v"],
                    timeframe,
                )
            return _bar_seconds(
                row[0], row[5], row[3], row[4], row[2], row[1], timeframe
            )
        if exchange == "htx":
            return _bar_seconds(
                row["id"],
                row["open"],
                row["high"],
                row["low"],
                row["close"],
                row["amount"],
                timeframe,
            )
        if exchange == "mexc":
            if isinstance(row, dict):
                return _bar_epoch(
                    row["time"],
                    row["open"],
                    row["high"],
                    row["low"],
                    row["close"],
                    row["vol"],
                    timeframe,
                )
            return _bar_epoch(row[0], row[1], row[2], row[3], row[4], row[5], timeframe)
    except (KeyError, IndexError, TypeError, ValueError) as exc:
        raise MDInvalidExchangeResponse(
            f"{exchange} kline row is invalid", details={"row": row}
        ) from exc
    return None


def _bar_ms(
    open_time: object,
    open_: object,
    high: object,
    low: object,
    close: object,
    volume: object,
    timeframe: str,
) -> Bar:
    time_ms = int(str(open_time))
    return Bar(
        time_ms,
        float(str(open_)),
        float(str(high)),
        float(str(low)),
        float(str(close)),
        float(str(volume)),
        close_time_ms(time_ms, timeframe),
    )


def _bar_seconds(
    open_time: object,
    open_: object,
    high: object,
    low: object,
    close: object,
    volume: object,
    timeframe: str,
) -> Bar:
    return _bar_ms(
        int(str(open_time)) * 1000, open_, high, low, close, volume, timeframe
    )


def _bar_epoch(
    open_time: object,
    open_: object,
    high: object,
    low: object,
    close: object,
    volume: object,
    timeframe: str,
) -> Bar:
    value = int(str(open_time))
    return _bar_ms(
        value if value >= 10_000_000_000 else value * 1000,
        open_,
        high,
        low,
        close,
        volume,
        timeframe,
    )


def _iso_ms(value: int) -> str:
    return (
        datetime.fromtimestamp(value / 1000, tz=timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )
