from __future__ import annotations

from marketdata_provider.errors import MDSymbolUnsupported
from marketdata_provider.timeframes import close_time_ms


def _minutes(timeframe: str) -> int:
    mapping = {
        "1m": 1,
        "3m": 3,
        "5m": 5,
        "15m": 15,
        "30m": 30,
        "1h": 60,
        "4h": 240,
        "1d": 1440,
    }
    key = timeframe.lower()
    if key not in mapping:
        raise MDSymbolUnsupported(f"Unsupported spot timeframe: {timeframe}")
    return mapping[key]


def _requested_limit(
    timeframe: str, start: int | None, end: int | None, *, max_limit: int
) -> int:
    if start is None or end is None or end <= start:
        return max_limit
    duration = close_time_ms(0, timeframe) + 1
    bars = max(1, (end - start + duration - 1) // duration)
    return min(max_limit, bars)


def _coinbase_granularity(timeframe: str) -> int:
    allowed = {1: 60, 5: 300, 15: 900, 60: 3600, 360: 21_600, 1440: 86_400}
    minutes = _minutes(timeframe)
    if minutes not in allowed:
        raise MDSymbolUnsupported(f"Unsupported Coinbase timeframe: {timeframe}")
    return allowed[minutes]


def _okx_bar(timeframe: str) -> str:
    mapping = {
        "1m": "1m",
        "3m": "3m",
        "5m": "5m",
        "15m": "15m",
        "30m": "30m",
        "1h": "1H",
        "4h": "4H",
        "1d": "1D",
    }
    try:
        return mapping[timeframe.lower()]
    except KeyError as exc:
        raise MDSymbolUnsupported(f"Unsupported OKX timeframe: {timeframe}") from exc


def _kucoin_type(timeframe: str) -> str:
    mapping = {
        "1m": "1min",
        "3m": "3min",
        "5m": "5min",
        "15m": "15min",
        "30m": "30min",
        "1h": "1hour",
        "4h": "4hour",
        "1d": "1day",
    }
    try:
        return mapping[timeframe.lower()]
    except KeyError as exc:
        raise MDSymbolUnsupported(f"Unsupported KuCoin timeframe: {timeframe}") from exc


def _bitget_granularity(timeframe: str) -> str:
    mapping = {
        "1m": "1min",
        "3m": "3min",
        "5m": "5min",
        "15m": "15min",
        "30m": "30min",
        "1h": "1h",
        "4h": "4h",
        "1d": "1day",
    }
    try:
        return mapping[timeframe.lower()]
    except KeyError as exc:
        raise MDSymbolUnsupported(f"Unsupported Bitget timeframe: {timeframe}") from exc


def _bitget_mix_granularity(timeframe: str) -> str:
    mapping = {
        "1m": "1m",
        "3m": "3m",
        "5m": "5m",
        "15m": "15m",
        "30m": "30m",
        "1h": "1H",
        "4h": "4H",
        "1d": "1D",
    }
    try:
        return mapping[timeframe.lower()]
    except KeyError as exc:
        raise MDSymbolUnsupported(f"Unsupported Bitget timeframe: {timeframe}") from exc


def _kraken_futures_interval(timeframe: str) -> str:
    mapping = {
        "1m": "1m",
        "5m": "5m",
        "15m": "15m",
        "30m": "30m",
        "1h": "1h",
        "4h": "4h",
        "1d": "1d",
    }
    try:
        return mapping[timeframe.lower()]
    except KeyError as exc:
        raise MDSymbolUnsupported(
            f"Unsupported Kraken futures timeframe: {timeframe}"
        ) from exc


def _gate_settlement(symbol: str, market: str) -> str:
    if market == "linear" or "USDT" in symbol.upper():
        return "usdt"
    base = symbol.upper().split("_", 1)[0].split("-", 1)[0]
    return base.lower() or "btc"


def _gate_interval(timeframe: str) -> str:
    mapping = {
        "1m": "1m",
        "5m": "5m",
        "15m": "15m",
        "30m": "30m",
        "1h": "1h",
        "4h": "4h",
        "1d": "1d",
    }
    try:
        return mapping[timeframe.lower()]
    except KeyError as exc:
        raise MDSymbolUnsupported(
            f"Unsupported Gate.io timeframe: {timeframe}"
        ) from exc


def _htx_period(timeframe: str) -> str:
    mapping = {
        "1m": "1min",
        "5m": "5min",
        "15m": "15min",
        "30m": "30min",
        "1h": "60min",
        "4h": "4hour",
        "1d": "1day",
    }
    try:
        return mapping[timeframe.lower()]
    except KeyError as exc:
        raise MDSymbolUnsupported(f"Unsupported HTX timeframe: {timeframe}") from exc


def _mexc_interval(timeframe: str) -> str:
    mapping = {
        "1m": "1m",
        "5m": "5m",
        "15m": "15m",
        "30m": "30m",
        "1h": "1h",
        "4h": "4h",
        "1d": "1d",
    }
    try:
        return mapping[timeframe.lower()]
    except KeyError as exc:
        raise MDSymbolUnsupported(f"Unsupported MEXC timeframe: {timeframe}") from exc


def _mexc_contract_interval(timeframe: str) -> str:
    mapping = {
        "1m": "Min1",
        "5m": "Min5",
        "15m": "Min15",
        "30m": "Min30",
        "1h": "Min60",
        "4h": "Hour4",
        "1d": "Day1",
    }
    try:
        return mapping[timeframe.lower()]
    except KeyError as exc:
        raise MDSymbolUnsupported(
            f"Unsupported MEXC contract timeframe: {timeframe}"
        ) from exc
