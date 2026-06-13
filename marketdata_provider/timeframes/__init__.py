from __future__ import annotations
import calendar
import re
from datetime import datetime, timezone
from marketdata_provider.errors import MDTimeframeUnsupported

_FIXED_UNITS = {"s": 1000, "m": 60_000, "h": 3_600_000}
_BINANCE = {
    "1s": "1s",
    "1": "1m",
    "1m": "1m",
    "3": "3m",
    "3m": "3m",
    "5": "5m",
    "5m": "5m",
    "15": "15m",
    "15m": "15m",
    "30": "30m",
    "30m": "30m",
    "60": "1h",
    "60m": "1h",
    "1h": "1h",
    "120": "2h",
    "120m": "2h",
    "2h": "2h",
    "240": "4h",
    "240m": "4h",
    "4h": "4h",
    "360": "6h",
    "360m": "6h",
    "6h": "6h",
    "720": "12h",
    "720m": "12h",
    "12h": "12h",
    "D": "1d",
    "1D": "1d",
    "1d": "1d",
    "W": "1w",
    "1W": "1w",
    "1w": "1w",
    "M": "1M",
    "1M": "1M",
}
_BYBIT = {
    "1": "1",
    "1m": "1",
    "3": "3",
    "3m": "3",
    "5": "5",
    "5m": "5",
    "15": "15",
    "15m": "15",
    "30": "30",
    "30m": "30",
    "60": "60",
    "1h": "60",
    "120": "120",
    "2h": "120",
    "240": "240",
    "4h": "240",
    "360": "360",
    "6h": "360",
    "720": "720",
    "12h": "720",
    "D": "D",
    "1D": "D",
    "1d": "D",
    "W": "W",
    "1W": "W",
    "1w": "W",
    "M": "M",
    "1M": "M",
}


def canonical_timeframe(tf: str) -> str:
    raw = tf.strip()
    if raw in {"D", "1D", "1d"}:
        return "1D"
    if raw in {"W", "1W", "1w"}:
        return "1W"
    if raw in {"M", "1M"}:
        return "1M"
    if raw.isdigit():
        return f"{int(raw)}m"
    m = re.fullmatch(r"(\d+)([smhd])", raw, re.I)
    if not m:
        raise MDTimeframeUnsupported(f"Unsupported timeframe: {tf}")
    n, unit = int(m.group(1)), m.group(2).lower()
    if unit == "d":
        if n == 1:
            return "1D"
        raise MDTimeframeUnsupported("Only 1D calendar timeframe supported in Stage A")
    return f"{n}{unit}"


def normalize_timeframe(tf: str) -> str:
    return canonical_timeframe(tf)


def is_calendar_timeframe(tf: str) -> bool:
    return canonical_timeframe(tf) in {"1D", "1W", "1M"}


def timeframe_ms(tf: str) -> int:
    c = canonical_timeframe(tf)
    if c == "1D":
        return 86_400_000
    if c == "1W":
        return 7 * 86_400_000
    if c == "1M":
        raise MDTimeframeUnsupported("1M has variable length; use close_time_ms")
    m = re.fullmatch(r"(\d+)([smh])", c)
    assert m is not None
    return int(m.group(1)) * _FIXED_UNITS[m.group(2)]


def timeframe_to_ms(tf: str) -> int:
    return timeframe_ms(tf)


def parse_time_ms(value: str | int) -> int:
    if isinstance(value, int):
        return value if value > 1_000_000_000_000 else value * 1000
    try:
        raw = int(value)
    except ValueError:
        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        return int(datetime.fromisoformat(text).timestamp() * 1000)
    return raw if raw > 1_000_000_000_000 else raw * 1000


def _dt(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, timezone.utc)


def _ms(d: datetime) -> int:
    return int(d.timestamp() * 1000)


def close_time_ms(open_time_ms: int, tf: str) -> int:
    c = canonical_timeframe(tf)
    d = _dt(open_time_ms)
    if c == "1D":
        return (
            _ms(datetime(d.year, d.month, d.day, tzinfo=timezone.utc)) + 86_400_000 - 1
        )
    if c == "1W":
        start = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
        monday_s = start.timestamp() - (start.weekday() * 86_400)
        return int(monday_s * 1000) + 7 * 86_400_000 - 1
    if c == "1M":
        last = calendar.monthrange(d.year, d.month)[1]
        return (
            _ms(datetime(d.year, d.month, last, tzinfo=timezone.utc)) + 86_400_000 - 1
        )
    return open_time_ms + timeframe_ms(c) - 1


def next_open_time_ms(open_time_ms: int, tf: str) -> int:
    return close_time_ms(open_time_ms, tf) + 1


def to_binance_interval(tf: str) -> str:
    c = canonical_timeframe(tf)
    return _BINANCE.get(tf) or _BINANCE.get(c) or (_raise_tf(tf))


def timeframe_to_binance_interval(tf: str) -> str:
    return to_binance_interval(tf)


def to_bybit_interval(tf: str) -> str:
    c = canonical_timeframe(tf)
    return _BYBIT.get(tf) or _BYBIT.get(c) or (_raise_tf(tf))


def timeframe_to_bybit_interval(tf: str) -> str:
    return to_bybit_interval(tf)


def default_intrabar_tf(chart_tf: str) -> str:
    c = canonical_timeframe(chart_tf) if chart_tf else "1m"
    if c in {"1D", "1W", "1M"}:
        return "60m"
    minutes = timeframe_ms(c) // 60_000
    return "1m" if minutes <= 60 else "5m"


def _raise_tf(tf: str) -> str:
    raise MDTimeframeUnsupported(f"Unsupported exchange timeframe: {tf}")
