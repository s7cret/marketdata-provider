from __future__ import annotations

import calendar
import re
from datetime import UTC, datetime

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
    if type(tf) is not str or not tf.strip():
        raise MDTimeframeUnsupported("Timeframe must be a nonempty string")
    raw = tf.strip()
    if raw in {"D", "1D", "1d"}:
        return "1D"
    if raw in {"W", "1W", "1w"}:
        return "1W"
    if raw in {"M", "1M"}:
        return "1M"
    # Uppercase M is calendar months, never minutes. This provider currently
    # supports one calendar month only; reject 2M instead of relabelling as 2m.
    if raw.endswith("M"):
        raise MDTimeframeUnsupported(f"Unsupported calendar month timeframe: {tf}")
    if re.fullmatch(r"[0-9]+", raw):
        if int(raw) <= 0:
            raise MDTimeframeUnsupported("Timeframe duration must be positive")
        return f"{int(raw)}m"
    m = re.fullmatch(r"([0-9]+)([smhd])", raw, re.IGNORECASE)
    if not m:
        raise MDTimeframeUnsupported(f"Unsupported timeframe: {tf}")
    n, unit = int(m.group(1)), m.group(2).lower()
    if n <= 0:
        raise MDTimeframeUnsupported("Timeframe duration must be positive")
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
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return int(parsed.timestamp() * 1000)
    return raw if raw > 1_000_000_000_000 else raw * 1000


def _dt(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, UTC)


def _ms(d: datetime) -> int:
    return int(d.timestamp() * 1000)


def close_time_ms(open_time_ms: int, tf: str) -> int:
    c = canonical_timeframe(tf)
    d = _dt(open_time_ms)
    if c == "1D":
        return _ms(datetime(d.year, d.month, d.day, tzinfo=UTC)) + 86_400_000 - 1
    if c == "1W":
        start = datetime(d.year, d.month, d.day, tzinfo=UTC)
        monday_s = start.timestamp() - (start.weekday() * 86_400)
        return int(monday_s * 1000) + 7 * 86_400_000 - 1
    if c == "1M":
        last = calendar.monthrange(d.year, d.month)[1]
        return _ms(datetime(d.year, d.month, last, tzinfo=UTC)) + 86_400_000 - 1
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


def to_pine_timeframe(tf: str) -> str:
    """Translate provider units to Pine notation without conflating m and M.

    Provider hours become Pine minutes. Calendar periods are preserved rather
    than approximated as fixed seconds. The Pine consumer still validates its
    language-specific multiplier limits.
    """
    value = canonical_timeframe(tf)
    if value in {"1D", "1W", "1M"}:
        return value
    if value.endswith("h"):
        return str(int(value[:-1]) * 60)
    if value.endswith("m"):
        return str(int(value[:-1]))
    if value.endswith("s"):
        return value[:-1] + "S"
    raise MDTimeframeUnsupported(f"Cannot translate provider timeframe to Pine: {tf}")
