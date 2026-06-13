from __future__ import annotations

import hashlib
import json
from decimal import Decimal
from typing import Iterable

from marketdata_provider.core.bar import MarketBar
from marketdata_provider.timeframes import canonical_timeframe


def _canon_number(v: float | int | None) -> str | None:
    if v is None:
        return None
    d = Decimal(str(v)).normalize()
    if d == 0:
        return "0"
    return format(d, "f")


def market_bar_checksum(bar: MarketBar) -> str:
    return bars_checksum([bar])


def bars_checksum(bars: Iterable[MarketBar]) -> str:
    h = hashlib.sha256()
    for b in sorted(bars, key=lambda x: x.time):
        _update_checksum(h, b)
    return h.hexdigest()


def _update_checksum(h: "hashlib._Hash", b: MarketBar) -> None:
    row = {
        "close": _canon_number(b.close),
        "exchange": b.exchange.lower(),
        "high": _canon_number(b.high),
        "is_closed": bool(b.is_closed),
        "low": _canon_number(b.low),
        "market": b.market.lower(),
        "open": _canon_number(b.open),
        "quote_volume": _canon_number(b.quote_volume),
        "source_kind": b.source_kind,
        "source_transport": b.source_transport,
        "symbol": b.symbol.upper(),
        "time": int(b.time),
        "time_close": int(b.time_close) if b.time_close is not None else None,
        "timeframe": canonical_timeframe(b.timeframe),
        "trades_count": int(b.trades_count) if b.trades_count is not None else None,
        "turnover": _canon_number(b.turnover),
        "volume": _canon_number(b.volume),
    }
    h.update(json.dumps(row, sort_keys=True, separators=(",", ":")).encode())
    h.update(b"\n")
