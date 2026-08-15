from __future__ import annotations

from collections.abc import Sequence

from marketdata_provider.core.bar import Bar
from marketdata_provider.errors import MDValidationError


def validate_bars(bars: Sequence[Bar], *, allow_empty: bool = True) -> None:
    if not bars and not allow_empty:
        raise MDValidationError("No bars returned")
    prev: int | None = None
    seen: set[int] = set()
    for b in bars:
        if b.time in seen:
            raise MDValidationError(f"Duplicate bar time: {b.time}")
        seen.add(b.time)
        if prev is not None and b.time <= prev:
            raise MDValidationError("Bars must be strictly sorted ASC by time")
        prev = b.time
        if b.high < max(b.open, b.close):
            raise MDValidationError(f"OHLC high violation at {b.time}")
        if b.low > min(b.open, b.close):
            raise MDValidationError(f"OHLC low violation at {b.time}")
        if b.time_close is not None and b.time_close < b.time:
            raise MDValidationError(f"time_close before time at {b.time}")


def exclude_open_candle(
    bars: Sequence[Bar], *, server_time_ms: int | None
) -> list[Bar]:
    if not bars:
        return []
    if server_time_ms is None:
        # Fail-closed: last REST row may still be forming.
        return list(bars[:-1])
    return [b for b in bars if b.time_close is None or b.time_close < server_time_ms]
