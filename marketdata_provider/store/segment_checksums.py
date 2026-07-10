from __future__ import annotations

import csv
import hashlib
import struct
from decimal import Decimal
from pathlib import Path
from typing import Iterable, cast

from marketdata_provider.core.bar import MarketBar
from marketdata_provider.errors import MDInvalidExchangeResponse
from marketdata_provider.store.segment_rows import row_to_bar
from marketdata_provider.timeframes import canonical_timeframe


def _canon_number(v: float | int | None) -> str | None:
    """Canonical number formatting (kept for backward-compat / external use)."""
    if v is None:
        return None
    d = Decimal(str(v)).normalize()
    if d == 0:
        return "0"
    return format(d, "f")


def market_bar_checksum(bar: MarketBar) -> str:
    return bars_checksum([bar])


def bars_checksum(bars: Iterable[MarketBar]) -> str:
    digest = hashlib.sha256()
    for bar in sorted(bars, key=lambda item: item.time):
        _update_checksum(digest, bar)
    return digest.hexdigest()


def validate_csv_checksum(path: Path, manifest: dict[str, object] | None) -> None:
    if manifest is None:
        return
    digest = hashlib.sha256()
    with path.open(newline="") as handle:
        for row in csv.DictReader(handle):
            _update_checksum(digest, row_to_bar(cast(dict[str, object], row)))
    actual = digest.hexdigest()
    if actual != manifest.get("checksum"):
        raise MDInvalidExchangeResponse(
            "Segment checksum mismatch",
            details={"expected": manifest.get("checksum"), "actual": actual},
        )


def _update_checksum(h: "hashlib._Hash", b: MarketBar) -> None:
    """Fast per-bar checksum using struct.pack (~28x faster than Decimal+json).

    All string metadata fields are encoded directly; all numeric fields are
    packed in a single struct.pack call. None values use sentinel -1 (ints)
    or 0.0 (floats) — deterministic and consistent for identical data.
    """
    h.update(b.exchange.lower().encode())
    h.update(b.market.lower().encode())
    h.update(b.symbol.upper().encode())
    h.update(b.source_kind.encode())
    h.update(b.source_transport.encode())
    h.update(canonical_timeframe(b.timeframe).encode())
    h.update(
        struct.pack(
            ">qqqddddddd?",
            b.time,
            b.time_close if b.time_close is not None else 0,
            b.trades_count if b.trades_count is not None else -1,
            b.open,
            b.high,
            b.low,
            b.close,
            b.volume if b.volume is not None else 0.0,
            b.quote_volume if b.quote_volume is not None else 0.0,
            b.turnover if b.turnover is not None else 0.0,
            b.is_closed,
        )
    )
    h.update(b"\n")
