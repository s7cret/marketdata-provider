from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from marketdata_provider.errors import MDValidationError

RUNTIME_CONTRACT_VERSION = "1.4"


@dataclass(frozen=True, slots=True)
class Bar:
    """Canonical market data bar.

    Times are UTC epoch milliseconds. time is the bar open time;
    range queries use start-inclusive/end-exclusive bounds.
    """

    time: int
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0
    time_close: int | None = None


@dataclass(frozen=True, slots=True)
class MarketBar(Bar):
    exchange: str = ""
    market: str = ""
    symbol: str = ""
    timeframe: str = ""
    source_transport: str = "rest"
    source_kind: str = "trade_kline"
    source: str = ""
    is_closed: bool | None = None
    quote_volume: float | None = None
    turnover: float | None = None
    trades_count: int | None = None
    taker_buy_base_volume: float | None = None
    taker_buy_quote_volume: float | None = None
    downloaded_at: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.is_closed is None:
            raise MDValidationError("is_closed required")

    def to_bar(self) -> Bar:
        return Bar(
            self.time,
            self.open,
            self.high,
            self.low,
            self.close,
            self.volume,
            self.time_close,
        )
