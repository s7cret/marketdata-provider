from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from marketdata_provider.contracts.errors import InvalidBarQueryError
from marketdata_provider.contracts.instrument import InstrumentKey
from marketdata_provider.contracts.series import CoverageReport
from marketdata_provider.contracts.timeframe import Timeframe

FootprintSource = Literal["storage", "provider", "auto"]
FootprintGapPolicy = Literal["fail", "allow_with_metadata"]


@dataclass(frozen=True, slots=True)
class FootprintQuery:
    instrument: InstrumentKey
    timeframe: Timeframe
    start_ms: int
    end_ms: int
    price_bucket: float | None = None
    tick_size: float | None = None
    ticks_per_row: int = 1
    source: FootprintSource = "auto"
    gap_policy: FootprintGapPolicy = "fail"

    def __post_init__(self) -> None:
        if self.start_ms >= self.end_ms:
            raise InvalidBarQueryError("start_ms must be less than end_ms")
        if self.timeframe.duration_ms is None:
            raise InvalidBarQueryError("footprint timeframe must have fixed duration")
        if self.source not in ("storage", "provider", "auto"):
            raise InvalidBarQueryError(f"unsupported footprint source: {self.source!r}")
        if self.gap_policy not in ("fail", "allow_with_metadata"):
            raise InvalidBarQueryError(f"unsupported gap_policy: {self.gap_policy!r}")
        if self.price_bucket is None:
            if self.tick_size is None or self.tick_size <= 0:
                raise InvalidBarQueryError(
                    "price_bucket or positive tick_size is required"
                )
            if self.ticks_per_row <= 0:
                raise InvalidBarQueryError("ticks_per_row must be positive")
        elif self.price_bucket <= 0:
            raise InvalidBarQueryError("price_bucket must be positive")

    @property
    def bucket_size(self) -> float:
        if self.price_bucket is not None:
            return self.price_bucket
        assert self.tick_size is not None
        return self.tick_size * self.ticks_per_row


@dataclass(frozen=True, slots=True)
class AggTrade:
    trade_id: int
    time: int
    price: float
    quantity: float
    buyer_maker: bool


@dataclass(frozen=True, slots=True)
class FootprintLevel:
    price_low: float
    price_high: float
    buy_volume: float = 0.0
    sell_volume: float = 0.0
    buy_count: int = 0
    sell_count: int = 0

    @property
    def total_volume(self) -> float:
        return self.buy_volume + self.sell_volume

    @property
    def volume_delta(self) -> float:
        return self.buy_volume - self.sell_volume


@dataclass(frozen=True, slots=True)
class FootprintBar:
    time: int
    time_close: int
    levels: tuple[FootprintLevel, ...]
    trades_count: int


@dataclass(frozen=True, slots=True)
class FootprintSeries:
    query: FootprintQuery
    bars: tuple[FootprintBar, ...]
    coverage: CoverageReport
