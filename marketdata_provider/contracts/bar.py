from __future__ import annotations

from dataclasses import dataclass

from marketdata_provider.contracts.errors import InvalidBarError
from marketdata_provider.contracts.instrument import InstrumentKey
from marketdata_provider.contracts.timeframe import Timeframe


@dataclass(frozen=True, slots=True)
class Bar:
    instrument: InstrumentKey
    timeframe: Timeframe
    time: int
    time_close: int
    open: float
    high: float
    low: float
    close: float
    volume: float | None
    closed: bool

    def __post_init__(self) -> None:
        if self.time_close <= self.time:
            raise InvalidBarError("time_close must be greater than time")
        if self.high < max(self.open, self.close):
            raise InvalidBarError("high must be >= max(open, close)")
        if self.low > min(self.open, self.close):
            raise InvalidBarError("low must be <= min(open, close)")
        if self.volume is not None and self.volume < 0:
            raise InvalidBarError("volume must be non-negative or None")
