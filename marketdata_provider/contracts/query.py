from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from marketdata_provider.contracts.errors import InvalidBarQueryError
from marketdata_provider.contracts.instrument import InstrumentKey
from marketdata_provider.contracts.timeframe import Timeframe

BarSource = Literal["storage", "provider", "auto"]
GapPolicy = Literal["fail", "allow_with_metadata"]
ErrorPolicy = Literal["raise"]


@dataclass(frozen=True, slots=True)
class BarQuery:
    instrument: InstrumentKey
    timeframe: Timeframe
    start_ms: int
    end_ms: int
    source: BarSource = "auto"
    gap_policy: GapPolicy = "fail"
    error_policy: ErrorPolicy = "raise"

    def __post_init__(self) -> None:
        if self.start_ms >= self.end_ms:
            raise InvalidBarQueryError("start_ms must be less than end_ms")
        if self.source not in ("storage", "provider", "auto"):
            raise InvalidBarQueryError(f"unsupported source: {self.source!r}")
        if self.gap_policy not in ("fail", "allow_with_metadata"):
            raise InvalidBarQueryError(f"unsupported gap_policy: {self.gap_policy!r}")
        if self.error_policy != "raise":
            raise InvalidBarQueryError("production error_policy must be 'raise'")
