from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from marketdata_provider.contracts.errors import InvalidTimeframeError
from marketdata_provider.errors import MDTimeframeUnsupported
from marketdata_provider.timeframes import canonical_timeframe, timeframe_ms

TimeframeUnit = Literal["minute", "hour", "day", "week", "month"]


@dataclass(frozen=True, slots=True, eq=False)
class Timeframe:
    raw: str
    canonical: str
    multiplier: int
    unit: TimeframeUnit
    duration_ms: int | None

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Timeframe):
            return NotImplemented
        return (
            self.canonical == other.canonical
            and self.multiplier == other.multiplier
            and self.unit == other.unit
            and self.duration_ms == other.duration_ms
        )

    def __hash__(self) -> int:
        return hash((self.canonical, self.multiplier, self.unit, self.duration_ms))


def parse_timeframe(value: str) -> Timeframe:
    raw = value.strip()
    if not raw:
        raise InvalidTimeframeError("timeframe must not be empty")
    try:
        canonical = canonical_timeframe(raw)
    except MDTimeframeUnsupported as exc:
        raise InvalidTimeframeError(str(exc)) from exc

    if canonical.endswith("m"):
        multiplier = int(canonical[:-1])
        unit: TimeframeUnit = "minute"
    elif canonical.endswith("h"):
        multiplier = int(canonical[:-1])
        unit = "hour"
    elif canonical == "1D":
        multiplier = 1
        unit = "day"
    elif canonical == "1W":
        multiplier = 1
        unit = "week"
    elif canonical == "1M":
        multiplier = 1
        unit = "month"
    else:
        raise InvalidTimeframeError(f"unsupported canonical timeframe: {canonical}")

    duration_ms = None if unit == "month" else timeframe_ms(canonical)
    return Timeframe(
        raw=raw,
        canonical=canonical,
        multiplier=multiplier,
        unit=unit,
        duration_ms=duration_ms,
    )
