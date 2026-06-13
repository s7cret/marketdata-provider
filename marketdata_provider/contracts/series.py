from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from marketdata_provider.contracts.bar import Bar
from marketdata_provider.contracts.query import BarQuery

CoverageStatus = Literal["valid", "gap", "duplicate", "unordered", "empty"]


@dataclass(frozen=True, slots=True)
class CoverageReport:
    requested_start_ms: int
    requested_end_ms: int
    delivered_start_ms: int | None
    delivered_end_ms: int | None
    missing_intervals: tuple[tuple[int, int], ...] = ()
    duplicate_timestamps: tuple[int, ...] = ()
    source_mix: tuple[str, ...] = ()
    status: CoverageStatus = "valid"

    @property
    def is_complete(self) -> bool:
        return (
            self.status == "valid"
            and not self.missing_intervals
            and not self.duplicate_timestamps
        )


@dataclass(frozen=True, slots=True)
class BarSeries:
    query: BarQuery
    bars: tuple[Bar, ...]
    coverage: CoverageReport


@dataclass(frozen=True, slots=True)
class StoreResult:
    success: bool
    rows_written: int = 0
    error: str | None = None
