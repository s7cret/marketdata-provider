from __future__ import annotations

from typing import Protocol

from marketdata_provider.contracts.query import BarQuery
from marketdata_provider.contracts.series import BarSeries, CoverageReport, StoreResult


class MarketDataProvider(Protocol):
    def fetch_bars(self, query: BarQuery) -> BarSeries: ...


class CandleStore(Protocol):
    def read(self, query: BarQuery) -> BarSeries: ...

    def write(self, series: BarSeries) -> StoreResult: ...

    def coverage(self, query: BarQuery) -> CoverageReport: ...
