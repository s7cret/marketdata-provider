from __future__ import annotations

from typing import AsyncIterator, Protocol, runtime_checkable

from marketdata_provider.contracts.events import LiveKlineEvent
from marketdata_provider.contracts.instrument import InstrumentKey
from marketdata_provider.contracts.query import BarQuery
from marketdata_provider.contracts.series import BarSeries, CoverageReport, StoreResult
from marketdata_provider.contracts.timeframe import Timeframe


@runtime_checkable
class MarketDataProvider(Protocol):
    def fetch_bars(self, query: BarQuery) -> BarSeries: ...


@runtime_checkable
class CandleStore(Protocol):
    def read(self, query: BarQuery) -> BarSeries: ...

    def write(self, series: BarSeries) -> StoreResult: ...

    def coverage(self, query: BarQuery) -> CoverageReport: ...


@runtime_checkable
class LiveKlineClient(Protocol):
    async def events(
        self,
        *,
        max_messages: int | None = None,
        timeout_s: float | None = None,
    ) -> AsyncIterator[LiveKlineEvent]: ...


@runtime_checkable
class LiveKlineClientFactory(Protocol):
    def create_live_kline_client(
        self,
        instrument: InstrumentKey,
        timeframe: Timeframe,
    ) -> LiveKlineClient: ...
