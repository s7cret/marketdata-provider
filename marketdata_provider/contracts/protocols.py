from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from typing import Any, Protocol, runtime_checkable

from marketdata_provider.canonical.bar import DataSnapshotV2
from marketdata_provider.contracts.events import LiveKlineEvent
from marketdata_provider.contracts.footprint import FootprintQuery, FootprintSeries
from marketdata_provider.contracts.instrument import InstrumentKey
from marketdata_provider.contracts.query import BarQuery
from marketdata_provider.contracts.series import StoreResult
from marketdata_provider.contracts.timeframe import Timeframe


@runtime_checkable
class MarketDataProvider(Protocol):
    def fetch_bars(self, query: BarQuery) -> DataSnapshotV2: ...


class FootprintProvider(Protocol):
    def fetch_footprint(self, query: FootprintQuery) -> FootprintSeries: ...


@runtime_checkable
class CandleStore(Protocol):
    def read(self, query: BarQuery) -> DataSnapshotV2: ...

    def write(self, snapshot: DataSnapshotV2) -> StoreResult: ...

    def coverage(self, query: BarQuery) -> Mapping[str, Any]: ...

    def latest_bar_time(self, query: BarQuery) -> int | None: ...


@runtime_checkable
class LiveKlineClient(Protocol):
    def events(
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
