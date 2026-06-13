from __future__ import annotations
from collections.abc import Sequence
from typing import Protocol, runtime_checkable
from marketdata_provider.core.bar import Bar


@runtime_checkable
class DataProvider(Protocol):
    """Canonical market data provider protocol.

    start_ms is inclusive and end_ms is exclusive. Implementations may
    expose compatibility keyword aliases or optional max_bars arguments, but
    product contracts should prefer explicit bounded windows.
    """

    def get_bars(
        self,
        symbol: str,
        timeframe: str,
        start_ms: int | None,
        end_ms: int | None,
        *,
        max_bars: int | None = None,
    ) -> Sequence[Bar]: ...


@runtime_checkable
class IntrabarDataProvider(Protocol):
    def get_intrabar_bars(
        self,
        symbol: str,
        chart_bar: Bar,
        lower_timeframe: str | None = None,
        *,
        max_bars: int | None = None,
    ) -> Sequence[Bar]: ...


@runtime_checkable
class HistoricalDataProvider(DataProvider, Protocol):
    def get_bars_before(
        self,
        symbol: str,
        timeframe: str,
        before_ms: int,
        limit: int,
    ) -> Sequence[Bar]: ...


@runtime_checkable
class LowerTimeframeDataProvider(DataProvider, Protocol):
    def get_lower_tf_bars(
        self,
        symbol: str,
        lower_timeframe: str,
        start_ms: int,
        end_ms: int,
    ) -> Sequence[Bar]: ...
