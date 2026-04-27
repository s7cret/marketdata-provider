from __future__ import annotations
from typing import Protocol, runtime_checkable
from marketdata_provider.core.bar import Bar

@runtime_checkable
class DataProvider(Protocol):
    def get_bars(self, symbol: str, timeframe: str, start: int | None, end: int | None, *, max_bars: int | None = None) -> list[Bar]: ...

@runtime_checkable
class IntrabarDataProvider(Protocol):
    def get_intrabar_bars(self, symbol: str, chart_bar: Bar, lower_timeframe: str | None = None, *, max_bars: int | None = None) -> list[Bar]: ...
