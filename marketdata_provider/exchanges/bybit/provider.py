from __future__ import annotations
from marketdata_provider.config import BybitConfig
from marketdata_provider.core.bar import Bar
from marketdata_provider.errors import MDNetworkUnavailable

async def bybit_get_bars(symbol: str, timeframe: str, start: int | None, end: int | None, cfg: BybitConfig, market: str = "linear", timeout: float = 15.0, max_retries: int = 5) -> list[Bar]:
    raise MDNetworkUnavailable("Live Bybit REST provider is intentionally not enabled in Stage A; use OfflineBybitRestAdapter fixtures")

async def bybit_get_intrabar_bars(symbol: str, chart_bar: Bar, lower_timeframe: str | None, cfg: BybitConfig, market: str = "linear", timeout: float = 15.0, max_retries: int = 5) -> list[Bar]:
    raise MDNetworkUnavailable("Live Bybit intrabar REST provider is intentionally not enabled in Stage A")
