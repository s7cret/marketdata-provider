from __future__ import annotations
from marketdata_provider.config import BinanceConfig
from marketdata_provider.core.bar import Bar
from marketdata_provider.errors import MDNetworkUnavailable

async def binance_get_bars(symbol: str, timeframe: str, start: int | None, end: int | None, cfg: BinanceConfig, market: str = "usdm", timeout: float = 15.0, max_retries: int = 5) -> list[Bar]:
    raise MDNetworkUnavailable("Live Binance REST provider is intentionally not enabled in Stage A; use OfflineBinanceRestAdapter fixtures")

async def binance_get_intrabar_bars(symbol: str, chart_bar: Bar, lower_timeframe: str | None, cfg: BinanceConfig, market: str = "usdm", timeout: float = 15.0, max_retries: int = 5) -> list[Bar]:
    raise MDNetworkUnavailable("Live Binance intrabar REST provider is intentionally not enabled in Stage A")
