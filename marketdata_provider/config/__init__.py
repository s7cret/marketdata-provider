from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from marketdata_provider.core.bar import RUNTIME_CONTRACT_VERSION
from marketdata_provider.symbols import DEFAULT_STABLE_QUOTE_ASSETS


@dataclass(frozen=True, slots=True)
class BinanceConfig:
    spot_base_url: str = "https://api.binance.com"
    usdm_base_url: str = "https://fapi.binance.com"
    coinm_base_url: str = "https://dapi.binance.com"
    max_limit_spot: int = 1000
    max_limit_usdm: int = 1500
    user_agent: str = "pinelib-marketdata/0.1"


@dataclass(frozen=True, slots=True)
class BybitConfig:
    base_url: str = "https://api.bybit.com"
    max_limit: int = 1000
    user_agent: str = "pinelib-marketdata/0.1"


@dataclass(frozen=True, slots=True)
class StreamingConfig:
    enabled: bool = False
    reconnect_backoff_sec: float = 1.0
    checkpoint_dir: Path | None = None


@dataclass(frozen=True, slots=True)
class StorageConfig:
    cache_dir: Path = Path(".marketdata-cache")
    parquet_enabled: bool = False
    sqlite_metadata: bool = False


@dataclass(frozen=True, slots=True)
class HistoryConfig:
    enabled: bool = True
    archive_first: bool = True
    base_timeframe: str = "1m"
    recent_lag_days: int = 2


@dataclass(frozen=True, slots=True)
class OfflineDataConfig:
    root: Path | None = None
    assume_utc: bool = True


@dataclass(frozen=True, slots=True)
class SymbolDiscoveryConfig:
    stable_quotes_only: bool = True
    stable_quote_assets: tuple[str, ...] = DEFAULT_STABLE_QUOTE_ASSETS
    max_results: int = 50


@dataclass(frozen=True, slots=True)
class MarketDataConfig:
    runtime_contract_version: str = RUNTIME_CONTRACT_VERSION
    include_open_candle: bool = False
    default_exchange: str | None = None
    default_market: str | None = None
    binance: BinanceConfig = field(default_factory=BinanceConfig)
    bybit: BybitConfig = field(default_factory=BybitConfig)
    history: HistoryConfig = field(default_factory=HistoryConfig)
    streaming: StreamingConfig = field(default_factory=StreamingConfig)
    storage: StorageConfig = field(default_factory=StorageConfig)
    offline: OfflineDataConfig = field(default_factory=OfflineDataConfig)
    symbols: SymbolDiscoveryConfig = field(default_factory=SymbolDiscoveryConfig)
