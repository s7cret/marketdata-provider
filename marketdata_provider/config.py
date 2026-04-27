from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path
from marketdata_provider.core.bar import RUNTIME_CONTRACT_VERSION

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
class OfflineDataConfig:
    root: Path | None = None
    assume_utc: bool = True

@dataclass(frozen=True, slots=True)
class MarketDataConfig:
    runtime_contract_version: str = RUNTIME_CONTRACT_VERSION
    include_open_candle: bool = False
    default_exchange: str | None = None
    default_market: str | None = None
    binance: BinanceConfig = field(default_factory=BinanceConfig)
    bybit: BybitConfig = field(default_factory=BybitConfig)
    streaming: StreamingConfig = field(default_factory=StreamingConfig)
    storage: StorageConfig = field(default_factory=StorageConfig)
    offline: OfflineDataConfig = field(default_factory=OfflineDataConfig)
