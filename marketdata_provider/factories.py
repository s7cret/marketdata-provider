from __future__ import annotations

from pathlib import Path
from typing import AsyncIterator

from marketdata_provider.config import MarketDataConfig
from marketdata_provider._adapters import contract_to_market_bar
from marketdata_provider._adapters import core_to_contract_bar
from marketdata_provider._adapters import series_from_core_bars
from marketdata_provider._adapters import series_from_market_bars
from marketdata_provider.contracts.events import LiveKlineEvent
from marketdata_provider.contracts.errors import CoverageValidationError
from marketdata_provider.contracts.instrument import InstrumentKey
from marketdata_provider.contracts.protocols import CandleStore as CandleStoreProtocol
from marketdata_provider.contracts.protocols import LiveKlineClient as LiveKlineClientProtocol
from marketdata_provider.contracts.protocols import MarketDataProvider as MarketDataProviderProtocol
from marketdata_provider.contracts.query import BarQuery
from marketdata_provider.contracts.series import BarSeries, CoverageReport, StoreResult
from marketdata_provider.contracts.timeframe import Timeframe, parse_timeframe
from marketdata_provider.core.bar import MarketBar
from marketdata_provider.errors import MDUnsupportedFeature
from marketdata_provider.exchanges.binance.provider import binance_get_bars_sync
from marketdata_provider.exchanges.bybit.provider import bybit_get_bars_sync
from marketdata_provider.providers.offline import OfflineDataProvider
from marketdata_provider.store.candle_store import CandleStore as SegmentCandleStore


def create_provider(config: MarketDataConfig) -> MarketDataProviderProtocol:
    """Create a canonical market-data provider from local package config."""

    if config.offline.root is not None:
        return _OfflineProviderAdapter(config.offline.root)
    return _ExchangeProviderAdapter(config)


def create_candle_store(config: MarketDataConfig) -> CandleStoreProtocol:
    """Create a canonical candle store from local package config."""

    return _CandleStoreAdapter(SegmentCandleStore(config.storage.cache_dir))


def create_live_kline_client(
    config: MarketDataConfig,
    *,
    instrument: InstrumentKey,
    timeframe: Timeframe,
) -> LiveKlineClientProtocol:
    """Create a canonical public kline stream client.

    Consumers depend on this factory/protocol instead of importing streaming
    implementation modules directly.
    """

    from marketdata_provider.streaming import PublicKlineWebSocketClient

    exchange = (config.default_exchange or instrument.exchange).lower()
    market = (config.default_market or instrument.market).lower()
    raw_client = PublicKlineWebSocketClient(
        exchange=exchange,  # type: ignore[arg-type]
        market=market,
        symbol=instrument.symbol,
        timeframe=timeframe.canonical,
    )
    return _LiveKlineClientAdapter(raw_client, instrument=instrument, timeframe=timeframe)


class _ExchangeProviderAdapter:
    def __init__(self, config: MarketDataConfig):
        self.config = config

    def fetch_bars(self, query: BarQuery) -> BarSeries:
        exchange = (self.config.default_exchange or query.instrument.exchange).lower()
        market = self.config.default_market or query.instrument.market
        if exchange == "binance":
            bars = binance_get_bars_sync(
                query.instrument.symbol,
                query.timeframe.canonical,
                query.start_ms,
                query.end_ms,
                self.config.binance,
                market=market,
                include_open_candle=self.config.include_open_candle,
            )
        elif exchange == "bybit":
            bars = bybit_get_bars_sync(
                query.instrument.symbol,
                query.timeframe.canonical,
                query.start_ms,
                query.end_ms,
                self.config.bybit,
                market=market,
                include_open_candle=self.config.include_open_candle,
            )
        else:
            raise MDUnsupportedFeature(f"Unsupported provider exchange: {exchange}")
        return series_from_core_bars(query, bars, source="provider")


class _OfflineProviderAdapter:
    def __init__(self, root: str | Path):
        self.provider = OfflineDataProvider(root)

    def fetch_bars(self, query: BarQuery) -> BarSeries:
        bars = self.provider.get_bars(
            query.instrument.symbol,
            query.timeframe.canonical,
            query.start_ms,
            query.end_ms,
        )
        return series_from_core_bars(query, bars, source="provider")


class _CandleStoreAdapter:
    def __init__(self, store: SegmentCandleStore):
        self.store = store

    def read(self, query: BarQuery) -> BarSeries:
        bars = tuple(self.store.get_market_bars(
            exchange=query.instrument.exchange,
            market=query.instrument.market,
            symbol=query.instrument.symbol,
            timeframe=query.timeframe.canonical,
            start=query.start_ms,
            end=query.end_ms,
        ))
        error = _stored_bars_read_error(query, bars)
        if error is not None:
            raise CoverageValidationError(error)
        return series_from_market_bars(query, bars, source="storage")

    def write(self, series: BarSeries) -> StoreResult:
        rows_written = 0
        try:
            error = _series_write_error(series)
            if error is not None:
                return StoreResult(success=False, rows_written=0, error=error)
            for bar in series.bars:
                market_bar = contract_to_market_bar(bar)
                if market_bar.is_closed:
                    result = self.store.commit_closed(market_bar)
                else:
                    result = self.store.upsert_open(market_bar)
                if result.status in {"committed", "upserted"}:
                    rows_written += 1
        except Exception as exc:
            return StoreResult(success=False, rows_written=rows_written, error=str(exc))
        return StoreResult(success=True, rows_written=rows_written)

    def coverage(self, query: BarQuery) -> CoverageReport:
        return self.read(query).coverage


def _series_write_error(series: BarSeries) -> str | None:
    """Reject series that would cross canonical store identity boundaries."""

    for bar in series.bars:
        if bar.instrument != series.query.instrument:
            return (
                "bar instrument does not match series query "
                f"({bar.instrument.serialize()} != {series.query.instrument.serialize()})"
            )
        if bar.timeframe != series.query.timeframe:
            return (
                "bar timeframe does not match series query "
                f"({bar.timeframe.canonical} != {series.query.timeframe.canonical})"
            )
    return None


def _stored_bars_read_error(query: BarQuery, bars: tuple[MarketBar, ...]) -> str | None:
    """Reject stored rows whose embedded identity disagrees with the read query."""

    for bar in bars:
        try:
            instrument = InstrumentKey(bar.exchange, bar.market, bar.symbol)
        except ValueError as exc:
            return f"stored bar instrument is invalid ({exc})"
        if instrument != query.instrument:
            return (
                "stored bar instrument does not match query "
                f"({instrument.serialize()} != {query.instrument.serialize()})"
            )
        try:
            timeframe = parse_timeframe(bar.timeframe)
        except ValueError as exc:
            return f"stored bar timeframe is invalid ({exc})"
        if timeframe != query.timeframe:
            return (
                "stored bar timeframe does not match query "
                f"({timeframe.canonical} != {query.timeframe.canonical})"
            )
    return None


class _LiveKlineClientAdapter:
    def __init__(self, raw_client, *, instrument: InstrumentKey, timeframe: Timeframe):
        self.raw_client = raw_client
        self.instrument = instrument
        self.timeframe = timeframe

    async def events(
        self,
        *,
        max_messages: int | None = None,
        timeout_s: float | None = None,
    ) -> AsyncIterator[LiveKlineEvent]:
        async for event in self.raw_client.events(max_messages=max_messages, timeout_s=timeout_s):
            update = event.update
            instrument = InstrumentKey(update.exchange, update.market, update.symbol)
            timeframe = parse_timeframe(update.timeframe)
            yield LiveKlineEvent(
                bar=core_to_contract_bar(instrument, timeframe, update.to_market_bar()),
                event_time=update.event_time,
                received_at=update.received_at,
                raw_payload=dict(event.raw_payload),
                diagnostic_code=event.diagnostic.code if event.diagnostic else None,
            )
