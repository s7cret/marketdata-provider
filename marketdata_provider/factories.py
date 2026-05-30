from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Iterable

from marketdata_provider.config import MarketDataConfig
from marketdata_provider.contracts.bar import Bar as ContractBar
from marketdata_provider.contracts.instrument import InstrumentKey
from marketdata_provider.contracts.protocols import CandleStore as CandleStoreProtocol
from marketdata_provider.contracts.protocols import LiveKlineClient as LiveKlineClientProtocol
from marketdata_provider.contracts.protocols import MarketDataProvider as MarketDataProviderProtocol
from marketdata_provider.contracts.query import BarQuery
from marketdata_provider.contracts.series import BarSeries, CoverageReport, StoreResult
from marketdata_provider.contracts.timeframe import Timeframe
from marketdata_provider.core.bar import Bar as CoreBar
from marketdata_provider.core.bar import MarketBar
from marketdata_provider.errors import MDUnsupportedFeature
from marketdata_provider.exchanges.binance.provider import binance_get_bars_sync
from marketdata_provider.exchanges.bybit.provider import bybit_get_bars_sync
from marketdata_provider.providers.offline import OfflineDataProvider
from marketdata_provider.store.candle_store import CandleStore as SegmentCandleStore
from marketdata_provider.timeframes import close_time_ms


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
    return PublicKlineWebSocketClient(
        exchange=exchange,  # type: ignore[arg-type]
        market=market,
        symbol=instrument.symbol,
        timeframe=timeframe.canonical,
    )


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
        return _series_from_core_bars(query, bars, source="provider")


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
        return _series_from_core_bars(query, bars, source="provider")


class _CandleStoreAdapter:
    def __init__(self, store: SegmentCandleStore):
        self.store = store

    def read(self, query: BarQuery) -> BarSeries:
        bars = self.store.get_market_bars(
            exchange=query.instrument.exchange,
            market=query.instrument.market,
            symbol=query.instrument.symbol,
            timeframe=query.timeframe.canonical,
            start=query.start_ms,
            end=query.end_ms,
        )
        return _series_from_market_bars(query, bars, source="storage")

    def write(self, series: BarSeries) -> StoreResult:
        rows_written = 0
        try:
            for bar in series.bars:
                market_bar = _contract_to_market_bar(bar)
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


def _series_from_core_bars(query: BarQuery, bars: Iterable[CoreBar], *, source: str) -> BarSeries:
    contract_bars = tuple(_core_to_contract_bar(query.instrument, query.timeframe, bar) for bar in bars)
    return BarSeries(query=query, bars=contract_bars, coverage=_coverage(query, contract_bars, source=source))


def _series_from_market_bars(query: BarQuery, bars: Iterable[MarketBar], *, source: str) -> BarSeries:
    contract_bars = tuple(
        _core_to_contract_bar(
            InstrumentKey(bar.exchange or query.instrument.exchange, bar.market or query.instrument.market, bar.symbol or query.instrument.symbol),
            query.timeframe,
            bar,
        )
        for bar in bars
    )
    return BarSeries(query=query, bars=contract_bars, coverage=_coverage(query, contract_bars, source=source))


def _core_to_contract_bar(instrument: InstrumentKey, timeframe: Timeframe, bar: CoreBar) -> ContractBar:
    time_close = bar.time_close if bar.time_close is not None else _default_time_close(bar.time, timeframe)
    return ContractBar(
        instrument=instrument,
        timeframe=timeframe,
        time=bar.time,
        time_close=time_close,
        open=bar.open,
        high=bar.high,
        low=bar.low,
        close=bar.close,
        volume=bar.volume,
        closed=getattr(bar, "is_closed", True),
    )


def _contract_to_market_bar(bar: ContractBar) -> MarketBar:
    return MarketBar(
        time=bar.time,
        open=bar.open,
        high=bar.high,
        low=bar.low,
        close=bar.close,
        volume=bar.volume or 0.0,
        time_close=bar.time_close,
        exchange=bar.instrument.exchange,
        market=bar.instrument.market,
        symbol=bar.instrument.symbol,
        timeframe=bar.timeframe.canonical,
        source_transport="api",
        source_kind="trade_kline",
        is_closed=bar.closed,
    )


def _coverage(query: BarQuery, bars: tuple[ContractBar, ...], *, source: str) -> CoverageReport:
    if not bars:
        return CoverageReport(
            requested_start_ms=query.start_ms,
            requested_end_ms=query.end_ms,
            delivered_start_ms=None,
            delivered_end_ms=None,
            missing_intervals=((query.start_ms, query.end_ms),),
            source_mix=(source,),
            status="empty",
        )

    ordered = sorted(bars, key=lambda bar: bar.time)
    counts = Counter(bar.time for bar in ordered)
    duplicate_timestamps = tuple(time for time, count in sorted(counts.items()) if count > 1)
    unordered = tuple(bars) != tuple(ordered)
    missing_intervals = _missing_intervals(query, tuple(ordered))
    status = "valid"
    if duplicate_timestamps:
        status = "duplicate"
    elif unordered:
        status = "unordered"
    elif missing_intervals:
        status = "gap"

    return CoverageReport(
        requested_start_ms=query.start_ms,
        requested_end_ms=query.end_ms,
        delivered_start_ms=ordered[0].time,
        delivered_end_ms=_bar_exclusive_end(ordered[-1], query.timeframe),
        missing_intervals=missing_intervals,
        duplicate_timestamps=duplicate_timestamps,
        source_mix=(source,),
        status=status,
    )


def _missing_intervals(query: BarQuery, bars: tuple[ContractBar, ...]) -> tuple[tuple[int, int], ...]:
    duration = query.timeframe.duration_ms
    if duration is None:
        return ()
    expected = range(query.start_ms, query.end_ms, duration)
    present = {bar.time for bar in bars if query.start_ms <= bar.time < query.end_ms}
    return tuple((start, min(start + duration, query.end_ms)) for start in expected if start not in present)


def _bar_exclusive_end(bar: ContractBar, timeframe: Timeframe) -> int:
    if timeframe.duration_ms is not None:
        return bar.time + timeframe.duration_ms
    return bar.time_close + 1


def _default_time_close(open_time_ms: int, timeframe: Timeframe) -> int:
    return close_time_ms(open_time_ms, timeframe.canonical)
