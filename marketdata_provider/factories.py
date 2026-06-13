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
from marketdata_provider.contracts.protocols import (
    LiveKlineClient as LiveKlineClientProtocol,
)
from marketdata_provider.contracts.protocols import (
    MarketDataProvider as MarketDataProviderProtocol,
)
from marketdata_provider.contracts.protocols import (
    FootprintProvider as FootprintProviderProtocol,
)
from marketdata_provider.contracts.query import BarQuery
from marketdata_provider.contracts.footprint import FootprintQuery, FootprintSeries
from marketdata_provider.contracts.series import BarSeries, CoverageReport, StoreResult
from marketdata_provider.contracts.timeframe import Timeframe, parse_timeframe
from marketdata_provider.core.bar import MarketBar
from marketdata_provider.errors import MDUnsupportedFeature
from marketdata_provider.exchanges.registry import list_exchanges
from marketdata_provider.providers.offline import OfflineDataProvider
from marketdata_provider.service import MarketDataService
from marketdata_provider.footprint.service import FootprintService
from marketdata_provider.store.candle_store import CandleStore as SegmentCandleStore


_NATIVE_EXCHANGE_IDS = {exchange.id for exchange in list_exchanges(native_only=True)}


def create_provider(config: MarketDataConfig) -> MarketDataProviderProtocol:
    """Create a canonical market-data provider from local package config."""

    if config.offline.root is not None:
        return _OfflineProviderAdapter(config.offline.root)
    return _ExchangeProviderAdapter(config)


def create_footprint_provider(config: MarketDataConfig) -> FootprintProviderProtocol:
    """Create the raw-trade footprint provider."""

    return _FootprintProviderAdapter(config)


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
    return _LiveKlineClientAdapter(
        raw_client, instrument=instrument, timeframe=timeframe
    )


class _ExchangeProviderAdapter:
    def __init__(self, config: MarketDataConfig):
        self.config = config
        self.service = MarketDataService(config)

    def fetch_bars(self, query: BarQuery) -> BarSeries:
        exchange = (self.config.default_exchange or query.instrument.exchange).lower()
        if exchange not in _NATIVE_EXCHANGE_IDS:
            raise MDUnsupportedFeature(f"Unsupported provider exchange: {exchange}")
        return self.service.fetch_bars(query)


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


class _FootprintProviderAdapter:
    def __init__(self, config: MarketDataConfig):
        self.service = FootprintService(config)

    def fetch_footprint(self, query: FootprintQuery) -> FootprintSeries:
        return self.service.fetch_footprint(query)


class _CandleStoreAdapter:
    def __init__(self, store: SegmentCandleStore):
        self.store = store

    def read(self, query: BarQuery) -> BarSeries:
        bars = tuple(
            self.store.get_market_bars(
                exchange=query.instrument.exchange,
                market=query.instrument.market,
                symbol=query.instrument.symbol,
                timeframe=query.timeframe.canonical,
                start=query.start_ms,
                end=query.end_ms,
            )
        )
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
            market_bars = [contract_to_market_bar(bar) for bar in series.bars]
            if _can_bulk_write_closed(market_bars):
                rows_written = self._bulk_write_closed(market_bars)
            else:
                for market_bar in market_bars:
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

    def latest_bar_time(self, query: BarQuery) -> int | None:
        if hasattr(self.store, "latest_bar_time"):
            return self.store.latest_bar_time(
                exchange=query.instrument.exchange,
                market=query.instrument.market,
                symbol=query.instrument.symbol,
                timeframe=query.timeframe.canonical,
            )
        coverage = self.coverage(query)
        return coverage.delivered_end_ms

    def _bulk_write_closed(self, bars: list[MarketBar]) -> int:
        first = bars[0]
        existing = self.store.segments.read_all(
            exchange=first.exchange,
            market=first.market,
            symbol=first.symbol,
            timeframe=first.timeframe,
            source_kind=first.source_kind,
        )
        by_time = {bar.time: bar for bar in existing}
        rows_written = 0
        for bar in bars:
            current = by_time.get(bar.time)
            if current is not None and not _same_candle_payload(current, bar):
                raise ValueError(f"conflicting closed candle at {bar.time}")
            if current is None:
                rows_written += 1
            by_time[bar.time] = bar
        self.store.segments.replace_all(
            list(by_time.values()),
            exchange=first.exchange,
            market=first.market,
            symbol=first.symbol,
            timeframe=first.timeframe,
            source_kind=first.source_kind,
        )
        return rows_written


def _same_candle_payload(left: MarketBar, right: MarketBar) -> bool:
    """Return true when the candle data is identical regardless of provenance."""

    return (
        left.time == right.time
        and left.time_close == right.time_close
        and left.open == right.open
        and left.high == right.high
        and left.low == right.low
        and left.close == right.close
        and left.volume == right.volume
        and left.exchange.lower() == right.exchange.lower()
        and left.market.lower() == right.market.lower()
        and left.symbol.upper() == right.symbol.upper()
        and parse_timeframe(left.timeframe) == parse_timeframe(right.timeframe)
        and left.source_kind == right.source_kind
        and left.is_closed == right.is_closed
        and left.quote_volume == right.quote_volume
        and left.turnover == right.turnover
        and left.trades_count == right.trades_count
    )


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


def _can_bulk_write_closed(bars: list[MarketBar]) -> bool:
    if not bars or not all(bar.is_closed for bar in bars):
        return False
    first = bars[0]
    return all(
        bar.exchange == first.exchange
        and bar.market == first.market
        and bar.symbol == first.symbol
        and bar.timeframe == first.timeframe
        and bar.source_kind == first.source_kind
        for bar in bars
    )


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
        async for event in self.raw_client.events(
            max_messages=max_messages, timeout_s=timeout_s
        ):
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
