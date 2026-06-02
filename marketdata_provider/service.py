from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from typing import Protocol

from marketdata_provider._adapters import series_from_market_bars
from marketdata_provider.config import MarketDataConfig
from marketdata_provider.contracts.query import BarQuery
from marketdata_provider.contracts.series import BarSeries
from marketdata_provider.contracts.timeframe import Timeframe, parse_timeframe
from marketdata_provider.core.bar import Bar, MarketBar
from marketdata_provider.errors import MDUnsupportedFeature
from marketdata_provider.exchanges.binance.archive import fetch_binance_archive_bars
from marketdata_provider.exchanges.binance.provider import binance_get_bars_sync
from marketdata_provider.exchanges.bybit.provider import bybit_get_bars_sync
from marketdata_provider.store.candle_store import CandleStore
from marketdata_provider.timeframes import close_time_ms


class HistoricalSource(Protocol):
    def fetch(self, query: BarQuery) -> list[MarketBar]: ...


class BinanceArchiveSource:
    def __init__(self, config: MarketDataConfig):
        self.config = config

    def fetch(self, query: BarQuery) -> list[MarketBar]:
        if query.start_ms >= _archive_cutoff_ms(self.config):
            return []
        end = min(query.end_ms, _archive_cutoff_ms(self.config))
        bars = fetch_binance_archive_bars(
            symbol=query.instrument.symbol,
            market=query.instrument.market,
            timeframe=query.timeframe.canonical,
            start=query.start_ms,
            end=end,
            cache_dir=self.config.storage.cache_dir,
        )
        return [
            _market_bar_from_core(
                bar,
                query=query,
                source_transport="archive",
            )
            for bar in bars
        ]


class BinanceRestSource:
    def __init__(self, config: MarketDataConfig):
        self.config = config

    def fetch(self, query: BarQuery) -> list[MarketBar]:
        bars = binance_get_bars_sync(
            query.instrument.symbol,
            query.timeframe.canonical,
            query.start_ms,
            query.end_ms,
            self.config.binance,
            market=query.instrument.market,
            include_open_candle=self.config.include_open_candle,
        )
        return [_market_bar_from_core(bar, query=query, source_transport="rest") for bar in bars]


class BybitRestSource:
    def __init__(self, config: MarketDataConfig):
        self.config = config

    def fetch(self, query: BarQuery) -> list[MarketBar]:
        bars = bybit_get_bars_sync(
            query.instrument.symbol,
            query.timeframe.canonical,
            query.start_ms,
            query.end_ms,
            self.config.bybit,
            market=query.instrument.market,
            include_open_candle=self.config.include_open_candle,
        )
        return [_market_bar_from_core(bar, query=query, source_transport="rest") for bar in bars]


class MarketDataService:
    """Canonical stored market data pipeline.

    Public provider calls flow through this service so exchange-specific REST
    and archive details stay in source adapters, while cache, coverage and
    aggregation behavior remain shared.
    """

    def __init__(self, config: MarketDataConfig):
        self.config = config
        self.store = CandleStore(config.storage.cache_dir)

    def fetch_bars(self, query: BarQuery) -> BarSeries:
        base_query = self._base_query(query)
        if base_query.timeframe == query.timeframe:
            self._ensure_stored(base_query)
            bars = self._stored_bars(base_query)
            return series_from_market_bars(query, bars, source="storage")

        base_changed = self._ensure_stored(base_query)
        derived = self._stored_bars(query)
        if not base_changed and _coverage_complete(derived, query):
            return series_from_market_bars(query, derived, source="storage")

        bars = self._stored_bars(base_query)
        derived = _aggregate_market_bars(bars, query=query)
        if derived:
            existing = self.store.get_market_bars(
                exchange=query.instrument.exchange,
                market=query.instrument.market,
                symbol=query.instrument.symbol,
                timeframe=query.timeframe.canonical,
            )
            by_time = {bar.time: bar for bar in existing}
            for bar in derived:
                by_time[bar.time] = bar
            self.store.segments.replace_all(
                list(by_time.values()),
                exchange=query.instrument.exchange,
                market=query.instrument.market,
                symbol=query.instrument.symbol,
                timeframe=query.timeframe.canonical,
            )
            derived = self._stored_bars(query)
            return series_from_market_bars(query, derived, source="storage")
        return series_from_market_bars(query, derived, source="storage")

    def _base_query(self, query: BarQuery) -> BarQuery:
        if not self.config.history.enabled:
            return query
        if query.instrument.exchange != "binance":
            return query
        base_timeframe = parse_timeframe(self.config.history.base_timeframe)
        if _can_derive_from_base(query, base_timeframe):
            return replace(query, timeframe=base_timeframe)
        return query

    def _stored_bars(self, query: BarQuery) -> list[MarketBar]:
        return self.store.get_market_bars(
            exchange=query.instrument.exchange,
            market=query.instrument.market,
            symbol=query.instrument.symbol,
            timeframe=query.timeframe.canonical,
            start=query.start_ms,
            end=query.end_ms,
        )

    def _ensure_stored(self, query: BarQuery) -> bool:
        current = self._stored_bars(query)
        if _coverage_complete(current, query):
            return False
        fetched = self._fetch_from_sources(query)
        if not fetched:
            return False
        by_time = {bar.time: bar for bar in current}
        for bar in fetched:
            by_time[bar.time] = bar
        self.store.segments.replace_all(
            list(by_time.values()),
            exchange=query.instrument.exchange,
            market=query.instrument.market,
            symbol=query.instrument.symbol,
            timeframe=query.timeframe.canonical,
        )
        return True

    def _fetch_from_sources(self, query: BarQuery) -> list[MarketBar]:
        if query.instrument.exchange == "binance":
            if self.config.history.archive_first:
                archive = BinanceArchiveSource(self.config).fetch(query)
                rest_query = _remaining_recent_query(query, archive, self.config)
                if rest_query is None:
                    return archive
                return _merge_bars(archive, BinanceRestSource(self.config).fetch(rest_query))
            return BinanceRestSource(self.config).fetch(query)
        if query.instrument.exchange == "bybit":
            return BybitRestSource(self.config).fetch(query)
        raise MDUnsupportedFeature(f"Unsupported provider exchange: {query.instrument.exchange}")


def _market_bar_from_core(bar: Bar, *, query: BarQuery, source_transport: str) -> MarketBar:
    return MarketBar(
        time=bar.time,
        open=bar.open,
        high=bar.high,
        low=bar.low,
        close=bar.close,
        volume=bar.volume,
        time_close=bar.time_close or close_time_ms(bar.time, query.timeframe.canonical),
        exchange=query.instrument.exchange,
        market=query.instrument.market,
        symbol=query.instrument.symbol,
        timeframe=query.timeframe.canonical,
        source_transport=source_transport,
        source_kind="trade_kline",
        is_closed=True,
    )


def _archive_cutoff_ms(config: MarketDataConfig) -> int:
    days = max(config.history.recent_lag_days, 0)
    now = datetime.now(timezone.utc)
    today_start = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
    return int(today_start.timestamp() * 1000) - days * 86_400_000


def _coverage_complete(bars: list[MarketBar], query: BarQuery) -> bool:
    duration = query.timeframe.duration_ms
    if duration is None:
        return bool(bars)
    present = {bar.time for bar in bars}
    return all(ts in present for ts in range(query.start_ms, query.end_ms, duration))


def _can_derive_from_base(query: BarQuery, base_timeframe: Timeframe) -> bool:
    query_duration = query.timeframe.duration_ms
    base_duration = base_timeframe.duration_ms
    if query_duration is None or base_duration is None:
        return False
    if base_duration > query_duration:
        return False
    return query_duration % base_duration == 0


def _remaining_recent_query(
    query: BarQuery,
    archive_bars: list[MarketBar],
    config: MarketDataConfig,
) -> BarQuery | None:
    cutoff = _archive_cutoff_ms(config)
    start = max(query.start_ms, cutoff)
    if start >= query.end_ms:
        return None
    duration = query.timeframe.duration_ms
    if duration is None:
        return replace(query, start_ms=start)
    if archive_bars and archive_bars[-1].time + duration > start:
        start = archive_bars[-1].time + duration
    if start >= query.end_ms:
        return None
    return replace(query, start_ms=start)


def _merge_bars(*groups: list[MarketBar]) -> list[MarketBar]:
    by_time: dict[int, MarketBar] = {}
    for bars in groups:
        for bar in bars:
            by_time[bar.time] = bar
    return [by_time[t] for t in sorted(by_time)]


def _aggregate_market_bars(bars: list[MarketBar], *, query: BarQuery) -> list[MarketBar]:
    duration = query.timeframe.duration_ms
    if duration is None:
        raise MDUnsupportedFeature(f"Aggregation unsupported for timeframe: {query.timeframe.canonical}")
    buckets: dict[int, list[MarketBar]] = {}
    for bar in bars:
        bucket_time = (bar.time // duration) * duration
        if query.start_ms <= bucket_time < query.end_ms:
            buckets.setdefault(bucket_time, []).append(bar)
    out: list[MarketBar] = []
    for bucket_time in sorted(buckets):
        bucket = sorted(buckets[bucket_time], key=lambda item: item.time)
        traded = [bar for bar in bucket if bar.volume > 0]
        price_bucket = traded or bucket
        out.append(
            MarketBar(
                time=bucket_time,
                open=price_bucket[0].open,
                high=max(bar.high for bar in bucket),
                low=min(bar.low for bar in bucket),
                close=price_bucket[-1].close,
                volume=sum(bar.volume for bar in bucket),
                time_close=close_time_ms(bucket_time, query.timeframe.canonical),
                exchange=query.instrument.exchange,
                market=query.instrument.market,
                symbol=query.instrument.symbol,
                timeframe=query.timeframe.canonical,
                source_transport="derived",
                source_kind="trade_kline",
                is_closed=True,
            )
        )
    return out
