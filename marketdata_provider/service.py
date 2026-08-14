from __future__ import annotations

from collections.abc import Iterable
from dataclasses import replace
from datetime import datetime, timezone
import threading
from typing import Protocol

from marketdata_provider._adapters import series_from_market_bars
from marketdata_provider.config import MarketDataConfig
from marketdata_provider.contracts.errors import CoverageValidationError
from marketdata_provider.contracts.query import BarQuery
from marketdata_provider.contracts.series import BarSeries
from marketdata_provider.contracts.timeframe import Timeframe, parse_timeframe
from marketdata_provider.core.bar import Bar, MarketBar
from marketdata_provider.errors import MDUnsupportedFeature
from marketdata_provider.exchanges.binance.archive import fetch_binance_archive_bars
from marketdata_provider.exchanges.binance.provider import binance_get_bars_sync
from marketdata_provider.exchanges.bybit.provider import bybit_get_bars_sync
from marketdata_provider.exchanges.public_spot import (
    SUPPORTED_PUBLIC_MARKET_EXCHANGES,
    public_market_get_bars_sync,
    public_spot_get_bars_sync,
)
from marketdata_provider.store.candle_store import CandleStore
from marketdata_provider.timeframes import close_time_ms


class HistoricalSource(Protocol):
    def fetch(self, query: BarQuery) -> list[MarketBar]: ...


class BinanceArchiveSource:
    def __init__(self, config: MarketDataConfig):
        self.config = config

    def fetch(self, query: BarQuery, progress_callback=None) -> list[MarketBar]:
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
            progress_callback=progress_callback,
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
        return [
            _market_bar_from_core(bar, query=query, source_transport="rest")
            for bar in bars
        ]


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
        return [
            _market_bar_from_core(bar, query=query, source_transport="rest")
            for bar in bars
        ]


class PublicMarketRestSource:
    def __init__(self, config: MarketDataConfig):
        self.config = config

    def fetch(self, query: BarQuery) -> list[MarketBar]:
        if query.instrument.market == "spot":
            bars = public_spot_get_bars_sync(
                exchange=query.instrument.exchange,
                symbol=query.instrument.symbol,
                timeframe=query.timeframe.canonical,
                start=query.start_ms,
                end=query.end_ms,
                user_agent=self.config.binance.user_agent,
                include_open_candle=self.config.include_open_candle,
            )
        else:
            bars = public_market_get_bars_sync(
                exchange=query.instrument.exchange,
                market=query.instrument.market,
                symbol=query.instrument.symbol,
                timeframe=query.timeframe.canonical,
                start=query.start_ms,
                end=query.end_ms,
                user_agent=self.config.binance.user_agent,
                include_open_candle=self.config.include_open_candle,
            )
        return [
            _market_bar_from_core(bar, query=query, source_transport="rest")
            for bar in bars
        ]


class MarketDataService:
    """Canonical stored market data pipeline.

    Public provider calls flow through this service so exchange-specific REST
    and archive details stay in source adapters, while cache, coverage and
    aggregation behavior remain shared.
    """

    def __init__(self, config: MarketDataConfig):
        self.config = config
        self.store = CandleStore(config.storage.cache_dir)
        self._flight_guard = threading.Lock()
        self._series_flights: dict[tuple[str, str, str, str], threading.Lock] = {}

    def fetch_bars(self, query: BarQuery, progress_callback=None) -> BarSeries:
        base_query = self._base_query(query)
        if base_query.timeframe == query.timeframe:
            bars = self._stored_bars(base_query)
            if _coverage_complete(bars, query):
                return series_from_market_bars(query, bars, source="storage")
            self._ensure_stored(base_query, progress_callback=progress_callback)
            bars = self._stored_bars(base_query)
            if bars and not _coverage_complete(bars, query):
                raise CoverageValidationError(
                    "Stored/provider bars do not cover every requested timestamp"
                )
            return series_from_market_bars(query, bars, source="storage")

        derived = self._stored_bars(query)
        if _coverage_complete(derived, query):
            return series_from_market_bars(query, derived, source="storage")

        self._ensure_stored(base_query, progress_callback=progress_callback)
        derived = self._stored_bars(query)
        if _coverage_complete(derived, query):
            return series_from_market_bars(query, derived, source="storage")

        derived = self._aggregate_stored_base(base_query, query)
        if derived:
            self._merge_derived_bars(query, derived)
            derived = self._stored_bars(query)
            if not _coverage_complete(derived, query):
                raise CoverageValidationError(
                    "Derived bars do not cover every requested timestamp"
                )
            return series_from_market_bars(query, derived, source="storage")
        return series_from_market_bars(query, [], source="storage")

    def precompute_bars(self, query: BarQuery) -> BarSeries:
        """Materialize derived bars before a batch run."""

        return self.fetch_bars(query)

    def materialize_bars(self, query: BarQuery) -> dict[str, object]:
        """Ensure requested bars exist without returning the full series."""

        base_query = self._base_query(query)
        if self._stored_coverage_complete(query):
            return {
                "ok": True,
                "span_ok": self._stored_span_complete(query),
                "changed": False,
                "bars_returned": 0,
            }
        if base_query.timeframe == query.timeframe:
            changed = self._ensure_stored(base_query)
            return {
                "ok": self._stored_coverage_complete(query),
                "span_ok": self._stored_span_complete(query),
                "changed": changed,
                "bars_returned": 0,
            }
        base_changed = self._ensure_stored(base_query)
        if self._stored_coverage_complete(query):
            return {
                "ok": True,
                "span_ok": self._stored_span_complete(query),
                "changed": base_changed,
                "bars_returned": 0,
            }
        if not base_changed and self._stored_span_complete(query):
            return {
                "ok": self._stored_coverage_complete(query),
                "span_ok": True,
                "changed": False,
                "bars_returned": 0,
            }
        derived = self._aggregate_stored_base(base_query, query)
        if derived:
            self._merge_derived_bars(query, derived)
        return {
            "ok": self._stored_coverage_complete(query),
            "span_ok": self._stored_span_complete(query),
            "changed": bool(derived),
            "bars_returned": 0,
            "rows_written": len(derived),
        }

    def _merge_derived_bars(self, query: BarQuery, derived: list[MarketBar]) -> None:
        """Merge derived bars under one series lock to prevent lost updates."""

        if not derived:
            return
        source_kind = derived[0].source_kind
        if any(bar.source_kind != source_kind for bar in derived):
            raise CoverageValidationError("Derived bars cross source-kind boundaries")
        exchange = query.instrument.exchange
        market = query.instrument.market
        symbol = query.instrument.symbol
        timeframe = query.timeframe.canonical
        with self.store.segments.series_writer_lock(
            exchange=exchange,
            market=market,
            symbol=symbol,
            timeframe=timeframe,
            source_kind=source_kind,
        ):
            existing = self.store.segments._read_all_locked(
                exchange=exchange,
                market=market,
                symbol=symbol,
                timeframe=timeframe,
                source_kind=source_kind,
            )
            by_time = {bar.time: bar for bar in existing}
            for bar in derived:
                by_time[bar.time] = bar
            self.store.segments._replace_all_locked(
                [by_time[item] for item in sorted(by_time)],
                exchange=exchange,
                market=market,
                symbol=symbol,
                timeframe=timeframe,
                source_kind=source_kind,
            )

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

    def _ensure_stored(self, query: BarQuery, progress_callback=None) -> bool:
        key = (
            query.instrument.exchange,
            query.instrument.market,
            query.instrument.symbol,
            query.timeframe.canonical,
        )
        guard = getattr(self, "_flight_guard", None)
        if guard is None:
            guard = threading.Lock()
            self._flight_guard = guard
            self._series_flights = {}
        with guard:
            flight = self._series_flights.setdefault(key, threading.Lock())
        with flight:
            return self._ensure_stored_locked(
                query, progress_callback=progress_callback
            )

    def _ensure_stored_locked(self, query: BarQuery, progress_callback=None) -> bool:
        if self._stored_coverage_complete(query):
            return False
        manifest = self.store.segments.manifest_for(
            exchange=query.instrument.exchange,
            market=query.instrument.market,
            symbol=query.instrument.symbol,
            timeframe=query.timeframe.canonical,
        )
        duration = query.timeframe.duration_ms
        if (
            manifest is not None
            and manifest.end_time is not None
            and duration is not None
        ):
            missing_start = max(query.start_ms, manifest.end_time + duration)
            if missing_start < query.end_ms:
                fetched_tail = self._fetch_from_sources(
                    replace(query, start_ms=missing_start),
                    progress_callback=progress_callback,
                )
                if fetched_tail:
                    self._append_stream(query, fetched_tail)
                    return True
        current = self._stored_bars(query)
        if _coverage_complete(current, query):
            return False
        fetched = self._fetch_from_sources(query, progress_callback=progress_callback)
        if not fetched:
            return False
        key = {
            "exchange": query.instrument.exchange,
            "market": query.instrument.market,
            "symbol": query.instrument.symbol,
            "timeframe": query.timeframe.canonical,
        }
        with self.store.segments.series_writer_lock(
            exchange=key["exchange"],
            market=key["market"],
            symbol=key["symbol"],
            timeframe=key["timeframe"],
        ):
            current = self.store.segments._read_all_locked(
                exchange=key["exchange"],
                market=key["market"],
                symbol=key["symbol"],
                timeframe=key["timeframe"],
            )
            by_time = {bar.time: bar for bar in current}
            for bar in fetched:
                by_time[bar.time] = bar
            self.store.segments._replace_all_locked(
                [by_time[item] for item in sorted(by_time)],
                exchange=key["exchange"],
                market=key["market"],
                symbol=key["symbol"],
                timeframe=key["timeframe"],
            )
        return True

    def _append_stream(self, query: BarQuery, fetched_tail: list[MarketBar]) -> None:
        self.store.segments.append_strictly_newer(
            fetched_tail,
            exchange=query.instrument.exchange,
            market=query.instrument.market,
            symbol=query.instrument.symbol,
            timeframe=query.timeframe.canonical,
        )

    def _stored_span_complete(self, query: BarQuery) -> bool:
        manifest = self.store.segments.manifest_for(
            exchange=query.instrument.exchange,
            market=query.instrument.market,
            symbol=query.instrument.symbol,
            timeframe=query.timeframe.canonical,
        )
        return self._manifest_spans(manifest, query, query.timeframe.duration_ms)

    def _stored_coverage_complete(self, query: BarQuery) -> bool:
        duration = query.timeframe.duration_ms
        if duration is None:
            return False
        expected = query.start_ms
        for item in self.store.segments.iter_all(
            exchange=query.instrument.exchange,
            market=query.instrument.market,
            symbol=query.instrument.symbol,
            timeframe=query.timeframe.canonical,
            start=query.start_ms,
            end=query.end_ms,
        ):
            if item.time != expected:
                return False
            expected += duration
        return expected >= query.end_ms

    @staticmethod
    def _manifest_spans(
        manifest: object, query: BarQuery, duration: int | None
    ) -> bool:
        start_time = getattr(manifest, "start_time", None)
        end_time = getattr(manifest, "end_time", None)
        return (
            duration is not None
            and isinstance(start_time, int)
            and isinstance(end_time, int)
            and start_time <= query.start_ms
            and end_time >= query.end_ms - duration
        )

    def _aggregate_stored_base(
        self, base_query: BarQuery, query: BarQuery
    ) -> list[MarketBar]:
        base_bars = self.store.segments.iter_all(
            exchange=base_query.instrument.exchange,
            market=base_query.instrument.market,
            symbol=base_query.instrument.symbol,
            timeframe=base_query.timeframe.canonical,
            start=query.start_ms,
            end=query.end_ms,
        )
        return _aggregate_market_bars(base_bars, query=query)

    def _fetch_from_sources(
        self, query: BarQuery, progress_callback=None
    ) -> list[MarketBar]:
        if query.instrument.exchange == "binance":
            if self.config.history.archive_first:
                archive = BinanceArchiveSource(self.config).fetch(
                    query, progress_callback=progress_callback
                )
                rest_query = _remaining_recent_query(query, archive, self.config)
                if rest_query is None:
                    return archive
                return _merge_bars(
                    archive, BinanceRestSource(self.config).fetch(rest_query)
                )
            return BinanceRestSource(self.config).fetch(query)
        if query.instrument.exchange == "bybit":
            return BybitRestSource(self.config).fetch(query)
        if query.instrument.exchange in SUPPORTED_PUBLIC_MARKET_EXCHANGES:
            return PublicMarketRestSource(self.config).fetch(query)
        raise MDUnsupportedFeature(
            f"Unsupported provider exchange: {query.instrument.exchange}"
        )


def _market_bar_from_core(
    bar: Bar, *, query: BarQuery, source_transport: str
) -> MarketBar:
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


def _aggregate_market_bars(
    bars: Iterable[MarketBar], *, query: BarQuery
) -> list[MarketBar]:
    duration = query.timeframe.duration_ms
    if duration is None:
        raise MDUnsupportedFeature(
            f"Aggregation unsupported for timeframe: {query.timeframe.canonical}"
        )
    out: list[MarketBar] = []
    current_bucket_time: int | None = None
    current_bucket: list[MarketBar] = []
    for bar in bars:
        bucket_time = (bar.time // duration) * duration
        if not (query.start_ms <= bucket_time < query.end_ms):
            continue
        if current_bucket_time is None:
            current_bucket_time = bucket_time
        if bucket_time != current_bucket_time:
            out.append(
                _aggregate_bucket(current_bucket_time, current_bucket, query=query)
            )
            current_bucket_time = bucket_time
            current_bucket = []
        current_bucket.append(bar)
    if current_bucket_time is not None and current_bucket:
        out.append(_aggregate_bucket(current_bucket_time, current_bucket, query=query))
    return out


def _aggregate_bucket(
    bucket_time: int, bucket: list[MarketBar], *, query: BarQuery
) -> MarketBar:
    traded = [bar for bar in bucket if bar.volume > 0]
    price_bucket = traded or bucket
    return MarketBar(
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
