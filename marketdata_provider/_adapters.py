from __future__ import annotations

from collections import Counter
from collections.abc import Iterable

from marketdata_provider.contracts.bar import Bar as ContractBar
from marketdata_provider.contracts.instrument import InstrumentKey
from marketdata_provider.contracts.query import BarQuery
from marketdata_provider.contracts.series import (
    BarSeries,
    CoverageReport,
    CoverageStatus,
)
from marketdata_provider.contracts.timeframe import Timeframe
from marketdata_provider.core.bar import Bar as CoreBar
from marketdata_provider.core.bar import MarketBar
from marketdata_provider.timeframes import close_time_ms


def series_from_core_bars(
    query: BarQuery, bars: Iterable[CoreBar], *, source: str
) -> BarSeries:
    contract_bars = tuple(
        core_to_contract_bar(query.instrument, query.timeframe, bar) for bar in bars
    )
    return BarSeries(
        query=query,
        bars=contract_bars,
        coverage=coverage_report(query, contract_bars, source=source),
    )


def series_from_market_bars(
    query: BarQuery, bars: Iterable[MarketBar], *, source: str
) -> BarSeries:
    contract_bars = tuple(
        core_to_contract_bar(
            InstrumentKey(
                bar.exchange or query.instrument.exchange,
                bar.market or query.instrument.market,
                bar.symbol or query.instrument.symbol,
            ),
            query.timeframe,
            bar,
        )
        for bar in bars
    )
    return BarSeries(
        query=query,
        bars=contract_bars,
        coverage=coverage_report(query, contract_bars, source=source),
    )


def core_to_contract_bar(
    instrument: InstrumentKey, timeframe: Timeframe, bar: CoreBar
) -> ContractBar:
    time_close = (
        bar.time_close
        if bar.time_close is not None
        else default_time_close(bar.time, timeframe)
    )
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


def contract_to_market_bar(bar: ContractBar) -> MarketBar:
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


def coverage_report(
    query: BarQuery, bars: tuple[ContractBar, ...], *, source: str
) -> CoverageReport:
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
    duplicate_timestamps = tuple(
        time for time, count in sorted(counts.items()) if count > 1
    )
    unordered = tuple(bars) != tuple(ordered)
    missing_intervals = missing_intervals_for(query, tuple(ordered))
    status: CoverageStatus = "valid"
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
        delivered_end_ms=bar_exclusive_end(ordered[-1], query.timeframe),
        missing_intervals=missing_intervals,
        duplicate_timestamps=duplicate_timestamps,
        source_mix=(source,),
        status=status,
    )


def missing_intervals_for(
    query: BarQuery, bars: tuple[ContractBar, ...]
) -> tuple[tuple[int, int], ...]:
    duration = query.timeframe.duration_ms
    if duration is None:
        return ()
    expected = range(query.start_ms, query.end_ms, duration)
    present = {bar.time for bar in bars if query.start_ms <= bar.time < query.end_ms}
    return tuple(
        (start, min(start + duration, query.end_ms))
        for start in expected
        if start not in present
    )


def bar_exclusive_end(bar: ContractBar, timeframe: Timeframe) -> int:
    if timeframe.duration_ms is not None:
        return bar.time + timeframe.duration_ms
    return bar.time_close + 1


def default_time_close(open_time_ms: int, timeframe: Timeframe) -> int:
    return close_time_ms(open_time_ms, timeframe.canonical)
