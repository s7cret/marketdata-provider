from __future__ import annotations

from marketdata_provider._adapters import (
    contract_to_market_bar,
    core_to_contract_bar,
    series_from_core_bars,
    series_from_market_bars,
)
from marketdata_provider.contracts import BarQuery, InstrumentKey, parse_timeframe
from marketdata_provider.contracts.bar import Bar as ContractBar
from marketdata_provider.core.bar import Bar as CoreBar
from marketdata_provider.core.bar import MarketBar


def _query() -> BarQuery:
    return BarQuery(
        instrument=InstrumentKey("binance", "spot", "BTCUSDT"),
        timeframe=parse_timeframe("1m"),
        start_ms=0,
        end_ms=180_000,
        source="provider",
    )


def test_core_to_contract_bar_fills_missing_close_time() -> None:
    query = _query()
    core_bar = CoreBar(60_000, 1.0, 2.0, 0.5, 1.5, 10.0)

    bar = core_to_contract_bar(query.instrument, query.timeframe, core_bar)

    assert bar.instrument == query.instrument
    assert bar.timeframe == query.timeframe
    assert bar.time_close == 119_999
    assert bar.closed is True


def test_series_from_core_bars_reports_gaps_duplicates_and_unordered() -> None:
    query = _query()

    gap = series_from_core_bars(
        query,
        (CoreBar(0, 1, 1, 1, 1), CoreBar(120_000, 1, 1, 1, 1)),
        source="provider",
    )
    duplicate = series_from_core_bars(
        query,
        (CoreBar(0, 1, 1, 1, 1), CoreBar(0, 2, 2, 2, 2)),
        source="provider",
    )
    unordered = series_from_core_bars(
        query,
        (
            CoreBar(60_000, 1, 1, 1, 1),
            CoreBar(0, 1, 1, 1, 1),
            CoreBar(120_000, 1, 1, 1, 1),
        ),
        source="provider",
    )

    assert gap.coverage.status == "gap"
    assert gap.coverage.missing_intervals == ((60_000, 120_000),)
    assert duplicate.coverage.status == "duplicate"
    assert duplicate.coverage.duplicate_timestamps == (0,)
    assert unordered.coverage.status == "unordered"


def test_series_from_market_bars_uses_bar_identity_when_available() -> None:
    query = _query()
    market_bar = MarketBar(
        time=0,
        open=1,
        high=1,
        low=1,
        close=1,
        volume=5,
        time_close=59_999,
        exchange="bybit",
        market="linear",
        symbol="ETHUSDT",
        timeframe="1m",
        is_closed=False,
    )

    series = series_from_market_bars(query, (market_bar,), source="storage")

    assert series.bars[0].instrument == InstrumentKey("bybit", "linear", "ETHUSDT")
    assert series.bars[0].closed is False
    assert series.coverage.source_mix == ("storage",)


def test_contract_to_market_bar_preserves_contract_metadata() -> None:
    timeframe = parse_timeframe("1m")
    instrument = InstrumentKey("binance", "spot", "BTCUSDT")
    bar = ContractBar(instrument, timeframe, 0, 59_999, 1, 2, 0.5, 1.5, None, False)

    market_bar = contract_to_market_bar(bar)

    assert market_bar.exchange == "binance"
    assert market_bar.market == "spot"
    assert market_bar.symbol == "BTCUSDT"
    assert market_bar.timeframe == "1m"
    assert market_bar.volume == 0.0
    assert market_bar.is_closed is False
