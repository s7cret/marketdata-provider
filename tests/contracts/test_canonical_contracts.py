from __future__ import annotations

import pytest

from marketdata_provider.contracts import (
    Bar,
    BarQuery,
    CoverageReport,
    InstrumentKey,
    InvalidBarError,
    InvalidBarQueryError,
    InvalidInstrumentError,
    InvalidTimeframeError,
    parse_timeframe,
)


def test_instrument_key_normalizes_and_serializes() -> None:
    instrument = InstrumentKey(
        exchange=" BINANCE ", market=" Spot ", symbol=" btcusdt "
    )

    assert instrument.exchange == "binance"
    assert instrument.market == "spot"
    assert instrument.symbol == "BTCUSDT"
    assert instrument.serialize() == "binance/spot/BTCUSDT"
    assert InstrumentKey.parse("binance/spot/BTCUSDT") == instrument


def test_instrument_key_rejects_empty_parts() -> None:
    with pytest.raises(InvalidInstrumentError):
        InstrumentKey(exchange="", market="spot", symbol="BTCUSDT")


@pytest.mark.parametrize(
    ("raw", "canonical", "multiplier", "unit", "duration_ms"),
    [
        ("15", "15m", 15, "minute", 900_000),
        ("15m", "15m", 15, "minute", 900_000),
        ("1h", "1h", 1, "hour", 3_600_000),
        ("1D", "1D", 1, "day", 86_400_000),
        ("1W", "1W", 1, "week", 604_800_000),
        ("1M", "1M", 1, "month", None),
    ],
)
def test_parse_timeframe_matrix(
    raw: str,
    canonical: str,
    multiplier: int,
    unit: str,
    duration_ms: int | None,
) -> None:
    parsed = parse_timeframe(raw)

    assert parsed.canonical == canonical
    assert parsed.multiplier == multiplier
    assert parsed.unit == unit
    assert parsed.duration_ms == duration_ms


def test_parse_timeframe_rejects_unknown_value() -> None:
    with pytest.raises(InvalidTimeframeError):
        parse_timeframe("coffee")


def test_bar_query_uses_start_inclusive_end_exclusive_window() -> None:
    instrument = InstrumentKey("binance", "spot", "BTCUSDT")
    timeframe = parse_timeframe("15")
    query = BarQuery(
        instrument=instrument,
        timeframe=timeframe,
        start_ms=1_000,
        end_ms=2_000,
        source="provider",
        gap_policy="fail",
        error_policy="raise",
    )

    assert query.start_ms == 1_000
    assert query.end_ms == 2_000


def test_bar_query_rejects_invalid_window_and_error_policy() -> None:
    instrument = InstrumentKey("binance", "spot", "BTCUSDT")
    timeframe = parse_timeframe("15")

    with pytest.raises(InvalidBarQueryError):
        BarQuery(instrument, timeframe, start_ms=2_000, end_ms=2_000)
    with pytest.raises(InvalidBarQueryError):
        BarQuery(instrument, timeframe, start_ms=1_000, end_ms=2_000, error_policy="ignore")  # type: ignore[arg-type]


def test_bar_validates_ohlcv_and_time_close() -> None:
    instrument = InstrumentKey("binance", "spot", "BTCUSDT")
    timeframe = parse_timeframe("15")

    bar = Bar(
        instrument=instrument,
        timeframe=timeframe,
        time=1_000,
        time_close=901_000,
        open=100.0,
        high=101.0,
        low=99.0,
        close=100.5,
        volume=None,
        closed=True,
    )

    assert bar.volume is None

    with pytest.raises(InvalidBarError):
        Bar(instrument, timeframe, 1_000, 1_000, 100.0, 101.0, 99.0, 100.5, 1.0, True)
    with pytest.raises(InvalidBarError):
        Bar(instrument, timeframe, 1_000, 901_000, 100.0, 99.0, 98.0, 100.5, 1.0, True)


def test_coverage_report_marks_complete_series() -> None:
    report = CoverageReport(
        requested_start_ms=1_000,
        requested_end_ms=2_000,
        delivered_start_ms=1_000,
        delivered_end_ms=2_000,
        source_mix=("provider",),
        status="valid",
    )

    assert report.is_complete
