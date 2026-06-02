import pytest

from marketdata_provider.contracts import FootprintQuery, InstrumentKey, parse_timeframe
from marketdata_provider.contracts.errors import InvalidBarQueryError


def test_footprint_query_is_separate_from_bar_query():
    query = FootprintQuery(
        instrument=InstrumentKey("binance", "usdm", "BTCUSDT"),
        timeframe=parse_timeframe("1m"),
        start_ms=0,
        end_ms=60_000,
        tick_size=0.1,
        ticks_per_row=5,
    )

    assert query.bucket_size == 0.5
    assert query.source == "auto"
    assert query.gap_policy == "fail"


def test_footprint_query_requires_price_bucket_or_tick_size():
    with pytest.raises(InvalidBarQueryError):
        FootprintQuery(
            instrument=InstrumentKey("binance", "usdm", "BTCUSDT"),
            timeframe=parse_timeframe("1m"),
            start_ms=0,
            end_ms=60_000,
        )

