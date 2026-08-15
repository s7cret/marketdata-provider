import pytest

from marketdata_provider.errors import MDValidationError
from marketdata_provider.exchanges.binance.rest import normalize_binance_klines
from marketdata_provider.exchanges.bybit.rest import normalize_bybit_klines


def test_binance_normalizes_and_excludes_open():
    rows = [
        [1000, "1", "2", "0.5", "1.5", "10", 60999],
        [61000, "1.5", "2", "1", "1.2", "5", 120999],
    ]
    bars = normalize_binance_klines(
        rows, symbol="BTCUSDT", market="spot", timeframe="1m", server_time_ms=70_000
    )
    assert len(bars) == 1 and bars[0].time == 1000


def test_binance_repairs_zero_duration_historical_kline():
    rows = [[1504713600000, "1", "1", "1", "1", "0", 1504713600000]]

    bars = normalize_binance_klines(
        rows,
        symbol="BTCUSDT",
        market="spot",
        timeframe="15m",
        server_time_ms=1504714500000,
    )

    assert bars[0].time_close == 1504714499999
    assert bars[0].is_closed is True


def test_bybit_sorts_newest_first():
    payload = {
        "result": {
            "list": [
                [61000, "1.5", "2", "1", "1.2", "5", "7"],
                [1000, "1", "2", "0.5", "1.5", "10", "15"],
            ]
        }
    }
    bars = normalize_bybit_klines(
        payload,
        symbol="BTCUSDT",
        market="linear",
        timeframe="1m",
        server_time_ms=200_000,
    )
    assert [b.time for b in bars] == [1000, 61000]
    assert bars[0].quote_volume == 15


def test_validation_duplicate_rejected():
    rows = [
        [1000, "1", "2", "0.5", "1.5", "10", 60999],
        [1000, "1", "2", "0.5", "1.5", "10", 60999],
    ]
    with pytest.raises(MDValidationError):
        normalize_binance_klines(
            rows,
            symbol="BTCUSDT",
            market="spot",
            timeframe="1m",
            server_time_ms=200_000,
        )
