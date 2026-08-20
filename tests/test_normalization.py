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
        server_time_ms=2_000_000_000_000,
    )

    assert bars[0].time_close == 1504714499999


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
            rows, symbol="BTCUSDT", market="spot", timeframe="1m", server_time_ms=70_000
        )


def test_missing_server_time_does_not_close_bars() -> None:
    rows = [[1000, "1", "2", "0.5", "1.5", "10", 60999]]
    with pytest.raises(MDValidationError, match="server_time_ms required"):
        normalize_binance_klines(rows, symbol="BTCUSDT", market="spot", timeframe="1m")
    with pytest.raises(MDValidationError, match="server_time_ms required"):
        normalize_bybit_klines(
            [[1000, "1", "2", "0.5", "1.5", "10"]],
            symbol="BTCUSDT",
            market="linear",
            timeframe="1m",
            include_open_candle=True,
        )


def test_exact_close_boundary_keeps_final_bar() -> None:
    rows = [[1000, "1", "2", "0.5", "1.5", "10", 60999]]
    bars = normalize_binance_klines(
        rows,
        symbol="BTCUSDT",
        market="spot",
        timeframe="1m",
        server_time_ms=60999,
    )
    assert len(bars) == 1
    assert bars[0].is_closed is True


def test_include_open_marks_open_not_final() -> None:
    rows = [[1000, "1", "2", "0.5", "1.5", "10", 60999]]
    bars = normalize_binance_klines(
        rows,
        symbol="BTCUSDT",
        market="spot",
        timeframe="1m",
        server_time_ms=60998,
        include_open_candle=True,
    )
    assert len(bars) == 1
    assert bars[0].is_closed is False


def test_exclude_open_requires_server_time() -> None:
    from marketdata_provider.validation import exclude_open_candle

    with pytest.raises(MDValidationError, match="server_time_ms"):
        exclude_open_candle([], server_time_ms=None)


def test_exclude_open_requires_close_time() -> None:
    from marketdata_provider.core.bar import Bar
    from marketdata_provider.validation import exclude_open_candle

    with pytest.raises(MDValidationError, match="time_close"):
        exclude_open_candle(
            [Bar(1000, 1, 2, 0.5, 1.5, 1, None)],
            server_time_ms=2000,
        )
