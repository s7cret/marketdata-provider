from marketdata_provider.contracts.v2 import (
    Finality,
    bar_finality,
    canonical_bar_v2_from_market_bar,
    snapshot_hash,
)
from marketdata_provider.core.bar import MarketBar
from marketdata_provider.exchanges.binance.rest import normalize_binance_klines


def test_unknown_server_time_marks_open() -> None:
    assert bar_finality(close_time_ms=120_000, server_time_ms=None) is Finality.OPEN


def test_binance_without_server_time_drops_last_row() -> None:
    rows = [
        [1000, "1", "2", "0.5", "1.5", "10", 60999],
        [61000, "1.5", "2", "1", "1.2", "5", 120999],
    ]
    bars = normalize_binance_klines(rows, symbol="BTCUSDT", market="spot", timeframe="1m")
    assert [bar.time for bar in bars] == [1000]
    assert bars[0].is_closed is False


def test_snapshot_hash_is_stable() -> None:
    bar = MarketBar(
        time=1000,
        open=1,
        high=2,
        low=0.5,
        close=1.5,
        volume=10,
        time_close=60999,
        exchange="binance",
        market="spot",
        symbol="BTCUSDT",
        timeframe="1m",
        is_closed=True,
    )
    left = canonical_bar_v2_from_market_bar(bar, snapshot_id="snap-1")
    right = canonical_bar_v2_from_market_bar(bar, snapshot_id="snap-1")
    assert snapshot_hash([left]) == snapshot_hash([right])
    assert left.finality is Finality.FINAL
