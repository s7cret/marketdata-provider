from marketdata_provider.timeframes import (
    normalize_timeframe,
    parse_time_ms,
    timeframe_to_binance_interval,
    timeframe_to_bybit_interval,
    timeframe_to_ms,
)


def test_timeframe_to_ms_contract_for_common_fixed_frames():
    assert timeframe_to_ms("1m") == 60_000
    assert timeframe_to_ms("3m") == 180_000
    assert timeframe_to_ms("5m") == 300_000
    assert timeframe_to_ms("15m") == 900_000
    assert timeframe_to_ms("30m") == 1_800_000
    assert timeframe_to_ms("1h") == 3_600_000
    assert timeframe_to_ms("4h") == 14_400_000
    assert timeframe_to_ms("1d") == 86_400_000


def test_timeframe_exchange_interval_contracts():
    assert normalize_timeframe("15") == "15m"
    assert timeframe_to_binance_interval("15m") == "15m"
    assert timeframe_to_bybit_interval("15m") == "15"


def test_parse_time_ms_accepts_iso_z_and_epoch_seconds_or_ms():
    assert parse_time_ms("2026-05-10T00:00:00Z") == 1_778_371_200_000
    assert parse_time_ms("1778371200") == 1_778_371_200_000
    assert parse_time_ms("1778371200000") == 1_778_371_200_000
