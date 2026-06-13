from datetime import datetime, timezone
from marketdata_provider.timeframes import (
    close_time_ms,
    next_open_time_ms,
    timeframe_ms,
    to_binance_interval,
    to_bybit_interval,
)


def ms(y, m, d, h=0, mi=0):
    return int(datetime(y, m, d, h, mi, tzinfo=timezone.utc).timestamp() * 1000)


def test_fixed_mapping_and_close():
    assert to_binance_interval("60") == "1h"
    assert to_bybit_interval("1h") == "60"
    assert timeframe_ms("5") == 300_000
    assert close_time_ms(ms(2024, 1, 1), "1m") == ms(2024, 1, 1, 0, 1) - 1


def test_day_week_month_leap_boundaries():
    assert close_time_ms(ms(2024, 2, 29), "D") == ms(2024, 3, 1) - 1
    assert close_time_ms(ms(2024, 2, 1), "M") == ms(2024, 3, 1) - 1
    assert close_time_ms(ms(2023, 12, 1), "M") == ms(2024, 1, 1) - 1
    assert next_open_time_ms(ms(2024, 12, 1), "M") == ms(2025, 1, 1)


def test_week_monday_anchor_even_midweek_input():
    assert close_time_ms(ms(2024, 1, 3), "W") == ms(2024, 1, 8) - 1
