from datetime import UTC, datetime

import marketdata_provider
from marketdata_provider import close_time_ms, to_pine_timeframe


def _ms(year: int, month: int, day: int) -> int:
    return int(datetime(year, month, day, tzinfo=UTC).timestamp() * 1000)


def test_top_level_close_time_ms_preserves_calendar_month_semantics() -> None:
    assert close_time_ms(_ms(2024, 2, 1), "1M") == _ms(2024, 3, 1) - 1


def test_top_level_to_pine_timeframe_converts_hour_units_to_pine_minutes() -> None:
    assert to_pine_timeframe("1h") == "60"
    assert to_pine_timeframe("12h") == "720"


def test_top_level_timeframe_helpers_are_documented_public_exports() -> None:
    assert {"close_time_ms", "to_pine_timeframe"} <= set(marketdata_provider.__all__)
