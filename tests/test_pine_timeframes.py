import pytest

from marketdata_provider.errors import MDTimeframeUnsupported
from marketdata_provider.timeframes import canonical_timeframe, to_pine_timeframe


@pytest.mark.parametrize(
    "source,expected",
    [
        ("1h", "60"),
        ("4H", "240"),
        ("60m", "60"),
        ("1m", "1"),
        ("1M", "1M"),
        ("M", "1M"),
        ("D", "1D"),
        ("1d", "1D"),
        ("1W", "1W"),
        ("W", "1W"),
        ("5s", "5S"),
        ("45S", "45S"),
        ("240", "240"),
        ("24h", "1440"),
    ],
)
def test_provider_timeframe_to_pine(source, expected):
    assert to_pine_timeframe(source) == expected


@pytest.mark.parametrize(
    "bad",
    [
        "0",
        "0m",
        "0s",
        "0h",
        "00",
        "00m",
        "2M",
        "60M",
        "0M",
        "-1m",
        "1.5h",
        None,
        True,
        1,
        "",
        "١",
    ],
)
def test_invalid_or_ambiguous_timeframe_is_not_reinterpreted(bad):
    with pytest.raises(MDTimeframeUnsupported):
        canonical_timeframe(bad)


def test_month_minute_remain_different():
    assert canonical_timeframe("1M") == "1M"
    assert canonical_timeframe("1m") == "1m"
