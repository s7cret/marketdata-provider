from collections.abc import Sequence

from marketdata_provider.core import Bar, DataProvider


class MemoryProvider:
    def __init__(self, bars: Sequence[Bar]):
        self.bars = list(bars)

    def get_bars(self, symbol: str, timeframe: str, start_ms: int | None, end_ms: int | None, *, max_bars: int | None = None):
        out = [bar for bar in self.bars if (start_ms is None or bar.time >= start_ms) and (end_ms is None or bar.time < end_ms)]
        return out if max_bars is None else out[:max_bars]


def test_data_provider_contract_uses_start_inclusive_end_exclusive():
    bars = [
        Bar(0, 1.0, 1.0, 1.0, 1.0),
        Bar(60_000, 2.0, 2.0, 2.0, 2.0),
        Bar(120_000, 3.0, 3.0, 3.0, 3.0),
    ]
    provider: DataProvider = MemoryProvider(bars)

    result = provider.get_bars("BTCUSDT", "1m", 60_000, 120_000)

    assert [bar.time for bar in result] == [60_000]
