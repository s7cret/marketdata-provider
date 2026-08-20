from __future__ import annotations

import pytest

from marketdata_provider.core.bar import MarketBar
from marketdata_provider.store.current_store import CurrentStore


def test_current_store_rejects_unknown_finality(tmp_path) -> None:
    bar = MarketBar(
        time=0,
        time_close=59_999,
        open=1.0,
        high=1.0,
        low=1.0,
        close=1.0,
        volume=1.0,
        exchange="binance",
        market="spot",
        symbol="BTCUSDT",
        timeframe="1m",
        is_closed=True,
    )
    object.__setattr__(bar, "is_closed", None)

    with pytest.raises(ValueError, match="is_closed is required"):
        CurrentStore(tmp_path / "current.sqlite").upsert_current(bar)
