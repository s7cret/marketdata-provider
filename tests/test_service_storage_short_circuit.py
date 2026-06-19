from __future__ import annotations

import pytest

from marketdata_provider.config import HistoryConfig, MarketDataConfig, StorageConfig
from marketdata_provider.contracts import BarQuery, InstrumentKey, parse_timeframe
from marketdata_provider.core.bar import MarketBar
from marketdata_provider.service import MarketDataService

DAY_MS = 86_400_000


def _daily_bar(t: int, close: float = 1.0) -> MarketBar:
    return MarketBar(
        time=t,
        open=close,
        high=close + 1.0,
        low=close - 1.0,
        close=close,
        volume=100.0,
        time_close=t + DAY_MS - 1,
        exchange="binance",
        market="spot",
        symbol="SOLUSDT",
        timeframe="1D",
        source_transport="storage",
        source_kind="trade_kline",
        is_closed=True,
    )


def _daily_query() -> BarQuery:
    return BarQuery(
        InstrumentKey("binance", "spot", "SOLUSDT"),
        parse_timeframe("1D"),
        0,
        2 * DAY_MS,
    )


def test_fetch_bars_uses_complete_requested_timeframe_when_base_cache_changed(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    service = MarketDataService(
        MarketDataConfig(
            storage=StorageConfig(cache_dir=tmp_path),
            history=HistoryConfig(enabled=True, base_timeframe="1m"),
        )
    )
    query = _daily_query()
    stored_daily = [_daily_bar(0, 10.0), _daily_bar(DAY_MS, 11.0)]

    monkeypatch.setattr(service, "_ensure_stored", lambda _query: True)
    monkeypatch.setattr(service, "_stored_bars", lambda _query: stored_daily)
    monkeypatch.setattr(
        service,
        "_aggregate_stored_base",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("complete requested timeframe should not be re-aggregated")
        ),
    )

    series = service.fetch_bars(query)

    assert [bar.time for bar in series.bars] == [0, DAY_MS]
    assert [bar.close for bar in series.bars] == [10.0, 11.0]


def test_materialize_bars_skips_reaggregation_when_requested_timeframe_complete(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    service = MarketDataService(
        MarketDataConfig(
            storage=StorageConfig(cache_dir=tmp_path),
            history=HistoryConfig(enabled=True, base_timeframe="1m"),
        )
    )
    query = _daily_query()

    monkeypatch.setattr(
        service,
        "_ensure_stored",
        lambda _query: (_ for _ in ()).throw(
            AssertionError("complete requested timeframe should not fetch base cache")
        ),
    )
    monkeypatch.setattr(service, "_stored_coverage_complete", lambda _query: True)
    monkeypatch.setattr(service, "_stored_span_complete", lambda _query: True)
    monkeypatch.setattr(
        service,
        "_aggregate_stored_base",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("complete requested timeframe should not be re-aggregated")
        ),
    )

    result = service.materialize_bars(query)

    assert result == {
        "ok": True,
        "span_ok": True,
        "changed": False,
        "bars_returned": 0,
    }
