from __future__ import annotations

from pathlib import Path

from marketdata_provider import create_candle_store, create_live_kline_client, create_provider
from marketdata_provider.config import MarketDataConfig, OfflineDataConfig, StorageConfig
from marketdata_provider.contracts import (
    Bar,
    BarQuery,
    BarSeries,
    CandleStore,
    CoverageReport,
    InstrumentKey,
    LiveKlineClient,
    MarketDataProvider,
    parse_timeframe,
)


def _query() -> BarQuery:
    return BarQuery(
        instrument=InstrumentKey("binance", "spot", "BTCUSDT"),
        timeframe=parse_timeframe("1m"),
        start_ms=60_000,
        end_ms=120_000,
        source="storage",
    )


def test_create_candle_store_returns_contract_protocol_and_preserves_window(tmp_path: Path) -> None:
    store = create_candle_store(MarketDataConfig(storage=StorageConfig(cache_dir=tmp_path)))
    assert isinstance(store, CandleStore)

    query = _query()
    write_query = BarQuery(
        instrument=query.instrument,
        timeframe=query.timeframe,
        start_ms=0,
        end_ms=180_000,
        source="storage",
    )
    bars = (
        Bar(query.instrument, query.timeframe, 0, 59_999, 1.0, 1.0, 1.0, 1.0, 1.0, True),
        Bar(query.instrument, query.timeframe, 60_000, 119_999, 2.0, 2.0, 2.0, 2.0, 2.0, True),
        Bar(query.instrument, query.timeframe, 120_000, 179_999, 3.0, 3.0, 3.0, 3.0, 3.0, True),
    )

    result = store.write(
        BarSeries(
            query=write_query,
            bars=bars,
            coverage=CoverageReport(0, 180_000, 0, 180_000, source_mix=("test",)),
        )
    )
    series = store.read(query)

    assert result.success
    assert result.rows_written == 3
    assert [bar.time for bar in series.bars] == [60_000]
    assert series.coverage.is_complete
    assert series.coverage.delivered_end_ms == 120_000


def test_create_provider_can_wrap_offline_data_as_canonical_protocol(tmp_path: Path) -> None:
    source = tmp_path / "bars.csv"
    source.write_text(
        "time,open,high,low,close,volume,time_close\n"
        "0,1,1,1,1,1,59999\n"
        "60000,2,2,2,2,2,119999\n"
        "120000,3,3,3,3,3,179999\n"
    )
    provider = create_provider(MarketDataConfig(offline=OfflineDataConfig(root=source)))
    assert isinstance(provider, MarketDataProvider)

    series = provider.fetch_bars(_query())

    assert [bar.time for bar in series.bars] == [60_000]
    assert series.coverage.is_complete


def test_create_live_kline_client_returns_contract_protocol() -> None:
    client = create_live_kline_client(
        MarketDataConfig(),
        instrument=InstrumentKey("binance", "spot", "BTCUSDT"),
        timeframe=parse_timeframe("1m"),
    )

    assert isinstance(client, LiveKlineClient)
