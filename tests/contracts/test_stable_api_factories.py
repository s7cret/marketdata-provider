from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path

import pytest
from openpine_contracts import Finality

from marketdata_provider import LiveKlineEvent as TopLevelLiveKlineEvent
from marketdata_provider import (
    create_candle_store,
    create_live_kline_client,
    create_provider,
)
from marketdata_provider.canonical.provider import ProviderRawBar, build_public_snapshot
from marketdata_provider.config import (
    MarketDataConfig,
    OfflineDataConfig,
    StorageConfig,
)
from marketdata_provider.contracts import (
    BarQuery,
    CandleStore,
    CoverageValidationError,
    InstrumentKey,
    LiveKlineClient,
    LiveKlineEvent,
    MarketDataProvider,
    parse_timeframe,
)
from marketdata_provider.core.bar import MarketBar
from marketdata_provider.streaming import KlineUpdate
from marketdata_provider.streaming.live import LiveKlineEvent as RawLiveKlineEvent


def _query() -> BarQuery:
    return BarQuery(
        instrument=InstrumentKey("binance", "spot", "BTCUSDT"),
        timeframe=parse_timeframe("1m"),
        start_ms=60_000,
        end_ms=120_000,
        source="storage",
    )


def test_create_candle_store_returns_contract_protocol_and_preserves_window(
    tmp_path: Path,
) -> None:
    store = create_candle_store(
        MarketDataConfig(storage=StorageConfig(cache_dir=tmp_path))
    )
    assert isinstance(store, CandleStore)

    query = _query()
    write_query = BarQuery(
        instrument=query.instrument,
        timeframe=query.timeframe,
        start_ms=0,
        end_ms=180_000,
        source="storage",
    )
    raw_bars = [
        ProviderRawBar(
            instrument_id=write_query.instrument.serialize(),
            timeframe="1m",
            open_time_utc_ms=time,
            close_time_utc_ms=time + 59_999,
            open=value,
            high=value,
            low=value,
            close=value,
            volume=value,
            finality=Finality.FINAL,
            provider="binance",
            provider_revision="fixture-v1",
        )
        for time, value in ((0, "1"), (60_000, "2"), (120_000, "3"))
    ]
    snapshot = build_public_snapshot(
        write_query, raw_bars, provider_revision="fixture-v1"
    )

    result = store.write(snapshot)
    restored = store.read(query)

    assert result.success
    assert result.rows_written == 3
    assert [bar["open_time_utc_ms"] for bar in restored["bars"]] == [60_000]
    assert restored["coverage"]["complete"] is True
    assert restored["coverage"]["covered_end_utc_ms"] == 120_000
    assert store.coverage(query)["complete"] is True
    assert store.latest_bar_time(query) == 120_000


def test_candle_store_write_rejects_mixed_series_before_persisting(
    tmp_path: Path,
) -> None:
    store = create_candle_store(
        MarketDataConfig(storage=StorageConfig(cache_dir=tmp_path))
    )
    query = _query()
    valid = build_public_snapshot(
        query,
        [
            ProviderRawBar(
                instrument_id=query.instrument.serialize(),
                timeframe="1m",
                open_time_utc_ms=60_000,
                close_time_utc_ms=119_999,
                open="2",
                high="2",
                low="2",
                close="2",
                volume="2",
                finality=Finality.FINAL,
                provider="binance",
                provider_revision="fixture-v1",
            )
        ],
        provider_revision="fixture-v1",
    )
    tampered = dict(valid)
    tampered["bars"] = [dict(valid["bars"][0], instrument_id="bybit/linear/ETHUSDT")]

    result = store.write(tampered)

    assert not result.success
    assert result.rows_written == 0
    assert result.error is not None
    assert "bar_content_hash verification failed" in result.error
    assert store.read(query)["bars"] == []


@pytest.mark.parametrize(
    (
        "stored_exchange",
        "stored_market",
        "stored_symbol",
        "stored_timeframe",
        "message",
    ),
    [
        ("bybit", "linear", "ETHUSDT", "1m", "instrument does not match"),
        ("binance", "spot", "BTCUSDT", "5m", "timeframe does not match"),
    ],
)
def test_candle_store_read_rejects_rows_with_mismatched_embedded_identity(
    tmp_path: Path,
    stored_exchange: str,
    stored_market: str,
    stored_symbol: str,
    stored_timeframe: str,
    message: str,
) -> None:
    store = create_candle_store(
        MarketDataConfig(storage=StorageConfig(cache_dir=tmp_path))
    )
    query = _query()
    corrupt_bar = MarketBar(
        time=60_000,
        open=1.0,
        high=1.0,
        low=1.0,
        close=1.0,
        volume=1.0,
        time_close=119_999,
        exchange=stored_exchange,
        market=stored_market,
        symbol=stored_symbol,
        timeframe=stored_timeframe,
        source_transport="ws",
        source_kind="trade_kline",
        is_closed=True,
        provider=stored_exchange,
        provider_revision="test-fixture-v1",
    )
    store.store.segments.replace_all(  # type: ignore[attr-defined]
        [corrupt_bar],
        exchange=query.instrument.exchange,
        market=query.instrument.market,
        symbol=query.instrument.symbol,
        timeframe=query.timeframe.canonical,
    )

    with pytest.raises(CoverageValidationError, match=message):
        store.read(query)


def test_create_provider_can_wrap_offline_data_as_canonical_protocol(
    tmp_path: Path,
) -> None:
    source = tmp_path / "bars.csv"
    source.write_text(
        "time,open,high,low,close,volume,time_close,finality,provider,provider_revision,revision_state,revision\n"
        "0,1,1,1,1,1,59999,FINAL,offline,fixture-v1,ORIGINAL,0\n"
        "60000,2,2,2,2,2,119999,FINAL,offline,fixture-v1,ORIGINAL,0\n"
        "120000,3,3,3,3,3,179999,FINAL,offline,fixture-v1,ORIGINAL,0\n"
    )
    provider = create_provider(MarketDataConfig(offline=OfflineDataConfig(root=source)))
    assert isinstance(provider, MarketDataProvider)

    snapshot = provider.fetch_bars(_query())

    assert [bar["open_time_utc_ms"] for bar in snapshot["bars"]] == [60_000]
    assert snapshot["coverage"]["complete"] is True


def test_create_live_kline_client_returns_contract_protocol() -> None:
    client = create_live_kline_client(
        MarketDataConfig(),
        instrument=InstrumentKey("binance", "spot", "BTCUSDT"),
        timeframe=parse_timeframe("1m"),
    )

    assert isinstance(client, LiveKlineClient)
    assert TopLevelLiveKlineEvent is LiveKlineEvent


@pytest.mark.asyncio
async def test_create_live_kline_client_yields_canonical_events(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeRawClient:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        async def events(
            self,
            *,
            max_messages: int | None = None,
            timeout_s: float | None = None,
        ) -> AsyncIterator[RawLiveKlineEvent]:
            yield RawLiveKlineEvent(
                update=KlineUpdate(
                    "binance",
                    "spot",
                    "BTCUSDT",
                    "1m",
                    123,
                    60_000,
                    119_999,
                    1.0,
                    2.0,
                    0.5,
                    1.5,
                    10.0,
                    is_closed=False,
                    received_at=456,
                    open_text="1.0",
                    high_text="2.0",
                    low_text="0.5",
                    close_text="1.5",
                    volume_text="10.0",
                ),
                raw_payload={"stream": "test"},
            )

    monkeypatch.setattr(
        "marketdata_provider.streaming.PublicKlineWebSocketClient", FakeRawClient
    )
    client = create_live_kline_client(
        MarketDataConfig(),
        instrument=InstrumentKey("binance", "spot", "BTCUSDT"),
        timeframe=parse_timeframe("1m"),
    )

    events = [event async for event in client.events(max_messages=1, timeout_s=1)]

    assert len(events) == 1
    assert isinstance(events[0], LiveKlineEvent)
    assert events[0].bar["instrument_id"] == "binance/spot/BTCUSDT"
    assert events[0].bar["timeframe"] == "1m"
    assert events[0].bar["finality"] is Finality.OPEN
    assert events[0].bar["close"] == "1.5"
    assert events[0].bar["snapshot_id"]
    assert events[0].bar["bar_content_hash"]
    assert events[0].event_time == 123
    assert events[0].received_at == 456
    assert events[0].raw_payload == {"stream": "test"}
