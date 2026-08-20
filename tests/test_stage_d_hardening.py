import importlib.util
import os
import sqlite3
from pathlib import Path

import pytest

from marketdata_provider._pathing import safe_path_part
from marketdata_provider.cli.main import main
from marketdata_provider.core.bar import MarketBar
from marketdata_provider.errors import MDUnsupportedFeature
from marketdata_provider.store import RawStore, SegmentStore
from marketdata_provider.streaming import (
    CoalescingKlineQueue,
    KlineUpdate,
    PublicKlineWebSocketClient,
)


def mb(t: int, close: float = 1.5) -> MarketBar:
    return MarketBar(
        time=t,
        open=1,
        high=2,
        low=0.5,
        close=close,
        volume=10,
        time_close=t + 59_999,
        exchange="binance",
        market="spot",
        symbol="BTCUSDT",
        timeframe="1m",
        source_transport="ws",
        source_kind="trade_kline",
        is_closed=True,
        downloaded_at=t + 60_000,
    )


def test_parquet_optional_fails_explicitly_when_pyarrow_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setattr(
        "marketdata_provider.store.segment_store.importlib.util.find_spec",
        lambda name: None if name == "pyarrow" else importlib.util.find_spec(name),
    )
    with pytest.raises(MDUnsupportedFeature, match="pyarrow"):
        SegmentStore(tmp_path, data_format="parquet")


def test_bounded_csv_segment_read_streams_without_full_load(
    tmp_path: Path, monkeypatch
):
    store = SegmentStore(tmp_path)
    store.replace_all(
        [mb(0), mb(60_000, close=1.6), mb(120_000, close=1.7)],
        exchange="binance",
        market="spot",
        symbol="BTCUSDT",
        timeframe="1m",
    )

    monkeypatch.setattr(
        store,
        "_read_csv",
        lambda _path: (_ for _ in ()).throw(
            AssertionError("bounded read should stream")
        ),
    )

    bars = store.read_all(
        exchange="binance",
        market="spot",
        symbol="BTCUSDT",
        timeframe="1m",
        start=60_000,
        end=120_000,
    )

    assert [b.time for b in bars] == [60_000]


def test_segment_store_parses_typed_bool_and_exposes_latest_time(tmp_path: Path):
    store = SegmentStore(tmp_path)
    row = {
        "time": 0,
        "open": 1,
        "high": 2,
        "low": 0.5,
        "close": 1.5,
        "volume": 10,
        "is_closed": True,
    }

    assert store._row_to_bar(row).is_closed is True

    store.replace_all(
        [mb(0), mb(60_000, close=1.6)],
        exchange="binance",
        market="spot",
        symbol="BTCUSDT",
        timeframe="1m",
    )
    assert (
        store.latest_bar_time(
            exchange="binance",
            market="spot",
            symbol="BTCUSDT",
            timeframe="1m",
        )
        == 60_000
    )


def test_segment_store_replaces_index_row_for_single_physical_series(tmp_path: Path):
    store = SegmentStore(tmp_path)

    store.replace_all_stream(
        [mb(0), mb(60_000)],
        exchange="binance",
        market="spot",
        symbol="BTCUSDT",
        timeframe="1m",
    )
    store.replace_all_stream(
        [mb(0), mb(60_000), mb(120_000)],
        exchange="binance",
        market="spot",
        symbol="BTCUSDT",
        timeframe="1m",
    )

    with sqlite3.connect(tmp_path / "index.sqlite") as db:
        count, summed_rows, max_rows = db.execute("""
            SELECT COUNT(*), SUM(rows_count), MAX(rows_count)
            FROM marketdata_segments
            WHERE exchange='binance' AND market='spot' AND symbol='BTCUSDT'
              AND timeframe='1m' AND source_kind='trade_kline'
            """).fetchone()

    assert count == 1
    assert summed_rows == 3
    assert max_rows == 3


def test_segment_store_vacuum_removes_stale_atomic_temp_files(tmp_path: Path):
    store = SegmentStore(tmp_path)
    store.replace_all(
        [mb(0)], exchange="binance", market="spot", symbol="BTCUSDT", timeframe="1m"
    )
    directory = store._dir(
        exchange="binance",
        market="spot",
        symbol="BTCUSDT",
        timeframe="1m",
        source_kind="trade_kline",
    )
    stale = directory / ".bars.csv.abandoned"
    live = directory / ".bars.csv.live"
    stale.write_text("partial", encoding="utf-8")
    live.write_text("partial", encoding="utf-8")
    os.utime(stale, (0, 0))

    result = store.vacuum()

    assert result["removed_stale_data_files"] >= 1
    assert not stale.exists()
    assert live.exists()


def test_raw_store_plain_ndjson_manifest_checksum(tmp_path: Path):
    store = RawStore(tmp_path)
    manifest = store.write_batch(
        [{"b": 2, "a": 1}],
        exchange="binance",
        market="spot",
        symbol="BTCUSDT",
        source_transport="ws",
    )
    assert manifest.compression == "plain"
    assert manifest.rows_count == 1
    assert store.read_batch(
        exchange="binance", market="spot", symbol="BTCUSDT", source_transport="ws"
    ) == [{"a": 1, "b": 2}]
    assert len(store.inspect()) == 1


def test_raw_store_path_components_are_sanitized(tmp_path: Path):
    store = RawStore(tmp_path / "raw")

    manifest = store.write_batch(
        [{"x": 1}],
        exchange="../binance",
        market="spot/../../bad",
        symbol="../BTC:USDT",
        source_transport="rest/../../x",
        source_kind="trade/../../kline",
        partition="../../2024-01-01",
    )

    manifests = list((tmp_path / "raw").glob("raw-v1/**/manifest.json"))
    assert len(manifests) == 1
    assert (tmp_path / "raw") in manifests[0].parents
    assert safe_path_part("..") == "UNKNOWN"
    assert ".." not in manifests[0].relative_to(tmp_path / "raw").as_posix().split("/")
    assert store.read_batch(
        exchange="../binance",
        market="spot/../../bad",
        symbol="../BTC:USDT",
        source_transport="rest/../../x",
        source_kind="trade/../../kline",
        partition="../../2024-01-01",
    ) == [{"x": 1}]
    assert manifest.file_name.startswith("payloads-")


def test_segment_store_path_components_are_sanitized(tmp_path: Path):
    store = SegmentStore(tmp_path / "segments")
    manifest = store.replace_all(
        [],
        exchange="../binance",
        market="spot/../../bad",
        symbol="../ETH:USDT",
        timeframe="1m",
        source_kind="trade/../../kline",
    )
    data_path, manifest_path = store._paths(
        exchange=manifest.exchange,
        market=manifest.market,
        symbol=manifest.symbol,
        timeframe=manifest.timeframe,
        source_kind=manifest.source_kind,
        data_format="csv",
    )
    root = (tmp_path / "segments").resolve()
    assert data_path.resolve().is_relative_to(root)
    assert manifest_path.resolve().is_relative_to(root)
    assert ".." not in data_path.relative_to(tmp_path / "segments").as_posix().split(
        "/"
    )
    assert ".." not in manifest_path.relative_to(
        tmp_path / "segments"
    ).as_posix().split("/")


def test_backpressure_coalesces_and_reports_drop():
    q = CoalescingKlineQueue(maxsize=1)
    q.put(
        KlineUpdate("binance", "spot", "BTCUSDT", "1m", 1, 0, 59999, 1, 2, 0.5, 1.1, 10)
    )
    q.put(
        KlineUpdate("binance", "spot", "BTCUSDT", "1m", 2, 0, 59999, 1, 2, 0.5, 1.2, 10)
    )
    q.put(
        KlineUpdate("binance", "spot", "ETHUSDT", "1m", 3, 0, 59999, 1, 2, 0.5, 1.3, 10)
    )
    assert q.coalesced == 1
    assert q.dropped == 1
    assert q.diagnostics[0].code == "MD_STREAM_BACKPRESSURE_DROP"
    assert q.drain()[0].symbol == "ETHUSDT"


def test_real_ws_endpoint_construction_no_fake_connection():
    b = PublicKlineWebSocketClient(
        exchange="binance", market="spot", symbol="BTCUSDT", timeframe="1m"
    )
    assert b.url.startswith("wss://stream.binance.com")
    bf = PublicKlineWebSocketClient(
        exchange="binance", market="usdm", symbol="BTCUSDT", timeframe="1m"
    )
    assert bf.url.startswith("wss://fstream.binancefuture.com")
    y = PublicKlineWebSocketClient(
        exchange="bybit", market="linear", symbol="BTCUSDT", timeframe="1m"
    )
    assert y.subscribe and "kline.1.BTCUSDT" in y.subscribe["args"]


def test_cli_new_stage_d_commands(tmp_path: Path, capsys):
    assert main(["vacuum", "--store-dir", str(tmp_path / "store")]) == 0
    assert "removed_stale_data_files" in capsys.readouterr().out
    raw = RawStore(tmp_path / "raw")
    raw.write_batch(
        [{"x": 1}],
        exchange="binance",
        market="spot",
        symbol="BTCUSDT",
        source_transport="rest",
    )
    assert main(["raw-inspect", "--raw-dir", str(tmp_path / "raw")]) == 0
    assert "stage-d-raw-1" in capsys.readouterr().out


@pytest.mark.live_network
@pytest.mark.asyncio
async def test_live_ws_smoke_is_mandatory(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("MARKETDATA_ALLOW_STREAM", "1")
    client = PublicKlineWebSocketClient(
        exchange="binance", market="spot", symbol="BTCUSDT", timeframe="1m"
    )
    seen = []
    async for event in client.events(max_messages=1, timeout_s=10):
        seen.append(event)
    assert seen
