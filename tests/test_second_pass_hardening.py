from __future__ import annotations

import asyncio
import json
import runpy
from pathlib import Path
from zipfile import ZipFile

import pytest

from marketdata_provider.config import HistoryConfig, MarketDataConfig, StorageConfig
from marketdata_provider.contracts.errors import (
    InvalidBarError,
    InvalidBarQueryError,
    InvalidInstrumentError,
    InvalidTimeframeError,
)
from marketdata_provider.contracts.instrument import InstrumentKey
from marketdata_provider.contracts.query import BarQuery
from marketdata_provider.contracts.timeframe import parse_timeframe
from marketdata_provider.core.bar import Bar, MarketBar, RUNTIME_CONTRACT_VERSION
from marketdata_provider.errors import (
    MDInvalidExchangeResponse,
    MDNetworkUnavailable,
    MDUnsupportedFeature,
)
from marketdata_provider.exchanges.binance import archive as binance_archive
from marketdata_provider.exchanges.binance import trades as binance_trades
from marketdata_provider.service import (
    BinanceArchiveSource,
    BinanceRestSource,
    BybitRestSource,
    MarketDataService,
    _aggregate_market_bars,
    _can_derive_from_base,
    _coverage_complete,
    _market_bar_from_core,
    _merge_bars,
    _remaining_recent_query,
)
from marketdata_provider.store.segment_store import SegmentStore, bars_checksum
from marketdata_provider.streaming.live import PublicKlineWebSocketClient


def mb(
    t: int, close: float = 1.5, *, source: str = "rest", timeframe: str = "1m"
) -> MarketBar:
    return MarketBar(
        time=t,
        open=1.0,
        high=max(2.0, close),
        low=0.5,
        close=close,
        volume=10.0,
        time_close=t + 59_999,
        exchange="binance",
        market="spot",
        symbol="BTCUSDT",
        timeframe=timeframe,
        source_transport=source,
        source_kind="trade_kline",
        is_closed=True,
        downloaded_at=t + 60_000,
    )


def query(
    tf: str = "1m",
    *,
    exchange: str = "binance",
    market: str = "spot",
    start: int = 0,
    end: int = 120_000,
) -> BarQuery:
    return BarQuery(
        InstrumentKey(exchange, market, "BTCUSDT"), parse_timeframe(tf), start, end
    )


def test_contract_validation_error_branches() -> None:
    with pytest.raises(InvalidInstrumentError):
        InstrumentKey("", "spot", "BTCUSDT")
    with pytest.raises(InvalidInstrumentError):
        InstrumentKey.parse("bad")
    with pytest.raises(InvalidTimeframeError):
        parse_timeframe("")
    with pytest.raises(InvalidBarQueryError):
        BarQuery(
            InstrumentKey("binance", "spot", "BTCUSDT"), parse_timeframe("1m"), 2, 1
        )
    with pytest.raises(InvalidBarQueryError):
        BarQuery(InstrumentKey("binance", "spot", "BTCUSDT"), parse_timeframe("1m"), 1, 2, source="bad")  # type: ignore[arg-type]
    with pytest.raises(InvalidBarQueryError):
        BarQuery(InstrumentKey("binance", "spot", "BTCUSDT"), parse_timeframe("1m"), 1, 2, gap_policy="bad")  # type: ignore[arg-type]
    with pytest.raises(InvalidBarQueryError):
        BarQuery(InstrumentKey("binance", "spot", "BTCUSDT"), parse_timeframe("1m"), 1, 2, error_policy="ignore")  # type: ignore[arg-type]
    with pytest.raises(InvalidBarError):
        from marketdata_provider.contracts.bar import Bar as ContractBar

        ContractBar(
            InstrumentKey("binance", "spot", "BTCUSDT"),
            parse_timeframe("1m"),
            10,
            10,
            1,
            2,
            0,
            1,
            1,
            True,
        )


def test_service_sources_and_remaining_query_branches(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cfg = MarketDataConfig(storage=StorageConfig(cache_dir=tmp_path))
    q = query(end=180_000)
    monkeypatch.setattr(
        "marketdata_provider.service._archive_cutoff_ms", lambda _cfg: 60_000
    )
    monkeypatch.setattr(
        "marketdata_provider.service.fetch_binance_archive_bars",
        lambda **kwargs: [Bar(0, 1, 2, 0.5, 1.5, 10, None)],
    )
    assert BinanceArchiveSource(cfg).fetch(query(start=60_000, end=120_000)) == []
    archive = BinanceArchiveSource(cfg).fetch(q)
    assert archive[0].source_transport == "archive"
    assert archive[0].time_close == 59_999
    monkeypatch.setattr(
        "marketdata_provider.service.binance_get_bars_sync",
        lambda *args, **kwargs: [Bar(60_000, 2, 3, 1, 2.5, 5, 119_999)],
    )
    assert BinanceRestSource(cfg).fetch(q)[0].source_transport == "rest"
    monkeypatch.setattr(
        "marketdata_provider.service.bybit_get_bars_sync",
        lambda *args, **kwargs: [Bar(60_000, 2, 3, 1, 2.5, 5, None)],
    )
    assert BybitRestSource(cfg).fetch(query(exchange="bybit"))[0].exchange == "bybit"

    assert _remaining_recent_query(q, [], cfg).start_ms == 60_000
    assert _remaining_recent_query(q, [mb(60_000)], cfg).start_ms == 120_000
    assert _remaining_recent_query(query(end=60_000), [], cfg) is None
    assert (
        _merge_bars([mb(0, close=1)], [mb(0, close=2), mb(60_000, close=3)])[0].close
        == 2
    )
    assert (
        _market_bar_from_core(
            Bar(0, 1, 2, 0.5, 1.5, None, None), query=q, source_transport="x"
        ).time_close
        == 59_999
    )


def test_service_fetch_and_materialize_tail_append_and_unsupported(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = MarketDataConfig(
        storage=StorageConfig(cache_dir=tmp_path),
        history=HistoryConfig(enabled=False, archive_first=False),
    )
    service = MarketDataService(cfg)
    base_q = query(end=180_000)
    service.store.segments.replace_all(
        [mb(0), mb(60_000)],
        exchange="binance",
        market="spot",
        symbol="BTCUSDT",
        timeframe="1m",
    )
    original_fetch = MarketDataService._fetch_from_sources
    with pytest.raises(MDUnsupportedFeature, match="Unsupported provider exchange"):
        original_fetch(service, query(exchange="bitstamp"))
    monkeypatch.setattr(
        MarketDataService,
        "_fetch_from_sources",
        lambda self, q, progress_callback=None: (
            [mb(120_000, close=3.0)] if q.start_ms == 120_000 else []
        ),
    )
    assert service._ensure_stored(base_q) is True
    assert [bar.time for bar in service._stored_bars(base_q)] == [0, 60_000, 120_000]
    assert service.materialize_bars(base_q)["ok"] is True
    assert len(service.precompute_bars(base_q).bars) == 3


def test_service_derived_materialize_no_rows_and_month_coverage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = MarketDataConfig(
        storage=StorageConfig(cache_dir=tmp_path),
        history=HistoryConfig(enabled=True, archive_first=False, base_timeframe="1m"),
    )
    service = MarketDataService(cfg)
    monkeypatch.setattr(
        MarketDataService,
        "_fetch_from_sources",
        lambda self, q, progress_callback=None: [],
    )
    result = service.materialize_bars(query("15m", end=900_000))
    assert result["changed"] is False
    assert result["rows_written"] == 0
    assert _coverage_complete([mb(0, timeframe="1M")], query("1M", end=1)) is True
    assert _can_derive_from_base(query("1m"), parse_timeframe("15m")) is False
    with pytest.raises(MDUnsupportedFeature):
        _aggregate_market_bars([mb(0)], query=query("1M", end=1))


def test_segment_store_integrity_streaming_and_vacuum(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = SegmentStore(tmp_path)
    assert (
        store.read_all(
            exchange="binance", market="spot", symbol="BTCUSDT", timeframe="1m"
        )
        == []
    )
    with pytest.raises(MDUnsupportedFeature):
        SegmentStore(tmp_path / "bad", data_format="bad")  # type: ignore[arg-type]

    manifest = store.replace_all(
        [mb(0), mb(60_000)],
        exchange="binance",
        market="spot",
        symbol="BTCUSDT",
        timeframe="1m",
    )
    assert manifest.checksum == bars_checksum([mb(0), mb(60_000)])
    assert store.get(("binance", "spot", "BTCUSDT", "1m", "trade_kline", 0)).time == 0
    assert store.get(("binance", "spot", "BTCUSDT", "1m", "trade_kline", 999)) is None
    assert (
        store.compact(
            exchange="binance", market="spot", symbol="BTCUSDT", timeframe="1m"
        ).rows_count
        == 2
    )
    assert store.upsert_closed(mb(120_000)).end_time == 120_000

    stale = next(tmp_path.glob("v1/**/bars.csv")).with_suffix(".parquet")
    stale.write_text("stale")
    assert store.vacuum()["removed_stale_data_files"] == 1

    with pytest.raises(MDInvalidExchangeResponse, match="strictly ordered"):
        store.replace_all_stream(
            [mb(60_000), mb(0)],
            exchange="binance",
            market="spot",
            symbol="BTCUSDT",
            timeframe="1m",
        )
    assert not list(tmp_path.glob("v1/**/.bars.csv.*"))

    manifest_path = next(tmp_path.glob("v1/**/manifest.json"))
    data = json.loads(manifest_path.read_text())
    data["runtime_contract_version"] = "bad"
    manifest_path.write_text(json.dumps(data))
    with pytest.raises(MDInvalidExchangeResponse, match="runtime contract"):
        store.read_all(
            exchange="binance", market="spot", symbol="BTCUSDT", timeframe="1m"
        )
    data["runtime_contract_version"] = RUNTIME_CONTRACT_VERSION
    data["checksum"] = "bad"
    manifest_path.write_text(json.dumps(data))
    # Checksum mismatch now auto-heals instead of raising
    result = store.read_all(
        exchange="binance", market="spot", symbol="BTCUSDT", timeframe="1m"
    )
    assert len(result) > 0
    # Manifest should be updated with correct checksum
    healed = json.loads(manifest_path.read_text())
    assert healed["checksum"] != "bad"

    assert store._parse_bool(None) is True
    assert store._parse_bool("") is True
    assert store._parse_bool(False) is False
    assert store._parse_bool(0) is False
    assert store._parse_bool("yes") is True
    assert store._parse_bool(object()) is True


def test_segment_store_parquet_read_unsupported(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = SegmentStore(tmp_path)
    key = {
        "exchange": "binance",
        "market": "spot",
        "symbol": "BTCUSDT",
        "timeframe": "1m",
        "source_kind": "trade_kline",
    }
    data_path, manifest_path = store._paths(**key, data_format="parquet")
    data_path.parent.mkdir(parents=True)
    data_path.write_bytes(b"not parquet")
    manifest_path.write_text(
        json.dumps(
            {
                "runtime_contract_version": RUNTIME_CONTRACT_VERSION,
                "schema_version": "stage-d-parquet-1",
                "exchange": "binance",
                "market": "spot",
                "symbol": "BTCUSDT",
                "timeframe": "1m",
                "source_transport": "ws",
                "source_kind": "trade_kline",
                "rows_count": 1,
                "start_time": 0,
                "end_time": 0,
                "checksum": "x",
                "data_format": "parquet",
            }
        )
    )
    monkeypatch.setattr(
        "marketdata_provider.store.segment_store.importlib.util.find_spec",
        lambda name: None,
    )
    with pytest.raises(MDUnsupportedFeature, match="Parquet"):
        store.read_all(
            exchange="binance", market="spot", symbol="BTCUSDT", timeframe="1m"
        )


def test_archive_and_trade_helpers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert (
        binance_archive.fetch_binance_archive_bars(
            symbol="BTCUSDT",
            market="bad",
            timeframe="1m",
            start=1,
            end=1,
            cache_dir=tmp_path,
        )
        == []
    )
    assert (
        binance_archive._dedupe_sorted([Bar(1, 1, 1, 1, 1), Bar(1, 2, 2, 2, 2)])[0].open
        == 2
    )
    assert binance_archive._range_coverage_complete(
        [Bar(0, 1, 1, 1, 1)], start=0, end=60_000, duration=60_000
    )
    assert (
        binance_archive.fill_binance_archive_gaps(
            [],
            symbol="BTCUSDT",
            market="spot",
            timeframe="1m",
            start=None,
            end=1,
            cache_dir=tmp_path,
        )
        == []
    )
    assert (
        binance_archive.fill_binance_archive_gaps(
            [Bar(0, 1, 1, 1, 1)],
            symbol="BTCUSDT",
            market="bad",
            timeframe="1m",
            start=0,
            end=120_000,
            cache_dir=tmp_path,
        )[0].time
        == 0
    )

    archive_zip = (
        tmp_path / "spot" / "daily" / "BTCUSDT" / "1m" / "BTCUSDT-1m-1970-01-01.zip"
    )
    archive_zip.parent.mkdir(parents=True)
    with ZipFile(archive_zip, "w") as zf:
        zf.writestr("bad.txt", "ignored")
    assert (
        binance_archive._load_archive_file(
            symbol="BTCUSDT",
            market="spot",
            timeframe="1m",
            start=0,
            end=60_000,
            period="daily",
            suffix="1970-01-01",
            cache_dir=tmp_path,
        )
        == []
    )
    archive_zip.unlink()
    with ZipFile(archive_zip, "w") as zf:
        zf.writestr(
            "data.csv",
            "open,high\nnotint,1\n0,1,2,0.5,1.5,10,59999\n60000,2,3,1,2.5,5,119999\n",
        )
    bars = binance_archive._load_archive_file(
        symbol="BTCUSDT",
        market="spot",
        timeframe="1m",
        start=0,
        end=120_000,
        period="daily",
        suffix="1970-01-01",
        cache_dir=tmp_path,
    )
    assert [bar.time for bar in bars] == [0, 60_000]

    with pytest.raises(MDInvalidExchangeResponse):
        binance_trades.normalize_binance_agg_trades({})
    with pytest.raises(MDInvalidExchangeResponse):
        binance_trades.normalize_binance_agg_trades([{"a": "bad"}])
    trades = binance_trades.normalize_binance_agg_trades(
        [
            {"a": 2, "T": 10, "p": "1", "q": "2", "m": True},
            {"a": 1, "T": 9, "p": "1", "q": "1", "m": False},
        ]
    )
    assert [trade.trade_id for trade in trades] == [1, 2]


def test_live_client_error_paths(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(Exception, match="Unsupported Binance"):
        PublicKlineWebSocketClient(
            exchange="binance", market="coinm", symbol="BTCUSDT", timeframe="1m"
        )
    with pytest.raises(Exception, match="Unsupported Bybit"):
        PublicKlineWebSocketClient(
            exchange="bybit", market="inverse", symbol="BTCUSDT", timeframe="1m"
        )
    with pytest.raises(MDUnsupportedFeature):
        PublicKlineWebSocketClient(exchange="kraken", market="spot", symbol="BTCUSDT", timeframe="1m")  # type: ignore[arg-type]

    monkeypatch.setenv("MARKETDATA_ALLOW_STREAM", "1")
    monkeypatch.setattr(
        "marketdata_provider.streaming.live.importlib.util.find_spec", lambda name: None
    )
    client = PublicKlineWebSocketClient(
        exchange="binance", market="spot", symbol="BTCUSDT", timeframe="1m"
    )

    async def collect() -> None:
        with pytest.raises(MDNetworkUnavailable, match="websockets"):
            async for _event in client.events(max_messages=1):
                pass

    asyncio.run(collect())


def test_package_main_entrypoint(monkeypatch: pytest.MonkeyPatch) -> None:
    called: list[bool] = []

    def fake_main() -> int:
        called.append(True)
        return 0

    monkeypatch.setattr("marketdata_provider.cli.main.main", fake_main)
    with pytest.raises(SystemExit) as exc:
        runpy.run_module("marketdata_provider.__main__", run_name="__main__")
    assert exc.value.code == 0
    assert called == [True]


def test_factories_store_provider_and_live_adapter(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import marketdata_provider.factories as factories
    from marketdata_provider.contracts.bar import Bar as ContractBar
    from marketdata_provider.contracts.series import BarSeries, CoverageReport
    from marketdata_provider.factories import (
        create_candle_store,
        create_live_kline_client,
        create_provider,
    )

    inst = InstrumentKey("binance", "spot", "BTCUSDT")
    tf = parse_timeframe("1m")
    q = BarQuery(inst, tf, 0, 60_000)
    contract_bar = ContractBar(inst, tf, 0, 59_999, 1, 2, 0.5, 1.5, 10, True)
    series = BarSeries(q, (contract_bar,), CoverageReport(0, 60_000, 0, 59_999))
    store = create_candle_store(
        MarketDataConfig(storage=StorageConfig(cache_dir=tmp_path / "store"))
    )
    assert store.write(series).rows_written == 1
    assert store.read(q).bars[0].close == 1.5
    assert store.coverage(q).is_complete
    assert store.latest_bar_time(q) == 0
    assert store.write(series).rows_written == 0

    bad_series = BarSeries(
        q,
        (
            ContractBar(
                InstrumentKey("bybit", "spot", "BTCUSDT"),
                tf,
                0,
                59_999,
                1,
                2,
                0.5,
                1.5,
                10,
                True,
            ),
        ),
        CoverageReport(0, 60_000, 0, 59_999),
    )
    assert store.write(bad_series).success is False

    class FakeService:
        def __init__(self, config: MarketDataConfig) -> None:
            self.config = config

        def fetch_bars(self, query: BarQuery, progress_callback=None):
            return series

    monkeypatch.setattr(factories, "MarketDataService", FakeService)
    assert create_provider(MarketDataConfig()).fetch_bars(q) is series
    with pytest.raises(MDUnsupportedFeature):
        create_provider(MarketDataConfig(default_exchange="bitstamp")).fetch_bars(q)

    raw_events = []

    class FakeRawClient:
        async def events(self, *, max_messages=None, timeout_s=None):
            from marketdata_provider.streaming.live import (
                LiveKlineEvent as RawLiveEvent,
            )
            from marketdata_provider.streaming.kline import KlineUpdate

            update = KlineUpdate(
                "binance",
                "spot",
                "BTCUSDT",
                "1m",
                7,
                0,
                59_999,
                1,
                2,
                0.5,
                1.5,
                10,
                True,
                8,
            )
            yield RawLiveEvent(update, {"x": 1})

    monkeypatch.setattr(
        factories,
        "PublicKlineWebSocketClient",
        lambda **kwargs: FakeRawClient(),
        raising=False,
    )
    # Patch import target used inside factory function.
    import marketdata_provider.streaming as streaming

    monkeypatch.setattr(
        streaming, "PublicKlineWebSocketClient", lambda **kwargs: FakeRawClient()
    )
    client = create_live_kline_client(MarketDataConfig(), instrument=inst, timeframe=tf)

    async def collect() -> None:
        async for event in client.events(max_messages=1, timeout_s=1):
            raw_events.append(event)

    asyncio.run(collect())
    assert raw_events[0].bar.close == 1.5
    assert raw_events[0].raw_payload == {"x": 1}


def test_live_events_with_fake_websockets(monkeypatch: pytest.MonkeyPatch) -> None:
    import sys
    import types

    monkeypatch.setenv("MARKETDATA_ALLOW_STREAM", "1")
    monkeypatch.setattr(
        "marketdata_provider.streaming.live.importlib.util.find_spec",
        lambda name: object(),
    )

    class FakeWS:
        def __init__(self, messages: list[str]) -> None:
            self.messages = messages
            self.sent: list[str] = []

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def send(self, data: str) -> None:
            self.sent.append(data)

        async def recv(self) -> str:
            if not self.messages:
                raise asyncio.TimeoutError
            return self.messages.pop(0)

    binance_payload = json.dumps(
        {
            "e": "kline",
            "E": 10,
            "s": "BTCUSDT",
            "k": {
                "s": "BTCUSDT",
                "i": "1m",
                "t": 0,
                "T": 59_999,
                "o": "1",
                "h": "2",
                "l": "0.5",
                "c": "1.5",
                "v": "10",
                "x": True,
            },
        }
    )
    created: list[FakeWS] = []
    websockets = types.ModuleType("websockets")

    def connect(url: str, **kwargs):
        ws = FakeWS([binance_payload])
        created.append(ws)
        return ws

    websockets.connect = connect
    monkeypatch.setitem(sys.modules, "websockets", websockets)
    client = PublicKlineWebSocketClient(
        exchange="binance", market="spot", symbol="BTCUSDT", timeframe="1m"
    )

    async def collect_binance():
        events = []
        async for event in client.events(max_messages=1, timeout_s=1):
            events.append(event)
        return events

    events = asyncio.run(collect_binance())
    assert events[0].update.close == 1.5

    bybit_payloads = [
        json.dumps({"op": "subscribe", "success": True}),
        json.dumps(
            {
                "topic": "kline.1.BTCUSDT",
                "ts": 11,
                "data": [
                    {
                        "start": 0,
                        "end": 59_999,
                        "open": "1",
                        "high": "2",
                        "low": "0.5",
                        "close": "1.6",
                        "volume": "2",
                        "confirm": True,
                    }
                ],
            }
        ),
    ]

    def connect_bybit(url: str, **kwargs):
        ws = FakeWS(list(bybit_payloads))
        created.append(ws)
        return ws

    websockets.connect = connect_bybit
    bybit = PublicKlineWebSocketClient(
        exchange="bybit", market="linear", symbol="BTCUSDT", timeframe="1m"
    )

    async def collect_bybit():
        events = []
        async for event in bybit.events(max_messages=1, timeout_s=1):
            events.append(event)
        return events

    bybit_events = asyncio.run(collect_bybit())
    assert bybit_events[0].update.close == 1.6
    assert created[-1].sent

    def connect_oserror(url: str, **kwargs):
        raise OSError("down")

    websockets.connect = connect_oserror
    with pytest.raises(MDNetworkUnavailable, match="connection failed"):
        asyncio.run(collect_binance())


def test_cli_source_commands_and_store_commands(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    from marketdata_provider.cli import main as cli

    source = tmp_path / "bars.csv"
    source.write_text(
        "time,open,high,low,close,volume,time_close\n0,1,2,0.5,1.5,10,59999\n60000,2,3,1,2.5,5,119999\n"
    )
    cache_dir = tmp_path / "cache"
    assert (
        cli.main(
            [
                "validate",
                "--path",
                str(source),
                "--timeframe",
                "1m",
                "--symbol",
                "OFFLINE:BTCUSDT",
            ]
        )
        == 0
    )
    assert '"bars": 2' in capsys.readouterr().out
    assert (
        cli.main(
            [
                "coverage",
                "--path",
                str(source),
                "--timeframe",
                "1m",
                "--symbol",
                "OFFLINE:BTCUSDT",
            ]
        )
        == 0
    )
    assert '"gaps": 0' in capsys.readouterr().out
    assert (
        cli.main(
            [
                "fetch",
                "--path",
                str(source),
                "--timeframe",
                "1m",
                "--symbol",
                "BINANCE:BTCUSDT",
                "--cache-dir",
                str(cache_dir),
            ]
        )
        == 0
    )
    assert '"ok": true' in capsys.readouterr().out
    out_csv = tmp_path / "out.csv"
    assert (
        cli.main(
            [
                "export",
                "--cache",
                "--cache-dir",
                str(cache_dir),
                "--timeframe",
                "1m",
                "--symbol",
                "BINANCE:BTCUSDT",
                "--output",
                str(out_csv),
                "--format",
                "csv",
            ]
        )
        == 0
    )
    assert out_csv.read_text().startswith("time,open")
    out_json = tmp_path / "out.json"
    assert (
        cli.main(
            [
                "export",
                "--path",
                str(source),
                "--timeframe",
                "1m",
                "--symbol",
                "OFFLINE:BTCUSDT",
                "--output",
                str(out_json),
                "--format",
                "json",
            ]
        )
        == 0
    )
    assert json.loads(out_json.read_text())[0]["close"] == 1.5
    assert (
        cli.main(["validate", "--timeframe", "1m", "--symbol", "BINANCE:BTCUSDT"]) == 2
    )
    assert "Choose an explicit data source" in capsys.readouterr().out

    store_dir = tmp_path / "store"
    store = SegmentStore(store_dir)
    store.replace_all(
        [mb(0)], exchange="binance", market="spot", symbol="BTCUSDT", timeframe="1m"
    )
    assert (
        cli.main(
            [
                "compact",
                "--store-dir",
                str(store_dir),
                "--symbol",
                "BINANCE:BTCUSDT",
                "--timeframe",
                "1m",
            ]
        )
        == 0
    )
    assert '"rows_count": 1' in capsys.readouterr().out
    assert (
        cli.main(
            [
                "current",
                "--store-dir",
                str(store_dir),
                "--symbol",
                "BINANCE:BTCUSDT",
                "--timeframe",
                "1m",
            ]
        )
        == 0
    )
    assert '"current": null' in capsys.readouterr().out
    assert cli.main(["checkpoints", "--store-dir", str(store_dir)]) == 0
    assert '"checkpoints"' in capsys.readouterr().out
    assert cli.main(["repair-logs", "--store-dir", str(store_dir)]) == 0
    assert '"logs"' in capsys.readouterr().out
    mock_events = tmp_path / "events.ndjson"
    mock_events.write_text(
        json.dumps(
            {
                "exchange": "binance",
                "market": "spot",
                "symbol": "BTCUSDT",
                "timeframe": "1m",
                "event_time": 1,
                "open_time": 0,
                "close_time": 59_999,
                "open": 1,
                "high": 2,
                "low": 0.5,
                "close": 1.5,
                "volume": 10,
                "is_closed": True,
            }
        )
        + "\n"
    )
    stream_dir = tmp_path / "stream-store"
    assert (
        cli.main(
            [
                "stream",
                "--store-dir",
                str(stream_dir),
                "--symbol",
                "BINANCE:BTCUSDT",
                "--timeframe",
                "1m",
                "--mock-events",
                str(mock_events),
                "--queue-maxsize",
                "2",
            ]
        )
        == 0
    )
    assert '"processed": 1' in capsys.readouterr().out
    bad_events = tmp_path / "bad_events.ndjson"
    bad_events.write_text('{"x":1}\n')
    assert (
        cli.main(
            [
                "stream",
                "--store-dir",
                str(tmp_path / "bad-stream-store"),
                "--symbol",
                "BINANCE:BTCUSDT",
                "--timeframe",
                "1m",
                "--mock-events",
                str(bad_events),
            ]
        )
        == 2
    )
    assert "Unsupported mock stream event" in capsys.readouterr().out
    assert (
        cli.main(["ws-info", "--symbol", "BINANCE:BTCUSDT", "--timeframe", "1m"]) == 0
    )
    assert "wss://stream.binance" in capsys.readouterr().out
    assert (
        cli.main(
            [
                "precompute",
                "--store-dir",
                str(store_dir),
                "--symbol",
                "BINANCE:BTCUSDT",
                "--timeframe",
                "1m",
                "--start",
                "0",
                "--end",
                "60000",
            ]
        )
        == 0
    )
    assert '"store_dir"' in capsys.readouterr().out


def test_async_http_client_retry_and_error_paths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import httpx
    import marketdata_provider.transport.async_client as ac

    assert ac.RetryConfig(base_sec=0.0).backoff(3) == 0.0

    class FakeResponse:
        def __init__(
            self,
            status_code: int,
            payload: object = None,
            headers: dict[str, str] | None = None,
        ) -> None:
            self.status_code = status_code
            self._payload = payload or {"ok": True}
            self.headers = headers or {}

        def raise_for_status(self) -> None:
            if self.status_code >= 400:
                raise httpx.HTTPStatusError(
                    "bad",
                    request=httpx.Request("GET", "https://x"),
                    response=httpx.Response(self.status_code),
                )

        def json(self) -> object:
            return self._payload

    class FakeAsyncClient:
        calls = 0

        def __init__(self, **kwargs):
            self.closed = False

        async def aclose(self):
            self.closed = True

        async def get(self, path, params=None):
            self.__class__.calls += 1
            if self.__class__.calls == 1:
                return FakeResponse(500)
            return FakeResponse(200, {"ok": True, "path": path})

    async def run_success() -> None:
        monkeypatch.setattr(ac.httpx, "AsyncClient", FakeAsyncClient)

        async def no_sleep(_value: float) -> None:
            return None

        monkeypatch.setattr(ac.asyncio, "sleep", no_sleep)
        async with ac.MarketDataHTTPClient("https://example.test/") as client:
            assert await client.get_json("/bars", {"a": 1}) == {
                "ok": True,
                "path": "/bars",
            }
        with pytest.raises(RuntimeError):
            await ac.MarketDataHTTPClient("https://example.test").get_json("/bars")

    asyncio.run(run_success())

    class RateLimitClient(FakeAsyncClient):
        calls = 0

        async def get(self, path, params=None):
            self.__class__.calls += 1
            return FakeResponse(429, headers={"Retry-After": "0"})

    async def run_rate_limit() -> None:
        monkeypatch.setattr(ac.httpx, "AsyncClient", RateLimitClient)

        async def no_sleep(_value: float) -> None:
            return None

        monkeypatch.setattr(ac.asyncio, "sleep", no_sleep)
        async with ac.MarketDataHTTPClient(
            "https://example.test", retry_config=ac.RetryConfig(max_retries=0)
        ) as client:
            with pytest.raises(RuntimeError, match="HTTP request failed"):
                await client.get_json("/bars")

    asyncio.run(run_rate_limit())


def test_distribution_cli_paths(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    from marketdata_provider import distribution

    root = tmp_path / "root"
    root.mkdir()
    (root / "a.py").write_text("print('x')\n")
    (root / "__pycache__").mkdir()
    (root / "__pycache__" / "a.pyc").write_bytes(b"x")
    assert distribution.main(["manifest", "--root", str(root)]) == 0
    assert '"forbidden_count": 0' in capsys.readouterr().out
    out = tmp_path / "pkg.zip"
    assert (
        distribution.main(
            [
                "build-zip",
                "--root",
                str(root),
                "--output",
                str(out),
                "--archive-root",
                "pkg",
            ]
        )
        == 0
    )
    assert out.exists()
