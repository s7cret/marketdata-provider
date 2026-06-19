from __future__ import annotations

import asyncio
import json
import runpy
import warnings
import sys
import types
import zipfile
from argparse import Namespace
from pathlib import Path
from typing import Any

import httpx
import pytest

from marketdata_provider.config import BinanceConfig, MarketDataConfig, StorageConfig
from marketdata_provider.contracts import BarQuery, InstrumentKey, parse_timeframe
from marketdata_provider.contracts.bar import Bar as ContractBar
from marketdata_provider.contracts.footprint import (
    AggTrade,
    FootprintQuery,
    FootprintSeries,
)
from marketdata_provider.contracts.series import BarSeries, CoverageReport
from marketdata_provider.core.bar import Bar, MarketBar
from marketdata_provider.errors import (
    MDNetworkUnavailable,
    MDPaginationStalled,
    MDSymbolUnsupported,
    MDUnsupportedFeature,
)


def _bar(time: int = 0, close: float = 1.5) -> Bar:
    return Bar(
        time=time,
        open=1.0,
        high=max(2.0, close),
        low=0.5,
        close=close,
        volume=1.0,
        time_close=time + 59_999,
    )


def _mb(
    time: int = 0, close: float = 1.5, *, closed: bool = True, market: str = "spot"
) -> MarketBar:
    return MarketBar(
        time=time,
        open=1.0,
        high=max(2.0, close),
        low=0.5,
        close=close,
        volume=1.0,
        time_close=time + 59_999,
        exchange="binance",
        market=market,
        symbol="BTCUSDT",
        timeframe="1m",
        source_transport="ws",
        source_kind="trade_kline",
        is_closed=closed,
        downloaded_at=time + 60_000,
    )


def _contract_query(start: int = 0, end: int = 60_000) -> BarQuery:
    return BarQuery(
        InstrumentKey("binance", "spot", "BTCUSDT"), parse_timeframe("1m"), start, end
    )


def test_cli_source_visual_branches_and_mock_events(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from marketdata_provider.cli import main as cli

    live_args = Namespace(
        path=None,
        symbol="BINANCE:BTCUSDT",
        timeframe="1m",
        start=0,
        end=60_000,
        max_bars=None,
        exchange=None,
        market=None,
        cache_dir=tmp_path,
        cache=False,
        live=True,
    )
    monkeypatch.delenv("MARKETDATA_ALLOW_NETWORK", raising=False)
    monkeypatch.delenv("RUN_MARKETDATA_NETWORK_TESTS", raising=False)
    with pytest.raises(MDNetworkUnavailable):
        cli._bars_from_source(live_args)

    monkeypatch.setenv("MARKETDATA_ALLOW_NETWORK", "1")
    live_args.end = None
    with pytest.raises(MDUnsupportedFeature, match="unbounded"):
        cli._bars_from_source(live_args)

    live_args.end = 60_000
    monkeypatch.setattr(cli, "binance_get_bars_sync", lambda *args, **kwargs: [_bar(0)])
    assert cli._bars_from_source(live_args)[0].time == 0
    live_args.symbol = "BYBIT:BTCUSDT"
    live_args.market = "linear"
    monkeypatch.setattr(
        cli, "bybit_get_bars_sync", lambda *args, **kwargs: [_bar(60_000)]
    )
    assert cli._bars_from_source(live_args)[0].time == 60_000

    monkeypatch.setattr(cli, "_bars_from_source", lambda args: [_bar(0), _bar(60_001)])
    export_path = tmp_path / "bars.json"
    assert cli._cmd_export(Namespace(output=export_path, format="json")) == 0
    assert json.loads(export_path.read_text())[0]["time"] == 0
    with pytest.raises(MDUnsupportedFeature, match="Unsupported export"):
        cli._cmd_export(Namespace(output=tmp_path / "bars.bad", format="xml"))
    assert cli._cmd_coverage(Namespace()) == 0
    assert '"gaps": 1' in capsys.readouterr().out

    direct = {
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
        "volume": 1,
        "is_closed": True,
    }
    binance = {
        "e": "kline",
        "E": 1,
        "s": "BTCUSDT",
        "k": {
            "s": "BTCUSDT",
            "i": "1m",
            "t": 60_000,
            "T": 119_999,
            "o": "1",
            "h": "2",
            "l": "0.5",
            "c": "1.5",
            "v": "1",
            "x": True,
        },
    }
    bybit = {
        "topic": "kline.1.BTCUSDT",
        "data": [
            {
                "start": 120_000,
                "end": 179_999,
                "open": "1",
                "high": "2",
                "low": "0.5",
                "close": "1.5",
                "volume": "1",
                "confirm": True,
            }
        ],
    }
    events_path = tmp_path / "events.jsonl"
    events_path.write_text(
        "\n" + "\n".join(json.dumps(obj) for obj in (direct, binance, bybit)) + "\n"
    )
    assert len(cli._load_mock_events(events_path, market="spot")) == 3
    events_path.write_text(json.dumps({"bad": True}) + "\n")
    with pytest.raises(MDUnsupportedFeature, match="Unsupported mock"):
        cli._load_mock_events(events_path, market="spot")

    monkeypatch.setenv("MARKETDATA_ALLOW_STREAM", "1")
    with pytest.raises(MDUnsupportedFeature, match="Live WebSocket"):
        cli._cmd_stream(
            Namespace(
                symbol="BINANCE:BTCUSDT",
                timeframe="1m",
                exchange=None,
                market=None,
                store_dir=tmp_path,
                mock_events=None,
                reconnect_after=None,
                queue_maxsize=None,
            )
        )

    with pytest.raises(MDUnsupportedFeature, match="precompute"):
        cli._cmd_precompute(
            Namespace(
                symbol="BINANCE:BTCUSDT",
                timeframe="1m",
                exchange=None,
                market=None,
                store_dir=tmp_path,
                start=None,
                end=60_000,
            )
        )


def test_cli_main_entrypoint_and_parser_error_branch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from marketdata_provider.cli import main as cli

    monkeypatch.setattr(
        cli,
        "_bars_from_source",
        lambda args: (_ for _ in ()).throw(MDUnsupportedFeature("boom")),
    )
    assert (
        cli.main(
            [
                "validate",
                "--symbol",
                "BINANCE:BTCUSDT",
                "--timeframe",
                "1m",
                "--live",
                "--start",
                "0",
                "--end",
                "60000",
            ]
        )
        == 2
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "marketdata",
            "validate",
            "--symbol",
            "BINANCE:BTCUSDT",
            "--timeframe",
            "1m",
            "--live",
            "--start",
            "0",
            "--end",
            "60000",
        ],
    )
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        with pytest.raises(SystemExit) as exc:
            runpy.run_module("marketdata_provider.cli.main", run_name="__main__")
    assert exc.value.code == 2


def test_timeframe_symbols_and_small_utility_branches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import marketdata_provider.contracts.timeframe as tf_module
    from marketdata_provider.distribution import _should_include
    from marketdata_provider.quality import (
        _python_files,
        architecture_report,
        duplicate_report,
    )
    from marketdata_provider.release import release_report
    from marketdata_provider.symbols import normalize_symbol
    from marketdata_provider.transport.async_client import RetryConfig

    monkeypatch.setattr(tf_module, "canonical_timeframe", lambda value: "2W")
    with pytest.raises(tf_module.InvalidTimeframeError, match="unsupported canonical"):
        tf_module.parse_timeframe("2W")
    with pytest.raises(MDSymbolUnsupported, match="Unsupported exchange"):
        normalize_symbol("BITSTAMP:BTCUSDT")
    assert normalize_symbol("BINANCE:BTCUSD").exchange_symbol == "BTCUSD"
    assert RetryConfig(base_sec=-1).backoff(0) == 0.0
    assert _should_include(Path("marketdata_provider/x.py"))
    assert not _should_include(Path(".pytest_cache/x"))
    assert _python_files(Path("missing")) == []
    assert duplicate_report(Path("missing")).duplicate_group_count == 0
    assert architecture_report(Path("missing")).oversized_count == 0
    assert release_report(Path.cwd()).package_version == "4.0.0"


class FakeResponse:
    def __init__(
        self, status_code: int, payload: Any, *, raise_http: bool = False
    ) -> None:
        self.status_code = status_code
        self._payload = payload
        self.headers: dict[str, str] = {}
        self._raise_http = raise_http

    def json(self) -> Any:
        return self._payload

    def raise_for_status(self) -> None:
        if self._raise_http or self.status_code >= 400:
            raise httpx.HTTPStatusError(
                "bad",
                request=httpx.Request("GET", "https://x"),
                response=httpx.Response(self.status_code),
            )


class FakeClient:
    def __init__(self, responses: list[FakeResponse]) -> None:
        self.responses = list(responses)

    def get(self, url: str, params: dict[str, Any]) -> FakeResponse:
        if not self.responses:
            raise httpx.ConnectError("empty")
        return self.responses.pop(0)

    def __enter__(self) -> "FakeClient":
        return self

    def __exit__(self, *args: Any) -> None:
        return None


def test_exchange_provider_remaining_http_and_async_branches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from marketdata_provider.config import BinanceConfig, BybitConfig
    from marketdata_provider.exchanges.binance import provider as bp
    from marketdata_provider.exchanges.bybit import provider as yp
    from marketdata_provider.exchanges.bybit.rest import BYBIT_ENDPOINT

    monkeypatch.setattr(bp.time, "sleep", lambda *_: None)
    assert (
        bp._server_time_ms(
            FakeClient([FakeResponse(200, {"serverTime": 7})]),
            "https://x",
            "coinm",
            max_retries=0,
        )
        == 7
    )
    monkeypatch.setattr(
        bp.httpx,
        "Client",
        lambda **kwargs: FakeClient(
            [FakeResponse(200, {"serverTime": 1000}), FakeResponse(200, [])]
        ),
    )
    assert (
        bp.binance_get_bars_sync(
            "BTCUSDT", "1m", 0, 60_000, BinanceConfig(), market="spot", max_retries=0
        )
        == []
    )
    assert (
        asyncio.run(
            bp.binance_get_bars(
                "BTCUSDT",
                "1m",
                0,
                60_000,
                BinanceConfig(),
                market="spot",
                max_retries=0,
            )
        )
        == []
    )

    monkeypatch.setattr(yp.time, "sleep", lambda *_: None)
    with pytest.raises(MDNetworkUnavailable):
        yp._get_json(FakeClient([FakeResponse(500, {})]), "u", {}, max_retries=0)
    with pytest.raises(MDNetworkUnavailable):
        yp._get_json(FakeClient([]), "u", {}, max_retries=0)
    payload = {"retCode": 0, "result": {"timeNano": "1000000000"}}
    monkeypatch.setattr(
        yp.httpx,
        "Client",
        lambda **kwargs: FakeClient(
            [
                FakeResponse(200, payload),
                FakeResponse(200, {"retCode": 0, "result": {"list": []}}),
            ]
        ),
    )
    assert (
        yp.bybit_get_bars_sync(
            "BTCUSDT", "1m", 0, 60_000, BybitConfig(), market="linear", max_retries=0
        )
        == []
    )
    assert (
        asyncio.run(
            yp.bybit_get_bars(
                "BTCUSDT",
                "1m",
                0,
                60_000,
                BybitConfig(),
                market="linear",
                max_retries=0,
            )
        )
        == []
    )
    assert BYBIT_ENDPOINT.endswith("/v5/market/kline")


def test_binance_archive_and_trade_remaining_branches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from marketdata_provider.exchanges.binance import archive as ba
    from marketdata_provider.exchanges.binance import trades

    with monkeypatch.context() as m:
        m.setattr(ba, "timeframe_ms", lambda timeframe: None)
        assert (
            ba.fetch_binance_archive_bars(
                symbol="BTCUSDT",
                market="spot",
                timeframe="1m",
                start=0,
                end=1,
                cache_dir=tmp_path,
            )
            == []
        )
    assert (
        ba.fill_binance_archive_gaps(
            [_bar(0)],
            symbol="BTCUSDT",
            market="spot",
            timeframe="1m",
            start=0,
            end=60_000,
            cache_dir=tmp_path,
        )[0].time
        == 0
    )
    assert (
        ba._load_archive_file(
            symbol="BTCUSDT",
            market="bad",
            timeframe="1m",
            start=0,
            end=60_000,
            period="daily",
            suffix="x",
            cache_dir=tmp_path,
        )
        == []
    )

    zpath = tmp_path / "spot" / "daily" / "BTCUSDT" / "1m" / "BTCUSDT-1m-1970-01-01.zip"
    zpath.parent.mkdir(parents=True)
    with zipfile.ZipFile(zpath, "w") as zf:
        zf.writestr("data.csv", "\n0,1,2,0.5,1.5,1,59999\n")
    bars = ba._load_archive_file(
        symbol="BTCUSDT",
        market="spot",
        timeframe="1m",
        start=0,
        end=60_000,
        period="daily",
        suffix="1970-01-01",
        cache_dir=tmp_path,
    )
    assert len(bars) == 1

    monkeypatch.setattr(trades.time, "sleep", lambda *_: None)
    assert trades.BINANCE_AGG_TRADES_ENDPOINTS["spot"].endswith("/api/v3/aggTrades")
    assert trades.BINANCE_AGG_TRADES_ENDPOINTS["usdm"].endswith("/fapi/v1/aggTrades")
    monkeypatch.setattr(
        trades.httpx, "Client", lambda **kwargs: FakeClient([FakeResponse(200, [])])
    )
    assert (
        trades.binance_get_agg_trades_sync(
            "BTCUSDT.P", 0, 1, BinanceConfig(), market="coinm", max_retries=0
        )
        == []
    )
    with pytest.raises(MDNetworkUnavailable):
        trades._get_json(FakeClient([FakeResponse(429, {})]), "u", {}, max_retries=0)
    with pytest.raises(MDNetworkUnavailable):
        trades._get_json(FakeClient([FakeResponse(500, {})]), "u", {}, max_retries=0)
    with pytest.raises(MDNetworkUnavailable):
        trades._get_json(
            FakeClient([FakeResponse(400, {}, raise_http=True)]), "u", {}, max_retries=0
        )
    monkeypatch.setattr(
        trades.httpx, "Client", lambda **kwargs: FakeClient([FakeResponse(200, [])])
    )
    assert (
        trades.binance_get_agg_trades_sync(
            "BTCUSDT", 0, 1, BinanceConfig(), market="spot", max_retries=0
        )
        == []
    )


def test_footprint_service_and_store_final_branches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from marketdata_provider.footprint.aggregate import aggregate_trades_to_footprint
    from marketdata_provider.footprint.service import (
        FootprintService,
        _coverage_for,
        _day_partition,
    )
    from marketdata_provider.store.footprint_store import FootprintStore

    inst = InstrumentKey("binance", "spot", "BTCUSDT")
    tf = parse_timeframe("1m")
    query = FootprintQuery(
        inst,
        tf,
        0,
        120_000,
        price_bucket=1.0,
        source="auto",
        gap_policy="allow_with_metadata",
    )
    trades = [
        AggTrade(1, -1, 1.1, 1, False),
        AggTrade(2, 0, 1.1, 2, False),
        AggTrade(3, 60_000, 2.1, 3, True),
    ]
    bars = aggregate_trades_to_footprint(trades, query)
    assert len(bars) == 2 and bars[0].levels[0].buy_volume == 2
    assert _coverage_for(query, tuple()).status == "empty"
    assert _day_partition(0) == "day=1970-01-01"

    store = FootprintStore(tmp_path)
    assert (
        store.write(
            FootprintSeries(query, tuple(bars), _coverage_for(query, tuple(bars)))
        ).rows_written
        == 2
    )
    assert store.read(query).bars[0].time == 0
    assert store.coverage(query).is_complete

    service = FootprintService(
        MarketDataConfig(storage=StorageConfig(cache_dir=tmp_path / "svc"))
    )
    storage_query = FootprintQuery(
        inst,
        tf,
        0,
        60_000,
        price_bucket=1.0,
        source="storage",
        gap_policy="allow_with_metadata",
    )
    assert service.fetch_footprint(storage_query).bars == ()
    missing_query = FootprintQuery(
        inst, tf, 0, 60_000, price_bucket=1.0, source="auto", gap_policy="fail"
    )
    monkeypatch.setattr(
        "marketdata_provider.footprint.service.binance_get_agg_trades_sync",
        lambda *args, **kwargs: [],
    )
    with pytest.raises(MDUnsupportedFeature, match="coverage incomplete"):
        service.fetch_footprint(missing_query)
    bad_exchange = FootprintQuery(
        InstrumentKey("bybit", "linear", "BTCUSDT"),
        tf,
        0,
        60_000,
        price_bucket=1.0,
        source="auto",
        gap_policy="allow_with_metadata",
    )
    with pytest.raises(MDUnsupportedFeature, match="Unsupported footprint"):
        service.fetch_footprint(bad_exchange)

    monkeypatch.setattr(
        "marketdata_provider.footprint.service.binance_get_agg_trades_sync",
        lambda *args, **kwargs: [AggTrade(1, 0, 1.0, 1.0, False)],
    )
    fetched = service.fetch_footprint(
        FootprintQuery(
            inst,
            tf,
            0,
            60_000,
            price_bucket=1.0,
            source="provider",
            gap_policy="allow_with_metadata",
        )
    )
    assert fetched.bars[0].trades_count == 1


def test_factories_and_service_remaining_branches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import marketdata_provider.factories as factories
    from marketdata_provider.factories import (
        _CandleStoreAdapter,
        create_footprint_provider,
    )
    from marketdata_provider.service import MarketDataService

    inst = InstrumentKey("binance", "spot", "BTCUSDT")
    query = _contract_query(0, 120_000)
    tf = query.timeframe
    bar = ContractBar(inst, tf, 0, 59_999, 1, 2, 0.5, 1.5, 1, True)
    second = ContractBar(inst, tf, 60_000, 119_999, 1.5, 2.5, 1, 2, 1, True)
    series = BarSeries(query, (bar, second), CoverageReport(0, 120_000, 0, 119_999))

    class FakeFootprintService:
        def __init__(self, config: MarketDataConfig) -> None:
            self.config = config

        def fetch_footprint(self, q: FootprintQuery):
            return "fp"

    monkeypatch.setattr(factories, "FootprintService", FakeFootprintService)
    assert (
        create_footprint_provider(MarketDataConfig()).fetch_footprint(
            types.SimpleNamespace()
        )
        == "fp"
    )

    adapter = _CandleStoreAdapter(types.SimpleNamespace())
    adapter.store.get_market_bars = lambda **kwargs: []
    assert adapter.read(query).bars == ()

    class MinimalStore:
        def __init__(self) -> None:
            self.rows: list[MarketBar] = []
            self.segments = types.SimpleNamespace(
                read_all=lambda **kwargs: self.rows,
                replace_all=lambda bars, **kwargs: self.rows.__setitem__(
                    slice(None), bars
                ),
            )

        def get_market_bars(self, **kwargs):
            return list(self.rows)

        def commit_closed(self, market_bar: MarketBar):
            self.rows.append(market_bar)
            return types.SimpleNamespace(status="committed")

        def upsert_open(self, market_bar: MarketBar):
            self.rows.append(market_bar)
            return types.SimpleNamespace(status="upserted")

    adapter = _CandleStoreAdapter(MinimalStore())
    assert adapter.write(series).rows_written == 2
    assert adapter.latest_bar_time(query) == 120_000
    wrong_query = BarSeries(
        query,
        (
            ContractBar(
                InstrumentKey("bybit", "linear", "BTCUSDT"),
                tf,
                0,
                59_999,
                1,
                2,
                0.5,
                1.5,
                1,
                True,
            ),
        ),
        series.coverage,
    )
    assert adapter.write(wrong_query).success is False

    service = MarketDataService(
        MarketDataConfig(storage=StorageConfig(cache_dir=tmp_path / "svc"))
    )
    bybit_query = BarQuery(InstrumentKey("bybit", "linear", "BTCUSDT"), tf, 0, 60_000)
    assert service._base_query(bybit_query) is bybit_query
    service.store.segments.manifest_for = lambda **kwargs: types.SimpleNamespace(
        start_time=0, end_time=60_000
    )
    assert service._manifest_spans(service.store.segments.manifest_for(), query, 60_000)
    assert not service._manifest_spans(
        types.SimpleNamespace(start_time=None, end_time=60_000), query, 60_000
    )


def test_store_and_repair_remaining_branches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from marketdata_provider.store.candle_store import CandleStore
    from marketdata_provider.store.current_store import CurrentStore
    from marketdata_provider.store.raw_store import RawStore
    from marketdata_provider.store.repair import (
        audit_against_source,
        repair_from_source,
    )
    from marketdata_provider.store.segment_store import SegmentStore

    candle = CandleStore(tmp_path / "candle")
    closed = _mb(0, closed=True)
    open_bar = _mb(60_000, closed=False)
    assert candle.upsert_open(closed).status in {"committed", "duplicate"}
    assert candle.commit_closed(open_bar).status == "upserted"

    current = CurrentStore(tmp_path / "current.sqlite")
    with pytest.raises(RuntimeError):
        with current._connect() as _db:
            raise RuntimeError("rollback")

    raw = RawStore(tmp_path / "raw")
    assert (
        raw.read_batch(
            exchange="binance", market="spot", symbol="BTCUSDT", source_transport="rest"
        )
        == []
    )
    root_dir = raw._dir(
        exchange="binance",
        market="spot",
        symbol="BTCUSDT",
        source_transport="rest",
        source_kind="agg_trades",
    )
    root_dir.mkdir(parents=True)
    (root_dir / "manifest.json").write_text(
        json.dumps(
            {"file_name": "payload.jsonl", "checksum": "bad", "compression": "plain"}
        )
    )
    (root_dir / "payload.jsonl").write_text("{}\n")
    with pytest.raises(MDUnsupportedFeature, match="checksum"):
        raw.read_batch(
            exchange="binance",
            market="spot",
            symbol="BTCUSDT",
            source_transport="rest",
            source_kind="agg_trades",
        )
    monkeypatch.setattr(
        "marketdata_provider.store.raw_store.importlib.util.find_spec",
        lambda name: None,
    )
    with pytest.raises(MDUnsupportedFeature, match="zstd"):
        raw._decompress(b"x", "zstd")

    store = SegmentStore(tmp_path / "segments")
    key = {
        "exchange": "binance",
        "market": "spot",
        "symbol": "BTCUSDT",
        "timeframe": "1m",
    }
    assert store.replace_all([], **key).rows_count == 0
    store.replace_all([_mb(0), _mb(60_000)], **key)
    assert store.read_all(**key, start=60_000, end=120_000)[0].time == 60_000
    assert store.vacuum()["removed_stale_data_files"] == 0
    path, manifest_path = store._paths(
        **key, source_kind="trade_kline", data_format="parquet"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(
        "marketdata_provider.store.segment_store.importlib.util.find_spec",
        lambda name: None,
    )
    with pytest.raises(MDUnsupportedFeature, match="Writing Parquet"):
        store._atomic_write_parquet(path, [_mb(0)])

    source = [_mb(0), _mb(60_000)]
    report = audit_against_source(
        candle,
        source,
        exchange="binance",
        market="spot",
        symbol="BTCUSDT",
        timeframe="1m",
        strict=True,
    )
    assert report.issues
    log = repair_from_source(
        candle,
        source,
        exchange="binance",
        market="spot",
        symbol="BTCUSDT",
        timeframe="1m",
        policy="strict",
    )
    assert log.changed >= 1


def test_streaming_async_and_pagination_remaining_branches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from marketdata_provider.pagination import next_cursor
    from marketdata_provider.streaming.live import (
        CoalescingKlineQueue,
        PublicKlineWebSocketClient,
    )
    from marketdata_provider.streaming.supervisor import overlap_start
    from marketdata_provider.transport.async_client import (
        MarketDataHTTPClient,
        RetryConfig,
    )

    with pytest.raises(ValueError):
        CoalescingKlineQueue(0)
    assert overlap_start(None, "1m", 3) is None
    with pytest.raises(MDPaginationStalled):
        next_cursor(0, "1m", 60_000)

    monkeypatch.setenv("MARKETDATA_ALLOW_STREAM", "1")
    monkeypatch.setattr(
        "marketdata_provider.streaming.live.importlib.util.find_spec",
        lambda name: object(),
    )

    class TimeoutWS:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def recv(self):
            raise asyncio.TimeoutError

    websockets = types.ModuleType("websockets")
    websockets.connect = lambda *args, **kwargs: TimeoutWS()
    monkeypatch.setitem(sys.modules, "websockets", websockets)
    client = PublicKlineWebSocketClient(
        exchange="binance", market="spot", symbol="BTCUSDT", timeframe="1m"
    )

    async def collect_timeout() -> list[Any]:
        out = []
        async for event in client.events(max_messages=None, timeout_s=0.0):
            out.append(event)
        return out

    assert asyncio.run(collect_timeout()) == []

    class FakeAsyncResponse:
        def __init__(self, status_code: int, payload: Any) -> None:
            self.status_code = status_code
            self.headers = {"Retry-After": "0"}
            self._payload = payload

        def raise_for_status(self) -> None:
            if self.status_code >= 400:
                raise httpx.HTTPStatusError(
                    "bad",
                    request=httpx.Request("GET", "https://x"),
                    response=httpx.Response(self.status_code),
                )

        def json(self) -> Any:
            return self._payload

    class FakeAsyncClient:
        def __init__(self) -> None:
            self.responses = [
                FakeAsyncResponse(429, {}),
                FakeAsyncResponse(200, {"ok": True}),
            ]

        async def get(
            self, path: str, params: dict[str, Any] | None = None
        ) -> FakeAsyncResponse:
            return self.responses.pop(0)

        async def aclose(self) -> None:
            return None

    async def async_http() -> None:
        http = MarketDataHTTPClient(
            "https://example",
            retry_config=RetryConfig(max_retries=1, base_sec=0, max_sec=0),
        )
        with pytest.raises(RuntimeError):
            await http.get_json("/before")
        http._client = FakeAsyncClient()  # type: ignore[assignment]

        async def fake_sleep(*_args: object) -> None:
            return None

        monkeypatch.setattr(
            "marketdata_provider.transport.async_client.asyncio.sleep", fake_sleep
        )
        assert await http.get_json("/x") == {"ok": True}

    asyncio.run(async_http())


def test_segment_store_parquet_seek_and_service_helper_edges(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import marketdata_provider.service as svc
    import marketdata_provider.store.segment_store as seg
    from marketdata_provider.store.segment_store import SegmentStore

    pa = types.ModuleType("pyarrow")
    pq = types.ModuleType("pyarrow.parquet")
    pa.Table = types.SimpleNamespace(from_pylist=lambda rows, schema=None: rows)

    class FakeParquetFile:
        def __init__(self, path: str | Path) -> None:
            self.path = Path(path)

        def read(self):
            return types.SimpleNamespace(
                to_pylist=lambda: [
                    {
                        "time": 0,
                        "open": 1,
                        "high": 2,
                        "low": 0.5,
                        "close": 1.5,
                        "volume": 1,
                        "time_close": 59_999,
                        "exchange": "binance",
                        "market": "spot",
                        "symbol": "BTCUSDT",
                        "timeframe": "1m",
                        "source_transport": "ws",
                        "source_kind": "trade_kline",
                        "source": "",
                        "is_closed": True,
                        "quote_volume": None,
                        "turnover": None,
                        "trades_count": None,
                        "taker_buy_base_volume": None,
                        "taker_buy_quote_volume": None,
                        "downloaded_at": 1,
                    }
                ]
            )

    pq.ParquetFile = FakeParquetFile
    pq.write_table = lambda table, tmp: Path(tmp).write_text("parquet")
    monkeypatch.setitem(sys.modules, "pyarrow", pa)
    monkeypatch.setitem(sys.modules, "pyarrow.parquet", pq)
    monkeypatch.setattr(
        seg.importlib.util,
        "find_spec",
        lambda name: object() if name.startswith("pyarrow") else None,
    )

    store = SegmentStore(tmp_path / "segments", data_format="parquet")
    key = {
        "exchange": "binance",
        "market": "spot",
        "symbol": "BTCUSDT",
        "timeframe": "1m",
    }
    store.replace_all([_mb(0)], **key)
    assert [bar.time for bar in store.iter_all(**key)] == [0]
    assert store.read_all(**key)[0].close == 1.5

    def raising_write(table: object, tmp: str) -> None:
        Path(tmp).write_text("partial")
        raise RuntimeError("boom")

    pq.write_table = raising_write
    with pytest.raises(RuntimeError):
        store._atomic_write_parquet(tmp_path / "segments" / "broken.parquet", [_mb(0)])

    csv_path = tmp_path / "seek.csv"
    csv_path.write_text(
        "time,open,high,low,close,volume,time_close,exchange,market,symbol,timeframe,source_transport,source_kind,source,is_closed,quote_volume,turnover,trades_count,taker_buy_base_volume,taker_buy_quote_volume,downloaded_at\n"
    )
    with csv_path.open("r") as fh:
        store._seek_csv_near_start(
            fh,
            csv_path,
            start=60_000,
            manifest={"start_time": 0, "rows_count": 1, "timeframe": "bad"},
        )
    csv_path.write_text("time,open\nnot-a-time,1\n")
    with csv_path.open("r") as fh:
        fh.readline()
        store._seek_csv_near_start(
            fh,
            csv_path,
            start=60_000,
            manifest={"start_time": 0, "rows_count": 2, "timeframe": "1m"},
        )

    q = _contract_query(0, 120_000)
    cfg = MarketDataConfig()
    monkeypatch.setattr(svc, "_archive_cutoff_ms", lambda config: 130_000)
    assert svc._remaining_recent_query(q, [], cfg) is None
    month_q = BarQuery(q.instrument, parse_timeframe("1M"), 0, 120_000)
    monkeypatch.setattr(svc, "_archive_cutoff_ms", lambda config: 60_000)
    assert svc._remaining_recent_query(month_q, [], cfg).start_ms == 60_000
    assert svc._coverage_complete([_mb(0)], month_q)
    assert not svc._can_derive_from_base(month_q, parse_timeframe("1m"))
    assert not svc._can_derive_from_base(q, parse_timeframe("1D"))
    assert (
        svc._aggregate_market_bars([_mb(-60_000), _mb(0), _mb(60_000)], query=q)[0].time
        == 0
    )

    service = svc.MarketDataService(
        MarketDataConfig(storage=StorageConfig(cache_dir=tmp_path / "svc"))
    )
    assert service._stored_coverage_complete(month_q) is False
    monkeypatch.setattr(
        svc,
        "BinanceArchiveSource",
        lambda config: types.SimpleNamespace(fetch=lambda query, progress_callback=None: [_mb(0)]),
    )
    monkeypatch.setattr(
        svc,
        "BinanceRestSource",
        lambda config: types.SimpleNamespace(fetch=lambda query: [_mb(60_000)]),
    )
    monkeypatch.setattr(
        svc, "_remaining_recent_query", lambda query, archive, config: q
    )
    assert [bar.time for bar in service._fetch_from_sources(q)] == [0, 60_000]
    monkeypatch.setattr(
        svc,
        "BybitRestSource",
        lambda config: types.SimpleNamespace(fetch=lambda query: [_mb(0)]),
    )
    assert (
        service._fetch_from_sources(
            BarQuery(
                InstrumentKey("bybit", "linear", "BTCUSDT"),
                parse_timeframe("1m"),
                0,
                60_000,
            )
        )[0].time
        == 0
    )
    with pytest.raises(MDUnsupportedFeature):
        service._fetch_from_sources(
            BarQuery(
                InstrumentKey("bitstamp", "spot", "BTCUSDT"),
                parse_timeframe("1m"),
                0,
                60_000,
            )
        )


def test_remaining_retries_offline_repair_and_timeout_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from marketdata_provider.exchanges.binance import trades
    from marketdata_provider.exchanges.bybit import provider as yp
    from marketdata_provider.footprint.aggregate import aggregate_trades_to_footprint
    from marketdata_provider.footprint.service import FootprintService
    from marketdata_provider.providers.offline import OfflineDataProvider
    from marketdata_provider.store.candle_store import CandleStore
    from marketdata_provider.store.repair import (
        audit_against_source,
        repair_from_source,
    )
    from marketdata_provider.streaming.live import PublicKlineWebSocketClient
    from marketdata_provider.transport.async_client import (
        MarketDataHTTPClient,
        RetryConfig,
    )

    monkeypatch.setattr(trades.time, "sleep", lambda *_: None)
    assert (
        trades._get_json(
            FakeClient([FakeResponse(429, {}), FakeResponse(200, [])]),
            "u",
            {},
            max_retries=1,
        )
        == []
    )
    assert (
        trades._get_json(
            FakeClient([FakeResponse(500, {}), FakeResponse(200, [])]),
            "u",
            {},
            max_retries=1,
        )
        == []
    )
    monkeypatch.setattr(yp.time, "sleep", lambda *_: None)
    assert yp._get_json(
        FakeClient([FakeResponse(429, {}), FakeResponse(200, {"retCode": 0})]),
        "u",
        {},
        max_retries=1,
    ) == {"retCode": 0}
    assert yp._get_json(
        FakeClient(
            [FakeResponse(200, {"retCode": 10006}), FakeResponse(200, {"retCode": 0})]
        ),
        "u",
        {},
        max_retries=1,
    ) == {"retCode": 0}

    with pytest.raises(MDUnsupportedFeature, match="Parquet"):
        OfflineDataProvider(tmp_path / "missing.parquet")._read_parquet("1m")

    inst = InstrumentKey("binance", "spot", "BTCUSDT")
    fp_query = FootprintQuery(
        inst,
        parse_timeframe("1m"),
        1,
        60_000,
        price_bucket=1.0,
        gap_policy="allow_with_metadata",
    )
    assert (
        aggregate_trades_to_footprint([AggTrade(1, 1, 1.0, 1.0, False)], fp_query) == []
    )
    service = FootprintService(
        MarketDataConfig(storage=StorageConfig(cache_dir=tmp_path / "fp"))
    )
    raw_query = FootprintQuery(
        inst,
        parse_timeframe("1m"),
        0,
        60_000,
        price_bucket=1.0,
        gap_policy="allow_with_metadata",
    )
    service.raw_store.write_batch(
        [
            {
                "trade_id": 2,
                "time": 0,
                "price": 1.0,
                "quantity": 1.0,
                "buyer_maker": False,
            }
        ],
        exchange="binance",
        market="spot",
        symbol="BTCUSDT",
        source_transport="rest",
        source_kind="agg_trades",
        partition="day=1970-01-01",
    )
    assert service._ensure_raw_trades(raw_query)[0].trade_id == 2

    candle = CandleStore(tmp_path / "repair")
    candle.commit_closed(_mb(0))
    source = [_mb(60_000)]
    report = audit_against_source(
        candle,
        source,
        exchange="binance",
        market="spot",
        symbol="BTCUSDT",
        timeframe="1m",
        strict=True,
    )
    assert any(issue.code == "MD_AUDIT_EXTRA_BAR" for issue in report.issues)
    repair = repair_from_source(
        candle,
        source,
        exchange="binance",
        market="spot",
        symbol="BTCUSDT",
        timeframe="1m",
        policy="strict",
    )
    assert repair.applied

    monkeypatch.setenv("MARKETDATA_ALLOW_STREAM", "1")
    monkeypatch.setattr(
        "marketdata_provider.streaming.live.importlib.util.find_spec",
        lambda name: object(),
    )

    class TimeoutWS:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def recv(self):
            raise asyncio.TimeoutError

    websockets = types.ModuleType("websockets")
    websockets.connect = lambda *args, **kwargs: TimeoutWS()
    monkeypatch.setitem(sys.modules, "websockets", websockets)

    async def timeout_collect() -> list[Any]:
        out = []
        async for event in PublicKlineWebSocketClient(
            exchange="binance", market="spot", symbol="BTCUSDT", timeframe="1m"
        ).events(max_messages=1, timeout_s=1):
            out.append(event)
        return out

    assert asyncio.run(timeout_collect()) == []

    class ErrorAsyncClient:
        async def get(self, path: str, params: dict[str, Any] | None = None) -> Any:
            raise httpx.ConnectError("offline")

    async def async_error() -> None:
        http = MarketDataHTTPClient(
            "https://x", retry_config=RetryConfig(max_retries=1, base_sec=0, max_sec=0)
        )
        http._client = ErrorAsyncClient()  # type: ignore[assignment]

        async def fake_sleep(*_args: object) -> None:
            return None

        monkeypatch.setattr(
            "marketdata_provider.transport.async_client.asyncio.sleep", fake_sleep
        )
        with pytest.raises(httpx.ConnectError):
            await http.get_json("/x")

    asyncio.run(async_error())
