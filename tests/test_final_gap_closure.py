from __future__ import annotations

import importlib.util
from contextlib import closing
import io
import runpy
import sqlite3
import sys
import types
import zipfile
from pathlib import Path
from typing import Any

import httpx
import pytest

from marketdata_provider.config import (
    BinanceConfig,
    BybitConfig,
    HistoryConfig,
    MarketDataConfig,
    StorageConfig,
)
from marketdata_provider.contracts import BarQuery, InstrumentKey, parse_timeframe
from marketdata_provider.contracts.bar import Bar as ContractBar
from marketdata_provider.contracts.footprint import FootprintQuery
from marketdata_provider.core.bar import Bar, MarketBar
from marketdata_provider.errors import MDNetworkUnavailable, MDUnsupportedFeature


def _bar(time: int = 0, close: float = 1.5) -> Bar:
    return Bar(time, 1.0, max(2.0, close), 0.5, close, 1.0, time + 59_999)


def _mb(
    time: int = 0,
    *,
    exchange: str = "binance",
    market: str = "spot",
    close: float = 1.5,
    closed: bool = True,
) -> MarketBar:
    return MarketBar(
        time=time,
        open=1.0,
        high=max(2.0, close),
        low=0.5,
        close=close,
        volume=1.0,
        time_close=time + 59_999,
        exchange=exchange,
        market=market,
        symbol="BTCUSDT",
        timeframe="1m",
        source_transport="rest",
        source_kind="trade_kline",
        is_closed=closed,
        downloaded_at=time + 60_000,
    )


def _cbar(time: int = 0, *, closed: bool = True, close: float = 1.5) -> ContractBar:
    return ContractBar(
        instrument=InstrumentKey("binance", "spot", "BTCUSDT"),
        timeframe=parse_timeframe("1m"),
        time=time,
        time_close=time + 60_000,
        open=1.0,
        high=max(2.0, close),
        low=0.5,
        close=close,
        volume=1.0,
        closed=closed,
    )


def _query(
    start: int = 0,
    end: int = 60_000,
    *,
    exchange: str = "binance",
    market: str = "spot",
    timeframe: str = "1m",
) -> BarQuery:
    return BarQuery(
        InstrumentKey(exchange, market, "BTCUSDT"),
        parse_timeframe(timeframe),
        start,
        end,
    )


class FakeResponse:
    def __init__(
        self,
        status_code: int = 200,
        payload: Any = None,
        *,
        raise_http: bool = False,
        headers: dict[str, str] | None = None,
    ):
        self.status_code = status_code
        self._payload = payload if payload is not None else {}
        self._raise_http = raise_http
        self.headers = headers or {}

    def json(self) -> Any:
        return self._payload

    def raise_for_status(self) -> None:
        if self._raise_http or self.status_code >= 400:
            raise httpx.HTTPStatusError(
                "boom",
                request=httpx.Request("GET", "https://x"),
                response=httpx.Response(self.status_code),
            )


class FakeClient:
    def __init__(self, responses: list[FakeResponse]):
        self.responses = responses
        self.calls: list[dict[str, Any]] = []

    def __enter__(self) -> "FakeClient":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        return False

    def get(self, url: str, params: dict[str, Any]) -> FakeResponse:
        self.calls.append({"url": url, "params": dict(params)})
        return self.responses.pop(0)


def test_distribution_quality_release_main_and_error_branches(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from marketdata_provider import distribution, quality, release

    assert not distribution._should_include(Path(".coverage"))
    suspicious = tmp_path / "safe__pycache__name.txt"
    suspicious.write_text("x")
    manifest = distribution.distribution_manifest(tmp_path)
    assert manifest.forbidden_count == 1
    monkeypatch.setattr(
        sys, "argv", ["distribution", "manifest", "--root", str(tmp_path)]
    )
    monkeypatch.delitem(sys.modules, "marketdata_provider.distribution", raising=False)
    with pytest.raises(SystemExit) as exc:
        runpy.run_module("marketdata_provider.distribution", run_name="__main__")
    assert exc.value.code == 1

    bad_root = tmp_path / "quality"
    bad_root.mkdir()
    (bad_root / "bad.py").write_text("def nope(:\n")
    assert quality.duplicate_report(bad_root).duplicate_group_count == 0
    (bad_root / "big.py").write_text("\n".join("x = 1" for _ in range(3)))
    assert quality.architecture_report(bad_root, max_lines=1).oversized_count == 1
    assert quality.main(["architecture", str(bad_root), "--max-lines", "1"]) == 1
    monkeypatch.setattr(sys, "argv", ["quality", "duplicates", str(bad_root)])
    monkeypatch.delitem(sys.modules, "marketdata_provider.quality", raising=False)
    with pytest.raises(SystemExit) as exc:
        runpy.run_module("marketdata_provider.quality", run_name="__main__")
    assert exc.value.code == 0
    out = capsys.readouterr().out
    assert "oversized_count" in out or "duplicate_group_count" in out

    release_root = tmp_path / "release"
    release_root.mkdir()
    (release_root / "pyproject.toml").write_text(
        '[project]\nname = "marketdata-provider"\nversion = "0.0.0"\n'
    )
    report = release.release_report(release_root)
    assert not report.ok and report.notes
    assert (
        release.main(
            ["--root", str(release_root), "--json", str(tmp_path / "release.json")]
        )
        == 1
    )
    monkeypatch.setattr(sys, "argv", ["release", "--root", str(release_root)])
    monkeypatch.delitem(sys.modules, "marketdata_provider.release", raising=False)
    with pytest.raises(SystemExit) as exc:
        runpy.run_module("marketdata_provider.release", run_name="__main__")
    assert exc.value.code == 1


def test_archive_download_duration_and_invalid_zip_branches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from marketdata_provider.exchanges.binance import archive

    class Download:
        def __enter__(self) -> "Download":
            return self

        def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
            return False

        def read(self) -> bytes:
            stream = io.BytesIO()
            with zipfile.ZipFile(stream, "w") as zf:
                zf.writestr("BTCUSDT-1m-1970-01-01.csv", "0,1,2,0.5,1.5,10\n")
            return stream.getvalue()

    monkeypatch.setattr(archive, "urlopen", lambda *args, **kwargs: Download())
    bars = archive._load_archive_file(
        symbol="BTCUSDT",
        market="spot",
        timeframe="1m",
        start=0,
        end=60_000,
        period="daily",
        suffix="1970-01-01",
        cache_dir=tmp_path,
    )
    assert bars and bars[0].time == 0
    monkeypatch.setattr(archive, "timeframe_ms", lambda value: None)
    assert (
        archive._load_archive_file(
            symbol="BTCUSDT",
            market="spot",
            timeframe="1m",
            start=0,
            end=60_000,
            period="daily",
            suffix="1970-01-03",
            cache_dir=tmp_path,
        )
        == []
    )
    monkeypatch.setattr(archive, "timeframe_ms", lambda value: 60_000)
    invalid_path = (
        tmp_path / "spot" / "daily" / "BTCUSDT" / "1m" / "BTCUSDT-1m-1970-01-02.zip"
    )
    invalid_path.parent.mkdir(parents=True, exist_ok=True)
    invalid_path.write_bytes(b"not a zip")
    assert (
        archive._load_archive_file(
            symbol="BTCUSDT",
            market="spot",
            timeframe="1m",
            start=0,
            end=60_000,
            period="daily",
            suffix="1970-01-02",
            cache_dir=tmp_path,
        )
        == []
    )


def test_exchange_retry_terminal_and_pagination_edges(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from marketdata_provider.exchanges.binance import provider as bp
    from marketdata_provider.exchanges.binance import trades as bt
    from marketdata_provider.exchanges.bybit import provider as yp

    monkeypatch.setattr(bp.time, "sleep", lambda *_: None)
    monkeypatch.setattr(bt.time, "sleep", lambda *_: None)
    monkeypatch.setattr(yp.time, "sleep", lambda *_: None)
    with pytest.raises(MDNetworkUnavailable):
        bp._get_json(FakeClient([]), "u", {}, max_retries=-1)
    with pytest.raises(MDNetworkUnavailable):
        yp._get_json(FakeClient([]), "u", {}, max_retries=-1)
    with pytest.raises(MDNetworkUnavailable, match="rate limit"):
        yp._get_json(FakeClient([FakeResponse(429, {})]), "u", {}, max_retries=0)
    assert (
        bt._get_json(
            FakeClient([FakeResponse(500, {}), FakeResponse(200, [])]),
            "u",
            {},
            max_retries=1,
        )
        == []
    )
    assert (
        bt._get_json(
            FakeClient([FakeResponse(400, {}, raise_http=True), FakeResponse(200, [])]),
            "u",
            {},
            max_retries=1,
        )
        == []
    )
    with pytest.raises(MDNetworkUnavailable):
        bt._get_json(
            FakeClient([FakeResponse(400, {}, raise_http=True)]), "u", {}, max_retries=0
        )
    with pytest.raises(MDNetworkUnavailable):
        bt._get_json(FakeClient([]), "u", {}, max_retries=-1)
    monkeypatch.setattr(
        bt,
        "normalize_symbol",
        lambda *args, **kwargs: types.SimpleNamespace(
            market="coinm", exchange_symbol="BTCUSDT"
        ),
    )
    assert (
        bt.binance_get_agg_trades_sync(
            "BTCUSDT", 0, 60_000, BinanceConfig(), market="coinm"
        )
        == []
    )
    end_client = FakeClient(
        [
            FakeResponse(200, {"serverTime": 10**12}),
            FakeResponse(
                200,
                [
                    [0, "1", "2", "0.5", "1.5", "1"],
                    [60_000, "1", "2", "0.5", "1.5", "1"],
                ],
            ),
        ]
    )
    monkeypatch.setattr(bp.httpx, "Client", lambda **_: end_client)
    assert (
        len(
            bp.binance_get_bars_sync(
                "BTCUSDT",
                "1m",
                0,
                120_000,
                BinanceConfig(max_limit_spot=2),
                market="spot",
                max_retries=0,
            )
        )
        == 2
    )
    bybit_end = FakeClient(
        [
            FakeResponse(200, {"retCode": 0, "result": {"timeNano": "1000000000000"}}),
            FakeResponse(
                200,
                {
                    "retCode": 0,
                    "result": {
                        "list": [
                            [0, "1", "2", "0.5", "1.5", "1"],
                            [60_000, "1", "2", "0.5", "1.5", "1"],
                        ]
                    },
                },
            ),
        ]
    )
    monkeypatch.setattr(yp.httpx, "Client", lambda **_: bybit_end)
    assert (
        len(
            yp.bybit_get_bars_sync(
                "BTCUSDT",
                "1m",
                0,
                120_000,
                BybitConfig(max_limit=2),
                market="linear",
                max_retries=0,
            )
        )
        == 2
    )


def test_bybit_rest_invalid_payload_and_offline_adapter() -> None:
    from marketdata_provider.exchanges.bybit.rest import (
        OfflineBybitRestAdapter,
        normalize_bybit_klines,
    )

    assert (
        normalize_bybit_klines(
            [[0, "1", "2", "0.5", "1.5", "1"]],
            symbol="BTCUSDT",
            market="linear",
            timeframe="1m",
            include_open_candle=True,
        )[0].time
        == 0
    )
    with pytest.raises(Exception, match="missing result.list"):
        normalize_bybit_klines({}, symbol="BTCUSDT", market="linear", timeframe="1m")
    with pytest.raises(Exception, match="row is invalid"):
        normalize_bybit_klines(
            [["bad"]], symbol="BTCUSDT", market="linear", timeframe="1m"
        )
    adapter = OfflineBybitRestAdapter(
        {
            "result": {
                "list": [
                    [0, "1", "2", "0.5", "1.5", "1", "2"],
                    [60_000, "1", "2", "0.5", "1.6", "1", "2"],
                ]
            }
        }
    )
    assert [
        bar.time
        for bar in adapter.get_klines(
            symbol="BTCUSDT",
            market="linear",
            interval="1m",
            start=1,
            end=120_000,
            limit=1,
            include_open_candle=True,
        )
    ] == [60_000]


def test_offline_parquet_and_zstd_optional_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from marketdata_provider.providers.offline import OfflineDataProvider
    from marketdata_provider.store.raw_store import RawStore

    table = types.SimpleNamespace(
        to_pylist=lambda: [
            {"time": 0, "open": 1, "high": 2, "low": 0.5, "close": 1.5, "volume": 1}
        ]
    )
    pq = types.SimpleNamespace(read_table=lambda path: table)
    monkeypatch.setitem(sys.modules, "pyarrow", types.ModuleType("pyarrow"))
    monkeypatch.setitem(sys.modules, "pyarrow.parquet", pq)
    parquet_path = tmp_path / "bars.parquet"
    parquet_path.write_bytes(b"stub")
    assert (
        OfflineDataProvider(parquet_path).get_bars("BTCUSDT", "1m", None, None)[0].time
        == 0
    )

    class Compressor:
        def compress(self, body: bytes) -> bytes:
            return b"z" + body

    class Decompressor:
        def decompress(self, body: bytes) -> bytes:
            return body[1:]

    zstd = types.SimpleNamespace(
        ZstdCompressor=lambda: Compressor(), ZstdDecompressor=lambda: Decompressor()
    )
    monkeypatch.setattr(
        importlib.util,
        "find_spec",
        lambda name: object() if name == "zstandard" else None,
    )
    monkeypatch.setitem(sys.modules, "zstandard", zstd)
    raw = RawStore(tmp_path / "raw", compression="zstd")
    raw.write_batch(
        [{"a": 1}],
        exchange="binance",
        market="spot",
        symbol="BTCUSDT",
        source_transport="rest",
    )
    assert raw.read_batch(
        exchange="binance", market="spot", symbol="BTCUSDT", source_transport="rest"
    ) == [{"a": 1}]


def test_factories_and_footprint_storage_edges(tmp_path: Path) -> None:
    from marketdata_provider.contracts.series import BarSeries, CoverageReport
    from marketdata_provider.factories import _CandleStoreAdapter
    from marketdata_provider.footprint.service import FootprintService

    class Segments:
        def read_all(self, **kwargs: Any) -> list[MarketBar]:
            return [_mb(0, close=99.0)]

        def replace_all(self, *args: Any, **kwargs: Any) -> None:
            raise RuntimeError("conflict")

    class Store:
        def __init__(self) -> None:
            self.segments = Segments()
            self.closed: list[MarketBar] = []
            self.opened: list[MarketBar] = []

        def commit_closed(self, bar: MarketBar) -> types.SimpleNamespace:
            self.closed.append(bar)
            return types.SimpleNamespace(status="committed")

        def upsert_open(self, bar: MarketBar) -> types.SimpleNamespace:
            self.opened.append(bar)
            return types.SimpleNamespace(status="upserted")

        def get_market_bars(self, **kwargs: Any) -> list[MarketBar]:
            return []

    q = _query()
    provider = _CandleStoreAdapter(Store())
    series = BarSeries(
        q, (_cbar(0, closed=True),), CoverageReport(0, 60_000, 0, 60_000)
    )
    result = provider.write(series)
    assert not result.success and result.error
    open_series = BarSeries(
        q, (_cbar(0, closed=False),), CoverageReport(0, 60_000, 0, 60_000)
    )
    ok = provider.write(open_series)
    assert ok.success and ok.rows_written == 1
    mixed = BarSeries(
        q,
        (_cbar(0, closed=True), _cbar(60_000, closed=False)),
        CoverageReport(0, 120_000, 0, 120_000),
    )
    mixed_result = provider.write(mixed)
    assert mixed_result.success and mixed_result.rows_written == 2

    svc = FootprintService(
        MarketDataConfig(storage=StorageConfig(cache_dir=tmp_path / "fp"))
    )
    fq = FootprintQuery(
        InstrumentKey("binance", "spot", "BTCUSDT"),
        start_ms=0,
        end_ms=60_000,
        timeframe=parse_timeframe("1m"),
        price_bucket=1.0,
        source="storage",
        gap_policy="fail",
    )
    with pytest.raises(MDUnsupportedFeature, match="coverage incomplete"):
        svc.fetch_footprint(fq)


def test_service_cached_materialize_and_fetch_edges(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from marketdata_provider import service as svcmod
    from marketdata_provider.service import MarketDataService

    config = MarketDataConfig(
        storage=StorageConfig(cache_dir=tmp_path / "svc"),
        history=HistoryConfig(enabled=True, archive_first=False, base_timeframe="1m"),
    )
    service = MarketDataService(config)
    base = [_mb(0), _mb(60_000), _mb(120_000), _mb(180_000), _mb(240_000)]
    service.store.segments.replace_all(
        base, exchange="binance", market="spot", symbol="BTCUSDT", timeframe="1m"
    )
    query = _query(0, 300_000, timeframe="5m")
    assert service.materialize_bars(query)["rows_written"] == 1
    assert service.materialize_bars(query)["changed"] is False
    assert service.fetch_bars(query).bars
    month_query = _query(0, 60_000, timeframe="1M")
    assert service._base_query(month_query) == month_query
    service_no_archive = MarketDataService(config)
    monkeypatch.setattr(
        svcmod,
        "BinanceRestSource",
        lambda config: types.SimpleNamespace(fetch=lambda q: [_mb(q.start_ms)]),
    )
    assert service_no_archive._fetch_from_sources(_query(0, 60_000))[0].time == 0


def test_segment_store_legacy_paths_and_vacuum_edges(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from marketdata_provider.store.segment_store import SegmentStore

    db_path = tmp_path / "legacy" / "index.sqlite"
    db_path.parent.mkdir()
    with closing(sqlite3.connect(db_path)) as db:
        db.execute(
            "CREATE TABLE marketdata_segments (id INTEGER PRIMARY KEY AUTOINCREMENT, exchange TEXT NOT NULL, market TEXT NOT NULL, symbol TEXT NOT NULL, timeframe TEXT NOT NULL, start_time INTEGER NOT NULL, end_time INTEGER NOT NULL, rows_count INTEGER NOT NULL, source_transport TEXT NOT NULL, source_kind TEXT NOT NULL, checksum TEXT NOT NULL, downloaded_at INTEGER NOT NULL)"
        )
    SegmentStore(tmp_path / "legacy")
    with closing(sqlite3.connect(db_path)) as db:
        cols = {row[1] for row in db.execute("PRAGMA table_info(marketdata_segments)")}
    assert "data_format" in cols
    monkeypatch.setattr(
        importlib.util,
        "find_spec",
        lambda name: None if name == "pyarrow" else object(),
    )
    with pytest.raises(MDUnsupportedFeature):
        SegmentStore(tmp_path / "parquet").replace_all(
            [_mb(0)],
            exchange="binance",
            market="spot",
            symbol="BTCUSDT",
            timeframe="1m",
            data_format="parquet",
        )
    store = SegmentStore(tmp_path / "segments")
    directory = store._dir(
        exchange="binance",
        market="spot",
        symbol="BTCUSDT",
        timeframe="1m",
        source_kind="trade_kline",
    )
    directory.mkdir(parents=True)
    (directory / "bars.parquet").write_bytes(b"old")
    store.replace_all(
        [_mb(0)], exchange="binance", market="spot", symbol="BTCUSDT", timeframe="1m"
    )
    assert not (directory / "bars.parquet").exists()
    (directory / "bars.parquet").write_bytes(b"old")
    store.replace_all_stream(
        [_mb(0), _mb(60_000)],
        exchange="binance",
        market="spot",
        symbol="BTCUSDT",
        timeframe="1m",
    )
    assert not (directory / "bars.parquet").exists()
    orphan = store.root / "v1" / "x" / "bars.csv"
    orphan.parent.mkdir(parents=True)
    orphan.write_text("time\n")
    assert store.vacuum()["removed_stale_data_files"] >= 0
    assert (
        store._seek_csv_near_start(
            io.StringIO(""), Path("missing.csv"), start=10, manifest={"rows_count": 0}
        )
        is None
    )
    data = directory / "bars.csv"
    assert (
        list(
            store._iter_csv_range(
                data,
                start=10**12,
                end=None,
                manifest={"start_time": 0, "rows_count": 2, "timeframe": "1m"},
            )
        )
        == []
    )
    header_only = tmp_path / "header-only.csv"
    header_only.write_text(
        "time,open,high,low,close,volume,time_close,exchange,market,symbol,timeframe,quote_volume,turnover,trades_count,taker_buy_base_volume,taker_buy_quote_volume,source_transport,source_kind,is_closed,downloaded_at\n"
    )
    with header_only.open() as handle:
        handle.readline()
        assert (
            store._seek_csv_near_start(
                handle,
                header_only,
                start=60_000,
                manifest={"start_time": 0, "rows_count": 1, "timeframe": "1m"},
            )
            is None
        )


def test_remaining_service_store_and_offline_edges(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from marketdata_provider.contracts.footprint import (
        FootprintBar,
        FootprintLevel,
        FootprintSeries,
    )
    from marketdata_provider.footprint.service import _coverage_for
    from marketdata_provider.providers.offline import OfflineDataProvider
    from marketdata_provider.service import MarketDataService
    from marketdata_provider.store.footprint_store import FootprintStore

    csv_path = tmp_path / "bars.csv"
    csv_path.write_text("time,open,high,low,close,volume\n0,1,2,0.5,1.5,1\n")
    with pytest.raises(Exception, match="intrabar"):
        OfflineDataProvider(csv_path).get_intrabar_bars("BTCUSDT", _bar(0))

    fq = FootprintQuery(
        InstrumentKey("binance", "spot", "BTCUSDT"),
        start_ms=60_000,
        end_ms=120_000,
        timeframe=parse_timeframe("1m"),
        price_bucket=1.0,
    )
    store = FootprintStore(tmp_path / "footprints")
    outside = FootprintQuery(
        InstrumentKey("binance", "spot", "BTCUSDT"),
        start_ms=0,
        end_ms=60_000,
        timeframe=parse_timeframe("1m"),
        price_bucket=1.0,
    )
    bar = FootprintBar(0, 59_999, (FootprintLevel(1.0, 2.0, 1.0, 0.0, 1, 0),), 1)
    store.write(FootprintSeries(outside, (bar,), _coverage_for(outside, (bar,))))
    assert store.read(fq).bars == ()

    service = MarketDataService(
        MarketDataConfig(
            storage=StorageConfig(cache_dir=tmp_path / "svc"),
            history=HistoryConfig(
                enabled=True, archive_first=True, base_timeframe="1m"
            ),
        )
    )
    q = _query(0, 120_000)
    service.store.segments.replace_all(
        [_mb(0), _mb(60_000)],
        exchange="binance",
        market="spot",
        symbol="BTCUSDT",
        timeframe="1m",
    )
    assert service._ensure_stored(q) is False

    class SpanningManifest:
        start_time = 0
        end_time = 60_000

    monkeypatch.setattr(
        service.store.segments, "manifest_for", lambda **kwargs: SpanningManifest()
    )
    monkeypatch.setattr(service, "_stored_coverage_complete", lambda query: False)
    monkeypatch.setattr(service, "_stored_bars", lambda query: [_mb(0), _mb(60_000)])
    monkeypatch.setattr(
        service,
        "_fetch_from_sources",
        lambda query: (_ for _ in ()).throw(AssertionError("not fetched")),
    )
    assert service._ensure_stored(q) is False
    monkeypatch.setattr(service.store.segments, "manifest_for", lambda **kwargs: None)
    assert service._ensure_stored(q) is False


def test_streaming_overlap_branch() -> None:
    from marketdata_provider.streaming.supervisor import overlap_start

    assert overlap_start(120_000, "1m", 1) == 60_000


def test_final_symbol_timeframe_validation_edges() -> None:
    from marketdata_provider.core.bar import Bar
    from marketdata_provider.errors import (
        MDTimeframeUnsupported,
        MDValidationError,
        MDSymbolUnsupported,
    )
    from marketdata_provider.symbols import normalize_symbol
    from marketdata_provider.timeframes import canonical_timeframe, timeframe_ms
    from marketdata_provider.validation import validate_bars

    with pytest.raises(MDSymbolUnsupported, match="Empty symbol"):
        normalize_symbol("", exchange="BINANCE")
    with pytest.raises(MDSymbolUnsupported, match="Unsupported exchange"):
        normalize_symbol("BTCUSDT", exchange="NYSE")
    assert canonical_timeframe("01d") == "1D"
    with pytest.raises(MDTimeframeUnsupported, match="variable length"):
        timeframe_ms("1M")

    valid = Bar(time=100, open=1, high=2, low=0.5, close=1.5, volume=1, time_close=200)
    duplicate = Bar(time=100, open=1, high=2, low=0.5, close=1.5, volume=1)
    unsorted = Bar(time=50, open=1, high=2, low=0.5, close=1.5, volume=1)
    high_bad = Bar(time=200, open=1, high=0.5, low=0.5, close=1.5, volume=1)
    low_bad = Bar(time=300, open=1, high=2, low=1.6, close=1.5, volume=1)
    close_bad = Bar(
        time=400, open=1, high=2, low=0.5, close=1.5, volume=1, time_close=399
    )

    with pytest.raises(MDValidationError, match="No bars"):
        validate_bars([], allow_empty=False)
    with pytest.raises(MDValidationError, match="Duplicate"):
        validate_bars([valid, duplicate])
    with pytest.raises(MDValidationError, match="strictly sorted"):
        validate_bars([valid, unsorted])
    with pytest.raises(MDValidationError, match="high violation"):
        validate_bars([high_bad])
    with pytest.raises(MDValidationError, match="low violation"):
        validate_bars([low_bad])
    with pytest.raises(MDValidationError, match="time_close"):
        validate_bars([close_bad])


def test_final_segment_store_context_and_row_helpers(tmp_path: Path) -> None:
    from marketdata_provider.store.segment_rows import parse_bool
    from marketdata_provider.store.segment_store import SegmentStore

    store = SegmentStore(tmp_path / "segments")
    with pytest.raises(RuntimeError, match="rollback branch"):
        with store._connect_index():
            raise RuntimeError("rollback branch")

    assert store._parse_bool(None) is True
    assert store._parse_bool(True) is True
    assert parse_bool(0) is False
    assert parse_bool(object()) is True
    row = {
        "time": "0",
        "open": "1",
        "high": "2",
        "low": "0.5",
        "close": "1.5",
        "volume": "10",
        "is_closed": "yes",
    }
    assert store._row_to_bar(row).is_closed is True
