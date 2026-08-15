from __future__ import annotations

import importlib.util
import json
import sqlite3
import zipfile
from dataclasses import replace
from pathlib import Path
from typing import Any

import httpx
import pytest
from typing_extensions import Self

from marketdata_provider.config import BinanceConfig, BybitConfig
from marketdata_provider.core.bar import RUNTIME_CONTRACT_VERSION, Bar, MarketBar
from marketdata_provider.errors import (
    MDInvalidExchangeResponse,
    MDNetworkUnavailable,
    MDPaginationStalled,
    MDTimeframeUnsupported,
    MDUnsupportedFeature,
)
from marketdata_provider.exchanges.binance import archive as ba
from marketdata_provider.exchanges.binance import provider as bp
from marketdata_provider.exchanges.binance.rest import (
    OfflineBinanceRestAdapter,
    normalize_binance_klines,
)
from marketdata_provider.exchanges.bybit import provider as yp
from marketdata_provider.exchanges.bybit.rest import (
    OfflineBybitRestAdapter,
    normalize_bybit_klines,
)
from marketdata_provider.providers.offline import OfflineDataProvider
from marketdata_provider.store.segment_store import (
    SegmentStore,
    bars_checksum,
    market_bar_checksum,
)
from marketdata_provider.timeframes import (
    canonical_timeframe,
    close_time_ms,
    next_open_time_ms,
    timeframe_ms,
    to_binance_interval,
    to_bybit_interval,
)


class FakeResponse:
    def __init__(
        self,
        status_code: int,
        payload: Any,
        headers: dict[str, str] | None = None,
        *,
        raise_http: bool = False,
    ) -> None:
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}
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
    def __init__(
        self, responses: list[FakeResponse], calls: list[dict[str, Any]] | None = None
    ) -> None:
        self.responses = list(responses)
        self.calls = calls if calls is not None else []

    def get(self, url: str, params: dict[str, Any]) -> FakeResponse:
        self.calls.append({"url": url, "params": dict(params)})
        if not self.responses:
            raise httpx.ConnectError("empty")
        return self.responses.pop(0)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        pass


def _binance_row(open_time: int, close: float = 2.0) -> list[Any]:
    return [
        open_time,
        "1",
        "3",
        "0.5",
        str(close),
        "10",
        open_time + 59_999,
        "20",
        "7",
        "3",
        "6",
    ]


def _bybit_payload(open_time: int, close: float = 2.0) -> dict[str, Any]:
    return {
        "retCode": 0,
        "result": {"list": [[open_time, "1", "3", "0.5", str(close), "10", "20"]]},
    }


def test_binance_provider_http_edges(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(bp.time, "sleep", lambda *_: None)
    assert bp._base_url(BinanceConfig(), "spot").endswith("binance.com")
    assert bp._base_url(BinanceConfig(), "coinm").endswith("dapi.binance.com")

    client = FakeClient(
        [FakeResponse(429, {}, {"Retry-After": "0"}), FakeResponse(200, {"ok": True})]
    )
    assert bp._get_json(client, "u", {}, max_retries=1) == {"ok": True}
    with pytest.raises(MDNetworkUnavailable, match="rate limit"):
        bp._get_json(FakeClient([FakeResponse(418, {})]), "u", {"x": 1}, max_retries=0)
    with pytest.raises(MDNetworkUnavailable, match="HTTP request"):
        bp._get_json(
            FakeClient([FakeResponse(400, {}, raise_http=True)]), "u", {}, max_retries=0
        )
    with pytest.raises(MDInvalidExchangeResponse, match="server time"):
        bp._server_time_ms(
            FakeClient([FakeResponse(200, {})]), "https://x", "spot", max_retries=0
        )


def test_binance_sync_pagination_and_async_intrabar(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, Any]] = []
    fake = FakeClient(
        [
            FakeResponse(200, {"serverTime": 10**12}),
            FakeResponse(200, [_binance_row(0)]),
        ],
        calls,
    )
    monkeypatch.setattr(bp.httpx, "Client", lambda **_: fake)
    bars = bp.binance_get_bars_sync(
        "BTCUSDT",
        "1m",
        0,
        60_000,
        BinanceConfig(max_limit_usdm=5),
        market="usdm",
        max_retries=0,
    )
    assert len(bars) == 1 and bars[0].time == 0
    assert calls[-1]["params"]["endTime"] == 59_999

    stalled = FakeClient(
        [
            FakeResponse(200, {"serverTime": 10**12}),
            FakeResponse(200, [_binance_row(0)]),
        ]
    )
    monkeypatch.setattr(bp.httpx, "Client", lambda **_: stalled)
    monkeypatch.setattr(bp, "next_open_time_ms", lambda *_: 0)
    with pytest.raises(MDPaginationStalled):
        bp.binance_get_bars_sync(
            "BTCUSDT",
            "1m",
            0,
            None,
            BinanceConfig(max_limit_usdm=1),
            market="usdm",
            max_retries=0,
            include_open_candle=True,
        )


def test_bybit_provider_http_edges(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(yp.time, "sleep", lambda *_: None)
    assert yp._category("spot") == "spot"
    assert yp._category("inverse") == "inverse"
    assert yp._get_json(
        FakeClient([FakeResponse(500, {}), FakeResponse(200, {"retCode": 0})]),
        "u",
        {},
        max_retries=1,
    ) == {"retCode": 0}
    with pytest.raises(MDNetworkUnavailable, match="rate limit"):
        yp._get_json(
            FakeClient([FakeResponse(200, {"retCode": 10006, "retMsg": "slow"})]),
            "u",
            {},
            max_retries=0,
        )
    with pytest.raises(MDNetworkUnavailable, match="HTTP request"):
        yp._get_json(
            FakeClient([FakeResponse(400, {}, raise_http=True)]), "u", {}, max_retries=0
        )
    assert (
        yp._server_time_ms(
            FakeClient([FakeResponse(200, {"result": {"timeSecond": "5"}})]),
            "https://x",
            max_retries=0,
        )
        == 5000
    )
    with pytest.raises(MDInvalidExchangeResponse, match="server time"):
        yp._server_time_ms(
            FakeClient([FakeResponse(200, {"result": {}})]), "https://x", max_retries=0
        )


def test_bybit_sync_pagination_and_response_validation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = FakeClient(
        [
            FakeResponse(200, {"result": {"timeNano": "1000000000000"}}),
            FakeResponse(200, _bybit_payload(0)),
        ]
    )
    monkeypatch.setattr(yp.httpx, "Client", lambda **_: fake)
    bars = yp.bybit_get_bars_sync(
        "BTCUSDT",
        "1m",
        0,
        60_000,
        BybitConfig(max_limit=5),
        market="linear",
        max_retries=0,
    )
    assert len(bars) == 1 and bars[0].time == 0

    bad = FakeClient(
        [
            FakeResponse(200, {"retCode": 0, "result": {"timeNano": "1"}}),
            FakeResponse(200, []),
        ]
    )
    monkeypatch.setattr(yp.httpx, "Client", lambda **_: bad)
    with pytest.raises(MDInvalidExchangeResponse, match="non-zero"):
        yp.bybit_get_bars_sync(
            "BTCUSDT", "1m", 0, 60_000, BybitConfig(), market="linear", max_retries=0
        )

    stalled_payload = {
        "retCode": 0,
        "result": {"list": [[0, "1", "3", "0.5", "2", "10"]]},
    }
    stalled = FakeClient(
        [
            FakeResponse(200, {"retCode": 0, "result": {"timeNano": "1000000000000"}}),
            FakeResponse(200, stalled_payload),
        ]
    )
    monkeypatch.setattr(yp.httpx, "Client", lambda **_: stalled)
    monkeypatch.setattr(yp, "next_open_time_ms", lambda *_: 0)
    with pytest.raises(MDPaginationStalled):
        yp.bybit_get_bars_sync(
            "BTCUSDT",
            "1m",
            0,
            None,
            BybitConfig(max_limit=1),
            market="linear",
            include_open_candle=True,
            max_retries=0,
        )


def test_archive_zip_loading_and_interval_helpers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    archive_root = tmp_path / "archives"
    zpath = (
        archive_root / "usdm" / "daily" / "BTCUSDT" / "1m" / "BTCUSDT-1m-1970-01-01.zip"
    )
    zpath.parent.mkdir(parents=True)
    with zipfile.ZipFile(zpath, "w") as zf:
        zf.writestr(
            "data.csv",
            "not_time,1,2,0,1,1\n0,1,2,0.5,1.5,1\n0,1.5,3,0.25,2,2\n60000000,1,2,0.5,1,1\n",
        )
    bars = ba._load_archive_file(
        symbol="btcusdt",
        market="usdm",
        timeframe="1m",
        start=0,
        end=60_000,
        period="daily",
        suffix="1970-01-01",
        cache_dir=archive_root,
    )
    assert len(bars) == 1 and bars[0].high == 3 and bars[0].volume == 3
    assert ba._epoch_to_ms(100_000_000_000_000) == 100_000_000_000
    assert ba._coalesce_intervals(
        ((0, 60_000), (60_000, 120_000), (240_000, 300_000))
    ) == ((0, 120_000), (240_000, 300_000))
    assert ba._archive_covers_intervals(bars, ((0, 60_000),), duration=60_000)
    assert not ba._archive_covers_intervals([], ((0, 60_000),), duration=None)
    assert ba._days_for_intervals(((0, 86_400_000),)) == ((1970, 1, 1),)
    assert ba._months_for_intervals(((0, 32 * 86_400_000),)) == ((1970, 1), (1970, 2))

    monkeypatch.setattr(
        ba, "urlopen", lambda *_, **__: (_ for _ in ()).throw(TimeoutError())
    )
    assert (
        ba._load_archive_file(
            symbol="BTCUSDT",
            market="usdm",
            timeframe="1m",
            start=0,
            end=60_000,
            period="daily",
            suffix="missing",
            cache_dir=archive_root,
        )
        == []
    )


def test_offline_and_rest_adapters_edges(tmp_path: Path) -> None:
    csv_path = tmp_path / "bars.csv"
    csv_path.write_text(
        "timestamp,open,high,low,close,volume,close_time\n0,1,2,0.5,1.5,,59999\n60000,2,3,1.5,2.5,7,119999\n"
    )
    provider = OfflineDataProvider(csv_path, timeframe="1m")
    assert len(provider.get_bars("BTC", "1m", 0, 120_000, max_bars=1)) == 1
    assert (
        len(
            provider.get_intrabar_bars("BTC", Bar(0, 1, 2, 0.5, 1.5, time_close=59_999))
        )
        == 1
    )
    with pytest.raises(MDUnsupportedFeature):
        OfflineDataProvider(tmp_path / "bars.txt").get_bars("BTC", "1m", None, None)
    with pytest.raises(MDUnsupportedFeature):
        OfflineDataProvider(tmp_path / "bars.parquet").get_bars("BTC", "1m", None, None)

    with pytest.raises(MDInvalidExchangeResponse):
        normalize_binance_klines(
            [[0, 1]],
            symbol="BTC",
            market="spot",
            timeframe="1m",
            include_open_candle=True,
        )
    assert (
        OfflineBinanceRestAdapter([_binance_row(0)], server_time_ms=10**12)
        .get_klines(symbol="btc", market="spot", interval="1m", start=0, end=60_000)[0]
        .symbol
        == "BTC"
    )
    with pytest.raises(MDInvalidExchangeResponse):
        normalize_bybit_klines(
            {"result": {}},
            symbol="BTC",
            market="linear",
            timeframe="1m",
            include_open_candle=True,
        )
    assert (
        OfflineBybitRestAdapter(_bybit_payload(0), server_time_ms=10**12)
        .get_klines(
            symbol="btc", market="linear", interval="1m", start=0, end=60_000, limit=1
        )[0]
        .symbol
        == "BTC"
    )


def _mb(time: int, close: float = 1.0) -> MarketBar:
    return MarketBar(
        time=time,
        open=1.0,
        high=max(2.0, close),
        low=0.5,
        close=close,
        volume=1.0,
        time_close=time + 59_999,
        exchange="binance",
        market="usdm",
        symbol="BTCUSDT",
        timeframe="1m",
        downloaded_at=1,
    )


def test_segment_store_integrity_and_private_helpers(tmp_path: Path) -> None:
    with pytest.raises(MDUnsupportedFeature):
        SegmentStore(tmp_path / "bad", data_format="xml")  # type: ignore[arg-type]
    if importlib.util.find_spec("pyarrow") is None:
        with pytest.raises(MDUnsupportedFeature):
            SegmentStore(tmp_path / "pq", data_format="parquet")
    else:
        assert (
            SegmentStore(tmp_path / "pq", data_format="parquet").data_format
            == "parquet"
        )

    store = SegmentStore(tmp_path / "store")
    bars = [_mb(0, 1.0), _mb(60_000, 2.0), _mb(120_000, 3.0)]
    manifest = store.replace_all(
        bars, exchange="BINANCE", market="USDM", symbol="BTCUSDT", timeframe="1m"
    )
    assert manifest.runtime_contract_version == RUNTIME_CONTRACT_VERSION
    assert (
        len(
            list(
                store.iter_all(
                    exchange="binance",
                    market="usdm",
                    symbol="BTCUSDT",
                    timeframe="1m",
                    start=60_000,
                    end=180_000,
                )
            )
        )
        == 2
    )
    assert (
        store.read_all(
            exchange="binance",
            market="usdm",
            symbol="BTCUSDT",
            timeframe="1m",
            start=60_000,
            end=120_000,
        )[0].time
        == 60_000
    )
    assert market_bar_checksum(bars[0]) == bars_checksum([bars[0]])
    assert store._parse_bool("YES") is True
    assert store._parse_bool("no") is False
    assert store._parse_bool("", default=False) is False
    assert (
        store._row_to_bar(
            {
                "time": "1",
                "open": "1",
                "high": "2",
                "low": "0.5",
                "close": "1.5",
                "volume": "1",
                "is_closed": "false",
            }
        ).is_closed
        is False
    )

    manifest_path = (
        store._dir(
            exchange="binance",
            market="usdm",
            symbol="BTCUSDT",
            timeframe="1m",
            source_kind="trade_kline",
        )
        / "manifest.json"
    )
    data = json.loads(manifest_path.read_text())
    data["runtime_contract_version"] = "0"
    manifest_path.write_text(json.dumps(data))
    with pytest.raises(MDInvalidExchangeResponse, match="Unsupported segment"):
        store.read_all(
            exchange="binance", market="usdm", symbol="BTCUSDT", timeframe="1m"
        )

    with pytest.raises(sqlite3.OperationalError), store._connect_index() as db:
        db.execute("SELECT * FROM missing_table")


def test_timeframe_mapping_edges() -> None:
    assert canonical_timeframe("60") == "60m"
    assert timeframe_ms("2h") == 7_200_000
    assert close_time_ms(0, "1m") == 59_999
    assert next_open_time_ms(0, "1m") == 60_000
    assert to_binance_interval("1h") == "1h"
    assert to_bybit_interval("1h") == "60"
    with pytest.raises(MDTimeframeUnsupported):
        timeframe_ms("bad")


def test_binance_agg_trades_edges(monkeypatch: pytest.MonkeyPatch) -> None:
    from marketdata_provider.exchanges.binance import trades as bt

    monkeypatch.setattr(bt.time, "sleep", lambda *_: None)
    payload = [
        {"a": 2, "T": 20, "p": "2", "q": "0.5", "m": True},
        {"a": 1, "T": 10, "p": "1", "q": "1.5", "m": False},
    ]
    assert [t.trade_id for t in bt.normalize_binance_agg_trades(payload)] == [1, 2]
    with pytest.raises(MDInvalidExchangeResponse):
        bt.normalize_binance_agg_trades({})
    with pytest.raises(MDInvalidExchangeResponse):
        bt.normalize_binance_agg_trades([{"bad": 1}])

    assert (
        bt._get_json(
            FakeClient([FakeResponse(500, {}), FakeResponse(200, payload)]),
            "u",
            {},
            max_retries=1,
        )
        == payload
    )
    with pytest.raises(MDNetworkUnavailable, match="rate limit"):
        bt._get_json(FakeClient([FakeResponse(429, {})]), "u", {}, max_retries=0)
    with pytest.raises(MDNetworkUnavailable, match="request failed"):
        bt._get_json(
            FakeClient([FakeResponse(400, {}, raise_http=True)]), "u", {}, max_retries=0
        )

    fake = FakeClient([FakeResponse(200, payload)])
    monkeypatch.setattr(bt.httpx, "Client", lambda **_: fake)
    trades = bt.binance_get_agg_trades_sync(
        "BTCUSDT", 0, 30, BinanceConfig(), market="usdm", max_retries=0, max_trades=2
    )
    assert [trade.trade_id for trade in trades] == [1, 2]
    fake = FakeClient([FakeResponse(200, [])])
    monkeypatch.setattr(bt.httpx, "Client", lambda **_: fake)
    assert (
        bt.binance_get_agg_trades_sync(
            "BTCUSDT", 0, 30, BinanceConfig(), market="coinm"
        )
        == []
    )


def test_cache_raw_store_and_distribution_edges(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    from marketdata_provider.cache.local import read_cache_segment, write_cache_segment
    from marketdata_provider.distribution import (
        distribution_manifest,
    )
    from marketdata_provider.distribution import (
        main as distribution_main,
    )
    from marketdata_provider.store.raw_store import RawStore

    bars = [Bar(0, 1, 2, 0.5, 1.5, 1, 59_999), Bar(60_000, 2, 3, 1.5, 2.5, 2, 119_999)]
    meta = write_cache_segment(
        tmp_path / "cache",
        bars,
        exchange="BINANCE",
        market="USDM",
        symbol="BTC/USDT",
        timeframe="1m",
    )
    assert meta.bars == 2
    assert (
        len(
            read_cache_segment(
                tmp_path / "cache",
                exchange="binance",
                market="usdm",
                symbol="BTC/USDT",
                timeframe="1m",
                start=60_000,
                max_bars=1,
            )
        )
        == 1
    )
    meta_path = next((tmp_path / "cache").rglob("metadata.json"))
    bad = json.loads(meta_path.read_text())
    bad["runtime_contract_version"] = "0"
    meta_path.write_text(json.dumps(bad))
    with pytest.raises(MDInvalidExchangeResponse, match="Unsupported cache"):
        read_cache_segment(
            tmp_path / "cache",
            exchange="binance",
            market="usdm",
            symbol="BTC/USDT",
            timeframe="1m",
        )
    with pytest.raises(MDUnsupportedFeature, match="Cache segment not found"):
        read_cache_segment(
            tmp_path / "missing",
            exchange="binance",
            market="usdm",
            symbol="BTC",
            timeframe="1m",
        )

    with pytest.raises(MDUnsupportedFeature):
        RawStore(tmp_path / "raw", compression="zip")  # type: ignore[arg-type]
    monkeypatch.setattr(
        "marketdata_provider.store.raw_store.importlib.util.find_spec",
        lambda name: None if name == "zstandard" else importlib.util.find_spec(name),
    )
    with pytest.raises(MDUnsupportedFeature):
        RawStore(tmp_path / "rawz", compression="zstd")
    monkeypatch.setattr(
        "marketdata_provider.store.segment_store.importlib.util.find_spec",
        lambda name: None if name == "pyarrow" else importlib.util.find_spec(name),
    )
    with pytest.raises(MDUnsupportedFeature, match="pyarrow"):
        SegmentStore(tmp_path / "segments-parquet", data_format="parquet")
    import builtins

    from marketdata_provider.providers.offline import OfflineDataProvider

    real_import = builtins.__import__

    def fail_pyarrow_import(name, *args, **kwargs):
        if name == "pyarrow.parquet":
            raise ImportError("no pyarrow")
        return real_import(name, *args, **kwargs)

    with monkeypatch.context() as mp:
        mp.setattr(builtins, "__import__", fail_pyarrow_import)
        with pytest.raises(MDUnsupportedFeature, match="pyarrow"):
            OfflineDataProvider(tmp_path / "offline.parquet").get_bars(
                "BTCUSDT", "1m", None, None
            )
    raw = RawStore(tmp_path / "raw")
    raw.write_batch(
        [{"a": 1}],
        exchange="BINANCE",
        market="USDM",
        symbol="BTCUSDT",
        source_transport="rest",
    )
    raw.write_batch(
        [{"b": 2}],
        exchange="BINANCE",
        market="USDM",
        symbol="BTCUSDT",
        source_transport="rest",
        partition="p1",
    )
    assert (
        len(
            raw.read_partitions(
                exchange="BINANCE",
                market="USDM",
                symbol="BTCUSDT",
                source_transport="rest",
                source_kind="trade_kline",
            )
        )
        == 2
    )
    manifest = next((tmp_path / "raw").rglob("manifest.json"))
    data = json.loads(manifest.read_text())
    data["checksum"] = "bad"
    manifest.write_text(json.dumps(data))
    with pytest.raises(MDUnsupportedFeature, match="checksum"):
        raw.read_batch(
            exchange="BINANCE", market="USDM", symbol="BTCUSDT", source_transport="rest"
        )

    assert distribution_manifest(Path.cwd()).forbidden_count == 0
    assert distribution_main(["manifest", "--root", "."]) == 0
    assert "forbidden_count" in capsys.readouterr().out
    out = tmp_path / "md.zip"
    assert (
        distribution_main(
            ["build-zip", "--root", ".", "--output", str(out), "--archive-root", "md"]
        )
        == 0
    )
    assert out.exists()


def test_factory_helper_boundaries(tmp_path: Path) -> None:
    from marketdata_provider.config import (
        MarketDataConfig,
        OfflineDataConfig,
        StorageConfig,
    )
    from marketdata_provider.contracts.bar import Bar as ContractBar
    from marketdata_provider.contracts.instrument import InstrumentKey
    from marketdata_provider.contracts.query import BarQuery
    from marketdata_provider.contracts.series import BarSeries, CoverageReport
    from marketdata_provider.contracts.timeframe import parse_timeframe
    from marketdata_provider.factories import (
        _can_bulk_write_closed,
        _CandleStoreAdapter,
        _same_candle_payload,
        _series_write_error,
        _stored_bars_read_error,
        create_candle_store,
        create_provider,
    )

    instrument = InstrumentKey("binance", "usdm", "BTCUSDT")
    tf = parse_timeframe("1m")
    query = BarQuery(instrument, tf, 0, 60_000)
    bar = ContractBar(instrument, tf, 0, 59_999, 1, 2, 0.5, 1.5, 1, True)
    series = BarSeries(query, (bar,), CoverageReport(0, 60_000, 0, 60_000))
    assert _series_write_error(series) is None
    wrong_series = BarSeries(
        query,
        (
            ContractBar(
                InstrumentKey("binance", "spot", "BTCUSDT"),
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
    assert "instrument" in (_series_write_error(wrong_series) or "")
    wrong_tf = BarSeries(
        query,
        (
            ContractBar(
                instrument, parse_timeframe("5m"), 0, 299_999, 1, 2, 0.5, 1.5, 1, True
            ),
        ),
        series.coverage,
    )
    assert "timeframe" in (_series_write_error(wrong_tf) or "")

    mbar = MarketBar(
        time=0,
        open=1,
        high=2,
        low=0.5,
        close=1.5,
        volume=1,
        time_close=59_999,
        exchange="binance",
        market="usdm",
        symbol="BTCUSDT",
        timeframe="1m",
        is_closed=True,
    )
    assert _same_candle_payload(mbar, mbar)
    assert _can_bulk_write_closed([mbar]) is True
    assert _can_bulk_write_closed([]) is False
    assert _can_bulk_write_closed([replace(mbar, is_closed=False)]) is False
    assert _stored_bars_read_error(query, (mbar,)) is None
    assert "instrument" in (
        _stored_bars_read_error(query, (replace(mbar, exchange=""),)) or ""
    )
    assert "timeframe" in (
        _stored_bars_read_error(query, (replace(mbar, timeframe="bad"),)) or ""
    )

    csv_path = tmp_path / "offline.csv"
    csv_path.write_text(
        "time,open,high,low,close,volume,time_close\n0,1,2,0.5,1.5,1,59999\n"
    )
    provider = create_provider(
        MarketDataConfig(offline=OfflineDataConfig(root=csv_path))
    )
    assert provider.fetch_bars(query).bars[0].close == 1.5
    store_adapter = create_candle_store(
        MarketDataConfig(storage=StorageConfig(cache_dir=tmp_path / "store"))
    )
    write_result = store_adapter.write(series)
    assert write_result.success and write_result.rows_written == 1
    assert store_adapter.read(query).bars[0].close == 1.5
    assert store_adapter.latest_bar_time(query) == 0
    assert _CandleStoreAdapter(store_adapter.store).coverage(query).is_complete


def test_service_aggregation_and_archive_helpers(tmp_path: Path) -> None:
    from marketdata_provider.config import HistoryConfig, MarketDataConfig
    from marketdata_provider.contracts.instrument import InstrumentKey
    from marketdata_provider.contracts.query import BarQuery
    from marketdata_provider.contracts.timeframe import parse_timeframe
    from marketdata_provider.service import (
        _aggregate_market_bars,
        _archive_cutoff_ms,
        _can_derive_from_base,
        _merge_bars,
        _remaining_recent_query,
    )

    q = BarQuery(
        InstrumentKey("binance", "usdm", "BTCUSDT"), parse_timeframe("2m"), 0, 240_000
    )
    bars = [
        MarketBar(
            time=0,
            open=1,
            high=2,
            low=0.5,
            close=1.5,
            volume=0,
            time_close=59_999,
            exchange="binance",
            market="usdm",
            symbol="BTCUSDT",
            timeframe="1m",
        ),
        MarketBar(
            time=60_000,
            open=1.5,
            high=3,
            low=1,
            close=2.5,
            volume=2,
            time_close=119_999,
            exchange="binance",
            market="usdm",
            symbol="BTCUSDT",
            timeframe="1m",
        ),
        MarketBar(
            time=120_000,
            open=2.5,
            high=4,
            low=2,
            close=3.5,
            volume=3,
            time_close=179_999,
            exchange="binance",
            market="usdm",
            symbol="BTCUSDT",
            timeframe="1m",
        ),
    ]
    agg = _aggregate_market_bars(bars, query=q)
    assert [b.time for b in agg] == [0, 120_000]
    assert agg[0].open == 1.5 and agg[0].close == 2.5
    assert _merge_bars([bars[0]], [replace(bars[0], close=9)])[0].close == 9
    assert _can_derive_from_base(q, parse_timeframe("1m")) is True
    cutoff = _archive_cutoff_ms(
        MarketDataConfig(history=HistoryConfig(recent_lag_days=1))
    )
    assert isinstance(cutoff, int)
    assert (
        _remaining_recent_query(
            q, [], MarketDataConfig(history=HistoryConfig(recent_lag_days=30_000))
        )
        == q
    )
    assert (
        _remaining_recent_query(
            q, bars, MarketDataConfig(history=HistoryConfig(recent_lag_days=30_000))
        )
        is None
    )
    q_long = BarQuery(q.instrument, q.timeframe, 0, 360_000)
    assert (
        _remaining_recent_query(
            q_long,
            bars,
            MarketDataConfig(history=HistoryConfig(recent_lag_days=30_000)),
        ).start_ms
        == 240_000
    )


def test_archive_fetch_fill_and_loader_branches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    b0 = Bar(0, 1, 2, 0.5, 1.5, 1, 59_999)
    b1 = Bar(60_000, 1, 2, 0.5, 1.6, 1, 119_999)

    calls: list[tuple[str, str]] = []

    def fake_load_file(**kwargs: Any) -> list[Bar]:
        calls.append((kwargs["period"], kwargs["suffix"]))
        return [b0, b1] if kwargs["period"] == "daily" else []

    monkeypatch.setattr(ba, "_load_archive_file", fake_load_file)
    repaired = ba.fill_binance_archive_gaps(
        [b0],
        symbol="BTCUSDT",
        market="usdm",
        timeframe="1m",
        start=0,
        end=120_000,
        cache_dir=tmp_path,
    )
    assert [b.time for b in repaired] == [0, 60_000]
    assert any(period == "daily" for period, _ in calls)

    assert (
        ba.fill_binance_archive_gaps(
            [],
            symbol="BTCUSDT",
            market="usdm",
            timeframe="1m",
            start=0,
            end=60_000,
            cache_dir=tmp_path,
        )
        == []
    )
    assert ba.fill_binance_archive_gaps(
        [b0],
        symbol="BTCUSDT",
        market="bad",
        timeframe="1m",
        start=0,
        end=60_000,
        cache_dir=tmp_path,
    ) == [b0]
    assert ba._missing_starts(
        {0: b0}, start=0, end=120_000, duration=60_000
    ) == frozenset({60_000})
    assert ba._missing_intervals({0: b0}, start=0, end=120_000, duration=60_000) == (
        (60_000, 120_000),
    )

    # Daily archive incomplete => monthly fallback branch.
    monkeypatch.setattr(
        ba,
        "_days_for_intervals",
        lambda intervals: ((1970, 1, 1),) * (ba.MAX_DAILY_ARCHIVE_DAYS + 1),
    )
    monkeypatch.setattr(ba, "_months_for_intervals", lambda intervals: ((1970, 1),))
    monkeypatch.setattr(
        ba,
        "_load_archive_file",
        lambda **kwargs: [b1] if kwargs["period"] == "monthly" else [],
    )
    assert ba._load_archive_bars(
        symbol="BTCUSDT",
        market="usdm",
        timeframe="1m",
        start=0,
        end=120_000,
        missing_intervals=((60_000, 120_000),),
        cache_dir=tmp_path,
    ) == [b1]

    monkeypatch.setattr(
        ba,
        "_months_for_intervals",
        lambda intervals: tuple(
            (1970, m) for m in range(1, ba.MAX_MONTHLY_ARCHIVE_MONTHS + 3)
        ),
    )
    assert (
        ba._load_archive_bars(
            symbol="BTCUSDT",
            market="usdm",
            timeframe="1m",
            start=0,
            end=120_000,
            missing_intervals=((60_000, 120_000),),
            cache_dir=tmp_path,
        )
        == []
    )

    monkeypatch.setattr(
        ba,
        "_load_archive_file",
        lambda **kwargs: [b0, b1] if kwargs["period"] == "monthly" else [],
    )
    monkeypatch.setattr(ba, "_months_for_intervals", lambda intervals: ((1970, 1),))
    assert ba.fetch_binance_archive_bars(
        symbol="BTCUSDT",
        market="usdm",
        timeframe="1m",
        start=0,
        end=120_000,
        cache_dir=tmp_path,
    ) == [b0, b1]
    assert (
        ba.fetch_binance_archive_bars(
            symbol="BTCUSDT",
            market="bad",
            timeframe="1m",
            start=0,
            end=120_000,
            cache_dir=tmp_path,
        )
        == []
    )


def test_provider_additional_branches_and_async_wrappers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import asyncio

    monkeypatch.setattr(bp.time, "sleep", lambda *_: None)
    assert bp._get_json(
        FakeClient([FakeResponse(500, {}), FakeResponse(200, {"ok": 1})]),
        "u",
        {},
        max_retries=1,
    ) == {"ok": 1}
    assert bp._get_json(
        FakeClient(
            [FakeResponse(400, {}, raise_http=True), FakeResponse(200, {"ok": 2})]
        ),
        "u",
        {},
        max_retries=1,
    ) == {"ok": 2}

    monkeypatch.setattr(
        bp.httpx,
        "Client",
        lambda **_: FakeClient(
            [FakeResponse(200, {"serverTime": 10**12}), FakeResponse(200, [])]
        ),
    )
    assert (
        bp.binance_get_bars_sync(
            "BTCUSDT",
            "1m",
            0,
            60_000,
            BinanceConfig(),
            market="usdm",
            max_bars=0,
            include_open_candle=True,
        )
        == []
    )
    monkeypatch.setattr(
        bp.httpx,
        "Client",
        lambda **_: FakeClient(
            [FakeResponse(200, {"serverTime": 10**12}), FakeResponse(200, "bad")]
        ),
    )
    with pytest.raises(MDInvalidExchangeResponse, match="payload"):
        bp.binance_get_bars_sync(
            "BTCUSDT",
            "1m",
            0,
            60_000,
            BinanceConfig(),
            market="usdm",
            max_retries=0,
            include_open_candle=True,
        )

    async def fake_binance(
        symbol: str,
        timeframe: str,
        start: int | None,
        end: int | None,
        cfg: BinanceConfig,
        market: str = "usdm",
        timeout: float = 15.0,
        max_retries: int = 3,
        max_bars: int | None = None,
    ) -> list[Bar]:
        return [Bar(start or 0, 1, 2, 0.5, 1.5, 1, end)]

    monkeypatch.setattr(bp, "binance_get_bars", fake_binance)
    assert (
        asyncio.run(
            bp.binance_get_intrabar_bars(
                "BTCUSDT", Bar(10, 1, 2, 0.5, 1.5, 1, 69), None, BinanceConfig()
            )
        )[0].time
        == 10
    )

    monkeypatch.setattr(yp.time, "sleep", lambda *_: None)
    assert yp._get_json(
        FakeClient(
            [FakeResponse(200, {"retCode": 10006}), FakeResponse(200, {"retCode": 0})]
        ),
        "u",
        {},
        max_retries=1,
    ) == {"retCode": 0}
    assert yp._get_json(
        FakeClient(
            [FakeResponse(400, {}, raise_http=True), FakeResponse(200, {"retCode": 0})]
        ),
        "u",
        {},
        max_retries=1,
    ) == {"retCode": 0}
    monkeypatch.setattr(
        yp.httpx,
        "Client",
        lambda **_: FakeClient(
            [
                FakeResponse(200, {"result": {"timeNano": "1000000000000"}}),
                FakeResponse(200, {"retCode": 0, "result": {"list": []}}),
            ]
        ),
    )
    assert (
        yp.bybit_get_bars_sync(
            "BTCUSDT",
            "1m",
            0,
            60_000,
            BybitConfig(),
            market="linear",
            max_bars=0,
            include_open_candle=True,
        )
        == []
    )

    async def fake_bybit(
        symbol: str,
        timeframe: str,
        start: int | None,
        end: int | None,
        cfg: BybitConfig,
        market: str = "linear",
        timeout: float = 15.0,
        max_retries: int = 3,
        max_bars: int | None = None,
    ) -> list[Bar]:
        return [Bar(start or 0, 1, 2, 0.5, 1.5, 1, end)]

    monkeypatch.setattr(yp, "bybit_get_bars", fake_bybit)
    assert (
        asyncio.run(
            yp.bybit_get_intrabar_bars(
                "BTCUSDT", Bar(10, 1, 2, 0.5, 1.5, 1, 69), None, BybitConfig()
            )
        )[0].time
        == 10
    )


def test_contract_error_edges_and_calendar_timeframes():
    from marketdata_provider.contracts.bar import Bar as ContractBar
    from marketdata_provider.contracts.errors import (
        InvalidBarError,
        InvalidBarQueryError,
        InvalidInstrumentError,
    )
    from marketdata_provider.contracts.footprint import FootprintLevel, FootprintQuery
    from marketdata_provider.contracts.instrument import InstrumentKey
    from marketdata_provider.contracts.timeframe import parse_timeframe
    from marketdata_provider.errors import MDTimeframeUnsupported
    from marketdata_provider.timeframes import (
        close_time_ms,
        default_intrabar_tf,
        is_calendar_timeframe,
        next_open_time_ms,
        parse_time_ms,
        timeframe_ms,
        timeframe_to_binance_interval,
        timeframe_to_bybit_interval,
    )

    instrument = InstrumentKey(" BINANCE ", " SPOT ", " btcusdt ")
    assert str(instrument) == "binance/spot/BTCUSDT"
    for bad in ("", "binance", "binance/spot"):
        with pytest.raises(InvalidInstrumentError):
            if bad:
                InstrumentKey.parse(bad)
            else:
                InstrumentKey(" ", "spot", "BTCUSDT")
    with pytest.raises(InvalidInstrumentError):
        InstrumentKey("binance", " ", "BTCUSDT")
    with pytest.raises(InvalidInstrumentError):
        InstrumentKey("binance", "spot", " ")

    tf_1m = parse_timeframe("1m")
    tf_1m_raw = parse_timeframe(" 1 ")
    assert tf_1m_raw.canonical == "1m"
    with pytest.raises(InvalidBarError):
        ContractBar(instrument, tf_1m, 1000, 1000, 1, 1, 1, 1, 1, True)
    with pytest.raises(InvalidBarError):
        ContractBar(instrument, tf_1m, 1000, 2000, 1, 0.9, 1, 1, 1, True)
    with pytest.raises(InvalidBarError):
        ContractBar(instrument, tf_1m, 1000, 2000, 1, 1, 1.1, 1, 1, True)
    with pytest.raises(InvalidBarError):
        ContractBar(instrument, tf_1m, 1000, 2000, 1, 1, 1, 1, -1, True)

    valid = FootprintQuery(instrument, tf_1m, 0, 60_000, tick_size=0.5, ticks_per_row=2)
    assert valid.bucket_size == 1.0
    assert (
        FootprintQuery(instrument, tf_1m, 0, 60_000, price_bucket=2.5).bucket_size
        == 2.5
    )
    monthly = parse_timeframe("1M")
    for kwargs in (
        {"start_ms": 10, "end_ms": 10, "tick_size": 1.0},
        {"start_ms": 0, "end_ms": 10, "tick_size": 1.0, "timeframe": monthly},
        {"start_ms": 0, "end_ms": 10, "tick_size": 1.0, "source": "bad"},
        {"start_ms": 0, "end_ms": 10, "tick_size": 1.0, "gap_policy": "bad"},
        {"start_ms": 0, "end_ms": 10, "tick_size": None},
        {"start_ms": 0, "end_ms": 10, "tick_size": 1.0, "ticks_per_row": 0},
        {"start_ms": 0, "end_ms": 10, "price_bucket": 0},
    ):
        params = {
            "instrument": instrument,
            "timeframe": kwargs.pop("timeframe", tf_1m),
            **kwargs,
        }
        with pytest.raises(InvalidBarQueryError):
            FootprintQuery(**params)  # type: ignore[arg-type]
    level = FootprintLevel(100, 101, buy_volume=3, sell_volume=2)
    assert level.total_volume == 5
    assert level.volume_delta == 1

    assert is_calendar_timeframe("1D") is True
    assert timeframe_ms("1W") == 7 * 86_400_000
    with pytest.raises(MDTimeframeUnsupported):
        timeframe_ms("1M")
    with pytest.raises(MDTimeframeUnsupported):
        timeframe_ms("2D")
    assert parse_time_ms(1_700_000_000) == 1_700_000_000_000
    assert parse_time_ms("1700000000") == 1_700_000_000_000
    assert parse_time_ms("2020-01-01T00:00:00Z") == 1_577_836_800_000
    assert close_time_ms(1_577_836_800_000, "1D") == 1_577_923_199_999
    assert close_time_ms(1_577_836_800_000, "1W") == 1_578_268_799_999
    assert close_time_ms(1_577_836_800_000, "1M") == 1_580_515_199_999
    assert next_open_time_ms(1_577_836_800_000, "1D") == 1_577_923_200_000
    assert timeframe_to_binance_interval("1D") == "1d"
    assert timeframe_to_bybit_interval("1D") == "D"
    with pytest.raises(MDTimeframeUnsupported):
        timeframe_to_binance_interval("bad")
    assert default_intrabar_tf("1D") == "60m"
    assert default_intrabar_tf("30m") == "1m"
    assert default_intrabar_tf("2h") == "5m"


def test_compat_import_shims_and_adapter_edge_branches():
    import marketdata_provider.core.timeframes as core_timeframes
    import marketdata_provider.core.validation as core_validation
    from marketdata_provider._adapters import (
        bar_exclusive_end,
        coverage_report,
        missing_intervals_for,
    )
    from marketdata_provider.contracts.bar import Bar as ContractBar
    from marketdata_provider.contracts.instrument import InstrumentKey
    from marketdata_provider.contracts.query import BarQuery
    from marketdata_provider.contracts.timeframe import parse_timeframe
    from marketdata_provider.errors import MDTimeframeUnsupported
    from marketdata_provider.timeframes import (
        timeframe_to_binance_interval,
        timeframe_to_bybit_interval,
    )

    assert core_timeframes.normalize_timeframe("1") == "1m"
    assert callable(core_validation.validate_bars)
    with pytest.raises(MDTimeframeUnsupported):
        timeframe_to_binance_interval("999m")
    with pytest.raises(MDTimeframeUnsupported):
        timeframe_to_bybit_interval("999m")

    instrument = InstrumentKey("binance", "spot", "BTCUSDT")
    monthly = parse_timeframe("1M")
    query = BarQuery(instrument, monthly, 0, 10)
    assert missing_intervals_for(query, ()) == ()
    bar = ContractBar(instrument, monthly, 0, 1, 1, 1, 1, 1, None, True)
    assert bar_exclusive_end(bar, monthly) == 2
    empty = coverage_report(query, (), source="unit")
    assert empty.status == "empty"
    assert empty.missing_intervals == ((0, 10),)


def test_cache_checksum_mismatch_and_distribution_forbidden(tmp_path):
    import json

    from marketdata_provider.cache.local import (
        cache_segment_dir,
        read_cache_segment,
        write_cache_segment,
    )
    from marketdata_provider.core.bar import Bar
    from marketdata_provider.distribution import distribution_manifest
    from marketdata_provider.errors import MDInvalidExchangeResponse

    bars = [Bar(0, 1, 1, 1, 1, 1, 59_999)]
    write_cache_segment(
        tmp_path,
        bars,
        exchange="binance",
        market="spot",
        symbol="BTCUSDT",
        timeframe="1m",
    )
    seg = cache_segment_dir(
        tmp_path, exchange="binance", market="spot", symbol="BTCUSDT", timeframe="1m"
    )
    meta_path = seg / "metadata.json"
    meta = json.loads(meta_path.read_text())
    meta["checksum"] = "bad"
    meta_path.write_text(json.dumps(meta))
    with pytest.raises(MDInvalidExchangeResponse):
        read_cache_segment(
            tmp_path,
            exchange="binance",
            market="spot",
            symbol="BTCUSDT",
            timeframe="1m",
        )

    root = tmp_path / "distroot"
    root.mkdir()
    (root / "ok.py").write_text("print('ok')\n")
    git_dir = root / ".git"
    git_dir.mkdir()
    (git_dir / "ignored").write_text("ignored")
    manifest = distribution_manifest(root)
    assert manifest.file_count == 1
    assert manifest.forbidden_count == 0
