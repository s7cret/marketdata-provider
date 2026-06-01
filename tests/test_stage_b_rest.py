from zipfile import ZipFile

import httpx
import pytest

from marketdata_provider.config import BinanceConfig, BybitConfig, MarketDataConfig, StorageConfig
from marketdata_provider.contracts.instrument import InstrumentKey
from marketdata_provider.contracts.query import BarQuery
from marketdata_provider.contracts.timeframe import parse_timeframe
from marketdata_provider.errors import MDNetworkUnavailable
from marketdata_provider.exchanges.binance import provider as binance_provider
from marketdata_provider.exchanges.bybit import provider as bybit_provider
from marketdata_provider.factories import create_candle_store
from marketdata_provider.service import MarketDataService


def _client_factory(monkeypatch, module, handler):
    real_client = httpx.Client
    def factory(*args, **kwargs):
        kwargs["transport"] = httpx.MockTransport(handler)
        return real_client(*args, **kwargs)
    monkeypatch.setattr(module.httpx, "Client", factory)


def test_binance_pagination_and_open_candle_exclusion(monkeypatch):
    calls = []
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/time"):
            return httpx.Response(200, json={"serverTime": 121000})
        calls.append(dict(request.url.params))
        start = int(request.url.params.get("startTime", "1000"))
        if start <= 1000:
            rows = [[1000, "1", "2", "0.5", "1.5", "10", 60999], [61000, "1.5", "2", "1", "1.2", "5", 120999]]
        else:
            rows = [[121000, "1", "2", "0.5", "1.1", "1", 180999]]
        return httpx.Response(200, json=rows)
    _client_factory(monkeypatch, binance_provider, handler)
    bars = binance_provider.binance_get_bars_sync("BINANCE:BTCUSDT", "1m", 1000, None, BinanceConfig(max_limit_spot=2), market="spot", max_bars=3)
    assert [b.time for b in bars] == [1000, 61000]
    assert calls[1]["startTime"] == "121000"


def test_binance_rate_limit_error(monkeypatch):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/time"):
            return httpx.Response(200, json={"serverTime": 999999})
        return httpx.Response(429, headers={"Retry-After": "0"}, json={"code": -1003})
    _client_factory(monkeypatch, binance_provider, handler)
    with pytest.raises(MDNetworkUnavailable) as e:
        binance_provider.binance_get_bars_sync("BINANCE:BTCUSDT", "1m", None, None, BinanceConfig(), market="spot", max_retries=0)
    assert "rate limit" in e.value.message.lower()


def test_marketdata_service_archive_first_prefers_monthly_cache(tmp_path):
    daily = (
        tmp_path
        / "archives"
        / "binance_klines"
        / "spot"
        / "daily"
        / "BTCUSDT"
        / "1m"
        / "BTCUSDT-1m-1970-01-01.zip"
    )
    monthly = (
        tmp_path
        / "archives"
        / "binance_klines"
        / "spot"
        / "monthly"
        / "BTCUSDT"
        / "1m"
        / "BTCUSDT-1m-1970-01.zip"
    )
    daily.parent.mkdir(parents=True)
    monthly.parent.mkdir(parents=True)
    with ZipFile(daily, "w") as zf:
        zf.writestr("BTCUSDT-1m-1970-01-01.csv", "0,1,1,1,1,1,59999\n")
    with ZipFile(monthly, "w") as zf:
        zf.writestr(
            "BTCUSDT-1m-1970-01.csv",
            "0,1,1,1,1,1,59999\n60799,2,2.5,1.5,2.1,4,120798\n120000,3,3,3,3,3,179999\n",
        )

    series = MarketDataService(MarketDataConfig(storage=StorageConfig(cache_dir=tmp_path))).fetch_bars(
        BarQuery(
            instrument=InstrumentKey("binance", "spot", "BTCUSDT"),
            timeframe=parse_timeframe("1m"),
            start_ms=0,
            end_ms=180000,
        )
    )

    assert [bar.time for bar in series.bars] == [0, 60000, 120000]
    assert series.bars[1].open == 2.0
    assert series.bars[1].time_close == 119999


def test_marketdata_service_archive_first_falls_back_to_daily_cache(tmp_path):
    daily = (
        tmp_path
        / "archives"
        / "binance_klines"
        / "spot"
        / "daily"
        / "BTCUSDT"
        / "1m"
        / "BTCUSDT-1m-1970-01-01.zip"
    )
    daily.parent.mkdir(parents=True)
    with ZipFile(daily, "w") as zf:
        zf.writestr("BTCUSDT-1m-1970-01-01.csv", "0,1,1,1,1,1,59999\n60000,2,2,2,2,2,119999\n")

    series = MarketDataService(MarketDataConfig(storage=StorageConfig(cache_dir=tmp_path))).fetch_bars(
        BarQuery(
            instrument=InstrumentKey("binance", "spot", "BTCUSDT"),
            timeframe=parse_timeframe("1m"),
            start_ms=0,
            end_ms=120000,
        )
    )

    assert [bar.time for bar in series.bars] == [0, 60000]
    assert series.bars[1].open == 2.0


def test_marketdata_service_daily_aggregation_uses_first_traded_open(tmp_path):
    monthly = (
        tmp_path
        / "archives"
        / "binance_klines"
        / "spot"
        / "monthly"
        / "BTCUSDT"
        / "1m"
        / "BTCUSDT-1m-1970-01.zip"
    )
    monthly.parent.mkdir(parents=True)
    with ZipFile(monthly, "w") as zf:
        zf.writestr(
            "BTCUSDT-1m-1970-01.csv",
            "0,10,10,10,10,0,59999\n60000,8,11,7,9,2,119999\n120000,9,9,9,9,0,179999\n",
        )

    series = MarketDataService(MarketDataConfig(storage=StorageConfig(cache_dir=tmp_path))).fetch_bars(
        BarQuery(
            instrument=InstrumentKey("binance", "spot", "BTCUSDT"),
            timeframe=parse_timeframe("1D"),
            start_ms=0,
            end_ms=86_400_000,
        )
    )

    assert len(series.bars) == 1
    assert series.bars[0].open == 8.0
    assert series.bars[0].close == 9.0


def test_candle_store_write_is_idempotent_across_provider_provenance(tmp_path):
    query = BarQuery(
        instrument=InstrumentKey("binance", "spot", "BTCUSDT"),
        timeframe=parse_timeframe("15m"),
        start_ms=0,
        end_ms=900000,
    )
    provider = MarketDataService(MarketDataConfig(storage=StorageConfig(cache_dir=tmp_path))).fetch_bars

    monthly = (
        tmp_path
        / "archives"
        / "binance_klines"
        / "spot"
        / "monthly"
        / "BTCUSDT"
        / "15m"
        / "BTCUSDT-15m-1970-01.zip"
    )
    monthly.parent.mkdir(parents=True)
    with ZipFile(monthly, "w") as zf:
        zf.writestr("BTCUSDT-15m-1970-01.csv", "0,1,2,0.5,1.5,10,899999\n")

    series = provider(query)
    store = create_candle_store(MarketDataConfig(storage=StorageConfig(cache_dir=tmp_path)))
    result = store.write(series)

    assert result.success
    assert result.rows_written == 0


def test_bybit_reverse_sort_pagination_and_open_candle_exclusion(monkeypatch):
    calls = []
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/time"):
            return httpx.Response(200, json={"retCode": 0, "result": {"timeNano": "121000000000"}})
        calls.append(dict(request.url.params))
        start = int(request.url.params.get("start", "1000"))
        if start <= 1000:
            rows = [[61000, "1.5", "2", "1", "1.2", "5"], [1000, "1", "2", "0.5", "1.5", "10"]]
        else:
            rows = [[121000, "1", "2", "0.5", "1.1", "1"]]
        return httpx.Response(200, json={"retCode": 0, "result": {"list": rows}})
    _client_factory(monkeypatch, bybit_provider, handler)
    bars = bybit_provider.bybit_get_bars_sync("BYBIT:BTCUSDT", "1m", 1000, None, BybitConfig(max_limit=2), market="spot", max_bars=3)
    assert [b.time for b in bars] == [1000, 61000]
    assert calls[1]["start"] == "121000"


def test_bybit_rate_limit_retcode(monkeypatch):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/time"):
            return httpx.Response(200, json={"retCode": 0, "result": {"timeSecond": "999"}})
        return httpx.Response(200, json={"retCode": 10006, "retMsg": "Too many visits"})
    _client_factory(monkeypatch, bybit_provider, handler)
    with pytest.raises(MDNetworkUnavailable):
        bybit_provider.bybit_get_bars_sync("BYBIT:BTCUSDT", "1m", None, None, BybitConfig(), market="spot", max_retries=0)


@pytest.mark.skipif(__import__("os").getenv("RUN_MARKETDATA_NETWORK_TESTS") != "1", reason="network tests disabled by default")
def test_network_smoke_binance_disabled_by_default():
    bars = binance_provider.binance_get_bars_sync("BINANCE:BTCUSDT", "1m", None, None, BinanceConfig(), market="spot", max_bars=2)
    assert len(bars) <= 2
