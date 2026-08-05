from __future__ import annotations

from marketdata_provider.config import MarketDataConfig, StorageConfig
from marketdata_provider.contracts import BarQuery, InstrumentKey, parse_timeframe
from marketdata_provider.core.bar import Bar
from marketdata_provider.exchanges.registry import list_exchanges
from marketdata_provider.service import MarketDataService
from marketdata_provider.symbols import search_symbols


class _FakeResponse:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code
        self.headers = {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"bad status: {self.status_code}")

    def json(self):
        return self._payload


class _FakeClient:
    calls: list[tuple[str, dict[str, object], dict[str, object]]] = []

    PAYLOADS = {
        "www.okx.com": {
            "data": [
                {
                    "instId": "BTC-USDT",
                    "baseCcy": "BTC",
                    "quoteCcy": "USDT",
                    "state": "live",
                }
            ]
        },
        "api.exchange.coinbase.com": [
            {
                "id": "BTC-USD",
                "base_currency": "BTC",
                "quote_currency": "USD",
                "status": "online",
                "trading_disabled": False,
            }
        ],
        "api.kraken.com": {
            "result": {
                "XXBTZUSD": {
                    "altname": "XBTUSD",
                    "wsname": "BTC/USD",
                    "status": "online",
                }
            }
        },
        "api.kucoin.com": {
            "data": [
                {
                    "symbol": "BTC-USDT",
                    "baseCurrency": "BTC",
                    "quoteCurrency": "USDT",
                    "enableTrading": True,
                }
            ]
        },
        "api.bitget.com": {
            "data": [
                {
                    "symbol": "BTCUSDT",
                    "baseCoin": "BTC",
                    "quoteCoin": "USDT",
                    "status": "online",
                }
            ]
        },
        "api.gateio.ws": [
            {
                "id": "BTC_USDT",
                "base": "BTC",
                "quote": "USDT",
                "trade_status": "tradable",
            }
        ],
        "api.huobi.pro": {
            "data": [
                {"sc": "btcusdt", "bcdn": "BTC", "qcdn": "USDT", "state": "online"}
            ]
        },
        "api.mexc.com": {
            "symbols": [
                {
                    "symbol": "BTCUSDT",
                    "baseAsset": "BTC",
                    "quoteAsset": "USDT",
                    "status": "ENABLED",
                }
            ]
        },
    }

    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def get(self, url, params=None):
        self.calls.append((url, params or {}, self.kwargs))
        if "api.exchange.coinbase.com/products/" in url:
            symbol = url.rsplit("/", 1)[-1]
            return _FakeResponse(
                {
                    "id": symbol,
                    "base_currency": "BTC",
                    "quote_currency": symbol.rsplit("-", 1)[-1],
                    "status": "online",
                    "trading_disabled": False,
                }
            )
        if "api.gateio.ws/api/v4/spot/currency_pairs/" in url:
            symbol = url.rsplit("/", 1)[-1]
            return _FakeResponse(
                {
                    "id": symbol,
                    "base": "BTC",
                    "quote": symbol.rsplit("_", 1)[-1],
                    "trade_status": "tradable",
                }
            )
        if "api.huobi.pro/market/detail/merged" in url:
            return _FakeResponse({"status": "ok", "tick": {"close": 1}})
        for host, payload in self.PAYLOADS.items():
            if host in url:
                return _FakeResponse(payload)
        raise AssertionError(f"unexpected url: {url}")


class _FakeHttpx:
    Client = _FakeClient


def test_exchange_registry_marks_all_top_ten_as_native_searchable_spot() -> None:
    native = list_exchanges(native_only=True)

    assert [exchange.id for exchange in native] == [
        "binance",
        "bybit",
        "okx",
        "coinbase",
        "kraken",
        "kucoin",
        "bitget",
        "gateio",
        "htx",
        "mexc",
    ]
    assert all("spot" in exchange.native_markets for exchange in native)


def test_search_symbols_supports_all_native_spot_exchanges_without_network(
    monkeypatch,
) -> None:
    import sys

    monkeypatch.setitem(sys.modules, "httpx", _FakeHttpx)
    _FakeClient.calls = []

    expected_symbols = {
        "okx": "BTC-USDT",
        "coinbase": "BTC-USDT",
        "kraken": "XBTUSD",
        "kucoin": "BTC-USDT",
        "bitget": "BTCUSDT",
        "gateio": "BTC_USDT",
        "htx": "BTCUSDT",
        "mexc": "BTCUSDT",
    }
    for exchange, symbol in expected_symbols.items():
        out = search_symbols(exchange, "spot", query="btc", limit=1)
        assert [item.symbol for item in out] == [symbol]
        assert out[0].exchange == exchange
        assert out[0].market == "spot"

    called_hosts = "\n".join(call[0] for call in _FakeClient.calls)
    for host in _FakeClient.PAYLOADS:
        assert host in called_hosts


def test_market_data_service_routes_native_spot_exchange_to_public_spot_source(
    monkeypatch, tmp_path
) -> None:
    import marketdata_provider.service as service_mod

    calls: list[dict[str, object]] = []

    def fake_public_spot_get_bars_sync(**kwargs):
        calls.append(kwargs)
        return [Bar(0, 1.0, 2.0, 0.5, 1.5, 10.0, 60_000)]

    monkeypatch.setattr(
        service_mod,
        "public_spot_get_bars_sync",
        fake_public_spot_get_bars_sync,
        raising=False,
    )
    service = MarketDataService(
        MarketDataConfig(
            storage=StorageConfig(cache_dir=tmp_path),
            history=MarketDataConfig().history,
        )
    )
    query = BarQuery(
        instrument=InstrumentKey("okx", "spot", "BTC-USDT"),
        timeframe=parse_timeframe("1m"),
        start_ms=0,
        end_ms=60_000,
    )

    series = service.fetch_bars(query)

    assert [bar.time for bar in series.bars] == [0]
    assert calls == [
        {
            "exchange": "okx",
            "symbol": "BTC-USDT",
            "timeframe": "1m",
            "start": 0,
            "end": 60_000,
            "user_agent": MarketDataConfig().binance.user_agent,
            "include_open_candle": False,
        }
    ]
