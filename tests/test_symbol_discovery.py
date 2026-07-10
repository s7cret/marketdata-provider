from __future__ import annotations

from marketdata_provider.symbols import (
    SymbolInfo,
    filter_symbol_infos,
    normalize_binance_exchange_info_symbols,
    normalize_bybit_instruments_info_symbols,
)


def test_filter_symbol_infos_defaults_to_stable_quotes():
    symbols = [
        SymbolInfo(
            exchange="binance",
            market="spot",
            symbol="BTCUSDT",
            base_asset="BTC",
            quote_asset="USDT",
        ),
        SymbolInfo(
            exchange="binance",
            market="spot",
            symbol="ETHBTC",
            base_asset="ETH",
            quote_asset="BTC",
        ),
        SymbolInfo(
            exchange="binance",
            market="spot",
            symbol="SOLUSDC",
            base_asset="SOL",
            quote_asset="USDC",
        ),
        SymbolInfo(
            exchange="binance",
            market="coinm",
            symbol="BTCUSD_PERP",
            base_asset="BTC",
            quote_asset="USD",
        ),
    ]

    stable = filter_symbol_infos(symbols)
    assert [item.symbol for item in stable] == ["BTCUSDT", "SOLUSDC", "BTCUSD_PERP"]
    assert [
        item.symbol for item in filter_symbol_infos(symbols, stable_quotes_only=False)
    ] == [
        "BTCUSDT",
        "ETHBTC",
        "SOLUSDC",
        "BTCUSD_PERP",
    ]
    assert [item.symbol for item in filter_symbol_infos(symbols, query="sol")] == [
        "SOLUSDC"
    ]


def test_normalize_binance_exchange_info_symbols_respects_default_stable_filter():
    payload = {
        "symbols": [
            {
                "symbol": "BTCUSDT",
                "status": "TRADING",
                "baseAsset": "BTC",
                "quoteAsset": "USDT",
            },
            {
                "symbol": "ETHBTC",
                "status": "TRADING",
                "baseAsset": "ETH",
                "quoteAsset": "BTC",
            },
            {
                "symbol": "SOLUSDC",
                "status": "TRADING",
                "baseAsset": "SOL",
                "quoteAsset": "USDC",
            },
            {
                "symbol": "DELISTEDUSDT",
                "status": "BREAK",
                "baseAsset": "DELISTED",
                "quoteAsset": "USDT",
            },
            {
                "symbol": "BTCUSD_PERP",
                "contractStatus": "TRADING",
                "baseAsset": "BTC",
                "quoteAsset": "USD",
            },
        ]
    }

    out = normalize_binance_exchange_info_symbols(payload, market="spot")
    assert [(item.symbol, item.quote_asset) for item in out] == [
        ("BTCUSDT", "USDT"),
        ("SOLUSDC", "USDC"),
        ("BTCUSD_PERP", "USD"),
    ]


def test_normalize_bybit_instruments_info_symbols_respects_default_stable_filter():
    payload = {
        "result": {
            "list": [
                {
                    "symbol": "BTCUSDT",
                    "status": "Trading",
                    "baseCoin": "BTC",
                    "quoteCoin": "USDT",
                },
                {
                    "symbol": "ETHBTC",
                    "status": "Trading",
                    "baseCoin": "ETH",
                    "quoteCoin": "BTC",
                },
                {
                    "symbol": "BTCUSD",
                    "status": "Trading",
                    "baseCoin": "BTC",
                    "quoteCoin": "USD",
                },
                {
                    "symbol": "OLDUSDT",
                    "status": "Settled",
                    "baseCoin": "OLD",
                    "quoteCoin": "USDT",
                },
            ]
        }
    }

    out = normalize_bybit_instruments_info_symbols(payload, market="inverse")
    assert [(item.symbol, item.market, item.quote_asset) for item in out] == [
        ("BTCUSDT", "inverse", "USDT"),
        ("BTCUSD", "inverse", "USD"),
    ]


class _FakeResponse:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code
        self.headers = {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError("bad status")

    def json(self):
        return self._payload


class _FakeClient:
    calls = []
    payloads = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def get(self, url, params=None):
        self.calls.append((url, params or {}, self.kwargs))
        return _FakeResponse(self.payloads.pop(0))


class _FakeHttpx:
    Client = _FakeClient


def test_symbol_edge_cases_and_search_symbols_without_network(monkeypatch):
    import pytest

    from marketdata_provider.config import MarketDataConfig
    from marketdata_provider.errors import (
        MDInvalidExchangeResponse,
        MDSymbolAmbiguous,
        MDSymbolUnsupported,
        MDUnsupportedFeature,
    )
    from marketdata_provider.exchanges.binance.provider import _base_url
    from marketdata_provider.exchanges.bybit.provider import _category
    from marketdata_provider.symbols import (
        normalize_symbol,
        quote_asset,
        search_symbols,
    )

    info = SymbolInfo(
        "binance", "spot", "BTCUSDT", "BTC", "USDT", contract_type="PERPETUAL"
    )
    assert info.to_dict()["contract_type"] == "PERPETUAL"
    assert quote_asset("BINANCE:BTCUSDT.P") == "USDT"
    with pytest.raises(MDSymbolAmbiguous):
        normalize_symbol("BTCUSDT")
    with pytest.raises(MDSymbolUnsupported):
        normalize_symbol("BTCUSDT", exchange="BINANCE", market="options")
    with pytest.raises(MDSymbolUnsupported):
        _base_url(MarketDataConfig().binance, "options")
    with pytest.raises(MDSymbolUnsupported):
        _category("options")

    rows = [
        SymbolInfo("binance", "spot", "BTCUSDT", "BTC", "USDT"),
        SymbolInfo("binance", "spot", "ETHUSDT", "ETH", "USDT"),
    ]
    assert [item.symbol for item in filter_symbol_infos(rows, limit=1)] == ["BTCUSDT"]
    with pytest.raises(MDInvalidExchangeResponse):
        normalize_binance_exchange_info_symbols({"symbols": "bad"}, market="spot")
    assert (
        normalize_binance_exchange_info_symbols(
            {"symbols": [object(), {"symbol": "", "status": "TRADING"}]}, market="spot"
        )
        == []
    )
    with pytest.raises(MDInvalidExchangeResponse):
        normalize_bybit_instruments_info_symbols([], market="spot")
    with pytest.raises(MDInvalidExchangeResponse):
        normalize_bybit_instruments_info_symbols({"result": {}}, market="spot")
    assert (
        normalize_bybit_instruments_info_symbols(
            {"result": {"list": [object(), {"symbol": "", "status": "Trading"}]}},
            market="spot",
        )
        == []
    )

    _FakeClient.calls = []
    _FakeClient.payloads = [
        {
            "symbols": [
                {
                    "symbol": "BTCUSDT",
                    "status": "TRADING",
                    "baseAsset": "BTC",
                    "quoteAsset": "USDT",
                }
            ]
        },
        {
            "symbols": [
                {
                    "symbol": "ETHUSDT",
                    "status": "TRADING",
                    "baseAsset": "ETH",
                    "quoteAsset": "USDT",
                }
            ]
        },
        {
            "symbols": [
                {
                    "symbol": "BTCUSD_PERP",
                    "contractStatus": "TRADING",
                    "baseAsset": "BTC",
                    "quoteAsset": "USD",
                }
            ]
        },
        {
            "result": {
                "list": [
                    {
                        "symbol": "BTCUSDT",
                        "status": "Trading",
                        "baseCoin": "BTC",
                        "quoteCoin": "USDT",
                    }
                ]
            }
        },
        {
            "data": [
                {
                    "instId": "BTC-USDT",
                    "state": "live",
                    "baseCcy": "BTC",
                    "quoteCcy": "USDT",
                }
            ]
        },
        {
            "data": [
                {
                    "instId": "BTC-USDT-SWAP",
                    "state": "live",
                    "baseCcy": "BTC",
                    "quoteCcy": "USDT",
                    "ctType": "linear",
                }
            ]
        },
    ]
    monkeypatch.setitem(__import__("sys").modules, "httpx", _FakeHttpx)

    assert search_symbols("binance", "spot")[0].symbol == "BTCUSDT"
    assert search_symbols("binance", "futures")[0].symbol == "ETHUSDT"
    assert search_symbols("binance", "delivery")[0].symbol == "BTCUSD_PERP"
    assert search_symbols("bybit", "linear")[0].symbol == "BTCUSDT"
    assert _FakeClient.calls[0][0].endswith("/api/v3/exchangeInfo")
    assert _FakeClient.calls[1][0].endswith("/fapi/v1/exchangeInfo")
    assert _FakeClient.calls[2][0].endswith("/dapi/v1/exchangeInfo")
    assert _FakeClient.calls[3][1] == {"category": "linear"}
    assert search_symbols("okx", "spot")[0].symbol == "BTC-USDT"
    assert _FakeClient.calls[4][0].endswith("/api/v5/public/instruments")
    assert _FakeClient.calls[4][1] == {"instType": "SPOT"}
    assert search_symbols("okx", "futures")[0].symbol == "BTC-USDT-SWAP"
    assert _FakeClient.calls[5][0].endswith("/api/v5/public/instruments")
    assert _FakeClient.calls[5][1] == {"instType": "SWAP"}

    import marketdata_provider.symbols as symbols_mod

    monkeypatch.setattr(
        symbols_mod, "_provider_market", lambda exchange, market: "options"
    )
    with pytest.raises(MDUnsupportedFeature):
        search_symbols("binance", "spot")
