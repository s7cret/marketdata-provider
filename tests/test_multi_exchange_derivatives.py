from __future__ import annotations

from typing import Any

import pytest

from marketdata_provider.config import MarketDataConfig, StorageConfig
from marketdata_provider.contracts import BarQuery, InstrumentKey, parse_timeframe
from marketdata_provider.core.bar import Bar
from marketdata_provider.exchanges.registry import get_exchange
from marketdata_provider.service import MarketDataService
from marketdata_provider.symbols import search_symbols

EXPECTED_NATIVE_MARKETS = {
    "binance": ("spot", "margin", "usdm", "coinm"),
    "bybit": ("spot", "linear", "inverse"),
    "okx": ("spot", "margin", "linear", "inverse", "delivery_futures"),
    "coinbase": ("spot",),
    "kraken": ("spot", "margin", "linear", "inverse", "delivery_futures"),
    "kucoin": ("spot", "margin", "linear", "inverse", "delivery_futures"),
    "bitget": ("spot", "margin", "linear", "inverse", "delivery_futures"),
    "gateio": ("spot", "margin", "linear", "inverse", "delivery_futures"),
    "htx": ("spot", "margin", "linear", "inverse", "delivery_futures"),
    "mexc": ("spot", "linear", "inverse"),
}


def test_registry_native_markets_cover_all_adapter_backed_public_markets() -> None:
    for exchange, markets in EXPECTED_NATIVE_MARKETS.items():
        assert get_exchange(exchange).native_markets == markets


def test_search_symbols_supports_native_derivatives_without_network(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import marketdata_provider.symbols as symbols_mod

    calls: list[tuple[str, dict[str, object]]] = []

    def fake_http_get_json(
        url: str,
        *,
        params: dict[str, object] | None = None,
        timeout: float,
        user_agent: str,
        httpx: object,
    ) -> Any:
        del timeout, user_agent, httpx
        params = params or {}
        calls.append((url, params))
        if "okx.com" in url:
            inst_type = params.get("instType")
            if inst_type == "MARGIN":
                return {
                    "data": [
                        {
                            "instId": "BTC-USDT",
                            "baseCcy": "BTC",
                            "quoteCcy": "USDT",
                            "state": "live",
                        }
                    ]
                }
            if inst_type == "SWAP":
                return {
                    "data": [
                        {
                            "instId": "BTC-USDT-SWAP",
                            "instFamily": "BTC-USDT",
                            "baseCcy": "",
                            "quoteCcy": "",
                            "settleCcy": "USDT",
                            "ctType": "linear",
                            "state": "live",
                        },
                        {
                            "instId": "BTC-USD-SWAP",
                            "instFamily": "BTC-USD",
                            "baseCcy": "",
                            "quoteCcy": "",
                            "settleCcy": "BTC",
                            "ctType": "inverse",
                            "state": "live",
                        },
                    ]
                }
            if inst_type == "FUTURES":
                return {
                    "data": [
                        {
                            "instId": "BTC-USDT-260626",
                            "instFamily": "BTC-USDT",
                            "baseCcy": "",
                            "quoteCcy": "",
                            "settleCcy": "USDT",
                            "ctType": "linear",
                            "state": "live",
                        }
                    ]
                }
        if "futures.kraken.com" in url:
            return {
                "result": "success",
                "instruments": [
                    {
                        "symbol": "PF_XBTUSD",
                        "type": "flexible_futures",
                        "tradeable": True,
                        "base": "BTC",
                        "quote": "USD",
                    },
                    {
                        "symbol": "PI_XBTUSD",
                        "type": "futures_inverse",
                        "tradeable": True,
                        "base": "BTC",
                        "quote": "USD",
                    },
                    {
                        "symbol": "FF_XBTUSD_260626",
                        "type": "flexible_futures",
                        "tradeable": True,
                        "base": "BTC",
                        "quote": "USD",
                    },
                ],
            }
        if "api-futures.kucoin.com" in url:
            return {
                "data": [
                    {
                        "symbol": "XBTUSDTM",
                        "baseCurrency": "XBT",
                        "quoteCurrency": "USDT",
                        "settleCurrency": "USDT",
                        "isInverse": False,
                        "status": "Open",
                        "expireDate": None,
                    },
                    {
                        "symbol": "XBTUSDM",
                        "baseCurrency": "XBT",
                        "quoteCurrency": "USD",
                        "settleCurrency": "XBT",
                        "isInverse": True,
                        "status": "Open",
                        "expireDate": None,
                    },
                    {
                        "symbol": "XBTMM26",
                        "baseCurrency": "XBT",
                        "quoteCurrency": "USD",
                        "settleCurrency": "XBT",
                        "isInverse": True,
                        "status": "Open",
                        "expireDate": 1782432000000,
                    },
                ]
            }
        if "api.kucoin.com/api/v3/margin/symbols" in url:
            return {
                "data": [
                    {
                        "symbol": "BTC-USDT",
                        "baseCurrency": "BTC",
                        "quoteCurrency": "USDT",
                        "enableTrading": True,
                    }
                ]
            }
        if "api.bitget.com/api/v2/margin/currencies" in url:
            return {
                "data": [
                    {
                        "symbol": "BTCUSDT",
                        "baseCoin": "BTC",
                        "quoteCoin": "USDT",
                        "status": "1",
                    }
                ]
            }
        if "api.bitget.com/api/v2/mix/market/contracts" in url:
            product = params.get("productType")
            if product == "USDT-FUTURES":
                return {
                    "data": [
                        {
                            "symbol": "BTCUSDT",
                            "baseCoin": "BTC",
                            "quoteCoin": "USDT",
                            "symbolType": "perpetual",
                            "status": "normal",
                        }
                    ]
                }
            if product == "COIN-FUTURES":
                return {
                    "data": [
                        {
                            "symbol": "BTCUSD",
                            "baseCoin": "BTC",
                            "quoteCoin": "USD",
                            "symbolType": "perpetual",
                            "status": "normal",
                        }
                    ]
                }
        if "gateio.ws/api/v4/margin/currency_pairs" in url:
            return [
                {
                    "id": "BTC_USDT",
                    "base": "BTC",
                    "quote": "USDT",
                    "trade_status": "tradable",
                }
            ]
        if "gateio.ws/api/v4/futures/usdt/contracts" in url:
            return [{"name": "BTC_USDT", "type": "direct", "in_delisting": False}]
        if "gateio.ws/api/v4/futures/btc/contracts" in url:
            return [{"name": "BTC_USD", "type": "inverse", "in_delisting": False}]
        if "gateio.ws/api/v4/delivery/usdt/contracts" in url:
            return [
                {"name": "BTC_USDT_20260925", "type": "direct", "in_delisting": False}
            ]
        if "linear-swap-api" in url:
            business_type = params.get("business_type")
            if business_type == "swap":
                return {
                    "status": "ok",
                    "data": [
                        {
                            "contract_code": "BTC-USDT",
                            "symbol": "BTC",
                            "trade_partition": "USDT",
                            "contract_status": 1,
                        }
                    ],
                }
            return {
                "status": "ok",
                "data": [
                    {
                        "contract_code": "BTC-USDT-260626",
                        "symbol": "BTC",
                        "trade_partition": "USDT",
                        "contract_status": 1,
                    }
                ],
            }
        if "swap-api" in url:
            return {
                "status": "ok",
                "data": [
                    {"contract_code": "BTC-USD", "symbol": "BTC", "contract_status": 1}
                ],
            }
        if "contract.mexc.com/api/v1/contract/detail" in url:
            return {
                "success": True,
                "data": [
                    {
                        "symbol": "BTC_USDT",
                        "baseCoin": "BTC",
                        "quoteCoin": "USDT",
                        "settleCoin": "USDT",
                        "state": 0,
                        "apiAllowed": True,
                        "automaticDelivery": 0,
                    },
                    {
                        "symbol": "BTC_USD",
                        "baseCoin": "BTC",
                        "quoteCoin": "USD",
                        "settleCoin": "BTC",
                        "state": 0,
                        "apiAllowed": True,
                        "automaticDelivery": 0,
                    },
                ],
            }
        raise AssertionError((url, params))

    monkeypatch.setattr(symbols_mod, "_http_get_json", fake_http_get_json)

    expected = {
        ("okx", "margin"): "BTC-USDT",
        ("okx", "futures"): "BTC-USDT-SWAP",
        ("okx", "delivery"): "BTC-USD-SWAP",
        ("kraken", "futures"): "PF_XBTUSD",
        ("kraken", "delivery"): "PI_XBTUSD",
        ("kucoin", "margin"): "BTC-USDT",
        ("kucoin", "futures"): "XBTUSDTM",
        ("kucoin", "delivery"): "XBTUSDM",
        ("bitget", "margin"): "BTCUSDT",
        ("bitget", "futures"): "BTCUSDT",
        ("bitget", "delivery"): "BTCUSD",
        ("gateio", "margin"): "BTC_USDT",
        ("gateio", "futures"): "BTC_USDT",
        ("gateio", "delivery"): "BTC_USD",
        ("htx", "futures"): "BTC-USDT",
        ("htx", "delivery"): "BTC-USD",
        ("mexc", "futures"): "BTC_USDT",
        ("mexc", "delivery"): "BTC_USD",
    }
    for (exchange, market), symbol in expected.items():
        out = search_symbols(exchange, market, query="btc", limit=1)
        assert [item.symbol for item in out] == [symbol]
        assert out[0].exchange == exchange

    assert any(params.get("instType") == "SWAP" for _, params in calls)
    assert any(params.get("productType") == "COIN-FUTURES" for _, params in calls)


def test_public_market_get_bars_maps_derivative_endpoints(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from marketdata_provider.exchanges import public_spot as ps

    calls: list[tuple[str, dict[str, object]]] = []

    def fake_get_json(
        url: str,
        *,
        params: dict[str, object],
        timeout: float,
        user_agent: str,
        max_retries: int,
    ) -> Any:
        del timeout, user_agent, max_retries
        calls.append((url, params))
        if "okx.com" in url:
            return {"data": [["0", "1", "2", "0.5", "1.5", "10"]]}
        if "futures.kraken.com" in url:
            return {
                "candles": [
                    {
                        "time": 0,
                        "open": "1",
                        "high": "2",
                        "low": "0.5",
                        "close": "1.5",
                        "volume": "10",
                    }
                ]
            }
        if "api-futures.kucoin.com" in url:
            return {"data": [[0, "1", "2", "0.5", "1.5", "10", "100"]]}
        if "api.bitget.com/api/v2/mix" in url:
            return {"data": [["0", "1", "2", "0.5", "1.5", "10", "100"]]}
        if "gateio.ws/api/v4/futures" in url or "gateio.ws/api/v4/delivery" in url:
            return [{"t": 0, "o": "1", "h": "2", "l": "0.5", "c": "1.5", "v": "10"}]
        if "hbdm.com" in url:
            return {
                "status": "ok",
                "data": [
                    {
                        "id": 0,
                        "open": 1,
                        "high": 2,
                        "low": 0.5,
                        "close": 1.5,
                        "amount": 10,
                    }
                ],
            }
        if "contract.mexc.com" in url:
            return {
                "success": True,
                "data": {
                    "time": [0],
                    "open": [1],
                    "high": [2],
                    "low": [0.5],
                    "close": [1.5],
                    "vol": [10],
                },
            }
        raise AssertionError((url, params))

    monkeypatch.setattr(ps, "_http_get_json", fake_get_json)

    samples = [
        ("okx", "futures", "BTC-USDT-SWAP"),
        ("kraken", "delivery", "PI_XBTUSD"),
        ("kucoin", "futures", "XBTUSDTM"),
        ("bitget", "delivery", "BTCUSD"),
        ("gateio", "futures", "BTC_USDT"),
        ("htx", "delivery", "BTC-USD"),
        ("mexc", "futures", "BTC_USDT"),
    ]
    for exchange, market, symbol in samples:
        bars = ps.public_market_get_bars_sync(
            exchange=exchange,
            market=market,
            symbol=symbol,
            timeframe="1m",
            start=0,
            end=60_000,
            user_agent="ua",
        )
        assert [(bar.time, bar.close, bar.volume) for bar in bars] == [(0, 1.5, 10.0)]

    called = "\n".join(f"{url} {params}" for url, params in calls)
    assert "/api/v5/market/candles" in called
    assert "api/charts/v1/trade/PI_XBTUSD/1m" in called
    assert "granularity': 1, 'from': 0, 'to': 60000" in called
    assert "interval': 'Min1'" in called
    assert "productType': 'COIN-FUTURES" in called
    assert "/linear-swap-ex/" not in called  # BTC-USD delivery is inverse coin swap.


def test_market_data_service_routes_public_native_markets(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    import marketdata_provider.service as service_mod

    calls: list[dict[str, object]] = []

    def fake_public_market_get_bars_sync(**kwargs: object) -> list[Bar]:
        calls.append(kwargs)
        return [Bar(0, 1.0, 2.0, 0.5, 1.5, 10.0, 60_000)]

    monkeypatch.setattr(
        service_mod,
        "public_market_get_bars_sync",
        fake_public_market_get_bars_sync,
        raising=False,
    )
    service = MarketDataService(
        MarketDataConfig(storage=StorageConfig(cache_dir=tmp_path))
    )
    service.store = service_mod.CandleStore(tmp_path)

    query = BarQuery(
        InstrumentKey("okx", "delivery", "BTC-USD-SWAP"),
        parse_timeframe("1m"),
        0,
        60_000,
    )
    series = service.fetch_bars(query)

    assert [bar.time for bar in series.bars] == [0]
    assert calls == [
        {
            "exchange": "okx",
            "market": "delivery",
            "symbol": "BTC-USD-SWAP",
            "timeframe": "1m",
            "start": 0,
            "end": 60_000,
            "user_agent": service.config.binance.user_agent,
            "include_open_candle": False,
        }
    ]
