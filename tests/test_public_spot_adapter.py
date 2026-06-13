from __future__ import annotations

from typing import Any

import httpx
import pytest

from marketdata_provider.errors import (
    MDInvalidExchangeResponse,
    MDNetworkUnavailable,
    MDSymbolUnsupported,
    MDUnsupportedFeature,
)
from marketdata_provider.exchanges import public_spot as ps
from marketdata_provider.symbols import (
    _candidate_spot_symbol,
    _normalize_query_first_payload,
    _query_base_asset,
    _query_first_url,
    _symbol_tuple,
    is_stable_quoted,
    normalize_public_spot_symbols,
    normalize_symbol,
    search_symbols,
)


class FakeResponse:
    def __init__(self, status_code: int, payload: Any, *, raise_http: bool = False) -> None:
        self.status_code = status_code
        self._payload = payload
        self._raise_http = raise_http

    def json(self) -> Any:
        return self._payload

    def raise_for_status(self) -> None:
        if self._raise_http or self.status_code >= 400:
            raise httpx.HTTPStatusError(
                "bad",
                request=httpx.Request("GET", "https://example.invalid"),
                response=httpx.Response(self.status_code),
            )


class FakeClient:
    responses: list[FakeResponse] = []
    calls: list[tuple[str, dict[str, object]]] = []
    raise_connect = False

    def __init__(self, **kwargs: object) -> None:
        assert kwargs["trust_env"] is False

    def __enter__(self) -> "FakeClient":
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def get(self, url: str, params: dict[str, object]) -> FakeResponse:
        self.calls.append((url, params))
        if self.raise_connect:
            raise httpx.ConnectError("no route")
        return self.responses.pop(0)


def _patch_client(monkeypatch: pytest.MonkeyPatch, responses: list[FakeResponse]) -> None:
    FakeClient.responses = responses
    FakeClient.calls = []
    FakeClient.raise_connect = False
    monkeypatch.setattr(ps.httpx, "Client", FakeClient)
    monkeypatch.setattr(ps.time, "sleep", lambda *_args: None)


def test_public_spot_get_bars_success_for_all_exchanges(monkeypatch: pytest.MonkeyPatch) -> None:
    payloads = {
        "okx": {"data": [["0", "1", "2", "0.5", "1.5", "10"]]},
        "coinbase": [[0, 0.5, 2, 1, 1.5, 10]],
        "kraken": {"result": {"XXBTZUSD": [[0, "1", "2", "0.5", "1.5", "1.2", "10"]], "last": 1}},
        "kucoin": {"data": [["0", "1", "1.5", "2", "0.5", "10"]]},
        "bitget": {"data": [["0", "1", "2", "0.5", "1.5", "10"]]},
        "gateio": [["0", "10", "1.5", "2", "0.5", "1"]],
        "htx": {"data": [{"id": 0, "open": 1, "high": 2, "low": 0.5, "close": 1.5, "amount": 10}]},
        "mexc": [[0, "1", "2", "0.5", "1.5", "10"]],
    }
    symbols = {
        "okx": "BTC-USDT",
        "coinbase": "BTC-USD",
        "kraken": "XBTUSD",
        "kucoin": "BTC-USDT",
        "bitget": "BTCUSDT",
        "gateio": "BTC_USDT",
        "htx": "btcusdt",
        "mexc": "BTCUSDT",
    }
    _patch_client(monkeypatch, [FakeResponse(200, payload) for payload in payloads.values()])

    for exchange, symbol in symbols.items():
        bars = ps.public_spot_get_bars_sync(
            exchange=exchange,
            symbol=symbol,
            timeframe="1m",
            start=0,
            end=60_000,
            user_agent="ua",
        )
        assert [(bar.time, bar.close, bar.volume) for bar in bars] == [(0, 1.5, 10.0)]

    assert len(FakeClient.calls) == len(symbols)
    assert FakeClient.calls[0][1]["instId"] == "BTC-USDT"
    assert FakeClient.calls[-1][1]["symbol"] == "BTCUSDT"


def test_public_spot_http_retry_and_error_paths(monkeypatch: pytest.MonkeyPatch) -> None:
    ok_payload = {"data": [["0", "1", "2", "0.5", "1.5", "10"]]}
    _patch_client(monkeypatch, [FakeResponse(429, {}), FakeResponse(200, ok_payload)])
    assert ps.public_spot_get_bars_sync(
        exchange="okx", symbol="BTC-USDT", timeframe="1m", start=0, end=60_000, user_agent="ua"
    )[0].close == 1.5

    _patch_client(monkeypatch, [FakeResponse(500, {}), FakeResponse(200, ok_payload)])
    assert ps.public_spot_get_bars_sync(
        exchange="okx", symbol="BTC-USDT", timeframe="1m", start=0, end=60_000, user_agent="ua"
    )[0].time == 0

    _patch_client(monkeypatch, [FakeResponse(429, {}), FakeResponse(429, {})])
    with pytest.raises(MDNetworkUnavailable, match="rate limit"):
        ps._http_get_json("https://x", params={}, timeout=1, user_agent="ua", max_retries=1)

    _patch_client(monkeypatch, [FakeResponse(400, {}, raise_http=True)])
    with pytest.raises(MDNetworkUnavailable, match="HTTP request failed"):
        ps._http_get_json("https://x", params={}, timeout=1, user_agent="ua", max_retries=0)

    _patch_client(monkeypatch, [])
    FakeClient.raise_connect = True
    with pytest.raises(MDNetworkUnavailable, match="HTTP request failed"):
        ps._http_get_json("https://x", params={}, timeout=1, user_agent="ua", max_retries=1)

    _patch_client(monkeypatch, [])
    FakeClient.raise_connect = True
    with pytest.raises(MDNetworkUnavailable, match="HTTP request failed"):
        ps._http_get_json("https://x", params={}, timeout=1, user_agent="ua", max_retries=0)

    _patch_client(monkeypatch, [])
    with pytest.raises(MDNetworkUnavailable, match="HTTP request failed"):
        ps._http_get_json("https://x", params={}, timeout=1, user_agent="ua", max_retries=-1)


def test_public_spot_timeframe_and_payload_error_paths() -> None:
    for exchange in ["okx", "kraken", "kucoin", "bitget", "gateio", "htx", "mexc"]:
        with pytest.raises(MDSymbolUnsupported):
            ps._spot_request(exchange, "BTCUSDT", "2m", 0, 60_000)
    with pytest.raises(MDSymbolUnsupported):
        ps._spot_request("coinbase", "BTC-USD", "3m", 0, 60_000)
    with pytest.raises(MDSymbolUnsupported):
        ps._spot_request("bitstamp", "BTCUSD", "1m", 0, 60_000)
    with pytest.raises(MDSymbolUnsupported):
        ps.public_spot_get_bars_sync(
            exchange="bitstamp", symbol="BTCUSD", timeframe="1m", start=0, end=60_000, user_agent="ua"
        )
    with pytest.raises(MDInvalidExchangeResponse, match="missing rows"):
        ps._normalize_spot_klines("okx", {}, symbol="BTC-USDT", timeframe="1m")
    with pytest.raises(MDInvalidExchangeResponse, match="row is invalid"):
        ps._normalize_spot_klines("mexc", [["bad"]], symbol="BTCUSDT", timeframe="1m")
    assert ps._row_to_bar("unknown", [], timeframe="1m") is None


def test_public_symbol_normalizers_cover_all_disabled_and_fallback_branches(monkeypatch: pytest.MonkeyPatch) -> None:
    payloads = {
        "okx": {"data": [object(), {"instId": "BTC-USDT", "state": "suspend", "baseCcy": "BTC", "quoteCcy": "USDT"}, {"instId": "ETH-USDT", "state": "live", "baseCcy": "ETH", "quoteCcy": "USDT"}]},
        "coinbase": [{"id": "BTC-USD", "base_currency": "BTC", "quote_currency": "USD", "trading_disabled": True}, {"id": "ETH-USD", "base_currency": "ETH", "quote_currency": "USD"}],
        "kraken": {"result": {"bad": {"altname": "XBTUSD", "status": "disabled"}, "good": {"altname": "ETHUSD", "wsname": "ETH/USD"}}},
        "kucoin": {"data": [{"symbol": "BTC-USDT", "baseCurrency": "BTC", "quoteCurrency": "USDT", "enableTrading": False}, {"symbol": "ETH-USDT", "baseCurrency": "ETH", "quoteCurrency": "USDT"}]},
        "bitget": {"data": [{"symbol": "BTCUSDT", "baseCoin": "BTC", "quoteCoin": "USDT", "status": "offline"}, {"symbol": "ETHUSDT", "baseCoin": "ETH", "quoteCoin": "USDT"}]},
        "gateio": [{"id": "BTC_USDT", "base": "BTC", "quote": "USDT", "trade_status": "untradable"}, {"id": "ETH_USDT", "base": "ETH", "quote": "USDT", "trade_status": "tradable"}],
        "htx": {"data": [{"sc": "btcusdt", "bcdn": "btc", "qcdn": "usdt", "state": "offline"}, {"sc": "ethusdt", "bcdn": "eth", "qcdn": "usdt", "state": "online"}]},
        "mexc": {"symbols": [{"symbol": "BTCUSDT", "baseAsset": "BTC", "quoteAsset": "USDT", "status": "BREAK"}, {"symbol": "ETHUSDT", "baseAsset": "ETH", "quoteAsset": "USDT"}]},
        "other": [{"symbol": "BTCUSDT", "base": "", "quote": "USDT"}, {"symbol": "ETHUSDT", "base": "ETH", "quote": ""}, {"symbol": "", "base": "BTC", "quote": "USDT"}],
    }
    expected = {
        "okx": "ETH-USDT",
        "coinbase": "ETH-USD",
        "kraken": "ETHUSD",
        "kucoin": "ETH-USDT",
        "bitget": "ETHUSDT",
        "gateio": "ETH_USDT",
        "htx": "ETHUSDT",
        "mexc": "ETHUSDT",
    }
    for exchange, symbol in expected.items():
        out = normalize_public_spot_symbols(
            exchange, payloads[exchange], stable_quotes_only=False
        )
        assert [item.symbol for item in out] == [symbol]
    assert normalize_public_spot_symbols("other", payloads["other"], stable_quotes_only=False) == []

    import marketdata_provider.symbols as symbols_mod

    monkeypatch.setattr(symbols_mod, "_provider_market", lambda exchange, market: "options")
    with pytest.raises(MDUnsupportedFeature, match="unsupported"):
        search_symbols("okx", "spot")
    monkeypatch.setattr(symbols_mod, "_provider_market", lambda exchange, market: "spot")
    with pytest.raises(MDUnsupportedFeature, match="unsupported for exchange"):
        search_symbols("bitstamp", "spot")


def test_symbol_helpers_cover_new_exchange_branches() -> None:
    with pytest.raises(MDSymbolUnsupported, match="Empty symbol"):
        normalize_symbol("", exchange="binance")
    with pytest.raises(MDSymbolUnsupported, match="Unsupported exchange"):
        normalize_symbol("BITSTAMP:BTCUSDT")
    with pytest.raises(MDSymbolUnsupported, match="Unsupported exchange"):
        normalize_symbol("BTCUSDT", exchange="bitstamp")

    bybit_perp = normalize_symbol("BYBIT:BTCUSDT.P")
    assert bybit_perp.market == "linear"
    assert bybit_perp.tv_symbol == "BYBIT:BTCUSDT.P"
    assert bybit_perp.is_perpetual
    assert normalize_symbol("OKX:BTC-USDT").market == "spot"
    assert is_stable_quoted("BTCUSDT")

    assert ps._minutes("1h") == 60
    assert ps._requested_limit("1m", 0, 30 * 60_000, max_limit=2000) == 30
    assert ps._requested_limit("1m", None, 30 * 60_000, max_limit=2000) == 2000
    _url, htx_params = ps._spot_request("htx", "BTCUSDT", "1m", 0, 30 * 60_000)
    assert htx_params["size"] == 30
    assert _symbol_tuple("ETHUSDT", "ETH", "") == ("ETHUSDT", "ETH", "USDT")
    assert _symbol_tuple("BTCUSDT", "", "USDT") == ("BTCUSDT", "BTC", "USDT")
    assert _symbol_tuple("", "BTC", "USDT") is None


def test_query_first_public_symbol_search_avoids_bulk_catalog(monkeypatch: pytest.MonkeyPatch) -> None:
    import marketdata_provider.symbols as symbols_mod

    calls: list[tuple[str, dict[str, object]]] = []

    def fake_http_get_json(
        url: str,
        *,
        params: dict[str, object] | None = None,
        timeout: float,
        user_agent: str,
        httpx: object,
    ) -> object:
        del timeout, user_agent, httpx
        calls.append((url, params or {}))
        if "coinbase" in url:
            return {"id": url.rsplit("/", 1)[-1], "base_currency": "BTC", "quote_currency": url.rsplit("-", 1)[-1]}
        if "kraken" in url:
            return {"result": {"XBTUSDT": {"altname": "XBTUSDT", "wsname": "XBT/USDT"}}}
        if "gateio" in url:
            return {"id": url.rsplit("/", 1)[-1], "base": "BTC", "quote": "USDT", "trade_status": "tradable"}
        if "huobi" in url:
            return {"status": "ok", "tick": {"close": 1}}
        raise AssertionError(url)

    monkeypatch.setattr(symbols_mod, "_http_get_json", fake_http_get_json)

    assert [item.symbol for item in search_symbols("coinbase", "spot", query="BTC", limit=1)] == ["BTC-USDT"]
    assert [item.symbol for item in search_symbols("kraken", "spot", query="BTC", limit=1)] == ["XBTUSDT"]
    assert [item.symbol for item in search_symbols("gateio", "spot", query="BTC", limit=1)] == ["BTC_USDT"]
    assert [item.symbol for item in search_symbols("htx", "spot", query="BTC", limit=1)] == ["BTCUSDT"]

    assert all("/products" not in url or url.endswith("BTC-USDT") for url, _ in calls[:1])
    assert any(params == {"pair": "XBTUSDT"} for _, params in calls)
    assert search_symbols("coinbase", "spot", query="", limit=1) == []


def test_query_first_public_symbol_search_skips_failed_candidates(monkeypatch: pytest.MonkeyPatch) -> None:
    import marketdata_provider.symbols as symbols_mod

    def fake_http_get_json(*args: object, **kwargs: object) -> object:
        raise MDNetworkUnavailable("down")

    monkeypatch.setattr(symbols_mod, "_http_get_json", fake_http_get_json)
    assert search_symbols("gateio", "spot", query="BTC", limit=1) == []


def test_query_first_helper_edge_branches() -> None:
    assert _query_base_asset("BTC-USDT") == "BTC"
    assert _query_base_asset("BTCUSDT") == "BTC"
    assert _candidate_spot_symbol("coinbase", "BTC", "USD") == "BTC-USD"
    assert _normalize_query_first_payload("htx", "btcusdt", "BTC", "USDT", {"status": "bad"}) == []
    assert _normalize_query_first_payload("unknown", "BTCUSDT", "BTC", "USDT", {}) == []
    with pytest.raises(MDUnsupportedFeature):
        _query_first_url("unknown", "BTCUSDT")
