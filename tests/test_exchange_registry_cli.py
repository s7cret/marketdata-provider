from __future__ import annotations

import json

import pytest

from marketdata_provider.cli.main import main
from marketdata_provider.exchanges.registry import (
    EXCHANGES,
    MARKET_TYPES,
    exchange_payloads,
    get_exchange,
    list_exchanges,
    list_market_types,
    market_type_payloads,
    normalize_exchange_id,
    normalize_market_type,
)


def _last_json(capsys: pytest.CaptureFixture[str]) -> dict[str, object]:
    out = capsys.readouterr().out.strip().splitlines()[-1]
    return json.loads(out)


def test_exchange_registry_lists_top_ten_and_native_subset() -> None:
    all_exchanges = list_exchanges()
    native = list_exchanges(native_only=True)
    assert len(all_exchanges) == 10
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
    assert all_exchanges[0].native_adapter is True
    assert all_exchanges[2].native_adapter is True
    assert exchange_payloads(native_only=True)[0]["native_adapter"] is True
    assert market_type_payloads("binance")[0]["id"] == "spot"


def test_exchange_registry_aliases_and_market_type_normalization() -> None:
    assert normalize_exchange_id("Gate-IO") == "gateio"
    assert normalize_exchange_id("coinbasepro") == "coinbase"
    assert normalize_exchange_id("HUOBI") == "htx"
    assert get_exchange("gate").id == "gateio"
    assert get_exchange("coinbase-exchange").name == "Coinbase Exchange"
    assert normalize_market_type("linear") == "usdt_futures"
    assert normalize_market_type("coinm") == "coin_futures"
    assert list_market_types("okx")[-1].id == "options"
    with pytest.raises(KeyError):
        get_exchange("unknown")
    with pytest.raises(KeyError):
        normalize_market_type("unknown")


def test_exchange_registry_payload_shapes_are_json_safe() -> None:
    payload = [exchange.to_dict() for exchange in EXCHANGES]
    market_payload = [market.to_dict() for market in MARKET_TYPES]
    encoded = json.dumps({"exchanges": payload, "market_types": market_payload})
    assert "official-bulk" in encoded
    assert "usdt_futures" in encoded


def test_cli_exchanges_json_and_table(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["exchanges", "--native-only"]) == 0
    payload = _last_json(capsys)
    exchanges = payload["exchanges"]
    assert isinstance(exchanges, list)
    assert [item["id"] for item in exchanges] == [
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

    assert main(["exchanges", "--exchange", "huobi", "--format", "table"]) == 0
    table = capsys.readouterr().out
    assert "htx" in table
    assert "official-partial" in table


def test_cli_market_types_and_errors(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["market-types"]) == 0
    payload = _last_json(capsys)
    market_types = payload["market_types"]
    assert isinstance(market_types, list)
    assert {item["id"] for item in market_types} >= {"spot", "usdt_futures"}

    assert main(["market-types", "--exchange", "binance", "--format", "table"]) == 0
    table = capsys.readouterr().out
    assert "USDT/USDC-margined" in table

    assert main(["exchanges", "--exchange", "not-real"]) == 2
    err = _last_json(capsys)
    assert err["ok"] is False
    assert err["code"] == "MD_UNSUPPORTED_FEATURE"

    assert main(["market-types", "--exchange", "not-real"]) == 2
    err = _last_json(capsys)
    assert err["ok"] is False
    assert err["code"] == "MD_UNSUPPORTED_FEATURE"
