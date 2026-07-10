import pytest
from marketdata_provider.config import MarketDataConfig
from marketdata_provider.errors import MDSymbolAmbiguous
from marketdata_provider.symbols import (
    DEFAULT_STABLE_QUOTE_ASSETS,
    is_stable_quoted,
    normalize_symbol,
    quote_asset,
)


def test_binance_spot_and_perp():
    assert normalize_symbol("BINANCE:BTCUSDT").market == "spot"
    n = normalize_symbol("BINANCE:BTCUSDT.P")
    assert (
        n.exchange == "binance"
        and n.market == "usdm"
        and n.exchange_symbol == "BTCUSDT"
        and n.tv_symbol == "BINANCE:BTCUSDT.P"
    )


def test_bybit_perp():
    assert normalize_symbol("BYBIT:BTCUSDT.P").market == "linear"


def test_stable_quote_assets_are_configured_not_usdt_hardcoded():
    cfg = MarketDataConfig()
    assert cfg.symbols.stable_quotes_only is True
    assert cfg.symbols.stable_quote_assets == DEFAULT_STABLE_QUOTE_ASSETS
    assert quote_asset("ETHFDUSD") == "FDUSD"
    assert quote_asset("BTCUSD_PERP") == "USD"
    assert is_stable_quoted("SOLUSDC") is True
    assert is_stable_quoted("ETHBTC") is False


def test_normalize_symbol_accepts_configured_stable_quotes_and_inverse_markets():
    assert (
        normalize_symbol("BINANCE:BTCUSDC", market="spot").exchange_symbol == "BTCUSDC"
    )
    assert normalize_symbol("BINANCE:BTCUSD_PERP", market="coinm").market == "coinm"
    assert normalize_symbol("BYBIT:BTCUSD.P", market="inverse").market == "inverse"


def test_ambiguous_plain_symbol_strict():
    with pytest.raises(MDSymbolAmbiguous):
        normalize_symbol("BTCUSDT")
    assert (
        normalize_symbol("BTCUSDT", exchange="BINANCE").tv_symbol == "BINANCE:BTCUSDT"
    )
