import pytest
from marketdata_provider.errors import MDSymbolAmbiguous
from marketdata_provider.symbols import normalize_symbol

def test_binance_spot_and_perp():
    assert normalize_symbol("BINANCE:BTCUSDT").market == "spot"
    n = normalize_symbol("BINANCE:BTCUSDT.P")
    assert n.exchange == "binance" and n.market == "usdm" and n.exchange_symbol == "BTCUSDT" and n.tv_symbol == "BINANCE:BTCUSDT.P"

def test_bybit_perp():
    assert normalize_symbol("BYBIT:BTCUSDT.P").market == "linear"

def test_ambiguous_plain_symbol_strict():
    with pytest.raises(MDSymbolAmbiguous): normalize_symbol("BTCUSDT")
    assert normalize_symbol("BTCUSDT", exchange="BINANCE").tv_symbol == "BINANCE:BTCUSDT"
