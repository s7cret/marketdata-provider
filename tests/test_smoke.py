#!/usr/bin/env python3
"""Smoke tests for marketdata_provider."""
from __future__ import annotations

from pathlib import Path
from marketdata_provider import (
    Bar,
    MarketDataConfig,
    OfflineDataProvider,
    OfflineDataConfig,
    DataProvider,
    IntrabarDataProvider,
    RUNTIME_CONTRACT_VERSION,
)


def test_runtime_contract_version():
    assert RUNTIME_CONTRACT_VERSION == "1.4"


def test_bar_creation():
    b = Bar(time=1704067200000, open=10.0, high=11.0, low=9.0, close=10.5, volume=100.0, time_close=1704070800000)
    assert b.time == 1704067200000
    assert b.open == 10.0
    assert b.high == 11.0
    assert b.low == 9.0
    assert b.close == 10.5
    assert b.volume == 100.0
    assert b.time_close == 1704070800000


def test_market_data_config_default():
    cfg = MarketDataConfig()
    assert cfg.runtime_contract_version == "1.4"
    assert cfg.include_open_candle is False
    assert cfg.default_exchange is None


def test_offline_data_provider_csv(tmp_path) -> None:
    csv_path = tmp_path / "test.csv"
    csv_path.write_text("time,open,high,low,close,volume\n1704067200000,10,11,9,10.5,100\n1704070800000,10.5,12,10,11,200\n")
    
    provider = OfflineDataProvider(csv_path, timeframe="60")
    bars = provider.get_bars("TEST", "60", None, None)
    
    assert len(bars) == 2
    assert bars[0].time == 1704067200000
    assert bars[0].close == 10.5
    assert bars[1].time == 1704070800000
    assert bars[1].close == 11.0


def test_offline_data_provider_protocols():
    """OfflineDataProvider satisfies DataProvider and IntrabarDataProvider protocols."""
    from typing import get_type_hints
    provider = OfflineDataProvider("/dev/null")
    
    # Check required methods exist
    assert hasattr(provider, "get_bars")
    assert hasattr(provider, "get_intrabar_bars")
    
    # Check method signatures match protocol
    import inspect
    get_bars_sig = inspect.signature(provider.get_bars)
    assert "symbol" in get_bars_sig.parameters
    assert "timeframe" in get_bars_sig.parameters


def test_offline_intrabar(tmp_path) -> None:
    csv_path = tmp_path / "intrabar.csv"
    csv_path.write_text("time,open,high,low,close,volume\n1704067200000,10,11,9,10.5,100\n1704067500000,10.5,11,10,10.8,50\n1704067800000,10.8,12,10.5,11.5,75\n")
    
    provider = OfflineDataProvider(csv_path, timeframe="5")
    chart_bar = Bar(time=1704067200000, open=10, high=11, low=9, close=10.5, volume=100, time_close=1704067800000)
    
    intrabars = provider.get_intrabar_bars("TEST", chart_bar, "5")
    assert len(intrabars) >= 1


def test_offline_data_provider_unsupported_format(tmp_path) -> None:
    bad_path = tmp_path / "test.txt"
    bad_path.write_text("not csv")
    
    provider = OfflineDataProvider(bad_path)
    try:
        provider.get_bars("TEST", "60", None, None)
        assert False, "Should raise"
    except Exception as e:
        assert "Unsupported" in str(e) or "not csv" in str(e).lower()
