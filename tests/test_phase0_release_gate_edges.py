from __future__ import annotations

import json
import os
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

import marketdata_provider.exchanges.binance.archive as binance_archive
import marketdata_provider.exchanges.public_spot as public_spot
import marketdata_provider.service as service_module
import marketdata_provider.symbols as symbols
import marketdata_provider.symbols.public_markets as public_markets
from marketdata_provider.contracts.timeframe import Timeframe
from marketdata_provider.core.bar import MarketBar
from marketdata_provider.errors import (
    MDInvalidExchangeResponse,
    MDSymbolUnsupported,
    MDUnsupportedFeature,
)
from marketdata_provider.service import MarketDataService
from marketdata_provider.store.segment_checksums import _canon_number
from marketdata_provider.store.segment_store import SegmentStore


def _bar(time: int = 0) -> MarketBar:
    return MarketBar(
        time=time,
        open=1.0,
        high=2.0,
        low=0.5,
        close=1.5,
        volume=3.0,
        time_close=time + 59_999,
        exchange="binance",
        market="spot",
        symbol="BTCUSDT",
        timeframe="1m",
    )


def test_small_contract_checksum_and_archive_callback_edges(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    timeframe = Timeframe("1", "1m", 1, "minute", 60_000)
    assert timeframe.__eq__(object()) is NotImplemented
    assert _canon_number(None) is None
    assert _canon_number(0.0) == "0"
    assert _canon_number(1.2300) == "1.23"

    callbacks: list[dict[str, Any]] = []
    monkeypatch.setattr(binance_archive, "_load_archive_file", lambda **_kwargs: [])
    assert (
        binance_archive.fetch_binance_archive_bars(
            symbol="BTCUSDT",
            market="spot",
            timeframe="1m",
            start=0,
            end=60_000,
            cache_dir=tmp_path,
            progress_callback=lambda **payload: callbacks.append(payload),
        )
        == []
    )
    assert [item["phase"] for item in callbacks] == ["archive_monthly", "archive_daily"]


def test_public_market_request_and_normalization_defensive_edges(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    kwargs = {
        "symbol": "BTCUSDT",
        "timeframe": "1m",
        "start": 0,
        "end": 60_000,
        "user_agent": "phase0-test",
    }
    with pytest.raises(MDSymbolUnsupported, match="Unsupported public market exchange"):
        public_spot.public_market_get_bars_sync(
            exchange="unknown", market="linear", **kwargs
        )

    monkeypatch.setattr(public_spot, "public_spot_get_bars_sync", lambda **_kwargs: [])
    assert (
        public_spot.public_market_get_bars_sync(exchange="okx", market="spot", **kwargs)
        == []
    )
    with pytest.raises(MDSymbolUnsupported, match="Unsupported Coinbase market"):
        public_spot.public_market_get_bars_sync(
            exchange="coinbase", market="linear", **kwargs
        )
    with pytest.raises(MDSymbolUnsupported, match="Unsupported public market type"):
        public_spot._public_provider_market("unknown")

    assert (
        "linear-swap"
        in public_spot._market_request("htx", "linear", "BTC-USDT", "1m", 0, 60_000)[0]
    )
    assert (
        "linear-swap"
        in public_spot._market_request(
            "htx", "delivery_futures", "BTC-USDT", "1m", 0, 60_000
        )[0]
    )
    with pytest.raises(MDSymbolUnsupported, match="Unsupported public market exchange"):
        public_spot._market_request("unknown", "linear", "BTC", "1m", None, None)
    with pytest.raises(MDInvalidExchangeResponse, match="missing rows"):
        public_spot._extract_market_rows("unknown", [])

    assert public_spot._mexc_contract_rows({"data": []}) == []
    assert public_spot._mexc_contract_rows({"data": "invalid"}) is None
    assert public_spot._mexc_contract_rows({"data": {"time": "invalid"}}) is None
    with pytest.raises(MDInvalidExchangeResponse, match="mexc kline row is invalid"):
        public_spot._mexc_contract_rows(
            {
                "data": {
                    "time": [1],
                    "open": [],
                    "high": [],
                    "low": [],
                    "close": [],
                    "vol": [],
                }
            }
        )

    assert public_spot._row_to_market_bar("gateio", [1, 2, 3, 4, 5, 6], timeframe="1m")
    assert public_spot._row_to_market_bar("mexc", [1, 2, 3, 4, 5, 6], timeframe="1m")
    with pytest.raises(MDInvalidExchangeResponse, match="gateio kline row is invalid"):
        public_spot._row_to_market_bar("gateio", [], timeframe="1m")
    assert public_spot._row_to_market_bar("unknown", [], timeframe="1m") is None

    for helper in (
        public_spot._bitget_mix_granularity,
        public_spot._kraken_futures_interval,
        public_spot._mexc_contract_interval,
    ):
        with pytest.raises(MDSymbolUnsupported):
            helper("2h")
    assert public_spot._gate_settlement("BTC_USD", "inverse") == "btc"


def test_service_materialization_short_circuit_edges(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bar = _bar()
    query = SimpleNamespace(timeframe="5m")
    base_query = SimpleNamespace(timeframe="1m")
    service = object.__new__(MarketDataService)
    stored = iter([[], [bar]])
    service._base_query = lambda _query: base_query  # type: ignore[method-assign]
    service._stored_bars = lambda _query: next(stored)  # type: ignore[method-assign]
    service._ensure_stored = lambda *_args, **_kwargs: True  # type: ignore[method-assign]
    monkeypatch.setattr(
        service_module, "_coverage_complete", lambda bars, _query: bool(bars)
    )
    monkeypatch.setattr(
        service_module,
        "series_from_market_bars",
        lambda _query, bars, *, source: list(bars),
    )
    assert service.fetch_bars(query) == [bar]  # type: ignore[arg-type]

    same_query = SimpleNamespace(timeframe="1m")
    same = object.__new__(MarketDataService)
    same._base_query = lambda _query: same_query  # type: ignore[method-assign]
    complete = iter([False, True])
    same._stored_coverage_complete = lambda _query: next(complete)  # type: ignore[method-assign]
    same._stored_span_complete = lambda _query: True  # type: ignore[method-assign]
    same._ensure_stored = lambda _query: True  # type: ignore[method-assign]
    assert same.materialize_bars(same_query) == {  # type: ignore[arg-type]
        "ok": True,
        "span_ok": True,
        "changed": True,
        "bars_returned": 0,
    }

    derived = object.__new__(MarketDataService)
    derived._base_query = lambda _query: base_query  # type: ignore[method-assign]
    complete = iter([False, True])
    derived._stored_coverage_complete = lambda _query: next(complete)  # type: ignore[method-assign]
    derived._stored_span_complete = lambda _query: True  # type: ignore[method-assign]
    derived._ensure_stored = lambda _query: True  # type: ignore[method-assign]
    assert derived.materialize_bars(query)["changed"] is True  # type: ignore[arg-type]

    spanned = object.__new__(MarketDataService)
    spanned._base_query = lambda _query: base_query  # type: ignore[method-assign]
    spanned._stored_coverage_complete = lambda _query: False  # type: ignore[method-assign]
    spanned._stored_span_complete = lambda _query: True  # type: ignore[method-assign]
    spanned._ensure_stored = lambda _query: False  # type: ignore[method-assign]
    assert spanned.materialize_bars(query) == {  # type: ignore[arg-type]
        "ok": False,
        "span_ok": True,
        "changed": False,
        "bars_returned": 0,
    }


def test_segment_manifest_heal_and_vacuum_race_edges(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = SegmentStore(tmp_path)
    bar = _bar()
    store.replace_all(
        [bar], exchange="binance", market="spot", symbol="BTCUSDT", timeframe="1m"
    )
    manifest_path = next(tmp_path.rglob("manifest.json"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["checksum"] = "stale"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    original_write_text = Path.write_text

    def fail_manifest_write(path: Path, *args: Any, **kwargs: Any) -> int:
        if path == manifest_path:
            raise OSError("read-only manifest")
        return original_write_text(path, *args, **kwargs)

    with monkeypatch.context() as scoped:
        scoped.setattr(Path, "write_text", fail_manifest_write)
        assert store.read_all(
            exchange="binance", market="spot", symbol="BTCUSDT", timeframe="1m"
        ) == [bar]

    stale = tmp_path / "v1" / "race" / ".bars.csv.tmp"
    stale.parent.mkdir(parents=True, exist_ok=True)
    stale.write_text("stale", encoding="utf-8")
    os.utime(stale, (0, 0))
    original_stat = Path.stat

    def race_stat(path: Path, *args: Any, **kwargs: Any):
        if path == stale:
            raise FileNotFoundError(path)
        return original_stat(path, *args, **kwargs)

    monkeypatch.setattr(Path, "stat", race_stat)
    assert store.vacuum()["removed_stale_data_files"] == 0


def test_symbol_discovery_and_public_market_filter_edges() -> None:
    assert symbols._query_base_asset("") == ""
    assert (
        symbols._search_public_spot_symbols_by_query(
            "coinbase",
            "",
            timeout=1.0,
            user_agent="phase0-test",
            quote_assets=("USDT",),
            result_limit=1,
            httpx=object(),
        )
        == []
    )

    assert public_markets._public_symbol_endpoint("htx", "delivery_futures")[1] == {
        "business_type": "futures"
    }
    with pytest.raises(MDUnsupportedFeature, match="Symbol discovery unsupported"):
        public_markets._public_symbol_endpoint("unknown", "linear")
    assert (
        public_markets.normalize_public_market_symbols(
            "okx", "linear", {"data": ["not-a-row"]}, stable_quotes_only=False
        )
        == []
    )

    rejected = [
        ("okx", "linear", {"state": "suspended"}),
        ("kraken", "linear", {"tradeable": False}),
        ("kucoin", "linear", {"status": "disabled"}),
        ("bitget", "linear", {"status": "disabled"}),
        ("gateio", "linear", {"in_delisting": True}),
        ("htx", "linear", {"contract_status": "disabled"}),
        ("mexc", "linear", {"state": "1"}),
    ]
    for exchange, market, row in rejected:
        assert (
            public_markets._parse_public_market_symbol_row(exchange, market, row)
            is None
        )
    assert public_markets._parse_public_market_symbol_row(
        "okx",
        "linear",
        {"state": "live", "ctType": "linear", "instId": "BTC-USDT-SWAP"},
    ) == ("BTC-USDT-SWAP", "BTC", "USDT", "linear")
    assert (
        public_markets._parse_public_market_symbol_row("unknown", "linear", {}) is None
    )
    assert public_markets._symbol_tuple_from_delimited("BTCUSDT") is None
