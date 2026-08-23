from __future__ import annotations

from pathlib import Path

import pytest

from marketdata_provider.canonical.provider import snapshot_from_market_bars
from marketdata_provider.contracts import BarQuery, InstrumentKey, parse_timeframe
from marketdata_provider.core.bar import Bar, MarketBar
from marketdata_provider.errors import MDMissingFinality, MDValidationError
from marketdata_provider.service import _aggregate_bucket
from marketdata_provider.store.repair import load_repair_source, market_bar_from_bar


def _query(*, timeframe: str = "3m", end_ms: int = 180_000) -> BarQuery:
    return BarQuery(
        InstrumentKey("binance", "spot", "BTCUSDT"),
        parse_timeframe(timeframe),
        0,
        end_ms,
        source="provider",
    )


def _source_bar(open_time_ms: int, provider_revision: str) -> MarketBar:
    return MarketBar(
        time=open_time_ms,
        time_close=open_time_ms + 59_999,
        open=1.0,
        high=2.0,
        low=0.5,
        close=1.5,
        volume=1.0,
        exchange="binance",
        market="spot",
        symbol="BTCUSDT",
        timeframe="1m",
        is_closed=True,
        provider="binance",
        provider_revision=provider_revision,
        open_text="1",
        high_text="2",
        low_text="0.5",
        close_text="1.5",
        volume_text="1",
    )


def test_aggregate_revision_binds_each_revision_to_source_bar() -> None:
    first = _aggregate_bucket(
        0,
        [_source_bar(0, "r1"), _source_bar(60_000, "r2")],
        query=_query(),
    )
    swapped = _aggregate_bucket(
        0,
        [_source_bar(0, "r2"), _source_bar(60_000, "r1")],
        query=_query(),
    )

    assert first.provider_revision != swapped.provider_revision


def test_public_snapshot_rejects_market_bar_without_exact_decimal_text() -> None:
    rounded_source = MarketBar(
        time=0,
        time_close=59_999,
        open=float("0.123456789123456789"),
        high=1.0,
        low=0.1,
        close=0.5,
        volume=10.0,
        exchange="binance",
        market="spot",
        symbol="BTCUSDT",
        timeframe="1m",
        is_closed=True,
        provider="binance",
        provider_revision="raw-r1",
    )

    with pytest.raises(MDValidationError, match="exact source decimal text"):
        snapshot_from_market_bars(
            _query(timeframe="1m", end_ms=60_000),
            [rounded_source],
            provider="binance",
            provider_revision="raw-r1",
        )


def _repair_bar(bar: Bar) -> MarketBar:
    return market_bar_from_bar(
        bar,
        exchange="binance",
        market="spot",
        symbol="BTCUSDT",
        timeframe="1m",
    )


def test_production_repair_rejects_missing_finality() -> None:
    legacy_bar = Bar(0, 1.0, 2.0, 0.5, 1.5, 1.0, 59_999)

    with pytest.raises(MDMissingFinality, match="finality"):
        _repair_bar(legacy_bar)


def test_production_repair_rejects_missing_close_time() -> None:
    missing_close = MarketBar(
        time=0,
        time_close=None,
        open=1.0,
        high=2.0,
        low=0.5,
        close=1.5,
        volume=1.0,
        is_closed=True,
        open_text="1",
        high_text="2",
        low_text="0.5",
        close_text="1.5",
        volume_text="1",
    )

    with pytest.raises(MDValidationError, match="close time"):
        _repair_bar(missing_close)


def test_production_repair_preserves_exact_decimal_text() -> None:
    exact_open = "0.123456789123456789"
    source = MarketBar(
        time=0,
        time_close=59_999,
        open=float(exact_open),
        high=1.0,
        low=0.1,
        close=0.5,
        volume=10.0,
        is_closed=True,
        provider="binance",
        provider_revision="repair-source-r1",
        open_text=exact_open,
        high_text="1",
        low_text="0.1",
        close_text="0.5",
        volume_text="10",
    )

    repaired = _repair_bar(source)

    assert repaired.open_text == exact_open


def test_repair_loader_preserves_explicit_source_contract(tmp_path: Path) -> None:
    exact_open = "0.123456789123456789"
    source_path = tmp_path / "repair.csv"
    source_path.write_text(
        "time,open,high,low,close,volume,time_close,finality,provider,"
        "provider_revision,revision_state,revision\n"
        f"0,{exact_open},1,0.1,0.5,10,59999,FINAL,binance,raw-r1,ORIGINAL,0\n",
        encoding="utf-8",
    )

    bars = load_repair_source(
        source_path,
        exchange="binance",
        market="spot",
        symbol="BTCUSDT",
        timeframe="1m",
    )

    assert bars[0].open_text == exact_open
    assert bars[0].is_closed is True
    assert bars[0].time_close == 59_999
    assert bars[0].provider_revision == "raw-r1"


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("is_closed", False, "must be FINAL"),
        ("provider", None, "provider identity"),
        ("open_text", None, "exact source decimal text"),
    ],
)
def test_repair_bar_rejects_incomplete_source_contract(
    field: str, value: object, message: str
) -> None:
    source = _source_bar(0, "repair-r1")
    object.__setattr__(source, field, value)

    with pytest.raises(MDValidationError, match=message):
        _repair_bar(source)


def _write_repair_csv(path: Path, **overrides: str) -> None:
    row = {
        "time": "0",
        "open": "1",
        "high": "2",
        "low": "0.5",
        "close": "1.5",
        "volume": "10",
        "time_close": "59999",
        "finality": "FINAL",
        "provider": "binance",
        "provider_revision": "raw-r1",
        "revision_state": "ORIGINAL",
        "revision": "0",
    }
    row.update(overrides)
    fields = list(row)
    path.write_text(
        ",".join(fields) + "\n" + ",".join(row[field] for field in fields) + "\n",
        encoding="utf-8",
    )


@pytest.mark.parametrize(
    ("overrides", "error", "message"),
    [
        ({"finality": ""}, MDMissingFinality, "finality"),
        ({"finality": "BROKEN"}, MDValidationError, "finality is invalid"),
        ({"finality": "OPEN"}, MDValidationError, "must be FINAL"),
        ({"time": ""}, MDValidationError, "time is required"),
        ({"time_close": ""}, MDValidationError, "time_close is required"),
        ({"provider_revision": ""}, MDValidationError, "provider identity"),
        ({"provider": "bybit"}, MDValidationError, "does not match exchange"),
        ({"revision_state": ""}, MDValidationError, "revision identity is required"),
        (
            {"revision_state": "BROKEN"},
            MDValidationError,
            "revision identity is invalid",
        ),
        ({"open": ""}, MDValidationError, "exact source decimal text"),
        ({"open": "not-a-number"}, MDValidationError, "row is invalid"),
    ],
)
def test_repair_loader_rejects_invalid_canonical_rows(
    tmp_path: Path,
    overrides: dict[str, str],
    error: type[Exception],
    message: str,
) -> None:
    source_path = tmp_path / "invalid.csv"
    _write_repair_csv(source_path, **overrides)

    with pytest.raises(error, match=message):
        load_repair_source(
            source_path,
            exchange="binance",
            market="spot",
            symbol="BTCUSDT",
            timeframe="1m",
        )


def test_repair_loader_rejects_non_csv_source(tmp_path: Path) -> None:
    source_path = tmp_path / "repair.json"
    source_path.write_text("{}", encoding="utf-8")

    with pytest.raises(MDValidationError, match="requires CSV"):
        load_repair_source(
            source_path,
            exchange="binance",
            market="spot",
            symbol="BTCUSDT",
            timeframe="1m",
        )
