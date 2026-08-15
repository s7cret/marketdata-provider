import pytest

from marketdata_provider.contracts.errors import InvalidBarError
from marketdata_provider.contracts.v2 import (
    DataQuery,
    bar_finality,
    build_data_snapshot,
    canonical_bars_from_binance_klines,
    make_canonical_bar,
)
from marketdata_provider.errors import MDValidationError
from marketdata_provider.exchanges.binance.rest import normalize_binance_klines
from openpine_contracts import Finality, RevisionState


def _bar(**overrides: object):
    payload = {
        "instrument_id": "binance:spot:BTCUSDT",
        "timeframe": "1m",
        "open_time_utc_ms": 1000,
        "close_time_utc_ms": 60999,
        "open": "1.0",
        "high": "2.0",
        "low": "0.5",
        "close": "1.5",
        "volume": "10",
        "finality": Finality.FINAL,
        "snapshot_id": "snap-1",
        "provider": "binance",
    }
    payload.update(overrides)
    return make_canonical_bar(**payload)  # type: ignore[arg-type]


def test_finality_uses_inclusive_close_boundary() -> None:
    assert bar_finality(close_time_ms=60999, server_time_ms=60999) is Finality.FINAL
    assert bar_finality(close_time_ms=60999, server_time_ms=60998) is Finality.OPEN


def test_missing_server_time_fail_closed() -> None:
    with pytest.raises(MDValidationError, match="server_time_ms required"):
        bar_finality(close_time_ms=60999, server_time_ms=None)
    with pytest.raises(MDValidationError, match="server_time_ms required"):
        normalize_binance_klines(
            [[1000, "1", "2", "0.5", "1.5", "10", 60999]],
            symbol="BTCUSDT",
            market="spot",
            timeframe="1m",
        )


def test_decimal_from_exchange_text_not_float() -> None:
    bar = _bar(open="1.123456789", close="1.123456780")
    assert bar.open == "1.123456789"
    assert bar.close == "1.12345678"
    with pytest.raises(InvalidBarError, match="float"):
        _bar(open=1.25)


def test_negative_zero_and_invalid_ohlc() -> None:
    bar = _bar(volume="-0")
    assert bar.volume == "0"
    with pytest.raises(InvalidBarError, match="OHLC"):
        _bar(high="1.0", low="0.5", open="2.0")
    with pytest.raises(InvalidBarError):
        _bar(open="NaN")


def test_close_time_from_timeframe_not_open_time() -> None:
    bar = _bar(close_time_utc_ms=1000)
    assert bar.close_time_utc_ms == 60999
    assert bar.close_time_utc_ms > bar.open_time_utc_ms


def test_canonical_kline_path_keeps_decimal_text() -> None:
    rows = [
        [1000, "1.2300", "2", "0.5", "1.5", "10", 60999],
        [61000, "1.5", "2", "1", "1.2", "5", 120999],
    ]
    bars = canonical_bars_from_binance_klines(
        rows,
        instrument_id="binance:spot:BTCUSDT",
        timeframe="1m",
        snapshot_id="snap-1",
        server_time_ms=70_000,
    )
    assert [bar.open_time_utc_ms for bar in bars] == [1000]
    assert bars[0].open == "1.23"
    assert bars[0].finality is Finality.FINAL


def test_closed_snapshot_excludes_open_and_hashes_stable() -> None:
    bars = [
        _bar(open_time_utc_ms=1000, close_time_utc_ms=60999, open="1.0"),
        _bar(open_time_utc_ms=1000, close_time_utc_ms=60999, open="1.0"),
    ]
    query = DataQuery(
        instrument_id="binance:spot:BTCUSDT",
        timeframe="1m",
        start_utc_ms=1000,
        end_utc_ms=61000,
    )
    left = build_data_snapshot(bars, query=query, created_at_utc_ms=1)
    right = build_data_snapshot(list(reversed(bars)), query=query, created_at_utc_ms=1)
    assert left.bar_count == 1
    assert left.series_hash == right.series_hash
    assert left.snapshot_id == right.snapshot_id
    open_bar = _bar(finality=Finality.OPEN)
    with pytest.raises(MDValidationError, match="OPEN"):
        build_data_snapshot([open_bar], query=query, created_at_utc_ms=1)


def test_conflict_blocks_closed_snapshot() -> None:
    query = DataQuery(
        instrument_id="binance:spot:BTCUSDT",
        timeframe="1m",
        start_utc_ms=1000,
        end_utc_ms=61000,
    )
    with pytest.raises(MDValidationError, match="conflict"):
        build_data_snapshot(
            [_bar(open="1.0"), _bar(open="1.1")],
            query=query,
            created_at_utc_ms=1,
        )


def test_no_local_finality_enum() -> None:
    import marketdata_provider.contracts as contracts
    import openpine_contracts

    assert contracts.Finality is openpine_contracts.Finality
    assert contracts.RevisionState is openpine_contracts.RevisionState
