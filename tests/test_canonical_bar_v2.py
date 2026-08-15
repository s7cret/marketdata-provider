from pathlib import Path

import pytest

from marketdata_provider.canonical.bar import (
    bar_finality,
    build_data_snapshot,
    make_canonical_bar,
)
from marketdata_provider.compat.v4 import finality_from_closed
from marketdata_provider.errors import MDValidationError
from openpine_contracts import Finality, RevisionState, decimal_string


def _bar(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
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
        "revision_state": RevisionState.ORIGINAL,
        "snapshot_id": "snap-1",
        "provider": "binance",
    }
    payload.update(overrides)
    return make_canonical_bar(**payload)


def test_exact_close_boundary_is_inclusive() -> None:
    assert bar_finality(close_time_ms=60999, server_time_ms=60999) is Finality.FINAL
    assert bar_finality(close_time_ms=60999, server_time_ms=60998) is Finality.OPEN


def test_missing_server_time_fail_closed() -> None:
    with pytest.raises(MDValidationError, match="server_time_ms required"):
        bar_finality(close_time_ms=60999, server_time_ms=None)


def test_missing_finality_is_rejected() -> None:
    with pytest.raises(MDValidationError, match="finality"):
        make_canonical_bar(
            instrument_id="binance:spot:BTCUSDT",
            timeframe="1m",
            open_time_utc_ms=1000,
            close_time_utc_ms=60999,
            open="1.0",
            high="2.0",
            low="0.5",
            close="1.5",
            volume="10",
            snapshot_id="snap-1",
            provider="binance",
        )


def test_float_ohlc_is_rejected() -> None:
    with pytest.raises(MDValidationError, match="float"):
        _bar(open=1.25)


def test_decimal_text_is_normalized_not_floated() -> None:
    bar = _bar(open="1.2300", volume="-0")
    assert bar["open"] == decimal_string("1.2300")
    assert bar["volume"] == "0"
    assert bar["finality"] is Finality.FINAL


def test_compat_v4_missing_closed_is_not_final() -> None:
    assert finality_from_closed(True) is Finality.FINAL
    assert finality_from_closed(False) is Finality.OPEN
    with pytest.raises(MDValidationError, match="missing"):
        finality_from_closed(None)


def test_open_bar_survives_snapshot_and_is_not_in_closed_only() -> None:
    open_bar = _bar(finality=Finality.OPEN, snapshot_id="live")
    final_bar = _bar(
        open_time_utc_ms=61000, close_time_utc_ms=120999, snapshot_id="live"
    )
    snapshot = build_data_snapshot(
        snapshot_id="live",
        instrument_id="binance:spot:BTCUSDT",
        timeframe="1m",
        start_utc_ms=1000,
        end_utc_ms=121000,
        bars=[open_bar, final_bar],
        finality_policy="CLOSED_BAR_ONLY",
    )
    assert snapshot["bar_count"] == 1
    assert all(bar["finality"] is Finality.FINAL for bar in snapshot["bars"])
    replay = build_data_snapshot(
        snapshot_id="live",
        instrument_id="binance:spot:BTCUSDT",
        timeframe="1m",
        start_utc_ms=1000,
        end_utc_ms=121000,
        bars=[open_bar, final_bar],
        finality_policy="ALLOW_OPEN",
    )
    assert replay["bars"][0]["finality"] is Finality.OPEN
    assert replay["series_hash"] != snapshot["series_hash"]


def test_corrected_bar_changes_snapshot_hash() -> None:
    original = _bar()
    corrected = _bar(revision_state=RevisionState.CORRECTED, revision=1, close="1.6")
    first = build_data_snapshot(
        snapshot_id="s1",
        instrument_id="binance:spot:BTCUSDT",
        timeframe="1m",
        start_utc_ms=1000,
        end_utc_ms=61000,
        bars=[original],
    )
    second = build_data_snapshot(
        snapshot_id="s2",
        instrument_id="binance:spot:BTCUSDT",
        timeframe="1m",
        start_utc_ms=1000,
        end_utc_ms=61000,
        bars=[corrected],
    )
    assert first["series_hash"] != second["series_hash"]
    assert corrected["revision_state"] is RevisionState.CORRECTED


def test_contracts_pin_is_exact_git_sha() -> None:
    text = Path("pyproject.toml").read_text(encoding="utf-8")
    assert (
        "openpine-contracts @ git+https://github.com/s7cret/openpine-contracts.git@"
        in text
    )
    assert "51e32ebaaf02eecb81443e8ca7e89b2543cb25a3" in text
    assert "openpine-contracts==" not in text


def test_adapter_fail_closed_edges() -> None:
    with pytest.raises(MDValidationError, match="required"):
        make_canonical_bar(
            instrument_id="",
            timeframe="1m",
            open_time_utc_ms=1000,
            open="1",
            high="1",
            low="1",
            close="1",
            volume="0",
            snapshot_id="s",
            provider="p",
            finality=Finality.FINAL,
        )
    with pytest.raises(MDValidationError, match="Finality"):
        make_canonical_bar(
            instrument_id="i",
            timeframe="1m",
            open_time_utc_ms=1000,
            open="1",
            high="1",
            low="1",
            close="1",
            volume="0",
            snapshot_id="s",
            provider="p",
            finality="FINAL",  # type: ignore[arg-type]
        )
    with pytest.raises(MDValidationError, match="revision"):
        _bar(revision=-1)
    with pytest.raises(MDValidationError, match="OHLC"):
        _bar(open="2.0", high="1.0")
    with pytest.raises(MDValidationError, match="invalid"):
        _bar(open="NaN")
    with pytest.raises(MDValidationError, match="volume"):
        _bar(volume="-1")
    with pytest.raises(MDValidationError, match="unsupported timeframe"):
        _bar(timeframe="7m", close_time_utc_ms=1000)
    with pytest.raises(MDValidationError, match="unknown finality_policy"):
        build_data_snapshot(
            snapshot_id="s",
            instrument_id="i",
            timeframe="1m",
            start_utc_ms=0,
            end_utc_ms=1,
            bars=[],
            finality_policy="MAYBE",
        )


def test_close_time_from_timeframe_and_revoked_excluded() -> None:
    bar = _bar(close_time_utc_ms=1000)
    assert bar["close_time_utc_ms"] == 60999
    revoked = _bar(revision_state=RevisionState.REVOKED)
    snapshot = build_data_snapshot(
        snapshot_id="s",
        instrument_id="binance:spot:BTCUSDT",
        timeframe="1m",
        start_utc_ms=0,
        end_utc_ms=70000,
        bars=[revoked],
    )
    assert snapshot["bar_count"] == 0
    orphan = dict(_bar())
    orphan.pop("revision_state", None)
    keep = build_data_snapshot(
        snapshot_id="s",
        instrument_id="binance:spot:BTCUSDT",
        timeframe="1m",
        start_utc_ms=0,
        end_utc_ms=70000,
        bars=[orphan],
    )
    assert keep["bar_count"] == 1
