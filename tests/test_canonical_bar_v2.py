from pathlib import Path

import pytest
from openpine_contracts import Finality, RevisionState, decimal_string

from marketdata_provider.canonical.bar import (
    bar_finality,
    build_data_snapshot,
    canonical_bars_from_binance_klines,
    make_canonical_bar,
)
from marketdata_provider.compat.v4 import finality_from_closed
from marketdata_provider.errors import MDValidationError

PRODUCER_COMMIT = "1" * 40
STACK_ID = "sha256:" + "2" * 64
PROVIDER_REVISION = {"known": True, "revision": "binance-rest-v1"}


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
        "provider_revision": PROVIDER_REVISION,
        "producer_commit": PRODUCER_COMMIT,
        "stack_id": STACK_ID,
    }
    payload.update(overrides)
    return make_canonical_bar(**payload)


def test_market_bar_requires_explicit_is_closed() -> None:
    from marketdata_provider.core.bar import MarketBar

    with pytest.raises(MDValidationError, match="is_closed required"):
        MarketBar(time=0, open=1.0, high=1.0, low=1.0, close=1.0)


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
            provider_revision=PROVIDER_REVISION,
            producer_commit=PRODUCER_COMMIT,
            stack_id=STACK_ID,
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
        provider_revision=PROVIDER_REVISION,
        producer_commit=PRODUCER_COMMIT,
        stack_id=STACK_ID,
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
        provider_revision=PROVIDER_REVISION,
        producer_commit=PRODUCER_COMMIT,
        stack_id=STACK_ID,
        start_utc_ms=1000,
        end_utc_ms=121000,
        bars=[open_bar, final_bar],
        finality_policy="ALLOW_OPEN",
    )
    assert replay["bars"][0]["finality"] is Finality.OPEN
    assert replay["series_hash"] != snapshot["series_hash"]


def test_corrected_bar_changes_snapshot_hash() -> None:
    original = _bar(snapshot_id="s1")
    corrected_source = _bar(snapshot_id="s2")
    corrected = _bar(
        snapshot_id="s2",
        revision_state=RevisionState.CORRECTED,
        revision=1,
        close="1.6",
        superseded_bar_hash=corrected_source["bar_content_hash"],
    )
    first = build_data_snapshot(
        snapshot_id="s1",
        instrument_id="binance:spot:BTCUSDT",
        timeframe="1m",
        provider_revision=PROVIDER_REVISION,
        producer_commit=PRODUCER_COMMIT,
        stack_id=STACK_ID,
        start_utc_ms=1000,
        end_utc_ms=61000,
        bars=[original],
    )
    second = build_data_snapshot(
        snapshot_id="s2",
        instrument_id="binance:spot:BTCUSDT",
        timeframe="1m",
        provider_revision=PROVIDER_REVISION,
        producer_commit=PRODUCER_COMMIT,
        stack_id=STACK_ID,
        start_utc_ms=1000,
        end_utc_ms=61000,
        bars=[corrected_source, corrected],
    )
    assert first["series_hash"] != second["series_hash"]
    assert corrected["revision_state"] is RevisionState.CORRECTED


def test_contracts_dependency_is_exact_publishable_rc4() -> None:
    text = Path("pyproject.toml").read_text(encoding="utf-8")
    assert '"openpine-contracts==5.0.0rc4"' in text
    assert "git+" not in text


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
            provider_revision=PROVIDER_REVISION,
            producer_commit=PRODUCER_COMMIT,
            stack_id=STACK_ID,
            finality=Finality.FINAL,
        )
    normalized = _bar(finality="FINAL", revision_state="ORIGINAL")  # type: ignore[arg-type]
    assert normalized["finality"] is Finality.FINAL
    assert normalized["revision_state"] is RevisionState.ORIGINAL
    with pytest.raises(MDValidationError, match="finality"):
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
            provider_revision=PROVIDER_REVISION,
            producer_commit=PRODUCER_COMMIT,
            stack_id=STACK_ID,
            finality="UNKNOWN",  # type: ignore[arg-type]
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
            provider_revision=PROVIDER_REVISION,
            producer_commit=PRODUCER_COMMIT,
            stack_id=STACK_ID,
            start_utc_ms=0,
            end_utc_ms=1,
            bars=[],
            finality_policy="MAYBE",
        )


def test_close_time_from_timeframe_and_revoked_excluded() -> None:
    bar = _bar(close_time_utc_ms=None)
    assert bar["close_time_utc_ms"] == 60999
    original = _bar(snapshot_id="s")
    revoked = _bar(
        snapshot_id="s",
        revision_state=RevisionState.REVOKED,
        revision=1,
        superseded_bar_hash=original["bar_content_hash"],
    )
    snapshot = build_data_snapshot(
        snapshot_id="s",
        instrument_id="binance:spot:BTCUSDT",
        timeframe="1m",
        provider_revision=PROVIDER_REVISION,
        producer_commit=PRODUCER_COMMIT,
        stack_id=STACK_ID,
        start_utc_ms=0,
        end_utc_ms=70000,
        bars=[original, revoked],
    )
    assert snapshot["bar_count"] == 0
    orphan = dict(_bar(snapshot_id="s"))
    orphan.pop("revision_state", None)
    with pytest.raises(MDValidationError, match="revision_state"):
        build_data_snapshot(
            snapshot_id="s",
            instrument_id="binance:spot:BTCUSDT",
            timeframe="1m",
            provider_revision=PROVIDER_REVISION,
            producer_commit=PRODUCER_COMMIT,
            stack_id=STACK_ID,
            start_utc_ms=0,
            end_utc_ms=70000,
            bars=[orphan],
        )


def test_canonical_klines_keep_decimal_text_and_open_finality() -> None:
    rows = [
        [1000, "1.2300", "2", "0.5", "1.5", "10", 60999],
        [61000, "1.5", "2", "1", "1.2", "5", 120999],
    ]
    bars = canonical_bars_from_binance_klines(
        rows,
        instrument_id="binance:spot:BTCUSDT",
        timeframe="1m",
        provider="binance",
        provider_revision=PROVIDER_REVISION,
        producer_commit=PRODUCER_COMMIT,
        stack_id=STACK_ID,
        snapshot_id="snap",
        server_time_ms=70000,
    )
    assert len(bars) == 1
    assert bars[0]["open"] == "1.23"
    assert bars[0]["finality"] is Finality.FINAL
    with pytest.raises(MDValidationError, match="server_time_ms required"):
        canonical_bars_from_binance_klines(
            rows,
            instrument_id="binance:spot:BTCUSDT",
            timeframe="1m",
            provider="binance",
            provider_revision=PROVIDER_REVISION,
            producer_commit=PRODUCER_COMMIT,
            stack_id=STACK_ID,
            snapshot_id="snap",
            server_time_ms=None,
        )
    with pytest.raises(MDValidationError, match="too short"):
        canonical_bars_from_binance_klines(
            [[1000, "1"]],
            instrument_id="binance:spot:BTCUSDT",
            timeframe="1m",
            provider="binance",
            provider_revision=PROVIDER_REVISION,
            producer_commit=PRODUCER_COMMIT,
            stack_id=STACK_ID,
            snapshot_id="snap",
            server_time_ms=70_000,
        )
