"""OP-04: an execution boundary must not silently rewrite canonical bars."""

from __future__ import annotations

import pytest
from openpine_contracts import Finality, RevisionState, seal_content_hash

from marketdata_provider.canonical.admission import (
    admit_canonical_bars,
    validate_canonical_bar,
)
from marketdata_provider.canonical.bar import make_canonical_bar
from marketdata_provider.errors import MDBarConflict, MDValidationError


def bar(index=0, **changes):
    args = {
        "instrument_id": "BINANCE:spot:BTCUSDT",
        "timeframe": "1m",
        "open_time_utc_ms": index * 60000,
        "close_time_utc_ms": (index + 1) * 60000 - 1,
        "open": "10",
        "high": "12",
        "low": "9",
        "close": "11",
        "volume": "0",
        "finality": Finality.FINAL,
        "revision_state": RevisionState.ORIGINAL,
        "revision": 0,
        "provider": "test",
        "provider_revision": {"known": True, "revision": "test-r1"},
        "snapshot_id": "snapshot-1",
        "producer_commit": "1" * 40,
        "stack_id": "sha256:" + "2" * 64,
        "created_at_utc_ms": 0,
    }
    args.update(changes)
    return make_canonical_bar(**args)


def test_open_is_not_final_and_zero_volume_is_preserved():
    raw = bar(finality=Finality.OPEN)
    assert validate_canonical_bar(raw)["finality"] is Finality.OPEN
    assert list(admit_canonical_bars([raw])) == []
    assert (
        next(iter(admit_canonical_bars([raw], finality_policy="ALLOW_OPEN")))["volume"]
        == "0"
    )


def test_correction_replaces_original_and_revocation_removes_timestamp():
    original = bar()
    corrected = bar(
        revision_state=RevisionState.CORRECTED,
        revision=1,
        close="12",
        superseded_bar_hash=original["bar_content_hash"],
    )
    revoked = bar(
        revision_state=RevisionState.REVOKED,
        revision=2,
        close="12",
        superseded_bar_hash=corrected["bar_content_hash"],
    )
    assert list(admit_canonical_bars([original, corrected])) == [corrected]
    assert list(admit_canonical_bars([original, corrected, revoked, bar(1)])) == [
        bar(1)
    ]


def test_duplicate_same_content_is_coalesced():
    raw = bar()
    assert list(admit_canonical_bars([raw, raw])) == [raw]


@pytest.mark.parametrize(
    "key,value",
    [
        ("instrument_id", "OTHER"),
        ("timeframe", "5m"),
        ("snapshot_id", "different"),
        ("provider_revision", "other"),
    ],
)
def test_bound_identity_rejects_cross_run_mix(key, value):
    with pytest.raises(MDValidationError, match=key):
        validate_canonical_bar(bar(), expected={key: value})


def test_resealed_outer_hash_does_not_hide_invalid_bar_identity():
    raw = bar()
    raw["bar_content_hash"] = "sha256:" + "9" * 64
    raw = seal_content_hash(raw, schema_id="openpine.marketdata.bar.v2")
    with pytest.raises(MDValidationError, match="bar_content_hash"):
        validate_canonical_bar(raw)


def test_out_of_order_snapshot_mix_and_orphan_correction_fail():
    with pytest.raises(MDValidationError, match="ordered"):
        list(admit_canonical_bars([bar(1), bar()]))
    with pytest.raises(MDValidationError, match="snapshot_id"):
        list(admit_canonical_bars([bar(), bar(1, snapshot_id="another")]))
    with pytest.raises(MDBarConflict, match="preceding"):
        list(
            admit_canonical_bars(
                [
                    bar(
                        revision_state=RevisionState.CORRECTED,
                        revision=1,
                        superseded_bar_hash="sha256:" + "9" * 64,
                    )
                ]
            )
        )


def test_conflicting_duplicate_revision_fails():
    with pytest.raises(MDBarConflict, match="same revision"):
        list(admit_canonical_bars([bar(), bar(close="12")]))


def test_revision_resolver_rejects_empty_group():
    from marketdata_provider.canonical.revisions import resolve_bar_revisions
    from marketdata_provider.errors import MDValidationError
    import pytest

    with pytest.raises(MDValidationError, match="must not be empty"):
        resolve_bar_revisions([])
