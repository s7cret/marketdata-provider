from __future__ import annotations

import json
import time
from pathlib import Path

import pytest
from openpine_contracts import Finality, RevisionState

import marketdata_provider.canonical.bar as canonical_bar_module
from marketdata_provider import __version__
from marketdata_provider.canonical.bar import build_data_snapshot, make_canonical_bar
from marketdata_provider.errors import MDTimeframeUnsupported, MDValidationError
from marketdata_provider.release import EXPECTED_VERSION
from marketdata_provider.timeframes import close_time_ms

INSTRUMENT = "binance:spot:BTCUSDT"


def bar(*, snapshot_id: str = "snap-a", **overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "instrument_id": INSTRUMENT,
        "timeframe": "1m",
        "open_time_utc_ms": 0,
        "close_time_utc_ms": 59_999,
        "open": "1",
        "high": "2",
        "low": "0.5",
        "close": "1.5",
        "volume": "10",
        "snapshot_id": snapshot_id,
        "provider": "binance",
        "provider_revision": "binance-rest-v1",
        "finality": Finality.FINAL,
        "revision_state": RevisionState.ORIGINAL,
        "revision": 0,
    }
    payload.update(overrides)
    return make_canonical_bar(**payload)


def snapshot(
    instance_id: str,
    bars: list[dict[str, object]],
    **overrides: object,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "snapshot_id": instance_id,
        "instrument_id": INSTRUMENT,
        "timeframe": "1m",
        "provider_revision": "binance-rest-v1",
        "start_utc_ms": 0,
        "end_utc_ms": 60_000,
        "bars": bars,
        "clock": lambda: 123,
    }
    payload.update(overrides)
    return build_data_snapshot(**payload)


def test_bar_content_hash_excludes_snapshot_instance_metadata() -> None:
    first = bar(snapshot_id="snap-a")
    second = bar(snapshot_id="snap-b")
    second.update(
        observed_at_utc_ms=100,
        ingested_at_utc_ms=200,
        storage_location="cache/b",
    )

    assert first["bar_content_hash"] == second["bar_content_hash"]


def test_provider_revision_is_required_and_changes_content_identity() -> None:
    first = bar(provider_revision="binance-rest-v1")
    second = bar(provider_revision="binance-rest-v2")

    assert first["bar_content_hash"] != second["bar_content_hash"]
    with pytest.raises(MDValidationError, match="provider_revision"):
        bar(provider_revision=None)
    with pytest.raises(MDValidationError, match="provider_revision"):
        snapshot("snap-a", [second], provider_revision="binance-rest-v1")


def test_series_hash_is_stable_across_snapshot_ids_and_creation_times() -> None:
    first = snapshot("snap-a", [bar(snapshot_id="snap-a")], clock=lambda: 111)
    second = snapshot("snap-b", [bar(snapshot_id="snap-b")], clock=lambda: 222)

    assert first["created_at_utc_ms"] == 111
    assert second["created_at_utc_ms"] == 222
    assert first["series_hash"] == second["series_hash"]


def test_json_roundtripped_enums_are_normalized_by_value() -> None:
    candidate = json.loads(json.dumps(bar()))

    result = snapshot("snap-json", [candidate])

    assert result["bars"][0]["finality"] is Finality.FINAL
    assert result["bars"][0]["revision_state"] is RevisionState.ORIGINAL


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("finality", "MAYBE"),
        ("finality", True),
        ("revision_state", "UNKNOWN"),
        ("revision_state", False),
    ],
)
def test_unknown_and_boolean_enum_values_are_rejected(
    field: str, value: object
) -> None:
    payload = bar()
    payload[field] = value

    with pytest.raises(MDValidationError, match=field):
        snapshot("snap-a", [payload], finality_policy="ALLOW_OPEN")


def test_missing_finality_raises_specific_typed_error() -> None:
    candidate = bar()
    candidate.pop("finality")

    with pytest.raises(MDValidationError) as exc_info:
        snapshot("snap-a", [candidate])

    assert type(exc_info.value) is not MDValidationError
    assert exc_info.value.code == "MD_MISSING_FINALITY"


@pytest.mark.parametrize("field", ["bar_content_hash", "provider", "revision"])
def test_snapshot_rejects_missing_required_bar_fields(field: str) -> None:
    candidate = bar()
    candidate.pop(field)

    with pytest.raises(MDValidationError, match=field):
        snapshot("snap-a", [candidate])


@pytest.mark.parametrize(
    ("overrides", "match"),
    [
        ({"snapshot_id": ""}, "snapshot_id"),
        ({"instrument_id": ""}, "instrument_id"),
        ({"timeframe": ""}, "timeframe"),
        ({"start_utc_ms": True}, "start_utc_ms"),
        ({"start_utc_ms": 60_000, "end_utc_ms": 60_000}, "range"),
    ],
)
def test_snapshot_rejects_invalid_required_query_fields(
    overrides: dict[str, object], match: str
) -> None:
    with pytest.raises(MDValidationError, match=match):
        snapshot("snap-a", [], **overrides)


@pytest.mark.parametrize(
    ("field", "value"),
    [("instrument_id", "bybit:linear:BTCUSDT"), ("timeframe", "5m")],
)
def test_snapshot_rejects_bar_series_identity_mismatch(field: str, value: str) -> None:
    overrides: dict[str, object] = {field: value}
    if field == "timeframe":
        overrides["close_time_utc_ms"] = 299_999
    candidate = bar(**overrides)
    with pytest.raises(MDValidationError, match=field):
        snapshot("snap-a", [candidate], end_utc_ms=300_000)


def test_snapshot_rejects_out_of_range_and_nonmonotonic_bars() -> None:
    outside = bar(open_time_utc_ms=60_000, close_time_utc_ms=119_999)
    with pytest.raises(MDValidationError, match="range"):
        snapshot("snap-a", [outside])

    later = bar(open_time_utc_ms=60_000, close_time_utc_ms=119_999)
    with pytest.raises(MDValidationError, match="monotonic"):
        snapshot("snap-a", [later, bar()], end_utc_ms=120_000)


def test_exact_duplicates_are_deduplicated_explicitly_and_deterministically() -> None:
    first = bar(snapshot_id="source-a")
    duplicate = bar(snapshot_id="source-b")

    singleton = snapshot("single", [first])
    result = snapshot("duplicate", [first, duplicate])

    assert result["bar_count"] == 1
    assert result["series_hash"] == singleton["series_hash"]
    assert result["duplicates"] == [
        {
            "open_time_utc_ms": 0,
            "revision": 0,
            "count": 2,
            "bar_content_hash": first["bar_content_hash"],
        }
    ]
    assert result["conflicts"] == []


def test_same_revision_content_conflict_raises_typed_error_with_metadata() -> None:
    with pytest.raises(MDValidationError) as exc_info:
        snapshot("snap-a", [bar(), bar(close="1.6")])

    assert type(exc_info.value) is not MDValidationError
    assert exc_info.value.code == "MD_BAR_CONFLICT"
    assert exc_info.value.details["conflicts"][0]["open_time_utc_ms"] == 0


def test_revision_chain_selects_latest_correction_and_applies_revocation() -> None:
    original = bar()
    corrected = bar(
        close="1.6",
        revision_state=RevisionState.CORRECTED,
        revision=1,
    )
    selected = snapshot("corrected", [original, corrected])

    assert selected["bars"] == [corrected]
    assert selected["revision_chains"][0]["selected_revision"] == 1

    revoked = bar(revision_state=RevisionState.REVOKED, revision=2)
    removed = snapshot("revoked", [original, corrected, revoked])
    assert removed["bar_count"] == 0
    assert removed["revision_chains"][0]["revoked"] is True


@pytest.mark.parametrize(
    ("revision_state", "revision"),
    [
        (RevisionState.ORIGINAL, 1),
        (RevisionState.CORRECTED, 0),
        (RevisionState.REVOKED, 0),
        (RevisionState.ORIGINAL, True),
    ],
)
def test_bar_rejects_incoherent_or_boolean_revision(
    revision_state: RevisionState, revision: object
) -> None:
    with pytest.raises(MDValidationError, match="revision"):
        bar(revision_state=revision_state, revision=revision)


def test_snapshot_rejects_invalid_revision_chain_order() -> None:
    corrected = bar(
        close="1.6",
        revision_state=RevisionState.CORRECTED,
        revision=2,
    )
    stale = bar(
        close="1.55",
        revision_state=RevisionState.CORRECTED,
        revision=1,
    )

    with pytest.raises(MDValidationError) as exc_info:
        snapshot("snap-a", [corrected, stale])

    assert exc_info.value.code == "MD_BAR_CONFLICT"


def test_snapshot_recomputes_and_rejects_tampered_bar_hash() -> None:
    candidate = bar()
    candidate["close"] = "1.6"

    with pytest.raises(MDValidationError, match="bar_content_hash"):
        snapshot("snap-a", [candidate])


def test_created_at_uses_real_time_by_default_and_supports_injected_clock() -> None:
    before_ms = time.time_ns() // 1_000_000
    real = build_data_snapshot(
        snapshot_id="real",
        instrument_id=INSTRUMENT,
        timeframe="1m",
        provider_revision="binance-rest-v1",
        start_utc_ms=0,
        end_utc_ms=60_000,
        bars=[],
    )
    after_ms = time.time_ns() // 1_000_000
    fixed = snapshot("fixed", [], clock=lambda: 987_654_321)

    assert before_ms <= real["created_at_utc_ms"] <= after_ms
    assert fixed["created_at_utc_ms"] == 987_654_321


def test_snapshot_reports_coverage_gaps_and_conflicts_metadata() -> None:
    first = bar()
    third = bar(open_time_utc_ms=120_000, close_time_utc_ms=179_999)

    result = snapshot("gapped", [first, third], end_utc_ms=180_000)

    assert result["gaps"] == [{"start_utc_ms": 60_000, "end_utc_ms": 120_000}]
    assert result["conflicts"] == []
    assert result["coverage"] == {
        "requested_start_utc_ms": 0,
        "requested_end_utc_ms": 180_000,
        "covered_start_utc_ms": 0,
        "covered_end_utc_ms": 180_000,
        "bar_count": 2,
        "gap_count": 1,
        "complete": False,
    }


def test_closed_bar_only_is_default_and_excludes_open_bars() -> None:
    final = bar()
    open_bar = bar(
        open_time_utc_ms=60_000,
        close_time_utc_ms=119_999,
        finality=Finality.OPEN,
    )

    result = snapshot("closed", [final, open_bar], end_utc_ms=120_000)

    assert result["query"]["finality_policy"] == "CLOSED_BAR_ONLY"
    assert result["bars"] == [final]


def test_canonical_bar_uses_the_shared_close_time_helper() -> None:
    expected = close_time_ms(0, "1D")
    candidate = bar(timeframe="1D", close_time_utc_ms=None)

    assert candidate["close_time_utc_ms"] == expected
    with pytest.raises(MDValidationError, match="close_time_utc_ms"):
        bar(close_time_utc_ms=59_998)
    with pytest.raises(MDValidationError, match="canonical timeframe"):
        bar(timeframe="1d", close_time_utc_ms=86_399_999)
    with pytest.raises(MDValidationError, match="unsupported timeframe"):
        bar(timeframe="7m", close_time_utc_ms=419_999)


@pytest.mark.parametrize("field", ["open", "high", "low", "close", "volume"])
def test_decimal_contract_boundary_rejects_float(field: str) -> None:
    with pytest.raises(MDValidationError, match="float"):
        bar(**{field: 1.25})


def test_release_identity_and_contract_dependency_are_rc3_publishable() -> None:
    project = Path("pyproject.toml").read_text(encoding="utf-8")

    assert __version__ == EXPECTED_VERSION == "5.0.0rc3"
    assert 'version = "5.0.0rc3"' in project
    assert '"openpine-contracts==5.0.0rc3"' in project
    assert "git+" not in project


def test_canonical_boundary_defensive_type_branches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unsupported(_value: str) -> str:
        raise MDTimeframeUnsupported("unsupported")

    monkeypatch.setattr(canonical_bar_module, "canonical_timeframe", unsupported)
    with pytest.raises(MDValidationError, match="unsupported timeframe"):
        bar()
    monkeypatch.undo()

    invalid_finality = bar()
    invalid_finality["finality"] = object()
    with pytest.raises(MDValidationError, match="Finality"):
        snapshot("bad-finality", [invalid_finality], finality_policy="ALLOW_OPEN")

    invalid_revision = bar()
    invalid_revision["revision_state"] = object()
    with pytest.raises(MDValidationError, match="RevisionState"):
        snapshot("bad-revision", [invalid_revision])

    with pytest.raises(MDValidationError, match="mapping"):
        snapshot("bad-type", [object()])  # type: ignore[list-item]


def test_snapshot_rejects_provider_switch_revocation_tail_and_overlap() -> None:
    provider_switch = bar(
        provider="bybit",
        revision_state=RevisionState.CORRECTED,
        revision=1,
        close="1.6",
    )
    with pytest.raises(MDValidationError, match="provider changed"):
        snapshot("provider-switch", [bar(), provider_switch])

    revoked = bar(revision_state=RevisionState.REVOKED, revision=1)
    after_revoke = bar(
        revision_state=RevisionState.CORRECTED,
        revision=2,
        close="1.6",
    )
    with pytest.raises(MDValidationError, match="terminal revocation"):
        snapshot("revocation-tail", [bar(), revoked, after_revoke])

    overlapping = bar(open_time_utc_ms=30_000, close_time_utc_ms=89_999)
    with pytest.raises(MDValidationError, match="overlap"):
        snapshot("overlap", [bar(), overlapping], end_utc_ms=90_000)
