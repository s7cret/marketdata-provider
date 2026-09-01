from __future__ import annotations

from collections.abc import Mapping
from typing import Any, cast

import pytest
from openpine_contracts import Finality, RevisionState, validate_payload
from openpine_contracts.hashing import seal_content_hash

import marketdata_provider.canonical.envelope as envelope_module
import marketdata_provider.canonical.store_adapter as store_adapter_module
from marketdata_provider.canonical.bar import build_data_snapshot, make_canonical_bar
from marketdata_provider.canonical.envelope import (
    envelope_metadata,
    known_provider_revision,
    normalize_provider_revision,
    validate_artifact_identity,
)
from marketdata_provider.canonical.provider import ProviderRawBar, build_public_snapshot
from marketdata_provider.canonical.store_adapter import validate_canonical_snapshot
from marketdata_provider.contracts import BarQuery, InstrumentKey, parse_timeframe
from marketdata_provider.errors import MDValidationError
from marketdata_provider.factories import _snapshot_source_identity

INSTRUMENT = "binance/spot/BTCUSDT"
PRODUCER_COMMIT = "8cb5bcce24317542a5f8d7a36e2d27fff76d010e"
STACK_ID = "sha256:1111111111111111111111111111111111111111111111111111111111111111"


def _bar(**overrides: object) -> dict[str, object]:
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
        "snapshot_id": "snapshot-1",
        "provider": "binance",
        "provider_revision": {"known": True, "revision": "provider-r1"},
        "producer_commit": PRODUCER_COMMIT,
        "stack_id": STACK_ID,
        "finality": Finality.FINAL,
        "revision_state": RevisionState.ORIGINAL,
        "revision": 0,
    }
    payload.update(overrides)
    return make_canonical_bar(**cast(Any, payload))


def test_make_canonical_bar_returns_sealed_standalone_contract_envelope() -> None:
    candidate = _bar()

    validate_payload("openpine.marketdata.bar.v2", candidate)
    assert candidate["schema_id"] == "openpine.marketdata.bar.v2"
    assert candidate["schema_version"] == "2.1.0"
    assert candidate["producer"] == "marketdata-provider"
    assert candidate["producer_version"] == "5.0.0-rc.6"
    assert candidate["producer_commit"] == PRODUCER_COMMIT
    assert candidate["stack_id"] == STACK_ID
    assert candidate["content_hash"]


def test_canonical_bar_provider_revision_is_typed_known_identity() -> None:
    revision = _bar()["provider_revision"]

    assert isinstance(revision, Mapping)
    assert revision == {"known": True, "revision": "provider-r1"}


def test_corrected_bar_rejects_missing_superseded_hash() -> None:
    with pytest.raises(MDValidationError, match="superseded_bar_hash"):
        _bar(
            revision_state=RevisionState.CORRECTED,
            revision=1,
            close="1.6",
        )


def test_snapshot_is_explicit_sealed_envelope_bundle() -> None:
    result = build_data_snapshot(
        snapshot_id="snapshot-1",
        instrument_id=INSTRUMENT,
        timeframe="1m",
        provider_revision={"known": True, "revision": "provider-r1"},
        producer_commit=PRODUCER_COMMIT,
        stack_id=STACK_ID,
        start_utc_ms=0,
        end_utc_ms=60_000,
        bars=[_bar()],
        clock=lambda: 123,
    )

    envelope = result["snapshot_envelope"]
    validate_payload("openpine.marketdata.v2", envelope)
    assert envelope["kind"] == "snapshot"
    assert result["bars"][0]["snapshot_id"] == envelope["body"]["snapshot_id"]


def test_snapshot_rejects_per_bar_provider_revision_mismatch() -> None:
    with pytest.raises(MDValidationError, match="provider_revision"):
        build_data_snapshot(
            snapshot_id="snapshot-1",
            instrument_id=INSTRUMENT,
            timeframe="1m",
            provider_revision={"known": True, "revision": "provider-r2"},
            producer_commit=PRODUCER_COMMIT,
            stack_id=STACK_ID,
            start_utc_ms=0,
            end_utc_ms=60_000,
            bars=[_bar(provider_revision={"known": True, "revision": "provider-r1"})],
            clock=lambda: 123,
        )


def _snapshot_bundle() -> dict[str, Any]:
    return build_data_snapshot(
        snapshot_id="snapshot-1",
        instrument_id=INSTRUMENT,
        timeframe="1m",
        provider_revision={"known": True, "revision": "provider-r1"},
        producer_commit=PRODUCER_COMMIT,
        stack_id=STACK_ID,
        start_utc_ms=0,
        end_utc_ms=60_000,
        bars=[_bar()],
        clock=lambda: 123,
    )


def test_stage_b_identity_helpers_fail_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(MDValidationError, match="ORIGINAL superseded_bar_hash"):
        _bar(superseded_bar_hash="sha256:" + "3" * 64)
    with pytest.raises(MDValidationError, match="revision is required"):
        known_provider_revision("")
    with pytest.raises(MDValidationError, match="typed identity"):
        normalize_provider_revision("provider-r1")
    with pytest.raises(MDValidationError, match="contain known and revision"):
        normalize_provider_revision({"known": True})
    with pytest.raises(MDValidationError, match="known in production"):
        normalize_provider_revision({"known": False, "revision": None})
    with pytest.raises(MDValidationError, match="producer_commit"):
        validate_artifact_identity(producer_commit="bad", stack_id=STACK_ID)
    with pytest.raises(MDValidationError, match="stack_id"):
        validate_artifact_identity(producer_commit=PRODUCER_COMMIT, stack_id="bad")
    with pytest.raises(MDValidationError, match="created_at_utc_ms"):
        envelope_metadata(
            producer_commit=PRODUCER_COMMIT,
            stack_id=STACK_ID,
            created_at_utc_ms=True,
        )

    monkeypatch.setattr(envelope_module, "verify_content_hash", lambda *_a, **_k: False)
    with pytest.raises(MDValidationError, match="after sealing"):
        _bar()


def test_stage_b_bar_integrity_and_lineage_fail_closed() -> None:
    base = _bar()
    unexpected = dict(base, unexpected=True)
    unexpected.pop("content_hash")
    unexpected = seal_content_hash(unexpected, schema_id="openpine.marketdata.bar.v2")
    with pytest.raises(MDValidationError, match="schema validation"):
        build_data_snapshot(
            snapshot_id="snapshot-1",
            instrument_id=INSTRUMENT,
            timeframe="1m",
            provider_revision={"known": True, "revision": "provider-r1"},
            producer_commit=PRODUCER_COMMIT,
            stack_id=STACK_ID,
            start_utc_ms=0,
            end_utc_ms=60_000,
            bars=[unexpected],
        )

    stale_semantic_hash = dict(base, close="1.6")
    stale_semantic_hash.pop("content_hash")
    stale_semantic_hash = seal_content_hash(
        stale_semantic_hash, schema_id="openpine.marketdata.bar.v2"
    )
    with pytest.raises(MDValidationError, match="bar_content_hash"):
        build_data_snapshot(
            snapshot_id="snapshot-1",
            instrument_id=INSTRUMENT,
            timeframe="1m",
            provider_revision={"known": True, "revision": "provider-r1"},
            producer_commit=PRODUCER_COMMIT,
            stack_id=STACK_ID,
            start_utc_ms=0,
            end_utc_ms=60_000,
            bars=[stale_semantic_hash],
        )

    wrong_hash = "sha256:" + "4" * 64
    orphan = _bar(
        revision_state=RevisionState.CORRECTED,
        revision=1,
        close="1.6",
        superseded_bar_hash=wrong_hash,
    )
    with pytest.raises(MDValidationError, match="missing the preceding"):
        build_data_snapshot(
            snapshot_id="snapshot-1",
            instrument_id=INSTRUMENT,
            timeframe="1m",
            provider_revision={"known": True, "revision": "provider-r1"},
            producer_commit=PRODUCER_COMMIT,
            stack_id=STACK_ID,
            start_utc_ms=0,
            end_utc_ms=60_000,
            bars=[orphan],
        )
    with pytest.raises(MDValidationError, match="immediately preceding"):
        build_data_snapshot(
            snapshot_id="snapshot-1",
            instrument_id=INSTRUMENT,
            timeframe="1m",
            provider_revision={"known": True, "revision": "provider-r1"},
            producer_commit=PRODUCER_COMMIT,
            stack_id=STACK_ID,
            start_utc_ms=0,
            end_utc_ms=60_000,
            bars=[base, orphan],
        )


def test_stage_b_public_provider_and_empty_snapshot_identity_fail_closed() -> None:
    query = BarQuery(
        InstrumentKey("binance", "spot", "BTCUSDT"),
        parse_timeframe("1m"),
        0,
        60_000,
    )
    raw = ProviderRawBar(
        instrument_id=INSTRUMENT,
        timeframe="1m",
        open_time_utc_ms=0,
        close_time_utc_ms=59_999,
        open="1",
        high="2",
        low="0.5",
        close="1.5",
        volume="10",
        finality=Finality.FINAL,
        provider="binance",
        provider_revision="provider-r2",
    )
    with pytest.raises(MDValidationError, match="does not match snapshot"):
        build_public_snapshot(
            query,
            [raw],
            provider_revision={"known": True, "revision": "provider-r1"},
            producer_commit=PRODUCER_COMMIT,
            stack_id=STACK_ID,
        )
    with pytest.raises(MDValidationError, match="empty snapshot"):
        _snapshot_source_identity(query, [], default_provider="binance")


def test_stage_b_store_bundle_validation_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    valid = _snapshot_bundle()
    envelope = valid["snapshot_envelope"]

    schema_invalid = dict(valid, snapshot_envelope=dict(envelope, unexpected=True))
    with pytest.raises(MDValidationError, match="envelope validation"):
        validate_canonical_snapshot(schema_invalid)

    wrong_root = dict(envelope, content_hash="sha256:" + "5" * 64)
    with pytest.raises(MDValidationError, match="content_hash is invalid"):
        validate_canonical_snapshot(dict(valid, snapshot_envelope=wrong_root))

    other_kind = {
        key: value
        for key, value in envelope.items()
        if key not in {"kind", "body", "content_hash"}
    }
    other_kind.update(kind="provider_revision", body={"revision": "provider-r1"})
    other_kind = seal_content_hash(other_kind, schema_id="openpine.marketdata.v2")
    with pytest.raises(MDValidationError, match="kind/body"):
        validate_canonical_snapshot(dict(valid, snapshot_envelope=other_kind))

    with monkeypatch.context() as scoped:
        scoped.setattr(store_adapter_module, "validate_payload", lambda *_a, **_k: None)
        scoped.setattr(
            store_adapter_module, "verify_content_hash", lambda *_a, **_k: True
        )
        malformed_body = dict(envelope["body"], query="bad")
        malformed_envelope = dict(envelope, body=malformed_body)
        with pytest.raises(MDValidationError, match="envelope query"):
            validate_canonical_snapshot(
                dict(valid, snapshot_envelope=malformed_envelope)
            )

    bad_revision = dict(valid)
    bad_revision["query"] = dict(
        valid["query"],
        provider_revision={"known": True, "revision": "other"},
    )
    with pytest.raises(MDValidationError, match="provider_revision"):
        validate_canonical_snapshot(bad_revision)
    with pytest.raises(MDValidationError, match="snapshot_id"):
        validate_canonical_snapshot(dict(valid, snapshot_id="other"))
    with pytest.raises(MDValidationError, match="series_hash"):
        validate_canonical_snapshot(dict(valid, series_hash="sha256:" + "6" * 64))
    with pytest.raises(MDValidationError, match="created_at"):
        validate_canonical_snapshot(dict(valid, created_at_utc_ms=124))

    with monkeypatch.context() as scoped:
        scoped.setattr(
            store_adapter_module,
            "build_data_snapshot",
            lambda **_kwargs: {"snapshot_envelope": {}},
        )
        with pytest.raises(MDValidationError, match="reconstruction"):
            validate_canonical_snapshot(valid)
