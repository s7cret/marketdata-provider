from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from openpine_contracts import Finality, RevisionState, validate_payload
from openpine_contracts.hashing import verify_content_hash

from marketdata_provider.canonical.bar import DataSnapshotV2, build_data_snapshot
from marketdata_provider.canonical.envelope import (
    SNAPSHOT_SCHEMA_ID,
    normalize_provider_revision,
)
from marketdata_provider.contracts.instrument import InstrumentKey
from marketdata_provider.core.bar import MarketBar
from marketdata_provider.errors import MDValidationError


def validate_canonical_snapshot(snapshot: Mapping[str, Any]) -> DataSnapshotV2:
    query = snapshot.get("query")
    bars = snapshot.get("bars")
    envelope = snapshot.get("snapshot_envelope")
    if (
        not isinstance(query, Mapping)
        or not isinstance(bars, list)
        or not isinstance(envelope, Mapping)
    ):
        raise MDValidationError(
            "canonical snapshot query/bars/snapshot_envelope are required"
        )
    try:
        validate_payload(SNAPSHOT_SCHEMA_ID, envelope)
    except Exception as exc:
        raise MDValidationError(
            "canonical snapshot envelope validation failed"
        ) from exc
    if not verify_content_hash(envelope, schema_id=SNAPSHOT_SCHEMA_ID):
        raise MDValidationError("canonical snapshot envelope content_hash is invalid")
    if envelope.get("kind") != "snapshot" or not isinstance(
        envelope.get("body"), Mapping
    ):
        raise MDValidationError("canonical snapshot envelope kind/body is invalid")
    body = envelope["body"]
    body_query = body.get("query")
    if not isinstance(body_query, Mapping):
        raise MDValidationError("canonical snapshot envelope query is invalid")
    created_at = snapshot.get("created_at_utc_ms")
    if isinstance(created_at, bool) or not isinstance(created_at, int):
        raise MDValidationError("canonical snapshot created_at_utc_ms is required")
    provider_revision = normalize_provider_revision(body.get("provider_revision"))
    if query.get("provider_revision") != provider_revision:
        raise MDValidationError(
            "canonical snapshot provider_revision does not match envelope"
        )
    if snapshot.get("snapshot_id") != body.get("snapshot_id"):
        raise MDValidationError(
            "canonical snapshot snapshot_id does not match envelope"
        )
    if snapshot.get("series_hash") != body.get("series_hash"):
        raise MDValidationError(
            "canonical snapshot series_hash does not match envelope"
        )
    if created_at != body.get("created_at_utc_ms"):
        raise MDValidationError("canonical snapshot created_at does not match envelope")

    validated = build_data_snapshot(
        snapshot_id=str(body["snapshot_id"]),
        instrument_id=str(body_query["instrument_id"]),
        timeframe=str(body_query["timeframe"]),
        provider_revision=provider_revision,
        producer_commit=str(envelope["producer_commit"]),
        stack_id=str(envelope["stack_id"]),
        start_utc_ms=int(body_query["start_utc_ms"]),
        end_utc_ms=int(body_query["end_utc_ms"]),
        bars=bars,
        finality_policy=str(body_query["finality_policy"]),
        clock=lambda: created_at,
    )
    if validated["snapshot_envelope"] != dict(envelope):
        raise MDValidationError("canonical snapshot envelope reconstruction failed")
    return validated


def market_bar_from_canonical(bar: Mapping[str, Any]) -> MarketBar:
    instrument = InstrumentKey.parse(str(bar["instrument_id"]))
    finality = Finality(str(bar["finality"]))
    revision_state = RevisionState(str(bar["revision_state"]))
    provider_revision = normalize_provider_revision(bar["provider_revision"])
    return MarketBar(
        time=int(bar["open_time_utc_ms"]),
        time_close=int(bar["close_time_utc_ms"]),
        open=float(str(bar["open"])),
        high=float(str(bar["high"])),
        low=float(str(bar["low"])),
        close=float(str(bar["close"])),
        volume=float(str(bar["volume"])),
        exchange=instrument.exchange,
        market=instrument.market,
        symbol=instrument.symbol,
        timeframe=str(bar["timeframe"]),
        source_transport="canonical",
        is_closed=finality is Finality.FINAL,
        provider=str(bar["provider"]),
        provider_revision=provider_revision["revision"],
        revision_state=revision_state,
        revision=int(bar["revision"]),
        open_text=str(bar["open"]),
        high_text=str(bar["high"]),
        low_text=str(bar["low"]),
        close_text=str(bar["close"]),
        volume_text=str(bar["volume"]),
    )
