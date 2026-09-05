from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from openpine_contracts import Finality, RevisionState
from openpine_contracts.hashing import content_hash

from marketdata_provider.canonical.bar import (
    DataSnapshotV2,
    build_data_snapshot,
    make_canonical_bar,
)
from marketdata_provider.canonical.envelope import (
    known_provider_revision,
    normalize_provider_revision,
)
from marketdata_provider.canonical.source_identity import snapshot_revision_identity
from marketdata_provider.compat.v4 import finality_from_closed
from marketdata_provider.contracts.query import BarQuery
from marketdata_provider.core.bar import MarketBar
from marketdata_provider.errors import MDMissingFinality, MDValidationError

_SNAPSHOT_ID_SCHEMA = "marketdata-provider.snapshot-id.v1"



@dataclass(frozen=True, slots=True)
class ProviderRawBar:
    instrument_id: str
    timeframe: str
    open_time_utc_ms: int
    close_time_utc_ms: int | None
    open: object
    high: object
    low: object
    close: object
    volume: object
    finality: Finality
    provider: str
    provider_revision: str
    revision_state: RevisionState = RevisionState.ORIGINAL
    revision: int = 0


def raw_bar_from_market_bar(
    bar: MarketBar,
    *,
    instrument_id: str,
    timeframe: str,
    provider: str,
) -> ProviderRawBar:
    if bar.is_closed is None:
        raise MDMissingFinality("stored/provider bar is missing finality")
    if bar.time_close is None:
        raise MDValidationError("stored/provider bar is missing close_time_utc_ms")
    source_provider = bar.provider or provider
    source_revision = bar.provider_revision
    if not source_provider:
        raise MDValidationError("provider is required")
    if not source_revision:
        raise MDValidationError("provider_revision is required")
    decimal_text = {
        "open": bar.open_text,
        "high": bar.high_text,
        "low": bar.low_text,
        "close": bar.close_text,
        "volume": bar.volume_text,
    }
    missing_text = [
        name
        for name, value in decimal_text.items()
        if not isinstance(value, str) or not value
    ]
    if missing_text:
        raise MDValidationError(
            "stored/provider bar is missing exact source decimal text: "
            + ", ".join(missing_text)
        )
    return ProviderRawBar(
        instrument_id=instrument_id,
        timeframe=timeframe,
        open_time_utc_ms=bar.time,
        close_time_utc_ms=bar.time_close,
        open=decimal_text["open"],
        high=decimal_text["high"],
        low=decimal_text["low"],
        close=decimal_text["close"],
        volume=decimal_text["volume"],
        finality=finality_from_closed(bar.is_closed),
        provider=source_provider,
        provider_revision=source_revision,
        revision_state=bar.revision_state,
        revision=bar.revision,
    )


def snapshot_source_identity(
    query: BarQuery,
    bars: list[MarketBar],
    *,
    default_provider: str,
) -> tuple[str, str]:
    del query
    providers = {bar.provider for bar in bars if bar.provider}
    if len(providers) > 1 or (providers and providers != {default_provider}):
        raise MDValidationError("stored bars disagree on provider identity")
    provider = next(iter(providers), default_provider)
    if not bars:
        raise MDValidationError(
            "provider_revision is unavailable for an empty snapshot"
        )
    if any(bar.provider_revision is None for bar in bars):
        raise MDValidationError("stored bars have partial provider_revision identity")
    revisions = {str(bar.provider_revision) for bar in bars}
    if len(revisions) == 1:
        return provider, next(iter(revisions))
    return provider, snapshot_revision_identity(
        provider,
        [(bar.time, bar.revision, str(bar.provider_revision)) for bar in bars],
    )


def build_public_snapshot(
    query: BarQuery,
    raw_bars: list[ProviderRawBar],
    *,
    provider_revision: object,
    producer_commit: str,
    stack_id: str,
    finality_policy: str = "CLOSED_BAR_ONLY",
    schema_validate: bool = True,
) -> DataSnapshotV2:
    expected_revision = normalize_provider_revision(provider_revision)
    ordered = sorted(raw_bars, key=lambda item: (item.open_time_utc_ms, item.revision))
    providers = {item.provider for item in ordered if item.provider}
    if ordered:
        if len(providers) != 1:
            raise MDValidationError("snapshot bars must share one provider")
        actual_revision = snapshot_revision_identity(
            next(iter(providers)),
            [
                (item.open_time_utc_ms, item.revision, item.provider_revision)
                for item in ordered
            ],
        )
        if actual_revision != expected_revision["revision"]:
            raise MDValidationError("raw bar provider_revision does not match snapshot")

    def canonicalize(snapshot_id: str) -> list[dict[str, Any]]:
        canonical: list[dict[str, Any]] = []
        previous_open: int | None = None
        previous_hash: str | None = None
        for item in ordered:
            if item.open_time_utc_ms != previous_open:
                previous_open = item.open_time_utc_ms
                previous_hash = None
            bar = make_canonical_bar(
                instrument_id=item.instrument_id,
                timeframe=item.timeframe,
                open_time_utc_ms=item.open_time_utc_ms,
                close_time_utc_ms=item.close_time_utc_ms,
                open=item.open,
                high=item.high,
                low=item.low,
                close=item.close,
                volume=item.volume,
                snapshot_id=snapshot_id,
                provider=item.provider,
                provider_revision=known_provider_revision(item.provider_revision),
                producer_commit=producer_commit,
                stack_id=stack_id,
                created_at_utc_ms=query.end_ms,
                superseded_bar_hash=(
                    previous_hash
                    if item.revision_state is not RevisionState.ORIGINAL
                    else None
                ),
                finality=item.finality,
                revision_state=item.revision_state,
                revision=item.revision,
                schema_validate=schema_validate,
            )
            canonical.append(bar)
            previous_hash = str(bar["bar_content_hash"])
        return canonical

    pending_bars = canonicalize("pending")
    identity: dict[str, Any] = {
        "instrument_id": query.instrument.serialize(),
        "timeframe": query.timeframe.canonical,
        "start_utc_ms": query.start_ms,
        "end_utc_ms": query.end_ms,
        "provider_revision": expected_revision,
        "finality_policy": finality_policy,
        "ordered_input_hashes": [bar["bar_content_hash"] for bar in pending_bars],
    }
    snapshot_id = content_hash(identity, schema_id=_SNAPSHOT_ID_SCHEMA)
    bars = canonicalize(snapshot_id)
    return build_data_snapshot(
        snapshot_id=snapshot_id,
        instrument_id=query.instrument.serialize(),
        timeframe=query.timeframe.canonical,
        provider_revision=expected_revision,
        producer_commit=producer_commit,
        stack_id=stack_id,
        start_utc_ms=query.start_ms,
        end_utc_ms=query.end_ms,
        bars=bars,
        finality_policy=finality_policy,
        clock=lambda: query.end_ms,
        schema_validate=schema_validate,
    )


def snapshot_from_market_bars(
    query: BarQuery,
    bars: list[MarketBar],
    *,
    provider: str,
    provider_revision: object,
    producer_commit: str,
    stack_id: str,
    finality_policy: str = "CLOSED_BAR_ONLY",
    schema_validate: bool = True,
) -> DataSnapshotV2:
    raw = [
        raw_bar_from_market_bar(
            bar,
            instrument_id=query.instrument.serialize(),
            timeframe=query.timeframe.canonical,
            provider=provider,
        )
        for bar in bars
    ]
    return build_public_snapshot(
        query,
        raw,
        provider_revision=provider_revision,
        producer_commit=producer_commit,
        stack_id=stack_id,
        finality_policy=finality_policy,
        schema_validate=schema_validate,
    )
