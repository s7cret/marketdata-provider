from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from openpine_contracts import Finality, RevisionState
from openpine_contracts.hashing import content_hash

from marketdata_provider.canonical.bar import (
    DataSnapshotV2,
    build_data_snapshot,
    make_canonical_bar,
)
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


def _decimal_from_legacy_number(value: object) -> object:
    if isinstance(value, float):
        return Decimal(str(value))
    return value


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
    return ProviderRawBar(
        instrument_id=instrument_id,
        timeframe=timeframe,
        open_time_utc_ms=bar.time,
        close_time_utc_ms=bar.time_close,
        open=(
            bar.open_text
            if bar.open_text is not None
            else _decimal_from_legacy_number(bar.open)
        ),
        high=(
            bar.high_text
            if bar.high_text is not None
            else _decimal_from_legacy_number(bar.high)
        ),
        low=(
            bar.low_text
            if bar.low_text is not None
            else _decimal_from_legacy_number(bar.low)
        ),
        close=(
            bar.close_text
            if bar.close_text is not None
            else _decimal_from_legacy_number(bar.close)
        ),
        volume=(
            bar.volume_text
            if bar.volume_text is not None
            else _decimal_from_legacy_number(bar.volume)
        ),
        finality=finality_from_closed(bar.is_closed),
        provider=source_provider,
        provider_revision=source_revision,
        revision_state=bar.revision_state,
        revision=bar.revision,
    )


def build_public_snapshot(
    query: BarQuery,
    raw_bars: list[ProviderRawBar],
    *,
    provider_revision: str,
    finality_policy: str = "CLOSED_BAR_ONLY",
) -> DataSnapshotV2:
    if not provider_revision:
        raise MDValidationError("provider_revision is required")
    ordered = sorted(raw_bars, key=lambda item: (item.open_time_utc_ms, item.revision))
    pending_bars = [
        make_canonical_bar(
            instrument_id=item.instrument_id,
            timeframe=item.timeframe,
            open_time_utc_ms=item.open_time_utc_ms,
            close_time_utc_ms=item.close_time_utc_ms,
            open=item.open,
            high=item.high,
            low=item.low,
            close=item.close,
            volume=item.volume,
            snapshot_id="pending",
            provider=item.provider,
            provider_revision=item.provider_revision,
            finality=item.finality,
            revision_state=item.revision_state,
            revision=item.revision,
        )
        for item in ordered
    ]
    identity: dict[str, Any] = {
        "instrument_id": query.instrument.serialize(),
        "timeframe": query.timeframe.canonical,
        "start_utc_ms": query.start_ms,
        "end_utc_ms": query.end_ms,
        "provider_revision": provider_revision,
        "finality_policy": finality_policy,
        "ordered_input_hashes": [bar["bar_content_hash"] for bar in pending_bars],
    }
    snapshot_id = content_hash(identity, schema_id=_SNAPSHOT_ID_SCHEMA)
    bars = [dict(bar, snapshot_id=snapshot_id) for bar in pending_bars]
    return build_data_snapshot(
        snapshot_id=snapshot_id,
        instrument_id=query.instrument.serialize(),
        timeframe=query.timeframe.canonical,
        provider_revision=provider_revision,
        start_utc_ms=query.start_ms,
        end_utc_ms=query.end_ms,
        bars=bars,
        finality_policy=finality_policy,
        clock=lambda: query.end_ms,
    )


def snapshot_from_market_bars(
    query: BarQuery,
    bars: list[MarketBar],
    *,
    provider: str,
    provider_revision: str,
    finality_policy: str = "CLOSED_BAR_ONLY",
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
        finality_policy=finality_policy,
    )
