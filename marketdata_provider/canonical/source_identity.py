from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from typing import Any

from openpine_contracts.hashing import content_hash

from marketdata_provider.contracts.query import BarQuery
from marketdata_provider.core.bar import MarketBar
from marketdata_provider.errors import MDValidationError

_SNAPSHOT_REVISION_SCHEMA = "marketdata-provider.snapshot-revision.v1"


def snapshot_revision_identity(
    provider: str,
    bars: list[tuple[int, int, str]],
) -> str:
    """Return one exact revision for a uniform or incremental snapshot."""

    if not provider:
        raise MDValidationError("provider is required")
    if not bars:
        raise MDValidationError(
            "provider_revision is unavailable for an empty snapshot"
        )
    ordered = sorted(bars, key=lambda item: (item[0], item[1], item[2]))
    revisions = {provider_revision for _, _, provider_revision in ordered}
    if "" in revisions:
        raise MDValidationError("provider_revision is required")
    if len(revisions) == 1:
        return next(iter(revisions))
    return content_hash(
        {
            "provider": provider,
            "bars": [
                {
                    "open_time_utc_ms": open_time,
                    "revision": revision,
                    "provider_revision": provider_revision,
                }
                for open_time, revision, provider_revision in ordered
            ],
        },
        schema_id=_SNAPSHOT_REVISION_SCHEMA,
    )


def _bar_source_revision(
    bar: MarketBar,
    *,
    query: BarQuery,
    provider: str,
    source_transport: str,
) -> str:
    return content_hash(
        {
            "provider": provider,
            "source_transport": source_transport,
            "instrument_id": query.instrument.serialize(),
            "timeframe": query.timeframe.canonical,
            "time": bar.time,
            "time_close": bar.time_close,
            "open": bar.open_text or str(bar.open),
            "high": bar.high_text or str(bar.high),
            "low": bar.low_text or str(bar.low),
            "close": bar.close_text or str(bar.close),
            "volume": bar.volume_text or str(bar.volume),
            "quote_volume": (
                None if bar.quote_volume is None else str(bar.quote_volume)
            ),
            "turnover": None if bar.turnover is None else str(bar.turnover),
            "trades_count": bar.trades_count,
            "taker_buy_base_volume": (
                None
                if bar.taker_buy_base_volume is None
                else str(bar.taker_buy_base_volume)
            ),
            "taker_buy_quote_volume": (
                None
                if bar.taker_buy_quote_volume is None
                else str(bar.taker_buy_quote_volume)
            ),
            "source_kind": bar.source_kind,
            "is_closed": bar.is_closed,
            "revision_state": bar.revision_state.value,
            "revision": bar.revision,
        },
        schema_id="marketdata-provider.source-bar-revision.v1",
    )


def bind_source_identity(
    bars: list[MarketBar],
    *,
    query: BarQuery,
    provider: str,
    source_transport: str,
) -> list[MarketBar]:
    if not bars:
        return []
    providers = {bar.provider for bar in bars if bar.provider}
    if providers and providers != {provider}:
        raise MDValidationError("source bars disagree on provider identity")
    has_explicit_revision = any(bar.provider_revision is not None for bar in bars)
    if has_explicit_revision:
        if any(bar.provider_revision is None for bar in bars):
            raise MDValidationError("source bars disagree on provider_revision")
    return [
        replace(
            item,
            provider=provider,
            provider_revision=(
                item.provider_revision
                if has_explicit_revision
                else _bar_source_revision(
                    item,
                    query=query,
                    provider=provider,
                    source_transport=source_transport,
                )
            ),
            source_transport=source_transport,
        )
        for item in bars
    ]


def verify_snapshot_bar_revisions(
    bars: list[Mapping[str, Any]],
    expected_provider_revision: Mapping[str, Any],
) -> None:
    """Fail closed when snapshot bars disagree on provider or aggregate revision."""

    if not bars:
        return
    providers = {str(bar["provider"]) for bar in bars}
    if len(providers) != 1:
        raise MDValidationError("snapshot bars must share one provider")
    actual_provider_revision = snapshot_revision_identity(
        next(iter(providers)),
        [
            (
                int(bar["open_time_utc_ms"]),
                int(bar["revision"]),
                str(bar["provider_revision"]["revision"]),
            )
            for bar in bars
        ],
    )
    if actual_provider_revision != expected_provider_revision["revision"]:
        raise MDValidationError("bar provider_revision does not match snapshot")
