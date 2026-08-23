from __future__ import annotations

from dataclasses import replace

from openpine_contracts.hashing import content_hash

from marketdata_provider.contracts.query import BarQuery
from marketdata_provider.core.bar import MarketBar
from marketdata_provider.errors import MDValidationError


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
    explicit_revisions = {
        bar.provider_revision for bar in bars if bar.provider_revision is not None
    }
    if explicit_revisions:
        if len(explicit_revisions) != 1 or any(
            bar.provider_revision is None for bar in bars
        ):
            raise MDValidationError("source bars disagree on provider_revision")
        provider_revision = next(iter(explicit_revisions))
    else:
        provider_revision = content_hash(
            {
                "provider": provider,
                "source_transport": source_transport,
                "instrument_id": query.instrument.serialize(),
                "timeframe": query.timeframe.canonical,
                "start_ms": query.start_ms,
                "end_ms": query.end_ms,
                "bars": [
                    {
                        "time": item.time,
                        "time_close": item.time_close,
                        "open": item.open_text or str(item.open),
                        "high": item.high_text or str(item.high),
                        "low": item.low_text or str(item.low),
                        "close": item.close_text or str(item.close),
                        "volume": item.volume_text or str(item.volume),
                        "is_closed": item.is_closed,
                    }
                    for item in bars
                ],
            },
            schema_id="marketdata-provider.source-revision.v1",
        )
    return [
        replace(
            item,
            provider=provider,
            provider_revision=provider_revision,
            source_transport=source_transport,
        )
        for item in bars
    ]
