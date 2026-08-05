from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from marketdata_provider.errors import MDInvalidExchangeResponse
from marketdata_provider.store.segment_checksums import CANONICAL_CHECKSUM


@dataclass(frozen=True, slots=True)
class SegmentManifest:
    runtime_contract_version: str
    schema_version: str
    exchange: str
    market: str
    symbol: str
    timeframe: str
    source_transport: str
    source_kind: str
    rows_count: int
    start_time: int | None
    end_time: int | None
    checksum: str
    data_format: str = "csv"
    checksum_algorithm: str = CANONICAL_CHECKSUM
    base_checksum: str | None = None
    base_rows_count: int | None = None


def load_segment_manifest(
    store: Any,
    *,
    exchange: str,
    market: str,
    symbol: str,
    timeframe: str,
    source_kind: str = "trade_kline",
) -> SegmentManifest | None:
    manifest_path = (
        store._dir(
            exchange=exchange,
            market=market,
            symbol=symbol,
            timeframe=timeframe,
            source_kind=source_kind,
        )
        / "manifest.json"
    )
    if not manifest_path.exists():
        return None
    payload = json.loads(manifest_path.read_text())
    if not isinstance(payload, dict):
        raise MDInvalidExchangeResponse("Segment manifest must be a JSON object")
    return SegmentManifest(**payload)
