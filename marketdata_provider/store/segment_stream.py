from __future__ import annotations

import csv
import hashlib
import json
import os
import tempfile
from collections.abc import Iterable
from dataclasses import asdict
from typing import Any

from marketdata_provider.core.bar import RUNTIME_CONTRACT_VERSION, MarketBar
from marketdata_provider.errors import MDInvalidExchangeResponse
from marketdata_provider.store.segment_checksums import (
    TAIL_CHAIN_CHECKSUM,
    _update_checksum,
)
from marketdata_provider.store.segment_manifest import SegmentManifest
from marketdata_provider.store.segment_replace import (
    begin_replacement,
    finish_replacement,
)
from marketdata_provider.timeframes import canonical_timeframe
from marketdata_provider.validation import validate_bars


def replace_all_stream(
    store: Any,
    bars: Iterable[MarketBar],
    *,
    exchange: str,
    market: str,
    symbol: str,
    timeframe: str,
    source_kind: str = "trade_kline",
) -> SegmentManifest:
    data_path, manifest_path = store._paths(
        exchange=exchange,
        market=market,
        symbol=symbol,
        timeframe=timeframe,
        source_kind=source_kind,
        data_format="csv",
    )
    data_path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=f".{data_path.name}.", dir=str(data_path.parent))
    digest = hashlib.sha256()
    rows_count = 0
    first: MarketBar | None = None
    last: MarketBar | None = None
    downloaded_at = 0
    previous_time: int | None = None
    try:
        with os.fdopen(fd, "w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=store.fields)
            writer.writeheader()
            for bar in bars:
                if previous_time is not None and bar.time <= previous_time:
                    raise MDInvalidExchangeResponse(
                        "Segment stream must be strictly ordered by time"
                    )
                previous_time = bar.time
                validate_bars([bar.to_bar()])
                writer.writerow({name: getattr(bar, name) for name in store.fields})
                _update_checksum(digest, bar)
                rows_count += 1
                first = first or bar
                last = bar
                downloaded_at = max(downloaded_at, bar.downloaded_at or 0)
            handle.flush()
            os.fsync(handle.fileno())
        initial_checksum = digest.hexdigest()
        manifest = SegmentManifest(
            runtime_contract_version=RUNTIME_CONTRACT_VERSION,
            schema_version="stage-d-csv-3",
            exchange=exchange.lower(),
            market=market.lower(),
            symbol=symbol.upper(),
            timeframe=canonical_timeframe(timeframe),
            source_transport=first.source_transport if first else "ws",
            source_kind=source_kind,
            rows_count=rows_count,
            start_time=first.time if first else None,
            end_time=last.time if last else None,
            checksum=initial_checksum,
            data_format="csv",
            checksum_algorithm=TAIL_CHAIN_CHECKSUM,
            base_checksum=initial_checksum,
            base_rows_count=rows_count,
        )
        old_manifest = store.manifest_for(
            exchange=exchange,
            market=market,
            symbol=symbol,
            timeframe=timeframe,
            source_kind=source_kind,
        )
        old_data_path = None
        if old_manifest is not None:
            old_data_path, _ = store._paths(
                exchange=exchange,
                market=market,
                symbol=symbol,
                timeframe=timeframe,
                source_kind=source_kind,
                data_format=old_manifest.data_format,
            )
        journal_path = begin_replacement(
            store,
            old_manifest=old_manifest,
            new_manifest=manifest,
            old_data_path=old_data_path,
            new_data_path=data_path,
            downloaded_at=downloaded_at,
        )
        os.replace(tmp, data_path)
        store._fsync_directory(data_path.parent)
        store._atomic_write_text(
            manifest_path,
            json.dumps(asdict(manifest), sort_keys=True, indent=2) + "\n",
        )
        store._replace_index_manifest(manifest, downloaded_at=downloaded_at)
        finish_replacement(store, journal_path)
        return manifest
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)
