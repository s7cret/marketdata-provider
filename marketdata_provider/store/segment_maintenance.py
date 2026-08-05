from __future__ import annotations

import json
import time
from typing import TYPE_CHECKING, Literal

from marketdata_provider.core.bar import MarketBar
from marketdata_provider.store.segment_append import series_lock
from marketdata_provider.store.segment_manifest import SegmentManifest

if TYPE_CHECKING:
    from marketdata_provider.store.segment_store import SegmentStore

SegmentFormat = Literal["csv", "parquet"]


def vacuum_segments(store: SegmentStore) -> dict[str, int]:
    removed = 0
    for path in store.root.glob("v1/**/bars.*"):
        with series_lock(path.parent / ".writer.lock"):
            manifest = path.parent / "manifest.json"
            if not manifest.exists():
                continue
            fmt = json.loads(manifest.read_text()).get("data_format", "csv")
            expected = path.parent / f"bars.{fmt}"
            if path != expected and path.exists():
                path.unlink()
                removed += 1
    stale_before = time.time() - 3600
    for pattern in ("v1/**/.bars.*", "v1/**/.manifest.json.*"):
        for path in store.root.glob(pattern):
            with series_lock(path.parent / ".writer.lock"):
                try:
                    if path.is_file() and path.stat().st_mtime < stale_before:
                        path.unlink()
                        removed += 1
                except FileNotFoundError:
                    continue
    with store._connect_index() as db:
        db.execute("VACUUM")
    return {"removed_stale_data_files": removed}


def compact_segment(
    store: SegmentStore,
    *,
    exchange: str,
    market: str,
    symbol: str,
    timeframe: str,
    source_kind: str = "trade_kline",
    data_format: SegmentFormat | None = None,
) -> SegmentManifest:
    key = {
        "exchange": exchange,
        "market": market,
        "symbol": symbol,
        "timeframe": timeframe,
        "source_kind": source_kind,
    }
    with store.series_writer_lock(**key):
        bars: list[MarketBar] = store.read_all(
            exchange=exchange,
            market=market,
            symbol=symbol,
            timeframe=timeframe,
            source_kind=source_kind,
        )
        return store._replace_all_locked(
            bars,
            exchange=exchange,
            market=market,
            symbol=symbol,
            timeframe=timeframe,
            source_kind=source_kind,
            data_format=data_format,
        )
