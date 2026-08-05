from __future__ import annotations

import csv
from collections.abc import Iterator
from pathlib import Path
from typing import Any, TextIO, cast

from marketdata_provider.core.bar import MarketBar
from marketdata_provider.store.segment_rows import row_to_bar
from marketdata_provider.timeframes import timeframe_ms


def read_csv(path: Path) -> list[MarketBar]:
    with path.open(newline="") as handle:
        return [
            row_to_bar(cast(dict[str, object], row))
            for row in csv.DictReader(handle)
        ]


def iter_csv_range(
    store: Any,
    path: Path,
    *,
    start: int | None,
    end: int | None,
    manifest: dict[str, object] | None = None,
) -> Iterator[MarketBar]:
    with path.open(newline="") as handle:
        fieldnames = next(csv.reader([handle.readline()]))
        if start is not None and manifest is not None:
            store._seek_csv_near_start(handle, path, start=start, manifest=manifest)
        reader = csv.DictReader(handle, fieldnames=fieldnames)
        for row in reader:
            bar = row_to_bar(cast(dict[str, object], row))
            if start is not None and bar.time < start:
                continue
            if end is not None and bar.time >= end:
                break
            yield bar


def seek_csv_near_start(
    fh: TextIO, path: Path, *, start: int, manifest: dict[str, object]
) -> None:
    start_time = manifest.get("start_time")
    rows_count = manifest.get("rows_count")
    timeframe = manifest.get("timeframe")
    if (
        not isinstance(start_time, int)
        or not isinstance(rows_count, int)
        or rows_count <= 0
        or not isinstance(timeframe, str)
    ):
        return
    try:
        duration = timeframe_ms(timeframe)
    except Exception:
        return
    if duration <= 0 or start <= start_time:
        return

    header_end = fh.tell()
    file_size = path.stat().st_size
    if file_size <= header_end:
        return
    low = header_end
    high = file_size - 1
    best = header_end
    for _ in range(24):
        if low >= high:
            break
        mid = (low + high) // 2
        fh.seek(mid)
        if mid > header_end:
            fh.readline()
        candidate_pos = fh.tell()
        line = fh.readline()
        if not line:
            high = max(header_end, mid - 1)
            continue
        try:
            candidate_time = int(line.split(",", 1)[0])
        except ValueError:
            fh.seek(header_end)
            return
        if candidate_time <= start:
            best = candidate_pos
            low = fh.tell()
        else:
            high = max(header_end, mid - 1)
    fh.seek(best)
