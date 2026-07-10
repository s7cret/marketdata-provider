from __future__ import annotations

from pathlib import Path
from typing import TextIO

from marketdata_provider.timeframes import timeframe_ms


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
