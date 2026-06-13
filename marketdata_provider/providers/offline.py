from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

from marketdata_provider.core.bar import Bar
from marketdata_provider.core.protocols import DataProvider, IntrabarDataProvider
from marketdata_provider.errors import MDIntrabarDataUnavailable, MDUnsupportedFeature
from marketdata_provider.timeframes import close_time_ms
from marketdata_provider.validation import validate_bars


class OfflineDataProvider(DataProvider, IntrabarDataProvider):
    def __init__(self, path: str | Path, *, timeframe: str | None = None):
        self.path = Path(path)
        self.timeframe = timeframe

    def get_bars(
        self,
        symbol: str,
        timeframe: str,
        start: int | None,
        end: int | None,
        *,
        max_bars: int | None = None,
    ) -> list[Bar]:
        suffix = self.path.suffix.lower()
        if suffix == ".csv":
            bars = self._read_csv(timeframe)
        elif suffix == ".parquet":
            bars = self._read_parquet(timeframe)
        else:
            raise MDUnsupportedFeature(
                f"Unsupported offline format: {self.path.suffix}"
            )
        out = [
            bar
            for bar in bars
            if (start is None or bar.time >= start) and (end is None or bar.time < end)
        ]
        if max_bars is not None:
            out = out[:max_bars]
        validate_bars(out)
        return out

    def get_intrabar_bars(
        self,
        symbol: str,
        chart_bar: Bar,
        lower_timeframe: str | None = None,
        *,
        max_bars: int | None = None,
    ) -> list[Bar]:
        timeframe = lower_timeframe or self.timeframe
        if timeframe is None:
            raise MDIntrabarDataUnavailable(
                "Offline intrabar requires lower_timeframe or provider timeframe"
            )
        end = chart_bar.time_close + 1 if chart_bar.time_close is not None else None
        return self.get_bars(symbol, timeframe, chart_bar.time, end, max_bars=max_bars)

    def _read_csv(self, timeframe: str) -> list[Bar]:
        with self.path.open(newline="") as handle:
            rows = list(csv.DictReader(handle))
        bars = [self._bar_from_row(row, timeframe) for row in rows]
        bars.sort(key=lambda bar: bar.time)
        validate_bars(bars)
        return bars

    @staticmethod
    def _bar_from_row(row: dict[str, Any], timeframe: str) -> Bar:
        open_time = int(
            str(row.get("time") or row.get("timestamp") or row.get("open_time") or 0)
        )
        close_time_raw = row.get("time_close") or row.get("close_time")
        close_time = (
            int(str(close_time_raw))
            if close_time_raw not in (None, "")
            else close_time_ms(open_time, timeframe)
        )
        return Bar(
            open_time,
            float(row["open"]),
            float(row["high"]),
            float(row["low"]),
            float(row["close"]),
            float(row.get("volume") or 0),
            close_time,
        )

    def _read_parquet(self, timeframe: str) -> list[Bar]:
        try:
            import pyarrow.parquet as pq
        except Exception as exc:
            raise MDUnsupportedFeature(
                "Parquet support requires pyarrow extra"
            ) from exc
        try:
            rows = pq.read_table(self.path).to_pylist()
        except Exception as exc:
            raise MDUnsupportedFeature(
                f"Parquet offline data unavailable: {self.path}"
            ) from exc
        bars = [self._parquet_bar(row, timeframe) for row in rows]
        bars.sort(key=lambda bar: bar.time)
        validate_bars(bars)
        return bars

    @staticmethod
    def _parquet_bar(row: dict[str, Any], timeframe: str) -> Bar:
        open_time = int(row["time"])
        return Bar(
            open_time,
            float(row["open"]),
            float(row["high"]),
            float(row["low"]),
            float(row["close"]),
            float(row.get("volume") or 0),
            int(row.get("time_close") or close_time_ms(open_time, timeframe)),
        )
