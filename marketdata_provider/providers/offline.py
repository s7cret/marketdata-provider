from __future__ import annotations
import csv
from pathlib import Path
from marketdata_provider.core.bar import Bar
from marketdata_provider.core.protocols import DataProvider, IntrabarDataProvider
from marketdata_provider.errors import MDIntrabarDataUnavailable, MDUnsupportedFeature
from marketdata_provider.timeframes import close_time_ms
from marketdata_provider.validation import validate_bars

class OfflineDataProvider(DataProvider, IntrabarDataProvider):
    def __init__(self, path: str | Path, *, timeframe: str | None = None):
        self.path = Path(path); self.timeframe = timeframe
    def get_bars(self, symbol: str, timeframe: str, start: int | None, end: int | None, *, max_bars: int | None = None) -> list[Bar]:
        if self.path.suffix.lower() == ".csv": bars = self._read_csv(timeframe)
        elif self.path.suffix.lower() == ".parquet": bars = self._read_parquet(timeframe)
        else: raise MDUnsupportedFeature(f"Unsupported offline format: {self.path.suffix}")
        out = [b for b in bars if (start is None or b.time >= start) and (end is None or b.time < end)]
        if max_bars is not None: out = out[:max_bars]
        validate_bars(out)
        return out
    def get_intrabar_bars(self, symbol: str, chart_bar: Bar, lower_timeframe: str | None = None, *, max_bars: int | None = None) -> list[Bar]:
        tf = lower_timeframe or self.timeframe
        if tf is None: raise MDIntrabarDataUnavailable("Offline intrabar requires lower_timeframe or provider timeframe")
        end = chart_bar.time_close + 1 if chart_bar.time_close is not None else None
        return self.get_bars(symbol, tf, chart_bar.time, end, max_bars=max_bars)
    def _read_csv(self, timeframe: str) -> list[Bar]:
        with self.path.open(newline="") as f:
            rows = list(csv.DictReader(f))
        bars: list[Bar] = []
        for r in rows:
            t = int(str(r.get("time") or r.get("timestamp") or r.get("open_time") or 0))
            tc = r.get("time_close") or r.get("close_time")
            bars.append(Bar(t, float(r["open"]), float(r["high"]), float(r["low"]), float(r["close"]), float(r.get("volume") or 0), int(str(tc)) if tc not in (None, "") else close_time_ms(t, timeframe)))
        bars.sort(key=lambda b: b.time); validate_bars(bars); return bars
    def _read_parquet(self, timeframe: str) -> list[Bar]:
        try: import pyarrow.parquet as pq
        except Exception as e: raise MDUnsupportedFeature("Parquet support requires pyarrow extra") from e
        tbl = pq.read_table(self.path).to_pylist()
        bars = [Bar(int(r["time"]), float(r["open"]), float(r["high"]), float(r["low"]), float(r["close"]), float(r.get("volume") or 0), int(r.get("time_close") or close_time_ms(int(r["time"]), timeframe))) for r in tbl]
        bars.sort(key=lambda b: b.time); validate_bars(bars); return bars
