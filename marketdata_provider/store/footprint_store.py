from __future__ import annotations

import csv
import hashlib
import json
import os
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import ClassVar

from marketdata_provider.contracts.footprint import (
    FootprintBar,
    FootprintLevel,
    FootprintQuery,
    FootprintSeries,
)
from marketdata_provider.contracts.series import (
    CoverageReport,
    CoverageStatus,
    StoreResult,
)


@dataclass(frozen=True, slots=True)
class FootprintSegmentManifest:
    schema_version: str
    exchange: str
    market: str
    symbol: str
    raw_source: str
    timeframe: str
    price_bucket: float
    start_time: int | None
    end_time: int | None
    rows_count: int
    checksum: str


class FootprintStore:
    fields: ClassVar[list[str]] = [
        "time",
        "time_close",
        "price_low",
        "price_high",
        "buy_volume",
        "sell_volume",
        "buy_count",
        "sell_count",
        "trades_count",
    ]

    def __init__(self, root: str | Path):
        self.root = Path(root)

    def read(self, query: FootprintQuery) -> FootprintSeries:
        path = self._data_path(query)
        if not path.exists():
            return FootprintSeries(query, (), _coverage_for(query, ()))
        by_time: dict[int, list[FootprintLevel]] = {}
        trades_count: dict[int, int] = {}
        closes: dict[int, int] = {}
        with path.open(newline="") as fh:
            for row in csv.DictReader(fh):
                time = int(row["time"])
                if not (query.start_ms <= time < query.end_ms):
                    continue
                closes[time] = int(row["time_close"])
                trades_count[time] = int(row["trades_count"])
                by_time.setdefault(time, []).append(
                    FootprintLevel(
                        price_low=float(row["price_low"]),
                        price_high=float(row["price_high"]),
                        buy_volume=float(row["buy_volume"]),
                        sell_volume=float(row["sell_volume"]),
                        buy_count=int(row["buy_count"]),
                        sell_count=int(row["sell_count"]),
                    )
                )
        bars = tuple(
            FootprintBar(
                time,
                closes[time],
                tuple(sorted(levels, key=lambda item: item.price_low)),
                trades_count[time],
            )
            for time, levels in sorted(by_time.items())
        )
        return FootprintSeries(query, bars, _coverage_for(query, bars))

    def write(self, series: FootprintSeries) -> StoreResult:
        existing = {bar.time: bar for bar in self.read(series.query).bars}
        for bar in series.bars:
            existing[bar.time] = bar
        bars = tuple(existing[time] for time in sorted(existing))
        path = self._data_path(series.query)
        path.parent.mkdir(parents=True, exist_ok=True)
        self._atomic_write_csv(path, bars)
        checksum = _checksum(bars)
        manifest = FootprintSegmentManifest(
            schema_version="footprint-v1-csv-1",
            exchange=series.query.instrument.exchange,
            market=series.query.instrument.market,
            symbol=series.query.instrument.symbol,
            raw_source="agg_trades",
            timeframe=series.query.timeframe.canonical,
            price_bucket=series.query.bucket_size,
            start_time=bars[0].time if bars else None,
            end_time=bars[-1].time_close if bars else None,
            rows_count=sum(len(bar.levels) for bar in bars),
            checksum=checksum,
        )
        self._atomic_write_text(
            path.parent / "manifest.json",
            json.dumps(asdict(manifest), sort_keys=True, indent=2) + "\n",
        )
        return StoreResult(success=True, rows_written=len(series.bars))

    def coverage(self, query: FootprintQuery) -> CoverageReport:
        return self.read(query).coverage

    def _data_path(self, query: FootprintQuery) -> Path:
        bucket = str(query.bucket_size).replace(".", "_")
        return (
            self.root
            / "footprint-v1"
            / f"exchange={query.instrument.exchange}"
            / f"market={query.instrument.market}"
            / f"symbol={query.instrument.symbol}"
            / "raw_source=agg_trades"
            / f"timeframe={query.timeframe.canonical}"
            / f"bucket={bucket}"
            / "footprint.csv"
        )

    def _atomic_write_csv(self, path: Path, bars: tuple[FootprintBar, ...]) -> None:
        fd, tmp = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
        with os.fdopen(fd, "w", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=self.fields)
            writer.writeheader()
            for bar in bars:
                for level in bar.levels:
                    writer.writerow(
                        {
                            "time": bar.time,
                            "time_close": bar.time_close,
                            "price_low": level.price_low,
                            "price_high": level.price_high,
                            "buy_volume": level.buy_volume,
                            "sell_volume": level.sell_volume,
                            "buy_count": level.buy_count,
                            "sell_count": level.sell_count,
                            "trades_count": bar.trades_count,
                        }
                    )
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)

    def _atomic_write_text(self, path: Path, content: str) -> None:
        fd, tmp = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
        with os.fdopen(fd, "w") as fh:
            fh.write(content)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)


def _coverage_for(
    query: FootprintQuery, bars: tuple[FootprintBar, ...]
) -> CoverageReport:
    duration = int(query.timeframe.duration_ms or 0)
    delivered = {bar.time for bar in bars}
    missing = tuple(
        (start, min(start + duration, query.end_ms))
        for start in range(query.start_ms, query.end_ms, duration)
        if start not in delivered
    )
    status: CoverageStatus = "empty" if not bars else "gap" if missing else "valid"
    return CoverageReport(
        query.start_ms,
        query.end_ms,
        bars[0].time if bars else None,
        bars[-1].time_close if bars else None,
        missing,
        (),
        ("footprint",),
        status,
    )


def _checksum(bars: tuple[FootprintBar, ...]) -> str:
    digest = hashlib.sha256()
    for bar in bars:
        for level in bar.levels:
            digest.update(
                json.dumps(
                    {
                        "time": bar.time,
                        "time_close": bar.time_close,
                        "price_low": level.price_low,
                        "price_high": level.price_high,
                        "buy_volume": level.buy_volume,
                        "sell_volume": level.sell_volume,
                        "buy_count": level.buy_count,
                        "sell_count": level.sell_count,
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode()
            )
            digest.update(b"\n")
    return digest.hexdigest()
