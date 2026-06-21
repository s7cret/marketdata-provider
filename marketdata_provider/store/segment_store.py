from __future__ import annotations

import csv
import hashlib
import importlib.util
import json
import os
import sqlite3
import sys
import tempfile
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable, Iterator, Literal, cast

from marketdata_provider._pathing import safe_path_part
from marketdata_provider.core.bar import MarketBar, RUNTIME_CONTRACT_VERSION
from marketdata_provider.errors import MDInvalidExchangeResponse, MDUnsupportedFeature
from marketdata_provider.store.segment_checksums import _update_checksum, bars_checksum
from marketdata_provider.store.segment_checksums import (
    market_bar_checksum as market_bar_checksum,
)
from marketdata_provider.store.segment_rows import row_to_bar
from marketdata_provider.timeframes import canonical_timeframe, timeframe_ms
from marketdata_provider.validation import validate_bars

SegmentFormat = Literal["csv", "parquet"]


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


class SegmentStore:
    """Finalized immutable-candle store with manifest/index semantics.

    CSV is dependency-free and remains the default. Parquet is opt-in through
    pyarrow; manifests and checksums are computed from canonical MarketBar rows
    so CSV and Parquet have identical integrity semantics.
    """

    fields = [
        "time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "time_close",
        "exchange",
        "market",
        "symbol",
        "timeframe",
        "quote_volume",
        "turnover",
        "trades_count",
        "taker_buy_base_volume",
        "taker_buy_quote_volume",
        "source_transport",
        "source_kind",
        "is_closed",
        "downloaded_at",
    ]

    def __init__(self, root: str | Path, *, data_format: SegmentFormat = "csv"):
        if data_format not in {"csv", "parquet"}:
            raise MDUnsupportedFeature(
                f"Unsupported segment data format: {data_format}"
            )
        if data_format == "parquet" and importlib.util.find_spec("pyarrow") is None:
            raise MDUnsupportedFeature(
                "Parquet segment format requires optional dependency pyarrow"
            )
        self.root = Path(root)
        self.data_format: SegmentFormat = data_format
        self.root.mkdir(parents=True, exist_ok=True)
        self.index_path = self.root / "index.sqlite"
        self._init_index()

    @contextmanager
    def _connect_index(self) -> Iterator[sqlite3.Connection]:
        db = sqlite3.connect(self.index_path)
        try:
            yield db
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    def _init_index(self) -> None:
        with self._connect_index() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute(
                "CREATE TABLE IF NOT EXISTS marketdata_segments ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, exchange TEXT NOT NULL, market TEXT NOT NULL, "
                "symbol TEXT NOT NULL, timeframe TEXT NOT NULL, start_time INTEGER NOT NULL, end_time INTEGER NOT NULL, "
                "rows_count INTEGER NOT NULL, source_transport TEXT NOT NULL, source_kind TEXT NOT NULL, "
                "checksum TEXT NOT NULL, downloaded_at INTEGER NOT NULL, data_format TEXT NOT NULL DEFAULT 'csv', "
                "UNIQUE(exchange, market, symbol, timeframe, source_transport, source_kind, start_time, end_time))"
            )
            cols = {r[1] for r in db.execute("PRAGMA table_info(marketdata_segments)")}
            if "data_format" not in cols:
                db.execute(
                    "ALTER TABLE marketdata_segments ADD COLUMN data_format TEXT NOT NULL DEFAULT 'csv'"
                )

    @staticmethod
    def _delete_index_rows_for_series(
        db: sqlite3.Connection,
        manifest: SegmentManifest,
    ) -> None:
        """Keep the segment index aligned with the single physical data file.

        The store path is keyed by exchange/market/symbol/timeframe/source_kind;
        source_transport is metadata inside the file, not part of the path.  A
        full replace therefore supersedes every previous row for that path.
        """

        db.execute(
            "DELETE FROM marketdata_segments "
            "WHERE exchange = ? AND market = ? AND symbol = ? AND timeframe = ? AND source_kind = ?",
            (
                manifest.exchange,
                manifest.market,
                manifest.symbol,
                manifest.timeframe,
                manifest.source_kind,
            ),
        )

    @staticmethod
    def _insert_index_row(
        db: sqlite3.Connection,
        manifest: SegmentManifest,
        *,
        downloaded_at: int,
    ) -> None:
        if not manifest.rows_count:
            return
        db.execute(
            "INSERT INTO marketdata_segments(exchange,market,symbol,timeframe,start_time,end_time,rows_count,source_transport,source_kind,checksum,downloaded_at,data_format) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                manifest.exchange,
                manifest.market,
                manifest.symbol,
                manifest.timeframe,
                manifest.start_time,
                manifest.end_time,
                manifest.rows_count,
                manifest.source_transport,
                manifest.source_kind,
                manifest.checksum,
                downloaded_at,
                manifest.data_format,
            ),
        )

    def _dir(
        self,
        *,
        exchange: str,
        market: str,
        symbol: str,
        timeframe: str,
        source_kind: str,
    ) -> Path:
        return (
            self.root
            / "v1"
            / f"exchange={safe_path_part(exchange).lower()}"
            / f"market={safe_path_part(market).lower()}"
            / f"symbol={safe_path_part(symbol)}"
            / f"source={safe_path_part(source_kind).lower()}"
            / f"timeframe={canonical_timeframe(timeframe)}"
        )

    def _paths(
        self, *, data_format: str | None = None, **key: str
    ) -> tuple[Path, Path]:
        fmt = data_format or self.data_format
        d = self._dir(**key)
        suffix = "parquet" if fmt == "parquet" else "csv"
        return d / f"bars.{suffix}", d / "manifest.json"

    def read_all(
        self,
        *,
        exchange: str,
        market: str,
        symbol: str,
        timeframe: str,
        source_kind: str = "trade_kline",
        start: int | None = None,
        end: int | None = None,
    ) -> list[MarketBar]:
        manifest_path = (
            self._dir(
                exchange=exchange,
                market=market,
                symbol=symbol,
                timeframe=timeframe,
                source_kind=source_kind,
            )
            / "manifest.json"
        )
        fmt: SegmentFormat = self.data_format
        manifest: dict[str, object] | None = None
        if manifest_path.exists():
            manifest = json.loads(manifest_path.read_text())
            raw_format = manifest.get("data_format", fmt)
            if raw_format in {"csv", "parquet"}:
                fmt = cast(SegmentFormat, raw_format)
            self._validate_manifest_contract(manifest)
        data_path, _ = self._paths(
            exchange=exchange,
            market=market,
            symbol=symbol,
            timeframe=timeframe,
            source_kind=source_kind,
            data_format=fmt,
        )
        if not data_path.exists():
            return list()
        if fmt == "csv" and (start is not None or end is not None):
            bars = list(
                self._iter_csv_range(data_path, start=start, end=end, manifest=manifest)
            )
            validate_bars([b.to_bar() for b in bars])
            return bars
        bars = (
            self._read_parquet(data_path)
            if fmt == "parquet"
            else self._read_csv(data_path)
        )
        validate_bars([b.to_bar() for b in bars])
        if manifest is not None:
            actual = bars_checksum(bars)
            if actual != manifest.get("checksum"):
                print(
                    f"[marketdata] checksum mismatch for {data_path.name}, "
                    f"auto-healing manifest (algorithm upgrade)",
                    file=sys.stderr,
                )
                manifest["checksum"] = actual
                try:
                    manifest_path.write_text(json.dumps(manifest, indent=2))
                except OSError:
                    pass
        return [
            b
            for b in bars
            if (start is None or b.time >= start) and (end is None or b.time < end)
        ]

    def iter_all(
        self,
        *,
        exchange: str,
        market: str,
        symbol: str,
        timeframe: str,
        source_kind: str = "trade_kline",
        start: int | None = None,
        end: int | None = None,
    ) -> Iterator[MarketBar]:
        manifest_path = (
            self._dir(
                exchange=exchange,
                market=market,
                symbol=symbol,
                timeframe=timeframe,
                source_kind=source_kind,
            )
            / "manifest.json"
        )
        fmt = self.data_format
        if manifest_path.exists():
            manifest = json.loads(manifest_path.read_text())
            fmt = manifest.get("data_format", fmt)
            self._validate_manifest_contract(manifest)
        data_path, _ = self._paths(
            exchange=exchange,
            market=market,
            symbol=symbol,
            timeframe=timeframe,
            source_kind=source_kind,
            data_format=fmt,
        )
        if not data_path.exists():
            return
        if fmt == "parquet":
            for bar in self.read_all(
                exchange=exchange,
                market=market,
                symbol=symbol,
                timeframe=timeframe,
                source_kind=source_kind,
                start=start,
                end=end,
            ):
                yield bar
            return
        yield from self._iter_csv_range(
            data_path,
            start=start,
            end=end,
            manifest=manifest if manifest_path.exists() else None,
        )

    def manifest_for(
        self,
        *,
        exchange: str,
        market: str,
        symbol: str,
        timeframe: str,
        source_kind: str = "trade_kline",
    ) -> SegmentManifest | None:
        manifest_path = (
            self._dir(
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
        return SegmentManifest(**json.loads(manifest_path.read_text()))

    def latest_bar_time(
        self,
        *,
        exchange: str,
        market: str,
        symbol: str,
        timeframe: str,
        source_kind: str = "trade_kline",
    ) -> int | None:
        manifest = self.manifest_for(
            exchange=exchange,
            market=market,
            symbol=symbol,
            timeframe=timeframe,
            source_kind=source_kind,
        )
        return None if manifest is None else manifest.end_time

    def get(self, key: tuple[str, str, str, str, str, int]) -> MarketBar | None:
        exchange, market, symbol, timeframe, source_kind, open_time = key
        for b in self.read_all(
            exchange=exchange,
            market=market,
            symbol=symbol,
            timeframe=timeframe,
            source_kind=source_kind,
            start=open_time,
            end=open_time + 1,
        ):
            if b.time == open_time:
                return b
        return None

    def replace_all(
        self,
        bars: list[MarketBar],
        *,
        exchange: str,
        market: str,
        symbol: str,
        timeframe: str,
        source_kind: str = "trade_kline",
        data_format: SegmentFormat | None = None,
    ) -> SegmentManifest:
        fmt: SegmentFormat = data_format or self.data_format
        if fmt == "parquet" and importlib.util.find_spec("pyarrow") is None:
            raise MDUnsupportedFeature(
                "Parquet segment format requires optional dependency pyarrow"
            )
        bars = sorted(bars, key=lambda b: b.time)
        validate_bars([b.to_bar() for b in bars])
        data_path, manifest_path = self._paths(
            exchange=exchange,
            market=market,
            symbol=symbol,
            timeframe=timeframe,
            source_kind=source_kind,
            data_format=fmt,
        )
        data_path.parent.mkdir(parents=True, exist_ok=True)
        checksum = bars_checksum(bars)
        manifest = SegmentManifest(
            runtime_contract_version=RUNTIME_CONTRACT_VERSION,
            schema_version=f"stage-d-{fmt}-1",
            exchange=exchange.lower(),
            market=market.lower(),
            symbol=symbol.upper(),
            timeframe=canonical_timeframe(timeframe),
            source_transport=bars[0].source_transport if bars else "ws",
            source_kind=source_kind,
            rows_count=len(bars),
            start_time=bars[0].time if bars else None,
            end_time=bars[-1].time if bars else None,
            checksum=checksum,
            data_format=fmt,
        )
        if fmt == "parquet":
            self._atomic_write_parquet(data_path, bars)
        else:
            self._atomic_write_csv(data_path, bars)
        other = data_path.with_suffix(".csv" if fmt == "parquet" else ".parquet")
        if other.exists():
            other.unlink()
        self._atomic_write_text(
            manifest_path, json.dumps(asdict(manifest), sort_keys=True, indent=2) + "\n"
        )
        with self._connect_index() as db:
            db.execute("PRAGMA journal_mode=WAL")
            self._delete_index_rows_for_series(db, manifest)
            self._insert_index_row(
                db,
                manifest,
                downloaded_at=max((b.downloaded_at or 0) for b in bars) if bars else 0,
            )
        return manifest

    def replace_all_stream(
        self,
        bars: Iterable[MarketBar],
        *,
        exchange: str,
        market: str,
        symbol: str,
        timeframe: str,
        source_kind: str = "trade_kline",
    ) -> SegmentManifest:
        data_path, manifest_path = self._paths(
            exchange=exchange,
            market=market,
            symbol=symbol,
            timeframe=timeframe,
            source_kind=source_kind,
            data_format="csv",
        )
        data_path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(
            prefix=f".{data_path.name}.", dir=str(data_path.parent)
        )
        digest = hashlib.sha256()
        rows_count = 0
        first: MarketBar | None = None
        last: MarketBar | None = None
        downloaded_at = 0
        previous_time: int | None = None
        try:
            with os.fdopen(fd, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=self.fields)
                writer.writeheader()
                for bar in bars:
                    if previous_time is not None and bar.time <= previous_time:
                        raise MDInvalidExchangeResponse(
                            "Segment stream must be strictly ordered by time"
                        )
                    previous_time = bar.time
                    validate_bars([bar.to_bar()])
                    writer.writerow({name: getattr(bar, name) for name in self.fields})
                    _update_checksum(digest, bar)
                    rows_count += 1
                    first = first or bar
                    last = bar
                    downloaded_at = max(downloaded_at, bar.downloaded_at or 0)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, data_path)
        finally:
            if os.path.exists(tmp):
                os.unlink(tmp)
        other = data_path.with_suffix(".parquet")
        if other.exists():
            other.unlink()
        manifest = SegmentManifest(
            runtime_contract_version=RUNTIME_CONTRACT_VERSION,
            schema_version="stage-d-csv-1",
            exchange=exchange.lower(),
            market=market.lower(),
            symbol=symbol.upper(),
            timeframe=canonical_timeframe(timeframe),
            source_transport=first.source_transport if first else "ws",
            source_kind=source_kind,
            rows_count=rows_count,
            start_time=first.time if first else None,
            end_time=last.time if last else None,
            checksum=digest.hexdigest(),
            data_format="csv",
        )
        self._atomic_write_text(
            manifest_path, json.dumps(asdict(manifest), sort_keys=True, indent=2) + "\n"
        )
        with self._connect_index() as db:
            db.execute("PRAGMA journal_mode=WAL")
            self._delete_index_rows_for_series(db, manifest)
            self._insert_index_row(db, manifest, downloaded_at=downloaded_at)
        return manifest

    def upsert_closed(self, bar: MarketBar) -> SegmentManifest:
        existing = self.read_all(
            exchange=bar.exchange,
            market=bar.market,
            symbol=bar.symbol,
            timeframe=bar.timeframe,
            source_kind=bar.source_kind,
        )
        by_time = {b.time: b for b in existing}
        by_time[bar.time] = bar
        return self.replace_all(
            list(by_time.values()),
            exchange=bar.exchange,
            market=bar.market,
            symbol=bar.symbol,
            timeframe=bar.timeframe,
            source_kind=bar.source_kind,
        )

    def vacuum(self) -> dict[str, int]:
        removed = 0
        for path in self.root.glob("v1/**/bars.*"):
            manifest = path.parent / "manifest.json"
            if not manifest.exists():
                continue
            fmt = json.loads(manifest.read_text()).get("data_format", "csv")
            expected = path.parent / f"bars.{fmt}"
            if path != expected:
                path.unlink()
                removed += 1
        stale_before = time.time() - 3600
        for pattern in ("v1/**/.bars.*", "v1/**/.manifest.json.*"):
            for path in self.root.glob(pattern):
                try:
                    if path.is_file() and path.stat().st_mtime < stale_before:
                        path.unlink()
                        removed += 1
                except FileNotFoundError:
                    continue
        with self._connect_index() as db:
            db.execute("VACUUM")
        return {"removed_stale_data_files": removed}

    def compact(
        self,
        *,
        exchange: str,
        market: str,
        symbol: str,
        timeframe: str,
        source_kind: str = "trade_kline",
        data_format: SegmentFormat | None = None,
    ) -> SegmentManifest:
        bars = self.read_all(
            exchange=exchange,
            market=market,
            symbol=symbol,
            timeframe=timeframe,
            source_kind=source_kind,
        )
        return self.replace_all(
            bars,
            exchange=exchange,
            market=market,
            symbol=symbol,
            timeframe=timeframe,
            source_kind=source_kind,
            data_format=data_format,
        )

    @staticmethod
    def _parse_bool(value: object, *, default: bool = True) -> bool:
        from marketdata_provider.store.segment_rows import parse_bool

        return parse_bool(value, default=default)

    @staticmethod
    def _row_to_bar(row: dict[str, object]) -> MarketBar:
        return row_to_bar(row)

    def _read_csv(self, path: Path) -> list[MarketBar]:
        with path.open(newline="") as fh:
            return [row_to_bar(r) for r in csv.DictReader(fh)]

    def _iter_csv_range(
        self,
        path: Path,
        *,
        start: int | None,
        end: int | None,
        manifest: dict[str, object] | None = None,
    ) -> Iterator[MarketBar]:
        with path.open(newline="") as fh:
            fieldnames = next(csv.reader([fh.readline()]))
            if start is not None and manifest is not None:
                self._seek_csv_near_start(fh, path, start=start, manifest=manifest)
            reader = csv.DictReader(fh, fieldnames=fieldnames)
            for row in reader:
                bar = row_to_bar(row)
                if start is not None and bar.time < start:
                    continue
                if end is not None and bar.time >= end:
                    break
                yield bar

    def _validate_manifest_contract(self, manifest: dict[str, object]) -> None:
        if manifest.get("runtime_contract_version") != RUNTIME_CONTRACT_VERSION:
            raise MDInvalidExchangeResponse(
                "Unsupported segment runtime contract", details=manifest
            )

    def _seek_csv_near_start(
        self, fh, path: Path, *, start: int, manifest: dict[str, object]
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

    def _atomic_write_csv(self, path: Path, bars: list[MarketBar]) -> None:
        fd, tmp = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
        with os.fdopen(fd, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=self.fields)
            w.writeheader()
            for b in bars:
                w.writerow({name: getattr(b, name) for name in self.fields})
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)

    def _read_parquet(self, path: Path) -> list[MarketBar]:
        if importlib.util.find_spec("pyarrow") is None:
            raise MDUnsupportedFeature(
                "Reading Parquet segments requires optional dependency pyarrow"
            )
        import pyarrow.parquet as pq

        rows = pq.ParquetFile(path).read().to_pylist()
        return [
            row_to_bar({k: "" if v is None else str(v) for k, v in r.items()})
            for r in rows
        ]

    def _atomic_write_parquet(self, path: Path, bars: list[MarketBar]) -> None:
        if importlib.util.find_spec("pyarrow") is None:
            raise MDUnsupportedFeature(
                "Writing Parquet segments requires optional dependency pyarrow"
            )
        import pyarrow as pa
        import pyarrow.parquet as pq

        rows = [{name: getattr(b, name) for name in self.fields} for b in bars]
        table = pa.Table.from_pylist(rows, schema=None)
        fd, tmp = tempfile.mkstemp(
            prefix=f".{path.name}.", suffix=".parquet", dir=str(path.parent)
        )
        os.close(fd)
        try:
            pq.write_table(table, tmp)
            os.replace(tmp, path)
        finally:
            if os.path.exists(tmp):
                os.unlink(tmp)

    def _atomic_write_text(self, path: Path, content: str) -> None:
        fd, tmp = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
        with os.fdopen(fd, "w") as f:
            f.write(content)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
