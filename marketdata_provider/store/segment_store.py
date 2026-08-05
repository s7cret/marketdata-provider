from __future__ import annotations

import csv
import importlib.util
import json
import os
import sqlite3
import tempfile
import threading
from contextlib import contextmanager
from dataclasses import asdict
from pathlib import Path
from typing import Iterable, Iterator, Literal

from marketdata_provider._pathing import safe_path_part
from marketdata_provider.core.bar import MarketBar, RUNTIME_CONTRACT_VERSION
from marketdata_provider.errors import (
    MDInvalidExchangeResponse,
    MDUnsupportedFeature,
)
from marketdata_provider.store.segment_append import (
    append_strictly_newer as append_segment_tail,
    recover_pending_appends,
    series_lock,
)
from marketdata_provider.store.segment_checksums import (
    CANONICAL_CHECKSUM,
    PERSISTED_MARKET_BAR_FIELDS,
    TAIL_CHAIN_CHECKSUM,
    bars_checksum,
)
from marketdata_provider.store.segment_checksums import (
    market_bar_checksum as market_bar_checksum,
)
from marketdata_provider.store.segment_csv import iter_csv_range, read_csv, seek_csv_near_start
from marketdata_provider.store.segment_manifest import SegmentManifest, load_segment_manifest
from marketdata_provider.store.segment_maintenance import compact_segment, vacuum_segments
from marketdata_provider.store.segment_read import iter_all as read_iter_all
from marketdata_provider.store.segment_read import read_all as read_segment_all
from marketdata_provider.store.segment_replace import (
    begin_replacement,
    finish_replacement,
    recover_pending_replacements,
    recover_replacement_journal,
)
from marketdata_provider.store.segment_rows import parse_bool, row_to_bar
from marketdata_provider.store.segment_stream import replace_all_stream
from marketdata_provider.timeframes import canonical_timeframe
from marketdata_provider.validation import validate_bars

SegmentFormat = Literal["csv", "parquet"]


class SegmentStore:
    """Finalized immutable-candle store with manifest/index semantics.

    CSV is dependency-free and remains the default. Parquet is opt-in through
    pyarrow; manifests and checksums are computed from canonical MarketBar rows
    so CSV and Parquet have identical integrity semantics.
    """

    fields = list(PERSISTED_MARKET_BAR_FIELDS)

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
        self._series_locks = threading.local()
        self._init_index()
        self._recover_pending_appends()
        recover_pending_replacements(self)

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

    @contextmanager
    def series_writer_lock(
        self,
        *,
        exchange: str,
        market: str,
        symbol: str,
        timeframe: str,
        source_kind: str = "trade_kline",
    ) -> Iterator[None]:
        """Serialize every mutation of one physical series across processes."""
        directory = self._dir(
            exchange=exchange,
            market=market,
            symbol=symbol,
            timeframe=timeframe,
            source_kind=source_kind,
        )
        directory.mkdir(parents=True, exist_ok=True)
        lock_path = directory / ".writer.lock"
        active = getattr(self._series_locks, "active", None)
        if active is None:
            active = set()
            self._series_locks.active = active
        key = str(lock_path.resolve())
        if key in active:
            yield
            return
        with series_lock(lock_path):
            active.add(key)
            journal_path = directory / ".append-journal.json"
            replace_journal_path = directory / ".replace-journal.json"
            try:
                if replace_journal_path.exists():
                    recover_replacement_journal(self, replace_journal_path)
                if journal_path.exists():
                    from marketdata_provider.store.segment_append import recover_append_journal

                    recover_append_journal(self, journal_path)
                yield
            finally:
                active.remove(key)

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
        key = {
            "exchange": exchange,
            "market": market,
            "symbol": symbol,
            "timeframe": timeframe,
            "source_kind": source_kind,
        }
        with self.series_writer_lock(**key):
            return self._read_all_locked(start=start, end=end, **key)

    def _read_all_locked(
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
        return read_segment_all(
            self,
            exchange=exchange,
            market=market,
            symbol=symbol,
            timeframe=timeframe,
            source_kind=source_kind,
            start=start,
            end=end,
        )

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
        key = {
            "exchange": exchange,
            "market": market,
            "symbol": symbol,
            "timeframe": timeframe,
            "source_kind": source_kind,
        }
        with self.series_writer_lock(**key):
            yield from read_iter_all(self, start=start, end=end, **key)

    def manifest_for(
        self,
        *,
        exchange: str,
        market: str,
        symbol: str,
        timeframe: str,
        source_kind: str = "trade_kline",
    ) -> SegmentManifest | None:
        return load_segment_manifest(
            self,
            exchange=exchange,
            market=market,
            symbol=symbol,
            timeframe=timeframe,
            source_kind=source_kind,
        )

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
        with self.series_writer_lock(
            exchange=exchange,
            market=market,
            symbol=symbol,
            timeframe=timeframe,
            source_kind=source_kind,
        ):
            return self._replace_all_locked(
                bars,
                exchange=exchange,
                market=market,
                symbol=symbol,
                timeframe=timeframe,
                source_kind=source_kind,
                data_format=data_format,
            )

    def _replace_all_locked(
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
            schema_version=f"stage-d-{fmt}-3",
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
            checksum_algorithm=(
                TAIL_CHAIN_CHECKSUM if fmt == "csv" else CANONICAL_CHECKSUM
            ),
            base_checksum=checksum if fmt == "csv" else None,
            base_rows_count=len(bars) if fmt == "csv" else None,
        )
        old_manifest = self.manifest_for(
            exchange=exchange,
            market=market,
            symbol=symbol,
            timeframe=timeframe,
            source_kind=source_kind,
        )
        old_data_path = None
        if old_manifest is not None:
            old_data_path, _ = self._paths(
                exchange=exchange,
                market=market,
                symbol=symbol,
                timeframe=timeframe,
                source_kind=source_kind,
                data_format=old_manifest.data_format,
            )
        downloaded_at = max((b.downloaded_at or 0) for b in bars) if bars else 0
        journal_path = begin_replacement(
            self,
            old_manifest=old_manifest,
            new_manifest=manifest,
            old_data_path=old_data_path,
            new_data_path=data_path,
            downloaded_at=downloaded_at,
        )
        if fmt == "parquet":
            self._atomic_write_parquet(data_path, bars)
        else:
            self._atomic_write_csv(data_path, bars)
        self._atomic_write_text(
            manifest_path, json.dumps(asdict(manifest), sort_keys=True, indent=2) + "\n"
        )
        self._replace_index_manifest(manifest, downloaded_at=downloaded_at)
        finish_replacement(self, journal_path)
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
        with self.series_writer_lock(
            exchange=exchange,
            market=market,
            symbol=symbol,
            timeframe=timeframe,
            source_kind=source_kind,
        ):
            return replace_all_stream(
                self,
                bars,
                exchange=exchange,
                market=market,
                symbol=symbol,
                timeframe=timeframe,
                source_kind=source_kind,
            )

    def append_strictly_newer(
        self,
        bars: Iterable[MarketBar],
        *,
        exchange: str,
        market: str,
        symbol: str,
        timeframe: str,
        source_kind: str = "trade_kline",
    ) -> SegmentManifest:
        return append_segment_tail(
            self,
            bars,
            exchange=exchange,
            market=market,
            symbol=symbol,
            timeframe=timeframe,
            source_kind=source_kind,
        )

    def _indexed_downloaded_at(self, manifest: SegmentManifest) -> int:
        with self._connect_index() as db:
            row = db.execute(
                "SELECT COALESCE(MAX(downloaded_at), 0) FROM marketdata_segments "
                "WHERE exchange=? AND market=? AND symbol=? AND timeframe=? AND source_kind=?",
                (
                    manifest.exchange,
                    manifest.market,
                    manifest.symbol,
                    manifest.timeframe,
                    manifest.source_kind,
                ),
            ).fetchone()
        return int(row[0]) if row else 0

    def _replace_index_manifest(
        self, manifest: SegmentManifest, *, downloaded_at: int
    ) -> None:
        with self._connect_index() as db:
            db.execute("PRAGMA journal_mode=WAL")
            self._delete_index_rows_for_series(db, manifest)
            self._insert_index_row(db, manifest, downloaded_at=downloaded_at)

    def _write_manifest_and_index(
        self,
        manifest: SegmentManifest,
        *,
        manifest_path: Path,
        downloaded_at: int,
    ) -> None:
        self._atomic_write_text(
            manifest_path, json.dumps(asdict(manifest), sort_keys=True, indent=2) + "\n"
        )
        self._replace_index_manifest(manifest, downloaded_at=downloaded_at)

    def _recover_pending_appends(self) -> None:
        recover_pending_appends(self)

    @staticmethod
    def _fsync_directory(path: Path) -> None:
        fd = os.open(path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def upsert_closed(self, bar: MarketBar) -> SegmentManifest:
        key = {
            "exchange": bar.exchange,
            "market": bar.market,
            "symbol": bar.symbol,
            "timeframe": bar.timeframe,
            "source_kind": bar.source_kind,
        }
        with self.series_writer_lock(**key):
            return self._upsert_closed_locked(bar)

    def _upsert_closed_locked(self, bar: MarketBar) -> SegmentManifest:
        existing = self.read_all(
            exchange=bar.exchange,
            market=bar.market,
            symbol=bar.symbol,
            timeframe=bar.timeframe,
            source_kind=bar.source_kind,
        )
        by_time = {item.time: item for item in existing}
        current = by_time.get(bar.time)
        if current is not None and market_bar_checksum(current) != market_bar_checksum(bar):
            from marketdata_provider.errors import MDCacheConflict

            raise MDCacheConflict(
                "Conflicting closed candle",
                details={"diagnostic": "MD_CACHE_CONFLICT", "time": bar.time},
            )
        by_time[bar.time] = bar
        return self._replace_all_locked(
            list(by_time.values()),
            exchange=bar.exchange,
            market=bar.market,
            symbol=bar.symbol,
            timeframe=bar.timeframe,
            source_kind=bar.source_kind,
        )

    vacuum = vacuum_segments
    compact = compact_segment

    _parse_bool = staticmethod(parse_bool)
    _row_to_bar = staticmethod(row_to_bar)

    _read_csv = staticmethod(read_csv)
    _iter_csv_range = iter_csv_range

    def _validate_manifest_contract(self, manifest: dict[str, object]) -> None:
        if manifest.get("runtime_contract_version") != RUNTIME_CONTRACT_VERSION:
            raise MDInvalidExchangeResponse(
                "Unsupported segment runtime contract", details=manifest
            )

    _seek_csv_near_start = staticmethod(seek_csv_near_start)

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
        self._fsync_directory(path.parent)
