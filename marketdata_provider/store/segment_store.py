from __future__ import annotations

import csv
import hashlib
import importlib.util
import json
import os
import sqlite3
import tempfile
from dataclasses import asdict, dataclass
from decimal import Decimal
from pathlib import Path
from typing import Iterable, Literal

from marketdata_provider.core.bar import MarketBar, RUNTIME_CONTRACT_VERSION
from marketdata_provider.errors import MDInvalidExchangeResponse, MDUnsupportedFeature
from marketdata_provider.timeframes import canonical_timeframe
from marketdata_provider.validation import validate_bars

SegmentFormat = Literal["csv", "parquet"]


def _safe(v: str) -> str:
    return v.upper().replace(":", "_").replace("/", "_")


def _canon_number(v: float | int | None) -> str | None:
    if v is None:
        return None
    d = Decimal(str(v)).normalize()
    if d == 0:
        return "0"
    return format(d, "f")


def market_bar_checksum(bar: MarketBar) -> str:
    return bars_checksum([bar])


def bars_checksum(bars: Iterable[MarketBar]) -> str:
    h = hashlib.sha256()
    for b in sorted(bars, key=lambda x: x.time):
        row = {
            "close": _canon_number(b.close),
            "exchange": b.exchange.lower(),
            "high": _canon_number(b.high),
            "is_closed": bool(b.is_closed),
            "low": _canon_number(b.low),
            "market": b.market.lower(),
            "open": _canon_number(b.open),
            "quote_volume": _canon_number(b.quote_volume),
            "source_kind": b.source_kind,
            "source_transport": b.source_transport,
            "symbol": b.symbol.upper(),
            "time": int(b.time),
            "time_close": int(b.time_close) if b.time_close is not None else None,
            "timeframe": canonical_timeframe(b.timeframe),
            "trades_count": int(b.trades_count) if b.trades_count is not None else None,
            "turnover": _canon_number(b.turnover),
            "volume": _canon_number(b.volume),
        }
        h.update(json.dumps(row, sort_keys=True, separators=(",", ":")).encode())
        h.update(b"\n")
    return h.hexdigest()


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
        "time", "open", "high", "low", "close", "volume", "time_close",
        "exchange", "market", "symbol", "timeframe", "quote_volume", "turnover",
        "trades_count", "taker_buy_base_volume", "taker_buy_quote_volume",
        "source_transport", "source_kind", "is_closed", "downloaded_at",
    ]

    def __init__(self, root: str | Path, *, data_format: SegmentFormat = "csv"):
        if data_format not in {"csv", "parquet"}:
            raise MDUnsupportedFeature(f"Unsupported segment data format: {data_format}")
        if data_format == "parquet" and importlib.util.find_spec("pyarrow") is None:
            raise MDUnsupportedFeature("Parquet segment format requires optional dependency pyarrow")
        self.root = Path(root)
        self.data_format: SegmentFormat = data_format
        self.root.mkdir(parents=True, exist_ok=True)
        self.index_path = self.root / "index.sqlite"
        self._init_index()

    def _init_index(self) -> None:
        with sqlite3.connect(self.index_path) as db:
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
                db.execute("ALTER TABLE marketdata_segments ADD COLUMN data_format TEXT NOT NULL DEFAULT 'csv'")

    def _dir(self, *, exchange: str, market: str, symbol: str, timeframe: str, source_kind: str) -> Path:
        return self.root / "v1" / f"exchange={exchange.lower()}" / f"market={market.lower()}" / f"symbol={_safe(symbol)}" / f"source={source_kind}" / f"timeframe={canonical_timeframe(timeframe)}"

    def _paths(self, *, data_format: str | None = None, **key: str) -> tuple[Path, Path]:
        fmt = data_format or self.data_format
        d = self._dir(**key)
        suffix = "parquet" if fmt == "parquet" else "csv"
        return d / f"bars.{suffix}", d / "manifest.json"

    def read_all(self, *, exchange: str, market: str, symbol: str, timeframe: str, source_kind: str = "trade_kline", start: int | None = None, end: int | None = None) -> list[MarketBar]:
        manifest_path = self._dir(exchange=exchange, market=market, symbol=symbol, timeframe=timeframe, source_kind=source_kind) / "manifest.json"
        fmt = self.data_format
        if manifest_path.exists():
            manifest = json.loads(manifest_path.read_text())
            fmt = manifest.get("data_format", fmt)
        data_path, _ = self._paths(exchange=exchange, market=market, symbol=symbol, timeframe=timeframe, source_kind=source_kind, data_format=fmt)
        if not data_path.exists():
            return list()
        bars = self._read_parquet(data_path) if fmt == "parquet" else self._read_csv(data_path)
        validate_bars([b.to_bar() for b in bars])
        if manifest_path.exists():
            manifest = json.loads(manifest_path.read_text())
            if manifest.get("runtime_contract_version") != RUNTIME_CONTRACT_VERSION:
                raise MDInvalidExchangeResponse("Unsupported segment runtime contract", details=manifest)
            actual = bars_checksum(bars)
            if actual != manifest.get("checksum"):
                raise MDInvalidExchangeResponse("Segment checksum mismatch", details={"expected": manifest.get("checksum"), "actual": actual})
        return [b for b in bars if (start is None or b.time >= start) and (end is None or b.time < end)]

    def get(self, key: tuple[str, str, str, str, str, int]) -> MarketBar | None:
        exchange, market, symbol, timeframe, source_kind, open_time = key
        for b in self.read_all(exchange=exchange, market=market, symbol=symbol, timeframe=timeframe, source_kind=source_kind, start=open_time, end=open_time + 1):
            if b.time == open_time:
                return b
        return None

    def replace_all(self, bars: list[MarketBar], *, exchange: str, market: str, symbol: str, timeframe: str, source_kind: str = "trade_kline", data_format: SegmentFormat | None = None) -> SegmentManifest:
        fmt: SegmentFormat = data_format or self.data_format
        if fmt == "parquet" and importlib.util.find_spec("pyarrow") is None:
            raise MDUnsupportedFeature("Parquet segment format requires optional dependency pyarrow")
        bars = sorted(bars, key=lambda b: b.time)
        validate_bars([b.to_bar() for b in bars])
        data_path, manifest_path = self._paths(exchange=exchange, market=market, symbol=symbol, timeframe=timeframe, source_kind=source_kind, data_format=fmt)
        data_path.parent.mkdir(parents=True, exist_ok=True)
        checksum = bars_checksum(bars)
        manifest = SegmentManifest(
            runtime_contract_version=RUNTIME_CONTRACT_VERSION,
            schema_version=f"stage-d-{fmt}-1",
            exchange=exchange.lower(), market=market.lower(), symbol=symbol.upper(), timeframe=canonical_timeframe(timeframe),
            source_transport=bars[0].source_transport if bars else "ws", source_kind=source_kind,
            rows_count=len(bars), start_time=bars[0].time if bars else None, end_time=bars[-1].time if bars else None,
            checksum=checksum, data_format=fmt,
        )
        if fmt == "parquet":
            self._atomic_write_parquet(data_path, bars)
        else:
            self._atomic_write_csv(data_path, bars)
        other = data_path.with_suffix(".csv" if fmt == "parquet" else ".parquet")
        if other.exists():
            other.unlink()
        self._atomic_write_text(manifest_path, json.dumps(asdict(manifest), sort_keys=True, indent=2) + "\n")
        with sqlite3.connect(self.index_path) as db:
            db.execute("PRAGMA journal_mode=WAL")
            if bars:
                db.execute(
                    "INSERT OR REPLACE INTO marketdata_segments(exchange,market,symbol,timeframe,start_time,end_time,rows_count,source_transport,source_kind,checksum,downloaded_at,data_format) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                    (manifest.exchange, manifest.market, manifest.symbol, manifest.timeframe, manifest.start_time, manifest.end_time, manifest.rows_count, manifest.source_transport, manifest.source_kind, manifest.checksum, max((b.downloaded_at or 0) for b in bars), fmt),
                )
        return manifest

    def upsert_closed(self, bar: MarketBar) -> SegmentManifest:
        existing = self.read_all(exchange=bar.exchange, market=bar.market, symbol=bar.symbol, timeframe=bar.timeframe, source_kind=bar.source_kind)
        by_time = {b.time: b for b in existing}
        by_time[bar.time] = bar
        return self.replace_all(list(by_time.values()), exchange=bar.exchange, market=bar.market, symbol=bar.symbol, timeframe=bar.timeframe, source_kind=bar.source_kind)

    def vacuum(self) -> dict[str, int]:
        removed = 0
        for path in self.root.glob("v1/**/bars.*"):
            manifest = path.parent / "manifest.json"
            if not manifest.exists():
                continue
            fmt = json.loads(manifest.read_text()).get("data_format", "csv")
            expected = path.parent / f"bars.{fmt}"
            if path != expected:
                path.unlink(); removed += 1
        with sqlite3.connect(self.index_path) as db:
            db.execute("VACUUM")
        return {"removed_stale_data_files": removed}

    def compact(self, *, exchange: str, market: str, symbol: str, timeframe: str, source_kind: str = "trade_kline", data_format: SegmentFormat | None = None) -> SegmentManifest:
        bars = self.read_all(exchange=exchange, market=market, symbol=symbol, timeframe=timeframe, source_kind=source_kind)
        return self.replace_all(bars, exchange=exchange, market=market, symbol=symbol, timeframe=timeframe, source_kind=source_kind, data_format=data_format)

    def _read_csv(self, path: Path) -> list[MarketBar]:
        return [self._row_to_bar(r) for r in csv.DictReader(path.open(newline=""))]

    def _atomic_write_csv(self, path: Path, bars: list[MarketBar]) -> None:
        fd, tmp = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
        with os.fdopen(fd, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=self.fields)
            w.writeheader()
            for b in bars:
                w.writerow({name: getattr(b, name) for name in self.fields})
            f.flush(); os.fsync(f.fileno())
        os.replace(tmp, path)

    def _read_parquet(self, path: Path) -> list[MarketBar]:
        if importlib.util.find_spec("pyarrow") is None:
            raise MDUnsupportedFeature("Reading Parquet segments requires optional dependency pyarrow")
        import pyarrow.parquet as pq
        rows = pq.ParquetFile(path).read().to_pylist()
        return [self._row_to_bar({k: "" if v is None else str(v) for k, v in r.items()}) for r in rows]

    def _atomic_write_parquet(self, path: Path, bars: list[MarketBar]) -> None:
        if importlib.util.find_spec("pyarrow") is None:
            raise MDUnsupportedFeature("Writing Parquet segments requires optional dependency pyarrow")
        import pyarrow as pa
        import pyarrow.parquet as pq
        rows = [{name: getattr(b, name) for name in self.fields} for b in bars]
        table = pa.Table.from_pylist(rows, schema=None)
        fd, tmp = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".parquet", dir=str(path.parent))
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
            f.write(content); f.flush(); os.fsync(f.fileno())
        os.replace(tmp, path)

    def _row_to_bar(self, r: dict[str, str]) -> MarketBar:
        def opt_float(name: str) -> float | None:
            return float(r[name]) if r.get(name) not in (None, "") else None
        def opt_int(name: str) -> int | None:
            return int(float(r[name])) if r.get(name) not in (None, "") else None
        return MarketBar(
            time=int(r["time"]), open=float(r["open"]), high=float(r["high"]), low=float(r["low"]), close=float(r["close"]), volume=float(r["volume"]), time_close=opt_int("time_close"),
            exchange=r.get("exchange", "").lower(), market=r.get("market", "").lower(), symbol=r.get("symbol", "").upper(), timeframe=canonical_timeframe(r.get("timeframe", "1m")),
            quote_volume=opt_float("quote_volume"), turnover=opt_float("turnover"), trades_count=opt_int("trades_count"), taker_buy_base_volume=opt_float("taker_buy_base_volume"), taker_buy_quote_volume=opt_float("taker_buy_quote_volume"),
            source_transport=r.get("source_transport") or "ws", source_kind=r.get("source_kind") or "trade_kline", is_closed=(r.get("is_closed", "True") in {"True", "true", "1", "1.0", "True"}), downloaded_at=opt_int("downloaded_at"),
        )
