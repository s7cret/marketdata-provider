from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable

from marketdata_provider.core.bar import Bar, RUNTIME_CONTRACT_VERSION
from marketdata_provider.timeframes import canonical_timeframe
from marketdata_provider.validation import validate_bars


@dataclass(frozen=True, slots=True)
class CacheSegmentMetadata:
    runtime_contract_version: str
    exchange: str
    market: str
    symbol: str
    timeframe: str
    start: int | None
    end: int | None
    bars: int
    first_time: int | None
    last_time: int | None
    checksum: str
    data_format: str = "csv"


def _safe(v: str) -> str:
    return v.upper().replace(":", "_").replace("/", "_")


def cache_segment_dir(root: str | Path, *, exchange: str, market: str, symbol: str, timeframe: str) -> Path:
    return Path(root) / exchange.lower() / market.lower() / _safe(symbol) / canonical_timeframe(timeframe)


def bars_checksum(bars: Iterable[Bar]) -> str:
    h = hashlib.sha256()
    for b in bars:
        row = {
            "time": int(b.time),
            "open": float(b.open),
            "high": float(b.high),
            "low": float(b.low),
            "close": float(b.close),
            "volume": float(b.volume),
            "time_close": int(b.time_close) if b.time_close is not None else None,
        }
        h.update(json.dumps(row, sort_keys=True, separators=(",", ":")).encode())
        h.update(b"\n")
    return h.hexdigest()


def write_cache_segment(root: str | Path, bars: list[Bar], *, exchange: str, market: str, symbol: str, timeframe: str, start: int | None = None, end: int | None = None) -> CacheSegmentMetadata:
    validate_bars(bars)
    seg = cache_segment_dir(root, exchange=exchange, market=market, symbol=symbol, timeframe=timeframe)
    seg.mkdir(parents=True, exist_ok=True)
    data_path = seg / "bars.csv"
    with data_path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["time", "open", "high", "low", "close", "volume", "time_close"])
        w.writeheader()
        for b in bars:
            w.writerow({"time": b.time, "open": repr(b.open), "high": repr(b.high), "low": repr(b.low), "close": repr(b.close), "volume": repr(b.volume), "time_close": b.time_close if b.time_close is not None else ""})
    meta = CacheSegmentMetadata(
        runtime_contract_version=RUNTIME_CONTRACT_VERSION,
        exchange=exchange.lower(), market=market.lower(), symbol=symbol.upper(), timeframe=canonical_timeframe(timeframe),
        start=start, end=end, bars=len(bars), first_time=bars[0].time if bars else None, last_time=bars[-1].time if bars else None,
        checksum=bars_checksum(bars),
    )
    (seg / "metadata.json").write_text(json.dumps(asdict(meta), sort_keys=True, indent=2) + "\n")
    return meta


def read_cache_segment(root: str | Path, *, exchange: str, market: str, symbol: str, timeframe: str, start: int | None = None, end: int | None = None, max_bars: int | None = None) -> list[Bar]:
    seg = cache_segment_dir(root, exchange=exchange, market=market, symbol=symbol, timeframe=timeframe)
    data_path = seg / "bars.csv"
    meta_path = seg / "metadata.json"
    if not data_path.exists() or not meta_path.exists():
        from marketdata_provider.errors import MDUnsupportedFeature
        raise MDUnsupportedFeature(f"Cache segment not found: {seg}")
    rows = list(csv.DictReader(data_path.open(newline="")))
    bars = [Bar(int(r["time"]), float(r["open"]), float(r["high"]), float(r["low"]), float(r["close"]), float(r.get("volume") or 0), int(r["time_close"]) if r.get("time_close") else None) for r in rows]
    validate_bars(bars)
    meta = json.loads(meta_path.read_text())
    if meta.get("runtime_contract_version") != RUNTIME_CONTRACT_VERSION:
        from marketdata_provider.errors import MDInvalidExchangeResponse
        raise MDInvalidExchangeResponse(f"Unsupported cache runtime_contract_version: {meta.get('runtime_contract_version')}")
    actual = bars_checksum(bars)
    if actual != meta.get("checksum"):
        from marketdata_provider.errors import MDInvalidExchangeResponse
        raise MDInvalidExchangeResponse("Cache checksum mismatch", details={"expected": meta.get("checksum"), "actual": actual})
    out = [b for b in bars if (start is None or b.time >= start) and (end is None or b.time < end)]
    if max_bars is not None:
        out = out[:max_bars]
    return out
