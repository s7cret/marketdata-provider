from __future__ import annotations

import json
import os
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Literal

from marketdata_provider.core.bar import MarketBar
from marketdata_provider.providers import OfflineDataProvider
from marketdata_provider.store.candle_store import CandleStore
from marketdata_provider.store.segment_store import market_bar_checksum
from marketdata_provider.timeframes import canonical_timeframe, close_time_ms

RepairPolicy = Literal["strict", "non-strict"]


@dataclass(frozen=True, slots=True)
class AuditIssue:
    code: str
    time: int
    message: str


@dataclass(frozen=True, slots=True)
class AuditReport:
    ok: bool
    checked: int
    issues: list[AuditIssue]


@dataclass(frozen=True, slots=True)
class RepairLog:
    schema_version: str
    exchange: str
    market: str
    symbol: str
    timeframe: str
    policy: str
    checked: int
    changed: int
    issues: list[AuditIssue]
    applied: bool


def market_bar_from_bar(bar, *, exchange: str, market: str, symbol: str, timeframe: str, source_transport: str = "rest", source_kind: str = "trade_kline") -> MarketBar:
    return MarketBar(time=bar.time, open=bar.open, high=bar.high, low=bar.low, close=bar.close, volume=bar.volume, time_close=bar.time_close or close_time_ms(bar.time, timeframe), exchange=exchange.lower(), market=market.lower(), symbol=symbol.upper(), timeframe=canonical_timeframe(timeframe), source_transport=source_transport, source_kind=source_kind, is_closed=True)


def load_repair_source(path: str | Path, *, exchange: str, market: str, symbol: str, timeframe: str, source_transport: str = "rest", source_kind: str = "trade_kline") -> list[MarketBar]:
    bars = OfflineDataProvider(path, timeframe=timeframe).get_bars(symbol, timeframe, None, None)
    return [market_bar_from_bar(b, exchange=exchange, market=market, symbol=symbol, timeframe=timeframe, source_transport=source_transport, source_kind=source_kind) for b in bars]


def _same_candle_values(a: MarketBar, b: MarketBar) -> bool:
    return (a.time, a.time_close, a.open, a.high, a.low, a.close, a.volume, a.quote_volume, a.turnover, a.trades_count) == (b.time, b.time_close, b.open, b.high, b.low, b.close, b.volume, b.quote_volume, b.turnover, b.trades_count)


def audit_against_source(store: CandleStore, source_bars: list[MarketBar], *, exchange: str, market: str, symbol: str, timeframe: str, source_kind: str = "trade_kline", strict: bool = False) -> AuditReport:
    existing = {b.time: b for b in store.get_market_bars(exchange=exchange, market=market, symbol=symbol, timeframe=timeframe, source_kind=source_kind)}
    source = {b.time: b for b in source_bars}
    issues: list[AuditIssue] = []
    for src in source_bars:
        cur = existing.get(src.time)
        if cur is None:
            issues.append(AuditIssue("MD_AUDIT_MISSING_BAR", src.time, "bar missing from finalized store"))
        elif not _same_candle_values(cur, src):
            issues.append(AuditIssue("MD_WS_REST_CANDLE_MISMATCH", src.time, "stored candle differs from source candle"))
    if strict:
        for t in sorted(set(existing) - set(source)):
            issues.append(AuditIssue("MD_AUDIT_EXTRA_BAR", t, "bar exists in store but not in reconciliation source"))
    return AuditReport(ok=not issues, checked=len(source_bars), issues=issues)


def repair_from_source(store: CandleStore, source_bars: list[MarketBar], *, exchange: str, market: str, symbol: str, timeframe: str, source_kind: str = "trade_kline", policy: RepairPolicy = "non-strict", log_path: str | Path | None = None) -> RepairLog:
    strict = policy == "strict"
    report = audit_against_source(store, source_bars, exchange=exchange, market=market, symbol=symbol, timeframe=timeframe, source_kind=source_kind, strict=strict)
    existing = {b.time: b for b in store.get_market_bars(exchange=exchange, market=market, symbol=symbol, timeframe=timeframe, source_kind=source_kind)}
    source = {b.time: b for b in source_bars}
    changed = 0
    applied = False
    for src in source_bars:
        cur = existing.get(src.time)
        if cur is None or not _same_candle_values(cur, src):
            existing[src.time] = src
            changed += 1
    if strict:
        for t in sorted(set(existing) - set(source)):
            existing.pop(t)
            changed += 1
    if changed:
        store.segments.replace_all(list(existing.values()), exchange=exchange, market=market, symbol=symbol, timeframe=timeframe, source_kind=source_kind)
        applied = True
    log = RepairLog("stage-d-repair-1", exchange.lower(), market.lower(), symbol.upper(), canonical_timeframe(timeframe), policy, report.checked, changed, report.issues, applied)
    if log_path is not None:
        write_repair_log(log_path, log)
    return log


def repair_log_path(root: str | Path, *, exchange: str, market: str, symbol: str, timeframe: str) -> Path:
    return Path(root) / "repair-logs" / f"{exchange.lower()}-{market.lower()}-{symbol.upper()}-{canonical_timeframe(timeframe)}.json"


def write_repair_log(path: str | Path, log: RepairLog) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    payload = asdict(log)
    fd, tmp = tempfile.mkstemp(prefix=f".{p.name}.", dir=str(p.parent))
    with os.fdopen(fd, "w") as f:
        f.write(json.dumps(payload, sort_keys=True, indent=2) + "\n"); f.flush(); os.fsync(f.fileno())
    os.replace(tmp, p)


def read_repair_logs(root: str | Path) -> list[dict]:
    return [json.loads(p.read_text()) for p in sorted(Path(root).glob("repair-logs/*.json"))]
