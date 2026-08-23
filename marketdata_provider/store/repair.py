from __future__ import annotations

import csv
import json
import os
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Literal

from openpine_contracts import Finality, RevisionState

from marketdata_provider.core.bar import MarketBar
from marketdata_provider.errors import MDMissingFinality, MDValidationError
from marketdata_provider.store.candle_store import CandleStore
from marketdata_provider.store.segment_checksums import same_canonical_candle
from marketdata_provider.timeframes import canonical_timeframe

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


def market_bar_from_bar(
    bar,
    *,
    exchange: str,
    market: str,
    symbol: str,
    timeframe: str,
    source_transport: str = "rest",
    source_kind: str = "trade_kline",
) -> MarketBar:
    if not isinstance(bar, MarketBar) or bar.is_closed is None:
        raise MDMissingFinality("repair source bar is missing explicit finality")
    if bar.time_close is None:
        raise MDValidationError("repair source bar is missing explicit close time")
    if bar.is_closed is not True:
        raise MDValidationError("repair source bar must be FINAL")
    if not bar.provider or not bar.provider_revision:
        raise MDValidationError("repair source bar is missing provider identity")
    decimal_text = {
        "open": bar.open_text,
        "high": bar.high_text,
        "low": bar.low_text,
        "close": bar.close_text,
        "volume": bar.volume_text,
    }
    missing_text = [
        name
        for name, value in decimal_text.items()
        if not isinstance(value, str) or not value
    ]
    if missing_text:
        raise MDValidationError(
            "repair source bar is missing exact source decimal text: "
            + ", ".join(missing_text)
        )
    return MarketBar(
        time=bar.time,
        open=bar.open,
        high=bar.high,
        low=bar.low,
        close=bar.close,
        volume=bar.volume,
        time_close=bar.time_close,
        exchange=exchange.lower(),
        market=market.lower(),
        symbol=symbol.upper(),
        timeframe=canonical_timeframe(timeframe),
        source_transport=source_transport,
        source_kind=source_kind,
        is_closed=bar.is_closed,
        provider=bar.provider,
        provider_revision=bar.provider_revision,
        revision_state=bar.revision_state,
        revision=bar.revision,
        open_text=decimal_text["open"],
        high_text=decimal_text["high"],
        low_text=decimal_text["low"],
        close_text=decimal_text["close"],
        volume_text=decimal_text["volume"],
        quote_volume=bar.quote_volume,
        turnover=bar.turnover,
        trades_count=bar.trades_count,
        taker_buy_base_volume=bar.taker_buy_base_volume,
        taker_buy_quote_volume=bar.taker_buy_quote_volume,
        downloaded_at=bar.downloaded_at,
    )


def load_repair_source(
    path: str | Path,
    *,
    exchange: str,
    market: str,
    symbol: str,
    timeframe: str,
    source_transport: str = "rest",
    source_kind: str = "trade_kline",
) -> list[MarketBar]:
    source_path = Path(path)
    if source_path.suffix.lower() != ".csv":
        raise MDValidationError("canonical repair source currently requires CSV")
    with source_path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    bars: list[MarketBar] = []
    for row in rows:
        finality_raw = row.get("finality")
        if finality_raw in (None, ""):
            raise MDMissingFinality("repair source row finality is required")
        try:
            finality = Finality(str(finality_raw))
        except ValueError as exc:
            raise MDValidationError("repair source row finality is invalid") from exc
        if finality is not Finality.FINAL:
            raise MDValidationError("repair source row must be FINAL")

        open_time_raw = row.get("time") or row.get("open_time")
        close_time_raw = row.get("time_close") or row.get("close_time")
        if open_time_raw in (None, ""):
            raise MDValidationError("repair source row time is required")
        if close_time_raw in (None, ""):
            raise MDValidationError("repair source row time_close is required")

        provider = str(row.get("provider") or "")
        provider_revision = str(row.get("provider_revision") or "")
        if not provider or not provider_revision:
            raise MDValidationError("repair source row provider identity is required")
        if provider.lower() != exchange.lower():
            raise MDValidationError(
                "repair source row provider does not match exchange"
            )

        revision_state_raw = row.get("revision_state")
        revision_raw = row.get("revision")
        if revision_state_raw in (None, "") or revision_raw in (None, ""):
            raise MDValidationError("repair source row revision identity is required")
        try:
            revision_state = RevisionState(str(revision_state_raw))
            revision = int(str(revision_raw))
        except ValueError as exc:
            raise MDValidationError(
                "repair source row revision identity is invalid"
            ) from exc

        decimal_text = {
            name: str(row.get(name) or "")
            for name in ("open", "high", "low", "close", "volume")
        }
        missing_text = [name for name, value in decimal_text.items() if not value]
        if missing_text:
            raise MDValidationError(
                "repair source row is missing exact source decimal text: "
                + ", ".join(missing_text)
            )
        try:
            source_bar = MarketBar(
                time=int(str(open_time_raw)),
                open=float(decimal_text["open"]),
                high=float(decimal_text["high"]),
                low=float(decimal_text["low"]),
                close=float(decimal_text["close"]),
                volume=float(decimal_text["volume"]),
                time_close=int(str(close_time_raw)),
                is_closed=True,
                provider=provider,
                provider_revision=provider_revision,
                revision_state=revision_state,
                revision=revision,
                open_text=decimal_text["open"],
                high_text=decimal_text["high"],
                low_text=decimal_text["low"],
                close_text=decimal_text["close"],
                volume_text=decimal_text["volume"],
            )
        except (TypeError, ValueError) as exc:
            raise MDValidationError("repair source row is invalid") from exc
        bars.append(
            market_bar_from_bar(
                source_bar,
                exchange=exchange,
                market=market,
                symbol=symbol,
                timeframe=timeframe,
                source_transport=source_transport,
                source_kind=source_kind,
            )
        )
    bars.sort(key=lambda bar: (bar.time, bar.revision))
    return bars


def _same_candle_values(a: MarketBar, b: MarketBar) -> bool:
    return same_canonical_candle(a, b)


def audit_against_source(
    store: CandleStore,
    source_bars: list[MarketBar],
    *,
    exchange: str,
    market: str,
    symbol: str,
    timeframe: str,
    source_kind: str = "trade_kline",
    strict: bool = False,
) -> AuditReport:
    existing = {
        b.time: b
        for b in store.get_market_bars(
            exchange=exchange,
            market=market,
            symbol=symbol,
            timeframe=timeframe,
            source_kind=source_kind,
        )
    }
    source = {b.time: b for b in source_bars}
    issues: list[AuditIssue] = []
    for src in source_bars:
        cur = existing.get(src.time)
        if cur is None:
            issues.append(
                AuditIssue(
                    "MD_AUDIT_MISSING_BAR", src.time, "bar missing from finalized store"
                )
            )
        elif not _same_candle_values(cur, src):
            issues.append(
                AuditIssue(
                    "MD_WS_REST_CANDLE_MISMATCH",
                    src.time,
                    "stored candle differs from source candle",
                )
            )
    if strict:
        for t in sorted(set(existing) - set(source)):
            issues.append(
                AuditIssue(
                    "MD_AUDIT_EXTRA_BAR",
                    t,
                    "bar exists in store but not in reconciliation source",
                )
            )
    return AuditReport(ok=not issues, checked=len(source_bars), issues=issues)


def repair_from_source(
    store: CandleStore,
    source_bars: list[MarketBar],
    *,
    exchange: str,
    market: str,
    symbol: str,
    timeframe: str,
    source_kind: str = "trade_kline",
    policy: RepairPolicy = "non-strict",
    log_path: str | Path | None = None,
) -> RepairLog:
    key = {
        "exchange": exchange,
        "market": market,
        "symbol": symbol,
        "timeframe": timeframe,
        "source_kind": source_kind,
    }
    with store.segments.series_writer_lock(**key):
        return _repair_from_source_locked(
            store,
            source_bars,
            exchange=exchange,
            market=market,
            symbol=symbol,
            timeframe=timeframe,
            source_kind=source_kind,
            policy=policy,
            log_path=log_path,
        )


def _repair_from_source_locked(
    store: CandleStore,
    source_bars: list[MarketBar],
    *,
    exchange: str,
    market: str,
    symbol: str,
    timeframe: str,
    source_kind: str,
    policy: RepairPolicy,
    log_path: str | Path | None,
) -> RepairLog:
    strict = policy == "strict"
    report = audit_against_source(
        store,
        source_bars,
        exchange=exchange,
        market=market,
        symbol=symbol,
        timeframe=timeframe,
        source_kind=source_kind,
        strict=strict,
    )
    existing = {
        b.time: b
        for b in store.get_market_bars(
            exchange=exchange,
            market=market,
            symbol=symbol,
            timeframe=timeframe,
            source_kind=source_kind,
        )
    }
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
        store.segments._replace_all_locked(
            list(existing.values()),
            exchange=exchange,
            market=market,
            symbol=symbol,
            timeframe=timeframe,
            source_kind=source_kind,
        )
        applied = True
    log = RepairLog(
        "stage-d-repair-1",
        exchange.lower(),
        market.lower(),
        symbol.upper(),
        canonical_timeframe(timeframe),
        policy,
        report.checked,
        changed,
        report.issues,
        applied,
    )
    if log_path is not None:
        write_repair_log(log_path, log)
    return log


def repair_log_path(
    root: str | Path, *, exchange: str, market: str, symbol: str, timeframe: str
) -> Path:
    return (
        Path(root)
        / "repair-logs"
        / f"{exchange.lower()}-{market.lower()}-{symbol.upper()}-{canonical_timeframe(timeframe)}.json"
    )


def write_repair_log(path: str | Path, log: RepairLog) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    payload = asdict(log)
    fd, tmp = tempfile.mkstemp(prefix=f".{p.name}.", dir=str(p.parent))
    with os.fdopen(fd, "w") as f:
        f.write(json.dumps(payload, sort_keys=True, indent=2) + "\n")
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, p)


def read_repair_logs(root: str | Path) -> list[dict]:
    return [
        json.loads(p.read_text()) for p in sorted(Path(root).glob("repair-logs/*.json"))
    ]
