"""OpenPine 5.0 marketdata adapter. Canonical types come from openpine-contracts."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Mapping, Sequence

from openpine_contracts import Finality, RevisionState, content_hash, decimal_string
from openpine_contracts.errors import MoneyError

from marketdata_provider.contracts.errors import InvalidBarError
from marketdata_provider.errors import MDValidationError
from marketdata_provider.timeframes import close_time_ms

SCHEMA_ID_BAR = "openpine.marketdata.bar.v2"
SCHEMA_ID_MARKETDATA = "openpine.marketdata.v2"


def resolve_close_time_utc_ms(
    *,
    open_time_utc_ms: int,
    close_time_utc_ms: int | None,
    timeframe: str,
) -> int:
    if close_time_utc_ms is not None and close_time_utc_ms > open_time_utc_ms:
        return int(close_time_utc_ms)
    try:
        computed = close_time_ms(open_time_utc_ms, timeframe)
    except Exception as exc:
        raise InvalidBarError(
            "close_time missing and timeframe cannot compute it"
        ) from exc
    if computed <= open_time_utc_ms:
        raise InvalidBarError("computed close_time must be greater than open_time")
    return computed


def bar_finality(*, close_time_ms: int, server_time_ms: int | None) -> Finality:
    if server_time_ms is None:
        raise MDValidationError(
            "server_time_ms required for finality",
            details={"code": "FINALITY_EVIDENCE_MISSING"},
        )
    return Finality.FINAL if server_time_ms >= close_time_ms else Finality.OPEN


def _decimal_field(name: str, value: str | int | Decimal) -> str:
    try:
        return decimal_string(value)
    except MoneyError as exc:
        raise InvalidBarError(f"{name} is not a finite decimal") from exc


def _require_text(name: str, value: str) -> str:
    text = value.strip()
    if not text:
        raise InvalidBarError(f"{name} must not be empty")
    return text


@dataclass(frozen=True, slots=True)
class CanonicalBar:
    schema_id: str
    series_id: str
    instrument_id: str
    timeframe: str
    open_time_utc_ms: int
    close_time_utc_ms: int
    open: str
    high: str
    low: str
    close: str
    volume: str
    finality: Finality
    revision_state: RevisionState
    revision: int
    provider: str
    snapshot_id: str
    bar_content_hash: str
    volume_scale: int | None = None
    provider_revision: str | None = None
    observed_at_utc_ms: int | None = None
    ingested_at_utc_ms: int | None = None
    session_id: str | None = None
    superseded_bar_hash: str | None = None

    def __post_init__(self) -> None:
        if self.schema_id != SCHEMA_ID_BAR:
            raise InvalidBarError("schema_id must be openpine.marketdata.bar.v2")
        if self.close_time_utc_ms <= self.open_time_utc_ms:
            raise InvalidBarError("open_time_utc_ms < close_time_utc_ms required")
        if self.revision < 0:
            raise InvalidBarError("revision must be >= 0")
        ohlc = {
            "open": Decimal(self.open),
            "high": Decimal(self.high),
            "low": Decimal(self.low),
            "close": Decimal(self.close),
            "volume": Decimal(self.volume),
        }
        if ohlc["volume"] < 0:
            raise InvalidBarError("volume must be nonnegative")
        if not (ohlc["low"] <= min(ohlc["open"], ohlc["close"]) <= max(ohlc["open"], ohlc["close"]) <= ohlc["high"]):
            raise InvalidBarError("OHLC invariant violated")

    def to_contract_payload(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "schema_id": self.schema_id,
            "series_id": self.series_id,
            "instrument_id": self.instrument_id,
            "timeframe": self.timeframe,
            "open_time_utc_ms": self.open_time_utc_ms,
            "close_time_utc_ms": self.close_time_utc_ms,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "finality": self.finality.value,
            "revision_state": self.revision_state.value,
            "revision": self.revision,
            "provider": self.provider,
            "snapshot_id": self.snapshot_id,
            "bar_content_hash": self.bar_content_hash,
        }
        if self.volume_scale is not None:
            payload["volume_scale"] = self.volume_scale
        if self.provider_revision is not None:
            payload["provider_revision"] = self.provider_revision
        if self.observed_at_utc_ms is not None:
            payload["observed_at_utc_ms"] = self.observed_at_utc_ms
        if self.ingested_at_utc_ms is not None:
            payload["ingested_at_utc_ms"] = self.ingested_at_utc_ms
        if self.session_id is not None:
            payload["session_id"] = self.session_id
        if self.superseded_bar_hash is not None:
            payload["superseded_bar_hash"] = self.superseded_bar_hash
        return payload


def bar_content_hash(payload: Mapping[str, object]) -> str:
    body = {
        key: payload[key]
        for key in (
            "instrument_id",
            "timeframe",
            "open_time_utc_ms",
            "close_time_utc_ms",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "finality",
            "revision_state",
            "revision",
            "provider",
        )
        if key in payload
    }
    return content_hash(body, schema_id=SCHEMA_ID_BAR)


def make_canonical_bar(
    *,
    instrument_id: str,
    timeframe: str,
    open_time_utc_ms: int,
    close_time_utc_ms: int | None,
    open: str | int | Decimal,
    high: str | int | Decimal,
    low: str | int | Decimal,
    close: str | int | Decimal,
    volume: str | int | Decimal,
    finality: Finality,
    snapshot_id: str,
    provider: str,
    revision: int = 0,
    revision_state: RevisionState = RevisionState.ORIGINAL,
    series_id: str | None = None,
) -> CanonicalBar:
    if isinstance(open, float) or isinstance(high, float) or isinstance(low, float) or isinstance(close, float) or isinstance(volume, float):
        raise InvalidBarError("float is forbidden on canonical bar boundary")
    resolved_close = resolve_close_time_utc_ms(
        open_time_utc_ms=open_time_utc_ms,
        close_time_utc_ms=close_time_utc_ms,
        timeframe=timeframe,
    )
    instrument = _require_text("instrument_id", instrument_id)
    tf = _require_text("timeframe", timeframe)
    provider_id = _require_text("provider", provider)
    snap = _require_text("snapshot_id", snapshot_id)
    series = series_id or f"{provider_id}:{instrument}:{tf}"
    open_s = _decimal_field("open", open)
    high_s = _decimal_field("high", high)
    low_s = _decimal_field("low", low)
    close_s = _decimal_field("close", close)
    volume_s = _decimal_field("volume", volume)
    hash_body = {
        "instrument_id": instrument,
        "timeframe": tf,
        "open_time_utc_ms": int(open_time_utc_ms),
        "close_time_utc_ms": resolved_close,
        "open": open_s,
        "high": high_s,
        "low": low_s,
        "close": close_s,
        "volume": volume_s,
        "finality": finality.value,
        "revision_state": revision_state.value,
        "revision": int(revision),
        "provider": provider_id,
    }
    return CanonicalBar(
        schema_id=SCHEMA_ID_BAR,
        series_id=series,
        instrument_id=instrument,
        timeframe=tf,
        open_time_utc_ms=int(open_time_utc_ms),
        close_time_utc_ms=resolved_close,
        open=open_s,
        high=high_s,
        low=low_s,
        close=close_s,
        volume=volume_s,
        finality=finality,
        revision_state=revision_state,
        revision=int(revision),
        provider=provider_id,
        snapshot_id=snap,
        bar_content_hash=bar_content_hash(hash_body),
    )


@dataclass(frozen=True, slots=True)
class DataQuery:
    instrument_id: str
    timeframe: str
    start_utc_ms: int
    end_utc_ms: int
    finality_policy: str = "CLOSED_BAR_ONLY"
    provider: str | None = None
    requested_snapshot_id: str | None = None

    def to_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "instrument_id": self.instrument_id,
            "timeframe": self.timeframe,
            "start_utc_ms": self.start_utc_ms,
            "end_utc_ms": self.end_utc_ms,
            "finality_policy": self.finality_policy,
        }
        if self.provider is not None:
            payload["provider"] = self.provider
        if self.requested_snapshot_id is not None:
            payload["requested_snapshot_id"] = self.requested_snapshot_id
        return payload


@dataclass(frozen=True, slots=True)
class BarConflict:
    instrument_id: str
    timeframe: str
    open_time_utc_ms: int
    hashes: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class DataSnapshot:
    schema_id: str
    snapshot_id: str
    query: DataQuery
    bars: tuple[CanonicalBar, ...]
    bar_count: int
    series_hash: str
    created_at_utc_ms: int
    conflicts: tuple[BarConflict, ...] = ()
    provider_revision: str | None = None

    def to_contract_payload(self) -> dict[str, object]:
        return {
            "schema_id": self.schema_id,
            "kind": "snapshot",
            "body": {
                "snapshot_id": self.snapshot_id,
                "query": self.query.to_dict(),
                "bar_count": self.bar_count,
                "series_hash": self.series_hash,
                "created_at_utc_ms": self.created_at_utc_ms,
                "conflicts": [
                    {
                        "instrument_id": item.instrument_id,
                        "timeframe": item.timeframe,
                        "open_time_utc_ms": item.open_time_utc_ms,
                        "hashes": list(item.hashes),
                    }
                    for item in self.conflicts
                ],
                "provider_revision": self.provider_revision,
            },
        }


def _detect_conflicts(bars: Sequence[CanonicalBar]) -> tuple[BarConflict, ...]:
    grouped: dict[tuple[str, str, int], list[CanonicalBar]] = {}
    for bar in bars:
        key = (bar.instrument_id, bar.timeframe, bar.open_time_utc_ms)
        grouped.setdefault(key, []).append(bar)
    conflicts: list[BarConflict] = []
    for key, items in grouped.items():
        hashes = tuple(dict.fromkeys(item.bar_content_hash for item in items))
        if len(hashes) > 1:
            conflicts.append(
                BarConflict(
                    instrument_id=key[0],
                    timeframe=key[1],
                    open_time_utc_ms=key[2],
                    hashes=hashes,
                )
            )
    return tuple(conflicts)


def build_data_snapshot(
    bars: Sequence[CanonicalBar],
    *,
    query: DataQuery,
    created_at_utc_ms: int,
    provider_revision: str | None = None,
) -> DataSnapshot:
    ordered = tuple(sorted(bars, key=lambda bar: (bar.open_time_utc_ms, bar.series_id)))
    if query.finality_policy == "CLOSED_BAR_ONLY":
        open_or_revoked = [
            bar for bar in ordered if bar.finality is not Finality.FINAL or bar.revision_state is RevisionState.REVOKED
        ]
        if open_or_revoked:
            raise MDValidationError(
                "closed snapshot cannot contain OPEN or REVOKED bars",
                details={"count": len(open_or_revoked)},
            )
    conflicts = _detect_conflicts(ordered)
    if conflicts and query.finality_policy == "CLOSED_BAR_ONLY":
        raise MDValidationError(
            "closed snapshot blocked by OHLCV conflict",
            details={"conflicts": len(conflicts)},
        )
    unique: list[CanonicalBar] = []
    seen: set[str] = set()
    for bar in ordered:
        if bar.bar_content_hash in seen:
            continue
        seen.add(bar.bar_content_hash)
        unique.append(bar)
    series_hash = content_hash(
        [bar.bar_content_hash for bar in unique],
        schema_id=SCHEMA_ID_MARKETDATA,
    )
    snapshot_id = content_hash(
        {"query": query.to_dict(), "series_hash": series_hash, "created_at_utc_ms": created_at_utc_ms},
        schema_id=SCHEMA_ID_MARKETDATA,
    )
    return DataSnapshot(
        schema_id=SCHEMA_ID_MARKETDATA,
        snapshot_id=snapshot_id,
        query=query,
        bars=tuple(unique),
        bar_count=len(unique),
        series_hash=series_hash,
        created_at_utc_ms=created_at_utc_ms,
        conflicts=conflicts,
        provider_revision=provider_revision,
    )


def canonical_bars_from_binance_klines(
    rows: Sequence[Sequence[Any]],
    *,
    instrument_id: str,
    timeframe: str,
    snapshot_id: str,
    server_time_ms: int,
    include_open_candle: bool = False,
    provider: str = "binance",
) -> list[CanonicalBar]:
    bars: list[CanonicalBar] = []
    for row in rows:
        if len(row) < 6:
            raise InvalidBarError("Binance kline row too short")
        open_time = int(row[0])
        raw_close = int(row[6]) if len(row) > 6 and row[6] is not None else None
        close_time = resolve_close_time_utc_ms(
            open_time_utc_ms=open_time,
            close_time_utc_ms=raw_close,
            timeframe=timeframe,
        )
        finality = bar_finality(close_time_ms=close_time, server_time_ms=server_time_ms)
        if not include_open_candle and finality is Finality.OPEN:
            continue
        bars.append(
            make_canonical_bar(
                instrument_id=instrument_id,
                timeframe=timeframe,
                open_time_utc_ms=open_time,
                close_time_utc_ms=close_time,
                open=row[1],
                high=row[2],
                low=row[3],
                close=row[4],
                volume=row[5],
                finality=finality,
                snapshot_id=snapshot_id,
                provider=provider,
            )
        )
    bars.sort(key=lambda bar: bar.open_time_utc_ms)
    return bars
