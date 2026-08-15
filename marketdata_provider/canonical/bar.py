from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from decimal import Decimal
from typing import Any

from openpine_contracts import Finality, RevisionState, decimal_string
from openpine_contracts.hashing import content_hash
from openpine_contracts.money import MoneyError

from marketdata_provider.errors import MDValidationError

_TIMEFRAME_MS: dict[str, int] = {
    "1m": 60_000,
    "3m": 180_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "2h": 7_200_000,
    "4h": 14_400_000,
    "1d": 86_400_000,
}


def bar_finality(*, close_time_ms: int, server_time_ms: int | None) -> Finality:
    if server_time_ms is None:
        raise MDValidationError("server_time_ms required")
    if server_time_ms >= close_time_ms:
        return Finality.FINAL
    return Finality.OPEN


def _decimal_field(name: str, value: object) -> str:
    if isinstance(value, float) and not isinstance(value, bool):
        raise MDValidationError(f"{name} float is forbidden on contract boundary")
    try:
        return decimal_string(value)  # type: ignore[arg-type]
    except (MoneyError, TypeError) as exc:
        raise MDValidationError(f"invalid {name}") from exc


def _close_time(
    timeframe: str, open_time_utc_ms: int, close_time_utc_ms: int | None
) -> int:
    if close_time_utc_ms is not None and close_time_utc_ms > open_time_utc_ms:
        return close_time_utc_ms
    duration = _TIMEFRAME_MS.get(timeframe)
    if duration is None:
        raise MDValidationError(f"unsupported timeframe: {timeframe}")
    return open_time_utc_ms + duration - 1


def make_canonical_bar(
    *,
    instrument_id: str,
    timeframe: str,
    open_time_utc_ms: int,
    open: object,
    high: object,
    low: object,
    close: object,
    volume: object,
    snapshot_id: str,
    provider: str,
    finality: Finality | None = None,
    revision_state: RevisionState = RevisionState.ORIGINAL,
    revision: int = 0,
    close_time_utc_ms: int | None = None,
) -> dict[str, Any]:
    if not instrument_id or not timeframe or not snapshot_id or not provider:
        raise MDValidationError("instrument/timeframe/snapshot/provider required")
    if finality is None:
        raise MDValidationError("finality is required")
    if not isinstance(finality, Finality):
        raise MDValidationError("finality must be openpine_contracts.Finality")
    if revision < 0:
        raise MDValidationError("revision must be >= 0")

    open_s = _decimal_field("open", open)
    high_s = _decimal_field("high", high)
    low_s = _decimal_field("low", low)
    close_s = _decimal_field("close", close)
    volume_s = _decimal_field("volume", volume)
    if decimal_string(volume_s).startswith("-") and volume_s != "0":
        raise MDValidationError("volume must be nonnegative")

    ohlc = [decimal_string(part) for part in (open_s, high_s, low_s, close_s)]
    open_d, high_d, low_d, close_d = (Decimal(part) for part in ohlc)
    if not (low_d <= min(open_d, close_d) <= max(open_d, close_d) <= high_d):
        raise MDValidationError("OHLC invariants violated")

    close_ms = _close_time(timeframe, open_time_utc_ms, close_time_utc_ms)
    body: dict[str, Any] = {
        "instrument_id": instrument_id,
        "timeframe": timeframe,
        "open_time_utc_ms": open_time_utc_ms,
        "close_time_utc_ms": close_ms,
        "open": open_s,
        "high": high_s,
        "low": low_s,
        "close": close_s,
        "volume": volume_s,
        "finality": finality,
        "revision_state": revision_state,
        "revision": revision,
        "snapshot_id": snapshot_id,
        "provider": provider,
        "series_id": f"{instrument_id}:{timeframe}",
    }
    body["bar_content_hash"] = content_hash(body, schema_id="openpine.marketdata.v2")
    return body


def build_data_snapshot(
    *,
    snapshot_id: str,
    instrument_id: str,
    timeframe: str,
    start_utc_ms: int,
    end_utc_ms: int,
    bars: Iterable[Mapping[str, Any]],
    finality_policy: str = "CLOSED_BAR_ONLY",
) -> dict[str, Any]:
    if finality_policy not in {"CLOSED_BAR_ONLY", "ALLOW_OPEN"}:
        raise MDValidationError(f"unknown finality_policy: {finality_policy}")
    kept: list[dict[str, Any]] = []
    for bar in bars:
        finality = bar["finality"]
        revision_state = bar.get("revision_state", RevisionState.ORIGINAL)
        if revision_state is RevisionState.REVOKED:
            continue
        if finality_policy == "CLOSED_BAR_ONLY" and finality is not Finality.FINAL:
            continue
        kept.append(dict(bar))
    kept.sort(key=lambda item: int(item["open_time_utc_ms"]))
    query = {
        "instrument_id": instrument_id,
        "timeframe": timeframe,
        "start_utc_ms": start_utc_ms,
        "end_utc_ms": end_utc_ms,
        "finality_policy": finality_policy,
    }
    snapshot: dict[str, Any] = {
        "snapshot_id": snapshot_id,
        "query": query,
        "bar_count": len(kept),
        "bars": kept,
        "created_at_utc_ms": 0,
    }
    snapshot["series_hash"] = content_hash(
        {"bars": [bar["bar_content_hash"] for bar in kept], "query": query},
        schema_id="openpine.marketdata.v2",
    )
    return snapshot


def canonical_bars_from_binance_klines(
    rows: Iterable[Sequence[Any]],
    *,
    instrument_id: str,
    timeframe: str,
    provider: str,
    snapshot_id: str,
    server_time_ms: int | None,
    include_open: bool = False,
) -> list[dict[str, Any]]:
    if server_time_ms is None:
        raise MDValidationError("server_time_ms required")
    bars: list[dict[str, Any]] = []
    for row in rows:
        items = list(row)
        if len(items) < 6:
            raise MDValidationError("kline row too short")
        open_time = int(items[0])
        close_time = int(items[6]) if len(items) > 6 and items[6] is not None else None
        bar = make_canonical_bar(
            instrument_id=instrument_id,
            timeframe=timeframe,
            open_time_utc_ms=open_time,
            close_time_utc_ms=close_time,
            open=items[1],
            high=items[2],
            low=items[3],
            close=items[4],
            volume=items[5],
            snapshot_id=snapshot_id,
            provider=provider,
            finality=bar_finality(
                close_time_ms=_close_time(timeframe, open_time, close_time),
                server_time_ms=server_time_ms,
            ),
        )
        if include_open or bar["finality"] is Finality.FINAL:
            bars.append(bar)
    return bars
