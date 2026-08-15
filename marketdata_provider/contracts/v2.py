from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from marketdata_provider.contracts.errors import InvalidBarError
from marketdata_provider.core.bar import MarketBar


class Finality(StrEnum):
    OPEN = "OPEN"
    FINAL = "FINAL"


class RevisionState(StrEnum):
    ORIGINAL = "ORIGINAL"
    CORRECTED = "CORRECTED"
    REVOKED = "REVOKED"


def _require_decimal_string(name: str, value: Any) -> str:
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise InvalidBarError(f"{name} must be decimal string or int, not {type(value).__name__}")
    text = str(value)
    if text == "":
        raise InvalidBarError(f"{name} must not be empty")
    return text


@dataclass(frozen=True, slots=True)
class CanonicalBarV2:
    schema_id: str
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

    def __post_init__(self) -> None:
        if self.schema_id != "openpine.marketdata.bar.v2":
            raise InvalidBarError("schema_id must be openpine.marketdata.bar.v2")
        if self.close_time_utc_ms <= self.open_time_utc_ms:
            raise InvalidBarError("close_time must be greater than open_time")
        if self.revision < 0:
            raise InvalidBarError("revision must be >= 0")

    def to_contract_payload(self) -> dict[str, object]:
        return {
            "schema_id": self.schema_id,
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
        }


def bar_finality(*, close_time_ms: int, server_time_ms: int | None) -> Finality:
    if server_time_ms is None:
        return Finality.OPEN
    return Finality.FINAL if close_time_ms <= server_time_ms else Finality.OPEN


def market_bar_from_finality(bar: MarketBar, *, server_time_ms: int | None) -> MarketBar:
    close_time = int(bar.time_close if bar.time_close is not None else bar.time)
    is_closed = bar_finality(close_time_ms=close_time, server_time_ms=server_time_ms) is Finality.FINAL
    if bar.is_closed == is_closed:
        return bar
    return MarketBar(
        time=bar.time,
        open=bar.open,
        high=bar.high,
        low=bar.low,
        close=bar.close,
        volume=bar.volume,
        time_close=bar.time_close,
        exchange=bar.exchange,
        market=bar.market,
        symbol=bar.symbol,
        timeframe=bar.timeframe,
        source_transport=bar.source_transport,
        source_kind=bar.source_kind,
        source=bar.source,
        is_closed=is_closed,
        quote_volume=bar.quote_volume,
        turnover=bar.turnover,
        trades_count=bar.trades_count,
        taker_buy_base_volume=bar.taker_buy_base_volume,
        taker_buy_quote_volume=bar.taker_buy_quote_volume,
        downloaded_at=bar.downloaded_at,
        metadata=bar.metadata,
    )


def canonical_bar_v2_from_market_bar(
    bar: MarketBar,
    *,
    snapshot_id: str,
    revision: int = 0,
    revision_state: RevisionState = RevisionState.ORIGINAL,
) -> CanonicalBarV2:
    close_time = int(bar.time_close if bar.time_close is not None else bar.time)
    instrument = ":".join(part for part in (bar.exchange, bar.market, bar.symbol) if part) or bar.symbol
    return CanonicalBarV2(
        schema_id="openpine.marketdata.bar.v2",
        instrument_id=instrument,
        timeframe=bar.timeframe,
        open_time_utc_ms=int(bar.time),
        close_time_utc_ms=close_time,
        open=_require_decimal_string("open", bar.open if isinstance(bar.open, str) else format(bar.open, "f")),
        high=_require_decimal_string("high", bar.high if isinstance(bar.high, str) else format(bar.high, "f")),
        low=_require_decimal_string("low", bar.low if isinstance(bar.low, str) else format(bar.low, "f")),
        close=_require_decimal_string("close", bar.close if isinstance(bar.close, str) else format(bar.close, "f")),
        volume=_require_decimal_string("volume", bar.volume if isinstance(bar.volume, str) else format(bar.volume, "f")),
        finality=Finality.FINAL if bar.is_closed else Finality.OPEN,
        revision_state=revision_state,
        revision=revision,
        provider=bar.exchange or bar.source or "unknown",
        snapshot_id=snapshot_id,
    )


def snapshot_hash(bars: list[CanonicalBarV2]) -> str:
    import hashlib
    import json

    payload = [bar.to_contract_payload() for bar in bars]
    encoded = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
    return f"sha256:{hashlib.sha256(encoded.encode('utf-8')).hexdigest()}"
