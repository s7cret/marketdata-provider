from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from openpine_contracts import RevisionState

from marketdata_provider.errors import MDValidationError

RUNTIME_CONTRACT_VERSION = "1.4"


@dataclass(frozen=True, slots=True)
class Bar:
    """Canonical market data bar.

    Times are UTC epoch milliseconds. time is the bar open time;
    range queries use start-inclusive/end-exclusive bounds.
    """

    time: int
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0
    time_close: int | None = None


@dataclass(frozen=True, slots=True)
class MarketBar(Bar):
    exchange: str = ""
    market: str = ""
    symbol: str = ""
    timeframe: str = ""
    source_transport: str = "rest"
    source_kind: str = "trade_kline"
    source: str = ""
    is_closed: bool | None = None
    provider: str = ""
    provider_revision: str | None = None
    revision_state: RevisionState = RevisionState.ORIGINAL
    revision: int = 0
    open_text: str | None = None
    high_text: str | None = None
    low_text: str | None = None
    close_text: str | None = None
    volume_text: str | None = None
    quote_volume: float | None = None
    turnover: float | None = None
    trades_count: int | None = None
    taker_buy_base_volume: float | None = None
    taker_buy_quote_volume: float | None = None
    downloaded_at: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.is_closed is None:
            raise MDValidationError("is_closed required")
        if not self.provider and isinstance(self.metadata.get("provider"), str):
            object.__setattr__(self, "provider", self.metadata["provider"])
        if self.provider_revision is None and isinstance(
            self.metadata.get("provider_revision"), str
        ):
            object.__setattr__(
                self, "provider_revision", self.metadata["provider_revision"]
            )
        if self.revision_state is RevisionState.ORIGINAL and self.revision != 0:
            raise MDValidationError("ORIGINAL revision must be 0")
        if self.revision_state is not RevisionState.ORIGINAL and self.revision < 1:
            raise MDValidationError("corrected/revoked revision must be >= 1")

    def to_bar(self) -> Bar:
        return Bar(
            self.time,
            self.open,
            self.high,
            self.low,
            self.close,
            self.volume,
            self.time_close,
        )
