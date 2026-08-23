from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from marketdata_provider.canonical.bar import CanonicalBarV2


@dataclass(frozen=True, slots=True)
class LiveKlineEvent:
    bar: CanonicalBarV2
    event_time: int
    received_at: int | None = None
    raw_payload: dict[str, Any] = field(default_factory=dict)
    diagnostic_code: str | None = None
