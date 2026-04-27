from __future__ import annotations
from dataclasses import dataclass
from marketdata_provider.timeframes import next_open_time_ms
from marketdata_provider.errors import MDPaginationStalled

@dataclass(frozen=True, slots=True)
class PageRequest:
    start: int
    end: int
    limit: int

def next_cursor(last_open_time_ms: int, timeframe: str, current_cursor: int) -> int:
    nxt = next_open_time_ms(last_open_time_ms, timeframe)
    if nxt <= current_cursor:
        raise MDPaginationStalled(f"Pagination cursor stalled at {current_cursor}")
    return nxt
