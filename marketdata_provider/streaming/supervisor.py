from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Iterable

from marketdata_provider.core.bar import MarketBar
from marketdata_provider.errors import MDNetworkUnavailable
from marketdata_provider.store.candle_store import CandleStore, CommitResult
from marketdata_provider.store.current_store import StreamCheckpoint
from marketdata_provider.streaming.kline import KlineUpdate
from marketdata_provider.timeframes import timeframe_ms


@dataclass(frozen=True, slots=True)
class MockStreamResult:
    processed: int
    open_updates: int
    closed_commits: int
    reconnects: int
    backfilled: int
    dropped: int = 0
    coalesced: int = 0
    diagnostics: list[str] | None = None


class MockWebSocketSupervisor:
    """Deterministic WS supervisor foundation for tests/local CLI.

    It intentionally does not connect to exchanges. Live WS mode must be implemented
    behind env-gated networking in a later stage.
    """

    def __init__(self, store: CandleStore):
        self.store = store

    def run(self, updates: Iterable[KlineUpdate], *, backfill_bars: Iterable[MarketBar] = (), reconnect_after: int | None = None, queue_maxsize: int | None = None) -> MockStreamResult:
        processed = open_updates = closed_commits = reconnects = backfilled = dropped = coalesced = 0
        diagnostics: list[str] = []
        if queue_maxsize is not None:
            from marketdata_provider.streaming.live import CoalescingKlineQueue
            q = CoalescingKlineQueue(queue_maxsize)
            for u in updates:
                q.put(u)
            updates = q.drain()
            dropped = q.dropped
            coalesced = q.coalesced
            diagnostics.extend(d.code for d in q.diagnostics)
        for update in updates:
            if reconnect_after is not None and processed == reconnect_after:
                reconnects += 1
                for b in backfill_bars:
                    if b.is_closed:
                        self.store.commit_closed(b, event_time=b.downloaded_at, received_at=b.downloaded_at)
                        backfilled += 1
                cp = self.store.current.get_checkpoint(exchange=update.exchange, market=update.market, symbol=update.symbol, timeframe=update.timeframe, source_kind=update.source_kind)
                self.store.current.update_checkpoint(StreamCheckpoint(exchange=update.exchange, market=update.market, symbol=update.symbol, timeframe=update.timeframe, source_transport="ws", source_kind=update.source_kind, last_closed_bar_time=cp.last_closed_bar_time if cp else None, last_event_time=cp.last_event_time if cp else None, last_received_at=cp.last_received_at if cp else None, last_reconnect_at=update.received_at or update.event_time, consecutive_reconnects=(cp.consecutive_reconnects if cp else 0) + 1, status="reconnected", updated_at=update.received_at or update.event_time))
            bar = update.to_market_bar()
            result: CommitResult = self.store.commit_closed(bar, event_time=update.event_time, received_at=update.received_at, raw_event_id=update.raw_event_id) if update.is_closed else self.store.upsert_open(bar, event_time=update.event_time, received_at=update.received_at, raw_event_id=update.raw_event_id)
            if result.status in {"upserted", "ignored"}:
                open_updates += 1
            if result.status in {"committed", "duplicate"}:
                closed_commits += 1
            processed += 1
        return MockStreamResult(processed=processed, open_updates=open_updates, closed_commits=closed_commits, reconnects=reconnects, backfilled=backfilled, dropped=dropped, coalesced=coalesced, diagnostics=diagnostics)


def require_live_stream_enabled() -> None:
    if os.getenv("RUN_MARKETDATA_STREAM_TESTS") != "1" and os.getenv("MARKETDATA_ALLOW_STREAM") != "1":
        raise MDNetworkUnavailable("Live WebSocket streaming is disabled unless RUN_MARKETDATA_STREAM_TESTS=1 or MARKETDATA_ALLOW_STREAM=1; use --mock-events for local deterministic stream ingestion")


def overlap_start(last_closed: int | None, timeframe: str, overlap_bars: int) -> int | None:
    if last_closed is None:
        return None
    return max(0, last_closed - timeframe_ms(timeframe) * overlap_bars)
