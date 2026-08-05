from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from marketdata_provider.core.bar import Bar, MarketBar
from marketdata_provider.errors import MDCacheConflict
from marketdata_provider.store.current_store import CurrentStore, StreamCheckpoint
from marketdata_provider.store.segment_store import SegmentStore, market_bar_checksum


@dataclass(frozen=True, slots=True)
class CommitResult:
    status: str
    diagnostic: str | None = None


class CandleStore:
    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.segments = SegmentStore(self.root)
        self.current = CurrentStore(self.root / "current.sqlite")

    def upsert_open(
        self,
        bar: MarketBar,
        *,
        event_time: int | None = None,
        received_at: int | None = None,
        raw_event_id: str | None = None,
    ) -> CommitResult:
        if bar.is_closed:
            return self.commit_closed(
                bar,
                event_time=event_time,
                received_at=received_at,
                raw_event_id=raw_event_id,
            )
        existing = self.segments.get(
            (
                bar.exchange,
                bar.market,
                bar.symbol,
                bar.timeframe,
                bar.source_kind,
                bar.time,
            )
        )
        if existing is not None:
            return CommitResult("ignored", "MD_WARNING_LATE_OPEN_IGNORED")
        self.current.upsert_current(
            bar,
            event_time=event_time,
            received_at=received_at,
            raw_event_id=raw_event_id,
        )
        self._touch_event_checkpoint(
            bar, event_time=event_time, received_at=received_at, status="open"
        )
        return CommitResult("upserted")

    def commit_closed(
        self,
        bar: MarketBar,
        *,
        event_time: int | None = None,
        received_at: int | None = None,
        raw_event_id: str | None = None,
    ) -> CommitResult:
        if not bar.is_closed:
            return self.upsert_open(
                bar,
                event_time=event_time,
                received_at=received_at,
                raw_event_id=raw_event_id,
            )
        segment_key = {
            "exchange": bar.exchange,
            "market": bar.market,
            "symbol": bar.symbol,
            "timeframe": bar.timeframe,
            "source_kind": bar.source_kind,
        }
        with self.segments.series_writer_lock(**segment_key):
            manifest = self.segments.manifest_for(**segment_key)
            if (
                manifest is not None
                and manifest.data_format == "csv"
                and manifest.end_time is not None
                and bar.time > manifest.end_time
            ):
                self.segments.append_strictly_newer([bar], **segment_key)
                self.current.delete_current(bar)
                self._closed_checkpoint(
                    bar, event_time=event_time, received_at=received_at
                )
                return CommitResult("committed")
            existing = self.segments.get(
                (
                    bar.exchange,
                    bar.market,
                    bar.symbol,
                    bar.timeframe,
                    bar.source_kind,
                    bar.time,
                )
            )
            if existing is not None:
                if market_bar_checksum(existing) == market_bar_checksum(bar):
                    self.current.delete_current(bar)
                    self._closed_checkpoint(
                        bar, event_time=event_time, received_at=received_at
                    )
                    return CommitResult("duplicate")
                raise MDCacheConflict(
                    "Conflicting closed candle",
                    details={
                        "diagnostic": "MD_CACHE_CONFLICT",
                        "time": bar.time,
                        "existing_checksum": market_bar_checksum(existing),
                        "new_checksum": market_bar_checksum(bar),
                    },
                )
            self.segments._upsert_closed_locked(bar)
        self.current.delete_current(bar)
        self._closed_checkpoint(bar, event_time=event_time, received_at=received_at)
        return CommitResult("committed")

    def get_bars(
        self,
        *,
        exchange: str,
        market: str,
        symbol: str,
        timeframe: str,
        start: int | None = None,
        end: int | None = None,
        source_kind: str = "trade_kline",
    ) -> list[Bar]:
        return [
            b.to_bar()
            for b in self.segments.read_all(
                exchange=exchange,
                market=market,
                symbol=symbol,
                timeframe=timeframe,
                source_kind=source_kind,
                start=start,
                end=end,
            )
        ]

    def get_market_bars(
        self,
        *,
        exchange: str,
        market: str,
        symbol: str,
        timeframe: str,
        start: int | None = None,
        end: int | None = None,
        source_kind: str = "trade_kline",
    ) -> list[MarketBar]:
        return self.segments.read_all(
            exchange=exchange,
            market=market,
            symbol=symbol,
            timeframe=timeframe,
            source_kind=source_kind,
            start=start,
            end=end,
        )

    def latest_bar_time(
        self,
        *,
        exchange: str,
        market: str,
        symbol: str,
        timeframe: str,
        source_kind: str = "trade_kline",
    ) -> int | None:
        return self.segments.latest_bar_time(
            exchange=exchange,
            market=market,
            symbol=symbol,
            timeframe=timeframe,
            source_kind=source_kind,
        )

    def get_current_candle(
        self,
        *,
        exchange: str,
        market: str,
        symbol: str,
        timeframe: str,
        source_kind: str = "trade_kline",
    ) -> Bar | None:
        b = self.current.get_current(
            exchange=exchange,
            market=market,
            symbol=symbol,
            timeframe=timeframe,
            source_kind=source_kind,
        )
        return b.to_bar() if b else None

    def get_current_market_candle(
        self,
        *,
        exchange: str,
        market: str,
        symbol: str,
        timeframe: str,
        source_kind: str = "trade_kline",
    ) -> MarketBar | None:
        return self.current.get_current(
            exchange=exchange,
            market=market,
            symbol=symbol,
            timeframe=timeframe,
            source_kind=source_kind,
        )

    def _touch_event_checkpoint(
        self,
        bar: MarketBar,
        *,
        event_time: int | None,
        received_at: int | None,
        status: str,
    ) -> None:
        cp = self.current.get_checkpoint(
            exchange=bar.exchange,
            market=bar.market,
            symbol=bar.symbol,
            timeframe=bar.timeframe,
            source_kind=bar.source_kind,
        )
        self.current.update_checkpoint(
            StreamCheckpoint(
                exchange=bar.exchange,
                market=bar.market,
                symbol=bar.symbol,
                timeframe=bar.timeframe,
                source_transport="ws",
                source_kind=bar.source_kind,
                last_closed_bar_time=cp.last_closed_bar_time if cp else None,
                last_event_time=event_time,
                last_received_at=received_at or bar.downloaded_at,
                last_reconnect_at=cp.last_reconnect_at if cp else None,
                consecutive_reconnects=cp.consecutive_reconnects if cp else 0,
                status=status,
                updated_at=received_at or bar.downloaded_at or event_time or 0,
            )
        )

    def _closed_checkpoint(
        self, bar: MarketBar, *, event_time: int | None, received_at: int | None
    ) -> None:
        cp = self.current.get_checkpoint(
            exchange=bar.exchange,
            market=bar.market,
            symbol=bar.symbol,
            timeframe=bar.timeframe,
            source_kind=bar.source_kind,
        )
        self.current.update_checkpoint(
            StreamCheckpoint(
                exchange=bar.exchange,
                market=bar.market,
                symbol=bar.symbol,
                timeframe=bar.timeframe,
                source_transport="ws",
                source_kind=bar.source_kind,
                last_closed_bar_time=bar.time,
                last_event_time=event_time,
                last_received_at=received_at or bar.downloaded_at,
                last_reconnect_at=cp.last_reconnect_at if cp else None,
                consecutive_reconnects=cp.consecutive_reconnects if cp else 0,
                status="connected",
                updated_at=received_at or bar.downloaded_at or event_time or 0,
            )
        )
