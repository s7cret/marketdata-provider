from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Iterator
from pathlib import Path

from marketdata_provider.core.bar import MarketBar
from marketdata_provider.timeframes import canonical_timeframe


@dataclass(frozen=True, slots=True)
class StreamCheckpoint:
    exchange: str
    market: str
    symbol: str
    timeframe: str
    source_transport: str = "ws"
    source_kind: str = "trade_kline"
    last_closed_bar_time: int | None = None
    last_event_time: int | None = None
    last_received_at: int | None = None
    last_reconnect_at: int | None = None
    consecutive_reconnects: int = 0
    status: str = "initialized"
    updated_at: int = 0


class CurrentStore:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init()

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        db = sqlite3.connect(self.path)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA journal_mode=WAL")
        try:
            yield db
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    def _init(self) -> None:
        with self._connect() as db:
            db.execute(
                "CREATE TABLE IF NOT EXISTS current_candles ("
                "exchange TEXT NOT NULL, market TEXT NOT NULL, symbol TEXT NOT NULL, source_transport TEXT NOT NULL, source_kind TEXT NOT NULL, timeframe TEXT NOT NULL, "
                "open_time INTEGER NOT NULL, close_time INTEGER NOT NULL, open REAL NOT NULL, high REAL NOT NULL, low REAL NOT NULL, close REAL NOT NULL, volume REAL NOT NULL, "
                "quote_volume REAL, turnover REAL, trades_count INTEGER, taker_buy_base_volume REAL, taker_buy_quote_volume REAL, is_closed INTEGER NOT NULL DEFAULT 0, event_time INTEGER, received_at INTEGER NOT NULL, raw_event_id TEXT, "
                "PRIMARY KEY(exchange, market, symbol, source_transport, source_kind, timeframe, open_time))"
            )
            db.execute(
                "CREATE TABLE IF NOT EXISTS stream_checkpoints ("
                "exchange TEXT NOT NULL, market TEXT NOT NULL, symbol TEXT NOT NULL, source_transport TEXT NOT NULL, source_kind TEXT NOT NULL, timeframe TEXT NOT NULL, "
                "last_closed_bar_time INTEGER, last_event_time INTEGER, last_received_at INTEGER, last_reconnect_at INTEGER, consecutive_reconnects INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL, updated_at INTEGER NOT NULL, "
                "PRIMARY KEY(exchange, market, symbol, source_transport, source_kind, timeframe))"
            )

    def upsert_current(
        self,
        bar: MarketBar,
        *,
        event_time: int | None = None,
        received_at: int | None = None,
        raw_event_id: str | None = None,
    ) -> None:
        with self._connect() as db:
            db.execute(
                "INSERT INTO current_candles(exchange,market,symbol,source_transport,source_kind,timeframe,open_time,close_time,open,high,low,close,volume,quote_volume,turnover,trades_count,taker_buy_base_volume,taker_buy_quote_volume,is_closed,event_time,received_at,raw_event_id) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(exchange,market,symbol,source_transport,source_kind,timeframe,open_time) DO UPDATE SET "
                "close_time=excluded.close_time, open=excluded.open, high=excluded.high, low=excluded.low, close=excluded.close, volume=excluded.volume, quote_volume=excluded.quote_volume, turnover=excluded.turnover, trades_count=excluded.trades_count, taker_buy_base_volume=excluded.taker_buy_base_volume, taker_buy_quote_volume=excluded.taker_buy_quote_volume, is_closed=excluded.is_closed, event_time=excluded.event_time, received_at=excluded.received_at, raw_event_id=excluded.raw_event_id",
                (
                    bar.exchange.lower(),
                    bar.market.lower(),
                    bar.symbol.upper(),
                    bar.source_transport,
                    bar.source_kind,
                    canonical_timeframe(bar.timeframe),
                    bar.time,
                    bar.time_close or bar.time,
                    bar.open,
                    bar.high,
                    bar.low,
                    bar.close,
                    bar.volume,
                    bar.quote_volume,
                    bar.turnover,
                    bar.trades_count,
                    bar.taker_buy_base_volume,
                    bar.taker_buy_quote_volume,
                    int(bar.is_closed),
                    event_time,
                    received_at or bar.downloaded_at or 0,
                    raw_event_id,
                ),
            )

    def get_current(
        self,
        *,
        exchange: str,
        market: str,
        symbol: str,
        timeframe: str,
        source_kind: str = "trade_kline",
    ) -> MarketBar | None:
        with self._connect() as db:
            row = db.execute(
                "SELECT * FROM current_candles WHERE exchange=? AND market=? AND symbol=? AND timeframe=? AND source_kind=? ORDER BY open_time DESC LIMIT 1",
                (
                    exchange.lower(),
                    market.lower(),
                    symbol.upper(),
                    canonical_timeframe(timeframe),
                    source_kind,
                ),
            ).fetchone()
        return self._row_to_bar(row) if row else None

    def delete_current(self, bar: MarketBar) -> None:
        with self._connect() as db:
            db.execute(
                "DELETE FROM current_candles WHERE exchange=? AND market=? AND symbol=? AND source_transport=? AND source_kind=? AND timeframe=? AND open_time=?",
                (
                    bar.exchange.lower(),
                    bar.market.lower(),
                    bar.symbol.upper(),
                    bar.source_transport,
                    bar.source_kind,
                    canonical_timeframe(bar.timeframe),
                    bar.time,
                ),
            )

    def update_checkpoint(self, cp: StreamCheckpoint) -> None:
        with self._connect() as db:
            db.execute(
                "INSERT INTO stream_checkpoints(exchange,market,symbol,source_transport,source_kind,timeframe,last_closed_bar_time,last_event_time,last_received_at,last_reconnect_at,consecutive_reconnects,status,updated_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(exchange,market,symbol,source_transport,source_kind,timeframe) DO UPDATE SET "
                "last_closed_bar_time=excluded.last_closed_bar_time,last_event_time=excluded.last_event_time,last_received_at=excluded.last_received_at,last_reconnect_at=excluded.last_reconnect_at,consecutive_reconnects=excluded.consecutive_reconnects,status=excluded.status,updated_at=excluded.updated_at",
                (
                    cp.exchange.lower(),
                    cp.market.lower(),
                    cp.symbol.upper(),
                    cp.source_transport,
                    cp.source_kind,
                    canonical_timeframe(cp.timeframe),
                    cp.last_closed_bar_time,
                    cp.last_event_time,
                    cp.last_received_at,
                    cp.last_reconnect_at,
                    cp.consecutive_reconnects,
                    cp.status,
                    cp.updated_at,
                ),
            )

    def get_checkpoint(
        self,
        *,
        exchange: str,
        market: str,
        symbol: str,
        timeframe: str,
        source_kind: str = "trade_kline",
    ) -> StreamCheckpoint | None:
        with self._connect() as db:
            row = db.execute(
                "SELECT * FROM stream_checkpoints WHERE exchange=? AND market=? AND symbol=? AND timeframe=? AND source_kind=?",
                (
                    exchange.lower(),
                    market.lower(),
                    symbol.upper(),
                    canonical_timeframe(timeframe),
                    source_kind,
                ),
            ).fetchone()
        if not row:
            return None
        return StreamCheckpoint(**dict(row))

    def checkpoints(self) -> list[StreamCheckpoint]:
        with self._connect() as db:
            return [
                StreamCheckpoint(**dict(r))
                for r in db.execute(
                    "SELECT * FROM stream_checkpoints ORDER BY exchange,market,symbol,timeframe"
                )
            ]

    def _row_to_bar(self, row: sqlite3.Row) -> MarketBar:
        return MarketBar(
            time=int(row["open_time"]),
            open=float(row["open"]),
            high=float(row["high"]),
            low=float(row["low"]),
            close=float(row["close"]),
            volume=float(row["volume"]),
            time_close=int(row["close_time"]),
            exchange=row["exchange"],
            market=row["market"],
            symbol=row["symbol"],
            timeframe=row["timeframe"],
            source_transport=row["source_transport"],
            source_kind=row["source_kind"],
            is_closed=bool(row["is_closed"]),
            quote_volume=row["quote_volume"],
            turnover=row["turnover"],
            trades_count=row["trades_count"],
            taker_buy_base_volume=row["taker_buy_base_volume"],
            taker_buy_quote_volume=row["taker_buy_quote_volume"],
            downloaded_at=row["received_at"],
        )
