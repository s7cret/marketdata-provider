from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone

from marketdata_provider.config import MarketDataConfig
from marketdata_provider.contracts.footprint import AggTrade, FootprintQuery, FootprintSeries
from marketdata_provider.contracts.series import CoverageReport
from marketdata_provider.errors import MDUnsupportedFeature
from marketdata_provider.exchanges.binance.trades import binance_get_agg_trades_sync
from marketdata_provider.footprint.aggregate import aggregate_trades_to_footprint
from marketdata_provider.store.footprint_store import FootprintStore
from marketdata_provider.store.raw_store import RawStore


class FootprintService:
    """Separate raw-trade to footprint pipeline, intentionally not OHLCV."""

    def __init__(self, config: MarketDataConfig):
        self.config = config
        self.raw_store = RawStore(config.storage.cache_dir)
        self.footprint_store = FootprintStore(config.storage.cache_dir)

    def fetch_footprint(self, query: FootprintQuery) -> FootprintSeries:
        if query.source in {"storage", "auto"}:
            stored = self.footprint_store.read(query)
            if stored.coverage.is_complete or query.source == "storage":
                if query.gap_policy == "fail" and not stored.coverage.is_complete:
                    raise MDUnsupportedFeature(f"footprint storage coverage incomplete: {stored.coverage.missing_intervals}")
                return stored
        if query.source == "storage":
            return stored

        trades = self._ensure_raw_trades(query)
        bars = tuple(aggregate_trades_to_footprint(trades, query))
        series = FootprintSeries(query, bars, _coverage_for(query, bars))
        if bars:
            self.footprint_store.write(series)
            series = self.footprint_store.read(query)
        if query.gap_policy == "fail" and not series.coverage.is_complete:
            raise MDUnsupportedFeature(f"footprint coverage incomplete: {series.coverage.missing_intervals}")
        return series

    def _ensure_raw_trades(self, query: FootprintQuery) -> list[AggTrade]:
        cached = self._read_raw_trades(query)
        if _raw_covers(cached, query):
            return cached
        if query.instrument.exchange != "binance":
            raise MDUnsupportedFeature(f"Unsupported footprint exchange: {query.instrument.exchange}")
        fetched = binance_get_agg_trades_sync(
            query.instrument.symbol,
            query.start_ms,
            query.end_ms,
            self.config.binance,
            market=query.instrument.market,
        )
        self._write_raw_trades(query, fetched)
        return self._read_raw_trades(query)

    def _read_raw_trades(self, query: FootprintQuery) -> list[AggTrade]:
        rows = self.raw_store.read_partitions(
            exchange=query.instrument.exchange,
            market=query.instrument.market,
            symbol=query.instrument.symbol,
            source_transport="rest",
            source_kind="agg_trades",
        )
        trades = [
            AggTrade(
                trade_id=int(row["trade_id"]),
                time=int(row["time"]),
                price=float(row["price"]),
                quantity=float(row["quantity"]),
                buyer_maker=bool(row["buyer_maker"]),
            )
            for row in rows
            if query.start_ms <= int(row["time"]) < query.end_ms
        ]
        by_id = {trade.trade_id: trade for trade in trades}
        return [by_id[trade_id] for trade_id in sorted(by_id)]

    def _write_raw_trades(self, query: FootprintQuery, trades: list[AggTrade]) -> None:
        by_partition: dict[str, list[dict[str, object]]] = defaultdict(list)
        for trade in trades:
            by_partition[_day_partition(trade.time)].append(
                {
                    "trade_id": trade.trade_id,
                    "time": trade.time,
                    "price": trade.price,
                    "quantity": trade.quantity,
                    "buyer_maker": trade.buyer_maker,
                }
            )
        for partition, rows in by_partition.items():
            self.raw_store.write_batch(
                rows,
                exchange=query.instrument.exchange,
                market=query.instrument.market,
                symbol=query.instrument.symbol,
                source_transport="rest",
                source_kind="agg_trades",
                partition=partition,
            )


def _day_partition(timestamp_ms: int) -> str:
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).strftime("day=%Y-%m-%d")


def _raw_covers(trades: list[AggTrade], query: FootprintQuery) -> bool:
    return bool(trades) and trades[0].time <= query.start_ms and trades[-1].time < query.end_ms


def _coverage_for(query: FootprintQuery, bars: tuple) -> CoverageReport:
    duration = int(query.timeframe.duration_ms or 0)
    delivered = {bar.time for bar in bars}
    missing = tuple((start, min(start + duration, query.end_ms)) for start in range(query.start_ms, query.end_ms, duration) if start not in delivered)
    status = "empty" if not bars else "gap" if missing else "valid"
    return CoverageReport(query.start_ms, query.end_ms, bars[0].time if bars else None, bars[-1].time_close if bars else None, missing, (), ("footprint",), status)

