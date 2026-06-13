from __future__ import annotations

import math

from marketdata_provider.contracts.footprint import (
    AggTrade,
    FootprintBar,
    FootprintLevel,
    FootprintQuery,
)


def aggregate_trades_to_footprint(
    trades: list[AggTrade], query: FootprintQuery
) -> list[FootprintBar]:
    duration = int(query.timeframe.duration_ms or 0)
    bucket_size = query.bucket_size
    buckets: dict[int, dict[float, list[float | int]]] = {}
    counts: dict[int, int] = {}
    for trade in trades:
        if not (query.start_ms <= trade.time < query.end_ms):
            continue
        bar_time = (trade.time // duration) * duration
        if not (query.start_ms <= bar_time < query.end_ms):
            continue
        price_low = math.floor(trade.price / bucket_size) * bucket_size
        level = buckets.setdefault(bar_time, {}).setdefault(price_low, [0.0, 0.0, 0, 0])
        if trade.buyer_maker:
            level[1] = float(level[1]) + trade.quantity
            level[3] = int(level[3]) + 1
        else:
            level[0] = float(level[0]) + trade.quantity
            level[2] = int(level[2]) + 1
        counts[bar_time] = counts.get(bar_time, 0) + 1
    bars: list[FootprintBar] = []
    for bar_time in sorted(buckets):
        levels = tuple(
            FootprintLevel(
                price_low=price,
                price_high=price + bucket_size,
                buy_volume=float(values[0]),
                sell_volume=float(values[1]),
                buy_count=int(values[2]),
                sell_count=int(values[3]),
            )
            for price, values in sorted(buckets[bar_time].items())
        )
        bars.append(
            FootprintBar(bar_time, bar_time + duration, levels, counts[bar_time])
        )
    return bars
