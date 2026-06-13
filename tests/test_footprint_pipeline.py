import httpx

from marketdata_provider.config import BinanceConfig, MarketDataConfig, StorageConfig
from marketdata_provider.contracts import (
    AggTrade,
    FootprintQuery,
    InstrumentKey,
    parse_timeframe,
)
from marketdata_provider.exchanges.binance import trades as binance_trades
from marketdata_provider.footprint.aggregate import aggregate_trades_to_footprint
from marketdata_provider.footprint.service import FootprintService


def _query(tmp_path):
    return FootprintQuery(
        instrument=InstrumentKey("binance", "usdm", "BTCUSDT"),
        timeframe=parse_timeframe("1m"),
        start_ms=0,
        end_ms=60_000,
        price_bucket=10.0,
    )


def test_binance_agg_trades_normalizer_maps_taker_side():
    trades = binance_trades.normalize_binance_agg_trades(
        [
            {"a": 2, "p": "101.0", "q": "0.5", "T": 2_000, "m": True},
            {"a": 1, "p": "100.0", "q": "1.0", "T": 1_000, "m": False},
        ]
    )

    assert [trade.trade_id for trade in trades] == [1, 2]
    assert trades[0].buyer_maker is False
    assert trades[1].buyer_maker is True


def test_aggregate_trades_to_footprint_buckets_price_and_side(tmp_path):
    query = _query(tmp_path)
    bars = aggregate_trades_to_footprint(
        [
            AggTrade(1, 1_000, 100.1, 1.0, False),
            AggTrade(2, 2_000, 109.9, 2.0, True),
            AggTrade(3, 3_000, 110.0, 3.0, False),
        ],
        query,
    )

    assert len(bars) == 1
    assert bars[0].trades_count == 3
    assert [
        (level.price_low, level.buy_volume, level.sell_volume)
        for level in bars[0].levels
    ] == [
        (100.0, 1.0, 2.0),
        (110.0, 3.0, 0.0),
    ]


def test_footprint_service_stores_raw_agg_trades_and_derived_footprint(
    tmp_path, monkeypatch
):
    calls = []

    def fake_fetch(
        symbol,
        start,
        end,
        cfg,
        *,
        market=None,
        timeout=15.0,
        max_retries=3,
        max_trades=None,
    ):
        calls.append((symbol, start, end, market))
        return [
            AggTrade(1, 1_000, 100.1, 1.0, False),
            AggTrade(2, 2_000, 109.9, 2.0, True),
        ]

    monkeypatch.setattr(
        "marketdata_provider.footprint.service.binance_get_agg_trades_sync", fake_fetch
    )
    query = _query(tmp_path)
    service = FootprintService(
        MarketDataConfig(
            storage=StorageConfig(cache_dir=tmp_path), binance=BinanceConfig()
        )
    )

    series = service.fetch_footprint(query)

    assert calls == [("BTCUSDT", 0, 60_000, "usdm")]
    assert series.coverage.is_complete
    assert series.bars[0].levels[0].buy_volume == 1.0
    assert service.raw_store.read_partitions(
        exchange="binance",
        market="usdm",
        symbol="BTCUSDT",
        source_transport="rest",
        source_kind="agg_trades",
    )

    calls.clear()
    warm = service.fetch_footprint(query)
    assert calls == []
    assert warm.bars == series.bars


def test_binance_get_agg_trades_uses_separate_endpoint(monkeypatch):
    seen = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((request.url.path, dict(request.url.params)))
        return httpx.Response(
            200, json=[{"a": 1, "p": "100", "q": "1", "T": 1000, "m": False}]
        )

    real_client = httpx.Client

    def factory(*args, **kwargs):
        kwargs["transport"] = httpx.MockTransport(handler)
        return real_client(*args, **kwargs)

    monkeypatch.setattr(binance_trades.httpx, "Client", factory)

    out = binance_trades.binance_get_agg_trades_sync(
        "BTCUSDT", 0, 60_000, BinanceConfig(), market="usdm"
    )

    assert len(out) == 1
    assert seen[0][0] == "/fapi/v1/aggTrades"
    assert seen[0][1]["startTime"] == "0"
