from pathlib import Path

import pytest

from marketdata_provider.cli.main import main
from marketdata_provider.core.bar import MarketBar, RUNTIME_CONTRACT_VERSION
from marketdata_provider.errors import MDCacheConflict
from marketdata_provider.store import CandleStore
from marketdata_provider.streaming import (
    KlineUpdate,
    MockWebSocketSupervisor,
    normalize_binance_kline,
    normalize_bybit_kline,
)


def mb(
    t: int, close: float = 1.5, *, closed: bool = True, transport: str = "ws"
) -> MarketBar:
    return MarketBar(
        time=t,
        open=1,
        high=2,
        low=0.5,
        close=close,
        volume=10,
        time_close=t + 59_999,
        exchange="binance",
        market="spot",
        symbol="BTCUSDT",
        timeframe="1m",
        source_transport=transport,
        source_kind="trade_kline",
        is_closed=closed,
        downloaded_at=t + 60_000,
    )


def test_segment_roundtrip_restart_manifest(tmp_path: Path):
    store = CandleStore(tmp_path)
    assert store.commit_closed(mb(0)).status == "committed"
    restarted = CandleStore(tmp_path)
    bars = restarted.get_market_bars(
        exchange="binance", market="spot", symbol="BTCUSDT", timeframe="1m"
    )
    assert len(bars) == 1
    assert bars[0].source_transport == "ws"
    manifest = next(tmp_path.glob("v1/**/manifest.json"))
    assert RUNTIME_CONTRACT_VERSION in manifest.read_text()
    assert (tmp_path / "index.sqlite").exists()


def test_open_closed_split_duplicate_conflict_late_open(tmp_path: Path):
    store = CandleStore(tmp_path)
    open_bar = mb(0, closed=False)
    assert store.upsert_open(open_bar).status == "upserted"
    assert (
        store.get_bars(
            exchange="binance", market="spot", symbol="BTCUSDT", timeframe="1m"
        )
        == []
    )
    assert (
        store.get_current_candle(
            exchange="binance", market="spot", symbol="BTCUSDT", timeframe="1m"
        )
        is not None
    )
    assert store.commit_closed(mb(0)).status == "committed"
    assert (
        store.get_current_candle(
            exchange="binance", market="spot", symbol="BTCUSDT", timeframe="1m"
        )
        is None
    )
    assert store.commit_closed(mb(0)).status == "duplicate"
    assert store.upsert_open(open_bar).diagnostic == "MD_WARNING_LATE_OPEN_IGNORED"
    with pytest.raises(MDCacheConflict):
        store.commit_closed(mb(0, close=1.8))


def test_checkpoint_recovery_after_restart(tmp_path: Path):
    store = CandleStore(tmp_path)
    store.commit_closed(mb(60_000), event_time=123, received_at=456)
    cp = CandleStore(tmp_path).current.get_checkpoint(
        exchange="binance", market="spot", symbol="BTCUSDT", timeframe="1m"
    )
    assert cp is not None
    assert cp.last_closed_bar_time == 60_000
    assert cp.last_event_time == 123


def test_mocked_reconnect_backfill(tmp_path: Path):
    store = CandleStore(tmp_path)
    updates = [
        KlineUpdate(
            "binance",
            "spot",
            "BTCUSDT",
            "1m",
            1,
            0,
            59_999,
            1,
            2,
            0.5,
            1.1,
            10,
            is_closed=False,
            received_at=1,
        ),
        KlineUpdate(
            "binance",
            "spot",
            "BTCUSDT",
            "1m",
            2,
            60_000,
            119_999,
            1,
            2,
            0.5,
            1.2,
            11,
            is_closed=True,
            received_at=2,
        ),
    ]
    res = MockWebSocketSupervisor(store).run(
        updates, backfill_bars=[mb(0)], reconnect_after=1
    )
    assert res.reconnects == 1
    assert res.backfilled == 1
    assert (
        len(
            store.get_bars(
                exchange="binance", market="spot", symbol="BTCUSDT", timeframe="1m"
            )
        )
        == 2
    )


def test_exchange_kline_normalizers():
    b = normalize_binance_kline(
        {
            "e": "kline",
            "E": 10,
            "s": "BTCUSDT",
            "k": {
                "s": "BTCUSDT",
                "i": "1m",
                "t": 0,
                "T": 59999,
                "o": "1",
                "h": "2",
                "l": "0.5",
                "c": "1.5",
                "v": "10",
                "q": "15",
                "n": 2,
                "V": "7",
                "Q": "9",
                "x": True,
            },
        },
        market="spot",
    )
    assert b.to_market_bar().source_transport == "ws"
    assert b.is_closed is True
    y = normalize_bybit_kline(
        {
            "topic": "kline.1.BTCUSDT",
            "ts": 10,
            "data": [
                {
                    "start": 0,
                    "end": 59999,
                    "open": "1",
                    "high": "2",
                    "low": "0.5",
                    "close": "1.5",
                    "volume": "10",
                    "turnover": "15",
                    "confirm": True,
                }
            ],
        },
        market="linear",
    )
    assert y[0].turnover == 15
    assert y[0].is_closed is True


def test_cli_audit_detects_and_repair_applies(tmp_path: Path, capsys):
    store_dir = tmp_path / "store"
    source = tmp_path / "source.csv"
    source.write_text(
        "time,open,high,low,close,volume,time_close\n0,1,2,0.5,1.5,10,59999\n"
    )
    store = CandleStore(store_dir)
    store.commit_closed(mb(0, close=1.8))
    args = [
        "--store-dir",
        str(store_dir),
        "--source-path",
        str(source),
        "--symbol",
        "BINANCE:BTCUSDT",
        "--timeframe",
        "1m",
    ]
    assert main(["audit", *args]) == 3
    assert "MD_WS_REST_CANDLE_MISMATCH" in capsys.readouterr().out
    assert main(["repair", *args]) == 0
    assert '"changed": 1' in capsys.readouterr().out
    assert main(["audit", *args]) == 0


def test_cli_stream_live_not_faked(capsys, tmp_path: Path):
    assert (
        main(
            [
                "stream",
                "--store-dir",
                str(tmp_path),
                "--symbol",
                "BINANCE:BTCUSDT",
                "--timeframe",
                "1m",
            ]
        )
        == 2
    )
    assert "MD_NETWORK_UNAVAILABLE" in capsys.readouterr().out
