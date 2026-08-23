from __future__ import annotations

import csv
import os
import time
from pathlib import Path

import pytest

from marketdata_provider.core.bar import MarketBar
from marketdata_provider.errors import MDInvalidExchangeResponse
from marketdata_provider.store import SegmentStore
from marketdata_provider.store.segment_checksums import validate_csv_checksum
from marketdata_provider.timeframes import parse_time_ms


def _bar() -> MarketBar:
    return MarketBar(
        time=1_704_067_200_000,
        open=1.0,
        high=2.0,
        low=0.5,
        close=1.5,
        volume=10.0,
        time_close=1_704_067_259_999,
        exchange="binance",
        market="spot",
        symbol="BTCUSDT",
        timeframe="1m",
        source_transport="rest",
        source_kind="trade_kline",
        is_closed=True,
        provider="binance",
        provider_revision="test-fixture-v1",
    )


def test_segment_data_corruption_is_rejected_instead_of_rewriting_manifest(
    tmp_path: Path,
) -> None:
    store = SegmentStore(tmp_path)
    store.replace_all(
        [_bar()],
        exchange="binance",
        market="spot",
        symbol="BTCUSDT",
        timeframe="1m",
    )
    data_path = next(tmp_path.rglob("bars.csv"))
    with data_path.open(newline="") as handle:
        rows = list(csv.DictReader(handle))
    rows[0]["close"] = "1.4"
    with data_path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=store.fields)
        writer.writeheader()
        writer.writerows(rows)

    key = {
        "exchange": "binance",
        "market": "spot",
        "symbol": "BTCUSDT",
        "timeframe": "1m",
    }
    with pytest.raises(MDInvalidExchangeResponse, match="checksum mismatch"):
        store.read_all(**key)
    with pytest.raises(MDInvalidExchangeResponse, match="checksum mismatch"):
        store.read_all(
            exchange="binance",
            market="spot",
            symbol="BTCUSDT",
            timeframe="1m",
            start=_bar().time,
            end=_bar().time_close,
        )
    with pytest.raises(MDInvalidExchangeResponse, match="checksum mismatch"):
        list(
            store.iter_all(
                exchange="binance",
                market="spot",
                symbol="BTCUSDT",
                timeframe="1m",
                start=_bar().time,
                end=_bar().time_close,
            )
        )

    validate_csv_checksum(data_path, None)
    manifest_path = next(tmp_path.rglob("manifest.json"))
    manifest_path.write_text("[]\n", encoding="utf-8")
    with pytest.raises(MDInvalidExchangeResponse, match="JSON object"):
        list(
            store.iter_all(
                exchange="binance",
                market="spot",
                symbol="BTCUSDT",
                timeframe="1m",
            )
        )


def test_naive_iso_timestamp_is_normalized_as_utc_independent_of_host_timezone(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert hasattr(time, "tzset"), "Phase 2 timezone acceptance requires POSIX tzset"
    monkeypatch.setenv("TZ", "Asia/Shanghai")
    time.tzset()
    try:
        expected = 1_704_067_200_000
        assert parse_time_ms("2024-01-01T00:00:00") == expected
        assert parse_time_ms("2024-01-01T00:00:00Z") == expected
        assert parse_time_ms("2024-01-01T08:00:00+08:00") == expected
    finally:
        monkeypatch.delenv("TZ", raising=False)
        os.environ.pop("TZ", None)
        time.tzset()
