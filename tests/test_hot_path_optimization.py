from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import asdict, replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any, TypedDict, cast

import pytest

from marketdata_provider._adapters import series_from_market_bars
from marketdata_provider.config import MarketDataConfig, StorageConfig
from marketdata_provider.contracts import BarQuery, InstrumentKey, parse_timeframe
from marketdata_provider.core.bar import MarketBar
from marketdata_provider.errors import MDInvalidExchangeResponse
from marketdata_provider.factories import _CandleStoreAdapter, _normalize_closed_batch
from marketdata_provider.service import MarketDataService
from marketdata_provider.store import SegmentStore
from marketdata_provider.store.candle_store import CandleStore
from marketdata_provider.store.segment_append import stable_store_read_lock
from marketdata_provider.store.segment_integrity import (
    INTEGRITY_GENERATION_NAME,
    integrity_generation_is_current,
    validate_or_trust_csv_generation,
)


class SegmentKey(TypedDict):
    exchange: str
    market: str
    symbol: str
    timeframe: str


KEY: SegmentKey = {
    "exchange": "binance",
    "market": "spot",
    "symbol": "BTCUSDT",
    "timeframe": "1m",
}


def bar(
    open_time: int,
    *,
    close: float = 1.5,
    transport: str = "rest",
    downloaded_at: int | None = None,
) -> MarketBar:
    return MarketBar(
        time=open_time,
        open=1.0,
        high=max(2.0, close),
        low=0.5,
        close=close,
        volume=10.0,
        time_close=open_time + 59_999,
        exchange="binance",
        market="spot",
        symbol="BTCUSDT",
        timeframe="1m",
        source_transport=transport,
        source_kind="trade_kline",
        is_closed=True,
        downloaded_at=downloaded_at or open_time + 60_000,
        provider="binance",
        provider_revision="test-fixture-v1",
    )


def query(end: int = 120_000) -> BarQuery:
    return BarQuery(
        InstrumentKey("binance", "spot", "BTCUSDT"),
        parse_timeframe("1m"),
        0,
        end,
    )


def test_bounded_read_trusts_writer_published_integrity_generation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = SegmentStore(tmp_path)
    store.replace_all([bar(0), bar(60_000)], **KEY)

    from marketdata_provider.store import segment_integrity

    calls = 0
    original = segment_integrity.validate_csv_checksum

    def checksum_spy(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(segment_integrity, "validate_csv_checksum", checksum_spy)

    assert [item.time for item in store.read_all(start=60_000, end=120_000, **KEY)] == [
        60_000
    ]
    assert calls == 0


def test_bounded_readers_share_series_lock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = SegmentStore(tmp_path)
    store.replace_all([bar(0), bar(60_000)], **KEY)
    entered = threading.Barrier(2)
    active = 0
    max_active = 0
    guard = threading.Lock()
    original = store._read_all_locked

    def paused_read(**kwargs):
        nonlocal active, max_active
        with guard:
            active += 1
            max_active = max(max_active, active)
        entered.wait(timeout=2)
        try:
            return original(**kwargs)
        finally:
            with guard:
                active -= 1

    monkeypatch.setattr(store, "_read_all_locked", paused_read)
    with ThreadPoolExecutor(max_workers=2) as pool:
        rows = list(
            pool.map(
                lambda _item: store.read_all(start=0, end=120_000, **KEY), range(2)
            )
        )

    assert max_active == 2
    assert [[item.time for item in result] for result in rows] == [
        [0, 60_000],
        [0, 60_000],
    ]


def test_bounded_read_revalidates_after_external_file_mutation(
    tmp_path: Path,
) -> None:
    store = SegmentStore(tmp_path)
    store.replace_all([bar(0), bar(60_000)], **KEY)
    data_path, _ = store._paths(
        exchange=KEY["exchange"],
        market=KEY["market"],
        symbol=KEY["symbol"],
        timeframe=KEY["timeframe"],
        source_kind="trade_kline",
        data_format="csv",
    )
    raw = data_path.read_bytes()
    data_path.write_bytes(raw.replace(b"1.5", b"1.6", 1))

    with pytest.raises(MDInvalidExchangeResponse, match="checksum mismatch"):
        store.read_all(start=60_000, end=120_000, **KEY)


def test_candle_store_duplicate_provenance_is_noop_without_full_replace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    raw_store = CandleStore(tmp_path)
    raw_store.segments.replace_all([bar(0), bar(60_000)], **KEY)
    adapter = _CandleStoreAdapter(raw_store)
    incoming = [
        replace(bar(0), source_transport="archive", downloaded_at=999_000),
        replace(bar(60_000), source_transport="archive", downloaded_at=999_001),
    ]
    monkeypatch.setattr(
        raw_store.segments,
        "_replace_all_locked",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("duplicate write replaced full history")
        ),
    )

    result = adapter.write(
        series_from_market_bars(query(), incoming, source="provider")
    )

    assert result.success is True
    assert result.rows_written == 0


def test_candle_store_strict_tail_uses_incremental_append(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    raw_store = CandleStore(tmp_path)
    raw_store.segments.replace_all([bar(0), bar(60_000)], **KEY)
    adapter = _CandleStoreAdapter(raw_store)
    calls: list[list[int]] = []
    original = raw_store.segments.append_strictly_newer

    def append_spy(items, **kwargs):
        rows = list(items)
        calls.append([item.time for item in rows])
        return original(rows, **kwargs)

    monkeypatch.setattr(raw_store.segments, "append_strictly_newer", append_spy)
    monkeypatch.setattr(
        raw_store.segments,
        "_replace_all_locked",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("strict tail replaced full history")
        ),
    )

    result = adapter.write(
        series_from_market_bars(
            query(180_000), [bar(60_000), bar(120_000)], source="provider"
        )
    )

    assert result.success is True
    assert result.rows_written == 1
    assert calls == [[120_000]]


def test_service_single_flight_serializes_same_series_fetch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    service = MarketDataService(
        MarketDataConfig(storage=StorageConfig(cache_dir=tmp_path))
    )
    requested = query()
    barrier = threading.Barrier(2)
    calls = 0
    calls_lock = threading.Lock()

    def fetch(*_args, **_kwargs):
        nonlocal calls
        with calls_lock:
            calls += 1
        time.sleep(0.05)
        return [bar(0), bar(60_000)]

    monkeypatch.setattr(service, "_fetch_from_sources", fetch)

    def run():
        barrier.wait()
        return service.fetch_bars(requested)

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _item: run(), range(2)))

    assert calls == 1
    assert [[item.time for item in result.bars] for result in results] == [
        [0, 60_000],
        [0, 60_000],
    ]


def test_bulk_write_backfills_missing_overlap_and_rejects_conflicts(
    tmp_path: Path,
) -> None:
    raw_store = CandleStore(tmp_path)
    raw_store.segments.replace_all([bar(0), bar(120_000)], **KEY)
    adapter = _CandleStoreAdapter(raw_store)

    result = adapter.write(
        series_from_market_bars(
            query(240_000), [bar(60_000), bar(120_000), bar(180_000)], source="provider"
        )
    )

    assert result.success is True
    assert result.rows_written == 2
    assert [item.time for item in raw_store.segments.read_all(**KEY)] == [
        0,
        60_000,
        120_000,
        180_000,
    ]

    conflict = adapter.write(
        series_from_market_bars(
            query(180_000), [bar(60_000, close=99.0)], source="provider"
        )
    )
    assert conflict.success is False
    assert conflict.error == "conflicting closed candle at 60000"


def test_normalize_closed_batch_rejects_conflicting_duplicate() -> None:
    with pytest.raises(ValueError, match="conflicting closed candle at 0"):
        _normalize_closed_batch([bar(0), bar(0, close=9.0)])


def test_bulk_write_legacy_store_rejects_existing_conflict(tmp_path: Path) -> None:
    class LegacySegments:
        @contextmanager
        def series_writer_lock(self, **_key):
            yield

        def read_all(self, **_key):
            return [bar(0)]

        def _replace_all_locked(self, *_args, **_kwargs):
            raise AssertionError("conflict must fail before replacement")

    raw_store = CandleStore(tmp_path)
    raw_store.segments = cast(Any, LegacySegments())
    adapter = _CandleStoreAdapter(raw_store)
    with pytest.raises(ValueError, match="conflicting closed candle at 0"):
        adapter._bulk_write_closed([bar(0, close=9.0)])


def test_bulk_write_revalidates_conflict_before_backfill_publish(
    tmp_path: Path,
) -> None:
    class RacingSegments:
        reads = 0

        @contextmanager
        def series_writer_lock(self, **_key):
            yield

        def manifest_for(self, **_key):
            return SimpleNamespace(end_time=120_000)

        def _read_all_locked(self, **_key):
            self.reads += 1
            return [] if self.reads == 1 else [bar(60_000)]

        def _replace_all_locked(self, *_args, **_kwargs):
            raise AssertionError("conflict must fail before replacement")

    raw_store = CandleStore(tmp_path)
    raw_store.segments = cast(Any, RacingSegments())
    adapter = _CandleStoreAdapter(raw_store)
    with pytest.raises(ValueError, match="conflicting closed candle at 60000"):
        adapter._bulk_write_closed([bar(60_000, close=9.0)])


def test_integrity_generation_invalid_and_refresh_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = SegmentStore(tmp_path)
    manifest = store.replace_all([bar(0)], **KEY)
    data_path, _ = store._paths(
        exchange=KEY["exchange"],
        market=KEY["market"],
        symbol=KEY["symbol"],
        timeframe=KEY["timeframe"],
        source_kind="trade_kline",
        data_format="csv",
    )
    generation = data_path.parent / INTEGRITY_GENERATION_NAME

    generation.write_text("[]\n", encoding="utf-8")
    assert integrity_generation_is_current(data_path, asdict(manifest)) is False
    generation.write_text("{broken", encoding="utf-8")
    assert integrity_generation_is_current(data_path, asdict(manifest)) is False

    from marketdata_provider.store import segment_integrity

    validated: list[Path] = []
    monkeypatch.setattr(
        segment_integrity,
        "validate_csv_checksum",
        lambda path, _manifest: validated.append(path),
    )
    validate_or_trust_csv_generation(store, data_path, asdict(manifest))
    assert validated == [data_path]
    assert integrity_generation_is_current(data_path, asdict(manifest)) is True


def test_nested_writer_read_lock_and_pending_recovery(tmp_path: Path) -> None:
    lock_path = tmp_path / "series" / ".writer.lock"
    active_store = SimpleNamespace(
        _series_locks=SimpleNamespace(active={str(lock_path.resolve())}),
        _dir=lambda **_identity: lock_path.parent,
    )
    with stable_store_read_lock(active_store, **KEY, source_kind="trade_kline"):
        pass

    calls: list[str] = []

    class RecoveringStore:
        _series_locks = SimpleNamespace(active=set())

        def _dir(self, **_identity):
            directory = tmp_path / "recover"
            directory.mkdir(parents=True, exist_ok=True)
            return directory

        @contextmanager
        def series_writer_lock(self, **_identity):
            calls.append("recover")
            (tmp_path / "recover" / ".append-journal.json").unlink(missing_ok=True)
            yield

    recovering = RecoveringStore()
    (recovering._dir() / ".append-journal.json").write_text("{}", encoding="utf-8")
    with stable_store_read_lock(recovering, **KEY, source_kind="trade_kline"):
        calls.append("read")
    assert calls == ["recover", "read"]
