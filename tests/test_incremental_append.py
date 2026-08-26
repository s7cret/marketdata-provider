from __future__ import annotations

import hashlib
import json
import sqlite3
import time
import tracemalloc
from pathlib import Path

import pytest

from marketdata_provider.config import MarketDataConfig, StorageConfig
from marketdata_provider.contracts import BarQuery, InstrumentKey, parse_timeframe
from marketdata_provider.contracts.errors import CoverageValidationError
from marketdata_provider.core.bar import MarketBar
from marketdata_provider.errors import MDCacheConflict, MDInvalidExchangeResponse
from marketdata_provider.service import MarketDataService
from marketdata_provider.store import SegmentStore
from marketdata_provider.store.candle_store import CandleStore
from marketdata_provider.store.segment_checksums import (
    bars_checksum,
    legacy_bars_checksum,
)

KEY = {
    "exchange": "binance",
    "market": "spot",
    "symbol": "BTCUSDT",
    "timeframe": "1m",
}


def bar(t: int, *, close: float = 1.5) -> MarketBar:
    return MarketBar(
        time=t,
        open=1.0,
        high=max(2.0, close),
        low=0.5,
        close=close,
        volume=10.0,
        time_close=t + 59_999,
        exchange="binance",
        market="spot",
        symbol="BTCUSDT",
        timeframe="1m",
        source_transport="rest",
        source_kind="trade_kline",
        is_closed=True,
        downloaded_at=t + 60_000,
        provider="binance",
        provider_revision="test-fixture-v1",
    )


def test_service_clean_tail_append_never_iterates_or_replaces_history(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    service = MarketDataService(
        MarketDataConfig(storage=StorageConfig(cache_dir=tmp_path))
    )
    service.store.segments.replace_all([bar(0), bar(60_000)], **KEY)
    query = BarQuery(
        InstrumentKey("binance", "spot", "BTCUSDT"),
        parse_timeframe("1m"),
        0,
        180_000,
    )
    calls: list[list[int]] = []
    original_append = service.store.segments.append_strictly_newer

    def append_spy(bars: list[MarketBar], **key: str):
        calls.append([item.time for item in bars])
        return original_append(bars, **key)

    monkeypatch.setattr(service.store.segments, "append_strictly_newer", append_spy)
    monkeypatch.setattr(
        service.store.segments,
        "iter_all",
        lambda **_key: (_ for _ in ()).throw(AssertionError("history was iterated")),
    )
    monkeypatch.setattr(
        service.store.segments,
        "replace_all_stream",
        lambda *_args, **_key: (_ for _ in ()).throw(
            AssertionError("history was rewritten")
        ),
    )

    service._append_stream(query, [bar(120_000)])

    assert calls == [[120_000]]
    manifest = service.store.segments.manifest_for(**KEY)
    assert manifest is not None
    assert (manifest.rows_count, manifest.start_time, manifest.end_time) == (
        3,
        0,
        120_000,
    )


def test_service_repairs_internal_gap_even_when_manifest_span_and_count_look_complete(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    service = MarketDataService(
        MarketDataConfig(storage=StorageConfig(cache_dir=tmp_path))
    )
    service.store.segments.replace_all(
        [bar(0), bar(60_000), bar(180_000), bar(240_000)], **KEY
    )
    query = BarQuery(
        InstrumentKey("binance", "spot", "BTCUSDT"),
        parse_timeframe("1m"),
        60_000,
        240_000,
    )
    fetches: list[tuple[int, int]] = []

    def fetch_missing(request: BarQuery, progress_callback=None) -> list[MarketBar]:
        del progress_callback
        fetches.append((request.start_ms, request.end_ms))
        return [bar(60_000), bar(120_000), bar(180_000)]

    monkeypatch.setattr(service, "_fetch_from_sources", fetch_missing)

    result = service.fetch_bars(query)

    assert fetches == [(60_000, 240_000)]
    assert [item.time for item in result.bars] == [60_000, 120_000, 180_000]


def test_service_fails_closed_when_provider_does_not_repair_internal_gap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    service = MarketDataService(
        MarketDataConfig(storage=StorageConfig(cache_dir=tmp_path))
    )
    service.store.segments.replace_all(
        [bar(0), bar(60_000), bar(180_000), bar(240_000)], **KEY
    )
    query = BarQuery(
        InstrumentKey("binance", "spot", "BTCUSDT"),
        parse_timeframe("1m"),
        60_000,
        240_000,
    )
    monkeypatch.setattr(
        service,
        "_fetch_from_sources",
        lambda _query, progress_callback=None: [bar(60_000), bar(180_000)],
    )

    with pytest.raises(CoverageValidationError, match="every requested timestamp"):
        service.fetch_bars(query)


@pytest.mark.parametrize(
    ("timeframe", "end_ms", "message"),
    [("1m", 60_000, "Stored/provider"), ("5m", 300_000, "Derived")],
)
def test_service_fails_closed_when_strict_coverage_is_empty(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    timeframe: str,
    end_ms: int,
    message: str,
) -> None:
    service = MarketDataService(
        MarketDataConfig(storage=StorageConfig(cache_dir=tmp_path))
    )
    query = BarQuery(
        InstrumentKey("binance", "spot", "BTCUSDT"),
        parse_timeframe(timeframe),
        0,
        end_ms,
    )
    monkeypatch.setattr(
        service, "_fetch_from_sources", lambda _query, progress_callback=None: []
    )

    with pytest.raises(CoverageValidationError, match=message):
        service.fetch_bars(query)


def test_service_returns_partial_coverage_when_gap_metadata_is_allowed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    service = MarketDataService(
        MarketDataConfig(storage=StorageConfig(cache_dir=tmp_path))
    )
    service.store.segments.replace_all([bar(60_000), bar(180_000)], **KEY)
    query = BarQuery(
        InstrumentKey("binance", "spot", "BTCUSDT"),
        parse_timeframe("1m"),
        60_000,
        240_000,
        gap_policy="allow_with_metadata",
    )
    monkeypatch.setattr(
        service,
        "_fetch_from_sources",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("partial stored coverage must not block on provider repair")
        ),
    )

    result = service.fetch_bars(query)

    assert [item.time for item in result.bars] == [60_000, 180_000]
    assert result.coverage.is_complete is False
    assert result.coverage.missing_intervals == ((120_000, 180_000),)


def test_live_strictly_newer_close_uses_append_without_full_history_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = CandleStore(tmp_path)
    store.segments.replace_all([bar(0), bar(60_000)], **KEY)
    original_read_all = store.segments.read_all
    monkeypatch.setattr(
        store.segments,
        "read_all",
        lambda **_key: (_ for _ in ()).throw(AssertionError("history was scanned")),
    )
    monkeypatch.setattr(
        store.segments,
        "_replace_all_locked",
        lambda *_args, **_key: (_ for _ in ()).throw(
            AssertionError("history was rewritten")
        ),
    )

    result = store.commit_closed(bar(120_000))

    assert result.status == "committed"
    assert [item.time for item in original_read_all(**KEY)] == [0, 60_000, 120_000]


def test_full_replace_recovers_old_generation_after_manifest_write_crash(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = SegmentStore(tmp_path)
    before = store.replace_all([bar(0)], **KEY)
    manifest_path = next(tmp_path.rglob("manifest.json"))
    original_atomic_write = store._atomic_write_text

    def crash_before_new_manifest(path: Path, content: str) -> None:
        if path == manifest_path and '"rows_count": 2' in content:
            raise BaseException("simulated replacement death")
        original_atomic_write(path, content)

    monkeypatch.setattr(store, "_atomic_write_text", crash_before_new_manifest)
    with pytest.raises(BaseException, match="replacement death"):
        store.replace_all([bar(0), bar(60_000)], **KEY)

    assert list(tmp_path.rglob(".replace-journal.json"))
    recovered = SegmentStore(tmp_path)
    assert recovered.manifest_for(**KEY) == before
    assert recovered.read_all(**KEY) == [bar(0)]
    assert not list(tmp_path.rglob(".replace-journal.json"))


def test_stream_replace_recovers_old_generation_after_manifest_write_crash(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = SegmentStore(tmp_path)
    before = store.replace_all([bar(0)], **KEY)
    manifest_path = next(tmp_path.rglob("manifest.json"))
    original_atomic_write = store._atomic_write_text

    def crash_before_new_manifest(path: Path, content: str) -> None:
        if path == manifest_path and '"rows_count": 2' in content:
            raise BaseException("simulated stream replacement death")
        original_atomic_write(path, content)

    monkeypatch.setattr(store, "_atomic_write_text", crash_before_new_manifest)
    with pytest.raises(BaseException, match="stream replacement death"):
        store.replace_all_stream(iter([bar(0), bar(60_000)]), **KEY)

    recovered = SegmentStore(tmp_path)
    assert recovered.manifest_for(**KEY) == before
    assert recovered.read_all(**KEY) == [bar(0)]
    assert not list(tmp_path.rglob(".replace-journal.json"))


def test_incremental_append_duplicate_conflict_and_ordering(tmp_path: Path) -> None:
    store = SegmentStore(tmp_path)
    original = store.replace_all([bar(0), bar(60_000)], **KEY)

    duplicate = store.append_strictly_newer([bar(60_000)], **KEY)
    assert duplicate == original
    assert store.read_all(**KEY) == [bar(0), bar(60_000)]

    with pytest.raises(MDCacheConflict, match="Conflicting tail candle"):
        store.append_strictly_newer([bar(60_000, close=9.0)], **KEY)
    with pytest.raises(MDCacheConflict, match="older than stored tail"):
        store.append_strictly_newer([bar(0)], **KEY)
    with pytest.raises(MDInvalidExchangeResponse, match="strictly ordered"):
        store.append_strictly_newer([bar(180_000), bar(120_000)], **KEY)
    with pytest.raises(MDCacheConflict, match="Conflicting append candle"):
        store.append_strictly_newer([bar(120_000), bar(120_000, close=8.0)], **KEY)


def test_append_recovers_data_manifest_and_index_after_manifest_write_crash(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = SegmentStore(tmp_path)
    before = store.replace_all([bar(0), bar(60_000)], **KEY)
    manifest_path = next(tmp_path.rglob("manifest.json"))
    data_path = next(tmp_path.rglob("bars.csv"))
    before_bytes = data_path.read_bytes()
    original_atomic_write = store._atomic_write_text

    def crash_on_new_manifest(path: Path, content: str) -> None:
        if path == manifest_path and '"rows_count": 3' in content:
            raise BaseException("simulated process death")
        original_atomic_write(path, content)

    monkeypatch.setattr(store, "_atomic_write_text", crash_on_new_manifest)
    with pytest.raises(BaseException, match="simulated process death"):
        store.append_strictly_newer([bar(120_000)], **KEY)

    assert data_path.stat().st_size > len(before_bytes)
    assert list(tmp_path.rglob(".append-journal.json"))

    recovered = SegmentStore(tmp_path)
    assert data_path.read_bytes() == before_bytes
    assert recovered.manifest_for(**KEY) == before
    assert not list(tmp_path.rglob(".append-journal.json"))
    with sqlite3.connect(recovered.index_path) as db:
        assert db.execute(
            "SELECT rows_count, start_time, end_time, checksum "
            "FROM marketdata_segments"
        ).fetchall() == [(2, 0, 60_000, before.checksum)]


def test_legacy_manifest_is_validated_once_then_migrated_without_rewrite(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = SegmentStore(tmp_path)
    legacy = store.replace_all([bar(0), bar(60_000)], **KEY)
    manifest_path = next(tmp_path.rglob("manifest.json"))
    data_path = next(tmp_path.rglob("bars.csv"))
    payload = json.loads(manifest_path.read_text())
    payload.pop("checksum_algorithm", None)
    payload.pop("base_checksum", None)
    payload.pop("base_rows_count", None)
    payload["checksum"] = legacy_bars_checksum([bar(0), bar(60_000)])
    payload["schema_version"] = "stage-d-csv-1"
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")
    inode = data_path.stat().st_ino
    calls = 0

    import marketdata_provider.store.segment_append as segment_append_module

    original_validate = segment_append_module.validate_csv_checksum

    def validate_spy(path: Path, manifest: dict[str, object] | None) -> None:
        nonlocal calls
        calls += 1
        original_validate(path, manifest)

    monkeypatch.setattr(segment_append_module, "validate_csv_checksum", validate_spy)
    first = store.append_strictly_newer([bar(120_000)], **KEY)
    second = store.append_strictly_newer([bar(180_000)], **KEY)

    assert calls == 1
    assert data_path.stat().st_ino == inode
    assert first.rows_count == 3
    assert second.rows_count == 4
    assert second.checksum_algorithm == "sha256-tail-chain-v4"
    assert second.base_checksum == bars_checksum([bar(0), bar(60_000)])
    assert second.base_rows_count == legacy.rows_count
    assert [item.time for item in store.read_all(**KEY)] == [
        0,
        60_000,
        120_000,
        180_000,
    ]


def test_legacy_manifest_corruption_blocks_migration_and_append(tmp_path: Path) -> None:
    store = SegmentStore(tmp_path)
    store.replace_all([bar(0), bar(60_000)], **KEY)
    manifest_path = next(tmp_path.rglob("manifest.json"))
    data_path = next(tmp_path.rglob("bars.csv"))
    payload = json.loads(manifest_path.read_text())
    payload.pop("checksum_algorithm", None)
    payload.pop("base_checksum", None)
    payload.pop("base_rows_count", None)
    payload["checksum"] = legacy_bars_checksum([bar(0), bar(60_000)])
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")
    original = data_path.read_bytes()
    data_path.write_bytes(original.replace(b",1.5,", b",1.6,", 1))

    with pytest.raises(MDInvalidExchangeResponse, match="checksum mismatch"):
        store.append_strictly_newer([bar(120_000)], **KEY)
    assert data_path.stat().st_size == len(original)
    assert not list(tmp_path.rglob(".append-journal.json"))


def test_tail_chain_detects_base_and_appended_corruption(tmp_path: Path) -> None:
    for occurrence in (1, 3):
        root = tmp_path / str(occurrence)
        store = SegmentStore(root)
        store.replace_all([bar(0), bar(60_000)], **KEY)
        store.append_strictly_newer([bar(120_000)], **KEY)
        data_path = next(root.rglob("bars.csv"))
        content = data_path.read_bytes()
        needle = b",1.5,"
        cursor = -1
        for _ in range(occurrence):
            cursor = content.find(needle, cursor + 1)
        assert cursor >= 0
        data_path.write_bytes(
            content[:cursor] + b",1.6," + content[cursor + len(needle) :]
        )

        with pytest.raises(MDInvalidExchangeResponse, match="checksum mismatch"):
            store.read_all(**KEY)


def test_large_synthetic_append_has_bounded_memory_and_does_not_replace_file(
    tmp_path: Path,
) -> None:
    store = SegmentStore(tmp_path)
    row_count = 30_000
    store.replace_all_stream((bar(i * 60_000) for i in range(row_count)), **KEY)
    data_path = next(tmp_path.rglob("bars.csv"))
    inode = data_path.stat().st_ino
    prefix_hash = hashlib.sha256(data_path.read_bytes()[:4096]).hexdigest()

    tracemalloc.start()
    tracemalloc.reset_peak()
    started = time.monotonic()
    store.append_strictly_newer([bar(row_count * 60_000)], **KEY)
    elapsed = time.monotonic() - started
    _current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    assert data_path.stat().st_ino == inode
    assert hashlib.sha256(data_path.read_bytes()[:4096]).hexdigest() == prefix_hash
    assert peak < 2_000_000
    assert elapsed < 5.0
    manifest = store.manifest_for(**KEY)
    assert manifest is not None
    assert manifest.rows_count == row_count + 1
    assert manifest.end_time == row_count * 60_000
    assert manifest.checksum != bars_checksum([bar(row_count * 60_000)])
