from __future__ import annotations

import csv
import hashlib
import json
import os
import sqlite3
import struct
import threading
from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import asdict, fields, replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Literal

import httpx
import pytest

import marketdata_provider.exchanges.binance.provider as binance_provider
from marketdata_provider import acceptance
from marketdata_provider.config import BinanceConfig
from marketdata_provider.contracts import BarQuery, InstrumentKey, parse_timeframe
from marketdata_provider.contracts.errors import CoverageValidationError
from marketdata_provider.core.bar import MarketBar
from marketdata_provider.errors import (
    MDCacheConflict,
    MDInvalidExchangeResponse,
    MDNetworkUnavailable,
)
from marketdata_provider.factories import _CandleStoreAdapter, _same_candle_payload
from marketdata_provider.service import MarketDataService
from marketdata_provider.store import SegmentStore, segment_append
from marketdata_provider.store.candle_store import CandleStore
from marketdata_provider.store.repair import _same_candle_values
from marketdata_provider.store.segment_append import recover_append_journal
from marketdata_provider.store.segment_checksums import (
    LEGACY_TAIL_CHAIN_CHECKSUM,
    PRESENCE_UNAWARE_TAIL_CHAIN_CHECKSUM,
    bars_checksum,
    extend_tail_chain,
    legacy_bars_checksum,
    market_bar_checksum,
    presence_unaware_bars_checksum,
)
from marketdata_provider.store.segment_read import iter_all
from marketdata_provider.store.segment_replace import (
    _journal_member_name,
    _validate_journal_path,
    finish_replacement,
    recover_replacement_journal,
)

KEY: dict[str, Any] = {
    "exchange": "binance",
    "market": "spot",
    "symbol": "BTCUSDT",
    "timeframe": "1m",
}


def bar(time: int, **changes: object) -> MarketBar:
    base = MarketBar(
        time=time,
        open=1.0,
        high=2.0,
        low=0.5,
        close=1.5,
        volume=10.0,
        time_close=time + 59_999,
        exchange="binance",
        market="spot",
        symbol="BTCUSDT",
        timeframe="1m",
        quote_volume=15.0,
        turnover=16.0,
        trades_count=7,
        taker_buy_base_volume=4.0,
        taker_buy_quote_volume=6.0,
        source_transport="rest",
        source_kind="trade_kline",
        is_closed=True,
        downloaded_at=time + 60_000,
        source="provider-runtime-only",
        metadata={"request_id": "runtime-only"},
        provider="binance",
        provider_revision="test-fixture-v1",
    )
    return replace(base, **changes)


PERSISTED_FIELD_CHANGES: dict[str, object] = {
    "time": 1,
    "open": 1.1,
    "high": 2.1,
    "low": 0.4,
    "close": 1.6,
    "volume": 11.0,
    "time_close": 60_000,
    "exchange": "bybit",
    "market": "linear",
    "symbol": "ETHUSDT",
    "timeframe": "5m",
    "quote_volume": 17.0,
    "turnover": 18.0,
    "trades_count": 8,
    "taker_buy_base_volume": 5.0,
    "taker_buy_quote_volume": 7.0,
    "source_transport": "ws",
    "source_kind": "mark_kline",
    "is_closed": False,
    "downloaded_at": 60_001,
}


@pytest.mark.parametrize("field_name", sorted(PERSISTED_FIELD_CHANGES))
def test_checksum_covers_every_persisted_market_bar_field(field_name: str) -> None:
    original = bar(0)
    changed = replace(original, **{field_name: PERSISTED_FIELD_CHANGES[field_name]})

    assert field_name in SegmentStore.fields
    assert market_bar_checksum(changed) != market_bar_checksum(original)


@pytest.mark.parametrize(
    "field_name",
    sorted(
        set(PERSISTED_FIELD_CHANGES) - {"time", "source_transport", "downloaded_at"}
    ),
)
def test_duplicate_with_any_changed_persisted_field_is_rejected(
    tmp_path: Path, field_name: str
) -> None:
    store = SegmentStore(tmp_path)
    original = bar(0)
    store.replace_all([original], **KEY)
    changed = replace(original, **{field_name: PERSISTED_FIELD_CHANGES[field_name]})

    with pytest.raises((MDCacheConflict, MDInvalidExchangeResponse)):
        store.append_strictly_newer([changed], **KEY)


@pytest.mark.parametrize(
    "field_name",
    sorted(
        set(PERSISTED_FIELD_CHANGES) - {"time", "source_transport", "downloaded_at"}
    ),
)
def test_bulk_and_repair_comparators_cover_every_persisted_field(
    field_name: str,
) -> None:
    original = bar(0)
    changed = replace(original, **{field_name: PERSISTED_FIELD_CHANGES[field_name]})

    assert not _same_candle_payload(original, changed)
    assert not _same_candle_values(original, changed)


@pytest.mark.parametrize("field_name", ["source_transport", "downloaded_at"])
def test_provenance_changes_do_not_conflict_with_canonical_candle(
    tmp_path: Path, field_name: str
) -> None:
    store = SegmentStore(tmp_path)
    original = bar(0)
    store.replace_all([original], **KEY)
    changed = replace(original, **{field_name: PERSISTED_FIELD_CHANGES[field_name]})

    store.append_strictly_newer([changed], **KEY)

    assert _same_candle_payload(original, changed)
    assert _same_candle_values(original, changed)
    assert store.manifest_for(**KEY).rows_count == 1  # type: ignore[union-attr]


def test_reads_reject_orphan_data_without_manifest(tmp_path: Path) -> None:
    store = SegmentStore(tmp_path)
    store.replace_all([bar(0)], **KEY)
    manifest_path = next(tmp_path.rglob("manifest.json"))
    manifest_path.unlink()

    with pytest.raises(MDInvalidExchangeResponse, match="manifest is missing"):
        store.read_all(**KEY)
    with pytest.raises(MDInvalidExchangeResponse, match="manifest is missing"):
        list(iter_all(store, **KEY))


def test_reads_reject_orphan_data_in_other_segment_format(tmp_path: Path) -> None:
    store = SegmentStore(tmp_path, data_format="csv")
    directory = store._dir(**KEY, source_kind="trade_kline")
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "bars.parquet").write_bytes(b"orphan")

    for read in (lambda: store.read_all(**KEY), lambda: list(iter_all(store, **KEY))):
        with pytest.raises(MDInvalidExchangeResponse, match="manifest is missing"):
            read()


def test_reads_return_empty_when_manifest_exists_but_current_data_is_absent(
    tmp_path: Path,
) -> None:
    store = SegmentStore(tmp_path)
    store.replace_all([bar(0)], **KEY)
    next(tmp_path.rglob("bars.csv")).unlink()

    assert store.read_all(**KEY) == []
    assert list(iter_all(store, **KEY)) == []


def test_reads_reject_cross_format_orphan_beside_existing_manifest(
    tmp_path: Path,
) -> None:
    store = SegmentStore(tmp_path, data_format="csv")
    store.replace_all([bar(0)], **KEY)
    data_path = next(tmp_path.rglob("bars.csv"))
    data_path.with_suffix(".parquet").write_bytes(b"orphan")
    data_path.unlink()

    for read in (lambda: store.read_all(**KEY), lambda: list(iter_all(store, **KEY))):
        with pytest.raises(MDInvalidExchangeResponse, match="other segment format"):
            read()


def test_replacement_journal_rejects_paths_outside_series(tmp_path: Path) -> None:
    store = SegmentStore(tmp_path / "store")
    store.replace_all([bar(0)], **KEY)
    directory = next((tmp_path / "store").rglob("manifest.json")).parent
    victim = tmp_path / "victim.txt"
    victim.write_text("keep", encoding="utf-8")
    journal = directory / ".replace-journal.json"
    manifest = store.manifest_for(**KEY)
    assert manifest is not None
    journal.write_text(
        json.dumps(
            {
                "version": "segment-replace-v1",
                "old_manifest": None,
                "new_manifest": asdict(manifest),
                "old_data_name": os.path.relpath(victim, directory),
                "new_data_name": "bars.csv",
                "backup_name": None,
                "old_downloaded_at": 0,
                "new_downloaded_at": 0,
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(MDInvalidExchangeResponse, match="journal path"):
        finish_replacement(store, journal)
    assert victim.read_text(encoding="utf-8") == "keep"

    with pytest.raises(MDInvalidExchangeResponse, match="journal path"):
        _validate_journal_path(store, directory / "wrong.json")
    with pytest.raises(MDInvalidExchangeResponse, match="journal path"):
        _journal_member_name(
            {"backup_name": "ordinary.csv"}, "backup_name", optional=False
        )


def test_committed_replacement_journal_cannot_delete_unrelated_csv(
    tmp_path: Path,
) -> None:
    store = SegmentStore(tmp_path)
    manifest = store.replace_all([bar(0)], **KEY)
    directory = next(tmp_path.rglob("manifest.json")).parent
    unrelated = directory / "notes.csv"
    unrelated.write_text("keep", encoding="utf-8")
    journal = directory / ".replace-journal.json"
    journal.write_text(
        json.dumps(
            {
                "version": "segment-replace-v1",
                "old_manifest": asdict(manifest),
                "new_manifest": asdict(manifest),
                "old_data_name": unrelated.name,
                "new_data_name": "bars.csv",
                "backup_name": None,
                "old_downloaded_at": 0,
                "new_downloaded_at": 0,
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(MDInvalidExchangeResponse, match="journal path"):
        recover_replacement_journal(store, journal)
    assert unrelated.read_text(encoding="utf-8") == "keep"


def test_replacement_journal_rejects_symlinked_series_directory(
    tmp_path: Path,
) -> None:
    store = SegmentStore(tmp_path / "store")
    manifest = store.replace_all([bar(0)], **KEY)
    directory = next((tmp_path / "store").rglob("manifest.json")).parent
    redirected = tmp_path / "store" / "redirected-series"
    directory.rename(redirected)
    directory.symlink_to(redirected, target_is_directory=True)
    journal = directory / ".replace-journal.json"
    journal.write_text(
        json.dumps(
            {
                "version": "segment-replace-v1",
                "old_manifest": asdict(manifest),
                "new_manifest": asdict(manifest),
                "old_data_name": "bars.csv",
                "new_data_name": "bars.csv",
                "backup_name": None,
                "old_downloaded_at": 0,
                "new_downloaded_at": 0,
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(MDInvalidExchangeResponse, match="journal path"):
        recover_replacement_journal(store, journal)


@pytest.mark.parametrize("member_name", ["bars.csv", ".replace-backup.csv"])
def test_replacement_journal_rejects_symlinked_members(
    tmp_path: Path, member_name: str
) -> None:
    store = SegmentStore(tmp_path / "store")
    manifest = store.replace_all([bar(0)], **KEY)
    directory = next((tmp_path / "store").rglob("manifest.json")).parent
    victim = tmp_path / "victim.csv"
    victim.write_text("keep", encoding="utf-8")
    member = directory / member_name
    member.unlink(missing_ok=True)
    member.symlink_to(victim)
    journal = directory / ".replace-journal.json"
    journal.write_text(
        json.dumps(
            {
                "version": "segment-replace-v1",
                "old_manifest": asdict(manifest),
                "new_manifest": asdict(manifest),
                "old_data_name": "bars.csv",
                "new_data_name": "bars.csv",
                "backup_name": (
                    ".replace-backup.csv"
                    if member_name == ".replace-backup.csv"
                    else None
                ),
                "old_downloaded_at": 0,
                "new_downloaded_at": 0,
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(MDInvalidExchangeResponse, match="journal path"):
        finish_replacement(store, journal)
    assert victim.read_text(encoding="utf-8") == "keep"


@pytest.mark.parametrize(
    ("data_format", "opposite_name"),
    [("csv", "bars.parquet"), ("parquet", "bars.csv")],
)
def test_replacement_journal_rejects_opposite_format_symlink(
    tmp_path: Path,
    data_format: Literal["csv", "parquet"],
    opposite_name: str,
) -> None:
    store = SegmentStore(tmp_path / "store")
    manifest = store.replace_all([bar(0)], **KEY)
    directory = next((tmp_path / "store").rglob("manifest.json")).parent
    data_name = f"bars.{data_format}"
    if data_format == "parquet":
        manifest = replace(manifest, data_format="parquet")
        (directory / "manifest.json").write_text(
            json.dumps(asdict(manifest)), encoding="utf-8"
        )
        (directory / data_name).write_bytes(b"parquet-placeholder")
    victim = tmp_path / "victim-bars"
    victim.write_text("keep", encoding="utf-8")
    opposite = directory / opposite_name
    opposite.unlink(missing_ok=True)
    opposite.symlink_to(victim)
    journal = directory / ".replace-journal.json"
    journal.write_text(
        json.dumps(
            {
                "version": "segment-replace-v1",
                "old_manifest": asdict(manifest),
                "new_manifest": asdict(manifest),
                "old_data_name": data_name,
                "new_data_name": data_name,
                "backup_name": None,
                "old_downloaded_at": 0,
                "new_downloaded_at": 0,
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(MDInvalidExchangeResponse, match="journal path"):
        finish_replacement(store, journal)
    assert victim.read_text(encoding="utf-8") == "keep"


def test_replacement_journal_rejects_mismatched_manifest_identity(
    tmp_path: Path,
) -> None:
    store = SegmentStore(tmp_path)
    new_manifest = store.replace_all([bar(0)], **KEY)
    old_manifest = replace(new_manifest, symbol="ETHUSDT")
    directory = next(tmp_path.rglob("manifest.json")).parent
    (directory / "manifest.json").unlink()
    journal = directory / ".replace-journal.json"
    journal.write_text(
        json.dumps(
            {
                "version": "segment-replace-v1",
                "old_manifest": asdict(old_manifest),
                "new_manifest": asdict(new_manifest),
                "old_data_name": "bars.csv",
                "new_data_name": "bars.csv",
                "backup_name": None,
                "old_downloaded_at": 0,
                "new_downloaded_at": 0,
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(MDInvalidExchangeResponse, match="manifest identity"):
        recover_replacement_journal(store, journal)


@pytest.mark.parametrize("field", ["new_downloaded_at", "old_downloaded_at"])
def test_replacement_journal_rejects_non_integer_timestamps(
    tmp_path: Path, field: str
) -> None:
    store = SegmentStore(tmp_path)
    manifest = store.replace_all([bar(0)], **KEY)
    directory = next(tmp_path.rglob("manifest.json")).parent
    journal = directory / ".replace-journal.json"
    payload: dict[str, object] = {
        "version": "segment-replace-v1",
        "old_manifest": asdict(manifest),
        "new_manifest": asdict(manifest),
        "old_data_name": "bars.csv",
        "new_data_name": "bars.csv",
        "backup_name": None,
        "old_downloaded_at": 0,
        "new_downloaded_at": 0,
    }
    payload[field] = "not-an-int"
    journal.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(MDInvalidExchangeResponse, match="Invalid segment replacement"):
        recover_replacement_journal(store, journal)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("new_data_name", "bars.parquet"),
        ("old_data_name", "bars.parquet"),
        ("backup_name", ".replace-backup.parquet"),
    ],
)
def test_replacement_journal_names_must_match_manifest_formats(
    tmp_path: Path, field: str, value: str
) -> None:
    store = SegmentStore(tmp_path)
    manifest = store.replace_all([bar(0)], **KEY)
    directory = next(tmp_path.rglob("manifest.json")).parent
    journal = directory / ".replace-journal.json"
    payload: dict[str, object] = {
        "version": "segment-replace-v1",
        "old_manifest": asdict(manifest),
        "new_manifest": asdict(manifest),
        "old_data_name": "bars.csv",
        "new_data_name": "bars.csv",
        "backup_name": None,
        "old_downloaded_at": 0,
        "new_downloaded_at": 0,
    }
    payload[field] = value
    journal.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(MDInvalidExchangeResponse, match=field):
        finish_replacement(store, journal)


def test_replacement_journal_path_binding_rejects_symlink_malformed_and_wrong_dirs(
    tmp_path: Path,
) -> None:
    store = SegmentStore(tmp_path / "store")
    manifest = store.replace_all([bar(0)], **KEY)
    directory = next((tmp_path / "store").rglob("manifest.json")).parent
    journal = directory / ".replace-journal.json"

    target = directory / "journal-target.json"
    target.write_text("{}", encoding="utf-8")
    journal.symlink_to(target)
    with pytest.raises(MDInvalidExchangeResponse, match="journal path"):
        _validate_journal_path(store, journal)
    journal.unlink()

    journal.write_text(json.dumps({"version": "segment-replace-v1"}), encoding="utf-8")
    with pytest.raises(MDInvalidExchangeResponse, match="Invalid segment replacement"):
        _validate_journal_path(store, journal)

    outside = tmp_path / "outside" / ".replace-journal.json"
    outside.parent.mkdir()
    outside.write_text("{}", encoding="utf-8")
    with pytest.raises(MDInvalidExchangeResponse, match="journal path"):
        _validate_journal_path(store, outside, new_manifest=manifest)

    wrong = tmp_path / "store" / "v1" / "wrong" / ".replace-journal.json"
    wrong.parent.mkdir(parents=True)
    wrong.write_text("{}", encoding="utf-8")
    with pytest.raises(MDInvalidExchangeResponse, match="journal path"):
        _validate_journal_path(store, wrong, new_manifest=manifest)


def test_append_recovery_rejects_versionless_journal(tmp_path: Path) -> None:
    store = SegmentStore(tmp_path)
    store.replace_all([bar(0)], **KEY)
    manifest = store.manifest_for(**KEY)
    assert manifest is not None
    directory = next(tmp_path.rglob("manifest.json")).parent
    data_path = directory / "bars.csv"
    journal = directory / ".append-journal.json"
    journal.write_text(
        json.dumps(
            {
                "data_size_before": data_path.stat().st_size,
                "data_size_after": data_path.stat().st_size,
                "old_manifest": asdict(manifest),
                "new_manifest": asdict(manifest),
                "downloaded_at": 0,
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(MDInvalidExchangeResponse, match="append journal"):
        recover_append_journal(store, journal)


@pytest.mark.parametrize("field_name", ["time_close", "trades_count", "downloaded_at"])
def test_presence_unaware_migration_rejects_negative_optional_integer(
    tmp_path: Path, field_name: str
) -> None:
    store = SegmentStore(tmp_path)
    original = replace(bar(0), **{field_name: None})
    store.replace_all([original], **KEY)
    manifest = store.manifest_for(**KEY)
    assert manifest is not None
    legacy = replace(
        manifest,
        schema_version="stage-d-csv-2",
        checksum=presence_unaware_bars_checksum([original]),
        checksum_algorithm="sha256-canonical-v2",
        base_checksum=None,
        base_rows_count=None,
    )
    manifest_path = next(tmp_path.rglob("manifest.json"))
    store._write_manifest_and_index(
        legacy,
        manifest_path=manifest_path,
        downloaded_at=0,
    )
    data_path = manifest_path.parent / "bars.csv"
    with data_path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    rows[0][field_name] = "-1"
    with data_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=store.fields)
        writer.writeheader()
        writer.writerows(rows)

    with pytest.raises(MDInvalidExchangeResponse, match="negative optional integer"):
        store.append_strictly_newer([], **KEY)


def test_service_derived_merge_holds_one_lock_across_read_and_replace() -> None:
    events: list[str] = []

    class Segments:
        locked = False

        @contextmanager
        def series_writer_lock(self, **_key):
            assert not self.locked
            self.locked = True
            events.append("lock")
            try:
                yield
            finally:
                self.locked = False
                events.append("unlock")

        def _read_all_locked(self, **_key):
            assert self.locked
            events.append("read")
            return [bar(0)]

        def _replace_all_locked(self, bars, **_key):
            assert self.locked
            events.append(f"replace:{[item.time for item in bars]}")

    segments = Segments()
    service = object.__new__(MarketDataService)
    service.store = SimpleNamespace(  # type: ignore[assignment]
        segments=segments,
    )
    query = SimpleNamespace(
        instrument=SimpleNamespace(exchange="binance", market="spot", symbol="BTCUSDT"),
        timeframe=SimpleNamespace(canonical="1m"),
    )

    service._merge_derived_bars(query, [])  # type: ignore[arg-type]
    with pytest.raises(CoverageValidationError, match="source-kind"):
        service._merge_derived_bars(
            query,  # type: ignore[arg-type]
            [bar(60_000), bar(120_000, source_kind="mark_kline")],
        )
    service._merge_derived_bars(query, [bar(60_000)])  # type: ignore[arg-type]

    assert events == ["lock", "read", "replace:[0, 60000]", "unlock"]


def test_service_fallback_merge_preserves_append_interleaved_during_fetch() -> None:
    events: list[str] = []

    class Segments:
        def __init__(self) -> None:
            self.rows = [bar(0)]
            self.locked = False
            self.lock_acquisitions = 0

        def manifest_for(self, **_key):
            return None

        @contextmanager
        def series_writer_lock(self, **_key):
            assert not self.locked, "writer lock was recursively acquired"
            self.locked = True
            self.lock_acquisitions += 1
            events.append("lock")
            try:
                yield
            finally:
                self.locked = False
                events.append("unlock")

        def _read_all_locked(self, **_key):
            assert self.locked
            events.append("locked-read")
            return list(self.rows)

        def _replace_all_locked(self, rows, **_key):
            assert self.locked
            events.append("locked-replace")
            self.rows = list(rows)

        def replace_all(self, rows, **_key):
            events.append("public-replace")
            self.rows = list(rows)

    segments = Segments()
    service = object.__new__(MarketDataService)
    service.store = SimpleNamespace(
        segments=segments,
        current=SimpleNamespace(delete_current=lambda _bar: None),
    )  # type: ignore[assignment]
    service._stored_coverage_complete = lambda _query: False  # type: ignore[method-assign]
    service._stored_bars = lambda _query: list(segments.rows)  # type: ignore[method-assign]

    def fetch(_query: BarQuery, progress_callback=None) -> list[MarketBar]:
        del progress_callback
        events.append("fetch")
        segments.rows.append(bar(60_000))
        return [bar(120_000)]

    service._fetch_from_sources = fetch  # type: ignore[method-assign]
    query = BarQuery(
        InstrumentKey("binance", "spot", "BTCUSDT"),
        parse_timeframe("1m"),
        0,
        180_000,
    )

    assert service._ensure_stored(query) is True
    assert [item.time for item in segments.rows] == [0, 60_000, 120_000]
    assert segments.lock_acquisitions == 1
    assert events == ["fetch", "lock", "locked-read", "locked-replace", "unlock"]


@pytest.mark.parametrize("field_name", sorted(PERSISTED_FIELD_CHANGES))
def test_disk_corruption_of_every_persisted_field_is_rejected(
    tmp_path: Path, field_name: str
) -> None:
    store = SegmentStore(tmp_path)
    store.replace_all([bar(0)], **KEY)
    data_path = next(tmp_path.rglob("bars.csv"))
    with data_path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    rows[0][field_name] = str(PERSISTED_FIELD_CHANGES[field_name])
    with data_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=store.fields)
        writer.writeheader()
        writer.writerows(rows)

    with pytest.raises(MDInvalidExchangeResponse, match="checksum mismatch"):
        store.read_all(**KEY)


def test_runtime_source_and_metadata_are_explicitly_not_persisted_or_hashed(
    tmp_path: Path,
) -> None:
    persisted = set(SegmentStore.fields)
    model_fields = {item.name for item in fields(MarketBar)}
    assert model_fields - persisted == {"source", "metadata"}

    original = bar(0)
    runtime_variant = replace(original, source="archive", metadata={"other": 1})
    assert market_bar_checksum(runtime_variant) == market_bar_checksum(original)

    store = SegmentStore(tmp_path)
    store.replace_all([original], **KEY)
    restored = store.read_all(**KEY)[0]
    assert restored.source == ""
    assert restored.metadata == {}


@pytest.mark.parametrize("field_name", ["time_close", "trades_count", "downloaded_at"])
def test_checksum_distinguishes_absent_optional_integer_from_minus_one(
    field_name: str,
) -> None:
    absent = replace(bar(0), **{field_name: None})
    minus_one = replace(bar(0), **{field_name: -1})

    assert market_bar_checksum(absent) != market_bar_checksum(minus_one)


def test_presence_unaware_checksum_and_tail_chain_remain_readable() -> None:
    item = bar(0, trades_count=None, downloaded_at=None)
    checksum = presence_unaware_bars_checksum([item])

    assert len(checksum) == 64
    assert (
        len(
            extend_tail_chain(
                checksum,
                bar(60_000),
                algorithm=PRESENCE_UNAWARE_TAIL_CHAIN_CHECKSUM,
            )
        )
        == 64
    )


def test_bulk_and_direct_upsert_reject_conflicting_closed_candle(
    tmp_path: Path,
) -> None:
    candle_store = CandleStore(tmp_path)
    original = bar(0)
    conflicting = bar(0, close=1.75)
    candle_store.segments.replace_all([original], **KEY)
    adapter = _CandleStoreAdapter(candle_store)

    with pytest.raises(ValueError, match="conflicting closed candle"):
        adapter._bulk_write_closed([conflicting])
    with pytest.raises(MDCacheConflict, match="Conflicting closed candle"):
        candle_store.segments.upsert_closed(conflicting)


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("taker_buy_base_volume", 9.0),
        ("taker_buy_quote_volume", 11.0),
    ],
)
def test_bulk_duplicate_comparator_includes_taker_buy_volumes(
    field_name: str, value: float
) -> None:
    original = bar(0)
    changed = replace(original, **{field_name: value})

    assert not _same_candle_payload(original, changed)


def _run_thread(
    operation: Callable[[], object], errors: list[BaseException], done: threading.Event
) -> None:
    try:
        operation()
    except BaseException as exc:  # pragma: no branch - asserted by callers
        errors.append(exc)
    finally:
        done.set()


def test_bulk_closed_write_holds_one_lock_across_read_merge_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    candle_store = CandleStore(tmp_path)
    candle_store.segments.replace_all([bar(0)], **KEY)
    adapter = _CandleStoreAdapter(candle_store)
    bulk_read_done = threading.Event()
    release_bulk = threading.Event()
    bulk_done = threading.Event()
    append_done = threading.Event()
    errors: list[BaseException] = []
    original_read_all = candle_store.segments._read_all_locked

    def paused_bulk_read(**key: Any) -> list[MarketBar]:
        rows = original_read_all(**key)
        if threading.current_thread().name == "bulk-writer":
            bulk_read_done.set()
            assert release_bulk.wait(2)
        return rows

    monkeypatch.setattr(candle_store.segments, "_read_all_locked", paused_bulk_read)
    bulk = threading.Thread(
        name="bulk-writer",
        target=_run_thread,
        args=(
            lambda: adapter._bulk_write_closed([bar(0), bar(60_000)]),
            errors,
            bulk_done,
        ),
    )
    append = threading.Thread(
        name="tail-appender",
        target=_run_thread,
        args=(
            lambda: candle_store.segments.append_strictly_newer([bar(120_000)], **KEY),
            errors,
            append_done,
        ),
    )

    bulk.start()
    assert bulk_read_done.wait(2)
    append.start()
    append_done.wait(0.2)
    release_bulk.set()
    bulk.join(2)
    append.join(2)

    assert bulk_done.is_set() and append_done.is_set()
    assert not errors
    assert [item.time for item in candle_store.segments.read_all(**KEY)] == [
        0,
        60_000,
        120_000,
    ]


def test_conflicting_closed_commits_check_and_commit_under_one_lock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    first_store = CandleStore(tmp_path)
    second_store = CandleStore(tmp_path)
    first_read_done = threading.Event()
    release_first = threading.Event()
    first_done = threading.Event()
    second_done = threading.Event()
    results: list[str] = []
    errors: list[BaseException] = []
    original_first_get = first_store.segments.get

    def paused_first_get(
        key: tuple[str, str, str, str, str, int],
    ) -> MarketBar | None:
        existing = original_first_get(key)
        first_read_done.set()
        assert release_first.wait(2)
        return existing

    monkeypatch.setattr(first_store.segments, "get", paused_first_get)

    def commit(store: CandleStore, item: MarketBar) -> None:
        results.append(store.commit_closed(item).status)

    first = threading.Thread(
        target=_run_thread,
        args=(lambda: commit(first_store, bar(0, close=1.6)), errors, first_done),
    )
    second = threading.Thread(
        target=_run_thread,
        args=(lambda: commit(second_store, bar(0, close=1.7)), errors, second_done),
    )

    first.start()
    assert first_read_done.wait(2)
    second.start()
    second_done.wait(0.2)
    release_first.set()
    first.join(2)
    second.join(2)

    assert first_done.is_set() and second_done.is_set()
    assert results == ["committed"]
    assert len(errors) == 1 and isinstance(errors[0], MDCacheConflict)


def test_append_and_replace_are_serialized_with_append_first(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = SegmentStore(tmp_path)
    store.replace_all([bar(0)], **KEY)
    append_entered = threading.Event()
    release_append = threading.Event()
    replace_done = threading.Event()
    append_done = threading.Event()
    errors: list[BaseException] = []
    original_append_bytes = segment_append.append_bytes

    def paused_append(path: Path, payload: bytes) -> None:
        append_entered.set()
        assert release_append.wait(2)
        original_append_bytes(path, payload)

    monkeypatch.setattr(segment_append, "append_bytes", paused_append)
    append_thread = threading.Thread(
        target=_run_thread,
        args=(
            lambda: store.append_strictly_newer([bar(60_000)], **KEY),
            errors,
            append_done,
        ),
    )
    replace_thread = threading.Thread(
        target=_run_thread,
        args=(
            lambda: store.replace_all([bar(0, close=1.7), bar(120_000)], **KEY),
            errors,
            replace_done,
        ),
    )

    append_thread.start()
    assert append_entered.wait(2)
    replace_thread.start()
    assert not replace_done.wait(0.2), "replace bypassed the per-series writer lock"
    release_append.set()
    append_thread.join(2)
    replace_thread.join(2)

    assert not errors
    assert append_done.is_set() and replace_done.is_set()
    expected = [
        replace(bar(0, close=1.7), source="", metadata={}),
        replace(bar(120_000), source="", metadata={}),
    ]
    assert store.read_all(**KEY) == expected


def test_two_appends_are_serialized_and_conflict_deterministically(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = SegmentStore(tmp_path)
    store.replace_all([bar(0)], **KEY)
    first_entered = threading.Event()
    release_first = threading.Event()
    second_done = threading.Event()
    first_done = threading.Event()
    errors: list[BaseException] = []
    original_append_bytes = segment_append.append_bytes
    calls = 0

    def paused_first_append(path: Path, payload: bytes) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            first_entered.set()
            assert release_first.wait(2)
        original_append_bytes(path, payload)

    monkeypatch.setattr(segment_append, "append_bytes", paused_first_append)
    first = threading.Thread(
        target=_run_thread,
        args=(
            lambda: store.append_strictly_newer([bar(60_000)], **KEY),
            errors,
            first_done,
        ),
    )
    second = threading.Thread(
        target=_run_thread,
        args=(
            lambda: store.append_strictly_newer([bar(60_000, close=1.8)], **KEY),
            errors,
            second_done,
        ),
    )

    first.start()
    assert first_entered.wait(2)
    second.start()
    assert not second_done.wait(
        0.2
    ), "second append bypassed the per-series writer lock"
    release_first.set()
    first.join(2)
    second.join(2)

    assert first_done.is_set() and second_done.is_set()
    assert len(errors) == 1 and isinstance(errors[0], MDCacheConflict)
    expected = [
        replace(bar(0), source="", metadata={}),
        replace(bar(60_000), source="", metadata={}),
    ]
    assert store.read_all(**KEY) == expected


def test_vacuum_uses_the_same_per_series_writer_lock(tmp_path: Path) -> None:
    store = SegmentStore(tmp_path)
    store.replace_all([bar(0)], **KEY)
    data_path = next(tmp_path.rglob("bars.csv"))
    stale_path = data_path.with_suffix(".parquet")
    stale_path.write_bytes(b"stale")
    done = threading.Event()
    errors: list[BaseException] = []

    with store.series_writer_lock(**KEY):
        worker = threading.Thread(
            target=_run_thread,
            args=(store.vacuum, errors, done),
        )
        worker.start()
        assert not done.wait(0.2), "vacuum bypassed the per-series writer lock"
    worker.join(2)

    assert done.is_set()
    assert not errors
    assert not stale_path.exists()


def _install_binance_transport(
    monkeypatch: pytest.MonkeyPatch, handler: Callable[[httpx.Request], httpx.Response]
) -> None:
    real_client = httpx.Client

    def factory(*args: object, **kwargs: object) -> httpx.Client:
        kwargs["transport"] = httpx.MockTransport(handler)
        return real_client(*args, **kwargs)

    monkeypatch.setattr(binance_provider.httpx, "Client", factory)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("failure", "expected"),
    [
        ("geo", "GEO_RESTRICTED"),
        ("dns", "DNS_FAILURE"),
        ("timeout", "TIMEOUT"),
    ],
)
async def test_provider_wrapped_transport_failures_keep_specific_classification(
    monkeypatch: pytest.MonkeyPatch, failure: str, expected: str
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if failure == "geo":
            return httpx.Response(
                451, request=request, text="Unavailable For Legal Reasons"
            )
        if failure == "dns":
            try:
                raise OSError("temporary failure in name resolution")
            except OSError as cause:
                raise httpx.ConnectError("connect failed", request=request) from cause
        raise httpx.ReadTimeout("read operation timed out", request=request)

    _install_binance_transport(monkeypatch, handler)

    def operation() -> object:
        return binance_provider.binance_get_bars_sync(
            "BTCUSDT",
            "1m",
            None,
            None,
            BinanceConfig(),
            market="spot",
            timeout=0.01,
            max_retries=0,
            max_bars=1,
        )

    result = await acceptance._run_live_check("binance_rest", operation)

    assert not result.passed
    assert result.evidence["failure_classification"] == expected


def test_nested_failure_evidence_handles_invalid_status_and_sequences() -> None:
    wrapped = MDNetworkUnavailable(
        "wrapped",
        details={
            "status": "not-an-integer",
            "causes": [{"status_code": 451, "message": "legal reasons"}],
        },
    )

    assert acceptance._classify_live_failure(wrapped) == "GEO_RESTRICTED"


def _v2_bars_checksum(items: list[MarketBar]) -> str:
    """Frozen compatibility fixture for the presence-unaware v2 row encoding."""
    digest = hashlib.sha256()
    for item in sorted(items, key=lambda candidate: candidate.time):
        for text in (
            item.exchange.lower(),
            item.market.lower(),
            item.symbol.upper(),
            item.source_kind,
            item.source_transport,
            item.timeframe,
        ):
            encoded = text.encode("utf-8")
            digest.update(struct.pack(">I", len(encoded)))
            digest.update(encoded)
        digest.update(
            struct.pack(
                ">qqqqddddddddd?????",
                item.time,
                item.time_close if item.time_close is not None else -1,
                item.trades_count if item.trades_count is not None else -1,
                item.downloaded_at if item.downloaded_at is not None else -1,
                item.open,
                item.high,
                item.low,
                item.close,
                item.volume,
                item.quote_volume if item.quote_volume is not None else 0.0,
                item.turnover if item.turnover is not None else 0.0,
                (
                    item.taker_buy_base_volume
                    if item.taker_buy_base_volume is not None
                    else 0.0
                ),
                (
                    item.taker_buy_quote_volume
                    if item.taker_buy_quote_volume is not None
                    else 0.0
                ),
                item.quote_volume is not None,
                item.turnover is not None,
                item.taker_buy_base_volume is not None,
                item.taker_buy_quote_volume is not None,
                item.is_closed,
            )
        )
        digest.update(b"\n")
    return digest.hexdigest()


def test_presence_unaware_v2_manifest_is_validated_and_migrated(tmp_path: Path) -> None:
    store = SegmentStore(tmp_path)
    items = [bar(0), bar(60_000)]
    store.replace_all(items, **KEY)
    manifest_path = next(tmp_path.rglob("manifest.json"))
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    v2_checksum = _v2_bars_checksum(items)
    payload.update(
        checksum=v2_checksum,
        checksum_algorithm="sha256-tail-chain-v2",
        base_checksum=v2_checksum,
        base_rows_count=len(items),
    )
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")

    migrated = store.append_strictly_newer([], **KEY)

    assert migrated.checksum_algorithm == "sha256-tail-chain-v4"
    assert migrated.base_checksum == bars_checksum(items)
    assert migrated.checksum == migrated.base_checksum
    assert store.read_all(**KEY) == [
        replace(item, source="", metadata={}) for item in items
    ]


def test_metadata_only_manifest_migration_is_journaled_and_recovers_index(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = SegmentStore(tmp_path)
    items = [bar(0), bar(60_000)]
    store.replace_all(items, **KEY)
    manifest_path = next(tmp_path.rglob("manifest.json"))
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload.pop("checksum_algorithm", None)
    payload.pop("base_checksum", None)
    payload.pop("base_rows_count", None)
    payload["schema_version"] = "stage-d-csv-1"
    payload["checksum"] = legacy_bars_checksum(items)
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")
    with sqlite3.connect(store.index_path) as db:
        db.execute(
            "UPDATE marketdata_segments SET checksum = ?", (payload["checksum"],)
        )
        db.commit()

    monkeypatch.setattr(
        store,
        "_replace_index_manifest",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            BaseException("simulated death after metadata manifest")
        ),
    )
    with pytest.raises(BaseException, match="metadata manifest"):
        store.append_strictly_newer([], **KEY)

    assert list(tmp_path.rglob(".append-journal.json"))
    recovered = SegmentStore(tmp_path)
    migrated = recovered.manifest_for(**KEY)
    assert migrated is not None
    assert migrated.checksum_algorithm == "sha256-tail-chain-v4"
    assert not list(tmp_path.rglob(".append-journal.json"))
    with sqlite3.connect(recovered.index_path) as db:
        indexed = db.execute(
            "SELECT rows_count, start_time, end_time, checksum FROM marketdata_segments"
        ).fetchall()
    assert indexed == [(2, 0, 60_000, migrated.checksum)]


def test_documented_pytest_selectors_match_release_gate() -> None:
    expected = '-m "not live_network"'
    assert expected in Path("scripts/release_gate.sh").read_text(encoding="utf-8")
    for path in (
        Path("README.md"),
        Path("docs/RELEASE_4_0.md"),
        Path("docs/DEVELOPMENT.md"),
    ):
        assert expected in path.read_text(encoding="utf-8"), path


def test_append_dispatches_pending_journal_recovery(tmp_path: Path) -> None:
    store = SegmentStore(tmp_path)
    initial = store.replace_all([bar(0)], **KEY)
    data_path, manifest_path = store._paths(
        **KEY, source_kind="trade_kline", data_format="csv"
    )
    journal = data_path.parent / ".append-journal.json"
    size = data_path.stat().st_size
    journal.write_text(
        json.dumps(
            {
                "version": "segment-append-v1",
                "data_size_before": size,
                "data_size_after": size,
                "old_manifest": asdict(initial),
                "new_manifest": asdict(initial),
                "downloaded_at": 60_000,
            }
        ),
        encoding="utf-8",
    )

    updated = store.append_strictly_newer([bar(60_000)], **KEY)

    assert not journal.exists()
    assert manifest_path.exists()
    assert updated.rows_count == 2


def test_tail_chain_legacy_and_unsupported_algorithm_branches() -> None:
    digest = extend_tail_chain("0" * 64, bar(0), algorithm=LEGACY_TAIL_CHAIN_CHECKSUM)
    assert len(digest) == 64
    with pytest.raises(MDInvalidExchangeResponse, match="Unsupported"):
        extend_tail_chain("0" * 64, bar(0), algorithm="unknown")
