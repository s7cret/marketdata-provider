from __future__ import annotations

import json
import socket
from dataclasses import asdict, replace
from pathlib import Path

import pytest

from marketdata_provider.acceptance import _classify_live_failure
from marketdata_provider.core.bar import MarketBar
from marketdata_provider.errors import MDInvalidExchangeResponse, MDUnsupportedFeature
from marketdata_provider.store import SegmentStore, segment_replace
from marketdata_provider.store.segment_append import read_last_csv_bar
from marketdata_provider.store.segment_checksums import (
    CANONICAL_CHECKSUM,
    LEGACY_CANONICAL_CHECKSUM,
    LEGACY_TAIL_CHAIN_CHECKSUM,
    PRESENCE_UNAWARE_CANONICAL_CHECKSUM,
    PRESENCE_UNAWARE_TAIL_CHAIN_CHECKSUM,
    TAIL_CHAIN_CHECKSUM,
    extend_tail_chain,
    legacy_bars_checksum,
    validate_csv_checksum,
)
from marketdata_provider.store.segment_read import _parquet_checksum
from marketdata_provider.store.segment_replace import (
    _atomic_copy,
    _load_journal,
    begin_replacement,
    finish_replacement,
    recover_replacement_journal,
)

KEY = {
    "exchange": "binance",
    "market": "spot",
    "symbol": "BTCUSDT",
    "timeframe": "1m",
}


def bar(t: int) -> MarketBar:
    return MarketBar(
        time=t,
        open=1.0,
        high=2.0,
        low=0.5,
        close=1.5,
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
    )


def manifest_path(root: Path) -> Path:
    return next(root.rglob("manifest.json"))


def data_path(root: Path) -> Path:
    return next(root.rglob("bars.csv"))


def make_legacy(root: Path) -> tuple[SegmentStore, dict[str, object]]:
    store = SegmentStore(root)
    store.replace_all([bar(0), bar(60_000)], **KEY)
    path = manifest_path(root)
    payload = json.loads(path.read_text())
    payload.pop("checksum_algorithm", None)
    payload.pop("base_checksum", None)
    payload.pop("base_rows_count", None)
    payload["checksum"] = legacy_bars_checksum([bar(0), bar(60_000)])
    payload["schema_version"] = "stage-d-csv-1"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return store, payload


def test_append_initialization_and_fail_closed_manifest_edges(tmp_path: Path) -> None:
    fresh = SegmentStore(tmp_path / "fresh")
    assert fresh.append_strictly_newer([bar(0)], **KEY).rows_count == 1

    non_csv = SegmentStore(tmp_path / "non-csv")
    non_csv.replace_all([bar(0)], **KEY)
    path = manifest_path(tmp_path / "non-csv")
    payload = json.loads(path.read_text())
    payload["data_format"] = "parquet"
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(MDUnsupportedFeature, match="only for CSV"):
        non_csv.append_strictly_newer([bar(60_000)], **KEY)

    missing = SegmentStore(tmp_path / "missing")
    missing.replace_all([bar(0)], **KEY)
    data_path(tmp_path / "missing").unlink()
    with pytest.raises(MDInvalidExchangeResponse, match="no CSV data"):
        missing.append_strictly_newer([bar(60_000)], **KEY)

    invalid = SegmentStore(tmp_path / "invalid")
    invalid.replace_all([bar(0)], **KEY)
    manifest_path(tmp_path / "invalid").write_text("[]", encoding="utf-8")
    with pytest.raises(MDInvalidExchangeResponse, match="JSON object"):
        invalid.append_strictly_newer([bar(60_000)], **KEY)


def test_empty_legacy_duplicate_and_tail_shape_edges(tmp_path: Path) -> None:
    legacy_empty, _ = make_legacy(tmp_path / "legacy-empty")
    migrated = legacy_empty.append_strictly_newer([], **KEY)
    assert migrated.checksum_algorithm == TAIL_CHAIN_CHECKSUM

    legacy_duplicate, _ = make_legacy(tmp_path / "legacy-duplicate")
    migrated = legacy_duplicate.append_strictly_newer([bar(60_000)], **KEY)
    assert migrated.rows_count == 2
    assert migrated.checksum_algorithm == TAIL_CHAIN_CHECKSUM

    empty = SegmentStore(tmp_path / "empty")
    empty.replace_all([], **KEY)
    with pytest.raises(MDInvalidExchangeResponse, match="empty segment"):
        empty.append_strictly_newer([bar(0)], **KEY)

    mismatch = SegmentStore(tmp_path / "tail-mismatch")
    mismatch.replace_all([bar(0), bar(60_000)], **KEY)
    path = manifest_path(tmp_path / "tail-mismatch")
    payload = json.loads(path.read_text())
    payload["end_time"] = 120_000
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(MDInvalidExchangeResponse, match="does not match manifest"):
        mismatch.append_strictly_newer([bar(180_000)], **KEY)

    identity = SegmentStore(tmp_path / "identity")
    identity.replace_all([bar(0)], **KEY)
    wrong = replace(bar(60_000), symbol="ETHUSDT")
    with pytest.raises(MDInvalidExchangeResponse, match="identity"):
        identity.append_strictly_newer([wrong], **KEY)
    assert identity.append_strictly_newer([bar(60_000), bar(60_000)], **KEY).rows_count == 2


def test_checksum_and_csv_tail_guards(tmp_path: Path) -> None:
    with pytest.raises(MDInvalidExchangeResponse, match="encoding"):
        extend_tail_chain("not-hex", bar(0))
    with pytest.raises(MDInvalidExchangeResponse, match="length"):
        extend_tail_chain("00", bar(0))

    csv_path = tmp_path / "bars.csv"
    csv_path.write_text("time\n", encoding="utf-8")
    with pytest.raises(MDInvalidExchangeResponse, match="Unsupported"):
        validate_csv_checksum(csv_path, {"checksum_algorithm": "unknown"})
    with pytest.raises(MDInvalidExchangeResponse, match="metadata"):
        validate_csv_checksum(
            csv_path,
            {
                "checksum_algorithm": TAIL_CHAIN_CHECKSUM,
                "base_rows_count": -1,
                "base_checksum": 1,
            },
        )

    header_only = tmp_path / "header.csv"
    header_only.write_text("time\n", encoding="utf-8")
    assert read_last_csv_bar(header_only) is None
    blank = tmp_path / "blank.csv"
    blank.write_bytes(b"\n\n")
    assert read_last_csv_bar(blank) is None
    huge = tmp_path / "huge.csv"
    huge.write_bytes(b"time\n" + b"x" * 1_048_577)
    with pytest.raises(MDInvalidExchangeResponse, match="too large"):
        read_last_csv_bar(huge)


def test_recovery_committed_invalid_and_truncated_journals(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "committed"
    store = SegmentStore(root)
    store.replace_all([bar(0), bar(60_000)], **KEY)
    original_unlink = Path.unlink

    def crash_before_journal_cleanup(path: Path, *args: object, **kwargs: object) -> None:
        if path.name == ".append-journal.json":
            raise BaseException("simulated death after commit")  # noqa: TRY002
        original_unlink(path, *args, **kwargs)

    with monkeypatch.context() as scoped:
        scoped.setattr(Path, "unlink", crash_before_journal_cleanup)
        with pytest.raises(BaseException, match="after commit"):
            store.append_strictly_newer([bar(120_000)], **KEY)
    assert list(root.rglob(".append-journal.json"))
    retried = store.append_strictly_newer([bar(180_000)], **KEY)
    assert retried.rows_count == 4
    assert not list(root.rglob(".append-journal.json"))

    invalid_root = tmp_path / "invalid-journal"
    invalid_dir = invalid_root / "v1" / "series"
    invalid_dir.mkdir(parents=True)
    (invalid_dir / ".append-journal.json").write_text("{}", encoding="utf-8")
    with pytest.raises(MDInvalidExchangeResponse, match="Invalid segment append journal"):
        SegmentStore(invalid_root)

    truncated_root = tmp_path / "truncated"
    truncated = SegmentStore(truncated_root)
    manifest = truncated.replace_all([bar(0)], **KEY)
    journal = data_path(truncated_root).parent / ".append-journal.json"
    journal.write_text(
        json.dumps(
            {
                "version": "segment-append-v1",
                "old_manifest": asdict(manifest),
                "new_manifest": asdict(manifest),
                "data_size_before": data_path(truncated_root).stat().st_size + 1,
                "data_size_after": data_path(truncated_root).stat().st_size + 1,
                "downloaded_at": 0,
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(MDInvalidExchangeResponse, match="truncated"):
        SegmentStore(truncated_root)


def test_dns_and_parquet_checksum_failure_classification(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert _classify_live_failure(socket.gaierror("getaddrinfo failed")) == "DNS_FAILURE"

    store = SegmentStore(tmp_path)
    manifest = store.replace_all([bar(0)], **KEY)
    csv_path = data_path(tmp_path)
    parquet_path = csv_path.with_suffix(".parquet")
    csv_path.rename(parquet_path)
    payload = asdict(manifest)
    payload["data_format"] = "parquet"
    payload["checksum"] = "bad"
    manifest_path(tmp_path).write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setattr(store, "_read_parquet", lambda _path: [bar(0)])
    with pytest.raises(MDInvalidExchangeResponse, match="checksum mismatch"):
        store.read_all(**KEY)


@pytest.mark.parametrize(
    ("manifest", "expected_algorithm"),
    [
        ({"schema_version": "stage-d-parquet-1"}, LEGACY_CANONICAL_CHECKSUM),
        (
            {"checksum_algorithm": PRESENCE_UNAWARE_CANONICAL_CHECKSUM},
            PRESENCE_UNAWARE_CANONICAL_CHECKSUM,
        ),
        ({"checksum_algorithm": LEGACY_TAIL_CHAIN_CHECKSUM}, LEGACY_TAIL_CHAIN_CHECKSUM),
        (
            {"checksum_algorithm": PRESENCE_UNAWARE_TAIL_CHAIN_CHECKSUM},
            PRESENCE_UNAWARE_TAIL_CHAIN_CHECKSUM,
        ),
        ({"checksum_algorithm": CANONICAL_CHECKSUM}, CANONICAL_CHECKSUM),
    ],
)
def test_parquet_checksum_legacy_and_presence_algorithms(
    manifest: dict[str, object], expected_algorithm: str
) -> None:
    assert _parquet_checksum([bar(0)], manifest)
    assert manifest.get("checksum_algorithm", expected_algorithm) == expected_algorithm


def test_parquet_checksum_and_manifest_fail_closed_edges(tmp_path: Path) -> None:
    with pytest.raises(MDInvalidExchangeResponse, match="Unsupported Parquet"):
        _parquet_checksum([bar(0)], {"checksum_algorithm": "unknown"})

    store = SegmentStore(tmp_path)
    directory = store._dir(**KEY, source_kind="trade_kline")
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "manifest.json").write_text("[]", encoding="utf-8")
    with pytest.raises(MDInvalidExchangeResponse, match="JSON object"):
        store.read_all(**KEY)

    (directory / ".replace-journal.json").write_text("{}", encoding="utf-8")
    with pytest.raises(MDInvalidExchangeResponse, match="Invalid segment replacement"):
        store.read_all(**KEY)


def test_replacement_helpers_cover_recovery_and_copy_faults(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = SegmentStore(tmp_path / "existing")
    manifest = store.replace_all([bar(0)], **KEY)
    current_data = data_path(tmp_path / "existing")
    directory = current_data.parent
    stale_journal = directory / ".replace-journal.json"
    stale_journal.write_text("{}", encoding="utf-8")
    recovered: list[Path] = []

    def discard_stale(_store: object, path: Path) -> None:
        recovered.append(path)
        path.unlink()

    def fail_link(_source: Path, _destination: Path) -> None:
        raise OSError("no hardlinks")

    def copy_fallback(source: Path, destination: Path) -> None:
        destination.write_bytes(source.read_bytes())

    with monkeypatch.context() as scoped:
        scoped.setattr(segment_replace, "recover_replacement_journal", discard_stale)
        scoped.setattr(segment_replace.os, "link", fail_link)
        scoped.setattr(segment_replace, "_atomic_copy", copy_fallback)
        journal = begin_replacement(
            store,
            old_manifest=manifest,
            new_manifest=manifest,
            old_data_path=current_data,
            new_data_path=current_data,
            downloaded_at=int(bar(0).downloaded_at or 0),
        )
    assert recovered == [stale_journal]
    assert journal.exists()
    recover_replacement_journal(store, journal)
    assert not journal.exists()

    cleanup_dir = directory
    old_path = cleanup_dir / "bars.csv"
    new_path = cleanup_dir / "bars.parquet"
    backup_path = cleanup_dir / ".replace-backup.csv"
    new_path.write_text("new", encoding="utf-8")
    backup_path.write_text("backup", encoding="utf-8")
    parquet_manifest = replace(manifest, data_format="parquet")
    cleanup_journal = cleanup_dir / ".replace-journal.json"
    cleanup_journal.write_text(
        json.dumps(
            {
                "version": "segment-replace-v1",
                "old_manifest": asdict(manifest),
                "new_manifest": asdict(parquet_manifest),
                "old_data_name": old_path.name,
                "new_data_name": new_path.name,
                "backup_name": backup_path.name,
                "old_downloaded_at": 0,
                "new_downloaded_at": 0,
            }
        ),
        encoding="utf-8",
    )
    finish_replacement(store, cleanup_journal)
    assert not old_path.exists()
    assert not backup_path.exists()

    source = tmp_path / "source.bin"
    destination = tmp_path / "destination.bin"
    source.write_bytes(b"payload")
    _atomic_copy(source, destination)
    assert destination.read_bytes() == b"payload"

    with monkeypatch.context() as scoped:
        scoped.setattr(segment_replace.os, "replace", lambda *_args: (_ for _ in ()).throw(OSError("replace")))
        with pytest.raises(OSError, match="replace"):
            _atomic_copy(source, tmp_path / "failed.bin")


def test_replacement_journal_invalid_and_rollback_paths(tmp_path: Path) -> None:
    store = SegmentStore(tmp_path)
    manifest = store.replace_all([bar(0)], **KEY)
    directory = data_path(tmp_path).parent

    malformed = directory / ".replace-journal.json"
    malformed.write_text(
        json.dumps({"version": "segment-replace-v1"}), encoding="utf-8"
    )
    with pytest.raises(MDInvalidExchangeResponse, match="Invalid segment replacement"):
        recover_replacement_journal(store, malformed)
    malformed.unlink()

    bad_json = directory / "bad-json.json"
    bad_json.write_text("{", encoding="utf-8")
    with pytest.raises(MDInvalidExchangeResponse, match="Invalid segment replacement"):
        _load_journal(bad_json)
    bad_json.write_text(json.dumps({"version": "wrong"}), encoding="utf-8")
    with pytest.raises(MDInvalidExchangeResponse, match="Invalid segment replacement"):
        _load_journal(bad_json)

    current_data = data_path(tmp_path)
    current_manifest = manifest_path(tmp_path)
    rollback_new = current_data
    rollback_new.write_text("new", encoding="utf-8")
    current_manifest.write_text("{", encoding="utf-8")
    no_old = directory / ".replace-journal.json"
    no_old.write_text(
        json.dumps(
            {
                "version": "segment-replace-v1",
                "old_manifest": None,
                "new_manifest": asdict(manifest),
                "old_data_name": None,
                "new_data_name": rollback_new.name,
                "backup_name": None,
                "old_downloaded_at": 0,
                "new_downloaded_at": 0,
            }
        ),
        encoding="utf-8",
    )
    recover_replacement_journal(store, no_old)
    assert not rollback_new.exists()
    assert not current_manifest.exists()

    parquet_manifest = replace(manifest, data_format="parquet")
    old_path = directory / "bars.parquet"
    new_path = directory / "bars.csv"
    old_path.write_bytes(b"old")
    new_path.write_text("new", encoding="utf-8")
    restore_old = directory / ".replace-journal.json"
    restore_old.write_text(
        json.dumps(
            {
                "version": "segment-replace-v1",
                "old_manifest": asdict(parquet_manifest),
                "new_manifest": asdict(manifest),
                "old_data_name": old_path.name,
                "new_data_name": new_path.name,
                "backup_name": None,
                "old_downloaded_at": 0,
                "new_downloaded_at": 0,
            }
        ),
        encoding="utf-8",
    )
    recover_replacement_journal(store, restore_old)
    assert not new_path.exists()

    current_manifest.unlink(missing_ok=True)
    old_path.unlink(missing_ok=True)
    missing_old = directory / ".replace-journal.json"
    missing_old.write_text(
        json.dumps(
            {
                "version": "segment-replace-v1",
                "old_manifest": asdict(parquet_manifest),
                "new_manifest": asdict(manifest),
                "old_data_name": "bars.parquet",
                "new_data_name": "bars.csv",
                "backup_name": None,
                "old_downloaded_at": 0,
                "new_downloaded_at": 0,
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(MDInvalidExchangeResponse, match="without old data"):
        recover_replacement_journal(store, missing_old)
