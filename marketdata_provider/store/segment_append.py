from __future__ import annotations

import csv
import fcntl
import io
import json
import os
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import asdict, replace
from pathlib import Path
from typing import Any, cast

from marketdata_provider.core.bar import MarketBar
from marketdata_provider.errors import (
    MDCacheConflict,
    MDInvalidExchangeResponse,
    MDUnsupportedFeature,
)
from marketdata_provider.store.segment_checksums import (
    TAIL_CHAIN_CHECKSUM,
    csv_canonical_checksum,
    extend_tail_chain,
    market_bar_checksum,
    validate_csv_checksum,
    validate_persisted_bar_semantics,
)
from marketdata_provider.store.segment_manifest import SegmentManifest
from marketdata_provider.store.segment_rows import row_to_bar
from marketdata_provider.timeframes import canonical_timeframe
from marketdata_provider.validation import validate_bars


def append_strictly_newer(
    store: Any,
    bars: Iterable[MarketBar],
    *,
    exchange: str,
    market: str,
    symbol: str,
    timeframe: str,
    source_kind: str = "trade_kline",
) -> SegmentManifest:
    """Append a CSV tail without parsing or replacing historical rows."""

    key = {
        "exchange": exchange,
        "market": market,
        "symbol": symbol,
        "timeframe": timeframe,
        "source_kind": source_kind,
    }
    data_path, manifest_path = store._paths(**key, data_format="csv")
    data_path.parent.mkdir(parents=True, exist_ok=True)
    with store.series_writer_lock(**key):
        journal_path = data_path.parent / ".append-journal.json"
        manifest = store.manifest_for(**key)
        incoming = validated_append_bars(bars, key=key)
        if manifest is None:
            return store._replace_all_locked(incoming, **key, data_format="csv")
        if manifest.data_format != "csv":
            raise MDUnsupportedFeature(
                "Bounded incremental append is supported only for CSV segments"
            )
        if not data_path.exists():
            raise MDInvalidExchangeResponse("Segment manifest has no CSV data file")
        stored_manifest = manifest

        raw_manifest = cast(
            dict[str, object], json.loads(manifest_path.read_text(encoding="utf-8"))
        )
        legacy = manifest.checksum_algorithm != TAIL_CHAIN_CHECKSUM or (
            manifest.base_checksum is None or manifest.base_rows_count is None
        )
        if legacy:
            validate_csv_checksum(data_path, cast(dict[str, object], raw_manifest))
            current_base_checksum = csv_canonical_checksum(data_path)
            manifest = replace(
                manifest,
                schema_version="stage-d-csv-3",
                checksum=current_base_checksum,
                checksum_algorithm=TAIL_CHAIN_CHECKSUM,
                base_checksum=current_base_checksum,
                base_rows_count=manifest.rows_count,
            )

        if not incoming:
            if legacy:
                commit_metadata_migration(
                    store,
                    journal_path=journal_path,
                    data_path=data_path,
                    manifest_path=manifest_path,
                    old_manifest=stored_manifest,
                    new_manifest=manifest,
                    downloaded_at=store._indexed_downloaded_at(stored_manifest),
                )
            return manifest

        last = read_last_csv_bar(data_path)
        if manifest.rows_count <= 0 or manifest.end_time is None or last is None:
            raise MDInvalidExchangeResponse("Cannot append to an empty segment manifest")
        if last.time != manifest.end_time:
            raise MDInvalidExchangeResponse(
                "Segment tail does not match manifest end_time",
                details={"manifest_end": manifest.end_time, "data_end": last.time},
            )

        first = incoming[0]
        if first.time < manifest.end_time:
            raise MDCacheConflict(
                "Append candle is older than stored tail",
                details={"time": first.time, "stored_tail": manifest.end_time},
            )
        if first.time == manifest.end_time:
            if market_bar_checksum(first) != market_bar_checksum(last):
                raise MDCacheConflict(
                    "Conflicting tail candle", details={"time": first.time}
                )
            incoming = incoming[1:]
        if not incoming:
            if legacy:
                commit_metadata_migration(
                    store,
                    journal_path=journal_path,
                    data_path=data_path,
                    manifest_path=manifest_path,
                    old_manifest=stored_manifest,
                    new_manifest=manifest,
                    downloaded_at=store._indexed_downloaded_at(stored_manifest),
                )
            return manifest

        checksum = manifest.checksum
        for item in incoming:
            checksum = extend_tail_chain(checksum, item)
        updated = replace(
            manifest,
            schema_version="stage-d-csv-3",
            rows_count=manifest.rows_count + len(incoming),
            end_time=incoming[-1].time,
            checksum=checksum,
            checksum_algorithm=TAIL_CHAIN_CHECKSUM,
        )
        payload = csv_append_payload(store.fields, incoming)
        size_before = data_path.stat().st_size
        size_after = size_before + len(payload)
        downloaded_at = max(
            store._indexed_downloaded_at(manifest),
            max((item.downloaded_at or 0) for item in incoming),
        )
        journal = {
            "version": "segment-append-v1",
            "data_size_before": size_before,
            "data_size_after": size_after,
            "old_manifest": asdict(stored_manifest),
            "new_manifest": asdict(updated),
            "downloaded_at": downloaded_at,
        }
        store._atomic_write_text(
            journal_path, json.dumps(journal, sort_keys=True, indent=2) + "\n"
        )
        append_bytes(data_path, payload)
        store._atomic_write_text(
            manifest_path,
            json.dumps(asdict(updated), sort_keys=True, indent=2) + "\n",
        )
        store._replace_index_manifest(updated, downloaded_at=downloaded_at)
        journal_path.unlink()
        fsync_directory(journal_path.parent)
        return updated


def commit_metadata_migration(
    store: Any,
    *,
    journal_path: Path,
    data_path: Path,
    manifest_path: Path,
    old_manifest: SegmentManifest,
    new_manifest: SegmentManifest,
    downloaded_at: int,
) -> None:
    """Atomically migrate manifest/index metadata without rewriting candle bytes."""
    data_size = data_path.stat().st_size
    journal = {
        "version": "segment-append-v1",
        "data_size_before": data_size,
        "data_size_after": data_size,
        "old_manifest": asdict(old_manifest),
        "new_manifest": asdict(new_manifest),
        "downloaded_at": downloaded_at,
    }
    store._atomic_write_text(
        journal_path, json.dumps(journal, sort_keys=True, indent=2) + "\n"
    )
    store._atomic_write_text(
        manifest_path,
        json.dumps(asdict(new_manifest), sort_keys=True, indent=2) + "\n",
    )
    store._replace_index_manifest(new_manifest, downloaded_at=downloaded_at)
    journal_path.unlink()
    fsync_directory(journal_path.parent)


def validated_append_bars(
    bars: Iterable[MarketBar], *, key: dict[str, str]
) -> list[MarketBar]:
    normalized: list[MarketBar] = []
    expected = (
        key["exchange"].lower(),
        key["market"].lower(),
        key["symbol"].upper(),
        canonical_timeframe(key["timeframe"]),
        key["source_kind"],
    )
    for bar in bars:
        validate_persisted_bar_semantics(bar)
        actual = (
            bar.exchange.lower(),
            bar.market.lower(),
            bar.symbol.upper(),
            canonical_timeframe(bar.timeframe),
            bar.source_kind,
        )
        if actual != expected:
            raise MDInvalidExchangeResponse(
                "Append candle identity does not match segment", details={"time": bar.time}
            )
        validate_bars([bar.to_bar()])
        if normalized and bar.time < normalized[-1].time:
            raise MDInvalidExchangeResponse(
                "Segment append must be strictly ordered by time"
            )
        if normalized and bar.time == normalized[-1].time:
            if market_bar_checksum(bar) != market_bar_checksum(normalized[-1]):
                raise MDCacheConflict(
                    "Conflicting append candle", details={"time": bar.time}
                )
            continue
        normalized.append(bar)
    return normalized


def csv_append_payload(fields: list[str], bars: Iterable[MarketBar]) -> bytes:
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=fields)
    for bar in bars:
        writer.writerow({name: getattr(bar, name) for name in fields})
    return output.getvalue().encode("utf-8")


def read_last_csv_bar(path: Path) -> MarketBar | None:
    with path.open("rb") as handle:
        header = handle.readline().decode("utf-8")
        fieldnames = next(csv.reader([header]))
        end = handle.seek(0, os.SEEK_END)
        if end <= len(header.encode("utf-8")):
            return None
        position = end
        suffix = b""
        while position > 0 and suffix.count(b"\n") < 2:
            read_size = min(4096, position)
            position -= read_size
            handle.seek(position)
            suffix = handle.read(read_size) + suffix
            if len(suffix) > 1_048_576:
                raise MDInvalidExchangeResponse("Segment CSV tail row is too large")
    lines = [line for line in suffix.decode("utf-8").splitlines() if line]
    if not lines:
        return None
    row = next(csv.DictReader([lines[-1]], fieldnames=fieldnames))
    return row_to_bar(cast(dict[str, object], row))


def append_bytes(path: Path, payload: bytes) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_APPEND)
    try:
        offset = 0
        while offset < len(payload):
            offset += os.write(fd, payload[offset:])
        os.fsync(fd)
    finally:
        os.close(fd)


@contextmanager
def series_lock(path: Path) -> Iterator[None]:
    with path.open("a+b") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def recover_pending_appends(store: Any) -> None:
    for journal_path in store.root.glob("v1/**/.append-journal.json"):
        with series_lock(journal_path.parent / ".writer.lock"):
            recover_append_journal(store, journal_path)


def recover_append_journal(store: Any, journal_path: Path) -> None:
    try:
        journal = json.loads(journal_path.read_text(encoding="utf-8"))
        if not isinstance(journal, dict) or journal.get("version") != "segment-append-v1":
            raise ValueError("unsupported append journal version")
        old_manifest = SegmentManifest(**journal["old_manifest"])
        new_manifest = SegmentManifest(**journal["new_manifest"])
        size_before = int(journal["data_size_before"])
        size_after = int(journal["data_size_after"])
        downloaded_at = int(journal["downloaded_at"])
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise MDInvalidExchangeResponse("Invalid segment append journal") from exc
    data_path = journal_path.parent / "bars.csv"
    manifest_path = journal_path.parent / "manifest.json"
    if not data_path.exists() or data_path.stat().st_size < size_before:
        raise MDInvalidExchangeResponse("Cannot recover truncated segment append")
    current_manifest: dict[str, object] | None = None
    if manifest_path.exists():
        loaded = json.loads(manifest_path.read_text(encoding="utf-8"))
        if isinstance(loaded, dict):
            current_manifest = cast(dict[str, object], loaded)
    committed = (
        data_path.stat().st_size == size_after
        and current_manifest == asdict(new_manifest)
    )
    if committed:
        store._replace_index_manifest(new_manifest, downloaded_at=downloaded_at)
    else:
        with data_path.open("r+b") as handle:
            handle.truncate(size_before)
            handle.flush()
            os.fsync(handle.fileno())
        store._write_manifest_and_index(
            old_manifest,
            manifest_path=manifest_path,
            downloaded_at=store._indexed_downloaded_at(old_manifest),
        )
    journal_path.unlink()
    fsync_directory(journal_path.parent)


def fsync_directory(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
