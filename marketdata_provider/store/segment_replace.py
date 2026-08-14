from __future__ import annotations

import json
import os
import shutil
import tempfile
from dataclasses import asdict
from pathlib import Path
from typing import Any, cast

from marketdata_provider.errors import MDInvalidExchangeResponse
from marketdata_provider.store.segment_append import fsync_directory, series_lock
from marketdata_provider.store.segment_integrity import publish_integrity_generation
from marketdata_provider.store.segment_manifest import SegmentManifest

JOURNAL_NAME = ".replace-journal.json"
BACKUP_NAME = ".replace-backup"


def begin_replacement(
    store: Any,
    *,
    old_manifest: SegmentManifest | None,
    new_manifest: SegmentManifest,
    old_data_path: Path | None,
    new_data_path: Path,
    downloaded_at: int,
) -> Path:
    """Persist an O(1) rollback point before replacing a segment generation."""

    directory = new_data_path.parent
    directory.mkdir(parents=True, exist_ok=True)
    journal_path = directory / JOURNAL_NAME
    _validate_manifest_identity(old_manifest, new_manifest)
    _validate_journal_path(store, journal_path, new_manifest=new_manifest)
    _reject_symlinked_members(
        directory,
        "manifest.json",
        "bars.csv",
        "bars.parquet",
        None if old_data_path is None else old_data_path.name,
        new_data_path.name,
        (None if old_data_path is None else f"{BACKUP_NAME}{old_data_path.suffix}"),
    )
    if journal_path.exists():
        recover_replacement_journal(store, journal_path)

    backup_path: Path | None = None
    if old_data_path is not None and old_data_path.exists():
        backup_path = directory / f"{BACKUP_NAME}{old_data_path.suffix}"
        backup_path.unlink(missing_ok=True)
        try:
            os.link(old_data_path, backup_path)
        except OSError:
            _atomic_copy(old_data_path, backup_path)
        fsync_directory(directory)

    journal = {
        "version": "segment-replace-v1",
        "old_manifest": None if old_manifest is None else asdict(old_manifest),
        "new_manifest": asdict(new_manifest),
        "old_data_name": None if old_data_path is None else old_data_path.name,
        "new_data_name": new_data_path.name,
        "backup_name": None if backup_path is None else backup_path.name,
        "old_downloaded_at": (
            0 if old_manifest is None else store._indexed_downloaded_at(old_manifest)
        ),
        "new_downloaded_at": downloaded_at,
    }
    store._atomic_write_text(
        journal_path, json.dumps(journal, sort_keys=True, indent=2) + "\n"
    )
    return journal_path


def finish_replacement(store: Any, journal_path: Path) -> None:
    """Remove the rollback generation after data, manifest and index commit."""

    _, _, new_manifest, old_name, new_name, backup_name = _validated_journal(
        store, journal_path
    )
    if old_name is not None and old_name != new_name:
        (journal_path.parent / old_name).unlink(missing_ok=True)
    new_path = journal_path.parent / new_name
    if new_path.suffix == ".csv":
        publish_integrity_generation(store, new_path, new_manifest)
    other_suffix = ".csv" if new_path.suffix == ".parquet" else ".parquet"
    new_path.with_suffix(other_suffix).unlink(missing_ok=True)
    if backup_name is not None:
        (journal_path.parent / backup_name).unlink(missing_ok=True)
    journal_path.unlink(missing_ok=True)
    fsync_directory(journal_path.parent)


def recover_pending_replacements(store: Any) -> None:
    for journal_path in store.root.glob(f"v1/**/{JOURNAL_NAME}"):
        with series_lock(journal_path.parent / ".writer.lock"):
            recover_replacement_journal(store, journal_path)


def recover_replacement_journal(store: Any, journal_path: Path) -> None:
    """Roll forward a committed manifest or roll back to the linked generation."""

    journal, old_manifest, new_manifest, old_name, new_name, backup_name = (
        _validated_journal(store, journal_path)
    )
    try:
        new_downloaded_at = int(cast(Any, journal["new_downloaded_at"]))
        old_downloaded_at = int(cast(Any, journal.get("old_downloaded_at", 0)))
    except (KeyError, TypeError, ValueError) as exc:
        raise MDInvalidExchangeResponse("Invalid segment replacement journal") from exc

    directory = journal_path.parent
    manifest_path = directory / "manifest.json"
    new_data_path = directory / new_name
    current_manifest: dict[str, object] | None = None
    if manifest_path.exists():
        try:
            loaded = json.loads(manifest_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            loaded = None
        if isinstance(loaded, dict):
            current_manifest = cast(dict[str, object], loaded)

    committed = current_manifest == asdict(new_manifest) and new_data_path.exists()
    if committed:
        store._replace_index_manifest(new_manifest, downloaded_at=new_downloaded_at)
        finish_replacement(store, journal_path)
        return

    old_data_path = directory / old_name if old_name is not None else None
    backup_path = directory / backup_name if backup_name is not None else None

    if old_manifest is None:
        new_data_path.unlink(missing_ok=True)
        manifest_path.unlink(missing_ok=True)
        with store._connect_index() as db:
            store._delete_index_rows_for_series(db, new_manifest)
    else:
        if (
            backup_path is not None
            and backup_path.exists()
            and old_data_path is not None
        ):
            os.replace(backup_path, old_data_path)
            fsync_directory(directory)
        if old_data_path is None or not old_data_path.exists():
            raise MDInvalidExchangeResponse(
                "Cannot recover segment replacement without old data generation"
            )
        if new_data_path != old_data_path:
            new_data_path.unlink(missing_ok=True)
        store._write_manifest_and_index(
            old_manifest,
            manifest_path=manifest_path,
            downloaded_at=old_downloaded_at,
        )

    if backup_path is not None:
        backup_path.unlink(missing_ok=True)
    journal_path.unlink(missing_ok=True)
    fsync_directory(directory)


def _load_journal(path: Path) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MDInvalidExchangeResponse("Invalid segment replacement journal") from exc
    if not isinstance(payload, dict) or payload.get("version") != "segment-replace-v1":
        raise MDInvalidExchangeResponse("Invalid segment replacement journal")
    return cast(dict[str, object], payload)


def _validated_journal(store: Any, journal_path: Path) -> tuple[
    dict[str, object],
    SegmentManifest | None,
    SegmentManifest,
    str | None,
    str,
    str | None,
]:
    journal = _load_journal(journal_path)
    try:
        old_raw = journal.get("old_manifest")
        old_manifest = (
            None if old_raw is None else SegmentManifest(**cast(Any, old_raw))
        )
        new_manifest = SegmentManifest(**cast(Any, journal["new_manifest"]))
    except (KeyError, TypeError, ValueError) as exc:
        raise MDInvalidExchangeResponse("Invalid segment replacement journal") from exc

    _validate_manifest_identity(old_manifest, new_manifest)
    _validate_journal_path(store, journal_path, new_manifest=new_manifest)
    old_name = _journal_member_name(journal, "old_data_name", optional=True)
    new_name = _journal_member_name(journal, "new_data_name", optional=False)
    backup_name = _journal_member_name(journal, "backup_name", optional=True)
    assert new_name is not None

    expected_new_name = f"bars.{new_manifest.data_format}"
    expected_old_name = (
        None if old_manifest is None else f"bars.{old_manifest.data_format}"
    )
    if new_name != expected_new_name:
        raise MDInvalidExchangeResponse(
            "Invalid segment replacement journal path: new_data_name"
        )
    if old_name != expected_old_name:
        raise MDInvalidExchangeResponse(
            "Invalid segment replacement journal path: old_data_name"
        )
    if backup_name is not None:
        expected_backup_name = (
            None
            if old_manifest is None
            else f"{BACKUP_NAME}.{old_manifest.data_format}"
        )
        if backup_name != expected_backup_name:
            raise MDInvalidExchangeResponse(
                "Invalid segment replacement journal path: backup_name"
            )
    _reject_symlinked_members(
        journal_path.parent,
        "manifest.json",
        "bars.csv",
        "bars.parquet",
        old_name,
        new_name,
        backup_name,
    )
    return (
        journal,
        old_manifest,
        new_manifest,
        old_name,
        new_name,
        backup_name,
    )


def _validate_journal_path(
    store: Any,
    journal_path: Path,
    *,
    new_manifest: SegmentManifest | None = None,
) -> None:
    if journal_path.name != JOURNAL_NAME:
        raise MDInvalidExchangeResponse("Invalid segment replacement journal path")
    if journal_path.is_symlink():
        raise MDInvalidExchangeResponse("Invalid segment replacement journal path")
    if new_manifest is None:
        journal = _load_journal(journal_path)
        try:
            new_manifest = SegmentManifest(**cast(Any, journal["new_manifest"]))
        except (KeyError, TypeError, ValueError) as exc:
            raise MDInvalidExchangeResponse(
                "Invalid segment replacement journal"
            ) from exc
    lexical_root = Path(os.path.abspath(store.root))
    lexical_directory = Path(os.path.abspath(journal_path.parent))
    try:
        relative_directory = lexical_directory.relative_to(lexical_root)
    except ValueError as exc:
        raise MDInvalidExchangeResponse(
            "Invalid segment replacement journal path"
        ) from exc
    component = lexical_root
    for part in relative_directory.parts:
        component /= part
        if component.is_symlink():
            raise MDInvalidExchangeResponse(
                "Invalid segment replacement journal path: symlink"
            )

    directory = lexical_directory.resolve()
    expected = store._dir(
        exchange=new_manifest.exchange,
        market=new_manifest.market,
        symbol=new_manifest.symbol,
        timeframe=new_manifest.timeframe,
        source_kind=new_manifest.source_kind,
    ).resolve()
    if directory != expected:
        raise MDInvalidExchangeResponse("Invalid segment replacement journal path")


def _validate_manifest_identity(
    old_manifest: SegmentManifest | None, new_manifest: SegmentManifest
) -> None:
    if old_manifest is None:
        return
    fields = ("exchange", "market", "symbol", "timeframe", "source_kind")
    if any(
        getattr(old_manifest, field) != getattr(new_manifest, field) for field in fields
    ):
        raise MDInvalidExchangeResponse(
            "Invalid segment replacement journal manifest identity"
        )


def _reject_symlinked_members(directory: Path, *names: str | None) -> None:
    for name in names:
        if name is not None and (directory / name).is_symlink():
            raise MDInvalidExchangeResponse(
                "Invalid segment replacement journal path: symlink"
            )


def _journal_member_name(
    journal: dict[str, object],
    field: str,
    *,
    optional: bool,
) -> str | None:
    value = journal.get(field)
    if value is None and optional:
        return None
    if not isinstance(value, str) or Path(value).name != value:
        raise MDInvalidExchangeResponse(
            f"Invalid segment replacement journal path: {field}"
        )
    allowed = (
        {f"{BACKUP_NAME}.csv", f"{BACKUP_NAME}.parquet"}
        if field == "backup_name"
        else {"bars.csv", "bars.parquet"}
    )
    if value not in allowed:
        raise MDInvalidExchangeResponse(
            f"Invalid segment replacement journal path: {field}"
        )
    return value


def _atomic_copy(source: Path, destination: Path) -> None:
    fd, tmp_name = tempfile.mkstemp(
        prefix=f".{destination.name}.", dir=destination.parent
    )
    try:
        with source.open("rb") as src, os.fdopen(fd, "wb") as dst:
            shutil.copyfileobj(src, dst, length=1024 * 1024)
            dst.flush()
            os.fsync(dst.fileno())
        os.replace(tmp_name, destination)
    finally:
        if os.path.exists(tmp_name):
            os.unlink(tmp_name)
