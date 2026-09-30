from __future__ import annotations

import sqlite3
import zipfile
from pathlib import Path

import pytest

from marketdata_provider import distribution


def test_distribution_omits_entire_runtime_cache_tree(tmp_path: Path) -> None:
    root = tmp_path / "source"
    root.mkdir()
    (root / "package.py").write_text("value = 1\n", encoding="utf-8")
    baseline = distribution.distribution_manifest(root)
    for relative in (
        ".marketdata-cache/current.sqlite",
        ".marketdata-cache/current.sqlite-wal",
        ".marketdata-cache/current.sqlite-shm",
        ".marketdata-cache/nested/metadata.json",
        ".marketdata-cache/dist/runtime.zip",
    ):
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"runtime-only")

    assert distribution.iter_files(root) == [root / "package.py"]
    assert distribution.distribution_manifest(root) == baseline
    archive = distribution.build_zip(
        root, tmp_path / "source.zip", archive_root="source"
    )
    with zipfile.ZipFile(archive) as built:
        assert built.namelist() == ["source/package.py"]
        assert built.read("source/package.py") == b"value = 1\n"


@pytest.mark.parametrize("operation", ["manifest", "zip"])
def test_distribution_ignores_sqlite_sidecar_removed_after_enumeration(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, operation: str
) -> None:
    root = tmp_path / "source"
    cache = root / ".marketdata-cache"
    cache.mkdir(parents=True)
    (root / "package.py").write_bytes(b"source")
    connection = sqlite3.connect(cache / "current.sqlite")
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("CREATE TABLE cache (value INTEGER)")
    connection.commit()
    sidecar = cache / "current.sqlite-shm"
    assert sidecar.is_file()
    original = distribution.iter_files

    def enumerate_then_close(base: str | Path) -> list[Path]:
        files = original(base)
        connection.close()
        assert not sidecar.exists()
        return files

    monkeypatch.setattr(distribution, "iter_files", enumerate_then_close)
    try:
        if operation == "manifest":
            assert distribution.distribution_manifest(root).file_count == 1
        else:
            archive = distribution.build_zip(root, tmp_path / "source.zip")
            with zipfile.ZipFile(archive) as built:
                assert built.namelist() == ["source/package.py"]
    finally:
        connection.close()


@pytest.mark.parametrize("operation", ["manifest", "zip"])
def test_distribution_fails_closed_when_source_disappears(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, operation: str
) -> None:
    root = tmp_path / "source"
    root.mkdir()
    source = root / "package.py"
    source.write_bytes(b"source")
    original = distribution.iter_files

    def enumerate_then_remove(base: str | Path) -> list[Path]:
        files = original(base)
        source.unlink()
        return files

    monkeypatch.setattr(distribution, "iter_files", enumerate_then_remove)
    with pytest.raises(FileNotFoundError):
        if operation == "manifest":
            distribution.distribution_manifest(root)
        else:
            distribution.build_zip(root, tmp_path / "source.zip")
