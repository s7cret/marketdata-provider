from __future__ import annotations

import subprocess
import sys
import textwrap
from pathlib import Path

from marketdata_provider.distribution import distribution_manifest, iter_files
from marketdata_provider.release import release_report

REQUIRED_RELEASE_DOCS = (
    "README.md",
    "CHANGELOG.md",
    "docs/README.md",
    "docs/ARCHITECTURE.md",
    "docs/DEVELOPMENT.md",
    "docs/EXCHANGES.md",
    "docs/RELEASE_4_0.md",
)


def _write_release_fixture(root: Path) -> None:
    (root / "pyproject.toml").write_text(
        '[project]\nname = "marketdata-provider"\nversion = "4.0.1"\n',
        encoding="utf-8",
    )
    (root / "marketdata_provider").mkdir()
    for relative_path in REQUIRED_RELEASE_DOCS:
        path = root / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("release fixture\n", encoding="utf-8")


def test_distribution_rejects_symlinked_files_outside_root(tmp_path: Path) -> None:
    _write_release_fixture(tmp_path)
    outside = tmp_path.parent / "secret-marketdata.txt"
    outside.write_text("do not archive\n", encoding="utf-8")
    linked = tmp_path / "linked-secret.txt"
    linked.symlink_to(outside)

    manifest = distribution_manifest(tmp_path)

    assert linked.is_file()
    assert "linked-secret.txt" not in {
        path.relative_to(tmp_path).as_posix() for path in iter_files(tmp_path)
    }
    assert manifest.forbidden_count == 0


def test_release_import_falls_back_to_tomli_without_stdlib_tomllib() -> None:
    script = textwrap.dedent("""
        import builtins
        import importlib
        import types

        fallback = types.ModuleType("tomli")
        fallback.loads = lambda value: {"project": {}}
        real_import = builtins.__import__

        def simulated_python_310_import(name, *args, **kwargs):
            if name == "tomllib":
                raise ModuleNotFoundError("No module named 'tomllib'", name="tomllib")
            if name == "tomli":
                return fallback
            return real_import(name, *args, **kwargs)

        builtins.__import__ = simulated_python_310_import
        release = importlib.import_module("marketdata_provider.release")
        assert release.tomllib is fallback
        """)

    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=Path.cwd(),
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr


def test_python_310_declares_tomli_runtime_dependency() -> None:
    project_config = Path("pyproject.toml").read_text(encoding="utf-8")
    project_section = project_config.split("[project]", 1)[1].split(
        "[project.urls]", 1
    )[0]

    assert "tomli>=2; python_version < '3.11'" in project_section


def test_release_report_fails_when_dist_contains_build_artifact(tmp_path: Path) -> None:
    _write_release_fixture(tmp_path)
    artifact = tmp_path / "dist" / "artifact.whl"
    artifact.parent.mkdir()
    artifact.write_bytes(b"wheel")

    report = release_report(tmp_path)

    assert report.distribution_ok is False
    assert report.ok is False


def test_forbidden_artifact_scan_is_deterministic_and_ignores_metadata(
    tmp_path: Path,
) -> None:
    (tmp_path / "package.py").write_text("value = 1\n", encoding="utf-8")
    for relative_path in (".git/index", ".pytest_cache/v/cache/nodeids"):
        path = tmp_path / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("metadata", encoding="utf-8")
    (tmp_path / "build").mkdir()
    (tmp_path / "build" / "package.tar.gz").write_bytes(b"sdist")
    (tmp_path / "marketdata-provider.zip").write_bytes(b"archive")

    manifest = distribution_manifest(tmp_path)

    assert manifest.file_count == 1
    assert manifest.forbidden == ["build/package.tar.gz", "marketdata-provider.zip"]


def test_distribution_excludes_local_environments_and_backup_trees(
    tmp_path: Path,
) -> None:
    (tmp_path / "package.py").write_text("value = 1\n", encoding="utf-8")
    for relative_path in (
        ".venv/lib/site-packages/dependency.py",
        "venv/lib/site-packages/dependency.py",
        ".tox/py/lib/package.py",
        ".nox/tests/lib/package.py",
        ".backup-stage-a/marketdata_provider/service.py",
    ):
        path = tmp_path / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("local-only\n", encoding="utf-8")

    files = [path.relative_to(tmp_path).as_posix() for path in iter_files(tmp_path)]
    manifest = distribution_manifest(tmp_path)

    assert files == ["package.py"]
    assert manifest.file_count == 1
    assert manifest.forbidden_count == 0
