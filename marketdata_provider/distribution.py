from __future__ import annotations

import argparse
import hashlib
import json
import zipfile
from dataclasses import asdict, dataclass
from pathlib import Path

EXCLUDE_PARTS = {
    ".git",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    "__pycache__",
    "build",
    "dist",
    ".release_gate_reports",
    "htmlcov",
}
EXCLUDE_SUFFIXES = {".pyc", ".pyo", ".coverage", ".log"}


@dataclass(frozen=True)
class DistributionManifest:
    file_count: int
    sha256: str
    forbidden_count: int
    forbidden: list[str]


def _should_include(relative_path: Path) -> bool:
    if any(
        part in EXCLUDE_PARTS or part.endswith(".egg-info")
        for part in relative_path.parts
    ):
        return False
    if relative_path.name in {".coverage"} or relative_path.suffix in EXCLUDE_SUFFIXES:
        return False
    return True


def iter_files(root: str | Path) -> list[Path]:
    base = Path(root)
    return sorted(
        p
        for p in base.rglob("*")
        if p.is_file() and _should_include(p.relative_to(base))
    )


def distribution_manifest(root: str | Path) -> DistributionManifest:
    base = Path(root)
    files = iter_files(base)
    digest = hashlib.sha256()
    forbidden: list[str] = []
    for path in files:
        rel = path.relative_to(base).as_posix()
        if "__pycache__" in rel or rel.endswith(".pyc") or rel.startswith(".git/"):
            forbidden.append(rel)
        digest.update(rel.encode())
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return DistributionManifest(
        len(files), digest.hexdigest(), len(forbidden), forbidden
    )


def build_zip(
    root: str | Path, output: str | Path, *, archive_root: str | None = None
) -> Path:
    base = Path(root)
    out = Path(output)
    archive_root = archive_root or base.name
    files = iter_files(base)
    out.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for path in files:
            rel = path.relative_to(base).as_posix()
            info = zipfile.ZipInfo(f"{archive_root}/{rel}")
            info.date_time = (2026, 1, 1, 0, 0, 0)
            info.external_attr = 0o644 << 16
            zf.writestr(info, path.read_bytes(), compress_type=zipfile.ZIP_DEFLATED)
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="distribution")
    sub = parser.add_subparsers(dest="command", required=True)
    man = sub.add_parser("manifest")
    man.add_argument("--root", default=".")
    bz = sub.add_parser("build-zip")
    bz.add_argument("--root", default=".")
    bz.add_argument("--output", required=True)
    bz.add_argument("--archive-root")
    args = parser.parse_args(argv)
    if args.command == "manifest":
        report = distribution_manifest(args.root)
        print(json.dumps(asdict(report), indent=2, sort_keys=True))
        return 0 if report.forbidden_count == 0 else 1
    out = build_zip(args.root, args.output, archive_root=args.archive_root)
    print(str(out))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
