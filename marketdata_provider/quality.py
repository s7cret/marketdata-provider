from __future__ import annotations

import argparse
import ast
import hashlib
import json
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(frozen=True)
class DuplicateReport:
    duplicate_group_count: int
    groups: list[list[str]]


@dataclass(frozen=True)
class ArchitectureReport:
    max_lines: int
    oversized_count: int
    oversized: list[dict[str, int | str]]


def _python_files(root: Path) -> list[Path]:
    ignored = {
        ".git",
        ".mypy_cache",
        ".pytest_cache",
        ".ruff_cache",
        "__pycache__",
        "build",
        "dist",
    }
    return sorted(
        p
        for p in root.rglob("*.py")
        if not any(part in ignored or part.endswith(".egg-info") for part in p.parts)
    )


def duplicate_report(root: str | Path) -> DuplicateReport:
    base = Path(root)
    groups: dict[str, list[str]] = {}
    for path in _python_files(base):
        try:
            tree = ast.parse(path.read_text())
        except SyntaxError:
            continue
        for node in tree.body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                digest = hashlib.sha256(ast.unparse(node).encode()).hexdigest()
                groups.setdefault(digest, []).append(
                    f"{path.relative_to(base)}:{node.name}"
                )
    duplicates = [items for items in groups.values() if len(items) > 1]
    return DuplicateReport(len(duplicates), duplicates)


def architecture_report(
    root: str | Path, *, max_lines: int = 700
) -> ArchitectureReport:
    base = Path(root)
    oversized: list[dict[str, int | str]] = []
    for path in _python_files(base):
        line_count = sum(1 for _ in path.open())
        if line_count > max_lines:
            oversized.append({"path": str(path.relative_to(base)), "lines": line_count})
    return ArchitectureReport(
        max_lines=max_lines, oversized_count=len(oversized), oversized=oversized
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="quality")
    sub = parser.add_subparsers(dest="command", required=True)
    dup = sub.add_parser("duplicates")
    dup.add_argument("root", nargs="?", default=".")
    arch = sub.add_parser("architecture")
    arch.add_argument("root", nargs="?", default=".")
    arch.add_argument("--max-lines", type=int, default=700)
    args = parser.parse_args(argv)
    if args.command == "duplicates":
        duplicate = duplicate_report(args.root)
        print(json.dumps(asdict(duplicate), indent=2, sort_keys=True))
        return 0 if duplicate.duplicate_group_count == 0 else 1
    architecture = architecture_report(args.root, max_lines=args.max_lines)
    print(json.dumps(asdict(architecture), indent=2, sort_keys=True))
    return 0 if architecture.oversized_count == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
