from __future__ import annotations

import argparse
import json
import tomllib
from dataclasses import asdict, dataclass
from pathlib import Path

from marketdata_provider.distribution import distribution_manifest
from marketdata_provider.quality import architecture_report, duplicate_report

EXPECTED_VERSION = "4.0.0"


@dataclass(frozen=True)
class ReleaseReport:
    ok: bool
    project: str
    package_version: str
    docs_ok: bool
    distribution_ok: bool
    duplicates_ok: bool
    architecture_ok: bool
    notes: list[str]


def release_report(root: str | Path = ".") -> ReleaseReport:
    base = Path(root)
    project = tomllib.loads((base / "pyproject.toml").read_text())["project"]
    version = str(project["version"])
    notes: list[str] = []
    if version != EXPECTED_VERSION:
        notes.append(f"expected version {EXPECTED_VERSION}, got {version}")
    required_docs = [
        "README.md",
        "CHANGELOG.md",
        "docs/README.md",
        "docs/ARCHITECTURE.md",
        "docs/DEVELOPMENT.md",
        "docs/EXCHANGES.md",
        "docs/RELEASE_4_0.md",
    ]
    missing_docs = [name for name in required_docs if not (base / name).exists()]
    if missing_docs:
        notes.append("missing docs: " + ", ".join(missing_docs))
    dist = distribution_manifest(base)
    dup = duplicate_report(base / "marketdata_provider")
    arch = architecture_report(base / "marketdata_provider", max_lines=700)
    docs_ok = not missing_docs
    distribution_ok = dist.forbidden_count == 0
    duplicates_ok = dup.duplicate_group_count == 0
    architecture_ok = arch.oversized_count == 0
    ok = (
        version == EXPECTED_VERSION
        and docs_ok
        and distribution_ok
        and duplicates_ok
        and architecture_ok
    )
    return ReleaseReport(
        ok,
        str(project["name"]),
        version,
        docs_ok,
        distribution_ok,
        duplicates_ok,
        architecture_ok,
        notes,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="release")
    parser.add_argument("--root", default=".")
    parser.add_argument("--json", dest="json_path")
    args = parser.parse_args(argv)
    report = release_report(args.root)
    payload = json.dumps(asdict(report), indent=2, sort_keys=True)
    if args.json_path:
        Path(args.json_path).write_text(payload + "\n")
    print(payload)
    return 0 if report.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
