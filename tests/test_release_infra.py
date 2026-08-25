from __future__ import annotations

import json
import zipfile
from pathlib import Path

from marketdata_provider import __version__
from marketdata_provider._pathing import safe_path_part
from marketdata_provider.diagnostics import DiagnosticCollector
from marketdata_provider.distribution import build_zip, distribution_manifest
from marketdata_provider.pagination import PageRequest, next_cursor
from marketdata_provider.quality import (
    architecture_report,
    duplicate_report,
)
from marketdata_provider.quality import (
    main as quality_main,
)
from marketdata_provider.release import main as release_main
from marketdata_provider.release import release_report
from marketdata_provider.transport.async_client import RetryConfig


def test_release_manifest_and_distribution_are_green(tmp_path: Path, capsys) -> None:
    assert __version__ == "5.0.0rc5"
    report = release_report(Path.cwd())
    assert report.ok, report
    assert distribution_manifest(Path.cwd()).forbidden_count == 0
    assert duplicate_report("marketdata_provider").duplicate_group_count == 0
    assert (
        architecture_report("marketdata_provider", max_lines=900).oversized_count == 0
    )

    out = tmp_path / "release.json"
    assert release_main(["--root", ".", "--json", str(out)]) == 0
    assert json.loads(out.read_text())["ok"] is True

    assert quality_main(["duplicates", "marketdata_provider"]) == 0
    assert "duplicate_group_count" in capsys.readouterr().out


def test_distribution_zip_builder_and_hygiene(tmp_path: Path) -> None:
    output = tmp_path / "marketdata-provider-5.0.0rc5.zip"
    build_zip(Path.cwd(), output, archive_root="marketdata-provider-5.0.0rc5")
    with zipfile.ZipFile(output) as zf:
        names = zf.namelist()
    assert "marketdata-provider-5.0.0rc5/pyproject.toml" in names
    assert not any("__pycache__" in name or name.endswith(".pyc") for name in names)


def test_small_uncovered_support_modules() -> None:
    assert safe_path_part("binance:BTC/USDT") == "BINANCE_BTC_USDT"
    collector = DiagnosticCollector()
    collector.add("MD_TEST", "hello")
    collector.add("MD_WARN", "careful", "warning", item=1)
    assert [d.code for d in collector.warnings_or_errors()] == ["MD_WARN"]

    req = PageRequest(start=0, end=60_000, limit=100)
    assert req.limit == 100
    assert next_cursor(0, "1m", 0) == 60_000

    cfg = RetryConfig(max_retries=1, base_sec=0.0, max_sec=0.0)
    assert cfg.backoff(3) == 0.0
