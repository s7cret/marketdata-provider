from pathlib import Path

from marketdata_provider.cache import (
    bars_checksum,
    read_cache_segment,
    write_cache_segment,
)
from marketdata_provider.cli.main import main
from marketdata_provider.core.bar import RUNTIME_CONTRACT_VERSION, Bar


def _csv(path: Path) -> None:
    path.write_text(
        "time,open,high,low,close,volume\n1000,1,2,0.5,1.5,10\n61000,1.5,2,1,1.2,5\n"
    )


def test_cache_roundtrip_metadata_checksum(tmp_path: Path):
    bars = [Bar(1000, 1, 2, 0.5, 1.5, 10, 60999), Bar(61000, 1.5, 2, 1, 1.2, 5, 120999)]
    meta = write_cache_segment(
        tmp_path,
        bars,
        exchange="binance",
        market="spot",
        symbol="BTCUSDT",
        timeframe="1m",
    )
    assert meta.runtime_contract_version == RUNTIME_CONTRACT_VERSION
    assert meta.checksum == bars_checksum(bars)
    assert (
        read_cache_segment(
            tmp_path,
            exchange="binance",
            market="spot",
            symbol="BTCUSDT",
            timeframe="1m",
        )
        == bars
    )


def test_cli_fetch_export_coverage_validate_cache(tmp_path: Path, capsys):
    src = tmp_path / "bars.csv"
    out = tmp_path / "out.csv"
    cache = tmp_path / "cache"
    _csv(src)
    assert (
        main(
            [
                "fetch",
                "--path",
                str(src),
                "--symbol",
                "BINANCE:BTCUSDT",
                "--timeframe",
                "1m",
                "--cache-dir",
                str(cache),
            ]
        )
        == 0
    )
    assert '"ok": true' in capsys.readouterr().out
    assert (
        main(
            [
                "coverage",
                "--cache",
                "--symbol",
                "BINANCE:BTCUSDT",
                "--timeframe",
                "1m",
                "--cache-dir",
                str(cache),
            ]
        )
        == 0
    )
    assert '"gaps": 0' in capsys.readouterr().out
    assert (
        main(
            [
                "validate",
                "--cache",
                "--symbol",
                "BINANCE:BTCUSDT",
                "--timeframe",
                "1m",
                "--cache-dir",
                str(cache),
            ]
        )
        == 0
    )
    assert (
        main(
            [
                "export",
                "--cache",
                "--symbol",
                "BINANCE:BTCUSDT",
                "--timeframe",
                "1m",
                "--cache-dir",
                str(cache),
                "--output",
                str(out),
            ]
        )
        == 0
    )
    assert out.exists() and "time,open,high" in out.read_text()


def test_cli_live_requires_env(capsys):
    assert (
        main(["coverage", "--live", "--symbol", "BINANCE:BTCUSDT", "--timeframe", "1m"])
        == 2
    )
    assert "MD_NETWORK_UNAVAILABLE" in capsys.readouterr().out
