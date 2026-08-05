from pathlib import Path

from marketdata_provider.cli.main import main
from marketdata_provider.providers import OfflineDataProvider


def test_offline_csv_provider(tmp_path: Path):
    p = tmp_path / "bars.csv"
    p.write_text(
        "time,open,high,low,close,volume\n1000,1,2,0.5,1.5,10\n61000,1.5,2,1,1.2,5\n"
    )
    bars = OfflineDataProvider(p).get_bars("BTCUSDT", "1m", None, None)
    assert len(bars) == 2 and bars[0].time_close == 60999


def test_cli_validate(tmp_path: Path, capsys):
    p = tmp_path / "bars.csv"
    p.write_text("time,open,high,low,close,volume\n1000,1,2,0.5,1.5,10\n")
    assert main(["validate", "--path", str(p), "--timeframe", "1m"]) == 0
    assert '"ok": true' in capsys.readouterr().out


def test_cli_fetch_fails_explicitly(capsys):
    assert main(["fetch", "--timeframe", "1m"]) == 2
    assert "MD_UNSUPPORTED_FEATURE" in capsys.readouterr().out
