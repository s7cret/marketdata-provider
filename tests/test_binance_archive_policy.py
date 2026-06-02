from datetime import datetime, timezone
from zipfile import ZipFile

from marketdata_provider.exchanges.binance.archive import fetch_binance_archive_bars


def test_wide_monthly_archive_does_not_expand_to_daily_fallback(tmp_path, monkeypatch):
    start = int(datetime(2020, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
    end = int(datetime(2021, 2, 1, tzinfo=timezone.utc).timestamp() * 1000)
    monthly_root = tmp_path / "archives" / "binance_klines" / "spot" / "monthly" / "BTCUSDT" / "1m"
    monthly_root.mkdir(parents=True)
    for month in range(1, 13):
        path = monthly_root / f"BTCUSDT-1m-2020-{month:02d}.zip"
        with ZipFile(path, "w") as zf:
            zf.writestr(f"BTCUSDT-1m-2020-{month:02d}.csv", f"{start},1,1,1,1,1,{start + 59999}\n")
    path = monthly_root / "BTCUSDT-1m-2021-01.zip"
    with ZipFile(path, "w") as zf:
        zf.writestr("BTCUSDT-1m-2021-01.csv", f"{start},1,1,1,1,1,{start + 59999}\n")

    calls = []

    def fail_daily_download(*args, **kwargs):
        calls.append(args)
        raise AssertionError("wide monthly reads must not fan out to daily downloads")

    monkeypatch.setattr("marketdata_provider.exchanges.binance.archive.urlopen", fail_daily_download)

    bars = fetch_binance_archive_bars(
        symbol="BTCUSDT",
        market="spot",
        timeframe="1m",
        start=start,
        end=end,
        cache_dir=tmp_path,
    )

    assert [bar.time for bar in bars] == [start]
    assert calls == []
