import csv
from datetime import UTC, datetime

import pytest

from marketdata_provider.errors import MDValidationError
from marketdata_provider.providers.offline import OfflineDataProvider


def row(time=0, **extra):
    return {
        "time": time,
        "open": 10,
        "high": 12,
        "low": 9,
        "close": 11,
        "volume": 0,
        **extra,
    }


def write(path, rows):
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    return path


@pytest.mark.parametrize("bad", [None, "", True, "1.5", 1.5, -1, "1e3", "NaN"])
def test_timestamp_never_defaulted_or_truncated(tmp_path, bad):
    provider = OfflineDataProvider(tmp_path / "a.csv")
    with pytest.raises(MDValidationError, match="time"):
        provider._bar_from_row(row(time=bad), "1m")


def test_missing_timestamp_and_conflicting_alias_are_not_epoch_zero(tmp_path):
    provider = OfflineDataProvider(tmp_path / "a.csv")
    value = row()
    del value["time"]
    with pytest.raises(MDValidationError, match="missing"):
        provider._bar_from_row(value, "1m")
    with pytest.raises(MDValidationError, match="disagree"):
        provider._bar_from_row(row(timestamp=60000), "1m")
    assert provider._bar_from_row(row(timestamp=0), "1m").time == 0


@pytest.mark.parametrize("field", ["open", "high", "low", "close", "volume"])
@pytest.mark.parametrize(
    "bad", ["", None, True, "NaN", "Infinity", "-Infinity", "1e999", "1e-999"]
)
def test_numeric_validation_before_float_cast(tmp_path, field, bad):
    with pytest.raises(MDValidationError):
        OfflineDataProvider(tmp_path / "a.csv")._bar_from_row(row(**{field: bad}), "1m")


def test_volume_fill_requires_explicit_policy(tmp_path):
    path = write(tmp_path / "a.csv", [row(volume="")])
    with pytest.raises(MDValidationError, match="volume"):
        OfflineDataProvider(path).get_bars("S", "1m", None, None)
    assert (
        OfflineDataProvider(path, missing_volume="zero")
        .get_bars("S", "1m", None, None)[0]
        .volume
        == 0
    )


@pytest.mark.parametrize(
    "changes",
    [
        {"low": 11},
        {"high": 10},
        {"volume": -1},
        {"time_close": 0},
        {"time_close": 60000},
    ],
)
def test_bad_bar_not_repaired(tmp_path, changes):
    with pytest.raises(MDValidationError):
        OfflineDataProvider(tmp_path / "a.csv")._bar_from_row(row(**changes), "1m")


@pytest.mark.parametrize(
    "unit,value,expected",
    [
        ("ms", "60000", 60000),
        ("s", "60", 60000),
        ("iso8601", "1970-01-01T01:01:00+01:00", 60000),
    ],
)
def test_timestamp_units_are_explicit(tmp_path, unit, value, expected):
    assert (
        OfflineDataProvider(tmp_path / "a.csv", timestamp_unit=unit)
        ._bar_from_row(row(time=value), "1m")
        .time
        == expected
    )


@pytest.mark.parametrize(
    "value", ["1970-01-01T00:01:00", "1970-01-01T00:01:00.000001Z", "garbage"]
)
def test_iso_rejects_naive_and_submillisecond(tmp_path, value):
    with pytest.raises(MDValidationError):
        OfflineDataProvider(tmp_path / "a.csv", timestamp_unit="iso8601")._bar_from_row(
            row(time=value), "1m"
        )


@pytest.mark.parametrize("times", [[60000, 0], [0, 0]])
def test_discarded_rows_still_validate_order(tmp_path, times):
    path = write(tmp_path / "a.csv", [row(t) for t in times])
    with pytest.raises(MDValidationError, match="strictly"):
        OfflineDataProvider(path).get_bars("S", "1m", 0, 1, max_bars=1)


@pytest.mark.parametrize(
    "text",
    [
        "time,time,open,high,low,close,volume\n0,0,10,12,9,11,1\n",
        "time,open,high,low,close,volume\n0,10,12,9,11\n",
        "time,open,high,low,close,volume\n0,10,12,9,11,1,extra\n",
    ],
)
def test_bad_csv_schema_reports_location(tmp_path, text):
    path = tmp_path / "a.csv"
    path.write_text(text)
    with pytest.raises(MDValidationError) as err:
        OfflineDataProvider(path).get_bars("S", "1m", None, None)
    assert err.value.details["path"] == str(path)
    assert "row" in err.value.details


def test_bound_identity_and_row_labels(tmp_path):
    path = write(tmp_path / "a.csv", [row(symbol="S", timeframe="1h")])
    p = OfflineDataProvider(path, symbol="S", timeframe="60")
    assert p.get_bars("S", "60m", None, None)[0].time_close == 3599999
    for symbol, tf in [("OTHER", "60"), ("S", "1m")]:
        with pytest.raises(MDValidationError):
            p.get_bars(symbol, tf, None, None)
    with pytest.raises(MDValidationError):
        OfflineDataProvider(path).get_bars("OTHER", "60", None, None)


@pytest.mark.parametrize(
    "name,value",
    [
        ("max_bars", -1),
        ("max_bars", True),
        ("max_bars", 1.1),
        ("start", -1),
        ("end", False),
    ],
)
def test_bad_query_rejected(tmp_path, name, value):
    opts = {"start": None, "end": None, "max_bars": None}
    opts[name] = value
    with pytest.raises(MDValidationError):
        OfflineDataProvider(tmp_path / "missing.csv").get_bars("S", "1m", **opts)


def test_csv_and_streaming_parquet_match_and_never_read_table(tmp_path, monkeypatch):
    import pyarrow as pa
    import pyarrow.parquet as pq

    records = [row(i * 60000) for i in range(100)]
    csvpath = write(tmp_path / "a.csv", records)
    pqpath = tmp_path / "a.parquet"
    pq.write_table(pa.Table.from_pylist(records), pqpath, row_group_size=7)
    monkeypatch.setattr(
        pq, "read_table", lambda *a, **kw: pytest.fail("whole-file read is forbidden")
    )
    a = OfflineDataProvider(csvpath).get_bars("S", "1m", 60000, 300000, max_bars=2)
    b = OfflineDataProvider(pqpath, batch_size=3).get_bars(
        "S", "1m", 60000, 300000, max_bars=2
    )
    assert a == b and [x.time for x in b] == [60000, 120000]
    assert OfflineDataProvider(pqpath).get_bars("S", "1m", None, None, max_bars=0) == []


def test_invalid_tail_not_hidden_by_max_bars_in_parquet(tmp_path):
    import pyarrow as pa
    import pyarrow.parquet as pq

    p = tmp_path / "a.parquet"
    pq.write_table(pa.Table.from_pylist([row(), row(60000, volume=-1)]), p)
    with pytest.raises(MDValidationError, match="volume"):
        OfflineDataProvider(p, batch_size=1).get_bars("S", "1m", 0, 1, max_bars=1)


def test_calendar_close_preserved(tmp_path):
    opened = int(datetime(2024, 2, 1, tzinfo=UTC).timestamp() * 1000)
    result = OfflineDataProvider(tmp_path / "a.csv")._bar_from_row(row(opened), "1M")
    assert (
        result.time_close
        == int(datetime(2024, 3, 1, tzinfo=UTC).timestamp() * 1000) - 1
    )


def test_cli_exposes_timestamp_and_missing_volume_policies(tmp_path, capsys):
    from marketdata_provider.cli.main import main

    path = write(tmp_path / "a.csv", [row(60, volume="")])
    assert main(["validate", "--path", str(path), "--timeframe", "1m"]) == 2
    capsys.readouterr()
    assert (
        main(
            [
                "validate",
                "--path",
                str(path),
                "--timeframe",
                "1m",
                "--timestamp-unit",
                "s",
                "--missing-volume",
                "zero",
            ]
        )
        == 0
    )
    import json

    assert json.loads(capsys.readouterr().out)["first"] == 60000
