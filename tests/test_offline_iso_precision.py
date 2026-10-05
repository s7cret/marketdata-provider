"""Frozen textual fixtures guard lossless ISO timestamp admission in both formats."""

import csv
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from marketdata_provider.errors import MDValidationError
from marketdata_provider.providers.offline import OfflineDataProvider

FIXTURE = json.loads(
    (Path(__file__).parent / "fixtures" / "offline_iso_precision.json").read_text()
)


def write_dataset(tmp_path, suffix, records):
    path = tmp_path / f"precision.{suffix}"
    if suffix == "csv":
        with path.open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(records[0]))
            writer.writeheader()
            writer.writerows(records)
    else:
        pq.write_table(pa.Table.from_pylist(records), path, row_group_size=1)
    return path


def record(timestamp, **extra):
    return dict(time=timestamp, open=10, high=12, low=9, close=11, volume=0, **extra)


@pytest.mark.parametrize("suffix", ["csv", "parquet"])
@pytest.mark.parametrize("timestamp", FIXTURE["invalid"] + FIXTURE["separator_invalid"])
@pytest.mark.parametrize("field", ["time", "timestamp", "time_close"])
def test_nonmillisecond_iso_components_are_not_silently_truncated(
    tmp_path, suffix, timestamp, field
):
    source = record("1970-01-01T00:01:00Z")
    source[field] = timestamp
    path = write_dataset(tmp_path, suffix, [source])
    with pytest.raises(MDValidationError, match="ISO 8601") as error:
        OfflineDataProvider(path, timestamp_unit="iso8601", batch_size=1).get_bars(
            "S", "1m", None, None
        )
    assert error.value.details["field"] == field
    assert error.value.details["row"] == (2 if suffix == "csv" else 1)


@pytest.mark.parametrize("suffix", ["csv", "parquet"])
@pytest.mark.parametrize("case", FIXTURE["valid"] + FIXTURE["separator_compatibility"])
def test_exact_milliseconds_and_zero_precision_tails_are_preserved(
    tmp_path, suffix, case
):
    path = write_dataset(tmp_path, suffix, [record(case["text"])])
    bars = OfflineDataProvider(path, timestamp_unit="iso8601").get_bars(
        "S", "1m", None, None
    )
    assert len(bars) == 1
    assert (bars[0].time, bars[0].time_close) == (
        case["utc_ms"],
        case["utc_ms"] + 59999,
    )
    assert (bars[0].open, bars[0].high, bars[0].low, bars[0].close, bars[0].volume) == (
        10,
        12,
        9,
        11,
        0,
    )


@pytest.mark.parametrize("suffix", ["csv", "parquet"])
@pytest.mark.parametrize("open_field", ["time", "timestamp", "open_time"])
@pytest.mark.parametrize("close_field", ["time_close", "close_time"])
@pytest.mark.parametrize(
    "case",
    [
        {
            "open": "1970-01-01T00:01:00+00:00:00.001000000",
            "close": "1970-01-01T00:02:00+00:00:00.002000000",
            "utc_ms": 59999,
        },
        {
            "open": "1970-01-01T00:01:00-00:00:00.001000000",
            "close": "1970-01-01T00:02:00Z",
            "utc_ms": 60001,
        },
    ],
)
def test_zero_integer_offset_preserved_in_open_and_close_aliases(
    tmp_path, suffix, open_field, close_field, case
):
    source = record(case["open"])
    source[open_field] = source.pop("time")
    source[close_field] = case["close"]
    path = write_dataset(tmp_path, suffix, [source])
    bars = OfflineDataProvider(path, timestamp_unit="iso8601").get_bars(
        "S", "1m", None, None
    )
    assert len(bars) == 1
    assert (bars[0].time, bars[0].time_close) == (
        case["utc_ms"],
        case["utc_ms"] + 59999,
    )


@pytest.mark.parametrize("suffix", ["csv", "parquet"])
def test_query_limit_and_range_do_not_hide_lossy_iso_tail(tmp_path, suffix):
    path = write_dataset(
        tmp_path,
        suffix,
        [
            record("1970-01-01T00:00:00Z"),
            record("1970-01-01T00:01:00.0000001Z"),
        ],
    )
    with pytest.raises(MDValidationError, match="ISO 8601"):
        OfflineDataProvider(path, timestamp_unit="iso8601", batch_size=1).get_bars(
            "S", "1m", 0, 1, max_bars=1
        )
