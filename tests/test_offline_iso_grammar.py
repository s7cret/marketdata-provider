"""Reject malformed ISO clock fractions before CPython can discard their digits."""

import csv

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from marketdata_provider.errors import MDValidationError
from marketdata_provider.providers.offline import OfflineDataProvider

MALFORMED_CLOCKS = [
    (
        "1970-01-01T00:01:00:0000001Z",
        "1970-01-01T00:01:59:9990001Z",
    ),
    ("19700101T0001000000001Z", "19700101T0001599990001Z"),
    (
        "1970-01-01T01:01:00+01:00:00:0000001",
        "1970-01-01T01:01:59.999+01:00:00:0000001",
    ),
    ("19700101T010100+0100000000001", "19700101T010159.999+0100000000001"),
    (
        "1970-01-01T00:01:00+00:00:00:0010001",
        "1970-01-01T00:01:59.999+00:00:00:0010001",
    ),
    ("19700101T000100+0000000010001", "19700101T000159.999+0000000010001"),
    (
        "1970-01-01T00:01:00-00:00:00:0010001",
        "1970-01-01T00:01:59.999-00:00:00:0010001",
    ),
    ("19700101T000100-0000000010001", "19700101T000159.999-0000000010001"),
]
FORM_IDS = [
    "extended-local-colon",
    "basic-local-undelimited",
    "extended-offset-colon",
    "basic-offset-undelimited",
    "extended-zero-offset-positive",
    "basic-zero-offset-positive",
    "extended-zero-offset-negative",
    "basic-zero-offset-negative",
]


def _record(timestamp):
    return dict(time=timestamp, open=10, high=12, low=9, close=11, volume=0)


def _write_dataset(tmp_path, suffix, records):
    path = tmp_path / f"grammar.{suffix}"
    if suffix == "csv":
        with path.open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(records[0]))
            writer.writeheader()
            writer.writerows(records)
    else:
        pq.write_table(pa.Table.from_pylist(records), path, row_group_size=1)
    return path


@pytest.mark.parametrize("suffix", ["csv", "parquet"])
@pytest.mark.parametrize("opened,closed", MALFORMED_CLOCKS, ids=FORM_IDS)
@pytest.mark.parametrize(
    "field", ["time", "timestamp", "open_time", "time_close", "close_time"]
)
def test_malformed_iso_clock_fraction_is_rejected_in_every_alias(
    tmp_path, suffix, opened, closed, field
):
    source = _record("1970-01-01T00:01:00Z")
    # Both lossy values would otherwise fit a valid bar, including its close.
    source[field] = closed if field in {"time_close", "close_time"} else opened
    path = _write_dataset(tmp_path, suffix, [source])
    with pytest.raises(MDValidationError, match="ISO 8601") as error:
        OfflineDataProvider(path, timestamp_unit="iso8601", batch_size=1).get_bars(
            "S", "1m", None, None
        )
    assert error.value.details["field"] == field
    assert error.value.details["row"] == (2 if suffix == "csv" else 1)


@pytest.mark.parametrize("suffix", ["csv", "parquet"])
@pytest.mark.parametrize("opened,closed", MALFORMED_CLOCKS[:2], ids=FORM_IDS[:2])
def test_query_filter_does_not_hide_malformed_iso_clock_tail(
    tmp_path, suffix, opened, closed
):
    path = _write_dataset(
        tmp_path,
        suffix,
        [_record("1970-01-01T00:00:00Z"), _record(opened)],
    )
    with pytest.raises(MDValidationError, match="ISO 8601") as error:
        OfflineDataProvider(path, timestamp_unit="iso8601", batch_size=1).get_bars(
            "S", "1m", 0, 1, max_bars=1
        )
    assert error.value.details["field"] == "time"
    assert error.value.details["row"] == (3 if suffix == "csv" else 2)
