"""Offline input boundary failures remain explicit; no silent repair."""

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from marketdata_provider import timeframes
from marketdata_provider.errors import MDTimeframeUnsupported, MDValidationError
from marketdata_provider.providers import offline
from marketdata_provider.providers.offline import OfflineDataProvider


@pytest.mark.parametrize(
    "options,message",
    [
        ({"timestamp_unit": "guess"}, "timestamp_unit"),
        ({"missing_volume": "guess"}, "missing_volume"),
        ({"batch_size": True}, "batch_size"),
        ({"batch_size": 0}, "batch_size"),
        ({"batch_size": 65537}, "batch_size"),
        ({"symbol": " "}, "symbol"),
        ({"symbol": 1}, "symbol"),
    ],
)
def test_invalid_reader_configuration_is_rejected(tmp_path, options, message):
    with pytest.raises(MDValidationError, match=message):
        OfflineDataProvider(tmp_path / "bars.csv", **options)


def test_defensive_timeframe_boundaries_reject_invalid_upstream_results(monkeypatch):
    # Fault injection checks the public boundary even if normalization regresses.
    monkeypatch.setattr(offline, "_timeframe_key", lambda _: 0)
    with pytest.raises(MDValidationError, match="positive duration"):
        OfflineDataProvider("unused.csv", timeframe="1m")
    monkeypatch.setattr(timeframes, "canonical_timeframe", lambda _: "invalid")
    with pytest.raises(MDTimeframeUnsupported, match="Cannot translate"):
        timeframes.to_pine_timeframe("1m")


def test_iso_timestamp_requires_nonempty_text(tmp_path):
    reader = OfflineDataProvider(tmp_path / "bars.csv", timestamp_unit="iso8601")
    for value in (None, 0, " "):
        with pytest.raises(MDValidationError, match="ISO 8601 timestamp"):
            reader._timestamp(value, 2, "time")


def test_interval_failure_is_contextualized(tmp_path, monkeypatch):
    def unavailable_interval(*args):
        raise OverflowError("calendar range exhausted")

    monkeypatch.setattr(offline, "close_time_ms", unavailable_interval)
    reader = OfflineDataProvider(tmp_path / "bars.csv")
    row = {
        "time": 253402300799999,
        "open": 10,
        "high": 12,
        "low": 9,
        "close": 11,
        "volume": 0,
    }
    with pytest.raises(MDValidationError, match="cannot derive the bar interval"):
        reader._bar_from_row(row, "1M", 2)


def test_legacy_csv_reader_retains_exact_interval_and_zero_volume(tmp_path):
    path = tmp_path / "bars.csv"
    path.write_text("time,open,high,low,close,volume\n0,10,12,9,11,0\n")
    bars = OfflineDataProvider(path)._read_csv("1m")
    assert len(bars) == 1
    assert (
        bars[0].time,
        bars[0].time_close,
        bars[0].open,
        bars[0].high,
        bars[0].low,
        bars[0].close,
        bars[0].volume,
    ) == (0, 59999, 10, 12, 9, 11, 0)


def test_incomplete_csv_schema_is_rejected(tmp_path):
    path = tmp_path / "bars.csv"
    path.write_text("time,open\n0,10\n")
    with pytest.raises(MDValidationError, match="timestamp and OHLC"):
        OfflineDataProvider(path).get_bars("BTCUSDT", "1m", None, None)


def test_incomplete_parquet_schema_is_rejected_not_wrapped(tmp_path):
    path = tmp_path / "bars.parquet"
    pq.write_table(pa.table({"time": [0], "open": [10]}), path)
    with pytest.raises(MDValidationError, match="unique timestamp and OHLC") as err:
        OfflineDataProvider(path).get_bars("BTCUSDT", "1m", None, None)
    assert err.value.details["field"] == "schema"


def test_row_timeframe_cannot_relabel_file(tmp_path):
    path = tmp_path / "bars.csv"
    path.write_text("time,open,high,low,close,volume,timeframe\n0,10,12,9,11,0,5m\n")
    with pytest.raises(MDValidationError, match="different interval"):
        OfflineDataProvider(path).get_bars("BTCUSDT", "1m", None, None)


def test_reversed_query_interval_is_rejected_before_file_io(tmp_path):
    with pytest.raises(MDValidationError, match="end precedes start"):
        OfflineDataProvider(tmp_path / "missing.csv").get_bars("BTCUSDT", "1m", 2, 1)
