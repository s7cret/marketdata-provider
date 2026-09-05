"""Strict, streaming offline OHLCV input. Query timestamps are always UTC ms."""

from __future__ import annotations

import csv
import math
import re
from collections.abc import Iterator, Mapping
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from marketdata_provider.core.bar import Bar
from marketdata_provider.core.protocols import DataProvider, IntrabarDataProvider
from marketdata_provider.errors import (
    MDIntrabarDataUnavailable,
    MDUnsupportedFeature,
    MDValidationError,
)
from marketdata_provider.timeframes import (
    canonical_timeframe,
    close_time_ms,
    timeframe_ms,
)

_OPEN = ("time", "timestamp", "open_time")
_CLOSE = ("time_close", "close_time")
_PRICES = ("open", "high", "low", "close", "volume")
_COLUMNS = frozenset((*_OPEN, *_CLOSE, *_PRICES, "symbol", "timeframe"))


def _timeframe_key(value: str) -> str | int:
    normalized = canonical_timeframe(value)
    return normalized if normalized in {"1D", "1W", "1M"} else timeframe_ms(normalized)


class OfflineDataProvider(DataProvider, IntrabarDataProvider):
    """Read all rows for validation, retain only the requested ordered range.

    No sorting, deduplication, unit inference, missing timestamp or OHLC repair.
    CSV and Parquet use the same conversion. Missing volume can be filled with
    zero only by explicitly choosing ``missing_volume='zero'``. Optional symbol
    and timeframe bindings prevent relabelling a file as another dataset.
    """

    def __init__(
        self,
        path: str | Path,
        *,
        timeframe: str | None = None,
        symbol: str | None = None,
        timestamp_unit: str = "ms",
        missing_volume: str = "error",
        batch_size: int = 4096,
    ) -> None:
        if timestamp_unit not in {"ms", "s", "iso8601"}:
            raise MDValidationError("timestamp_unit must be ms, s or iso8601")
        if missing_volume not in {"error", "zero"}:
            raise MDValidationError("missing_volume must be error or zero")
        if type(batch_size) is not int or not 1 <= batch_size <= 65536:
            raise MDValidationError("batch_size must be an integer from 1 to 65536")
        if symbol is not None and (type(symbol) is not str or not symbol.strip()):
            raise MDValidationError("bound symbol must be nonempty")
        if timeframe is not None:
            self._validate_timeframe(timeframe)
        self.path, self.timeframe, self.symbol = Path(path), timeframe, symbol
        self.timestamp_unit, self.missing_volume, self.batch_size = (
            timestamp_unit,
            missing_volume,
            batch_size,
        )

    @staticmethod
    def _validate_timeframe(timeframe: str) -> None:
        key = _timeframe_key(timeframe)
        if isinstance(key, int) and key <= 0:
            raise MDValidationError("timeframe must have a positive duration")

    def _error(self, row: int, field: str, reason: str) -> MDValidationError:
        return MDValidationError(
            f"{self.path.name}: row {row}, {field}: {reason}",
            details={
                "path": str(self.path),
                "row": row,
                "field": field,
                "reason": reason,
            },
        )

    def _timestamp(self, value: Any, row: int, field: str) -> int:
        if self.timestamp_unit == "iso8601":
            if not isinstance(value, str) or not value.strip():
                raise self._error(
                    row, field, "an ISO 8601 timestamp with timezone is required"
                )
            try:
                stamp = datetime.fromisoformat(value.strip())
                if (
                    stamp.tzinfo is None
                    or stamp.utcoffset() is None
                    or stamp.microsecond % 1000
                ):
                    raise ValueError(
                        "timezone and exact millisecond precision are required"
                    )
                delta = stamp.astimezone(UTC) - datetime(1970, 1, 1, tzinfo=UTC)
                result = (
                    delta.days * 86400 + delta.seconds
                ) * 1000 + delta.microseconds // 1000
            except (ValueError, OverflowError) as exc:
                raise self._error(
                    row, field, "invalid or timezone-free ISO 8601 timestamp"
                ) from exc
        else:
            if type(value) is int:
                result = value
            elif isinstance(value, str) and re.fullmatch(r"[0-9]+", value.strip()):
                result = int(value)
            else:
                raise self._error(
                    row,
                    field,
                    "an explicit integer timestamp is required; units are not inferred",
                )
            if self.timestamp_unit == "s":
                result *= 1000
        if not 0 <= result <= 253402300799999:
            raise self._error(
                row, field, "timestamp is outside supported UTC epoch range"
            )
        return result

    def _aliases(
        self, data: Mapping, names: tuple[str, ...], row: int, *, required: bool
    ) -> int | None:
        present = [key for key in names if key in data]
        if not present:
            if required:
                raise self._error(row, names[0], "timestamp column is missing")
            return None
        values = [self._timestamp(data[key], row, key) for key in present]
        if len(set(values)) != 1:
            raise self._error(row, names[0], "timestamp aliases disagree")
        return values[0]

    def _bar_from_row(
        self, row: Mapping[str, Any], timeframe: str, row_number: int = 1
    ) -> Bar:
        opened = self._aliases(row, _OPEN, row_number, required=True)
        assert opened is not None
        closed = self._aliases(row, _CLOSE, row_number, required=False)
        try:
            expected_close = close_time_ms(opened, timeframe)
        except (ValueError, OverflowError, OSError) as exc:
            raise self._error(
                row_number, "time", "cannot derive the bar interval"
            ) from exc
        if closed is not None and closed != expected_close:
            raise self._error(
                row_number,
                "time_close",
                "does not match the declared timeframe (inclusive milliseconds)",
            )
        numbers: dict[str, Decimal] = {}
        for field in _PRICES:
            value = row.get(field)
            if (
                field == "volume"
                and (value is None or value == "")
                and self.missing_volume == "zero"
            ):
                value = 0
            try:
                if isinstance(value, bool) or value is None or value == "":
                    raise ValueError("missing or boolean value")
                number = Decimal(str(value))
                if not number.is_finite() or not math.isfinite(float(number)):
                    raise ValueError("not finite")
                if number != 0 and float(number) == 0:
                    raise ValueError("underflows runtime range")
                numbers[field] = number
            except (InvalidOperation, ValueError, OverflowError) as exc:
                raise self._error(
                    row_number,
                    field,
                    "a finite numeric value in runtime range is required",
                ) from exc
        if (
            not numbers["low"]
            <= min(numbers["open"], numbers["close"])
            <= max(numbers["open"], numbers["close"])
            <= numbers["high"]
        ):
            raise self._error(
                row_number, "OHLC", "low/open/close/high bounds are inconsistent"
            )
        if numbers["volume"] < 0:
            raise self._error(row_number, "volume", "must be nonnegative")
        return Bar(opened, *(float(numbers[key]) for key in _PRICES), expected_close)

    # Kept as explicit readers for callers using the old internal diagnostic API.
    # Normal get_bars never materializes an intermediate whole-file list.
    def _read_csv(self, timeframe: str) -> list[Bar]:
        return self._collect(self._csv_rows(), self.symbol, timeframe, None, None, None)

    def _read_parquet(self, timeframe: str) -> list[Bar]:
        return self._collect(
            self._parquet_rows(), self.symbol, timeframe, None, None, None
        )

    def _csv_rows(self) -> Iterator[tuple[int, Mapping[str, Any]]]:
        with self.path.open(encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            names = reader.fieldnames
            if (
                not names
                or len(set(names)) != len(names)
                or any(not n or n.strip() != n for n in names)
            ):
                raise self._error(
                    1, "header", "missing, duplicate or whitespace-padded column names"
                )
            if not set(names).intersection(_OPEN) or not set(_PRICES[:-1]).issubset(
                names
            ):
                raise self._error(
                    1, "header", "timestamp and OHLC columns are required"
                )
            for record in reader:
                if None in record or any(value is None for value in record.values()):
                    raise self._error(
                        reader.line_num, "record", "CSV field count differs from header"
                    )
                yield reader.line_num, record

    def _parquet_rows(self) -> Iterator[tuple[int, Mapping[str, Any]]]:
        try:
            import pyarrow.parquet as pq
        except ImportError as exc:
            raise MDUnsupportedFeature(
                "Parquet support requires pyarrow extra"
            ) from exc
        try:
            with pq.ParquetFile(self.path) as source:
                names = source.schema_arrow.names
                if (
                    len(set(names)) != len(names)
                    or not set(names).intersection(_OPEN)
                    or not set(_PRICES[:-1]).issubset(names)
                ):
                    raise self._error(
                        0, "schema", "unique timestamp and OHLC columns are required"
                    )
                row_number = 0
                for batch in source.iter_batches(
                    batch_size=self.batch_size,
                    columns=[name for name in names if name in _COLUMNS],
                ):
                    for record in batch.to_pylist():
                        row_number += 1
                        yield row_number, record
        except MDValidationError:
            raise
        except Exception as exc:
            raise MDUnsupportedFeature(
                f"Parquet offline data unavailable: {self.path}"
            ) from exc

    def _collect(self, rows, symbol, timeframe, start, end, max_bars) -> list[Bar]:
        output: list[Bar] = []
        previous: int | None = None
        for number, record in rows:
            if "symbol" in record and symbol is not None and record["symbol"] != symbol:
                raise self._error(
                    number, "symbol", "file contains a different instrument"
                )
            if "timeframe" in record and _timeframe_key(
                str(record["timeframe"])
            ) != _timeframe_key(timeframe):
                raise self._error(
                    number, "timeframe", "file contains a different interval"
                )
            bar = self._bar_from_row(record, timeframe, number)
            if previous is not None and bar.time <= previous:
                raise self._error(
                    number,
                    "time",
                    "timestamps must increase strictly; duplicates and reordering are not repaired",
                )
            previous = bar.time
            if (
                (start is None or bar.time >= start)
                and (end is None or bar.time < end)
                and (max_bars is None or len(output) < max_bars)
            ):
                output.append(bar)
        return output

    def get_bars(
        self,
        symbol: str,
        timeframe: str,
        start: int | None,
        end: int | None,
        *,
        max_bars: int | None = None,
    ) -> list[Bar]:
        self._validate_timeframe(timeframe)
        if self.symbol is not None and symbol != self.symbol:
            raise MDValidationError(
                "query symbol differs from the bound offline dataset"
            )
        if self.timeframe is not None and _timeframe_key(timeframe) != _timeframe_key(
            self.timeframe
        ):
            raise MDValidationError(
                "query timeframe differs from the bound offline dataset"
            )
        for name, value in (("start", start), ("end", end), ("max_bars", max_bars)):
            if value is not None and (type(value) is not int or value < 0):
                raise MDValidationError(f"{name} must be a nonnegative integer or None")
        if start is not None and end is not None and end < start:
            raise MDValidationError("query end precedes start")
        suffix = self.path.suffix.lower()
        if suffix not in {".csv", ".parquet"}:
            raise MDUnsupportedFeature(
                f"Unsupported offline format: {self.path.suffix}"
            )
        # Validate even discarded rows: a range/max_bars must not hide bad source data.
        return self._collect(
            self._csv_rows() if suffix == ".csv" else self._parquet_rows(),
            symbol,
            timeframe,
            start,
            end,
            max_bars,
        )

    def get_intrabar_bars(
        self,
        symbol: str,
        chart_bar: Bar,
        lower_timeframe: str | None = None,
        *,
        max_bars: int | None = None,
    ) -> list[Bar]:
        timeframe = lower_timeframe or self.timeframe
        if timeframe is None or chart_bar.time_close is None:
            raise MDIntrabarDataUnavailable(
                "Offline intrabar requires an explicit timeframe and chart close time"
            )
        return self.get_bars(
            symbol,
            timeframe,
            chart_bar.time,
            chart_bar.time_close + 1,
            max_bars=max_bars,
        )
