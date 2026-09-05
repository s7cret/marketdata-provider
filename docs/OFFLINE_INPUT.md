# Strict offline input

`OfflineDataProvider` reads CSV and genuine Parquet. Query `start` is inclusive,
`end` exclusive; query bounds always use UTC milliseconds. Input timestamps use
milliseconds unless `timestamp_unit="s"` or `"iso8601"` is explicitly selected.
ISO timestamps require an offset/timezone and exact millisecond precision.
Timestamp units are never inferred from magnitude. Missing timestamps are errors,
not the Unix epoch. Equal aliases such as `time=0,timestamp=0` are accepted;
conflicting aliases are rejected.

OHLC and volume must be finite, representable numeric values. Missing volume is an
error by default. An explicitly configured `missing_volume="zero"` permits filling
missing volume only, not OHLC or time. Duplicate/out-of-order rows are not silently
sorted or deduplicated. Explicit close timestamps use the same input unit and must
match the timeframe's inclusive final millisecond; omitting the close column
allows it to be computed. Empty supplied close cells are invalid.

Bind `symbol` and `timeframe` when constructing a provider to prevent reusing the
file under another query identity. Optional row-level symbol/timeframe columns are
checked too. A file without symbol metadata still relies on the caller's explicit
binding: no external instrument provenance is invented.

CSV is iterated row by row. Parquet uses `ParquetFile.iter_batches` (default 4096
rows, configurable from 1 to 65536), projecting relevant columns. All rows,
including those outside the selected range or after `max_bars`, are validated.
Only matching bars are retained. This bounds intermediate parsing memory but does
not avoid scanning the whole file; the returned selected list still materializes.
There is no claim of immutable snapshot proof, row-group predicate pushdown or
concurrent-file-write protection. Do not mutate files while importing.

```sh
marketdata validate --path bars.csv --timeframe 1m --timestamp-unit ms
marketdata validate --path seconds.csv --timeframe 1m --timestamp-unit s --missing-volume zero
```

Errors identify the file, source row and offending field. Existing valid datasets
retain their values and row ordering. No file is rewritten during validation.
