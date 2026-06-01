from __future__ import annotations

import csv
from collections.abc import Iterable
from datetime import datetime, timezone
from io import TextIOWrapper
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import urlopen
from zipfile import ZipFile

from marketdata_provider.core.bar import Bar
from marketdata_provider.timeframes import timeframe_ms, to_binance_interval

BINANCE_ARCHIVE_BASE_URL = "https://data.binance.vision/data"
BINANCE_ARCHIVE_MARKET_PATHS = {
    "spot": "spot",
    "usdm": "futures/um",
    "usd-m": "futures/um",
    "um": "futures/um",
    "linear": "futures/um",
    "coinm": "futures/cm",
    "coin-m": "futures/cm",
    "cm": "futures/cm",
    "inverse": "futures/cm",
}
MAX_DAILY_ARCHIVE_DAYS = 45
MAX_MONTHLY_ARCHIVE_MONTHS = 12


def fetch_binance_archive_bars(
    *,
    symbol: str,
    market: str,
    timeframe: str,
    start: int,
    end: int,
    cache_dir: Path,
) -> list[Bar]:
    """Fetch Binance archive klines for a closed historical range.

    Monthly ZIPs are preferred for authoritative historical reads because old
    BTCUSDT daily ZIPs can match REST while the monthly ZIP contains different
    shifted historical rows used by TradingView. Daily ZIPs are only a fallback
    when the monthly archive is unavailable or incomplete.
    """

    if start >= end or _archive_market_path(market) is None:
        return []
    duration = timeframe_ms(timeframe)
    if duration is None:
        return []

    archive_root = Path(cache_dir) / "archives" / "binance_klines"
    intervals = ((start, end),)
    bars: list[Bar] = []
    for year, month in _months_for_intervals(intervals):
        bars.extend(
            _load_archive_file(
                symbol=symbol,
                market=market,
                timeframe=timeframe,
                start=start,
                end=end,
                period="monthly",
                suffix=f"{year:04d}-{month:02d}",
                cache_dir=archive_root,
            )
        )
    bars = _dedupe_sorted(bars)
    if _range_coverage_complete(bars, start=start, end=end, duration=duration):
        return bars

    daily_bars: list[Bar] = []
    for year, month, day in _days_for_intervals(intervals):
        daily_bars.extend(
            _load_archive_file(
                symbol=symbol,
                market=market,
                timeframe=timeframe,
                start=start,
                end=end,
                period="daily",
                suffix=f"{year:04d}-{month:02d}-{day:02d}",
                cache_dir=archive_root,
            )
        )
    if _range_coverage_complete(daily_bars, start=start, end=end, duration=duration):
        return daily_bars
    return bars


def fill_binance_archive_gaps(
    bars: Iterable[Bar],
    *,
    symbol: str,
    market: str,
    timeframe: str,
    start: int | None,
    end: int | None,
    cache_dir: Path,
) -> list[Bar]:
    """Fill missing Binance klines from data.binance.vision archives.

    The archive is used only for bars missing from the primary REST result and
    only at the same requested timeframe. Narrow repairs try daily ZIPs first;
    monthly ZIPs are a fallback for old Binance cases where daily ZIPs are also
    missing shifted historical rows.
    """

    current = {bar.time: bar for bar in bars}
    if start is None or end is None or not current:
        return [current[t] for t in sorted(current)]

    duration = timeframe_ms(timeframe)
    if duration is None or _archive_market_path(market) is None:
        return [current[t] for t in sorted(current)]

    missing = _coalesce_intervals(_missing_intervals(current, start=start, end=end, duration=duration))
    if not missing:
        return [current[t] for t in sorted(current)]

    archive_root = Path(cache_dir) / "archives" / "binance_klines"
    wanted = _missing_starts(current, start=start, end=end, duration=duration)
    archive_bars = _load_archive_bars(
        symbol=symbol,
        market=market,
        timeframe=timeframe,
        start=start,
        end=end,
        missing_intervals=missing,
        cache_dir=archive_root,
    )
    for bar in archive_bars:
        if bar.time in wanted:
            current[bar.time] = bar
    return [current[t] for t in sorted(current)]


def _dedupe_sorted(bars: Iterable[Bar]) -> list[Bar]:
    by_time = {bar.time: bar for bar in bars}
    return [by_time[t] for t in sorted(by_time)]


def _range_coverage_complete(bars: Iterable[Bar], *, start: int, end: int, duration: int) -> bool:
    delivered = {bar.time for bar in bars}
    expected = set(range(start, end, duration))
    return bool(expected) and expected.issubset(delivered)


def _load_archive_bars(
    *,
    symbol: str,
    market: str,
    timeframe: str,
    start: int,
    end: int,
    missing_intervals: tuple[tuple[int, int], ...],
    cache_dir: Path,
) -> list[Bar]:
    days = _days_for_intervals(missing_intervals)
    if len(days) <= MAX_DAILY_ARCHIVE_DAYS:
        bars: list[Bar] = []
        for year, month, day in days:
            bars.extend(
                _load_archive_file(
                    symbol=symbol,
                    market=market,
                    timeframe=timeframe,
                    start=start,
                    end=end,
                    period="daily",
                    suffix=f"{year:04d}-{month:02d}-{day:02d}",
                    cache_dir=cache_dir,
                )
            )
        if _archive_covers_intervals(bars, missing_intervals, duration=timeframe_ms(timeframe)):
            return bars

    months = _months_for_intervals(missing_intervals)
    if len(months) > MAX_MONTHLY_ARCHIVE_MONTHS:
        return []
    bars = []
    for year, month in months:
        bars.extend(
            _load_archive_file(
                symbol=symbol,
                market=market,
                timeframe=timeframe,
                start=start,
                end=end,
                period="monthly",
                suffix=f"{year:04d}-{month:02d}",
                cache_dir=cache_dir,
            )
        )
    return bars


def _load_archive_file(
    *,
    symbol: str,
    market: str,
    timeframe: str,
    start: int,
    end: int,
    period: str,
    suffix: str,
    cache_dir: Path,
) -> list[Bar]:
    interval = to_binance_interval(timeframe)
    symbol = symbol.upper()
    name = f"{symbol}-{interval}-{suffix}.zip"
    path = cache_dir / market.lower() / period / symbol / interval / name
    if not path.exists():
        market_path = _archive_market_path(market)
        if market_path is None:
            return []
        path.parent.mkdir(parents=True, exist_ok=True)
        url = f"{BINANCE_ARCHIVE_BASE_URL}/{market_path}/{period}/klines/{symbol}/{interval}/{name}"
        try:
            with urlopen(url, timeout=30) as response:
                path.write_bytes(response.read())
        except (HTTPError, URLError, TimeoutError):
            return []

    duration = timeframe_ms(timeframe)
    if duration is None:
        return []

    by_time: dict[int, Bar] = {}
    try:
        with ZipFile(path) as archive:
            csv_name = next((item for item in archive.namelist() if item.endswith(".csv")), None)
            if csv_name is None:
                return []
            with archive.open(csv_name) as raw_file:
                reader = csv.reader(TextIOWrapper(raw_file, encoding="utf-8"))
                for row in reader:
                    if not row:
                        continue
                    try:
                        raw_open_time = _epoch_to_ms(int(row[0]))
                    except ValueError:
                        continue
                    open_time = _normalize_open_time(raw_open_time, duration)
                    if not (start <= open_time < end):
                        continue
                    bar = Bar(
                        time=open_time,
                        open=float(row[1]),
                        high=float(row[2]),
                        low=float(row[3]),
                        close=float(row[4]),
                        volume=float(row[5]),
                        time_close=open_time + duration - 1,
                    )
                    by_time[open_time] = _merge_same_open_time(by_time.get(open_time), bar)
    except Exception:
        return []
    return [by_time[t] for t in sorted(by_time)]


def _merge_same_open_time(current: Bar | None, new: Bar) -> Bar:
    if current is None:
        return new
    return Bar(
        time=current.time,
        open=current.open,
        high=max(current.high, new.high),
        low=min(current.low, new.low),
        close=new.close,
        volume=current.volume + new.volume,
        time_close=current.time_close,
    )


def _archive_market_path(market: str) -> str | None:
    return BINANCE_ARCHIVE_MARKET_PATHS.get(market.lower())


def _normalize_open_time(open_time_ms: int, duration: int) -> int:
    return (open_time_ms // duration) * duration


def _epoch_to_ms(value: int) -> int:
    # Binance archives changed some files from millisecond to microsecond
    # timestamps in newer history; normalize both onto the runtime ms contract.
    return value // 1000 if value >= 100_000_000_000_000 else value


def _missing_starts(
    bars: dict[int, Bar],
    *,
    start: int,
    end: int,
    duration: int,
) -> frozenset[int]:
    return frozenset(ts for ts in range(start, end, duration) if ts not in bars)


def _missing_intervals(
    bars: dict[int, Bar],
    *,
    start: int,
    end: int,
    duration: int,
) -> tuple[tuple[int, int], ...]:
    return tuple((ts, min(ts + duration, end)) for ts in _missing_starts(bars, start=start, end=end, duration=duration))


def _coalesce_intervals(intervals: tuple[tuple[int, int], ...]) -> tuple[tuple[int, int], ...]:
    if not intervals:
        return ()
    ordered = sorted(intervals)
    merged: list[tuple[int, int]] = []
    current_start, current_end = ordered[0]
    for start, end in ordered[1:]:
        if start <= current_end:
            current_end = max(current_end, end)
            continue
        merged.append((current_start, current_end))
        current_start, current_end = start, end
    merged.append((current_start, current_end))
    return tuple(merged)


def _archive_covers_intervals(
    bars: list[Bar],
    intervals: tuple[tuple[int, int], ...],
    *,
    duration: int | None,
) -> bool:
    if duration is None:
        return False
    delivered = {bar.time for bar in bars}
    expected = {ts for start, end in intervals for ts in range(start, end, duration)}
    return bool(expected) and expected.issubset(delivered)


def _days_for_intervals(intervals: tuple[tuple[int, int], ...]) -> tuple[tuple[int, int, int], ...]:
    days: set[tuple[int, int, int]] = set()
    one_day_ms = 86_400_000
    for start, end in intervals:
        cursor = (start // one_day_ms) * one_day_ms
        last = (max(start, end - 1) // one_day_ms) * one_day_ms
        while cursor <= last:
            day = datetime.fromtimestamp(cursor / 1000, timezone.utc)
            days.add((day.year, day.month, day.day))
            cursor += one_day_ms
    return tuple(sorted(days))


def _months_for_intervals(intervals: tuple[tuple[int, int], ...]) -> tuple[tuple[int, int], ...]:
    months: set[tuple[int, int]] = set()
    for start, end in intervals:
        cursor = datetime.fromtimestamp(start / 1000, timezone.utc).replace(day=1)
        last = datetime.fromtimestamp(max(start, end - 1) / 1000, timezone.utc).replace(day=1)
        while cursor <= last:
            months.add((cursor.year, cursor.month))
            year = cursor.year + (1 if cursor.month == 12 else 0)
            month = 1 if cursor.month == 12 else cursor.month + 1
            cursor = cursor.replace(year=year, month=month)
    return tuple(sorted(months))
