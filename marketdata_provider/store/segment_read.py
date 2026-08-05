from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any, Literal, cast

from marketdata_provider.core.bar import MarketBar
from marketdata_provider.errors import MDInvalidExchangeResponse
from marketdata_provider.store.segment_checksums import (
    CANONICAL_CHECKSUM,
    LEGACY_CANONICAL_CHECKSUM,
    LEGACY_TAIL_CHAIN_CHECKSUM,
    PRESENCE_UNAWARE_CANONICAL_CHECKSUM,
    PRESENCE_UNAWARE_TAIL_CHAIN_CHECKSUM,
    TAIL_CHAIN_CHECKSUM,
    bars_checksum,
    legacy_bars_checksum,
    presence_unaware_bars_checksum,
    validate_csv_checksum,
    validate_persisted_bar_semantics,
)
from marketdata_provider.validation import validate_bars

SegmentFormat = Literal["csv", "parquet"]


def _reject_cross_format_data(data_path: Path, fmt: SegmentFormat) -> None:
    other_suffix = ".parquet" if fmt == "csv" else ".csv"
    if data_path.with_suffix(other_suffix).exists():
        raise MDInvalidExchangeResponse(
            "Persisted data exists in the other segment format"
        )


def _parquet_checksum(bars: list[MarketBar], manifest: dict[str, object]) -> str:
    for bar in bars:
        validate_persisted_bar_semantics(bar)
    algorithm = manifest.get("checksum_algorithm")
    if algorithm is None:
        algorithm = (
            LEGACY_CANONICAL_CHECKSUM
            if manifest.get("schema_version") == "stage-d-parquet-1"
            else CANONICAL_CHECKSUM
        )
    if algorithm == LEGACY_CANONICAL_CHECKSUM:
        return legacy_bars_checksum(bars)
    if algorithm == PRESENCE_UNAWARE_CANONICAL_CHECKSUM:
        return presence_unaware_bars_checksum(bars)
    if algorithm == CANONICAL_CHECKSUM:
        return bars_checksum(bars)
    if algorithm == LEGACY_TAIL_CHAIN_CHECKSUM:
        return legacy_bars_checksum(bars)
    if algorithm == PRESENCE_UNAWARE_TAIL_CHAIN_CHECKSUM:
        return presence_unaware_bars_checksum(bars)
    if algorithm == TAIL_CHAIN_CHECKSUM:
        return bars_checksum(bars)
    raise MDInvalidExchangeResponse(
        "Unsupported Parquet checksum algorithm", details={"algorithm": algorithm}
    )


def read_all(
    store: Any,
    *,
    exchange: str,
    market: str,
    symbol: str,
    timeframe: str,
    source_kind: str = "trade_kline",
    start: int | None = None,
    end: int | None = None,
) -> list[MarketBar]:
    manifest_path = (
        store._dir(
            exchange=exchange,
            market=market,
            symbol=symbol,
            timeframe=timeframe,
            source_kind=source_kind,
        )
        / "manifest.json"
    )
    fmt: SegmentFormat = store.data_format
    manifest: dict[str, object] | None = None
    if manifest_path.exists():
        loaded = json.loads(manifest_path.read_text())
        if not isinstance(loaded, dict):
            raise MDInvalidExchangeResponse("Segment manifest must be a JSON object")
        manifest = cast(dict[str, object], loaded)
        raw_format = manifest.get("data_format", fmt)
        if raw_format in {"csv", "parquet"}:
            fmt = cast(SegmentFormat, raw_format)
        store._validate_manifest_contract(manifest)
    data_path, _ = store._paths(
        exchange=exchange,
        market=market,
        symbol=symbol,
        timeframe=timeframe,
        source_kind=source_kind,
        data_format=fmt,
    )
    if manifest is None:
        directory = manifest_path.parent
        if (directory / "bars.csv").exists() or (directory / "bars.parquet").exists():
            raise MDInvalidExchangeResponse(
                "Segment manifest is missing for persisted data"
            )
        return []
    _reject_cross_format_data(data_path, fmt)
    if not data_path.exists():
        return []
    if fmt == "csv":
        validate_csv_checksum(data_path, manifest)
        if start is not None or end is not None:
            bars = list(
                store._iter_csv_range(
                    data_path, start=start, end=end, manifest=manifest
                )
            )
            validate_bars([bar.to_bar() for bar in bars])
            return bars
    bars = (
        store._read_parquet(data_path)
        if fmt == "parquet"
        else store._read_csv(data_path)
    )
    validate_bars([bar.to_bar() for bar in bars])
    if manifest is not None and fmt == "parquet":
        actual = _parquet_checksum(bars, manifest)
        if actual != manifest.get("checksum"):
            raise MDInvalidExchangeResponse(
                "Segment checksum mismatch",
                details={"expected": manifest.get("checksum"), "actual": actual},
            )
    return [
        bar
        for bar in bars
        if (start is None or bar.time >= start) and (end is None or bar.time < end)
    ]


def iter_all(
    store: Any,
    *,
    exchange: str,
    market: str,
    symbol: str,
    timeframe: str,
    source_kind: str = "trade_kline",
    start: int | None = None,
    end: int | None = None,
) -> Iterator[MarketBar]:
    manifest_path = (
        store._dir(
            exchange=exchange,
            market=market,
            symbol=symbol,
            timeframe=timeframe,
            source_kind=source_kind,
        )
        / "manifest.json"
    )
    fmt: SegmentFormat = store.data_format
    manifest: dict[str, object] | None = None
    if manifest_path.exists():
        loaded = json.loads(manifest_path.read_text())
        if not isinstance(loaded, dict):
            raise MDInvalidExchangeResponse("Segment manifest must be a JSON object")
        manifest = cast(dict[str, object], loaded)
        raw_format = manifest.get("data_format", fmt)
        if raw_format in {"csv", "parquet"}:
            fmt = cast(SegmentFormat, raw_format)
        store._validate_manifest_contract(manifest)
    data_path, _ = store._paths(
        exchange=exchange,
        market=market,
        symbol=symbol,
        timeframe=timeframe,
        source_kind=source_kind,
        data_format=fmt,
    )
    if manifest is None:
        directory = manifest_path.parent
        if (directory / "bars.csv").exists() or (directory / "bars.parquet").exists():
            raise MDInvalidExchangeResponse(
                "Segment manifest is missing for persisted data"
            )
        return
    _reject_cross_format_data(data_path, fmt)
    if not data_path.exists():
        return
    if fmt == "parquet":
        yield from read_all(
            store,
            exchange=exchange,
            market=market,
            symbol=symbol,
            timeframe=timeframe,
            source_kind=source_kind,
            start=start,
            end=end,
        )
        return
    validate_csv_checksum(data_path, manifest)
    yield from store._iter_csv_range(
        data_path,
        start=start,
        end=end,
        manifest=manifest,
    )
