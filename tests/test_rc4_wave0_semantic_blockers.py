from __future__ import annotations

from collections.abc import Mapping
from inspect import Parameter, signature
from pathlib import Path
from typing import Any

import pytest
from openpine_contracts import Finality, RevisionState

import marketdata_provider.service as service_module
from marketdata_provider import create_provider
from marketdata_provider.canonical.bar import (
    build_data_snapshot,
    canonical_bars_from_binance_klines,
    make_canonical_bar,
)
from marketdata_provider.config import (
    HistoryConfig,
    MarketDataConfig,
    OfflineDataConfig,
    StorageConfig,
)
from marketdata_provider.contracts import (
    BarQuery,
    BarSeries,
    InstrumentKey,
    parse_timeframe,
)
from marketdata_provider.core.bar import MarketBar
from marketdata_provider.errors import MDMissingFinality, MDValidationError
from marketdata_provider.exchanges.binance.rest import normalize_binance_klines
from marketdata_provider.store.segment_rows import row_to_bar


def _query() -> BarQuery:
    return BarQuery(
        instrument=InstrumentKey("binance", "spot", "BTCUSDT"),
        timeframe=parse_timeframe("1m"),
        start_ms=0,
        end_ms=60_000,
        source="provider",
    )


def _fetch_seeded_public_result(tmp_path: Path) -> object:
    provider = create_provider(
        MarketDataConfig(
            history=HistoryConfig(enabled=False),
            storage=StorageConfig(cache_dir=tmp_path),
        )
    )
    provider_bar = MarketBar(
        time=0,
        time_close=59_999,
        open=1.25,
        high=2.5,
        low=1.0,
        close=2.0,
        volume=3.75,
        exchange="binance",
        market="spot",
        symbol="BTCUSDT",
        timeframe="1m",
        source="fixture",
        source_transport="fixture",
        is_closed=True,
        provider="binance",
        provider_revision="fixture-v1",
    )
    service = provider.service
    service.store.segments.replace_all(
        [provider_bar],
        exchange="binance",
        market="spot",
        symbol="BTCUSDT",
        timeframe="1m",
    )
    return provider.fetch_bars(_query())


def test_public_provider_rejects_legacy_row_without_explicit_finality(
    tmp_path: Path,
) -> None:
    source = tmp_path / "legacy.csv"
    source.write_text(
        "time,open,high,low,close,volume,time_close\n" "0,1.25,2.5,1,2,3.75,59999\n",
        encoding="utf-8",
    )
    provider = create_provider(MarketDataConfig(offline=OfflineDataConfig(root=source)))

    with pytest.raises(MDMissingFinality):
        provider.fetch_bars(_query())


def test_public_provider_returns_canonical_v2_snapshot_not_float_bar_series(
    tmp_path: Path,
) -> None:
    result = _fetch_seeded_public_result(tmp_path)

    assert not isinstance(result, BarSeries), "public provider leaked legacy BarSeries"
    assert isinstance(result, Mapping)
    assert {
        "snapshot_id",
        "query",
        "bar_count",
        "bars",
        "series_hash",
    } <= result.keys()
    assert isinstance(result["snapshot_id"], str) and result["snapshot_id"]
    assert isinstance(result["series_hash"], str) and result["series_hash"]


def test_public_provider_returns_canonical_v2_bar_fields(tmp_path: Path) -> None:
    result = _fetch_seeded_public_result(tmp_path)
    bars = result["bars"] if isinstance(result, Mapping) else result.bars
    assert bars
    bar = bars[0]

    assert isinstance(bar, Mapping), "public provider leaked legacy float Bar"
    assert {
        "instrument_id",
        "timeframe",
        "open_time_utc_ms",
        "close_time_utc_ms",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "finality",
        "revision_state",
        "revision",
        "snapshot_id",
        "provider",
        "provider_revision",
        "series_id",
        "bar_content_hash",
    } <= bar.keys()
    assert {
        field: bar[field] for field in ("open", "high", "low", "close", "volume")
    } == {
        "open": "1.25",
        "high": "2.5",
        "low": "1",
        "close": "2",
        "volume": "3.75",
    }
    assert bar["finality"] is Finality.FINAL
    assert bar["revision_state"] is RevisionState.ORIGINAL
    assert bar["revision"] == 0
    assert isinstance(bar["provider_revision"], str) and bar["provider_revision"]
    assert isinstance(bar["series_id"], str) and bar["series_id"]
    assert isinstance(bar["bar_content_hash"], str) and bar["bar_content_hash"]
    if isinstance(result, Mapping):
        assert bar["snapshot_id"] == result["snapshot_id"]


def test_canonical_builders_require_provider_revision_in_signature() -> None:
    builders: tuple[Any, ...] = (
        make_canonical_bar,
        build_data_snapshot,
        canonical_bars_from_binance_klines,
    )
    optional = [
        builder.__name__
        for builder in builders
        if signature(builder).parameters["provider_revision"].default
        is not Parameter.empty
    ]

    assert optional == [], f"provider_revision must be required for: {optional}"


def test_public_snapshot_identity_is_deterministic_for_same_query_and_revision(
    tmp_path: Path,
) -> None:
    first = _fetch_seeded_public_result(tmp_path)
    second = _fetch_seeded_public_result(tmp_path)

    assert isinstance(first, Mapping) and isinstance(second, Mapping)
    assert first["snapshot_id"] == second["snapshot_id"]
    assert first["series_hash"] == second["series_hash"]


def test_storage_row_requires_finality_and_preserves_canonical_provenance() -> None:
    row: dict[str, object] = {
        "time": "0",
        "open": "1",
        "high": "2",
        "low": "0.5",
        "close": "1.5",
        "volume": "10",
        "time_close": "59999",
        "exchange": "binance",
        "market": "spot",
        "symbol": "BTCUSDT",
        "timeframe": "1m",
        "provider": "binance",
        "provider_revision": "binance-canonical-v2",
        "revision_state": "CORRECTED",
        "revision": "1",
    }
    with pytest.raises(MDMissingFinality):
        row_to_bar(row)

    row["is_closed"] = "true"
    without_revision = dict(row)
    without_revision.pop("provider_revision")
    with pytest.raises(MDValidationError, match="provider_revision"):
        row_to_bar(without_revision)

    restored = row_to_bar(row)
    assert restored.provider == "binance"
    assert restored.provider_revision == "binance-canonical-v2"
    assert restored.revision_state is RevisionState.CORRECTED
    assert restored.revision == 1


def test_public_closed_bar_policy_excludes_open_storage_rows(tmp_path: Path) -> None:
    provider = create_provider(
        MarketDataConfig(
            history=HistoryConfig(enabled=False),
            storage=StorageConfig(cache_dir=tmp_path),
        )
    )
    common = {
        "open": 1.0,
        "high": 2.0,
        "low": 0.5,
        "close": 1.5,
        "volume": 10.0,
        "exchange": "binance",
        "market": "spot",
        "symbol": "BTCUSDT",
        "timeframe": "1m",
        "provider": "binance",
        "provider_revision": "binance-canonical-v2",
    }
    service = provider.service
    service.store.segments.replace_all(
        [
            MarketBar(time=0, time_close=59_999, is_closed=True, **common),
            MarketBar(time=60_000, time_close=119_999, is_closed=False, **common),
        ],
        exchange="binance",
        market="spot",
        symbol="BTCUSDT",
        timeframe="1m",
    )
    query = BarQuery(
        instrument=_query().instrument,
        timeframe=_query().timeframe,
        start_ms=0,
        end_ms=120_000,
        source="provider",
    )

    snapshot = provider.fetch_bars(query)

    assert snapshot["bar_count"] == 1
    assert snapshot["bars"][0]["finality"] is Finality.FINAL


def test_open_binance_rest_bar_survives_service_and_public_boundaries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    open_bar = MarketBar(
        time=0,
        time_close=59_999,
        open=1.0,
        high=2.0,
        low=0.5,
        close=1.5,
        volume=10.0,
        exchange="binance",
        market="spot",
        symbol="BTCUSDT",
        timeframe="1m",
        source_transport="rest",
        is_closed=False,
        provider="binance",
        provider_revision="binance-rest-response-1",
    )
    monkeypatch.setattr(
        service_module,
        "binance_get_bars_sync",
        lambda *args, **kwargs: [open_bar],
    )
    provider = create_provider(
        MarketDataConfig(
            history=HistoryConfig(enabled=False, archive_first=False),
            storage=StorageConfig(cache_dir=tmp_path),
            include_open_candle=True,
        )
    )

    snapshot = provider.fetch_bars(_query())

    stored = provider.service._stored_bars(_query())
    assert stored == [], "OPEN REST bar was promoted into the finalized segment store"
    current = provider.service.store.get_current_market_candle(
        exchange="binance",
        market="spot",
        symbol="BTCUSDT",
        timeframe="1m",
    )
    assert current is not None and current.is_closed is False
    assert current.provider_revision == "binance-rest-response-1"
    assert snapshot["query"]["provider_revision"] == "binance-rest-response-1"
    assert snapshot["bar_count"] == 0

    closed_bar = MarketBar(
        time=0,
        time_close=59_999,
        open=1.0,
        high=2.0,
        low=0.5,
        close=1.5,
        volume=10.0,
        exchange="binance",
        market="spot",
        symbol="BTCUSDT",
        timeframe="1m",
        source_transport="rest",
        is_closed=True,
        provider="binance",
        provider_revision="binance-rest-response-2",
    )
    monkeypatch.setattr(
        service_module,
        "binance_get_bars_sync",
        lambda *args, **kwargs: [closed_bar],
    )
    finalized = provider.fetch_bars(_query())
    assert (
        provider.service.store.get_current_market_candle(
            exchange="binance", market="spot", symbol="BTCUSDT", timeframe="1m"
        )
        is None
    )
    assert finalized["bar_count"] == 1
    assert finalized["bars"][0]["finality"] is Finality.FINAL


def test_binance_decimal_text_survives_rest_storage_and_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    exact_open = "0.123456789123456789"
    normalized = normalize_binance_klines(
        [[0, exact_open, "1", "0.1", "0.5", "10", 59_999]],
        symbol="BTCUSDT",
        market="spot",
        timeframe="1m",
        server_time_ms=60_000,
    )
    monkeypatch.setattr(
        service_module,
        "binance_get_bars_sync",
        lambda *args, **kwargs: normalized,
    )
    provider = create_provider(
        MarketDataConfig(
            history=HistoryConfig(enabled=False, archive_first=False),
            storage=StorageConfig(cache_dir=tmp_path),
        )
    )

    first = provider.fetch_bars(_query())
    second = provider.fetch_bars(_query())

    assert first["bars"][0]["open"] == exact_open
    assert second["bars"][0]["open"] == exact_open
    assert first["series_hash"] == second["series_hash"]
