from datetime import UTC, datetime

import pytest

from marketdata_provider import create_provider
from marketdata_provider.config import (
    ArtifactIdentityConfig,
    HistoryConfig,
    MarketDataConfig,
    StorageConfig,
)
from marketdata_provider.contracts import BarQuery, InstrumentKey, parse_timeframe
from marketdata_provider.exchanges.binance.provider import binance_get_bars_sync
from marketdata_provider.service import MarketDataService

pytestmark = pytest.mark.live_network


def _ms(year, month, day):
    return int(datetime(year, month, day, tzinfo=UTC).timestamp() * 1000)


def test_real_binance_btcusdt_1d_derived_from_1m_matches_official(tmp_path):
    start = _ms(2024, 1, 1)
    end = _ms(2024, 1, 3)
    instrument = InstrumentKey("binance", "spot", "BTCUSDT")
    query = BarQuery(instrument, parse_timeframe("1D"), start, end, gap_policy="fail")

    derived = MarketDataService(
        MarketDataConfig(
            storage=StorageConfig(cache_dir=tmp_path / "derived"),
            history=HistoryConfig(enabled=True, base_timeframe="1m"),
        )
    ).fetch_bars(query)
    official = binance_get_bars_sync(
        "BTCUSDT", "1D", start, end, MarketDataConfig().binance, market="spot"
    )

    assert derived.coverage.is_complete
    assert len(derived.bars) == len(official) == 2
    for left, right in zip(derived.bars, official, strict=True):
        assert (left.time, left.open, left.high, left.low, left.close) == (
            right.time,
            right.open,
            right.high,
            right.low,
            right.close,
        )
        assert left.closed is True


def test_real_binance_btcusdt_15m_archive_rest_coverage_has_no_gaps(tmp_path):
    start = _ms(2024, 1, 1)
    end = _ms(2024, 1, 2)
    query = BarQuery(
        InstrumentKey("binance", "spot", "BTCUSDT"),
        parse_timeframe("15m"),
        start,
        end,
        gap_policy="fail",
    )

    series = MarketDataService(
        MarketDataConfig(storage=StorageConfig(cache_dir=tmp_path))
    ).fetch_bars(query)

    assert series.coverage.is_complete
    assert len(series.bars) == (end - start) // 900_000
    assert all(bar.closed for bar in series.bars)
    assert [bar.time for bar in series.bars] == list(range(start, end, 900_000))


def test_public_create_provider_binance_spot_solusdt_1m(tmp_path):
    end = (int(datetime.now(UTC).timestamp() * 1000) // 60_000 - 1) * 60_000
    start = end - 3 * 60_000
    query = BarQuery(
        InstrumentKey("binance", "spot", "SOLUSDT"),
        parse_timeframe("1m"),
        start,
        end,
        source="provider",
        gap_policy="fail",
    )
    provider = create_provider(
        MarketDataConfig(
            history=HistoryConfig(enabled=False, archive_first=False),
            storage=StorageConfig(cache_dir=tmp_path),
            artifact_identity=ArtifactIdentityConfig(
                producer_commit="1" * 40,
                stack_id="sha256:" + "2" * 64,
            ),
        )
    )

    snapshot = provider.fetch_bars(query)

    assert snapshot["bar_count"] == 3
    assert snapshot["snapshot_envelope"]["schema_id"] == "openpine.marketdata.v2"
    assert snapshot["query"]["instrument_id"] == "binance:spot:SOLUSDT"
    assert snapshot["query"]["timeframe"] == "1m"
    assert [bar["open_time_utc_ms"] for bar in snapshot["bars"]] == list(
        range(start, end, 60_000)
    )
    assert all(bar["provider"] == "binance" for bar in snapshot["bars"])
    assert all(bar["provider_revision"]["known"] is True for bar in snapshot["bars"])
