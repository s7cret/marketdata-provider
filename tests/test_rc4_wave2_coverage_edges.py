from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest
from openpine_contracts import Finality, RevisionState

from marketdata_provider.canonical.bar import build_data_snapshot
from marketdata_provider.canonical.provider import (
    ProviderRawBar,
    _decimal_from_legacy_number,
    build_public_snapshot,
    raw_bar_from_market_bar,
    snapshot_from_market_bars,
)
from marketdata_provider.canonical.source_identity import bind_source_identity
from marketdata_provider.compat.v4 import create_legacy_provider
from marketdata_provider.config import (
    HistoryConfig,
    MarketDataConfig,
    OfflineDataConfig,
    StorageConfig,
)
from marketdata_provider.contracts import (
    BarQuery,
    CoverageValidationError,
    InstrumentKey,
    parse_timeframe,
)
from marketdata_provider.core.bar import Bar, MarketBar
from marketdata_provider.errors import (
    MDMissingFinality,
    MDUnsupportedFeature,
    MDValidationError,
)
from marketdata_provider.exchanges.binance.archive import _merge_same_open_time
from marketdata_provider.factories import (
    _CandleStoreAdapter,
    _LiveKlineClientAdapter,
    _OfflineProviderAdapter,
    _snapshot_source_identity,
    _validate_canonical_snapshot,
    create_provider,
)
from marketdata_provider.service import MarketDataService, _aggregate_bucket
from marketdata_provider.store.candle_store import CandleStore as SegmentCandleStore
from marketdata_provider.store.current_store import CurrentStore
from marketdata_provider.store.segment_checksums import (
    PROVENANCE_CANONICAL_CHECKSUM,
    PROVENANCE_TAIL_CHAIN_CHECKSUM,
    bars_checksum,
    extend_tail_chain,
    provenance_bars_checksum,
    validate_csv_checksum,
)
from marketdata_provider.store.segment_read import _parquet_checksum
from marketdata_provider.store.segment_rows import row_to_bar


def _query(*, end: int = 60_000) -> BarQuery:
    return BarQuery(
        InstrumentKey("binance", "spot", "BTCUSDT"),
        parse_timeframe("1m"),
        0,
        end,
    )


def _bar(**changes: object) -> MarketBar:
    payload: dict[str, object] = {
        "time": 0,
        "time_close": 59_999,
        "open": 1.0,
        "high": 2.0,
        "low": 0.5,
        "close": 1.5,
        "volume": 10.0,
        "exchange": "binance",
        "market": "spot",
        "symbol": "BTCUSDT",
        "timeframe": "1m",
        "is_closed": True,
        "provider": "binance",
        "provider_revision": "fixture-v1",
    }
    payload.update(changes)
    return MarketBar(**payload)  # type: ignore[arg-type]


def test_canonical_provider_fail_closed_edges() -> None:
    assert _decimal_from_legacy_number("1.25") == "1.25"
    bar = _bar()

    object.__setattr__(bar, "is_closed", None)
    with pytest.raises(MDMissingFinality):
        raw_bar_from_market_bar(
            bar,
            instrument_id="binance/spot/BTCUSDT",
            timeframe="1m",
            provider="binance",
        )

    missing_close = _bar()
    object.__setattr__(missing_close, "time_close", None)
    with pytest.raises(MDValidationError, match="close_time"):
        raw_bar_from_market_bar(
            missing_close,
            instrument_id="binance/spot/BTCUSDT",
            timeframe="1m",
            provider="binance",
        )

    no_provider = _bar(provider="")
    with pytest.raises(MDValidationError, match="provider is required"):
        raw_bar_from_market_bar(
            no_provider,
            instrument_id="binance/spot/BTCUSDT",
            timeframe="1m",
            provider="",
        )
    no_revision = _bar(provider_revision=None)
    with pytest.raises(MDValidationError, match="provider_revision"):
        raw_bar_from_market_bar(
            no_revision,
            instrument_id="binance/spot/BTCUSDT",
            timeframe="1m",
            provider="binance",
        )
    with pytest.raises(MDValidationError, match="provider_revision"):
        build_public_snapshot(_query(), [], provider_revision="")


def test_source_identity_and_market_bar_validation_edges() -> None:
    query = _query()
    with pytest.raises(MDValidationError, match="provider identity"):
        bind_source_identity(
            [_bar(provider="bybit")],
            query=query,
            provider="binance",
            source_transport="rest",
        )
    with pytest.raises(MDValidationError, match="provider_revision"):
        bind_source_identity(
            [_bar(), _bar(time=60_000, time_close=119_999, provider_revision=None)],
            query=_query(end=120_000),
            provider="binance",
            source_transport="rest",
        )

    metadata_bar = _bar(
        provider="",
        provider_revision=None,
        metadata={"provider": "binance", "provider_revision": "metadata-v1"},
    )
    assert metadata_bar.provider == "binance"
    assert metadata_bar.provider_revision == "metadata-v1"
    with pytest.raises(MDValidationError, match="ORIGINAL"):
        _bar(revision=1)
    with pytest.raises(MDValidationError, match="corrected/revoked"):
        _bar(revision_state=RevisionState.CORRECTED, revision=0)


def test_factory_offline_and_snapshot_identity_edges(tmp_path: Path) -> None:
    query = _query()
    with pytest.raises(MDValidationError, match="provider identity"):
        _snapshot_source_identity(
            query, [_bar(provider="bybit")], default_provider="binance"
        )
    with pytest.raises(MDValidationError, match="partial provider_revision"):
        _snapshot_source_identity(
            query,
            [_bar(), _bar(time=60_000, time_close=119_999, provider_revision=None)],
            default_provider="binance",
        )

    first_order = [
        _bar(provider_revision="r1"),
        _bar(time=60_000, time_close=119_999, provider_revision="r2"),
    ]
    swapped_order = [
        _bar(provider_revision="r2"),
        _bar(time=60_000, time_close=119_999, provider_revision="r1"),
    ]
    aggregate_a = _snapshot_source_identity(
        _query(end=120_000), first_order, default_provider="binance"
    )[1]
    aggregate_b = _snapshot_source_identity(
        _query(end=120_000), swapped_order, default_provider="binance"
    )[1]
    assert aggregate_a != aggregate_b
    snapshot = snapshot_from_market_bars(
        _query(end=120_000),
        first_order,
        provider="binance",
        provider_revision=aggregate_a,
    )
    assert [bar["provider_revision"] for bar in snapshot["bars"]] == ["r1", "r2"]

    with pytest.raises(MDUnsupportedFeature, match="Canonical v2 finality"):
        create_provider(MarketDataConfig(default_exchange="okx")).fetch_bars(query)

    unsupported = tmp_path / "bars.parquet"
    unsupported.write_bytes(b"not parquet")
    with pytest.raises(MDUnsupportedFeature, match="requires CSV"):
        _OfflineProviderAdapter(unsupported).fetch_bars(query)

    invalid_finality = tmp_path / "invalid.csv"
    invalid_finality.write_text(
        "time,open,high,low,close,volume,time_close,finality,provider_revision\n"
        "0,1,2,0.5,1.5,1,59999,MAYBE,fixture-v1\n"
    )
    with pytest.raises(MDValidationError, match="finality is invalid"):
        _OfflineProviderAdapter(invalid_finality).fetch_bars(query)

    missing_revision = tmp_path / "missing-revision.csv"
    missing_revision.write_text(
        "time,open,high,low,close,volume,time_close,finality\n"
        "0,1,2,0.5,1.5,1,59999,FINAL\n"
    )
    with pytest.raises(MDValidationError, match="provider_revision"):
        _OfflineProviderAdapter(missing_revision).fetch_bars(query)

    required_columns = (
        (
            "provider",
            "time_close,finality,provider,provider_revision,revision_state,revision",
            "59999,FINAL,,r1,ORIGINAL,0",
        ),
        (
            "time_close",
            "time_close,finality,provider,provider_revision,revision_state,revision",
            ",FINAL,offline,r1,ORIGINAL,0",
        ),
        (
            "revision_state",
            "time_close,finality,provider,provider_revision,revision_state,revision",
            "59999,FINAL,offline,r1,,0",
        ),
        (
            "revision",
            "time_close,finality,provider,provider_revision,revision_state,revision",
            "59999,FINAL,offline,r1,ORIGINAL,",
        ),
    )
    for name, suffix_header, suffix_row in required_columns:
        path = tmp_path / f"missing-{name}.csv"
        path.write_text(
            f"time,open,high,low,close,volume,{suffix_header}\n"
            f"0,1,2,0.5,1.5,1,{suffix_row}\n"
        )
        with pytest.raises(MDValidationError, match=name):
            _OfflineProviderAdapter(path).fetch_bars(query)

    malformed_revision = tmp_path / "malformed-revision.csv"
    malformed_revision.write_text(
        "time,open,high,low,close,volume,time_close,finality,provider,provider_revision,revision_state,revision\n"
        "0,1,2,0.5,1.5,1,59999,FINAL,offline,r1,BROKEN,nope\n"
    )
    with pytest.raises(MDValidationError, match="revision identity"):
        _OfflineProviderAdapter(malformed_revision).fetch_bars(query)

    mixed = tmp_path / "mixed.csv"
    mixed.write_text(
        "time,open,high,low,close,volume,time_close,finality,provider,provider_revision,revision_state,revision\n"
        "0,1,2,0.5,1.5,1,59999,FINAL,offline,r1,ORIGINAL,0\n"
        "60000,1,2,0.5,1.5,1,119999,FINAL,offline,r2,ORIGINAL,0\n"
    )
    with pytest.raises(MDValidationError, match="one provider_revision"):
        _OfflineProviderAdapter(mixed).fetch_bars(_query(end=120_000))

    legacy = tmp_path / "legacy.csv"
    legacy.write_text(
        "time,open,high,low,close,volume,time_close\n" "0,1,2,0.5,1.5,1,59999\n"
    )
    provider = create_legacy_provider(
        MarketDataConfig(offline=OfflineDataConfig(root=legacy))
    )
    assert provider.fetch_bars(query).bars[0].close == 1.5


def test_service_open_tail_and_aggregate_provider_edges(tmp_path: Path) -> None:
    service = MarketDataService(
        MarketDataConfig(
            history=HistoryConfig(enabled=False, archive_first=False),
            storage=StorageConfig(cache_dir=tmp_path),
        )
    )
    open_bar = _bar(is_closed=False)
    service._append_stream(_query(), [open_bar])
    assert (
        service.store.get_current_market_candle(
            exchange="binance", market="spot", symbol="BTCUSDT", timeframe="1m"
        )
        is not None
    )

    with pytest.raises(MDValidationError, match="one provider"):
        _aggregate_bucket(0, [_bar(provider="")], query=_query())
    assert (
        _aggregate_bucket(0, [_bar(is_closed=False)], query=_query()).is_closed is False
    )


def _create_legacy_current_table(path: Path) -> None:
    with sqlite3.connect(path) as db:
        db.execute(
            "CREATE TABLE current_candles ("
            "exchange TEXT NOT NULL, market TEXT NOT NULL, symbol TEXT NOT NULL, "
            "source_transport TEXT NOT NULL, source_kind TEXT NOT NULL, timeframe TEXT NOT NULL, "
            "open_time INTEGER NOT NULL, close_time INTEGER NOT NULL, open REAL NOT NULL, "
            "high REAL NOT NULL, low REAL NOT NULL, close REAL NOT NULL, volume REAL NOT NULL, "
            "quote_volume REAL, turnover REAL, trades_count INTEGER, taker_buy_base_volume REAL, "
            "taker_buy_quote_volume REAL, is_closed INTEGER NOT NULL DEFAULT 0, event_time INTEGER, "
            "received_at INTEGER NOT NULL, raw_event_id TEXT, "
            "PRIMARY KEY(exchange, market, symbol, source_transport, source_kind, timeframe, open_time))"
        )
        db.execute(
            "INSERT INTO current_candles VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "binance",
                "spot",
                "BTCUSDT",
                "ws",
                "trade_kline",
                "1m",
                0,
                59_999,
                1.0,
                2.0,
                0.5,
                1.5,
                10.0,
                None,
                None,
                None,
                None,
                None,
                0,
                1,
                1,
                "evt-1",
            ),
        )


def test_current_store_migration_and_identity_edges(tmp_path: Path) -> None:
    db_path = tmp_path / "current.sqlite"
    _create_legacy_current_table(db_path)
    store = CurrentStore(db_path)
    with sqlite3.connect(db_path) as db:
        columns = {row[1] for row in db.execute("PRAGMA table_info(current_candles)")}
    assert {"provider", "provider_revision", "revision_state", "revision"} <= columns

    with pytest.raises(ValueError, match="provider and provider_revision"):
        store.upsert_current(_bar(provider="", provider_revision=None))
    missing_close = _bar()
    object.__setattr__(missing_close, "time_close", None)
    with pytest.raises(ValueError, match="close time"):
        store.upsert_current(missing_close)
    with pytest.raises(ValueError, match="provider identity"):
        store.get_current(
            exchange="binance", market="spot", symbol="BTCUSDT", timeframe="1m"
        )

    with sqlite3.connect(db_path) as db:
        db.execute(
            "UPDATE current_candles SET provider=?, provider_revision=?",
            ("binance", "fixture-v1"),
        )
    with pytest.raises(ValueError, match="revision identity"):
        store.get_current(
            exchange="binance", market="spot", symbol="BTCUSDT", timeframe="1m"
        )


@pytest.mark.parametrize(
    ("removed", "match"),
    [
        ("provider", "missing provider"),
        ("time_close", "missing time_close"),
        ("revision_state", "missing revision_state"),
        ("revision", "missing revision"),
    ],
)
def test_segment_row_missing_identity_edges(removed: str, match: str) -> None:
    row: dict[str, object] = {
        "time": "0",
        "open": "1",
        "high": "2",
        "low": "0.5",
        "close": "1.5",
        "volume": "1",
        "time_close": "59999",
        "is_closed": "true",
        "provider": "binance",
        "provider_revision": "fixture-v1",
        "revision_state": "ORIGINAL",
        "revision": "0",
    }
    row.pop(removed)
    with pytest.raises(MDValidationError, match=match):
        row_to_bar(row)


def test_segment_row_invalid_revision_state() -> None:
    row: dict[str, object] = {
        "time": "0",
        "open": "1",
        "high": "2",
        "low": "0.5",
        "close": "1.5",
        "volume": "1",
        "time_close": "59999",
        "is_closed": "true",
        "provider": "binance",
        "provider_revision": "fixture-v1",
        "revision_state": "BROKEN",
        "revision": "0",
    }
    with pytest.raises(MDValidationError, match="invalid persisted revision_state"):
        row_to_bar(row)


def test_snapshot_and_canonical_store_validation_edges(tmp_path: Path) -> None:
    query = _query()
    valid = build_public_snapshot(
        query,
        [
            ProviderRawBar(
                instrument_id=query.instrument.serialize(),
                timeframe="1m",
                open_time_utc_ms=0,
                close_time_utc_ms=59_999,
                open="1",
                high="2",
                low="0.5",
                close="1.5",
                volume="1",
                finality=Finality.FINAL,
                provider="binance",
                provider_revision="fixture-v1",
            )
        ],
        provider_revision="fixture-v1",
    )
    assert _validate_canonical_snapshot(valid)["series_hash"] == valid["series_hash"]

    with pytest.raises(MDValidationError, match="query/bars"):
        _validate_canonical_snapshot({})
    bad_created = dict(valid, created_at_utc_ms=True)
    with pytest.raises(MDValidationError, match="created_at"):
        _validate_canonical_snapshot(bad_created)
    bad_series = dict(valid, series_hash="sha256:" + "0" * 64)
    with pytest.raises(MDValidationError, match="series_hash"):
        _validate_canonical_snapshot(bad_series)

    foreign = dict(valid["bars"][0], snapshot_id="foreign")
    with pytest.raises(MDValidationError, match="snapshot_id"):
        build_data_snapshot(
            snapshot_id=str(valid["snapshot_id"]),
            instrument_id=query.instrument.serialize(),
            timeframe="1m",
            provider_revision="fixture-v1",
            start_utc_ms=0,
            end_utc_ms=60_000,
            bars=[foreign],
        )

    legacy_store = SegmentCandleStore(tmp_path / "legacy")
    legacy_adapter = _CandleStoreAdapter(legacy_store)
    legacy_store.segments.replace_all(
        [_bar(exchange="bybit", market="linear", provider="bybit")],
        exchange="binance",
        market="spot",
        symbol="BTCUSDT",
        timeframe="1m",
    )
    with pytest.raises(CoverageValidationError, match="instrument does not match"):
        legacy_adapter.read(query)


def test_v3_checksum_compatibility_and_legacy_archive_merge(tmp_path: Path) -> None:
    first = _bar(open_text="1.000000000000000001")
    second = _bar(time=60_000, time_close=119_999, open_text="2.0")
    v3 = provenance_bars_checksum([first, second])
    assert v3 != bars_checksum([first, second])
    assert extend_tail_chain(v3, second, algorithm=PROVENANCE_TAIL_CHAIN_CHECKSUM)

    store = SegmentCandleStore(tmp_path).segments
    store.replace_all(
        [first, second],
        exchange="binance",
        market="spot",
        symbol="BTCUSDT",
        timeframe="1m",
    )
    data_path = next(tmp_path.rglob("bars.csv"))
    manifest_path = next(tmp_path.rglob("manifest.json"))
    manifest = json.loads(manifest_path.read_text())
    manifest.update(
        checksum=v3,
        checksum_algorithm=PROVENANCE_CANONICAL_CHECKSUM,
        base_checksum=None,
        base_rows_count=None,
    )
    validate_csv_checksum(data_path, manifest)
    assert (
        _parquet_checksum(
            [first, second],
            {"checksum_algorithm": PROVENANCE_CANONICAL_CHECKSUM},
        )
        == v3
    )
    assert (
        _parquet_checksum(
            [first, second],
            {"checksum_algorithm": PROVENANCE_TAIL_CHAIN_CHECKSUM},
        )
        == v3
    )

    merged = _merge_same_open_time(
        Bar(0, 1.0, 2.0, 0.5, 1.5, 1.0, 59_999),
        Bar(0, 1.0, 3.0, 0.25, 2.0, 2.0, 59_999),
    )
    assert merged.high == 3.0 and merged.volume == 3.0


@pytest.mark.asyncio
async def test_live_adapter_rejects_missing_close_and_revision() -> None:
    class Update:
        exchange = "binance"
        market = "spot"
        symbol = "BTCUSDT"
        timeframe = "1m"
        event_time = 1
        received_at = 2

        def __init__(self, bar: MarketBar):
            self.bar = bar

        def to_market_bar(self) -> MarketBar:
            return self.bar

    class Raw:
        def __init__(self, bar: MarketBar):
            self.bar = bar

        async def events(self, **_kwargs: object):
            yield type(
                "Event",
                (),
                {"update": Update(self.bar), "raw_payload": {}, "diagnostic": None},
            )()

    missing_close = _bar()
    object.__setattr__(missing_close, "time_close", None)
    adapter = _LiveKlineClientAdapter(
        Raw(missing_close), instrument=_query().instrument, timeframe=_query().timeframe
    )
    with pytest.raises(MDValidationError, match="close time"):
        _ = [event async for event in adapter.events()]

    missing_revision = _bar(provider_revision=None)
    adapter = _LiveKlineClientAdapter(
        Raw(missing_revision),
        instrument=_query().instrument,
        timeframe=_query().timeframe,
    )
    with pytest.raises(MDValidationError, match="provider_revision"):
        _ = [event async for event in adapter.events()]
