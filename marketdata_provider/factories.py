from __future__ import annotations

import csv
from collections.abc import AsyncIterator, Mapping
from pathlib import Path
from typing import Any

from openpine_contracts import Finality, RevisionState
from openpine_contracts.hashing import content_hash

from marketdata_provider._adapters import (
    contract_to_market_bar,
    series_from_core_bars,
    series_from_market_bars,
)
from marketdata_provider.canonical.bar import DataSnapshotV2, build_data_snapshot
from marketdata_provider.canonical.provider import (
    ProviderRawBar,
    build_public_snapshot,
    snapshot_from_market_bars,
)
from marketdata_provider.config import MarketDataConfig
from marketdata_provider.contracts.errors import CoverageValidationError
from marketdata_provider.contracts.events import LiveKlineEvent
from marketdata_provider.contracts.footprint import FootprintQuery, FootprintSeries
from marketdata_provider.contracts.instrument import InstrumentKey
from marketdata_provider.contracts.protocols import CandleStore as CandleStoreProtocol
from marketdata_provider.contracts.protocols import (
    FootprintProvider as FootprintProviderProtocol,
)
from marketdata_provider.contracts.protocols import (
    LiveKlineClient as LiveKlineClientProtocol,
)
from marketdata_provider.contracts.protocols import (
    MarketDataProvider as MarketDataProviderProtocol,
)
from marketdata_provider.contracts.query import BarQuery
from marketdata_provider.contracts.series import BarSeries, CoverageReport, StoreResult
from marketdata_provider.contracts.timeframe import Timeframe, parse_timeframe
from marketdata_provider.core.bar import MarketBar
from marketdata_provider.errors import (
    MDMissingFinality,
    MDUnsupportedFeature,
    MDValidationError,
)
from marketdata_provider.exchanges.registry import list_exchanges
from marketdata_provider.footprint.service import FootprintService
from marketdata_provider.providers.offline import OfflineDataProvider
from marketdata_provider.service import MarketDataService
from marketdata_provider.store.candle_store import CandleStore as SegmentCandleStore
from marketdata_provider.store.segment_checksums import same_canonical_candle

_NATIVE_EXCHANGE_IDS = {exchange.id for exchange in list_exchanges(native_only=True)}
_CANONICAL_V2_EXCHANGE_IDS = {"binance", "bybit"}


def create_provider(config: MarketDataConfig) -> MarketDataProviderProtocol:
    """Create a canonical market-data provider from local package config."""

    if config.offline.root is not None:
        return _OfflineProviderAdapter(config.offline.root)
    return _ExchangeProviderAdapter(config)


def _create_legacy_provider(config: MarketDataConfig):
    """Build the explicitly requested pre-v5 BarSeries compatibility adapter."""

    if config.offline.root is not None:
        return _LegacyOfflineProviderAdapter(config.offline.root)
    return _LegacyExchangeProviderAdapter(config)


def create_footprint_provider(config: MarketDataConfig) -> FootprintProviderProtocol:
    """Create the raw-trade footprint provider."""

    return _FootprintProviderAdapter(config)


def create_candle_store(config: MarketDataConfig) -> CandleStoreProtocol:
    """Create a canonical candle store from local package config."""

    return _CanonicalCandleStoreAdapter(SegmentCandleStore(config.storage.cache_dir))


def _create_legacy_candle_store(config: MarketDataConfig):
    """Build the explicit pre-v5 BarSeries candle-store adapter."""

    return _CandleStoreAdapter(SegmentCandleStore(config.storage.cache_dir))


def create_live_kline_client(
    config: MarketDataConfig,
    *,
    instrument: InstrumentKey,
    timeframe: Timeframe,
) -> LiveKlineClientProtocol:
    """Create a canonical public kline stream client.

    Consumers depend on this factory/protocol instead of importing streaming
    implementation modules directly.
    """

    from marketdata_provider.streaming import PublicKlineWebSocketClient

    exchange = (config.default_exchange or instrument.exchange).lower()
    market = (config.default_market or instrument.market).lower()
    raw_client = PublicKlineWebSocketClient(
        exchange=exchange,  # type: ignore[arg-type]
        market=market,
        symbol=instrument.symbol,
        timeframe=timeframe.canonical,
    )
    return _LiveKlineClientAdapter(
        raw_client, instrument=instrument, timeframe=timeframe
    )


class _ExchangeProviderAdapter:
    # MarketDataService atomically persists every provider fetch before returning.
    persists_fetches = True

    def __init__(self, config: MarketDataConfig):
        self.config = config
        self.service = MarketDataService(config)

    def fetch_bars(self, query: BarQuery, progress_callback=None) -> DataSnapshotV2:
        exchange = (self.config.default_exchange or query.instrument.exchange).lower()
        if exchange not in _NATIVE_EXCHANGE_IDS:
            raise MDUnsupportedFeature(f"Unsupported provider exchange: {exchange}")
        if exchange not in _CANONICAL_V2_EXCHANGE_IDS:
            raise MDUnsupportedFeature(
                f"Canonical v2 finality is unavailable for exchange: {exchange}"
            )
        self.service.fetch_bars(query, progress_callback=progress_callback)
        bars = self.service._stored_bars(query)
        current = self.service.store.get_current_market_candle(
            exchange=query.instrument.exchange,
            market=query.instrument.market,
            symbol=query.instrument.symbol,
            timeframe=query.timeframe.canonical,
        )
        if current is not None and query.start_ms <= current.time < query.end_ms:
            bars = [*bars, current]
            bars.sort(key=lambda item: (item.time, item.revision))
        provider, provider_revision = _snapshot_source_identity(
            query, bars, default_provider=exchange
        )
        return snapshot_from_market_bars(
            query,
            bars,
            provider=provider,
            provider_revision=provider_revision,
        )


def _snapshot_source_identity(
    query: BarQuery,
    bars: list[MarketBar],
    *,
    default_provider: str,
) -> tuple[str, str]:
    providers = {bar.provider for bar in bars if bar.provider}
    if len(providers) > 1 or (providers and providers != {default_provider}):
        raise MDValidationError("stored bars disagree on provider identity")
    provider = next(iter(providers), default_provider)
    revisions = {
        bar.provider_revision for bar in bars if bar.provider_revision is not None
    }
    if len(revisions) == 1 and all(bar.provider_revision is not None for bar in bars):
        return provider, next(iter(revisions))
    if revisions and any(bar.provider_revision is None for bar in bars):
        raise MDValidationError("stored bars have partial provider_revision identity")
    revision = content_hash(
        {
            "provider": provider,
            "instrument_id": query.instrument.serialize(),
            "timeframe": query.timeframe.canonical,
            "start_ms": query.start_ms,
            "end_ms": query.end_ms,
            "source_revisions": sorted(revisions),
            "bars": [
                {
                    "provider_revision": bar.provider_revision,
                    "time": bar.time,
                    "time_close": bar.time_close,
                    "open": bar.open_text or str(bar.open),
                    "high": bar.high_text or str(bar.high),
                    "low": bar.low_text or str(bar.low),
                    "close": bar.close_text or str(bar.close),
                    "volume": bar.volume_text or str(bar.volume),
                    "is_closed": bar.is_closed,
                }
                for bar in bars
            ],
        },
        schema_id="marketdata-provider.snapshot-source.v1",
    )
    return provider, revision


class _OfflineProviderAdapter:
    def __init__(self, root: str | Path):
        self.provider = OfflineDataProvider(root)

    def fetch_bars(self, query: BarQuery) -> DataSnapshotV2:
        if self.provider.path.suffix.lower() != ".csv":
            raise MDUnsupportedFeature(
                "canonical offline boundary currently requires CSV"
            )
        with self.provider.path.open(newline="") as handle:
            rows = list(csv.DictReader(handle))
        raw_bars: list[ProviderRawBar] = []
        revisions: set[str] = set()
        for row in rows:
            open_time = int(str(row.get("time") or row.get("open_time") or 0))
            if not (query.start_ms <= open_time < query.end_ms):
                continue
            finality_raw = row.get("finality")
            if finality_raw in (None, ""):
                raise MDMissingFinality("offline row finality is required")
            try:
                finality = Finality(str(finality_raw))
            except ValueError as exc:
                raise MDValidationError("offline row finality is invalid") from exc
            provider_revision = str(row.get("provider_revision") or "")
            if not provider_revision:
                raise MDValidationError("offline row provider_revision is required")
            provider = str(row.get("provider") or "")
            if not provider:
                raise MDValidationError("offline row provider is required")
            close_time_raw = row.get("time_close")
            if close_time_raw in (None, ""):
                raise MDValidationError("offline row time_close is required")
            revision_state_raw = row.get("revision_state")
            if revision_state_raw in (None, ""):
                raise MDValidationError("offline row revision_state is required")
            revision_raw = row.get("revision")
            if revision_raw in (None, ""):
                raise MDValidationError("offline row revision is required")
            try:
                revision_state = RevisionState(str(revision_state_raw))
                revision = int(str(revision_raw))
            except ValueError as exc:
                raise MDValidationError(
                    "offline row revision identity is invalid"
                ) from exc
            revisions.add(provider_revision)
            raw_bars.append(
                ProviderRawBar(
                    instrument_id=query.instrument.serialize(),
                    timeframe=query.timeframe.canonical,
                    open_time_utc_ms=open_time,
                    close_time_utc_ms=int(str(close_time_raw)),
                    open=row["open"],
                    high=row["high"],
                    low=row["low"],
                    close=row["close"],
                    volume=row.get("volume") or "0",
                    finality=finality,
                    provider=provider,
                    provider_revision=provider_revision,
                    revision_state=revision_state,
                    revision=revision,
                )
            )
        if len(revisions) != 1:
            raise MDValidationError("offline query requires one provider_revision")
        return build_public_snapshot(
            query, raw_bars, provider_revision=next(iter(revisions))
        )


class _LegacyExchangeProviderAdapter:
    def __init__(self, config: MarketDataConfig):
        self.service = MarketDataService(config)

    def fetch_bars(self, query: BarQuery, progress_callback=None) -> BarSeries:
        return self.service.fetch_bars(query, progress_callback=progress_callback)


class _LegacyOfflineProviderAdapter:
    def __init__(self, root: str | Path):
        self.provider = OfflineDataProvider(root)

    def fetch_bars(self, query: BarQuery) -> BarSeries:
        bars = self.provider.get_bars(
            query.instrument.symbol,
            query.timeframe.canonical,
            query.start_ms,
            query.end_ms,
        )
        return series_from_core_bars(query, bars, source="provider")


class _FootprintProviderAdapter:
    def __init__(self, config: MarketDataConfig):
        self.service = FootprintService(config)

    def fetch_footprint(self, query: FootprintQuery) -> FootprintSeries:
        return self.service.fetch_footprint(query)


class _CanonicalCandleStoreAdapter:
    def __init__(self, store: SegmentCandleStore):
        self.store = store

    def read(self, query: BarQuery) -> DataSnapshotV2:
        bars = self.store.get_market_bars(
            exchange=query.instrument.exchange,
            market=query.instrument.market,
            symbol=query.instrument.symbol,
            timeframe=query.timeframe.canonical,
            start=query.start_ms,
            end=query.end_ms,
        )
        error = _stored_bars_read_error(query, tuple(bars))
        if error is not None:
            raise CoverageValidationError(error)
        provider, provider_revision = _snapshot_source_identity(
            query, bars, default_provider=query.instrument.exchange
        )
        return snapshot_from_market_bars(
            query,
            bars,
            provider=provider,
            provider_revision=provider_revision,
        )

    def write(self, snapshot: DataSnapshotV2) -> StoreResult:
        rows_written = 0
        try:
            validated = _validate_canonical_snapshot(snapshot)
            for raw_bar in validated["bars"]:
                market_bar = _market_bar_from_canonical(raw_bar)
                result = (
                    self.store.commit_closed(market_bar)
                    if market_bar.is_closed
                    else self.store.upsert_open(market_bar)
                )
                if result.status in {"committed", "upserted"}:
                    rows_written += 1
        except Exception as exc:
            return StoreResult(success=False, rows_written=rows_written, error=str(exc))
        return StoreResult(success=True, rows_written=rows_written)

    def coverage(self, query: BarQuery) -> Mapping[str, Any]:
        return self.read(query)["coverage"]

    def latest_bar_time(self, query: BarQuery) -> int | None:
        return self.store.latest_bar_time(
            exchange=query.instrument.exchange,
            market=query.instrument.market,
            symbol=query.instrument.symbol,
            timeframe=query.timeframe.canonical,
        )


def _validate_canonical_snapshot(snapshot: Mapping[str, Any]) -> DataSnapshotV2:
    query = snapshot.get("query")
    bars = snapshot.get("bars")
    if not isinstance(query, Mapping) or not isinstance(bars, list):
        raise MDValidationError("canonical snapshot query/bars are required")
    created_at = snapshot.get("created_at_utc_ms")
    if isinstance(created_at, bool) or not isinstance(created_at, int):
        raise MDValidationError("canonical snapshot created_at_utc_ms is required")
    validated = build_data_snapshot(
        snapshot_id=str(snapshot["snapshot_id"]),
        instrument_id=str(query["instrument_id"]),
        timeframe=str(query["timeframe"]),
        provider_revision=str(query["provider_revision"]),
        start_utc_ms=int(query["start_utc_ms"]),
        end_utc_ms=int(query["end_utc_ms"]),
        bars=bars,
        finality_policy=str(query["finality_policy"]),
        clock=lambda: created_at,
    )
    if validated["series_hash"] != snapshot.get("series_hash"):
        raise MDValidationError("canonical snapshot series_hash verification failed")
    return validated


def _market_bar_from_canonical(bar: Mapping[str, Any]) -> MarketBar:
    instrument = InstrumentKey.parse(str(bar["instrument_id"]))
    finality = Finality(str(bar["finality"]))
    revision_state = RevisionState(str(bar["revision_state"]))
    return MarketBar(
        time=int(bar["open_time_utc_ms"]),
        time_close=int(bar["close_time_utc_ms"]),
        open=float(str(bar["open"])),
        high=float(str(bar["high"])),
        low=float(str(bar["low"])),
        close=float(str(bar["close"])),
        volume=float(str(bar["volume"])),
        exchange=instrument.exchange,
        market=instrument.market,
        symbol=instrument.symbol,
        timeframe=str(bar["timeframe"]),
        source_transport="canonical",
        is_closed=finality is Finality.FINAL,
        provider=str(bar["provider"]),
        provider_revision=str(bar["provider_revision"]),
        revision_state=revision_state,
        revision=int(bar["revision"]),
        open_text=str(bar["open"]),
        high_text=str(bar["high"]),
        low_text=str(bar["low"]),
        close_text=str(bar["close"]),
        volume_text=str(bar["volume"]),
    )


class _CandleStoreAdapter:
    def __init__(self, store: SegmentCandleStore):
        self.store = store

    def read(self, query: BarQuery) -> BarSeries:
        bars = tuple(
            self.store.get_market_bars(
                exchange=query.instrument.exchange,
                market=query.instrument.market,
                symbol=query.instrument.symbol,
                timeframe=query.timeframe.canonical,
                start=query.start_ms,
                end=query.end_ms,
            )
        )
        error = _stored_bars_read_error(query, bars)
        if error is not None:
            raise CoverageValidationError(error)
        return series_from_market_bars(query, bars, source="storage")

    def write(self, series: BarSeries) -> StoreResult:
        rows_written = 0
        try:
            error = _series_write_error(series)
            if error is not None:
                return StoreResult(success=False, rows_written=0, error=error)
            market_bars = [contract_to_market_bar(bar) for bar in series.bars]
            if _can_bulk_write_closed(market_bars):
                rows_written = self._bulk_write_closed(market_bars)
            else:
                for market_bar in market_bars:
                    if market_bar.is_closed:
                        result = self.store.commit_closed(market_bar)
                    else:
                        result = self.store.upsert_open(market_bar)
                    if result.status in {"committed", "upserted"}:
                        rows_written += 1
        except Exception as exc:
            return StoreResult(success=False, rows_written=rows_written, error=str(exc))
        return StoreResult(success=True, rows_written=rows_written)

    def coverage(self, query: BarQuery) -> CoverageReport:
        return self.read(query).coverage

    def latest_bar_time(self, query: BarQuery) -> int | None:
        if hasattr(self.store, "latest_bar_time"):
            return self.store.latest_bar_time(
                exchange=query.instrument.exchange,
                market=query.instrument.market,
                symbol=query.instrument.symbol,
                timeframe=query.timeframe.canonical,
            )
        coverage = self.coverage(query)
        return coverage.delivered_end_ms

    def _bulk_write_closed(self, bars: list[MarketBar]) -> int:
        first = bars[0]
        key = {
            "exchange": first.exchange,
            "market": first.market,
            "symbol": first.symbol,
            "timeframe": first.timeframe,
            "source_kind": first.source_kind,
        }
        with self.store.segments.series_writer_lock(**key):
            incoming = _normalize_closed_batch(bars)
            if not hasattr(self.store.segments, "manifest_for"):
                existing = self.store.segments.read_all(
                    exchange=first.exchange,
                    market=first.market,
                    symbol=first.symbol,
                    timeframe=first.timeframe,
                    source_kind=first.source_kind,
                )
                by_time = {bar.time: bar for bar in existing}
                rows_written = 0
                for bar in incoming:
                    current = by_time.get(bar.time)
                    if current is not None and not _same_candle_payload(current, bar):
                        raise ValueError(f"conflicting closed candle at {bar.time}")
                    if current is None:
                        rows_written += 1
                        by_time[bar.time] = bar
                self.store.segments._replace_all_locked(
                    [by_time[item] for item in sorted(by_time)],
                    exchange=first.exchange,
                    market=first.market,
                    symbol=first.symbol,
                    timeframe=first.timeframe,
                    source_kind=first.source_kind,
                )
                return rows_written
            manifest = self.store.segments.manifest_for(**key)
            if manifest is None or manifest.end_time is None:
                self.store.segments._replace_all_locked(
                    incoming,
                    exchange=first.exchange,
                    market=first.market,
                    symbol=first.symbol,
                    timeframe=first.timeframe,
                    source_kind=first.source_kind,
                )
                return len(incoming)

            overlap = [bar for bar in incoming if bar.time <= manifest.end_time]
            tail = [bar for bar in incoming if bar.time > manifest.end_time]
            requires_backfill = False
            if overlap:
                existing = self.store.segments._read_all_locked(
                    start=overlap[0].time,
                    end=overlap[-1].time + 1,
                    **key,
                )
                by_time = {bar.time: bar for bar in existing}
                for bar in overlap:
                    current = by_time.get(bar.time)
                    if current is None:
                        requires_backfill = True
                        continue
                    if not _same_candle_payload(current, bar):
                        raise ValueError(f"conflicting closed candle at {bar.time}")

            if not requires_backfill:
                if tail:
                    self.store.segments.append_strictly_newer(tail, **key)
                return len(tail)

            existing = self.store.segments._read_all_locked(
                exchange=first.exchange,
                market=first.market,
                symbol=first.symbol,
                timeframe=first.timeframe,
                source_kind=first.source_kind,
            )
            by_time = {bar.time: bar for bar in existing}
            rows_written = 0
            for bar in incoming:
                current = by_time.get(bar.time)
                if current is not None and not _same_candle_payload(current, bar):
                    raise ValueError(f"conflicting closed candle at {bar.time}")
                if current is None:
                    rows_written += 1
                    by_time[bar.time] = bar
            self.store.segments._replace_all_locked(
                [by_time[item] for item in sorted(by_time)],
                exchange=first.exchange,
                market=first.market,
                symbol=first.symbol,
                timeframe=first.timeframe,
                source_kind=first.source_kind,
            )
            return rows_written


def _same_candle_payload(left: MarketBar, right: MarketBar) -> bool:
    """Return true when the candle data is identical regardless of provenance."""

    return same_canonical_candle(left, right)


def _normalize_closed_batch(bars: list[MarketBar]) -> list[MarketBar]:
    by_time: dict[int, MarketBar] = {}
    for bar in sorted(bars, key=lambda item: item.time):
        current = by_time.get(bar.time)
        if current is not None and not _same_candle_payload(current, bar):
            raise ValueError(f"conflicting closed candle at {bar.time}")
        by_time.setdefault(bar.time, bar)
    return list(by_time.values())


def _series_write_error(series: BarSeries) -> str | None:
    """Reject series that would cross canonical store identity boundaries."""

    for bar in series.bars:
        if bar.instrument != series.query.instrument:
            return (
                "bar instrument does not match series query "
                f"({bar.instrument.serialize()} != {series.query.instrument.serialize()})"
            )
        if bar.timeframe != series.query.timeframe:
            return (
                "bar timeframe does not match series query "
                f"({bar.timeframe.canonical} != {series.query.timeframe.canonical})"
            )
    return None


def _can_bulk_write_closed(bars: list[MarketBar]) -> bool:
    if not bars or not all(bar.is_closed for bar in bars):
        return False
    first = bars[0]
    return all(
        bar.exchange == first.exchange
        and bar.market == first.market
        and bar.symbol == first.symbol
        and bar.timeframe == first.timeframe
        and bar.source_kind == first.source_kind
        for bar in bars
    )


def _stored_bars_read_error(query: BarQuery, bars: tuple[MarketBar, ...]) -> str | None:
    """Reject stored rows whose embedded identity disagrees with the read query."""

    for bar in bars:
        try:
            instrument = InstrumentKey(bar.exchange, bar.market, bar.symbol)
        except ValueError as exc:
            return f"stored bar instrument is invalid ({exc})"
        if instrument != query.instrument:
            return (
                "stored bar instrument does not match query "
                f"({instrument.serialize()} != {query.instrument.serialize()})"
            )
        try:
            timeframe = parse_timeframe(bar.timeframe)
        except ValueError as exc:
            return f"stored bar timeframe is invalid ({exc})"
        if timeframe != query.timeframe:
            return (
                "stored bar timeframe does not match query "
                f"({timeframe.canonical} != {query.timeframe.canonical})"
            )
    return None


class _LiveKlineClientAdapter:
    def __init__(self, raw_client, *, instrument: InstrumentKey, timeframe: Timeframe):
        self.raw_client = raw_client
        self.instrument = instrument
        self.timeframe = timeframe

    async def events(
        self,
        *,
        max_messages: int | None = None,
        timeout_s: float | None = None,
    ) -> AsyncIterator[LiveKlineEvent]:
        async for event in self.raw_client.events(
            max_messages=max_messages, timeout_s=timeout_s
        ):
            update = event.update
            instrument = InstrumentKey(update.exchange, update.market, update.symbol)
            timeframe = parse_timeframe(update.timeframe)
            market_bar = update.to_market_bar()
            if market_bar.time_close is None:
                raise MDValidationError("live kline is missing explicit close time")
            if not market_bar.provider_revision:
                raise MDValidationError("live kline is missing provider_revision")
            snapshot = snapshot_from_market_bars(
                BarQuery(
                    instrument,
                    timeframe,
                    market_bar.time,
                    market_bar.time_close + 1,
                    source="provider",
                ),
                [market_bar],
                provider=market_bar.provider or instrument.exchange,
                provider_revision=market_bar.provider_revision,
                finality_policy="ALLOW_OPEN",
            )
            yield LiveKlineEvent(
                bar=snapshot["bars"][0],
                event_time=update.event_time,
                received_at=update.received_at,
                raw_payload=dict(event.raw_payload),
                diagnostic_code=event.diagnostic.code if event.diagnostic else None,
            )
