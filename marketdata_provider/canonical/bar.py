from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from decimal import Decimal
from typing import Any, cast

from openpine_contracts import Finality, RevisionState, decimal_string, validate_payload
from openpine_contracts.hashing import content_hash, verify_content_hash
from openpine_contracts.money import MoneyError

from marketdata_provider.canonical.envelope import (
    BAR_SCHEMA_ID,
    SNAPSHOT_SCHEMA_ID,
    envelope_metadata,
    normalize_provider_revision,
    seal_and_validate,
    utc_now_ms,
)
from marketdata_provider.canonical.revisions import resolve_bar_revisions
from marketdata_provider.canonical.source_identity import verify_snapshot_bar_revisions
from marketdata_provider.errors import (
    MDMissingFinality,
    MDTimeframeUnsupported,
    MDValidationError,
)
from marketdata_provider.timeframes import canonical_timeframe
from marketdata_provider.timeframes import close_time_ms as canonical_close_time_ms

_BAR_CONTENT_SCHEMA_ID = "openpine.marketdata.bar.v2"
_SERIES_HASH_SCHEMA_ID = "marketdata-provider.series.v1"
CanonicalBarV2 = dict[str, Any]
DataSnapshotV2 = dict[str, Any]
_SUPPORTED_TIMEFRAMES = frozenset(
    {
        "1s",
        "1m",
        "3m",
        "5m",
        "15m",
        "30m",
        "1h",
        "2h",
        "4h",
        "6h",
        "12h",
        "1D",
        "1W",
        "1M",
    }
)
_REQUIRED_BAR_FIELDS = (
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
    "superseded_bar_hash",
)


def bar_finality(*, close_time_ms: int, server_time_ms: int | None) -> Finality:
    if server_time_ms is None:
        raise MDValidationError("server_time_ms required")
    if server_time_ms >= close_time_ms:
        return Finality.FINAL
    return Finality.OPEN


def _required_text(name: str, value: object) -> str:
    if not isinstance(value, str) or not value:
        raise MDValidationError(f"{name} is required")
    return value


def _integer_field(name: str, value: object, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise MDValidationError(f"{name} must be an integer")
    if value < minimum:
        raise MDValidationError(f"{name} must be >= {minimum}")
    return value


def _canonical_timeframe(value: object) -> str:
    timeframe = _required_text("timeframe", value)
    try:
        normalized = canonical_timeframe(timeframe)
    except MDTimeframeUnsupported as exc:
        raise MDValidationError(f"unsupported timeframe: {timeframe}") from exc
    if normalized != timeframe:
        raise MDValidationError(
            f"canonical timeframe required: {timeframe!r} normalizes to {normalized!r}"
        )
    if normalized not in _SUPPORTED_TIMEFRAMES:
        raise MDValidationError(f"unsupported timeframe: {timeframe}")
    return normalized


def _normalize_finality(value: object) -> Finality:
    if value is None:
        raise MDMissingFinality("finality is required")
    if isinstance(value, bool):
        raise MDValidationError("finality bool is invalid")
    if isinstance(value, Finality):
        return value
    if isinstance(value, str):
        try:
            return Finality(value)
        except ValueError as exc:
            raise MDValidationError(f"unknown finality: {value}") from exc
    raise MDValidationError("finality must be a Finality value")


def _normalize_revision_state(value: object) -> RevisionState:
    if isinstance(value, bool):
        raise MDValidationError("revision_state bool is invalid")
    if isinstance(value, RevisionState):
        return value
    if isinstance(value, str):
        try:
            return RevisionState(value)
        except ValueError as exc:
            raise MDValidationError(f"unknown revision_state: {value}") from exc
    raise MDValidationError("revision_state must be a RevisionState value")


def _normalize_revision(value: object, state: RevisionState) -> int:
    revision = _integer_field("revision", value)
    if state is RevisionState.ORIGINAL and revision != 0:
        raise MDValidationError("ORIGINAL revision must be 0")
    if state is not RevisionState.ORIGINAL and revision == 0:
        raise MDValidationError(f"{state.value} revision must be >= 1")
    return revision


def _decimal_field(name: str, value: object) -> str:
    if isinstance(value, float) and not isinstance(value, bool):
        raise MDValidationError(f"{name} float is forbidden on contract boundary")
    try:
        return decimal_string(cast(str | int | Decimal, value))
    except (MoneyError, TypeError) as exc:
        raise MDValidationError(f"invalid {name}") from exc


def _validated_prices(
    *, open: object, high: object, low: object, close: object, volume: object
) -> tuple[str, str, str, str, str]:
    open_s = _decimal_field("open", open)
    high_s = _decimal_field("high", high)
    low_s = _decimal_field("low", low)
    close_s = _decimal_field("close", close)
    volume_s = _decimal_field("volume", volume)
    if Decimal(volume_s) < 0:
        raise MDValidationError("volume must be nonnegative")

    open_d, high_d, low_d, close_d = (
        Decimal(part) for part in (open_s, high_s, low_s, close_s)
    )
    if not (low_d <= min(open_d, close_d) <= max(open_d, close_d) <= high_d):
        raise MDValidationError("OHLC invariants violated")
    return open_s, high_s, low_s, close_s, volume_s


def _canonical_close_time(
    timeframe: str, open_time_utc_ms: int, supplied: object
) -> int:
    expected = canonical_close_time_ms(open_time_utc_ms, timeframe)
    if supplied is None:
        return expected
    close_time_utc_ms = _integer_field("close_time_utc_ms", supplied)
    if close_time_utc_ms != expected:
        raise MDValidationError(
            "close_time_utc_ms does not match canonical timeframe boundary"
        )
    return close_time_utc_ms


def _enum_value(value: object) -> object:
    return value.value if isinstance(value, (Finality, RevisionState)) else value


def _bar_identity_payload(bar: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "series_id": bar["series_id"],
        "instrument_id": bar["instrument_id"],
        "timeframe": bar["timeframe"],
        "open_time_utc_ms": bar["open_time_utc_ms"],
        "close_time_utc_ms": bar["close_time_utc_ms"],
        "open": bar["open"],
        "high": bar["high"],
        "low": bar["low"],
        "close": bar["close"],
        "volume": bar["volume"],
        "finality": _enum_value(bar["finality"]),
        "revision_state": _enum_value(bar["revision_state"]),
        "revision": bar["revision"],
        "provider": bar["provider"],
        "provider_revision": bar["provider_revision"],
        "superseded_bar_hash": bar["superseded_bar_hash"],
    }


def _bar_content_hash(bar: Mapping[str, Any]) -> str:
    return content_hash(_bar_identity_payload(bar), schema_id=_BAR_CONTENT_SCHEMA_ID)


def _superseded_hash(value: object, state: RevisionState) -> str | None:
    if state is RevisionState.ORIGINAL:
        if value is not None:
            raise MDValidationError("ORIGINAL superseded_bar_hash must be null")
        return None
    if (
        not isinstance(value, str)
        or not value.startswith("sha256:")
        or len(value) != 71
    ):
        raise MDValidationError(
            f"{state.value} superseded_bar_hash must bind the preceding canonical bar"
        )
    return value


def make_canonical_bar(
    *,
    instrument_id: str,
    timeframe: str,
    open_time_utc_ms: int,
    open: object,
    high: object,
    low: object,
    close: object,
    volume: object,
    snapshot_id: str,
    provider: str,
    provider_revision: object,
    producer_commit: str,
    stack_id: str,
    superseded_bar_hash: str | None = None,
    finality: Finality | str | None = None,
    revision_state: RevisionState | str = RevisionState.ORIGINAL,
    revision: int = 0,
    close_time_utc_ms: int | None = None,
    created_at_utc_ms: int | None = None,
    schema_validate: bool = True,
) -> dict[str, Any]:
    instrument = _required_text("instrument_id", instrument_id)
    canonical_tf = _canonical_timeframe(timeframe)
    snapshot = _required_text("snapshot_id", snapshot_id)
    source_provider = _required_text("provider", provider)
    source_provider_revision = normalize_provider_revision(provider_revision)
    open_ms = _integer_field("open_time_utc_ms", open_time_utc_ms)
    normalized_finality = _normalize_finality(finality)
    normalized_state = _normalize_revision_state(revision_state)
    normalized_revision = _normalize_revision(revision, normalized_state)
    lineage_hash = _superseded_hash(superseded_bar_hash, normalized_state)
    open_s, high_s, low_s, close_s, volume_s = _validated_prices(
        open=open,
        high=high,
        low=low,
        close=close,
        volume=volume,
    )
    close_ms = _canonical_close_time(canonical_tf, open_ms, close_time_utc_ms)
    created_at = utc_now_ms() if created_at_utc_ms is None else created_at_utc_ms

    envelope: dict[str, Any] = {
        "schema_id": BAR_SCHEMA_ID,
        **envelope_metadata(
            producer_commit=producer_commit,
            stack_id=stack_id,
            created_at_utc_ms=created_at,
        ),
        "instrument_id": instrument,
        "timeframe": canonical_tf,
        "open_time_utc_ms": open_ms,
        "close_time_utc_ms": close_ms,
        "open": open_s,
        "high": high_s,
        "low": low_s,
        "close": close_s,
        "volume": volume_s,
        "finality": normalized_finality,
        "revision_state": normalized_state,
        "revision": normalized_revision,
        "snapshot_id": snapshot,
        "provider": source_provider,
        "provider_revision": source_provider_revision,
        "series_id": f"{instrument}:{canonical_tf}",
        "superseded_bar_hash": lineage_hash,
    }
    envelope["bar_content_hash"] = _bar_content_hash(envelope)
    return seal_and_validate(BAR_SCHEMA_ID, envelope, schema_validate=schema_validate)


def _normalize_snapshot_bar(bar: Mapping[str, Any]) -> dict[str, Any]:
    if "finality" not in bar:
        raise MDMissingFinality("finality is required")
    for field in _REQUIRED_BAR_FIELDS:
        if field not in bar:
            raise MDValidationError(f"bar missing required field: {field}")

    normalized = dict(bar)
    normalized["finality"] = _normalize_finality(bar["finality"])
    normalized["revision_state"] = _normalize_revision_state(bar["revision_state"])
    normalized["revision"] = _normalize_revision(
        bar["revision"], normalized["revision_state"]
    )
    normalized["provider_revision"] = normalize_provider_revision(
        bar["provider_revision"]
    )
    normalized["timeframe"] = _canonical_timeframe(bar["timeframe"])
    normalized["open_time_utc_ms"] = _integer_field(
        "open_time_utc_ms", bar["open_time_utc_ms"]
    )
    normalized["close_time_utc_ms"] = _canonical_close_time(
        normalized["timeframe"],
        normalized["open_time_utc_ms"],
        bar["close_time_utc_ms"],
    )
    (
        normalized["open"],
        normalized["high"],
        normalized["low"],
        normalized["close"],
        normalized["volume"],
    ) = _validated_prices(
        open=bar["open"],
        high=bar["high"],
        low=bar["low"],
        close=bar["close"],
        volume=bar["volume"],
    )
    normalized["superseded_bar_hash"] = _superseded_hash(
        bar["superseded_bar_hash"], normalized["revision_state"]
    )
    try:
        validate_payload(BAR_SCHEMA_ID, normalized)
    except Exception as exc:
        raise MDValidationError(f"bar schema validation failed: {exc}") from exc
    if not verify_content_hash(normalized, schema_id=BAR_SCHEMA_ID):
        raise MDValidationError("bar content_hash verification failed")
    if normalized["bar_content_hash"] != _bar_content_hash(normalized):
        raise MDValidationError("bar_content_hash verification failed")
    return normalized


def _coverage_metadata(
    bars: Sequence[Mapping[str, Any]], start_utc_ms: int, end_utc_ms: int
) -> tuple[dict[str, Any], list[dict[str, int]]]:
    gaps: list[dict[str, int]] = []
    cursor = start_utc_ms
    for bar in bars:
        open_time = int(bar["open_time_utc_ms"])
        close_end = int(bar["close_time_utc_ms"]) + 1
        if open_time > cursor:
            gaps.append({"start_utc_ms": cursor, "end_utc_ms": open_time})
        cursor = max(cursor, close_end)
    if cursor < end_utc_ms:
        gaps.append({"start_utc_ms": cursor, "end_utc_ms": end_utc_ms})

    coverage = {
        "requested_start_utc_ms": start_utc_ms,
        "requested_end_utc_ms": end_utc_ms,
        "covered_start_utc_ms": (int(bars[0]["open_time_utc_ms"]) if bars else None),
        "covered_end_utc_ms": (
            int(bars[-1]["close_time_utc_ms"]) + 1 if bars else None
        ),
        "bar_count": len(bars),
        "gap_count": len(gaps),
        "complete": not gaps,
    }
    return coverage, gaps


def _contract_coverage(
    bars: Sequence[Mapping[str, Any]], instrument_id: str, timeframe: str
) -> list[dict[str, Any]]:
    ranges: list[dict[str, Any]] = []
    for bar in bars:
        start = int(bar["open_time_utc_ms"])
        end = int(bar["close_time_utc_ms"]) + 1
        if ranges and start <= int(ranges[-1]["end_utc_ms"]):
            ranges[-1]["end_utc_ms"] = max(int(ranges[-1]["end_utc_ms"]), end)
        else:
            ranges.append(
                {
                    "instrument_id": instrument_id,
                    "timeframe": timeframe,
                    "start_utc_ms": start,
                    "end_utc_ms": end,
                }
            )
    return ranges


def build_data_snapshot(
    *,
    snapshot_id: str,
    instrument_id: str,
    timeframe: str,
    provider_revision: object,
    producer_commit: str,
    stack_id: str,
    start_utc_ms: int,
    end_utc_ms: int,
    bars: Iterable[Mapping[str, Any]],
    finality_policy: str = "CLOSED_BAR_ONLY",
    clock: Callable[[], int] | None = None,
    schema_validate: bool = True,
) -> dict[str, Any]:
    snapshot_instance_id = _required_text("snapshot_id", snapshot_id)
    instrument = _required_text("instrument_id", instrument_id)
    canonical_tf = _canonical_timeframe(timeframe)
    expected_provider_revision = normalize_provider_revision(provider_revision)
    start_ms = _integer_field("start_utc_ms", start_utc_ms)
    end_ms = _integer_field("end_utc_ms", end_utc_ms)
    if end_ms <= start_ms:
        raise MDValidationError("snapshot range must satisfy start_utc_ms < end_utc_ms")
    if finality_policy not in {"CLOSED_BAR_ONLY", "ALLOW_OPEN"}:
        raise MDValidationError(f"unknown finality_policy: {finality_policy}")

    normalized: list[dict[str, Any]] = []
    previous_open: int | None = None
    for raw_bar in bars:
        if not isinstance(raw_bar, Mapping):
            raise MDValidationError("each bar must be a mapping")
        bar = _normalize_snapshot_bar(raw_bar)
        if bar["instrument_id"] != instrument:
            raise MDValidationError("bar instrument_id does not match snapshot")
        if bar["timeframe"] != canonical_tf:
            raise MDValidationError("bar timeframe does not match snapshot")
        if bar["snapshot_id"] != snapshot_instance_id:
            raise MDValidationError("bar snapshot_id does not match snapshot")
        open_time = int(bar["open_time_utc_ms"])
        close_time = int(bar["close_time_utc_ms"])
        if open_time < start_ms or close_time >= end_ms:
            raise MDValidationError("bar is outside requested range")
        if previous_open is not None and open_time < previous_open:
            raise MDValidationError("bar open_time is not monotonic")
        previous_open = open_time
        normalized.append(bar)

    verify_snapshot_bar_revisions(normalized, expected_provider_revision)

    kept: list[dict[str, Any]] = []
    duplicates: list[dict[str, Any]] = []
    revision_chains: list[dict[str, Any]] = []
    index = 0
    previous_close: int | None = None
    while index < len(normalized):
        open_time = int(normalized[index]["open_time_utc_ms"])
        next_index = index + 1
        while (
            next_index < len(normalized)
            and int(normalized[next_index]["open_time_utc_ms"]) == open_time
        ):
            next_index += 1
        selected, group_duplicates, chain = resolve_bar_revisions(
            normalized[index:next_index]
        )
        duplicates.extend(group_duplicates)
        if chain is not None:
            revision_chains.append(chain)
        if selected is not None:
            if previous_close is not None and open_time <= previous_close:
                raise MDValidationError(
                    "bar ranges overlap despite monotonic open_time"
                )
            previous_close = int(selected["close_time_utc_ms"])
            if (
                finality_policy == "ALLOW_OPEN"
                or selected["finality"] is Finality.FINAL
            ):
                kept.append(selected)
        index = next_index

    contract_query = {
        "instrument_id": instrument,
        "timeframe": canonical_tf,
        "start_utc_ms": start_ms,
        "end_utc_ms": end_ms,
        "finality_policy": finality_policy,
    }
    compatibility_query = {
        **contract_query,
        "provider_revision": expected_provider_revision,
    }
    coverage_details, gaps = _coverage_metadata(kept, start_ms, end_ms)
    ordered_hashes = [bar["bar_content_hash"] for bar in kept]
    series_identity = {
        "query": contract_query,
        "provider_revision": expected_provider_revision,
        "ordered_bar_content_hashes": ordered_hashes,
    }
    created_at = _integer_field("created_at_utc_ms", (clock or utc_now_ms)())
    series_hash = content_hash(series_identity, schema_id=_SERIES_HASH_SCHEMA_ID)
    body = {
        "snapshot_id": snapshot_instance_id,
        "query": contract_query,
        "bar_count": len(kept),
        "series_hash": series_hash,
        "coverage": _contract_coverage(kept, instrument, canonical_tf),
        "gaps": gaps,
        "conflicts": [],
        "provider_revision": expected_provider_revision,
        "created_at_utc_ms": created_at,
    }
    snapshot_envelope = seal_and_validate(
        SNAPSHOT_SCHEMA_ID,
        {
            "schema_id": SNAPSHOT_SCHEMA_ID,
            **envelope_metadata(
                producer_commit=producer_commit,
                stack_id=stack_id,
                created_at_utc_ms=created_at,
            ),
            "kind": "snapshot",
            "body": body,
        },
        schema_validate=schema_validate,
    )
    diagnostics = {
        "coverage": coverage_details,
        "gaps": gaps,
        "duplicates": duplicates,
        "conflicts": [],
        "revision_chains": revision_chains,
    }
    return {
        "snapshot_envelope": snapshot_envelope,
        "bars": kept,
        "diagnostics": diagnostics,
        # Stable bundle conveniences; the contract envelope is always explicit above.
        "snapshot_id": snapshot_instance_id,
        "query": compatibility_query,
        "provider_revision": expected_provider_revision,
        "bar_count": len(kept),
        "coverage": coverage_details,
        "gaps": gaps,
        "duplicates": duplicates,
        "conflicts": [],
        "revision_chains": revision_chains,
        "created_at_utc_ms": created_at,
        "series_hash": series_hash,
    }


def canonical_bars_from_binance_klines(
    rows: Iterable[Sequence[Any]],
    *,
    instrument_id: str,
    timeframe: str,
    provider: str,
    provider_revision: object,
    producer_commit: str,
    stack_id: str,
    snapshot_id: str,
    server_time_ms: int | None,
    include_open: bool = False,
) -> list[dict[str, Any]]:
    if server_time_ms is None:
        raise MDValidationError("server_time_ms required")
    canonical_tf = _canonical_timeframe(timeframe)
    bars: list[dict[str, Any]] = []
    for row in rows:
        items = list(row)
        if len(items) < 6:
            raise MDValidationError("kline row too short")
        open_time = _integer_field("open_time_utc_ms", items[0])
        expected_close = canonical_close_time_ms(open_time, canonical_tf)
        supplied_close = items[6] if len(items) > 6 else None
        bar = make_canonical_bar(
            instrument_id=instrument_id,
            timeframe=canonical_tf,
            open_time_utc_ms=open_time,
            close_time_utc_ms=supplied_close,
            open=items[1],
            high=items[2],
            low=items[3],
            close=items[4],
            volume=items[5],
            snapshot_id=snapshot_id,
            provider=provider,
            provider_revision=provider_revision,
            producer_commit=producer_commit,
            stack_id=stack_id,
            finality=bar_finality(
                close_time_ms=expected_close,
                server_time_ms=server_time_ms,
            ),
        )
        if include_open or bar["finality"] is Finality.FINAL:
            bars.append(bar)
    return bars
