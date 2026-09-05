"""Shared admission of canonical bars at execution boundaries.

Consumers must not turn transport records into OHLC tuples before admitting
content identities, revision lineage, instrument identity and finality.  This
module reuses the snapshot validator so batch and streamed consumers enforce
exactly the same market-data rules.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from typing import Any

from openpine_contracts import Finality

from marketdata_provider.canonical.bar import (
    _normalize_snapshot_bar,
    _resolve_bar_group,
)
from marketdata_provider.errors import MDValidationError

_CONTEXT_FIELDS = (
    "instrument_id",
    "series_id",
    "timeframe",
    "snapshot_id",
    "provider_revision",
    "stack_id",
)


def validate_canonical_bar(
    bar: Mapping[str, Any],
    *,
    expected: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Validate both content hashes and return a detached normalized envelope.

    ``expected`` is an admitted snapshot or execution context. Only identity
    fields actually present in it are compared; no identity is inferred from
    ticker strings. This function deliberately does not discard OPEN or
    REVOKED records: stream admission owns that decision.
    """
    if not isinstance(bar, Mapping):
        raise MDValidationError("canonical bar must be an object")
    admitted = _normalize_snapshot_bar(bar)
    if admitted["series_id"] != f"{admitted['instrument_id']}:{admitted['timeframe']}":
        raise MDValidationError(
            "canonical bar series_id does not match instrument/timeframe"
        )
    for key in _CONTEXT_FIELDS:
        if expected is not None and key in expected and admitted[key] != expected[key]:
            raise MDValidationError(
                f"canonical bar {key} differs from execution identity"
            )
    return admitted


def admit_canonical_bars(
    bars: Iterable[Mapping[str, Any]],
    *,
    expected: Mapping[str, Any] | None = None,
    finality_policy: str = "CLOSED_BAR_ONLY",
) -> Iterator[dict[str, Any]]:
    """Resolve ordered revision chains and yield only executable envelopes.

    Memory is bounded by one timestamp's revision chain. Duplicates are
    coalesced, corrected bars replace their ancestors, revocations remove the
    timestamp, and a missing ancestor/conflicting revision fails closed. The
    selected envelope retains all provenance; callers may project it to a
    numeric engine bar only *after* this operation.
    """
    if finality_policy not in {"CLOSED_BAR_ONLY", "ALLOW_OPEN"}:
        raise MDValidationError("unsupported finality policy")
    identity = dict(expected or {})
    group: list[dict[str, Any]] = []
    previous_close: int | None = None

    def select() -> dict[str, Any] | None:
        selected, _, _ = _resolve_bar_group(group)
        if selected is not None and (
            finality_policy == "ALLOW_OPEN" or selected["finality"] is Finality.FINAL
        ):
            return selected
        return None

    for raw in bars:
        bar = validate_canonical_bar(raw, expected=identity)
        if not group:
            # Bind a stream to its first snapshot even when no external
            # execution context was supplied.
            for key in _CONTEXT_FIELDS:
                identity.setdefault(key, bar[key])
        elif bar["open_time_utc_ms"] < group[-1]["open_time_utc_ms"]:
            raise MDValidationError("canonical bars are not ordered by open time")
        elif bar["open_time_utc_ms"] != group[-1]["open_time_utc_ms"]:
            selected = select()
            previous_close = int(group[-1]["close_time_utc_ms"])
            if selected is not None:
                yield selected
            group = []
        if (
            not group
            and previous_close is not None
            and int(bar["open_time_utc_ms"]) <= previous_close
        ):
            raise MDValidationError("canonical bar intervals overlap")
        group.append(bar)
    if group:
        selected = select()
        if selected is not None:
            yield selected
