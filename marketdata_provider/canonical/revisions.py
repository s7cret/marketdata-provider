"""Resolve one validated bar revision chain without changing OHLCV semantics."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, NoReturn

from openpine_contracts import RevisionState
from marketdata_provider.errors import MDBarConflict, MDValidationError


def _raise_bar_conflict(
    reason: str, open_time_utc_ms: int, bars: Sequence[Mapping[str, Any]]
) -> NoReturn:
    conflict = {
        "open_time_utc_ms": open_time_utc_ms,
        "reason": reason,
        "revisions": [bar["revision"] for bar in bars],
        "bar_content_hashes": [bar["bar_content_hash"] for bar in bars],
    }
    raise MDBarConflict(
        f"bar content conflict at {open_time_utc_ms}: {reason}",
        details={"conflicts": [conflict]},
    )


def resolve_bar_revisions(
    group: list[dict[str, Any]],
) -> tuple[
    dict[str, Any] | None,
    list[dict[str, Any]],
    dict[str, Any] | None,
]:
    if not group:
        raise MDValidationError("revision group must not be empty")
    open_time = int(group[0]["open_time_utc_ms"])
    unique: list[dict[str, Any]] = []
    counts: dict[str, int] = {}
    for bar in group:
        hash_value = str(bar["bar_content_hash"])
        counts[hash_value] = counts.get(hash_value, 0) + 1
        if counts[hash_value] == 1:
            unique.append(bar)

    duplicates = [
        {
            "open_time_utc_ms": open_time,
            "revision": bar["revision"],
            "count": counts[str(bar["bar_content_hash"])],
            "bar_content_hash": bar["bar_content_hash"],
        }
        for bar in unique
        if counts[str(bar["bar_content_hash"])] > 1
    ]

    providers = {str(bar["provider"]) for bar in unique}
    if len(providers) != 1:
        _raise_bar_conflict("provider changed within revision chain", open_time, unique)

    seen_revisions: dict[int, dict[str, Any]] = {}
    previous_revision: int | None = None
    previous_state: RevisionState | None = None
    previous_bar: dict[str, Any] | None = None
    for bar in unique:
        revision = int(bar["revision"])
        state = bar["revision_state"]
        if revision in seen_revisions:
            _raise_bar_conflict(
                "same revision has different content", open_time, unique
            )
        if previous_revision is not None and revision <= previous_revision:
            _raise_bar_conflict(
                "revision chain is not strictly increasing", open_time, unique
            )
        if previous_state is RevisionState.REVOKED:
            _raise_bar_conflict(
                "revision follows terminal revocation", open_time, unique
            )
        if previous_bar is None:
            if state is not RevisionState.ORIGINAL:
                _raise_bar_conflict(
                    "revision chain is missing the preceding canonical bar",
                    open_time,
                    unique,
                )
        elif bar["superseded_bar_hash"] != previous_bar["bar_content_hash"]:
            _raise_bar_conflict(
                "superseded_bar_hash does not bind the immediately preceding bar",
                open_time,
                unique,
            )
        seen_revisions[revision] = bar
        previous_revision = revision
        previous_state = state
        previous_bar = bar

    selected = unique[-1]
    revoked = selected["revision_state"] is RevisionState.REVOKED
    chain = None
    if len(unique) > 1 or selected["revision_state"] is not RevisionState.ORIGINAL:
        chain = {
            "open_time_utc_ms": open_time,
            "revisions": [bar["revision"] for bar in unique],
            "revision_states": [bar["revision_state"].value for bar in unique],
            "selected_revision": None if revoked else selected["revision"],
            "revoked": revoked,
        }
    return (None if revoked else selected), duplicates, chain
