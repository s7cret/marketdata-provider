from __future__ import annotations

import csv
import hashlib
import struct
from collections.abc import Iterable
from decimal import Decimal
from pathlib import Path
from typing import cast

from marketdata_provider.core.bar import MarketBar
from marketdata_provider.errors import MDInvalidExchangeResponse
from marketdata_provider.store.segment_rows import row_to_bar
from marketdata_provider.timeframes import canonical_timeframe

LEGACY_CANONICAL_CHECKSUM = "sha256-canonical-v1"
LEGACY_TAIL_CHAIN_CHECKSUM = "sha256-tail-chain-v1"
PRESENCE_UNAWARE_CANONICAL_CHECKSUM = "sha256-canonical-v2"
PRESENCE_UNAWARE_TAIL_CHAIN_CHECKSUM = "sha256-tail-chain-v2"
CANONICAL_CHECKSUM = "sha256-canonical-v3"
TAIL_CHAIN_CHECKSUM = "sha256-tail-chain-v3"


# SegmentStore deliberately persists every MarketBar field except the runtime-only
# ``source`` label and arbitrary ``metadata`` mapping. Keep this tuple aligned with
# SegmentStore.fields and cover every member in the row digest below.
PERSISTED_MARKET_BAR_FIELDS = (
    "time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "time_close",
    "exchange",
    "market",
    "symbol",
    "timeframe",
    "quote_volume",
    "turnover",
    "trades_count",
    "taker_buy_base_volume",
    "taker_buy_quote_volume",
    "source_transport",
    "source_kind",
    "is_closed",
    "downloaded_at",
)


def _canon_number(v: float | None) -> str | None:
    """Canonical number formatting (kept for backward-compat / external use)."""
    if v is None:
        return None
    d = Decimal(str(v)).normalize()
    if d == 0:
        return "0"
    return format(d, "f")


def market_bar_checksum(bar: MarketBar) -> str:
    return bars_checksum([bar])


def bars_checksum(bars: Iterable[MarketBar]) -> str:
    digest = hashlib.sha256()
    for bar in sorted(bars, key=lambda item: item.time):
        _update_checksum(digest, bar)
    return digest.hexdigest()


def legacy_bars_checksum(bars: Iterable[MarketBar]) -> str:
    """Compute the pre-v2 digest when constructing or validating legacy fixtures."""
    digest = hashlib.sha256()
    for bar in sorted(bars, key=lambda item: item.time):
        _update_checksum_v1(digest, bar)
    return digest.hexdigest()


def presence_unaware_bars_checksum(bars: Iterable[MarketBar]) -> str:
    """Compute the v2 digest for validation and metadata-only migration."""
    digest = hashlib.sha256()
    for bar in sorted(bars, key=lambda item: item.time):
        _update_checksum_v2(digest, bar)
    return digest.hexdigest()


def csv_canonical_checksum(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open(newline="") as handle:
        for row in csv.DictReader(handle):
            bar = row_to_bar(cast(dict[str, object], row))
            validate_persisted_bar_semantics(bar)
            _update_checksum(digest, bar)
    return digest.hexdigest()


def extend_tail_chain(
    checksum: str,
    bar: MarketBar,
    *,
    algorithm: str = TAIL_CHAIN_CHECKSUM,
) -> str:
    """Extend a versioned, domain-separated checksum chain."""
    try:
        previous = bytes.fromhex(checksum)
    except ValueError as exc:
        raise MDInvalidExchangeResponse("Invalid segment checksum encoding") from exc
    if len(previous) != hashlib.sha256().digest_size:
        raise MDInvalidExchangeResponse("Invalid segment checksum length")
    if algorithm == TAIL_CHAIN_CHECKSUM:
        row_digest = bytes.fromhex(market_bar_checksum(bar))
    elif algorithm == PRESENCE_UNAWARE_TAIL_CHAIN_CHECKSUM:
        row_digest = bytes.fromhex(presence_unaware_bars_checksum([bar]))
    elif algorithm == LEGACY_TAIL_CHAIN_CHECKSUM:
        row_digest = bytes.fromhex(legacy_bars_checksum([bar]))
    else:
        raise MDInvalidExchangeResponse(
            "Unsupported segment checksum algorithm", details={"algorithm": algorithm}
        )
    return hashlib.sha256(
        algorithm.encode("ascii") + b"\0" + previous + row_digest
    ).hexdigest()


def validate_csv_checksum(path: Path, manifest: dict[str, object] | None) -> None:
    if manifest is None:
        return
    algorithm = manifest.get("checksum_algorithm", LEGACY_CANONICAL_CHECKSUM)
    supported = {
        LEGACY_CANONICAL_CHECKSUM,
        LEGACY_TAIL_CHAIN_CHECKSUM,
        PRESENCE_UNAWARE_CANONICAL_CHECKSUM,
        PRESENCE_UNAWARE_TAIL_CHAIN_CHECKSUM,
        CANONICAL_CHECKSUM,
        TAIL_CHAIN_CHECKSUM,
    }
    if algorithm not in supported:
        raise MDInvalidExchangeResponse(
            "Unsupported segment checksum algorithm", details={"algorithm": algorithm}
        )
    base_rows = manifest.get("base_rows_count")
    base_checksum = manifest.get("base_checksum")
    is_tail = algorithm in {
        LEGACY_TAIL_CHAIN_CHECKSUM,
        PRESENCE_UNAWARE_TAIL_CHAIN_CHECKSUM,
        TAIL_CHAIN_CHECKSUM,
    }
    if is_tail and (
        not isinstance(base_rows, int)
        or base_rows < 0
        or not isinstance(base_checksum, str)
    ):
        raise MDInvalidExchangeResponse("Invalid tail-chain checksum metadata")

    digest = hashlib.sha256()
    chain = base_checksum if isinstance(base_checksum, str) else ""
    rows_count = 0
    with path.open(newline="") as handle:
        for row in csv.DictReader(handle):
            bar = row_to_bar(cast(dict[str, object], row))
            validate_persisted_bar_semantics(bar)
            if not is_tail or rows_count < cast(int, base_rows):
                if algorithm in {LEGACY_CANONICAL_CHECKSUM, LEGACY_TAIL_CHAIN_CHECKSUM}:
                    _update_checksum_v1(digest, bar)
                elif algorithm in {
                    PRESENCE_UNAWARE_CANONICAL_CHECKSUM,
                    PRESENCE_UNAWARE_TAIL_CHAIN_CHECKSUM,
                }:
                    _update_checksum_v2(digest, bar)
                else:
                    _update_checksum(digest, bar)
            else:
                chain = extend_tail_chain(chain, bar, algorithm=cast(str, algorithm))
            rows_count += 1

    if is_tail:
        actual_base = digest.hexdigest()
        if rows_count < cast(int, base_rows) or actual_base != base_checksum:
            raise MDInvalidExchangeResponse(
                "Segment checksum mismatch",
                details={"expected": base_checksum, "actual": actual_base},
            )
        actual = chain
    else:
        actual = digest.hexdigest()
    if actual != manifest.get("checksum"):
        raise MDInvalidExchangeResponse(
            "Segment checksum mismatch",
            details={"expected": manifest.get("checksum"), "actual": actual},
        )


def validate_persisted_bar_semantics(bar: MarketBar) -> None:
    """Reject sentinel values that collide in legacy presence-unaware digests."""

    for field in ("time_close", "trades_count", "downloaded_at"):
        value = getattr(bar, field)
        if value is not None and value < 0:
            raise MDInvalidExchangeResponse(
                "Persisted bar has negative optional integer",
                details={"field": field, "value": value, "time": bar.time},
            )


def _update_text(h: hashlib._Hash, value: str) -> None:
    encoded = value.encode("utf-8")
    h.update(struct.pack(">I", len(encoded)))
    h.update(encoded)


def _update_checksum_identity(h: hashlib._Hash, b: MarketBar) -> None:
    _update_text(h, b.exchange.lower())
    _update_text(h, b.market.lower())
    _update_text(h, b.symbol.upper())
    _update_text(h, b.source_kind)
    _update_text(h, b.source_transport)
    _update_text(h, canonical_timeframe(b.timeframe))


def _update_checksum(h: hashlib._Hash, b: MarketBar) -> None:
    """Hash every persisted semantic field using the presence-aware v3 format."""
    _update_checksum_identity(h, b)
    h.update(
        struct.pack(
            ">qqqqddddddddd????????",
            b.time,
            b.time_close if b.time_close is not None else 0,
            b.trades_count if b.trades_count is not None else 0,
            b.downloaded_at if b.downloaded_at is not None else 0,
            b.open,
            b.high,
            b.low,
            b.close,
            b.volume,
            b.quote_volume if b.quote_volume is not None else 0.0,
            b.turnover if b.turnover is not None else 0.0,
            b.taker_buy_base_volume if b.taker_buy_base_volume is not None else 0.0,
            b.taker_buy_quote_volume if b.taker_buy_quote_volume is not None else 0.0,
            b.time_close is not None,
            b.trades_count is not None,
            b.downloaded_at is not None,
            b.quote_volume is not None,
            b.turnover is not None,
            b.taker_buy_base_volume is not None,
            b.taker_buy_quote_volume is not None,
            b.is_closed,
        )
    )
    h.update(b"\n")


def _update_checksum_v2(h: hashlib._Hash, b: MarketBar) -> None:
    """Presence-unaware digest retained for v2 manifest validation."""
    _update_checksum_identity(h, b)
    h.update(
        struct.pack(
            ">qqqqddddddddd?????",
            b.time,
            b.time_close if b.time_close is not None else -1,
            b.trades_count if b.trades_count is not None else -1,
            b.downloaded_at if b.downloaded_at is not None else -1,
            b.open,
            b.high,
            b.low,
            b.close,
            b.volume,
            b.quote_volume if b.quote_volume is not None else 0.0,
            b.turnover if b.turnover is not None else 0.0,
            b.taker_buy_base_volume if b.taker_buy_base_volume is not None else 0.0,
            b.taker_buy_quote_volume if b.taker_buy_quote_volume is not None else 0.0,
            b.quote_volume is not None,
            b.turnover is not None,
            b.taker_buy_base_volume is not None,
            b.taker_buy_quote_volume is not None,
            b.is_closed,
        )
    )
    h.update(b"\n")


def _update_checksum_v1(h: hashlib._Hash, b: MarketBar) -> None:
    """Original partial row digest retained only for legacy manifest validation."""
    h.update(b.exchange.lower().encode())
    h.update(b.market.lower().encode())
    h.update(b.symbol.upper().encode())
    h.update(b.source_kind.encode())
    h.update(b.source_transport.encode())
    h.update(canonical_timeframe(b.timeframe).encode())
    h.update(
        struct.pack(
            ">qqqddddddd?",
            b.time,
            b.time_close if b.time_close is not None else 0,
            b.trades_count if b.trades_count is not None else -1,
            b.open,
            b.high,
            b.low,
            b.close,
            b.volume,
            b.quote_volume if b.quote_volume is not None else 0.0,
            b.turnover if b.turnover is not None else 0.0,
            b.is_closed,
        )
    )
    h.update(b"\n")
