from __future__ import annotations

from openpine_contracts import Finality

from marketdata_provider.errors import MDValidationError


def create_legacy_candle_store(config):
    """Explicit opt-in adapter for the pre-v5 BarSeries candle store."""

    from marketdata_provider.factories import _create_legacy_candle_store

    return _create_legacy_candle_store(config)


def create_legacy_provider(config):
    """Explicit opt-in adapter for the pre-v5 BarSeries boundary."""

    from marketdata_provider.factories import _create_legacy_provider

    return _create_legacy_provider(config)


def finality_from_closed(closed: bool | None) -> Finality:
    if closed is True:
        return Finality.FINAL
    if closed is False:
        return Finality.OPEN
    raise MDValidationError("missing closed/finality; cannot default to FINAL")
