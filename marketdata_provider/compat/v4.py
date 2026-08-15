from __future__ import annotations

from openpine_contracts import Finality

from marketdata_provider.errors import MDValidationError


def finality_from_closed(closed: bool | None) -> Finality:
    if closed is True:
        return Finality.FINAL
    if closed is False:
        return Finality.OPEN
    raise MDValidationError("missing closed/finality; cannot default to FINAL")
