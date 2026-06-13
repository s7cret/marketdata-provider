from __future__ import annotations

from dataclasses import dataclass

from marketdata_provider.contracts.errors import InvalidInstrumentError


@dataclass(frozen=True, slots=True)
class InstrumentKey:
    """Canonical instrument identity.

    Case normalization is explicit: exchange and market are lowercase, symbol is uppercase.
    """

    exchange: str
    market: str
    symbol: str

    def __post_init__(self) -> None:
        exchange = self.exchange.strip().lower()
        market = self.market.strip().lower()
        symbol = self.symbol.strip().upper()
        if not exchange:
            raise InvalidInstrumentError("exchange must not be empty")
        if not market:
            raise InvalidInstrumentError("market must not be empty")
        if not symbol:
            raise InvalidInstrumentError("symbol must not be empty")
        object.__setattr__(self, "exchange", exchange)
        object.__setattr__(self, "market", market)
        object.__setattr__(self, "symbol", symbol)

    def serialize(self) -> str:
        return f"{self.exchange}/{self.market}/{self.symbol}"

    @classmethod
    def parse(cls, value: str) -> "InstrumentKey":
        parts = value.split("/")
        if len(parts) != 3:
            raise InvalidInstrumentError(
                "instrument key must be exchange/market/symbol"
            )
        return cls(parts[0], parts[1], parts[2])

    def __str__(self) -> str:
        return self.serialize()
