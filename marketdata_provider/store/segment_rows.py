from __future__ import annotations

from marketdata_provider.core.bar import MarketBar
from marketdata_provider.timeframes import canonical_timeframe


def parse_bool(value: object, *, default: bool = True) -> bool:
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "1.0", "yes", "y"}
    return bool(value)


def row_to_bar(r: dict[str, object]) -> MarketBar:
    def text(name: str, default: str = "") -> str:
        value = r.get(name, default)
        return str(value if value not in (None, "") else default)

    def required_number(name: str) -> str:
        return str(r[name])

    def opt_float(name: str) -> float | None:
        value = r.get(name)
        return float(str(value)) if value not in (None, "") else None

    def opt_int(name: str) -> int | None:
        value = r.get(name)
        return int(float(str(value))) if value not in (None, "") else None

    return MarketBar(
        time=int(float(required_number("time"))),
        open=float(required_number("open")),
        high=float(required_number("high")),
        low=float(required_number("low")),
        close=float(required_number("close")),
        volume=float(required_number("volume")),
        time_close=opt_int("time_close"),
        exchange=text("exchange").lower(),
        market=text("market").lower(),
        symbol=text("symbol").upper(),
        timeframe=canonical_timeframe(text("timeframe", "1m")),
        quote_volume=opt_float("quote_volume"),
        turnover=opt_float("turnover"),
        trades_count=opt_int("trades_count"),
        taker_buy_base_volume=opt_float("taker_buy_base_volume"),
        taker_buy_quote_volume=opt_float("taker_buy_quote_volume"),
        source_transport=text("source_transport", "ws"),
        source_kind=text("source_kind", "trade_kline"),
        is_closed=parse_bool(r.get("is_closed"), default=True),
        downloaded_at=opt_int("downloaded_at"),
    )
