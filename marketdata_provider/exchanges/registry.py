from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Literal

ProviderStatus = Literal["native", "planned"]
ArchiveAvailability = Literal[
    "official-bulk", "official-partial", "api-only", "third-party"
]
MarketType = Literal[
    "spot",
    "margin",
    "usdt_futures",
    "coin_futures",
    "delivery_futures",
    "options",
]


@dataclass(frozen=True, slots=True)
class MarketTypeInfo:
    """Canonical market-type descriptor used by CLI and provider planning."""

    id: MarketType
    label: str
    aliases: tuple[str, ...]
    description: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ExchangeInfo:
    """Exchange capability metadata.

    ``status`` marks whether this package currently has a native fetch adapter.
    Planned exchanges are intentionally not silently accepted by the live fetch
    path; they are listed so users can see the integration roadmap and preferred
    acquisition strategy before a dedicated adapter lands.
    """

    id: str
    name: str
    rank: int
    status: ProviderStatus
    native_markets: tuple[str, ...]
    listed_market_types: tuple[MarketType, ...]
    public_rest_api: bool
    public_websocket_api: bool
    archive: ArchiveAvailability
    recommended_source: str
    api_docs_url: str
    archive_docs_url: str | None = None
    notes: str = ""

    @property
    def native_adapter(self) -> bool:
        return self.status == "native"

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["native_adapter"] = self.native_adapter
        return payload


MARKET_TYPES: tuple[MarketTypeInfo, ...] = (
    MarketTypeInfo(
        "spot",
        "Spot",
        ("spot", "cash"),
        "Immediate settlement spot markets and spot OHLCV/candle feeds.",
    ),
    MarketTypeInfo(
        "margin",
        "Margin",
        ("margin", "cross-margin", "isolated-margin"),
        "Spot-margin markets. Public market data usually mirrors spot feeds.",
    ),
    MarketTypeInfo(
        "usdt_futures",
        "USDT/USDC-margined futures/perpetuals",
        ("linear", "usdm", "usdt-futures", "usdc-futures", "swap"),
        "Linear derivatives quoted and settled in stablecoins.",
    ),
    MarketTypeInfo(
        "coin_futures",
        "Coin-margined futures/perpetuals",
        ("inverse", "coinm", "coin-futures", "inverse-swap"),
        "Inverse derivatives settled in the base coin.",
    ),
    MarketTypeInfo(
        "delivery_futures",
        "Delivery futures",
        ("futures", "delivery", "dated-futures"),
        "Dated futures contracts with expiry/delivery.",
    ),
    MarketTypeInfo(
        "options",
        "Options",
        ("option", "options"),
        "Listed crypto options where the exchange publishes public market data.",
    ),
)


EXCHANGES: tuple[ExchangeInfo, ...] = (
    ExchangeInfo(
        id="binance",
        name="Binance",
        rank=1,
        status="native",
        native_markets=("spot", "margin", "usdm", "coinm"),
        listed_market_types=(
            "spot",
            "margin",
            "usdt_futures",
            "coin_futures",
            "options",
        ),
        public_rest_api=True,
        public_websocket_api=True,
        archive="official-bulk",
        recommended_source="Official bulk archive first for deep history; REST/WebSocket for recent/live gaps.",
        api_docs_url="https://developers.binance.com/docs/binance-spot-api-docs/rest-api/market-data-endpoints",
        archive_docs_url="https://data.binance.vision/",
        notes="Best first-class source for bulk OHLCV/trades because public daily/monthly archives are available.",
    ),
    ExchangeInfo(
        id="bybit",
        name="Bybit",
        rank=2,
        status="native",
        native_markets=("spot", "linear", "inverse"),
        listed_market_types=("spot", "usdt_futures", "coin_futures", "options"),
        public_rest_api=True,
        public_websocket_api=True,
        archive="official-partial",
        recommended_source="REST/WebSocket for candles/live data; official history downloads where the required dataset is available.",
        api_docs_url="https://bybit-exchange.github.io/docs/",
        archive_docs_url="https://www.bybit.com/derivatives/en/history-data",
        notes="Native adapter currently covers spot and linear kline REST paths.",
    ),
    ExchangeInfo(
        id="okx",
        name="OKX",
        rank=3,
        status="native",
        native_markets=("spot", "margin", "linear", "inverse", "delivery_futures"),
        listed_market_types=(
            "spot",
            "margin",
            "usdt_futures",
            "coin_futures",
            "delivery_futures",
            "options",
        ),
        public_rest_api=True,
        public_websocket_api=True,
        archive="api-only",
        recommended_source="Official REST/WebSocket for candles and live data; use a licensed archive vendor for deep order-book/trade replay.",
        api_docs_url="https://www.okx.com/docs-v5/en/",
        notes="Strong API coverage across spot, margin, swaps, futures, and options.",
    ),
    ExchangeInfo(
        id="coinbase",
        name="Coinbase Exchange",
        rank=4,
        status="native",
        native_markets=("spot",),
        listed_market_types=("spot",),
        public_rest_api=True,
        public_websocket_api=True,
        archive="api-only",
        recommended_source="Official public Exchange API for spot candles and live feeds; no first-class public bulk archive in this package metadata.",
        api_docs_url="https://docs.cdp.coinbase.com/exchange/introduction/welcome",
        notes="Good regulated spot source; keep derivative/international APIs as separate future adapters.",
    ),
    ExchangeInfo(
        id="kraken",
        name="Kraken",
        rank=5,
        status="native",
        native_markets=("spot", "margin", "linear", "inverse", "delivery_futures"),
        listed_market_types=(
            "spot",
            "margin",
            "usdt_futures",
            "coin_futures",
            "delivery_futures",
        ),
        public_rest_api=True,
        public_websocket_api=True,
        archive="official-partial",
        recommended_source="Official downloadable OHLCVT/history where available; REST OHLC is suitable for recent windows only.",
        api_docs_url="https://docs.kraken.com/api/docs/category/rest-api/market-data/",
        archive_docs_url="https://support.kraken.com/articles/360047124832-downloadable-historical-ohlcvt-open-high-low-close-volume-trades-data",
        notes="REST OHLC endpoint is capped, so deep history should prefer downloadable files or a dedicated archive source.",
    ),
    ExchangeInfo(
        id="kucoin",
        name="KuCoin",
        rank=6,
        status="native",
        native_markets=("spot", "margin", "linear", "inverse", "delivery_futures"),
        listed_market_types=(
            "spot",
            "margin",
            "usdt_futures",
            "coin_futures",
            "delivery_futures",
        ),
        public_rest_api=True,
        public_websocket_api=True,
        archive="api-only",
        recommended_source="Official REST/WebSocket for candles/live data; page by time for history.",
        api_docs_url="https://www.kucoin.com/docs-new/introduction",
        notes="Useful broad altcoin venue; futures kline endpoint is public and paginated.",
    ),
    ExchangeInfo(
        id="bitget",
        name="Bitget",
        rank=7,
        status="native",
        native_markets=("spot", "margin", "linear", "inverse", "delivery_futures"),
        listed_market_types=(
            "spot",
            "margin",
            "usdt_futures",
            "coin_futures",
            "delivery_futures",
        ),
        public_rest_api=True,
        public_websocket_api=True,
        archive="api-only",
        recommended_source="Official historical-candle REST endpoints by product type; split long ranges by documented granularity limits.",
        api_docs_url="https://www.bitget.com/api-doc/uta/public/Get-Candle-Data",
        notes="Good derivatives coverage; historical endpoints enforce per-request limits.",
    ),
    ExchangeInfo(
        id="gateio",
        name="Gate.io",
        rank=8,
        status="native",
        native_markets=("spot", "margin", "linear", "inverse", "delivery_futures"),
        listed_market_types=(
            "spot",
            "margin",
            "usdt_futures",
            "coin_futures",
            "delivery_futures",
            "options",
        ),
        public_rest_api=True,
        public_websocket_api=True,
        archive="third-party",
        recommended_source="Official API for current/paginated market data; licensed third-party archives for complete deep history.",
        api_docs_url="https://www.gate.com/docs/developers/apiv4/en/",
        notes="Broad product surface; prefer explicit adapter tests before enabling live fetch.",
    ),
    ExchangeInfo(
        id="htx",
        name="HTX / Huobi",
        rank=9,
        status="native",
        native_markets=("spot", "margin", "linear", "inverse", "delivery_futures"),
        listed_market_types=(
            "spot",
            "margin",
            "usdt_futures",
            "coin_futures",
            "delivery_futures",
        ),
        public_rest_api=True,
        public_websocket_api=True,
        archive="official-partial",
        recommended_source="Official REST/WebSocket and HTX download-history pages where available; third-party archive for complete replay datasets.",
        api_docs_url="https://www.htx.com/en-us/opend/newApiPages/",
        archive_docs_url="https://www.htx.com/en-us/opend/newApiPages/",
        notes="Former Huobi naming appears in third-party archives and some legacy endpoints.",
    ),
    ExchangeInfo(
        id="mexc",
        name="MEXC",
        rank=10,
        status="native",
        native_markets=("spot", "linear", "inverse"),
        listed_market_types=("spot", "usdt_futures", "coin_futures"),
        public_rest_api=True,
        public_websocket_api=True,
        archive="official-partial",
        recommended_source="Official spot historical downloads when sufficient; REST kline/futures market endpoints for recent and segmented history.",
        api_docs_url="https://www.mexc.com/api-docs/spot-v3/market-data-endpoints/klinecandlestick-data",
        archive_docs_url="https://www.mexc.com/api-docs/spot-v3/market-data-endpoints/download-historical-market-data",
        notes="Spot historical download is documented from 2023; futures market endpoints are public but should be range-paginated.",
    ),
)

_EXCHANGE_BY_ID = {exchange.id: exchange for exchange in EXCHANGES}
_MARKET_TYPE_BY_ID = {market.id: market for market in MARKET_TYPES}
_ALIAS_TO_MARKET_TYPE = {
    alias: market.id
    for market in MARKET_TYPES
    for alias in (market.id, *market.aliases)
}


def normalize_exchange_id(exchange: str) -> str:
    normalized = exchange.strip().lower().replace("_", "-")
    aliases = {
        "binanceusdm": "binance",
        "bybit-v5": "bybit",
        "coinbase-exchange": "coinbase",
        "coinbasepro": "coinbase",
        "gate": "gateio",
        "gate-io": "gateio",
        "huobi": "htx",
        "mexc-global": "mexc",
    }
    return aliases.get(normalized, normalized)


def normalize_market_type(market_type: str) -> MarketType:
    normalized = market_type.strip().lower().replace("_", "-")
    normalized = normalized.replace(" ", "-")
    try:
        return _ALIAS_TO_MARKET_TYPE[normalized]
    except KeyError as exc:
        raise KeyError(f"Unknown market type: {market_type}") from exc


def list_exchanges(*, native_only: bool = False) -> tuple[ExchangeInfo, ...]:
    exchanges = EXCHANGES
    if native_only:
        exchanges = tuple(exchange for exchange in exchanges if exchange.native_adapter)
    return tuple(sorted(exchanges, key=lambda exchange: exchange.rank))


def get_exchange(exchange: str) -> ExchangeInfo:
    exchange_id = normalize_exchange_id(exchange)
    try:
        return _EXCHANGE_BY_ID[exchange_id]
    except KeyError as exc:
        raise KeyError(f"Unknown exchange: {exchange}") from exc


def list_market_types(exchange: str | None = None) -> tuple[MarketTypeInfo, ...]:
    if exchange is None:
        return MARKET_TYPES
    info = get_exchange(exchange)
    return tuple(
        _MARKET_TYPE_BY_ID[market_type] for market_type in info.listed_market_types
    )


def exchange_payloads(*, native_only: bool = False) -> list[dict[str, object]]:
    return [exchange.to_dict() for exchange in list_exchanges(native_only=native_only)]


def market_type_payloads(exchange: str | None = None) -> list[dict[str, object]]:
    return [market_type.to_dict() for market_type in list_market_types(exchange)]
