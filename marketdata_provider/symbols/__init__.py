from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Sequence

from marketdata_provider.errors import (
    MDInvalidExchangeResponse,
    MDNetworkUnavailable,
    MDSymbolAmbiguous,
    MDSymbolUnsupported,
    MDUnsupportedFeature,
)

DEFAULT_STABLE_QUOTE_ASSETS: tuple[str, ...] = (
    "USDT",
    "USDC",
    "FDUSD",
    "BUSD",
    "TUSD",
    "USDP",
    "DAI",
    "USD",
)


@dataclass(frozen=True, slots=True)
class NormalizedSymbol:
    exchange: str
    market: str
    base_symbol: str
    tv_symbol: str
    exchange_symbol: str
    is_perpetual: bool


@dataclass(frozen=True, slots=True)
class SymbolInfo:
    exchange: str
    market: str
    symbol: str
    base_asset: str
    quote_asset: str
    active: bool = True
    contract_type: str | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "exchange": self.exchange,
            "market": self.market,
            "symbol": self.symbol,
            "base_asset": self.base_asset,
            "quote_asset": self.quote_asset,
            "active": self.active,
            "contract_type": self.contract_type,
        }


_EXCHANGES = {
    "BINANCE",
    "BYBIT",
    "OKX",
    "COINBASE",
    "KRAKEN",
    "KUCOIN",
    "BITGET",
    "GATEIO",
    "HTX",
    "MEXC",
}
_MARKET_ALIASES = {
    "BINANCE": {
        "spot": "spot",
        "margin": "spot",
        "cash": "spot",
        "usdm": "usdm",
        "linear": "usdm",
        "futures": "usdm",
        "usdt_futures": "usdm",
        "usdt-futures": "usdm",
        "usdc_futures": "usdm",
        "usdc-futures": "usdm",
        "swap": "usdm",
        "coinm": "coinm",
        "inverse": "coinm",
        "coin_futures": "coinm",
        "coin-futures": "coinm",
        "delivery": "coinm",
        "delivery_futures": "coinm",
        "delivery-futures": "coinm",
    },
    "BYBIT": {
        "spot": "spot",
        "margin": "spot",
        "linear": "linear",
        "usdm": "linear",
        "futures": "linear",
        "usdt_futures": "linear",
        "usdt-futures": "linear",
        "usdc_futures": "linear",
        "usdc-futures": "linear",
        "swap": "linear",
        "inverse": "inverse",
        "coinm": "inverse",
        "coin_futures": "inverse",
        "coin-futures": "inverse",
        "delivery": "inverse",
        "delivery_futures": "inverse",
        "delivery-futures": "inverse",
    },
}
_DERIVATIVE_MARKET_ALIASES = {
    "linear": "linear",
    "usdm": "linear",
    "futures": "linear",
    "usdt_futures": "linear",
    "usdt-futures": "linear",
    "usdc_futures": "linear",
    "usdc-futures": "linear",
    "swap": "linear",
    "inverse": "inverse",
    "coinm": "inverse",
    "coin_futures": "inverse",
    "coin-futures": "inverse",
    "delivery": "inverse",
    "delivery_futures": "delivery_futures",
    "delivery-futures": "delivery_futures",
    "dated_futures": "delivery_futures",
    "dated-futures": "delivery_futures",
}
_MARGIN_MARKET_ALIASES = {"spot": "spot", "cash": "spot", "margin": "margin"}
for _exchange in ("OKX", "KRAKEN", "KUCOIN", "BITGET", "GATEIO", "HTX"):
    _MARKET_ALIASES[_exchange] = {
        **_MARGIN_MARKET_ALIASES,
        **_DERIVATIVE_MARKET_ALIASES,
    }
_MARKET_ALIASES["COINBASE"] = {"spot": "spot", "cash": "spot"}
_MARKET_ALIASES["MEXC"] = {"spot": "spot", "cash": "spot", **_DERIVATIVE_MARKET_ALIASES}


def normalize_symbol(
    symbol: str, *, exchange: str | None = None, market: str | None = None
) -> NormalizedSymbol:
    raw = symbol.strip().upper()
    if not raw:
        raise MDSymbolUnsupported("Empty symbol")
    parsed_exchange = None
    if ":" in raw:
        parsed_exchange, raw = raw.split(":", 1)
        if parsed_exchange not in _EXCHANGES:
            raise MDSymbolUnsupported(f"Unsupported exchange: {parsed_exchange}")
    ex = (exchange or parsed_exchange or "").upper()
    if not ex:
        raise MDSymbolAmbiguous(
            "Symbol without exchange is ambiguous; pass exchange='BINANCE' or use BINANCE:BTCUSDT"
        )
    if ex not in _EXCHANGES:
        raise MDSymbolUnsupported(f"Unsupported exchange: {ex}")
    is_perp = raw.endswith(".P")
    contract_symbol = raw[:-2] if is_perp else raw
    requested_market = (
        (market or "").strip().lower().replace(" ", "_").replace("-", "_")
    )
    if requested_market:
        mkt = _provider_market(ex, requested_market)
    elif is_perp:
        mkt = "usdm" if ex == "BINANCE" else "linear"
    else:
        mkt = "spot"
    quote = quote_asset(contract_symbol)
    base = contract_symbol[: -len(quote)] if quote else contract_symbol
    tv_suffix = ".P" if is_perp or mkt in {"usdm", "linear", "coinm", "inverse"} else ""
    tv = f"{ex}:{contract_symbol}{tv_suffix}"
    return NormalizedSymbol(
        ex.lower(),
        mkt,
        base,
        tv,
        contract_symbol,
        mkt in {"usdm", "linear", "coinm", "inverse"},
    )


def _provider_market(exchange: str, market: str) -> str:
    normalized = market.lower().replace("-", "_")
    try:
        return _MARKET_ALIASES[exchange][normalized]
    except KeyError as exc:
        raise MDSymbolUnsupported(f"Unsupported {exchange} market: {market}") from exc


def quote_asset(
    symbol: str, quote_assets: Sequence[str] = DEFAULT_STABLE_QUOTE_ASSETS
) -> str | None:
    raw = symbol.strip().upper()
    if ":" in raw:
        raw = raw.split(":", 1)[1]
    if raw.endswith(".P"):
        raw = raw[:-2]
    root = raw.split("_", 1)[0]
    for quote in sorted((q.upper() for q in quote_assets), key=len, reverse=True):
        if root.endswith(quote):
            return quote
    return None


def is_stable_quoted(
    symbol: str, quote_assets: Sequence[str] = DEFAULT_STABLE_QUOTE_ASSETS
) -> bool:
    return quote_asset(symbol, quote_assets) is not None


def filter_symbol_infos(
    symbols: Iterable[SymbolInfo],
    *,
    query: str = "",
    stable_quotes_only: bool = True,
    stable_quote_assets: Sequence[str] = DEFAULT_STABLE_QUOTE_ASSETS,
    limit: int | None = None,
) -> list[SymbolInfo]:
    q = query.strip().upper()
    out: list[SymbolInfo] = []
    for item in symbols:
        if stable_quotes_only and item.quote_asset.upper() not in {
            a.upper() for a in stable_quote_assets
        }:
            continue
        if (
            q
            and q not in item.symbol.upper()
            and q not in item.base_asset.upper()
            and q not in item.quote_asset.upper()
        ):
            continue
        out.append(item)
        if limit is not None and len(out) >= limit:
            break
    return out


def normalize_binance_exchange_info_symbols(
    payload: Any,
    *,
    market: str,
    stable_quotes_only: bool = True,
    stable_quote_assets: Sequence[str] = DEFAULT_STABLE_QUOTE_ASSETS,
    query: str = "",
    limit: int | None = None,
) -> list[SymbolInfo]:
    if not isinstance(payload, dict) or not isinstance(payload.get("symbols"), list):
        raise MDInvalidExchangeResponse("Binance exchangeInfo payload missing symbols")
    items: list[SymbolInfo] = []
    for row in payload["symbols"]:
        if not isinstance(row, dict):
            continue
        status = str(row.get("status") or row.get("contractStatus") or "").upper()
        if status != "TRADING":
            continue
        symbol = str(row.get("symbol") or "").upper()
        base = str(row.get("baseAsset") or "").upper()
        quote = str(row.get("quoteAsset") or quote_asset(symbol) or "").upper()
        if not symbol or not base or not quote:
            continue
        items.append(SymbolInfo("binance", market, symbol, base, quote, active=True))
    return filter_symbol_infos(
        items,
        query=query,
        stable_quotes_only=stable_quotes_only,
        stable_quote_assets=stable_quote_assets,
        limit=limit,
    )


def normalize_bybit_instruments_info_symbols(
    payload: Any,
    *,
    market: str,
    stable_quotes_only: bool = True,
    stable_quote_assets: Sequence[str] = DEFAULT_STABLE_QUOTE_ASSETS,
    query: str = "",
    limit: int | None = None,
) -> list[SymbolInfo]:
    if not isinstance(payload, dict):
        raise MDInvalidExchangeResponse(
            "Bybit instruments-info payload must be an object"
        )
    rows = payload.get("result", {}).get("list")
    if not isinstance(rows, list):
        raise MDInvalidExchangeResponse(
            "Bybit instruments-info payload missing result.list"
        )
    items: list[SymbolInfo] = []
    for row in rows:
        if not isinstance(row, dict) or row.get("status") != "Trading":
            continue
        symbol = str(row.get("symbol") or "").upper()
        base = str(row.get("baseCoin") or "").upper()
        quote = str(row.get("quoteCoin") or quote_asset(symbol) or "").upper()
        if not symbol or not base or not quote:
            continue
        items.append(SymbolInfo("bybit", market, symbol, base, quote, active=True))
    return filter_symbol_infos(
        items,
        query=query,
        stable_quotes_only=stable_quotes_only,
        stable_quote_assets=stable_quote_assets,
        limit=limit,
    )


def _normalize_asset(raw: str) -> str:
    return {"XBT": "BTC"}.get(raw, raw)


def _symbol_tuple(
    symbol: object, base: object, quote: object
) -> tuple[str, str, str] | None:
    symbol_text = str(symbol or "").strip().upper()
    base_text = _normalize_asset(str(base or "").strip().upper())
    quote_text = _normalize_asset(str(quote or "").strip().upper())
    if not quote_text:
        quote_text = quote_asset(symbol_text) or ""
    if not base_text and quote_text and symbol_text.endswith(quote_text):
        base_text = symbol_text[: -len(quote_text)]
    if not symbol_text or not base_text or not quote_text:
        return None
    return symbol_text, base_text, quote_text


from marketdata_provider.symbols.public_markets import (  # noqa: E402
    _PUBLIC_SPOT_SYMBOL_ENDPOINTS,
    _QUERY_FIRST_PUBLIC_SPOT_EXCHANGES,
    _public_symbol_endpoint,
    normalize_public_market_symbols,
    normalize_public_spot_symbols,
)


def _query_base_asset(query: str) -> str:
    raw = query.strip().upper().replace("/", "-").replace("_", "-")
    if not raw:
        return ""
    if "-" in raw:
        return raw.split("-", 1)[0]
    quote = quote_asset(raw)
    if quote and len(raw) > len(quote):
        return raw[: -len(quote)]
    return raw


def _candidate_spot_symbol(exchange: str, base: str, quote: str) -> str:
    if exchange in {"coinbase", "okx", "kucoin"}:
        return f"{base}-{quote}"
    if exchange == "gateio":
        return f"{base}_{quote}"
    return f"{base}{quote}"


def _normalize_query_first_payload(
    exchange: str, symbol: str, base: str, quote: str, payload: Any
) -> list[SymbolInfo]:
    if exchange == "coinbase":
        return normalize_public_spot_symbols(
            exchange, [payload], stable_quotes_only=False
        )
    if exchange == "kraken":
        return normalize_public_spot_symbols(
            exchange, payload, stable_quotes_only=False
        )
    if exchange == "gateio":
        return normalize_public_spot_symbols(
            exchange, [payload], stable_quotes_only=False
        )
    if exchange == "htx":
        if (
            not isinstance(payload, dict)
            or payload.get("status") != "ok"
            or not payload.get("tick")
        ):
            return []
        parsed = _symbol_tuple(symbol, base, quote)
        return [SymbolInfo(exchange, "spot", *parsed, active=True)] if parsed else []
    return []


def _query_first_url(exchange: str, symbol: str) -> tuple[str, dict[str, object]]:
    if exchange == "coinbase":
        return f"https://api.exchange.coinbase.com/products/{symbol}", {}
    if exchange == "kraken":
        return "https://api.kraken.com/0/public/AssetPairs", {"pair": symbol}
    if exchange == "gateio":
        return f"https://api.gateio.ws/api/v4/spot/currency_pairs/{symbol}", {}
    if exchange == "htx":
        return "https://api.huobi.pro/market/detail/merged", {"symbol": symbol.lower()}
    raise MDUnsupportedFeature(
        f"Query-first discovery unsupported for exchange: {exchange}"
    )


def _search_public_spot_symbols_by_query(
    exchange: str,
    query: str,
    *,
    timeout: float,
    user_agent: str,
    quote_assets: Sequence[str],
    result_limit: int,
    httpx: Any,
) -> list[SymbolInfo]:
    base = _query_base_asset(query)
    if not base:
        return []
    lookup_base = "XBT" if exchange == "kraken" and base == "BTC" else base
    items: list[SymbolInfo] = []
    for quote in quote_assets:
        symbol = _candidate_spot_symbol(exchange, lookup_base, quote.upper())
        url, params = _query_first_url(exchange, symbol)
        try:
            payload = _http_get_json(
                url,
                params=params,
                timeout=timeout,
                user_agent=user_agent,
                httpx=httpx,
            )
        except MDNetworkUnavailable:
            continue
        for item in _normalize_query_first_payload(
            exchange, symbol, base, quote.upper(), payload
        ):
            if item.symbol not in {existing.symbol for existing in items}:
                items.append(item)
        if len(items) >= result_limit:
            break
    return items[:result_limit]


def search_symbols(
    exchange: str,
    market: str,
    query: str = "",
    *,
    config: Any | None = None,
    stable_quotes_only: bool | None = None,
    stable_quote_assets: Sequence[str] | None = None,
    limit: int | None = None,
    timeout: float = 10.0,
) -> list[SymbolInfo]:
    from marketdata_provider.config import MarketDataConfig
    import httpx

    cfg = config or MarketDataConfig()
    symbols_cfg = cfg.symbols
    stable_only = (
        symbols_cfg.stable_quotes_only
        if stable_quotes_only is None
        else stable_quotes_only
    )
    quote_assets = stable_quote_assets or symbols_cfg.stable_quote_assets
    result_limit = limit if limit is not None else symbols_cfg.max_results
    ex = exchange.strip().lower()
    if ex == "binance":
        provider_market = _provider_market("BINANCE", market)
        if provider_market == "spot":
            base_url, endpoint = cfg.binance.spot_base_url, "/api/v3/exchangeInfo"
        elif provider_market == "usdm":
            base_url, endpoint = cfg.binance.usdm_base_url, "/fapi/v1/exchangeInfo"
        elif provider_market == "coinm":
            base_url, endpoint = cfg.binance.coinm_base_url, "/dapi/v1/exchangeInfo"
        else:
            raise MDUnsupportedFeature(f"Unsupported Binance symbol market: {market}")
        payload = _http_get_json(
            base_url + endpoint,
            timeout=timeout,
            user_agent=cfg.binance.user_agent,
            httpx=httpx,
        )
        return normalize_binance_exchange_info_symbols(
            payload,
            market=provider_market,
            query=query,
            stable_quotes_only=stable_only,
            stable_quote_assets=quote_assets,
            limit=result_limit,
        )
    if ex == "bybit":
        provider_market = _provider_market("BYBIT", market)
        payload = _http_get_json(
            cfg.bybit.base_url + "/v5/market/instruments-info",
            params={"category": provider_market},
            timeout=timeout,
            user_agent=cfg.bybit.user_agent,
            httpx=httpx,
        )
        return normalize_bybit_instruments_info_symbols(
            payload,
            market=provider_market,
            query=query,
            stable_quotes_only=stable_only,
            stable_quote_assets=quote_assets,
            limit=result_limit,
        )
    if ex in _PUBLIC_SPOT_SYMBOL_ENDPOINTS:
        provider_market = _provider_market(ex.upper(), market)
        if (
            provider_market == "spot"
            and ex in _QUERY_FIRST_PUBLIC_SPOT_EXCHANGES
            and query
        ):
            items = _search_public_spot_symbols_by_query(
                ex,
                query,
                timeout=timeout,
                user_agent=cfg.binance.user_agent,
                quote_assets=quote_assets,
                result_limit=result_limit,
                httpx=httpx,
            )
            return items
        url, params = _public_symbol_endpoint(ex, provider_market)
        payload = _http_get_json(
            url,
            params=params,
            timeout=timeout,
            user_agent=cfg.binance.user_agent,
            httpx=httpx,
        )
        return normalize_public_market_symbols(
            ex,
            provider_market,
            payload,
            query=query,
            stable_quotes_only=stable_only,
            stable_quote_assets=quote_assets,
            limit=result_limit,
        )
    raise MDUnsupportedFeature(f"Symbol discovery unsupported for exchange: {exchange}")


def _http_get_json(
    url: str,
    *,
    params: dict[str, object] | None = None,
    timeout: float,
    user_agent: str,
    httpx: Any,
) -> Any:
    try:
        with httpx.Client(
            timeout=timeout, headers={"User-Agent": user_agent}, trust_env=False
        ) as client:
            response = client.get(url, params=params or {})
            response.raise_for_status()
            return response.json()
    except Exception as exc:  # pragma: no cover - exercised by integration callers.
        raise MDNetworkUnavailable(
            "Symbol discovery request failed", details={"url": url, "error": str(exc)}
        ) from exc
