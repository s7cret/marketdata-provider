from __future__ import annotations

from typing import Any, Iterable, Sequence

from marketdata_provider.errors import MDUnsupportedFeature
from marketdata_provider.symbols import (
    DEFAULT_STABLE_QUOTE_ASSETS,
    SymbolInfo,
    _symbol_tuple,
    filter_symbol_infos,
)

_PUBLIC_SPOT_SYMBOL_ENDPOINTS: dict[str, tuple[str, dict[str, object]]] = {
    "okx": ("https://www.okx.com/api/v5/public/instruments", {"instType": "SPOT"}),
    "coinbase": ("https://api.exchange.coinbase.com/products", {}),
    "kraken": ("https://api.kraken.com/0/public/AssetPairs", {}),
    "kucoin": ("https://api.kucoin.com/api/v2/symbols", {}),
    "bitget": ("https://api.bitget.com/api/v2/spot/public/symbols", {}),
    "gateio": ("https://api.gateio.ws/api/v4/spot/currency_pairs", {}),
    "htx": ("https://api.huobi.pro/v2/settings/common/symbols", {}),
    "mexc": ("https://api.mexc.com/api/v3/exchangeInfo", {}),
}
_QUERY_FIRST_PUBLIC_SPOT_EXCHANGES = {"coinbase", "kraken", "gateio", "htx"}


def _public_symbol_endpoint(
    exchange: str, market: str
) -> tuple[str, dict[str, object]]:
    if market not in {"spot", "margin", "linear", "inverse", "delivery_futures"}:
        raise MDUnsupportedFeature(
            f"Symbol discovery unsupported for {exchange} market: {market}"
        )
    if market in {"spot", "margin"}:
        if exchange == "okx":
            return "https://www.okx.com/api/v5/public/instruments", {
                "instType": "MARGIN" if market == "margin" else "SPOT"
            }
        if exchange == "kucoin" and market == "margin":
            return "https://api.kucoin.com/api/v3/margin/symbols", {}
        if exchange == "bitget" and market == "margin":
            return "https://api.bitget.com/api/v2/margin/currencies", {}
        if exchange == "gateio" and market == "margin":
            return "https://api.gateio.ws/api/v4/margin/currency_pairs", {}
        return _PUBLIC_SPOT_SYMBOL_ENDPOINTS[exchange]
    if exchange == "okx":
        return "https://www.okx.com/api/v5/public/instruments", {
            "instType": "FUTURES" if market == "delivery_futures" else "SWAP"
        }
    if exchange == "kraken":
        return "https://futures.kraken.com/derivatives/api/v3/instruments", {}
    if exchange == "kucoin":
        return "https://api-futures.kucoin.com/api/v1/contracts/active", {}
    if exchange == "bitget":
        product = "USDT-FUTURES" if market == "linear" else "COIN-FUTURES"
        return "https://api.bitget.com/api/v2/mix/market/contracts", {
            "productType": product
        }
    if exchange == "gateio":
        settlement = "usdt" if market == "linear" else "btc"
        namespace = "delivery" if market == "delivery_futures" else "futures"
        return f"https://api.gateio.ws/api/v4/{namespace}/{settlement}/contracts", {}
    if exchange == "htx":
        if market == "linear":
            return "https://api.hbdm.com/linear-swap-api/v1/swap_contract_info", {
                "business_type": "swap"
            }
        if market == "delivery_futures":
            return "https://api.hbdm.com/linear-swap-api/v1/swap_contract_info", {
                "business_type": "futures"
            }
        return "https://api.hbdm.com/swap-api/v1/swap_contract_info", {}
    if exchange == "mexc":
        return "https://contract.mexc.com/api/v1/contract/detail", {}
    raise MDUnsupportedFeature(
        f"Symbol discovery unsupported for {exchange} market: {market}"
    )


def _is_disabled_status(value: object) -> bool:
    status = str(value or "").strip().upper()
    return status in {
        "BREAK",
        "DISABLED",
        "OFFLINE",
        "SETTLED",
        "SUSPENDED",
        "HALT",
        "CANCEL_ONLY",
    }


def normalize_public_spot_symbols(
    exchange: str,
    payload: Any,
    *,
    query: str = "",
    stable_quotes_only: bool = True,
    stable_quote_assets: Sequence[str] = DEFAULT_STABLE_QUOTE_ASSETS,
    limit: int | None = None,
) -> list[SymbolInfo]:
    ex = exchange.lower()
    rows: Iterable[Any]
    if ex == "okx":
        rows = payload.get("data", []) if isinstance(payload, dict) else []
    elif ex == "kraken":
        result = payload.get("result", {}) if isinstance(payload, dict) else {}
        rows = result.values() if isinstance(result, dict) else []
    elif ex == "kucoin":
        rows = payload.get("data", []) if isinstance(payload, dict) else []
    elif ex == "bitget":
        rows = payload.get("data", []) if isinstance(payload, dict) else []
    elif ex == "htx":
        rows = payload.get("data", []) if isinstance(payload, dict) else []
    elif ex == "mexc":
        rows = payload.get("symbols", []) if isinstance(payload, dict) else []
    else:
        rows = payload if isinstance(payload, list) else []

    items: list[SymbolInfo] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        parsed = _parse_public_spot_symbol_row(ex, row)
        if parsed is None:
            continue
        symbol, base, quote = parsed
        items.append(SymbolInfo(ex, "spot", symbol, base, quote, active=True))
    return filter_symbol_infos(
        items,
        query=query,
        stable_quotes_only=stable_quotes_only,
        stable_quote_assets=stable_quote_assets,
        limit=limit,
    )


def normalize_public_market_symbols(
    exchange: str,
    market: str,
    payload: Any,
    *,
    query: str = "",
    stable_quotes_only: bool = True,
    stable_quote_assets: Sequence[str] = DEFAULT_STABLE_QUOTE_ASSETS,
    limit: int | None = None,
) -> list[SymbolInfo]:
    ex = exchange.lower()
    provider_market = market.lower()
    if provider_market in {"spot", "margin"}:
        spot_items = normalize_public_spot_symbols(
            ex,
            payload,
            query=query,
            stable_quotes_only=stable_quotes_only,
            stable_quote_assets=stable_quote_assets,
            limit=limit,
        )
        if provider_market == "spot":
            return spot_items
        return [
            SymbolInfo(
                item.exchange,
                "margin",
                item.symbol,
                item.base_asset,
                item.quote_asset,
                item.active,
                item.contract_type,
            )
            for item in spot_items
        ]

    rows = _public_market_rows(ex, payload)
    items: list[SymbolInfo] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        parsed = _parse_public_market_symbol_row(ex, provider_market, row)
        if parsed is None:
            continue
        symbol, base, quote, contract_type = parsed
        items.append(
            SymbolInfo(
                ex,
                provider_market,
                symbol,
                base,
                quote,
                active=True,
                contract_type=contract_type,
            )
        )
    return filter_symbol_infos(
        items,
        query=query,
        stable_quotes_only=stable_quotes_only,
        stable_quote_assets=stable_quote_assets,
        limit=limit,
    )


def _public_market_rows(exchange: str, payload: Any) -> Iterable[Any]:
    if exchange in {"okx", "bitget", "htx"}:
        rows = payload.get("data", []) if isinstance(payload, dict) else []
    elif exchange == "kraken":
        rows = payload.get("instruments", []) if isinstance(payload, dict) else []
    elif exchange == "kucoin":
        rows = payload.get("data", []) if isinstance(payload, dict) else []
    elif exchange == "mexc":
        rows = payload.get("data", []) if isinstance(payload, dict) else []
    else:
        rows = payload if isinstance(payload, list) else []
    return rows if isinstance(rows, list) else []


def _parse_public_market_symbol_row(
    exchange: str, market: str, row: dict[str, Any]
) -> tuple[str, str, str, str | None] | None:
    if exchange == "okx":
        if str(row.get("state") or "").lower() not in {"live", ""}:
            return None
        ct_type = str(row.get("ctType") or "").lower()
        if market in {"linear", "inverse"} and ct_type and ct_type != market:
            return None
        parsed = _symbol_tuple(
            row.get("instId"),
            row.get("baseCcy"),
            row.get("quoteCcy") or row.get("settleCcy"),
        )
        if parsed is None:
            family = _symbol_tuple_from_delimited(str(row.get("instFamily") or ""))
            if family is not None:
                parsed = (
                    str(row.get("instId") or family[0]).strip().upper(),
                    family[1],
                    family[2],
                )
        if parsed is None:
            parsed = _symbol_tuple_from_delimited(str(row.get("instId") or ""))
        return (*parsed, ct_type or market) if parsed else None
    if exchange == "kraken":
        if row.get("tradeable") is False or _is_disabled_status(row.get("status")):
            return None
        symbol = str(row.get("symbol") or "").upper()
        kind = str(row.get("type") or "").lower()
        contract_market = (
            "inverse"
            if kind == "futures_inverse" or symbol.startswith("PI_")
            else "linear"
        )
        if symbol.startswith(("FF_", "FI_")):
            contract_market = "delivery_futures"
        if market != contract_market:
            return None
        parsed = _symbol_tuple(symbol, row.get("base"), row.get("quote"))
        return (*parsed, contract_market) if parsed else None
    if exchange == "kucoin":
        if _is_disabled_status(row.get("status")):
            return None
        is_inverse = bool(row.get("isInverse"))
        expire = row.get("expireDate")
        contract_market = (
            "delivery_futures"
            if expire not in {None, "", 0}
            else ("inverse" if is_inverse else "linear")
        )
        if market != contract_market:
            return None
        parsed = _symbol_tuple(
            row.get("symbol"), row.get("baseCurrency"), row.get("quoteCurrency")
        )
        return (*parsed, contract_market) if parsed else None
    if exchange == "bitget":
        if _is_disabled_status(row.get("status")):
            return None
        parsed = _symbol_tuple(
            row.get("symbol"), row.get("baseCoin"), row.get("quoteCoin")
        )
        return (*parsed, market) if parsed else None
    if exchange == "gateio":
        if row.get("in_delisting") is True or _is_disabled_status(
            row.get("trade_status")
        ):
            return None
        symbol = str(row.get("name") or row.get("id") or "").upper()
        parsed = _symbol_tuple(
            symbol, row.get("base"), row.get("quote")
        ) or _symbol_tuple_from_delimited(symbol)
        return (*parsed, market) if parsed else None
    if exchange == "htx":
        if _is_disabled_status(row.get("contract_status")):
            return None
        symbol = str(row.get("contract_code") or "").upper()
        parsed = _symbol_tuple(
            symbol, row.get("symbol"), row.get("trade_partition")
        ) or _symbol_tuple_from_delimited(symbol)
        return (*parsed, market) if parsed else None
    if exchange == "mexc":
        if (
            str(row.get("state") or "0") not in {"0", ""}
            or row.get("apiAllowed") is False
        ):
            return None
        settle = str(row.get("settleCoin") or "").upper()
        contract_market = "linear" if settle in {"USDT", "USDC"} else "inverse"
        if market != contract_market:
            return None
        parsed = _symbol_tuple(
            row.get("symbol"), row.get("baseCoin"), row.get("quoteCoin")
        ) or _symbol_tuple_from_delimited(str(row.get("symbol") or ""))
        return (*parsed, contract_market) if parsed else None
    return None


def _symbol_tuple_from_delimited(symbol: str) -> tuple[str, str, str] | None:
    raw = symbol.strip().upper()
    for sep in ("_", "-"):
        parts = raw.split(sep)
        if len(parts) >= 2 and parts[0] and parts[1]:
            return raw, parts[0], parts[1]
    return None


def _parse_public_spot_symbol_row(
    exchange: str, row: dict[str, Any]
) -> tuple[str, str, str] | None:
    if exchange == "okx":
        if str(row.get("state") or "").lower() not in {"live", ""}:
            return None
        return _symbol_tuple(row.get("instId"), row.get("baseCcy"), row.get("quoteCcy"))
    if exchange == "coinbase":
        if row.get("trading_disabled") is True or _is_disabled_status(
            row.get("status")
        ):
            return None
        return _symbol_tuple(
            row.get("id"), row.get("base_currency"), row.get("quote_currency")
        )
    if exchange == "kraken":
        if _is_disabled_status(row.get("status")):
            return None
        symbol = row.get("altname")
        wsname = str(row.get("wsname") or "")
        base = row.get("base")
        quote = row.get("quote")
        if "/" in wsname:
            base, quote = wsname.split("/", 1)
        return _symbol_tuple(symbol, base, quote)
    if exchange == "kucoin":
        if row.get("enableTrading") is False or _is_disabled_status(row.get("status")):
            return None
        return _symbol_tuple(
            row.get("symbol"), row.get("baseCurrency"), row.get("quoteCurrency")
        )
    if exchange == "bitget":
        if _is_disabled_status(row.get("status")):
            return None
        return _symbol_tuple(
            row.get("symbol"), row.get("baseCoin"), row.get("quoteCoin")
        )
    if exchange == "gateio":
        if str(row.get("trade_status") or "").lower() not in {"tradable", ""}:
            return None
        return _symbol_tuple(row.get("id"), row.get("base"), row.get("quote"))
    if exchange == "htx":
        if str(row.get("state") or "").lower() not in {"online", ""}:
            return None
        return _symbol_tuple(row.get("sc"), row.get("bcdn"), row.get("qcdn"))
    if exchange == "mexc":
        if _is_disabled_status(row.get("status")):
            return None
        return _symbol_tuple(
            row.get("symbol"), row.get("baseAsset"), row.get("quoteAsset")
        )
    return None
