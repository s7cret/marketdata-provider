from __future__ import annotations

import asyncio
import importlib.util
import json
import time
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Literal

from marketdata_provider.errors import (
    MDNetworkUnavailable,
    MDSymbolUnsupported,
    MDUnsupportedFeature,
)
from marketdata_provider.streaming.kline import (
    KlineUpdate,
    bybit_topic,
    normalize_binance_kline,
    normalize_bybit_kline,
)
from marketdata_provider.streaming.supervisor import require_live_stream_enabled
from marketdata_provider.timeframes import to_binance_interval


@dataclass(frozen=True, slots=True)
class StreamDiagnostic:
    code: str
    message: str
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class LiveKlineEvent:
    update: KlineUpdate
    raw_payload: dict[str, Any]
    diagnostic: StreamDiagnostic | None = None


class CoalescingKlineQueue:
    """Bounded queue with per-candle coalescing and explicit drop diagnostics."""

    def __init__(self, maxsize: int = 1024):
        if maxsize < 1:
            raise ValueError("maxsize must be >= 1")
        self.maxsize = maxsize
        self._items: dict[tuple[str, str, str, str, str, int], KlineUpdate] = {}
        self.dropped = 0
        self.coalesced = 0
        self.diagnostics: list[StreamDiagnostic] = []

    def put(self, update: KlineUpdate) -> None:
        key = (
            update.exchange,
            update.market,
            update.symbol,
            update.timeframe,
            update.source_kind,
            update.open_time,
        )
        if key in self._items:
            self._items[key] = update
            self.coalesced += 1
            return
        if len(self._items) >= self.maxsize:
            oldest = next(iter(self._items))
            self._items.pop(oldest)
            self.dropped += 1
            self.diagnostics.append(
                StreamDiagnostic(
                    "MD_STREAM_BACKPRESSURE_DROP",
                    "bounded stream queue overflow; oldest candle update dropped",
                    {"maxsize": self.maxsize},
                )
            )
        self._items[key] = update

    def drain(self) -> list[KlineUpdate]:
        out = list(self._items.values())
        self._items.clear()
        return out


class PublicKlineWebSocketClient:
    def __init__(
        self,
        *,
        exchange: Literal["binance", "bybit"],
        market: str,
        symbol: str,
        timeframe: str,
        source_kind: Literal[
            "trade_kline", "mark_kline", "index_kline"
        ] = "trade_kline",
    ):
        self.exchange = exchange
        self.market = market.lower()
        self.symbol = symbol.upper()
        self.timeframe = timeframe
        self.source_kind = source_kind
        self.url, self.subscribe = self._endpoint()

    def _endpoint(self) -> tuple[str, dict[str, Any] | None]:
        if self.exchange == "binance":
            interval = to_binance_interval(self.timeframe)
            stream = f"{self.symbol.lower()}@kline_{interval}"
            if self.market == "spot":
                return f"wss://stream.binance.com:9443/ws/{stream}", None
            if self.market == "usdm":
                # Binance USDⓈ-M futures public streams are documented on
                # fstream.binance.com, but in some regions that endpoint accepts
                # the WebSocket handshake and then stays silent. The
                # binancefuture.com host is the production futures stream host
                # that emits the same kline payload shape.
                return f"wss://fstream.binancefuture.com/ws/{stream}", None
            raise MDSymbolUnsupported(
                f"Unsupported Binance WebSocket market: {self.market}"
            )
        if self.exchange == "bybit":
            if self.market not in {"spot", "linear"}:
                raise MDSymbolUnsupported(
                    f"Unsupported Bybit WebSocket market: {self.market}"
                )
            base = (
                "wss://stream.bybit.com/v5/public/spot"
                if self.market == "spot"
                else "wss://stream.bybit.com/v5/public/linear"
            )
            topic = bybit_topic(self.timeframe, self.symbol)
            return base, {"op": "subscribe", "args": [topic]}
        raise MDUnsupportedFeature(f"Unsupported WebSocket exchange: {self.exchange}")

    async def events(
        self, *, max_messages: int | None = None, timeout_s: float | None = None
    ) -> AsyncIterator[LiveKlineEvent]:
        require_live_stream_enabled()
        if importlib.util.find_spec("websockets") is None:
            raise MDNetworkUnavailable(
                "Live WebSocket requires optional dependency websockets"
            )
        import websockets

        seen = 0
        deadline = None if timeout_s is None else time.monotonic() + timeout_s
        try:
            async with websockets.connect(
                self.url, ping_interval=20, close_timeout=5
            ) as ws:
                if self.subscribe is not None:
                    await ws.send(json.dumps(self.subscribe, separators=(",", ":")))
                while max_messages is None or seen < max_messages:
                    if deadline is not None and time.monotonic() >= deadline:
                        break
                    remaining = (
                        None
                        if deadline is None
                        else max(0.1, deadline - time.monotonic())
                    )
                    raw_text = await asyncio.wait_for(ws.recv(), timeout=remaining)
                    payload = json.loads(raw_text)
                    if self.exchange == "bybit" and payload.get("op") == "subscribe":
                        continue
                    if self.exchange == "binance":
                        update = normalize_binance_kline(
                            payload,
                            market=self.market,
                            timeframe=self.timeframe,
                            source_kind=self.source_kind,
                            received_at=int(time.time() * 1000),
                        )
                        seen += 1
                        yield LiveKlineEvent(update, payload)
                    else:
                        for update in normalize_bybit_kline(
                            payload,
                            market=self.market,
                            source_kind=self.source_kind,
                            received_at=int(time.time() * 1000),
                        ):
                            seen += 1
                            yield LiveKlineEvent(update, payload)
                            if max_messages is not None and seen >= max_messages:
                                break
        except asyncio.TimeoutError:
            return
        except OSError as e:
            raise MDNetworkUnavailable(
                "Live WebSocket connection failed",
                details={
                    "exchange": self.exchange,
                    "market": self.market,
                    "url": self.url,
                    "error": str(e),
                },
            ) from e
