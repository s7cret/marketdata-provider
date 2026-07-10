"""Phase 2 market-data acceptance evidence for deterministic and live execution."""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import tempfile
import time
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

from marketdata_provider._adapters import series_from_market_bars
from marketdata_provider.acceptance_models import (
    REQUIRED_PHASE2_CAPABILITIES,
    AcceptanceCheck,
    AcceptanceReport,
)
from marketdata_provider.config import BinanceConfig, BybitConfig
from marketdata_provider.contracts import BarQuery, InstrumentKey, parse_timeframe
from marketdata_provider.core.bar import MarketBar
from marketdata_provider.errors import (
    MDInvalidExchangeResponse,
    MDNetworkUnavailable,
    MarketDataError,
)
from marketdata_provider.exchanges.binance.provider import binance_get_bars_sync
from marketdata_provider.exchanges.binance.rest import normalize_binance_klines
from marketdata_provider.exchanges.bybit.provider import bybit_get_bars_sync
from marketdata_provider.exchanges.bybit.rest import normalize_bybit_klines
from marketdata_provider.store import CandleStore, SegmentStore
from marketdata_provider.store.repair import audit_against_source, repair_from_source
from marketdata_provider.streaming import (
    LiveKlineEvent,
    PublicKlineWebSocketClient,
    normalize_binance_kline,
    normalize_bybit_kline,
)
from marketdata_provider.timeframes import parse_time_ms


class _EventClient(Protocol):
    def events(self, *, max_messages: int, timeout_s: float) -> Any: ...


def _utc_iso(ms: int) -> str:
    return (
        datetime.fromtimestamp(ms / 1000, timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def _error_text(exc: BaseException) -> str:
    if isinstance(exc, MarketDataError):
        return f"{exc.code}: {exc}"
    return f"{type(exc).__name__}: {exc}"


async def probe_websocket_reconnect(
    *,
    exchange: str,
    market: str,
    client_factory: Callable[[], _EventClient] | None = None,
    connections: int = 2,
    attempts_per_connection: int = 2,
    timeout_s: float = 10.0,
    retry_backoff_s: float = 0.25,
) -> dict[str, object]:
    """Read one message per fresh connection, retrying each connection fail-closed."""

    if connections < 1 or attempts_per_connection < 1:
        raise ValueError("connections and attempts_per_connection must be positive")
    if client_factory is None:

        def default_client_factory() -> _EventClient:
            return PublicKlineWebSocketClient(
                exchange=exchange,  # type: ignore[arg-type]
                market=market,
                symbol="BTCUSDT",
                timeframe="1m",
            )

        client_factory = default_client_factory

    attempts = retries = messages = 0
    latest_open_time: int | None = None
    for connection_index in range(connections):
        connected = False
        last_error: BaseException | None = None
        for attempt_index in range(attempts_per_connection):
            attempts += 1
            try:
                client = client_factory()
                received: LiveKlineEvent | None = None
                async for event in client.events(max_messages=1, timeout_s=timeout_s):
                    received = event
                    break
                if received is None:
                    raise MDNetworkUnavailable(
                        "WebSocket produced no kline before timeout"
                    )
                if (
                    received.update.exchange != exchange
                    or received.update.market != market
                ):
                    raise MDInvalidExchangeResponse(
                        "WebSocket event identity mismatch",
                        details={
                            "expected": [exchange, market],
                            "actual": [
                                received.update.exchange,
                                received.update.market,
                            ],
                        },
                    )
                latest_open_time = received.update.open_time
                messages += 1
                connected = True
                break
            except (MarketDataError, OSError, asyncio.TimeoutError) as exc:
                last_error = exc
                if attempt_index + 1 < attempts_per_connection:
                    retries += 1
                    if retry_backoff_s:
                        await asyncio.sleep(retry_backoff_s)
        if not connected:
            raise MDNetworkUnavailable(
                "WebSocket reconnect retries exhausted",
                details={
                    "exchange": exchange,
                    "market": market,
                    "connection_index": connection_index,
                    "attempts": attempts,
                    "error": _error_text(last_error) if last_error else None,
                },
            ) from last_error

    assert latest_open_time is not None
    return {
        "exchange": exchange,
        "market": market,
        "connections": connections,
        "reconnects": max(0, connections - 1),
        "attempts": attempts,
        "retries": retries,
        "messages": messages,
        "latest_open_time_ms": latest_open_time,
        "latest_open_time_utc": _utc_iso(latest_open_time),
    }


def probe_live_rest(
    exchange: str, market: str, *, timeout_s: float
) -> dict[str, object]:
    if exchange == "binance":
        bars = binance_get_bars_sync(
            "BTCUSDT",
            "1m",
            None,
            None,
            BinanceConfig(),
            market=market,
            timeout=timeout_s,
            max_retries=2,
            max_bars=3,
        )
    elif exchange == "bybit":
        bars = bybit_get_bars_sync(
            "BTCUSDT",
            "1m",
            None,
            None,
            BybitConfig(),
            market=market,
            timeout=timeout_s,
            max_retries=2,
            max_bars=3,
        )
    else:
        raise ValueError(f"unsupported exchange: {exchange}")
    if not bars:
        raise MDInvalidExchangeResponse(f"{exchange} REST returned no closed bars")
    strictly_sorted = all(left.time < right.time for left, right in zip(bars, bars[1:]))
    if not strictly_sorted:
        raise MDInvalidExchangeResponse(
            f"{exchange} REST bars are not strictly ordered"
        )
    return {
        "exchange": exchange,
        "market": market,
        "bars": len(bars),
        "first_time_ms": bars[0].time,
        "last_time_ms": bars[-1].time,
        "strictly_sorted": strictly_sorted,
    }


def _market_bar(time_ms: int, *, close: float = 1.5) -> MarketBar:
    return MarketBar(
        time=time_ms,
        open=1.0,
        high=2.0,
        low=0.5,
        close=close,
        volume=10.0,
        time_close=time_ms + 59_999,
        exchange="binance",
        market="spot",
        symbol="BTCUSDT",
        timeframe="1m",
        source_transport="rest",
        source_kind="trade_kline",
        is_closed=True,
        downloaded_at=time_ms + 60_000,
    )


def _deterministic_transport_checks() -> list[AcceptanceCheck]:
    base = 1_704_067_200_000
    binance_rows = [
        [base, "1", "2", "0.5", "1.5", "10", base + 59_999, "15", 7, "4", "6"]
    ]
    binance_rest = normalize_binance_klines(
        binance_rows,
        symbol="BTCUSDT",
        market="spot",
        timeframe="1m",
        server_time_ms=base + 60_000,
    )
    bybit_rest = normalize_bybit_klines(
        {"result": {"list": [[base, "1", "2", "0.5", "1.5", "10", "15"]]}},
        symbol="BTCUSDT",
        market="linear",
        timeframe="1m",
        server_time_ms=base + 60_000,
    )
    binance_ws = normalize_binance_kline(
        {
            "E": base + 5_000,
            "s": "BTCUSDT",
            "k": {
                "s": "BTCUSDT",
                "i": "1m",
                "t": base,
                "T": base + 59_999,
                "o": "1",
                "h": "2",
                "l": "0.5",
                "c": "1.5",
                "v": "10",
                "q": "15",
                "n": 7,
                "V": "4",
                "Q": "6",
                "x": True,
            },
        },
        market="spot",
        received_at=base + 5_100,
    )
    bybit_ws = normalize_bybit_kline(
        {
            "topic": "kline.1.BTCUSDT",
            "ts": base + 5_000,
            "data": [
                {
                    "start": base,
                    "end": base + 59_999,
                    "timestamp": base + 5_000,
                    "open": "1",
                    "high": "2",
                    "low": "0.5",
                    "close": "1.5",
                    "volume": "10",
                    "turnover": "15",
                    "confirm": True,
                }
            ],
        },
        market="linear",
        received_at=base + 5_100,
    )
    return [
        AcceptanceCheck(
            "binance_rest",
            len(binance_rest) == 1 and binance_rest[0].time == base,
            {"bars": len(binance_rest), "time_ms": binance_rest[0].time},
        ),
        AcceptanceCheck(
            "binance_ws",
            binance_ws.is_closed and binance_ws.open_time == base,
            {"messages": 1, "time_ms": binance_ws.open_time},
        ),
        AcceptanceCheck(
            "bybit_rest",
            len(bybit_rest) == 1 and bybit_rest[0].time == base,
            {"bars": len(bybit_rest), "time_ms": bybit_rest[0].time},
        ),
        AcceptanceCheck(
            "bybit_ws",
            len(bybit_ws) == 1 and bybit_ws[0].is_closed,
            {"messages": len(bybit_ws), "time_ms": bybit_ws[0].open_time},
        ),
    ]


async def _deterministic_reconnect_check() -> AcceptanceCheck:
    base = 1_704_067_200_000

    class Client:
        async def events(self, *, max_messages: int, timeout_s: float):
            update = normalize_binance_kline(
                {
                    "E": base,
                    "s": "BTCUSDT",
                    "k": {
                        "s": "BTCUSDT",
                        "i": "1m",
                        "t": base,
                        "T": base + 59_999,
                        "o": "1",
                        "h": "2",
                        "l": "0.5",
                        "c": "1.5",
                        "v": "10",
                        "x": False,
                    },
                },
                market="spot",
                received_at=base,
            )
            yield LiveKlineEvent(update, {"fixture": True})

    evidence = await probe_websocket_reconnect(
        exchange="binance",
        market="spot",
        client_factory=Client,
        connections=2,
        attempts_per_connection=1,
        timeout_s=0.01,
        retry_backoff_s=0,
    )
    return AcceptanceCheck("reconnect", evidence["reconnects"] == 1, evidence)


def _deterministic_integrity_checks() -> list[AcceptanceCheck]:
    base = 1_704_067_200_000
    bars = [_market_bar(base), _market_bar(base + 120_000)]
    query = BarQuery(
        InstrumentKey("binance", "spot", "BTCUSDT"),
        parse_timeframe("1m"),
        base,
        base + 180_000,
        gap_policy="allow_with_metadata",
    )
    series = series_from_market_bars(query, bars, source="phase2-fixture")
    gap_evidence: dict[str, object] = {
        "coverage_status": series.coverage.status,
        "missing_intervals": [list(item) for item in series.coverage.missing_intervals],
    }

    with tempfile.TemporaryDirectory(prefix="marketdata-phase2-") as temp_dir:
        root = Path(temp_dir)
        segment_store = SegmentStore(root / "checksum")
        manifest = segment_store.replace_all(
            [_market_bar(base)],
            exchange="binance",
            market="spot",
            symbol="BTCUSDT",
            timeframe="1m",
        )
        data_path = next((root / "checksum").rglob("bars.csv"))
        with data_path.open(newline="", encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
        rows[0]["close"] = "1.4"
        with data_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=segment_store.fields)
            writer.writeheader()
            writer.writerows(rows)
        checksum_rejected = False
        try:
            segment_store.read_all(
                exchange="binance",
                market="spot",
                symbol="BTCUSDT",
                timeframe="1m",
            )
        except MDInvalidExchangeResponse:
            checksum_rejected = True

        candle_store = CandleStore(root / "repair")
        source_bars = [_market_bar(base), _market_bar(base + 60_000, close=1.6)]
        candle_store.commit_closed(source_bars[0])
        before = audit_against_source(
            candle_store,
            source_bars,
            exchange="binance",
            market="spot",
            symbol="BTCUSDT",
            timeframe="1m",
        )
        repaired = repair_from_source(
            candle_store,
            source_bars,
            exchange="binance",
            market="spot",
            symbol="BTCUSDT",
            timeframe="1m",
        )
        after = audit_against_source(
            candle_store,
            source_bars,
            exchange="binance",
            market="spot",
            symbol="BTCUSDT",
            timeframe="1m",
        )

    expected_time = base
    timezone_values: dict[str, object] = {
        "naive_utc": parse_time_ms("2024-01-01T00:00:00"),
        "zulu": parse_time_ms("2024-01-01T00:00:00Z"),
        "offset": parse_time_ms("2024-01-01T08:00:00+08:00"),
    }
    return [
        AcceptanceCheck(
            "gap_detection",
            series.coverage.status == "gap" and bool(series.coverage.missing_intervals),
            gap_evidence,
        ),
        AcceptanceCheck(
            "checksum_validation",
            checksum_rejected,
            {
                "manifest_sha256": manifest.checksum,
                "corruption_rejected": checksum_rejected,
            },
        ),
        AcceptanceCheck(
            "segment_repair",
            not before.ok and repaired.applied and repaired.changed == 1 and after.ok,
            {
                "issues_before": len(before.issues),
                "changed": repaired.changed,
                "applied": repaired.applied,
                "issues_after": len(after.issues),
            },
        ),
        AcceptanceCheck(
            "timezone_normalization",
            set(timezone_values.values()) == {expected_time},
            timezone_values,
        ),
    ]


async def _run_live_check(
    capability: str, operation: Callable[[], Any]
) -> AcceptanceCheck:
    try:
        value = operation()
        if asyncio.iscoroutine(value):
            value = await value
        evidence = value if isinstance(value, dict) else {"result": value}
        return AcceptanceCheck(capability, bool(evidence), evidence)
    except Exception as exc:
        return AcceptanceCheck(capability, False, {"attempted": True}, _error_text(exc))


async def run_phase2_acceptance(
    *, mode: str = "deterministic", timeout_s: float = 10.0
) -> AcceptanceReport:
    if mode not in {"deterministic", "live"}:
        raise ValueError("mode must be 'deterministic' or 'live'")
    started = 0 if mode == "deterministic" else int(time.time() * 1000)
    if mode == "deterministic":
        checks = _deterministic_transport_checks()
        checks.append(await _deterministic_reconnect_check())
        checks.extend(_deterministic_integrity_checks())
        finished = 0
    else:
        previous_allow_stream = os.environ.get("MARKETDATA_ALLOW_STREAM")
        os.environ["MARKETDATA_ALLOW_STREAM"] = "1"
        try:
            checks = [
                await _run_live_check(
                    "binance_rest",
                    lambda: probe_live_rest("binance", "spot", timeout_s=timeout_s),
                ),
                await _run_live_check(
                    "binance_ws",
                    lambda: probe_websocket_reconnect(
                        exchange="binance", market="spot", timeout_s=timeout_s
                    ),
                ),
                await _run_live_check(
                    "bybit_rest",
                    lambda: probe_live_rest("bybit", "linear", timeout_s=timeout_s),
                ),
                await _run_live_check(
                    "bybit_ws",
                    lambda: probe_websocket_reconnect(
                        exchange="bybit", market="linear", timeout_s=timeout_s
                    ),
                ),
            ]
        finally:
            if previous_allow_stream is None:
                os.environ.pop("MARKETDATA_ALLOW_STREAM", None)
            else:
                os.environ["MARKETDATA_ALLOW_STREAM"] = previous_allow_stream
        reconnect_checks = [
            check for check in checks if check.capability.endswith("_ws")
        ]
        checks.append(
            AcceptanceCheck(
                "reconnect",
                all(
                    check.passed and check.evidence.get("reconnects") == 1
                    for check in reconnect_checks
                ),
                {
                    "binance_reconnects": reconnect_checks[0].evidence.get(
                        "reconnects"
                    ),
                    "bybit_reconnects": reconnect_checks[1].evidence.get("reconnects"),
                },
            )
        )
        integrity_capabilities = REQUIRED_PHASE2_CAPABILITIES[5:]
        try:
            raw_integrity_checks = _deterministic_integrity_checks()
        except Exception as exc:
            error = _error_text(exc)
            integrity_checks = [
                AcceptanceCheck(
                    capability,
                    False,
                    {"attempted": True},
                    error,
                )
                for capability in integrity_capabilities
            ]
        else:
            integrity_checks = []
            for capability in integrity_capabilities:
                matches = [
                    check
                    for check in raw_integrity_checks
                    if check.capability == capability
                ]
                if len(matches) == 1:
                    integrity_checks.append(matches[0])
                else:
                    integrity_checks.append(
                        AcceptanceCheck(
                            capability,
                            False,
                            {"attempted": True},
                            "deterministic integrity evidence was missing or duplicated",
                        )
                    )
        checks.extend(integrity_checks)
        finished = int(time.time() * 1000)
    return AcceptanceReport(mode, started, finished, tuple(checks))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m marketdata_provider.acceptance")
    parser.add_argument(
        "--mode", choices=("deterministic", "live"), default="deterministic"
    )
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument("--json")
    args = parser.parse_args(argv)
    report = asyncio.run(run_phase2_acceptance(mode=args.mode, timeout_s=args.timeout))
    payload = json.dumps(report.to_dict(), indent=2, sort_keys=True)
    if args.json:
        Path(args.json).write_text(payload + "\n", encoding="utf-8")
    print(payload)
    return 0 if report.ok else 1


if __name__ == "__main__":  # pragma: no cover - exercised as a module CLI
    raise SystemExit(main())
