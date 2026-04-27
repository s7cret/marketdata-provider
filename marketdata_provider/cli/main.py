from __future__ import annotations

import argparse
import csv
import json
import os
from dataclasses import asdict
from pathlib import Path

from marketdata_provider.cache.local import read_cache_segment, write_cache_segment
from marketdata_provider.config import BinanceConfig, BybitConfig
from marketdata_provider.errors import MDNetworkUnavailable, MDUnsupportedFeature, MarketDataError
from marketdata_provider.store import CandleStore
from marketdata_provider.store.repair import audit_against_source, load_repair_source, repair_from_source
from marketdata_provider.streaming import KlineUpdate, MockWebSocketSupervisor, normalize_binance_kline, normalize_bybit_kline, require_live_stream_enabled
from marketdata_provider.exchanges.binance.provider import binance_get_bars_sync
from marketdata_provider.exchanges.bybit.provider import bybit_get_bars_sync
from marketdata_provider.providers import OfflineDataProvider
from marketdata_provider.symbols import normalize_symbol
from marketdata_provider.validation import validate_bars


def _json(obj: object) -> None:
    print(json.dumps(obj, ensure_ascii=False, sort_keys=True))


def _bars_from_source(args: argparse.Namespace):
    if getattr(args, "path", None):
        return OfflineDataProvider(args.path, timeframe=args.timeframe).get_bars(args.symbol, args.timeframe, args.start, args.end, max_bars=args.max_bars)
    ns = normalize_symbol(args.symbol, exchange=getattr(args, "exchange", None), market=getattr(args, "market", None))
    cache_dir = Path(getattr(args, "cache_dir", ".marketdata-cache"))
    if getattr(args, "cache", False):
        return read_cache_segment(cache_dir, exchange=ns.exchange, market=ns.market, symbol=ns.exchange_symbol, timeframe=args.timeframe, start=args.start, end=args.end, max_bars=args.max_bars)
    if getattr(args, "live", False):
        if os.getenv("RUN_MARKETDATA_NETWORK_TESTS") != "1" and os.getenv("MARKETDATA_ALLOW_NETWORK") != "1":
            raise MDNetworkUnavailable("Live REST is disabled unless RUN_MARKETDATA_NETWORK_TESTS=1 or MARKETDATA_ALLOW_NETWORK=1")
        if args.end is None and args.max_bars is None:
            raise MDUnsupportedFeature("Live REST requires --end or --max-bars to avoid unbounded history fetches")
        if ns.exchange == "binance":
            return binance_get_bars_sync(args.symbol, args.timeframe, args.start, args.end, BinanceConfig(), market=ns.market, max_bars=args.max_bars)
        if ns.exchange == "bybit":
            return bybit_get_bars_sync(args.symbol, args.timeframe, args.start, args.end, BybitConfig(), market=ns.market, max_bars=args.max_bars)
    raise MDUnsupportedFeature("Choose an explicit data source: --path for offline, --cache for local cache, or --live with network env opt-in")


def _cmd_validate(args: argparse.Namespace) -> int:
    bars = _bars_from_source(args)
    validate_bars(bars)
    _json({"ok": True, "bars": len(bars), "first": bars[0].time if bars else None, "last": bars[-1].time if bars else None})
    return 0


def _cmd_fetch(args: argparse.Namespace) -> int:
    if not args.live and not args.path:
        raise MDUnsupportedFeature("fetch requires --path offline source or --live with explicit network env opt-in")
    bars = _bars_from_source(args)
    ns = normalize_symbol(args.symbol, exchange=args.exchange, market=args.market)
    meta = write_cache_segment(args.cache_dir, bars, exchange=ns.exchange, market=ns.market, symbol=ns.exchange_symbol, timeframe=args.timeframe, start=args.start, end=args.end)
    _json({"ok": True, "bars": meta.bars, "cache_dir": str(args.cache_dir), "checksum": meta.checksum})
    return 0


def _write_csv(path: Path, bars) -> None:
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["time", "open", "high", "low", "close", "volume", "time_close"])
        w.writeheader()
        for b in bars:
            w.writerow({"time": b.time, "open": repr(b.open), "high": repr(b.high), "low": repr(b.low), "close": repr(b.close), "volume": repr(b.volume), "time_close": b.time_close if b.time_close is not None else ""})


def _cmd_export(args: argparse.Namespace) -> int:
    bars = _bars_from_source(args)
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    if args.format == "csv":
        _write_csv(out, bars)
    elif args.format == "json":
        out.write_text(json.dumps([b.__dict__ if hasattr(b, "__dict__") else {"time": b.time, "open": b.open, "high": b.high, "low": b.low, "close": b.close, "volume": b.volume, "time_close": b.time_close} for b in bars], sort_keys=True) + "\n")
    else:
        raise MDUnsupportedFeature(f"Unsupported export format: {args.format}")
    _json({"ok": True, "bars": len(bars), "output": str(out)})
    return 0


def _cmd_coverage(args: argparse.Namespace) -> int:
    bars = _bars_from_source(args)
    gaps = 0
    for prev, cur in zip(bars, bars[1:]):
        if prev.time_close is not None and cur.time != prev.time_close + 1:
            gaps += 1
    _json({"ok": True, "bars": len(bars), "first": bars[0].time if bars else None, "last": bars[-1].time if bars else None, "gaps": gaps})
    return 0


def _store_ns(args: argparse.Namespace):
    return normalize_symbol(args.symbol, exchange=getattr(args, "exchange", None), market=getattr(args, "market", None))


def _cmd_current(args: argparse.Namespace) -> int:
    ns = _store_ns(args)
    cur = CandleStore(args.store_dir).get_current_market_candle(exchange=ns.exchange, market=ns.market, symbol=ns.exchange_symbol, timeframe=args.timeframe)
    _json({"ok": True, "current": None if cur is None else {"time": cur.time, "time_close": cur.time_close, "open": cur.open, "high": cur.high, "low": cur.low, "close": cur.close, "volume": cur.volume}})
    return 0


def _cmd_checkpoints(args: argparse.Namespace) -> int:
    cps = CandleStore(args.store_dir).current.checkpoints()
    _json({"ok": True, "checkpoints": [asdict(cp) for cp in cps]})
    return 0


def _load_mock_events(path: Path, *, market: str) -> list[KlineUpdate]:
    events: list[KlineUpdate] = []
    for line in path.read_text().splitlines():
        if not line.strip():
            continue
        obj = json.loads(line)
        if "open_time" in obj:
            events.append(KlineUpdate(**obj))
        elif obj.get("e") == "kline" or "k" in obj:
            events.append(normalize_binance_kline(obj, market=market))
        elif str(obj.get("topic", "")).startswith("kline."):
            events.extend(normalize_bybit_kline(obj, market=market))
        else:
            raise MDUnsupportedFeature("Unsupported mock stream event shape")
    return events


def _cmd_stream(args: argparse.Namespace) -> int:
    ns = _store_ns(args)
    store = CandleStore(args.store_dir)
    if not args.mock_events:
        require_live_stream_enabled()
        raise MDUnsupportedFeature("Live WebSocket streaming is not implemented in Stage C; use --mock-events for deterministic local ingestion")
    events = _load_mock_events(args.mock_events, market=ns.market)
    result = MockWebSocketSupervisor(store).run(events, reconnect_after=args.reconnect_after)
    _json({"ok": True, **asdict(result)})
    return 0


def _cmd_audit(args: argparse.Namespace) -> int:
    ns = _store_ns(args)
    store = CandleStore(args.store_dir)
    source = load_repair_source(args.source_path, exchange=ns.exchange, market=ns.market, symbol=ns.exchange_symbol, timeframe=args.timeframe)
    report = audit_against_source(store, source, exchange=ns.exchange, market=ns.market, symbol=ns.exchange_symbol, timeframe=args.timeframe)
    _json({"ok": report.ok, "checked": report.checked, "issues": [asdict(i) for i in report.issues]})
    return 0 if report.ok else 3


def _cmd_repair(args: argparse.Namespace) -> int:
    ns = _store_ns(args)
    store = CandleStore(args.store_dir)
    source = load_repair_source(args.source_path, exchange=ns.exchange, market=ns.market, symbol=ns.exchange_symbol, timeframe=args.timeframe)
    changed = repair_from_source(store, source, exchange=ns.exchange, market=ns.market, symbol=ns.exchange_symbol, timeframe=args.timeframe)
    _json({"ok": True, "changed": changed})
    return 0


def _common(sub: argparse.ArgumentParser, *, source_required: bool = True) -> None:
    sub.add_argument("--symbol", default="OFFLINE:UNKNOWN")
    sub.add_argument("--timeframe", required=True)
    sub.add_argument("--start", type=int)
    sub.add_argument("--end", type=int)
    sub.add_argument("--max-bars", type=int)
    sub.add_argument("--exchange")
    sub.add_argument("--market")
    sub.add_argument("--path", help="offline CSV/parquet source")
    sub.add_argument("--cache", action="store_true", help="read source bars from local cache")
    sub.add_argument("--live", action="store_true", help="use public REST only when MARKETDATA_ALLOW_NETWORK=1 or RUN_MARKETDATA_NETWORK_TESTS=1")
    sub.add_argument("--cache-dir", type=Path, default=Path(".marketdata-cache"))


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="marketdata", description="MarketData Provider Stage C CLI")
    sub = p.add_subparsers(dest="command", required=True)
    v = sub.add_parser("validate", help="validate offline/cache/live bars")
    _common(v); v.set_defaults(func=_cmd_validate)
    f = sub.add_parser("fetch", help="fetch offline/live bars into local cache")
    _common(f); f.set_defaults(func=_cmd_fetch)
    e = sub.add_parser("export", help="export offline/cache/live bars to CSV/JSON")
    _common(e); e.add_argument("--output", required=True); e.add_argument("--format", choices=["csv", "json"], default="csv"); e.set_defaults(func=_cmd_export)
    c = sub.add_parser("coverage", help="summarize offline/cache/live coverage")
    _common(c); c.set_defaults(func=_cmd_coverage)

    cur = sub.add_parser("current", help="show mutable current/open candle from CandleStore")
    cur.add_argument("--store-dir", type=Path, default=Path(".marketdata-store")); cur.add_argument("--symbol", required=True); cur.add_argument("--timeframe", required=True); cur.add_argument("--exchange"); cur.add_argument("--market"); cur.set_defaults(func=_cmd_current)
    cps = sub.add_parser("checkpoints", help="list stream checkpoints")
    cps.add_argument("--store-dir", type=Path, default=Path(".marketdata-store")); cps.set_defaults(func=_cmd_checkpoints)
    s = sub.add_parser("stream", help="ingest mocked stream events; live WS fails unless explicitly implemented/env-gated")
    s.add_argument("--store-dir", type=Path, default=Path(".marketdata-store")); s.add_argument("--symbol", required=True); s.add_argument("--timeframe", required=True); s.add_argument("--exchange"); s.add_argument("--market"); s.add_argument("--mock-events", type=Path); s.add_argument("--reconnect-after", type=int); s.set_defaults(func=_cmd_stream)
    a = sub.add_parser("audit", help="compare CandleStore finalized bars against REST/offline source data")
    a.add_argument("--store-dir", type=Path, default=Path(".marketdata-store")); a.add_argument("--source-path", type=Path, required=True); a.add_argument("--symbol", required=True); a.add_argument("--timeframe", required=True); a.add_argument("--exchange"); a.add_argument("--market"); a.set_defaults(func=_cmd_audit)
    r = sub.add_parser("repair", help="rewrite finalized store bars from REST/offline source data")
    r.add_argument("--store-dir", type=Path, default=Path(".marketdata-store")); r.add_argument("--source-path", type=Path, required=True); r.add_argument("--symbol", required=True); r.add_argument("--timeframe", required=True); r.add_argument("--exchange"); r.add_argument("--market"); r.set_defaults(func=_cmd_repair)
    return p


def main(argv: list[str] | None = None) -> int:
    parser = build_parser(); args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except MarketDataError as e:
        _json({"ok": False, "code": e.code, "message": e.message, "details": e.details})
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
