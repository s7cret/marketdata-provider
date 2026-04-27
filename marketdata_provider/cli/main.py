from __future__ import annotations
import argparse
import json
from marketdata_provider.errors import MarketDataError, MDUnsupportedFeature
from marketdata_provider.providers import OfflineDataProvider
from marketdata_provider.validation import validate_bars


def _cmd_validate(args: argparse.Namespace) -> int:
    provider = OfflineDataProvider(args.path, timeframe=args.timeframe)
    bars = provider.get_bars(args.symbol, args.timeframe, args.start, args.end, max_bars=args.max_bars)
    validate_bars(bars)
    print(json.dumps({"ok": True, "bars": len(bars), "first": bars[0].time if bars else None, "last": bars[-1].time if bars else None}, sort_keys=True))
    return 0

def _unsupported(name: str):
    def inner(args: argparse.Namespace) -> int:
        raise MDUnsupportedFeature(f"marketdata {name} requires live exchange/cache implementation planned for Stage B; no fake network success")
    return inner

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="marketdata", description="MarketData Provider Stage A CLI")
    sub = p.add_subparsers(dest="command", required=True)
    v = sub.add_parser("validate", help="validate an offline CSV/parquet dataset")
    v.add_argument("path"); v.add_argument("--symbol", default="OFFLINE:UNKNOWN"); v.add_argument("--timeframe", required=True)
    v.add_argument("--start", type=int); v.add_argument("--end", type=int); v.add_argument("--max-bars", type=int)
    v.set_defaults(func=_cmd_validate)
    for name in ("fetch", "export", "coverage"):
        c = sub.add_parser(name, help=f"{name} command placeholder (fails explicitly in Stage A unless offline path is added later)")
        c.set_defaults(func=_unsupported(name))
    return p

def main(argv: list[str] | None = None) -> int:
    parser = build_parser(); args = parser.parse_args(argv)
    try: return int(args.func(args))
    except MarketDataError as e:
        print(json.dumps({"ok": False, "code": e.code, "message": e.message}, ensure_ascii=False), flush=True)
        return 2

if __name__ == "__main__":
    raise SystemExit(main())
