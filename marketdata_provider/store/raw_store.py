from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Literal

from marketdata_provider.errors import MDUnsupportedFeature

Compression = Literal["plain", "zstd"]


@dataclass(frozen=True, slots=True)
class RawManifest:
    schema_version: str
    exchange: str
    market: str
    symbol: str
    source_transport: str
    source_kind: str
    compression: str
    rows_count: int
    checksum: str
    file_name: str


class RawStore:
    """Append/replace raw WS/REST payload retention as deterministic NDJSON.

    zstd is opt-in and requires the optional zstandard package. The default is
    plain NDJSON, so environments without zstd remain deterministic.
    """

    def __init__(self, root: str | Path, *, compression: Compression = "plain"):
        if compression not in {"plain", "zstd"}:
            raise MDUnsupportedFeature(f"Unsupported raw compression: {compression}")
        if compression == "zstd" and importlib.util.find_spec("zstandard") is None:
            raise MDUnsupportedFeature("RawStore zstd compression requires optional dependency zstandard; use plain NDJSON otherwise")
        self.root = Path(root)
        self.compression: Compression = compression
        self.root.mkdir(parents=True, exist_ok=True)

    def _dir(self, *, exchange: str, market: str, symbol: str, source_transport: str, source_kind: str) -> Path:
        return self.root / "raw-v1" / f"exchange={exchange.lower()}" / f"market={market.lower()}" / f"symbol={symbol.upper()}" / f"transport={source_transport}" / f"source={source_kind}"

    def write_batch(self, payloads: Iterable[dict[str, Any]], *, exchange: str, market: str, symbol: str, source_transport: str, source_kind: str = "trade_kline") -> RawManifest:
        rows = [json.dumps(p, sort_keys=True, separators=(",", ":")) for p in payloads]
        body = "".join(r + "\n" for r in rows).encode()
        checksum = hashlib.sha256(body).hexdigest()
        d = self._dir(exchange=exchange, market=market, symbol=symbol, source_transport=source_transport, source_kind=source_kind)
        d.mkdir(parents=True, exist_ok=True)
        suffix = ".ndjson.zst" if self.compression == "zstd" else ".ndjson"
        file_name = f"payloads-{checksum[:16]}{suffix}"
        data_path = d / file_name
        manifest = RawManifest("stage-d-raw-1", exchange.lower(), market.lower(), symbol.upper(), source_transport, source_kind, self.compression, len(rows), checksum, file_name)
        self._atomic_write_bytes(data_path, self._compress(body))
        self._atomic_write_text(d / "manifest.json", json.dumps(asdict(manifest), sort_keys=True, indent=2) + "\n")
        return manifest

    def read_batch(self, *, exchange: str, market: str, symbol: str, source_transport: str, source_kind: str = "trade_kline") -> list[dict[str, Any]]:
        d = self._dir(exchange=exchange, market=market, symbol=symbol, source_transport=source_transport, source_kind=source_kind)
        manifest_path = d / "manifest.json"
        if not manifest_path.exists():
            return []
        manifest = json.loads(manifest_path.read_text())
        data = self._decompress((d / manifest["file_name"]).read_bytes(), manifest.get("compression", "plain"))
        actual = hashlib.sha256(data).hexdigest()
        if actual != manifest.get("checksum"):
            raise MDUnsupportedFeature("RawStore checksum mismatch", details={"expected": manifest.get("checksum"), "actual": actual})
        return [json.loads(line) for line in data.decode().splitlines() if line.strip()]

    def inspect(self) -> list[RawManifest]:
        out: list[RawManifest] = []
        for path in sorted(self.root.glob("raw-v1/**/manifest.json")):
            out.append(RawManifest(**json.loads(path.read_text())))
        return out

    def _compress(self, body: bytes) -> bytes:
        if self.compression == "plain":
            return body
        import zstandard as zstd
        return zstd.ZstdCompressor().compress(body)

    def _decompress(self, body: bytes, compression: str) -> bytes:
        if compression == "plain":
            return body
        if importlib.util.find_spec("zstandard") is None:
            raise MDUnsupportedFeature("Reading zstd RawStore payloads requires optional dependency zstandard")
        import zstandard as zstd
        return zstd.ZstdDecompressor().decompress(body)

    def _atomic_write_bytes(self, path: Path, content: bytes) -> None:
        fd, tmp = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
        with os.fdopen(fd, "wb") as f:
            f.write(content); f.flush(); os.fsync(f.fileno())
        os.replace(tmp, path)

    def _atomic_write_text(self, path: Path, content: str) -> None:
        fd, tmp = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
        with os.fdopen(fd, "w") as f:
            f.write(content); f.flush(); os.fsync(f.fileno())
        os.replace(tmp, path)
