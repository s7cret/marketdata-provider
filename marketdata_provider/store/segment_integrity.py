from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

from marketdata_provider.store.segment_checksums import validate_csv_checksum
from marketdata_provider.store.segment_manifest import SegmentManifest

INTEGRITY_GENERATION_NAME = ".integrity-generation.json"
INTEGRITY_GENERATION_VERSION = "segment-integrity-v1"


def _generation_payload(data_path: Path, manifest: SegmentManifest) -> dict[str, object]:
    stat = data_path.stat()
    return {
        "version": INTEGRITY_GENERATION_VERSION,
        "device": stat.st_dev,
        "inode": stat.st_ino,
        "size": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
        "checksum": manifest.checksum,
        "checksum_algorithm": manifest.checksum_algorithm,
        "rows_count": manifest.rows_count,
    }


def publish_integrity_generation(
    store: Any, data_path: Path, manifest: SegmentManifest
) -> None:
    """Publish the exact writer-validated immutable file generation."""
    payload = _generation_payload(data_path, manifest)
    store._atomic_write_text(
        data_path.parent / INTEGRITY_GENERATION_NAME,
        json.dumps(payload, sort_keys=True, indent=2) + "\n",
    )


def integrity_generation_is_current(
    data_path: Path, manifest: dict[str, object]
) -> bool:
    generation_path = data_path.parent / INTEGRITY_GENERATION_NAME
    try:
        payload = json.loads(generation_path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            return False
        stat = data_path.stat()
    except (OSError, json.JSONDecodeError):
        return False
    return payload == {
        "version": INTEGRITY_GENERATION_VERSION,
        "device": stat.st_dev,
        "inode": stat.st_ino,
        "size": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
        "checksum": manifest.get("checksum"),
        "checksum_algorithm": manifest.get("checksum_algorithm"),
        "rows_count": manifest.get("rows_count"),
    }


def validate_or_trust_csv_generation(
    store: Any, data_path: Path, manifest: dict[str, object]
) -> None:
    """Avoid O(N) validation for an unchanged writer-published generation."""
    if integrity_generation_is_current(data_path, manifest):
        return
    validate_csv_checksum(data_path, manifest)
    publish_integrity_generation(
        store, data_path, SegmentManifest(**cast(Any, manifest))
    )
