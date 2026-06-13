from __future__ import annotations

import re

_SAFE_PART_RE = re.compile(r"[^A-Z0-9._-]+")


def safe_path_part(value: str) -> str:
    sanitized = _SAFE_PART_RE.sub("_", str(value).strip().upper()).strip("._")
    return sanitized or "UNKNOWN"
