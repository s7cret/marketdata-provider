"""Data models for Phase 2 market-data acceptance evidence."""

from __future__ import annotations

from dataclasses import asdict, dataclass

REQUIRED_PHASE2_CAPABILITIES = (
    "binance_rest",
    "binance_ws",
    "bybit_rest",
    "bybit_ws",
    "reconnect",
    "gap_detection",
    "checksum_validation",
    "segment_repair",
    "timezone_normalization",
)


@dataclass(frozen=True, slots=True)
class AcceptanceCheck:
    capability: str
    passed: bool
    evidence: dict[str, object]
    error: str | None = None


@dataclass(frozen=True, slots=True)
class AcceptanceReport:
    mode: str
    started_at_ms: int
    finished_at_ms: int
    checks: tuple[AcceptanceCheck, ...]

    @property
    def failed_capabilities(self) -> tuple[str, ...]:
        by_capability: dict[str, list[AcceptanceCheck]] = {}
        for check in self.checks:
            by_capability.setdefault(check.capability, []).append(check)
        return tuple(
            capability
            for capability in REQUIRED_PHASE2_CAPABILITIES
            if len(by_capability.get(capability, ())) != 1
            or not by_capability[capability][0].passed
            or not by_capability[capability][0].evidence
        )

    @property
    def ok(self) -> bool:
        known = set(REQUIRED_PHASE2_CAPABILITIES)
        return (
            not self.failed_capabilities
            and len(self.checks) == len(known)
            and {check.capability for check in self.checks} == known
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": "phase2-acceptance-v1",
            "mode": self.mode,
            "started_at_ms": self.started_at_ms,
            "finished_at_ms": self.finished_at_ms,
            "ok": self.ok,
            "failed_capabilities": list(self.failed_capabilities),
            "checks": [asdict(check) for check in self.checks],
        }
