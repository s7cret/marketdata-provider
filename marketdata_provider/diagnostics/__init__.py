from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Literal

Severity = Literal["info", "warning", "error"]

@dataclass(frozen=True, slots=True)
class Diagnostic:
    code: str
    message: str
    severity: Severity = "info"
    details: dict[str, Any] = field(default_factory=dict)

@dataclass(slots=True)
class DiagnosticCollector:
    items: list[Diagnostic] = field(default_factory=list)
    def add(self, code: str, message: str, severity: Severity = "info", **details: Any) -> None:
        self.items.append(Diagnostic(code, message, severity, details))
    def warnings_or_errors(self) -> list[Diagnostic]:
        return [d for d in self.items if d.severity in {"warning", "error"}]
