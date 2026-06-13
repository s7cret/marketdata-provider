# Changelog

## 4.0.0

- Added exchange registry CLI commands for native/planned exchanges and canonical market types.
- Added top-10 exchange capability metadata with API/archive acquisition guidance.
- Primary release cleanup for the OpenPine 4.x package family.
- Added deterministic release/distribution/quality gates.
- Added `python -m marketdata_provider` entrypoint.
- Tightened standalone test behavior and archive hygiene.
- Fixed first-pass runtime/resource-management issues found during review.
- Split or constrained large modules so the architecture budget stays under 700 lines.
- Raised the default coverage gate to 100%.
- Added hermetic third-pass tests for cache integrity, Binance archive fallback, provider branches, contract adapters, timeframe contracts, service helpers, and distribution hygiene.
- Fixed deterministic distribution path filtering for non-current roots.
- Updated README and canonical docs to match the actual release gate.
