# Changelog

## 4.0.2

- Removed repeated full-file checksum scans and duplicate persistence from online hot paths.
- Added canonical candle comparison, keyed single-flight reads, bounded range reads, and crash-safe integrity generations.
- Added regression coverage for large-cache tail reads, duplicate batches, concurrent fetches, and recovery behavior.

## 4.0.1

- Added true bounded-memory strictly-newer CSV tail append without historical iteration or file replacement.
- Added crash-recoverable append journals, tail-chain integrity manifests, legacy checksum migration, and deterministic duplicate/conflict handling.
- Added separately scheduled/manual Binance and Bybit live-network acceptance canaries with timeout, DNS, network, and geo-restriction classification; the `stream` extra now includes SOCKS transport support for proxied runners.
- Preserved 100% deterministic test coverage and the 700-line architecture budget through surgical storage-module extraction.

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
