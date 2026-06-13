# Architecture

`marketdata_provider` is an independent OpenPine stack library. Core modules avoid hard runtime coupling to sibling repositories unless integration adapters explicitly need them.

The package exposes deterministic dataclass/protocol contracts and keeps network or sibling-repository behavior behind explicit tests, environment gates, or optional imports.


## Hardening layout

The second 4.0.0 pass keeps modules under the 700-line architecture budget and moves optional integration behavior behind standalone-tested boundaries. `marketdata_provider` should remain importable and testable without sibling OpenPine repositories.

## Exchange registry boundary

`marketdata_provider.exchanges.registry` is metadata only. It lists native adapters and top-priority planned exchanges, plus market-type and archive/source guidance. Fetching remains fail-closed: planned exchanges are not accepted by `normalize_symbol()` or live REST fetch paths until a dedicated adapter and tests are added.
