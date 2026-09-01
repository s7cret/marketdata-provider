from __future__ import annotations

from pathlib import Path

from openpine_contracts import Finality

from marketdata_provider.canonical.envelope import known_provider_revision
from marketdata_provider.canonical.provider import (
    ProviderRawBar,
    build_public_snapshot,
    snapshot_revision_identity,
)
from marketdata_provider.canonical.source_identity import bind_source_identity
from marketdata_provider.contracts import BarQuery, InstrumentKey, parse_timeframe
from marketdata_provider.core.bar import MarketBar


CONTRACTS_RC6_COMMIT = "904e8f660834a10d3382cd1b2ed7380c24b73072"
PRODUCER_COMMIT = "1" * 40
STACK_ID = "sha256:" + "2" * 64


def _query(*, start_ms: int = 0, end_ms: int = 120_000) -> BarQuery:
    return BarQuery(
        InstrumentKey("binance", "spot", "SOLUSDT"),
        parse_timeframe("1m"),
        start_ms,
        end_ms,
        source="provider",
    )


def _bar(open_time_ms: int) -> MarketBar:
    return MarketBar(
        time=open_time_ms,
        time_close=open_time_ms + 59_999,
        open=100.0 + open_time_ms,
        high=101.0 + open_time_ms,
        low=99.0 + open_time_ms,
        close=100.5 + open_time_ms,
        volume=10.0,
        exchange="binance",
        market="spot",
        symbol="SOLUSDT",
        timeframe="1m",
        source_transport="rest",
        is_closed=True,
        open_text=str(100.0 + open_time_ms),
        high_text=str(101.0 + open_time_ms),
        low_text=str(99.0 + open_time_ms),
        close_text=str(100.5 + open_time_ms),
        volume_text="10.0",
    )


def test_ci_and_live_workflows_pin_exact_rc6_contracts_commit() -> None:
    for relative_path in (
        ".github/workflows/ci.yml",
        ".github/workflows/live-acceptance.yml",
    ):
        workflow = Path(relative_path).read_text(encoding="utf-8")
        assert f"ref: {CONTRACTS_RC6_COMMIT}" in workflow


def test_source_bar_identity_is_stable_across_query_windows() -> None:
    narrow = bind_source_identity(
        [_bar(0)],
        query=_query(end_ms=60_000),
        provider="binance",
        source_transport="rest",
    )
    wide = bind_source_identity(
        [_bar(0), _bar(60_000)],
        query=_query(end_ms=120_000),
        provider="binance",
        source_transport="rest",
    )

    assert narrow[0].provider_revision == wide[0].provider_revision
    assert wide[0].provider_revision != wide[1].provider_revision


def test_public_snapshot_preserves_per_bar_revisions_under_aggregate_identity() -> None:
    raw = [
        ProviderRawBar(
            instrument_id=_query().instrument.serialize(),
            timeframe="1m",
            open_time_utc_ms=open_time,
            close_time_utc_ms=open_time + 59_999,
            open="100",
            high="101",
            low="99",
            close="100.5",
            volume="10",
            finality=Finality.FINAL,
            provider="binance",
            provider_revision=revision,
        )
        for open_time, revision in ((0, "source-r1"), (60_000, "source-r2"))
    ]
    aggregate_revision = snapshot_revision_identity(
        "binance",
        [(item.open_time_utc_ms, item.revision, item.provider_revision) for item in raw],
    )

    snapshot = build_public_snapshot(
        _query(),
        raw,
        provider_revision=known_provider_revision(aggregate_revision),
        producer_commit=PRODUCER_COMMIT,
        stack_id=STACK_ID,
    )

    assert snapshot["provider_revision"] == known_provider_revision(aggregate_revision)
    assert [bar["provider_revision"] for bar in snapshot["bars"]] == [
        known_provider_revision("source-r1"),
        known_provider_revision("source-r2"),
    ]


def test_readme_and_changelog_identify_rc6_release() -> None:
    readme = Path("README.md").read_text(encoding="utf-8")
    changelog = Path("CHANGELOG.md").read_text(encoding="utf-8")

    assert "# MarketData Provider 5.0.0rc6" in readme
    assert "version-5.0.0rc6-blue" in readme
    assert "python-%3E%3D3.11-blue" in readme
    assert "## 5.0.0rc6" in changelog