from __future__ import annotations

from pathlib import Path

import pytest

from marketdata_provider import acceptance
from marketdata_provider.errors import MDInvalidExchangeResponse, MDNetworkUnavailable


@pytest.mark.asyncio
async def test_live_failures_are_classified_for_geo_network_and_timeout() -> None:
    cases = [
        (
            MDInvalidExchangeResponse("HTTP 451 unavailable for legal reasons"),
            "GEO_RESTRICTED",
        ),
        (MDNetworkUnavailable("connection refused"), "NETWORK_UNAVAILABLE"),
        (TimeoutError("timed out"), "TIMEOUT"),
    ]
    for error, classification in cases:
        check = await acceptance._run_live_check(
            "probe", lambda error=error: (_ for _ in ()).throw(error)
        )
        assert not check.passed
        assert check.evidence == {
            "attempted": True,
            "failure_classification": classification,
        }


def test_live_acceptance_workflow_is_scheduled_manual_bounded_and_separate() -> None:
    path = Path(".github/workflows/live-acceptance.yml")
    workflow = path.read_text(encoding="utf-8")
    deterministic_ci = Path(".github/workflows/ci.yml").read_text(encoding="utf-8")

    assert "schedule:" in workflow
    assert "cron:" in workflow
    assert "workflow_dispatch:" in workflow
    assert "timeout-minutes: 10" in workflow
    assert 'python -m pip install -e ".[dev,stream]"' in workflow
    assert (
        "timeout 330s python -m marketdata_provider.acceptance --mode live" in workflow
    )
    assert "--timeout 15" in workflow
    assert "live-acceptance-report" in workflow
    assert "if-no-files-found: error" in workflow
    assert "secrets." not in workflow
    assert "--mode live" not in deterministic_ci


def test_live_stream_extra_supports_socks_proxy_environments() -> None:
    pyproject = Path("pyproject.toml").read_text(encoding="utf-8")

    assert 'stream = ["websockets>=12", "socksio>=1.0,<2"]' in pyproject
