from __future__ import annotations

import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

import marketdata_provider.acceptance as acceptance
from marketdata_provider.acceptance import (
    REQUIRED_PHASE2_CAPABILITIES,
    AcceptanceCheck,
    AcceptanceReport,
    probe_websocket_reconnect,
    run_phase2_acceptance,
)
from marketdata_provider.errors import MDInvalidExchangeResponse, MDNetworkUnavailable
from marketdata_provider.streaming import KlineUpdate, LiveKlineEvent


def _checks() -> tuple[AcceptanceCheck, ...]:
    return tuple(
        AcceptanceCheck(capability, True, {"proof": capability})
        for capability in REQUIRED_PHASE2_CAPABILITIES
    )


def _event(exchange: str, market: str) -> LiveKlineEvent:
    return LiveKlineEvent(
        KlineUpdate(
            exchange,
            market,
            "BTCUSDT",
            "1m",
            1_704_067_205_000,
            1_704_067_200_000,
            1_704_067_259_999,
            1.0,
            2.0,
            0.5,
            1.5,
            10.0,
            received_at=1_704_067_205_100,
        ),
        {"source": "fixture"},
    )


def test_acceptance_report_requires_every_unique_capability_with_evidence() -> None:
    complete = AcceptanceReport(
        "deterministic", 1_704_067_200_000, 1_704_067_201_000, _checks()
    )
    assert complete.ok
    assert complete.failed_capabilities == ()
    assert complete.to_dict()["schema_version"] == "phase2-acceptance-v1"

    missing = AcceptanceReport("deterministic", 0, 1, _checks()[:-1])
    duplicate = AcceptanceReport("deterministic", 0, 1, _checks() + (_checks()[0],))
    no_evidence = AcceptanceReport(
        "deterministic",
        0,
        1,
        tuple(
            (
                AcceptanceCheck(check.capability, check.passed, {})
                if check.capability == "gap_detection"
                else check
            )
            for check in _checks()
        ),
    )
    explicit_failure = AcceptanceReport(
        "deterministic",
        0,
        1,
        tuple(
            (
                AcceptanceCheck(check.capability, False, {"proof": "attempted"}, "boom")
                if check.capability == "bybit_ws"
                else check
            )
            for check in _checks()
        ),
    )

    assert missing.failed_capabilities == (REQUIRED_PHASE2_CAPABILITIES[-1],)
    assert duplicate.failed_capabilities == (REQUIRED_PHASE2_CAPABILITIES[0],)
    assert no_evidence.failed_capabilities == ("gap_detection",)
    assert explicit_failure.failed_capabilities == ("bybit_ws",)
    assert not missing.ok
    assert not duplicate.ok
    assert not no_evidence.ok
    assert not explicit_failure.ok


@pytest.mark.asyncio
async def test_websocket_probe_retries_then_forces_a_real_reconnect_path() -> None:
    clients: list[object] = []

    class Client:
        def __init__(self, fail: bool) -> None:
            self.fail = fail

        async def events(self, *, max_messages: int, timeout_s: float):
            assert max_messages == 1
            assert timeout_s == 0.01
            if self.fail:
                raise MDNetworkUnavailable("temporary disconnect")
            yield _event("bybit", "linear")

    def factory() -> Client:
        client = Client(fail=not clients)
        clients.append(client)
        return client

    evidence = await probe_websocket_reconnect(
        exchange="bybit",
        market="linear",
        client_factory=factory,
        connections=2,
        attempts_per_connection=2,
        timeout_s=0.01,
        retry_backoff_s=0,
    )

    assert evidence["connections"] == 2
    assert evidence["reconnects"] == 1
    assert evidence["attempts"] == 3
    assert evidence["retries"] == 1
    assert evidence["messages"] == 2
    assert evidence["latest_open_time_utc"] == "2024-01-01T00:00:00Z"


@pytest.mark.asyncio
async def test_websocket_probe_fails_closed_when_retries_are_exhausted() -> None:
    class FailedClient:
        async def events(self, *, max_messages: int, timeout_s: float):
            raise MDNetworkUnavailable("still unavailable")
            yield _event("binance", "spot")

    with pytest.raises(MDNetworkUnavailable, match="retries exhausted") as raised:
        await probe_websocket_reconnect(
            exchange="binance",
            market="spot",
            client_factory=FailedClient,
            connections=1,
            attempts_per_connection=2,
            timeout_s=0.01,
            retry_backoff_s=0,
        )
    assert raised.value.details["attempts"] == 2


@pytest.mark.asyncio
async def test_deterministic_phase2_runner_proves_all_required_capabilities() -> None:
    report = await run_phase2_acceptance(mode="deterministic")

    assert report.ok, json.dumps(report.to_dict(), indent=2)
    assert (
        tuple(check.capability for check in report.checks)
        == REQUIRED_PHASE2_CAPABILITIES
    )
    assert all(check.evidence for check in report.checks)
    assert report.to_dict()["mode"] == "deterministic"


@pytest.mark.asyncio
async def test_live_runner_uses_both_exchanges_and_fails_closed_per_check(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rest_calls: list[tuple[str, str]] = []
    ws_calls: list[tuple[str, str]] = []
    monkeypatch.setenv("MARKETDATA_ALLOW_STREAM", "preserve")

    def rest_probe(
        exchange: str, market: str, *, timeout_s: float
    ) -> dict[str, object]:
        rest_calls.append((exchange, market))
        if exchange == "bybit":
            raise MDNetworkUnavailable("blocked")
        return {"exchange": exchange, "market": market, "bars": 3}

    async def ws_probe(
        *, exchange: str, market: str, timeout_s: float, **_kwargs: object
    ) -> dict[str, object]:
        ws_calls.append((exchange, market))
        return {
            "exchange": exchange,
            "market": market,
            "connections": 2,
            "reconnects": 1,
            "attempts": 2,
            "retries": 0,
            "messages": 2,
        }

    monkeypatch.setattr(acceptance, "probe_live_rest", rest_probe)
    monkeypatch.setattr(acceptance, "probe_websocket_reconnect", ws_probe)

    report = await run_phase2_acceptance(mode="live", timeout_s=0.01)

    assert rest_calls == [("binance", "spot"), ("bybit", "linear")]
    assert ws_calls == [("binance", "spot"), ("bybit", "linear")]
    assert os.environ["MARKETDATA_ALLOW_STREAM"] == "preserve"
    assert report.failed_capabilities == ("bybit_rest",)
    assert not report.ok
    assert (
        next(check.error for check in report.checks if check.capability == "bybit_rest")
        == "MD_NETWORK_UNAVAILABLE: blocked"
    )

    monkeypatch.delenv("MARKETDATA_ALLOW_STREAM", raising=False)
    second = await run_phase2_acceptance(mode="live", timeout_s=0.01)
    assert second.failed_capabilities == ("bybit_rest",)
    assert "MARKETDATA_ALLOW_STREAM" not in os.environ

    def broken_integrity() -> list[AcceptanceCheck]:
        raise RuntimeError("integrity unavailable")

    monkeypatch.setattr(acceptance, "_deterministic_integrity_checks", broken_integrity)
    third = await run_phase2_acceptance(mode="live", timeout_s=0.01)
    assert len(third.checks) == len(REQUIRED_PHASE2_CAPABILITIES)
    assert third.failed_capabilities == (
        "bybit_rest",
        "gap_detection",
        "checksum_validation",
        "segment_repair",
        "timezone_normalization",
    )
    assert all(
        check.error == "RuntimeError: integrity unavailable"
        for check in third.checks[5:]
    )

    monkeypatch.setattr(
        acceptance,
        "_deterministic_integrity_checks",
        lambda: [
            AcceptanceCheck("gap_detection", True, {"proof": 1}),
            AcceptanceCheck("gap_detection", True, {"proof": 2}),
        ],
    )
    fourth = await run_phase2_acceptance(mode="live", timeout_s=0.01)
    assert all(not check.passed for check in fourth.checks[5:])
    assert all(
        check.error == "deterministic integrity evidence was missing or duplicated"
        for check in fourth.checks[5:]
    )


@pytest.mark.asyncio
async def test_acceptance_edge_paths_fail_closed_and_live_rest_is_exercised(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(ValueError, match="positive"):
        await probe_websocket_reconnect(
            exchange="binance", market="spot", connections=0
        )
    with pytest.raises(ValueError, match="mode"):
        await run_phase2_acceptance(mode="unknown")

    class DefaultClient:
        async def events(self, *, max_messages: int, timeout_s: float):
            yield _event("binance", "spot")

    monkeypatch.setattr(
        acceptance,
        "PublicKlineWebSocketClient",
        lambda **_kwargs: DefaultClient(),
    )
    default_evidence = await probe_websocket_reconnect(
        exchange="binance",
        market="spot",
        connections=1,
        attempts_per_connection=1,
        timeout_s=0.01,
        retry_backoff_s=0,
    )
    assert default_evidence["messages"] == 1

    class EmptyClient:
        async def events(self, *, max_messages: int, timeout_s: float):
            if False:
                yield _event("binance", "spot")

    with pytest.raises(MDNetworkUnavailable, match="retries exhausted"):
        await probe_websocket_reconnect(
            exchange="binance",
            market="spot",
            client_factory=EmptyClient,
            connections=1,
            attempts_per_connection=1,
            timeout_s=0.01,
            retry_backoff_s=0,
        )

    class WrongIdentityClient:
        async def events(self, *, max_messages: int, timeout_s: float):
            yield _event("bybit", "linear")

    with pytest.raises(MDNetworkUnavailable, match="retries exhausted"):
        await probe_websocket_reconnect(
            exchange="binance",
            market="spot",
            client_factory=WrongIdentityClient,
            connections=1,
            attempts_per_connection=2,
            timeout_s=0.01,
            retry_backoff_s=0.001,
        )

    calls: list[str] = []

    def fake_binance(*_args: object, **_kwargs: object) -> list[SimpleNamespace]:
        calls.append("binance")
        return [SimpleNamespace(time=1), SimpleNamespace(time=2)]

    def fake_bybit(*_args: object, **_kwargs: object) -> list[SimpleNamespace]:
        calls.append("bybit")
        return [SimpleNamespace(time=3)]

    monkeypatch.setattr(acceptance, "binance_get_bars_sync", fake_binance)
    monkeypatch.setattr(acceptance, "bybit_get_bars_sync", fake_bybit)
    assert acceptance.probe_live_rest("binance", "spot", timeout_s=0.01)["bars"] == 2
    assert acceptance.probe_live_rest("bybit", "linear", timeout_s=0.01)["bars"] == 1
    assert calls == ["binance", "bybit"]
    with pytest.raises(ValueError, match="unsupported exchange"):
        acceptance.probe_live_rest("other", "spot", timeout_s=0.01)

    monkeypatch.setattr(acceptance, "binance_get_bars_sync", lambda *_a, **_kw: [])
    with pytest.raises(MDInvalidExchangeResponse, match="no closed bars"):
        acceptance.probe_live_rest("binance", "spot", timeout_s=0.01)

    monkeypatch.setattr(
        acceptance,
        "binance_get_bars_sync",
        lambda *_a, **_kw: [SimpleNamespace(time=2), SimpleNamespace(time=1)],
    )
    with pytest.raises(MDInvalidExchangeResponse, match="strictly ordered"):
        acceptance.probe_live_rest("binance", "spot", timeout_s=0.01)

    failed = await acceptance._run_live_check(
        "runtime_error", lambda: (_ for _ in ()).throw(RuntimeError("boom"))
    )
    assert failed.error == "RuntimeError: boom"


def test_acceptance_cli_writes_report_and_returns_nonzero_for_incomplete_report(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output = tmp_path / "report.json"
    assert acceptance.main(["--mode", "deterministic", "--json", str(output)]) == 0
    assert json.loads(output.read_text())["ok"] is True

    async def incomplete(**_kwargs: object) -> AcceptanceReport:
        return AcceptanceReport("live", 0, 1, _checks()[:-1])

    monkeypatch.setattr(acceptance, "run_phase2_acceptance", incomplete)
    assert acceptance.main(["--mode", "live"]) == 1
