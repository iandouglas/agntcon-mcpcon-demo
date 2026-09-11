"""Tests for the chaos state the audience drives.

Covers the rules the tug-of-war depends on: crowd taps accumulate, a broken
component stays broken until someone fixes it, and "down" means below the 50%
threshold rather than exactly zero.
"""

from __future__ import annotations

import socket
import threading
import time

import httpx
import pytest

import uvicorn  # noqa: E402

from fake_mcp.server import app  # noqa: E402


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def server() -> str:
    port = _free_port()
    srv = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error"))
    threading.Thread(target=srv.run, daemon=True).start()

    base = f"http://127.0.0.1:{port}"
    for _ in range(100):
        try:
            httpx.get(f"{base}/chaos", timeout=0.5)
            break
        except Exception:  # noqa: BLE001 - still booting
            time.sleep(0.05)
    else:
        pytest.fail("fake server did not start")

    yield base
    srv.should_exit = True


def test_agent_worker_and_llm_are_exposed(server: str):
    """The agent's watchdog and LLM proxy read their health from here."""
    health = httpx.get(f"{server}/chaos", timeout=5).json()
    assert "agent_worker" in health
    assert "llm_api" in health


def test_taps_accumulate_until_down(server: str):
    httpx.post(f"{server}/reset", timeout=5)

    for _ in range(4):
        health = httpx.post(
            f"{server}/chaos/payment_api/tap", params={"delta": -20}, timeout=5
        ).json()
    assert health["payment_api"] == 20      # 100 - 4*20

    health = httpx.post(
        f"{server}/chaos/payment_api/tap", params={"delta": -20}, timeout=5
    ).json()
    assert health["payment_api"] == 0

    # Fixers push back, and it clamps at 100 rather than overshooting.
    for _ in range(10):
        health = httpx.post(
            f"{server}/chaos/payment_api/tap", params={"delta": 20}, timeout=5
        ).json()
    assert health["payment_api"] == 100


def test_soft_broken_service_stays_down_until_repaired(server: str):
    """A payment API nobody fixes stays broken — no free recovery."""
    httpx.post(f"{server}/reset", timeout=5)
    httpx.post(f"{server}/chaos/payment_api/down", timeout=5)

    time.sleep(2.0)  # well past any plausible auto-heal window
    assert httpx.get(f"{server}/chaos", timeout=5).json()["payment_api"] == 0

    httpx.post(f"{server}/chaos/payment_api/up", timeout=5)
    assert httpx.get(f"{server}/chaos", timeout=5).json()["payment_api"] == 100


def test_a_killed_worker_stays_dead_until_someone_fixes_it(server: str):
    """The load-bearing rule of the tug-of-war.

    Nothing revives a broken component on its own. An earlier version auto-
    healed after 5 seconds, which made the audience's kill cosmetic and their
    fixers decorative — the worker came back whether anyone tapped or not.
    """
    httpx.post(f"{server}/reset", timeout=5)
    httpx.post(f"{server}/chaos/agent_worker/down", timeout=5)
    assert httpx.get(f"{server}/chaos", timeout=5).json()["agent_worker"] == 0

    time.sleep(2.0)  # well past the old auto-heal window
    assert httpx.get(f"{server}/chaos", timeout=5).json()["agent_worker"] == 0, \
        "nothing may revive a component except a fixer"

    # Only an explicit repair brings it back.
    httpx.post(f"{server}/chaos/agent_worker/up", timeout=5)
    assert httpx.get(f"{server}/chaos", timeout=5).json()["agent_worker"] == 100


def test_health_below_the_threshold_counts_as_down(server: str):
    """Down is < 50, not <= 0 — a service at 30% is broken, not healthy.

    The watchdog used to test `<= 0`, so against the board's percentages a
    worker the room had dragged to 30% read as perfectly fine.
    """
    from richards_agent.worker import HEALTH_THRESHOLD, read_health

    assert HEALTH_THRESHOLD == 50

    # Flat shape, as the local fake reports it.
    assert read_health({"agent_worker": 30}, "agent_worker") == 30
    assert read_health({"agent_worker": 30}, "agent_worker") < HEALTH_THRESHOLD

    # Ian's board shape: nested, booleans today, percentages soon.
    assert read_health({"state": {"agent_worker": False}}, "agent_worker") == 0
    assert read_health({"state": {"agent_worker": True}}, "agent_worker") == 100
    assert read_health({"state": {"agent_worker": 72}}, "agent_worker") == 72
    assert read_health({"state": {"agent_worker": {"percent": 12}}}, "agent_worker") == 12

    # Absent means "not a switch on this feed", NOT down — otherwise pointing
    # CHAOS_URL at a board without an agent_worker switch would kill the
    # worker instantly and permanently.
    assert read_health({"state": {"service_1": True}}, "agent_worker") is None
    assert read_health({}, "agent_worker") is None
