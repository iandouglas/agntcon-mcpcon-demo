"""Tests for the chaos state the audience drives.

Covers the two behaviours the demo depends on: crowd taps accumulate, and a
hard-broken worker comes back on its own so the demo can't be stalled forever.
"""

from __future__ import annotations

import os
import socket
import threading
import time

import httpx
import pytest

# Must be set before the server module reads it.
os.environ["FAKE_RESTART_SECONDS"] = "1.0"

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

    time.sleep(2.0)  # twice the restart window
    assert httpx.get(f"{server}/chaos", timeout=5).json()["payment_api"] == 0

    httpx.post(f"{server}/chaos/payment_api/up", timeout=5)
    assert httpx.get(f"{server}/chaos", timeout=5).json()["payment_api"] == 100


def test_killed_worker_is_restarted_by_the_supervisor(server: str):
    """The audience can kill the worker, but not keep it dead.

    Without this the demo can be stalled indefinitely by a determined room.
    Health returns low, not full, so it's easy to knock down again.
    """
    httpx.post(f"{server}/reset", timeout=5)
    httpx.post(f"{server}/chaos/agent_worker/down", timeout=5)
    assert httpx.get(f"{server}/chaos", timeout=5).json()["agent_worker"] == 0

    deadline = time.time() + 5.0
    while time.time() < deadline:
        health = httpx.get(f"{server}/chaos", timeout=5).json()["agent_worker"]
        if health > 0:
            break
        time.sleep(0.2)
    else:
        pytest.fail("supervisor never restarted agent_worker")

    assert 0 < health < 100, f"should come back degraded, got {health}"
