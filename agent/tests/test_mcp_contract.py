"""Live check of the MCP client against the fake server, over real HTTP.

Verifies the one thing the whole demo rests on: that a business outcome and
an infrastructure failure come back as different *kinds* of thing.
"""

from __future__ import annotations

import asyncio
import socket
import threading
import time

import httpx
import pytest
import uvicorn

from fake_mcp.server import app
from richards_agent import mcp_client


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def server() -> str:
    port = _free_port()
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error")
    srv = uvicorn.Server(config)
    thread = threading.Thread(target=srv.run, daemon=True)
    thread.start()

    base = f"http://127.0.0.1:{port}"
    for _ in range(100):
        try:
            httpx.get(f"{base}/chaos", timeout=0.5)
            break
        except Exception:  # noqa: BLE001 - server still booting
            time.sleep(0.05)
    else:
        pytest.fail("fake MCP server did not start")

    yield base
    srv.should_exit = True


def _reset(base: str) -> None:
    httpx.post(f"{base}/reset", timeout=5)


@pytest.mark.asyncio
async def test_tools_are_discovered_at_runtime(server: str):
    _reset(server)
    tools = await mcp_client.list_tools(f"{server}/mcp")
    names = [t.name for t in tools]
    assert "order_a_taco" in names
    assert "take_payment" in names
    # Schemas come from the server, not from us.
    assert all(t.input_schema.get("type") == "object" for t in tools)


@pytest.mark.asyncio
async def test_out_of_stock_returns_it_does_not_raise(server: str):
    """Business outcome: the call SUCCEEDS and carries bad news."""
    _reset(server)
    content, is_error, _ = await mcp_client.call_tool(
        f"{server}/mcp", "order_a_taco", {"item": "chicken taco", "quantity": 12}, "k1"
    )
    assert is_error is True
    assert "OUT_OF_STOCK" in content


@pytest.mark.asyncio
async def test_server_down_raises_retryable(server: str):
    """Infrastructure failure: the call RAISES, so Temporal retries it."""
    _reset(server)
    httpx.post(f"{server}/chaos/mcp_worker/down", timeout=5)
    try:
        with pytest.raises(mcp_client.McpInfrastructureError):
            await mcp_client.call_tool(
                f"{server}/mcp", "get_specials", {}, "k2"
            )
    finally:
        httpx.post(f"{server}/chaos/mcp_worker/up", timeout=5)


@pytest.mark.asyncio
async def test_unreachable_server_raises_retryable():
    """Nothing listening at all — still retryable, not a crash."""
    with pytest.raises(mcp_client.McpInfrastructureError):
        await mcp_client.list_tools("http://127.0.0.1:1/mcp", timeout=2.0)


@pytest.mark.asyncio
async def test_same_key_dedupes_the_charge(server: str):
    """Two calls, same idempotency key, one charge."""
    _reset(server)
    first_content, _, first_dedup = await mcp_client.call_tool(
        f"{server}/mcp", "take_payment", {"order_id": "4471", "amount": 14.20}, "pay-1"
    )
    second_content, _, second_dedup = await mcp_client.call_tool(
        f"{server}/mcp", "take_payment", {"order_id": "4471", "amount": 14.20}, "pay-1"
    )

    assert first_dedup is False
    assert second_dedup is True
    assert first_content == second_content

    state = httpx.get(f"{server}/state", timeout=5).json()
    assert list(state["payments"].keys()) == ["4471"]


@pytest.mark.asyncio
async def test_different_keys_are_two_charges(server: str):
    """Sanity check on the other side: dedup must not be over-eager."""
    _reset(server)
    _, _, d1 = await mcp_client.call_tool(
        f"{server}/mcp", "take_payment", {"order_id": "a", "amount": 1}, "pay-a"
    )
    _, _, d2 = await mcp_client.call_tool(
        f"{server}/mcp", "take_payment", {"order_id": "b", "amount": 2}, "pay-b"
    )
    assert (d1, d2) == (False, False)
    state = httpx.get(f"{server}/state", timeout=5).json()
    assert sorted(state["payments"].keys()) == ["a", "b"]


@pytest.mark.asyncio
async def test_business_error_is_not_cached_by_idempotency(server: str):
    """A retry after restocking must be allowed to succeed.

    Caching the out-of-stock result under its key would permanently wedge the
    order — the agent restocks, tries again, and gets the stale failure back.
    """
    _reset(server)
    _, is_error, _ = await mcp_client.call_tool(
        f"{server}/mcp", "order_a_taco", {"item": "taco", "quantity": 12}, "same-key"
    )
    assert is_error is True

    await mcp_client.call_tool(
        f"{server}/mcp", "order_more_inventory", {"ingredient": "chicken", "pounds": 40}, "restock"
    )

    content, is_error, dedup = await mcp_client.call_tool(
        f"{server}/mcp", "order_a_taco", {"item": "taco", "quantity": 12}, "same-key"
    )
    assert is_error is False, content
    assert dedup is False
    assert "placed" in content
