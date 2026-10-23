"""Contract tests for the MCP client, against the local stand-in over real HTTP.

These exist because of the four traps in Ian's notes, each of which is silent in
rehearsal and fatal once the audience holds a service down. The SSE test is the
important one: a JSON-only parser passes every other test in this file.
"""

from __future__ import annotations

import os
import socket
import threading
import time

import httpx
import pytest

# Set before the server module reads them: keep the retry loop and the SSE
# upgrade fast enough to test.
os.environ["FAKE_ATTEMPT_INTERVAL"] = "0.05"
os.environ["FAKE_SSE_AFTER"] = "0.1"
os.environ["FAKE_RESTART_SECONDS"] = "1.0"

import uvicorn  # noqa: E402

from fake_mcp.server import app  # noqa: E402
from richards_agent import mcp_client  # noqa: E402

TOKEN = "test-token"


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def base() -> str:
    port = _free_port()
    srv = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error"))
    threading.Thread(target=srv.run, daemon=True).start()

    url = f"http://127.0.0.1:{port}"
    for _ in range(100):
        try:
            httpx.get(f"{url}/healthz", timeout=0.5)
            break
        except Exception:  # noqa: BLE001 - still booting
            time.sleep(0.05)
    else:
        pytest.fail("fake MCP server did not start")

    yield url
    srv.should_exit = True


def _reset(base: str) -> None:
    httpx.post(f"{base}/reset", timeout=5)


def _set(base: str, component: str, action: str) -> None:
    httpx.post(f"{base}/chaos/{component}/{action}", timeout=5)


# ---------------------------------------------------------------- discovery
@pytest.mark.asyncio
async def test_tools_are_discovered_not_hardcoded(base: str):
    _reset(base)
    tools = await mcp_client.list_tools(f"{base}/mcp", TOKEN)
    names = [t.name for t in tools]

    assert "order_taco" in names
    assert len(tools) == 4
    # Zero-argument by design — every tool takes {}.
    assert all(t.input_schema.get("properties") == {} for t in tools)
    # Descriptions matter: the model picks tools from them.
    assert all(t.description for t in tools)
    # Titles are the server's `service_N_name` env vars, and label each step in
    # the Temporal UI. Dropping them sends every step back to the bare tool id.
    assert {t.name: t.title for t in tools}["order_taco"] == "Order Taco"


@pytest.mark.asyncio
async def test_envelope_is_sent_on_every_request(base: str):
    """list_tools must carry params._meta or the real server answers -32602.

    Verified against the live server: the envelope is required on tools/list,
    not just tools/call.
    """
    _reset(base)
    # Raw request WITHOUT the envelope — should be rejected.
    raw = httpx.post(
        f"{base}/mcp",
        json={"jsonrpc": "2.0", "id": 1, "method": "tools/list"},
        headers={"Authorization": f"Bearer {TOKEN}"},
        timeout=5,
    ).json()
    assert raw["error"]["code"] == -32602

    # Our client sends it, so it works.
    assert await mcp_client.list_tools(f"{base}/mcp", TOKEN)


# ---------------------------------------------------------------- the big one
@pytest.mark.asyncio
async def test_broken_service_is_slow_not_failed_and_comes_back_over_sse(base: str):
    """The behaviour the whole demo rests on.

    A downed service does not produce an error. The call blocks, the transport
    upgrades to text/event-stream with ping comments, and the result arrives
    once the service is repaired. `attempts` counts the retries.
    """
    _reset(base)
    _set(base, "service_1", "down")

    async def repair_shortly() -> None:
        import asyncio
        await asyncio.sleep(0.6)
        _set(base, "service_1", "up")

    import asyncio
    repair = asyncio.create_task(repair_shortly())

    result = await mcp_client.call_tool(
        f"{base}/mcp", TOKEN, "order_taco", {}, "key-sse-1", read_seconds=30
    )
    await repair

    assert result.is_error is False
    assert "completed" in result.content
    assert result.attempts > 1, "should have retried while the service was down"
    assert result.workflow_id.startswith("order_taco-")


@pytest.mark.asyncio
async def test_fast_call_returns_plain_json(base: str):
    """The other encoding. Both paths have to work in one client."""
    _reset(base)
    result = await mcp_client.call_tool(
        f"{base}/mcp", TOKEN, "order_taco", {}, "key-fast-1", read_seconds=30
    )
    assert result.is_error is False
    assert result.attempts == 1


# ---------------------------------------------------------------- auth
@pytest.mark.asyncio
async def test_bad_token_is_terminal_not_retried(base: str):
    """A wrong token will never come right by retrying — fail fast and say so."""
    _reset(base)
    os.environ["FAKE_AGENT_TOKEN"] = TOKEN
    import fake_mcp.server as server
    server.AGENT_TOKEN = TOKEN
    try:
        with pytest.raises(mcp_client.McpProtocolError, match="token"):
            await mcp_client.list_tools(f"{base}/mcp", "wrong-token")
    finally:
        server.AGENT_TOKEN = ""
        os.environ.pop("FAKE_AGENT_TOKEN", None)


@pytest.mark.asyncio
async def test_unreachable_server_is_retryable():
    """Nothing listening: Temporal's problem, and it should retry."""
    with pytest.raises(mcp_client.McpInfrastructureError):
        await mcp_client.list_tools("http://127.0.0.1:1/mcp", TOKEN, read_seconds=2)


# ---------------------------------------------------------------- idempotency
@pytest.mark.asyncio
async def test_same_key_attaches_instead_of_ordering_twice(base: str):
    _reset(base)
    first = await mcp_client.call_tool(
        f"{base}/mcp", TOKEN, "send_receipt", {}, "receipt-1", read_seconds=30
    )
    second = await mcp_client.call_tool(
        f"{base}/mcp", TOKEN, "send_receipt", {}, "receipt-1", read_seconds=30
    )

    assert first.deduplicated is False
    assert second.deduplicated is True
    assert second.workflow_id == first.workflow_id

    ledger = httpx.get(f"{base}/ledger", timeout=5).json()
    assert ledger["fulfilled"] == 1, "a retry must not fulfil a second order"


@pytest.mark.asyncio
async def test_different_keys_are_two_orders(base: str):
    """Dedup must not be over-eager in the other direction."""
    _reset(base)
    a = await mcp_client.call_tool(
        f"{base}/mcp", TOKEN, "order_taco", {}, "taco-a", read_seconds=30
    )
    b = await mcp_client.call_tool(
        f"{base}/mcp", TOKEN, "order_taco", {}, "taco-b", read_seconds=30
    )
    assert (a.deduplicated, b.deduplicated) == (False, False)
    assert a.workflow_id != b.workflow_id

    ledger = httpx.get(f"{base}/ledger", timeout=5).json()
    assert ledger["fulfilled"] == 2


# ---------------------------------------------------------------- the gap
@pytest.mark.asyncio
async def test_ledger_gap_tracks_orders_stuck_in_retry(base: str):
    """`ordered - fulfilled` is Ian's recommended headline number: how many
    orders are mid-retry right now. It should spike while broken."""
    _reset(base)
    _set(base, "service_3", "down")

    import asyncio
    call = asyncio.create_task(
        mcp_client.call_tool(
            f"{base}/mcp", TOKEN, "restock_inventory", {}, "gap-1", read_seconds=30
        )
    )
    await asyncio.sleep(0.4)

    mid = httpx.get(f"{base}/ledger", timeout=5).json()
    assert mid["gap"] == 1, "one order should be stuck in retry"

    _set(base, "service_3", "up")
    await call

    after = httpx.get(f"{base}/ledger", timeout=5).json()
    assert after["gap"] == 0, "gap should drain to zero once repaired"
