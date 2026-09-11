"""Verifies activities actually heartbeat while blocked.

Why this test exists: every activity here is one long `await`, and without
heartbeats Temporal cannot tell a busy worker from a dead one. It waits out
`start_to_close` instead — which for `call_richards_tool` is 17 minutes of a
stalled demo after the audience lands its kill.

That bug shipped once. This test is how we notice if it comes back.
"""

from __future__ import annotations

import asyncio

import pytest
from temporalio.testing import ActivityEnvironment

from richards_agent.activities import _heartbeat_while


@pytest.mark.asyncio
async def test_heartbeats_while_the_work_blocks():
    beats: list[tuple] = []
    env = ActivityEnvironment()
    env.on_heartbeat = lambda *args: beats.append(args)

    async def slow() -> str:
        await asyncio.sleep(0.35)
        return "done"

    async def run() -> str:
        # 50ms interval over a 350ms body: expect roughly 6 beats.
        return await _heartbeat_while(slow(), interval=0.05)

    result = await env.run(run)

    assert result == "done"
    assert len(beats) >= 3, f"expected repeated heartbeats, got {len(beats)}"


@pytest.mark.asyncio
async def test_fast_work_returns_without_waiting_for_an_interval():
    """The heartbeat wrapper must not add latency to a quick call."""
    beats: list[tuple] = []
    env = ActivityEnvironment()
    env.on_heartbeat = lambda *args: beats.append(args)

    async def quick() -> int:
        return 42

    async def run() -> int:
        return await _heartbeat_while(quick(), interval=5.0)

    loop = asyncio.get_running_loop()
    started = loop.time()
    result = await env.run(run)
    elapsed = loop.time() - started

    assert result == 42
    assert elapsed < 1.0, "wrapper should not block on the heartbeat interval"
    assert beats == []


@pytest.mark.asyncio
async def test_exceptions_propagate_unchanged():
    """The wrapper must not swallow or wrap errors — the classification in
    mcp_client is what decides retryable vs terminal, and it only works if the
    original exception type survives."""
    env = ActivityEnvironment()

    class Boom(Exception):
        pass

    async def explode() -> None:
        await asyncio.sleep(0.01)
        raise Boom("downstream said no")

    async def run() -> None:
        await _heartbeat_while(explode(), interval=0.05)

    with pytest.raises(Boom, match="downstream said no"):
        await env.run(run)


@pytest.mark.asyncio
async def test_cancellation_cancels_the_inner_work():
    """A cancelled activity must not leave its HTTP call running."""
    env = ActivityEnvironment()
    state = {"cancelled": False}

    async def long() -> None:
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            state["cancelled"] = True
            raise

    async def run() -> None:
        await _heartbeat_while(long(), interval=0.05)

    task = asyncio.create_task(env.run(run))
    await asyncio.sleep(0.15)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # Give the inner task a tick to observe its own cancellation.
    await asyncio.sleep(0.05)
    assert state["cancelled"] is True


def test_every_activity_call_sets_a_heartbeat_timeout():
    """Belt and braces: heartbeating is useless unless the workflow declares a
    heartbeat_timeout, and it is easy to add an activity and forget."""
    import pathlib

    source = (
        pathlib.Path(__file__).parent.parent
        / "src" / "richards_agent" / "workflow.py"
    ).read_text()

    starts = source.count("start_to_close_timeout=")
    heartbeats = source.count("heartbeat_timeout=")
    assert starts > 0
    assert heartbeats == starts, (
        f"{starts} activity call(s) but only {heartbeats} heartbeat_timeout(s) — "
        "an activity without one stalls until start_to_close expires"
    )
