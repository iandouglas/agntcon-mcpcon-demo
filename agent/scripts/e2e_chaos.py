"""End-to-end chaos run against a real Temporal cluster.

Everything is real except the model: real worker, real activities, real HTTP
to the MCP server, real Temporal retries. The LLM turn is scripted so the run
is deterministic and needs no API key.

Mid-flight it takes the MCP server down, leaves it down through several
retries, brings it back, and then checks the customer was charged once.

    temporal server start-dev &
    uv run uvicorn fake_mcp.server:app --port 8931 &
    uv run python scripts/e2e_chaos.py
"""

from __future__ import annotations

import asyncio
import os
import sys
import uuid

import httpx
from temporalio import activity
from temporalio.client import Client
from temporalio.worker import Worker

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from richards_agent.activities import call_richards_tool, list_richards_tools  # noqa: E402
from richards_agent.shared import (  # noqa: E402
    LlmTurn,
    LlmTurnInput,
    LlmUsage,
    OrderRequest,
    ToolCall,
)
from richards_agent.workflow import TacoOrderAgent  # noqa: E402

MCP_BASE = "http://localhost:8931"

SCRIPT: list[tuple[str, dict]] = [
    ("get_specials", {}),
    ("order_a_taco", {"item": "chicken taco", "quantity": 2}),
    ("order_more_inventory", {"ingredient": "chicken", "pounds": 40}),
    ("order_a_taco", {"item": "chicken taco", "quantity": 2}),
    ("take_payment", {"order_id": "4471", "amount": 14.20}),
    ("email_receipt", {"order_id": "4471", "email": "hungry@example.com"}),
]

llm_invocations = 0


@activity.defn(name="llm_turn")
async def scripted_llm_turn(payload: LlmTurnInput) -> LlmTurn:
    """Stands in for the model. Counts its own invocations so we can prove
    the retries below never reached it."""
    global llm_invocations
    llm_invocations += 1

    step = payload.turn
    if step >= len(SCRIPT):
        return LlmTurn(
            assistant_content=[{"type": "text", "text": "Order 4471 confirmed — $14.20."}],
            tool_calls=[],
            text="Order 4471 confirmed — $14.20.",
            usage=LlmUsage(input_tokens=400, output_tokens=60),
            stop_reason="end_turn",
        )

    name, args = SCRIPT[step]
    tid = f"tu_{step}"
    return LlmTurn(
        assistant_content=[{"type": "tool_use", "id": tid, "name": name, "input": args}],
        tool_calls=[ToolCall(tool_use_id=tid, name=name, arguments=args, idempotency_key="")],
        usage=LlmUsage(input_tokens=400, output_tokens=60),
        stop_reason="tool_use",
    )


async def chaos(action: str, component: str = "mcp_worker") -> None:
    async with httpx.AsyncClient(timeout=5) as c:
        await c.post(f"{MCP_BASE}/chaos/{component}/{action}")


async def main() -> int:
    os.environ.setdefault("MCP_ENDPOINT", f"{MCP_BASE}/mcp")

    async with httpx.AsyncClient(timeout=5) as c:
        await c.post(f"{MCP_BASE}/reset")

    client = await Client.connect("localhost:7233")
    task_queue = f"e2e-{uuid.uuid4()}"

    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[TacoOrderAgent],
        activities=[list_richards_tools, scripted_llm_turn, call_richards_tool],
    ):
        handle = await client.start_workflow(
            TacoOrderAgent.run,
            OrderRequest(order_id="4471", request="two chicken tacos and the special"),
            id=f"taco-e2e-{uuid.uuid4()}",
            task_queue=task_queue,
        )
        print(f"started {handle.id}")

        async def sabotage() -> None:
            await asyncio.sleep(2.0)
            print("\n>>> BREAKING the MCP server (audience taps C4)\n")
            await chaos("down")
            await asyncio.sleep(6.0)
            print("\n>>> REPAIRING the MCP server\n")
            await chaos("up")

        async def watch() -> None:
            last = ""
            while True:
                await asyncio.sleep(0.4)
                try:
                    s = await handle.query(TacoOrderAgent.status)
                except Exception:  # noqa: BLE001
                    continue
                line = (
                    f"  [{s.status:12}] {s.step:22} turns={s.turns} "
                    f"tools={s.tool_calls} tokens={s.tokens_spent} dedup={s.dedup_hits}"
                )
                if line != last:
                    print(line)
                    last = line
                if s.finished:
                    return

        saboteur = asyncio.create_task(sabotage())
        watcher = asyncio.create_task(watch())
        result = await handle.result()
        await saboteur
        watcher.cancel()

        # Must happen while the worker is still polling — a query needs a
        # worker to answer it, and the `async with` below would close ours.
        state = await handle.query(TacoOrderAgent.status)

    async with httpx.AsyncClient(timeout=5) as c:
        world = (await c.get(f"{MCP_BASE}/state")).json()

    print(f"\nresult: {result}\n")
    print("--- checks " + "-" * 52)
    checks = [
        ("order completed", state.finished is True),
        ("charged exactly once", list(world["payments"].keys()) == ["4471"]),
        ("receipt sent exactly once", list(world["receipts"].keys()) == ["4471"]),
        ("agent got past the out-of-stock branch", world["chicken_lb"] > 0),
        ("model was asked once per turn, not once per retry",
         llm_invocations == state.turns),
    ]
    ok = True
    for label, passed in checks:
        print(f"  {'PASS' if passed else 'FAIL'}  {label}")
        ok = ok and passed

    print(f"\n  model invocations: {llm_invocations}")
    print(f"  logical tool calls: {state.tool_calls}")
    print(f"  tokens spent: {state.tokens_spent}")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
