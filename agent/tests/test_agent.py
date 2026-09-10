"""Tests for the agent loop, with the model and MCP server mocked.

The point of these is the failure taxonomy, not the happy path. If the
business-vs-infrastructure line ever moves, test_out_of_stock_is_not_retried
is the one that catches it.
"""

from __future__ import annotations

import uuid

import pytest
from temporalio import activity
from temporalio.client import Client
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from richards_agent.mcp_client import McpInfrastructureError
from richards_agent.shared import (
    AgentState,
    LlmTurn,
    LlmTurnInput,
    LlmUsage,
    OrderRequest,
    ToolCall,
    ToolOutcome,
    ToolSpec,
)
from richards_agent.workflow import TacoOrderAgent

TOOLS = [
    ToolSpec("get_specials", "today's special", {"type": "object", "properties": {}}),
    ToolSpec("order_a_taco", "place an order", {"type": "object", "properties": {}}),
    ToolSpec("order_more_inventory", "restock", {"type": "object", "properties": {}}),
    ToolSpec("take_payment", "charge", {"type": "object", "properties": {}}),
]


def _turn(tools: list[tuple[str, str]] | None = None, text: str = "") -> LlmTurn:
    calls = [
        ToolCall(tool_use_id=tid, name=name, arguments={}, idempotency_key="")
        for tid, name in (tools or [])
    ]
    content: list[dict] = []
    if text:
        content.append({"type": "text", "text": text})
    for c in calls:
        content.append({"type": "tool_use", "id": c.tool_use_id, "name": c.name, "input": {}})
    return LlmTurn(
        assistant_content=content,
        tool_calls=calls,
        text=text,
        usage=LlmUsage(input_tokens=100, output_tokens=50),
        stop_reason="tool_use" if calls else "end_turn",
    )


class Harness:
    """Records what happened so tests can assert on the agent's behaviour."""

    def __init__(self, script: list[LlmTurn]) -> None:
        self.script = script
        self.llm_calls = 0
        self.tool_attempts: list[str] = []
        self.keys_seen: list[str] = []
        self.flaky_remaining = 0

    def activities(self):
        harness = self

        @activity.defn(name="list_richards_tools")
        async def list_richards_tools() -> list[ToolSpec]:
            return TOOLS

        @activity.defn(name="llm_turn")
        async def llm_turn(payload: LlmTurnInput) -> LlmTurn:
            turn = harness.script[min(harness.llm_calls, len(harness.script) - 1)]
            harness.llm_calls += 1
            return turn

        @activity.defn(name="call_richards_tool")
        async def call_richards_tool(call: ToolCall) -> ToolOutcome:
            harness.tool_attempts.append(call.name)
            harness.keys_seen.append(call.idempotency_key)

            if harness.flaky_remaining > 0:
                harness.flaky_remaining -= 1
                raise McpInfrastructureError("supplier API 503")

            if call.name == "order_a_taco" and "order_more_inventory" not in harness.tool_attempts:
                # Business outcome: succeeds, but reports bad news.
                return ToolOutcome(
                    tool_use_id=call.tool_use_id,
                    content="OUT_OF_STOCK: chicken.",
                    is_error=True,
                )
            return ToolOutcome(tool_use_id=call.tool_use_id, content=f"{call.name} ok")

        return [list_richards_tools, llm_turn, call_richards_tool]


async def _run(client: Client, harness: Harness, order: OrderRequest) -> tuple[str, AgentState]:
    task_queue = f"test-{uuid.uuid4()}"
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[TacoOrderAgent],
        activities=harness.activities(),
    ):
        handle = await client.start_workflow(
            TacoOrderAgent.run, order, id=f"wf-{uuid.uuid4()}", task_queue=task_queue
        )
        result = await handle.result()
        state = await handle.query(TacoOrderAgent.status)
        return result, state


@pytest.mark.asyncio
async def test_happy_path_chains_tools():
    async with await WorkflowEnvironment.start_time_skipping() as env:
        harness = Harness(
            [
                _turn([("t1", "get_specials")]),
                _turn([("t2", "order_a_taco")]),
                _turn([("t3", "order_more_inventory")]),
                _turn([("t4", "order_a_taco")]),
                _turn([("t5", "take_payment")]),
                _turn(text="Two chicken tacos, $14.20. Order 4471 confirmed."),
            ]
        )
        result, state = await _run(
            env.client, harness, OrderRequest(order_id="o1", request="two tacos")
        )

    assert "4471" in result
    assert state.finished is True
    assert state.tool_calls == 5
    assert state.turns == 6
    assert state.tokens_spent == 900  # 6 turns x 150
    assert state.tools_available == [t.name for t in TOOLS]


@pytest.mark.asyncio
async def test_out_of_stock_is_not_retried():
    """The load-bearing test.

    A business outcome must reach the model exactly once so it can re-plan.
    If someone later makes the MCP client raise on isError, this fails with a
    retry storm instead of a clean single attempt.
    """
    async with await WorkflowEnvironment.start_time_skipping() as env:
        harness = Harness(
            [
                _turn([("t1", "order_a_taco")]),
                _turn([("t2", "order_more_inventory")]),
                _turn([("t3", "order_a_taco")]),
                _turn(text="done"),
            ]
        )
        _, state = await _run(
            env.client, harness, OrderRequest(order_id="o2", request="two tacos")
        )

    # Exactly two order attempts: the out-of-stock one and the retry after
    # restocking. Not a loop.
    assert harness.tool_attempts == ["order_a_taco", "order_more_inventory", "order_a_taco"]
    assert state.tool_calls == 3


@pytest.mark.asyncio
async def test_infrastructure_failure_retries_without_calling_the_model_again():
    """Two 503s, then success — and the model is asked exactly as many times
    as it would have been with no failure at all. That is the token story."""
    async with await WorkflowEnvironment.start_time_skipping() as env:
        harness = Harness([_turn([("t1", "get_specials")]), _turn(text="ok")])
        harness.flaky_remaining = 2

        _, state = await _run(
            env.client, harness, OrderRequest(order_id="o3", request="two tacos")
        )

    assert len(harness.tool_attempts) == 3   # 2 failures + 1 success
    assert state.tool_calls == 1             # but only one logical tool call
    assert harness.llm_calls == 2            # model untouched by the retries
    assert state.tokens_spent == 300


@pytest.mark.asyncio
async def test_idempotency_key_is_stable_across_retries():
    """Every attempt of the same call must carry the same key, or the MCP
    server sees a retry as a brand new order and charges twice."""
    async with await WorkflowEnvironment.start_time_skipping() as env:
        harness = Harness([_turn([("t1", "take_payment")]), _turn(text="ok")])
        harness.flaky_remaining = 3

        _, _ = await _run(env.client, harness, OrderRequest(order_id="o4", request="pay"))

    assert len(harness.keys_seen) == 4
    assert len(set(harness.keys_seen)) == 1, harness.keys_seen
    assert harness.keys_seen[0].endswith("-t0-c0")
