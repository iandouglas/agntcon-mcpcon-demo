"""The agent, as a Temporal workflow.

This file is the one that goes on the code screen. It should read like the
agent loop you'd write anyway — because it is. There is no retry code, no
backoff, no circuit breaker, no idempotency bookkeeping. Those are policy
arguments on two activity calls.
"""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from .activities import call_richards_tool, list_richards_tools, llm_turn
    from .shared import AgentState, LlmTurnInput, OrderRequest, ToolOutcome

# Unlimited. Not a typo, and worth saying out loud on stage: 0 means the
# activity retries until it succeeds. Nothing the room does can exhaust it.
FOREVER = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=10),
    maximum_attempts=0,
)


# Without this, Temporal waits out start_to_close before it will believe a
# worker is dead — up to 17 minutes for a tool call. With it, a killed worker is
# noticed in seconds and the activity is rescheduled on the replacement. The
# activities heartbeat every 2s (see activities._heartbeat_while).
HEARTBEAT_TIMEOUT = timedelta(seconds=5)


@workflow.defn
class TacoOrderAgent:
    def __init__(self) -> None:
        self.state = AgentState()

    @workflow.query
    def status(self) -> AgentState:
        """Read by the stage screen. Cheap, and safe to call constantly."""
        return self.state

    @workflow.run
    async def run(self, order: OrderRequest) -> str:
        self.state.order_id = order.order_id

        # Ask the server what Richard sells. Runtime discovery, because the
        # tool names live in the MCP server's env vars.
        self.state.step = "discovering tools"
        tools = await workflow.execute_activity(
            list_richards_tools,
            start_to_close_timeout=timedelta(seconds=30),
            heartbeat_timeout=HEARTBEAT_TIMEOUT,
            retry_policy=FOREVER,
        )
        self.state.tools_available = [t.name for t in tools]

        messages: list[dict] = [{"role": "user", "content": order.request}]

        for turn in range(order.max_turns):
            self.state.status = "thinking"
            self.state.step = f"llm_turn {turn + 1}"

            reply = await workflow.execute_activity(
                llm_turn,
                LlmTurnInput(
                    messages=messages,
                    tools=tools,
                    order_id=order.order_id,
                    turn=turn,
                ),
                start_to_close_timeout=timedelta(seconds=120),
                heartbeat_timeout=HEARTBEAT_TIMEOUT,
                retry_policy=FOREVER,
            )

            self.state.turns += 1
            self.state.tokens_spent += reply.usage.total
            self.state.cache_read_tokens += reply.usage.cache_read_tokens
            messages.append({"role": "assistant", "content": reply.assistant_content})

            if not reply.tool_calls:
                self.state.status = "done"
                self.state.step = "complete"
                self.state.finished = True
                self.state.answer = reply.text
                return reply.text

            # All tool results for one assistant turn go back in ONE user
            # message. Splitting them teaches the model to stop batching.
            results: list[dict] = []
            for index, call in enumerate(reply.tool_calls):
                # Derived from workflow state only, so it is byte-identical on
                # every replay. A UUID here would break dedup the moment the
                # worker was killed — the retry would look like a new order.
                call.idempotency_key = f"{workflow.info().workflow_id}-t{turn}-c{index}"

                self.state.status = "calling tool"
                self.state.step = call.name

                outcome = await self._call_tool(call)

                self.state.tool_calls += 1
                self.state.downstream_attempts += outcome.attempts
                if outcome.workflow_id:
                    self.state.last_workflow_id = outcome.workflow_id
                if outcome.deduplicated:
                    self.state.dedup_hits += 1

                results.append(
                    {
                        "type": "tool_result",
                        "tool_use_id": outcome.tool_use_id,
                        "content": outcome.content,
                        # Surfaces the server's business outcome to the model
                        # as a bad result it can reason about — not as a
                        # failure that stops the workflow.
                        "is_error": outcome.is_error,
                    }
                )

            messages.append({"role": "user", "content": results})

        raise ApplicationError(
            f"agent used all {order.max_turns} turns without finishing",
            type="AgentMaxTurns",
            non_retryable=True,
        )

    async def _call_tool(self, call) -> ToolOutcome:
        """Run one tool call, turning a terminal failure into model input.

        An infrastructure failure never gets here — it retries forever inside
        execute_activity. Only a genuinely unrecoverable call (bad tool name,
        malformed arguments) escapes, and the right move then is to tell the
        model what went wrong so it can try something else.
        """
        try:
            return await workflow.execute_activity(
                call_richards_tool,
                call,
                # Has to clear Railway's 15-minute request ceiling, which sits
                # above the MCP server's own 840s wait cap. A service the
                # audience is holding down makes this call SLOW, not failed —
                # time out early and we re-issue work that is still running.
                start_to_close_timeout=timedelta(minutes=17),
                heartbeat_timeout=HEARTBEAT_TIMEOUT,
                retry_policy=FOREVER,
            )
        except ApplicationError as exc:
            workflow.logger.warning("tool %s failed terminally: %s", call.name, exc)
            return ToolOutcome(
                tool_use_id=call.tool_use_id,
                content=f"Tool {call.name} could not be called: {exc}",
                is_error=True,
            )
