"""Activities: everything that touches the outside world.

Each of these is retried by Temporal, so each must be safe to run again.
The LLM call is safe because it's a pure function of the message history.
The tool call is safe because it carries an idempotency key the MCP server
uses to attach to an existing execution instead of running twice.
"""

from __future__ import annotations

import anthropic
from temporalio import activity
from temporalio.exceptions import ApplicationError

from . import config, mcp_client
from .shared import LlmTurn, LlmTurnInput, LlmUsage, ToolCall, ToolOutcome, ToolSpec

_client: anthropic.AsyncAnthropic | None = None


def _anthropic() -> anthropic.AsyncAnthropic:
    """One client per worker process.

    max_retries=0 is the important argument. By default the SDK quietly
    retries 429s and 5xxs inside this call — which would bury a rate limit
    inside the activity where Temporal can't see it, can't back off from,
    and can't show on the timeline. Temporal is the retry layer here.
    """
    global _client
    if _client is None:
        _client = anthropic.AsyncAnthropic(max_retries=0)
    return _client


@activity.defn
async def list_richards_tools() -> list[ToolSpec]:
    """Ask the MCP server what it can do.

    Called once per order. The result lands in workflow history, so a replay
    reuses the exact tool list the run started with even if the server has
    since been reconfigured — which is the correct behaviour, not a bug.
    """
    activity.logger.info("discovering tools at %s", config.MCP_ENDPOINT)
    try:
        tools = await mcp_client.list_tools(config.MCP_ENDPOINT, config.MCP_AUTH_TOKEN)
    except mcp_client.McpProtocolError as exc:
        raise ApplicationError(str(exc), type="McpProtocolError", non_retryable=True) from exc

    activity.logger.info("discovered: %s", ", ".join(t.name for t in tools))
    return tools


@activity.defn
async def llm_turn(payload: LlmTurnInput) -> LlmTurn:
    """One trip to the model.

    If this activity is retried, the model is called again — that IS a real
    token cost. If the *workflow* replays after a crash, this activity is not
    re-run at all; its result comes from history. That difference is the
    number on the scoreboard.
    """
    client = _anthropic()

    tools = [
        {
            "name": spec.name,
            "description": spec.description,
            "input_schema": spec.input_schema,
        }
        for spec in payload.tools
    ]

    # Thinking is on by default on Opus 5; only pass the parameter to turn it off.
    extra: dict = {}
    if config.THINKING == "off":
        extra["thinking"] = {"type": "disabled"}

    try:
        response = await client.messages.create(
            model=config.MODEL,
            max_tokens=config.MAX_TOKENS,
            output_config={"effort": config.EFFORT},
            **extra,
            # System prompt and tool list are byte-stable across every turn of
            # every order, so they cache. Volatile content (the conversation)
            # comes after, which is where it has to be for the prefix to hold.
            system=[
                {
                    "type": "text",
                    "text": config.SYSTEM_PROMPT,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            tools=tools,
            messages=payload.messages,
        )
    except anthropic.APIStatusError as exc:
        if exc.status_code >= 500 or exc.status_code == 429:
            # Let it out — Temporal retries with backoff, visibly.
            raise
        raise ApplicationError(
            f"model rejected the request ({exc.status_code}): {exc.message}",
            type="AnthropicBadRequest",
            non_retryable=True,
        ) from exc

    if response.stop_reason == "refusal":
        detail = getattr(response, "stop_details", None)
        raise ApplicationError(
            f"model refused: {getattr(detail, 'category', 'unknown')}",
            type="AnthropicRefusal",
            non_retryable=True,
        )

    if response.stop_reason == "max_tokens":
        # Truncated mid-turn. Retrying gives the same truncation, so fail loudly
        # rather than let the workflow read "no tool calls" as "order finished".
        raise ApplicationError(
            f"turn truncated at max_tokens={config.MAX_TOKENS} — raise "
            "AGENT_MAX_TOKENS or set AGENT_THINKING=off",
            type="AnthropicTruncated",
            non_retryable=True,
        )

    assistant_content: list[dict] = []
    tool_calls: list[ToolCall] = []
    text_parts: list[str] = []

    for index, block in enumerate(response.content):
        assistant_content.append(block.model_dump(exclude_none=True))
        if block.type == "text":
            text_parts.append(block.text)
        elif block.type == "tool_use":
            tool_calls.append(
                ToolCall(
                    tool_use_id=block.id,
                    name=block.name,
                    arguments=dict(block.input or {}),
                    # Filled in by the workflow, which is the only place that
                    # can derive a key that survives replay unchanged.
                    idempotency_key="",
                )
            )

    usage = LlmUsage(
        input_tokens=response.usage.input_tokens or 0,
        output_tokens=response.usage.output_tokens or 0,
        cache_read_tokens=getattr(response.usage, "cache_read_input_tokens", 0) or 0,
    )

    activity.logger.info(
        "turn %s: %s tool call(s), %s tokens (%s cached)",
        payload.turn,
        len(tool_calls),
        usage.total,
        usage.cache_read_tokens,
    )

    return LlmTurn(
        assistant_content=assistant_content,
        tool_calls=tool_calls,
        text="\n".join(text_parts).strip(),
        usage=usage,
        stop_reason=response.stop_reason or "",
    )


@activity.defn
async def call_richards_tool(call: ToolCall) -> ToolOutcome:
    """Call one MCP tool.

    Note what does NOT happen here: no branching on which tool it is, no
    special handling for payment, no retry loop. The tool is whatever the
    server said it was, and the retrying belongs to Temporal.
    """
    activity.logger.info("calling %s (key=%s)", call.name, call.idempotency_key)

    try:
        result = await mcp_client.call_tool(
            config.MCP_ENDPOINT,
            config.MCP_AUTH_TOKEN,
            call.name,
            call.arguments,
            call.idempotency_key,
            read_seconds=config.MCP_CALL_TIMEOUT_SECONDS,
        )
    except mcp_client.McpProtocolError as exc:
        # Retrying a malformed call just wastes stage time. Fail it terminally
        # and let the workflow hand the model an error result to react to.
        raise ApplicationError(str(exc), type="McpProtocolError", non_retryable=True) from exc
    # McpInfrastructureError deliberately propagates: Temporal retries it.

    if result.attempts > 1:
        activity.logger.info(
            "%s took %s attempts (~%ss of outage)", call.name, result.attempts, result.attempts
        )

    # Was this call re-issued? If our activity is on attempt 2+, the first
    # attempt already reached the MCP server (severed stream, timeout) and the
    # idempotency key is the only reason we aren't ordering a second taco.
    #
    # Derived from Temporal rather than from the server's response: Ian's
    # server does not currently set _meta.deduplicated, and we don't need it
    # to — this is measurable entirely on our side. Honour the flag too, in
    # case he adds it.
    reissued = activity.info().attempt > 1
    if reissued or result.deduplicated:
        activity.logger.info(
            "%s was re-issued (activity attempt %s) — idempotency key attached it "
            "to the original workflow",
            call.name,
            activity.info().attempt,
        )

    return ToolOutcome(
        tool_use_id=call.tool_use_id,
        content=result.content,
        is_error=result.is_error,
        attempts=result.attempts,
        workflow_id=result.workflow_id,
        deduplicated=result.deduplicated or reissued,
    )
