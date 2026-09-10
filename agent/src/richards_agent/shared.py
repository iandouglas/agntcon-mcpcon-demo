"""Types shared between the workflow and its activities.

Deliberately dependency-free: this module is imported inside the workflow
sandbox, so it must not pull in httpx, anthropic, or anything else heavy.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

TASK_QUEUE = "richards-agent"


@dataclass
class OrderRequest:
    """What the customer asked Richard for."""

    order_id: str
    request: str
    max_turns: int = 12


@dataclass
class ToolSpec:
    """One tool as the MCP server advertised it in tools/list."""

    name: str
    description: str
    input_schema: dict[str, Any]


@dataclass
class ToolCall:
    """A tool call the model asked for, plus the key that makes it idempotent."""

    tool_use_id: str
    name: str
    arguments: dict[str, Any]
    idempotency_key: str


@dataclass
class ToolOutcome:
    """The result of one tool call, as the model will see it.

    Reaching this type at all means the activity SUCCEEDED. Infrastructure
    failures never produce a ToolOutcome — they raise, and Temporal retries
    them without the model ever being asked again.

    `is_error` is the MCP server saying "the tool ran, and the answer is bad
    news" (out of stock, card declined). That is a business outcome: it goes
    back to the model so it can re-plan.
    """

    tool_use_id: str
    content: str
    is_error: bool = False
    deduplicated: bool = False


@dataclass
class LlmUsage:
    input_tokens: int = 0
    output_tokens: int = 0
    cache_read_tokens: int = 0

    @property
    def total(self) -> int:
        return self.input_tokens + self.output_tokens


@dataclass
class LlmTurn:
    """One trip to the model."""

    assistant_content: list[dict[str, Any]]
    tool_calls: list[ToolCall] = field(default_factory=list)
    text: str = ""
    usage: LlmUsage = field(default_factory=LlmUsage)
    stop_reason: str = ""


@dataclass
class LlmTurnInput:
    messages: list[dict[str, Any]]
    tools: list[ToolSpec]
    order_id: str
    turn: int


@dataclass
class AgentState:
    """Live progress, exposed by query for the stage screen."""

    order_id: str = ""
    status: str = "starting"
    step: str = ""
    turns: int = 0
    tool_calls: int = 0
    tokens_spent: int = 0
    cache_read_tokens: int = 0
    dedup_hits: int = 0
    tools_available: list[str] = field(default_factory=list)
    finished: bool = False
    answer: str = ""
