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
class ToolResult:
    """What the MCP server told us, before it's tied back to a tool_use block."""

    content: str
    is_error: bool = False
    # Ian's server retries a broken downstream internally. `attempts` is the
    # count, and because Temporal's retry timers floor at 1s it is effectively
    # seconds of outage — a far better line than "4 retries a second".
    attempts: int = 0
    workflow_id: str = ""
    deduplicated: bool = False


@dataclass
class ToolOutcome:
    """The result of one tool call, as the model will see it.

    Reaching this type at all means the activity SUCCEEDED. Infrastructure
    failures never produce a ToolOutcome — they raise, and Temporal retries
    them without the model ever being asked again.

    `is_error` is the MCP server saying "the tool ran, and the answer is bad
    news". Ian's services cannot currently produce this (they return only
    200-true or a bare 500), but the path costs nothing and is where an
    out-of-stock outcome would land if he adds one.
    """

    tool_use_id: str
    content: str
    is_error: bool = False
    attempts: int = 0
    workflow_id: str = ""
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
    # Summed `attempts` across every tool call — roughly total seconds the
    # audience kept something broken. The headline "how bad was it" number.
    downstream_attempts: int = 0
    tools_available: list[str] = field(default_factory=list)
    last_workflow_id: str = ""
    finished: bool = False
    answer: str = ""
