"""Tests for the stage collector's bookkeeping.

Most of these guard bugs that actually shipped and showed up on the projector:
a completion event re-firing twice a second forever, a finished order from a
previous session being adopted on startup, and the timeline resetting whenever
a new order began.
"""

from __future__ import annotations

import pytest

from richards_agent.shared import AgentState
from stage.collector import Collector


class _Handle:
    def __init__(self, state: AgentState) -> None:
        self._state = state

    async def query(self, *_args, **_kwargs) -> AgentState:
        return self._state

    async def describe(self):  # pragma: no cover - only hit on query failure
        raise AssertionError("describe() should not be needed while queries work")


class _Client:
    def __init__(self, handle: _Handle) -> None:
        self._handle = handle

    def get_workflow_handle(self, _workflow_id: str) -> _Handle:
        return self._handle


def _collector(state: AgentState) -> Collector:
    # Empty board config: board.fetch short-circuits, so these tests exercise
    # only the workflow-polling path.
    return Collector(_Client(_Handle(state)), "taco-order-1", "", "")


@pytest.mark.asyncio
async def test_completion_is_logged_exactly_once():
    """The bug: the "already logged" marker was written into self._order, which
    is rebuilt from the query on every tick — so it was wiped each pass and the
    event re-fired at the poll rate, filling the screen with duplicates."""
    state = AgentState(
        order_id="1", status="done", step="complete", turns=4, tool_calls=4,
        tokens_spent=1432, finished=True, answer="Two chicken tacos confirmed.",
    )
    collector = _collector(state)

    for _ in range(10):
        await collector._poll_workflow()

    completions = [e for e in collector._timeline if e.message == "order complete"]
    assert len(completions) == 1, f"expected 1 completion event, got {len(completions)}"


@pytest.mark.asyncio
async def test_step_transitions_are_logged_once_each():
    """Same class of bug for step changes — only transitions should log."""
    state = AgentState(order_id="1", status="thinking", step="llm_turn 1")
    collector = _collector(state)

    await collector._poll_workflow()
    await collector._poll_workflow()
    state.step = "order_taco"
    state.status = "calling tool"
    await collector._poll_workflow()
    await collector._poll_workflow()

    steps = [e.message for e in collector._timeline if e.kind == "ok"]
    assert steps == ["llm_turn 1", "order_taco"]


@pytest.mark.asyncio
async def test_the_timeline_is_continuous_across_orders():
    """One log for the whole session, not one per order.

    The room should still be able to see the crash from two orders ago sitting
    above the current one — that history is the evidence the demo is built on.
    Only the per-order numbers reset.
    """
    state = AgentState(
        order_id="1", status="done", step="complete", turns=4,
        tokens_spent=1432, finished=True, answer="done",
    )
    collector = _collector(state)
    await collector._poll_workflow()

    before = [e.message for e in collector._timeline]
    assert before, "first order should have logged something"

    collector.set_workflow("taco-order-2")

    after = [e.message for e in collector._timeline]
    assert after[: len(before)] == before, "earlier events must survive a new order"
    assert after[-1] == "new order started"

    # Per-order numbers do reset, and a second completion may be logged.
    assert collector._score.tokens_spent == 0
    assert collector._completion_logged is False


@pytest.mark.asyncio
async def test_timeline_timestamps_never_go_backwards():
    """The clock is session-scoped. A per-order clock would restart at 0.0s
    partway down a continuous timeline, so times would appear to rewind."""
    collector = _collector(AgentState(order_id="1", status="thinking", step="llm_turn 1"))
    await collector._poll_workflow()

    collector.set_workflow("taco-order-2")
    collector._push("ok", "later event")

    stamps = [e.t for e in collector._timeline]
    assert stamps == sorted(stamps), f"timestamps rewound: {stamps}"


@pytest.mark.asyncio
async def test_scoreboard_tracks_the_workflow_state():
    state = AgentState(
        order_id="1", status="calling tool", step="order_taco", turns=3,
        tool_calls=2, tokens_spent=900, cache_read_tokens=400,
        dedup_hits=1, downstream_attempts=143,
    )
    collector = _collector(state)
    await collector._poll_workflow()

    score = collector._score
    assert (score.turns, score.tool_calls, score.tokens_spent) == (3, 2, 900)
    assert score.cache_read_tokens == 400
    assert score.reissued_calls == 1
    assert score.downstream_attempts == 143
    # Nothing died, so the durability counters stay honest at zero.
    assert score.worker_deaths == 0
    assert score.tokens_not_respent == 0


def test_snapshot_seq_advances_so_the_client_can_skip_repeats():
    collector = _collector(AgentState(order_id="1"))
    first = collector.snapshot()["seq"]
    second = collector.snapshot()["seq"]
    assert second == first + 1


@pytest.mark.asyncio
async def test_cumulative_counts_survive_a_new_order():
    """Orders completed and worker deaths describe the whole session.

    Zeroing them whenever a new order starts would erase the tally the audience
    has been building — which is the entire point of those two tiles.
    """
    collector = _collector(AgentState(order_id="1", turns=3, tool_calls=2, tokens_spent=900))
    await collector._poll_workflow()

    collector._score.orders_completed = 7
    collector._score.worker_deaths = 3
    collector._score.tokens_not_respent = 11240
    collector._score.calls_served_from_history = 5

    collector.set_workflow("taco-order-2")

    # Session-scoped: carried forward.
    assert collector._score.orders_completed == 7
    assert collector._score.worker_deaths == 3
    assert collector._score.tokens_not_respent == 11240
    assert collector._score.calls_served_from_history == 5

    # Per-order: reset, because they describe the order we just left.
    assert collector._score.turns == 0
    assert collector._score.tool_calls == 0
    assert collector._score.tokens_spent == 0
