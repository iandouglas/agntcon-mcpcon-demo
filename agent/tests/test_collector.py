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


class _Status:
    def __init__(self, name: str) -> None:
        self.name = name


class _Description:
    def __init__(self, status: str) -> None:
        self.status = _Status(status)


class _Handle:
    """Queries always succeed, which is the case the freeze lived in.

    `describe` is counted, not forbidden. It used to raise on the grounds that
    a working query meant the workflow was alive -- which is exactly the wrong
    assumption: Temporal answers queries from the history of a CLOSED workflow
    too, so a failed order keeps replying `finished=False` forever.
    """

    def __init__(self, state: AgentState, status: str = "RUNNING") -> None:
        self._state = state
        self.status = status
        self.describes = 0

    async def query(self, *_args, **_kwargs) -> AgentState:
        return self._state

    async def describe(self) -> _Description:
        self.describes += 1
        return _Description(self.status)


class _Client:
    def __init__(self, handle: _Handle) -> None:
        self._handle = handle

    def get_workflow_handle(self, _workflow_id: str) -> _Handle:
        return self._handle


def _collector(state: AgentState, status: str = "RUNNING") -> Collector:
    # Empty board config: board.fetch short-circuits, so these tests exercise
    # only the workflow-polling path.
    return Collector(_Client(_Handle(state, status)), "taco-order-1", "", "")


def _handle_of(collector: Collector) -> _Handle:
    return collector._client.get_workflow_handle("taco-order-1")


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


# ------------------------------------------------- releasing a dead order
#
# The freeze these guard: the screen sat on an order that had failed ten hours
# earlier, while twenty real orders ran to completion behind it. A query
# against a closed workflow succeeds and replays its last state, so `finished`
# stayed False and `following_live` stayed True forever. `_following_closed`
# existed for this, but only the query-raised path ever set it.


@pytest.mark.asyncio
async def test_a_failed_order_releases_the_screen():
    """The load-bearing one. Query works, says unfinished; the workflow is
    over. Nothing here raises, so the old `except`-only guard never fired."""
    state = AgentState(order_id="1", status="thinking", step="llm_turn 1")
    collector = _collector(state, status="FAILED")

    assert collector.following_live is True
    await collector._poll_workflow()

    assert collector.following_live is False, "auto-follow can never move on"
    assert collector._following_closed is True


@pytest.mark.asyncio
async def test_a_running_order_is_not_released():
    """The other half: an order mid-flight must keep the screen."""
    state = AgentState(order_id="1", status="thinking", step="llm_turn 1")
    collector = _collector(state, status="RUNNING")

    for _ in range(5):
        collector._last_closed_check = 0.0
        await collector._poll_workflow()

    assert collector.following_live is True


@pytest.mark.asyncio
async def test_a_finished_order_never_costs_a_describe():
    """It already said `finished`. Asking Temporal to confirm is pure cost."""
    state = AgentState(
        order_id="1", status="done", step="complete", finished=True, answer="done",
    )
    collector = _collector(state, status="COMPLETED")

    for _ in range(5):
        await collector._poll_workflow()

    assert _handle_of(collector).describes == 0


@pytest.mark.asyncio
async def test_the_closed_check_is_throttled():
    """Polling runs at 2Hz. One describe per poll would double the RPC load on
    every healthy order to catch a case that is rare."""
    state = AgentState(order_id="1", status="thinking", step="llm_turn 1")
    collector = _collector(state, status="RUNNING")

    for _ in range(20):
        await collector._poll_workflow()

    assert _handle_of(collector).describes == 1, "throttle is not holding"


@pytest.mark.asyncio
async def test_releasing_is_announced_once_not_at_the_poll_rate():
    """Same bug class as the duplicate-completion one above."""
    state = AgentState(order_id="1", status="thinking", step="llm_turn 1")
    collector = _collector(state, status="FAILED")

    for _ in range(20):
        collector._last_closed_check = 0.0
        await collector._poll_workflow()

    ended = [e for e in collector._timeline if e.message == "order ended without finishing"]
    assert len(ended) == 1, f"expected 1 event, got {len(ended)}"


@pytest.mark.asyncio
async def test_a_fresh_order_is_given_a_grace_period():
    """`set_workflow` arms the throttle, so adopting an order does not spend a
    describe on a workflow that started milliseconds ago."""
    state = AgentState(order_id="1", status="thinking", step="llm_turn 1")
    collector = _collector(state, status="RUNNING")

    collector.set_workflow("taco-order-2")
    await collector._poll_workflow()

    assert _handle_of(collector).describes == 0


@pytest.mark.asyncio
async def test_a_closed_order_does_not_report_a_worker_that_is_fine():
    """STOP ALL ORDERS terminates the followed workflow, which makes the query
    raise. The old code answered every raise with "worker down", so pressing
    the button told you the worker had crashed when you had simply stopped the
    order -- and the worker is right there, still polling."""

    class _Closed(_Handle):
        async def query(self, *_args, **_kwargs):
            raise RuntimeError("workflow is terminated")

    collector = _collector(AgentState(order_id="1", status="thinking", step="llm_turn 1"))
    handle = _Closed(AgentState(order_id="1"), status="TERMINATED")
    collector._client = _Client(handle)

    await collector._poll_workflow()

    assert collector._order["status"] == "order ended"
    assert collector._score.worker_deaths == 0, "nothing died; do not bank a death"
    assert collector.following_live is False, "auto-follow must be free to move on"


@pytest.mark.asyncio
async def test_a_genuinely_dead_worker_still_says_so():
    """The other half, and the one that matters on stage: when the workflow is
    still open, a failing query really is a dead worker and must be scored."""

    class _Unreachable(_Handle):
        async def query(self, *_args, **_kwargs):
            raise RuntimeError("no worker")

        async def describe(self):
            self.describes += 1
            return _Description("RUNNING")

    collector = _collector(AgentState(order_id="1", status="thinking", step="llm_turn 1"))
    collector._client = _Client(_Unreachable(AgentState(order_id="1")))
    collector._score.tokens_spent = 630

    await collector._poll_workflow()

    assert collector._order["status"] == "worker down"
    assert collector._score.worker_deaths == 1
    assert collector._score.tokens_not_respent == 630
    assert collector.following_live is True, "do not cut away from the kill"
