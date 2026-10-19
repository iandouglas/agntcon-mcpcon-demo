"""Watches the agent workflow and the audience board, and keeps one snapshot.

The interesting problem here is measuring **work not redone**, which is the
whole claim of the talk and is not directly observable from a workflow query.

The trick: when our worker is killed, the query stops answering ("no poller
seen"). When it answers again, the workflow has replayed — and `tokens_spent`
comes back *unchanged*, because every completed llm_turn was served from event
history instead of being re-sent to the model. So at each recovery we record
the tokens that were already banked at the moment of death. That sum is what a
naive agent would have had to spend again.

It is derived from something we actually saw happen, not a model of what a
hypothetical non-durable agent would do. That distinction matters if anyone in
the room pushes on the number.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from datetime import datetime, timezone
from dataclasses import asdict, dataclass, field
from typing import Any

import httpx
from temporalio.client import Client

from richards_agent.shared import TASK_QUEUE
from richards_agent.worker import read_health
from richards_agent.workflow import TacoOrderAgent

from . import board

log = logging.getLogger(__name__)

# One continuous timeline for the whole session, so a crash from two orders
# ago is still visible above the current one. Only the last ~24 render.
MAX_TIMELINE = 200

# How often to ask Temporal whether the order we are following is actually
# still open. Only consulted while the query says the order is unfinished, so
# a healthy order costs one extra RPC every few seconds and nothing else.
CLOSED_CHECK_SECONDS = 5.0

# Our own chaos feed. The audience board has no switch for the agent worker
# yet, so its health is read from here instead.
LOCAL_CHAOS_URL = os.getenv("CHAOS_URL", "http://localhost:8931/chaos")
LOCAL_COMPONENTS = ("agent_worker",)

# Which components the screen shows. Defaults to just the agent worker, because
# it is currently the only thing anyone can actually break — Ian's five
# switches render as permanently UP until he arms the floor, which is noise on
# a projector. Set STAGE_COMPONENTS=all once the floor is live, or a
# comma-separated list to pick.
_only = os.getenv("STAGE_COMPONENTS", "agent_worker").strip()
SHOW_ALL = _only.lower() in ("", "all", "*")
SHOW_ONLY = {c.strip() for c in _only.split(",") if c.strip()} if not SHOW_ALL else set()


@dataclass
class Event:
    t: float
    kind: str          # ok | crash | recover | info | switch
    message: str
    detail: str = ""


@dataclass
class Scoreboard:
    # --- on the projector -------------------------------------------------
    # Counted across every order since this stage server started, so it keeps
    # working when several orders are in flight at once.
    orders_completed: int = 0
    worker_deaths: int = 0
    turns: int = 0
    tool_calls: int = 0

    # --- collected, shown only in the footer line -------------------------
    # tokens_not_respent is the thesis number but reads as 0 until something
    # actually breaks, so it lives in the footer rather than on a big tile.
    tokens_spent: int = 0
    cache_read_tokens: int = 0
    tokens_not_respent: int = 0
    calls_served_from_history: int = 0
    reissued_calls: int = 0
    # Summed `attempts` from Ian's server — roughly seconds of outage absorbed.
    downstream_attempts: int = 0


class Collector:
    def __init__(
        self,
        client: Client,
        workflow_id: str | None,
        board_url: str,
        board_token: str,
        poll_seconds: float = 0.5,
    ) -> None:
        self._client = client
        self._workflow_id = workflow_id
        self._board_url = board_url
        self._board_token = board_token
        self._poll = poll_seconds

        self._board = board.BoardState()
        self._order: dict[str, Any] = {"running": False}
        self._score = Scoreboard()
        self._timeline: list[Event] = []
        # Session clock. Never restarted — a per-order clock would make
        # timestamps jump backwards partway down a continuous timeline.
        self._started = time.monotonic()

        self._worker_up = True
        self._last_step = ""
        self._last_switches: dict[str, bool] = {}
        self._seq = 0
        # Set when the followed workflow turns out to be closed rather than
        # orphaned by a dead worker. Without it `following_live` would stay
        # true forever on an order that failed or was terminated, and the
        # screen would sit on a corpse while the room watched nothing happen.
        self._following_closed = False
        # Throttle for the closed check below. Starts at 0.0 so the first
        # unfinished poll checks immediately: on startup we adopt whatever
        # visibility hands us, and that may already be a corpse.
        self._last_closed_check = 0.0
        # Separate flag, NOT a field on self._order — that dict is rebuilt from
        # the query every tick, so a marker written into it is wiped on the
        # next pass and the completion event re-fires twice a second forever.
        self._completion_logged = False

        # Order counts come from Temporal visibility rather than from the one
        # workflow we're following, so they stay correct when several orders
        # run at once. Scoped to this process's start so a morning of
        # rehearsals doesn't inflate the number on stage.
        self._session_start = datetime.now(timezone.utc).isoformat()
        self._orders_in_flight = 0
        self._last_count = 0.0
        self._count_error_logged = False

    # ------------------------------------------------------------------ api
    @property
    def workflow_id(self) -> str | None:
        return self._workflow_id

    @property
    def following_live(self) -> bool:
        """True while the order on screen is still worth watching.

        A dead worker counts as live on purpose: the order is still open, and
        jumping to a different one at the exact moment the room lands its kill
        would skip the replay we are there to show.
        """
        if not self._workflow_id or self._following_closed:
            return False
        return not self._order.get("finished", False)

    def set_workflow(self, workflow_id: str) -> None:
        """Follow a different order.

        The timeline is NOT cleared and the clock is NOT restarted: it is one
        continuous log of the whole session, so the room can see the crash from
        two orders ago still sitting above the current one. Only the per-order
        numbers reset.

        Cumulative counters also survive — "orders completed" and "worker deaths
        survived" describe the session, and zeroing them on every new order
        would erase the tally the audience has been building.
        """
        self._workflow_id = workflow_id
        self._following_closed = False
        # A just-started order is not a corpse. Give it the full interval
        # before spending a describe() on it.
        self._last_closed_check = time.monotonic()
        carried = {
            "orders_completed": self._score.orders_completed,
            "worker_deaths": self._score.worker_deaths,
            "tokens_not_respent": self._score.tokens_not_respent,
            "calls_served_from_history": self._score.calls_served_from_history,
        }
        self._score = Scoreboard(**carried)
        self._worker_up = True
        self._last_step = ""
        self._completion_logged = False
        self._order = {"running": False}
        self._push("info", "new order started", workflow_id)

    def snapshot(self) -> dict:
        self._seq += 1
        return {
            "seq": self._seq,
            "ts": time.time(),
            "elapsed": round(time.monotonic() - self._started, 1),
            "workflow_id": self._workflow_id or "",
            "board": {
                "reachable": self._board.reachable,
                "armed": self._board.armed,
                "conns": self._board.conns,
                "bus_healthy": self._board.bus_healthy,
                "error": self._board.error,
                "components": [asdict(c) for c in self._board.components],
            },
            "order": self._order,
            "orders_in_flight": self._orders_in_flight,
            "scoreboard": asdict(self._score),
            "timeline": [asdict(e) for e in self._timeline[-24:]],
        }

    async def run(self) -> None:
        async with httpx.AsyncClient() as http:
            while True:
                try:
                    await self._tick(http)
                except Exception as exc:  # noqa: BLE001 - a collector that dies blanks the screen
                    self._push("info", f"collector hiccup: {type(exc).__name__}")
                await asyncio.sleep(self._poll)

    # -------------------------------------------------------------- internals
    def _push(self, kind: str, message: str, detail: str = "") -> None:
        self._timeline.append(
            Event(t=round(time.monotonic() - self._started, 1), kind=kind,
                  message=message, detail=detail)
        )
        if len(self._timeline) > MAX_TIMELINE:
            self._timeline.pop(0)

    async def _tick(self, http: httpx.AsyncClient) -> None:
        self._board = await board.fetch(http, self._board_url, self._board_token)
        for component in await self._local_components(http):
            self._board.components.append(component)
        self._board.components = self._visible(self._board.components)
        self._note_switch_changes()
        await self._count_orders()
        await self._poll_workflow()

    async def _local_components(self, http: httpx.AsyncClient) -> list[board.Component]:
        """Read the components Ian's board doesn't carry yet.

        Only the agent worker today. Once he adds an `agent_worker` switch this
        becomes redundant and the component arrives from the board instead —
        same id, so the screen doesn't change.
        """
        if any(c.id in LOCAL_COMPONENTS for c in self._board.components):
            return []  # the board owns it now; don't double-render

        try:
            payload = (await http.get(LOCAL_CHAOS_URL, timeout=3.0)).json()
        except Exception:  # noqa: BLE001 - no local feed is a fine state to be in
            return []

        found: list[board.Component] = []
        for component_id in LOCAL_COMPONENTS:
            health = read_health(payload, component_id)
            if health is None:
                continue
            found.append(
                board.Component(
                    id=component_id,
                    name=component_id.replace("_", " ").upper(),
                    up=health >= board.THRESHOLD,
                    percent=health,
                    kind="worker",
                    source="local",
                )
            )
        return found

    def _visible(self, components: list[board.Component]) -> list[board.Component]:
        if SHOW_ALL:
            return components
        return [c for c in components if c.id in SHOW_ONLY]

    def _note_switch_changes(self) -> None:
        for component in self._board.components:
            previous = self._last_switches.get(component.id)
            if previous is None:
                self._last_switches[component.id] = component.up
                continue
            if previous != component.up:
                self._last_switches[component.id] = component.up
                verb = "back up" if component.up else "taken down"
                detail = f"{component.percent}%" if component.percent is not None else ""
                self._push("switch", f"{component.name} {verb}", detail)

    async def _count_orders(self, every: float = 2.0) -> None:
        """Count orders via visibility, not from the followed workflow.

        Two reasons: several orders can be in flight at once, and the one we
        follow tells us nothing about the others. Throttled because this is
        two RPCs, not a local read.
        """
        now = time.monotonic()
        if now - self._last_count < every:
            return
        self._last_count = now

        base = (
            f'WorkflowType = "TacoOrderAgent" AND TaskQueue = "{TASK_QUEUE}" '
            f'AND StartTime > "{self._session_start}"'
        )
        try:
            completed = await self._client.count_workflows(
                f'{base} AND ExecutionStatus = "Completed"'
            )
            running = await self._client.count_workflows(
                f'{base} AND ExecutionStatus = "Running"'
            )
        except Exception as exc:  # noqa: BLE001 - keep the last known count
            # Log it. A silent return here means the tile reads 0 forever and
            # looks like a demo that never completes an order, with nothing
            # anywhere saying why.
            if not self._count_error_logged:
                self._count_error_logged = True
                log.warning(
                    "order count query failed, tile will stay at its last value: %s: %s\n"
                    "  query: %s", type(exc).__name__, exc, base,
                )
            return

        if self._count_error_logged:
            self._count_error_logged = False
            log.info("order count query recovered")

        if completed.count > self._score.orders_completed:
            gained = completed.count - self._score.orders_completed
            self._push("info", f"order{'s' if gained > 1 else ''} completed",
                       f"{completed.count} since this screen started")
        self._score.orders_completed = completed.count
        self._orders_in_flight = running.count

    async def _poll_workflow(self) -> None:
        if not self._workflow_id:
            self._order = {"running": False, "status": "no order"}
            return

        handle = self._client.get_workflow_handle(self._workflow_id)
        try:
            state = await handle.query(TacoOrderAgent.status)
        except Exception:
            # Either the worker is gone or the workflow has closed. Distinguish
            # them: a closed workflow is not a crash, and calling it one would
            # put a fake death on the scoreboard at the end of every clean run.
            if self._worker_up and not self._order.get("finished"):
                closed = await self._workflow_closed(handle)
                if closed:
                    # Closed, but never reported `finished` through the query —
                    # failed, terminated, or timed out. Release it so
                    # `_auto_follow` can pick up the next order.
                    self._following_closed = True
                    # Not "worker down". Pressing STOP ALL ORDERS lands here,
                    # and a screen that answers it with "worker down" sends
                    # you hunting a crash you caused on purpose.
                    self._order = {**self._order, "running": False, "status": "order ended"}
                    return
                else:
                    self._worker_up = False
                    self._score.worker_deaths += 1
                    self._score.tokens_not_respent += self._score.tokens_spent
                    self._score.calls_served_from_history += self._score.tool_calls
                    self._push(
                        "crash",
                        "agent worker lost",
                        f"{self._score.tokens_spent:,} tokens already banked in history",
                    )
            self._order = {**self._order, "running": False, "status": "worker down"}
            return

        if not self._worker_up:
            self._worker_up = True
            self._push(
                "recover",
                "worker back · workflow replayed",
                f"{self._score.tokens_spent:,} tokens not re-spent · model not re-prompted",
            )

        if state.step and state.step != self._last_step:
            self._last_step = state.step
            self._push("ok", state.step, state.status)

        self._score.turns = state.turns
        self._score.tool_calls = state.tool_calls
        self._score.tokens_spent = state.tokens_spent
        self._score.cache_read_tokens = state.cache_read_tokens
        self._score.reissued_calls = state.dedup_hits
        self._score.downstream_attempts = state.downstream_attempts

        self._order = {
            "running": not state.finished,
            "status": state.status,
            "step": state.step,
            "finished": state.finished,
            "answer": state.answer,
            "tools_available": list(state.tools_available),
            "last_tool_workflow": state.last_workflow_id,
        }

        if state.finished and not self._completion_logged:
            self._completion_logged = True
            self._push("info", "order complete", state.answer[:120])

        if not state.finished:
            await self._release_if_closed(handle)

    async def _release_if_closed(self, handle) -> None:
        """Let go of an order that is over but never said so.

        A query against a CLOSED workflow succeeds: Temporal replays its
        history and answers from it. So a failed order keeps returning its
        last state, in which `finished` is False, and the only thing that ever
        set `_following_closed` was the `except` branch above, which needs the
        query to raise. It never does. The result was a screen stuck on a
        workflow that died ten hours earlier while real orders came and went
        behind it.

        A dead worker is deliberately NOT caught here: there the query raises
        and `_poll_workflow` handles it, which is what keeps the screen on the
        order the room just killed instead of cutting away from the replay.
        """
        if self._following_closed:
            # Already let go. `_auto_follow` adopts the next order when one
            # starts; until then we keep showing this one's last state rather
            # than re-announcing its death every few seconds.
            return

        now = time.monotonic()
        if now - self._last_closed_check < CLOSED_CHECK_SECONDS:
            return
        self._last_closed_check = now

        if not await self._workflow_closed(handle):
            return

        self._following_closed = True
        self._push(
            "crash",
            "order ended without finishing",
            self._workflow_id or "",
        )

    async def _workflow_closed(self, handle) -> bool:
        """True if the workflow has finished — so a failing query is expected."""
        try:
            description = await handle.describe()
            status = getattr(description, "status", None)
            return status is not None and getattr(status, "name", "") != "RUNNING"
        except Exception:  # noqa: BLE001 - can't tell; assume still running
            return False
