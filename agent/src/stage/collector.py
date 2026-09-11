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
import time
from dataclasses import asdict, dataclass, field
from typing import Any

import httpx
from temporalio.client import Client

from richards_agent.workflow import TacoOrderAgent

from . import board

MAX_TIMELINE = 60


@dataclass
class Event:
    t: float
    kind: str          # ok | crash | recover | info | switch
    message: str
    detail: str = ""


@dataclass
class Scoreboard:
    turns: int = 0
    tool_calls: int = 0
    tokens_spent: int = 0
    cache_read_tokens: int = 0
    # Banked work that survived a worker death. See the module docstring.
    tokens_not_respent: int = 0
    calls_served_from_history: int = 0
    worker_deaths: int = 0
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
        self._started = time.monotonic()

        self._worker_up = True
        self._last_step = ""
        self._last_switches: dict[str, bool] = {}
        self._seq = 0

    # ------------------------------------------------------------------ api
    def set_workflow(self, workflow_id: str) -> None:
        """Follow a different order. Resets the scoreboard; the timeline keeps
        a marker so it is obvious on screen that the numbers restarted."""
        self._workflow_id = workflow_id
        self._score = Scoreboard()
        self._started = time.monotonic()
        self._worker_up = True
        self._last_step = ""
        self._push("info", f"now following {workflow_id}")

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
        self._note_switch_changes()
        await self._poll_workflow()

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
                if not closed:
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

        if state.finished and self._order.get("status") != "done-logged":
            self._push("info", "order complete", state.answer[:120])
            self._order["status"] = "done-logged"

    async def _workflow_closed(self, handle) -> bool:
        """True if the workflow has finished — so a failing query is expected."""
        try:
            description = await handle.describe()
            status = getattr(description, "status", None)
            return status is not None and getattr(status, "name", "") != "RUNNING"
        except Exception:  # noqa: BLE001 - can't tell; assume still running
            return False
