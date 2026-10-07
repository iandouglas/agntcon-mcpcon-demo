"""The stage screen's backend: merges our workflow state with Ian's board.

    uv run uvicorn stage.server:app --port 8900
    open http://localhost:8900

Follows the most recent TacoOrderAgent execution automatically, so starting a
new order mid-talk needs no clicks. Override with ?workflow_id=... or POST
/api/follow.

Orders are placed from the screen itself via POST /api/orders -- see
`orders.py` for why a batch is submitted in the background rather than inline.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import pathlib

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse

from richards_agent.shared import TASK_QUEUE
from richards_agent.worker import connect

from .collector import Collector
from .orders import MAX_BATCH, LauncherBusy, OrderLauncher

BOARD_URL = os.getenv("BOARD_URL", "https://audience-site-production.up.railway.app")
BOARD_ADMIN_TOKEN = os.getenv("BOARD_ADMIN_TOKEN", "")
SNAPSHOT_HZ = float(os.getenv("STAGE_SNAPSHOT_HZ", "5"))
ORDER_REQUEST = os.getenv("STAGE_ORDER_REQUEST", "").strip()

STATIC = pathlib.Path(__file__).parent / "static"

_collector: Collector | None = None
_launcher: OrderLauncher | None = None
_task: asyncio.Task | None = None
_follow_task: asyncio.Task | None = None


async def _running_workflow_id(client) -> str | None:
    """The newest RUNNING TacoOrderAgent execution, or None.

    Deliberately ignores closed executions. Adopting the most recent one of
    those meant the screen booted showing a finished rehearsal order — four
    turns, a full scoreboard, no workflow in sight — which is exactly the
    confusing state you do not want on a projector before you have started.

    An order that finishes *while we are watching* stays on screen, because we
    keep following it; we just never adopt a stale one on startup.
    """
    query = (
        f'WorkflowType = "TacoOrderAgent" AND TaskQueue = "{TASK_QUEUE}" '
        'AND ExecutionStatus = "Running"'
    )
    try:
        async for execution in client.list_workflows(query):
            return execution.id
    except Exception:  # noqa: BLE001 - visibility may lag or be unavailable
        return None
    return None


async def _auto_follow(client) -> None:
    """Adopt a new order the moment one starts, so nothing needs clicking.

    Only ever adopts when we are *not* mid-order. With a batch of hundreds in
    flight, `_running_workflow_id` returns a different execution on almost
    every pass; adopting each one reset the per-order counters twice a second
    and filled the timeline with "new order started" instead of the run the
    room was watching. Following one order to its end and then picking up the
    next is both calmer and what the screen claims to be showing.
    """
    while True:
        await asyncio.sleep(1.5)
        if _collector is None or _collector.following_live:
            continue
        running = await _running_workflow_id(client)
        if running and running != _collector.workflow_id:
            _collector.set_workflow(running)


app = FastAPI(title="Chaos Kitchen — stage screen")


@app.on_event("startup")
async def startup() -> None:
    global _collector, _launcher, _task, _follow_task
    client = await connect()
    _collector = Collector(
        client,
        await _running_workflow_id(client),
        BOARD_URL,
        BOARD_ADMIN_TOKEN,
    )
    _launcher = OrderLauncher(client, **({"request": ORDER_REQUEST} if ORDER_REQUEST else {}))
    _task = asyncio.create_task(_collector.run())
    _follow_task = asyncio.create_task(_auto_follow(client))


@app.on_event("shutdown")
async def shutdown() -> None:
    if _launcher:
        await _launcher.aclose()
    for task in (_task, _follow_task):
        if task:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task


def _snapshot() -> dict:
    """The collector's snapshot plus whatever the launcher is doing.

    Merged here rather than inside the collector: the launcher is a control
    surface, not something observed about the run, and the collector's job is
    to stay the one place that reads state rather than writes it.
    """
    assert _collector is not None
    snapshot = _collector.snapshot()
    snapshot["launcher"] = _launcher.progress() if _launcher else {}
    return snapshot


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(STATIC / "index.html")


@app.get("/api/snapshot")
async def snapshot() -> JSONResponse:
    if _collector is None:
        return JSONResponse({"error": "collector not started"}, status_code=503)
    return JSONResponse(_snapshot())


@app.post("/api/orders")
async def place_orders(request: Request) -> JSONResponse:
    """Queue `count` individual orders.

    `count` is a number of separate `TacoOrderAgent` workflows, not a quantity
    of tacos on one order. Thirty means thirty orders the room can watch pile
    up, which is the whole reason this exists.
    """
    if _launcher is None:
        return JSONResponse({"error": "launcher not started"}, status_code=503)
    try:
        body = await request.json()
    except (ValueError, UnicodeDecodeError):
        return JSONResponse({"error": "body must be JSON"}, status_code=400)
    if not isinstance(body, dict):
        return JSONResponse({"error": "body must be a JSON object"}, status_code=400)

    count = body.get("count", 1)
    # `isinstance(True, int)` is True in Python, and `{"count": true}` meaning
    # "one order" would be a silly way to find that out on stage.
    if isinstance(count, bool) or not isinstance(count, int):
        return JSONResponse({"error": "'count' must be a whole number"}, status_code=400)
    if not 1 <= count <= MAX_BATCH:
        return JSONResponse(
            {"error": f"'count' must be between 1 and {MAX_BATCH}"}, status_code=400
        )

    try:
        return JSONResponse(_launcher.submit(count), status_code=202)
    except LauncherBusy as exc:
        return JSONResponse({"error": str(exc), **_launcher.progress()}, status_code=409)


@app.post("/api/orders/stop")
async def stop_orders() -> JSONResponse:
    """Stop submitting the rest of the batch. Started orders are left alone."""
    if _launcher is None:
        return JSONResponse({"error": "launcher not started"}, status_code=503)
    return JSONResponse(_launcher.stop())


@app.post("/api/follow")
async def follow(request: Request) -> JSONResponse:
    body = await request.json()
    workflow_id = body.get("workflow_id")
    if not workflow_id or _collector is None:
        return JSONResponse({"error": "workflow_id required"}, status_code=400)
    _collector.set_workflow(workflow_id)
    return JSONResponse({"following": workflow_id})


@app.get("/api/stream")
async def stream() -> StreamingResponse:
    """Full-state snapshots, not diffs — a dropped frame self-corrects."""

    async def gen():
        interval = 1.0 / max(SNAPSHOT_HZ, 1.0)
        while True:
            if _collector is not None:
                yield f"data: {json.dumps(_snapshot())}\n\n"
            await asyncio.sleep(interval)

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
