"""The stage screen's backend: merges our workflow state with Ian's board.

    uv run uvicorn stage.server:app --port 8900
    open http://localhost:8900

Follows the most recent TacoOrderAgent execution automatically, so starting a
new order mid-talk needs no clicks. Override with ?workflow_id=... or POST
/api/follow.
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

BOARD_URL = os.getenv("BOARD_URL", "https://audience-site-production.up.railway.app")
BOARD_ADMIN_TOKEN = os.getenv("BOARD_ADMIN_TOKEN", "")
SNAPSHOT_HZ = float(os.getenv("STAGE_SNAPSHOT_HZ", "5"))

STATIC = pathlib.Path(__file__).parent / "static"

_collector: Collector | None = None
_task: asyncio.Task | None = None
_follow_task: asyncio.Task | None = None


async def _latest_workflow_id(client) -> str | None:
    """Most recent TacoOrderAgent execution, running or not."""
    query = f'WorkflowType = "TacoOrderAgent" AND TaskQueue = "{TASK_QUEUE}"'
    try:
        async for execution in client.list_workflows(query):
            return execution.id
    except Exception:  # noqa: BLE001 - visibility may lag or be unavailable
        return None
    return None


async def _auto_follow(client) -> None:
    """Switch to a newer order when one appears, so the screen never goes stale."""
    while True:
        await asyncio.sleep(2.0)
        if _collector is None:
            continue
        latest = await _latest_workflow_id(client)
        if latest and latest != _collector._workflow_id:  # noqa: SLF001
            _collector.set_workflow(latest)


app = FastAPI(title="Chaos Kitchen — stage screen")


@app.on_event("startup")
async def startup() -> None:
    global _collector, _task, _follow_task
    client = await connect()
    _collector = Collector(
        client,
        await _latest_workflow_id(client),
        BOARD_URL,
        BOARD_ADMIN_TOKEN,
    )
    _task = asyncio.create_task(_collector.run())
    _follow_task = asyncio.create_task(_auto_follow(client))


@app.on_event("shutdown")
async def shutdown() -> None:
    for task in (_task, _follow_task):
        if task:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(STATIC / "index.html")


@app.get("/api/snapshot")
async def snapshot() -> JSONResponse:
    if _collector is None:
        return JSONResponse({"error": "collector not started"}, status_code=503)
    return JSONResponse(_collector.snapshot())


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
                yield f"data: {json.dumps(_collector.snapshot())}\n\n"
            await asyncio.sleep(interval)

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
