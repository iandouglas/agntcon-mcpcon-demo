"""A local stand-in for Ian's durable MCP server + audience board.

Mirrors the observable contract of the real thing so the agent can be developed
and rehearsed offline. Crucially it copies the *behaviour* that matters:

    a tool call never fails because a service is down — it just takes longer

A broken service here is absorbed and retried internally, with the call
blocking and upgrading to SSE, exactly as Railway does. An earlier version of
this file returned 503 instead, which trained the agent for a failure mode that
does not exist.

What it is NOT: Temporal-backed. Tool calls are asyncio loops, not workflows,
and dedup is a dict. Delete this once the real server is the only target.

Run:    uv run uvicorn fake_mcp.server:app --port 8931
Break:  uv run python scripts/audience.py break service_1
"""

from __future__ import annotations

import asyncio
import json
import os
import time
import uuid
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Header, Request
from fastapi.responses import JSONResponse, StreamingResponse

PROTOCOL_VERSION = "2026-07-28"

# Ian's live tool names, in service order. Read from /healthz in real life —
# these have already changed once (order_coffee -> order_churro).
SERVICES: list[dict[str, str]] = [
    {"key": "service_1", "name": "order_taco", "title": "Order Taco",
     "description": "Place an order for a taco and wait until the kitchen confirms it."},
    {"key": "service_2", "name": "order_churro", "title": "Order Churro",
     "description": "Place an order for a churro and wait until the kitchen confirms it."},
    {"key": "service_3", "name": "restock_inventory", "title": "Restock Inventory",
     "description": "Reorder stock from the supplier and wait for confirmation."},
    {"key": "service_4", "name": "send_receipt", "title": "Send Receipt",
     "description": "Send the customer their receipt and wait for delivery confirmation."},
]
BY_NAME = {s["name"]: s for s in SERVICES}

# Temporal's retry timers floor at one second, so on the real server `attempts`
# is effectively seconds of outage. Shrunk here so tests aren't slow.
ATTEMPT_INTERVAL = float(os.getenv("FAKE_ATTEMPT_INTERVAL", "1.0"))
# Real server commits SSE after ~15s of silence.
SSE_AFTER = float(os.getenv("FAKE_SSE_AFTER", "15.0"))
MAX_WAIT = float(os.getenv("FAKE_MAX_WAIT", "840.0"))
RESTART_SECONDS = float(os.getenv("FAKE_RESTART_SECONDS", "5.0"))
AGENT_TOKEN = os.getenv("FAKE_AGENT_TOKEN", "")  # empty = accept anything

HARD_BREAK = {"agent_worker", "mcp_worker"}

HEALTH: dict[str, int] = {
    "agent_worker": 100,
    "llm_api": 100,
    "mcp_worker": 100,
    "service_1": 100,
    "service_2": 100,
    "service_3": 100,
    "service_4": 100,
}
DOWN_AT: dict[str, float] = {}
SEEN: dict[str, dict] = {}
LEDGER: dict[str, int] = {"ordered": 0, "fulfilled": 0}


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(_supervisor())
    yield
    task.cancel()


async def _supervisor() -> None:
    """Bring killed workers back, so the room can't stall the demo forever.

    Health returns degraded rather than full — easy to knock down again.
    """
    while True:
        await asyncio.sleep(0.5)
        now = time.monotonic()
        for name in HARD_BREAK:
            if HEALTH.get(name, 100) <= 0 and DOWN_AT.get(name):
                if now - DOWN_AT[name] >= RESTART_SECONDS:
                    HEALTH[name] = 30
                    DOWN_AT[name] = 0.0


app = FastAPI(title="fake durable MCP", lifespan=lifespan)


# --------------------------------------------------------------------------
# public: no auth, this is how the agent learns the tool names
# --------------------------------------------------------------------------
@app.get("/healthz")
async def healthz() -> dict:
    return {"ok": True, "tools": [s["name"] for s in SERVICES]}


# --------------------------------------------------------------------------
# MCP
# --------------------------------------------------------------------------
def _rpc_error(req_id: Any, code: int, message: str) -> JSONResponse:
    return JSONResponse({"jsonrpc": "2.0", "id": req_id, "error": {"code": code, "message": message}})


def _check_envelope(params: dict) -> str | None:
    """The real server rejects any request whose params._meta lacks these."""
    meta = params.get("_meta")
    if not isinstance(meta, dict):
        return (
            "params._meta must be an object carrying the required "
            "'io.modelcontextprotocol/protocolVersion' and "
            "'io.modelcontextprotocol/clientCapabilities' envelope keys"
        )
    for key in (
        "io.modelcontextprotocol/protocolVersion",
        "io.modelcontextprotocol/clientCapabilities",
    ):
        if key not in meta:
            return f"params._meta is missing required envelope key '{key}'"
    return None


def _tool_table() -> list[dict]:
    return [
        {
            "name": s["name"],
            "title": s["title"],
            "description": s["description"],
            "inputSchema": {"type": "object", "properties": {}, "title": f"{s['name']}Arguments"},
            "outputSchema": {"type": "object", "additionalProperties": True},
        }
        for s in SERVICES
    ]


def _available(service_key: str) -> bool:
    return HEALTH.get("mcp_worker", 100) > 0 and HEALTH.get(service_key, 100) > 0


def _success(service: dict, attempts: int, key: str) -> dict:
    short = service["name"].replace("order_", "").replace("_inventory", "").replace("send_", "")
    return {
        "service": service["title"],
        "completed": True,
        "response": {short: True},
        "attempts": attempts,
        "workflow_id": f"{service['name']}-{key}",
    }


@app.post("/mcp")
async def mcp(request: Request, authorization: str | None = Header(None)) -> Any:
    if AGENT_TOKEN and authorization != f"Bearer {AGENT_TOKEN}":
        return JSONResponse({"error": "unauthorized"}, status_code=401)

    body = await request.json()
    req_id = body.get("id", 1)
    method = body.get("method")
    params = body.get("params") or {}

    # Enforced on EVERY method, tools/list included — verified against the
    # real server, which is why the agent sends it unconditionally.
    problem = _check_envelope(params)
    if problem:
        return _rpc_error(req_id, -32602, problem)

    if method == "tools/list":
        return JSONResponse({
            "jsonrpc": "2.0", "id": req_id,
            "result": {"cacheScope": "private", "resultType": "complete",
                       "tools": _tool_table(), "ttlMs": 30000},
        })

    if method != "tools/call":
        return _rpc_error(req_id, -32601, f"unknown method {method}")

    name = params.get("name")
    service = BY_NAME.get(name)
    if service is None:
        return _rpc_error(req_id, -32602, f"unknown tool '{name}'")

    key = (params.get("_meta") or {}).get("idempotencyKey") or f"anon-{uuid.uuid4().hex[:8]}"

    # A retried call attaches to the original instead of ordering a second taco.
    if key in SEEN:
        result = dict(SEEN[key])
        return JSONResponse({
            "jsonrpc": "2.0", "id": req_id,
            "result": {"structuredContent": result,
                       "content": [{"type": "text", "text": json.dumps(result)}],
                       "isError": False,
                       "_meta": {"deduplicated": True}},
        })

    LEDGER["ordered"] += 1

    # Fast path: available right now, answer as plain JSON.
    if _available(service["key"]):
        result = _success(service, 1, key)
        SEEN[key] = result
        LEDGER["fulfilled"] += 1
        return JSONResponse({
            "jsonrpc": "2.0", "id": req_id,
            "result": {"structuredContent": result,
                       "content": [{"type": "text", "text": json.dumps(result)}],
                       "isError": False},
        })

    # Slow path: the service is down. Do NOT fail — block, retry, and stream
    # keepalive pings, which is what the real transport does past ~15s.
    return StreamingResponse(
        _retry_stream(service, key, req_id),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


async def _retry_stream(service: dict, key: str, req_id: Any):
    started = time.monotonic()
    attempts = 0
    last_ping = started

    while True:
        attempts += 1
        if _available(service["key"]):
            result = _success(service, attempts, key)
            SEEN[key] = result
            LEDGER["fulfilled"] += 1
            payload = {
                "jsonrpc": "2.0", "id": req_id,
                "result": {"structuredContent": result,
                           "content": [{"type": "text", "text": json.dumps(result)}],
                           "isError": False},
            }
            yield f"data: {json.dumps(payload)}\n\n"
            return

        now = time.monotonic()
        if now - started > MAX_WAIT:
            payload = {
                "jsonrpc": "2.0", "id": req_id,
                "error": {"code": -32000,
                          "message": f"tool call exceeded max wait; workflow "
                                     f"{service['name']}-{key} still running"},
            }
            yield f"data: {json.dumps(payload)}\n\n"
            return

        # Keepalive comments are the only reason a 14-minute call survives a proxy.
        if now - last_ping >= min(SSE_AFTER, 15.0):
            last_ping = now
            yield ": ping\n\n"

        await asyncio.sleep(ATTEMPT_INTERVAL)


# --------------------------------------------------------------------------
# audience board stand-in
# --------------------------------------------------------------------------
@app.get("/chaos")
async def chaos_state() -> dict:
    return HEALTH


@app.get("/ledger")
async def ledger() -> dict:
    """ordered - fulfilled is the gap: how many orders are stuck in retry."""
    return {**LEDGER, "gap": LEDGER["ordered"] - LEDGER["fulfilled"]}


@app.post("/chaos/{component}/tap")
async def chaos_tap(component: str, delta: int = -20) -> dict:
    """One audience tap. Declared before /{action} — FastAPI matches in
    declaration order and the wildcard would otherwise swallow 'tap'."""
    before = HEALTH.get(component, 100)
    HEALTH[component] = max(0, min(100, before + delta))
    if before > 0 and HEALTH[component] == 0:
        DOWN_AT[component] = time.monotonic()
    elif HEALTH[component] > 0:
        DOWN_AT[component] = 0.0
    return HEALTH


@app.post("/chaos/{component}/{action}")
async def chaos_set(component: str, action: str) -> Any:
    """Presenter shortcut. The real audience only ever taps."""
    if action == "down":
        HEALTH[component] = 0
        DOWN_AT[component] = time.monotonic()
    elif action == "up":
        HEALTH[component] = 100
        DOWN_AT[component] = 0.0
    else:
        return JSONResponse({"error": f"unknown action '{action}'"}, status_code=400)
    return HEALTH


@app.post("/reset")
async def reset() -> dict:
    SEEN.clear()
    DOWN_AT.clear()
    LEDGER.update({"ordered": 0, "fulfilled": 0})
    for k in HEALTH:
        HEALTH[k] = 100
    return {"reset": True}


@app.get("/state")
async def state() -> dict:
    return {"health": HEALTH, "ledger": await ledger(), "idempotency_keys_seen": len(SEEN)}
