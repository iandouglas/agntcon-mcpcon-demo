"""A stand-in for Richard's MCP server.

Exists so the agent can be built and broken before the real server is ready.
It is NOT the real thing and makes no claim to be — no Temporal, no workflow
per tool call, dedup faked with a dict. Delete it once the real endpoint is up.

It does implement the one contract that matters, so the agent is developed
against the right shape from day one:

    out of stock  -> 200, isError: true   (a business outcome the model reads)
    server down   -> 503                   (a fault Temporal retries)

Run:  uv run uvicorn fake_mcp.server:app --port 8931
Break: curl -X POST localhost:8931/chaos/mcp_worker/down
Fix:   curl -X POST localhost:8931/chaos/mcp_worker/up
"""

from __future__ import annotations

import asyncio
import os
import time
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Header, Request
from fastapi.responses import JSONResponse

# Components that die and get restarted by a supervisor, rather than just
# refusing work. A hard break has to heal on its own or the demo stalls: the
# audience gets to kill the worker, not to keep it dead forever.
HARD_BREAK = {"agent_worker", "mcp_worker"}
RESTART_SECONDS = float(os.getenv("FAKE_RESTART_SECONDS", "5.0"))


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(_supervisor())
    yield
    task.cancel()


async def _supervisor() -> None:
    """Bring downed workers back a few seconds later.

    Stands in for a docker restart policy. Health comes back low, not full, so
    a couple of breakers can put it straight back down — which is the fight we
    want, rather than a component that's either pristine or permanently dead.
    """
    while True:
        await asyncio.sleep(0.5)
        now = time.monotonic()
        for name in HARD_BREAK:
            if HEALTH.get(name, 100) <= 0 and DOWN_AT.get(name):
                if now - DOWN_AT[name] >= RESTART_SECONDS:
                    HEALTH[name] = 30
                    DOWN_AT[name] = 0.0


app = FastAPI(title="fake Richard's MCP", lifespan=lifespan)

# --- mutable world state --------------------------------------------------
STATE: dict[str, Any] = {
    "chicken_lb": 2.0,          # deliberately short — this is the branch point
    "orders": {},
    "payments": {},
    "receipts": {},
}

# Component health, mirroring what the real chaos feed will provide.
# agent_worker and llm_api are not this server's business — it just carries
# their health so the agent's watchdog and LLM proxy have somewhere to read it
# until the real audience site exists.
HEALTH: dict[str, int] = {
    "agent_worker": 100,
    "llm_api": 100,
    "mcp_worker": 100,
    "payment_api": 100,
    "email_api": 100,
    "inventory_api": 100,
}

# monotonic timestamp a hard-break component went down, for the supervisor.
DOWN_AT: dict[str, float] = {}

# idempotency_key -> the result we already produced for it.
SEEN: dict[str, dict] = {}

SPECIALS = {
    0: "Menudo Monday", 1: "Taco Tuesday", 2: "Chile Relleno Wednesday",
    3: "Tinga Thursday", 4: "Fajita Friday", 5: "Sopes Saturday", 6: "Sunday Pozole",
}

TOOLS = [
    {
        "name": "get_specials",
        "description": "Today's special at Richard's. Call this before ordering.",
        "inputSchema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "order_a_taco",
        "description": (
            "Place a taco order. Returns an error result if an ingredient is out "
            "of stock — order more inventory, then try again."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "item": {"type": "string"},
                "quantity": {"type": "integer"},
            },
            "required": ["item", "quantity"],
        },
    },
    {
        "name": "order_more_inventory",
        "description": "Order more of an ingredient from the supplier. Waits for delivery.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "ingredient": {"type": "string"},
                "pounds": {"type": "number"},
            },
            "required": ["ingredient", "pounds"],
        },
    },
    {
        "name": "take_payment",
        "description": "Charge the customer. IRREVERSIBLE — call exactly once per order.",
        "inputSchema": {
            "type": "object",
            "properties": {"order_id": {"type": "string"}, "amount": {"type": "number"}},
            "required": ["order_id", "amount"],
        },
    },
    {
        "name": "email_receipt",
        "description": "Email the receipt. IRREVERSIBLE — call exactly once per order.",
        "inputSchema": {
            "type": "object",
            "properties": {"order_id": {"type": "string"}, "email": {"type": "string"}},
            "required": ["order_id", "email"],
        },
    },
]

TOOL_COMPONENT = {
    "take_payment": "payment_api",
    "email_receipt": "email_api",
    "order_more_inventory": "inventory_api",
}


def _ok(text: str, meta: dict | None = None) -> dict:
    result: dict[str, Any] = {"content": [{"type": "text", "text": text}], "isError": False}
    if meta:
        result["_meta"] = meta
    return result


def _business_error(text: str) -> dict:
    """A tool that ran and has bad news. 200 OK, isError true — the model's problem."""
    return {"content": [{"type": "text", "text": text}], "isError": True}


async def _run_tool(name: str, args: dict) -> dict:
    if name == "get_specials":
        # Fixed day so the demo says the same thing at every rehearsal.
        return _ok(f"Today is {SPECIALS[4]}. Fajita plate is $6.50.")

    if name == "order_a_taco":
        qty = int(args.get("quantity", 1))
        needed = qty * 0.25
        if STATE["chicken_lb"] < needed:
            return _business_error(
                f"OUT_OF_STOCK: chicken. Need {needed:.2f} lb, have "
                f"{STATE['chicken_lb']:.2f} lb. Order more inventory first."
            )
        STATE["chicken_lb"] -= needed
        order_id = f"4471"
        STATE["orders"][order_id] = {"item": args.get("item"), "quantity": qty}
        return _ok(f"Order {order_id} placed: {qty} x {args.get('item')}. Total $14.20.")

    if name == "order_more_inventory":
        pounds = float(args.get("pounds", 10))
        await asyncio.sleep(2.0)  # the delivery truck
        STATE["chicken_lb"] += pounds
        return _ok(f"Delivered {pounds:.0f} lb of {args.get('ingredient')}.")

    if name == "take_payment":
        order_id = str(args.get("order_id", "4471"))
        STATE["payments"][order_id] = args.get("amount")
        return _ok(f"Charged ${args.get('amount')} for order {order_id}.")

    if name == "email_receipt":
        order_id = str(args.get("order_id", "4471"))
        STATE["receipts"][order_id] = args.get("email")
        return _ok(f"Receipt for order {order_id} emailed.")

    return _business_error(f"UNKNOWN_TOOL: {name}")


@app.post("/mcp")
async def mcp(request: Request, idempotency_key: str | None = Header(None)) -> JSONResponse:
    body = await request.json()
    method = body.get("method")
    req_id = body.get("id", 1)

    if method == "tools/list":
        return JSONResponse({"jsonrpc": "2.0", "id": req_id, "result": {"tools": TOOLS}})

    if method != "tools/call":
        return JSONResponse(
            {"jsonrpc": "2.0", "id": req_id,
             "error": {"code": -32601, "message": f"unknown method {method}"}}
        )

    params = body.get("params") or {}
    name = params.get("name")
    args = params.get("arguments") or {}
    key = idempotency_key or (params.get("_meta") or {}).get("idempotencyKey") or ""

    # --- infrastructure failures: 503, the agent must not "handle" these ---
    if HEALTH.get("mcp_worker", 100) <= 0:
        return JSONResponse({"error": "mcp worker down"}, status_code=503)
    component = TOOL_COMPONENT.get(name)
    if component and HEALTH.get(component, 100) <= 0:
        return JSONResponse({"error": f"{component} unreachable"}, status_code=503)

    # --- idempotency: a retried call attaches, it does not run again -------
    if key and key in SEEN:
        cached = dict(SEEN[key])
        cached["_meta"] = {**(cached.get("_meta") or {}), "deduplicated": True}
        return JSONResponse({"jsonrpc": "2.0", "id": req_id, "result": cached})

    result = await _run_tool(name, args)
    # Only successful, irreversible work is worth remembering. Caching a
    # business error would stop the agent from retrying after it restocks.
    if key and not result.get("isError"):
        SEEN[key] = result

    return JSONResponse({"jsonrpc": "2.0", "id": req_id, "result": result})


# --- chaos controls, standing in for the real audience site ---------------
@app.get("/chaos")
async def chaos_state() -> dict:
    return HEALTH


@app.post("/chaos/{component}/tap")
async def chaos_tap(component: str, delta: int = -20) -> dict:
    """One audience tap. Negative damages, positive repairs.

    Declared BEFORE the /{action} route below — FastAPI matches in declaration
    order, so the wildcard would otherwise swallow "tap" as an action and snap
    health to 100 instead of nudging it.
    """
    before = HEALTH.get(component, 100)
    HEALTH[component] = max(0, min(100, before + delta))
    if before > 0 and HEALTH[component] == 0:
        DOWN_AT[component] = time.monotonic()
    elif HEALTH[component] > 0:
        DOWN_AT[component] = 0.0
    return HEALTH


@app.post("/chaos/{component}/{action}")
async def chaos_set(component: str, action: str) -> dict:
    """Presenter shortcut: down/up snaps health to 0/100 in one call.

    The audience never uses this — a real room only ever taps. Keep it for
    rehearsal and for the fallback controls if conference wifi collapses.
    """
    if action == "down":
        HEALTH[component] = 0
        DOWN_AT[component] = time.monotonic()
    elif action == "up":
        HEALTH[component] = 100
        DOWN_AT[component] = 0.0
    else:
        return JSONResponse(
            {"error": f"unknown action '{action}' — use down, up or tap"}, status_code=400
        )
    return HEALTH


@app.post("/reset")
async def reset() -> dict:
    STATE.update({"chicken_lb": 2.0, "orders": {}, "payments": {}, "receipts": {}})
    SEEN.clear()
    for k in HEALTH:
        HEALTH[k] = 100
    DOWN_AT.clear()
    return {"reset": True}


@app.get("/state")
async def state() -> dict:
    return {**STATE, "health": HEALTH, "idempotency_keys_seen": len(SEEN)}
