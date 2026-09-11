"""Adapter for Ian's audience board ("Mise en Panic").

Deliberately tolerant about the switch representation, because it is changing
under us. As deployed on 2026-09-11 the board reports booleans:

    "state": {"mcp": false, "service_1": true, ...}

Ian is mid-change to a tug-of-war: a 0-100 percentage where <50 is down and
>=50 is up, so the room has to battle a switch across the threshold rather than
flip it. When that lands, `state` values become numbers instead of booleans.

`_interpret` handles both, so his deploy is a non-event on our side. Do not
"simplify" it back down to one shape until the threshold version has been live
for a while.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import httpx

# <50 down, >=50 up. Ian's threshold; 50/50 is the resting state and reads UP
# on the board, so the boundary is inclusive.
THRESHOLD = 50


@dataclass
class Component:
    id: str
    name: str
    up: bool
    # None when the board is still reporting booleans — the screen then draws a
    # plain state pill instead of a tug-of-war bar.
    percent: int | None = None
    taps: int = 0
    kind: str = "api"


@dataclass
class BoardState:
    reachable: bool = False
    armed: bool = False
    conns: int = 0
    bus_healthy: bool = True
    components: list[Component] = field(default_factory=list)
    error: str = ""


def _interpret(value: Any) -> tuple[bool, int | None]:
    """Return (up, percent) from whatever the board reported."""
    if isinstance(value, bool):
        return value, None
    if isinstance(value, (int, float)):
        percent = max(0, min(100, int(round(value))))
        return percent >= THRESHOLD, percent
    if isinstance(value, dict):
        # A likely shape for the threshold version: {"up": true, "percent": 62}
        for key in ("percent", "pct", "value", "v"):
            inner = value.get(key)
            if isinstance(inner, (int, float)) and not isinstance(inner, bool):
                percent = max(0, min(100, int(round(inner))))
                up = bool(value.get("up", percent >= THRESHOLD))
                return up, percent
        # A dict we don't recognise: fail open, same as any other unknown
        # shape. Defaulting to down here would paint the screen red on a
        # parsing bug.
        return bool(value.get("up", True)), None
    # Unknown shape: assume healthy rather than painting the screen red on a
    # parsing bug. A false "everything is broken" is the worse failure here.
    return True, None


def parse(payload: dict) -> BoardState:
    names: dict[str, str] = payload.get("names") or {}
    raw_state: dict[str, Any] = payload.get("state") or {}
    taps: dict[str, Any] = payload.get("taps") or {}
    kinds = {s.get("id"): s.get("kind", "api") for s in payload.get("services") or []}

    components: list[Component] = []
    for cid, value in raw_state.items():
        up, percent = _interpret(value)
        tap_count = taps.get(cid)
        components.append(
            Component(
                id=cid,
                name=names.get(cid, cid),
                up=up,
                percent=percent,
                taps=int(tap_count) if isinstance(tap_count, (int, float)) else 0,
                kind=kinds.get(cid, "api"),
            )
        )

    # mcp first, then service_1..N — the order the call path runs in.
    components.sort(key=lambda c: (c.id != "mcp", c.id))

    bus = payload.get("bus") or {}
    return BoardState(
        reachable=True,
        armed=bool(payload.get("armed", False)),
        conns=int(payload.get("conns", 0) or 0),
        bus_healthy=bool(bus.get("healthy", True)),
        components=components,
    )


async def fetch(client: httpx.AsyncClient, base_url: str, token: str) -> BoardState:
    """Poll the board. Never raises — an unreachable board must not take the
    screen down mid-talk, it just greys that panel out."""
    if not base_url or not token:
        return BoardState(error="board URL or admin token not configured")
    try:
        response = await client.get(
            f"{base_url.rstrip('/')}/api/admin/state",
            headers={"Authorization": f"Bearer {token}"},
            timeout=5.0,
        )
        if response.status_code == 401:
            return BoardState(error="board rejected the admin token (401)")
        response.raise_for_status()
        return parse(response.json())
    except Exception as exc:  # noqa: BLE001 - the screen degrades, it never dies
        return BoardState(error=f"board unreachable: {type(exc).__name__}")
